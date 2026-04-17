//! Substep 3.3 of `convex-native/STATUS.md` — churn-tolerant
//! `WorkerPool`.
//!
//! Replaces the Phase-2 `Vec<Arc<dyn WorkerClient>>` fixed pool
//! that `DistributedFunctionRunner::new` takes with a dynamic
//! `WorkerPool` that admits + retires workers at arbitrary times
//! during the backend's lifetime. The admission server
//! (substep 3.4) feeds it; the `FunctionRunner` impl
//! (substep 3.6) reads from it.
//!
//! The Phase-2 `DistributedFunctionRunner` stays — it becomes a
//! specialisation of this: a `WorkerPool` seeded at construction
//! with one entry per `CONVEX_NATIVE_WORKERS` endpoint,
//! equivalent to "admit these N workers, never retire". The
//! new type is additive.
//!
//! ## Semantics
//!
//! - **Stable ids.** Each admitted worker gets a `WorkerId(u64)` handed out in
//!   admission order. The id is stable for that worker's lifetime in the pool —
//!   a single worker restarting produces a **new** id even if it comes back
//!   with the same endpoint.
//! - **by_function index.** The pool maintains a `name → Vec<WorkerId>` map
//!   derived from each worker's advertised inventory. Dispatch picks from the
//!   list for the requested function; empty list means 503 (substep 3.7).
//! - **Registry-version floor.** The operator-settable floor (Phase 4.7 from
//!   the plan) steers traffic away from stragglers during a rolling deploy.
//!   Workers below the floor aren't considered for dispatch even if their name
//!   is in the index.
//! - **No implicit P2C here.** The pool returns a slice of eligible worker ids;
//!   the caller (substep 3.6) picks one via the existing `Chooser` trait and
//!   in-flight estimate. Keeping the pool ignorant of dispatch policy means new
//!   strategies can land without touching the pool.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::{
        atomic::{
            AtomicU64,
            Ordering,
        },
        Arc,
    },
};

use parking_lot::RwLock;

use crate::client::WorkerClient;

/// Stable handle for a worker's membership in the pool. Newly
/// admitted workers get a fresh id — a restart does **not**
/// resurrect the previous id.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerId(pub u64);

/// Runtime kind the worker is using. Mirrors
/// `pb::worker_admission::WorkerKind` but tonic-free so the
/// pool doesn't leak generated proto types into the dispatch
/// surface. Substep 6.1 of `convex-native/STATUS.md`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum WorkerKind {
    /// The default for Phase 3..5 native workers — no explicit
    /// kind was set in the envelope (treated as native Rust for
    /// backward compatibility).
    Unspecified,
    /// Worker is built on `convex_native_distributed`'s native
    /// Rust runner.
    NativeRust,
    /// Worker is built on the V8 isolate farm. Phase 6.
    Javascript,
}

impl WorkerKind {
    /// Decode a proto enum-int into the native variant. Unknown
    /// integers fall back to `Unspecified` — the wire contract
    /// pins the known values, but a forward-compatible
    /// admission handler tolerates unknowns rather than bailing.
    pub fn from_proto_i32(v: i32) -> Self {
        // Match the proto values without importing `pb` here —
        // keeps this module free of the generated types.
        match v {
            1 => Self::NativeRust,
            2 => Self::Javascript,
            _ => Self::Unspecified,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::NativeRust => "native-rust",
            Self::Javascript => "javascript",
        }
    }
}

/// One entry in the pool. Carries the transport client + the
/// worker's advertised registration metadata. Substep 3.4 grows
/// this with status-update fields (`in_flight`, `cpu_percent`,
/// last-heartbeat timestamp) so the dispatcher can skip workers
/// whose stream is stale.
pub struct WorkerEntry {
    /// Transport handle the dispatcher uses for function calls.
    pub client: Arc<dyn WorkerClient>,
    /// `registry_version` the worker advertised at admission
    /// time. Used to gate dispatch via the pool-wide floor.
    pub registry_version: String,
    /// Dotted function names the worker advertised. Kept here so
    /// retirement can remove the worker from the `by_function`
    /// index without re-parsing the envelope.
    pub functions: Vec<String>,
    /// Runtime kind the worker advertised. Populated from the
    /// admission envelope's `WorkerKind` field — defaults to
    /// `Unspecified` for workers that didn't set it. Substep
    /// 6.1 of `convex-native/STATUS.md`; dispatch routing
    /// currently treats all kinds as interchangeable from
    /// `eligible_for`'s perspective, but operator dashboards
    /// use this to show the NativeRust vs JS mix.
    pub kind: WorkerKind,
}

/// Dynamic pool of workers. Thread-safe via a single `RwLock`
/// on the inner state — the admission rate is tiny (seconds
/// between admits/retires on a real pool) so a coarser lock
/// than `DashMap` is simpler and sufficient. Upgrade to sharded
/// maps if the pool ever exceeds a few hundred members.
pub struct WorkerPool {
    inner: RwLock<PoolInner>,
    next_id: AtomicU64,
}

struct PoolInner {
    workers: HashMap<WorkerId, WorkerEntry>,
    /// Dispatch-time name → eligible worker ids lookup.
    /// Invariants:
    /// - Every `WorkerId` in any value list is also a key in `workers`.
    /// - `admit` appends to each of the worker's advertised function lists;
    ///   `retire` removes the id from every list it appears in (and drops empty
    ///   lists).
    by_function: HashMap<String, Vec<WorkerId>>,
    /// `min_registry_version` floor. Workers whose
    /// `registry_version` doesn't meet the floor are skipped in
    /// `eligible_for`. `None` means no floor.
    min_registry_version: Option<String>,
    /// Substep 6.2 of `convex-native/STATUS.md` — per-function
    /// routing preference by runtime kind. When set for a
    /// function name, `eligible_for(name)` returns only
    /// workers of the preferred kind **unless** the preferred
    /// set is empty, in which case it falls back to the
    /// unfiltered set (so a preference is a soft routing hint,
    /// not a hard requirement — the dispatcher never 503s on a
    /// preference mismatch when a fallback exists). Default
    /// empty map ⇒ kind-agnostic routing.
    kind_preferences: HashMap<String, WorkerKind>,
}

impl Default for WorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerPool {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(PoolInner {
                workers: HashMap::new(),
                by_function: HashMap::new(),
                min_registry_version: None,
                kind_preferences: HashMap::new(),
            }),
            next_id: AtomicU64::new(0),
        }
    }

    /// Admit a worker. Returns the newly allocated `WorkerId`.
    /// The worker is immediately eligible for dispatch on every
    /// function name it advertised.
    pub fn admit(&self, entry: WorkerEntry) -> WorkerId {
        let id = WorkerId(self.next_id.fetch_add(1, Ordering::SeqCst));
        let mut inner = self.inner.write();
        for name in &entry.functions {
            inner.by_function.entry(name.clone()).or_default().push(id);
        }
        inner.workers.insert(id, entry);
        id
    }

    /// Retire a worker. Removes it from the pool and from every
    /// function list it appears in. Returns `true` when the id
    /// was present; `false` for a double-retirement (idempotent).
    pub fn retire(&self, id: WorkerId) -> bool {
        let mut inner = self.inner.write();
        let Some(entry) = inner.workers.remove(&id) else {
            return false;
        };
        for name in &entry.functions {
            if let Some(list) = inner.by_function.get_mut(name) {
                list.retain(|&wid| wid != id);
            }
        }
        inner.by_function.retain(|_, list| !list.is_empty());
        true
    }

    pub fn len(&self) -> usize {
        self.inner.read().workers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().workers.is_empty()
    }

    /// Set the operator-level `min_registry_version` floor.
    /// `None` clears the floor.
    pub fn set_min_registry_version(&self, floor: Option<String>) {
        self.inner.write().min_registry_version = floor;
    }

    /// Snapshot the current floor.
    pub fn min_registry_version(&self) -> Option<String> {
        self.inner.read().min_registry_version.clone()
    }

    /// Look up the workers eligible to serve `function_name`.
    /// Filters out workers whose `registry_version` doesn't meet
    /// the pool-wide floor. Returns each eligible worker's
    /// `(WorkerId, Arc<dyn WorkerClient>)` so the caller can
    /// run P2C / failover without re-entering the pool.
    ///
    /// Substep 6.2 layers an optional per-function kind
    /// preference on top: when
    /// `kind_preferences[function_name]` is set and at least
    /// one worker of that kind serves the name, the result is
    /// restricted to that kind. If no worker of the preferred
    /// kind is present the dispatcher falls back to the full
    /// floor-filtered set — a preference is a soft hint, not
    /// a hard filter (avoids turning a preference mis-config
    /// into a 503).
    pub fn eligible_for(&self, function_name: &str) -> Vec<(WorkerId, Arc<dyn WorkerClient>)> {
        let inner = self.inner.read();
        let Some(ids) = inner.by_function.get(function_name) else {
            return Vec::new();
        };
        let floor = inner.min_registry_version.as_deref();
        let base: Vec<(WorkerId, Arc<dyn WorkerClient>, WorkerKind)> = ids
            .iter()
            .filter_map(|id| {
                let entry = inner.workers.get(id)?;
                if let Some(f) = floor
                    && !meets_floor(&entry.registry_version, f)
                {
                    return None;
                }
                Some((*id, entry.client.clone(), entry.kind))
            })
            .collect();
        // Apply the per-function kind preference if one is set,
        // soft-falling-back to the full set when the preferred
        // slice is empty.
        if let Some(&pref) = inner.kind_preferences.get(function_name) {
            let preferred: Vec<(WorkerId, Arc<dyn WorkerClient>)> = base
                .iter()
                .filter(|(_, _, k)| *k == pref)
                .map(|(id, c, _)| (*id, c.clone()))
                .collect();
            if !preferred.is_empty() {
                return preferred;
            }
        }
        base.into_iter().map(|(id, c, _)| (id, c)).collect()
    }

    /// Substep 6.2: pin a routing preference for `function_name`
    /// so dispatches for that name prefer workers of `kind`
    /// when at least one is available. Passing the same name
    /// again overwrites the prior setting.
    pub fn set_kind_preference(&self, function_name: impl Into<String>, kind: WorkerKind) {
        self.inner
            .write()
            .kind_preferences
            .insert(function_name.into(), kind);
    }

    /// Clear a per-function kind preference.
    pub fn clear_kind_preference(&self, function_name: &str) {
        self.inner.write().kind_preferences.remove(function_name);
    }

    /// Snapshot the current per-function kind preferences for
    /// operator dashboards (and tests). Ordered by function
    /// name via `BTreeMap` so the output is deterministic.
    pub fn kind_preferences(&self) -> BTreeMap<String, WorkerKind> {
        self.inner
            .read()
            .kind_preferences
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Debug-friendly snapshot — `registry_version → count` of
    /// workers currently in the pool. Used by the Phase-7
    /// operator dashboards (and by tests here).
    pub fn by_version(&self) -> BTreeMap<String, usize> {
        let inner = self.inner.read();
        let mut out = BTreeMap::new();
        for entry in inner.workers.values() {
            *out.entry(entry.registry_version.clone()).or_default() += 1;
        }
        out
    }

    /// Substep 6.1 of `convex-native/STATUS.md` — snapshot
    /// worker counts grouped by `WorkerKind`. Shape matches
    /// `by_version()` so operator tooling can render either
    /// grouping through a single helper.
    pub fn by_kind(&self) -> BTreeMap<WorkerKind, usize> {
        let inner = self.inner.read();
        let mut out = BTreeMap::new();
        for entry in inner.workers.values() {
            *out.entry(entry.kind).or_default() += 1;
        }
        out
    }
}

/// Lexicographic parts comparison — same shape as the version
/// gate on `FunctionExecutionServer` in `server.rs`. Keeps the
/// two floor checks consistent: worker accepts its own request
/// iff the pool admits its dispatch.
fn meets_floor(have: &str, want: &str) -> bool {
    fn parts(s: &str) -> Vec<u64> {
        s.split(['.', '-', '+'])
            .filter_map(|p| p.parse::<u64>().ok())
            .collect()
    }
    parts(have) >= parts(want)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use async_trait::async_trait;
    use common::types::UdfType;
    use convex_native::distributed::{
        ExecuteRequest,
        ExecuteResponse,
    };
    use pb::function_execution as proto;
    use tonic::Status;

    use super::*;

    /// Stub client that reports a constant in_flight and label.
    /// Matches the `WorkerClient` trait surface but never makes a
    /// network call — the pool only cares about `label()` and
    /// doesn't inspect the handler.
    struct StubClient {
        label: String,
    }

    #[async_trait]
    impl WorkerClient for StubClient {
        async fn execute(
            &self,
            _req: ExecuteRequest,
            _udf_type: UdfType,
        ) -> Result<ExecuteResponse, Status> {
            Err(Status::unimplemented("StubClient::execute"))
        }

        async fn health(&self) -> Result<proto::HealthResponse, Status> {
            Err(Status::unimplemented("StubClient::health"))
        }

        fn in_flight_estimate(&self) -> u64 {
            0
        }

        fn label(&self) -> &str {
            &self.label
        }
    }

    fn stub(label: &str) -> Arc<dyn WorkerClient> {
        Arc::new(StubClient {
            label: label.to_string(),
        })
    }

    fn entry(label: &str, version: &str, functions: &[&str]) -> WorkerEntry {
        WorkerEntry {
            client: stub(label),
            registry_version: version.to_string(),
            functions: functions.iter().map(|s| s.to_string()).collect(),
            kind: WorkerKind::NativeRust,
        }
    }

    #[test]
    fn admit_assigns_stable_ids_in_order() {
        // Pool hands out `WorkerId(0)`, `WorkerId(1)`, … in
        // admission order. Restarting a worker (admit → retire →
        // admit again) must produce a **new** id, not reuse the
        // old one — otherwise in-flight dispatches could race
        // against a stale client.
        let pool = WorkerPool::new();
        let a = pool.admit(entry("a", "1.0", &["get"]));
        let b = pool.admit(entry("b", "1.0", &["get"]));
        assert_eq!(a, WorkerId(0));
        assert_eq!(b, WorkerId(1));
        pool.retire(a);
        let a2 = pool.admit(entry("a", "1.0", &["get"]));
        assert_eq!(a2, WorkerId(2), "restart = fresh id");
    }

    #[test]
    fn eligible_for_returns_workers_serving_the_function() {
        let pool = WorkerPool::new();
        pool.admit(entry("w1", "1.0", &["get", "list"]));
        pool.admit(entry("w2", "1.0", &["get"]));
        pool.admit(entry("w3", "1.0", &["unrelated"]));

        let eligible_get: Vec<_> = pool
            .eligible_for("get")
            .into_iter()
            .map(|(_, c)| c.label().to_string())
            .collect();
        assert_eq!(eligible_get.len(), 2);
        let mut names = eligible_get;
        names.sort();
        assert_eq!(names, vec!["w1".to_string(), "w2".to_string()]);

        let eligible_list: Vec<_> = pool
            .eligible_for("list")
            .into_iter()
            .map(|(_, c)| c.label().to_string())
            .collect();
        assert_eq!(eligible_list, vec!["w1".to_string()]);

        assert!(pool.eligible_for("missing").is_empty());
    }

    #[test]
    fn retire_removes_from_both_maps() {
        let pool = WorkerPool::new();
        let id = pool.admit(entry("w", "1.0", &["get"]));
        assert_eq!(pool.eligible_for("get").len(), 1);
        assert!(pool.retire(id));
        assert_eq!(pool.len(), 0);
        assert!(
            pool.eligible_for("get").is_empty(),
            "retired worker drops out of the by_function index",
        );
        // Idempotent retire.
        assert!(!pool.retire(id));
    }

    #[test]
    fn floor_filters_eligible_set() {
        let pool = WorkerPool::new();
        pool.admit(entry("old", "1.0.0", &["get"]));
        pool.admit(entry("new", "1.2.0", &["get"]));

        // No floor → both eligible.
        assert_eq!(pool.eligible_for("get").len(), 2);

        // Floor at 1.1.0 → only `new` survives.
        pool.set_min_registry_version(Some("1.1.0".to_string()));
        let eligible: Vec<_> = pool
            .eligible_for("get")
            .into_iter()
            .map(|(_, c)| c.label().to_string())
            .collect();
        assert_eq!(eligible, vec!["new".to_string()]);

        // Clearing the floor → both back.
        pool.set_min_registry_version(None);
        assert_eq!(pool.eligible_for("get").len(), 2);
    }

    fn js_entry(label: &str, version: &str, functions: &[&str]) -> WorkerEntry {
        WorkerEntry {
            client: stub(label),
            registry_version: version.to_string(),
            functions: functions.iter().map(|s| s.to_string()).collect(),
            kind: WorkerKind::Javascript,
        }
    }

    #[test]
    fn kind_preference_routes_to_preferred_kind_when_available() {
        // Substep 6.2: a preference for `compute_heavy = NativeRust`
        // filters dispatch to the native-Rust worker even
        // though a JS worker also serves the name.
        let pool = WorkerPool::new();
        pool.admit(entry("rust-1", "1.0.0", &["compute_heavy"]));
        pool.admit(js_entry("js-1", "1.0.0", &["compute_heavy"]));
        pool.set_kind_preference("compute_heavy", WorkerKind::NativeRust);
        let eligible: Vec<_> = pool
            .eligible_for("compute_heavy")
            .into_iter()
            .map(|(_, c)| c.label().to_string())
            .collect();
        assert_eq!(eligible, vec!["rust-1".to_string()]);
    }

    #[test]
    fn kind_preference_falls_back_to_full_set_when_preferred_absent() {
        // Substep 6.2: preference is a soft hint — if the
        // preferred kind isn't available, fall back to any
        // worker serving the name so we don't 503 on a
        // preference mis-config.
        let pool = WorkerPool::new();
        pool.admit(js_entry("js-only", "1.0.0", &["transform"]));
        pool.set_kind_preference("transform", WorkerKind::NativeRust);
        let eligible: Vec<_> = pool
            .eligible_for("transform")
            .into_iter()
            .map(|(_, c)| c.label().to_string())
            .collect();
        assert_eq!(
            eligible,
            vec!["js-only".to_string()],
            "no NativeRust worker ⇒ fall back to the JS one; preference is a soft hint",
        );
    }

    #[test]
    fn kind_preference_ignored_for_unlisted_function() {
        // A preference for one function name doesn't leak into
        // routing for other names.
        let pool = WorkerPool::new();
        pool.admit(entry("rust-1", "1.0.0", &["compute_heavy", "cheap"]));
        pool.admit(js_entry("js-1", "1.0.0", &["cheap"]));
        pool.set_kind_preference("compute_heavy", WorkerKind::NativeRust);
        let eligible: Vec<_> = pool
            .eligible_for("cheap")
            .into_iter()
            .map(|(_, c)| c.label().to_string())
            .collect();
        assert_eq!(
            eligible.len(),
            2,
            "no preference set for \"cheap\" ⇒ both kinds are eligible: {eligible:?}",
        );
    }

    #[test]
    fn kind_preference_clearable() {
        let pool = WorkerPool::new();
        pool.admit(entry("rust", "1.0.0", &["x"]));
        pool.admit(js_entry("js", "1.0.0", &["x"]));
        pool.set_kind_preference("x", WorkerKind::NativeRust);
        assert_eq!(pool.eligible_for("x").len(), 1);
        pool.clear_kind_preference("x");
        assert_eq!(pool.eligible_for("x").len(), 2);
    }

    #[test]
    fn by_kind_groups_native_and_js_workers() {
        // Substep 6.1: operator dashboards need to see the
        // Rust vs JS worker mix at a glance.
        // `by_kind()` produces a `kind → count` snapshot
        // directly from the pool's entries.
        let pool = WorkerPool::new();
        pool.admit(WorkerEntry {
            client: stub("a"),
            registry_version: "1.0".to_string(),
            functions: vec!["get".to_string()],
            kind: WorkerKind::NativeRust,
        });
        pool.admit(WorkerEntry {
            client: stub("b"),
            registry_version: "1.0".to_string(),
            functions: vec!["list".to_string()],
            kind: WorkerKind::Javascript,
        });
        pool.admit(WorkerEntry {
            client: stub("c"),
            registry_version: "1.0".to_string(),
            functions: vec!["crunch".to_string()],
            kind: WorkerKind::NativeRust,
        });
        let mix = pool.by_kind();
        assert_eq!(mix.get(&WorkerKind::NativeRust), Some(&2));
        assert_eq!(mix.get(&WorkerKind::Javascript), Some(&1));
    }

    #[test]
    fn worker_kind_from_proto_i32_handles_known_and_unknown() {
        assert_eq!(WorkerKind::from_proto_i32(0), WorkerKind::Unspecified);
        assert_eq!(WorkerKind::from_proto_i32(1), WorkerKind::NativeRust);
        assert_eq!(WorkerKind::from_proto_i32(2), WorkerKind::Javascript);
        // Forward-compat: an unknown proto value (e.g. a future
        // kind enum member this binary doesn't know about) maps
        // to `Unspecified` rather than panicking.
        assert_eq!(WorkerKind::from_proto_i32(99), WorkerKind::Unspecified);
    }

    #[test]
    fn by_version_groups_workers_for_observability() {
        // Substep 3.3 + Phase-7 dashboard shape: pool exposes
        // a count-by-registry-version snapshot so operators can
        // tell at-a-glance how the rolling update is going.
        let pool = WorkerPool::new();
        pool.admit(entry("a", "1.0.0", &["get"]));
        pool.admit(entry("b", "1.0.0", &["get"]));
        pool.admit(entry("c", "1.1.0", &["get"]));
        let groups = pool.by_version();
        assert_eq!(groups.get("1.0.0"), Some(&2));
        assert_eq!(groups.get("1.1.0"), Some(&1));
    }

    // Silence "unused import" on an internal helper used only
    // by the in-file StubClient.
    const _: fn() = || {
        let _ = AtomicU64::new(0);
    };
}
