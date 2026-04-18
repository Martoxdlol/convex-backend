//! Native cron driver.
//!
//! Handlers tagged `#[convex::cron(name, schedule, target)]` land in
//! the `inventory`-collected `CronRegistry`. Nothing in the repo
//! previously drove those schedules — the admission envelope shipped
//! cron registrations to the backend but the backend dropped them on
//! the floor. This module turns the registrations into an actually-
//! firing schedule: parse each cron expression with `saffron`, compute
//! `next_after(now)`, sleep until that instant, dispatch the target
//! function through the configured runner, repeat.
//!
//! ## Semantics
//!
//! - **Schedules are parsed once at boot** (or on the first admission that
//!   introduces a new cron) and re-used. The macro's build-time validation
//!   already guarantees the expression parses — still, the driver bails loudly
//!   if parsing fails so a schema drift surfaces immediately.
//! - **Missed fires are skipped.** If the backend is down when a cron would
//!   have fired, `next_after(now)` returns the *next* occurrence, not the one
//!   it missed. Matches the JS cron worker's catch-up semantics — crons are
//!   at-most-once, not exactly-once.
//! - **Overlapping runs are avoided**: each cron entry serialises through its
//!   own task so a slow handler can't fire twice concurrently.
//! - **Target kind aware**: `target_kind = "mutation"` dispatches as
//!   `UdfType::Mutation` through the runner, `target_kind = "action"` as
//!   `UdfType::Action`. Anything else fails to install with a clear error.
//!
//! ## Wire-up
//!
//! A `NativeCronDriver` is built from either the backend's
//! `CronRegistry::collect()` (monolith; the backend image carries the
//! handlers locally) or from the union of worker-advertised
//! registrations on an `Arc<WorkerPool>` (distributed; each worker
//! admission delivers its own `CronRegistration` list).
//!
//! Monolith uses `Arc<NativeFunctionRunner>` to dispatch in-process;
//! distributed uses a `CronDispatcher` trait so the backend can plug
//! in `PoolFunctionRunner`-backed gRPC dispatch.

use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{
    DateTime,
    Utc,
};
use parking_lot::Mutex;
use saffron::Cron;
use value::{
    ConvexObject,
    TableNamespace,
};

/// Descriptor for one cron job the driver knows about.
#[derive(Clone)]
pub struct CronJob {
    pub name: String,
    pub schedule_expr: String,
    pub target: String,
    pub target_kind: CronTargetKind,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CronTargetKind {
    Mutation,
    Action,
}

impl CronTargetKind {
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "mutation" => Ok(Self::Mutation),
            "action" => Ok(Self::Action),
            other => {
                anyhow::bail!("cron target_kind {other:?}: expected \"mutation\" or \"action\"")
            },
        }
    }
}

/// Abstract dispatcher the driver calls when a schedule fires.
/// Implementations wrap either an in-process `NativeFunctionRunner`
/// (monolith) or a `PoolFunctionRunner` (distributed).
#[async_trait]
pub trait CronDispatcher: Send + Sync + 'static {
    /// Fire the cron target. `kind` tells the implementation
    /// whether to dispatch as a mutation or an action; `name` is
    /// the target function's bare identifier (matches the native
    /// registry key).
    async fn fire(&self, name: &str, kind: CronTargetKind) -> anyhow::Result<()>;
}

/// In-process dispatcher. Used when the backend image carries the
/// native handlers itself (monolith) so the cron fire path stays
/// cheap and avoids a gRPC round-trip.
pub struct InProcessDispatcher {
    runner: Arc<convex_native_core::NativeFunctionRunner>,
    database: database::Database<runtime::prod::ProdRuntime>,
}

impl InProcessDispatcher {
    pub fn new(
        runner: Arc<convex_native_core::NativeFunctionRunner>,
        database: database::Database<runtime::prod::ProdRuntime>,
    ) -> Self {
        Self { runner, database }
    }
}

#[async_trait]
impl CronDispatcher for InProcessDispatcher {
    async fn fire(&self, name: &str, kind: CronTargetKind) -> anyhow::Result<()> {
        let args = ConvexObject::empty();
        match kind {
            CronTargetKind::Mutation => {
                let usage = usage_tracking::FunctionUsageTracker::new();
                let mut tx = self
                    .database
                    .begin_with_ts(
                        keybroker::Identity::system(),
                        *self.database.now_ts_for_reads(),
                        usage,
                    )
                    .await?;
                self.runner
                    .run_mutation(name, &mut tx, TableNamespace::Global, args)
                    .await?;
                self.database
                    .commit_with_write_source(tx, database::WriteSource::system("native_cron"))
                    .await?;
                Ok(())
            },
            CronTargetKind::Action => {
                let callbacks: Arc<dyn convex_native_core::callbacks::NativeActionCallbacks> =
                    Arc::new(convex_native_core::callbacks::NoopCallbacks);
                self.runner
                    .run_action_with_callbacks(name, TableNamespace::Global, args, callbacks)
                    .await?;
                Ok(())
            },
        }
    }
}

/// Pool-backed dispatcher for the distributed topology. Fires a
/// cron by dispatching the target through the first eligible
/// worker in `WorkerPool::eligible_for(name)`. Used when the
/// backend image carries no native handlers and the admission
/// pool owns them.
pub struct PoolDispatcher {
    pool: Arc<crate::pool::WorkerPool>,
}

impl PoolDispatcher {
    pub fn new(pool: Arc<crate::pool::WorkerPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CronDispatcher for PoolDispatcher {
    async fn fire(&self, name: &str, kind: CronTargetKind) -> anyhow::Result<()> {
        let eligible = self.pool.eligible_for(name);
        let (_id, client) = eligible
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no worker in pool serves cron target {name:?}"))?;
        let udf_type = match kind {
            CronTargetKind::Mutation => common::types::UdfType::Mutation,
            CronTargetKind::Action => common::types::UdfType::Action,
        };
        let exec_req = convex_native_core::distributed::ExecuteRequest {
            name: name.to_string(),
            namespace: TableNamespace::Global,
            args: ConvexObject::empty(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
            // Mutation needs a begin_timestamp; we don't own a
            // `Database` on the backend here (the worker opens
            // its own at its local `now_ts_for_reads`). Leave
            // the field absent — the worker's server-side
            // handler falls through to `now_ts_for_reads()` when
            // `begin_timestamp` is None (Phase-2 contract).
            begin_timestamp: None,
            existing_writes: Vec::new(),
            http_request: None,
            identity: Vec::new(),
        };
        let response = client
            .execute(exec_req, udf_type)
            .await
            .map_err(|status| anyhow::anyhow!("pool cron dispatch {name}: {status}"))?;
        if let Err(msg) = response.result {
            anyhow::bail!("cron handler {name:?} returned error: {msg}");
        }
        Ok(())
    }
}

/// Cron driver. Owns the schedule map and a tokio task per cron that
/// sleeps until `next_after(now)` and calls `dispatcher.fire(...)`.
pub struct NativeCronDriver {
    inner: Arc<CronDriverInner>,
}

struct CronDriverInner {
    dispatcher: Arc<dyn CronDispatcher>,
    /// Schedules keyed by cron name. Wrapped in a `Mutex` so
    /// admission-driven updates (a new worker advertising a cron
    /// that's not in the map yet) can add entries at runtime.
    schedules: Mutex<HashMap<String, CronJob>>,
    /// Handles to the per-cron firing tasks so admin surfaces can
    /// stop or replace them. Phase-7 operator tooling can grow a
    /// `NativeCronDriver::stop(name)` off this; not wired today.
    task_handles: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

impl NativeCronDriver {
    pub fn new(dispatcher: Arc<dyn CronDispatcher>) -> Self {
        Self {
            inner: Arc::new(CronDriverInner {
                dispatcher,
                schedules: Mutex::new(HashMap::new()),
                task_handles: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Install a batch of cron jobs. Idempotent on job name:
    /// re-installing the same name with the same schedule + target
    /// is a no-op; changing either tears down the prior task and
    /// spawns a fresh one. Returns the set of jobs that landed.
    pub fn install(&self, jobs: Vec<CronJob>) -> anyhow::Result<Vec<String>> {
        let mut installed = Vec::new();
        for job in jobs {
            let _cron = job
                .schedule_expr
                .parse::<Cron>()
                .map_err(|e| anyhow::anyhow!("cron {:?}: parse {e:?}", job.name))?;

            let mut schedules = self.inner.schedules.lock();
            let already = schedules.get(&job.name);
            let same = already
                .map(|existing| {
                    existing.schedule_expr == job.schedule_expr
                        && existing.target == job.target
                        && existing.target_kind == job.target_kind
                })
                .unwrap_or(false);
            if same {
                continue;
            }
            schedules.insert(job.name.clone(), job.clone());
            drop(schedules);

            if let Some(prior) = self.inner.task_handles.lock().remove(&job.name) {
                prior.abort();
            }
            let handle = tokio::spawn(run_cron_loop(self.inner.clone(), job.clone()));
            self.inner
                .task_handles
                .lock()
                .insert(job.name.clone(), handle);
            installed.push(job.name);
        }
        Ok(installed)
    }

    /// Current schedule map. Snapshot; safe to call from admin
    /// surfaces while tasks run.
    pub fn jobs(&self) -> Vec<CronJob> {
        self.inner.schedules.lock().values().cloned().collect()
    }

    /// Remove a cron from the driver. Aborts the task and drops
    /// the schedule entry. No-op when the name isn't registered.
    pub fn remove(&self, name: &str) {
        self.inner.schedules.lock().remove(name);
        if let Some(handle) = self.inner.task_handles.lock().remove(name) {
            handle.abort();
        }
    }
}

impl Drop for CronDriverInner {
    fn drop(&mut self) {
        for (_, handle) in self.task_handles.lock().drain() {
            handle.abort();
        }
    }
}

/// Per-cron firing loop. Computes `next_after(now)`, sleeps until
/// that instant, and calls `dispatcher.fire(target, kind)`. On
/// dispatch error the loop logs + continues — crons are at-most-
/// once so a handler failure doesn't wedge the schedule.
async fn run_cron_loop(inner: Arc<CronDriverInner>, job: CronJob) {
    let cron: Cron = match job.schedule_expr.parse() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: "convex_native_cron",
                cron = %job.name,
                "cron schedule {:?} failed to parse inside firing task: {e:?}",
                job.schedule_expr,
            );
            return;
        },
    };
    loop {
        let now: DateTime<Utc> = Utc::now();
        let Some(next) = cron.next_after(now) else {
            tracing::warn!(
                target: "convex_native_cron",
                cron = %job.name,
                "cron has no future occurrence; dropping schedule",
            );
            return;
        };
        let sleep_duration = next
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::from_secs(0));
        if !sleep_duration.is_zero() {
            tokio::time::sleep(sleep_duration).await;
        }
        let dispatch_start = std::time::Instant::now();
        match inner.dispatcher.fire(&job.target, job.target_kind).await {
            Ok(()) => {
                tracing::info!(
                    target: "convex_native_cron",
                    cron = %job.name,
                    target = %job.target,
                    kind = ?job.target_kind,
                    elapsed_ms = dispatch_start.elapsed().as_millis() as u64,
                    "cron fired",
                );
            },
            Err(e) => {
                tracing::error!(
                    target: "convex_native_cron",
                    cron = %job.name,
                    target = %job.target,
                    "cron dispatch failed: {e:#}",
                );
            },
        }
    }
}

/// Build a `Vec<CronJob>` from the collected `CronRegistry` for the
/// monolith topology. Used by `local_backend::make_app` when the
/// backend image carries native handlers.
pub fn collect_from_inventory() -> anyhow::Result<Vec<CronJob>> {
    let registry = convex_native_core::cron::CronRegistry::collect()?;
    let mut out = Vec::with_capacity(registry.len());
    for entry in registry.iter() {
        out.push(CronJob {
            name: entry.name.to_string(),
            schedule_expr: entry.schedule.to_string(),
            target: entry.target.to_string(),
            target_kind: CronTargetKind::from_str(entry.target_kind)?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicU64,
        Ordering,
    };

    use super::*;

    struct RecordingDispatcher {
        fired: AtomicU64,
    }

    #[async_trait]
    impl CronDispatcher for RecordingDispatcher {
        async fn fire(&self, _name: &str, _kind: CronTargetKind) -> anyhow::Result<()> {
            self.fired.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn target_kind_parses_mutation_and_action() {
        assert_eq!(
            CronTargetKind::from_str("mutation").unwrap(),
            CronTargetKind::Mutation,
        );
        assert_eq!(
            CronTargetKind::from_str("action").unwrap(),
            CronTargetKind::Action,
        );
        assert!(CronTargetKind::from_str("query").is_err());
    }

    #[tokio::test]
    async fn install_validates_schedules_and_spawns_one_task_per_cron() {
        let dispatcher = Arc::new(RecordingDispatcher {
            fired: AtomicU64::new(0),
        });
        let driver = NativeCronDriver::new(dispatcher.clone());
        // Schedule that never fires in the test lifespan (once per
        // hour on the hour) — we just want to verify install
        // spawns a task and `jobs()` snapshots it.
        let installed = driver
            .install(vec![CronJob {
                name: "hourly".to_string(),
                schedule_expr: "0 * * * *".to_string(),
                target: "do_work".to_string(),
                target_kind: CronTargetKind::Mutation,
            }])
            .unwrap();
        assert_eq!(installed, vec!["hourly".to_string()]);
        assert_eq!(driver.jobs().len(), 1);
        // Idempotent on re-install with same content.
        let again = driver
            .install(vec![CronJob {
                name: "hourly".to_string(),
                schedule_expr: "0 * * * *".to_string(),
                target: "do_work".to_string(),
                target_kind: CronTargetKind::Mutation,
            }])
            .unwrap();
        assert!(
            again.is_empty(),
            "re-installing identical cron is a no-op: {again:?}",
        );
        // remove() drops the entry.
        driver.remove("hourly");
        assert!(driver.jobs().is_empty());
    }

    #[tokio::test]
    async fn invalid_schedule_fails_to_install() {
        let dispatcher = Arc::new(RecordingDispatcher {
            fired: AtomicU64::new(0),
        });
        let driver = NativeCronDriver::new(dispatcher.clone());
        let err = driver
            .install(vec![CronJob {
                name: "broken".to_string(),
                schedule_expr: "this is not a cron expr".to_string(),
                target: "x".to_string(),
                target_kind: CronTargetKind::Mutation,
            }])
            .unwrap_err();
        assert!(
            err.to_string().contains("broken"),
            "error surfaces the cron name: {err}",
        );
    }
}
