//! Developer-facing builder for assembling a native Convex app.
//!
//! ```ignore
//! use convex_native_core::{ConvexBackend, NativeActionCallbacks};
//! use std::sync::Arc;
//!
//! let built = ConvexBackend::new()
//!     .with_native_functions()   // collect #[convex::{query, mutation, action}]
//!     .with_native_schema()      // collect #[derive(ConvexDocument)] tables
//!     .with_http_routes()        // collect #[convex::http_action]
//!     .with_callbacks(Arc::new(my_backend_callbacks))
//!     .build()?;
//!
//! // `built` carries the registry, schema, router, and callbacks —
//! // the full backend's `make_app()` plugs these into the composite
//! // function runner.
//! ```
//!
//! This surface intentionally doesn't *start* a backend — that
//! lives in the sibling `crates/convex_native_backend/` crate
//! which carries the `function_runner` dep and the V8 build
//! prerequisite. What you get here is the full "collected app"
//! object a developer can introspect, validate, and describe
//! without paying the isolate build cost. The production binary
//! picks the inventory up directly through
//! `NativeFunctionRunner::from_inventory()` inside
//! `local_backend::make_app()`, so `BuiltBackend` is useful
//! mostly for tooling (dev-time JSON introspection, startup
//! cron/target validation, summary logs).

use std::sync::Arc;

use common::schemas::DatabaseSchema;

use crate::{
    callbacks::{
        NativeActionCallbacks,
        NoopCallbacks,
    },
    http::HttpRouter,
    runner::NativeFunctionRunner,
    schema::NativeSchema,
};

/// Mutable builder. Defaults are deliberately empty — you opt each
/// piece in explicitly so test harnesses can compose a minimal backend
/// without pulling in every capability.
#[derive(Default)]
pub struct ConvexBackend {
    include_fns: bool,
    include_schema: bool,
    include_http: bool,
    include_crons: bool,
    callbacks: Option<Arc<dyn NativeActionCallbacks>>,
}

impl ConvexBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Opt into collecting every `#[convex::{query,mutation,action}]`
    /// the binary statically registered.
    pub fn with_native_functions(mut self) -> Self {
        self.include_fns = true;
        self
    }

    /// Opt into collecting every `#[derive(ConvexDocument)]` table.
    pub fn with_native_schema(mut self) -> Self {
        self.include_schema = true;
        self
    }

    /// Opt into collecting every `#[convex::http_action]` route.
    pub fn with_http_routes(mut self) -> Self {
        self.include_http = true;
        self
    }

    /// Opt into collecting every `#[convex::cron]` registration.
    pub fn with_crons(mut self) -> Self {
        self.include_crons = true;
        self
    }

    /// Provide backend callbacks. If omitted, `BuiltBackend` exposes
    /// [`NoopCallbacks`] so unit tests can still construct a backend.
    pub fn with_callbacks(mut self, callbacks: Arc<dyn NativeActionCallbacks>) -> Self {
        self.callbacks = Some(callbacks);
        self
    }

    /// Finalize: collect everything opted-in into a [`BuiltBackend`].
    pub fn build(self) -> anyhow::Result<BuiltBackend> {
        let runner = if self.include_fns {
            Some(Arc::new(NativeFunctionRunner::from_inventory()?))
        } else {
            None
        };
        let schema = if self.include_schema {
            Some(NativeSchema::collect()?)
        } else {
            None
        };
        let router = if self.include_http {
            Some(HttpRouter::collect()?)
        } else {
            None
        };
        let crons = if self.include_crons {
            Some(crate::cron::CronRegistry::collect()?)
        } else {
            None
        };
        let callbacks = self.callbacks.unwrap_or_else(|| Arc::new(NoopCallbacks));
        Ok(BuiltBackend {
            runner,
            schema,
            router,
            crons,
            callbacks,
        })
    }
}

/// Fully assembled native-convex app, ready to hand to a backend
/// adapter.
pub struct BuiltBackend {
    pub runner: Option<Arc<NativeFunctionRunner>>,
    pub schema: Option<DatabaseSchema>,
    pub router: Option<HttpRouter>,
    pub crons: Option<crate::cron::CronRegistry>,
    pub callbacks: Arc<dyn NativeActionCallbacks>,
}

impl BuiltBackend {
    /// Convenience: did the builder opt into functions?
    pub fn has_runner(&self) -> bool {
        self.runner.is_some()
    }

    /// Convenience: did the builder opt into schema collection?
    pub fn has_schema(&self) -> bool {
        self.schema.is_some()
    }

    /// Convenience: did the builder opt into HTTP routes?
    pub fn has_http(&self) -> bool {
        self.router.is_some()
    }

    /// Number of registered functions (queries + mutations + actions).
    pub fn function_count(&self) -> usize {
        self.runner.as_ref().map_or(0, |r| r.len())
    }

    /// Number of declared tables.
    pub fn table_count(&self) -> usize {
        self.schema.as_ref().map_or(0, |s| s.tables.len())
    }

    /// Number of registered HTTP routes.
    pub fn route_count(&self) -> usize {
        self.router.as_ref().map_or(0, |r| r.len())
    }

    /// Number of registered cron entries.
    pub fn cron_count(&self) -> usize {
        self.crons.as_ref().map_or(0, |c| c.len())
    }

    /// The `convex_native_core` crate version this binary was built with.
    /// Useful for deployment traceability.
    pub fn convex_native_version(&self) -> &'static str {
        crate::VERSION
    }

    /// A one-line human-readable summary suitable for startup logs.
    pub fn summary(&self) -> String {
        format!(
            "convex_native_core {} — {} fn · {} table · {} route · {} cron",
            self.convex_native_version(),
            self.function_count(),
            self.table_count(),
            self.route_count(),
            self.cron_count(),
        )
    }

    /// Produce the warm-up plan for the collected schema. Returns an
    /// empty vec if the builder didn't opt into schema collection.
    pub fn warmup_plan(&self) -> Vec<crate::warmup::WarmupEntry> {
        match &self.schema {
            None => Vec::new(),
            Some(schema) => crate::warmup::plan_warmup(schema),
        }
    }

    /// Stable JSON envelope describing the collected app. Useful for
    /// dev tooling and CI checks; see [`crate::introspect::describe_json`]
    /// for the shape.
    pub fn describe_json(&self) -> serde_json::Value {
        let functions = self.runner.as_ref().and_then(|r| r.registry_ref());
        crate::introspect::describe_json_full(
            self.schema.as_ref(),
            functions,
            self.router.as_ref(),
            self.crons.as_ref(),
        )
    }

    /// Pretty-printed string version of [`describe_json`].
    pub fn describe_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.describe_json()).unwrap_or_else(|_| String::new())
    }

    /// Cross-check internal consistency — today that's verifying every
    /// `#[convex::cron]` target actually names a registered mutation
    /// or action. Call this at startup so misconfigured crons crash
    /// the binary before accepting traffic rather than silently
    /// skipping.
    pub fn validate(&self) -> anyhow::Result<()> {
        let (Some(crons), Some(runner)) = (self.crons.as_ref(), self.runner.as_ref()) else {
            // If either side is missing we can't validate; treat as OK.
            return Ok(());
        };
        for entry in crons.iter() {
            let registration = runner.get(entry.target).ok_or_else(|| {
                anyhow::anyhow!(
                    "cron {:?} targets unknown function {:?}",
                    entry.name,
                    entry.target,
                )
            })?;
            let expected_kind = match registration.handler {
                crate::registry::HandlerFn::Query(_) => "query",
                crate::registry::HandlerFn::Mutation(_) => "mutation",
                crate::registry::HandlerFn::Action(_) => "action",
            };
            anyhow::ensure!(
                expected_kind == entry.target_kind,
                "cron {:?} target_kind = {:?}, but function {:?} is a {}",
                entry.name,
                entry.target_kind,
                entry.target,
                expected_kind,
            );
        }
        Ok(())
    }

    /// Run a registered native action directly. Useful for integration
    /// tests that want to exercise an action without standing up the
    /// full backend.
    pub async fn run_action(
        &self,
        name: &str,
        namespace: value::TableNamespace,
        args: value::ConvexObject,
    ) -> anyhow::Result<value::ConvexValue> {
        let runner = self.runner.as_ref().ok_or_else(|| {
            anyhow::anyhow!("BuiltBackend has no runner — call `.with_native_functions()`")
        })?;
        runner
            .run_action_with_callbacks(name, namespace, args, self.callbacks.clone())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_builder_defaults_are_empty() {
        let built = ConvexBackend::new().build().unwrap();
        assert!(!built.has_runner());
        assert!(!built.has_schema());
        assert!(!built.has_http());
    }

    #[test]
    fn opting_in_picks_up_each_capability() {
        let built = ConvexBackend::new()
            .with_native_functions()
            .with_native_schema()
            .with_http_routes()
            .build()
            .unwrap();
        assert!(built.has_runner());
        assert!(built.has_schema());
        assert!(built.has_http());
    }

    #[test]
    fn counts_return_zero_when_capabilities_were_not_opted_in() {
        // A builder that opted into nothing yields zeroes from the
        // `_count` helpers, not a panic. Summary-logging paths rely
        // on that to be safe to call unconditionally.
        let built = ConvexBackend::new().build().unwrap();
        assert_eq!(built.function_count(), 0);
        assert_eq!(built.table_count(), 0);
        assert_eq!(built.route_count(), 0);
        assert_eq!(built.cron_count(), 0);
    }

    #[test]
    fn summary_includes_all_four_counts_and_the_crate_version() {
        // `summary()` is typed into startup logs. Pin the format
        // fragments so a silent reshape (e.g. dropping the crate
        // version) doesn't go unnoticed.
        let built = ConvexBackend::new().build().unwrap();
        let s = built.summary();
        assert!(s.contains("convex_native_core"), "summary names the crate: {s}");
        assert!(s.contains(crate::VERSION), "summary includes version: {s}");
        assert!(s.contains("0 fn"), "function count shows up: {s}");
        assert!(s.contains("0 table"), "table count shows up: {s}");
        assert!(s.contains("0 route"), "route count shows up: {s}");
        assert!(s.contains("0 cron"), "cron count shows up: {s}");
    }

    #[test]
    fn validate_is_a_noop_when_crons_or_runner_are_missing() {
        // `validate()` can only cross-check cron↔function consistency
        // when both sides were opted in. If either is missing, it
        // treats the backend as "nothing to validate" and returns
        // Ok — pin that so a future refactor doesn't surprise
        // callers with a mandatory-opt-in error.
        let only_runner = ConvexBackend::new()
            .with_native_functions()
            .build()
            .unwrap();
        only_runner.validate().expect("runner-only ok");

        let only_crons = ConvexBackend::new().with_crons().build().unwrap();
        only_crons.validate().expect("crons-only ok");
    }

    #[test]
    fn warmup_plan_is_empty_without_schema() {
        // `warmup_plan()` is safe to call unconditionally — it
        // returns an empty Vec when no schema was collected rather
        // than panicking.
        let built = ConvexBackend::new().build().unwrap();
        assert!(built.warmup_plan().is_empty());
    }

    #[test]
    fn convex_native_version_matches_crate_version() {
        // Deployment traceability: `BuiltBackend::convex_native_version()`
        // must track `crate::VERSION`, which is also exposed in the
        // introspect envelope and the worker's Health response.
        let built = ConvexBackend::new().build().unwrap();
        assert_eq!(built.convex_native_version(), crate::VERSION);
    }

    #[tokio::test]
    async fn run_action_without_runner_errors_with_a_helpful_message() {
        // `.with_native_functions()` is required for `run_action`.
        // When it's missing, the error must explain how to fix the
        // build rather than returning a generic "None".
        let built = ConvexBackend::new().build().unwrap();
        let empty_obj = value::ConvexObject::try_from(std::collections::BTreeMap::<
            value::FieldName,
            value::ConvexValue,
        >::new())
        .unwrap();
        let err = built
            .run_action("x", value::TableNamespace::Global, empty_obj)
            .await
            .expect_err("no runner");
        assert!(
            format!("{err}").contains("with_native_functions"),
            "error points at the missing builder call: {err}",
        );
    }

    #[test]
    fn describe_pretty_emits_valid_json_even_when_empty() {
        // `describe_pretty` uses `unwrap_or_else(String::new)` on the
        // serde error path. The normal path for an empty backend
        // must still produce valid JSON — pin that by parsing the
        // output back.
        let built = ConvexBackend::new().build().unwrap();
        let pretty = built.describe_pretty();
        assert!(!pretty.is_empty(), "not the serde-error fallback");
        let _: serde_json::Value = serde_json::from_str(&pretty).expect("valid JSON");
    }
}
