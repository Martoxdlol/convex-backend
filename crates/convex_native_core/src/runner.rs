//! Native function dispatch.
//!
//! `NativeFunctionRunner` is the narrow entry point downstream code calls
//! to execute a native query, mutation, or action. It deliberately
//! does **not** implement the full `function_runner::FunctionRunner`
//! trait — that trait has six JS-specific methods (`analyze`,
//! `evaluate_app_definitions`, `evaluate_component_initializer`,
//! `evaluate_schema`, `evaluate_auth_config`, plus HTTP-action dispatch
//! inside `run_function`) whose implementation requires V8 integration.
//! The `CompositeFunctionRunner` in `crates/convex_native_backend/`
//! implements the full trait by wrapping a JS runner and delegating
//! those methods through, intercepting only the native names on
//! `run_function`.
//!
//! What this module does provide:
//!
//! - [`NativeFunctionRunner::run_query`] / [`run_mutation`] — drive a handler
//!   from the registry against a borrowed `Transaction<RT>` and return the
//!   handler's `ConvexValue`.
//! - [`NativeFunctionRunner::has_function`] — name-based check so the composite
//!   runner knows whether to dispatch natively or fall through to V8.

use std::{
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        Instant,
    },
};

use common::types::UdfType;
use database::Transaction;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

use crate::{
    circuit_breaker::CircuitBreaker,
    ctx::{
        action::ActionCtx,
        mutation::MutationCtx,
        query::QueryCtx,
    },
    metrics::{
        NativeMetricsSink,
        NoopMetrics,
        Outcome,
    },
    registry::{
        HandlerFn,
        NativeFunctionRegistration,
        NativeFunctionRegistry,
        Rt,
    },
};

/// Runtime-installable handle over the collected native function registry.
///
/// Cheap to `Clone` — internally `Arc`'d so multiple subsystems (backend
/// schema bootstrap, composite runner, etc.) share the same table.
/// Shared state for graceful shutdown: when `draining` is set, the
/// runner rejects new invocations, and `in_flight` lets the caller
/// wait for outstanding ones to finish.
struct DrainState {
    draining: AtomicBool,
    in_flight: AtomicU64,
}

impl DrainState {
    fn new() -> Self {
        Self {
            draining: AtomicBool::new(false),
            in_flight: AtomicU64::new(0),
        }
    }
}

#[derive(Clone)]
pub struct NativeFunctionRunner {
    inner: Arc<NativeFunctionRegistry>,
    metrics: Arc<dyn NativeMetricsSink>,
    default_timeout: Option<Duration>,
    drain: Arc<DrainState>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl NativeFunctionRunner {
    /// Collect every `inventory::submit!`ed registration in the binary.
    /// Metrics default to [`NoopMetrics`]; use [`with_metrics`] to
    /// install a real sink. Default timeout is unlimited; use
    /// [`with_default_timeout`] to cap every handler.
    pub fn from_inventory() -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(NativeFunctionRegistry::collect()?),
            metrics: Arc::new(NoopMetrics),
            default_timeout: None,
            drain: Arc::new(DrainState::new()),
            circuit_breaker: None,
        })
    }

    /// Attach a shared circuit breaker. Clones inherit the same
    /// breaker so repeated failures from one runner affect all.
    pub fn with_circuit_breaker(mut self, breaker: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = Some(breaker);
        self
    }

    /// Begin draining: new invocations are rejected. Clones of this
    /// runner share the drain state, so calling this on any clone
    /// shuts down the whole set.
    pub fn begin_drain(&self) {
        self.drain.draining.store(true, Ordering::SeqCst);
    }

    /// `true` once [`begin_drain`] has been called.
    pub fn is_draining(&self) -> bool {
        self.drain.draining.load(Ordering::Relaxed)
    }

    /// Current number of in-flight handler invocations.
    pub fn in_flight(&self) -> u64 {
        self.drain.in_flight.load(Ordering::Relaxed)
    }

    /// Await until every in-flight invocation has completed or
    /// `timeout` elapses. Returns `true` if drained cleanly, `false`
    /// on timeout.
    pub async fn await_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.in_flight() == 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn check_drain(&self, name: &str) -> anyhow::Result<()> {
        if self.is_draining() {
            anyhow::bail!("native runner is draining — rejecting {name:?}");
        }
        Ok(())
    }

    fn check_breaker(&self, name: &str) -> anyhow::Result<()> {
        if let Some(cb) = &self.circuit_breaker {
            cb.before_call(name)?;
        }
        Ok(())
    }

    fn report_breaker(&self, name: &str, ok: bool) {
        if let Some(cb) = &self.circuit_breaker {
            cb.after_call(name, ok);
        }
    }

    fn enter(&self) -> InFlightGuard<'_> {
        self.drain.in_flight.fetch_add(1, Ordering::SeqCst);
        InFlightGuard { drain: &self.drain }
    }

    /// Attach a metrics sink. Returns a new runner that shares the
    /// same registry — existing clones keep the previous sink.
    pub fn with_metrics(mut self, metrics: Arc<dyn NativeMetricsSink>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Attach a default per-function timeout. Any handler that takes
    /// longer than this is aborted with a clear error and recorded as
    /// `Outcome::Err` in metrics.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Borrow the attached metrics sink.
    pub fn metrics(&self) -> &Arc<dyn NativeMetricsSink> {
        &self.metrics
    }

    /// Timeout-wrap a handler future. Uses the per-function
    /// `timeout_ms` from the registration when set, otherwise falls
    /// back to the runner-level default. If neither is set, the
    /// future runs unchanged.
    async fn run_with_timeout<F, T>(
        &self,
        fut: F,
        name: &str,
        reg_timeout_ms: u64,
    ) -> anyhow::Result<T>
    where
        F: std::future::Future<Output = anyhow::Result<T>> + Send,
    {
        let effective = if reg_timeout_ms > 0 {
            Some(Duration::from_millis(reg_timeout_ms))
        } else {
            self.default_timeout
        };
        match effective {
            None => fut.await,
            Some(t) => match tokio::time::timeout(t, fut).await {
                Ok(r) => r,
                Err(_) => Err(anyhow::anyhow!(
                    "native function {name:?} timed out after {:?}",
                    t,
                )),
            },
        }
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// `true` if a native function with this name is registered.
    pub fn has_function(&self, name: &str) -> bool {
        self.inner.get(name).is_some()
    }

    /// `true` if the named function is registered and of the given kind.
    pub fn has_function_of_type(&self, name: &str, udf_type: UdfType) -> bool {
        match self.inner.get(name) {
            Some(r) => r.udf_type() == udf_type,
            None => false,
        }
    }

    /// Look up the full registration. Returns `None` if unknown.
    pub fn get(&self, name: &str) -> Option<&'static NativeFunctionRegistration> {
        self.inner.get(name)
    }

    /// Execute a named native query. Errors if the name isn't registered
    /// or isn't a query. Records latency + outcome to the attached
    /// metrics sink. Emits a fastrace span covering the whole dispatch.
    #[fastrace::trace]
    pub async fn run_query(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.run_query_inner(name, tx, namespace, args, None, None)
            .await
    }

    /// Same as [`run_query`] but threads an externally-owned
    /// `LogBuffer` through the ctx so the caller can drain
    /// `ctx.log()` output after the handler returns. The distributed
    /// worker uses this to populate `ExecuteResponse::log_lines`.
    #[fastrace::trace]
    pub async fn run_query_with_log_buffer(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
        log_buffer: crate::logging::LogBuffer,
    ) -> anyhow::Result<ConvexValue> {
        self.run_query_inner(name, tx, namespace, args, Some(log_buffer), None)
            .await
    }

    /// Variant of [`run_query_with_log_buffer`] that also pins the
    /// per-invocation [`Observed`] handle the worker uses to surface
    /// `observed_identity` / `observed_rng` / `observed_time` flags
    /// in the response (so the distributed dispatch path can build
    /// a `UdfOutcome` with the same fidelity the in-process composite
    /// runner produces).
    #[fastrace::trace]
    pub async fn run_query_with_log_buffer_and_observed(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
        log_buffer: crate::logging::LogBuffer,
        observed: std::sync::Arc<crate::ctx::query::Observed>,
    ) -> anyhow::Result<ConvexValue> {
        self.run_query_inner(name, tx, namespace, args, Some(log_buffer), Some(observed))
            .await
    }

    async fn run_query_inner(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
        log_buffer: Option<crate::logging::LogBuffer>,
        observed: Option<std::sync::Arc<crate::ctx::query::Observed>>,
    ) -> anyhow::Result<ConvexValue> {
        self.check_drain(name)?;
        self.check_breaker(name)?;
        let _guard = self.enter();
        let registration = self
            .inner
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no native function registered with name {name:?}"))?;
        let HandlerFn::Query(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not a query (got {:?})",
                registration.udf_type(),
            );
        };
        let mut ctx = match (log_buffer, observed) {
            (Some(buf), Some(obs)) => {
                QueryCtx::with_log_buffer_and_observed(tx, namespace, buf, obs)
            },
            (Some(buf), None) => QueryCtx::with_log_buffer(tx, namespace, buf),
            (None, _) => QueryCtx::new(tx, namespace),
        };
        let started = Instant::now();
        let result = self
            .run_with_timeout(handler(&mut ctx, args), name, registration.timeout_ms)
            .await;
        self.report_breaker(name, result.is_ok());
        self.metrics.record(
            name,
            UdfType::Query,
            if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Err
            },
            started.elapsed(),
        );
        result
    }

    /// Execute a named native mutation. Errors if the name isn't
    /// registered or isn't a mutation.
    #[fastrace::trace]
    pub async fn run_mutation(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.run_mutation_inner(name, tx, namespace, args, None, None)
            .await
    }

    /// Mutation counterpart to [`run_query_with_log_buffer`].
    #[fastrace::trace]
    pub async fn run_mutation_with_log_buffer(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
        log_buffer: crate::logging::LogBuffer,
    ) -> anyhow::Result<ConvexValue> {
        self.run_mutation_inner(name, tx, namespace, args, Some(log_buffer), None)
            .await
    }

    /// Mutation counterpart to
    /// [`run_query_with_log_buffer_and_observed`].
    #[fastrace::trace]
    pub async fn run_mutation_with_log_buffer_and_observed(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
        log_buffer: crate::logging::LogBuffer,
        observed: std::sync::Arc<crate::ctx::query::Observed>,
    ) -> anyhow::Result<ConvexValue> {
        self.run_mutation_inner(name, tx, namespace, args, Some(log_buffer), Some(observed))
            .await
    }

    async fn run_mutation_inner(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
        log_buffer: Option<crate::logging::LogBuffer>,
        observed: Option<std::sync::Arc<crate::ctx::query::Observed>>,
    ) -> anyhow::Result<ConvexValue> {
        self.check_drain(name)?;
        self.check_breaker(name)?;
        let _guard = self.enter();
        let registration = self
            .inner
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no native function registered with name {name:?}"))?;
        let HandlerFn::Mutation(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not a mutation (got {:?})",
                registration.udf_type(),
            );
        };
        let mut ctx = match (log_buffer, observed) {
            (Some(buf), Some(obs)) => {
                MutationCtx::with_log_buffer_and_observed(tx, namespace, buf, obs)
            },
            (Some(buf), None) => MutationCtx::with_log_buffer(tx, namespace, buf),
            (None, _) => MutationCtx::new(tx, namespace),
        };
        let started = Instant::now();
        let result = self
            .run_with_timeout(handler(&mut ctx, args), name, registration.timeout_ms)
            .await;
        self.report_breaker(name, result.is_ok());
        self.metrics.record(
            name,
            UdfType::Mutation,
            if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Err
            },
            started.elapsed(),
        );
        result
    }

    /// Execute a named native action with `NoopCallbacks`. Actions that
    /// need to sub-call queries/mutations or touch storage/scheduler
    /// should go through [`run_action_with_callbacks`] instead.
    pub async fn run_action(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.run_action_with_callbacks(
            name,
            namespace,
            args,
            Arc::new(crate::callbacks::NoopCallbacks),
        )
        .await
    }

    /// Execute a named native action with explicit callbacks. The
    /// action's `ActionCtx` gets those callbacks, so `run_query`,
    /// `run_mutation`, `scheduler()`, and `storage()` all route through
    /// them.
    #[fastrace::trace]
    pub async fn run_action_with_callbacks(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
    ) -> anyhow::Result<ConvexValue> {
        self.run_action_inner(name, namespace, args, callbacks, None)
            .await
    }

    /// Same as [`run_action_with_callbacks`] but threads an
    /// externally-owned `LogBuffer` through the ctx so the caller can
    /// drain `ctx.log()` output after the handler returns. The
    /// composite backend uses this to populate the `log_lines` field
    /// on the `UdfOutcome` / `ActionOutcome` — without it, native
    /// `ctx.log()` output never reaches the backend's log-streaming
    /// path.
    #[fastrace::trace]
    pub async fn run_action_with_callbacks_and_log_buffer(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
        log_buffer: crate::logging::LogBuffer,
    ) -> anyhow::Result<ConvexValue> {
        self.run_action_inner(name, namespace, args, callbacks, Some(log_buffer))
            .await
    }

    async fn run_action_inner(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
        log_buffer: Option<crate::logging::LogBuffer>,
    ) -> anyhow::Result<ConvexValue> {
        self.check_drain(name)?;
        self.check_breaker(name)?;
        let _guard = self.enter();
        let registration = self
            .inner
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no native function registered with name {name:?}"))?;
        let HandlerFn::Action(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not an action (got {:?})",
                registration.udf_type(),
            );
        };
        let mut ctx = match log_buffer {
            Some(buf) => ActionCtx::<Rt>::with_callbacks_and_log_buffer(
                Some(self.clone()),
                callbacks,
                namespace,
                buf,
            ),
            None => ActionCtx::<Rt>::with_callbacks(Some(self.clone()), callbacks, namespace),
        };
        let started = Instant::now();
        let result = self
            .run_with_timeout(handler(&mut ctx, args), name, registration.timeout_ms)
            .await;
        self.report_breaker(name, result.is_ok());
        self.metrics.record(
            name,
            UdfType::Action,
            if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Err
            },
            started.elapsed(),
        );
        result
    }

    /// Execute a named native HTTP action with `NoopCallbacks`.
    /// HTTP actions registered through `#[convex::http_action]`
    /// land here when the worker dispatches a `UdfType::HttpAction`
    /// over `FunctionExecutionService`.
    pub async fn run_http_action(
        self: &Arc<Self>,
        name: &str,
        request: crate::http::HttpRequest,
    ) -> anyhow::Result<crate::http::HttpResponse> {
        self.run_http_action_with_callbacks(
            name,
            request,
            Arc::new(crate::callbacks::NoopCallbacks),
            None,
        )
        .await
    }

    /// Variant of [`run_http_action`] that takes explicit
    /// callbacks (so HTTP actions making `ctx.run_mutation(...)`
    /// sub-calls route to the backend's Committer) and an
    /// optional `LogBuffer` for capturing `ctx.log()` output.
    #[fastrace::trace]
    pub async fn run_http_action_with_callbacks(
        self: &Arc<Self>,
        name: &str,
        request: crate::http::HttpRequest,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
        log_buffer: Option<crate::logging::LogBuffer>,
    ) -> anyhow::Result<crate::http::HttpResponse> {
        self.check_drain(name)?;
        self.check_breaker(name)?;
        let _guard = self.enter();
        let registration = self.inner.get(name).ok_or_else(|| {
            anyhow::anyhow!("no native HTTP action registered with name {name:?}")
        })?;
        let HandlerFn::Http(handler) = registration.handler else {
            anyhow::bail!(
                "native function {name:?} is not an HTTP action (got {:?})",
                registration.udf_type(),
            );
        };
        let mut http_ctx = match log_buffer.clone() {
            Some(buf) => crate::http::HttpActionCtx::<'_, Rt>::with_callbacks_and_log_buffer(
                Some(self.clone()),
                callbacks,
                TableNamespace::Global,
                buf,
            ),
            None => crate::http::HttpActionCtx::<'_, Rt>::with_callbacks(
                Some(self.clone()),
                callbacks,
                TableNamespace::Global,
            ),
        };
        let started = Instant::now();
        let result = self
            .run_with_timeout(
                handler(&mut http_ctx, request),
                name,
                registration.timeout_ms,
            )
            .await;
        self.report_breaker(name, result.is_ok());
        self.metrics.record(
            name,
            UdfType::HttpAction,
            if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Err
            },
            started.elapsed(),
        );
        result
    }

    /// Iterate the registered functions (metadata only).
    pub fn iter(&self) -> impl Iterator<Item = &'static NativeFunctionRegistration> + '_ {
        self.inner.iter()
    }

    /// Borrow the inner registry — used by introspection tooling.
    pub fn registry_ref(&self) -> Option<&NativeFunctionRegistry> {
        Some(&self.inner)
    }
}

struct InFlightGuard<'a> {
    drain: &'a DrainState,
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.drain.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_from_inventory() {
        let runner = NativeFunctionRunner::from_inventory().expect("from_inventory");
        // No functions are registered at crate-unit-test scope.
        assert!(runner.is_empty());
        assert!(!runner.has_function("anything"));
    }

    #[test]
    fn fresh_runner_is_not_draining_and_has_no_in_flight() {
        let runner = NativeFunctionRunner::from_inventory().unwrap();
        assert!(!runner.is_draining(), "drain must start false");
        assert_eq!(runner.in_flight(), 0, "no calls yet");
    }

    #[test]
    fn begin_drain_flips_is_draining_and_is_shared_across_clones() {
        // `begin_drain` uses shared `Arc<DrainState>`, so clones of
        // one runner must all observe the drain flag. This is the
        // contract that lets the backend adapter share a single
        // runner across its HTTP handlers and the worker server
        // and drain them in lockstep.
        let runner = NativeFunctionRunner::from_inventory().unwrap();
        let clone = runner.clone();
        assert!(!runner.is_draining());
        assert!(!clone.is_draining());
        clone.begin_drain();
        assert!(runner.is_draining(), "drain propagates via shared Arc");
        assert!(clone.is_draining());
    }

    #[tokio::test]
    async fn await_drain_returns_true_immediately_when_idle() {
        // No in-flight work => drained already; await_drain should
        // return true on the first poll without sleeping out the
        // timeout.
        let runner = NativeFunctionRunner::from_inventory().unwrap();
        let drained = runner.await_drain(Duration::from_millis(100)).await;
        assert!(drained, "an idle runner is already drained");
    }

    #[tokio::test]
    async fn run_action_on_empty_runner_errors_with_name() {
        // `run_action` on a runner that has no function registered
        // must bail with a message that names the requested
        // function — user-facing error clarity matters here.
        let runner = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
        let empty_obj = value::ConvexObject::try_from(std::collections::BTreeMap::<
            value::FieldName,
            ConvexValue,
        >::new())
        .unwrap();
        let err = runner
            .run_action("missing", TableNamespace::Global, empty_obj)
            .await
            .expect_err("missing action");
        assert!(
            format!("{err}").contains("no native function registered"),
            "error names the missing registration: {err}",
        );
        assert!(
            format!("{err}").contains("missing"),
            "error mentions the requested name: {err}",
        );
    }

    #[tokio::test]
    async fn draining_runner_rejects_calls_with_a_drain_specific_message() {
        // After `begin_drain()`, every dispatch path must refuse with
        // a message mentioning "draining" so the caller can distinguish
        // "wrong name" from "server shutting down". Tests via
        // `run_action` because it doesn't need a Transaction<Rt>.
        let runner = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
        runner.begin_drain();
        assert!(runner.is_draining());
        let empty_obj = value::ConvexObject::try_from(std::collections::BTreeMap::<
            value::FieldName,
            ConvexValue,
        >::new())
        .unwrap();
        let err = runner
            .run_action("anything", TableNamespace::Global, empty_obj)
            .await
            .expect_err("draining");
        assert!(
            format!("{err}").contains("draining"),
            "error mentions drain: {err}",
        );
    }

    #[test]
    fn with_default_timeout_stores_the_value() {
        // The builder chains must actually persist the value they
        // take — accidental `let _ = self.default_timeout = ...`
        // refactors would drop the setting without a type error.
        let runner = NativeFunctionRunner::from_inventory()
            .unwrap()
            .with_default_timeout(Duration::from_millis(250));
        assert_eq!(runner.default_timeout, Some(Duration::from_millis(250)));
    }
}
