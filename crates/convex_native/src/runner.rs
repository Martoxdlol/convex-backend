//! Native function dispatch.
//!
//! `NativeFunctionRunner` is the entry point the backend calls to execute
//! a native query or mutation. It does **not** yet implement the full
//! `function_runner::FunctionRunner` trait — that trait has six
//! JS-specific methods (`analyze`, `evaluate_app_definitions`,
//! `evaluate_component_initializer`, `evaluate_schema`,
//! `evaluate_auth_config`, plus HTTP-action dispatch inside
//! `run_function`) whose full implementation requires V8 integration.
//! Phase 1.5 wraps this runner behind a "composite" trait impl that
//! delegates those methods to the existing V8 runner and intercepts
//! only the native names.
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
        })
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

    /// Timeout-wrap a handler future. If the runner has no default
    /// timeout, the future runs unchanged.
    async fn run_with_timeout<F>(&self, fut: F, name: &str) -> anyhow::Result<ConvexValue>
    where
        F: std::future::Future<Output = anyhow::Result<ConvexValue>> + Send,
    {
        match self.default_timeout {
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
    /// metrics sink.
    pub async fn run_query(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.check_drain(name)?;
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
        let mut ctx = QueryCtx::new(tx, namespace);
        let started = Instant::now();
        let result = self.run_with_timeout(handler(&mut ctx, args), name).await;
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
    pub async fn run_mutation(
        &self,
        name: &str,
        tx: &mut Transaction<Rt>,
        namespace: TableNamespace,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.check_drain(name)?;
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
        let mut ctx = MutationCtx::new(tx, namespace);
        let started = Instant::now();
        let result = self.run_with_timeout(handler(&mut ctx, args), name).await;
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
    pub async fn run_action_with_callbacks(
        self: &Arc<Self>,
        name: &str,
        namespace: TableNamespace,
        args: ConvexObject,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
    ) -> anyhow::Result<ConvexValue> {
        self.check_drain(name)?;
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
        let mut ctx = ActionCtx::<Rt>::with_callbacks(Some(self.clone()), callbacks, namespace);
        let started = Instant::now();
        let result = self.run_with_timeout(handler(&mut ctx, args), name).await;
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

    /// Iterate the registered functions (metadata only).
    pub fn iter(&self) -> impl Iterator<Item = &'static NativeFunctionRegistration> + '_ {
        self.inner.iter()
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
}
