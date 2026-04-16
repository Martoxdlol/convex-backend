//! Typed scheduler for native functions.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.5 + 2.8 wiring.
//!
//! Obtained via `MutationCtx::scheduler()` / `ActionCtx::scheduler()` /
//! `HttpActionCtx::scheduler()`. When the context was built with
//! [`NativeActionCallbacks`] attached the `run_after` /
//! `run_action_after` calls are routed through to the backend; when
//! attached to the no-op callbacks (e.g. in unit tests) they return a
//! clear "no callbacks attached" error.

use std::{
    sync::Arc,
    time::Duration,
};

use value::{
    ConvexValue,
    DeveloperDocumentId,
    TableNamespace,
};

use crate::{
    callbacks::NativeActionCallbacks,
    convert::ToConvex,
    function_ref::{
        ConvexActionFunction,
        ConvexMutationFunction,
    },
};

/// Which ctx handed out this scheduler. Mutations and actions share
/// the same typed API but the scheduled-job id returned by
/// `run_after` is only meaningful under scheduler terms.
#[derive(Copy, Clone, Debug)]
pub enum SchedulerScope {
    Mutation,
    Action,
}

/// Borrowed handle onto the scheduler.
pub struct Scheduler<'a> {
    pub(crate) scope: SchedulerScope,
    pub(crate) namespace: TableNamespace,
    pub(crate) callbacks: Arc<dyn NativeActionCallbacks>,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Scheduler<'a> {
    pub(crate) fn new_with_callbacks(
        scope: SchedulerScope,
        namespace: TableNamespace,
        callbacks: Arc<dyn NativeActionCallbacks>,
    ) -> Self {
        Self {
            scope,
            namespace,
            callbacks,
            _marker: std::marker::PhantomData,
        }
    }

    /// Schedule a mutation to run after `delay`. Returns the scheduled
    /// job id. Errors if no callbacks are attached.
    pub async fn run_after<F: ConvexMutationFunction>(
        &self,
        delay: Duration,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let obj = match ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("scheduled args must serialize to an object"),
        };
        let _ = self.scope;
        self.callbacks
            .schedule(self.namespace, F::name(), obj, delay)
            .await
    }

    /// Schedule an action to run after `delay`.
    pub async fn run_action_after<F: ConvexActionFunction>(
        &self,
        delay: Duration,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let obj = match ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("scheduled args must serialize to an object"),
        };
        let _ = self.scope;
        self.callbacks
            .schedule(self.namespace, F::name(), obj, delay)
            .await
    }

    /// Cancel a previously scheduled job. Idempotent.
    pub async fn cancel(&self, id: DeveloperDocumentId) -> anyhow::Result<()> {
        self.callbacks.cancel_scheduled(self.namespace, id).await
    }
}
