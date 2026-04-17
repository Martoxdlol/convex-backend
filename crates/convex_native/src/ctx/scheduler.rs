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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        callbacks::NoopCallbacks,
        convert::FromConvex,
    };

    /// Test-only marker whose `Args` deliberately serialise to a
    /// non-object `ConvexValue` so we can trigger the "scheduled args
    /// must serialize to an object" branch in `run_after` and
    /// `run_action_after`. Real `#[convex::mutation]` / action markers
    /// always carry a struct `Args` (which serialises to an object),
    /// but we want the error branch covered so a refactor flattening
    /// args can't silently turn a user bug into a wrong-shaped
    /// scheduled job.
    struct ScalarMarker;

    impl ConvexMutationFunction for ScalarMarker {
        type Args = i64;
        type Output = i64;

        fn name() -> &'static str {
            "scalar_mutation"
        }
    }

    impl ConvexActionFunction for ScalarMarker {
        type Args = i64;
        type Output = i64;

        fn name() -> &'static str {
            "scalar_action"
        }
    }

    /// Object-valued marker — `Args` serialises to a `ConvexObject`,
    /// matching the shape real macro-generated markers emit. The
    /// `NoopCallbacks::schedule` method then bails because no backend
    /// is attached, which is exactly what we want to prove: args
    /// parsing accepted, delegation happened.
    struct ObjectArgs(i64);

    impl ToConvex for ObjectArgs {
        fn to_convex(self) -> anyhow::Result<ConvexValue> {
            use std::collections::BTreeMap;

            use value::{
                ConvexObject,
                FieldName,
            };
            let mut fields: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
            fields.insert("n".parse().unwrap(), ConvexValue::Int64(self.0));
            Ok(ConvexValue::Object(ConvexObject::try_from(fields)?))
        }
    }

    impl FromConvex for ObjectArgs {
        fn from_convex(_: ConvexValue) -> anyhow::Result<Self> {
            unreachable!("not exercised")
        }
    }

    struct ObjectMarker;

    impl ConvexMutationFunction for ObjectMarker {
        type Args = ObjectArgs;
        type Output = i64;

        fn name() -> &'static str {
            "object_mutation"
        }
    }

    fn scheduler(scope: SchedulerScope) -> Scheduler<'static> {
        Scheduler::new_with_callbacks(scope, TableNamespace::Global, Arc::new(NoopCallbacks))
    }

    #[tokio::test]
    async fn run_after_rejects_args_that_serialise_to_non_object() {
        let sched = scheduler(SchedulerScope::Mutation);
        let err = sched
            .run_after(Duration::from_secs(1), ScalarMarker, 42_i64)
            .await
            .expect_err("scalar args must be rejected");
        assert!(
            format!("{err}").contains("must serialize to an object"),
            "error names the args-shape contract: {err}",
        );
    }

    #[tokio::test]
    async fn run_action_after_rejects_args_that_serialise_to_non_object() {
        let sched = scheduler(SchedulerScope::Action);
        let err = sched
            .run_action_after(Duration::from_secs(1), ScalarMarker, 42_i64)
            .await
            .expect_err("scalar args must be rejected");
        assert!(format!("{err}").contains("must serialize to an object"));
    }

    #[tokio::test]
    async fn run_after_forwards_to_callbacks_when_args_are_object() {
        // Object-shaped args pass the serialisation gate. The next
        // step is `callbacks.schedule(...)` which `NoopCallbacks`
        // bails on — so seeing that specific error tells us the
        // forward happened.
        let sched = scheduler(SchedulerScope::Mutation);
        let err = sched
            .run_after(Duration::from_secs(1), ObjectMarker, ObjectArgs(7))
            .await
            .expect_err("noop callbacks bail");
        assert!(
            format!("{err}").contains("cannot schedule"),
            "delegated to callbacks.schedule: {err}",
        );
    }

    #[tokio::test]
    async fn cancel_forwards_to_callbacks() {
        let sched = scheduler(SchedulerScope::Mutation);
        let err = sched
            .cancel(DeveloperDocumentId::MIN)
            .await
            .expect_err("noop callbacks bail");
        assert!(format!("{err}").contains("cannot cancel"));
    }
}
