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
    time::{
        Duration,
        SystemTime,
    },
};

use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentPath,
    },
    execution_context::{
        ExecutionContext,
        RequestId,
    },
    runtime::{
        Runtime,
        UnixTimestamp,
    },
};
use database::Transaction;
use model::scheduled_jobs::VirtualSchedulerModel;
use sync_types::UdfPath;
use value::{
    ConvexArray,
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

    /// Schedule a mutation to fire at the absolute wall-clock time
    /// `timestamp`. Returns the scheduled job id.
    ///
    /// `timestamp` in the past is clamped to "now" (delay = 0). The
    /// underlying `NativeActionCallbacks::schedule` API only takes a
    /// `Duration`, so this method computes `delay = timestamp - now`
    /// using `SystemTime::now()`. Tests that drive a mocked runtime
    /// clock won't see the mocked time here — if you need to schedule
    /// off a transaction-runtime clock, compute the delay yourself
    /// from `ctx.unix_timestamp()` and use `run_after` directly.
    pub async fn run_at<F: ConvexMutationFunction>(
        &self,
        timestamp: UnixTimestamp,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let delay = delay_until(timestamp)?;
        self.run_after(delay, marker, args).await
    }

    /// Schedule an action to fire at the absolute wall-clock time
    /// `timestamp`. Same semantics as [`run_at`](Self::run_at).
    pub async fn run_action_at<F: ConvexActionFunction>(
        &self,
        timestamp: UnixTimestamp,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let delay = delay_until(timestamp)?;
        self.run_action_after(delay, marker, args).await
    }

    /// Cancel a previously scheduled job. Idempotent.
    pub async fn cancel(&self, id: DeveloperDocumentId) -> anyhow::Result<()> {
        self.callbacks.cancel_scheduled(self.namespace, id).await
    }
}

/// Compute the `Duration` between `SystemTime::now()` and an absolute
/// `UnixTimestamp`. Returns `Duration::ZERO` when the timestamp is in
/// the past. Errors only if the host clock predates the Unix epoch
/// (effectively impossible on real systems).
pub(crate) fn delay_until(timestamp: UnixTimestamp) -> anyhow::Result<Duration> {
    let now = UnixTimestamp::from_system_time(SystemTime::now())
        .ok_or_else(|| anyhow::anyhow!("system clock predates UNIX epoch"))?;
    Ok(timestamp.checked_sub(now).unwrap_or(Duration::ZERO))
}

/// Scheduler bound to a live mutation transaction.
///
/// Unlike the callback-based [`Scheduler`] (used by actions and HTTP
/// actions), this variant writes directly into the mutation's own
/// `Transaction<RT>` via [`VirtualSchedulerModel`]. Scheduled jobs
/// therefore commit atomically with the rest of the mutation's
/// writes — if the mutation bails, the scheduled job is never
/// persisted. This matches the JS `ctx.scheduler.runAfter` semantics.
///
/// Every scheduling call uses a freshly-minted [`ExecutionContext`]
/// anchored at the root component. When the native runner gains a
/// path to propagate an existing request's [`ExecutionContext`] into
/// the mutation, callers can switch to
/// [`MutationScheduler::with_execution_context`] to preserve the
/// parent request-id chain.
pub struct MutationScheduler<'a, RT: Runtime> {
    pub(crate) tx: &'a mut Transaction<RT>,
    pub(crate) namespace: TableNamespace,
    pub(crate) context: ExecutionContext,
}

impl<'a, RT: Runtime> MutationScheduler<'a, RT> {
    /// Construct with a default (newly-minted) [`ExecutionContext`].
    pub(crate) fn new(tx: &'a mut Transaction<RT>, namespace: TableNamespace) -> Self {
        Self {
            tx,
            namespace,
            context: default_context(),
        }
    }

    /// Override the [`ExecutionContext`]. Used by callers that
    /// propagate the parent request's request-id / execution-id into
    /// scheduled jobs (e.g. when the mutation is running under a
    /// known request context rather than a synthetic one).
    pub fn with_execution_context(mut self, context: ExecutionContext) -> Self {
        self.context = context;
        self
    }

    /// Schedule a mutation to run after `delay`. Returns the scheduled
    /// job id. The job is recorded against the mutation's own
    /// transaction, so the schedule is committed atomically with the
    /// rest of the mutation's writes.
    pub async fn run_after<F: ConvexMutationFunction>(
        &mut self,
        delay: Duration,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        self.schedule_by_name(F::name(), args_to_single_arg_array(args)?, delay)
            .await
    }

    /// Schedule an action to run after `delay`.
    pub async fn run_action_after<F: ConvexActionFunction>(
        &mut self,
        delay: Duration,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        self.schedule_by_name(F::name(), args_to_single_arg_array(args)?, delay)
            .await
    }

    /// Schedule a mutation to fire at the absolute wall-clock time
    /// `timestamp`. Delegates to [`run_after`](Self::run_after) with a
    /// delay computed from `SystemTime::now()`.
    pub async fn run_at<F: ConvexMutationFunction>(
        &mut self,
        timestamp: UnixTimestamp,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let delay = delay_until(timestamp)?;
        self.run_after(delay, marker, args).await
    }

    /// Schedule an action to fire at the absolute wall-clock time.
    pub async fn run_action_at<F: ConvexActionFunction>(
        &mut self,
        timestamp: UnixTimestamp,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let delay = delay_until(timestamp)?;
        self.run_action_after(delay, marker, args).await
    }

    /// Cancel a previously scheduled job. Idempotent.
    pub async fn cancel(&mut self, id: DeveloperDocumentId) -> anyhow::Result<()> {
        VirtualSchedulerModel::new(self.tx, self.namespace)
            .cancel(id)
            .await
    }

    async fn schedule_by_name(
        &mut self,
        name: &str,
        args: ConvexArray,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let path = udf_path_for(name)?;
        // `VirtualSchedulerModel::schedule` takes a wall-clock
        // UnixTimestamp. Compose from "now + delay" using
        // `SystemTime::now()` — tests that mock the runtime clock
        // don't see that mocked time here; if precise scheduling
        // against a mocked clock is required, compute the absolute
        // timestamp yourself from `ctx.unix_timestamp()` and call
        // `schedule_by_name` (or add an override).
        let now = UnixTimestamp::from_system_time(SystemTime::now())
            .ok_or_else(|| anyhow::anyhow!("system clock predates UNIX epoch"))?;
        let target = now + delay;
        VirtualSchedulerModel::new(self.tx, self.namespace)
            .schedule(path, args, target, self.context.clone())
            .await
    }
}

fn default_context() -> ExecutionContext {
    // Root request, no parent scheduled job. Fresh RequestId /
    // ExecutionId so scheduled jobs can be correlated in logs even
    // when no parent context was propagated.
    ExecutionContext::new_from_parts(RequestId::new(), Default::default(), None, true)
}

/// Parse `name` as a UDF path and root it in the default component.
/// Bare identifiers (e.g. `"bump"`) are treated as a default export
/// of module `bump`; `module:function` syntax is also supported.
fn udf_path_for(name: &str) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
    let udf: UdfPath = name.parse()?;
    Ok(CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: udf.canonicalize(),
    })
}

/// Serialise a single `Args` struct into the `ConvexArray` that
/// `VirtualSchedulerModel::schedule` expects. Native function
/// handlers accept exactly one object arg, so the array always has
/// one entry — an object shape.
fn args_to_single_arg_array<A: ToConvex>(args: A) -> anyhow::Result<ConvexArray> {
    let value = args.to_convex()?;
    match value {
        ConvexValue::Object(_) => ConvexArray::try_from(vec![value]).map_err(Into::into),
        _ => anyhow::bail!("scheduled args must serialize to an object"),
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
    async fn run_at_forwards_to_callbacks_when_args_are_object() {
        let sched = scheduler(SchedulerScope::Mutation);
        // Far future so the conversion is non-zero — and we don't care
        // about the precise delay; we only care that the call routed
        // through to `callbacks.schedule`.
        let ts = UnixTimestamp::from_secs_f64(4_102_444_800.0).unwrap(); // 2100-01-01
        let err = sched
            .run_at(ts, ObjectMarker, ObjectArgs(7))
            .await
            .expect_err("noop callbacks bail");
        assert!(format!("{err}").contains("cannot schedule"));
    }

    #[tokio::test]
    async fn run_action_at_forwards_to_callbacks_when_args_are_object() {
        // Use the action-shaped marker; reuses ObjectArgs.
        struct ActionObjectMarker;
        impl ConvexActionFunction for ActionObjectMarker {
            type Args = ObjectArgs;
            type Output = i64;

            fn name() -> &'static str {
                "object_action"
            }
        }

        let sched = scheduler(SchedulerScope::Action);
        let ts = UnixTimestamp::from_secs_f64(4_102_444_800.0).unwrap();
        let err = sched
            .run_action_at(ts, ActionObjectMarker, ObjectArgs(7))
            .await
            .expect_err("noop callbacks bail");
        assert!(format!("{err}").contains("cannot schedule"));
    }

    #[tokio::test]
    async fn run_at_rejects_args_that_serialise_to_non_object() {
        let sched = scheduler(SchedulerScope::Mutation);
        let ts = UnixTimestamp::from_secs_f64(4_102_444_800.0).unwrap();
        let err = sched
            .run_at(ts, ScalarMarker, 42_i64)
            .await
            .expect_err("scalar args must be rejected");
        assert!(format!("{err}").contains("must serialize to an object"));
    }

    #[test]
    fn delay_until_clamps_past_timestamps_to_zero() {
        // Epoch is firmly in the past; delay should be Duration::ZERO.
        let past = UnixTimestamp::from_secs_f64(0.0).unwrap();
        assert_eq!(delay_until(past).unwrap(), Duration::ZERO);
    }

    #[test]
    fn delay_until_returns_positive_for_future_timestamps() {
        // Far enough in the future that wall-clock drift between the
        // two reads of `SystemTime::now()` (one inside delay_until,
        // one in the assert) can't make it negative.
        let now = UnixTimestamp::from_system_time(SystemTime::now()).expect("clock after epoch");
        let future = UnixTimestamp::from_secs_f64(now.as_secs_f64() + 3_600.0).unwrap();
        let delay = delay_until(future).unwrap();
        assert!(
            delay >= Duration::from_secs(3_500) && delay <= Duration::from_secs(3_700),
            "delay should be ~1h: {delay:?}",
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

    /// End-to-end `run_at` → `delay_until` → `callbacks.schedule`
    /// round-trip using `TestCallbacks`, which records the computed
    /// delay. Proves the whole pipeline: a future timestamp one hour
    /// out lands as a ~1h `Duration` at the callbacks layer.
    ///
    /// Written as a tolerance range so clock drift between
    /// `SystemTime::now()` inside `delay_until` and the second read
    /// in this test can't flake the assert.
    #[tokio::test]
    async fn run_at_computed_delay_reaches_callbacks_as_expected_duration() {
        use crate::testing::{
            CallRecord,
            TestCallbacks,
        };
        let (callbacks, history) = TestCallbacks::new().build();
        let sched = Scheduler::new_with_callbacks(
            SchedulerScope::Mutation,
            TableNamespace::Global,
            callbacks,
        );

        let now = UnixTimestamp::from_system_time(SystemTime::now()).expect("clock after epoch");
        let future = UnixTimestamp::from_secs_f64(now.as_secs_f64() + 3_600.0).unwrap();

        sched
            .run_at(future, ObjectMarker, ObjectArgs(7))
            .await
            .expect("stubbed callbacks succeed");

        let records = history.snapshot();
        assert_eq!(records.len(), 1, "exactly one schedule call recorded");
        match &records[0] {
            CallRecord::Schedule { name, delay } => {
                assert_eq!(name, "object_mutation");
                assert!(
                    *delay >= Duration::from_secs(3_500) && *delay <= Duration::from_secs(3_700),
                    "delay should be ~1h: {delay:?}",
                );
            },
            other => panic!("unexpected record {other:?}"),
        }
    }

    // ── MutationScheduler helpers ─────────────────────────────────

    #[test]
    fn args_to_single_arg_array_wraps_object_args_into_single_element_array() {
        // `VirtualSchedulerModel::schedule` takes a `ConvexArray` (the
        // scheduled-jobs metadata shape) but native handlers always
        // receive a single object — so the outgoing array has exactly
        // one entry, an object. This pins that shape so a refactor
        // that tries to flatten args into multiple positional
        // arguments fails loudly.
        let arr = args_to_single_arg_array(ObjectArgs(42)).expect("object args round-trip");
        assert_eq!(arr.len(), 1, "handlers take a single object arg");
        assert!(
            matches!(arr.into_iter().next(), Some(ConvexValue::Object(_))),
            "entry is an object value",
        );
    }

    #[test]
    fn args_to_single_arg_array_rejects_non_object_args() {
        // Same gate as the callback-based scheduler: a scalar `Args`
        // must not silently land in the scheduler queue — users would
        // then have their handler fail at read time with a shape
        // mismatch that's hard to trace back.
        let err = args_to_single_arg_array(42_i64).expect_err("scalar must fail");
        assert!(
            format!("{err}").contains("must serialize to an object"),
            "error names the contract: {err}",
        );
    }

    #[test]
    fn udf_path_for_roots_bare_identifiers_in_default_component() {
        let path = udf_path_for("bump").expect("bare identifier parses");
        assert_eq!(path.component, ComponentPath::root());
        // Bare identifiers map to module `bump` default export — the
        // same convention as the action-side callback path.
    }

    #[test]
    fn udf_path_for_rejects_empty_name() {
        assert!(udf_path_for("").is_err());
    }

    #[test]
    fn default_context_is_root_with_no_parent_job() {
        let ctx = default_context();
        assert!(
            ctx.is_root(),
            "mutation-scoped schedule defaults to a root request",
        );
    }
}
