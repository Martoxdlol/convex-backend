//! Context wrapper for `#[convex::action]` functions.
//!
//! Actions differ from queries and mutations in two ways:
//!
//! 1. They run **outside** a database transaction — they can do external I/O
//!    (`reqwest`, file storage, etc.).
//! 2. They can **sub-call** queries and mutations, which run in their own
//!    transactions.
//!
//! As such `ActionCtx` does not wrap a `Transaction<RT>` — it holds a
//! handle to the `NativeFunctionRunner` (for typed sub-calls) and to
//! the backend's `ActionCallbacks` trait (for raw sub-calls /
//! scheduling / storage / etc.).
//!
//! Today this context is a skeleton: typed sub-calls and scheduler
//! hooks are stubbed with `todo!()` or `bail!("not yet implemented")`.
//! The shape is correct so developers' `#[convex::action]` functions
//! compile against the final API; execution lands alongside the full
//! backend wiring.

use std::sync::Arc;

use common::runtime::Runtime;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

use crate::runner::NativeFunctionRunner;

/// Context passed to `#[convex::action]` functions. Actions don't hold
/// a transaction — they do side-effectful I/O and route sub-calls
/// through the runner / backend callbacks.
pub struct ActionCtx<'a, RT: Runtime> {
    pub(crate) runner: Option<Arc<NativeFunctionRunner>>,
    pub(crate) namespace: TableNamespace,
    _rt: std::marker::PhantomData<&'a RT>,
}

impl<'a, RT: Runtime> ActionCtx<'a, RT> {
    pub fn new(runner: Option<Arc<NativeFunctionRunner>>, namespace: TableNamespace) -> Self {
        Self {
            runner,
            namespace,
            _rt: std::marker::PhantomData,
        }
    }

    /// The namespace this action is running in.
    pub fn namespace(&self) -> TableNamespace {
        self.namespace
    }

    /// Scheduler handle — see [`super::scheduler::Scheduler`].
    pub fn scheduler(&mut self) -> super::scheduler::Scheduler<'_> {
        super::scheduler::Scheduler::new(super::scheduler::SchedulerScope::Action)
    }

    /// Invoke a native query by name with already-serialized args.
    /// Returns the `ConvexValue` the query produced. Typed sub-calls
    /// land in Step 2.4 once generated args structs exist.
    ///
    /// Today this is a stub — full end-to-end sub-call support needs
    /// the `ActionCallbacks`-backed transaction orchestrator from the
    /// backend. We surface the API with a clear error so developers
    /// see the final shape while iterating on the rest of the crate.
    pub async fn run_query_raw(
        &mut self,
        _name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        anyhow::bail!(
            "ActionCtx::run_query_raw is not yet wired — pending full backend integration (see \
             convex-native/COMPOSITE_RUNNER.md)"
        )
    }

    /// Same as [`run_query_raw`] but for mutations.
    pub async fn run_mutation_raw(
        &mut self,
        _name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        anyhow::bail!(
            "ActionCtx::run_mutation_raw is not yet wired — pending full backend integration"
        )
    }

    /// Typed sub-call: invoke a `#[convex::query]` by marker type.
    /// Returns the function's typed `Output`. Routes through
    /// `run_query_raw` — which is currently a stub — so this fails
    /// cleanly with a "not yet wired" error today, but the API surface
    /// is the final shape.
    pub async fn run_query<F: crate::function_ref::ConvexQueryFunction>(
        &mut self,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        let obj = match crate::convert::ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("typed args must serialize to an object"),
        };
        let ret = self.run_query_raw(F::name(), obj).await?;
        <F::Output as crate::convert::FromConvex>::from_convex(ret)
    }

    /// Typed sub-call: invoke a `#[convex::mutation]` by marker type.
    pub async fn run_mutation<F: crate::function_ref::ConvexMutationFunction>(
        &mut self,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        let obj = match crate::convert::ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("typed args must serialize to an object"),
        };
        let ret = self.run_mutation_raw(F::name(), obj).await?;
        <F::Output as crate::convert::FromConvex>::from_convex(ret)
    }

    /// Typed sub-call: invoke a `#[convex::action]` by marker type.
    /// Unlike queries/mutations, actions run entirely inside the
    /// native runner (no separate transaction), so this path works
    /// end-to-end today.
    pub async fn run_action<F: crate::function_ref::ConvexActionFunction>(
        &mut self,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        let obj = match crate::convert::ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("typed args must serialize to an object"),
        };
        let runner = self
            .runner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ActionCtx: no runner attached for sub-call"))?
            .clone();
        let ret = runner.run_action(F::name(), self.namespace, obj).await?;
        <F::Output as crate::convert::FromConvex>::from_convex(ret)
    }

    /// Whether a native function with the given name is available.
    /// Useful for smoke-testing registration wiring without needing the
    /// full execution path.
    pub fn has_function(&self, name: &str) -> bool {
        self.runner.as_ref().is_some_and(|r| r.has_function(name))
    }
}
