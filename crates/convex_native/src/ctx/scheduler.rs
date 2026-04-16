//! Typed scheduler for native functions.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.5.
//!
//! Obtained via `MutationCtx::scheduler()` or
//! `ActionCtx::scheduler()`. Expose a type-safe `run_after(delay,
//! marker, args)` that serializes the typed args and routes to the
//! backend's `VirtualSchedulerModel` when full integration lands.
//! Today the method serializes the args correctly but `bail!`s at the
//! scheduling step — the shape is final so developers can write code
//! against it while the backend glue is built out.

use std::time::Duration;

use value::ConvexValue;

use crate::{
    convert::ToConvex,
    function_ref::{
        ConvexActionFunction,
        ConvexMutationFunction,
    },
};

/// Borrowed handle onto the scheduler.
pub struct Scheduler<'a> {
    pub(crate) scope: SchedulerScope,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

/// Which ctx handed out this scheduler. Mutations and actions share
/// the same typed API but have slightly different backend wire-up.
#[derive(Copy, Clone, Debug)]
pub enum SchedulerScope {
    Mutation,
    Action,
}

impl<'a> Scheduler<'a> {
    pub(crate) fn new(scope: SchedulerScope) -> Self {
        Self {
            scope,
            _marker: std::marker::PhantomData,
        }
    }

    /// Schedule a mutation to run after `delay` with the given typed
    /// args. Returns a placeholder `()` until the scheduler backend
    /// integration lands.
    pub async fn run_after<F: ConvexMutationFunction>(
        &self,
        delay: Duration,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<()> {
        let _obj = match ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("scheduled args must serialize to an object"),
        };
        let _ = (delay, self.scope);
        anyhow::bail!(
            "Scheduler::run_after for mutations is not yet wired — pending VirtualSchedulerModel \
             backend integration"
        )
    }

    /// Schedule an action to run after `delay` with the given typed
    /// args.
    pub async fn run_action_after<F: ConvexActionFunction>(
        &self,
        delay: Duration,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<()> {
        let _obj = match ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("scheduled args must serialize to an object"),
        };
        let _ = (delay, self.scope);
        anyhow::bail!(
            "Scheduler::run_action_after is not yet wired — pending VirtualSchedulerModel backend \
             integration"
        )
    }
}
