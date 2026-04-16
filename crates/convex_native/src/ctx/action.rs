//! Context wrapper for `#[convex::action]` functions.
//!
//! Actions differ from queries and mutations in two ways:
//!
//! 1. They run **outside** a database transaction — they can do external I/O
//!    (`reqwest`, file storage, etc.).
//! 2. They can **sub-call** queries and mutations, which run in their own
//!    transactions.
//!
//! `ActionCtx` holds:
//! - an optional `Arc<NativeFunctionRunner>` — used to dispatch action
//!   sub-calls directly (no new transaction needed); and
//! - an optional `Arc<dyn NativeActionCallbacks>` — used for everything else
//!   (query/mutation sub-calls with new transactions, scheduler, file storage).
//!   When the callbacks are absent we fall through to a [`NoopCallbacks`]
//!   instance so every API still returns a clear error rather than panicking.

use std::sync::Arc;

use common::runtime::Runtime;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

use crate::{
    callbacks::{
        NativeActionCallbacks,
        NoopCallbacks,
    },
    runner::NativeFunctionRunner,
};

/// Context passed to `#[convex::action]` functions. Actions don't hold
/// a transaction — they do side-effectful I/O and route sub-calls
/// through the runner / backend callbacks.
pub struct ActionCtx<'a, RT: Runtime> {
    pub(crate) runner: Option<Arc<NativeFunctionRunner>>,
    pub(crate) callbacks: Arc<dyn NativeActionCallbacks>,
    pub(crate) namespace: TableNamespace,
    pub(crate) log_buffer: crate::logging::LogBuffer,
    _rt: std::marker::PhantomData<&'a RT>,
}

impl<'a, RT: Runtime> ActionCtx<'a, RT> {
    /// Constructor used by the runner's action dispatch path. The
    /// callbacks default to [`NoopCallbacks`] so the context is usable
    /// in unit tests or when the backend adapter isn't attached.
    pub fn new(runner: Option<Arc<NativeFunctionRunner>>, namespace: TableNamespace) -> Self {
        Self::with_callbacks(runner, Arc::new(NoopCallbacks), namespace)
    }

    /// Constructor that takes explicit callbacks. The backend adapter
    /// wires a real `NativeActionCallbacks` through here.
    pub fn with_callbacks(
        runner: Option<Arc<NativeFunctionRunner>>,
        callbacks: Arc<dyn NativeActionCallbacks>,
        namespace: TableNamespace,
    ) -> Self {
        Self {
            runner,
            callbacks,
            namespace,
            log_buffer: crate::logging::LogBuffer::new(),
            _rt: std::marker::PhantomData,
        }
    }

    /// Same as [`with_callbacks`] but attaches an externally-owned log
    /// buffer so callers can inspect the captured lines after the
    /// handler returns. The runner uses this to surface log lines
    /// through the standard log-streaming path.
    pub fn with_callbacks_and_log_buffer(
        runner: Option<Arc<NativeFunctionRunner>>,
        callbacks: Arc<dyn NativeActionCallbacks>,
        namespace: TableNamespace,
        log_buffer: crate::logging::LogBuffer,
    ) -> Self {
        Self {
            runner,
            callbacks,
            namespace,
            log_buffer,
            _rt: std::marker::PhantomData,
        }
    }

    /// Borrow a logger that writes into the ctx's log buffer.
    pub fn log(&self) -> crate::logging::Logger<'_> {
        crate::logging::Logger {
            buffer: &self.log_buffer,
        }
    }

    /// The namespace this action is running in.
    pub fn namespace(&self) -> TableNamespace {
        self.namespace
    }

    /// Scheduler handle — see [`super::scheduler::Scheduler`].
    pub fn scheduler(&mut self) -> super::scheduler::Scheduler<'_> {
        super::scheduler::Scheduler::new_with_callbacks(
            super::scheduler::SchedulerScope::Action,
            self.namespace,
            self.callbacks.clone(),
        )
    }

    /// File-storage handle — see [`super::storage::StorageCtx`].
    pub fn storage(&mut self) -> super::storage::StorageCtx<'_> {
        super::storage::StorageCtx::new_with_callbacks(self.namespace, self.callbacks.clone())
    }

    /// Invoke a native query by name with already-serialized args.
    /// Routes through the attached callbacks.
    pub async fn run_query_raw(
        &mut self,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.callbacks
            .run_query_by_name(self.namespace, name, args)
            .await
    }

    /// Invoke a native mutation by name with already-serialized args.
    pub async fn run_mutation_raw(
        &mut self,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.callbacks
            .run_mutation_by_name(self.namespace, name, args)
            .await
    }

    /// Typed sub-call: invoke a `#[convex::query]` by marker type.
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
    /// Actions are dispatched directly through the native runner (no
    /// new transaction). If no runner is attached, returns an error.
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

    /// Whether a native function with the given name is available on
    /// the attached runner.
    pub fn has_function(&self, name: &str) -> bool {
        self.runner.as_ref().is_some_and(|r| r.has_function(name))
    }
}
