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
    pub(crate) execution_context: Option<common::execution_context::ExecutionContext>,
    /// Identity the action is running under. Populated by the
    /// worker-side dispatch path from the decoded
    /// `ExecuteRequest.identity` bytes. `None` ⇒ unknown (for
    /// tests and in-process callers that don't thread identity
    /// explicitly; `auth()` treats this as anonymous).
    pub(crate) identity: Option<keybroker::Identity>,
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
            execution_context: None,
            identity: None,
            _rt: std::marker::PhantomData,
        }
    }

    /// Attach an `Identity` to the ctx so `auth()` returns a
    /// meaningful view. Used by the worker-side dispatch path
    /// after decoding the wire identity bytes, and by tests that
    /// want to exercise auth-gated handlers.
    pub fn with_identity(mut self, identity: keybroker::Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Borrowed view of the caller's identity. Returns an
    /// anonymous view (`AuthInfo::new(&Identity::Unknown(None))`)
    /// when no identity was attached — matches the QueryCtx /
    /// MutationCtx semantics for unauthenticated callers.
    pub fn auth(&self) -> crate::auth::AuthInfo<'_> {
        static ANON: std::sync::OnceLock<keybroker::Identity> = std::sync::OnceLock::new();
        let id = self
            .identity
            .as_ref()
            .unwrap_or_else(|| ANON.get_or_init(|| keybroker::Identity::Unknown(None)));
        crate::auth::AuthInfo::new(id)
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
            execution_context: None,
            identity: None,
            _rt: std::marker::PhantomData,
        }
    }

    /// Attach the enclosing request's `ExecutionContext` — exposed
    /// through `execution_context()`. The action-scoped scheduler
    /// reads its context off the attached callbacks (not the ctx),
    /// so this slot is purely for observability / structured logs.
    pub fn with_execution_context(
        mut self,
        execution_context: common::execution_context::ExecutionContext,
    ) -> Self {
        self.execution_context = Some(execution_context);
        self
    }

    /// Borrow the enclosing request's `ExecutionContext`, if any.
    pub fn execution_context(&self) -> Option<&common::execution_context::ExecutionContext> {
        self.execution_context.as_ref()
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

    /// Read-only, snapshot-pinned database handle.
    ///
    /// Unlike `QueryCtx::db()`, this handle doesn't own a
    /// `Transaction<RT>` — actions never do. Instead each read routes
    /// through the attached
    /// [`NativeActionCallbacks::read_document_at_snapshot`], which opens a
    /// short-lived read-only transaction at the action's pinned snapshot
    /// timestamp. Multiple `ctx.db().get(...)` calls inside one action
    /// therefore observe one consistent world, matching the
    /// [`super::query::QueryDb::get`] contract.
    ///
    /// Writes are not supported on this handle — mutations still go
    /// through `ctx.run_mutation(...)` / `ctx.run_mutation_raw(...)`
    /// so they commit in their own transaction (actions don't own
    /// one). This method is a convenience on top of the composite
    /// runner path; the distributed worker and [`NoopCallbacks`]
    /// both bail at read time with a clear error.
    pub fn db(&mut self) -> ActionDb<'_> {
        ActionDb {
            callbacks: self.callbacks.clone(),
            namespace: self.namespace,
            _marker: std::marker::PhantomData,
        }
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
    /// First tries the local native runner — same-worker action
    /// sub-calls take this path and avoid a callback round-trip.
    /// Falls back to `NativeActionCallbacks::run_action_by_name`
    /// when the local runner doesn't carry the target (e.g.
    /// distributed deployment where the callee lives on a
    /// different worker, or JS-side action behind the callback).
    pub async fn run_action<F: crate::function_ref::ConvexActionFunction>(
        &mut self,
        _marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        let obj = match crate::convert::ToConvex::to_convex(args)? {
            ConvexValue::Object(o) => o,
            _ => anyhow::bail!("typed args must serialize to an object"),
        };
        let ret = self.run_action_raw(F::name(), obj).await?;
        <F::Output as crate::convert::FromConvex>::from_convex(ret)
    }

    /// Untyped action sub-call. Mirrors [`run_query_raw`] /
    /// [`run_mutation_raw`]. Routes through the local native
    /// runner when the target name is known there; otherwise
    /// dispatches via `NativeActionCallbacks::run_action_by_name`
    /// (which the distributed adapter translates into a `RunAction`
    /// RPC and the monolith adapter translates into the JS path).
    pub async fn run_action_raw(
        &mut self,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        if let Some(runner) = self.runner.as_ref()
            && runner.has_function(name)
        {
            return runner.run_action(name, self.namespace, args).await;
        }
        self.callbacks
            .run_action_by_name(self.namespace, name, args)
            .await
    }

    /// Whether a native function with the given name is available on
    /// the attached runner.
    pub fn has_function(&self, name: &str) -> bool {
        self.runner.as_ref().is_some_and(|r| r.has_function(name))
    }
}

/// Read-only database handle returned by [`ActionCtx::db`].
///
/// Holds only a clone of the action's callbacks — the actual
/// transaction is opened fresh (at the pinned snapshot ts) for each
/// read by the backend adapter. That pattern matches how the JS
/// action runtime exposes `ctx.runQuery` / `ctx.db` under the hood:
/// actions are transactionless at the top level but individual reads
/// project through a backend-owned tx.
pub struct ActionDb<'a> {
    callbacks: Arc<dyn NativeActionCallbacks>,
    namespace: TableNamespace,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ActionDb<'a> {
    /// Fetch a document by id against the action's pinned snapshot.
    /// Returns `None` if the document doesn't exist.
    pub async fn get<T: crate::document::ConvexDocument>(
        &self,
        id: crate::id::Id<T>,
    ) -> anyhow::Result<Option<T>> {
        let table = T::table_name();
        let dev_id = id.into_developer_id();
        let maybe_obj = self
            .callbacks
            .read_document_at_snapshot(self.namespace, table, dev_id)
            .await?;
        maybe_obj.map(T::from_convex_object).transpose()
    }

    /// Fetch a document by id, erroring when it doesn't exist.
    /// Saves the `.ok_or_else(...)` ceremony in the common case.
    pub async fn try_get<T: crate::document::ConvexDocument>(
        &self,
        id: crate::id::Id<T>,
    ) -> anyhow::Result<T> {
        self.get(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("document {id} not found in {}", T::table_name()))
    }

    /// Bulk-fetch documents by id. Returns `None` for ids that don't
    /// resolve, preserving input order — same contract as
    /// [`super::query::QueryDb::get_many`].
    pub async fn get_many<T: crate::document::ConvexDocument>(
        &self,
        ids: impl IntoIterator<Item = crate::id::Id<T>>,
    ) -> anyhow::Result<Vec<Option<T>>> {
        let mut out = Vec::new();
        for id in ids {
            out.push(self.get(id).await?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use value::FieldName;

    use super::*;
    use crate::{
        logging::{
            LogBuffer,
            LogLevel,
        },
        registry::Rt,
    };

    fn empty_obj() -> ConvexObject {
        ConvexObject::try_from(BTreeMap::<FieldName, ConvexValue>::new()).unwrap()
    }

    #[test]
    fn new_defaults_to_noop_callbacks_and_carries_namespace() {
        // `ActionCtx::new(None, ns)` must be usable without a backend —
        // that's the "unit-test-friendly" contract the module docs
        // promise. We can't read the callbacks field directly (it's
        // `pub(crate)`) but we can probe it behaviourally by invoking
        // a method that forwards to `callbacks` and confirm the
        // NoopCallbacks error surfaces.
        let ctx: ActionCtx<'_, Rt> = ActionCtx::new(None, TableNamespace::Global);
        assert_eq!(ctx.namespace(), TableNamespace::Global);
        assert!(
            !ctx.has_function("missing"),
            "no runner means no function known",
        );
    }

    #[tokio::test]
    async fn run_query_raw_through_noop_ctx_surfaces_a_clear_error() {
        // End-to-end verification that `ActionCtx::new(None, ns)`
        // actually wires NoopCallbacks: invoking any callback-backed
        // method must error with a clear "no callbacks attached"
        // message rather than panicking or returning a bogus result.
        let mut ctx: ActionCtx<'_, Rt> = ActionCtx::new(None, TableNamespace::Global);
        let err = ctx
            .run_query_raw("anything", empty_obj())
            .await
            .expect_err("noop callbacks bail");
        assert!(format!("{err}").contains("cannot run query"));
    }

    #[test]
    fn log_writes_into_the_internal_buffer() {
        // `ctx.log()` borrows the ActionCtx's own LogBuffer. Writes
        // through the returned Logger must show up when the same ctx
        // is inspected afterwards (the runner uses exactly this
        // pattern to drain log lines into the streaming path).
        let ctx: ActionCtx<'_, Rt> = ActionCtx::new(None, TableNamespace::Global);
        ctx.log().info("hello from action");
        ctx.log().warn("something worth noting");
        let lines = ctx.log_buffer.snapshot();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].level, LogLevel::Info);
        assert_eq!(lines[0].message, "hello from action");
        assert_eq!(lines[1].level, LogLevel::Warn);
    }

    #[test]
    fn with_callbacks_and_log_buffer_uses_the_supplied_buffer() {
        // Passing an external `LogBuffer` into the constructor wires
        // `ctx.log()` to that buffer — same contract the runner uses
        // to capture lines into a caller-owned sink.
        let external = LogBuffer::new();
        let ctx: ActionCtx<'_, Rt> = ActionCtx::with_callbacks_and_log_buffer(
            None,
            Arc::new(NoopCallbacks),
            TableNamespace::Global,
            external.clone(),
        );
        ctx.log().error("boom");
        // External buffer sees the write because it's Arc-shared with
        // the ctx's internal handle.
        let snap = external.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].level, LogLevel::Error);
        assert_eq!(snap[0].message, "boom");
    }

    #[test]
    fn has_function_is_false_when_no_runner_is_attached() {
        let ctx: ActionCtx<'_, Rt> = ActionCtx::new(None, TableNamespace::Global);
        assert!(!ctx.has_function("any_name"));
    }
}
