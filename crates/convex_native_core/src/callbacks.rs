//! Backend-injected callbacks used by `ActionCtx` / `HttpActionCtx`.
//!
//! The problem: an `#[convex::action]` (or HTTP action) running natively
//! may need to call **queries** or **mutations**, which require a fresh
//! database transaction per call. It may also need to schedule future
//! work, or read/write file storage. Those operations live on the
//! backend side of the boundary — the runner doesn't own a
//! `Database<RT>` directly; instead the `CompositeFunctionRunner`
//! (documented in `COMPOSITE_RUNNER.md`) owns it and hands the native
//! side a callback object.
//!
//! This module defines that callback trait in terms of types
//! `convex_native_core` already depends on, so the crate stays lightweight.
//! The backend integration crate (future `crates/convex_native_backend/`)
//! implements `NativeActionCallbacks` by delegating to the full
//! `udf::ActionCallbacks` trait.

use std::time::{
    Duration,
    SystemTime,
};

use async_trait::async_trait;
use common::runtime::UnixTimestamp;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableName,
    TableNamespace,
};

use crate::ctx::storage::StorageId;

/// Callbacks the native action context calls out to for operations
/// that require backend-owned state (database transactions, scheduler,
/// file storage).
///
/// A no-op default is shipped as [`NoopCallbacks`] for unit-test
/// scenarios where no backend is attached.
#[async_trait]
pub trait NativeActionCallbacks: Send + Sync + 'static {
    /// Execute a registered query by name against a *new* transaction
    /// and return the serialized result.
    async fn run_query_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue>;

    /// Execute a registered mutation by name against a *new* transaction
    /// and return the serialized result.
    async fn run_mutation_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue>;

    /// Execute a registered action by name and return the
    /// serialized result. Used when an action sub-calls another
    /// action that lives outside the local worker (e.g. on
    /// another worker in the pool, or on a JS worker).
    ///
    /// The default implementation bails so adapters that don't
    /// support cross-worker action sub-calls fail loudly; the
    /// distributed `BackendCallbackClient` overrides to dispatch
    /// via `RunAction`, and the in-process `BackendCallbacks`
    /// adapter overrides to short-circuit through the local
    /// native runner / fall back to `ActionCallbacks::execute_action`.
    async fn run_action_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let _ = (namespace, name, args);
        anyhow::bail!(
            "NativeActionCallbacks::run_action_by_name not implemented by this backend adapter"
        )
    }

    /// Schedule a (mutation | action) to run after `delay`. Returns the
    /// scheduled-job id.
    async fn schedule(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId>;

    /// Cancel a previously scheduled job. Returns `Ok(())` whether or
    /// not the job was still pending — idempotent.
    async fn cancel_scheduled(
        &self,
        namespace: TableNamespace,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        let _ = (namespace, id);
        anyhow::bail!(
            "NativeActionCallbacks::cancel_scheduled not implemented by this backend adapter"
        )
    }

    /// Store a blob in file storage. Returns its new id.
    async fn storage_store(
        &self,
        namespace: TableNamespace,
        body: bytes::Bytes,
        content_type: &str,
    ) -> anyhow::Result<StorageId>;

    /// Presign or otherwise expose a stored file.
    async fn storage_get_url(
        &self,
        namespace: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<String>>;

    /// Delete a stored file. Returns true if it existed.
    async fn storage_delete(
        &self,
        namespace: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<bool>;

    /// Fetch metadata for a stored file. Returns `None` when the id
    /// doesn't resolve to a stored file.
    ///
    /// The default implementation bails so backends unable to read
    /// file metadata (`NoopCallbacks`, test stubs) fail loudly; the
    /// `BackendCallbacks` adapter overrides to reach
    /// `ActionCallbacks::storage_get_file_entry` and project the
    /// result into a [`FileMetadata`].
    async fn storage_get_metadata(
        &self,
        namespace: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<crate::ctx::storage::FileMetadata>> {
        let _ = (namespace, id);
        anyhow::bail!(
            "NativeActionCallbacks::storage_get_metadata not implemented by this backend adapter"
        )
    }

    /// Return "now" for the purposes of action-scoped `run_at`.
    ///
    /// The default implementation uses `SystemTime::now()`, which
    /// doesn't honour a mocked runtime clock. Backend adapters that
    /// do hold a real `Runtime` (e.g. `BackendCallbacks` via its
    /// `Database<RT>`) should override to return
    /// `runtime.unix_timestamp()` so that tests driving a mocked
    /// clock can assert on the exact computed delay. Mutation-scoped
    /// `run_at` already uses the transaction's runtime clock
    /// directly; this method exists so actions can match that.
    fn unix_timestamp_now(&self) -> UnixTimestamp {
        // Fall back to wall clock. `SystemTime::now()` is guaranteed
        // non-negative relative to the Unix epoch on all supported
        // platforms, so the fallback path is
        // `Duration::default()` only when the host clock is before
        // 1970 — effectively never.
        UnixTimestamp::from_system_time(SystemTime::now()).unwrap_or_else(|| {
            UnixTimestamp::from_secs_f64(0.0).expect("zero is a valid unix timestamp")
        })
    }

    /// Fetch a document by id using the callback's snapshot view —
    /// without opening a full user query. `BackendCallbacks` opens a
    /// fresh read-only transaction pinned to the action's pinned
    /// snapshot timestamp (so multiple `ctx.db().get(...)` calls
    /// inside one action see a consistent world), runs
    /// `UserFacingModel::get_with_ts`, and returns the serialized
    /// object.
    ///
    /// Backends without a native `Database<RT>` hook (e.g. the
    /// distributed worker) and `NoopCallbacks` both bail — native
    /// `ctx.db()` is a convenience on top of the composite runner
    /// path, and callers that need portability should keep using
    /// typed `ctx.run_query(...)` sub-calls.
    async fn read_document_at_snapshot(
        &self,
        namespace: TableNamespace,
        table: TableName,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<Option<ConvexObject>> {
        let _ = (namespace, table, id);
        anyhow::bail!(
            "NativeActionCallbacks::read_document_at_snapshot not implemented by this backend \
             adapter"
        )
    }
}

/// Fallback that `bail!`s on every callback — used when an `ActionCtx`
/// is built without a real backend attached (e.g. in a plain unit test).
pub struct NoopCallbacks;

#[async_trait]
impl NativeActionCallbacks for NoopCallbacks {
    async fn run_query_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        anyhow::bail!("no callbacks attached — cannot run query {name:?}")
    }

    async fn run_mutation_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        anyhow::bail!("no callbacks attached — cannot run mutation {name:?}")
    }

    async fn run_action_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        anyhow::bail!("no callbacks attached — cannot run action {name:?}")
    }

    async fn schedule(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
        _delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        anyhow::bail!("no callbacks attached — cannot schedule {name:?}")
    }

    async fn cancel_scheduled(
        &self,
        _ns: TableNamespace,
        _id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        anyhow::bail!("no callbacks attached — cannot cancel scheduled job")
    }

    async fn storage_store(
        &self,
        _ns: TableNamespace,
        _body: bytes::Bytes,
        _content_type: &str,
    ) -> anyhow::Result<StorageId> {
        anyhow::bail!("no callbacks attached — cannot store in file storage")
    }

    async fn storage_get_url(
        &self,
        _ns: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        anyhow::bail!("no callbacks attached — cannot read file storage url")
    }

    async fn storage_delete(&self, _ns: TableNamespace, _id: StorageId) -> anyhow::Result<bool> {
        anyhow::bail!("no callbacks attached — cannot delete from file storage")
    }

    async fn read_document_at_snapshot(
        &self,
        _ns: TableNamespace,
        table: TableName,
        _id: DeveloperDocumentId,
    ) -> anyhow::Result<Option<ConvexObject>> {
        anyhow::bail!("no callbacks attached — cannot read document from table {table}")
    }
}

#[cfg(test)]
mod tests {
    //! Every `NoopCallbacks` method must fail loudly when invoked.
    //! An accidental `Ok(())` would silently drop user operations
    //! (e.g. a scheduled job that never runs, a mutation that never
    //! commits) — these tests pin the "every path bails" contract.

    use std::collections::BTreeMap;

    use value::{
        DeveloperDocumentId,
        FieldName,
    };

    use super::*;

    fn empty_obj() -> ConvexObject {
        ConvexObject::try_from(BTreeMap::<FieldName, ConvexValue>::new()).unwrap()
    }

    fn id() -> DeveloperDocumentId {
        DeveloperDocumentId::MIN
    }

    #[tokio::test]
    async fn run_query_by_name_bails() {
        let err = NoopCallbacks
            .run_query_by_name(TableNamespace::Global, "get_user", empty_obj())
            .await
            .expect_err("no callbacks attached");
        assert!(
            format!("{err}").contains("cannot run query"),
            "error names the operation: {err}",
        );
    }

    #[tokio::test]
    async fn run_mutation_by_name_bails() {
        let err = NoopCallbacks
            .run_mutation_by_name(TableNamespace::Global, "set_user", empty_obj())
            .await
            .expect_err("no callbacks attached");
        assert!(format!("{err}").contains("cannot run mutation"));
    }

    #[tokio::test]
    async fn run_action_by_name_bails() {
        let err = NoopCallbacks
            .run_action_by_name(TableNamespace::Global, "send_email", empty_obj())
            .await
            .expect_err("no callbacks attached");
        assert!(
            format!("{err}").contains("cannot run action"),
            "error names the operation: {err}",
        );
    }

    #[tokio::test]
    async fn schedule_bails() {
        let err = NoopCallbacks
            .schedule(
                TableNamespace::Global,
                "bg_job",
                empty_obj(),
                Duration::from_secs(1),
            )
            .await
            .expect_err("no callbacks attached");
        assert!(format!("{err}").contains("cannot schedule"));
    }

    #[tokio::test]
    async fn cancel_scheduled_bails() {
        let err = NoopCallbacks
            .cancel_scheduled(TableNamespace::Global, id())
            .await
            .expect_err("no callbacks attached");
        assert!(format!("{err}").contains("cannot cancel"));
    }

    #[tokio::test]
    async fn storage_store_bails() {
        let err = NoopCallbacks
            .storage_store(
                TableNamespace::Global,
                bytes::Bytes::from_static(b"abc"),
                "text/plain",
            )
            .await
            .expect_err("no callbacks attached");
        assert!(format!("{err}").contains("cannot store"));
    }

    #[tokio::test]
    async fn storage_get_url_bails() {
        let err = NoopCallbacks
            .storage_get_url(TableNamespace::Global, StorageId("abc".into()))
            .await
            .expect_err("no callbacks attached");
        assert!(format!("{err}").contains("cannot read"));
    }

    #[tokio::test]
    async fn storage_delete_bails() {
        let err = NoopCallbacks
            .storage_delete(TableNamespace::Global, StorageId("abc".into()))
            .await
            .expect_err("no callbacks attached");
        assert!(format!("{err}").contains("cannot delete"));
    }
}
