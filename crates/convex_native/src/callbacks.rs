//! Backend-injected callbacks used by `ActionCtx` / `HttpActionCtx`.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.8.
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
//! `convex_native` already depends on, so the crate stays lightweight.
//! The backend integration crate (future `crates/convex_native_backend/`)
//! implements `NativeActionCallbacks` by delegating to the full
//! `udf::ActionCallbacks` trait.

use std::time::Duration;

use async_trait::async_trait;
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
