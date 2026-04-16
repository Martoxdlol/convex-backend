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
}
