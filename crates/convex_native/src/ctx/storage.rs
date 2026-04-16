//! File storage handle exposed to actions and HTTP actions.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.6 + 2.8 wiring.
//!
//! Methods route through the attached [`NativeActionCallbacks`]. In
//! unit tests that use `NoopCallbacks`, every method returns a clear
//! "no callbacks attached" error.

use std::sync::Arc;

use bytes::Bytes;
use value::TableNamespace;

use crate::callbacks::NativeActionCallbacks;

/// Opaque storage id (uuid-like string). Returned from `store` and
/// accepted by `get_url` / `delete`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StorageId(pub String);

impl std::fmt::Display for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for StorageId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(Self(s.to_string()))
    }
}

/// Borrowed storage handle. Obtained via `ActionCtx::storage()` (or
/// the HTTP action equivalent).
pub struct StorageCtx<'a> {
    pub(crate) namespace: TableNamespace,
    pub(crate) callbacks: Arc<dyn NativeActionCallbacks>,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> StorageCtx<'a> {
    pub(crate) fn new_with_callbacks(
        namespace: TableNamespace,
        callbacks: Arc<dyn NativeActionCallbacks>,
    ) -> Self {
        Self {
            namespace,
            callbacks,
            _marker: std::marker::PhantomData,
        }
    }

    /// Store raw bytes. Returns a [`StorageId`] that can be handed back
    /// to `get_url` / `delete`.
    pub async fn store(&self, body: Bytes, content_type: &str) -> anyhow::Result<StorageId> {
        self.callbacks
            .storage_store(self.namespace, body, content_type)
            .await
    }

    /// Get a presigned URL for a stored file.
    pub async fn get_url(&self, id: StorageId) -> anyhow::Result<Option<String>> {
        self.callbacks.storage_get_url(self.namespace, id).await
    }

    /// Delete a stored file. Returns true if the file existed and was
    /// removed.
    pub async fn delete(&self, id: StorageId) -> anyhow::Result<bool> {
        self.callbacks.storage_delete(self.namespace, id).await
    }
}
