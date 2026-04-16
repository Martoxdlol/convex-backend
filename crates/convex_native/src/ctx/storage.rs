//! File storage handle exposed to actions and HTTP actions.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.6.
//!
//! Today the methods serialize the right types and `bail!` pending
//! file_storage backend integration. The shape is final so developers
//! can write code against it.

use bytes::Bytes;

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
/// the HTTP action equivalent). Methods are stubbed pending backend
/// integration — see `COMPOSITE_RUNNER.md`.
pub struct StorageCtx<'a> {
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> StorageCtx<'a> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }

    /// Store raw bytes. Returns a [`StorageId`] that can be handed back
    /// to `get_url` / `delete`.
    pub async fn store(&self, _body: Bytes, _content_type: &str) -> anyhow::Result<StorageId> {
        anyhow::bail!("StorageCtx::store is not yet wired — pending file_storage backend")
    }

    /// Get a presigned URL for a stored file.
    pub async fn get_url(&self, _id: StorageId) -> anyhow::Result<Option<String>> {
        anyhow::bail!("StorageCtx::get_url is not yet wired — pending file_storage backend")
    }

    /// Delete a stored file. Returns true if the file existed and was
    /// removed.
    pub async fn delete(&self, _id: StorageId) -> anyhow::Result<bool> {
        anyhow::bail!("StorageCtx::delete is not yet wired — pending file_storage backend")
    }
}
