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

/// Metadata for a stored file. Returned from [`StorageCtx::get_metadata`].
///
/// Mirrors the JS `ctx.storage.getMetadata()` shape, minus the
/// storage-internal object key (not exposed to developer code).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    /// MIME type the file was stored with, when known.
    pub content_type: Option<String>,
    /// Size in bytes.
    pub size: i64,
    /// Hex-encoded SHA-256 digest of the stored bytes.
    pub sha256: String,
}

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

    /// Fetch metadata for a stored file. Returns `None` when the id
    /// doesn't resolve to any stored object.
    pub async fn get_metadata(&self, id: StorageId) -> anyhow::Result<Option<FileMetadata>> {
        self.callbacks
            .storage_get_metadata(self.namespace, id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{
            hash_map::DefaultHasher,
            HashSet,
        },
        hash::{
            Hash,
            Hasher,
        },
        str::FromStr,
    };

    use super::*;
    use crate::callbacks::NoopCallbacks;

    fn hash_one<T: Hash>(t: &T) -> u64 {
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }

    #[test]
    fn display_emits_the_inner_string_verbatim() {
        let id = StorageId("abc-123".into());
        assert_eq!(id.to_string(), "abc-123");
    }

    #[test]
    fn from_str_wraps_the_input_without_validation() {
        // StorageId is an opaque token; from_str accepts any string
        // because the backend owns the id format. We only pin the
        // round-trip: what goes in comes out.
        let id: StorageId = "arbitrary_opaque_token".parse().unwrap();
        assert_eq!(id.to_string(), "arbitrary_opaque_token");
    }

    #[test]
    fn equal_ids_hash_identically() {
        let a = StorageId("x".into());
        let b = StorageId("x".into());
        assert_eq!(a, b);
        assert_eq!(hash_one(&a), hash_one(&b));
    }

    #[test]
    fn storage_id_is_usable_as_hashset_key() {
        // The `Eq + Hash` derives are load-bearing — callers stash
        // `StorageId` in `HashSet`/`HashMap` to dedupe references.
        // A silent removal of either would break that without a
        // type-level signal, so pin the behaviour.
        let mut set: HashSet<StorageId> = HashSet::new();
        set.insert(StorageId("a".into()));
        set.insert(StorageId("a".into()));
        set.insert(StorageId("b".into()));
        assert_eq!(set.len(), 2);
    }

    #[tokio::test]
    async fn storage_ctx_delegates_store_errors_from_noop_callbacks() {
        // End-to-end shape check: `StorageCtx.store(...)` must forward
        // the error from the callbacks layer rather than swallowing it.
        // Tested via `NoopCallbacks` so we don't need a real backend.
        let ctx = StorageCtx::new_with_callbacks(TableNamespace::Global, Arc::new(NoopCallbacks));
        let err = ctx
            .store(Bytes::from_static(b"data"), "text/plain")
            .await
            .expect_err("noop callbacks bail");
        assert!(format!("{err}").contains("cannot store"));
    }

    #[tokio::test]
    async fn storage_ctx_get_metadata_bails_through_noop_callbacks() {
        // `NoopCallbacks` inherits the trait default on
        // `storage_get_metadata` (bails with "not implemented by
        // this backend adapter"). Pin that the ctx layer forwards
        // that error through rather than silently returning
        // `Ok(None)` — tests written against the shape would pass
        // a silent None and hide a wiring mistake.
        let ctx = StorageCtx::new_with_callbacks(TableNamespace::Global, Arc::new(NoopCallbacks));
        let err = ctx
            .get_metadata(StorageId("any".into()))
            .await
            .expect_err("noop callbacks bail");
        assert!(format!("{err}").contains("storage_get_metadata not implemented"));
    }

    #[tokio::test]
    async fn storage_ctx_delegates_delete_and_get_url_to_callbacks() {
        // Same delegation-shape check for get_url + delete — the
        // methods are tiny forwarders but "tiny forwarders" are
        // exactly where silent off-by-one mistakes (wrong namespace,
        // dropped argument) tend to sneak in.
        let ctx = StorageCtx::new_with_callbacks(TableNamespace::Global, Arc::new(NoopCallbacks));
        assert!(ctx
            .get_url(StorageId("x".into()))
            .await
            .expect_err("noop bails")
            .to_string()
            .contains("cannot read"));
        assert!(ctx
            .delete(StorageId("x".into()))
            .await
            .expect_err("noop bails")
            .to_string()
            .contains("cannot delete"));
    }

    #[test]
    fn from_str_never_fails_on_any_utf8_string() {
        // Confirms the `Err = anyhow::Error` in the `FromStr` impl is
        // structural, not a hidden validation. If we ever want to
        // reject malformed ids, this test will flip and force a
        // deliberate API change.
        for s in ["", " ", "a b c", "with/slashes/and/stuff", "🎉"] {
            let id = StorageId::from_str(s).expect("never errors");
            assert_eq!(id.to_string(), s);
        }
    }
}
