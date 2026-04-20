//! Wires the Phase-4 `BackendCallbackService` (defined in
//! `convex_native_distributed`) onto `local_backend`'s `Application`
//! handle.
//!
//! Two trait implementations live here:
//!
//! - [`BackendFileBytesImpl`] — implements
//!   `convex_native_distributed::backend_callbacks_server::BackendFileBytes` on
//!   top of `FileStorage<ProdRuntime>`. Powers the streaming `StorageStore` /
//!   `StorageGet` callback RPCs so a remote worker's action can upload /
//!   download bytes through the backend's existing file-storage stack.
//! - [`ApplicationComponentResolver`] — implements `ComponentResolver` against
//!   the backend's `Database`, mapping the wire `ComponentPath` strings workers
//!   send into the `ComponentId` form the `ActionCallbacks` storage /
//!   scheduling methods consume.

use async_trait::async_trait;
use bytes::Bytes;
use common::components::{
    ComponentId,
    ComponentPath,
};
use convex_native_distributed::backend_callbacks_server::{
    BackendFileBytes,
    BackendFileBytesResponse,
    ComponentResolver,
};
use database::Database;
use file_storage::FileStorage;
use futures::{
    stream,
    StreamExt,
};
use keybroker::Identity;
use model::file_storage::FileStorageId;
use runtime::prod::ProdRuntime;
use usage_tracking::FunctionUsageTracker;
use value::{
    sha256::Sha256Digest,
    DeveloperDocumentId,
};

/// Backend-side `BackendFileBytes` impl wrapping the
/// `FileStorage` handle the `Application` already owns. Lets the
/// gRPC `BackendCallbackService` route a worker's
/// `ctx.storage().store(...)` / `ctx.storage().get(...)` calls
/// through the same upload/download path the HTTP storage API
/// uses.
pub struct BackendFileBytesImpl {
    file_storage: FileStorage<ProdRuntime>,
    database: Database<ProdRuntime>,
}

impl BackendFileBytesImpl {
    pub fn new(file_storage: FileStorage<ProdRuntime>, database: Database<ProdRuntime>) -> Self {
        Self {
            file_storage,
            database,
        }
    }
}

#[async_trait]
impl BackendFileBytes for BackendFileBytesImpl {
    async fn store_bytes(
        &self,
        _identity: Identity,
        component: ComponentId,
        content_type: Option<String>,
        expected_sha256: Option<Sha256Digest>,
        body: Bytes,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let content_type_header = content_type
            .as_ref()
            .map(|ct| ct.parse::<headers::ContentType>())
            .transpose()?;
        let content_length_header = Some(headers::ContentLength(body.len() as u64));
        let body_clone = body.clone();
        let stream = stream::once(async move { Ok::<Bytes, anyhow::Error>(body_clone) });
        let usage_tracker = FunctionUsageTracker::new();
        self.file_storage
            .store_file(
                component.into(),
                content_length_header,
                content_type_header,
                stream,
                expected_sha256,
                &usage_tracker,
            )
            .await
    }

    async fn get_bytes(
        &self,
        identity: Identity,
        component: ComponentId,
        storage_id: FileStorageId,
    ) -> anyhow::Result<BackendFileBytesResponse> {
        let mut tx = self.database.begin(identity).await?;
        let entry = self
            .file_storage
            .transactional_file_storage
            .get_file_entry(&mut tx, component.into(), storage_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("storage entry not found"))?;
        let component_path = tx
            .must_component_path(component)
            .map_err(|e| anyhow::anyhow!("get_bytes component path: {e}"))?;
        let content_type = entry.content_type.clone();
        let content_length = entry.size as u64;
        let sha256 = entry.sha256.clone();
        let usage_tracker = FunctionUsageTracker::new();
        let mut stream = self
            .file_storage
            .transactional_file_storage
            .get_file_stream(component_path, entry, usage_tracker)
            .await?;
        let mut body = Vec::with_capacity(content_length as usize);
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk?);
        }
        Ok(BackendFileBytesResponse {
            content_type,
            content_length,
            sha256,
            body: Bytes::from(body),
        })
    }
}

/// Resolves wire `ComponentPath` strings into `ComponentId` by
/// consulting the backend's database. Plugs into
/// `BackendCallbackServer::with_component_resolver` so non-root
/// component callbacks (storage / scheduling) work in
/// component-using deployments. Root paths short-circuit so the
/// common case takes no transaction.
pub struct ApplicationComponentResolver {
    database: Database<ProdRuntime>,
}

impl ApplicationComponentResolver {
    pub fn new(database: Database<ProdRuntime>) -> Self {
        Self { database }
    }
}

#[async_trait]
impl ComponentResolver for ApplicationComponentResolver {
    async fn resolve(&self, path: &ComponentPath) -> anyhow::Result<ComponentId> {
        if path.is_root() {
            return Ok(ComponentId::Root);
        }
        let mut tx = self.database.begin(Identity::system()).await?;
        let (_, component_id) =
            database::BootstrapComponentsModel::new(&mut tx).must_component_path_to_ids(path)?;
        Ok(component_id)
    }
}

/// `BackendDocumentReader` impl for the production `Database`.
/// Powers `ReadDocument` from the worker-side
/// `BackendCallbackClient::read_document_at_snapshot` —
/// distributed-action `ctx.db().get(...)` lands here.
pub struct ApplicationDocumentReader {
    database: Database<ProdRuntime>,
}

impl ApplicationDocumentReader {
    pub fn new(database: Database<ProdRuntime>) -> Self {
        Self { database }
    }
}

#[async_trait]
impl convex_native_distributed::backend_callbacks_server::BackendDocumentReader
    for ApplicationDocumentReader
{
    async fn read_document(
        &self,
        identity: Identity,
        namespace: value::TableNamespace,
        table: value::TableName,
        id: value::DeveloperDocumentId,
    ) -> anyhow::Result<Option<value::ConvexObject>> {
        let mut tx = self.database.begin(identity).await?;
        // Resolve the table id within the namespace, then read by
        // (tablet, id). When the table doesn't exist or the id
        // isn't present return `None` — matches the Convex JS
        // semantics for `ctx.db.get(missingId)`.
        let table_mapping = tx.table_mapping().clone();
        let Some(tablet) = table_mapping
            .namespace(namespace)
            .id_and_number_if_exists(&table)
        else {
            return Ok(None);
        };
        let tablet_id = tablet.tablet_id;
        let resolved_id = id.to_resolved(|_| Ok(tablet_id))?;
        match tx.get(resolved_id).await? {
            Some(doc) => Ok(Some(doc.into_value().0)),
            None => Ok(None),
        }
    }
}
