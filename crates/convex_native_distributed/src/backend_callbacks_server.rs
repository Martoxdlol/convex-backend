//! Substep 4.2 of `convex-native/STATUS.md` — backend-side
//! `BackendCallbackServer`.
//!
//! Wraps an `Arc<dyn udf::ActionCallbacks>` and exposes it over
//! the `BackendCallbackService` gRPC surface. The worker-side
//! `BackendCallbackClient` (substep 4.3) dials this server from
//! inside a running action; each RPC resolves to the matching
//! method on the `ActionCallbacks` trait so every write still
//! flows through the backend's Committer.
//!
//! ## Identity + context decoding
//!
//! The RPC envelope carries `identity` as raw bytes encoded as the
//! `pb::convex_identity::UncheckedIdentity` proto. Empty bytes
//! map to `Identity::system()`; non-empty bytes decode through
//! `Identity::from_proto_unchecked`. A `component_path` string
//! parses through `ComponentPath::from_str`; for storage /
//! scheduling callbacks that need a `ComponentId`, the
//! `ComponentResolver` (default `RootOnlyComponentResolver`) maps
//! the path back to an id.
//!
//! ## What delegates today
//!
//! - `RunQuery` / `RunMutation` / `RunAction` →
//!   `ActionCallbacks::execute_{query,mutation,action}`.
//! - `Schedule` / `CancelJob` → `ActionCallbacks::{schedule_job,cancel_job}`.
//! - `StorageGetUrl` / `StorageDelete` →
//!   `ActionCallbacks::{storage_get_url,storage_delete}`.
//! - `StorageStore` / `StorageGet` (streaming) → the `BackendFileBytes` trait
//!   the backend supplies via `BackendCallbackServer::with_file_bytes(...)`.
//! - `VectorSearch` → `ActionCallbacks::vector_search`, results serialised as
//!   JSON.
//! - `LookupFunctionHandle` / `CreateFunctionHandle` →
//!   `ActionCallbacks::{lookup_function_handle,create_function_handle}`,
//!   handles encoded with the `function://` prefix.

use std::sync::Arc;

use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentPath,
    },
    execution_context::ExecutionContext,
};
use keybroker::Identity;
use pb::backend_callbacks::{
    self as proto,
    backend_callback_service_server::BackendCallbackService,
};
use sync_types::{
    types::SerializedArgs,
    CanonicalizedUdfPath,
};
use tonic::{
    Request,
    Response,
    Status,
};
use udf::ActionCallbacks;

/// Resolves a `ComponentPath` (the wire form workers send) to a
/// `ComponentId` (the form `ActionCallbacks` methods consume).
/// The mapping requires backend state — the worker doesn't know
/// component IDs — so the backend's `Application` provides the
/// resolver when it constructs the server.
#[tonic::async_trait]
pub trait ComponentResolver: Send + Sync + 'static {
    async fn resolve(
        &self,
        path: &ComponentPath,
    ) -> anyhow::Result<common::components::ComponentId>;
}

/// Default resolver: every path resolves to `ComponentId::Root`.
/// Correct for deployments that only use the root component
/// (the common case for native handlers today). Tests can plug
/// in a richer resolver via `BackendCallbackServer::with_component_resolver`.
pub struct RootOnlyComponentResolver;

#[tonic::async_trait]
impl ComponentResolver for RootOnlyComponentResolver {
    async fn resolve(
        &self,
        path: &ComponentPath,
    ) -> anyhow::Result<common::components::ComponentId> {
        if path.is_root() {
            Ok(common::components::ComponentId::Root)
        } else {
            anyhow::bail!(
                "RootOnlyComponentResolver: cannot resolve non-root component path {path:?}; wire \
                 a `ComponentResolver` impl that consults the backend's component registry to \
                 enable component-scoped callbacks"
            )
        }
    }
}

/// Server-side impl of the `BackendCallbackService` RPC trait.
/// Construct with an `Arc<dyn ActionCallbacks>` from the
/// backend's `Application` and spawn via tonic.
#[derive(Clone)]
pub struct BackendCallbackServer {
    callbacks: Arc<dyn ActionCallbacks>,
    /// Resolves wire `ComponentPath` strings into the
    /// `ComponentId` form `ActionCallbacks` consumes for storage
    /// + scheduling. Defaults to `RootOnlyComponentResolver` so
    /// root-component deployments work out of the box.
    component_resolver: Arc<dyn ComponentResolver>,
    /// Optional file-bytes ops handle wiring `storage_store` /
    /// `storage_get` through the backend's `FileStorage`. Tests
    /// and root-only deployments can leave it unset; production
    /// backends supply the impl from `local_backend`.
    file_bytes: Option<Arc<dyn BackendFileBytes>>,
    /// Optional document reader for the `ReadDocument` RPC.
    /// When unset, `ctx.db().get(...)` from a worker-side
    /// action returns `Status::unimplemented`.
    document_reader: Option<Arc<dyn BackendDocumentReader>>,
}

impl BackendCallbackServer {
    pub fn new(callbacks: Arc<dyn ActionCallbacks>) -> Self {
        Self {
            callbacks,
            component_resolver: Arc::new(RootOnlyComponentResolver),
            file_bytes: None,
            document_reader: None,
        }
    }

    pub fn with_component_resolver(mut self, resolver: Arc<dyn ComponentResolver>) -> Self {
        self.component_resolver = resolver;
        self
    }

    pub fn with_file_bytes(mut self, file_bytes: Arc<dyn BackendFileBytes>) -> Self {
        self.file_bytes = Some(file_bytes);
        self
    }

    pub fn with_document_reader(mut self, document_reader: Arc<dyn BackendDocumentReader>) -> Self {
        self.document_reader = Some(document_reader);
        self
    }

    async fn resolve_component_id(
        &self,
        path: &ComponentPath,
    ) -> Result<common::components::ComponentId, Status> {
        self.component_resolver.resolve(path).await.map_err(|e| {
            Status::failed_precondition(format!(
                "BackendCallbackServer: component resolution failed: {e}"
            ))
        })
    }
}

/// Streaming file-bytes operations the backend supplies for
/// `BackendCallbackService::StorageStore` / `StorageGet`. The
/// `ActionCallbacks` trait already covers metadata-level storage
/// ops; raw byte upload + download require a richer handle.
/// `local_backend` wires this from `Application::file_storage`;
/// tests can ship an in-memory impl.
#[tonic::async_trait]
pub trait BackendFileBytes: Send + Sync + 'static {
    async fn store_bytes(
        &self,
        identity: Identity,
        component: common::components::ComponentId,
        content_type: Option<String>,
        expected_sha256: Option<value::sha256::Sha256Digest>,
        body: bytes::Bytes,
    ) -> anyhow::Result<value::DeveloperDocumentId>;

    async fn get_bytes(
        &self,
        identity: Identity,
        component: common::components::ComponentId,
        storage_id: model::file_storage::FileStorageId,
    ) -> anyhow::Result<BackendFileBytesResponse>;
}

/// Bytes + metadata returned by `BackendFileBytes::get_bytes`.
pub struct BackendFileBytesResponse {
    pub content_type: Option<String>,
    pub content_length: u64,
    pub sha256: value::sha256::Sha256Digest,
    pub body: bytes::Bytes,
}

/// Backend-supplied document reader. Powers the
/// `ReadDocument` RPC by opening a short-lived read-only
/// transaction at the action's snapshot ts. Unwired by
/// default (`None` ⇒ the RPC bails with `Unimplemented`);
/// `local_backend` plugs in a `Database<ProdRuntime>`-backed
/// impl so distributed-action `ctx.db().get(...)` works.
#[async_trait::async_trait]
pub trait BackendDocumentReader: Send + Sync + 'static {
    async fn read_document(
        &self,
        identity: Identity,
        namespace: value::TableNamespace,
        table: value::TableName,
        id: value::DeveloperDocumentId,
    ) -> anyhow::Result<Option<value::ConvexObject>>;
}

/// Spawn a tonic `BackendCallbackService` on `bind_addr`.
/// Backend processes call this when
/// `CONVEX_BACKEND_CALLBACK_BIND_ADDR` is set. Workers reach
/// this server from inside an action via the
/// `CONVEX_BACKEND_CALLBACK_ENDPOINT` env var.
///
/// `callbacks` carries the backend's `ActionCallbacks` (typically
/// `application.runner()`). The optional `component_resolver`
/// maps non-root component paths into `ComponentId`; the
/// optional `file_bytes` enables the streaming `StorageStore` /
/// `StorageGet` paths. Both are optional so root-only / no-file
/// deployments can wire less.
///
/// The server runs on a tokio `spawn` and stays alive until the
/// runtime drops. Transport errors land in tracing.
pub async fn spawn_backend_callback_server(
    bind_addr: std::net::SocketAddr,
    callbacks: Arc<dyn ActionCallbacks>,
    component_resolver: Option<Arc<dyn ComponentResolver>>,
    file_bytes: Option<Arc<dyn BackendFileBytes>>,
    document_reader: Option<Arc<dyn BackendDocumentReader>>,
) -> anyhow::Result<()> {
    use pb::backend_callbacks::backend_callback_service_server::BackendCallbackServiceServer;
    use tonic::transport::Server;
    let mut server = BackendCallbackServer::new(callbacks);
    if let Some(resolver) = component_resolver {
        server = server.with_component_resolver(resolver);
    }
    if let Some(file_bytes) = file_bytes {
        server = server.with_file_bytes(file_bytes);
    }
    if let Some(reader) = document_reader {
        server = server.with_document_reader(reader);
    }
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(BackendCallbackServiceServer::new(server))
            .serve(bind_addr)
            .await
        {
            tracing::error!("BackendCallbackService exited: {e}");
        }
    });
    Ok(())
}

/// Decode a `CallbackContext` into the `(identity, component_path,
/// execution_context)` triple every `ActionCallbacks` method
/// needs. Worker-side clients set empty identity bytes today —
/// map that to `Identity::system()`. Phase-4 follow-up plumbing
/// grows this to decode a real `convex_identity::Identity`.
fn decode_context(
    ctx: Option<proto::CallbackContext>,
) -> Result<(Identity, ComponentPath, ExecutionContext), Status> {
    let ctx = ctx.ok_or_else(|| Status::invalid_argument("CallbackContext is required"))?;
    let identity = if ctx.identity.is_empty() {
        Identity::system()
    } else {
        let unchecked: pb::convex_identity::UncheckedIdentity =
            <pb::convex_identity::UncheckedIdentity as prost::Message>::decode(
                ctx.identity.as_slice(),
            )
            .map_err(|e| {
                Status::invalid_argument(format!(
                    "BackendCallbackServer: identity bytes are not a valid UncheckedIdentity \
                     proto: {e}"
                ))
            })?;
        Identity::from_proto_unchecked(unchecked).map_err(|e| {
            Status::invalid_argument(format!(
                "BackendCallbackServer: identity decoding failed: {e}"
            ))
        })?
    };
    let component_path: ComponentPath = if ctx.component_path.is_empty() {
        ComponentPath::root()
    } else {
        ctx.component_path.parse().map_err(|e| {
            Status::invalid_argument(format!(
                "BackendCallbackServer: component_path {:?}: {e}",
                ctx.component_path,
            ))
        })?
    };
    let execution_context = ctx
        .execution_context
        .map(ExecutionContext::try_from)
        .transpose()
        .map_err(|e| Status::invalid_argument(format!("bad execution_context: {e}")))?
        .unwrap_or_else(|| {
            use common::execution_context::{
                ExecutionId,
                RequestId,
            };
            ExecutionContext::new_from_parts(RequestId::new(), ExecutionId::new(), None, true)
        });
    Ok((identity, component_path, execution_context))
}

/// Build a `CanonicalizedComponentFunctionPath` from a dotted
/// name + component. Matches the shape
/// `ActionCallbacks::execute_*` wants.
fn build_path(
    component: ComponentPath,
    dotted_name: &str,
) -> Result<CanonicalizedComponentFunctionPath, Status> {
    let udf_path: CanonicalizedUdfPath = dotted_name
        .parse()
        .map_err(|e| Status::invalid_argument(format!("bad function_name {dotted_name:?}: {e}")))?;
    Ok(CanonicalizedComponentFunctionPath {
        component,
        udf_path,
    })
}

fn build_args(args_json: &[u8]) -> Result<SerializedArgs, Status> {
    // `args_json` is the native-side single-object encoding.
    // `ActionCallbacks` wants a `SerializedArgs` whose inner
    // JSON is an array; wrap the object in a 1-element array.
    let single: serde_json::Value = serde_json::from_slice(args_json)
        .map_err(|e| Status::invalid_argument(format!("args_json parse: {e}")))?;
    SerializedArgs::from_args(vec![single])
        .map_err(|e| Status::internal(format!("SerializedArgs::from_args: {e}")))
}

fn function_result_to_proto(
    result: udf::FunctionResult,
) -> Result<pb::common::FunctionResult, Status> {
    pb::common::FunctionResult::try_from(result)
        .map_err(|e| Status::internal(format!("FunctionResult → proto: {e}")))
}

#[tonic::async_trait]
impl BackendCallbackService for BackendCallbackServer {
    type StorageGetStream =
        tokio_stream::wrappers::ReceiverStream<Result<proto::StorageGetChunk, Status>>;

    async fn run_query(
        &self,
        request: Request<proto::RunQueryRequest>,
    ) -> Result<Response<proto::RunQueryResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, execution_context) = decode_context(req.ctx)?;
        let path = build_path(component, &req.function_name)?;
        let args = build_args(&req.args_json)?;
        let result = self
            .callbacks
            .execute_query(identity, path, args, execution_context)
            .await
            .map_err(|e| Status::internal(format!("execute_query: {e}")))?;
        Ok(Response::new(proto::RunQueryResponse {
            result: Some(function_result_to_proto(result)?),
        }))
    }

    async fn run_mutation(
        &self,
        request: Request<proto::RunMutationRequest>,
    ) -> Result<Response<proto::RunMutationResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, execution_context) = decode_context(req.ctx)?;
        let path = build_path(component, &req.function_name)?;
        let args = build_args(&req.args_json)?;
        let result = self
            .callbacks
            .execute_mutation(identity, path, args, execution_context)
            .await
            .map_err(|e| Status::internal(format!("execute_mutation: {e}")))?;
        Ok(Response::new(proto::RunMutationResponse {
            result: Some(function_result_to_proto(result)?),
        }))
    }

    async fn run_action(
        &self,
        request: Request<proto::RunActionRequest>,
    ) -> Result<Response<proto::RunActionResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, execution_context) = decode_context(req.ctx)?;
        let path = build_path(component, &req.function_name)?;
        let args = build_args(&req.args_json)?;
        let result = self
            .callbacks
            .execute_action(identity, path, args, execution_context)
            .await
            .map_err(|e| Status::internal(format!("execute_action: {e}")))?;
        Ok(Response::new(proto::RunActionResponse {
            result: Some(function_result_to_proto(result)?),
        }))
    }

    async fn schedule(
        &self,
        request: Request<proto::ScheduleRequest>,
    ) -> Result<Response<proto::ScheduleResponse>, Status> {
        use common::runtime::UnixTimestamp;
        let req = request.into_inner();
        let (identity, component, execution_context) = decode_context(req.ctx)?;
        let scheduled_path = build_path(component.clone(), &req.function_name)?;
        let args = build_args(&req.args_json)?;
        let scheduled_ts = UnixTimestamp::from_nanos(req.fire_at_unix_nanos);
        // `scheduling_component` is the component scheduling the
        // job; matches the enclosing action's component. We
        // resolve it from the decoded component_path using the
        // optional `ComponentResolver` the backend wires in.
        // Without a resolver we use Root (matches the historic
        // pre-Phase-4 behaviour and is correct for the common
        // root-component deployment).
        let scheduling_component = self.resolve_component_id(&component).await?;
        let _ = component;
        let id = self
            .callbacks
            .schedule_job(
                identity,
                scheduling_component,
                scheduled_path,
                args,
                scheduled_ts,
                execution_context,
            )
            .await
            .map_err(|e| Status::internal(format!("schedule_job: {e}")))?;
        Ok(Response::new(proto::ScheduleResponse {
            scheduled_job_id: id.encode(),
        }))
    }

    async fn cancel_job(
        &self,
        request: Request<proto::CancelJobRequest>,
    ) -> Result<Response<proto::CancelJobResponse>, Status> {
        let req = request.into_inner();
        let (identity, _component, _execution_context) = decode_context(req.ctx)?;
        let virtual_id: value::DeveloperDocumentId = req.scheduled_job_id.parse().map_err(|e| {
            Status::invalid_argument(format!(
                "cancel_job scheduled_job_id {:?}: {e}",
                req.scheduled_job_id
            ))
        })?;
        self.callbacks
            .cancel_job(identity, virtual_id)
            .await
            .map_err(|e| Status::internal(format!("cancel_job: {e}")))?;
        Ok(Response::new(proto::CancelJobResponse {}))
    }

    async fn storage_store(
        &self,
        request: Request<tonic::Streaming<proto::StorageStoreChunk>>,
    ) -> Result<Response<proto::StorageStoreResponse>, Status> {
        use bytes::BytesMut;
        use tokio_stream::StreamExt;

        let file_bytes = self.file_bytes.clone().ok_or_else(|| {
            Status::failed_precondition(
                "BackendCallbackServer::storage_store: no BackendFileBytes handle wired; the \
                 backend must construct the server with `.with_file_bytes(...)` to enable raw \
                 byte upload",
            )
        })?;
        let mut stream = request.into_inner();
        let mut meta: Option<proto::StorageStoreMeta> = None;
        let mut body = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            match chunk.content {
                Some(proto::storage_store_chunk::Content::Meta(m)) => {
                    if meta.is_some() {
                        return Err(Status::invalid_argument(
                            "storage_store: meta frame must appear exactly once and first",
                        ));
                    }
                    meta = Some(m);
                },
                Some(proto::storage_store_chunk::Content::Body(b)) => {
                    if meta.is_none() {
                        return Err(Status::invalid_argument(
                            "storage_store: body frame received before meta frame",
                        ));
                    }
                    body.extend_from_slice(&b);
                },
                None => {
                    return Err(Status::invalid_argument(
                        "storage_store: chunk with empty content variant",
                    ));
                },
            }
        }
        let meta = meta.ok_or_else(|| {
            Status::invalid_argument("storage_store: stream closed without a meta frame")
        })?;
        let (identity, component, _execution_context) = decode_context(meta.ctx)?;
        let component_id = self.resolve_component_id(&component).await?;
        let content_type = if meta.content_type.is_empty() {
            None
        } else {
            Some(meta.content_type)
        };
        let expected_sha256 = if meta.expected_sha256.is_empty() {
            None
        } else {
            Some({
                let arr: [u8; 32] = meta.expected_sha256.as_slice().try_into().map_err(|e| {
                    Status::invalid_argument(format!(
                        "storage_store expected_sha256 (must be 32 bytes): {e}"
                    ))
                })?;
                value::sha256::Sha256Digest::from(arr)
            })
        };
        let storage_id = file_bytes
            .store_bytes(
                identity,
                component_id,
                content_type,
                expected_sha256,
                body.freeze(),
            )
            .await
            .map_err(|e| Status::internal(format!("storage_store: {e}")))?;
        Ok(Response::new(proto::StorageStoreResponse {
            storage_id: storage_id.encode(),
        }))
    }

    async fn storage_get(
        &self,
        request: Request<proto::StorageGetRequest>,
    ) -> Result<Response<Self::StorageGetStream>, Status> {
        let req = request.into_inner();
        let (identity, component, _execution_context) = decode_context(req.ctx)?;
        let component_id = self.resolve_component_id(&component).await?;
        let storage_id = parse_storage_id(&req.storage_id)?;
        let file_bytes = self.file_bytes.clone().ok_or_else(|| {
            Status::failed_precondition(
                "BackendCallbackServer::storage_get: no BackendFileBytes handle wired; the \
                 backend must construct the server with `.with_file_bytes(...)` to enable raw \
                 byte download",
            )
        })?;
        let response = file_bytes
            .get_bytes(identity, component_id, storage_id)
            .await
            .map_err(|e| Status::internal(format!("storage_get: {e}")))?;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<proto::StorageGetChunk, Status>>(8);
        let meta = proto::StorageGetMeta {
            content_type: response.content_type.unwrap_or_default(),
            content_length: response.content_length,
            sha256: response.sha256.as_ref().to_vec(),
        };
        // Send synchronously into a buffered channel; for typical
        // file sizes this fits without blocking. Spawn a task to
        // chunk + send body so the response stream returns
        // immediately.
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(proto::StorageGetChunk {
                    content: Some(proto::storage_get_chunk::Content::Meta(meta)),
                }))
                .await;
            const CHUNK: usize = 64 * 1024;
            for slice in response.body.chunks(CHUNK) {
                if tx
                    .send(Ok(proto::StorageGetChunk {
                        content: Some(proto::storage_get_chunk::Content::Body(slice.to_vec())),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn storage_get_url(
        &self,
        request: Request<proto::StorageGetUrlRequest>,
    ) -> Result<Response<proto::StorageGetUrlResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, _execution_context) = decode_context(req.ctx)?;
        let component_id = self.resolve_component_id(&component).await?;
        let storage_id = parse_storage_id(&req.storage_id)?;
        let url = self
            .callbacks
            .storage_get_url(identity, component_id, storage_id)
            .await
            .map_err(|e| Status::internal(format!("storage_get_url: {e}")))?;
        Ok(Response::new(proto::StorageGetUrlResponse { url }))
    }

    async fn storage_delete(
        &self,
        request: Request<proto::StorageDeleteRequest>,
    ) -> Result<Response<proto::StorageDeleteResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, _execution_context) = decode_context(req.ctx)?;
        let component_id = self.resolve_component_id(&component).await?;
        let storage_id = parse_storage_id(&req.storage_id)?;
        self.callbacks
            .storage_delete(identity, component_id, storage_id)
            .await
            .map_err(|e| Status::internal(format!("storage_delete: {e}")))?;
        Ok(Response::new(proto::StorageDeleteResponse {}))
    }

    async fn vector_search(
        &self,
        request: Request<proto::VectorSearchRequest>,
    ) -> Result<Response<proto::VectorSearchResponse>, Status> {
        let req = request.into_inner();
        let (identity, _component, _execution_context) = decode_context(req.ctx)?;
        let query: serde_json::Value = serde_json::from_slice(&req.query_json)
            .map_err(|e| Status::invalid_argument(format!("vector_search query_json: {e}")))?;
        let (results, _usage) = self
            .callbacks
            .vector_search(identity, query)
            .await
            .map_err(|e| Status::internal(format!("vector_search: {e}")))?;
        let json_results: Vec<serde_json::Value> =
            results.into_iter().map(serde_json::Value::from).collect();
        let results_json = serde_json::to_vec(&json_results)
            .map_err(|e| Status::internal(format!("encode vector_search results: {e}")))?;
        Ok(Response::new(proto::VectorSearchResponse { results_json }))
    }

    async fn lookup_function_handle(
        &self,
        request: Request<proto::LookupFunctionHandleRequest>,
    ) -> Result<Response<proto::LookupFunctionHandleResponse>, Status> {
        use common::bootstrap_model::components::handles::FunctionHandle;
        let req = request.into_inner();
        let (identity, _component, _execution_context) = decode_context(req.ctx)?;
        let handle: FunctionHandle = req.function_handle_id.parse().map_err(|e| {
            Status::invalid_argument(format!(
                "lookup_function_handle handle_id {:?}: {e}",
                req.function_handle_id,
            ))
        })?;
        let path = self
            .callbacks
            .lookup_function_handle(identity, handle)
            .await
            .map_err(|e| Status::internal(format!("lookup_function_handle: {e}")))?;
        Ok(Response::new(proto::LookupFunctionHandleResponse {
            function_path: path.udf_path.to_string(),
        }))
    }

    async fn create_function_handle(
        &self,
        request: Request<proto::CreateFunctionHandleRequest>,
    ) -> Result<Response<proto::CreateFunctionHandleResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, _execution_context) = decode_context(req.ctx)?;
        let path = build_path(component, &req.function_path)?;
        let handle = self
            .callbacks
            .create_function_handle(identity, path)
            .await
            .map_err(|e| Status::internal(format!("create_function_handle: {e}")))?;
        Ok(Response::new(proto::CreateFunctionHandleResponse {
            function_handle_id: String::from(handle),
        }))
    }

    async fn read_document(
        &self,
        request: Request<proto::ReadDocumentRequest>,
    ) -> Result<Response<proto::ReadDocumentResponse>, Status> {
        let req = request.into_inner();
        let (identity, component, _execution_context) = decode_context(req.ctx)?;
        let component_id = self.resolve_component_id(&component).await?;
        let reader = self.document_reader.as_ref().ok_or_else(|| {
            Status::unimplemented(
                "BackendCallbackServer::read_document: no BackendDocumentReader handle wired; the \
                 backend must construct the server with `.with_document_reader(...)` to enable \
                 distributed-action `ctx.db().get(...)`",
            )
        })?;
        let table_name: value::TableName = req.table.parse().map_err(|e| {
            Status::invalid_argument(format!("ReadDocument table {:?}: {e}", req.table))
        })?;
        let doc_id: value::DeveloperDocumentId = req
            .id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("ReadDocument id {:?}: {e}", req.id)))?;
        let namespace: value::TableNamespace = component_id.into();
        let maybe_obj = reader
            .read_document(identity, namespace, table_name, doc_id)
            .await
            .map_err(|e| Status::internal(format!("read_document: {e}")))?;
        let document_json = match maybe_obj {
            None => Vec::new(),
            Some(obj) => {
                let v = value::ConvexValue::Object(obj);
                let json: serde_json::Value = v.into();
                serde_json::to_vec(&json)
                    .map_err(|e| Status::internal(format!("read_document encode: {e}")))?
            },
        };
        Ok(Response::new(proto::ReadDocumentResponse { document_json }))
    }
}

fn parse_storage_id(raw: &str) -> Result<model::file_storage::FileStorageId, Status> {
    raw.parse()
        .map_err(|e| Status::invalid_argument(format!("storage_id {raw:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use common::{
        bootstrap_model::components::handles::FunctionHandle,
        components::{
            CanonicalizedComponentFunctionPath,
            ComponentId,
            ComponentPath,
        },
        execution_context::ExecutionContext,
        runtime::UnixTimestamp,
    };
    use keybroker::Identity;
    use model::file_storage::{
        types::FileStorageEntry,
        FileStorageId,
    };
    use serde_json::Value as JsonValue;
    use sync_types::types::SerializedArgs;
    use udf::FunctionResult;
    use usage_tracking::FunctionUsageStats;
    use value::{
        DeveloperDocumentId,
        InternalId,
        JsonPackedValue,
        TableNumber,
    };

    use super::*;

    /// In-process `ActionCallbacks` stub. Records the latest
    /// `execute_mutation` call + emits a canned result.
    #[derive(Default)]
    struct RecordingCallbacks {
        last_mutation_path: std::sync::Mutex<Option<String>>,
    }

    #[async_trait]
    impl ActionCallbacks for RecordingCallbacks {
        async fn execute_query(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
            _args: SerializedArgs,
            _context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            Ok(FunctionResult {
                result: Ok(JsonPackedValue::from_network("\"from_query\"".to_string())?),
            })
        }

        async fn execute_mutation(
            &self,
            _identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            _args: SerializedArgs,
            _context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            *self.last_mutation_path.lock().unwrap() = Some(format!("{path:?}"));
            Ok(FunctionResult {
                result: Ok(JsonPackedValue::from_network("\"mutated\"".to_string())?),
            })
        }

        async fn execute_action(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
            _args: SerializedArgs,
            _context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            Ok(FunctionResult {
                result: Ok(JsonPackedValue::from_network(
                    "\"from_action\"".to_string(),
                )?),
            })
        }

        async fn storage_get_url(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _storage_id: FileStorageId,
        ) -> anyhow::Result<Option<String>> {
            Ok(Some("https://stub.test/file".to_string()))
        }

        async fn storage_delete(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _storage_id: FileStorageId,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn storage_get_file_entry(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _storage_id: FileStorageId,
        ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
            Ok(None)
        }

        async fn storage_store_file_entry(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _entry: FileStorageEntry,
        ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
            anyhow::bail!("not used in this test")
        }

        async fn schedule_job(
            &self,
            _identity: Identity,
            _scheduling_component: ComponentId,
            _scheduled_path: CanonicalizedComponentFunctionPath,
            _udf_args: SerializedArgs,
            _scheduled_ts: UnixTimestamp,
            _context: ExecutionContext,
        ) -> anyhow::Result<DeveloperDocumentId> {
            Ok(DeveloperDocumentId::new(
                TableNumber::try_from(1u32).unwrap(),
                InternalId::MIN,
            ))
        }

        async fn cancel_job(
            &self,
            _identity: Identity,
            _virtual_id: DeveloperDocumentId,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn vector_search(
            &self,
            _identity: Identity,
            _query: JsonValue,
        ) -> anyhow::Result<(
            Vec<vector::PublicVectorSearchQueryResult>,
            FunctionUsageStats,
        )> {
            anyhow::bail!("not used in this test")
        }

        async fn lookup_function_handle(
            &self,
            _identity: Identity,
            _handle: FunctionHandle,
        ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
            anyhow::bail!("not used in this test")
        }

        async fn create_function_handle(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
        ) -> anyhow::Result<FunctionHandle> {
            anyhow::bail!("not used in this test")
        }
    }

    fn empty_ctx() -> proto::CallbackContext {
        proto::CallbackContext {
            identity: vec![],
            execution_context: None,
            component_path: String::new(),
        }
    }

    #[tokio::test]
    async fn run_mutation_delegates_to_action_callbacks() {
        let callbacks = Arc::new(RecordingCallbacks::default());
        let server = BackendCallbackServer::new(callbacks.clone());
        let resp = server
            .run_mutation(Request::new(proto::RunMutationRequest {
                ctx: Some(empty_ctx()),
                function_name: "users:set".to_string(),
                args_json: b"{}".to_vec(),
            }))
            .await
            .expect("run_mutation");
        let inner = resp.into_inner().result.expect("result populated");
        // Should be the canned "mutated" string result.
        match inner.result.expect("function result variant") {
            pb::common::function_result::Result::JsonPackedValue(v) => {
                assert_eq!(v, "\"mutated\"")
            },
            pb::common::function_result::Result::JsError(e) => {
                panic!("expected success, got error {:?}", e.message)
            },
        }
        let captured = callbacks.last_mutation_path.lock().unwrap().clone();
        // `CanonicalizedUdfPath` parsing canonicalises the
        // module (e.g. "users" → "users.js"), so the captured
        // path contains "users" + ":set" but not the literal
        // original.
        let captured_str = captured.as_deref().unwrap_or("");
        assert!(
            captured_str.contains("users") && captured_str.contains(":set"),
            "server forwarded the dotted path to the callbacks: {captured_str:?}",
        );
    }

    #[tokio::test]
    async fn schedule_delegates_to_action_callbacks() {
        let callbacks = Arc::new(RecordingCallbacks::default());
        let server = BackendCallbackServer::new(callbacks);
        let resp = server
            .schedule(Request::new(proto::ScheduleRequest {
                ctx: Some(empty_ctx()),
                function_name: "cron:tick".to_string(),
                args_json: b"{}".to_vec(),
                fire_at_unix_nanos: 0,
            }))
            .await
            .expect("schedule");
        let id = resp.into_inner().scheduled_job_id;
        assert!(!id.is_empty(), "server returned a non-empty scheduled id");
    }

    #[tokio::test]
    async fn storage_get_url_delegates_to_action_callbacks() {
        let callbacks = Arc::new(RecordingCallbacks::default());
        let server = BackendCallbackServer::new(callbacks);
        // Use a valid DeveloperDocumentId string form — the
        // server parses storage_id through FromStr for
        // FileStorageId.
        let canonical_id =
            DeveloperDocumentId::new(TableNumber::try_from(1u32).unwrap(), InternalId::MIN)
                .encode();
        let resp = server
            .storage_get_url(Request::new(proto::StorageGetUrlRequest {
                ctx: Some(empty_ctx()),
                storage_id: canonical_id,
            }))
            .await
            .expect("storage_get_url");
        assert_eq!(
            resp.into_inner().url.as_deref(),
            Some("https://stub.test/file"),
        );
    }

    #[tokio::test]
    async fn garbage_identity_bytes_surface_as_invalid_argument() {
        // Identity decoding is now wired (UncheckedIdentity proto →
        // keybroker::Identity). Garbage bytes that don't deserialize
        // as a valid `UncheckedIdentity` proto must fail loudly with
        // `InvalidArgument` rather than being silently routed under
        // `Identity::system()`.
        let callbacks = Arc::new(RecordingCallbacks::default());
        let server = BackendCallbackServer::new(callbacks);
        let err = server
            .run_mutation(Request::new(proto::RunMutationRequest {
                ctx: Some(proto::CallbackContext {
                    identity: b"some-principal-bytes".to_vec(),
                    execution_context: None,
                    component_path: String::new(),
                }),
                function_name: "users:set".to_string(),
                args_json: b"{}".to_vec(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("UncheckedIdentity"));
    }

    #[tokio::test]
    async fn valid_identity_bytes_decode_to_system() {
        // A round-tripped Identity::System encoded as
        // UncheckedIdentity proto bytes should decode cleanly back
        // to Identity::System on the server side.
        use prost::Message;
        let callbacks = Arc::new(RecordingCallbacks::default());
        let server = BackendCallbackServer::new(callbacks);
        let identity_proto: pb::convex_identity::UncheckedIdentity = Identity::system().into();
        let identity_bytes = identity_proto.encode_to_vec();
        let resp = server
            .run_mutation(Request::new(proto::RunMutationRequest {
                ctx: Some(proto::CallbackContext {
                    identity: identity_bytes,
                    execution_context: None,
                    component_path: String::new(),
                }),
                function_name: "users:set".to_string(),
                args_json: b"{}".to_vec(),
            }))
            .await
            .expect("identity decoding round-trip");
        assert!(resp.into_inner().result.is_some());
    }

    #[tokio::test]
    async fn nonempty_component_path_decodes_into_component_path() {
        // The decoder used to bail with Unimplemented when
        // component_path was non-empty. Now a parseable path
        // (single component name) decodes cleanly and the request
        // proceeds to the underlying ActionCallbacks.
        let callbacks = Arc::new(RecordingCallbacks::default());
        let server = BackendCallbackServer::new(callbacks.clone());
        let resp = server
            .run_mutation(Request::new(proto::RunMutationRequest {
                ctx: Some(proto::CallbackContext {
                    identity: vec![],
                    execution_context: None,
                    component_path: "subapp".to_string(),
                }),
                function_name: "users:set".to_string(),
                args_json: b"{}".to_vec(),
            }))
            .await
            .expect("component path decoding");
        assert!(resp.into_inner().result.is_some());
        let captured = callbacks.last_mutation_path.lock().unwrap().clone();
        let captured_str = captured.as_deref().unwrap_or("");
        assert!(
            captured_str.contains("subapp"),
            "component path threaded into the canonical function path: {captured_str:?}",
        );
    }
}
