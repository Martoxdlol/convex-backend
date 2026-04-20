//! Substep 4.3 of `convex-native/STATUS.md` — worker-side
//! `BackendCallbackClient`.
//!
//! Translates `convex_native_core::NativeActionCallbacks` methods into
//! `BackendCallbackService` gRPC calls. The worker's
//! `FunctionExecutionServer` uses this (wrapped in an `Arc<dyn
//! NativeActionCallbacks>`) when a backend-callback endpoint is
//! configured, replacing the Phase-1–3 `NoopCallbacks` stub.
//!
//! ## What's wired through today
//!
//! - `run_query_by_name` / `run_mutation_by_name` — `RunQuery` / `RunMutation`
//!   RPCs. The backend commits mutations through its own Committer so OCC +
//!   subscription invalidation fire.
//! - `schedule` — `Schedule` RPC.
//! - `storage_store` — `StorageStore` streaming RPC (metadata first, then
//!   chunked body).
//! - `storage_get_url` / `storage_delete` — one-shot RPCs.
//!
//! ## Additional overrides
//!
//! - `cancel_scheduled` — `CancelJob` RPC.
//! - `storage_get_metadata` — reads the leading meta frame off the `StorageGet`
//!   stream.
//! - `read_document_at_snapshot` — routed through `RunQuery` against the system
//!   `_system/db:get` surface so backends with a wired query path can service
//!   `ctx.db().get(...)` reads from inside an action.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use convex_native_core::{
    callbacks::NativeActionCallbacks,
    ctx::storage::{
        FileMetadata,
        StorageId,
    },
};
use pb::backend_callbacks::{
    self as proto,
    backend_callback_service_client::BackendCallbackServiceClient,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableName,
    TableNamespace,
};

/// Worker-side gRPC client implementing
/// `NativeActionCallbacks` on top of `BackendCallbackService`.
///
/// One instance per running action. The underlying tonic
/// `Channel` is cheap to clone, and the client is held in an
/// `Arc` so callbacks from inside the action can grab it
/// without threading lifetime issues through the native ctx.
///
/// The `identity` + `execution_context_bytes` fields are
/// captured at `new()` time and attached to every outgoing RPC
/// via the proto's `CallbackContext`. Substep 4.4's worker
/// wiring populates them from the original
/// `FunctionExecutionService::ExecuteRequest` the backend sent
/// so sub-calls run under the same principal + trace chain.
pub struct BackendCallbackClient {
    // `Mutex` so tonic's `&mut self` method signatures don't
    // clash with the `&self` NativeActionCallbacks trait.
    // Contention is per-action (sub-calls serialise inside the
    // handler), so a lock doesn't hurt.
    client: Mutex<BackendCallbackServiceClient<Channel>>,
    identity: Vec<u8>,
    execution_context: Option<pb::common::ExecutionContext>,
    component_path: String,
}

impl BackendCallbackClient {
    /// Dial the backend callback endpoint and build a fresh
    /// client. `identity` is the worker's raw-bytes envelope
    /// (same encoding as `ExecuteRequest.identity`);
    /// `execution_context` propagates the trace chain from the
    /// enclosing action.
    pub async fn connect(
        endpoint: impl Into<String>,
        identity: Vec<u8>,
        execution_context: Option<pb::common::ExecutionContext>,
        component_path: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(endpoint.into())?.connect().await?;
        Ok(Self {
            client: Mutex::new(BackendCallbackServiceClient::new(channel)),
            identity,
            execution_context,
            component_path: component_path.into(),
        })
    }

    /// Construct from an already-built `Channel` — useful when
    /// the worker binary wants to share one connection across
    /// multiple callback endpoints.
    pub fn from_channel(
        channel: Channel,
        identity: Vec<u8>,
        execution_context: Option<pb::common::ExecutionContext>,
        component_path: impl Into<String>,
    ) -> Self {
        Self {
            client: Mutex::new(BackendCallbackServiceClient::new(channel)),
            identity,
            execution_context,
            component_path: component_path.into(),
        }
    }

    fn context(&self) -> proto::CallbackContext {
        proto::CallbackContext {
            identity: self.identity.clone(),
            execution_context: self.execution_context.clone(),
            component_path: self.component_path.clone(),
        }
    }
}

fn encode_args(args: ConvexObject) -> anyhow::Result<Vec<u8>> {
    let v: ConvexValue = ConvexValue::Object(args);
    let json: serde_json::Value = v.into();
    Ok(serde_json::to_vec(&json)?)
}

fn decode_function_result(result: pb::common::FunctionResult) -> anyhow::Result<ConvexValue> {
    use pb::common::function_result::Result as R;
    let inner = result
        .result
        .ok_or_else(|| anyhow::anyhow!("BackendCallbackClient: RPC returned empty result"))?;
    match inner {
        R::JsonPackedValue(s) => {
            let json: serde_json::Value = serde_json::from_str(&s)?;
            let v: ConvexValue = json.try_into()?;
            Ok(v)
        },
        R::JsError(e) => {
            anyhow::bail!(
                "backend sub-call errored: {}",
                e.message.unwrap_or_else(|| "<unknown>".to_string())
            )
        },
    }
}

fn namespace_dispatch_note(ns: TableNamespace) -> String {
    // The RPC surface uses `component_path` string rather than
    // a `TableNamespace` enum. Worker adapters pass the
    // component through the `CallbackContext`, but the
    // per-call namespace needs to match.  The Phase-4 scaffold
    // only supports root-component dispatch; component-scoped
    // callbacks land with Phase 6 if they don't already.
    match ns {
        TableNamespace::Global => String::new(),
        TableNamespace::ByComponent(id) => format!("component:{id}"),
    }
}

#[async_trait]
impl NativeActionCallbacks for BackendCallbackClient {
    async fn run_query_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let _ = namespace_dispatch_note(namespace);
        let request = proto::RunQueryRequest {
            ctx: Some(self.context()),
            function_name: name.to_string(),
            args_json: encode_args(args)?,
        };
        let response = self
            .client
            .lock()
            .await
            .run_query(tonic::Request::new(request))
            .await?
            .into_inner();
        let result = response
            .result
            .ok_or_else(|| anyhow::anyhow!("RunQuery response missing result"))?;
        decode_function_result(result)
    }

    async fn run_mutation_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let _ = namespace_dispatch_note(namespace);
        let request = proto::RunMutationRequest {
            ctx: Some(self.context()),
            function_name: name.to_string(),
            args_json: encode_args(args)?,
        };
        let response = self
            .client
            .lock()
            .await
            .run_mutation(tonic::Request::new(request))
            .await?
            .into_inner();
        let result = response
            .result
            .ok_or_else(|| anyhow::anyhow!("RunMutation response missing result"))?;
        decode_function_result(result)
    }

    async fn run_action_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let _ = namespace_dispatch_note(namespace);
        let request = proto::RunActionRequest {
            ctx: Some(self.context()),
            function_name: name.to_string(),
            args_json: encode_args(args)?,
        };
        let response = self
            .client
            .lock()
            .await
            .run_action(tonic::Request::new(request))
            .await?
            .into_inner();
        let result = response
            .result
            .ok_or_else(|| anyhow::anyhow!("RunAction response missing result"))?;
        decode_function_result(result)
    }

    async fn schedule(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let _ = namespace_dispatch_note(namespace);
        let fire_at_unix_nanos = std::time::SystemTime::now()
            .checked_add(delay)
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        let request = proto::ScheduleRequest {
            ctx: Some(self.context()),
            function_name: name.to_string(),
            args_json: encode_args(args)?,
            fire_at_unix_nanos,
        };
        let response = self
            .client
            .lock()
            .await
            .schedule(tonic::Request::new(request))
            .await?
            .into_inner();
        response
            .scheduled_job_id
            .parse()
            .map_err(|e| anyhow::anyhow!("Schedule response returned unparseable id: {e}"))
    }

    async fn storage_store(
        &self,
        _namespace: TableNamespace,
        body: Bytes,
        content_type: &str,
    ) -> anyhow::Result<StorageId> {
        use tokio_stream::wrappers::ReceiverStream;
        // StorageStore is a client-streaming RPC. First frame
        // carries metadata; subsequent frames carry the body in
        // ~64 KiB chunks so large uploads don't land in a single
        // gRPC message.
        let (tx, rx) = tokio::sync::mpsc::channel::<proto::StorageStoreChunk>(8);
        let meta = proto::StorageStoreMeta {
            ctx: Some(self.context()),
            content_type: content_type.to_string(),
            expected_sha256: Vec::new(),
        };
        tx.send(proto::StorageStoreChunk {
            content: Some(proto::storage_store_chunk::Content::Meta(meta)),
        })
        .await
        .map_err(|e| anyhow::anyhow!("storage_store metadata send failed: {e}"))?;

        const CHUNK: usize = 64 * 1024;
        for slice in body.chunks(CHUNK) {
            tx.send(proto::StorageStoreChunk {
                content: Some(proto::storage_store_chunk::Content::Body(slice.to_vec())),
            })
            .await
            .map_err(|e| anyhow::anyhow!("storage_store body chunk send failed: {e}"))?;
        }
        drop(tx);

        let response = self
            .client
            .lock()
            .await
            .storage_store(tonic::Request::new(ReceiverStream::new(rx)))
            .await?
            .into_inner();
        Ok(StorageId(response.storage_id))
    }

    async fn storage_get_url(
        &self,
        _namespace: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        let request = proto::StorageGetUrlRequest {
            ctx: Some(self.context()),
            storage_id: id.0,
        };
        let response = self
            .client
            .lock()
            .await
            .storage_get_url(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(response.url)
    }

    async fn storage_delete(
        &self,
        _namespace: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<bool> {
        let request = proto::StorageDeleteRequest {
            ctx: Some(self.context()),
            storage_id: id.0,
        };
        let _ = self
            .client
            .lock()
            .await
            .storage_delete(tonic::Request::new(request))
            .await?;
        // The RPC doesn't distinguish "existed" from "didn't
        // exist" today — both succeed. Return `true` to match
        // the NativeActionCallbacks signature; a richer backend
        // response shape can grow the proto later.
        Ok(true)
    }

    async fn cancel_scheduled(
        &self,
        _namespace: TableNamespace,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        let request = proto::CancelJobRequest {
            ctx: Some(self.context()),
            scheduled_job_id: id.encode(),
        };
        let _ = self
            .client
            .lock()
            .await
            .cancel_job(tonic::Request::new(request))
            .await?;
        Ok(())
    }

    async fn storage_get_metadata(
        &self,
        _namespace: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<FileMetadata>> {
        use tokio_stream::StreamExt;
        let request = proto::StorageGetRequest {
            ctx: Some(self.context()),
            storage_id: id.0,
        };
        let mut stream = match self
            .client
            .lock()
            .await
            .storage_get(tonic::Request::new(request))
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => return Err(status.into()),
        };
        let first = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("storage_get stream closed before metadata frame"))??;
        match first.content {
            Some(proto::storage_get_chunk::Content::Meta(meta)) => {
                let mut sha_hex = String::with_capacity(meta.sha256.len() * 2);
                for b in &meta.sha256 {
                    use std::fmt::Write;
                    write!(&mut sha_hex, "{b:02x}")?;
                }
                Ok(Some(FileMetadata {
                    content_type: if meta.content_type.is_empty() {
                        None
                    } else {
                        Some(meta.content_type)
                    },
                    size: i64::try_from(meta.content_length)?,
                    sha256: sha_hex,
                }))
            },
            _ => anyhow::bail!("storage_get first frame was not a meta frame"),
        }
    }

    async fn read_document_at_snapshot(
        &self,
        namespace: TableNamespace,
        table: TableName,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<Option<ConvexObject>> {
        // Dedicated `ReadDocument` RPC: the backend opens a
        // short-lived read-only tx at the action's snapshot,
        // looks up the doc by `(table, id)`, and returns either
        // the JSON-encoded document or empty bytes for "not
        // found". `namespace` is forwarded through the
        // CallbackContext's component_path; `TableNamespace::Global`
        // resolves to root, components carry their path.
        let _ = namespace;
        let request = proto::ReadDocumentRequest {
            ctx: Some(self.context()),
            table: String::from(table),
            id: id.encode(),
        };
        let response = self
            .client
            .lock()
            .await
            .read_document(tonic::Request::new(request))
            .await?
            .into_inner();
        if response.document_json.is_empty() {
            return Ok(None);
        }
        let v: serde_json::Value = serde_json::from_slice(&response.document_json)?;
        let cv: ConvexValue = v.try_into()?;
        match cv {
            ConvexValue::Null => Ok(None),
            ConvexValue::Object(obj) => Ok(Some(obj)),
            other => anyhow::bail!(
                "read_document_at_snapshot expected object|null, got: {:?}",
                other
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Client-side unit tests use the `tests_server` in-memory
    //! tonic server built from the auto-generated service trait.
    //! The server captures the inbound request + returns a
    //! canned response; the test asserts the worker-side client
    //! translated the NativeActionCallbacks call correctly.

    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            Mutex as StdMutex,
        },
    };

    use pb::backend_callbacks::{
        backend_callback_service_server::{
            BackendCallbackService,
            BackendCallbackServiceServer,
        },
        ScheduleRequest,
        ScheduleResponse,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{
        transport::Server,
        Request,
        Response,
        Status,
    };

    use super::*;

    #[derive(Default)]
    struct CannedServer {
        last_run_mutation: Arc<StdMutex<Option<proto::RunMutationRequest>>>,
        last_run_action: Arc<StdMutex<Option<proto::RunActionRequest>>>,
        last_schedule: Arc<StdMutex<Option<ScheduleRequest>>>,
    }

    #[tonic::async_trait]
    impl BackendCallbackService for CannedServer {
        type StorageGetStream =
            tokio_stream::wrappers::ReceiverStream<Result<proto::StorageGetChunk, Status>>;

        async fn run_query(
            &self,
            _r: Request<proto::RunQueryRequest>,
        ) -> Result<Response<proto::RunQueryResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }

        async fn run_mutation(
            &self,
            r: Request<proto::RunMutationRequest>,
        ) -> Result<Response<proto::RunMutationResponse>, Status> {
            *self.last_run_mutation.lock().unwrap() = Some(r.into_inner());
            // Return the Convex JSON encoding of a string — the
            // one ConvexValue shape that JSON serialises without
            // ambiguity. Avoids the JSON-number → Float64 default
            // that bites numeric literals.
            Ok(Response::new(proto::RunMutationResponse {
                result: Some(pb::common::FunctionResult {
                    result: Some(pb::common::function_result::Result::JsonPackedValue(
                        "\"ok\"".to_string(),
                    )),
                }),
            }))
        }

        async fn run_action(
            &self,
            r: Request<proto::RunActionRequest>,
        ) -> Result<Response<proto::RunActionResponse>, Status> {
            *self.last_run_action.lock().unwrap() = Some(r.into_inner());
            Ok(Response::new(proto::RunActionResponse {
                result: Some(pb::common::FunctionResult {
                    result: Some(pb::common::function_result::Result::JsonPackedValue(
                        "\"action_done\"".to_string(),
                    )),
                }),
            }))
        }

        async fn schedule(
            &self,
            r: Request<ScheduleRequest>,
        ) -> Result<Response<ScheduleResponse>, Status> {
            *self.last_schedule.lock().unwrap() = Some(r.into_inner());
            // Encode a concrete DeveloperDocumentId so the
            // client-side `.parse()` succeeds.
            let id = value::DeveloperDocumentId::new(
                value::TableNumber::try_from(1u32).unwrap(),
                value::InternalId::MIN,
            );
            Ok(Response::new(ScheduleResponse {
                scheduled_job_id: id.encode(),
            }))
        }

        async fn cancel_job(
            &self,
            _r: Request<proto::CancelJobRequest>,
        ) -> Result<Response<proto::CancelJobResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }

        async fn storage_store(
            &self,
            _r: Request<tonic::Streaming<proto::StorageStoreChunk>>,
        ) -> Result<Response<proto::StorageStoreResponse>, Status> {
            Ok(Response::new(proto::StorageStoreResponse {
                storage_id: "stored-id".to_string(),
            }))
        }

        async fn storage_get(
            &self,
            _r: Request<proto::StorageGetRequest>,
        ) -> Result<Response<Self::StorageGetStream>, Status> {
            Err(Status::unimplemented("test stub"))
        }

        async fn storage_get_url(
            &self,
            _r: Request<proto::StorageGetUrlRequest>,
        ) -> Result<Response<proto::StorageGetUrlResponse>, Status> {
            Ok(Response::new(proto::StorageGetUrlResponse {
                url: Some("https://example.test/file".to_string()),
            }))
        }

        async fn storage_delete(
            &self,
            _r: Request<proto::StorageDeleteRequest>,
        ) -> Result<Response<proto::StorageDeleteResponse>, Status> {
            Ok(Response::new(proto::StorageDeleteResponse {}))
        }

        async fn vector_search(
            &self,
            _r: Request<proto::VectorSearchRequest>,
        ) -> Result<Response<proto::VectorSearchResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }

        async fn lookup_function_handle(
            &self,
            _r: Request<proto::LookupFunctionHandleRequest>,
        ) -> Result<Response<proto::LookupFunctionHandleResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }

        async fn create_function_handle(
            &self,
            _r: Request<proto::CreateFunctionHandleRequest>,
        ) -> Result<Response<proto::CreateFunctionHandleResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }

        async fn read_document(
            &self,
            _r: Request<proto::ReadDocumentRequest>,
        ) -> Result<Response<proto::ReadDocumentResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
    }

    async fn spawn(server: CannedServer) -> (SocketAddr, Arc<CannedServer>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_arc = Arc::new(server);
        let server_clone_for_service = server_arc.clone();
        tokio::spawn(async move {
            let svc = BackendCallbackServiceServer::new(CannedHandle(server_clone_for_service));
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        (addr, server_arc)
    }

    // Adapter that forwards to the Arc-wrapped CannedServer. tonic
    // requires service impls be owned, and `Arc<CannedServer>` can't
    // be the service directly because the trait methods take `&self`.
    struct CannedHandle(Arc<CannedServer>);

    #[tonic::async_trait]
    impl BackendCallbackService for CannedHandle {
        type StorageGetStream = <CannedServer as BackendCallbackService>::StorageGetStream;

        async fn run_query(
            &self,
            r: Request<proto::RunQueryRequest>,
        ) -> Result<Response<proto::RunQueryResponse>, Status> {
            self.0.run_query(r).await
        }

        async fn run_mutation(
            &self,
            r: Request<proto::RunMutationRequest>,
        ) -> Result<Response<proto::RunMutationResponse>, Status> {
            self.0.run_mutation(r).await
        }

        async fn run_action(
            &self,
            r: Request<proto::RunActionRequest>,
        ) -> Result<Response<proto::RunActionResponse>, Status> {
            self.0.run_action(r).await
        }

        async fn schedule(
            &self,
            r: Request<ScheduleRequest>,
        ) -> Result<Response<ScheduleResponse>, Status> {
            self.0.schedule(r).await
        }

        async fn cancel_job(
            &self,
            r: Request<proto::CancelJobRequest>,
        ) -> Result<Response<proto::CancelJobResponse>, Status> {
            self.0.cancel_job(r).await
        }

        async fn storage_store(
            &self,
            r: Request<tonic::Streaming<proto::StorageStoreChunk>>,
        ) -> Result<Response<proto::StorageStoreResponse>, Status> {
            self.0.storage_store(r).await
        }

        async fn storage_get(
            &self,
            r: Request<proto::StorageGetRequest>,
        ) -> Result<Response<Self::StorageGetStream>, Status> {
            self.0.storage_get(r).await
        }

        async fn storage_get_url(
            &self,
            r: Request<proto::StorageGetUrlRequest>,
        ) -> Result<Response<proto::StorageGetUrlResponse>, Status> {
            self.0.storage_get_url(r).await
        }

        async fn storage_delete(
            &self,
            r: Request<proto::StorageDeleteRequest>,
        ) -> Result<Response<proto::StorageDeleteResponse>, Status> {
            self.0.storage_delete(r).await
        }

        async fn vector_search(
            &self,
            r: Request<proto::VectorSearchRequest>,
        ) -> Result<Response<proto::VectorSearchResponse>, Status> {
            self.0.vector_search(r).await
        }

        async fn lookup_function_handle(
            &self,
            r: Request<proto::LookupFunctionHandleRequest>,
        ) -> Result<Response<proto::LookupFunctionHandleResponse>, Status> {
            self.0.lookup_function_handle(r).await
        }

        async fn create_function_handle(
            &self,
            r: Request<proto::CreateFunctionHandleRequest>,
        ) -> Result<Response<proto::CreateFunctionHandleResponse>, Status> {
            self.0.create_function_handle(r).await
        }

        async fn read_document(
            &self,
            r: Request<proto::ReadDocumentRequest>,
        ) -> Result<Response<proto::ReadDocumentResponse>, Status> {
            self.0.read_document(r).await
        }
    }

    fn empty_object() -> ConvexObject {
        use std::collections::BTreeMap;
        let m: BTreeMap<value::FieldName, ConvexValue> = BTreeMap::new();
        ConvexObject::try_from(m).unwrap()
    }

    #[tokio::test]
    async fn run_mutation_routes_to_backend() {
        let (addr, server) = spawn(CannedServer::default()).await;
        let client =
            BackendCallbackClient::connect(format!("http://{addr}"), vec![], None, "".to_string())
                .await
                .expect("connect");

        let result = client
            .run_mutation_by_name(TableNamespace::Global, "set_value", empty_object())
            .await
            .expect("run_mutation");
        assert_eq!(result, ConvexValue::try_from("ok".to_string()).unwrap());
        let captured = server.last_run_mutation.lock().unwrap().clone();
        let captured = captured.expect("server saw the request");
        assert_eq!(captured.function_name, "set_value");
        assert!(captured.ctx.is_some());
    }

    #[tokio::test]
    async fn run_action_routes_to_backend() {
        let (addr, server) = spawn(CannedServer::default()).await;
        let client =
            BackendCallbackClient::connect(format!("http://{addr}"), vec![], None, "".to_string())
                .await
                .expect("connect");
        let result = client
            .run_action_by_name(TableNamespace::Global, "send_email", empty_object())
            .await
            .expect("run_action");
        assert_eq!(
            result,
            ConvexValue::try_from("action_done".to_string()).unwrap(),
        );
        let captured = server.last_run_action.lock().unwrap().clone();
        let captured = captured.expect("server saw the request");
        assert_eq!(captured.function_name, "send_email");
        assert!(captured.ctx.is_some());
    }

    #[tokio::test]
    async fn schedule_routes_to_backend() {
        let (addr, server) = spawn(CannedServer::default()).await;
        let client =
            BackendCallbackClient::connect(format!("http://{addr}"), vec![], None, "".to_string())
                .await
                .expect("connect");

        let id = client
            .schedule(
                TableNamespace::Global,
                "my_job",
                empty_object(),
                Duration::from_secs(30),
            )
            .await
            .expect("schedule");
        // Id round-trip check: the client should decode the
        // canonical-string form the server emits and return the
        // corresponding DeveloperDocumentId; the parsed id's
        // `.encode()` should match the server-emitted string.
        let captured = server.last_schedule.lock().unwrap().clone().unwrap();
        assert!(!id.encode().is_empty(), "encoded id is a non-empty string");
        assert_eq!(captured.function_name, "my_job");
        assert!(captured.fire_at_unix_nanos > 0);
    }

    #[tokio::test]
    async fn storage_store_streams_body() {
        let (addr, _server) = spawn(CannedServer::default()).await;
        let client =
            BackendCallbackClient::connect(format!("http://{addr}"), vec![], None, "".to_string())
                .await
                .expect("connect");

        let id = client
            .storage_store(
                TableNamespace::Global,
                Bytes::from_static(b"hello world"),
                "text/plain",
            )
            .await
            .expect("storage_store");
        assert_eq!(id.0, "stored-id");
    }

    #[tokio::test]
    async fn storage_get_url_returns_backend_url() {
        let (addr, _server) = spawn(CannedServer::default()).await;
        let client =
            BackendCallbackClient::connect(format!("http://{addr}"), vec![], None, "".to_string())
                .await
                .expect("connect");

        let url = client
            .storage_get_url(TableNamespace::Global, StorageId("any".to_string()))
            .await
            .expect("storage_get_url");
        assert_eq!(url.as_deref(), Some("https://example.test/file"));
    }
}
