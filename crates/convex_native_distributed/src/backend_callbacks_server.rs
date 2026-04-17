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
//! The RPC envelope carries `identity` as raw bytes. The Phase-4
//! scaffold treats an empty byte vec as `Identity::system()` (the
//! worker-side client currently sends empty because the native
//! ctx doesn't yet forward the acting principal). A follow-up
//! substep decodes the full `convex_identity::Identity` proto
//! into `keybroker::Identity` once the worker-side plumbing is
//! in place.
//!
//! ## What delegates today
//!
//! - `RunQuery` / `RunMutation` / `RunAction` →
//!   `ActionCallbacks::execute_{query,mutation,action}`.
//! - `Schedule` / `CancelJob` → `ActionCallbacks::{schedule_job,cancel_job}`.
//! - `StorageGetUrl` / `StorageDelete` →
//!   `ActionCallbacks::{storage_get_url,storage_delete}`.
//!
//! ## What returns `Unimplemented` for now
//!
//! - `StorageStore` (streaming — needs a `FileStorage` + `Application` handle
//!   the worker's proto-level `storage_id` can't round-trip yet), `StorageGet`
//!   (same), `VectorSearch` (needs `VectorSearchQuery` JSON wire-shape
//!   validation), `LookupFunctionHandle` / `CreateFunctionHandle` (needs the
//!   `FunctionHandle` string encoding contract pinned).
//!
//! Wiring those in is additive and lands as each deployer
//! action exercises the path.

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

/// Server-side impl of the `BackendCallbackService` RPC trait.
/// Construct with an `Arc<dyn ActionCallbacks>` from the
/// backend's `Application` and spawn via tonic.
#[derive(Clone)]
pub struct BackendCallbackServer {
    callbacks: Arc<dyn ActionCallbacks>,
}

impl BackendCallbackServer {
    pub fn new(callbacks: Arc<dyn ActionCallbacks>) -> Self {
        Self { callbacks }
    }
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
        // TODO(phase-4 follow-up): decode
        // `convex_identity::Identity` → `keybroker::Identity`
        // properly. The worker-side client doesn't emit
        // non-empty identity today, so hard-fail if we see one
        // to avoid silently dropping the principal on the floor.
        return Err(Status::unimplemented(
            "BackendCallbackServer: non-empty identity decoding is not yet wired; the worker \
             should send empty identity bytes until substep 4.4 threads the acting principal",
        ));
    };
    let component_path: ComponentPath = if ctx.component_path.is_empty() {
        ComponentPath::root()
    } else {
        return Err(Status::unimplemented(
            "BackendCallbackServer: component-scoped callbacks aren't wired yet — substep 4.4 \
             grows this to map `component:<id>` strings into ComponentPath",
        ));
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
        // job; matches the enclosing action's component. Phase-4
        // scaffold pins it to root until substep 4.4 threads
        // component scoping through.
        let scheduling_component = common::components::ComponentId::Root;
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
        _request: Request<tonic::Streaming<proto::StorageStoreChunk>>,
    ) -> Result<Response<proto::StorageStoreResponse>, Status> {
        Err(Status::unimplemented(
            "BackendCallbackServer::storage_store: follow-up substep wires this through the \
             backend's Application::file_storage handle",
        ))
    }

    async fn storage_get(
        &self,
        _request: Request<proto::StorageGetRequest>,
    ) -> Result<Response<Self::StorageGetStream>, Status> {
        Err(Status::unimplemented(
            "BackendCallbackServer::storage_get: follow-up substep",
        ))
    }

    async fn storage_get_url(
        &self,
        request: Request<proto::StorageGetUrlRequest>,
    ) -> Result<Response<proto::StorageGetUrlResponse>, Status> {
        let req = request.into_inner();
        let (identity, _component, _execution_context) = decode_context(req.ctx)?;
        let storage_id = parse_storage_id(&req.storage_id)?;
        let url = self
            .callbacks
            .storage_get_url(identity, common::components::ComponentId::Root, storage_id)
            .await
            .map_err(|e| Status::internal(format!("storage_get_url: {e}")))?;
        Ok(Response::new(proto::StorageGetUrlResponse { url }))
    }

    async fn storage_delete(
        &self,
        request: Request<proto::StorageDeleteRequest>,
    ) -> Result<Response<proto::StorageDeleteResponse>, Status> {
        let req = request.into_inner();
        let (identity, _component, _execution_context) = decode_context(req.ctx)?;
        let storage_id = parse_storage_id(&req.storage_id)?;
        self.callbacks
            .storage_delete(identity, common::components::ComponentId::Root, storage_id)
            .await
            .map_err(|e| Status::internal(format!("storage_delete: {e}")))?;
        Ok(Response::new(proto::StorageDeleteResponse {}))
    }

    async fn vector_search(
        &self,
        _request: Request<proto::VectorSearchRequest>,
    ) -> Result<Response<proto::VectorSearchResponse>, Status> {
        Err(Status::unimplemented(
            "BackendCallbackServer::vector_search: follow-up substep",
        ))
    }

    async fn lookup_function_handle(
        &self,
        _request: Request<proto::LookupFunctionHandleRequest>,
    ) -> Result<Response<proto::LookupFunctionHandleResponse>, Status> {
        Err(Status::unimplemented(
            "BackendCallbackServer::lookup_function_handle: follow-up substep",
        ))
    }

    async fn create_function_handle(
        &self,
        _request: Request<proto::CreateFunctionHandleRequest>,
    ) -> Result<Response<proto::CreateFunctionHandleResponse>, Status> {
        Err(Status::unimplemented(
            "BackendCallbackServer::create_function_handle: follow-up substep",
        ))
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
    async fn nonempty_identity_bytes_surface_as_unimplemented() {
        // The Phase-4 scaffold doesn't decode a real identity
        // yet — pin the loud failure so a future plumbing
        // change doesn't silently route the sub-call under
        // `Identity::system()` when the worker sends a real
        // principal.
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
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("identity decoding"));
    }
}
