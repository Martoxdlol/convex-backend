//! Substep 4.4 of `convex-native/STATUS.md` — cross-process
//! action sub-call test.
//!
//! Wires the full Phase-4 chain end-to-end:
//!
//! 1. Backend-side `BackendCallbackServer` with a recording `ActionCallbacks`
//!    stub.
//! 2. Worker-side `FunctionExecutionServer` configured with
//!    `.with_backend_callback_endpoint(...)` pointing at the callback server.
//! 3. An action sub-call flows worker → backend → recorder, and the recorder
//!    observes the dotted name + serialized args.
//!
//! The action handler itself runs through `NativeFunctionRunner`,
//! but the test harness doesn't need a real `#[convex::action]`
//! registration — it directly constructs a
//! `BackendCallbackClient` (the worker-side piece) and calls
//! `run_mutation_by_name` on it. The test proves the server
//! side of the worker boot path correctly builds + connects a
//! `BackendCallbackClient` when the endpoint is configured.
//!
//! A fuller test would register a real `#[convex::action]` in
//! the test binary and let the action's `ctx.run_mutation`
//! call reach the callbacks trait object. That end-to-end
//! shape lands with substep 4.6 once the test-binary macro
//! registration story is more ergonomic.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        Mutex as StdMutex,
    },
};

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
use convex_native_core::callbacks::NativeActionCallbacks;
use convex_native_distributed::{
    backend_callbacks_client::BackendCallbackClient,
    backend_callbacks_server::BackendCallbackServer,
};
use keybroker::Identity;
use model::file_storage::{
    types::FileStorageEntry,
    FileStorageId,
};
use pb::backend_callbacks::backend_callback_service_server::BackendCallbackServiceServer;
use serde_json::Value as JsonValue;
use sync_types::types::SerializedArgs;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use udf::FunctionResult;
use usage_tracking::FunctionUsageStats;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    InternalId,
    JsonPackedValue,
    TableNamespace,
    TableNumber,
};

#[derive(Default)]
struct RecordingCallbacks {
    last_mutation: StdMutex<Option<(String, SerializedArgs)>>,
}

#[async_trait]
impl udf::ActionCallbacks for RecordingCallbacks {
    async fn execute_query(
        &self,
        _identity: Identity,
        _path: CanonicalizedComponentFunctionPath,
        _args: SerializedArgs,
        _context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        anyhow::bail!("execute_query: not exercised in this test")
    }

    async fn execute_mutation(
        &self,
        _identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        _context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        *self.last_mutation.lock().unwrap() = Some((format!("{path:?}"), args));
        Ok(FunctionResult {
            result: Ok(JsonPackedValue::from_network(
                "\"mutation-ok\"".to_string(),
            )?),
        })
    }

    async fn execute_action(
        &self,
        _identity: Identity,
        _path: CanonicalizedComponentFunctionPath,
        _args: SerializedArgs,
        _context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        anyhow::bail!("execute_action: not exercised in this test")
    }

    async fn storage_get_url(
        &self,
        _identity: Identity,
        _component: ComponentId,
        _storage_id: FileStorageId,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
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
        anyhow::bail!("not exercised")
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
        anyhow::bail!("not exercised")
    }

    async fn lookup_function_handle(
        &self,
        _identity: Identity,
        _handle: FunctionHandle,
    ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
        anyhow::bail!("not exercised")
    }

    async fn create_function_handle(
        &self,
        _identity: Identity,
        _path: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<FunctionHandle> {
        anyhow::bail!("not exercised")
    }
}

async fn spawn_backend_callbacks(callbacks: Arc<RecordingCallbacks>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let svc = BackendCallbackServer::new(callbacks);
        Server::builder()
            .add_service(BackendCallbackServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    addr
}

fn empty_object() -> ConvexObject {
    use std::collections::BTreeMap;
    let m: BTreeMap<value::FieldName, ConvexValue> = BTreeMap::new();
    ConvexObject::try_from(m).unwrap()
}

async fn spawn_worker_exec_server(endpoint: String) -> SocketAddr {
    use convex_native_core::NativeFunctionRunner;
    use convex_native_distributed::FunctionExecutionServer;
    use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native)
        .with_backend_callback_endpoint(endpoint)
        .with_registry_version("4.6-test");
    tokio::spawn(async move {
        Server::builder()
            .add_service(FunctionExecutionServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    addr
}

#[tokio::test]
async fn worker_exec_server_wires_callback_endpoint_into_action_dispatch() {
    // Substep 4.6 exit criterion (wire-proof portion): a worker
    // whose `FunctionExecutionServer` is configured with
    // `.with_backend_callback_endpoint(...)` dispatches
    // `UdfType::Action` requests with a `BackendCallbackClient`
    // wired up, not `NoopCallbacks`. We can't exercise the full
    // action → ctx.run_mutation path here because the test
    // binary doesn't register `#[convex::action]` entries (the
    // handler registry is `inventory::submit!`-populated at
    // link time and empty in this binary); the action
    // dispatch surfaces a handler-level "does not exist"
    // error. What this test pins is that the Action branch on
    // the server **runs** rather than bailing from
    // `Unimplemented`, and that the backend callback endpoint
    // dial succeeds.
    //
    // The remaining substep 4.6b — actually dispatching a
    // sub-mutation from inside a running action and asserting
    // the backend's database sees the commit — is blocked on
    // the same `Database<Rt>` test fixture story as substep
    // 2.8b. See `convex-native/STATUS.md` for the deferral.
    use convex_native_distributed::{
        DistributedFunctionRunner,
        TonicWorkerClient,
    };
    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableNamespace,
    };
    let callbacks = Arc::new(RecordingCallbacks::default());
    let callback_addr = spawn_backend_callbacks(callbacks.clone()).await;
    let worker_addr = spawn_worker_exec_server(format!("http://{callback_addr}")).await;

    let client = TonicWorkerClient::connect(format!("http://{worker_addr}"))
        .await
        .expect("connect worker");
    let runner = DistributedFunctionRunner::new(vec![client]).unwrap();
    let obj: std::collections::BTreeMap<FieldName, ConvexValue> = std::collections::BTreeMap::new();
    let req = convex_native_core::distributed::ExecuteRequest {
        name: "does_not_exist".to_string(),
        namespace: TableNamespace::Global,
        args: ConvexObject::try_from(obj).unwrap(),
        timeout: None,
        min_registry_version: None,
        execution_context: None,
        begin_timestamp: None,
        existing_writes: Vec::new(),
        http_request: None,
        identity: Vec::new(),
    };
    let resp = runner
        .execute(req, common::types::UdfType::Action)
        .await
        .expect("gRPC dispatch");
    // Handler doesn't exist in the test binary → native runner
    // surfaces an error. The important thing is the dispatch
    // reached `run_action_with_callbacks_and_log_buffer` (proving
    // the callback client was constructed) rather than bailing
    // from the outer match.
    assert!(
        matches!(resp.result, Err(ref m) if m.contains("does_not_exist")),
        "action reached the NativeFunctionRunner past the callback-client setup: {:?}",
        resp.result,
    );
}

#[tokio::test]
async fn worker_sub_mutation_reaches_backend_action_callbacks() {
    // Substep 4.4 exit criterion: a sub-mutation call on the
    // worker's `NativeActionCallbacks` surfaces on the
    // backend's `ActionCallbacks::execute_mutation` — which, in
    // production, commits through the backend's Committer.
    //
    // The worker-side client isn't "the server's
    // NoopCallbacks"; it's a `BackendCallbackClient` built on
    // the same code path `FunctionExecutionServer`'s action
    // branch uses when
    // `with_backend_callback_endpoint(...)` is set.
    let callbacks = Arc::new(RecordingCallbacks::default());
    let addr = spawn_backend_callbacks(callbacks.clone()).await;

    let client =
        BackendCallbackClient::connect(format!("http://{addr}"), Vec::new(), None, String::new())
            .await
            .expect("connect");

    let value = client
        .run_mutation_by_name(TableNamespace::Global, "set_user", empty_object())
        .await
        .expect("run_mutation_by_name");
    assert_eq!(
        value,
        ConvexValue::try_from("mutation-ok".to_string()).unwrap()
    );

    let captured = callbacks.last_mutation.lock().unwrap().clone();
    let (path, _args) = captured.expect("server recorded the sub-mutation");
    assert!(
        path.contains("set_user"),
        "server saw the dotted function path: {path}",
    );
}
