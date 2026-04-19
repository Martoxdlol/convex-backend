//! End-to-end distributed action → sub-call coverage.
//!
//! Wires the full chain: worker-side `FunctionExecutionServer`
//! (with `.with_backend_callback_endpoint(...)`) + backend-side
//! `BackendCallbackServer` with an `ActionCallbacks` impl that
//! returns a canned `FunctionResult` for `count_pending`.
//!
//! Dispatches the fixture's `summarise` action; the action runs
//! `ctx.run_query(CountPending, ...)` which routes through
//! `BackendCallbackClient` → `BackendCallbackServer` →
//! `ActionCallbacks::execute_query` → back to the worker → the
//! action returns the value.
//!
//! Proves every link in Phase 4 of DISTRIBUTED_PLAN.md is wired
//! up against the real fixture app (not a stub runner).

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
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
    types::UdfType,
};
use convex_native_core::{
    distributed::ExecuteRequest,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    backend_callbacks_server::BackendCallbackServer,
    DistributedFunctionRunner,
    FunctionExecutionServer,
    TonicWorkerClient,
};
use keybroker::Identity;
use model::file_storage::{
    types::FileStorageEntry,
    FileStorageId,
};
use pb::{
    backend_callbacks::backend_callback_service_server::BackendCallbackServiceServer,
    function_execution::function_execution_service_server::FunctionExecutionServiceServer,
};
use serde_json::Value as JsonValue;
use sync_types::types::SerializedArgs;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use udf::{
    ActionCallbacks,
    FunctionResult,
};
use usage_tracking::FunctionUsageStats;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    FieldName,
    JsonPackedValue,
    TableNamespace,
};
use vector::PublicVectorSearchQueryResult;

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

/// `ActionCallbacks` stub that returns a fixed `String("stub-id")`
/// for any `execute_mutation` call and bails on every other
/// method. Mirrors `FixedQueryCallbacks` but for the sub-mutation
/// leg of the Phase-4 chain.
struct FixedMutationCallbacks;

#[async_trait]
impl ActionCallbacks for FixedMutationCallbacks {
    async fn execute_query(
        &self,
        _: Identity,
        _: CanonicalizedComponentFunctionPath,
        _: SerializedArgs,
        _: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        anyhow::bail!("unused")
    }

    async fn execute_mutation(
        &self,
        _identity: Identity,
        _path: CanonicalizedComponentFunctionPath,
        _args: SerializedArgs,
        _context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        // JsonPackedValue for String("stub-id") — wrapped in the
        // network JSON shape.
        Ok(FunctionResult {
            result: Ok(JsonPackedValue::from_network("\"stub-id\"".to_string())?),
        })
    }

    async fn execute_action(
        &self,
        _: Identity,
        _: CanonicalizedComponentFunctionPath,
        _: SerializedArgs,
        _: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        anyhow::bail!("unused")
    }

    async fn storage_get_url(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageId,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    async fn storage_delete(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn storage_get_file_entry(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageId,
    ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
        Ok(None)
    }

    async fn storage_store_file_entry(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageEntry,
    ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
        anyhow::bail!("unused")
    }

    async fn schedule_job(
        &self,
        _: Identity,
        _: ComponentId,
        _: CanonicalizedComponentFunctionPath,
        _: SerializedArgs,
        _: UnixTimestamp,
        _: ExecutionContext,
    ) -> anyhow::Result<DeveloperDocumentId> {
        anyhow::bail!("unused")
    }

    async fn cancel_job(&self, _: Identity, _: DeveloperDocumentId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn vector_search(
        &self,
        _: Identity,
        _: JsonValue,
    ) -> anyhow::Result<(Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)> {
        anyhow::bail!("unused")
    }

    async fn lookup_function_handle(
        &self,
        _: Identity,
        _: FunctionHandle,
    ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
        anyhow::bail!("unused")
    }

    async fn create_function_handle(
        &self,
        _: Identity,
        _: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<FunctionHandle> {
        anyhow::bail!("unused")
    }
}

/// `ActionCallbacks` stub that returns a fixed `Int64(7)` for any
/// `execute_query` call and bails on every other method. Proves
/// the action's `ctx.run_query(...)` sub-call crosses the wire
/// and the returned value flows back into the action's result.
struct FixedQueryCallbacks;

#[async_trait]
impl ActionCallbacks for FixedQueryCallbacks {
    async fn execute_query(
        &self,
        _identity: Identity,
        _path: CanonicalizedComponentFunctionPath,
        _args: SerializedArgs,
        _context: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        Ok(FunctionResult {
            result: Ok(JsonPackedValue::from_network(
                "{\"$integer\":\"BwAAAAAAAAA=\"}".to_string(),
            )?),
        })
    }

    async fn execute_mutation(
        &self,
        _: Identity,
        _: CanonicalizedComponentFunctionPath,
        _: SerializedArgs,
        _: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        anyhow::bail!("unused")
    }

    async fn execute_action(
        &self,
        _: Identity,
        _: CanonicalizedComponentFunctionPath,
        _: SerializedArgs,
        _: ExecutionContext,
    ) -> anyhow::Result<FunctionResult> {
        anyhow::bail!("unused")
    }

    async fn storage_get_url(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageId,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    async fn storage_delete(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn storage_get_file_entry(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageId,
    ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
        Ok(None)
    }

    async fn storage_store_file_entry(
        &self,
        _: Identity,
        _: ComponentId,
        _: FileStorageEntry,
    ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
        anyhow::bail!("unused")
    }

    async fn schedule_job(
        &self,
        _: Identity,
        _: ComponentId,
        _: CanonicalizedComponentFunctionPath,
        _: SerializedArgs,
        _: UnixTimestamp,
        _: ExecutionContext,
    ) -> anyhow::Result<DeveloperDocumentId> {
        anyhow::bail!("unused")
    }

    async fn cancel_job(&self, _: Identity, _: DeveloperDocumentId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn vector_search(
        &self,
        _: Identity,
        _: JsonValue,
    ) -> anyhow::Result<(Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)> {
        anyhow::bail!("unused")
    }

    async fn lookup_function_handle(
        &self,
        _: Identity,
        _: FunctionHandle,
    ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
        anyhow::bail!("unused")
    }

    async fn create_function_handle(
        &self,
        _: Identity,
        _: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<FunctionHandle> {
        anyhow::bail!("unused")
    }
}

async fn spawn_backend_callbacks() -> SocketAddr {
    spawn_backend_callbacks_with(Arc::new(FixedQueryCallbacks)).await
}

async fn spawn_backend_callbacks_with(impl_: Arc<dyn ActionCallbacks>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = BackendCallbackServer::new(impl_);
    tokio::spawn(async move {
        Server::builder()
            .add_service(BackendCallbackServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    addr
}

async fn spawn_worker(backend_callback_endpoint: String) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native)
        .with_backend_callback_endpoint(backend_callback_endpoint)
        .with_registry_version("integration-1.0.0");
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

#[tokio::test(flavor = "multi_thread")]
async fn summarise_action_routes_sub_query_through_backend_callbacks() -> anyhow::Result<()> {
    let cb_addr = spawn_backend_callbacks().await;
    let worker_addr = spawn_worker(format!("http://{cb_addr}")).await;

    let client = TonicWorkerClient::connect(format!("http://{worker_addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let mut map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    map.insert(
        "owner".parse::<FieldName>()?,
        ConvexValue::try_from("alice".to_string())?,
    );
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "summarise".to_string(),
                namespace: TableNamespace::Global,
                args: ConvexObject::try_from(map)?,
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: None,
                existing_writes: Vec::new(),
                http_request: None,
                identity: Vec::new(),
            },
            UdfType::Action,
        )
        .await?;
    let v = resp.result.expect("summarise succeeds");
    match v {
        ConvexValue::Int64(n) => assert_eq!(
            n, 7,
            "summarise returns the stubbed count_pending value; got {n}",
        ),
        other => panic!("expected Int64, got {other:?}"),
    }
    assert!(
        resp.final_tx.is_none(),
        "actions never carry final_tx (Phase-4 invariant)",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn chain_create_action_routes_sub_mutation_through_backend_callbacks() -> anyhow::Result<()> {
    // Mirror of summarise_action_routes_sub_query_through_backend_callbacks
    // but for the sub-*mutation* leg. chain_create_from_action
    // does ctx.run_mutation_raw("create_todo", ...) from inside
    // an action; with a stubbed backend-side execute_mutation
    // returning "stub-id", the whole chain must return that id
    // to the action caller over the wire.
    let cb_addr = spawn_backend_callbacks_with(Arc::new(FixedMutationCallbacks)).await;
    let worker_addr = spawn_worker(format!("http://{cb_addr}")).await;

    let client = TonicWorkerClient::connect(format!("http://{worker_addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let mut map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    map.insert(
        "owner".parse::<FieldName>()?,
        ConvexValue::try_from("alice".to_string())?,
    );
    map.insert(
        "text".parse::<FieldName>()?,
        ConvexValue::try_from("over-the-wire".to_string())?,
    );
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "chain_create_from_action".to_string(),
                namespace: TableNamespace::Global,
                args: ConvexObject::try_from(map)?,
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: None,
                existing_writes: Vec::new(),
                http_request: None,
                identity: Vec::new(),
            },
            UdfType::Action,
        )
        .await?;
    let v = resp.result.expect("chain_create_from_action succeeds");
    match v {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "stub-id"),
        other => panic!("expected 'stub-id', got {other:?}"),
    }
    Ok(())
}
