//! Distributed-topology coverage for `ExecutionContext`
//! propagation.
//!
//! The backend builds a request carrying an `ExecutionContext`
//! with a specific `request_id`; the worker must decode it and
//! surface it through `ctx.execution_context()` so log/trace
//! correlation works across the process boundary. The wire
//! shape is `pb::common::ExecutionContext` — pinned here against
//! the fixture's `request_id` query handler.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
};

use common::{
    execution_context::{
        ExecutionContext,
        ExecutionId,
        RequestId,
    },
    types::UdfType,
};
use convex_native_core::{
    distributed::ExecuteRequest,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    DistributedFunctionRunner,
    FunctionExecutionServer,
    TonicWorkerClient,
};
use convex_native_integration_tests::db_fixture::DbFixture;
use database::Database;
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use runtime::prod::ProdRuntime;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use value::{
    ConvexObject,
    ConvexValue,
    FieldName,
    TableNamespace,
};

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

async fn spawn_worker(db: Database<ProdRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native)
        .with_database(db)
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
async fn request_id_round_trips_through_the_worker_ctx() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let context =
        ExecutionContext::new_from_parts(RequestId::new(), ExecutionId::new(), None, false);
    let expected = context.request_id.to_string();

    let args: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "request_id".to_string(),
                namespace: TableNamespace::Global,
                args: ConvexObject::try_from(args)?,
                timeout: None,
                min_registry_version: None,
                execution_context: Some(context),
                begin_timestamp: Some(u64::from(*fx.database.now_ts_for_reads())),
                existing_writes: Vec::new(),
                http_request: None,
                identity: Vec::new(),
            },
            UdfType::Query,
        )
        .await?;
    let got = resp.result.expect("query succeeds");
    match got {
        ConvexValue::String(s) => assert_eq!(
            s.to_string(),
            expected,
            "worker-side ctx.execution_context().request_id should match what the backend sent",
        ),
        other => panic!("expected string, got {other:?}"),
    }
    Ok(())
}
