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

#[tokio::test(flavor = "multi_thread")]
async fn anonymous_identity_round_trips_and_whoami_reports_anonymous() -> anyhow::Result<()> {
    // Every existing distributed test sends `identity: Vec::new()`,
    // which the encode_identity_for_wire shortcut hands the
    // worker as Identity::System. Proving an explicitly-forged
    // Identity::Unknown(None) encodes through UncheckedIdentity,
    // reaches the worker, and re-hydrates into ctx.auth() as
    // the anonymous principal is what pins the "caller identity
    // survives the wire" contract at full fidelity.
    use convex_native_distributed::function_runner_impl::encode_identity_for_wire;
    use keybroker::Identity;

    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let anon_bytes = encode_identity_for_wire(&Identity::Unknown(None));
    assert!(
        !anon_bytes.is_empty(),
        "anonymous identity must round-trip through UncheckedIdentity (empty-bytes reserved for \
         the system short-circuit)",
    );

    let args: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "whoami".to_string(),
                namespace: TableNamespace::Global,
                args: ConvexObject::try_from(args)?,
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: Some(u64::from(*fx.database.now_ts_for_reads())),
                existing_writes: Vec::new(),
                http_request: None,
                identity: anon_bytes,
            },
            UdfType::Query,
        )
        .await?;
    let got = resp.result.expect("query succeeds");
    match got {
        ConvexValue::String(s) => assert_eq!(
            s.to_string(),
            "anonymous",
            "worker ctx.auth() should see the forged Unknown identity",
        ),
        other => panic!("expected string, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn execution_id_round_trips_through_the_worker_ctx() -> anyhow::Result<()> {
    // Companion to request_id_round_trips_through_the_worker_ctx;
    // execution_id lives in a separate proto field
    // (pb::common::ExecutionContext.execution_id) from request_id
    // and has its own parse path, so a regression in its wire
    // encoding or ctx hydration wouldn't fall out of the
    // request-id test alone.
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let context =
        ExecutionContext::new_from_parts(RequestId::new(), ExecutionId::new(), None, false);
    let expected = context.execution_id.to_string();

    let args: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "execution_id".to_string(),
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
            "worker-side ctx.execution_context().execution_id should match what the backend sent",
        ),
        other => panic!("expected string, got {other:?}"),
    }
    Ok(())
}
