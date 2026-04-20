//! Distributed-topology action + error-propagation coverage.
//!
//! Mirrors `standalone_actions.rs` but drives every dispatch
//! through a real `FunctionExecutionServer` + `TonicWorkerClient`
//! + `DistributedFunctionRunner`. Confirms that:
//!
//! - Pure actions (`echo_action`) round-trip their result through the wire.
//! - Action `ctx.log(...)` lines arrive back on the backend via
//!   `ExecuteResponse::log_lines`.
//! - `final_tx` is always `None` for actions (actions don't open an enclosing
//!   tx).
//! - `errors::bad_request` from a mutation handler surfaces as a handler-level
//!   `Err(String)` on the wire — not a transport error.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
};

use common::types::UdfType;
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

fn args(pairs: &[(&str, ConvexValue)]) -> ConvexObject {
    let mut map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k.parse::<FieldName>().unwrap(), v.clone());
    }
    ConvexObject::try_from(map).unwrap()
}

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

fn request(name: &str, args: ConvexObject) -> ExecuteRequest {
    ExecuteRequest {
        name: name.to_string(),
        namespace: TableNamespace::Global,
        args,
        timeout: None,
        min_registry_version: None,
        execution_context: None,
        begin_timestamp: None,
        existing_writes: Vec::new(),
        http_request: None,
        identity: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pure_action_round_trips_over_the_wire() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            request(
                "echo_action",
                args(&[("message", ConvexValue::try_from("wire".to_string())?)]),
            ),
            UdfType::Action,
        )
        .await?;
    let v = resp.result.expect("action succeeds");
    match v {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "echo:wire"),
        other => panic!("expected string, got {other:?}"),
    }
    assert!(
        resp.final_tx.is_none(),
        "actions never open a tx; final_tx must stay None",
    );
    assert!(
        resp.log_lines
            .iter()
            .any(|line| line.contains("echo: wire")),
        "expected action log line over the wire; got {:?}",
        resp.log_lines,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_user_error_surfaces_verbatim_over_the_wire() -> anyhow::Result<()> {
    // `always_bad_request` returns `errors::bad_request(...)`; the
    // wire contract is that handler-level errors come back as
    // `Err(String)` on `ExecuteResponse.result`, not a tonic
    // transport error. Pin the contract so a regression can't
    // accidentally convert user errors into 500s.
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            ExecuteRequest {
                begin_timestamp: Some(u64::from(*fx.database.now_ts_for_reads())),
                ..request("always_bad_request", ConvexObject::empty())
            },
            UdfType::Mutation,
        )
        .await?;
    let err = resp.result.expect_err("always_bad_request errors");
    assert!(
        err.contains("deliberately broken"),
        "expected the bad_request message on the wire; got: {err}",
    );
    Ok(())
}
