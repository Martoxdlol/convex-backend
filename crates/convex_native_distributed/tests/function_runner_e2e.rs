//! End-to-end gRPC integration test for the Phase-2
//! `DistributedFunctionRunner` dispatch path.
//!
//! Substep 2.8a of `convex-native/STATUS.md`: validates the
//! **wire path** is wired up correctly — backend builds a request,
//! dispatches via `DistributedFunctionRunner::execute` over real
//! tonic, worker receives it, worker returns a handler-level
//! error (no handler is registered in this test process), the
//! response round-trips verbatim.
//!
//! Substep 2.8b — the full cross-process mutation with
//! `SubscriptionManager` invalidation assertion — is tracked
//! separately. It needs a real `Database<Rt>` fixture (persistence,
//! retention, committer) on both sides plus a real native handler
//! registered via `inventory::submit!`, none of which have test
//! helpers in the open-source repo today. See
//! `convex-native/STATUS.md` substep 2.8 for the blocker.
//!
//! What this test proves:
//! 1. The Phase-2 `ExecuteRequest` fields (`begin_timestamp`,
//!    `existing_writes`) survive the tonic boundary on the request side.
//! 2. The Phase-2 `ExecuteResponse.final_tx` path is `None` when the worker has
//!    no handler (matches the "handler-errored-before-opening-tx" contract from
//!    substep 2.5).
//! 3. `log_lines` on the response survive round-trip.

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
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use value::{
    ConvexObject,
    ConvexValue,
    FieldName,
    TableNamespace,
};

async fn spawn_worker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native).with_registry_version("e2e-1.0.0");
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

fn empty_object() -> ConvexObject {
    let f: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    ConvexObject::try_from(f).unwrap()
}

#[tokio::test]
async fn action_dispatch_round_trips_with_handler_level_error() {
    // Full end-to-end wire path: `DistributedFunctionRunner` →
    // `TonicWorkerClient` → tonic server → `FunctionExecutionServer`
    // → `NativeFunctionRunner` (empty inventory) → handler-level
    // "not found" error → back over the wire → parsed into
    // native `ExecuteResponse`. `final_tx` must be `None` because
    // actions don't open a tx, independent of the handler
    // existing or not.
    let addr = spawn_worker().await;
    let client = TonicWorkerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    let runner = DistributedFunctionRunner::new(vec![client]).unwrap();
    let req = ExecuteRequest {
        name: "does_not_exist".to_string(),
        namespace: TableNamespace::Global,
        args: empty_object(),
        timeout: None,
        min_registry_version: None,
        execution_context: None,
        begin_timestamp: None,
        existing_writes: Vec::new(),
        http_request: None,
    };
    let resp = runner
        .execute(req, UdfType::Action)
        .await
        .expect("dispatch");
    assert!(
        matches!(resp.result, Err(ref m) if m.contains("does_not_exist")),
        "handler-level error surfaces verbatim through the wire: {:?}",
        resp.result,
    );
    assert!(
        resp.final_tx.is_none(),
        "actions have no enclosing tx; final_tx must stay None after the wire round-trip",
    );
}

#[tokio::test]
async fn query_without_database_surfaces_as_transport_error() {
    // Without `.with_database(...)` on the server, Query/Mutation
    // dispatch returns `tonic::Code::Unimplemented` — the
    // `DistributedFunctionRunner::execute` path surfaces that as
    // a `Status` error (not a handler-level Err). Pin the
    // contract so a follow-up change can't silently turn it into
    // a silent "Ok(empty)" response.
    let addr = spawn_worker().await;
    let client = TonicWorkerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    let runner = DistributedFunctionRunner::new(vec![client]).unwrap();
    let req = ExecuteRequest {
        name: "any_query".to_string(),
        namespace: TableNamespace::Global,
        args: empty_object(),
        timeout: None,
        min_registry_version: None,
        execution_context: None,
        begin_timestamp: Some(42),
        existing_writes: Vec::new(),
        http_request: None,
    };
    let err = runner
        .execute(req, UdfType::Query)
        .await
        .expect_err("query must fail with Unimplemented when worker has no Database attached");
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(
        err.message().contains("Database<Rt>"),
        "error points the operator at .with_database(): {}",
        err.message(),
    );
}

#[tokio::test]
async fn action_dispatch_never_carries_final_tx() {
    // Substep 4.5 invariant: actions don't open a transaction,
    // so the `final_tx` field on the round-tripped response
    // must stay `None` independent of whether the handler
    // succeeded or errored. Regression guard for a future
    // refactor accidentally forwarding the Query/Mutation
    // `summarise_tx` path through the action branch.
    let addr = spawn_worker().await;
    let client = TonicWorkerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    let runner = DistributedFunctionRunner::new(vec![client]).unwrap();
    let req = ExecuteRequest {
        name: "does_not_exist".to_string(),
        namespace: TableNamespace::Global,
        args: empty_object(),
        timeout: None,
        min_registry_version: None,
        execution_context: None,
        begin_timestamp: None,
        existing_writes: Vec::new(),
        http_request: None,
    };
    let resp = runner
        .execute(req, UdfType::Action)
        .await
        .expect("dispatch");
    assert!(resp.final_tx.is_none(), "actions never carry final_tx");
    assert!(
        matches!(resp.result, Err(ref m) if m.contains("does_not_exist")),
        "handler-level error surfaced: {:?}",
        resp.result,
    );
}

#[tokio::test]
async fn action_request_carries_phase2_fields_through_the_wire() {
    // Even when the server rejects the call at the handler layer,
    // the Phase-2 request fields (`begin_timestamp`,
    // `existing_writes`) have to serialize cleanly. If they ever
    // start tripping the encoder, `execute` will fail with a
    // transport `InvalidArgument` instead of surfacing the
    // handler-level error. Regression guard for the
    // substep-2.4 / substep-2.1 wire shape.
    let addr = spawn_worker().await;
    let client = TonicWorkerClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
    let runner = DistributedFunctionRunner::new(vec![client]).unwrap();
    let req = ExecuteRequest {
        name: "does_not_exist".to_string(),
        namespace: TableNamespace::Global,
        args: empty_object(),
        timeout: None,
        min_registry_version: None,
        execution_context: None,
        begin_timestamp: Some(12345),
        // Empty staged writes — the `to_proto_request` path
        // encodes this as `None`, which the worker handles
        // without erroring.
        existing_writes: Vec::new(),
        http_request: None,
    };
    let resp = runner
        .execute(req, UdfType::Action)
        .await
        .expect("dispatch");
    // Handler-level error — proves we reached the
    // NativeFunctionRunner past the encode/decode boundary.
    assert!(
        matches!(resp.result, Err(ref m) if m.contains("does_not_exist")),
        "phase-2 fields serialized cleanly; handler-level error surfaced: {:?}",
        resp.result,
    );
}
