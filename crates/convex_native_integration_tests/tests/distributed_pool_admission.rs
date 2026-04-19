//! End-to-end admission + pool coverage against the real fixture
//! inventory.
//!
//! A worker with the fixture app linked in registers against an
//! admission server → the pool ends up carrying the fixture's
//! function specs and can look them up for native HTTP / WebSocket
//! validation (how the backend accepts a worker-registered
//! handler without needing `_modules` rows). Mirrors the shape
//! `local_backend::make_app` sets up in production.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use common::types::UdfType;
use convex_native_core::NativeFunctionRunner;
use convex_native_distributed::{
    admission_client::WorkerRegistration,
    admission_server::WorkerAdmissionServer,
    pool::WorkerPool,
    FunctionExecutionServer,
};
use pb::{
    function_execution::function_execution_service_server::FunctionExecutionServiceServer,
    worker_admission::worker_admission_service_server::WorkerAdmissionServiceServer,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

async fn spawn_admission(pool: Arc<WorkerPool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let service = WorkerAdmissionServer::new(pool);
        Server::builder()
            .add_service(WorkerAdmissionServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    addr
}

async fn spawn_worker_exec() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native).with_registry_version("fixture-1.0.0");
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

async fn wait_for<F: Fn() -> bool>(pred: F) -> bool {
    for _ in 0..50 {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pred()
}

#[tokio::test(flavor = "multi_thread")]
async fn pool_sees_fixture_function_specs_after_admission() -> anyhow::Result<()> {
    let pool = Arc::new(WorkerPool::new());
    let admission_addr = spawn_admission(pool.clone()).await;
    let exec_addr = spawn_worker_exec().await;

    let _registration = WorkerRegistration::register(
        format!("http://{admission_addr}"),
        format!("http://{exec_addr}"),
        "fixture-1.0.0".to_string(),
    )
    .await?;

    assert!(
        wait_for(|| pool.len() == 1).await,
        "worker admitted within timeout",
    );

    // Fixture function specs arrive through the admission
    // envelope. The pool must know each handler's kind so
    // `udf::validation` can accept routes without a `_modules`
    // row.
    let create = pool
        .lookup_function("create_todo")
        .expect("create_todo advertised");
    assert_eq!(create.udf_type, UdfType::Mutation);
    assert!(!create.is_internal);

    let internal = pool
        .lookup_function("internal_delete")
        .expect("internal_delete advertised");
    assert_eq!(internal.udf_type, UdfType::Mutation);
    assert!(
        internal.is_internal,
        "internal modifier must propagate into pool's FunctionSpec",
    );

    let summarise = pool
        .lookup_function("summarise")
        .expect("summarise advertised");
    assert_eq!(summarise.udf_type, UdfType::Action);

    let ping = pool
        .lookup_function("__http::POST:/api/ping")
        .expect("http_action advertised under synthetic name");
    assert_eq!(ping.udf_type, UdfType::HttpAction);

    // eligible_for_http is the router-aware lookup the backend
    // consults when forwarding /http/* traffic to workers. A
    // regression in the HTTP-route indexing would leave the
    // name-based lookup working but break the method+path
    // dispatch. Pin both the happy path and the method-miss
    // path.
    let eligible = pool.eligible_for_http("POST", "/api/ping");
    assert!(
        !eligible.is_empty(),
        "eligible_for_http should return at least one worker for a registered route",
    );
    let wrong_method = pool.eligible_for_http("GET", "/api/ping");
    assert!(
        wrong_method.is_empty(),
        "method miss must return empty (GET /api/ping is not registered as a GET route)",
    );

    assert!(
        pool.lookup_function("does_not_exist").is_none(),
        "unknown names don't resolve",
    );
    Ok(())
}
