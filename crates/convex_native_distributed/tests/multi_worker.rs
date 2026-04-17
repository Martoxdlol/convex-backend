//! Phase 3.6 integration tests: multiple live workers behind one
//! `DistributedFunctionRunner` over real gRPC.
//!
//! Each test spins up N local tonic servers on ephemeral ports,
//! connects a `TonicWorkerClient` per server, and hands them to a
//! conductor-side `DistributedFunctionRunner`. The conductor then
//! dispatches requests and we inspect where they landed via a
//! counter attached to each server.
//!
//! These complement the single-worker smoke tests in
//! `src/tonic_client.rs` and the pure-conductor P2C tests in
//! `src/client.rs`. The value here is the interaction of the three
//! pieces at once: mode helpers + tonic transport + P2C routing.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{
            AtomicU64,
            Ordering,
        },
        Arc,
    },
};

use async_trait::async_trait;
use common::types::UdfType;
use convex_native::{
    distributed::ExecuteRequest,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    build_conductor_runner,
    FunctionExecutionServer,
    TonicWorkerClient,
    WorkerClient,
};
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

/// Spin up a worker bound to an ephemeral port, tagged with the
/// given version, and return `(addr, server_handle)`.
async fn spawn_worker(version: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native).with_registry_version(version);
    tokio::spawn(async move {
        Server::builder()
            .add_service(FunctionExecutionServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    // Give the OS a moment to accept the first connect.
    tokio::task::yield_now().await;
    addr
}

fn empty_request() -> ExecuteRequest {
    let obj: std::collections::BTreeMap<value::FieldName, ConvexValue> =
        std::collections::BTreeMap::new();
    ExecuteRequest {
        name: "absent".to_string(),
        namespace: TableNamespace::Global,
        args: ConvexObject::try_from(obj).unwrap(),
        timeout: None,
        min_registry_version: None,
    }
}

#[tokio::test]
async fn two_workers_both_reachable_dispatch_succeeds() {
    let a = spawn_worker("worker-a").await;
    let b = spawn_worker("worker-b").await;

    let runner = build_conductor_runner(&[format!("http://{a}"), format!("http://{b}")])
        .await
        .expect("build");

    assert_eq!(runner.worker_count(), 2);

    // Each dispatch completes over the wire, even though the handler
    // isn't registered on either side — that's expected: the wire
    // path works, the handler returns a runner-level error.
    for _ in 0..5 {
        let resp = runner
            .execute(empty_request(), UdfType::Action)
            .await
            .expect("dispatch");
        assert!(matches!(resp.result, Err(ref m) if m.contains("absent")));
    }
}

#[tokio::test]
async fn unreachable_worker_rejects_build() {
    let live = spawn_worker("live").await;
    // Port 1 is reserved; tonic `connect` fails fast.
    let result =
        build_conductor_runner(&[format!("http://{live}"), "http://127.0.0.1:1".to_string()]).await;
    let err = match result {
        Ok(_) => panic!("expected unreachable worker to fail build"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(msg.contains("127.0.0.1:1"), "error: {msg}");
}

#[tokio::test]
async fn rolling_update_floor_rejects_older_workers() {
    // Both workers tagged 0.1.0; conductor pins floor to 9.9.9.
    // Every dispatch should reject with FailedPrecondition at the
    // per-worker version gate, even after the built-in retry.
    let a = spawn_worker("0.1.0").await;
    let b = spawn_worker("0.1.0").await;
    let runner = build_conductor_runner(&[format!("http://{a}"), format!("http://{b}")])
        .await
        .expect("build")
        .with_min_registry_version("9.9.9");

    let status = runner
        .execute(empty_request(), UdfType::Action)
        .await
        .expect_err("floor should reject older workers");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn rolling_update_floor_accepts_compliant_workers() {
    // Workers tagged 2.0.0 meet the 2.0.0 floor.
    let a = spawn_worker("2.0.0").await;
    let b = spawn_worker("2.0.0").await;
    let runner = build_conductor_runner(&[format!("http://{a}"), format!("http://{b}")])
        .await
        .expect("build")
        .with_min_registry_version("2.0.0");

    let resp = runner
        .execute(empty_request(), UdfType::Action)
        .await
        .expect("dispatch");
    // The handler is missing (expected), but the wire path + floor
    // check both passed.
    assert!(matches!(resp.result, Err(ref m) if m.contains("absent")));
}

#[tokio::test]
async fn per_call_floor_overrides_runner_floor() {
    // Runner floor is 2.0.0 (workers are 2.0.0, so they'd pass).
    // Per-call floor is 9.9.9 — that should apply instead and reject.
    let a = spawn_worker("2.0.0").await;
    let b = spawn_worker("2.0.0").await;
    let runner = build_conductor_runner(&[format!("http://{a}"), format!("http://{b}")])
        .await
        .expect("build")
        .with_min_registry_version("2.0.0");

    let mut req = empty_request();
    req.min_registry_version = Some("9.9.9".to_string());

    let status = runner
        .execute(req, UdfType::Action)
        .await
        .expect_err("per-call floor should override runner floor");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

/// Regression: the conductor fails over from a worker that
/// returns `Unavailable`. Hard to simulate at the tonic layer
/// without a fault-injecting service; use a `WorkerClient` mock
/// wrapping a real `TonicWorkerClient` so the transport path
/// stays exercised but the first call returns `Unavailable`.
#[tokio::test]
async fn failover_from_unavailable_to_healthy_worker() {
    use tonic::Status;

    let healthy_addr = spawn_worker("healthy").await;
    let real = TonicWorkerClient::connect(format!("http://{healthy_addr}"))
        .await
        .expect("connect");

    // Worker #0 always returns Unavailable; worker #1 is the real
    // tonic client. Pin the chooser so #0 is primary.
    struct AlwaysUnavailable {
        calls: AtomicU64,
    }
    #[async_trait]
    impl convex_native_distributed::WorkerClient for AlwaysUnavailable {
        async fn execute(
            &self,
            _: ExecuteRequest,
            _: UdfType,
        ) -> Result<convex_native::distributed::ExecuteResponse, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(Status::unavailable("synthetic"))
        }

        async fn health(&self) -> Result<pb::function_execution::HealthResponse, Status> {
            unreachable!()
        }

        fn in_flight_estimate(&self) -> u64 {
            0
        }
    }

    let flaky = Arc::new(AlwaysUnavailable {
        calls: AtomicU64::new(0),
    });
    let runner =
        convex_native_distributed::DistributedFunctionRunner::new(vec![flaky.clone(), real])
            .unwrap()
            .with_chooser(Arc::new(convex_native_distributed::client::FixedChooser((
                0, 1,
            ))));

    let resp = runner
        .execute(empty_request(), UdfType::Action)
        .await
        .expect("dispatch should failover to healthy worker");
    assert!(matches!(resp.result, Err(ref m) if m.contains("absent")));
    assert_eq!(
        flaky.calls.load(Ordering::SeqCst),
        1,
        "flaky worker should have been tried exactly once"
    );
}

#[tokio::test]
async fn health_probes_report_per_worker_versions() {
    let a_addr = spawn_worker("v-a").await;
    let b_addr = spawn_worker("v-b").await;

    let a = TonicWorkerClient::connect(format!("http://{a_addr}"))
        .await
        .expect("a connect");
    let b = TonicWorkerClient::connect(format!("http://{b_addr}"))
        .await
        .expect("b connect");

    let a_health = a.health().await.expect("a health");
    let b_health = b.health().await.expect("b health");

    assert_eq!(a_health.registry_version, "v-a");
    assert_eq!(b_health.registry_version, "v-b");
    assert!(a_health.accepts_traffic);
    assert!(b_health.accepts_traffic);
}
