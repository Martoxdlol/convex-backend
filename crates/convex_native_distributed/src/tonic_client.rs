//! `TonicWorkerClient` — real gRPC transport for the dispatcher's
//! `WorkerClient` trait.
//!
//! Wraps a generated
//! `pb::function_execution::function_execution_service_client::FunctionExecutionServiceClient<Channel>`
//! (cloned per call, since tonic clients are `Clone` + share a
//! `Channel`), tracks in-flight requests locally via an
//! `AtomicU64` so the P2C load balancer in `client.rs` can pick
//! the less busy worker without a network round-trip.

use std::sync::{
    atomic::{
        AtomicU64,
        Ordering,
    },
    Arc,
};

use async_trait::async_trait;
use common::types::UdfType;
use convex_native::distributed::{
    ExecuteRequest,
    ExecuteResponse,
};
use pb::function_execution::{
    self as proto,
    function_execution_service_client::FunctionExecutionServiceClient,
};
use tonic::{
    transport::Channel,
    Request,
    Status,
};

use crate::{
    client::WorkerClient,
    conversions,
};

/// Real gRPC-backed worker client.
///
/// Constructed via [`TonicWorkerClient::connect`] (opens a new
/// `Channel` to the endpoint) or [`TonicWorkerClient::from_channel`]
/// (reuses an existing one — useful when the dispatcher multiplexes
/// over a shared connection pool).
pub struct TonicWorkerClient {
    grpc: FunctionExecutionServiceClient<Channel>,
    in_flight: Arc<AtomicU64>,
    endpoint: String,
}

impl TonicWorkerClient {
    /// Connect to a worker over a freshly-established channel.
    /// Returns an `Arc<Self>` so callers can plug it straight into
    /// `DistributedFunctionRunner::new(vec![client])`.
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Arc<Self>> {
        let endpoint = endpoint.into();
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        Ok(Self::from_channel(channel, endpoint))
    }

    /// Wrap an existing `Channel`. Callers that multiplex several
    /// services over one channel should prefer this.
    pub fn from_channel(channel: Channel, endpoint: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            grpc: FunctionExecutionServiceClient::new(channel),
            in_flight: Arc::new(AtomicU64::new(0)),
            endpoint: endpoint.into(),
        })
    }

    /// Reportable endpoint string (the URL the client was created
    /// against). Used by metrics and logs; not inspected by the
    /// dispatch path.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

struct InFlightGuard<'a>(&'a AtomicU64);

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl WorkerClient for TonicWorkerClient {
    async fn execute(
        &self,
        req: ExecuteRequest,
        udf_type: UdfType,
    ) -> Result<ExecuteResponse, Status> {
        let proto_req = conversions::to_proto_request(&req, udf_type)
            .map_err(|e| Status::invalid_argument(format!("encode ExecuteRequest: {e}")))?;

        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let _guard = InFlightGuard(&self.in_flight);

        // `FunctionExecutionServiceClient` requires `&mut self` for
        // `execute`, so clone the cheap handle (which shares the
        // underlying Channel) to keep our outer `&self` method
        // signature.
        let mut client = self.grpc.clone();
        let resp = client.execute(Request::new(proto_req)).await?;
        let proto_resp = resp.into_inner();
        conversions::from_proto_response(&proto_resp)
            .map_err(|e| Status::internal(format!("decode ExecuteResponse: {e}")))
    }

    async fn health(&self) -> Result<proto::HealthResponse, Status> {
        let mut client = self.grpc.clone();
        let resp = client.health(Request::new(proto::HealthRequest {})).await?;
        Ok(resp.into_inner())
    }

    fn in_flight_estimate(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }

    fn label(&self) -> &str {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::SocketAddr,
        sync::Arc,
    };

    use convex_native::NativeFunctionRunner;
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

    use super::*;
    use crate::{
        server::FunctionExecutionServer,
        DistributedFunctionRunner,
    };

    /// Spin up a real gRPC server bound to an ephemeral port and
    /// return its address. The server owns an empty NativeFunctionRunner
    /// (no registrations), so `Health` succeeds but `Execute` for
    /// any name reports a runner-level error.
    async fn spawn_test_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
        let server = FunctionExecutionServer::new(native).with_registry_version("test-1.0.0");
        tokio::spawn(async move {
            Server::builder()
                .add_service(FunctionExecutionServiceServer::new(server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        // Give the server a moment to start listening — tonic's
        // `connect()` will retry its own handshake, but the OS
        // accept queue needs to be up first.
        tokio::task::yield_now().await;
        addr
    }

    fn sample_request() -> ExecuteRequest {
        let obj: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        ExecuteRequest {
            name: "does_not_exist".to_string(),
            namespace: TableNamespace::Global,
            args: ConvexObject::try_from(obj).unwrap(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
        }
    }

    #[tokio::test]
    async fn health_round_trips_over_real_grpc() {
        let addr = spawn_test_server().await;
        let client = TonicWorkerClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let health = client.health().await.expect("health");
        assert_eq!(health.registry_version, "test-1.0.0");
        assert!(health.accepts_traffic);
        assert_eq!(health.registered_functions, 0);
    }

    #[tokio::test]
    async fn execute_round_trips_over_real_grpc() {
        // Server has no registrations, so the action handler call
        // will surface a runner error via the ExecuteResponse.result
        // (handler-level error, not a gRPC Status).
        let addr = spawn_test_server().await;
        let client = TonicWorkerClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let resp = client
            .execute(sample_request(), UdfType::Action)
            .await
            .expect("execute");
        assert!(
            matches!(resp.result, Err(ref m) if m.contains("does_not_exist")),
            "expected runner-level error, got {:?}",
            resp.result,
        );
    }

    #[tokio::test]
    async fn distributed_runner_routes_over_real_grpc() {
        // Full end-to-end: dispatcher P2C client → TonicWorkerClient
        // → tonic server → FunctionExecutionServer → NativeFunctionRunner.
        let addr = spawn_test_server().await;
        let worker = TonicWorkerClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let runner = DistributedFunctionRunner::new(vec![worker]).unwrap();
        let resp = runner
            .execute(sample_request(), UdfType::Action)
            .await
            .expect("dispatch");
        assert!(matches!(resp.result, Err(ref m) if m.contains("does_not_exist")));
    }

    #[tokio::test]
    async fn query_branch_returns_unimplemented_over_grpc() {
        let addr = spawn_test_server().await;
        let client = TonicWorkerClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let err = client
            .execute(sample_request(), UdfType::Query)
            .await
            .expect_err("query should be unimplemented today");
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn in_flight_tracks_around_call() {
        // Not a race-free test — we just assert it's back to 0 after
        // the call drains.
        let addr = spawn_test_server().await;
        let client = TonicWorkerClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        assert_eq!(client.in_flight_estimate(), 0);
        let _ = client.health().await;
        assert_eq!(client.in_flight_estimate(), 0);
        let _ = client.execute(sample_request(), UdfType::Action).await;
        assert_eq!(client.in_flight_estimate(), 0);
    }
}
