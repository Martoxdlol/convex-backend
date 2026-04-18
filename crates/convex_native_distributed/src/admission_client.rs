//! Substep 3.5 of `convex-native/STATUS.md` — worker-side
//! registration loop.
//!
//! A worker process dials the backend's admission endpoint on
//! startup (`CONVEX_BACKEND_ENDPOINT=grpc://backend:5678`), opens
//! a `WorkerAdmissionService::Register` bidirectional stream,
//! sends one `RegistrationEnvelope` as the first message, then
//! keeps the stream open for the worker's lifetime — periodic
//! `WorkerStatus` updates go out; `DrainNotice` +
//! `RegistryFloorUpdate` messages come back.
//!
//! ## Lifecycle
//!
//! 1. Worker binds its `FunctionExecutionService` (unchanged —
//!    [`crate::mode::serve_worker_with_database`] handles that).
//! 2. Worker collects its inventory via
//!    [`crate::admission::collect_inventory`].
//! 3. Worker dials the backend admission service and calls
//!    [`WorkerRegistration::register`], which sends the envelope as the
//!    stream's first message.
//! 4. Worker task-spawns the status-push loop and the drain-signal listener.
//!    The registration handle exposes `drain_received()` so the embedding
//!    binary can wire a shutdown into its top-level `select!`.
//!
//! This module keeps the transport glue; the per-deployment
//! wiring (which `status` values to report, how often) lives on
//! top of it.

use std::{
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use pb::worker_admission::{
    self as proto,
    worker_admission_service_client::WorkerAdmissionServiceClient,
};
use tokio::sync::{
    mpsc,
    Notify,
};
use tokio_stream::{
    wrappers::ReceiverStream,
    StreamExt,
};
use tonic::transport::Channel;

use crate::admission::collect_inventory;

/// Handle to an active registration stream. Dropping it closes
/// the stream — the backend retires the worker on the next
/// admission loop tick.
///
/// The runner's `in_flight_counter` is injected so the status
/// pusher can surface accurate values without reaching back into
/// the tonic transport layer.
pub struct WorkerRegistration {
    /// Sends `WorkerToBackend` messages (`WorkerStatus` after the
    /// initial envelope) upstream.
    status_tx: mpsc::Sender<proto::WorkerToBackend>,
    /// Fired by the drain-signal listener task when the backend
    /// sends a `DrainNotice`. The binary awaits this in its
    /// top-level shutdown `select!`.
    drain_notify: Arc<Notify>,
    /// `registry_version` the worker reports on every
    /// `WorkerStatus`. Cached to avoid re-reading the envelope.
    registry_version: String,
}

impl WorkerRegistration {
    /// Open the `Register` stream, send the `RegistrationEnvelope`
    /// as the first message, and return a handle the worker
    /// binary uses to push status updates + await drain.
    ///
    /// `backend_endpoint` is typically `grpc://backend:5678`.
    /// `execute_endpoint` is the URL the backend should dial back
    /// to for function execution — usually derived from
    /// `CONVEX_WORKER_BIND_ADDR` plus the worker's externally-
    /// visible host.
    /// `registry_version` is the Cargo version of the deployer's
    /// worker crate; the backend enforces that workers sharing
    /// this string also share an inventory SHA.
    pub async fn register(
        backend_endpoint: impl Into<String>,
        execute_endpoint: String,
        registry_version: String,
    ) -> anyhow::Result<Self> {
        let (inventory, inventory_sha256) = collect_inventory()?;
        Self::register_inner(
            backend_endpoint,
            execute_endpoint,
            registry_version,
            crate::pool::WorkerKind::NativeRust,
            inventory,
            inventory_sha256.to_vec(),
        )
        .await
    }

    /// Phase 6.3 variant of [`register`] that lets a JS worker
    /// (or any non-native worker) advertise its kind explicitly.
    /// Used by `examples/js_worker.rs` and any deployer-built
    /// worker binary that wraps a non-Rust handler stack.
    pub async fn register_with_kind(
        backend_endpoint: impl Into<String>,
        execute_endpoint: String,
        registry_version: String,
        kind: crate::pool::WorkerKind,
        inventory: pb::worker_admission::FunctionInventory,
    ) -> anyhow::Result<Self> {
        // The hash is purely consistency-check; for a custom-
        // built inventory we hash the prost-serialised bytes the
        // same way `collect_inventory` does.
        use prost::Message as _;
        use sha2::{
            Digest,
            Sha256,
        };
        let mut hasher = Sha256::new();
        hasher.update(inventory.encode_to_vec());
        let inventory_sha256 = hasher.finalize().to_vec();
        Self::register_inner(
            backend_endpoint,
            execute_endpoint,
            registry_version,
            kind,
            inventory,
            inventory_sha256,
        )
        .await
    }

    async fn register_inner(
        backend_endpoint: impl Into<String>,
        execute_endpoint: String,
        registry_version: String,
        kind: crate::pool::WorkerKind,
        inventory: pb::worker_admission::FunctionInventory,
        inventory_sha256: Vec<u8>,
    ) -> anyhow::Result<Self> {
        let endpoint_str: String = backend_endpoint.into();
        let channel = Channel::from_shared(endpoint_str.clone())
            .context("parsing CONVEX_BACKEND_ENDPOINT")?
            .connect()
            .await
            .with_context(|| format!("connecting to backend admission at {endpoint_str:?}"))?;
        let mut client = WorkerAdmissionServiceClient::new(channel);
        let proto_kind = match kind {
            crate::pool::WorkerKind::Unspecified => proto::WorkerKind::Unspecified,
            crate::pool::WorkerKind::NativeRust => proto::WorkerKind::NativeRust,
            crate::pool::WorkerKind::Javascript => proto::WorkerKind::Javascript,
        };
        let envelope = proto::RegistrationEnvelope {
            execute_endpoint,
            registry_version: registry_version.clone(),
            kind: proto_kind as i32,
            inventory: Some(inventory),
            inventory_sha256,
        };

        // Build the outbound channel: first message is the
        // envelope, subsequent `WorkerStatus` messages come from
        // the status pusher the embedder feeds via
        // `push_status(...)`.
        let (status_tx, status_rx) = mpsc::channel::<proto::WorkerToBackend>(16);
        status_tx
            .send(proto::WorkerToBackend {
                msg: Some(proto::worker_to_backend::Msg::Register(envelope)),
            })
            .await
            .context("admission channel closed before envelope was sent")?;

        let outbound = ReceiverStream::new(status_rx);
        let inbound = client
            .register(tonic::Request::new(outbound))
            .await
            .context("admission server rejected registration")?
            .into_inner();

        // Spawn the drain listener — converts `DrainNotice` into
        // a `Notify` wakeup the binary awaits in its shutdown
        // path. `RegistryFloorUpdate` is informational right now;
        // logged for operator debugging via `tracing`.
        let drain_notify = Arc::new(Notify::new());
        tokio::spawn(drain_listener(inbound, drain_notify.clone()));

        Ok(Self {
            status_tx,
            drain_notify,
            registry_version,
        })
    }

    /// Push one status snapshot upstream. The worker binary's
    /// heartbeat loop calls this every N seconds (the frequency
    /// is binary-controlled — no internal timer here so tests
    /// can drive the loop deterministically).
    ///
    /// Returns `Err` when the stream has closed (either the
    /// backend drained us or the connection dropped). The worker
    /// binary treats that as "exit the heartbeat loop".
    pub async fn push_status(&self, in_flight: u64, cpu_percent: u32) -> anyhow::Result<()> {
        let status = proto::WorkerStatus {
            in_flight,
            cpu_percent,
            registry_version: self.registry_version.clone(),
        };
        self.status_tx
            .send(proto::WorkerToBackend {
                msg: Some(proto::worker_to_backend::Msg::Status(status)),
            })
            .await
            .context("admission stream closed; can't push WorkerStatus")?;
        Ok(())
    }

    /// Future that resolves when the backend sends a
    /// `DrainNotice`. The worker binary `select!`s this against
    /// its HTTP/gRPC server shutdowns so draining is coordinated.
    pub async fn drain_signaled(&self) {
        self.drain_notify.notified().await;
    }

    /// Convenience: poll the drain signal without awaiting.
    /// Lets a heartbeat loop check on each tick whether it
    /// should bail. Uses `Notify::notified` internally but
    /// wrapped in a `now_or_never` pattern via
    /// `FutureExt::poll`.
    pub fn drain_requested(&self) -> bool {
        use std::{
            pin::pin,
            task::{
                Context,
                Poll,
                Waker,
            },
        };
        let fut = self.drain_notify.notified();
        let mut fut = pin!(fut);
        let waker = Waker::noop();
        let mut ctx = Context::from_waker(&waker);
        matches!(fut.as_mut().poll(&mut ctx), Poll::Ready(()))
    }
}

async fn drain_listener(
    mut inbound: tonic::Streaming<proto::BackendToWorker>,
    drain_notify: Arc<Notify>,
) {
    while let Some(msg) = inbound.next().await {
        let Ok(msg) = msg else { break };
        match msg.msg {
            Some(proto::backend_to_worker::Msg::Drain(_)) => {
                drain_notify.notify_waiters();
                break;
            },
            Some(proto::backend_to_worker::Msg::FloorUpdate(_)) => {
                // Informational. Operator dashboards (Phase 7)
                // surface the floor; the worker itself doesn't
                // act on it — it keeps serving until the backend
                // sends a DrainNotice or the stream closes.
            },
            None => {
                // Empty frame — ignore.
            },
        }
    }
    // Stream closed without a drain notice — treat as "backend
    // disconnected"; wake the notifier so the binary exits
    // cleanly instead of hanging.
    drain_notify.notify_waiters();
}

/// Reasonable default for how often the worker pushes status
/// upstream. `register()` doesn't enforce it — this constant is
/// exported so deployer binaries can share the default.
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::Arc,
    };

    use pb::worker_admission::worker_admission_service_server::WorkerAdmissionServiceServer;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::{
        admission_server::WorkerAdmissionServer,
        pool::WorkerPool,
    };

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
        use pb::function_execution::function_execution_service_server as fes;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let native = Arc::new(convex_native_core::NativeFunctionRunner::from_inventory().unwrap());
        let server = crate::server::FunctionExecutionServer::new(native);
        tokio::spawn(async move {
            Server::builder()
                .add_service(fes::FunctionExecutionServiceServer::new(server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        addr
    }

    #[tokio::test]
    async fn worker_registers_and_push_status_flows_upstream() {
        let pool = Arc::new(WorkerPool::new());
        let admission_addr = spawn_admission(pool.clone()).await;
        let exec_addr = spawn_worker_exec().await;

        let reg = WorkerRegistration::register(
            format!("http://{admission_addr}"),
            format!("http://{exec_addr}"),
            "3.5-test".to_string(),
        )
        .await
        .expect("register");

        // Wait for the admission server to insert into the pool.
        for _ in 0..20 {
            if pool.len() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(pool.len(), 1, "worker visible in pool after register()");

        // Heartbeat push doesn't error while the stream is
        // healthy. The admission server drains status messages
        // without acting on them (substep 3.4).
        reg.push_status(3, 42).await.expect("push_status succeeds");

        // Drain hasn't been signalled yet.
        assert!(!reg.drain_requested(), "no drain signal yet");

        // Dropping the handle closes the stream — retire fires
        // on the admission server's retirement task.
        drop(reg);
        for _ in 0..20 {
            if pool.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            pool.is_empty(),
            "dropping WorkerRegistration retires the worker from the pool",
        );
    }
}
