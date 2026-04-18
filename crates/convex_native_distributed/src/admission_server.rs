//! Substep 3.4 of `convex-native/STATUS.md` — backend-side
//! `WorkerAdmissionService` tonic implementation.
//!
//! Workers dial the backend's admission port and open a Register
//! stream. This module parses the first message
//! (`RegistrationEnvelope`), dials back to the worker's advertised
//! `execute_endpoint` to build a [`TonicWorkerClient`], admits
//! the worker to the shared [`WorkerPool`], and keeps the stream
//! alive so the backend can push drain/floor updates later (the
//! outbound side is a substep-3.8 refinement; for now the stream
//! just serves as the retirement trigger — when it closes the
//! pool drops the worker).
//!
//! The admission protocol is described in
//! `crates/pb/protos/worker_admission.proto`. Only the
//! RegistrationEnvelope processing and pool book-keeping live
//! here; everything dispatch-related lands in the
//! `WorkerPool::eligible_for`-driven `FunctionRunner` impl
//! (substep 3.6).

use std::{
    collections::HashMap,
    sync::Arc,
};

use parking_lot::Mutex;
use pb::worker_admission as proto;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    Response,
    Status,
    Streaming,
};

use crate::{
    pool::{
        WorkerEntry,
        WorkerId,
        WorkerKind,
        WorkerPool,
    },
    tonic_client::TonicWorkerClient,
};

/// Server-side implementation of the admission RPC.
///
/// Holds a shared `Arc<WorkerPool>` that every `register` call
/// admits into / retires from. The server is cheap to clone
/// (just `Arc`s) so a tonic `Server::builder()` can wrap it in a
/// `Routes`.
/// Spawn a tonic `WorkerAdmissionService` on `bind_addr`. The
/// returned pool is shared between the admission server (which
/// populates it) and the dispatcher side (`PoolFunctionRunner`,
/// substep 3.6) that reads from it. Substep 3.8 — callable from
/// `local_backend` without pulling in the `tonic` / `pb` crates
/// as direct dependencies.
///
/// The admission server runs until the tokio runtime drops it
/// (via shutdown); the returned pool stays alive for the whole
/// backend lifetime.
pub async fn spawn_admission_server(
    bind_addr: std::net::SocketAddr,
) -> anyhow::Result<Arc<WorkerPool>> {
    let (pool, _server) = spawn_admission_server_with_handle(bind_addr).await?;
    Ok(pool)
}

/// Substep 7.4 variant of `spawn_admission_server` that also
/// returns the `WorkerAdmissionServer` handle so callers can
/// wire it into the substep-7.2 admin HTTP surface
/// (`AdminState::with_admission(server)`) for
/// operator-triggered drains.
pub async fn spawn_admission_server_with_handle(
    bind_addr: std::net::SocketAddr,
) -> anyhow::Result<(Arc<WorkerPool>, WorkerAdmissionServer)> {
    use pb::worker_admission::worker_admission_service_server::WorkerAdmissionServiceServer;
    use tonic::transport::Server;
    let pool = Arc::new(WorkerPool::new());
    let service = WorkerAdmissionServer::new(pool.clone());
    let service_for_spawn = service.clone();
    let pool_for_return = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(WorkerAdmissionServiceServer::new(service_for_spawn))
            .serve(bind_addr)
            .await
        {
            // Logged here rather than bubbled because the spawn
            // is fire-and-forget; the caller has already returned
            // the pool handle and no longer has a place to
            // surface a late transport failure.
            tracing::error!("WorkerAdmissionService exited: {e}");
        }
    });
    Ok((pool_for_return, service))
}

#[derive(Clone)]
pub struct WorkerAdmissionServer {
    pool: Arc<WorkerPool>,
    /// Per-worker outbound channel handles. Populated on `register`,
    /// removed on retire. Lets an operator-facing drain/floor-
    /// update path push messages through the backend → worker
    /// side of the bidirectional stream.
    outbound: Arc<Mutex<HashMap<WorkerId, mpsc::Sender<Result<proto::BackendToWorker, Status>>>>>,
    /// Optional native cron driver. When attached, each admitted
    /// worker's cron registrations (drained off the envelope's
    /// `inventory.crons`) are installed on the driver so the
    /// backend drives worker-side schedules on behalf of the
    /// pool. Missing driver ⇒ crons from workers are ignored
    /// (the legacy shape).
    cron_driver: Arc<Mutex<Option<Arc<crate::cron_driver::NativeCronDriver>>>>,
}

impl WorkerAdmissionServer {
    pub fn new(pool: Arc<WorkerPool>) -> Self {
        Self {
            pool,
            outbound: Arc::new(Mutex::new(HashMap::new())),
            cron_driver: Arc::new(Mutex::new(None)),
        }
    }

    /// Attach a `NativeCronDriver`. Called from
    /// `local_backend::make_app` under the distributed topology
    /// so the backend drives worker-advertised crons.
    pub fn with_cron_driver(self, driver: Arc<crate::cron_driver::NativeCronDriver>) -> Self {
        *self.cron_driver.lock() = Some(driver);
        self
    }

    /// Interior-mutable variant of `with_cron_driver`. Set on one
    /// clone, reflected on every sibling clone because the slot
    /// is backed by an `Arc<Mutex<...>>`.
    pub fn set_cron_driver(&self, driver: Arc<crate::cron_driver::NativeCronDriver>) {
        *self.cron_driver.lock() = Some(driver);
    }

    pub fn pool(&self) -> &Arc<WorkerPool> {
        &self.pool
    }

    /// Send a `DrainNotice` to the given worker. The worker
    /// receives it on its `Register` stream, wakes its
    /// drain-signalled future (substep 3.5), and exits after
    /// finishing in-flight dispatches; on stream close the
    /// server's retirement task removes the worker from the
    /// pool. Substep 3.8 plumbing for operator-triggered retires.
    ///
    /// Returns `Ok(false)` when the worker isn't currently
    /// admitted (idempotent — double-drain is a no-op), `Ok(true)`
    /// when the notice was delivered, `Err` on transport failure
    /// (which usually means the worker is already disconnecting).
    pub async fn request_drain(
        &self,
        worker_id: WorkerId,
        reason: impl Into<String>,
    ) -> anyhow::Result<bool> {
        let sender = {
            let outbound = self.outbound.lock();
            outbound.get(&worker_id).cloned()
        };
        let Some(sender) = sender else {
            return Ok(false);
        };
        let notice = proto::DrainNotice {
            deadline_unix_nanos: 0,
            reason: reason.into(),
        };
        let msg = proto::BackendToWorker {
            msg: Some(proto::backend_to_worker::Msg::Drain(notice)),
        };
        sender
            .send(Ok(msg))
            .await
            .map_err(|e| anyhow::anyhow!("worker {worker_id:?} drain notice undeliverable: {e}"))?;
        Ok(true)
    }
}

#[tonic::async_trait]
impl proto::worker_admission_service_server::WorkerAdmissionService for WorkerAdmissionServer {
    type RegisterStream = ReceiverStream<Result<proto::BackendToWorker, Status>>;

    async fn register(
        &self,
        request: Request<Streaming<proto::WorkerToBackend>>,
    ) -> Result<Response<Self::RegisterStream>, Status> {
        let mut inbound = request.into_inner();

        // Substep 3.4: first message must be a RegistrationEnvelope.
        // Any other shape → close the stream with
        // `FailedPrecondition`. Subsequent messages (`WorkerStatus`)
        // arrive on the same stream and are ignored here — the
        // substep-3.6 dispatcher reads in-flight off the
        // `TonicWorkerClient` directly.
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::cancelled("admission stream closed before registration"))?;
        let envelope = match first.msg {
            Some(proto::worker_to_backend::Msg::Register(env)) => env,
            _ => {
                return Err(Status::failed_precondition(
                    "first message on WorkerAdmissionService::Register must be a \
                     RegistrationEnvelope",
                ))
            },
        };

        // Dial back to the worker's advertised endpoint to build
        // a transport client. This opens a second connection —
        // the admission stream (worker → backend) and the
        // dispatch channel (backend → worker) are separate by
        // design so dispatch latency isn't head-of-line-blocked
        // behind heartbeat traffic.
        let execute_endpoint = envelope.execute_endpoint.clone();
        let client: Arc<TonicWorkerClient> = TonicWorkerClient::connect(execute_endpoint.clone())
            .await
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "WorkerAdmissionServer: failed to dial worker's execute_endpoint {:?}: {e}",
                    execute_endpoint,
                ))
            })?;

        let functions: Vec<String> = envelope
            .inventory
            .as_ref()
            .map(|inv| inv.functions.iter().map(|f| f.name.clone()).collect())
            .unwrap_or_default();
        // HTTP routes the worker advertised — drained into the
        // pool's `by_http_route` index so the backend can dispatch
        // HTTP actions to a remote worker when the local
        // `HttpRouter` has no match.
        let http_routes: Vec<crate::pool::HttpRouteEntry> = envelope
            .inventory
            .as_ref()
            .map(|inv| {
                inv.routes
                    .iter()
                    .map(|r| crate::pool::HttpRouteEntry {
                        method: r.method.clone(),
                        path: r.path.clone(),
                        name: r.handler.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Substep 7.3 of `convex-native/STATUS.md` — log the
        // inventory diff against the pool's current active
        // version when the incoming worker introduces a new
        // `registry_version`. `diff_against_active_inventory`
        // returns `None` when the incoming version already is
        // the active one (nothing to log) or when the pool is
        // empty (no comparison possible).
        if let Some(diff) = self
            .pool
            .diff_against_active_inventory(&envelope.registry_version, &functions)
        {
            tracing::info!(
                target: "convex_admission",
                active = %diff.active_version,
                incoming = %diff.incoming_version,
                added = ?diff.added,
                removed = ?diff.removed,
                carried_over = diff.carried_over.len(),
                "inventory diff on new registry_version",
            );
        }
        let entry = WorkerEntry {
            client,
            registry_version: envelope.registry_version.clone(),
            functions,
            // Substep 6.1 of `convex-native/STATUS.md` — record
            // the worker's advertised runtime kind so operator
            // tooling can surface the Rust vs JS mix. Unknown
            // proto values fall back to `Unspecified`
            // (forward-compat for a future kind variant).
            kind: WorkerKind::from_proto_i32(envelope.kind),
            http_routes,
            status: parking_lot::Mutex::new(Default::default()),
        };
        let worker_id = self.pool.admit(entry);

        // Drain worker-advertised cron registrations into the
        // attached `NativeCronDriver`. Each admission is
        // idempotent on `(name, schedule, target, kind)` so
        // multiple workers advertising the same cron don't double
        // up; a rolling deploy that changes a schedule tears
        // down the old firing task and spawns a fresh one.
        let cron_driver = self.cron_driver.lock().as_ref().cloned();
        if let Some(driver) = cron_driver {
            let cron_jobs: Vec<crate::cron_driver::CronJob> = envelope
                .inventory
                .as_ref()
                .map(|inv| &inv.crons[..])
                .unwrap_or(&[])
                .iter()
                .filter_map(|c| {
                    // Default to mutation on empty for
                    // forward-compat with workers that predate
                    // the proto `kind` field.
                    let raw_kind = if c.kind.is_empty() {
                        "mutation"
                    } else {
                        c.kind.as_str()
                    };
                    let kind = match crate::cron_driver::CronTargetKind::from_str(raw_kind) {
                        Ok(k) => k,
                        Err(e) => {
                            tracing::warn!(
                                target: "convex_admission",
                                cron = %c.name,
                                "skipping cron with unknown kind: {e:#}",
                            );
                            return None;
                        },
                    };
                    Some(crate::cron_driver::CronJob {
                        name: c.name.clone(),
                        schedule_expr: c.schedule.clone(),
                        target: c.handler.clone(),
                        target_kind: kind,
                    })
                })
                .collect();
            if !cron_jobs.is_empty() {
                match driver.install(cron_jobs) {
                    Ok(installed) if !installed.is_empty() => {
                        tracing::info!(
                            target: "convex_admission",
                            count = installed.len(),
                            worker = ?worker_id,
                            "installed cron(s) from admission envelope",
                        );
                    },
                    Ok(_) => {},
                    Err(e) => tracing::warn!(
                        target: "convex_admission",
                        "cron install failed for worker {worker_id:?}: {e:#}",
                    ),
                }
            }
        }

        // Outbound stream — the backend side of the bidirectional
        // channel. Substep 3.8 plumbs `DrainNotice` through this
        // sender via `request_drain`; the handle is stored in
        // `self.outbound` so the operator-facing API can push
        // messages without re-entering this handler.
        let (outbound_tx, outbound_rx) = mpsc::channel::<Result<proto::BackendToWorker, Status>>(8);
        self.outbound.lock().insert(worker_id, outbound_tx.clone());

        // Spawn a retirement task that consumes status messages
        // and retires the worker when the inbound stream closes.
        // Holding `outbound_tx` here keeps the outbound stream
        // open for the worker's lifetime — the task drops it on
        // retire so the worker sees EOF cleanly.
        let pool = self.pool.clone();
        let outbound_map = self.outbound.clone();
        tokio::spawn(retirement_loop(
            pool,
            worker_id,
            inbound,
            outbound_tx,
            outbound_map,
        ));

        Ok(Response::new(ReceiverStream::new(outbound_rx)))
    }
}

/// Consume the inbound stream until it closes, then retire the
/// worker. Substep 3.4 doesn't act on `WorkerStatus` messages —
/// they're drained to keep the stream flowing. Phase-7 operator
/// tooling consumes them later.
async fn retirement_loop(
    pool: Arc<WorkerPool>,
    worker_id: WorkerId,
    mut inbound: Streaming<proto::WorkerToBackend>,
    outbound_tx: mpsc::Sender<Result<proto::BackendToWorker, Status>>,
    outbound_map: Arc<
        Mutex<HashMap<WorkerId, mpsc::Sender<Result<proto::BackendToWorker, Status>>>>,
    >,
) {
    loop {
        match inbound.message().await {
            Ok(Some(msg)) => {
                if let Some(proto::worker_to_backend::Msg::Status(status)) = msg.msg {
                    pool.update_worker_status(
                        worker_id,
                        crate::pool::WorkerLiveStatus {
                            in_flight: status.in_flight,
                            cpu_percent: status.cpu_percent,
                        },
                    );
                }
            },
            Ok(None) => break, // Clean close.
            Err(_) => break,   // Transport error → treat as close.
        }
    }
    // Drop the outbound sender so the worker sees EOF; retire
    // from the pool so dispatch stops routing to it. Also purge
    // the outbound map so a stale `request_drain` can't try to
    // push through a closed channel.
    drop(outbound_tx);
    outbound_map.lock().remove(&worker_id);
    let removed = pool.retire(worker_id);
    if !removed {
        // Retire was already called (via a DrainNotice path, or
        // another code path). Harmless — retire is idempotent.
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use pb::{
        function_execution::function_execution_service_server::FunctionExecutionServiceServer,
        worker_admission::{
            worker_admission_service_client::WorkerAdmissionServiceClient,
            worker_admission_service_server::WorkerAdmissionServiceServer,
        },
    };
    use tokio::net::TcpListener;
    use tokio_stream::{
        wrappers::TcpListenerStream,
        StreamExt,
    };
    use tonic::transport::Server;

    use super::*;
    use crate::{
        admission::collect_inventory,
        server::FunctionExecutionServer,
    };

    async fn spawn_worker_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let native = Arc::new(convex_native_core::NativeFunctionRunner::from_inventory().unwrap());
        let server = FunctionExecutionServer::new(native).with_registry_version("test-1.0.0");
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

    async fn spawn_admission_server(pool: Arc<WorkerPool>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = WorkerAdmissionServer::new(pool);
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerAdmissionServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        addr
    }

    #[tokio::test]
    async fn worker_registers_and_shows_up_in_pool() {
        // Full end-to-end admission: start a FunctionExecutionServer
        // on one port, a WorkerAdmissionServer on another, dial
        // the admission server, send a RegistrationEnvelope, and
        // assert the pool now has the worker.
        let worker_addr = spawn_worker_server().await;
        let pool = Arc::new(WorkerPool::new());
        let admission_addr = spawn_admission_server(pool.clone()).await;

        let mut client = WorkerAdmissionServiceClient::connect(format!("http://{admission_addr}"))
            .await
            .unwrap();

        let (inv, _hash) = collect_inventory().unwrap();
        let envelope = proto::RegistrationEnvelope {
            execute_endpoint: format!("http://{worker_addr}"),
            registry_version: "test-1.0.0".into(),
            kind: proto::WorkerKind::NativeRust as i32,
            inventory: Some(inv),
            inventory_sha256: vec![],
        };
        let (tx, rx) = mpsc::channel::<proto::WorkerToBackend>(4);
        tx.send(proto::WorkerToBackend {
            msg: Some(proto::worker_to_backend::Msg::Register(envelope)),
        })
        .await
        .unwrap();

        let resp = client
            .register(tonic_request_from_channel(rx))
            .await
            .expect("register accepted");
        let mut outbound = resp.into_inner();

        // Give the admission server a moment to admit.
        for _ in 0..20 {
            if pool.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(pool.len(), 1, "worker admitted");

        // Closing the client side retires the worker. Drop the
        // sender to close the inbound stream and wait for the
        // outbound to EOF.
        drop(tx);
        while outbound.next().await.is_some() {}
        for _ in 0..20 {
            if pool.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            pool.is_empty(),
            "worker retired when stream closed (churn-tolerance, substep 3.3 + 3.4)",
        );
    }

    #[tokio::test]
    async fn non_registration_first_message_is_rejected() {
        // Substep 3.4: first message must be a
        // RegistrationEnvelope. A WorkerStatus or an empty frame
        // should fail with FailedPrecondition.
        let pool = Arc::new(WorkerPool::new());
        let admission_addr = spawn_admission_server(pool.clone()).await;

        let mut client = WorkerAdmissionServiceClient::connect(format!("http://{admission_addr}"))
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel::<proto::WorkerToBackend>(4);
        // Send a WorkerStatus instead of a RegistrationEnvelope —
        // the server must reject.
        tx.send(proto::WorkerToBackend {
            msg: Some(proto::worker_to_backend::Msg::Status(proto::WorkerStatus {
                in_flight: 0,
                cpu_percent: 0,
                registry_version: "1.0.0".into(),
            })),
        })
        .await
        .unwrap();
        drop(tx);

        let err = client
            .register(tonic_request_from_channel(rx))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    fn tonic_request_from_channel(
        rx: mpsc::Receiver<proto::WorkerToBackend>,
    ) -> tonic::Request<impl tokio_stream::Stream<Item = proto::WorkerToBackend> + Send + 'static>
    {
        tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}
