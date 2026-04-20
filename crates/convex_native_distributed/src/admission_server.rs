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

/// Decode a `pb::common::UdfType` enum-int into
/// `common::types::UdfType`. Unknown values fall back to `Query`
/// — the pool's `lookup_function` uses this for validation, not
/// dispatch, so the fallback just keeps an unknown-kind handler
/// addressable; the real dispatch path rejects mismatches.
fn udf_type_from_proto_i32(v: i32) -> common::types::UdfType {
    use pb::common::UdfType as P;
    match P::try_from(v).unwrap_or(P::Query) {
        P::Query => common::types::UdfType::Query,
        P::Mutation => common::types::UdfType::Mutation,
        P::Action => common::types::UdfType::Action,
        P::HttpAction => common::types::UdfType::HttpAction,
    }
}

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
    use anyhow::Context;
    use pb::worker_admission::worker_admission_service_server::WorkerAdmissionServiceServer;
    use tonic::transport::Server;
    let pool = Arc::new(WorkerPool::new());
    let service = WorkerAdmissionServer::new(pool.clone());
    let service_for_spawn = service.clone();
    let pool_for_return = pool.clone();
    // Bind synchronously so boot fails loud on port-in-use —
    // `Server::serve` previously swallowed the bind error into
    // a tracing message and the backend kept running with no
    // admission surface.
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("WorkerAdmissionService: bind {bind_addr} failed"))?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(WorkerAdmissionServiceServer::new(service_for_spawn))
            .serve_with_incoming(incoming)
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

/// Hook invoked on worker registration to install the worker's
/// advertised schema (the JSON blob off
/// `envelope.inventory.schema.schema_json`) into the backend's
/// own `Database<RT>`. Without this the backend can't commit
/// writes the worker reports in its `FinalTxSummary` — the
/// tablet ids referenced by the summary are unknown to the
/// backend's `IndexRegistry` and the commit bails with
/// "Missing `by_id` index for table …".
///
/// Implementations typically delegate to
/// `convex_native_backend::publish_schema`. The hook is fallible
/// and async; a failure is logged and the worker is still
/// admitted so operator workflows can still see/drain it, but
/// dispatch will fail until the schema mismatch is resolved.
#[async_trait::async_trait]
pub trait SchemaApplier: Send + Sync + 'static {
    async fn apply(&self, schema: common::schemas::DatabaseSchema) -> anyhow::Result<()>;
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
    /// Optional schema applier. When attached, each worker's
    /// advertised schema is mirrored into the backend's own
    /// Database<RT> so the tablet registry knows about the
    /// tables the worker writes to.
    schema_applier: Arc<Mutex<Option<Arc<dyn SchemaApplier>>>>,
}

impl WorkerAdmissionServer {
    pub fn new(pool: Arc<WorkerPool>) -> Self {
        Self {
            pool,
            outbound: Arc::new(Mutex::new(HashMap::new())),
            cron_driver: Arc::new(Mutex::new(None)),
            schema_applier: Arc::new(Mutex::new(None)),
        }
    }

    /// Attach a schema applier. See [`SchemaApplier`] for why this
    /// matters under the distributed topology.
    pub fn set_schema_applier(&self, applier: Arc<dyn SchemaApplier>) {
        *self.schema_applier.lock() = Some(applier);
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
    /// Broadcast a `RegistryFloorUpdate` to every admitted
    /// worker. Workers log the new floor but keep serving
    /// traffic — the backend is the authority on dispatch
    /// filtering, so this is informational only. Returns the
    /// number of workers the message was delivered to; failures
    /// are counted as "not delivered" and logged via tracing
    /// so one stale stream doesn't abort the broadcast.
    pub async fn broadcast_floor_update(&self, floor: Option<String>) -> usize {
        let senders: Vec<_> = {
            let outbound = self.outbound.lock();
            outbound.iter().map(|(id, tx)| (*id, tx.clone())).collect()
        };
        let msg = proto::BackendToWorker {
            msg: Some(proto::backend_to_worker::Msg::FloorUpdate(
                proto::RegistryFloorUpdate {
                    min_registry_version: floor.unwrap_or_default(),
                },
            )),
        };
        let mut delivered = 0usize;
        for (id, tx) in senders {
            match tx.send(Ok(msg.clone())).await {
                Ok(()) => delivered += 1,
                Err(e) => tracing::warn!(
                    target: "convex_admission",
                    worker = ?id,
                    "RegistryFloorUpdate delivery failed: {e}",
                ),
            }
        }
        delivered
    }

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

        // Inventory SHA-256 verification. The proto contract says
        // the backend rejects a mismatch as a hard error (so a
        // worker accidentally shipping the wrong inventory bytes
        // surfaces immediately rather than serving stale routes).
        // Empty SHA is allowed for legacy workers that didn't
        // populate the field.
        if !envelope.inventory_sha256.is_empty() {
            if let Some(inventory) = envelope.inventory.as_ref() {
                use prost::Message as _;
                use sha2::{
                    Digest,
                    Sha256,
                };
                let mut hasher = Sha256::new();
                hasher.update(inventory.encode_to_vec());
                let computed = hasher.finalize();
                if computed.as_slice() != envelope.inventory_sha256.as_slice() {
                    return Err(Status::failed_precondition(format!(
                        "inventory_sha256 mismatch: computed {} bytes, advertised {} bytes — \
                         worker likely shipped a stale inventory or hash",
                        computed.len(),
                        envelope.inventory_sha256.len(),
                    )));
                }
            }
        }

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
        // Parallel `name → (udf_type, is_internal)` map the pool's
        // `lookup_function` serves to the backend's HTTP / WebSocket
        // validation path. Unknown `UdfType` proto values fall back
        // to `Query` so validation doesn't silently drop a handler
        // just because a newer worker advertised a kind this
        // backend doesn't know about (it still won't dispatch — the
        // kind mismatch check fires at dispatch time).
        let function_specs: std::collections::BTreeMap<String, crate::pool::FunctionSpec> = envelope
            .inventory
            .as_ref()
            .map(|inv| {
                inv.functions
                    .iter()
                    .map(|f| {
                        (
                            f.name.clone(),
                            crate::pool::FunctionSpec {
                                udf_type: udf_type_from_proto_i32(f.udf_type),
                                is_internal: f.is_internal,
                            },
                        )
                    })
                    .collect()
            })
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
            function_specs,
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

        // Apply the worker's advertised schema into the backend's
        // own Database<RT>. Without this the backend can't commit
        // writes the worker reports (tablet ids referenced by the
        // FinalTxSummary are unknown to the backend's
        // IndexRegistry). Best-effort: a decode/apply failure is
        // logged; the worker stays admitted so operators can
        // still see/drain it through the admin surface.
        let schema_applier = self.schema_applier.lock().as_ref().cloned();
        if let Some(applier) = schema_applier {
            let maybe_schema = envelope
                .inventory
                .as_ref()
                .and_then(|inv| inv.schema.as_ref())
                .filter(|s| !s.schema_json.is_empty());
            if let Some(schema_proto) = maybe_schema {
                let decode = serde_json::from_slice::<common::schemas::json::DatabaseSchemaJson>(
                    &schema_proto.schema_json,
                )
                .map_err(anyhow::Error::from)
                .and_then(|json| common::schemas::DatabaseSchema::try_from(json));
                match decode {
                    Ok(schema) => {
                        if let Err(e) = applier.apply(schema).await {
                            tracing::warn!(
                                target: "convex_admission",
                                worker = ?worker_id,
                                "schema applier failed for worker; dispatch may fail with \
                                 'Missing `by_id` index': {e:#}",
                            );
                        } else {
                            tracing::info!(
                                target: "convex_admission",
                                worker = ?worker_id,
                                "applied worker-advertised schema to backend database",
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            target: "convex_admission",
                            worker = ?worker_id,
                            "failed to decode worker's schema_json: {e:#}",
                        );
                    },
                }
            }
        }

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
        // Seed the outbound stream with the pool's current floor
        // so a freshly-admitted worker knows the filtering
        // threshold without waiting for the next operator bump.
        // Empty (`None`) ⇒ no floor; the worker's drain listener
        // logs the empty update as informational.
        if let Some(floor) = self.pool.min_registry_version() {
            let seed = proto::BackendToWorker {
                msg: Some(proto::backend_to_worker::Msg::FloorUpdate(
                    proto::RegistryFloorUpdate {
                        min_registry_version: floor,
                    },
                )),
            };
            // `try_send` — channel was just created with capacity
            // 8 so this succeeds in practice; a failure here
            // isn't fatal.
            let _ = outbound_tx.try_send(Ok(seed));
        }
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

    #[tokio::test]
    async fn mismatched_inventory_sha_is_rejected() {
        // Worker sends a `RegistrationEnvelope` with an
        // `inventory_sha256` that doesn't match the inventory
        // bytes — the backend must reject with
        // FailedPrecondition rather than silently admitting a
        // worker advertising stale routes.
        let pool = Arc::new(WorkerPool::new());
        let admission_addr = spawn_admission_server(pool.clone()).await;

        let mut client = WorkerAdmissionServiceClient::connect(format!("http://{admission_addr}"))
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel::<proto::WorkerToBackend>(4);
        let envelope = proto::RegistrationEnvelope {
            execute_endpoint: "http://127.0.0.1:1".to_string(),
            registry_version: "1.0.0".to_string(),
            kind: proto::WorkerKind::NativeRust as i32,
            inventory: Some(proto::FunctionInventory {
                functions: vec![proto::FunctionRegistration {
                    name: "real".to_string(),
                    udf_type: 0,
                    is_internal: false,
                }],
                schema: None,
                routes: vec![],
                crons: vec![],
            }),
            // Garbage SHA — definitely doesn't match the
            // inventory above.
            inventory_sha256: vec![0xde, 0xad, 0xbe, 0xef],
        };
        tx.send(proto::WorkerToBackend {
            msg: Some(proto::worker_to_backend::Msg::Register(envelope)),
        })
        .await
        .unwrap();
        drop(tx);

        let err = client
            .register(tonic_request_from_channel(rx))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("inventory_sha256 mismatch"),
            "error message points at the SHA mismatch: {}",
            err.message(),
        );
        assert!(
            pool.is_empty(),
            "rejected worker must not appear in the pool"
        );
    }
}
