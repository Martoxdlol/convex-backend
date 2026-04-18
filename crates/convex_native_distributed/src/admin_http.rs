//! Substep 7.2 of `convex-native/STATUS.md` — admin HTTP
//! surface for the dynamic worker pool.
//!
//! Exposes the Phase-3/6 primitives (pool introspection, floor
//! bump, kind preferences, drain trigger) through an axum
//! router. Mount with
//! `admin_http::router(pool, admission_server)` and
//! `serve(bind_addr)` alongside the public HTTP server — the
//! operator binds it to a separate port (e.g. `127.0.0.1:9090`)
//! behind whatever auth/network policy already guards the
//! backend's existing admin endpoints.
//!
//! ## Routes
//!
//! - `GET /admin/health` — lightweight status check: convex_native_core
//!   version, pool size, current floor, cron/admission wiring flags. Use for
//!   operator dashboards + external monitoring probes.
//! - `GET /admin/pool` — returns a `PoolSnapshot` as JSON.
//! - `POST /admin/pool/floor { "min_registry_version": "X.Y.Z" }` sets the
//!   pool-wide `min_registry_version` floor. Send `{ "min_registry_version":
//!   null }` to clear.
//! - `POST /admin/pool/kind_preference` `{ "function_name": "n", "kind":
//!   "native-rust" }` pins a per-function routing preference. `kind` is one of
//!   `"native-rust"`, `"javascript"`. Send `{ "function_name": "n", "kind":
//!   null }` to clear.
//! - `POST /admin/pool/drain { "worker_id": 42, "reason": "..." }` sends a
//!   `DrainNotice` to the specified worker.
//! - `POST /admin/pool/diff { "incoming_version": "X.Y.Z", "functions":
//!   ["get_user", "list_todos"] }` previews what a rolling-update bump would
//!   look like against the pool's current active version — returns `{
//!   "active_version", "incoming_version", "added", "removed", "carried_over"
//!   }` or `null` when the pool is empty / the incoming version already is the
//!   active one.
//! - `GET /admin/crons` — returns the live `NativeCronDriver` job list as JSON.
//!   Returns 501 when the driver isn't attached.
//! - `POST /admin/crons/remove { "name": "..." }` drops a cron from the firing
//!   schedule. Idempotent; `{"removed": true|false}` reports whether the name
//!   was actually present.
//!
//! The router is intentionally tiny — operator tooling lives
//! on top of it (Phase 7 CLI / dashboard work); it just
//! translates HTTP JSON into the corresponding pool methods.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{
        get,
        post,
    },
    Json,
    Router,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    admission_server::WorkerAdmissionServer,
    pool::{
        PoolSnapshot,
        WorkerId,
        WorkerKind,
        WorkerPool,
    },
};

/// Shared state the router hands to every handler. `admission`
/// is optional — admin deployments that don't want to expose
/// operator-triggered drain can pass `None` and the
/// `/admin/pool/drain` route will error with `501`.
#[derive(Clone)]
pub struct AdminState {
    pub pool: Arc<WorkerPool>,
    pub admission: Option<WorkerAdmissionServer>,
    /// Optional native cron driver. When set, the
    /// `/admin/crons` routes return live data and the
    /// `/admin/crons/remove` route can drop a cron from the
    /// firing schedule. Unset ⇒ both routes return 501.
    pub cron_driver: Option<Arc<crate::cron_driver::NativeCronDriver>>,
}

impl AdminState {
    pub fn new(pool: Arc<WorkerPool>) -> Self {
        Self {
            pool,
            admission: None,
            cron_driver: None,
        }
    }

    pub fn with_admission(mut self, admission: WorkerAdmissionServer) -> Self {
        self.admission = Some(admission);
        self
    }

    pub fn with_cron_driver(
        mut self,
        cron_driver: Arc<crate::cron_driver::NativeCronDriver>,
    ) -> Self {
        self.cron_driver = Some(cron_driver);
        self
    }
}

/// Build an axum router serving the admin surface against the
/// provided pool + admission server.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/health", get(get_health))
        .route("/admin/pool", get(get_pool))
        .route("/admin/pool/floor", post(set_floor))
        .route("/admin/pool/kind_preference", post(set_kind_preference))
        .route("/admin/pool/drain", post(drain_worker))
        .route("/admin/pool/diff", post(diff_inventory))
        .route("/admin/crons", get(get_crons))
        .route("/admin/crons/remove", post(remove_cron))
        .with_state(state)
}

/// Substep 7.4 of `convex-native/STATUS.md` — spawn the admin
/// router on `bind_addr` as a background task. `local_backend`
/// calls this when `CONVEX_ADMIN_BIND_ADDR` is set.
///
/// Runs forever until the tokio runtime shuts down; transport
/// failures are logged to stderr (same pattern as
/// `spawn_admission_server`).
pub async fn spawn_admin_server(
    bind_addr: std::net::SocketAddr,
    state: AdminState,
) -> anyhow::Result<()> {
    let app = router(state);
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("AdminHttpServer: bind {bind_addr} failed: {e}");
                return;
            },
        };
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("AdminHttpServer exited: {e}");
        }
    });
    Ok(())
}

async fn get_pool(State(state): State<AdminState>) -> Json<PoolSnapshot> {
    Json(state.pool.snapshot())
}

#[derive(Serialize)]
struct HealthResponse {
    /// `convex_native_core`'s crate version — the backend/worker
    /// wire-compat baseline.
    convex_native_version: &'static str,
    /// Total workers currently in the pool.
    pool_size: usize,
    /// Current `min_registry_version` floor, or `null` when
    /// unset.
    min_registry_version: Option<String>,
    /// Whether a `NativeCronDriver` is installed.
    cron_driver_attached: bool,
    /// Cron job count when a driver is attached, else 0.
    cron_jobs: usize,
    /// Whether a `WorkerAdmissionServer` is attached (i.e.
    /// operator-initiated drain is available).
    admission_attached: bool,
}

async fn get_health(State(state): State<AdminState>) -> Json<HealthResponse> {
    let cron_jobs = state
        .cron_driver
        .as_ref()
        .map(|d| d.jobs().len())
        .unwrap_or(0);
    Json(HealthResponse {
        convex_native_version: convex_native_core::VERSION,
        pool_size: state.pool.len(),
        min_registry_version: state.pool.min_registry_version(),
        cron_driver_attached: state.cron_driver.is_some(),
        cron_jobs,
        admission_attached: state.admission.is_some(),
    })
}

#[derive(Deserialize)]
struct SetFloorRequest {
    /// Null clears the floor.
    min_registry_version: Option<String>,
}

#[derive(Serialize)]
struct FloorAck {
    min_registry_version: Option<String>,
}

async fn set_floor(
    State(state): State<AdminState>,
    Json(req): Json<SetFloorRequest>,
) -> Json<FloorAck> {
    state
        .pool
        .set_min_registry_version(req.min_registry_version.clone());
    // Notify every admitted worker so operator dashboards + log
    // aggregators see the floor change immediately rather than
    // waiting for the next heartbeat to come up blank.
    if let Some(admission) = state.admission.as_ref() {
        let delivered = admission
            .broadcast_floor_update(req.min_registry_version.clone())
            .await;
        tracing::info!(
            target: "convex_admission",
            floor = ?req.min_registry_version,
            delivered,
            "broadcast RegistryFloorUpdate",
        );
    }
    Json(FloorAck {
        min_registry_version: state.pool.min_registry_version(),
    })
}

#[derive(Deserialize)]
struct SetKindPreferenceRequest {
    function_name: String,
    /// Null clears the per-function preference.
    kind: Option<String>,
}

async fn set_kind_preference(
    State(state): State<AdminState>,
    Json(req): Json<SetKindPreferenceRequest>,
) -> Result<Json<PoolSnapshot>, (StatusCode, String)> {
    match req.kind.as_deref() {
        None => state.pool.clear_kind_preference(&req.function_name),
        Some(s) => {
            let kind = match s {
                "native-rust" => WorkerKind::NativeRust,
                "javascript" => WorkerKind::Javascript,
                "unspecified" => WorkerKind::Unspecified,
                other => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "unknown kind {other:?}; accepted values: \"native-rust\", \
                             \"javascript\", \"unspecified\", or null to clear"
                        ),
                    ))
                },
            };
            state.pool.set_kind_preference(req.function_name, kind);
        },
    }
    Ok(Json(state.pool.snapshot()))
}

#[derive(Deserialize)]
struct DrainRequest {
    worker_id: u64,
    #[serde(default = "default_drain_reason")]
    reason: String,
}

fn default_drain_reason() -> String {
    "operator-triggered drain".to_string()
}

#[derive(Serialize)]
struct DrainAck {
    worker_id: u64,
    delivered: bool,
}

async fn drain_worker(
    State(state): State<AdminState>,
    Json(req): Json<DrainRequest>,
) -> Result<Json<DrainAck>, (StatusCode, String)> {
    let Some(admission) = state.admission.as_ref() else {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            "AdminState built without a WorkerAdmissionServer — drain can't be triggered from \
             this admin surface. Pass `.with_admission(server)` when building the AdminState."
                .to_string(),
        ));
    };
    let id = WorkerId(req.worker_id);
    let delivered = admission.request_drain(id, req.reason).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("drain failed: {e}"),
        )
    })?;
    Ok(Json(DrainAck {
        worker_id: req.worker_id,
        delivered,
    }))
}

#[derive(Serialize)]
struct CronsList {
    jobs: Vec<CronJobView>,
}

#[derive(Serialize)]
struct CronJobView {
    name: String,
    schedule: String,
    target: String,
    kind: String,
}

async fn get_crons(
    State(state): State<AdminState>,
) -> Result<Json<CronsList>, (StatusCode, String)> {
    let driver = state.cron_driver.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_IMPLEMENTED,
            "AdminState built without a NativeCronDriver — install one via \
             `.with_cron_driver(driver)` to enable /admin/crons."
                .to_string(),
        )
    })?;
    let jobs = driver
        .jobs()
        .into_iter()
        .map(|j| CronJobView {
            name: j.name,
            schedule: j.schedule_expr,
            target: j.target,
            kind: match j.target_kind {
                crate::cron_driver::CronTargetKind::Mutation => "mutation".to_string(),
                crate::cron_driver::CronTargetKind::Action => "action".to_string(),
            },
        })
        .collect();
    Ok(Json(CronsList { jobs }))
}

#[derive(Deserialize)]
struct RemoveCronRequest {
    name: String,
}

#[derive(Serialize)]
struct RemoveCronAck {
    name: String,
    removed: bool,
}

#[derive(Deserialize)]
struct DiffRequest {
    /// Hypothetical `registry_version` to diff against the
    /// pool's current active version.
    incoming_version: String,
    /// Function names the hypothetical version would advertise.
    functions: Vec<String>,
}

async fn diff_inventory(
    State(state): State<AdminState>,
    Json(req): Json<DiffRequest>,
) -> Result<Json<Option<crate::pool::InventoryDiff>>, (StatusCode, String)> {
    let diff = state
        .pool
        .diff_against_active_inventory(&req.incoming_version, &req.functions);
    Ok(Json(diff))
}

async fn remove_cron(
    State(state): State<AdminState>,
    Json(req): Json<RemoveCronRequest>,
) -> Result<Json<RemoveCronAck>, (StatusCode, String)> {
    let driver = state.cron_driver.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_IMPLEMENTED,
            "AdminState built without a NativeCronDriver — install one via \
             `.with_cron_driver(driver)` to enable /admin/crons/remove."
                .to_string(),
        )
    })?;
    let was_present = driver.jobs().iter().any(|j| j.name == req.name);
    driver.remove(&req.name);
    Ok(Json(RemoveCronAck {
        name: req.name,
        removed: was_present,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::Request,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::pool::{
        WorkerEntry,
        WorkerKind,
        WorkerPool,
    };

    fn stub_pool_with_one_worker() -> Arc<WorkerPool> {
        use std::sync::atomic::AtomicU64;

        use async_trait::async_trait;
        use common::types::UdfType;
        use convex_native_core::distributed::{
            ExecuteRequest,
            ExecuteResponse,
        };
        use pb::function_execution as proto;
        use tonic::Status;

        use crate::client::WorkerClient;

        struct StubClient;

        #[async_trait]
        impl WorkerClient for StubClient {
            async fn execute(
                &self,
                _req: ExecuteRequest,
                _udf_type: UdfType,
            ) -> Result<ExecuteResponse, Status> {
                Err(Status::unimplemented("stub"))
            }

            async fn health(&self) -> Result<proto::HealthResponse, Status> {
                Err(Status::unimplemented("stub"))
            }

            fn in_flight_estimate(&self) -> u64 {
                0
            }

            fn label(&self) -> &str {
                "stub"
            }
        }
        let _ = AtomicU64::new(0);

        let pool = Arc::new(WorkerPool::new());
        pool.admit(WorkerEntry {
            client: Arc::new(StubClient),
            registry_version: "1.0.0".to_string(),
            functions: vec!["get".to_string()],
            kind: WorkerKind::NativeRust,
            http_routes: Vec::new(),
            status: parking_lot::Mutex::new(Default::default()),
        });
        pool
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn get_health_returns_status_shape() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["pool_size"], 1);
        assert_eq!(json["cron_driver_attached"], false);
        assert_eq!(json["admission_attached"], false);
        assert!(
            json["convex_native_version"].as_str().is_some(),
            "convex_native_version exposed: {json}",
        );
    }

    #[tokio::test]
    async fn get_pool_returns_snapshot_json() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/pool")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["total"], 1);
        assert_eq!(json["by_kind"]["native-rust"], 1);
    }

    #[tokio::test]
    async fn set_floor_updates_pool_state() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool.clone()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/pool/floor")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"min_registry_version":"2.0.0"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(pool.min_registry_version().as_deref(), Some("2.0.0"));
    }

    #[tokio::test]
    async fn set_floor_null_clears_the_floor() {
        let pool = stub_pool_with_one_worker();
        pool.set_min_registry_version(Some("1.5.0".to_string()));
        let app = router(AdminState::new(pool.clone()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/pool/floor")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"min_registry_version":null}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(pool.min_registry_version().is_none());
    }

    #[tokio::test]
    async fn set_kind_preference_routes_to_native_rust() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool.clone()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/pool/kind_preference")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"function_name":"compute","kind":"native-rust"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let prefs = pool.kind_preferences();
        assert_eq!(prefs.get("compute"), Some(&WorkerKind::NativeRust));
    }

    #[tokio::test]
    async fn set_kind_preference_rejects_unknown_kind() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/pool/kind_preference")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"function_name":"x","kind":"wasm"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn drain_without_admission_server_returns_not_implemented() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/pool/drain")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"worker_id":0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn diff_inventory_returns_added_removed() {
        use crate::pool::{
            HttpRouteEntry as _Unused,
            WorkerEntry,
            WorkerKind,
            WorkerPool,
        };
        let _ = _Unused {
            method: String::new(),
            path: String::new(),
            name: String::new(),
        };
        use std::sync::atomic::AtomicU64;

        use async_trait::async_trait;
        use common::types::UdfType;
        use convex_native_core::distributed::{
            ExecuteRequest,
            ExecuteResponse,
        };
        use pb::function_execution as proto;

        struct Stub;
        #[async_trait]
        impl crate::client::WorkerClient for Stub {
            async fn execute(
                &self,
                _req: ExecuteRequest,
                _udf_type: UdfType,
            ) -> Result<ExecuteResponse, tonic::Status> {
                Err(tonic::Status::unimplemented("stub"))
            }

            async fn health(&self) -> Result<proto::HealthResponse, tonic::Status> {
                Err(tonic::Status::unimplemented("stub"))
            }

            fn in_flight_estimate(&self) -> u64 {
                0
            }

            fn label(&self) -> &str {
                "stub"
            }
        }
        let _ = AtomicU64::new(0);

        let pool = Arc::new(WorkerPool::new());
        pool.admit(WorkerEntry {
            client: Arc::new(Stub),
            registry_version: "1.0.0".to_string(),
            functions: vec!["get".to_string(), "list".to_string()],
            kind: WorkerKind::NativeRust,
            http_routes: Vec::new(),
            status: parking_lot::Mutex::new(Default::default()),
        });
        let app = router(AdminState::new(pool));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/pool/diff")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"incoming_version":"2.0.0","functions":["get","new_fn"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["active_version"], "1.0.0");
        assert_eq!(json["incoming_version"], "2.0.0");
        assert_eq!(json["added"].as_array().unwrap().len(), 1);
        assert_eq!(json["added"][0], "new_fn");
        assert_eq!(json["removed"].as_array().unwrap().len(), 1);
        assert_eq!(json["removed"][0], "list");
        assert_eq!(json["carried_over"].as_array().unwrap().len(), 1);
        assert_eq!(json["carried_over"][0], "get");
    }

    #[tokio::test]
    async fn get_crons_without_driver_returns_not_implemented() {
        let pool = stub_pool_with_one_worker();
        let app = router(AdminState::new(pool));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/crons")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn get_crons_with_driver_lists_jobs() {
        use std::sync::atomic::AtomicU64;

        use async_trait::async_trait;

        struct StubDispatcher;
        #[async_trait]
        impl crate::cron_driver::CronDispatcher for StubDispatcher {
            async fn fire(
                &self,
                _name: &str,
                _kind: crate::cron_driver::CronTargetKind,
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }
        let _ = AtomicU64::new(0);

        let pool = stub_pool_with_one_worker();
        let driver = Arc::new(crate::cron_driver::NativeCronDriver::new(Arc::new(
            StubDispatcher,
        )));
        driver
            .install(vec![crate::cron_driver::CronJob {
                name: "every_hour".to_string(),
                schedule_expr: "0 * * * *".to_string(),
                target: "do_work".to_string(),
                target_kind: crate::cron_driver::CronTargetKind::Mutation,
            }])
            .unwrap();
        let state = AdminState::new(pool).with_cron_driver(driver.clone());
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/admin/crons")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(json["jobs"][0]["name"], "every_hour");
        assert_eq!(json["jobs"][0]["schedule"], "0 * * * *");
        assert_eq!(json["jobs"][0]["target"], "do_work");
        assert_eq!(json["jobs"][0]["kind"], "mutation");
        // Drop the cron via the remove route.
        let app = router(AdminState::new(stub_pool_with_one_worker()).with_cron_driver(driver));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/crons/remove")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"every_hour"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["removed"], true);
    }
}
