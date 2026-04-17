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
}

impl AdminState {
    pub fn new(pool: Arc<WorkerPool>) -> Self {
        Self {
            pool,
            admission: None,
        }
    }

    pub fn with_admission(mut self, admission: WorkerAdmissionServer) -> Self {
        self.admission = Some(admission);
        self
    }
}

/// Build an axum router serving the admin surface against the
/// provided pool + admission server.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/pool", get(get_pool))
        .route("/admin/pool/floor", post(set_floor))
        .route("/admin/pool/kind_preference", post(set_kind_preference))
        .route("/admin/pool/drain", post(drain_worker))
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
        use convex_native::distributed::{
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
}
