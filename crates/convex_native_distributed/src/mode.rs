//! Binary-level mode switching. Phase 3.5 of
//! `convex-native/IMPLEMENTATION_PLAN.md`.
//!
//! A process embedding `convex_native` reads `CONVEX_MODE` on startup
//! and uses these helpers to parse + validate the deployment topology
//! before wiring up a runner or starting a gRPC server. The actual
//! binary glue (spawn a `tonic::transport::Server` in worker mode;
//! build a `DistributedFunctionRunner` in conductor mode) lives in
//! `examples/worker.rs` and `examples/conductor.rs` of this crate —
//! this module just decodes the env vars and assembles the pieces.
//! `convex-local-backend` also consumes [`read_mode_from_env`] to
//! refuse boot on any `CONVEX_MODE` other than `Standalone`.
//!
//! ## Env vars
//!
//! - `CONVEX_MODE`: `standalone` (default) | `conductor` | `worker`.
//!   Case-insensitive; whitespace stripped; unknown → `Standalone`.
//! - `CONVEX_WORKER_ENDPOINTS` (conductor mode): comma-separated list
//!   of gRPC URLs, e.g. `"http://host-a:4567,http://host-b:4567"`.
//!   Empty list fails validation (conductors need workers).
//! - `CONVEX_WORKER_BIND_ADDR` (worker mode): the `host:port` the worker should
//!   bind its gRPC server to. Defaults to `0.0.0.0:4567` when unset.

use std::{
    net::SocketAddr,
    sync::Arc,
};

use convex_native::{
    distributed::ConvexMode,
    NativeFunctionRunner,
    Rt,
};
use database::Database;
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use tonic::transport::Server;

use crate::{
    server::FunctionExecutionServer,
    DistributedFunctionRunner,
    TonicWorkerClient,
    WorkerClient,
};

/// Default bind address when `CONVEX_WORKER_BIND_ADDR` is unset.
pub const DEFAULT_WORKER_BIND_ADDR: &str = "0.0.0.0:4567";

/// Read `CONVEX_MODE` from the environment. Absent or unknown values
/// default to `Standalone`.
pub fn read_mode_from_env() -> ConvexMode {
    ConvexMode::from_env_str(&std::env::var("CONVEX_MODE").unwrap_or_default())
}

/// Parse a comma-separated list of worker endpoints. Empty strings
/// after trimming are skipped; the final list is required to be
/// non-empty (conductors must know of at least one worker).
pub fn parse_worker_endpoints(raw: &str) -> anyhow::Result<Vec<String>> {
    let endpoints: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if endpoints.is_empty() {
        anyhow::bail!(
            "CONVEX_WORKER_ENDPOINTS is empty — conductor mode needs at least one worker URL, \
             comma-separated"
        );
    }
    Ok(endpoints)
}

/// Read the conductor's worker list from `CONVEX_WORKER_ENDPOINTS`.
pub fn read_worker_endpoints_from_env() -> anyhow::Result<Vec<String>> {
    let raw = std::env::var("CONVEX_WORKER_ENDPOINTS").map_err(|_| {
        anyhow::anyhow!(
            "CONVEX_WORKER_ENDPOINTS must be set in conductor mode (comma-separated gRPC URLs)"
        )
    })?;
    parse_worker_endpoints(&raw)
}

/// Read the worker's bind address from `CONVEX_WORKER_BIND_ADDR`,
/// falling back to [`DEFAULT_WORKER_BIND_ADDR`].
pub fn read_worker_bind_addr_from_env() -> anyhow::Result<SocketAddr> {
    let raw = std::env::var("CONVEX_WORKER_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_WORKER_BIND_ADDR.into());
    raw.parse().map_err(|e| {
        anyhow::anyhow!("CONVEX_WORKER_BIND_ADDR={raw:?}: invalid socket address: {e}")
    })
}

/// Build (but do not start) a tonic `Server` configured to serve the
/// `FunctionExecutionService` against the given native runner. The
/// caller picks the transport — `serve(addr)`, `serve_with_incoming`,
/// `serve_with_shutdown`, etc. — so the same configuration can drive
/// both production and test servers.
pub fn build_worker_server(
    native: Arc<NativeFunctionRunner>,
) -> (
    Server,
    FunctionExecutionServiceServer<FunctionExecutionServer>,
) {
    let server = FunctionExecutionServer::new(native);
    (
        Server::builder(),
        FunctionExecutionServiceServer::new(server),
    )
}

/// Serve `FunctionExecutionService` on `addr`, wired to the given
/// native runner and a worker-local `Database<Rt>`. Blocks until the
/// tonic server exits (error or process shutdown).
///
/// This is the "consumer" variant of [`build_worker_server`] —
/// callers that just want "bind, attach the database, run forever"
/// don't need to pull `tonic` or `pb` as direct dependencies.
///
/// Used by `convex-local-backend` under `CONVEX_MODE=worker` to
/// expose native functions over gRPC without duplicating the tonic
/// wiring. Callers that need coordinated shutdown (e.g. to drain on
/// the same signal the HTTP server uses) should prefer
/// [`serve_worker_with_shutdown`].
pub async fn serve_worker_with_database(
    addr: SocketAddr,
    native: Arc<NativeFunctionRunner>,
    database: Database<Rt>,
) -> anyhow::Result<()> {
    serve_worker_with_shutdown(addr, native, database, std::future::pending::<()>()).await
}

/// Serve `FunctionExecutionService` on `addr` with a
/// caller-provided shutdown future. When the future resolves, tonic
/// stops accepting new connections, drains in-flight RPCs, and
/// returns `Ok(())` (mapped into `anyhow::Result`). A transport error
/// from `serve_with_shutdown` still surfaces as `Err`.
///
/// Use this variant when the worker server is embedded in a larger
/// process (e.g. `convex-local-backend` under `CONVEX_MODE=worker`)
/// and must drain in lockstep with the rest of the process. The
/// shutdown future is typically an `async_broadcast::Receiver<()>`
/// wrapped in `async move { let _ = rx.recv().await; }`.
pub async fn serve_worker_with_shutdown<F>(
    addr: SocketAddr,
    native: Arc<NativeFunctionRunner>,
    database: Database<Rt>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let server = FunctionExecutionServer::new(native).with_database(database);
    Server::builder()
        .add_service(FunctionExecutionServiceServer::new(server))
        .serve_with_shutdown(addr, shutdown)
        .await
        .map_err(|e| anyhow::anyhow!("FunctionExecutionService serve({addr}): {e}"))
}

/// Connect a `TonicWorkerClient` per endpoint and wrap the set in a
/// `DistributedFunctionRunner`. Failures from any individual
/// `connect` bubble up — the conductor refuses to start with an
/// unreachable worker rather than quietly serving a reduced pool.
pub async fn build_conductor_runner(
    endpoints: &[String],
) -> anyhow::Result<DistributedFunctionRunner> {
    let mut workers: Vec<Arc<dyn WorkerClient>> = Vec::with_capacity(endpoints.len());
    for ep in endpoints {
        let client = TonicWorkerClient::connect(ep.clone())
            .await
            .map_err(|e| anyhow::anyhow!("conductor: connect to worker {ep:?} failed: {e}"))?;
        workers.push(client);
    }
    DistributedFunctionRunner::new(workers)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        OnceLock,
    };

    use super::*;

    /// Serialize every test that mutates `CONVEX_WORKER_BIND_ADDR`
    /// so the default-behaviour assertion doesn't race with the
    /// explicit-value ones under the default parallel test runner.
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    #[test]
    fn parse_worker_endpoints_trims_and_skips_blanks() {
        let ep = parse_worker_endpoints("http://a:1 , http://b:2 ,  ,http://c:3").unwrap();
        assert_eq!(
            ep,
            vec![
                "http://a:1".to_string(),
                "http://b:2".to_string(),
                "http://c:3".to_string(),
            ],
        );
    }

    #[test]
    fn parse_worker_endpoints_rejects_empty() {
        assert!(parse_worker_endpoints("").is_err());
        assert!(parse_worker_endpoints("   ,  , ").is_err());
    }

    #[test]
    fn read_worker_bind_addr_defaults_when_unset() {
        let _guard = env_guard();
        // SAFETY: env_guard serializes writes across tests in this module.
        unsafe {
            std::env::remove_var("CONVEX_WORKER_BIND_ADDR");
        }
        let addr = read_worker_bind_addr_from_env().unwrap();
        assert_eq!(
            addr,
            DEFAULT_WORKER_BIND_ADDR.parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn read_worker_bind_addr_parses_explicit_value() {
        let _guard = env_guard();
        // SAFETY: env_guard serializes writes across tests in this module.
        unsafe {
            std::env::set_var("CONVEX_WORKER_BIND_ADDR", "127.0.0.1:9999");
        }
        let addr = read_worker_bind_addr_from_env().unwrap();
        assert_eq!(addr, "127.0.0.1:9999".parse::<SocketAddr>().unwrap());
        // SAFETY: env_guard serializes writes across tests in this module.
        unsafe {
            std::env::remove_var("CONVEX_WORKER_BIND_ADDR");
        }
    }

    #[test]
    fn read_worker_bind_addr_rejects_garbage() {
        let _guard = env_guard();
        // SAFETY: env_guard serializes writes across tests in this module.
        unsafe {
            std::env::set_var("CONVEX_WORKER_BIND_ADDR", "not-a-socket-addr");
        }
        assert!(read_worker_bind_addr_from_env().is_err());
        // SAFETY: env_guard serializes writes across tests in this module.
        unsafe {
            std::env::remove_var("CONVEX_WORKER_BIND_ADDR");
        }
    }

    #[tokio::test]
    async fn build_worker_server_returns_usable_builder() {
        let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
        let (_builder, service) = build_worker_server(native);
        // If this compiles, the service is addressable via the
        // generated tonic trait object — which is the entire API
        // contract this helper provides.
        drop(service);
    }

    #[tokio::test]
    async fn build_conductor_runner_rejects_unreachable_worker() {
        // Port 1 is reserved; connect should fail fast.
        let endpoints = vec!["http://127.0.0.1:1".to_string()];
        assert!(build_conductor_runner(&endpoints).await.is_err());
    }

    #[tokio::test]
    async fn build_worker_server_drains_on_shutdown_future() {
        // Exercise the same `serve_with_shutdown` path
        // `serve_worker_with_shutdown` drives, but without a
        // Database<Rt> so the test stays self-contained. The only
        // thing we care about is that firing the shutdown future
        // causes tonic to return `Ok(())` rather than hanging.
        //
        // Bind on an ephemeral port and fire the signal immediately;
        // `serve_with_shutdown` must complete within the tokio timeout.
        use std::time::Duration;
        let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
        let (mut builder, service) = build_worker_server(native);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // release the port so tonic can rebind.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let serve = tokio::spawn(async move {
            builder
                .add_service(service)
                .serve_with_shutdown(addr, async move {
                    let _ = rx.await;
                })
                .await
        });
        // Give tonic a moment to actually start listening before we
        // signal shutdown — otherwise the serve future can race and
        // return before binding.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(());
        let result = tokio::time::timeout(Duration::from_secs(2), serve)
            .await
            .expect("serve must exit within timeout")
            .expect("task must not panic");
        assert!(
            result.is_ok(),
            "serve_with_shutdown should return Ok after drain: {result:?}",
        );
    }
}
