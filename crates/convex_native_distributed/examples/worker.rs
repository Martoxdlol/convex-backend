//! Minimal worker binary. Demonstrates Phase 3.5 mode helpers
//! end-to-end and acts as an integration smoke test a deployer
//! can run by hand.
//!
//! Usage:
//!
//! ```sh
//! CONVEX_MODE=worker \
//!   CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
//!   cargo run -p convex_native_distributed --example worker
//! ```
//!
//! The binary:
//! 1. Reads `CONVEX_MODE` and refuses to run unless it's `worker` (a deployer
//!    pointing the wrong topology at this binary should see a loud error).
//! 2. Reads `CONVEX_WORKER_BIND_ADDR` (defaults to `0.0.0.0:4567`).
//! 3. Collects the static `NativeFunctionRunner` registry — empty in the
//!    out-of-the-box binary, populated when a real deployer links their
//!    `#[convex::query/mutation/action]` registrations into the binary at build
//!    time.
//! 4. Boots a `tonic::transport::Server` serving the `FunctionExecutionService`
//!    and blocks until Ctrl-C.
//!
//! Queries and mutations will return `Code::Unimplemented` until
//! this example is extended with a Database handle via
//! `FunctionExecutionServer::with_database(...)` — that's a
//! deployer-specific wiring step.
//!
//! When `CONVEX_BACKEND_ENDPOINT` is also set the binary dials
//! the backend's `WorkerAdmissionService` so the worker auto-
//! registers + heartbeats; the registration handle is held for
//! the worker's lifetime so dropping it (Ctrl-C) retires the
//! worker cleanly.

use std::sync::Arc;

use convex_native_core::{
    distributed::ConvexMode,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    admission_client::WorkerRegistration,
    build_worker_server,
    read_backend_endpoint_from_env,
    read_mode_from_env,
    read_worker_bind_addr_from_env,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    if !matches!(mode, ConvexMode::Worker | ConvexMode::Standalone) {
        anyhow::bail!(
            "examples/worker: CONVEX_MODE={mode:?} is not a worker-capable mode; set \
             CONVEX_MODE=worker or CONVEX_MODE=standalone to run this binary"
        );
    }

    let addr = read_worker_bind_addr_from_env()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);
    eprintln!(
        "examples/worker: listening on {addr}, {} native function(s) registered",
        native.len(),
    );

    // Optionally dial backend admission. The handle is held for
    // the binary's lifetime; dropping it cleanly retires the
    // worker on the backend side. When wired we also spawn a
    // 5-second heartbeat loop so the backend's
    // `WorkerStatus`-driven dashboards have fresh data. The
    // in-flight gauge comes from `NativeFunctionRunner::in_flight()`
    // — the same counter the health RPC exposes — so the
    // worker's pool-side `reported_in_flight` tracks its real
    // concurrent handler count.
    let _registration: Option<Arc<WorkerRegistration>> = match read_backend_endpoint_from_env()? {
        Some(backend_endpoint) => {
            let registry_version = convex_native_core::VERSION.to_string();
            let execute_endpoint = format!("http://{addr}");
            let reg =
                WorkerRegistration::register(backend_endpoint, execute_endpoint, registry_version)
                    .await?;
            eprintln!("examples/worker: registered with backend admission service");
            let reg = Arc::new(reg);
            let runner = native.clone();
            let _heartbeat = reg
                .clone()
                .spawn_heartbeat_loop(std::time::Duration::from_secs(5), move || {
                    runner.in_flight()
                });
            Some(reg)
        },
        None => None,
    };

    let (mut builder, service) = build_worker_server(native);
    builder.add_service(service).serve(addr).await?;
    Ok(())
}
