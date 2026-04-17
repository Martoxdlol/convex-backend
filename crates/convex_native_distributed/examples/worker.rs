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

use std::sync::Arc;

use convex_native_core::{
    distributed::ConvexMode,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    build_worker_server,
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

    let (mut builder, service) = build_worker_server(native);
    builder.add_service(service).serve(addr).await?;
    Ok(())
}
