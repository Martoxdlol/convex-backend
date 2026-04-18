//! Reference JavaScript worker binary — Phase 6.3 of
//! `convex-native/STATUS.md`.
//!
//! The native worker (see `worker.rs`) registers with the
//! backend's `WorkerAdmissionService` advertising
//! `WorkerKind::NATIVE_RUST` and serves native handler dispatch
//! over `FunctionExecutionService`. A JS worker advertises
//! `WorkerKind::JAVASCRIPT` and serves V8-isolate JS handler
//! dispatch through the same wire shape.
//!
//! ## Usage
//!
//! ```sh
//! CONVEX_MODE=worker \
//!   CONVEX_BACKEND_ENDPOINT=http://convex-backend-admission:5678 \
//!   CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
//!   CONVEX_WORKER_KIND=javascript \
//!   cargo run -p convex_native_distributed --example js_worker
//! ```
//!
//! ## Topology
//!
//! 1. Reads `CONVEX_BACKEND_ENDPOINT` (admission), `CONVEX_WORKER_BIND_ADDR`
//!    (dispatch listen), and `CONVEX_BACKEND_CALLBACK_ENDPOINT` (sub-call
//!    routing — same endpoint a Rust worker uses).
//! 2. Builds an empty `NativeFunctionRunner` (this binary has no `#[convex::*]`
//!    registrations) and exposes `FunctionExecutionService` against it.
//!    Dispatch requests targeting JS handler names will land on
//!    `Code::NotFound` until a deployer wires up their own JS function runner —
//!    that wiring step is intentionally a deployer concern (the in-process V8
//!    isolate stack lives in the `function_runner` + `isolate` crates and
//!    requires the full `Application` shell to drive).
//! 3. Registers with the admission service as `WorkerKind::JAVASCRIPT`. The
//!    backend's pool tracks the worker; if the deployer wires `kind_preference`
//!    to route JS-style names here, dispatch lands on this binary.
//!
//! ## Why this binary is intentionally minimal
//!
//! The full JS dispatch path requires `Application` (which owns
//! `Database<RT>`, `FileStorage<RT>`, the V8 isolate-backed
//! `InProcessFunctionRunner`, etc.). Embedding that inside this
//! example would duplicate `convex-local-backend` itself; for a
//! production JS worker, deployers should:
//!
//! - Use `convex-local-backend` with `CONVEX_MODE=worker` (existing path —
//!   Phase-3 admission already works), then layer this example's admission-side
//!   wiring (kind=javascript) on top, OR
//! - Build a custom binary that constructs `Application` and wires
//!   `FunctionExecutionServer` to dispatch JS function names through
//!   `application.runner().execute_{query,mutation,action}`.
//!
//! The example below covers (1): admission with the right kind +
//! dispatch wired through the existing
//! `serve_worker_with_options` helper.

use std::sync::Arc;

use convex_native_core::{
    distributed::ConvexMode,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    admission::collect_inventory,
    admission_client::WorkerRegistration,
    pool::WorkerKind,
    read_backend_callback_endpoint_from_env,
    read_backend_endpoint_from_env,
    read_mode_from_env,
    read_worker_bind_addr_from_env,
    serve_worker_with_options,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    if !matches!(mode, ConvexMode::Worker | ConvexMode::Standalone) {
        anyhow::bail!(
            "examples/js_worker: CONVEX_MODE={mode:?} is not a worker-capable mode; set \
             CONVEX_MODE=worker"
        );
    }

    let bind_addr = read_worker_bind_addr_from_env()?;
    let backend_endpoint = read_backend_endpoint_from_env()?;
    let callback_endpoint = read_backend_callback_endpoint_from_env()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);

    eprintln!(
        "examples/js_worker: listening on {bind_addr}, registering as WorkerKind::JAVASCRIPT, {} \
         function(s) in inventory",
        native.len(),
    );

    // Register with the admission service when an endpoint is
    // configured (Phase-3 dynamic-pool topology). Static-pool
    // (`CONVEX_NATIVE_WORKERS` on the backend) deployments don't
    // need this — the backend dials us directly.
    let _registration = if let Some(backend) = backend_endpoint {
        let (inventory, _hash) = collect_inventory()?;
        let envelope_kind = WorkerKind::Javascript;
        let registry_version = convex_native_core::VERSION.to_string();
        Some(
            WorkerRegistration::register_with_kind(
                backend,
                format!("http://{bind_addr}"),
                registry_version,
                envelope_kind,
                inventory,
            )
            .await?,
        )
    } else {
        None
    };

    // Serve dispatch on the worker's bind address. JS handler
    // dispatch lands on `NotFound` from the empty native registry
    // until a custom binary wires `Application`-backed
    // `execute_*` methods through; for the standard
    // `convex-local-backend` JS path, run this binary alongside
    // a `convex-local-backend` process and let the backend's
    // `kind_preference` route JS names there.
    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
    };
    serve_worker_with_options(bind_addr, native, None, callback_endpoint, shutdown).await?;
    Ok(())
}
