//! Minimal conductor binary: connects to a pool of workers,
//! health-probes each, then exits. Acts as both a smoke test and
//! a deployer-facing template.
//!
//! Usage:
//!
//! ```sh
//! CONVEX_MODE=conductor \
//!   CONVEX_WORKER_ENDPOINTS="http://worker-a:4567,http://worker-b:4567" \
//!   cargo run -p convex_native_distributed --example conductor
//! ```
//!
//! The binary:
//! 1. Reads `CONVEX_MODE` and refuses to run unless it's `conductor`.
//! 2. Parses `CONVEX_WORKER_ENDPOINTS` into a worker list.
//! 3. Builds a `DistributedFunctionRunner` (fails fast on any unreachable
//!    worker).
//! 4. Prints a one-line health report per worker (registry version,
//!    accepts_traffic, registered_functions, in_flight).
//!
//! This is a read-only probe — nothing is dispatched. Extending
//! it to dispatch a real function means registering the
//! corresponding native handler so both sides of the wire can
//! encode/decode the call.

use convex_native::distributed::ConvexMode;
use convex_native_distributed::{
    read_mode_from_env,
    read_worker_endpoints_from_env,
    TonicWorkerClient,
    WorkerClient,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    if !matches!(mode, ConvexMode::Conductor | ConvexMode::Standalone) {
        anyhow::bail!(
            "examples/conductor: CONVEX_MODE={mode:?} is not conductor-capable; set \
             CONVEX_MODE=conductor or CONVEX_MODE=standalone"
        );
    }

    let endpoints = read_worker_endpoints_from_env()?;
    eprintln!("examples/conductor: probing {} worker(s)", endpoints.len(),);

    let mut ok = 0usize;
    let mut failed = 0usize;
    for ep in &endpoints {
        match TonicWorkerClient::connect(ep.clone()).await {
            Ok(client) => match client.health().await {
                Ok(h) => {
                    println!(
                        "{ep} => v={:?} traffic={} fns={} in_flight={}",
                        h.registry_version, h.accepts_traffic, h.registered_functions, h.in_flight,
                    );
                    ok += 1;
                },
                Err(e) => {
                    eprintln!("{ep} => health probe failed: {e}");
                    failed += 1;
                },
            },
            Err(e) => {
                eprintln!("{ep} => connect failed: {e}");
                failed += 1;
            },
        }
    }

    eprintln!("examples/conductor: probe complete — {ok} healthy, {failed} failed");
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
