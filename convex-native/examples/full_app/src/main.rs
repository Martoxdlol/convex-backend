//! `convex_full_app_example` — fully self-contained reference app.
//!
//! Boot modes (picked up from `CONVEX_MODE`):
//!
//! - **standalone** (default, no env var) — prints the
//!   introspection envelope and exits. Good for CI, schema
//!   diffing, and sanity-checking a deployer build.
//! - **worker** — spawns the tonic `FunctionExecutionService` on
//!   `CONVEX_WORKER_BIND_ADDR` with every function in this app
//!   registered. Block until Ctrl-C.
//!
//! Run:
//!
//! ```sh
//! cargo run -p convex_full_app_example                         # standalone
//! CONVEX_MODE=worker \
//!   CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
//!   cargo run -p convex_full_app_example
//! ```
//!
//! For the full standalone topology (HTTP + WebSocket + native
//! dispatch), see `convex-native/STANDALONE.md` — it covers how
//! to depend on `local_backend` as a library and reuse the
//! modules in this crate.

use std::sync::Arc;

use convex_native::{
    distributed::ConvexMode,
    ConvexBackend,
    NativeFunctionRunner,
    NoopCallbacks,
};
use convex_native_distributed::{
    build_worker_server,
    read_mode_from_env,
    read_worker_bind_addr_from_env,
};

// Pull every functions/schema module into the compilation unit so
// the `inventory::submit!` calls for their registrations get
// linked into the final binary. Without this, `NativeFunctionRunner`
// would collect an empty registry.
#[allow(unused_imports)]
use convex_full_app_example as _;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logging. The worker path below expects `RUST_LOG`;
    // the default filter keeps the noise down in standalone mode.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,convex_native=debug".into()),
        )
        .init();

    let mode = read_mode_from_env();
    match mode {
        ConvexMode::Standalone => run_standalone().await,
        ConvexMode::Worker => run_worker().await,
        ConvexMode::Conductor => {
            anyhow::bail!(
                "convex_full_app: conductor mode lives in a separate binary. See \
                 convex-native/examples/README.md §1 for the dispatch-side example."
            );
        },
    }
}

/// Build a `ConvexBackend` from the registered inventory and print
/// the introspection envelope. This is the CI-friendly shape — no
/// server is bound, no persistence is touched, the binary exits
/// after printing.
async fn run_standalone() -> anyhow::Result<()> {
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(Arc::new(NoopCallbacks))
        .build()?;

    built.validate()?;
    eprintln!(
        "convex_full_app: {} function(s), {} table(s), {} route(s), {} cron(s)",
        built.function_count(),
        built.table_count(),
        built.route_count(),
        built.cron_count(),
    );
    println!("{}", built.describe_pretty());
    Ok(())
}

/// Boot the native worker server. Every `#[convex::query/mutation/action]`
/// registered through this crate is callable via the gRPC
/// `FunctionExecutionService`.
async fn run_worker() -> anyhow::Result<()> {
    let addr = read_worker_bind_addr_from_env()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);
    tracing::info!(
        addr = %addr,
        registered_functions = native.len(),
        "convex_full_app worker starting",
    );
    for reg in native.iter() {
        tracing::info!(name = reg.name, kind = ?reg.udf_type(), "registered");
    }
    let (mut builder, service) = build_worker_server(native);
    builder.add_service(service).serve(addr).await?;
    Ok(())
}
