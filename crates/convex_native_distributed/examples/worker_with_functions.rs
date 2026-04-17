//! Worker example with real native functions registered.
//!
//! Unlike `worker.rs` (empty-registry template), this binary
//! actually declares `#[convex::query]` / `#[convex::mutation]` /
//! `#[convex::action]` handlers so the gRPC server has something to
//! dispatch to. Useful as a copy-paste starting point for a
//! deployer's own worker binary.
//!
//! The action demonstrates the `WorkerActionCallbacks` wiring added
//! this session: `ctx.run_query(...)` sub-calls from inside an
//! action work against the worker's own native registry without a
//! JS runtime.
//!
//! Run with:
//!
//! ```sh
//! cargo build --examples -p convex_native_distributed
//!
//! # Pick an ephemeral port and boot the worker.
//! CONVEX_MODE=worker \
//!   CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
//!   ./target/debug/examples/worker_with_functions
//!
//! # In another shell, drive it end-to-end:
//! CONVEX_MODE=conductor \
//!   CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:4567 \
//!   ./target/debug/examples/conductor
//! ```
//!
//! The conductor example probes health and prints the pool summary;
//! for a dispatch-and-assert flow see the `client_e2e_smoke`
//! integration test.

use std::sync::Arc;

use convex_native::{
    convex,
    distributed::ConvexMode,
    prelude::*,
    ActionCtx,
    ConvexDocument,
    MutationCtx,
    NativeFunctionRunner,
    QueryCtx,
    Rt,
};
use convex_native_distributed::{
    build_worker_server,
    read_mode_from_env,
    read_worker_bind_addr_from_env,
};

// ── Schema ─────────────────────────────────────────────────────────

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "widgets")]
#[convex(index(name = "by_owner", fields = ["owner"]))]
pub struct Widget {
    pub owner: String,
    pub name: String,
    pub created_at: f64,
}

// ── Functions ──────────────────────────────────────────────────────

#[convex::query]
pub async fn list_for_owner(
    ctx: &mut QueryCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<Vec<Widget>> {
    ctx.db()
        .query::<Widget>()
        .with_index(WidgetIndex::ByOwner)
        .eq(WidgetField::Owner, owner)?
        .collect()
        .await
}

#[convex::mutation]
pub async fn create(
    ctx: &mut MutationCtx<'_, Rt>,
    owner: String,
    name: String,
) -> anyhow::Result<Id<Widget>> {
    let now = ctx.unix_timestamp().as_secs_f64();
    ctx.db()
        .insert(Widget {
            owner,
            name,
            created_at: now,
        })
        .await
}

#[convex::action]
pub async fn touch(
    ctx: &mut ActionCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<i64> {
    // Demonstrates WorkerActionCallbacks end-to-end: this action
    // sub-calls the native query above through the worker's own
    // Database handle. On workers built with `.with_database(db)`,
    // the sub-call opens a fresh transaction, runs the handler,
    // drops the tx. On workers without a database it bails with
    // "no callbacks attached" — which is the expected failure mode
    // for the pure-dispatcher topology.
    let widgets: Vec<Widget> = ctx
        .run_query(
            ListForOwner,
            ListForOwnerArgs {
                owner: owner.clone(),
            },
        )
        .await?;
    ctx.log().info(format!(
        "touch: owner={owner:?} has {} widget(s)",
        widgets.len()
    ));
    Ok(widgets.len() as i64)
}

// ── Entry point ────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    anyhow::ensure!(
        matches!(mode, ConvexMode::Worker | ConvexMode::Standalone),
        "expected CONVEX_MODE=worker (or standalone), got {mode:?}",
    );
    let addr = read_worker_bind_addr_from_env()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);
    eprintln!(
        "worker_with_functions: listening on {addr}, {} fn(s): {:?}",
        native.len(),
        native
            .iter()
            .map(|r| (r.name, r.udf_type()))
            .collect::<Vec<_>>(),
    );

    let (mut builder, service) = build_worker_server(native);
    builder.add_service(service).serve(addr).await?;
    Ok(())
}
