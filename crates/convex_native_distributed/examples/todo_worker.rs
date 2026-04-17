//! Runnable worker with real handlers.
//!
//! A deployer-shaped example that declares a `Todo` schema and a
//! query/mutation/action trio, then boots the
//! `FunctionExecutionService` so a backend can dispatch to it over
//! gRPC. Pair with `convex-native/examples/HOW_TO_RUN.md` for the
//! end-to-end walkthrough.
//!
//! Run:
//!
//! ```sh
//! CONVEX_MODE=worker \
//!   CONVEX_WORKER_BIND_ADDR=127.0.0.1:4567 \
//!   cargo run -p convex_native_distributed --example todo_worker
//! ```
//!
//! The binary prints the collected registry on startup so you can see
//! your handlers are linked in, then serves gRPC on the bind address
//! until Ctrl-C.
//!
//! No `Database` is attached — query/mutation calls will return
//! `Code::Unimplemented`. That is the deployer-specific wiring step
//! covered by `serve_worker_with_database(...)` when the worker also
//! opens its own persistence, or by the backend-owned transaction
//! path (Phase 1 of `DISTRIBUTED_PLAN.md`) when the backend supplies
//! `begin_timestamp` + `existing_writes` on each request.
//!
//! For a version that actually executes queries against a real DB,
//! run `convex-local-backend` with `CONVEX_MODE=worker` — it reuses
//! the same registry discovery via `inventory` and wires the
//! `Database<Rt>` for you.

use std::sync::Arc;

use convex_native_core::{
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

// ── Schema ────────────────────────────────────────────────────────

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "todos")]
#[convex(index(name = "by_owner", fields = ["owner"]))]
pub struct Todo {
    pub owner: String,
    pub text: String,
    pub done: bool,
    pub created_at: f64,
}

// ── Functions ─────────────────────────────────────────────────────

#[convex::query]
pub async fn list_for_owner(
    ctx: &mut QueryCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .with_index(TodoIndex::ByOwner)
        .eq(TodoField::Owner, owner)?
        .collect()
        .await
}

#[convex::mutation]
pub async fn create(
    ctx: &mut MutationCtx<'_, Rt>,
    owner: String,
    text: String,
) -> anyhow::Result<Id<Todo>> {
    let now = ctx.unix_timestamp().as_secs_f64();
    let id = ctx
        .db()
        .insert(Todo {
            owner,
            text,
            done: false,
            created_at: now,
        })
        .await?;
    ctx.log().info(format!("created todo {id}"));
    Ok(id)
}

#[convex::mutation]
pub async fn mark_done(ctx: &mut MutationCtx<'_, Rt>, id: Id<Todo>) -> anyhow::Result<()> {
    ctx.db()
        .patch(
            id,
            TodoPatch {
                done: Some(true),
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

#[convex::action]
pub async fn summarise(ctx: &mut ActionCtx<'_, Rt>, owner: String) -> anyhow::Result<i64> {
    let todos = ctx
        .run_query(ListForOwner, ListForOwnerArgs { owner })
        .await?;
    Ok(todos.into_iter().filter(|t| !t.done).count() as i64)
}

// ── main ──────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = read_mode_from_env();
    if !matches!(mode, ConvexMode::Worker | ConvexMode::Standalone) {
        anyhow::bail!(
            "todo_worker: CONVEX_MODE={mode:?} is not worker-capable; set CONVEX_MODE=worker"
        );
    }

    let addr = read_worker_bind_addr_from_env()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);
    eprintln!(
        "todo_worker: listening on {addr}, {} native function(s) registered:",
        native.len(),
    );
    for reg in native.iter() {
        eprintln!("  - {}", reg.name);
    }

    let (mut builder, service) = build_worker_server(native);
    builder.add_service(service).serve(addr).await?;
    Ok(())
}
