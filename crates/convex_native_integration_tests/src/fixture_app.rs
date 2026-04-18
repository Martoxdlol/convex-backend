//! Comprehensive `convex_native` fixture app — one instance of
//! every feature the crate exposes, wired through `inventory` so
//! every test binary in this crate picks the whole thing up with a
//! single `use crate::fixture_app as _;`.
//!
//! **What lives here vs what lives in the test files.**
//! - The fixture is deployer-shaped code — the same shapes a real user of
//!   `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]` /
//!   `#[convex::http_action]` / `#[convex::cron]` / `#[derive(ConvexDocument)]`
//!   writes.
//! - The test files stand up one of the two topologies (standalone or
//!   distributed), drive these handlers, and assert on the observable
//!   behaviour. No handler code lives next to the tests; adding a new handler
//!   means growing the fixture + both topology files get a new assertion.

#![allow(dead_code)]

use convex_native::{
    convex,
    prelude::*,
    ActionCtx,
    ConvexDocument,
    ConvexEnum,
    ConvexNested,
    ConvexUnion,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    MutationCtx,
    QueryCtx,
    Rt,
};

// ---------------------------------------------------------------------------
// Schema — primary "Todo" table plus a messages table used for text/vector
// index coverage. Keeping each table tiny makes the fixture readable; the
// point of the crate is breadth, not depth.
// ---------------------------------------------------------------------------

/// Priority enum — exercises `#[derive(ConvexEnum)]` and its
/// rename attribute.
#[derive(ConvexEnum, Debug, Clone, PartialEq)]
pub enum Priority {
    Low,
    Medium,
    High,
}

/// Nested struct — exercises `#[derive(ConvexNested)]` composing
/// into a top-level `ConvexDocument`.
#[derive(ConvexNested, Debug, Clone, PartialEq)]
pub struct Metadata {
    pub source: String,
    pub priority: Priority,
}

/// Tagged union — exercises `#[derive(ConvexUnion)]`.
#[derive(ConvexUnion, Debug, Clone, PartialEq)]
#[convex(tag = "kind")]
pub enum Notification {
    Email { to: String },
    Push { token: String },
}

/// Primary table — every feature that touches an `Id<Todo>` goes
/// through this.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "todos")]
#[convex(index(name = "by_owner", fields = ["owner"]))]
#[convex(index(name = "by_owner_done", fields = ["owner", "done"]))]
pub struct Todo {
    pub owner: String,
    pub text: String,
    pub done: bool,
    pub created_at: f64,
    pub metadata: Option<Metadata>,
}

/// Secondary table — plain table used by tests that want a second
/// tablet alongside `todos`. Text + vector indexes live in a
/// separate fixture module that only the relevant tests opt into,
/// because wiring their backfill requires the
/// `SearchAndVectorBootstrapWorker` on top of the plain
/// `SchemaWorker`.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(index(name = "by_channel", fields = ["channel"]))]
pub struct Message {
    pub channel: String,
    pub body: String,
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// List every todo for an owner. Uses `.eq(...)` as a
/// post-scan filter (no `.with_index(...)` — the fixture's
/// `DbFixture` intentionally doesn't publish the schema so index
/// dispatch paths are covered separately; see `db_fixture.rs` for
/// why).
#[convex::query]
pub async fn list_todos(ctx: &mut QueryCtx<'_, Rt>, owner: String) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .eq(TodoField::Owner, owner)?
        .collect()
        .await
}

/// Count the unfinished todos for an owner. Used by the
/// `summarise` action to exercise action → query sub-calls.
#[convex::query]
pub async fn count_pending(ctx: &mut QueryCtx<'_, Rt>, owner: String) -> anyhow::Result<i64> {
    let todos = ctx
        .db()
        .query::<Todo>()
        .eq(TodoField::Owner, owner)?
        .collect()
        .await?;
    Ok(todos.into_iter().filter(|t| !t.done).count() as i64)
}

/// Exercises `ctx.auth()` so the distributed identity-forwarding
/// path has something to assert against.
#[convex::query]
pub async fn whoami(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<String> {
    Ok(if ctx.auth().is_system() {
        "system".to_string()
    } else if ctx.auth().is_admin() {
        "admin".to_string()
    } else if ctx.auth().is_authenticated() {
        "user".to_string()
    } else {
        "anonymous".to_string()
    })
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/// Insert a todo. Exercises `ctx.db().insert(...)` +
/// `ctx.unix_timestamp()` + `ctx.log()`.
#[convex::mutation]
pub async fn create_todo(
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
            metadata: None,
        })
        .await?;
    ctx.log().info(format!("created todo {id}"));
    Ok(id)
}

/// Flip a todo's `done` flag. Exercises `ctx.db().patch(...)` +
/// `TodoPatch`.
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

/// Internal-only mutation — exercises the `internal` modifier. A
/// direct external call should be refused by the validation layer;
/// internal sub-calls still work.
#[convex::mutation(internal)]
pub async fn internal_delete(ctx: &mut MutationCtx<'_, Rt>, id: Id<Todo>) -> anyhow::Result<()> {
    ctx.db().delete(id).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

/// Fan out to a sub-query through `ctx.run_query(...)`. Covers the
/// typed sub-call path end-to-end.
#[convex::action]
pub async fn summarise(ctx: &mut ActionCtx<'_, Rt>, owner: String) -> anyhow::Result<i64> {
    let pending = ctx
        .run_query(CountPending, CountPendingArgs { owner })
        .await?;
    Ok(pending)
}

// ---------------------------------------------------------------------------
// HTTP actions
// ---------------------------------------------------------------------------

/// Simple echo handler — exercises the HTTP-action ctx's request /
/// response helpers. The distributed HTTP dispatch path routes
/// through `WorkerPool::eligible_for_http` + the `http_request`
/// proto field.
#[convex::http_action(method = "POST", path = "/api/ping")]
pub async fn ping(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let body = req.body_text()?;
    Ok(HttpResponse::text(200, format!("pong:{body}")))
}

// ---------------------------------------------------------------------------
// Crons
// ---------------------------------------------------------------------------

/// Target mutation for the nightly cron below. Kept as a
/// no-argument internal mutation so the cron dispatcher can fire it
/// without any arg wiring.
#[convex::mutation(internal)]
pub async fn nightly_cleanup(_ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<()> {
    Ok(())
}

#[convex::cron(
    name = "nightly-cleanup",
    schedule = "0 3 * * *",
    target = "nightly_cleanup"
)]
fn _nightly_cleanup_cron() {}
