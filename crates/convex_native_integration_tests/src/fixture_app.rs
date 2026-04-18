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

/// Pure action with no sub-calls — exercises plain action
/// dispatch + `ctx.log()` in the action ctx without any callbacks
/// wiring required.
#[convex::action]
pub async fn echo_action(ctx: &mut ActionCtx<'_, Rt>, message: String) -> anyhow::Result<String> {
    ctx.log().info(format!("echo: {message}"));
    Ok(format!("echo:{message}"))
}

// ---------------------------------------------------------------------------
// Errors — surfaces every `errors::*` helper so both topologies
// can assert the error metadata survives the dispatch layer.
// ---------------------------------------------------------------------------

/// Always-failing mutation used to prove `errors::bad_request`
/// surfaces a user-facing error (not a 500).
#[convex::mutation]
pub async fn always_bad_request(_ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<()> {
    Err(convex_native::errors::bad_request("BadInput", "deliberately broken").into())
}

/// Handler that errors with each `errors::*` helper based on the
/// input tag — lets one test file assert every status-coded error
/// without needing N handlers.
#[convex::query]
pub async fn error_of_kind(_ctx: &mut QueryCtx<'_, Rt>, kind: String) -> anyhow::Result<()> {
    let e = match kind.as_str() {
        "bad_request" => convex_native::errors::bad_request("S", "bad_request msg"),
        "unauthenticated" => convex_native::errors::unauthenticated("S", "unauthenticated msg"),
        "forbidden" => convex_native::errors::forbidden("S", "forbidden msg"),
        "not_found" => convex_native::errors::not_found("S", "not_found msg"),
        "conflict" => convex_native::errors::conflict("S", "conflict msg"),
        "rate_limited" => convex_native::errors::rate_limited("S", "rate_limited msg"),
        "overloaded" => convex_native::errors::overloaded("S", "overloaded msg"),
        other => anyhow::bail!("unknown kind {other}"),
    };
    Err(e.into())
}

// ---------------------------------------------------------------------------
// RNG + deterministic randomness. `ctx.rng_u64()` reads seeds from
// the outcome so a handler re-run with the same seed produces the
// same sequence.
// ---------------------------------------------------------------------------

/// Return a deterministic u64 from `ctx.rng_u64()`. Test asserts
/// it's deterministic across independent invocations (seeded from
/// the outcome context, which defaults to a fixed seed when the
/// handler is called without an explicit `Observed`).
#[convex::query]
pub async fn pull_rng(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<i64> {
    // i64 so it survives the ConvexValue round-trip as `Int64`.
    Ok(ctx.rng_u64() as i64)
}

/// Long-running action — loops forever. Tests the runner's
/// `with_default_timeout` / per-function-timeout path by
/// letting the runner abort it.
#[convex::action(timeout_ms = 100)]
pub async fn sleep_forever(_ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    // Deliberately spin on `tokio::time::sleep` in a long loop so
    // the runner's `tokio::time::timeout` wrapper aborts us.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

// ---------------------------------------------------------------------------
// Single-doc read APIs — exercises `ctx.db().get(...)` /
// `get_with_meta` / `exists` / `normalize_id`. Each is a tiny
// query so tests can compose them against seeded data.
// ---------------------------------------------------------------------------

/// Look up a todo by id; returns `None` when missing.
#[convex::query]
pub async fn get_todo(ctx: &mut QueryCtx<'_, Rt>, id: Id<Todo>) -> anyhow::Result<Option<Todo>> {
    ctx.db().get(id).await
}

/// Existence check by id.
#[convex::query]
pub async fn todo_exists(ctx: &mut QueryCtx<'_, Rt>, id: Id<Todo>) -> anyhow::Result<bool> {
    ctx.db().exists(id).await
}

/// Validate a raw id string against the `Todo` table. Returns
/// `true` when the string is a valid id for this table,
/// regardless of whether the referenced document exists.
#[convex::query]
pub async fn normalize_todo_id(ctx: &mut QueryCtx<'_, Rt>, raw: String) -> anyhow::Result<bool> {
    Ok(ctx.db().normalize_id::<Todo>(&raw).is_some())
}

// ---------------------------------------------------------------------------
// Typed query operators — covers `.first()`, `.take(n)`,
// `.count()`, `.gte` / `.lt` filter operators.
// ---------------------------------------------------------------------------

/// Count every todo in the table (no filters).
#[convex::query]
pub async fn count_all_todos(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<i64> {
    Ok(ctx.db().query::<Todo>().count().await? as i64)
}

/// Return the first (index order) todo for an owner, or `None`.
#[convex::query]
pub async fn first_todo_for_owner(
    ctx: &mut QueryCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<Option<Todo>> {
    ctx.db()
        .query::<Todo>()
        .eq(TodoField::Owner, owner)?
        .first()
        .await
}

/// Take the first N todos across the whole table.
#[convex::query]
pub async fn take_todos(ctx: &mut QueryCtx<'_, Rt>, n: i64) -> anyhow::Result<Vec<Todo>> {
    ctx.db().query::<Todo>().take(n as usize).await
}

/// Filter by creation timestamp range — exercises `.gte` / `.lt`.
#[convex::query]
pub async fn todos_in_time_range(
    ctx: &mut QueryCtx<'_, Rt>,
    from: f64,
    to: f64,
) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .gte(TodoField::CreatedAt, from)?
        .lt(TodoField::CreatedAt, to)?
        .collect()
        .await
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
