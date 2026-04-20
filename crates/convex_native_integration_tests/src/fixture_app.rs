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

/// Tertiary table exercising the less-common field-type arms of
/// `#[derive(ConvexDocument)]`: `Vec<u8>` (which the macro
/// special-cases to `Validator::Bytes` rather than
/// `Validator::Array(Byte)`) and `BTreeMap<String, String>`
/// (which maps to `Validator::Record(String, String)`). Keeps the
/// schema surface exercised end-to-end without bloating the Todo
/// / Message tables.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "attachments")]
pub struct Attachment {
    pub payload: Vec<u8>,
    pub tags: std::collections::BTreeMap<String, String>,
}

/// Secondary table. Declares a text index *and* a vector index so
/// search-index collection paths (`NativeSchema::collect`,
/// `warmup::plan_warmup`, the admission envelope's schema JSON)
/// are exercised by the fixture. The indexes aren't backfilled —
/// `DbFixture::new_in_memory()` intentionally skips
/// `publish_native_schema` — but the declaration side is what
/// survives through the wire and is what regressions tend to
/// flatten.
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(index(name = "by_channel", fields = ["channel"]))]
#[convex(text_index(
    name = "by_body",
    search_field = "body",
    filter_fields = ["channel"]
))]
#[convex(vector_index(
    name = "by_embedding",
    vector_field = "embedding",
    dimensions = 8,
    filter_fields = ["channel"]
))]
pub struct Message {
    pub channel: String,
    pub body: String,
    pub embedding: Vec<f64>,
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

/// Secondary-table query — ensures handlers dispatched against a
/// non-`Todo` table compile and run through the same `QueryCtx`
/// plumbing. Uses the `messages` table (declared elsewhere in
/// this module with text + vector indexes) as the second
/// tablet so registry + dispatch paths see multi-table traffic.
#[convex::query]
pub async fn list_messages_in_channel(
    ctx: &mut QueryCtx<'_, Rt>,
    channel: String,
) -> anyhow::Result<Vec<Message>> {
    ctx.db()
        .query::<Message>()
        .eq(MessageField::Channel, channel)?
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

/// Internal-only query — exercises the `internal` modifier on
/// `#[convex::query]`. External clients must be refused by the
/// validation layer; internal sub-calls still work. Mirrors the
/// existing `internal_delete` mutation + `internal_action` so
/// the is_internal flag is exercised on every UdfType kind.
#[convex::query(internal)]
pub async fn internal_count_all(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<i64> {
    Ok(ctx.db().query::<Todo>().count().await? as i64)
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

/// Mutation whose args include an `Option<String>`. Exercises
/// the Option<T> arg-decoding path on the generated
/// `CreateTodoOptionalArgs` struct — distinct from the
/// Option<T> *field* path already covered by `Todo::metadata`.
/// Returns the owner back (or `"none"` when absent) so tests can
/// round-trip both Some and None through ConvexValue args.
#[convex::mutation]
pub async fn echo_optional_owner(
    _ctx: &mut MutationCtx<'_, Rt>,
    owner: Option<String>,
) -> anyhow::Result<String> {
    Ok(owner.unwrap_or_else(|| "none".to_string()))
}

/// Mutation whose args include a `#[derive(ConvexNested)]`
/// struct. Exercises the nested-object arg-decoding path —
/// distinct from the primitive/Option/Vec paths already
/// covered. The Metadata struct embeds a ConvexEnum (Priority),
/// so this handler also round-trips an enum-inside-struct
/// through the generated args decoder.
#[convex::mutation]
pub async fn echo_metadata(_ctx: &mut MutationCtx<'_, Rt>, md: Metadata) -> anyhow::Result<String> {
    Ok(format!("{}/{:?}", md.source, md.priority))
}

/// Query that *returns* a `#[derive(ConvexUnion)]` value — the
/// return-side symmetry for the ConvexUnion derive. Existing
/// tests cover arg decoding + `to_convex`/`from_convex` round
/// trips at the type level, but no handler exercises the
/// return-type path where the generated ToConvex impl runs
/// through the runner's response serialisation.
#[convex::query]
pub async fn fetch_notification(
    _ctx: &mut QueryCtx<'_, Rt>,
    kind: String,
) -> anyhow::Result<Notification> {
    Ok(match kind.as_str() {
        "email" => Notification::Email {
            to: "a@b.com".to_string(),
        },
        _ => Notification::Push {
            token: "dev-token".to_string(),
        },
    })
}

/// Mutation that *accepts* a `#[derive(ConvexUnion)]` value. The
/// complement to fetch_notification; exercises the arg-side of
/// the ConvexUnion derive at the dispatch layer. Returns the
/// inner email/token so tests can assert the variant was
/// decoded correctly.
#[convex::mutation]
pub async fn echo_notification(
    _ctx: &mut MutationCtx<'_, Rt>,
    note: Notification,
) -> anyhow::Result<String> {
    Ok(match note {
        Notification::Email { to } => format!("email:{to}"),
        Notification::Push { token } => format!("push:{token}"),
    })
}

/// Mutation that accepts a `Vec<u8>` arg — the Bytes special-case.
/// Distinct from Vec<u8> as a document field (covered by
/// Attachment::payload). The args-side of the macro threads
/// Vec<u8> through FromConvex → ConvexValue::Bytes, a separate
/// code path from the field-level derive. Returns the byte
/// count so tests can confirm the full payload survived the
/// boundary.
#[convex::mutation]
pub async fn echo_bytes_len(
    _ctx: &mut MutationCtx<'_, Rt>,
    payload: Vec<u8>,
) -> anyhow::Result<i64> {
    Ok(payload.len() as i64)
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

/// Replace the whole document — exercises `ctx.db().replace(...)`.
#[convex::mutation]
pub async fn replace_todo(
    ctx: &mut MutationCtx<'_, Rt>,
    id: Id<Todo>,
    owner: String,
    text: String,
    done: bool,
) -> anyhow::Result<()> {
    ctx.db()
        .replace(
            id,
            Todo {
                owner,
                text,
                done,
                created_at: 0.0,
                metadata: None,
            },
        )
        .await?;
    Ok(())
}

/// Insert a todo and then read it back in the same mutation tx.
/// The returned bool is whether the read found the inserted row
/// — proving writes-are-visible-within-a-tx semantics.
#[convex::mutation]
pub async fn insert_then_read(
    ctx: &mut MutationCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<bool> {
    let id = ctx
        .db()
        .insert(Todo {
            owner: owner.clone(),
            text: "probe".to_string(),
            done: false,
            created_at: 0.0,
            metadata: None,
        })
        .await?;
    let got = ctx.db().get(id).await?;
    Ok(got.is_some())
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

/// Action that sub-calls another action (`echo_action`) via
/// `ctx.run_action(...)`. Exercises the action → action chain's
/// same-worker fast path: the runner has the callee in its
/// inventory, so `run_action_raw` dispatches locally without
/// round-tripping through callbacks. The distinguishing feature
/// vs `ctx.run_query`/`run_mutation` is that `run_action` prefers
/// the local runner when available.
#[convex::action]
pub async fn chain_echo(ctx: &mut ActionCtx<'_, Rt>, message: String) -> anyhow::Result<String> {
    ctx.run_action(EchoAction, EchoActionArgs { message }).await
}

/// Action that reads a Todo by id from inside the action
/// context. Exercises `ActionCtx::db().get(id)`, which routes
/// through `NativeActionCallbacks::read_document_at_snapshot`
/// (not the per-ctx `ctx.run_query(...)` sub-call path). Returns
/// the text of the read document, or the string `"(missing)"`
/// when the callback resolves to `None`.
#[convex::action]
pub async fn read_todo_from_action(
    ctx: &mut ActionCtx<'_, Rt>,
    id: Id<Todo>,
) -> anyhow::Result<String> {
    let got = ctx.db().get(id).await?;
    Ok(match got {
        Some(t) => t.text,
        None => "(missing)".to_string(),
    })
}

/// Action that sub-calls `create_todo` through the untyped
/// mutation-by-name callback path. Exercises the action →
/// sub-mutation route — distinct from `summarise` (sub-query)
/// and `chain_echo` (sub-action / local runner). Returns the
/// raw id string so tests can stub the callback and assert.
#[convex::action]
pub async fn chain_create_from_action(
    ctx: &mut ActionCtx<'_, Rt>,
    owner: String,
    text: String,
) -> anyhow::Result<String> {
    let mut map: std::collections::BTreeMap<
        convex_native_core::__private::FieldName,
        convex_native_core::__private::ConvexValue,
    > = std::collections::BTreeMap::new();
    map.insert(
        "owner".parse()?,
        convex_native_core::__private::ConvexValue::try_from(owner)?,
    );
    map.insert(
        "text".parse()?,
        convex_native_core::__private::ConvexValue::try_from(text)?,
    );
    let args = convex_native_core::__private::ConvexObject::try_from(map)?;
    let ret = ctx.run_mutation_raw("create_todo", args).await?;
    match ret {
        convex_native_core::__private::ConvexValue::String(s) => Ok(s.to_string()),
        other => anyhow::bail!("chain_create_from_action expected String, got {other:?}"),
    }
}

/// Emit one line at each of the four log levels so tests can
/// assert that `ctx.log().debug/info/warn/error(...)` each land
/// in the shared `LogBuffer` with the matching `LogLevel`.
/// The existing log-buffer test only covers `info`.
#[convex::action]
pub async fn emit_every_log_level(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    ctx.log().debug("lvl:debug");
    ctx.log().info("lvl:info");
    ctx.log().warn("lvl:warn");
    ctx.log().error("lvl:error");
    Ok(())
}

/// Exercise the untyped sub-call surface: dispatch
/// `count_pending` by string name through `ctx.run_query_raw(...)`
/// instead of the typed marker. Returns the raw i64. The
/// string-name path is what deployers use when the callee is
/// picked dynamically at runtime (e.g. plugin dispatch) and has a
/// slightly different failure shape from the typed path — a
/// regression in name resolution or arg serialization inside
/// `run_query_raw` would slip past the existing `summarise`-based
/// test (which goes through `run_query`).
#[convex::action]
pub async fn untyped_count_pending(
    ctx: &mut ActionCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<i64> {
    let mut map: std::collections::BTreeMap<
        convex_native_core::__private::FieldName,
        convex_native_core::__private::ConvexValue,
    > = std::collections::BTreeMap::new();
    map.insert(
        "owner".parse()?,
        convex_native_core::__private::ConvexValue::try_from(owner)?,
    );
    let args = convex_native_core::__private::ConvexObject::try_from(map)?;
    let ret = ctx.run_query_raw("count_pending", args).await?;
    match ret {
        convex_native_core::__private::ConvexValue::Int64(n) => Ok(n),
        other => anyhow::bail!("untyped_count_pending expected Int64, got {other:?}"),
    }
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

/// Fill a 16-byte buffer with `ctx.rng_fill(...)` and return it
/// as a hex string. Lets tests assert the byte stream is
/// deterministic *and* that `rng_fill` flows from the same
/// seeded generator `rng_u64` reads from (so mixing the two in a
/// single handler is safe).
#[convex::query]
pub async fn pull_rng_bytes(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<String> {
    let mut buf = [0u8; 16];
    ctx.rng_fill(&mut buf);
    let mut hex = String::with_capacity(buf.len() * 2);
    for b in buf {
        hex.push_str(&format!("{b:02x}"));
    }
    Ok(hex)
}

/// Return `ctx.execution_context().request_id` (as a string) or
/// an empty string when absent. Lets tests assert the dispatch
/// layer threads the caller's ExecutionContext through.
#[convex::query]
pub async fn request_id(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<String> {
    Ok(ctx
        .execution_context()
        .map(|c| c.request_id.to_string())
        .unwrap_or_default())
}

/// Return `ctx.execution_context().execution_id` as a string or
/// an empty string when absent. Companion to `request_id`; the
/// two ids serialize through separate proto fields, so a
/// regression in execution-id wire encoding wouldn't fall out
/// of the request-id test alone.
#[convex::query]
pub async fn execution_id(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<String> {
    Ok(ctx
        .execution_context()
        .map(|c| c.execution_id.to_string())
        .unwrap_or_default())
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
// Scheduler + Storage — exercises `ctx.scheduler()` and
// `ctx.storage()`. Both flow through `NativeActionCallbacks`, so
// `TestCallbacks` + `CallRecord` are the assertion surface.
// ---------------------------------------------------------------------------

use std::time::Duration;

use bytes::Bytes;

/// Action that schedules a followup mutation through the
/// ctx-scoped scheduler. Used to assert the scheduler call
/// reaches the callbacks.
#[convex::action]
pub async fn schedule_follow_up(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    ctx.scheduler()
        .run_after(
            Duration::from_secs(60),
            NightlyCleanup,
            NightlyCleanupArgs {},
        )
        .await?;
    Ok(())
}

/// Action that schedules a followup via the absolute-timestamp
/// entry point. Exercises `Scheduler::run_at(UnixTimestamp, ...)`
/// on the action-ctx (distinct from the mutation-ctx run_at
/// that writes through VirtualSchedulerModel). Computes the
/// target timestamp relative to the action-ctx clock so mocked
/// runtimes see the mocked time.
#[convex::action]
pub async fn schedule_at_absolute_time_action(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    let ts = ctx.unix_timestamp() + Duration::from_secs(3600);
    ctx.scheduler()
        .run_at(ts, NightlyCleanup, NightlyCleanupArgs {})
        .await?;
    Ok(())
}

/// Action that schedules a followup *action* via `run_action_after`.
/// Distinct wrapper from `run_after` at the scheduler surface —
/// both methods delegate to `schedule_by_name` underneath, but
/// the typed entry points have independent type-parameter
/// constraints (`ConvexActionFunction` vs `ConvexMutationFunction`).
/// A regression that only wired the mutation entry point would
/// leave run_action_after working at compile time but silently
/// routing to the mutation side.
#[convex::action]
pub async fn schedule_follow_up_action(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    ctx.scheduler()
        .run_action_after(
            Duration::from_secs(90),
            InternalAction,
            InternalActionArgs {},
        )
        .await?;
    Ok(())
}

/// Action-ctx `run_action_at` — absolute-timestamp entry for
/// action-kind targets. Exercises the fourth scheduler entry
/// (the other three are run_after / run_at / run_action_after).
/// Each has its own type-parameter constraint + delegation
/// shape, so a regression that only wired the other three would
/// slip past their tests.
#[convex::action]
pub async fn schedule_action_at_absolute_time(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    let ts = ctx.unix_timestamp() + Duration::from_secs(120);
    ctx.scheduler()
        .run_action_at(ts, InternalAction, InternalActionArgs {})
        .await?;
    Ok(())
}

/// Mutation that schedules a follow-up mutation through the
/// mutation-ctx scheduler. Unlike the action-ctx scheduler
/// (which routes through `NativeActionCallbacks::schedule`),
/// the mutation-ctx scheduler writes directly through
/// `VirtualSchedulerModel` onto the mutation's own transaction
/// — the job commits atomically with the rest of the
/// mutation's writes. Returns the scheduled job id as a string
/// so tests can round-trip it through ConvexValue.
#[convex::mutation]
pub async fn schedule_from_mutation(ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<String> {
    let id = ctx
        .scheduler()
        .run_after(
            Duration::from_secs(30),
            NightlyCleanup,
            NightlyCleanupArgs {},
        )
        .await?;
    Ok(id.to_string())
}

/// Schedule a mutation and immediately cancel it inside the same
/// tx. Exercises `MutationScheduler::cancel(id)`, which goes
/// through `VirtualSchedulerModel::cancel` on the live tx —
/// distinct from the action-ctx scheduler's callback-routed
/// cancel. Returns a best-effort `"ok"` so the test assertion
/// has something to check for; the real contract is "doesn't
/// error".
#[convex::mutation]
pub async fn schedule_then_cancel(ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<String> {
    let id = ctx
        .scheduler()
        .run_after(
            Duration::from_secs(30),
            NightlyCleanup,
            NightlyCleanupArgs {},
        )
        .await?;
    ctx.scheduler().cancel(id).await?;
    // Idempotency: cancelling the same id again is a no-op.
    ctx.scheduler().cancel(id).await?;
    Ok("ok".to_string())
}

/// Schedule a mutation at an absolute wall-clock timestamp one
/// hour after the current runtime clock. Exercises
/// `MutationScheduler::run_at(UnixTimestamp, ...)`, which
/// internally converts to a `Duration` via
/// `self.tx.runtime().unix_timestamp()` — mocked runtimes see
/// mocked time, so deterministic tests can assert on the
/// recorded delay. Returns the job id.
#[convex::mutation]
pub async fn schedule_at_absolute_time(ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<String> {
    let ts = ctx.unix_timestamp() + Duration::from_secs(3600);
    let id = ctx
        .scheduler()
        .run_at(ts, NightlyCleanup, NightlyCleanupArgs {})
        .await?;
    Ok(id.to_string())
}

/// Internal action used as a cron target to exercise the
/// `target_kind = "action"` code path in `CronRegistry`.
#[convex::action(internal)]
pub async fn internal_action(_ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    Ok(())
}

#[convex::cron(
    name = "hourly-probe",
    schedule = "0 * * * *",
    target = "internal_action",
    target_kind = "action"
)]
fn _hourly_probe_cron() {}

/// Action that stores + retrieves + deletes a blob through
/// `ctx.storage()`. `TestCallbacks` records the three calls.
#[convex::action]
pub async fn full_storage_flow(
    ctx: &mut ActionCtx<'_, Rt>,
    content_type: String,
) -> anyhow::Result<Option<String>> {
    let id = ctx
        .storage()
        .store(Bytes::from_static(b"hello"), &content_type)
        .await?;
    let url = ctx.storage().get_url(id.clone()).await?;
    let _meta = ctx.storage().get_metadata(id.clone()).await?;
    let _deleted = ctx.storage().delete(id).await?;
    Ok(url)
}

/// Action that stores a blob and returns the `content_type` from
/// the `FileMetadata` returned by `ctx.storage().get_metadata(...)`.
/// Exercises the metadata round-trip through the callbacks layer:
/// `full_storage_flow` discards the metadata, so a regression in
/// the `FileMetadata` decoding would slip past that test.
#[convex::action]
pub async fn storage_metadata_probe(
    ctx: &mut ActionCtx<'_, Rt>,
    content_type: String,
) -> anyhow::Result<Option<String>> {
    let id = ctx
        .storage()
        .store(Bytes::from_static(b"probe"), &content_type)
        .await?;
    let meta = ctx.storage().get_metadata(id).await?;
    Ok(meta.and_then(|m| m.content_type))
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

/// Try-variant — errors if the document doesn't exist.
#[convex::query]
pub async fn try_get_todo(ctx: &mut QueryCtx<'_, Rt>, id: Id<Todo>) -> anyhow::Result<Todo> {
    ctx.db().try_get(id).await
}

/// Batch read — returns `Vec<Option<Todo>>` for every id in the
/// input list.
#[convex::query]
pub async fn get_many_todos(
    ctx: &mut QueryCtx<'_, Rt>,
    ids: Vec<Id<Todo>>,
) -> anyhow::Result<Vec<Option<Todo>>> {
    ctx.db().get_many(ids).await
}

/// Read with creation metadata — returns a tuple `(owner,
/// creation_time)` as a signal the `DocumentWithMeta<Todo>`
/// shape is wired up. Using a primitive tuple over a complex
/// struct keeps the ConvexValue round-trip simple.
#[convex::query]
pub async fn get_todo_creation_time(
    ctx: &mut QueryCtx<'_, Rt>,
    id: Id<Todo>,
) -> anyhow::Result<Option<f64>> {
    Ok(ctx
        .db()
        .get_with_meta(id)
        .await?
        .map(|doc| f64::from(doc.creation_time)))
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

/// Paginate — fetch one page and report `[page_len, is_done_as_i64,
/// has_cursor_as_i64]` as a Vec<i64> so the test can assert
/// without round-tripping the opaque cursor through ConvexValue
/// args.
#[convex::query]
pub async fn page_todos_probe(
    ctx: &mut QueryCtx<'_, Rt>,
    page_size: i64,
) -> anyhow::Result<Vec<i64>> {
    let page = ctx
        .db()
        .query::<Todo>()
        .page(None, page_size as usize)
        .await?;
    Ok(vec![
        page.items.len() as i64,
        if page.is_done { 1 } else { 0 },
        if page.cursor.is_some() { 1 } else { 0 },
    ])
}

/// Expect at most one row for `owner`; errors if more than one
/// match — exercises `.unique()`.
#[convex::query]
pub async fn unique_todo_for_owner(
    ctx: &mut QueryCtx<'_, Rt>,
    owner: String,
) -> anyhow::Result<Option<Todo>> {
    ctx.db()
        .query::<Todo>()
        .eq(TodoField::Owner, owner)?
        .unique()
        .await
}

/// Return every todo sorted by creation time descending —
/// exercises `.order(Order::Desc)`.
#[convex::query]
pub async fn todos_by_created_desc(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .order(convex_native::Order::Desc)
        .collect()
        .await
}

/// Ascending-order counterpart — exercises `.order(Order::Asc)`
/// explicitly. Asc is the default when `.order(...)` is omitted,
/// but explicitly setting it flows through the same code path
/// `.order(...)` uses for Desc, so a regression that only wired
/// the default case would slip past the existing `_by_created_desc`
/// test.
#[convex::query]
pub async fn todos_by_created_asc(ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .order(convex_native::Order::Asc)
        .collect()
        .await
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

/// List todos for an owner using an explicit
/// `.with_index(TodoIndex::ByOwner)`. Exercises the index-
/// selection branch of `TypedQueryBuilder` — a distinct code
/// path from the implicit-scan form used by `list_todos`.
/// DbFixture::new_in_memory intentionally doesn't publish the
/// schema, so the backfilled index isn't available for real
/// traversal; handlers that reach here receive an error from
/// the db layer. The test pins that error surface so a
/// regression that silently accepted missing indexes (and
/// returned wrong results) would flag.
#[convex::query]
pub async fn list_todos_with_index(
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

/// Filter with the strict `.gt` / `.lte` pair — the closed-upper
/// open-lower counterpart to `todos_in_time_range`. Lets tests
/// prove both operator directions survive the dispatch + field
/// encoding paths.
#[convex::query]
pub async fn todos_in_time_range_exclusive(
    ctx: &mut QueryCtx<'_, Rt>,
    from: f64,
    to: f64,
) -> anyhow::Result<Vec<Todo>> {
    ctx.db()
        .query::<Todo>()
        .gt(TodoField::CreatedAt, from)?
        .lte(TodoField::CreatedAt, to)?
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

/// Read a JSON body + a custom header, round-trip them through a
/// JSON response. Exercises `req.body_json(...)`, `req.header(...)`,
/// and `HttpResponse::json(...)`.
#[convex::http_action(method = "POST", path = "/api/echo")]
pub async fn echo_json(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    #[derive(serde::Deserialize)]
    struct Body {
        name: String,
    }
    let body: Body = req.body_json()?;
    let via = req.header("x-via").unwrap_or("unknown").to_string();
    Ok(HttpResponse::json(
        200,
        serde_json::json!({ "hello": body.name, "via": via }),
    ))
}

/// HTTP handler that sub-calls a native query. Exercises
/// `HttpActionCtx::run_query(...)` — the HTTP-action surface
/// onto `NativeActionCallbacks` — distinct from the already-covered
/// action-ctx and mutation-ctx sub-call paths.
#[convex::http_action(method = "GET", path = "/api/pending")]
pub async fn pending_count_http(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let owner = req.header("x-owner").unwrap_or("").to_string();
    let n = ctx
        .run_query(CountPending, CountPendingArgs { owner })
        .await?;
    Ok(HttpResponse::text(200, n.to_string()))
}

/// Echo `req.path_remainder()` back in the response body. The
/// router-level dispatch populates `routed_path` (which
/// `path_remainder()` aliases to), so a handler can see what
/// sub-path the router matched. Complement to the `body_*` /
/// header accessors covered by the ping / echo_json handlers.
#[convex::http_action(method = "GET", path = "/api/remainder")]
pub async fn path_remainder_probe(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    Ok(HttpResponse::text(
        200,
        format!("remainder={}", req.path_remainder()),
    ))
}

/// HTTP handler that issues a sub-*mutation* (as opposed to the
/// sub-query path covered by `pending_count_http`). Exercises
/// `HttpActionCtx::run_mutation_raw(...)` — the untyped surface
/// that routes through `NativeActionCallbacks::run_mutation_by_name`
/// without the typed-result `FromConvex` check, so tests can stub
/// the callback return value directly.
#[convex::http_action(method = "POST", path = "/api/create")]
pub async fn create_via_http(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let owner = req.header("x-owner").unwrap_or("anon").to_string();
    let text = req.body_text().unwrap_or_default();
    let mut map: std::collections::BTreeMap<
        convex_native_core::__private::FieldName,
        convex_native_core::__private::ConvexValue,
    > = std::collections::BTreeMap::new();
    map.insert(
        "owner".parse()?,
        convex_native_core::__private::ConvexValue::try_from(owner)?,
    );
    map.insert(
        "text".parse()?,
        convex_native_core::__private::ConvexValue::try_from(text)?,
    );
    let args = convex_native_core::__private::ConvexObject::try_from(map)?;
    let ret = ctx.run_mutation_raw("create_todo", args).await?;
    let body = match ret {
        convex_native_core::__private::ConvexValue::String(s) => s.to_string(),
        other => anyhow::bail!("create_via_http expected String from stub, got {other:?}"),
    };
    Ok(HttpResponse::text(200, body))
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
