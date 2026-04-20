//! Minimal end-to-end example of a `convex_native_core` app.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p convex_native_core --example tiny_app
//! ```
//!
//! The binary prints the collected schema, functions, and cron
//! registrations as JSON. It doesn't boot a real backend — that
//! requires linking the whole V8 + persistence stack via
//! `convex-local-backend` — but it exercises the full developer
//! surface (derive macros, attribute macros, ConvexBackend builder,
//! validation, introspection) in isolation, which is useful both
//! as a sanity check and as a runnable starting point a new user
//! can crib from.
//!
//! To see the same registrations serve real traffic, link them
//! into `convex-local-backend` (the
//! `convex_native_backend::CompositeFunctionRunner` picks up any
//! statically-registered native functions automatically) or into
//! the `convex_native_distributed::examples/worker` binary for a
//! split-topology deployment.

use bytes::Bytes;
use convex_native_core::{
    convex,
    prelude::*,
    ActionCtx,
    ConvexBackend,
    ConvexDocument,
    ConvexEnum,
    ConvexNested,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    MutationCtx,
    NoopCallbacks,
    QueryCtx,
    Rt,
};

// ── Schema ────────────────────────────────────────────────────────

#[derive(ConvexEnum, Debug, Clone)]
pub enum Tier {
    Free,
    Pro,
}

#[derive(ConvexNested, Debug, Clone)]
pub struct Profile {
    pub tier: Tier,
    pub display_name: String,
}

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub email: String,
    pub profile: Profile,
    pub created_at: f64,
}

// ── Functions ─────────────────────────────────────────────────────

#[convex::query]
pub async fn get_by_email(
    ctx: &mut QueryCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Option<User>> {
    ctx.db()
        .query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, email)?
        .unique()
        .await
}

#[convex::mutation]
pub async fn create(ctx: &mut MutationCtx<'_, Rt>, email: String) -> anyhow::Result<Id<User>> {
    let now = ctx.unix_timestamp().as_secs_f64();
    let id = ctx
        .db()
        .insert(User {
            email,
            profile: Profile {
                tier: Tier::Free,
                display_name: "anon".into(),
            },
            created_at: now,
        })
        .await?;
    ctx.log().info(format!("created user {id}"));
    Ok(id)
}

#[convex::action]
pub async fn send_welcome(ctx: &mut ActionCtx<'_, Rt>, email: String) -> anyhow::Result<()> {
    // Typed sub-query; routes through the attached callbacks in a
    // real deployment.
    let _ = ctx.run_query(GetByEmail, GetByEmailArgs { email }).await;
    let _ = ctx
        .storage()
        .store(Bytes::from_static(b"welcome"), "text/plain")
        .await;
    Ok(())
}

#[convex::mutation(internal)]
pub async fn nightly_cleanup(_ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<()> {
    Ok(())
}

#[convex::cron(
    name = "nightly-cleanup",
    schedule = "0 3 * * *",
    target = "nightly_cleanup"
)]
#[allow(dead_code)]
fn _nightly_cleanup_cron() {}

#[convex::http_action(method = "GET", path = "/api/health")]
#[allow(dead_code)]
async fn health(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    _req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    Ok(HttpResponse::json(200, serde_json::json!({"status": "ok"})))
}

// ── main ─────────────────────────────────────────────────────────

fn main() -> anyhow::Result<()> {
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(std::sync::Arc::new(NoopCallbacks))
        .build()?;

    // Cross-check cron targets exist.
    built.validate()?;

    eprintln!("startup: {}", built.summary());
    println!("{}", built.describe_pretty());
    Ok(())
}
