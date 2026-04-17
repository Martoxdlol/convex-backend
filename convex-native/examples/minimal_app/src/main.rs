//! Minimal deployer-side app.
//!
//! Demonstrates the shape of a real project built against
//! `convex_native`: schema (`#[derive(ConvexDocument)]`), typed
//! queries/mutations/actions, an HTTP action, a cron, and
//! introspection through `ConvexBackend::build()`.
//!
//! Run modes:
//!
//! - `cargo run` with no env — prints the introspection envelope
//!   and exits. Good for CI / code-gen.
//! - `CONVEX_MODE=worker CONVEX_WORKER_BIND_ADDR=... cargo run` —
//!   boots a tonic worker server exposing every function declared
//!   below. Mirrors the in-tree `convex_native_distributed
//!   ::examples::worker` binary.

use std::{
    sync::Arc,
    time::Duration,
};

use convex_native::{
    convex,
    prelude::*,
    ActionCtx,
    ConvexBackend,
    ConvexDocument,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    MutationCtx,
    NoopCallbacks,
    QueryCtx,
    Rt,
};

// ── Schema ─────────────────────────────────────────────────────────

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub email: String,
    pub display_name: String,
    pub created_at: f64,
}

// ── Queries ────────────────────────────────────────────────────────

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

// ── Mutations ──────────────────────────────────────────────────────

#[convex::mutation]
pub async fn create_user(
    ctx: &mut MutationCtx<'_, Rt>,
    email: String,
    display_name: String,
) -> anyhow::Result<Id<User>> {
    let now = ctx.unix_timestamp().as_secs_f64();
    let id = ctx
        .db()
        .insert(User {
            email,
            display_name,
            created_at: now,
        })
        .await?;
    // Example: schedule a follow-up action. The scheduler inherits
    // the caller's `ExecutionContext`, so the scheduled job's logs
    // correlate with this mutation's request-id.
    ctx.scheduler()
        .run_action_after(
            Duration::from_secs(60),
            SendWelcome,
            SendWelcomeArgs { user_id: id },
        )
        .await?;
    Ok(id)
}

// ── Actions ────────────────────────────────────────────────────────

#[convex::action]
pub async fn send_welcome(
    ctx: &mut ActionCtx<'_, Rt>,
    user_id: Id<User>,
) -> anyhow::Result<()> {
    // Snapshot-pinned direct read — all reads in one action see the
    // same world.
    let Some(user) = ctx.db().get(user_id).await? else {
        return Ok(());
    };
    ctx.log().info(format!("welcome to {}!", user.email));
    // External I/O would go here (reqwest to an email provider, etc.)
    Ok(())
}

// ── HTTP action ────────────────────────────────────────────────────

#[convex::http_action(method = "POST", path = "/api/webhooks/signup")]
async fn signup_webhook(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    #[derive(serde::Deserialize)]
    struct Body {
        email: String,
        display_name: String,
    }
    let body: Body = req.body_json()?;
    let id = ctx
        .run_mutation(
            CreateUser,
            CreateUserArgs {
                email: body.email,
                display_name: body.display_name,
            },
        )
        .await?;
    HttpResponse::json(200, serde_json::json!({ "id": id.to_string() }))
        .with_header("X-Request-Id", &format!("{:?}", ctx.execution_context()))
}

// ── Cron ───────────────────────────────────────────────────────────

// Every day at 03:00 UTC, kick off a maintenance action.
#[convex::cron(name = "daily_cleanup", schedule = "0 3 * * *", target = "nightly_cleanup", target_kind = "action")]
#[allow(dead_code)]
fn _daily_cleanup() {}

#[convex::action(internal)]
pub async fn nightly_cleanup(ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    ctx.log().info("running nightly cleanup");
    Ok(())
}

// ── Entry point ────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = convex_native_distributed::read_mode_from_env();

    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(Arc::new(NoopCallbacks))
        .build()?;

    // Always print a one-line summary so `cargo run --example` gives
    // a deployer something to look at.
    eprintln!(
        "minimal_app: mode={mode:?}, {} native function(s), {} HTTP route(s)",
        built.functions().map(|r| r.len()).unwrap_or(0),
        built.router().map(|r| r.len()).unwrap_or(0),
    );

    use convex_native::distributed::ConvexMode;
    match mode {
        ConvexMode::Standalone => {
            // No runtime loop — a deployer would wire this into
            // `convex-local-backend` via the composite runner.
            println!("{}", built.describe_pretty());
        },
        ConvexMode::Worker => {
            let addr = convex_native_distributed::read_worker_bind_addr_from_env()?;
            eprintln!("minimal_app worker: listening on {addr}");
            let (mut builder, service) = convex_native_distributed::build_worker_server(
                Arc::new(convex_native::NativeFunctionRunner::from_inventory()?),
            );
            builder.add_service(service).serve(addr).await?;
        },
        ConvexMode::Conductor => {
            // Deployers building a conductor binary would pool
            // workers here. The in-tree
            // `convex_native_distributed::examples::conductor` binary
            // is the end-to-end template.
            eprintln!(
                "minimal_app: conductor mode not wired in this minimal sample; see the in-tree \
                 convex_native_distributed/examples/conductor.rs"
            );
        },
    }
    Ok(())
}
