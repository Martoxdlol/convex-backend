# convex_native — Quickstart

A condensed tour of the developer-facing surface of the
`convex_native` crate. This is the 10-minute walkthrough — it aims
for "you have a running app" fast and leaves corners uncovered.

- For the full per-topic feature reference, read **`USAGE.md`**.
- For what's shipped vs. missing, read **`STATUS.md`**.
- For porting a JS app to Rust, read **`MIGRATION.md`**.
- For the target architecture + phase breakdown, read
  **`DISTRIBUTED_PLAN.md`**.

## Adding the dep

```toml
# Cargo.toml
[dependencies]
convex_native = { path = "../convex-backend/crates/convex_native" }
anyhow = "1"
tokio = { version = "1", features = ["full"] }
```

Every derive and attribute macro is re-exported through
`convex_native` — you don't need a separate `convex_macro` dep.

## 1. Define your schema

```rust
use convex_native::prelude::*;

#[derive(ConvexEnum, Debug, Clone)]
pub enum Tier {
    Free,
    Pro,
    Enterprise,
}

#[derive(ConvexNested, Debug, Clone)]
pub struct Profile {
    pub tier: Tier,
    pub display_name: String,
}

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
#[convex(index(name = "by_tier", fields = ["profile.tier"]))]
pub struct User {
    pub email: String,
    pub profile: Profile,
    pub created_at: f64,
}
```

What the derive gets you, for `User`:

- `impl ConvexDocument for User`
- `UserField` enum — `{ Email, Profile, CreatedAt }` with
  `as_str() -> "email" | ...`
- `UserIndex` enum — `{ ByEmail, ByTier }` with
  `as_str()` and `fields()`
- `UserPatch` — every field wrapped in `Option<_>`, `Default`
- `UserWithId` — `{ id: Id<User>, doc: User }` with `Deref`
- `ToConvex` / `FromConvex` impls so `User` can be a function
  argument or return value
- An `inventory::submit!` of the `TableRegistration` — picked up
  automatically by `NativeSchema::collect()` (or the builder)

Index fields are validated at compile time: referencing a field
that doesn't exist on the struct fails with a clear error.

### Text + vector search

```rust
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "docs")]
#[convex(text_index(
    name = "by_body",
    search_field = "body",
    filter_fields = ["category"]
))]
#[convex(vector_index(
    name = "by_embedding",
    vector_field = "embedding",
    dimensions = 1536,
    filter_fields = ["category"]
))]
pub struct Doc {
    pub body: String,
    pub category: String,
    pub embedding: Vec<f64>,
}
```

### Tagged unions

```rust
#[derive(ConvexUnion, Debug, Clone)]
#[convex(tag = "type")]
pub enum Notification {
    Email { address: String },
    Sms { phone: String },
    #[convex(rename = "push_notification")]
    Push { device_token: String },
}
// Serialized as { "type": "email", "address": "..." }
```

## 2. Write functions

```rust
use convex_native::{convex, prelude::*, MutationCtx, QueryCtx, ActionCtx, Rt};

#[convex::query]
pub async fn get_user_by_email(
    ctx: &mut QueryCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Option<User>> {
    ctx.db()
        .query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, email)?
        .first()
        .await
}

#[convex::mutation]
pub async fn create_user(
    ctx: &mut MutationCtx<'_, Rt>,
    email: String,
    tier: Tier,
) -> anyhow::Result<Id<User>> {
    ctx.db()
        .insert(User {
            email,
            profile: Profile {
                tier,
                display_name: "anon".into(),
            },
            created_at: 0.0,
        })
        .await
}

#[convex::action]
pub async fn send_welcome(
    ctx: &mut ActionCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<()> {
    // Typed sub-calls go through `ActionCallbacks` in the backend
    // adapter. In tests you'll use `convex_native::testing::TestCallbacks`.
    let user: Option<User> = ctx
        .run_query(GetUserByEmail, GetUserByEmailArgs { email })
        .await?;
    let _ = user;
    Ok(())
}
```

Each attribute macro generates:

- The original async fn (still callable as plain Rust).
- A hidden handler fn pointer registered in the inventory.
- A PascalCase ZST marker (`GetUserByEmail`) implementing
  `ConvexQueryFunction` / `ConvexMutationFunction` /
  `ConvexActionFunction`, with `Args` / `Output` associated types.
- A PascalCase `...Args` struct (one field per non-`ctx`
  parameter) that `ToConvex` / `FromConvex` round-trip.

## 3. Schedulers, storage, HTTP actions

```rust
// Inside a mutation:
ctx.scheduler()
    .run_after(
        Duration::from_secs(60),
        SendWelcome,
        SendWelcomeArgs { email: email.clone() },
    )
    .await?;

// Absolute wall-clock scheduling — `run_at` (mutations) and
// `run_action_at` (actions) take a `UnixTimestamp` and compute the
// delay from `SystemTime::now()`. Past timestamps clamp to "now".
let deadline = UnixTimestamp::from_secs_f64(1_893_456_000.0).unwrap(); // 2030-01-01
ctx.scheduler()
    .run_at(deadline, SendWelcome, SendWelcomeArgs { email })
    .await?;

// Inside an action:
let id = ctx
    .storage()
    .store(Bytes::from(body), "image/png")
    .await?;

// HTTP action:
#[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
async fn stripe_webhook(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let _body = req.body_bytes();
    Ok(HttpResponse::json(200, serde_json::json!({"ok": true})))
}
```

## 4. Assemble with `ConvexBackend`

```rust
use convex_native::ConvexBackend;
use std::sync::Arc;

let callbacks: Arc<dyn NativeActionCallbacks> =
    /* backend adapter supplies the real one */;

let built = ConvexBackend::new()
    .with_native_functions()
    .with_native_schema()
    .with_http_routes()
    .with_callbacks(callbacks)
    .build()?;

// `built.runner`, `built.schema`, `built.router` carry the collected app.
// `built.warmup_plan()` lists every declared index for startup cache priming.
```

## 5. Unit-testing your functions

```rust
use convex_native::testing::{TestCallbacks, CallRecord};

#[tokio::test]
async fn welcome_action_sub_calls_query() {
    let (cb, history) = TestCallbacks::new()
        .on_query("get_user_by_email", |_args| {
            Ok(ConvexValue::Null)
        })
        .build();

    let runner = Arc::new(
        convex_native::NativeFunctionRunner::from_inventory()?,
    );
    runner
        .run_action_with_callbacks(
            "send_welcome",
            TableNamespace::Global,
            args_obj,
            cb,
        )
        .await?;

    assert_eq!(
        history.count(|r| matches!(
            r,
            CallRecord::Query { name, .. } if name == "get_user_by_email",
        )),
        1,
    );
}
```

## 6. Returning user-facing errors

Tag errors with [`convex_native::errors`][errors] so the HTTP / RPC
layer maps them to the right status code. Without the tag, any
returned `anyhow::Error` is surfaced to the client as a generic 500
— which is the right default for internal bugs but wrong for
actionable user errors.

```rust
use convex_native::errors;

#[convex::mutation]
async fn invite(ctx: &mut MutationCtx, email: String) -> Result<()> {
    if !ctx.auth().is_authenticated() {
        return Err(errors::unauthenticated(
            "MissingToken",
            "An auth token is required to send invites.",
        )
        .into());
    }
    if ctx.db().query::<Invite>().eq(InviteField::Email, email.clone())?
        .first().await?.is_some() {
        return Err(errors::conflict(
            "DuplicateInvite",
            "That email already has a pending invite.",
        )
        .into());
    }
    // ...
    Ok(())
}
```

Helpers: `bad_request` (400), `unauthenticated` (401), `forbidden`
(403), `not_found` (404), `conflict` (409), `rate_limited` (429),
`overloaded` (503). Prefer a bare `anyhow::bail!` over
`errors::overloaded` unless a specific custom message helps the
caller recover.

[errors]: https://docs.rs/convex_native/latest/convex_native/errors/index.html

## Operational knobs

All optional, configured on `NativeFunctionRunner`:

```rust
let runner = NativeFunctionRunner::from_inventory()?
    .with_metrics(Arc::new(CountingMetrics::new()))
    .with_default_timeout(Duration::from_secs(30))
    .with_circuit_breaker(Arc::new(CircuitBreaker::new(
        CircuitBreakerConfig::default(),
    )));
```

- Metrics: attach any `NativeMetricsSink` — latency + `Ok`/`Err`
  recorded per call.
- Timeouts: wraps every handler in `tokio::time::timeout`.
- Circuit breaker: opens on consecutive failures, half-open probe
  after cooldown.
- Drain: `runner.begin_drain()` rejects new calls;
  `runner.await_drain(Duration::from_secs(10)).await` waits for
  in-flight to finish.

## Schema evolution

```rust
let old_schema = /* read from deployment */;
let new_schema = NativeSchema::collect()?;
for change in convex_native::diff_schemas(&old_schema, &new_schema) {
    if change.is_destructive() {
        eprintln!("destructive: {change:?}");
    }
}
```

## Running against a real backend

The `crates/convex_native_backend/` adapter bridges everything in
this crate onto the backend's `FunctionRunner` trait and on to
`udf::ActionCallbacks`. `crates/local_backend/src/lib.rs` now wraps
the V8 runner in a `convex_native_backend::CompositeFunctionRunner`
ahead of `Application::new`, so any `#[convex::query/mutation/action]`
linked into the `convex-local-backend` binary is dispatched
natively with zero extra wiring. Raw-byte uploads from
`ctx.storage().store(...)` route through `FileStorage::store_file`
when the composite is built with `.with_file_storage(fs)`.

## Deploying in a distributed topology

Prebuilt **backend** image coordinates OCC, subscriptions, and
committing; a pool of **workers** (the image you build from
your crate) executes your native handlers and routes reads /
writes / action sub-calls back over gRPC.

All phases of `DISTRIBUTED_PLAN.md` are shipped in source.
Pull the two ready-to-deploy Dockerfiles:

```sh
# Backend (carries no deployer code — one-time build).
docker build -f convex-native/examples/deploy/docker/Dockerfile.backend \
  -t getconvex/convex-backend:dev .

# Worker (links your #[convex::*] handlers; rebuild on code change).
docker build -f convex-native/examples/deploy/docker/Dockerfile.worker \
  -t myco/my-worker:dev .
```

Then follow the 6-step docker-compose-style bring-up recipe in
`DEPLOYMENT.md` §"Bringing up Topology B locally" (worker auto-
registers via `CONVEX_BACKEND_ENDPOINT`; admin HTTP surface on
a loopback port exposes pool snapshot / floor / drain /
kind_preference). For kubernetes, apply both
`backend-deployment.yaml` + `worker-deployment.yaml` from
`convex-native/examples/deploy/kubernetes/`.

The monolith topology (`local_backend` linked as a library
with your handlers) still works for local development / small
deployments — see `STANDALONE.md`.

See `USAGE.md` §19 for the complete env-var matrix across both
sides.

## Feature coverage

**`STATUS.md` is authoritative.** Every phase of
`DISTRIBUTED_PLAN.md` is shipped in source, and the
framework-level gaps that earlier revisions of this file called
out have all landed:

- **Document-shape validation** — `#[derive(ConvexDocument)]`
  emits `document_type: Some(DocumentSchema::Union(...))` built
  from each field's `ConvexSchema::validator()`. The database
  enforces the shape on write; see `schema_type.rs`.
- **Mutation-scoped scheduler** — `MutationCtx::scheduler()`
  returns a `MutationScheduler` that writes scheduled jobs
  directly into the mutation's `Transaction<RT>` through
  `VirtualSchedulerModel`. Schedules commit atomically with the
  rest of the mutation's writes, matching JS
  `ctx.scheduler.runAfter` semantics.
- **`ActionCtx::db()`** — reads route through
  `NativeActionCallbacks::read_document_at_snapshot`, which opens
  a short-lived read-only tx at the action's pinned snapshot
  timestamp. Multiple `ctx.db().get(...)` calls inside one action
  see one consistent world. Writes from an action still flow
  through `ctx.run_mutation(...)` so they commit under the
  backend's Committer.

At the distributed level: all phases 1–7 of
`DISTRIBUTED_PLAN.md` have shipped, including the previously-
deferred live-DB fixture assertions (2.8b + 4.6b), the CI/release
pipeline (5.3), and the reference JS worker binary (6.3).

Non-indexed filters (`.eq(Field, v)` without a preceding
`.with_index(...)`) **are** supported: they lower to a
`FullTableScan` + stacked `QueryOperator::Filter(...)` predicates.
Prefer an indexed lookup when one exists — the filter path reads
every row.
