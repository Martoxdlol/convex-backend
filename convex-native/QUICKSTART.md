# convex_native — Quickstart

A condensed tour of the developer-facing surface of the `convex_native`
crate. For the rationale behind the design, read `native-rust-functions.md`.
For phase-by-phase progress, read `README.md`.

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
        SendWelcomeArgs { email },
    )
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

## What's not yet wired

The list in `README.md` tracks this accurately. In short: everything
described in this quickstart compiles and runs today; the one thing
you need that isn't in-crate yet is the **backend adapter** that
implements `NativeActionCallbacks` against a real
`database::Transaction` + `udf::ActionCallbacks`. That adapter lives
in a future `crates/convex_native_backend` crate — see
`COMPOSITE_RUNNER.md` for the reference implementation shape.
