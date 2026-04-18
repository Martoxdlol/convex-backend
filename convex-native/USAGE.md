# Using `convex_native`

Comprehensive reference for every feature the crate exposes. Read
this when you want the full picture; read `QUICKSTART.md` first if
you're building something for the first time and want a narrow,
step-by-step walkthrough.

- **Porting from JS?** `MIGRATION.md` has JS↔Rust side-by-sides.
- **Target architecture** (the "where does this run" answer):
  `DISTRIBUTED_PLAN.md` — all phases shipped in source.
  This file describes the *developer surface*, which is stable
  across the monolith + distributed topologies.
- **Current state of the tree**: `STATUS.md`.
- **Operational / deploy guide**: `DEPLOYMENT.md`.
- **Monolith alternative** (`local_backend` as a library):
  `STANDALONE.md`.

## Table of contents

1. [Adding the dependency](#1-adding-the-dependency)
2. [Defining schema](#2-defining-schema)
3. [Writing functions](#3-writing-functions)
4. [Context APIs](#4-context-apis)
5. [Typed queries](#5-typed-queries)
6. [Sub-calls from actions](#6-sub-calls-from-actions)
7. [Scheduler](#7-scheduler)
8. [Storage](#8-storage)
9. [HTTP actions](#9-http-actions)
10. [Crons](#10-crons)
11. [Error handling](#11-error-handling)
12. [Logging](#12-logging)
13. [Assembling with `ConvexBackend`](#13-assembling-with-convexbackend)
14. [Introspection](#14-introspection)
15. [Schema evolution](#15-schema-evolution)
16. [Operational knobs](#16-operational-knobs)
17. [Testing](#17-testing)
18. [Running against a real backend](#18-running-against-a-real-backend)
19. [Distributed topology](#19-distributed-topology)
20. [Where the crate stops](#20-where-the-crate-stops)

## 1. Adding the dependency

```toml
# Cargo.toml
[dependencies]
convex_native = { path = "crates/convex_native" }
# or, once published:
# convex_native = "0.1"
```

`convex_native` re-exports `convex_macro`, so you do not depend on
it directly. The crate is `isolate`-free and builds without `rush
install`; the `convex_native_backend` adapter does pull in `isolate`
and needs the full `rush install` in `npm-packages/`.

## 2. Defining schema

Derive `ConvexDocument` on each table:

```rust
use convex_native::prelude::*;

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub email: String,
    pub display_name: String,
    pub created_at: f64,
}
```

The macro generates:

| Item              | Purpose                                           |
|-------------------|---------------------------------------------------|
| `impl ConvexDocument` | table name, `to_convex_object`, `from_convex_object`, `table_definition()` |
| `UserField`       | One variant per field, `impl FieldReference`.     |
| `UserIndex`       | One variant per declared index, `impl IndexReference` (uninhabited when none declared). |
| `UserPatch`       | Each field wrapped in `Option`, `Default`, `impl ConvexPatch`. |
| `UserWithId`      | `{ id: Id<User>, doc: User }`, derefs to `User`.  |
| `inventory::submit!` | `TableRegistration` collected by `NativeSchema::collect()`. |

Field-path validation is compile-time: `#[convex(index(..., fields
= ["nonexistent"]))]` fails with a clear error naming the declared
fields.

### Text + vector search

```rust
#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(text_index(
    name = "by_body",
    search_field = "body",
    filter_fields = ["channel"]
))]
#[convex(vector_index(
    name = "by_embedding",
    vector_field = "embedding",
    dimensions = 1536,
    filter_fields = ["channel"]
))]
pub struct Message {
    pub channel: String,
    pub body: String,
    pub embedding: Vec<f64>,
}
```

Fields land in `TableDefinition::text_indexes` /
`TableDefinition::vector_indexes` with the same compile-time
field-name validation as database indexes.

### Nested objects and enums

```rust
#[derive(ConvexEnum, Debug, Clone)]
pub enum Tier { Free, Pro, Enterprise }
// Wire form: "free" / "pro" / "enterprise". Override per variant
// with #[convex(rename = "...")].

#[derive(ConvexNested, Debug, Clone)]
pub struct Profile {
    pub tier: Tier,
    pub display_name: String,
}
```

`ConvexNested` emits `ToConvex` / `FromConvex` but no
`TableRegistration` — use it for embedded shapes.

### Tagged unions

```rust
#[derive(ConvexUnion, Debug, Clone)]
#[convex(tag = "kind")]
pub enum Notification {
    Email { to: String },
    Push { token: String },
}
```

Default discriminant field is `"type"`; override with
`#[convex(tag = "kind")]`. Per-variant `#[convex(rename = "…")]`
customises the wire value. Duplicate tags fail at compile time.

### Collecting the schema

```rust
let schema: common::schemas::DatabaseSchema = convex_native::NativeSchema::collect()?;
```

Enumerates every derived table in the binary via `inventory`.

### Write-time document shape validation

`#[derive(ConvexDocument)]` emits `TableDefinition::document_type`
populated from the struct's Rust fields, so the database layer
rejects shape-violating writes before they hit disk. Reflection
rules:

- Primitives (`String`, `i64`, `f64`, `bool`) → their matching
  `Validator` (`String`, `Int64`, `Float64`, `Boolean`).
- `Vec<T>` → `Validator::Array(T)`. `Vec<u8>` → `Validator::Bytes`
  (special-cased inside the derive).
- `Option<T>` → `Validator::Union(Null, T)` **and** the field is
  marked optional (`v.optional(...)` in JS parlance). Absent fields
  and present-as-Null both pass.
- `BTreeMap<String, V>` → `Validator::Record(String, V)`.
- `Id<T>` → `Validator::Id(T::table_name())`. Cross-table Id mixups
  fail at validation time as well as compile time.
- Nested `#[derive(ConvexNested)]` / `ConvexEnum` / `ConvexUnion`
  → emit their own `ConvexSchema` impl so they compose through
  `Vec<...>` / `Option<...>` / `BTreeMap<...>` without boilerplate.

If you need an explicit "any shape" field, type it as
`value::ConvexValue` — `ConvexSchema` maps that to `Validator::Any`.

## 3. Writing functions

### Query / Mutation / Action

```rust
use convex_native::{convex, prelude::*, QueryCtx, MutationCtx, ActionCtx, Rt};

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
pub async fn create(
    ctx: &mut MutationCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Id<User>> {
    ctx.db().insert(User { email, display_name: "anon".into(), created_at: 0.0 }).await
}

#[convex::action]
pub async fn send_welcome(
    ctx: &mut ActionCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<()> {
    let _user = ctx.run_query(GetByEmail, GetByEmailArgs { email }).await?;
    Ok(())
}
```

### What the macro emits

For each `#[convex::query]` / `mutation` / `action` on `foo_bar`:

1. The original async fn (still callable as plain Rust).
2. A hidden type-erased handler fn registered via `inventory`.
3. A PascalCase `FooBar` ZST marker implementing
   `ConvexQueryFunction` / `ConvexMutationFunction` /
   `ConvexActionFunction` with associated types `Args` and `Output`
   plus `fn name() -> &'static str`.
4. A PascalCase `FooBarArgs` struct with one field per non-`ctx`
   parameter, deriving `ToConvex` / `FromConvex`.

### Modifiers

```rust
#[convex::mutation(internal)]                 // internal-only (not callable from external clients)
#[convex::action(timeout_ms = 5_000)]         // per-function timeout override
#[convex::mutation(internal, timeout_ms = 30_000)]  // compose modifiers
```

`timeout_ms = 0` means "inherit from runner default" (the macro's
default). `internal` lands on
`NativeFunctionRegistration::is_internal`; backends reject external
client calls but internal sub-calls still work.

## 4. Context APIs

| Ctx | Source | Capabilities |
|-----|--------|--------------|
| `QueryCtx<'tx, Rt>` | `ctx.db()` | Read DB, `auth`, `unix_timestamp`, `log` |
| `MutationCtx<'tx, Rt>` | as query + `insert/patch/replace/delete`, `scheduler` (tx-scoped), `tx()` escape-hatch |
| `ActionCtx<'a, Rt>` | no tx; `db()` (snapshot reads), sub-calls, `scheduler`, `storage`, `run_query/mutation/action`, `log` |
| `HttpActionCtx<'a, Rt>` | wraps `ActionCtx`; same surface + HTTP req/resp types |

Shared methods across all ctxs:

- `ctx.auth() -> AuthInfo<'_>` — `is_authenticated()`, `is_admin()`,
  `is_system()`, `user_identity()` (JS-analogue
  `ctx.auth.getUserIdentity()`), `subject()` shortcut for the JWT
  `sub` claim, `.raw()` for the escape-hatch to
  `keybroker::Identity`.
- `ctx.unix_timestamp() -> UnixTimestamp` — runtime-sourced clock,
  so tests driving a mock runtime see the mocked value.
- `ctx.log() -> Logger<'_>` — `.debug/.info/.warn/.error(msg)` pushes
  a line into the ctx's `LogBuffer`; the runner drains the buffer
  after the handler returns and surfaces it through the backend's
  log-streaming path.
- `ctx.rng_u64() -> u64` / `ctx.rng_fill(&mut [u8])` — deterministic
  randomness seeded from the outcome's `rng_seed`. Re-running with
  the same seed produces the same sequence, so retried handlers
  stay reproducible. Calling either flips `observed_rng` on the
  ctx; the runner drains that bit into
  `UdfOutcome::observed_rng`.
- `ctx.execution_context() -> Option<&ExecutionContext>` — the
  enclosing request's context (request-id / execution-id /
  parent-scheduled-job chain). `None` in tests that build a ctx
  without attaching one. Use the returned reference's
  `request_id` / `execution_id` for structured logs that should
  correlate with the enclosing request. The mutation scheduler
  picks the same context up automatically, so scheduled jobs
  chain off the caller's id without the handler doing anything.

## 5. Typed queries

`ctx.db().query::<T>()` returns a `TypedQueryBuilder<'_, 'tx, Rt, T>`
with a type-checked API:

```rust
let users: Vec<User> = ctx
    .db()
    .query::<User>()
    .with_index(UserIndex::ByEmail)          // optional — if absent, falls back to a full-table scan with post-scan filters
    .gte(UserField::CreatedAt, 1_000.0)?
    .lt(UserField::CreatedAt, 2_000.0)?
    .order(Order::Desc)
    .limit(100)
    .collect()
    .await?;
```

Filter operators: `.eq`, `.gt`, `.gte`, `.lt`, `.lte`. Each takes
`T::Field` + a value that converts into `ConvexValue`.

Terminals:

| Terminal         | Returns                    | Notes                                     |
|------------------|----------------------------|-------------------------------------------|
| `.collect()`     | `Vec<T>`                   | reads the full range                      |
| `.first()`       | `Option<T>`                | first match or none                       |
| `.unique()`      | `Option<T>`                | errors if more than one matches           |
| `.take(n)`       | `Vec<T>`                   | hard cap                                  |
| `.count()`       | `usize`                    |                                           |
| `.page(cur, n)`  | `TypedPage<T>`             | bounded at `n` rows; feed `cursor` back in|

`TypedPage<T>` has `items`, `cursor: Option<Cursor>`, `is_done`.
Pass `cursor` unchanged into the next call (cursors are
query-fingerprinted by the database; mixing them across queries
errors).

### Reading single docs

```rust
let user: Option<User> = ctx.db().get(id).await?;
let user: User = ctx.db().try_get(id).await?;           // errors if not found
let exists: bool = ctx.db().exists(id).await?;

let with_meta: Option<DocumentWithMeta<User>> =
    ctx.db().get_with_meta(id).await?;
// DocumentWithMeta { id, creation_time, doc }; derefs to `&T`.

let batch: Vec<Option<User>> = ctx.db().get_many(ids).await?;

// Validate an untrusted id string against the target table:
let maybe_id: Option<Id<User>> = ctx.db().normalize_id(raw_string);
```

`normalize_id` parses the string and checks the embedded table
number against `User::table_name()`. Returns `None` on malformed
input or a different table — the typed counterpart of
`ctx.db.normalizeId("users", raw)` in JS.

## 6. Sub-calls from actions

Actions don't own a transaction, but they can still read at the
request's pinned snapshot and sub-call queries, mutations, or other
actions.

```rust
// Direct read at the action's pinned snapshot (composite backend
// only — distributed workers and the noop stub bail):
let user: Option<User> = ctx.db().get(id).await?;
let user: User = ctx.db().try_get(id).await?;
let batch: Vec<Option<User>> = ctx.db().get_many(ids).await?;

// Typed sub-calls (compile-time-checked):
let user: Option<User> = ctx.run_query(GetByEmail, GetByEmailArgs { email }).await?;
let id: Id<User> = ctx.run_mutation(Create, CreateArgs { email }).await?;
let sent: bool = ctx.run_action(SendEmail, SendEmailArgs { .. }).await?;

// Untyped (dynamic dispatch — prefer the typed form):
let v: ConvexValue = ctx.run_query_raw("get_by_email", args_obj).await?;
```

`ctx.db()` is a thin convenience over `NativeActionCallbacks::read_document_at_snapshot`
— backed by `BackendCallbacks` it opens a short-lived read-only tx
at the pinned snapshot ts for each read, so sequential `get(...)`
calls observe a consistent world. Typed sub-calls skip name
resolution entirely and preserve the
dispatch and JS-target fallback.

When the name resolves to a registered native function, the
`BackendCallbacks` adapter short-circuits: it opens a fresh
`Transaction<Rt>` on the composite's `Database<RT>`, runs the
handler, commits (for mutations), and returns the result.

Query sub-calls within a single action share a snapshot `ts`:
`dispatch_native_action` pins `database.now_ts_for_reads()` once
and threads it into `BackendCallbacks::with_snapshot_ts(...)`, so
every native query sub-call inside one action opens its
transaction at the same ts. Mutations still commit at a fresh ts.

## 7. Scheduler

```rust
use std::time::Duration;
use common::runtime::UnixTimestamp;

// Relative delay:
let job_id = ctx.scheduler()
    .run_after(Duration::from_secs(60), SendWelcome, SendWelcomeArgs { email })
    .await?;

// Absolute wall-clock timestamp:
let deadline = UnixTimestamp::from_secs_f64(1_893_456_000.0).unwrap(); // 2030-01-01
ctx.scheduler().run_at(deadline, SendWelcome, SendWelcomeArgs { email }).await?;

// Actions: `run_action_after` / `run_action_at` with an action marker.
ctx.scheduler()
    .run_action_after(Duration::from_secs(30), PollExternal, PollExternalArgs { .. })
    .await?;

// Cancel (idempotent):
ctx.scheduler().cancel(job_id).await?;
```

Caveats:

- **`run_at` honours the runtime clock** when the scheduler is
  attached to a backend that wires `Database<RT>` (the standard
  composite path). `BackendCallbacks::unix_timestamp_now()` returns
  `database.runtime().unix_timestamp()`, so mocked-clock tests see
  the mocked time. `NoopCallbacks` and JS-only adapters fall back
  to `SystemTime::now()` — pure unit tests with `NoopCallbacks`
  should compute delays from `ctx.unix_timestamp()` and use
  `run_after(Duration, ...)` directly.
- **Mutation vs. action scheduler semantics differ internally.**
  `MutationCtx::scheduler()` writes through `VirtualSchedulerModel`
  on the mutation's own transaction, so scheduled jobs commit
  atomically with the rest of the mutation's writes. The
  action-scoped scheduler goes through `NativeActionCallbacks`
  (callback-based) because actions don't own a transaction. The
  developer-facing API is the same on both.
- **Past timestamps clamp to "now"** (delay = `Duration::ZERO`).

## 8. Storage

Available inside actions and HTTP actions:

```rust
let id: StorageId = ctx.storage()
    .store(Bytes::from(body), "image/png")
    .await?;
let url: Option<String> = ctx.storage().get_url(id.clone()).await?;
let meta: Option<FileMetadata> = ctx.storage().get_metadata(id.clone()).await?;
let existed: bool = ctx.storage().delete(id).await?;
```

`FileMetadata { content_type, size, sha256 }` mirrors the JS
`ctx.storage.getMetadata` shape. `BackendCallbacks` implements it
via `ActionCallbacks::storage_get_file_entry`; `NoopCallbacks`
bails.

Wired through `NativeActionCallbacks::storage_store`. The
`BackendCallbacks` adapter forwards `store` straight into
`FileStorage::store_file` when the composite is built with
`.with_file_storage(fs)`; without it, the call bails with a clear
"requires a FileStorage handle" error.

## 9. HTTP actions

```rust
use convex_native::{HttpActionCtx, HttpRequest, HttpResponse};

#[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
pub async fn stripe_webhook(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let payload = req.body_text()?;
    ctx.run_mutation(RecordWebhook, RecordWebhookArgs { payload }).await?;
    Ok(HttpResponse::new(204))
}
```

`HttpRequest` helpers: `.header(name)`, `.body_bytes()`,
`.body_text()`, `.body_json::<T>()`, `.path_remainder()`.

`HttpResponse` builders: `::new(status)`, `::json(status, value)`,
`::text(status, body)`, `::redirect(status, location)` plus
chainable `.with_header(name, value)?` / `.with_body(bytes)`.

`HttpRouter::collect()` enumerates the registered routes;
`lookup(method, path)` returns the matching
`HttpRouteRegistration` (case-sensitive on the method).

### How native HTTP actions are served

`local_backend` mounts a `NativeHttpDispatcher` at boot (in
`make_app`, after `Application::new`) that holds the collected
`HttpRouter`, the `NativeFunctionRunner`, and the backend's
`ActionCallbacks` + `Database` + `FileStorage` handles. Requests
arriving at the backend's `/http/*` surface consult the
dispatcher first: an exact `(method, path)` match runs the
native handler directly, any sub-calls inside the handler
(`ctx.run_mutation`, `ctx.scheduler`, `ctx.storage`) flow
through a `BackendCallbacks` pinned to the request's snapshot
timestamp, and the response body is streamed back through the
same `HttpActionResponseStreamer` the JS path uses. A miss
falls through to the JS `execute_http_action` path unchanged,
so mixed deployments (some routes in Rust, others in JS)
work without extra configuration. Installing the dispatcher is
gated on `HttpRouter::collect().len() > 0 || admission_pool` —
JS-only deployments pay no overhead.

Under the **Phase-5 distributed topology** the backend image
carries zero native inventory; handlers live in workers. When
`CONVEX_ADMISSION_BIND_ADDR` is set, the dispatcher also takes
a reference to the admission `WorkerPool`. An incoming HTTP
request that misses the local router is looked up in
`WorkerPool::eligible_for_http(method, path)`; the first
eligible worker receives a `FunctionExecutionService::Execute`
call with `udf_type=HttpAction` and the request bytes in
`ExecuteRequest.http_request`. The worker's
`FunctionExecutionServer` runs the handler through
`NativeFunctionRunner::run_http_action_with_callbacks` with a
`BackendCallbackClient` that routes `ctx.run_mutation`,
`ctx.storage`, and `ctx.scheduler` calls back through the
backend's Committer over
`CONVEX_BACKEND_CALLBACK_ENDPOINT`. The backend decodes the
response's `http_response` field and streams bytes to the HTTP
client. Handler-level errors come back as a 500 with the
`anyhow::Error` display form so a user sees a well-formed
reply.

## 10. Crons

Recurring scheduled jobs via an inventory-collected attribute. The
schedule string is parsed at macro-expansion time with `saffron`,
so typos fail the build.

```rust
#[convex::mutation(internal)]
async fn nightly_cleanup(ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<()> { .. }

#[convex::cron(
    name = "nightly-cleanup",
    schedule = "0 3 * * *",
    target = "nightly_cleanup",
)]
fn _nightly_cleanup_cron() {}
```

The attribute attaches to a placeholder item (name doesn't
matter). `ConvexBackend::new().with_crons().build()` collects every
registration into `BuiltBackend::crons`. Call
`BuiltBackend::validate()` at startup so misconfigured crons (e.g.
`target` naming an unknown function, or kind mismatch) crash the
binary rather than silently skipping. The schedule itself is
driven by the backend adapter, not by this crate.

## 11. Error handling

Tag user-facing errors with `ErrorMetadata` so the HTTP / RPC
layer maps them to the right status code:

```rust
use convex_native::errors;

if !ctx.auth().is_authenticated() {
    return Err(errors::unauthenticated(
        "MissingToken",
        "An auth token is required.",
    ).into());
}
```

Available helpers:

| Helper                              | HTTP | Use for                                        |
|-------------------------------------|------|------------------------------------------------|
| `errors::bad_request(short, msg)`   | 400  | Invalid input, malformed payload.              |
| `errors::unauthenticated(..)`       | 401  | Missing or invalid credentials.                |
| `errors::forbidden(..)`             | 403  | Authenticated but not allowed.                 |
| `errors::not_found(..)`             | 404  | Resource does not exist.                       |
| `errors::conflict(..)`              | 409  | Unique-constraint / optimistic-locking clash.  |
| `errors::rate_limited(..)`          | 429  | Caller exceeded a rate limit.                  |
| `errors::overloaded(..)`            | 503  | Defensive backpressure — retry later.          |

Prefer a bare `anyhow::bail!` over `errors::overloaded` unless a
specific custom message helps the caller recover — the bare form
produces a generic 500 and is the safer default for uncategorised
internal errors.

`ErrorMetadata` is re-exported from
`convex_native::errors::ErrorMetadata` for callers that need the
raw type.

## 12. Logging

```rust
ctx.log().info(format!("processing user {id}"));
ctx.log().warn("slow path taken");
```

`Logger<'_>` writes into a shared `LogBuffer`. The runner
snapshots it after the handler returns and drains into the
backend's log-streaming path. `LogBuffer::with_min_level(LogLevel::Warn)`
filters below the threshold on push.

Clones of a `LogBuffer` share the underlying `Arc<Mutex<_>>`, so
the ctx and the runner can each hold one and see the same writes.

## 13. Assembling with `ConvexBackend`

```rust
use convex_native::{ConvexBackend, NoopCallbacks};
use std::sync::Arc;

let built = ConvexBackend::new()
    .with_native_functions()     // collects every #[convex::query/mutation/action]
    .with_native_schema()        // collects every #[derive(ConvexDocument)]
    .with_http_routes()          // collects every #[convex::http_action]
    .with_crons()                // collects every #[convex::cron]
    .with_callbacks(Arc::new(NoopCallbacks))
    .build()?;

built.validate()?;               // cross-check cron targets
eprintln!("startup: {}", built.summary());
```

`BuiltBackend` carries:

- `Arc<NativeFunctionRunner>` — dispatch entry point.
- `Option<DatabaseSchema>` — the collected schema.
- `Option<HttpRouter>` — registered HTTP routes.
- `Option<CronRegistry>` — registered cron entries.
- `Arc<dyn NativeActionCallbacks>` — attached callbacks.

Methods worth knowing:

- `summary() -> String` — one-line `"convex_native 0.1.0 — 4 fn · 1 table · 1 route · 1 cron"`.
- `function_count() / table_count() / route_count() / cron_count()`.
- `describe_json() / describe_pretty()` — see section 14.
- `warmup_plan() -> Vec<WarmupEntry>` — every declared db / text /
  vector index; use at startup to prime the backend's index cache.
- `run_action(name, namespace, args)` — invoke one of the
  registered actions through the attached callbacks.

## 14. Introspection

```rust
println!("{}", built.describe_pretty());
```

Emits a stable JSON envelope (version 1):

```json
{
  "version": 1,
  "convex_native_version": "0.1.0",
  "schema": { "tables": [...], "schema_validation": true },
  "functions": { "entries": [{ "name", "kind", "args", "internal", "timeout_ms"? }, ...] },
  "http_routes": { "routes": [{ "method", "path", "name" }, ...] },
  "crons": { "entries": [{ "name", "schedule", "target", "target_kind" }, ...] }
}
```

Additions are additive-only. For standalone callers that aren't
routing through `BuiltBackend`, the helpers
`introspect::describe_json(schema, functions, router)` and
`describe_json_full(.., crons)` plus their `_pretty` counterparts
are public.

## 15. Schema evolution

```rust
let old_schema = /* previously-deployed schema */;
let new_schema = convex_native::NativeSchema::collect()?;
for change in convex_native::diff_schemas(&old_schema, &new_schema) {
    if change.is_destructive() {
        eprintln!("destructive: {change:?}");
    }
}
```

`SchemaChange` variants cover table add/remove, index add/remove,
and modified index field-sets. `is_destructive()` flags entries
that can drop data or break existing queries.

## 16. Operational knobs

All optional, configured on `NativeFunctionRunner` or the
`CompositeFunctionRunner`:

```rust
let runner = NativeFunctionRunner::from_inventory()?
    .with_metrics(Arc::new(CountingMetrics::new()))
    .with_default_timeout(Duration::from_secs(30))
    .with_circuit_breaker(Arc::new(CircuitBreaker::new(
        CircuitBreakerConfig::default(),
    )));
```

- **Metrics**: any `NativeMetricsSink` implementor. Latency +
  `Ok`/`Err` outcome recorded per call. `CountingMetrics` is a
  simple in-memory sink useful for tests / dev dashboards;
  `NoopMetrics` (the default) discards.
- **Timeouts**: wraps every handler in `tokio::time::timeout`.
  Runaway handlers abort with a clear error and record as
  `Outcome::Err`. Per-function override via
  `#[convex::action(timeout_ms = N)]`.
- **Circuit breaker**: failure count per function name; past the
  configured threshold the breaker opens and new calls error with
  "circuit breaker is open". After `cooldown` elapses, one
  half-open probe passes through; success closes the breaker.
- **Drain**: `runner.begin_drain()` flips the runner into
  drain mode — new invocations error with a clear "draining"
  message. `runner.await_drain(timeout)` polls `in_flight()` on a
  10ms cadence until drained or `timeout` elapses.

Clones of a runner share the drain state and metrics sink, so
flipping one instance shuts down every clone.

## 17. Testing

```rust
use convex_native::testing::{TestCallbacks, CallRecord, args};

let (cb, history) = TestCallbacks::new()
    .on_query("get_by_email", |_args| Ok(ConvexValue::Null))
    .on_mutation("create", |_args| Ok(ConvexValue::Null))
    .build();

// Now drive an action:
let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
runner
    .run_action_with_callbacks("send_welcome", TableNamespace::Global, args_obj, cb)
    .await?;

assert_eq!(
    history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "get_by_email")),
    1,
);
```

`CallRecord` covers queries, mutations, schedules, and the three
storage operations. `TestHistory::count(pred)` lets tests assert
which paths fired without writing full mocks.

For ad-hoc args construction:

```rust
let obj = args! { "email" => "alice@example.com".to_string(), "count" => 42_i64 };
```

## 18. Running against a real backend

`crates/convex_native_backend/CompositeFunctionRunner` wraps the
V8 `FunctionRunner` and intercepts native names.
`crates/local_backend/src/lib.rs` wires it in ahead of
`Application::new`, so every build of `convex-local-backend`
transparently picks up statically-registered native functions:

```sh
cd npm-packages && rush install && cd ..
cargo build -p local_backend
./target/debug/convex-local-backend
```

For production, bundle your own binary that depends on
`local_backend` as a library and embeds your
`#[convex::query/mutation/action]` handlers. The inventory
collection happens at link time — nothing you need to call.

## 19. Distributed topology

> **All phases shipped in source.** See `DISTRIBUTED_PLAN.md`
> for the full architecture; `STATUS.md` for the per-substep
> tracker. Below is the deployer-facing reference.

The backend coordinates OCC, subscriptions, and committing;
workers execute handlers and return transaction summaries over
gRPC; action sub-calls route back through
`BackendCallbackService` so every write still lands in the
backend's Committer.

### Env-var matrix

| Var | Side | When to set | Effect |
|-----|------|-------------|--------|
| `CONVEX_MODE` | worker binary | always in a worker | `worker` opts a `convex-local-backend` into serving `FunctionExecutionService`; `standalone` (default) is monolith HTTP-only. `conductor` is rejected — the Phase-5 backend image replaces it. |
| `CONVEX_WORKER_BIND_ADDR` | worker | always | `host:port` the worker serves gRPC dispatch on. Default `0.0.0.0:4567`. |
| `CONVEX_BACKEND_ENDPOINT` | worker | Phase-3+ workers | `grpc://backend:5678` — worker dials this on startup, registers via `WorkerAdmissionService`, stays connected for its lifetime. Unset → worker runs in the pre-Phase-3 fixed-pool shape (backend dials it instead). |
| `CONVEX_ADMISSION_BIND_ADDR` | backend | distributed topology | `host:port` the backend binds `WorkerAdmissionService` on. Workers dial here. Unset → no dynamic admission; backend uses `CONVEX_NATIVE_WORKERS` instead. |
| `CONVEX_NATIVE_WORKERS` | backend | Phase-2 fixed pool | Comma-separated `grpc://host:port,…` worker URLs. The Phase-2 fallback shape when the admission service isn't running. |
| `CONVEX_ADMIN_BIND_ADDR` | backend | operator tooling | `host:port` (typically `127.0.0.1:9090`) for the admin HTTP router. Phase-7 operator surface. |
| `CONVEX_REFUSE_NATIVE_HANDLERS` | backend | Phase-5 image CI | `1` → backend boot fails if it has any `#[convex::*]` registrations linked in. Catches "worker code accidentally ended up in the backend image" in CI. |

### Worker binary boot

```sh
# Worker dials the backend's admission port on startup; the
# backend dials back to CONVEX_WORKER_BIND_ADDR for dispatch.
CONVEX_MODE=worker \
  CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
  CONVEX_BACKEND_ENDPOINT=http://convex-backend:5678 \
  ./target/release/my-convex-worker
```

Boots the worker's `FunctionExecutionService` and registers
against the backend's `WorkerAdmissionService` with a
`RegistrationEnvelope` built from the crate's
`collect_inventory()` (every `#[convex::*]`-decorated handler,
`NativeSchema::collect()`, `HttpRouter::collect()`,
`CronRegistry::collect()`). Closing the admission stream (SIGTERM,
process exit) retires the worker from the pool cleanly.

Actions that call `ctx.run_mutation(...)` /
`ctx.scheduler().run_after(...)` / `ctx.storage().store(...)`
etc. route back to the backend via
`BackendCallbackService`. Wire with
`FunctionExecutionServer::with_backend_callback_endpoint(url)`
or use the bundled worker binary which sets it from
`CONVEX_BACKEND_ENDPOINT` by convention.

### Backend binary boot

```sh
# Prebuilt backend image, distributed topology.
docker run -d --name convex-backend \
  -e CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678 \
  -e CONVEX_ADMIN_BIND_ADDR=127.0.0.1:9090 \
  -p 3210:3210 -p 5678:5678 \
  getconvex/convex-backend:X.Y.Z
```

The image at
`convex-native/examples/deploy/docker/Dockerfile.backend`
defaults `CONVEX_ADMISSION_BIND_ADDR=0.0.0.0:5678` so the above
`-e` line is optional; `CONVEX_ADMIN_BIND_ADDR` is opt-in and
loopback-only so operator tooling isn't exposed on the public
network.

### Admin HTTP surface

With `CONVEX_ADMIN_BIND_ADDR` set, the backend mounts:

- `GET /admin/pool` — JSON snapshot (total workers, by_version,
  by_kind, kind_preferences, per-worker detail with
  `in_flight`).
- `POST /admin/pool/floor` body
  `{"min_registry_version":"X.Y.Z"}` — set rolling-update
  floor; `null` clears.
- `POST /admin/pool/kind_preference` body
  `{"function_name":"compute","kind":"native-rust"}` — pin
  per-function Rust vs JS routing (soft hint — falls back to
  any-kind if the preferred kind is absent).
- `POST /admin/pool/drain` body
  `{"worker_id":N,"reason":"…"}` — operator-initiated drain;
  the worker's `drain_signaled()` future wakes and it retires
  cleanly.

### Rolling-update shape

1. Build new worker image (v2). Push.
2. Roll the worker Deployment. v2 workers register; they
   coexist with v1 in the pool.
3. Operator bumps the floor:
   ```sh
   curl -X POST http://127.0.0.1:9090/admin/pool/floor \
     -d '{"min_registry_version":"v2"}'
   ```
   Backend stops routing dispatches to v1 workers.
4. Either wait for v1 autoscaler to scale down, or
   explicitly drain:
   ```sh
   curl -X POST http://127.0.0.1:9090/admin/pool/drain \
     -d '{"worker_id":3,"reason":"v1 retirement"}'
   ```
5. Backend logs the inventory diff on each new
   `registry_version` join via
   `tracing::info!(target="convex_admission", …)` so the
   rollout reads as "v2 added [foo, bar], removed [legacy]".

### Reference deployment artifacts

- Dockerfile.backend — `convex-native/examples/deploy/docker/`
- Dockerfile.worker — same directory
- `backend-deployment.yaml` + `worker-deployment.yaml` —
  `convex-native/examples/deploy/kubernetes/` (Namespace,
  PVC, Deployment, two Services: public-HTTP +
  internal-admission)
- Bringing-up-locally recipe — `DEPLOYMENT.md` §"Bringing up
  Topology B locally"

## 20. Where the crate stops

This crate is the **framework**: schema, typed ctx, function
registry, HTTP + storage + scheduler surface, metrics / drain /
breaker primitives, builder + introspection.

Out of scope, handled by the backend adapter
(`convex_native_backend`) or the backend binary:

- Durable execution of scheduled jobs and crons.
- HTTP serving + the preempt / ctrl-C shutdown glue.
- Auth token verification and identity construction (this crate
  only surfaces the `AuthInfo` read API).
- File storage persistence.
- V8 / JS interop (only the `CompositeFunctionRunner` knows the
  JS runner exists).
- Persistence — the database lives in `database::Database<RT>` and
  is owned by the backend.

For what's not shipped anywhere yet, read `STATUS.md`.
