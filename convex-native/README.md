# Convex Native

Framework crates for writing Convex server functions (queries, mutations,
actions) in native Rust.

- **`QUICKSTART.md`** — the shipped, working-today developer surface
  (read this if you want to use the crate).
- **`MIGRATION.md`** — side-by-side JS → Rust cheatsheet for porting
  an existing Convex app.
- **`native-rust-functions.md`** — the original design doc (rationale
  and high-level architecture).
- **`IMPLEMENTATION_PLAN.md`** — phase-by-phase roadmap.
- **`COMPOSITE_RUNNER.md`** — reference for the `convex_native_backend`
  adapter crate and its `run_function` TODO list.

## Current state

**Phase 1 COMPLETE** (1.0.1 → 1.3.3, 1.2.4 / 1.2.5, 1.4.1, 1.5.2,
1.6.1–1.6.3), **Phase 2 COMPLETE** (2.1–2.8), **Phase 3 partial** (3.5
mode alias + executor trait stub), **Phase 4 partial** (4.1 fastrace spans + 4.2 metrics sink + 4.3
graceful drain + 4.4 timeouts + 4.5 circuit breaker + 4.6 index-cache
warmup plan),
**Phase 5 partial** (5.1 schema diff + 5.2 compile-time index
validation + 5.3 text/vector search + 5.4 bulk `get_many`).

Remaining: end-to-end smoke test against a live backend, real
distributed gRPC service (3.1–3.6), and production hardening (4.7
rolling updates). Everything up to the `make_app()` wiring is now
in-tree: `crates/convex_native_backend/` provides the
`CompositeFunctionRunner` (native dispatch) and `BackendCallbacks`
(native → `udf::ActionCallbacks` bridge), and
`crates/local_backend/src/lib.rs` now instantiates the composite
runner ahead of the `Application::new` call, so every build of
`convex-local-backend` transparently picks up any statically
registered native functions.

### What works

- The `convex_native` crate compiles, and `cargo test -p convex_native`
  runs **15 tests** — 6 unit + 9 derive integration — all green.
- `#[derive(ConvexDocument)]` on a struct generates, for `Foo`:
  - `impl ConvexDocument for Foo` (table name, to_convex_object,
    from_convex_object, table_definition with indexes)
  - `pub enum FooField` — one variant per field, `impl FieldReference`
  - `pub enum FooIndex` — one variant per `#[convex(index(...))]`, `impl
    IndexReference` (uninhabited when none declared)
  - `pub struct FooPatch` — every field wrapped in `Option`, `Default`,
    `impl ConvexPatch`
  - `pub struct FooWithId { pub id: Id<Foo>, pub doc: Foo }` with `Deref`
  - `inventory::submit!` of a `TableRegistration` so `NativeSchema::collect()`
    picks it up automatically
- `NativeSchema::collect()` walks the linker-section registrations and
  returns a `DatabaseSchema` populated with every derived type from the
  binary.
- `Id<T: ConvexDocument>` is phantom-typed; `Id<User>` and `Id<Message>`
  are compile-time distinct.

### Consumer surface

Developers write:

```rust
use convex_native::prelude::*;

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub name: String,
    pub email: String,
    pub created_at: f64,
}
```

and get the generated companions for free. They depend on `convex_native`
only; `convex_macro` is re-exported.

### New — `get_with_meta` exposes document metadata

`QueryDb::get_with_meta(id)` / `MutationDb::get_with_meta(id)` return
a `DocumentWithMeta<T>` that carries the document's `id`,
`creation_time`, and typed body:

```rust
if let Some(m) = ctx.db().get_with_meta(user_id).await? {
    println!("{} created at {:?}", m.doc.name, m.creation_time);
    let same_id: Id<User> = m.id;
}
```

`DocumentWithMeta<T>` derefs to `T` so existing code that operates on
the typed body keeps working.

### New — `#[convex::action(timeout_ms = N)]` per-function timeout

Query/mutation/action attribute macros now accept `timeout_ms = N`
as a per-function override of the runner's default timeout:

```rust
#[convex::action(timeout_ms = 5_000)]
async fn poll_external_api(ctx: &mut ActionCtx) -> Result<()> { .. }
```

The value lands on `NativeFunctionRegistration::timeout_ms` and the
runner honours it ahead of its own default (if any). `timeout_ms = 0`
(the default) means "no per-function override — inherit from runner".
Surfaced in the JSON introspection output alongside `internal`.

Modifiers compose:

```rust
#[convex::mutation(internal, timeout_ms = 30_000)]
async fn heavy_backfill(ctx: &mut MutationCtx) -> Result<()> { .. }
```

### New — `ctx.log()` helper + `convex_native::VERSION`

`QueryCtx` / `MutationCtx` / `ActionCtx` now expose `ctx.log()`
returning a `Logger<'_>` with `debug / info / warn / error` methods:

```rust
#[convex::mutation]
async fn create(ctx: &mut MutationCtx, name: String) -> Result<Id<User>> {
    ctx.log().info(format!("creating user {name}"));
    // ...
}
```

Lines land in a shared `LogBuffer` (cheap to clone, thread-safe). The
runner uses `with_callbacks_and_log_buffer` (or the query/mutation
equivalents) to inject a buffer it can drain into the backend's
log-streaming path after the handler returns. `LogBuffer` supports
a minimum-severity filter via `with_min_level(LogLevel::Warn)` for
production — lines below the threshold are dropped on push.

Also added a `convex_native::VERSION` constant (derived from
`CARGO_PKG_VERSION` at build time). Surfaced through
`introspect::describe_json` as `convex_native_version` for deployment
traceability.

### New — `#[convex::cron(...)]`

Recurring scheduled jobs via an inventory-collected attribute. The
schedule string is parsed with `saffron` at macro-expansion time, so
typos like `"0 3 * *"` (missing a field) fail the build rather than
surfacing at runtime.

```rust
#[convex::mutation(internal)]
async fn nightly_cleanup(ctx: &mut MutationCtx) -> Result<()> { .. }

#[convex::cron(
    name = "nightly-cleanup",
    schedule = "0 3 * * *",
    target = "nightly_cleanup",
)]
fn _nightly_cleanup_cron() {}
```

Attaches to a placeholder item (name doesn't matter) and emits a
`CronRegistration`. `ConvexBackend::new().with_crons().build()`
collects everything into `BuiltBackend::crons`, and introspection
includes a `crons.entries[]` array.
`BuiltBackend::validate()` cross-checks every cron `target` exists
in the function registry and has the matching `target_kind` —
call it at startup so misconfigured crons crash the binary rather
than silently skipping.
Actually *running* the schedule on time is still the backend
adapter's responsibility — this crate stops at registration.

### New — `#[convex::query(internal)]` modifier

Mark a query/mutation/action as internal-only:

```rust
#[convex::mutation(internal)]
async fn rebuild_index(ctx: &mut MutationCtx, token: String) -> Result<()> { .. }
```

The flag lands on `NativeFunctionRegistration::is_internal` and in
the JSON introspection output (`functions.entries[].internal`). The
backend adapter is expected to reject external client calls to
internal functions — they remain callable from other native
functions and from trusted server-side callers.

### New — `convex_native::errors` helpers

User-facing vs system errors are distinguished in Convex by the
`ErrorMetadata` tag attached to an `anyhow::Error`. Native functions
now have ergonomic builders:

```rust
if !ctx.auth().is_authenticated() {
    return Err(convex_native::errors::unauthenticated(
        "MissingToken",
        "Request requires an auth token",
    ).into());
}
```

Exposes: `bad_request` (400), `not_found` (404), `unauthenticated`
(401), `forbidden` (403), `conflict` (409). The `ErrorMetadata` type
is re-exported too for callers that need it directly.

### New — `BuiltBackend::summary()` + `*_count()` helpers

One-line startup log format:

```
convex_native 0.1.0 — 4 fn · 1 table · 1 route · 1 cron
```

`BuiltBackend::summary()` produces the above; individual counts are
exposed as `function_count()`, `table_count()`, `route_count()`, and
`cron_count()`. Crate version is also available via
`convex_native_version()`. The `tiny_app` example prints the summary
to stderr on startup.

### New — JSON introspection for dev tooling

`BuiltBackend::describe_json()` returns a stable JSON envelope
listing every declared table (with indexes / text-indexes /
vector-indexes), every registered function (with name / kind / args),
and every HTTP route. `describe_pretty()` formats the same output as
an indented string.

Shape (version 1):

```json
{
  "version": 1,
  "schema": { "tables": [...], "schema_validation": true },
  "functions": { "entries": [{ "name", "kind", "args" }, ...] },
  "http_routes": { "routes": [{ "method", "path", "name" }, ...] }
}
```

### New — `ctx.auth()` + `ctx.unix_timestamp()`

`QueryCtx` and `MutationCtx` now expose:

- `ctx.auth()` — an `AuthInfo<'_>` with `is_authenticated()`,
  `is_admin()`, `is_system()`, and `.raw()` for the escape-hatch.
  Wraps the underlying `keybroker::Identity`.
- `ctx.unix_timestamp()` — wall-clock time sourced from the
  transaction's runtime, so tests driving a mock clock see the
  mocked value.

### New — `convex_native::testing` unit-test utilities

Writing a full `NativeActionCallbacks` mock per test is tedious. The
`testing` module provides a builder:

```rust
use convex_native::testing::{TestCallbacks, CallRecord};

let (cb, history) = TestCallbacks::new()
    .on_query("get_user_count", |_args| Ok(ConvexValue::Int64(7)))
    .on_mutation("create_user", |_args| Ok(ConvexValue::Null))
    .build();

// ... runner.run_action_with_callbacks(..., cb).await?

assert_eq!(
    history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "get_user_count")),
    1,
);
```

`TestHistory::count(pred)` lets tests assert which paths fired
without writing boilerplate. `CallRecord` covers queries, mutations,
schedules, and the three storage operations.

For object construction, the module also exposes an `args!` macro:

```rust
use convex_native::testing::args;

let obj = args! {
    "email" => "alice@example.com".to_string(),
    "count" => 42_i64,
};
```

### New in Phase 4.1 / 4.6 — fastrace spans + index-cache warmup

- Every `run_query` / `run_mutation` / `run_action_with_callbacks`
  entry point is annotated `#[fastrace::trace]`, so the existing
  `fastrace` tracing infrastructure sees native function invocations
  alongside JS ones with zero extra wiring from the adapter.
- `convex_native::plan_warmup(schema)` / `BuiltBackend::warmup_plan()`
  walk the collected schema and emit a `Vec<WarmupEntry>` covering
  every declared db / text / vector index. The backend adapter uses
  this at startup to prime its in-memory index cache before accepting
  traffic.

### New in Phase 4.5 — circuit breaker

- `CircuitBreaker` with configurable `failure_threshold` and
  `cooldown` (`CircuitBreakerConfig`).
- `NativeFunctionRunner::with_circuit_breaker(Arc<CircuitBreaker>)`
  installs the breaker. Failures are counted per function name. Past
  the threshold the breaker opens: new calls error with a clear
  "circuit breaker is open" message. After `cooldown` elapses one
  probe call passes through in half-open state; if it succeeds the
  breaker closes again.
- Minimal today (consecutive-failure counter); can later swap for the
  rolling-window flavor used elsewhere in the codebase.

### New in Phase 5.3 — text + vector search indexes

`#[derive(ConvexDocument)]` now understands:

```rust
#[convex(text_index(
    name = "by_body",
    search_field = "body",
    filter_fields = ["category", "author"]
))]
#[convex(vector_index(
    name = "by_embedding",
    vector_field = "embedding",
    dimensions = 1536,
    filter_fields = ["category"]
))]
```

These emit proper `TextIndexSchema` and `VectorIndexSchema` entries
into `TableDefinition::text_indexes` / `vector_indexes`. Field
references are validated at compile time against the struct's
declared fields (same rules as database indexes).

### New in Phase 4.3 — graceful shutdown drain

- `NativeFunctionRunner::begin_drain()` — flip the runner into
  draining mode; new invocations error with a clear "draining" message.
- `is_draining()` / `in_flight()` — inspect drain state.
- `await_drain(timeout)` — wait for outstanding invocations to finish
  (polls `in_flight` on a 10ms cadence; returns `true` on clean drain,
  `false` on timeout).

Clones of a runner share the drain state, so flipping one instance
shuts down every clone.

### New in Phase 4.2 / 4.4 — metrics + timeouts

- `NativeMetricsSink` trait — record per-function latency and
  `Ok`/`Err` outcome without pulling a specific metrics backend into
  the crate.
- `NoopMetrics` (discards) and `CountingMetrics` (in-memory counters
  + total-latency, useful for tests / dev dashboards).
- `NativeFunctionRunner::with_metrics(Arc<dyn NativeMetricsSink>)`
  attaches a sink; every `run_query` / `run_mutation` /
  `run_action_with_callbacks` now records a sample.
- `NativeFunctionRunner::with_default_timeout(Duration)` wraps every
  handler invocation in `tokio::time::timeout` — runaway handlers
  abort with a clear error and record as `Outcome::Err`.

### New in Phase 3 (partial) — distributed scaffolding

- `ConvexMode::{Standalone, Conductor, Worker}` — the operating-mode
  enum described in the design doc §10, parseable from
  `CONVEX_MODE=...` env strings.
- `ExecuteRequest` / `ExecuteResponse` — request/response payloads the
  distributed gRPC service will serialize. Full protobuf definition
  lives in Phase 3.1 (`crates/pb/proto/function_execution.proto`,
  not yet added); these Rust types are the architectural handoff point
  the future `convex_native_distributed` crate consumes.
- `FunctionExecutor` trait — workers implement this to accept remote
  calls.

### New in Phase 5 (partial) — index validation + schema diff + get_many

- **Compile-time index-field validation (5.2).**
  `#[derive(ConvexDocument)]` now rejects `#[convex(index(... fields =
  ["nonexistent"]))]` with a clear error pointing out which declared
  fields are available. Nested field paths (`"profile.name"`) validate
  their top-level segment only — the nested struct carries the rest.
- **Schema diff tool (5.1).** `convex_native::diff_schemas(old, new)`
  returns a `Vec<SchemaChange>` describing additions, removals, and
  modified index field-sets. `SchemaChange::is_destructive()` flags
  entries that can drop data or break queries (removed table / index
  / changed index fields). Intended for dev-time migration tooling.
- **Bulk `get_many` (5.4).** `QueryDb::get_many(ids)` and
  `MutationDb::get_many(ids)` — fetch multiple documents in one call.
  Useful for chasing foreign keys (`message.author: Id<User>`) across
  a batch without writing the loop.

### New in Phase 1.5.2 — `ConvexBackend` builder

`ConvexBackend::new()` is the developer-facing assembly point:

```rust
let built = ConvexBackend::new()
    .with_native_functions()     // collect #[convex::{query,mutation,action}]
    .with_native_schema()        // collect #[derive(ConvexDocument)]
    .with_http_routes()          // collect #[convex::http_action]
    .with_callbacks(my_callbacks)
    .build()?;

let ret = built.run_action("send_email", ns, args).await?;
```

`BuiltBackend` carries an `Arc<NativeFunctionRunner>`, the collected
`DatabaseSchema`, the `HttpRouter`, and the callbacks. It's the
handoff object the future backend adapter consumes when integrating
with `make_app()`.

### New in Phase 2.8 — `NativeActionCallbacks`

- `convex_native::NativeActionCallbacks` — async trait the backend
  adapter implements to fulfill action-time capabilities:
  - `run_query_by_name`, `run_mutation_by_name` (sub-calls)
  - `schedule` (returns a `DeveloperDocumentId` for the scheduled job)
  - `storage_store` / `storage_get_url` / `storage_delete`
- `NoopCallbacks` — a built-in fallback that bails with a clear error
  on every method, so unit tests run without a backend attached.
- `ActionCtx::with_callbacks(runner, callbacks, namespace)` and
  `NativeFunctionRunner::run_action_with_callbacks(...)` — how the
  backend adapter injects a real implementation at dispatch time.
- `ActionCtx::run_query_raw` / `run_mutation_raw` and the typed
  `run_query` / `run_mutation` now route through the attached
  callbacks, and `scheduler()` / `storage()` likewise.

This closes the loop for the in-crate surface: `#[convex::action]`
bodies can do typed sub-calls, schedule future work, and touch file
storage entirely through compile-time-checked APIs. All of it works
end-to-end today against any `Arc<dyn NativeActionCallbacks>` —
`tests/callbacks_wiring.rs` demonstrates a full mock driving
sub-queries, sub-mutations, scheduler, and storage through one
`#[convex::action]` body.

### New in Phase 2.6 / 2.7 — Storage + HTTP actions

- `StorageCtx` — obtained from `ActionCtx::storage()` or
  `HttpActionCtx::storage()`. Exposes `store(bytes, content_type) ->
  StorageId`, `get_url(id)`, and `delete(id)`. Each method serializes
  its inputs then `bail!`s pending file_storage backend integration.
- `#[convex::http_action(method = "...", path = "...")]` attribute
  macro. Registers HTTP routes via inventory:

  ```rust
  #[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
  async fn stripe_webhook(
      ctx: &mut HttpActionCtx<'_, Rt>,
      req: HttpRequest,
  ) -> Result<HttpResponse> { .. }
  ```

- `HttpRequest` / `HttpResponse` types with helpers: `req.header(name)`,
  `req.body_bytes()`, `req.body_text()`, `req.body_json::<T>()`,
  `HttpResponse::json(status, value)`, `HttpResponse::redirect(status,
  location)`, `.with_header` / `.with_body`.
- `HttpActionCtx` wraps `ActionCtx` and delegates `run_query`,
  `run_mutation`, `run_action`, `scheduler`, `storage`.
- `HttpRouter::collect()` — runtime enumeration of registered routes
  with exact-match `lookup(method, path)`.

### New in Phase 2.5 — Scheduler (surface)

`MutationCtx::scheduler()` and `ActionCtx::scheduler()` now return a
`Scheduler<'_>` with:

- `run_after<F: ConvexMutationFunction>(delay, marker, args)` — schedule
  a typed mutation.
- `run_action_after<F: ConvexActionFunction>(delay, marker, args)` —
  schedule a typed action.

Both serialize the typed args correctly, then `bail!` at the actual
scheduling step pending `VirtualSchedulerModel` backend integration.
Developers can already write scheduling code against the final API
shape.

### New in Phase 2.3 / 2.4 — Function markers + typed sub-calls

Every `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
now also emits:

- `XxxArgs` struct (PascalCase + `Args`): one field per non-`ctx`
  parameter with `ToConvex` / `FromConvex` impls.
- `Xxx` ZST marker (PascalCase of the fn name). Implements the matching
  marker trait — `ConvexQueryFunction` / `ConvexMutationFunction` /
  `ConvexActionFunction` — carrying `type Args`, `type Output`, and
  `fn name()`.

That lets `ActionCtx` offer typed sub-calls:

```rust
let result: Option<User> = ctx.run_query(GetUser, GetUserArgs {
    email: "a@b".into(),
}).await?;

let id: Id<User> = ctx.run_mutation(CreateUser, CreateUserArgs { .. }).await?;

let sent: bool = ctx.run_action(SendEmail, SendEmailArgs { .. }).await?;
```

Today `run_query` / `run_mutation` still `bail!` through the raw
helpers pending backend integration. `run_action` runs end-to-end
because actions don't require a new transaction.

> **Note on API:** the design doc's example uses the function name
> (`ctx.run_query(get_user, …)`) but Rust forbids a `fn` and a `struct`
> with the same name in one scope, so the ZST marker is PascalCase
> (`GetUser`). The function itself remains callable as `get_user(...)`.

### New in Phase 2 foundations — Actions

- `#[convex::action]` attribute macro. Developers write:

  ```rust
  #[convex::action]
  async fn send_email(ctx: &mut ActionCtx, user_id: Id<User>) -> Result<()> { .. }
  ```

  The macro follows the same shape as `#[convex::query]` /
  `#[convex::mutation]` and registers the function under
  `UdfType::Action`.
- `convex_native::ActionCtx<'a, RT>` — context for actions. Unlike
  `QueryCtx` / `MutationCtx`, it does NOT hold a transaction; instead
  it carries an optional `Arc<NativeFunctionRunner>` for sub-calls and
  a namespace.
- `NativeFunctionRunner::run_action(name, namespace, args)` dispatches
  an action end-to-end — **actions with no external I/O or sub-calls
  actually execute today** (see the `action_dispatch_returns_handler_result`
  test). Typed / raw sub-calls (`ctx.run_query_raw` etc.) are still
  stubbed `bail!` pending backend integration (Step 2.4+).

### New in Phase 1.6 — additional derive macros

- `#[derive(ConvexEnum)]` — string-valued unit-only enums. Each variant
  becomes its snake_case name on the wire
  (`Admin` ↔ `"admin"`). `#[convex(rename = "custom")]` overrides per
  variant.
- `#[derive(ConvexNested)]` — structs that round-trip through a
  `ConvexObject` without registering a table. Use for embedded shapes
  like `Address` inside `User`.
- `#[derive(ConvexUnion)]` — tagged unions where each variant is a
  struct-variant with named fields. Configurable discriminant field
  name (`#[convex(tag = "...")]`, default `"type"`) and per-variant
  `rename`. Duplicate tags are compile errors.

All three emit only `ToConvex` / `FromConvex` impls — no inventory
registrations, no schema entries.

### New in Phase 1.4.1 — `NativeFunctionRunner`

- `convex_native::NativeFunctionRunner` wraps an `Arc<NativeFunctionRegistry>`
  and exposes:
  - `from_inventory()` — build from static inventory entries
  - `has_function(name)` / `has_function_of_type(name, UdfType)` — name-based
    dispatch decisions
  - `run_query(name, tx, namespace, args)` / `run_mutation(...)` — execute
    a handler directly against a borrowed `Transaction<Rt>`
- `NativeFunctionRunner` is deliberately standalone and does **not** yet
  implement `function_runner::FunctionRunner`. The adapter that does
  (wrapping a JS runner and delegating unmapped calls) is documented in
  `COMPOSITE_RUNNER.md` — it's not built in-workspace because
  `function_runner` transitively depends on `isolate` (V8), which needs
  `rush install` + build steps to compile. When the full backend build
  lands in a new integration crate, the composite code moves there.

### New in Phase 1.2.4 / 1.2.5 — function attribute macros

- `#[convex::query]` and `#[convex::mutation]` (imported via
  `use convex_native::convex;`).
- Both accept async fns whose first parameter is `ctx: &mut QueryCtx`
  (or `MutationCtx`). Subsequent parameters must be `FromConvex`;
  return values must be `ToConvex + Send`.
- Derive macro now also emits `ToConvex` / `FromConvex` impls for the
  decorated struct, so developers don't need to implement them
  manually.
- Native function dispatch is pinned to
  `runtime::prod::ProdRuntime` (aliased as `convex_native::Rt`) — see
  `registry.rs` module docs for the rationale.
- `NativeFunctionRegistration` carries `name`, `arg_names`, and a
  `HandlerFn::{Query|Mutation}(fn_ptr)` — the runner (Phase 1.4) will
  consume these. `NativeFunctionRegistry::collect()` surfaces all
  registrations with O(1) lookup by name.

### New in Phase 1 Layer 3 (steps 1.3.1–1.3.3)

- `QueryCtx<'tx, RT>` / `QueryDb<'tx, RT>` wrap a
  `database::Transaction<RT>`. `db().get::<T>(Id<T>)` returns
  `Option<T>` parsed through the derived `from_convex_object`.
- `MutationCtx<'tx, RT>` / `MutationDb<'tx, RT>` wrap the same
  transaction and add `insert`, `patch`, `replace`, `delete` — all typed
  by `ConvexDocument` / `ConvexPatch`.
- `TypedQueryBuilder` type-checks against `T::Index` and `T::Field`.
  Terminals `.collect()` / `.first()` / `.unique()` / `.take(n)` /
  `.count()` execute via `database::DeveloperQuery`. Index-range
  source when `.with_index()` was used, full-table-scan otherwise
  (filters without an index still rejected at runtime for now).
  Supports equality (`.eq`) and range comparators (`.gt` / `.gte` /
  `.lt` / `.lte`). `.unique()` errors if more than one doc matches.

### What doesn't work yet

- **No end-to-end execution yet.** `NativeFunctionRunner` can dispatch
  handlers given a `Transaction<Rt>`, but building that transaction
  and rendering the result as `FunctionOutcome` /
  `FunctionFinalTransaction` is Phase 1.4.2+ TODO work; see
  `COMPOSITE_RUNNER.md` for the full todo list.
- **No backend wiring.** `make_app()` hasn't been updated. The
  `CompositeFunctionRunner` integration shape is documented but not
  in-tree; it belongs in a future `crates/convex_native_backend` crate
  that can depend on `function_runner`.
- **Non-indexed filters.** `.eq()` currently requires
  `.with_index(...)`. Full-table-scan + post-scan filtering is a later
  convenience, not MVP-critical.
- **No function macros.** `#[convex::query]`, `#[convex::mutation]`, and
  `#[convex::action]` are not implemented. Phase 1.2.4 + 1.2.5 + 2.2.
- **No context wrappers.** `QueryCtx`, `MutationCtx`, `ActionCtx` don't
  exist. Phase 1.3.
- **Placeholder handler signature.** `registry::HandlerFn = fn()` until
  the context wrappers pin down the real shape.
- **Document type validation is off.** `table_definition()` emits
  `document_type: None` — i.e. every derived type currently gets an "any"
  schema shape. Enforcing the shape against the struct's fields is
  Phase 1 extension work, not part of the MVP critical path.

## Architecture (today)

```
crates/convex_native/
├── src/
│   ├── lib.rs         -- re-exports, __private module for macro-generated code
│   ├── convert.rs     -- ToConvex / FromConvex
│   ├── id.rs          -- Id<T: ConvexDocument>
│   ├── document.rs    -- ConvexDocument / FieldReference / IndexReference / ConvexPatch
│   ├── schema.rs      -- TableRegistration + NativeSchema::collect()
│   ├── registry.rs    -- NativeFunctionRegistration + NativeFunctionRegistry
│   ├── prelude.rs     -- glob-import target
│   ├── runner.rs            -- NativeFunctionRunner (dispatch)
│   └── ctx/
│       ├── mod.rs
│       ├── query.rs         -- QueryCtx + QueryDb
│       ├── query_builder.rs -- TypedQueryBuilder (typed, executable)
│       ├── mutation.rs      -- MutationCtx + MutationDb
│       └── action.rs        -- ActionCtx (Phase 2 skeleton + dispatch)
└── tests/
    ├── backend_builder.rs                  -- ConvexBackend end-to-end
    ├── callbacks_wiring.rs                 -- NativeActionCallbacks sub-calls / scheduler / storage
    ├── ctx_types.rs                        -- compile-time surface tests for ctx wrappers
    ├── drain.rs                            -- graceful shutdown drain
    ├── derive_document.rs                  -- integration tests for ConvexDocument
    ├── derive_enums_nested_unions.rs       -- ConvexEnum / ConvexNested / ConvexUnion
    ├── derive_functions.rs                 -- integration tests for fn attribute macros
    ├── function_refs.rs                    -- marker types + ConvexQueryFunction / etc.
    ├── golden_path.rs                      -- full realistic app end-to-end
    ├── http_actions.rs                     -- HTTP action registration + request/response
    ├── metrics_wiring.rs                   -- runner metrics + timeout enforcement
    ├── runner_dispatch.rs                  -- NativeFunctionRunner lookup & dispatch
    ├── search_indexes.rs                   -- text/vector search index registration
    └── testing_utilities.rs                -- TestCallbacks builder smoke test

crates/convex_macro/
├── src/
│   ├── lib.rs                -- #[proc_macro_derive(ConvexDocument)] entry point
│   ├── convex_document.rs    -- derive implementation
│   ├── (instrument_future, v8_op — pre-existing, unchanged)
```

`inventory::collect!` is the collection backbone for both the schema
(`TableRegistration`) and the function registry
(`NativeFunctionRegistration`). Developers never touch the registration APIs
directly — the derive/attribute macros emit the `submit!` calls.

### Why `::convex_native::__private::...` paths in generated code?

Proc-macro output can't assume what's in scope at the call site. To avoid
forcing users to `use common; use value;` in their own crates, we re-export
the handful of concrete types the generated code needs (`FieldName`,
`ConvexValue`, `ConvexObject`, `TableName`, `TableDefinition`,
`IndexSchema`, `IndexDescriptor`, `FieldPath`, `IndexedFields`) through a
hidden `convex_native::__private` module. The derive emits absolute paths
through that module. This keeps developers' `Cargo.toml` minimal (just
`convex_native`) and frees us to relocate internal types without breaking
downstream callers.

## Runnable example

`crates/convex_native/examples/tiny_app.rs` is a compile-and-run
demonstration of the full surface — schema, nested types, enum,
query, mutation, action, internal mutation, cron, HTTP action,
builder, validation, introspection. It's the fastest way to
sanity-check the crate end-to-end:

```sh
cargo run -p convex_native --example tiny_app
```

Prints the JSON envelope from `BuiltBackend::describe_pretty()`.

## Development

```sh
cargo check -p convex_native
cargo test -p convex_native
cargo +nightly fmt -p convex_native -p convex_macro
```

## Next up

Per `IMPLEMENTATION_PLAN.md`:

1. **Step 1.3.1** — `QueryCtx` + `QueryDb` (read-only database handle
   wrapping `Transaction<RT>`).
2. **Step 1.3.2** — `TypedQueryBuilder<T>` with typed field/index filters.
3. **Step 1.3.3** — `MutationCtx` + `MutationDb` (read + write).
4. **Step 1.2.4** — `#[convex::query]` proc macro.
5. **Step 1.2.5** — `#[convex::mutation]` proc macro.
6. **Step 1.4.x** — `NativeFunctionRunner` implementing the
   `FunctionRunner` trait.

Agents iterating on this project: please keep this document honest about
what is merged vs what is planned, after each commit.
