# Convex Native

Framework crates for writing Convex server functions (queries, mutations,
actions) in native Rust. See `native-rust-functions.md` for the design and
`IMPLEMENTATION_PLAN.md` for the phased roadmap.

## Current state

**Phase 1 COMPLETE** (1.0.1 → 1.3.3, 1.2.4 / 1.2.5, 1.4.1, 1.5.2,
1.6.1–1.6.3), **Phase 2 COMPLETE** (2.1–2.8), **Phase 3 partial** (3.5
mode alias + executor trait stub), **Phase 4 partial** (4.2 metrics
sink + 4.3 graceful drain + 4.4 timeouts), **Phase 5 partial** (5.1 schema diff + 5.2
compile-time index validation + 5.4 bulk `get_many`).

Remaining: concrete backend adapter implementing
`NativeActionCallbacks` and wiring the composite runner into
`make_app()` (future `crates/convex_native_backend` crate — 1.5.1 /
1.5.3), real distributed gRPC service (3.1–3.6), the rest of
production hardening (4.1 + 4.3–4.7), typed vector/text search (5.3).

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
  Terminal methods `.collect()` / `.first()` now actually execute via
  `database::DeveloperQuery`: index-range source when `.with_index()`
  was used, full-table-scan otherwise (filters without an index still
  rejected at runtime for now).

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
    └── runner_dispatch.rs                  -- NativeFunctionRunner lookup & dispatch

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
