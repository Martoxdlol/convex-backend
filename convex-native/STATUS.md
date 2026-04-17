# Project status

Authoritative list of what is shipped, what is missing, and what the
remaining work looks like. For day-to-day developer usage read
`USAGE.md`; for a guided walkthrough read `QUICKSTART.md`; for the
JS → Rust cheatsheet read `MIGRATION.md`.

## One-line summary

Phases 1 / 2 / 4 / 5 of `IMPLEMENTATION_PLAN.md` are fully shipped.
Phase 3 (distributed execution) is shipped at the crate level plus a
`convex-local-backend` `CONVEX_MODE=worker` mode. Outstanding work
is polish around the edges of Phases 1–2, not new phases.

## Test tallies

```
cargo test -p convex_native              # 211 tests
cargo test -p convex_native_backend      # 10 tests
cargo test -p convex_native_distributed  # 46 tests
```

All green at HEAD.

## Shipped

### Phase 1 — schema, type system, single-node queries/mutations
- `#[derive(ConvexDocument)]` with tables, indexes, text-indexes,
  vector-indexes, field-validation at compile time.
- Companion types per derived document: `FooField`, `FooIndex`,
  `FooPatch`, `FooWithId`.
- `#[derive(ConvexEnum)]`, `#[derive(ConvexNested)]`,
  `#[derive(ConvexUnion)]`.
- `#[convex::query]`, `#[convex::mutation]` attribute macros.
- `QueryCtx` / `MutationCtx` / `QueryDb` / `MutationDb`.
- `TypedQueryBuilder` with `.with_index`, `.eq/.gt/.gte/.lt/.lte`,
  `.order`, `.limit`, `.take`, `.first`, `.unique`, `.collect`,
  `.count`, `.page(start_cursor, page_size) -> TypedPage<T>`.
- `NativeFunctionRunner` — static registry dispatch.
- `NativeSchema::collect()` — inventory-driven schema aggregation.
- `ConvexBackend` builder + `BuiltBackend` introspection /
  validation.
- `CompositeFunctionRunner` in `convex_native_backend` wired into
  `local_backend::make_app` ahead of `Application::new`.

### Phase 2 — actions, scheduler, storage, HTTP
- `#[convex::action]` + `ActionCtx` with sub-call support.
- Typed markers (`ConvexQueryFunction` / `Mutation` / `Action`) +
  `XxxArgs` structs emitted per function.
- `ctx.run_query(Marker, Args)` / `run_mutation` / `run_action`
  typed sub-calls routing through `NativeActionCallbacks`.
- `Scheduler`: `run_after`, `run_action_after`, `run_at`,
  `run_action_at`, `cancel`.
- `StorageCtx::store / get_url / delete`.
- `#[convex::http_action]` + `HttpRequest` / `HttpResponse` /
  `HttpRouter`.
- `NativeActionCallbacks` trait + `NoopCallbacks` default +
  `convex_native_backend::BackendCallbacks` bridge to
  `udf::ActionCallbacks` / `FileStorage::store_file`.
- Native-to-native cross-call resolver in `BackendCallbacks`.

### Phase 3 — distributed execution
- Proto contract at `crates/pb/protos/function_execution.proto`.
- `convex_native_distributed` crate with `FunctionExecutionServer`,
  `DistributedFunctionRunner` (P2C load balancer + retry),
  `TonicWorkerClient`, `CONVEX_MODE` env helpers, runnable
  `examples/worker` and `examples/conductor`, and multi-worker
  integration + subprocess smoke tests.
- `convex-local-backend` accepts `CONVEX_MODE=standalone` (default)
  or `CONVEX_MODE=worker` (adds a tonic `FunctionExecutionService`
  beside the HTTP server, sharing the `Database` and draining
  together on Ctrl-C / `/preempt`). Conductor mode still runs from
  the dedicated `convex_native_distributed::examples::conductor`
  binary.

### Phase 4 — production hardening
- Fastrace span propagation on every runner entry point.
- `NativeMetricsSink` + `NoopMetrics` + `CountingMetrics`.
- Graceful drain: `begin_drain` / `is_draining` / `in_flight` /
  `await_drain`.
- Per-function timeouts via `with_default_timeout(Duration)` and
  `#[convex::action(timeout_ms = N)]` per-function override.
- `CircuitBreaker` with `CircuitBreakerConfig`, closed → open →
  half-open state machine.
- `warmup::plan_warmup(schema)` surfaces every declared db / text /
  vector index for backend warmup.
- Rolling updates with version-aware routing:
  `min_registry_version` floor + per-call override in
  `ExecuteRequest`.

### Phase 5 — developer ergonomics
- Compile-time index-field validation.
- Schema migration diff: `diff_schemas(old, new) ->
  Vec<SchemaChange>` with `is_destructive()`.
- Text + vector search index derives.
- Bulk `get_many(ids)` on query and mutation DB handles.

### Cross-cutting
- `convex_native::errors` helpers: `bad_request` (400),
  `unauthenticated` (401), `forbidden` (403), `not_found` (404),
  `conflict` (409), `rate_limited` (429), `overloaded` (503).
- `ctx.auth()` / `ctx.unix_timestamp()` / `ctx.log()`.
- `convex_native::testing::{TestCallbacks, CallRecord, args!}`
  for unit tests.
- `BuiltBackend::describe_json() / describe_pretty()` stable
  introspection envelope; `crons` surfaces through
  `describe_pretty_full` / `describe_json_full`.
- `convex_native::VERSION` constant surfaced through
  introspection + `worker` `Health` response.

## Missing / outstanding

### No end-to-end smoke test driven from a real client
The composite runner dispatches native queries/mutations, the
`make_app()` wiring is in place, and `cargo build --bin
convex-local-backend` completes cleanly. What's missing is a
scripted test that boots the backend with a registered
`#[convex::query]` and drives it through the websocket or HTTP
client path. Integration today is verified by `cargo test -p
convex_native` plus the successful binary build.

**Effort**: small-to-medium. Mechanics exist — needs a test harness
that owns the backend lifecycle and a client reaching in.

### Document shape validation is off
`table_definition()` currently emits `document_type: None`, i.e.
every derived document currently gets an "any" schema shape.
Enforcing the shape against the struct's fields would let the
database layer reject shape-violating writes instead of relying on
`from_convex_object` failing downstream at read time.

**Effort**: medium. Requires reflecting struct fields through the
proc macro into a concrete `DocumentSchema::Union(Vec<ObjectValidator>)`
and handling the recursive `Validator` type for nested / enum /
union fields.

### Mutation-scoped scheduling is a no-op
`MutationCtx::scheduler()` returns a scheduler bound to
`NoopCallbacks`, so `run_after` inside a `#[convex::mutation]` bails
with "no callbacks attached". The correct wiring is through
`VirtualSchedulerModel` on the mutation's active transaction so the
scheduled job commits atomically with the rest of the mutation's
writes (matching the JS `ctx.scheduler.runAfter` semantics).

**Effort**: medium. The `NativeActionCallbacks` trait is
`&self`-async and doesn't have a path to the live
`&mut Transaction<RT>`. A clean fix needs either a new scheduler
type dedicated to the mutation scope or a transaction-aware
callbacks trait. Action-scoped scheduling works today because
actions dispatch through `run_action_with_callbacks`, which
attaches `BackendCallbacks`.

**Workaround today**: schedule through an action
(`ctx.run_action(...)` → that action's scheduler), or write to the
`_scheduled_functions` system table directly via `ctx.tx()`.

### Native action `FunctionFinalTransaction` is always `None`
The composite intercepts `UdfType::Action` and dispatches native
actions via `run_action_with_callbacks`, producing a real
`FunctionOutcome::Action(ActionOutcome { .. })`. What's not threaded
through is a transaction snapshot — native `ActionCtx` has no
`Transaction<Rt>` today, so the returned `final_tx` is always
`None`. JS actions behave the same way (no transaction writes);
native's extra limitation is that `ctx.db().get(...)` isn't
available directly on the ctx — you route through
`ctx.run_query(...)` / `ctx.run_query_by_name(...)`.

**Mitigation shipped**: query sub-calls within a single action
share a snapshot — `dispatch_native_action` pins
`database.now_ts_for_reads()` once and threads it into
`BackendCallbacks::with_snapshot_ts(...)`, so every native query
sub-call inside one action opens its transaction at the same ts.
Mutations still commit at a fresh ts (committing at a stale one
would lose writes).

**Effort**: medium-to-large. Either extend `ActionCtx` with an
optional `Transaction<Rt>` borrow, or add a read-only pass-through
API on the ctx that reuses the pinned snapshot.

### Non-obvious caveats

- `run_at` reads real wall-clock time via `SystemTime::now()`, not
  the runtime clock. Tests that mock the runtime need to compute
  the delay from `ctx.unix_timestamp()` and call `run_after` with
  a `Duration`.
- The typed `.page()` does not drive reactive pagination — the sync
  layer calls a different code path. `.page()` is the right shape
  for one-shot scrolls inside a query/mutation handler, not for
  live-updating UI paginators.
- `CONVEX_MODE=conductor` isn't accepted from `convex-local-backend`
  itself (a conductor-only role doesn't fit a binary that always
  boots a local `Database`). Use
  `convex_native_distributed::examples::conductor` for that mode.
