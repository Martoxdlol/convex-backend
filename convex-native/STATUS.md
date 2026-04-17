# Project status

Authoritative list of what is shipped, what is missing, and what the
remaining work looks like. For day-to-day developer usage read
`USAGE.md`; for a guided walkthrough read `QUICKSTART.md`; for the
JS → Rust cheatsheet read `MIGRATION.md`.

## One-line summary

Phases 1 / 2 / 4 / 5 of `IMPLEMENTATION_PLAN.md` are fully shipped.
Phase 3 (distributed execution) is shipped at the crate level plus a
`convex-local-backend` `CONVEX_MODE=worker` mode. Mutation-scoped
scheduling wires through `VirtualSchedulerModel`, every derived
document emits a concrete `DocumentSchema` for write-time shape
validation, native `ActionCtx::db()` reads at a pinned snapshot via
`NativeActionCallbacks::read_document_at_snapshot`, and an end-to-end
client smoke test drives a registered native action through a real
`TonicWorkerClient` over gRPC. The residual gap is a WebSocket/HTTP
client smoke test driven from `convex-local-backend`, which is
blocked on `rush install` rather than on native-dispatch behaviour
(see "End-to-end smoke coverage" below).

## Test tallies

```
cargo test -p convex_native              # 239 tests
cargo test -p convex_native_backend      # 10 tests
cargo test -p convex_native_distributed  # 57 tests

# total: 306 — all green
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
- `ExecuteRequest.execution_context` propagates across the
  gRPC boundary — request-id / execution-id / parent-scheduled-job
  chains span both processes. The mutation scheduler inherits it
  automatically.
- `ExecuteResponse.log_lines` carries worker-side `ctx.log()`
  output over the wire; `ConductorLogSink` forwards them into the
  conductor's log-streaming path.
- `WorkerActionCallbacks` — native-only callbacks the worker
  installs when a `Database<Rt>` is attached, so actions
  dispatched through the distributed path can sub-call
  queries/mutations, schedule jobs, and do snapshot reads.
- `DistributedFunctionRunner::in_flight_per_worker()` gauge
  snapshot (`native_funrun_in_flight_per_worker`).

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
- Conductor-side metrics hook
  (`convex_native_distributed::ConductorMetricsSink` +
  `CountingConductorMetrics`) records every
  `DistributedFunctionRunner::execute` with the attributed worker
  label, `ConductorOutcome::{Ok, Retried, Error(tonic::Code)}`, and
  total latency — the `native_funrun_{requests,duration,errors}`
  surface named in design §12.3.

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
- Every ctx exposes `ctx.auth()` (with `user_identity()` +
  `subject()` matching `ctx.auth.getUserIdentity()`),
  `ctx.unix_timestamp()`, `ctx.log()`, `ctx.rng_u64()` /
  `ctx.rng_fill(...)`, and `ctx.execution_context()`. Query /
  mutation ctxs additionally expose
  `ctx.db().normalize_id(...)`; the action ctx exposes
  `ctx.db().get<T>(id)` (snapshot-pinned) +
  `ctx.storage().get_metadata(id)`.
- `observed_identity` / `observed_time` / `observed_rng` are
  all populated on `UdfOutcome` based on what the handler
  actually read.
- `convex_native::testing::{TestCallbacks, CallRecord, args!}`
  for unit tests.
- `BuiltBackend::describe_json() / describe_pretty()` stable
  introspection envelope; `crons` surfaces through
  `describe_pretty_full` / `describe_json_full`.
- `convex_native::VERSION` constant surfaced through
  introspection + `worker` `Health` response.
- `ctx.log()` output is drained into the backend's log-streaming
  path. Query / mutation lines populate `UdfOutcome::log_lines`;
  action lines stream through the `log_line_sender` the function
  runner passes in. Level + message mapping is 1:1 to
  `common::log_lines::LogLine`.

## Missing / outstanding

### End-to-end smoke coverage (shipped for the gRPC client path)
`crates/convex_native_distributed/tests/client_e2e_smoke.rs` boots
a `FunctionExecutionServer` on an ephemeral port with an
inventory-collected `#[convex::action]` registered, connects a real
`TonicWorkerClient` over TCP, and round-trips typed args through the
wire format into the registered handler and back. A companion test
covers the "unknown function" path to pin that a handler miss
surfaces as `ExecuteResponse::Err("...")` rather than as a
transport-level failure — any regression in proto conversions,
registry lookup, or tonic wiring is caught at the layer closest to
the break.

**Still outstanding**: HTTP/WebSocket client path. Exercising the
`convex-local-backend` binary directly needs `rush install` under
`npm-packages/` (the V8-backed `isolate` crate is a build-time
dependency of `convex_native_backend`). The dispatch behaviour the
gRPC smoke test validates is the same behaviour the composite
runner exposes through HTTP/WebSocket, so the gap is the bundling
(boot the full local backend and drive through the Convex client)
rather than a dispatch correctness risk.

### Document shape validation (shipped)
`#[derive(ConvexDocument)]` now emits
`document_type: Some(DocumentSchema::Union(vec![ObjectValidator]))`
— reflected at macro-expansion time from the struct's fields so the
database layer rejects shape-violating writes up-front instead of
relying on `from_convex_object` failing downstream at read time.

A new `ConvexSchema` trait (`convex_native::ConvexSchema`) carries
the reflection: `fn validator() -> Validator` plus an
`field_is_optional()` signal for `Option<T>`. Impls cover primitives
(`String`, `i64`, `f64`, `bool`, `ConvexValue`), containers (`Vec<T>`,
`Option<T>`, `BTreeMap<String, V>`), and `Id<T>`. The
`#[derive(ConvexDocument)]`, `#[derive(ConvexNested)]`,
`#[derive(ConvexEnum)]`, and `#[derive(ConvexUnion)]` macros emit
`ConvexSchema` impls in addition to their `ToConvex`/`FromConvex`
output, so nested types compose.

`Vec<u8>` is special-cased inside the derive macros to emit
`Validator::Bytes` rather than `Validator::Array(Int64)` — `u8`
has no standalone `ConvexSchema` impl by design, so the bytes-vs-
array distinction lives at the macro AST level.

### Mutation-scoped scheduling (shipped)
`MutationCtx::scheduler()` now returns a
`MutationScheduler<'_, RT>` that writes scheduled jobs through
`VirtualSchedulerModel` on the mutation's own transaction — so the
scheduled job commits atomically with the rest of the mutation's
writes, matching the JS `ctx.scheduler.runAfter` semantics. The
action-scoped `Scheduler` (callback-based) is unchanged; only the
mutation path switched.

API: `ctx.scheduler().run_after(Duration, Marker, Args)` /
`run_at(UnixTimestamp, Marker, Args)` for mutations, the same for
actions (`run_action_after` / `run_action_at`), and
`cancel(DeveloperDocumentId)`. Every scheduling call uses a
freshly-minted `ExecutionContext` by default; override via
`scheduler().with_execution_context(ctx)` when a parent
request-id chain needs to be preserved.

`run_at` / `run_action_at` on the mutation-scoped scheduler compute
the delay against the transaction's runtime clock
(`self.tx.runtime().unix_timestamp()`), so tests driving a mocked
runtime see the mocked time. The action-scoped scheduler still
routes through `BackendCallbacks::schedule`, which computes against
`SystemTime::now()` — the action's callbacks don't hold a `Runtime`
handle today; see the "Non-obvious caveats" section.

### Native action snapshot-pinned `ctx.db()` (shipped)
Native `ActionCtx` now exposes a read-only `ctx.db()` returning an
`ActionDb<'_>` with `get<T>(id)` / `try_get<T>(id)` /
`get_many<T>(ids)`. The handle routes each read through
`NativeActionCallbacks::read_document_at_snapshot`, which the
backend adapter implements by opening a fresh read-only
`Transaction<Rt>` pinned to the action's snapshot ts — so multiple
`ctx.db().get(...)` calls inside one action observe one consistent
world, matching how `dispatch_native_action` already pins query
sub-calls via `BackendCallbacks::with_snapshot_ts(...)`.

Writes still go through `ctx.run_mutation(...)` (mutations commit
in their own transaction; actions don't own one). The
`FunctionOutcome::Action(ActionOutcome { final_tx: None, .. })`
shape is unchanged — matching JS-action semantics — so the
"always None" report is expected, not a gap.

**Limitations**: the distributed worker and `NoopCallbacks` both
bail on `read_document_at_snapshot` with a clear error. Tests can
register handlers via
`TestCallbacksBuilder::on_doc_read(table, fn)`.

### Non-obvious caveats

- **`run_at` / `run_action_at`** compute their delay against
  `NativeActionCallbacks::unix_timestamp_now()`.
  `BackendCallbacks` overrides that to return
  `database.runtime().unix_timestamp()` when a `Database<RT>` is
  wired, so mocked-clock tests driving a real backend see the
  mocked time and can assert on the computed delay. Fallback
  callbacks (`NoopCallbacks`, JS-only `BackendCallbacks` without
  a native `Database`) still use `SystemTime::now()`.
  Mutation-scoped scheduling goes through `VirtualSchedulerModel`
  and reads the runtime clock directly from the transaction.
- The typed `.page()` does not drive reactive pagination — the sync
  layer calls a different code path. `.page()` is the right shape
  for one-shot scrolls inside a query/mutation handler, not for
  live-updating UI paginators.
- `CONVEX_MODE=conductor` isn't accepted from `convex-local-backend`
  itself (a conductor-only role doesn't fit a binary that always
  boots a local `Database`). Use
  `convex_native_distributed::examples::conductor` for that mode.
