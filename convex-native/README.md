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

**Phase 1 COMPLETE** (1.0.1 → 1.3.3, 1.2.4 / 1.2.5, 1.4.1–1.4.4, 1.5.1, 1.5.2,
1.6.1–1.6.3), **Phase 2 COMPLETE** (2.1–2.8), **Phase 3 partial** (3.1
proto contract + 3.2 crate skeleton/conversions + 3.3 worker server
full + 3.4 conductor client + P2C + real transport + 3.5 mode
switching helpers + 3.6 multi-worker integration tests + executor
trait stub), **Phase 4
partial** (4.1 fastrace spans + 4.2 metrics sink + 4.3 graceful drain
+ 4.4 timeouts + 4.5 circuit breaker + 4.6 index-cache warmup plan
+ 4.7 rolling update routing),
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

- Current test tallies (all green):
  - `cargo test -p convex_native` — **92 tests** (unit + derive +
    runtime integration).
  - `cargo test -p convex_native_backend` — **10 tests** (pure
    helpers; deeper paths covered by end-to-end convex-local-backend
    builds).
  - `cargo test -p convex_native_distributed` — **41 tests** (33
    unit + 7 multi-worker integration + 1 subprocess smoke).
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

### New — native-action storage uploads via `FileStorage`

`ctx.storage().store(bytes, content_type)` from inside a
`#[convex::action]` now uploads directly through `FileStorage::store_file`.
The composite runner accepts an optional `.with_file_storage(fs)`
builder; `local_backend::make_app()` wires in the same `file_storage`
it hands to `Application::new`, so native actions get raw-byte
uploads for free. Without the builder the call returns a clear
"requires a FileStorage handle" error instead of silently failing.

Under the hood the adapter wraps the `bytes::Bytes` payload in a
single-chunk `futures::stream::once`, builds a `ContentLength`
header from the buffer size, and parses the supplied content-type
string into a `headers::ContentType`. The resulting
`DeveloperDocumentId` becomes the `StorageId` returned to the
handler.

### New — native-to-native cross-call resolver in `BackendCallbacks`

`ctx.run_query_by_name("get_user", …)` and
`ctx.run_mutation_by_name(...)` from inside a `#[convex::action]`
now route through the native registry first. When the name matches
a registered native query/mutation and kinds agree, the adapter
opens a fresh `Transaction<Rt>` on the composite's `Database<RT>`
(TypeId-guarded, same pattern as the main dispatch path), runs the
handler, and returns the result (mutations also commit via
`commit_with_write_source`). Unknown names fall back to the existing
JS `ActionCallbacks` path so JS targets still work.

Typed sub-calls `ctx.run_query(Marker, Args { .. })` remain the
preferred form — they skip name resolution entirely and preserve
the compile-time arg/return signature. The name-based form is
mostly a fallback for JS targets and dynamic dispatch.

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
  distributed gRPC service serializes. The protobuf contract now
  lives at `crates/pb/protos/function_execution.proto` and generates
  `pb::function_execution::{ExecuteRequest, ExecuteResponse,
  HealthRequest, HealthResponse, FunctionExecutionService}` via
  `tonic_build`. The Rust shapes in `convex_native::distributed` are
  the tonic-free equivalents.
- `FunctionExecutor` trait — workers implement this to accept remote
  calls.
- **Phase 3.1 — proto service contract (shipped):** the `.proto` file
  defines two RPCs, `Execute` (dispatches one function) and `Health`
  (used for circuit-breaking + rolling-deploy version detection).
  Both messages pull from `common.proto` so the conductor can feed
  responses straight into the existing `UdfOutcome` envelope.
- **Phase 3.2 — crate skeleton + proto↔native conversions (shipped):**
  new `crates/convex_native_distributed/` crate now owns the wire
  boundary. `conversions.rs` maps `pb::function_execution::{Execute,
  Health}{Request,Response}` to/from the tonic-free
  `convex_native::distributed` types, plus helpers for
  `TableNamespace`, `ConvexObject` args, and `UdfType` round-trips.
- **Phase 3.4 — conductor-side client + P2C load balancer
  (shipped):** `DistributedFunctionRunner` holds a
  `Vec<Arc<dyn WorkerClient>>` and a `Chooser` (default
  `RandomChooser`). Each `execute(req, udf_type)` picks two worker
  indices via the chooser, sends to whichever has the lower
  `in_flight_estimate()`, and retries against the other on
  `tonic::Code::Unavailable`. Non-transient gRPC errors bubble up
  without retry.
- **Phase 3.4 transport — `TonicWorkerClient` (shipped):** real
  gRPC client wrapping a generated
  `FunctionExecutionServiceClient<Channel>`. Constructed via
  `TonicWorkerClient::connect("http://<addr>")` (opens a fresh
  channel) or `TonicWorkerClient::from_channel(chan, endpoint)`
  (reuses an existing one when the conductor multiplexes). Tracks
  in-flight locally through an `AtomicU64` guarded by a drop-bound
  `InFlightGuard`, so the P2C load balancer sees accurate
  counts without a network round-trip. Real end-to-end tests
  (conductor P2C → TonicWorkerClient → tonic server →
  `FunctionExecutionServer` → `NativeFunctionRunner`) spin up a
  gRPC server on an ephemeral port, bring up a client, exercise
  health + action execute + unimplemented query path, and verify
  `in_flight_estimate` returns to zero after the call drains.
- **Subprocess smoke test for worker+conductor (shipped):**
  `crates/convex_native_distributed/tests/examples_smoke.rs`
  picks an ephemeral port, spawns the `worker` example as a
  subprocess with matching `CONVEX_WORKER_BIND_ADDR`, waits
  until its listening socket is up, runs the `conductor`
  example against it, and asserts on the conductor's stdout
  + exit code. Catches proto drift / tonic-version mismatch /
  unused-import regressions that unit tests might miss because
  they exercise the crate API in-process. Auto-skips when the
  example binaries aren't built (the CI invocation needs
  `cargo build --examples -p convex_native_distributed` first;
  local dev loops pick up the build automatically).
- **Runnable worker/conductor examples (shipped):**
  `crates/convex_native_distributed/examples/worker.rs` reads
  `CONVEX_MODE` + `CONVEX_WORKER_BIND_ADDR` and boots a tonic
  server serving `FunctionExecutionService`.
  `crates/convex_native_distributed/examples/conductor.rs` reads
  `CONVEX_MODE` + `CONVEX_WORKER_ENDPOINTS`, probes health on
  each worker, prints a per-worker line, and exits non-zero if
  any probe failed. Running one against the other end-to-end is
  the lightest available smoke test of the distributed stack:
  ```sh
  CONVEX_MODE=worker cargo run -p convex_native_distributed --example worker &
  CONVEX_MODE=conductor CONVEX_WORKER_ENDPOINTS=http://127.0.0.1:4567 \
    cargo run -p convex_native_distributed --example conductor
  ```
  The conductor prints e.g.
  `http://127.0.0.1:4567 => v="0.1.0" traffic=true fns=0 in_flight=0`
  and exits 0.
- **Phase 4.7 — rolling updates with version-aware routing
  (shipped):** the worker's version gate lands upstream via
  `ExecuteRequest::min_registry_version` (added to both
  `convex_native::distributed::ExecuteRequest` and the proto).
  `DistributedFunctionRunner::with_min_registry_version(v)` sets a
  cluster-wide floor every dispatch inherits; per-call
  `ExecuteRequest::min_registry_version` overrides it. Workers
  that don't meet the floor reject with
  `tonic::Code::FailedPrecondition` at the top of `execute`, which
  the conductor surfaces to the caller so half-deployed clusters
  fail loud. Two integration tests cover the common case (floor
  rejects older workers; floor accepts compliant workers); a
  third verifies per-call override. Task 53 closed.
- **Phase 3.6 — multi-worker integration tests (shipped):** a
  dedicated integration-test binary at
  `crates/convex_native_distributed/tests/multi_worker.rs` spins
  up N real tonic servers on ephemeral ports, connects a
  `TonicWorkerClient` per worker, and drives the conductor through
  the full stack. Five scenarios:
  `two_workers_both_reachable_dispatch_succeeds` (N=2 live pool),
  `unreachable_worker_rejects_build` (asserts the conductor
  refuses partial connectivity),
  `version_gate_rejects_request_when_worker_is_older`,
  `failover_from_unavailable_to_healthy_worker` (a `WorkerClient`
  stub that returns `Unavailable` paired with a real tonic
  client), and `health_probes_report_per_worker_versions`.
  Total suite now 37/37 green (32 unit + 5 integration).
- **Unified `convex-local-backend` binary with `CONVEX_MODE=worker`
  (shipped):** `convex-local-backend` now accepts two modes:
  - `standalone` (default): HTTP only, as before.
  - `worker`: HTTP **plus** a tonic `FunctionExecutionService` bound
    to `CONVEX_WORKER_BIND_ADDR` (defaulting to `0.0.0.0:4567`). The
    tonic server shares the same `Database<Rt>` and
    `NativeFunctionRunner` the HTTP path uses, so a remote conductor
    and a local HTTP client see one consistent read timeline.
  Conductor mode is still rejected from this binary (a conductor
  doesn't own a `Database`; boot the dedicated
  `convex_native_distributed::examples::conductor` binary instead).
  The wiring lives in `local_backend::make_app` and hops through
  `convex_native_distributed::serve_worker_with_shutdown(addr,
  native, db, shutdown_future)`, which keeps `pb` and `tonic` out of
  `local_backend`'s direct dep graph. The shutdown future is a clone
  of the HTTP server's broadcast receiver, so Ctrl-C / the `/preempt`
  endpoint drains HTTP, the site proxy, and the worker gRPC server
  together — tonic `serve_with_shutdown` stops accepting new
  connections and lets in-flight RPCs finish before returning. The
  no-shutdown `serve_worker_with_database(...)` shim is still
  re-exported for callers that want "bind and run forever".
- **Phase 3.5 — binary-level mode switching helpers (shipped):**
  `mode.rs` decodes `CONVEX_MODE`, `CONVEX_WORKER_ENDPOINTS`
  (comma-separated gRPC URLs), and `CONVEX_WORKER_BIND_ADDR`
  (defaults to `0.0.0.0:4567`). `build_worker_server(native)`
  returns a `(tonic::transport::Server, FunctionExecutionServiceServer)`
  pair the binary wires onto the transport of its choice;
  `build_conductor_runner(endpoints)` connects a
  `TonicWorkerClient` per endpoint and wraps them in a
  `DistributedFunctionRunner`. Failing to reach any worker
  during `build_conductor_runner` refuses to start — safer
  than silently serving a reduced pool. 6 new unit tests
  (endpoint parsing, default bind-address resolution, malformed
  input rejection, unreachable-worker rejection, service
  construction).
- **Phase 3.3 — worker-side gRPC server (shipped):**
  `FunctionExecutionServer` implements the generated tonic trait.
  `Health` is fully wired: reports `registry_version` (defaults to
  `convex_native::VERSION`, overridable via `.with_registry_version`),
  `accepts_traffic` (flips off when the runner is draining),
  `registered_functions`, and `in_flight`. `Execute` handles
  `UdfType::Action` through `run_action_with_callbacks` (with
  `NoopCallbacks` until the worker is wired to real ones). With
  `.with_database(db)`, queries and mutations dispatch inline
  against a fresh `Transaction<Rt>` (queries drop the tx;
  mutations commit locally via `commit_with_write_source`) — this
  matches the "pure worker" topology where the worker owns its
  own `Database<Rt>`. Without a database handle, query and
  mutation requests return `Code::Unimplemented` so the conductor
  learns the worker wasn't provisioned for transactional traffic.
  A Phase-4.7 version gate at the top of `execute` rejects
  requests whose `min_registry_version` exceeds the worker's own.

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
`DatabaseSchema`, the `HttpRouter`, and the callbacks. The
`convex_native_backend::CompositeFunctionRunner` consumes the
runner + schema directly from the registry inventory; `BuiltBackend`
stays useful as an introspection and validation surface (for
`describe_json`, `validate`, `summary`).

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
  StorageId`, `get_url(id)`, and `delete(id)`. Each method routes
  through the attached `NativeActionCallbacks`; the
  `convex_native_backend::BackendCallbacks` adapter forwards
  `store` straight into `FileStorage::store_file` when the
  composite is built with `.with_file_storage(fs)`.
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

Both serialize the typed args correctly and then delegate to the
attached `NativeActionCallbacks::schedule`.
`convex_native_backend::BackendCallbacks` forwards the call to
`udf::ActionCallbacks::schedule_job`, so scheduling from a native
mutation or action now reaches the real backend scheduler when
the composite runner is wired.

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

`ctx.run_query` / `run_mutation` / `run_action` all route through
the attached `NativeActionCallbacks`. The
`convex_native_backend::BackendCallbacks` adapter short-circuits
native names (opens a fresh `Transaction<Rt>` on the composite's
`Database<RT>`, commits for mutations) and falls back to
`udf::ActionCallbacks::execute_query` / `execute_mutation` for
JS-side targets.

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
  an action end-to-end with `NoopCallbacks`; for real deployments the
  composite runner calls `run_action_with_callbacks` and injects a
  `convex_native_backend::BackendCallbacks` so sub-queries,
  sub-mutations, scheduling, and storage all reach the real
  backend.

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
- `NativeFunctionRunner` is deliberately standalone and does
  **not** implement `function_runner::FunctionRunner`. The
  adapter that does (wrapping a JS runner and delegating
  unmapped calls) lives in the sibling
  `convex_native_backend::CompositeFunctionRunner`. Keeping the
  trait impl out of this crate avoids pulling the `isolate` / V8
  build cost into every consumer — framework-only users
  (tests, CLI tools, codegen tooling) stay lightweight while the
  production binary picks up the adapter through
  `local_backend/src/lib.rs`.

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

- **No end-to-end smoke test driven from a real client.** The
  composite runner dispatches native queries/mutations, the wiring
  in `make_app()` is in place, and `cargo build --bin
  convex-local-backend` completes cleanly. What's missing is a
  scripted test that boots the backend with a registered
  `#[convex::query]` and exercises it through the HTTP client path —
  currently the integration is verified by `cargo test -p
  convex_native` plus the successful binary build.
- **Document shape validation is still off** (see next bullet) — the
  non-indexed filter limitation is gone: `.eq()`/`.gt()`/etc. without a
  preceding `.with_index(...)` now lower to a full-table scan plus a
  stack of `QueryOperator::Filter` predicates (covered by
  `make_query_*` unit tests in `ctx::query_builder`). The indexed path
  is still faster — use an index when one exists.
- **Document type validation is off.** `table_definition()` emits
  `document_type: None` — i.e. every derived type currently gets an "any"
  schema shape. Enforcing the shape against the struct's fields is
  Phase 1 extension work, not part of the MVP critical path.
- **Native action `FunctionFinalTransaction` is always `None`.** The
  composite now intercepts `UdfType::Action` and dispatches native
  actions via `run_action_with_callbacks`, building a real
  `FunctionOutcome::Action(ActionOutcome { .. })`. What's not
  threaded through is a transaction snapshot — native `ActionCtx`
  has no tx today, so the returned `final_tx` is always `None`. JS
  actions behave the same way (no transaction writes), but the lack
  of a tx means native actions can't observe read-time consistency
  from a direct `ctx.db().get(...)` call (you still have to route
  through `ctx.run_query(...)` / `ctx.run_query_by_name(...)`).
  Query sub-calls _do_ now share a snapshot: `dispatch_native_action`
  pins `database.now_ts_for_reads()` once and threads it into
  `BackendCallbacks::with_snapshot_ts(...)`, so every native query
  sub-call inside one action opens its transaction at the same ts.
  Mutations still commit at a fresh timestamp — committing at a
  stale ts would lose writes — so interleaved mutations are visible
  to later queries only if the caller explicitly re-reads through a
  new action.

## Architecture (today)

```
crates/convex_native/              -- framework crate (no isolate dep)
├── src/
│   ├── lib.rs                     -- re-exports, __private module for generated code
│   ├── auth.rs                    -- AuthInfo (ctx.auth())
│   ├── backend.rs                 -- ConvexBackend builder + BuiltBackend
│   ├── callbacks.rs               -- NativeActionCallbacks trait + NoopCallbacks
│   ├── circuit_breaker.rs         -- CircuitBreaker + config
│   ├── convert.rs                 -- ToConvex / FromConvex
│   ├── ctx/
│   │   ├── action.rs              -- ActionCtx
│   │   ├── mutation.rs            -- MutationCtx + MutationDb
│   │   ├── query.rs               -- QueryCtx + QueryDb
│   │   ├── query_builder.rs       -- TypedQueryBuilder (typed, executable)
│   │   ├── scheduler.rs           -- Scheduler
│   │   └── storage.rs             -- StorageCtx + StorageId
│   ├── distributed.rs             -- ConvexMode + ExecuteRequest/Response + trait stub
│   ├── document.rs                -- ConvexDocument + FieldReference + IndexReference
│   ├── errors.rs                  -- bad_request / unauthenticated / ... helpers
│   ├── function_ref.rs            -- ConvexQueryFunction / Mutation / Action marker traits
│   ├── http.rs                    -- HttpActionCtx + HttpRequest/Response + HttpRouter
│   ├── id.rs                      -- Id<T: ConvexDocument>
│   ├── introspect.rs              -- describe_json / describe_pretty
│   ├── logging.rs                 -- LogBuffer + Logger (ctx.log())
│   ├── metrics.rs                 -- NativeMetricsSink + CountingMetrics
│   ├── prelude.rs                 -- glob-import target
│   ├── registry.rs                -- NativeFunctionRegistration + NativeFunctionRegistry
│   ├── runner.rs                  -- NativeFunctionRunner (dispatch, timeout, drain, breaker)
│   ├── schema.rs                  -- TableRegistration + NativeSchema::collect()
│   ├── schema_diff.rs             -- diff(old, new) -> Vec<SchemaChange>
│   ├── testing.rs                 -- TestCallbacks + args! macro
│   └── warmup.rs                  -- plan_warmup(schema)
├── examples/
│   └── tiny_app.rs                -- end-to-end runnable demo
└── tests/
    ├── backend_builder.rs         -- ConvexBackend end-to-end
    ├── callbacks_wiring.rs        -- NativeActionCallbacks sub-calls / scheduler / storage
    ├── ctx_types.rs               -- compile-time surface tests for ctx wrappers
    ├── drain.rs                   -- graceful shutdown drain
    ├── derive_document.rs         -- ConvexDocument integration
    ├── derive_enums_nested_unions.rs -- ConvexEnum / Nested / Union
    ├── derive_functions.rs        -- function attribute macros
    ├── function_refs.rs           -- marker types
    ├── golden_path.rs             -- full realistic app end-to-end
    ├── http_actions.rs            -- HTTP action registration
    ├── metrics_wiring.rs          -- runner metrics + timeout enforcement
    ├── runner_dispatch.rs         -- NativeFunctionRunner dispatch
    ├── search_indexes.rs          -- text/vector search indexes
    └── testing_utilities.rs       -- TestCallbacks smoke test

crates/convex_native_backend/      -- backend adapter (requires isolate dep)
├── src/
│   ├── lib.rs                     -- re-exports
│   ├── composite_runner.rs        -- CompositeFunctionRunner<RT>: FunctionRunner impl
│   └── callbacks_adapter.rs       -- BackendCallbacks: NativeActionCallbacks -> ActionCallbacks

crates/convex_macro/
├── src/
│   ├── lib.rs                     -- proc macro entry points
│   ├── convex_document.rs         -- #[derive(ConvexDocument)]
│   ├── convex_enum.rs             -- #[derive(ConvexEnum)]
│   ├── convex_nested.rs           -- #[derive(ConvexNested)]
│   ├── convex_union.rs            -- #[derive(ConvexUnion)]
│   ├── cron.rs                    -- #[convex::cron(...)]
│   ├── http_action.rs             -- #[convex::http_action(...)]
│   └── native_function.rs         -- #[convex::query/mutation/action]
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

Per `IMPLEMENTATION_PLAN.md`, the remaining shippable items are:

1. **End-to-end client smoke test.** Boot `convex-local-backend` with
   a registered `#[convex::query]`, drive it through the websocket /
   HTTP client, assert the response matches what the handler returns.
2. **Step 3.1–3.6** — real `convex_native_distributed` gRPC crate
   using the `ExecuteRequest` / `ExecuteResponse` scaffolding that
   already lives in `convex_native::distributed`.
3. **Step 4.7** — rolling updates with version-aware routing.
4. **Native `ActionCtx` snapshot transaction.** Today the native
   `ActionCtx` has no transaction at all; sub-calls happen through
   `run_query_by_name` which opens its own. If an action needs a
   stable read-time view, we'd either need to pass a snapshot `ts`
   through the `BackendCallbacks` or give `ActionCtx` its own
   optional `Transaction<Rt>`.

Agents iterating on this project: please keep this document honest about
what is merged vs what is planned, after each commit.
