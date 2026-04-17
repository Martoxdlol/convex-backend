# CompositeFunctionRunner — integration reference (monolith topology)

> **This describes the monolith topology's dispatch path.** Under
> the monolith (`local_backend` + native functions linked in —
> see `STANDALONE.md`), the composite runner is how native
> registrations reach the `Application` layer. The target
> distributed architecture in `DISTRIBUTED_PLAN.md` replaces this
> with a `WorkerPool` that implements `FunctionRunner` by
> dispatching over gRPC; this file remains the reference for the
> alternative monolith shape.

**Shipped in-workspace** at
`crates/convex_native_backend/src/composite_runner.rs`. This doc
describes the adapter's responsibilities and the one-to-one mapping
of `FunctionRunner` trait methods onto either the native path or the
wrapped JS runner.

Related docs: `README.md` (project landing page), `USAGE.md`
(developer-facing feature reference — section 18 covers running
against a real backend, section 20 lists what lives outside this
crate), `STATUS.md` (outstanding work, including the "native
`ActionCtx` snapshot transaction" gap that's relevant here).

The adapter depends on `function_runner` (transitively on `isolate`),
which means the `npm-packages/` rush install + build step must have
run at least once for the host crate to compile. `convex_native`
itself stays isolate-free and builds on its own.

## How it is wired

`crates/local_backend/src/lib.rs` instantiates the composite ahead of
the `Application::new` call:

```rust
let js_runner: Arc<dyn FunctionRunner<ProdRuntime>> = Arc::new(
    InProcessFunctionRunner::new(... database.clone() ...)?,
);
let native_runner = Arc::new(convex_native::NativeFunctionRunner::from_inventory()?);
tracing::info!(
    "Native function registry: {} registered",
    native_runner.len(),
);
let function_runner: Arc<dyn FunctionRunner<ProdRuntime>> = Arc::new(
    convex_native_backend::CompositeFunctionRunner::new(
        native_runner,
        js_runner,
        database.clone(),
    )
    .with_file_storage(file_storage.clone()),
);
```

The `.with_file_storage(...)` chain lets native `ctx.storage().store(...)`
uploads bypass the JS callback path and land directly in the backend's
`FileStorage::store_file`; omit it and storage calls from native
actions error at dispatch time.

Every build of `convex-local-backend` therefore transparently picks
up any `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
statically registered via `inventory::submit!`.

## Method-by-method behaviour

| Trait method | Composite behaviour |
|---|---|
| `run_function` (query/mutation, native name) | `dispatch_native`: open `Transaction<RT>` via `Database::begin_with_ts`, run native handler, convert to `FunctionFinalTransaction`, build synthetic `UdfOutcome`. |
| `run_function` (query/mutation, non-native) | Delegate to wrapped JS runner. |
| `run_function` (action, native name) | `dispatch_native_action`: resolve the cached `ActionCallbacks` via the `Weak` stored from `set_action_callbacks`, pin `database.now_ts_for_reads()` as the action's read snapshot, wrap everything in a `BackendCallbacks::with_native(...).with_snapshot_ts(ts)`, call `NativeFunctionRunner::run_action_with_callbacks`, synthesize an `ActionOutcome`. Returns `final_tx = None` because native actions don't take a transaction. |
| `run_function` (action, non-native; http_action) | Delegate to wrapped JS runner. |
| `analyze` | Delegate to JS. |
| `evaluate_app_definitions` | Delegate to JS. |
| `evaluate_component_initializer` | Delegate to JS. |
| `evaluate_schema` | Merge `NativeSchema::collect()` with the JS schema; collision on table name is a hard error. |
| `evaluate_auth_config` | Delegate to JS. |
| `set_action_callbacks` | Store a `Weak<dyn ActionCallbacks>` locally (for native action dispatch) and forward the `Arc` to the wrapped JS runner. |

## The native dispatch path

`dispatch_native::<RT>` (in `composite_runner.rs`):

1. TypeId-checks that `RT == convex_native::Rt` — native handlers
   are monomorphic over `ProdRuntime` because `inventory` can't hold
   generic fn pointers. Other runtimes bail with a clear error.
2. Opens `database.begin_with_ts(identity, *ts, usage_tracker)` and
   then performs a guarded unsafe cast from `&mut Transaction<RT>`
   to `&mut Transaction<Rt>`. The TypeId check above is what makes
   the cast sound.
3. Decodes the serialized args into a single `ConvexObject` (native
   handlers accept a single object, mirroring the JS calling
   convention).
4. Dispatches on `HandlerFn::{Query, Mutation}` — a kind mismatch
   between the request's `UdfType` and the registered handler kind
   is a hard error.
5. Converts the finished `Transaction<RT>` into
   `FunctionFinalTransaction::try_from(tx)?`.
6. Maps the handler's `anyhow::Result<ConvexValue>` to
   `Result<JsonPackedValue, JsError>` via `JsError::from_error_ref`
   (preserves `ErrorMetadata` categorization for user-vs-system
   errors).
7. Wraps the result in a `UdfOutcome` (with `rng_seed` drawn from the
   runtime's RNG, `unix_timestamp` from the runtime's clock,
   `user_execution_time` from the measured `Instant::elapsed`) and
   returns the `(final_tx, outcome, usage_stats)` tuple.

## `BackendCallbacks`

`callbacks_adapter.rs` implements `convex_native::NativeActionCallbacks`
on top of `udf::ActionCallbacks`:

- `run_query_by_name` / `run_mutation_by_name`: wrap the callback's
  UDF invocation and canonicalize the path through
  `CanonicalizedComponentFunctionPath { component: root(), udf_path }`.
- `schedule` / `cancel_scheduled`: delegate to the underlying
  callbacks.
- `storage_store`: when `with_file_storage(...)` was called on
  construction, uploads the raw `bytes::Bytes` payload directly via
  `FileStorage::store_file`. Wraps the byte buffer in a
  single-chunk stream, builds `ContentLength` from the buffer size,
  parses the supplied content-type string. Returns the resulting
  `DeveloperDocumentId` as a `StorageId`. Without `with_file_storage`
  it errors with a helpful message pointing at the builder.
- `storage_get_url` / `storage_delete`: delegate.

## Known limitations

- **Cross-call name resolution.** `BackendCallbacks::run_query_by_name`
  and `run_mutation_by_name` now short-circuit native names: before
  the path is parsed as a JS `module:function` reference, the adapter
  asks the native registry whether the bare name is registered. If
  it is — and the kind matches — the sub-call runs inline against a
  fresh `Database::begin_with_ts` transaction (queries drop the tx,
  mutations commit via `commit_with_write_source`). This makes
  `ctx.run_query_by_name("get_user", …)` land on the native handler
  even though "get_user" isn't a valid JS path.
  Known limitation: sub-mutations commit in a **separate** transaction
  from the caller's action, so there's no outer atomicity. Actions
  don't have transactions in the first place, so this matches the
  user-visible semantics; but a `#[convex::action]` that calls two
  mutations in a row will not see them as atomic.
- **Per-action query snapshot.** `dispatch_native_action` pins
  `database.now_ts_for_reads()` once per action and threads it into
  `BackendCallbacks::with_snapshot_ts(ts)`. Every native query
  sub-call inside that action therefore opens its `Transaction<Rt>`
  at the same read timestamp — two `ctx.run_query(...)` calls see
  one consistent world. Mutations deliberately **do not** honour the
  snapshot (committing at a stale ts would lose writes), so a
  mutation sub-call's writes are **not** visible to subsequent query
  sub-calls in the same action; the caller re-observes them through
  a fresh action or query.
- **Write threading.** `begin_tx_with_writes` now forwards
  `existing_writes.updates` through `tx.merge_writes` after opening
  the transaction, mirroring the JS `FunctionRunnerCore::begin_tx`
  behaviour. Multi-UDF-per-request batching therefore sees earlier
  writes inside one `ApplicationFunctionRunner` call. Empty update
  sets skip the merge to avoid a round-trip through `tx.writes`.
- **Log lines.** The composite threads a shared `LogBuffer` into
  the native ctx and drains it after the handler returns, mapping
  each `NativeLogLine` to a `common::log_lines::LogLine` stamped
  with the runtime's current `UnixTimestamp`. For queries and
  mutations the drained lines populate `UdfOutcome::log_lines`.
  For actions they're streamed through the
  `log_line_sender: mpsc::UnboundedSender<LogLine>` `run_function`
  hands to the runner — matching the JS action path. When no
  sender is wired (e.g. dispatch from a test harness) the lines
  are dropped silently; the handler itself still succeeds.
- **Observed flags.** `observed_identity`, `observed_time`, and
  `observed_rng` are all tracked per-invocation. The runner seeds
  an `Arc<convex_native::ctx::query::Observed>` (carrying a
  ChaCha20Rng seeded from the same `rng_seed` written to
  `UdfOutcome::rng_seed`) and threads it through the ctx
  constructor (`with_log_buffer_and_observed`). `ctx.auth()`,
  `ctx.unix_timestamp()`, and `ctx.rng_u64()` / `ctx.rng_fill(buf)`
  each flip their matching bit; the runner drains all three into
  the outcome after the handler returns.
