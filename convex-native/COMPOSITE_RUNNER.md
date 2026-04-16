# CompositeFunctionRunner — integration reference

**Shipped in-workspace** at
`crates/convex_native_backend/src/composite_runner.rs`. This doc
describes the adapter's responsibilities and the one-to-one mapping
of `FunctionRunner` trait methods onto either the native path or the
wrapped JS runner.

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
    ),
);
```

Every build of `convex-local-backend` therefore transparently picks
up any `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
statically registered via `inventory::submit!`.

## Method-by-method behaviour

| Trait method | Composite behaviour |
|---|---|
| `run_function` (query/mutation, native name) | `dispatch_native`: open `Transaction<RT>` via `Database::begin_with_ts`, run native handler, convert to `FunctionFinalTransaction`, build synthetic `UdfOutcome`. |
| `run_function` (query/mutation, non-native) | Delegate to wrapped JS runner. |
| `run_function` (action/http_action) | Delegate to wrapped JS runner. Native actions go through `NativeFunctionRunner::run_action_with_callbacks` elsewhere; they do not fit the "run inside a transaction" shape. |
| `analyze` | Delegate to JS. |
| `evaluate_app_definitions` | Delegate to JS. |
| `evaluate_component_initializer` | Delegate to JS. |
| `evaluate_schema` | Merge `NativeSchema::collect()` with the JS schema; collision on table name is a hard error. |
| `evaluate_auth_config` | Delegate to JS. |
| `set_action_callbacks` | Delegate to JS. |

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
- `storage_store`: errors with a clear "not implemented" message —
  `udf::ActionCallbacks` only accepts pre-uploaded
  `FileStorageEntry` values, so forwarding raw bytes needs a direct
  path into the `file_storage` backend that bypasses the JS shape.
- `storage_get_url` / `storage_delete`: delegate.

## Known limitations

- **Cross-call path naming.** `BackendCallbacks::path_for` parses the
  `name` argument through `UdfPath::from_str`, which uses the JS
  `module:function` convention. Native functions are registered in
  the inventory by bare identifier (the Rust fn name, e.g.
  `"get_user"`), so calling a native function from a native action
  via `ctx.run_query_by_name("get_user", ...)` returns a parse error
  today. Cross-calls that go `native → JS` work as long as the name
  is fully qualified (`"users:get"`); cross-calls that go
  `native → native` need a registry-aware resolver that maps the
  bare identifier to a synthetic `UdfPath` before handing off. This
  is captured by the test `path_for_rejects_malformed_input` in
  `callbacks_adapter.rs`.
- **Write threading.** `begin_tx_with_writes` ignores
  `existing_writes` and opens a fresh transaction at `ts`. One-UDF-per-request
  flows work; JS-style batching inside a single
  `ApplicationFunctionRunner` call does not.
- **Log lines.** The composite builds a `UdfOutcome` with
  `log_lines: vec![].into()`. The native `LogBuffer` that
  `ctx.log()` fills is not yet drained into the outcome. Not a
  correctness issue; it means `ctx.log()` output does not surface in
  the backend's log-streaming path yet.
- **Observed flags.** `observed_identity`, `observed_rng`,
  `observed_time` are hard-coded `false`. The JS path tracks whether
  the UDF actually looked at identity/rng/time; native code could do
  the same with a one-bit flag per ctx accessor, but the
  determinism-check behaviour is the same in practice today because
  every native invocation rebuilds its transaction.
- **Storage.** See `BackendCallbacks::storage_store` above.
