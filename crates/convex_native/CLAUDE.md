# convex_native — agent notes

Short guide for agents iterating on this crate. Read alongside
`../../convex-native/README.md` (current state + features) and
`../../convex-native/QUICKSTART.md` (developer-facing API).

## Crate layout

```
src/
├── lib.rs              -- re-exports, __private module for macro code
├── auth.rs             -- AuthInfo (ctx.auth())
├── backend.rs          -- ConvexBackend builder + BuiltBackend
├── callbacks.rs        -- NativeActionCallbacks trait + NoopCallbacks
├── circuit_breaker.rs  -- CircuitBreaker + CircuitBreakerConfig
├── convert.rs          -- ToConvex / FromConvex
├── cron.rs             -- CronRegistration inventory + collect
├── ctx/
│   ├── action.rs       -- ActionCtx
│   ├── mutation.rs     -- MutationCtx + MutationDb
│   ├── query.rs        -- QueryCtx + QueryDb
│   ├── query_builder.rs-- TypedQueryBuilder
│   ├── scheduler.rs    -- Scheduler
│   └── storage.rs      -- StorageCtx + StorageId
├── distributed.rs      -- ConvexMode + ExecuteRequest/Response + FunctionExecutor stub
├── document.rs         -- ConvexDocument / FieldReference / IndexReference
├── errors.rs           -- bad_request / forbidden / ... helpers
├── function_ref.rs     -- ConvexQueryFunction / etc. marker traits
├── http.rs             -- HttpActionCtx + HttpRequest/Response + HttpRouter
├── id.rs               -- Id<T: ConvexDocument>
├── introspect.rs       -- describe_json / describe_pretty
├── logging.rs          -- LogBuffer + Logger (ctx.log())
├── metrics.rs          -- NativeMetricsSink + CountingMetrics
├── prelude.rs          -- glob-import target
├── registry.rs         -- NativeFunctionRegistration + Registry
├── runner.rs           -- NativeFunctionRunner (dispatch, timeout, drain, breaker, metrics)
├── schema.rs           -- NativeSchema::collect()
├── schema_diff.rs      -- diff(old, new) -> Vec<SchemaChange>
├── testing.rs          -- TestCallbacks builder + args! macro
└── warmup.rs           -- plan_warmup(schema) -> Vec<WarmupEntry>
```

Proc macros live in `../convex_macro/src/`:

```
convex_document.rs      -- #[derive(ConvexDocument)]
convex_enum.rs          -- #[derive(ConvexEnum)]
convex_nested.rs        -- #[derive(ConvexNested)]
convex_union.rs         -- #[derive(ConvexUnion)]
http_action.rs          -- #[convex::http_action(...)]
native_function.rs      -- #[convex::query/mutation/action(...)]
```

## Conventions

- **All paths in generated code go through `::convex_native::...`**
  or `::convex_native::__private::...` — never `::common::` or
  `::value::` directly. If you need a new type available in generated
  code, re-export it through `src/lib.rs`'s `__private` module.
- **Native handlers are monomorphic over `runtime::prod::ProdRuntime`**
  (aliased `Rt`). `inventory` can't hold generic fn pointers, so this
  is a hard constraint — don't try to make the registry generic over
  RT.
- **`convex_native_backend` is in-tree and wired.** The composite
  runner lives at `crates/convex_native_backend/` and is instantiated
  in `crates/local_backend/src/lib.rs` ahead of the `Application::new`
  call. Building it requires the `isolate` crate (needs `rush install`
  in `npm-packages/`); once those steps have run, `cargo build --bin
  convex-local-backend` succeeds. The reference notes in
  `convex-native/COMPOSITE_RUNNER.md` now describe the shipped
  behaviour, not a plan.
- **Every significant change gets a commit.** Prefer small, focused
  commits with a conventional-commits subject (`feat(convex_native):
  …`) and a body explaining *why*. Update
  `../../convex-native/README.md` in the same commit.

## Testing

`cargo test -p convex_native` runs the full suite. Each new feature
should come with at least one test — if it's a derive-macro change,
test it through `tests/derive_*.rs`; if it's a runtime feature,
through `tests/<feature>.rs`. The `tests/golden_path.rs` test
exercises the full developer surface and catches most regressions.

## Dev workflow

```sh
cargo check -p convex_native -p convex_macro
cargo test  -p convex_native
cargo +nightly fmt -p convex_native -p convex_macro
```

`rustfmt` is strict about line length inside `quote!` blocks in the
proc macros — keep generated code wrapped at ~100 cols or `rustfmt`
will fail with `error_on_line_overflow`. If you hit that, hand-wrap
the offending `quote! { ... }` block.

## Sibling crates

- `crates/convex_native_backend/` — in-process backend adapter
  (`CompositeFunctionRunner`, `BackendCallbacks`). Wired into
  `local_backend/src/lib.rs` ahead of `Application::new`. Depends on
  `isolate` / `function_runner` transitively, so it only builds after
  `rush install` in `npm-packages/`.
- `crates/convex_native_distributed/` — split-topology support
  (worker gRPC server, conductor P2C client, `TonicWorkerClient` real
  transport, `CONVEX_MODE` env helpers, runnable `examples/worker` +
  `examples/conductor` binaries). Depends only on `pb` + `tonic` + this
  crate; doesn't pull in `isolate`.

## What's actually shipped vs planned

`README.md` tracks this authoritatively. One-line summary: Phase
1/2/4/5 complete; Phase 3 shipped at the crate level (3.1–3.6)
via `convex_native_distributed`, and `convex-local-backend` now
accepts `CONVEX_MODE=standalone` (default) or
`CONVEX_MODE=worker` (adds a tonic `FunctionExecutionService`
beside the HTTP server, sharing the same Database, draining on
Ctrl-C together). Outstanding: `CONVEX_MODE=conductor` still
requires the dedicated `convex_native_distributed::examples::conductor`
binary — a conductor-only role doesn't fit a binary that always
boots a local `Database`.
