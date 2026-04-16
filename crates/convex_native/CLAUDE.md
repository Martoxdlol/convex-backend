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
├── ctx/
│   ├── action.rs       -- ActionCtx
│   ├── mutation.rs     -- MutationCtx + MutationDb
│   ├── query.rs        -- QueryCtx + QueryDb
│   ├── query_builder.rs-- TypedQueryBuilder
│   ├── scheduler.rs    -- Scheduler
│   └── storage.rs      -- StorageCtx + StorageId
├── distributed.rs      -- ConvexMode + ExecuteRequest/Response trait stubs
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

## What's actually shipped vs planned

`README.md` tracks this accurately. The crate exposes the complete
Phase 1/2/5 developer surface plus most of Phase 3 scaffolding and
Phase 4 operational knobs. The backend adapter crate
(`crates/convex_native_backend/`) is now in-tree and wired into
`local_backend::make_app()`. The remaining gaps are: an end-to-end
smoke test driven from a real client, the distributed gRPC service
(Phase 3.1–3.6), and rolling-update routing (Phase 4.7).
