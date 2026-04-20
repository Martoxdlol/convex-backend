# convex_native — agent notes

The framework crate: derives, ctx surface, registry, runner,
introspection. **Load-bearing for every phase of
`../../convex-native/DISTRIBUTED_PLAN.md`** — this crate is what
the worker process runs inside. The distributed replan does not
touch this crate's public API.

Read alongside:
- `../../convex-native/DISTRIBUTED_PLAN.md` — target architecture.
- `../../convex-native/STATUS.md` — what survived, what was
  removed, active phase.
- `../../convex-native/USAGE.md` — comprehensive feature reference.
- `../../convex-native/README.md` — landing page.

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
├── distributed.rs      -- ConvexMode + ExecuteRequest/Response + FunctionExecutor trait
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
- **This crate stays isolate-free.** The composite runner and the
  distributed scaffolding live in sibling crates
  (`convex_native_backend`, `convex_native_distributed`). Anything
  that would pull `isolate` / `function_runner` into this crate's
  dependencies is in the wrong place.
- **Introspection is the worker's registration payload.**
  `describe_json` / `describe_pretty` are what Phase 3 of
  `DISTRIBUTED_PLAN.md` carries in the `WorkerAdmissionService`
  registration envelope. Keep them stable.
- **Every significant change gets a commit.** Small focused commits
  with a conventional-commits subject (`feat(convex_native): …`)
  and a body explaining *why*. Doc updates land in the same
  commit — in priority order: `STATUS.md` (plan delta) →
  `USAGE.md` (new feature surface) → `README.md` (only if the
  architecture diagram changes).

## Testing

`cargo test -p convex_native` runs the full suite (242 tests at
last known count). Each new feature should come with at least one
test — derive-macro changes go through `tests/derive_*.rs`; runtime
features through `tests/<feature>.rs`. `tests/golden_path.rs`
exercises the full developer surface and catches most regressions.

## Dev workflow

```sh
cargo check -p convex_native -p convex_macro
cargo test  -p convex_native
cargo +nightly fmt -p convex_native -p convex_macro
```

`rustfmt` is strict about line length inside `quote!` blocks in
the proc macros — keep generated code wrapped at ~100 cols or
`rustfmt` fails with `error_on_line_overflow`. Hand-wrap the
offending `quote! { ... }` block if you hit that.

## Sibling crates

- `crates/convex_native_backend/` — in-process adapter
  (`CompositeFunctionRunner`, `BackendCallbacks`) wired into
  `local_backend/src/lib.rs` for the **monolith** topology
  (`STANDALONE.md` + `COMPOSITE_RUNNER.md`). Depends on `isolate`
  / `function_runner` transitively.
- `crates/convex_native_distributed/` — gRPC transport
  (worker server, `DistributedFunctionRunner` P2C client,
  `TonicWorkerClient`). Currently pre-Phase-1; `DISTRIBUTED_PLAN.md`
  drives it to a correct distributed shape.

## What's shipped vs planned

`../../convex-native/STATUS.md` is authoritative. One-line summary:
framework-level pieces (derives, ctx surface, schema reflection,
registry, introspection) are solid and reused. The distributed
dispatch layer is being rebuilt; Phase 1 is the `ExecuteResponse`
proto change and the commit-moves-to-backend flip.
