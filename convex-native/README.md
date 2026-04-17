# Convex Native

Framework crates for writing Convex server functions (queries,
mutations, actions) in native Rust. Compile your handlers into the
backend binary; skip V8; keep the typed schema and the `inventory`
registry machinery doing the glue.

## Docs map

| File                      | Read when you want…                                          |
|---------------------------|---------------------------------------------------------------|
| `QUICKSTART.md`           | A 10-minute walkthrough that ends at a running app.           |
| `USAGE.md`                | The comprehensive per-topic feature reference.                |
| `DEPLOYMENT.md`           | How to run it: topologies, env vars, observability, rolling updates. |
| `STANDALONE.md`           | Build a standalone binary that depends on this repo as a library, without forking it. |
| `MIGRATION.md`            | Side-by-side JS ↔ Rust porting examples.                     |
| `STATUS.md`               | What's shipped vs. outstanding, with effort estimates.        |
| `COMPOSITE_RUNNER.md`     | How the `convex_native_backend` adapter plugs into the backend. |
| `examples/`               | Reading samples — `minimal_app/` shows a full deployer project shape. |
| `IMPLEMENTATION_PLAN.md`  | Historical phase-by-phase plan.                               |
| `native-rust-functions.md`| The original design doc (kept for rationale).                 |

If you're starting from scratch: `QUICKSTART.md` → `USAGE.md` → link
back here when you need operational details.

## At a glance

```rust
use convex_native::prelude::*;

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User { pub email: String, pub display_name: String }

#[convex::query]
pub async fn get_by_email(
    ctx: &mut QueryCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Option<User>> {
    ctx.db().query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, email)?
        .unique()
        .await
}
```

Link the crate into `convex-local-backend` (already wired) and the
query is callable through the normal HTTP / websocket client path —
no JS, no codegen, same `inventory::submit!` trick the rest of the
crate uses.

## Status snapshot

Phases 1–5 of `IMPLEMENTATION_PLAN.md` are shipped. `STATUS.md`
is authoritative. Test counts at HEAD:

```
cargo test -p convex_native              # 242 tests
cargo test -p convex_native_backend      # 10 tests
cargo test -p convex_native_distributed  # 57 tests
```

All green (309 total). Every item previously under "outstanding"
in `STATUS.md` has been closed — mutation-scoped scheduling,
document-shape validation, snapshot-pinned `ctx.db()` on
`ActionCtx`, and an end-to-end gRPC client smoke test all ship.
The residual gaps are external-toolchain or design-decision
items documented in `STATUS.md`'s "Non-obvious caveats" section.

## Architecture

```
crates/convex_native/              -- framework crate, no isolate dep
├── src/
│   ├── auth.rs                    -- AuthInfo (ctx.auth())
│   ├── backend.rs                 -- ConvexBackend builder + BuiltBackend
│   ├── callbacks.rs               -- NativeActionCallbacks trait + NoopCallbacks
│   ├── circuit_breaker.rs         -- CircuitBreaker + config
│   ├── convert.rs                 -- ToConvex / FromConvex
│   ├── cron.rs                    -- CronRegistration inventory + collect
│   ├── ctx/                       -- QueryCtx / MutationCtx / ActionCtx / ...
│   ├── distributed.rs             -- ConvexMode + ExecuteRequest/Response
│   ├── document.rs                -- ConvexDocument + FieldReference + IndexReference
│   ├── errors.rs                  -- bad_request / unauthenticated / ... helpers
│   ├── function_ref.rs            -- ConvexQueryFunction / Mutation / Action markers
│   ├── http.rs                    -- HttpActionCtx + HttpRequest/Response + HttpRouter
│   ├── id.rs                      -- Id<T: ConvexDocument>
│   ├── introspect.rs              -- describe_json / describe_pretty
│   ├── logging.rs                 -- LogBuffer + Logger
│   ├── metrics.rs                 -- NativeMetricsSink + CountingMetrics
│   ├── registry.rs                -- NativeFunctionRegistration + Registry
│   ├── runner.rs                  -- NativeFunctionRunner (dispatch, timeout, drain)
│   ├── schema.rs                  -- TableRegistration + NativeSchema::collect()
│   ├── schema_diff.rs             -- diff(old, new) -> Vec<SchemaChange>
│   ├── schema_type.rs             -- ConvexSchema trait — primitives / containers / Id<T>
│   ├── testing.rs                 -- TestCallbacks + args! macro
│   └── warmup.rs                  -- plan_warmup(schema)
└── tests/                         -- integration tests; see CLAUDE.md for the per-file map

crates/convex_native_backend/      -- backend adapter, pulls in isolate
├── composite_runner.rs            -- CompositeFunctionRunner<RT>: FunctionRunner impl
└── callbacks_adapter.rs           -- BackendCallbacks: NativeActionCallbacks -> ActionCallbacks

crates/convex_native_distributed/  -- split-topology gRPC (worker + conductor)
├── server.rs                      -- FunctionExecutionServer (worker-side tonic impl)
├── client.rs                      -- DistributedFunctionRunner + ConductorMetricsSink + ConductorLogSink
├── tonic_client.rs                -- TonicWorkerClient (real gRPC transport)
├── worker_callbacks.rs            -- WorkerActionCallbacks: native-only sub-call/schedule adapter
├── conversions.rs                 -- proto <-> native shape (carries ExecutionContext + log_lines)
├── mode.rs                        -- CONVEX_MODE env parsers
└── examples/                      -- runnable worker + conductor binaries

crates/convex_macro/               -- proc macros
├── convex_document.rs             -- #[derive(ConvexDocument)]
├── convex_enum.rs                 -- #[derive(ConvexEnum)]
├── convex_nested.rs               -- #[derive(ConvexNested)]
├── convex_union.rs                -- #[derive(ConvexUnion)]
├── cron.rs                        -- #[convex::cron(...)]
├── http_action.rs                 -- #[convex::http_action(...)]
└── native_function.rs             -- #[convex::query/mutation/action]
```

`inventory::collect!` is the collection backbone for both the schema
(`TableRegistration`) and the function registry
(`NativeFunctionRegistration`). Developers never touch the
registration APIs directly — the derive / attribute macros emit the
`submit!` calls. Generated code goes through
`::convex_native::__private::...` absolute paths so callers only
need `convex_native` in their `Cargo.toml`.

`CompositeFunctionRunner` is instantiated inside
`crates/local_backend/src/lib.rs` ahead of the `Application::new`
call, so every `convex-local-backend` build transparently picks up
statically-registered native functions. `CONVEX_MODE=worker`
additionally spawns a tonic `FunctionExecutionService` that shares
the same `Database<Rt>` and drains together with HTTP on Ctrl-C.
`CONVEX_MODE=conductor` stays behind the dedicated
`convex_native_distributed::examples::conductor` binary.

## Runnable example

```sh
cargo run -p convex_native --example tiny_app
```

Prints `BuiltBackend::describe_pretty()` for a handful of derived
types and functions — a quick sanity check of the full surface
(schema, nested / enum / union, query, mutation, action, internal
mutation, cron, HTTP action, builder, validation, introspection).

## Development

```sh
cargo check -p convex_native -p convex_macro
cargo test  -p convex_native
cargo +nightly fmt -p convex_native -p convex_macro
```

## For agents iterating here

Keep the three docs honest, in this order of priority:

1. `STATUS.md` — any gap closed / opened must land here first.
2. `USAGE.md` — add new features to the right topic section, not a
   dated "New in Phase X" blurb.
3. This README — the landing page stays short. Resist adding
   feature writeups; link to `USAGE.md` instead.

When docs drift from code, code wins — fix the doc in the same
commit, not later.
