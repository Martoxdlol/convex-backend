# convex_native_backend — agent notes

In-process adapter between `convex_native` and the backend's
V8-based `FunctionRunner`. Read alongside
`../../convex-native/COMPOSITE_RUNNER.md` (the behavioural
reference for what this crate does) and
`../../convex-native/README.md` (shipped-vs-planned at the
project level).

## Crate layout

```
src/
├── lib.rs               -- re-exports CompositeFunctionRunner + BackendCallbacks
├── composite_runner.rs  -- wraps a JS FunctionRunner<RT>, intercepts native names
└── callbacks_adapter.rs -- NativeActionCallbacks implemented on top of udf::ActionCallbacks
```

## Conventions

- **Runtime monomorphism via TypeId.** Native handlers are pinned
  to `convex_native::Rt` but the composite is generic over `RT`.
  Cross-cast via `std::any::TypeId::of::<RT>() == TypeId::of::<Rt>()`
  guards + `unsafe { &mut *((&mut tx) as *mut Transaction<RT> as *mut Transaction<Rt>) }`.
  Keep those unsafe blocks localized — the pattern lives in
  `composite_runner::dispatch_native` and
  `callbacks_adapter::try_run_native_{query,mutation}` only.
- **Builder-style wiring.** `CompositeFunctionRunner::new(native, js, db)`
  is the minimal construction; `.with_file_storage(fs)` adds
  raw-byte upload support. Same pattern on `BackendCallbacks`:
  `new(inner, identity, context)` for JS-only fallback,
  `with_native(...)` for native short-circuit, chainable
  `.with_file_storage(fs)` to enable `storage_store`, and
  `.with_snapshot_ts(ts)` to pin every native query sub-call in one
  action to the same read timestamp.
- **`isolate` build dependency.** `function_runner` pulls in
  `isolate` / V8, which needs `rush install` in `npm-packages/`
  before this crate builds. That's a one-time setup cost; the
  rest of the dev loop is normal cargo.
- **BackendCallbacks tests.** Pure helpers (`args_to_serialized`,
  `path_for`) are module-private free functions with unit tests.
  Anything that touches a real `Database<RT>` or
  `ActionCallbacks` is tested indirectly — through
  `convex_native_distributed` integration tests, or by linking
  the crate into `convex-local-backend` and exercising through
  an actual client.

## Dev workflow

```sh
cargo check -p convex_native_backend
cargo test -p convex_native_backend     # 10 tests last known
cargo +nightly fmt -p convex_native_backend

# Full backend build (validates the local_backend wire-up):
cargo build -p local_backend
```

## What's shipped vs planned

`convex-native/README.md` tracks this authoritatively. Short
version:

- `CompositeFunctionRunner` intercepts every
  `FunctionRunner::run_function` for native names:
  - Query/Mutation: opens a `Transaction<Rt>` via
    `Database::begin_with_ts`, threads `existing_writes` through
    `merge_writes`, dispatches through `NativeFunctionRunner::run_query`
    / `run_mutation`, extracts `FunctionFinalTransaction`, wraps
    in a synthetic `UdfOutcome`.
  - Action: routes to `NativeFunctionRunner::run_action_with_callbacks`
    with a `BackendCallbacks` built from the cached
    `Weak<dyn ActionCallbacks>`.
  - HttpAction + non-native requests: delegate to the wrapped JS
    runner unchanged.
- `BackendCallbacks::run_query_by_name` / `run_mutation_by_name`
  short-circuit to the native registry first (inline tx + commit
  for mutations), fall back to `udf::ActionCallbacks` for JS
  targets.
- `storage_store` uploads raw bytes through `FileStorage::store_file`
  when `.with_file_storage(fs)` is wired.

Known limitations live in `../../convex-native/COMPOSITE_RUNNER.md`
under "Known limitations" (cross-call path naming, log-line drain,
observed-* flags). Outstanding integration work at the project
level is tracked in `../../convex-native/README.md`'s "What
doesn't work yet".
