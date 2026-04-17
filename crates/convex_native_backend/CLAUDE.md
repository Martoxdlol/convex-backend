# convex_native_backend — agent notes

In-process adapter between `convex_native` and the backend's
V8-based `FunctionRunner`. **This is the monolith-topology dispatch
path.** Under the distributed architecture in
`../../convex-native/DISTRIBUTED_PLAN.md`, the worker pool
implements `FunctionRunner` directly and this crate is not in the
hot path; it remains load-bearing for the "linked into
`local_backend` as a library" shape documented in
`../../convex-native/STANDALONE.md`.

Read alongside:
- `../../convex-native/COMPOSITE_RUNNER.md` — behavioural reference
  for what this crate does.
- `../../convex-native/STANDALONE.md` — the monolith topology this
  adapter supports.
- `../../convex-native/DISTRIBUTED_PLAN.md` — the target
  distributed architecture this crate is *not* part of.
- `../../convex-native/STATUS.md` — project-wide shipped-vs-planned.

## Crate layout

```
src/
├── lib.rs               -- re-exports CompositeFunctionRunner + BackendCallbacks
├── composite_runner.rs  -- wraps a JS FunctionRunner<RT>, intercepts native names
└── callbacks_adapter.rs -- NativeActionCallbacks implemented on top of udf::ActionCallbacks
```

## Conventions

- **Monolith topology only.** New distributed work goes into
  `convex_native_distributed`. If you're about to add a feature
  here that's already being built for the distributed path,
  confirm it against `DISTRIBUTED_PLAN.md` first — the monolith
  adapter shouldn't drift into a second implementation of the
  same logic.
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
  `.with_snapshot_ts(ts)` to pin every native query sub-call in
  one action to the same read timestamp.
- **`isolate` build dependency.** `function_runner` pulls in
  `isolate` / V8, which needs `rush install` in `npm-packages/`
  before this crate builds. One-time setup cost.
- **Tests.** Pure helpers (`args_to_serialized`, `path_for`) are
  module-private free functions with unit tests. Anything touching
  a real `Database<RT>` or `ActionCallbacks` is exercised through
  linking into `convex-local-backend` and hitting it with an
  actual client.

## Dev workflow

```sh
cargo check -p convex_native_backend
cargo test -p convex_native_backend     # 10 tests last known
cargo +nightly fmt -p convex_native_backend

# Full monolith build:
cargo build -p local_backend
```

## What's shipped

`CompositeFunctionRunner` intercepts every
`FunctionRunner::run_function` for native names:
- Query/Mutation: opens a `Transaction<Rt>` via
  `Database::begin_with_ts`, threads `existing_writes` through
  `merge_writes`, dispatches through
  `NativeFunctionRunner::run_query` / `run_mutation`, extracts
  `FunctionFinalTransaction`, wraps in a synthetic `UdfOutcome`.
- Action: routes to
  `NativeFunctionRunner::run_action_with_callbacks` with a
  `BackendCallbacks` built from the cached
  `Weak<dyn ActionCallbacks>` and pinned to
  `database.now_ts_for_reads()` via `.with_snapshot_ts(ts)` —
  every query sub-call inside the action sees one consistent
  read snapshot.
- HttpAction + non-native requests: delegate to the wrapped JS
  runner.

`BackendCallbacks::run_query_by_name` / `run_mutation_by_name`
short-circuit to the native registry first (inline tx + commit
for mutations), fall back to `udf::ActionCallbacks` for JS
targets.

`storage_store` uploads raw bytes through
`FileStorage::store_file` when `.with_file_storage(fs)` is wired.

## Known limitations

- **Native action sub-mutations commit separately.** A
  `#[convex::action]` that calls two mutations in a row will not
  see them as atomic — they land in distinct transactions.
  Actions don't have an enclosing transaction, so this matches
  Convex's JS semantics.
- **Per-action query snapshot is read-only.** Queries share a
  pinned `begin_ts`; mutation sub-calls commit at fresh ts, and
  their writes are not visible to subsequent query sub-calls in
  the same action (re-observe through a fresh action or query).
- **Log line timestamps** come from the runtime clock at drain
  time; for actions they stream through the
  `log_line_sender: mpsc::UnboundedSender<LogLine>` handed to the
  runner, matching the JS action path.
- **Observed flags.** `observed_identity`, `observed_time`,
  `observed_rng` are tracked per-invocation and flushed into the
  `UdfOutcome` after the handler returns.

See `../../convex-native/COMPOSITE_RUNNER.md` for behavioural
detail. Project-wide outstanding work (distributed-topology
replan) is tracked in `../../convex-native/STATUS.md`.
