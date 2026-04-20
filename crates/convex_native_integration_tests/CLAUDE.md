# convex_native_integration_tests — agent notes

Breadth-over-depth integration tests that exercise every
deployer-facing feature of `convex_native` through both the
monolith (`STANDALONE.md`) and the distributed
(`DISTRIBUTED_PLAN.md`) topology.

Read alongside:
- `../../convex-native/USAGE.md` — full feature reference (this
  crate's fixture app should exercise every section).
- `../../convex-native/STATUS.md` — per-file coverage matrix.
- `../convex_native_core/CLAUDE.md` — framework crate the fixture
  app depends on.

## Crate layout

```
src/
├── lib.rs          -- re-exports db_fixture + fixture_app
├── db_fixture.rs   -- DbFixture::new_in_memory() (in-memory sqlite Database<ProdRuntime>)
└── fixture_app.rs  -- one #[convex::*] handler per feature; inventory-linked
tests/
├── standalone_*.rs     -- drive the fixture through NativeFunctionRunner + DbFixture
├── distributed_*.rs    -- drive the fixture through FunctionExecutionServer + tonic +
│                          DistributedFunctionRunner
├── derive_round_trips.rs -- wire-independent macro-symmetry tests
└── standalone_crons_and_introspection.rs -- wire-independent inventory coverage
```

## Conventions

- **Every integration-test file starts with
  `#[allow(dead_code)] type _ForceLink =
  convex_native_integration_tests::fixture_app::Todo;`**. Without
  an explicit reference the linker can drop the lib rlib's
  `inventory::submit!` sections and the runner sees an empty
  registry. The type alias costs nothing at runtime.
- **One feature per test file.** Keeps failure messages specific —
  the compilation cost of many tiny test binaries is worth the
  bisection ergonomics.
- **The fixture app is deployer-shaped**. It uses the public
  `convex_native` re-exports (not `convex_native_core` internals)
  — the same shape a real user writes. A feature that isn't usable
  from `convex_native::prelude::*` doesn't belong in the fixture.
- **`DbFixture::new_in_memory()` intentionally skips
  `publish_native_schema`.** Activating a pending schema with
  indexes declared needs the `SchemaWorker` + `IndexWorker` +
  `SearchAndVectorBootstrapWorker` trio. That's a full
  `Application::new` and it doubles test-build cost. Queries use
  post-scan filters (`.eq(field, ...)` without `.with_index(...)`);
  index declaration is still exercised via `NativeSchema::collect`
  and the admission envelope.
- **No V8.** Nothing in this crate should transitively pull in
  `isolate` / `function_runner` beyond what the fixture already
  needs — keeps the test-build time sane.

## Dev workflow

```sh
cargo check -p convex_native_integration_tests
cargo test  -p convex_native_integration_tests
cargo test  -p convex_native_integration_tests --test standalone_golden_path
cargo +nightly fmt -p convex_native_integration_tests
```

## Adding a new feature

1. Add a handler (or doc derive) to `src/fixture_app.rs`.
2. Add a standalone-topology assertion to the relevant
   `tests/standalone_*.rs`.
3. Add a distributed-topology mirror to the matching
   `tests/distributed_*.rs` **when the feature is observable over
   the wire** (runner-local knobs like drain / metrics / circuit
   breaker don't need mirrors — the distributed worker inherits
   them through its own `NativeFunctionRunner`).
4. Update `../../convex-native/STATUS.md`'s coverage matrix.
