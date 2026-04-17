# Implementation Plan: Native Rust Functions for Convex

## Status (updated per commit)

The plan below is chronological; for the current shipped-vs-planned
breakdown, `README.md` is authoritative. One-line summary:

- **Phase 1 (schema, types, single-node queries/mutations):** complete.
  Crate surface in `crates/convex_native/`; proc macros in
  `crates/convex_macro/`; composite backend integration in
  `crates/convex_native_backend/`; wired into
  `crates/local_backend/src/lib.rs` ahead of `Application::new`.
- **Phase 2 (actions, scheduler, storage, HTTP actions):** complete.
  Raw byte uploads land through `FileStorage::store_file` via
  `BackendCallbacks::storage_store` when `.with_file_storage(fs)` is
  wired; native-to-native cross-calls short-circuit via a
  registry-aware resolver in `BackendCallbacks`.
- **Phase 3 (distributed gRPC service):** 3.1–3.6 shipped as a crate
  feature in `crates/convex_native_distributed/` (proto contract +
  conversions + worker server with `.with_database(db)` for
  queries/mutations + conductor P2C client + `TonicWorkerClient`
  real gRPC transport + `CONVEX_MODE` env helpers + multi-worker
  integration tests + runnable `examples/worker` / `examples/conductor`
  + subprocess smoke test). Not yet shipped: convex-local-backend
  binary-level switching on `CONVEX_MODE`.
- **Phase 4 (operational hardening):** 4.1–4.7 shipped (fastrace
  spans, per-function metrics sink, graceful drain, per-function
  timeouts, circuit breaker, index-cache warmup plan, rolling
  updates with version-aware routing via `min_registry_version`
  floor + per-call override).
- **Phase 5 (developer ergonomics extensions):** 5.1–5.4 shipped
  (schema migration diff, compile-time index-field validation,
  text/vector search indexes, bulk `get_many`).

Outstanding items relative to this plan: Phase 3.5 binary-level
`CONVEX_MODE` switch inside `convex-local-backend` itself is
**half-done**: the binary now *detects* `CONVEX_MODE` at startup,
logs the detected mode, and refuses to boot when it's not
`Standalone` — pointing the operator at the
`convex_native_distributed::examples/worker` + `examples/conductor`
binaries for split-topology deployments. A single `convex-local-backend`
binary that actually switches into Worker or Conductor mode is
still not wired; the two example binaries are the deployment path
for split topologies today. Phase 5 doesn't have a concrete
"fully complete" state — the plan lists four items, all shipped.

## Context

This project adds support for writing Convex server functions (queries, mutations, actions) in native Rust. The design doc (`native-rust-functions.md`) is complete. This plan breaks implementation into incremental, session-sized steps -- each producing a compilable, testable increment.

**Two new crates:** `convex_native` (core types, context wrappers, NativeFunctionRunner) and extensions to existing `convex_macro` (proc macros). A third crate `convex_native_distributed` comes in Phase 3.

**Key integration point:** The `FunctionRunner` trait at `crates/function_runner/src/lib.rs:84` is the core abstraction. `InProcessFunctionRunner` at `crates/function_runner/src/in_process_function_runner.rs:98` is the reference implementation. The new `NativeFunctionRunner` will implement this same trait, and be wired into `make_app()` at `crates/local_backend/src/lib.rs:214`.

---

## Phase 1: Schema, Type System, and Single-Node Queries/Mutations (MVP)

### Layer 0: Foundation

**Step 1.0.1 -- Crate skeleton and workspace wiring**

Create `crates/convex_native/` with minimal Cargo.toml and lib.rs. Add `inventory = "0.3"` to workspace dependencies (not currently used in this codebase).

- Create: `crates/convex_native/Cargo.toml`, `crates/convex_native/src/lib.rs`
- Modify: `Cargo.toml` (workspace root -- add `inventory` to `[workspace.dependencies]`)
- Note: workspace `members = ["crates/*"]` auto-discovers new crates
- Verify: `cargo check -p convex_native`

**Step 1.0.2 -- `ToConvex` / `FromConvex` conversion traits**

Define two-way conversion traits between Rust types and `ConvexValue`. Implement for: `String`, `i64`, `f64`, `bool`, `Vec<u8>`, `Vec<T>`, `Option<T>`, `BTreeMap<String, V>`.

- Create: `crates/convex_native/src/convert.rs`
- Integrates with: `ConvexValue` at `crates/value/src/lib.rs:122`, `ConvexObject` at `crates/value/src/object.rs:35`
- Verify: unit tests for round-trip serialization of all types
- Risk: `Option<T>` / `Null` semantics need care (`from_convex(None)` vs `from_convex(Some(Null))`)

**Step 1.0.3 -- `Id<T>` phantom-typed document ID**

Create `Id<T: ConvexDocument>` wrapping `DeveloperDocumentId`. Implement `ToConvex`/`FromConvex`, `Display`, `FromStr`, `Clone`, `Eq`, `Hash`.

- Create: `crates/convex_native/src/id.rs`
- Integrates with: `DeveloperDocumentId` at `crates/value/src/document_id.rs:40`
- Verify: `Id<User>` cannot be assigned to `Id<Message>` at compile time
- Note: `ConvexDocument` trait doesn't exist yet -- use forward declaration or placeholder

---

### Layer 1: Core Traits and Registry

**Step 1.1.1 -- `ConvexDocument`, `FieldReference`, `IndexReference`, `ConvexPatch` traits**

Define the core traits that derive macros will implement. Also `TableRegistration` for inventory and `NativeSchema::collect() -> DatabaseSchema`.

- Create: `crates/convex_native/src/document.rs`, `crates/convex_native/src/schema.rs`
- Integrates with: `DatabaseSchema` at `crates/common/src/schemas/mod.rs:144`, `TableDefinition` at line 447
- Verify: `NativeSchema::collect()` returns empty `DatabaseSchema` when no tables registered

**Step 1.1.2 -- `NativeFunctionRegistration` and `NativeFunctionRegistry`**

Function registry types using `inventory` for static collection. Stores name, `UdfType`, handler fn pointer, arg names.

- Create: `crates/convex_native/src/registry.rs`
- Integrates with: `UdfType` at `crates/common/src/types/functions.rs:33`
- Verify: manual registration + `collect()` + `get()` work in unit tests
- Risk: handler function signature needs careful design -- revisited when proc macro is built

**Step 1.1.3 -- `prelude` module**

Re-exports of all key types for `use convex_native::prelude::*`.

- Create: `crates/convex_native/src/prelude.rs`
- Verify: all key types resolve through prelude import

---

### Layer 2: Proc Macros

**Step 1.2.1 -- `#[derive(ConvexDocument)]` -- basic struct + field enum + table registration**

Parse `#[convex(table = "...")]`, generate: `impl ConvexDocument` (table_name, to_convex_object, from_convex_object), `XxxField` enum, `inventory::submit!`. NO indexes or patches yet.

- Modify: `crates/convex_macro/Cargo.toml` (add `inventory`), `crates/convex_macro/src/lib.rs`
- Create: `crates/convex_macro/src/convex_document.rs`
- Verify: `#[derive(ConvexDocument)] #[convex(table = "users")] struct User { name: String }` compiles, round-trips
- Risk: **HIGH COMPLEXITY** -- proc macros are the hardest part. Start with simple field types only.

**Step 1.2.2 -- Add index generation to `#[derive(ConvexDocument)]`**

Parse `#[convex(index(name = "by_email", fields = ["email"]))]`. Generate `XxxIndex` enum and `table_definition()` with indexes.

- Modify: `crates/convex_macro/src/convex_document.rs`
- Integrates with: `IndexSchema` at `crates/common/src/schemas/mod.rs:513`
- Verify: `UserIndex::ByEmail.as_str()` returns `"by_email"`, `table_definition()` includes indexes
- Risk: nested attribute parsing (`index(name="...", fields=["..."])`) is nontrivial in `syn`

**Step 1.2.3 -- Add `XxxPatch` and `XxxWithId` generation**

Generate companion types: `XxxPatch` (all Optional fields, Default), `XxxWithId` (id + doc with Deref).

- Modify: `crates/convex_macro/src/convex_document.rs`
- Verify: `UserPatch { name: Some("Bob".into()), ..Default::default() }.to_object()` works
- Risk: `Option<Option<T>>` for nullable fields adds complexity

**Step 1.2.4 -- `#[convex::query]` proc macro**

Parse function with `ctx: &mut QueryCtx` first param. Generate: renamed inner fn, `inventory::submit!` of `NativeFunctionRegistration`, arg deserialization.

- Modify: `crates/convex_macro/src/lib.rs`
- Create: `crates/convex_macro/src/native_function.rs`
- Verify: decorated function appears in `NativeFunctionRegistry::collect()`
- Risk: **HIGH COMPLEXITY** -- bridging type-erased registry with typed functions

**Step 1.2.5 -- `#[convex::mutation]` proc macro**

Same as query but validates `ctx: &mut MutationCtx`, registers as `UdfType::Mutation`.

- Modify: `crates/convex_macro/src/lib.rs`, `crates/convex_macro/src/native_function.rs`
- Verify: mutation function appears in registry with correct UdfType

---

### Layer 3: Context Wrappers

**Step 1.3.1 -- `QueryCtx` + `QueryDb` (read-only database handle)**

Wrap `Transaction<RT>` with typed read API: `get()`, `query()`, `count()`.

- Create: `crates/convex_native/src/ctx/mod.rs`, `crates/convex_native/src/ctx/query.rs`
- Integrates with: `Transaction<RT>` at `crates/database/src/transaction.rs:150`, `UserFacingModel` at `crates/database/src/bootstrap_model/user_facing.rs`
- Verify: compiles against Transaction API
- Note: `RT: Runtime` generic propagates everywhere

**Step 1.3.2 -- `TypedQueryBuilder<T>` with typed index/field filters**

Builder: `with_index(T::Index)`, `eq(T::Field, value)`, `order()`, `limit()`, `collect()`, `first()`, `page()`.

- Create: `crates/convex_native/src/ctx/query_builder.rs`
- Integrates with: `DeveloperQuery` in `crates/database/src/query/`
- Verify: `query::<User>().with_index(UserIndex::ByEmail)` compiles; `query::<User>().with_index(MessageIndex::X)` fails at compile time
- Risk: **MEDIUM** -- mapping typed builder to internal `DeveloperQuery` / `RangeRequest`

**Step 1.3.3 -- `MutationCtx` + `MutationDb` (read + write)**

Extends query context with: `insert()`, `patch()`, `replace()`, `delete()`.

- Create: `crates/convex_native/src/ctx/mutation.rs`
- Integrates with: `UserFacingModel::insert/patch/replace/delete`
- Verify: typed insert/patch/delete compiles

---

### Layer 4: NativeFunctionRunner

**Step 1.4.1 -- `NativeFunctionRunner` struct + `FunctionRunner` trait stub**

Create the runner struct modeled after `InProcessFunctionRunner`. Implement `FunctionRunner<RT>` with `todo!()` bodies that type-check.

- Create: `crates/convex_native/src/runner.rs`
- Integrates with: `FunctionRunner` trait at `crates/function_runner/src/lib.rs:84` (7 methods: `run_function`, `analyze`, `evaluate_app_definitions`, `evaluate_component_initializer`, `evaluate_schema`, `evaluate_auth_config`, `set_action_callbacks`)
- Verify: `NativeFunctionRunner` compiles and satisfies `FunctionRunner<RT>` bound

**Step 1.4.2 -- Implement `run_function` for queries**

Complete query execution path: create `Transaction` from DB snapshot at `ts`, build `QueryCtx`, lookup in registry, call handler, extract reads into `FunctionFinalTransaction`, build `UdfOutcome`.

- Modify: `crates/convex_native/src/runner.rs`
- Integrates with: `FunctionFinalTransaction::try_from(Transaction<RT>)` at `crates/function_runner/src/lib.rs:159`, `UdfOutcome`, `FunctionOutcome::Query`
- Verify: native query returns valid `FunctionOutcome` with correct value and read set

**Step 1.4.3 -- Implement `run_function` for mutations**

Same flow as queries but with writes enabled. Writes captured in Transaction, extracted into `FunctionWrites`.

- Modify: `crates/convex_native/src/runner.rs`
- Verify: native mutation returns `FunctionOutcome::Mutation` with write set containing inserted docs

**Step 1.4.4 -- Implement `evaluate_schema()` via `NativeSchema::collect()`**

Instead of evaluating JS, calls `NativeSchema::collect()` to gather registered tables.

- Modify: `crates/convex_native/src/runner.rs`
- Verify: returns `DatabaseSchema` with all registered table definitions and indexes

**Step 1.4.5 -- Implement `analyze()` for native functions**

Returns synthetic `AnalyzedModule` data from the function registry.

- Modify: `crates/convex_native/src/runner.rs`
- Integrates with: `AnalyzedModule` from `crates/model/src/modules/module_versions.rs`
- Risk: **MEDIUM** -- need to understand AnalyzedModule structure and produce valid synthetic versions

---

### Layer 5: Backend Integration

**Step 1.5.1 -- Wire `NativeFunctionRunner` into `make_app()`**

Create a `CompositeFunctionRunner` that tries native registry first, falls back to V8 runner. Modify `make_app()` at `crates/local_backend/src/lib.rs:214` to use composite.

- Create: `crates/convex_native/src/composite_runner.rs`
- Modify: `crates/local_backend/src/lib.rs` (line 214), `crates/local_backend/Cargo.toml`
- Risk: **MEDIUM** -- must implement ALL 7 `FunctionRunner` methods. Schema/analyze must merge native + V8 results.
- Verify: backend starts, JS functions still work, native functions callable via API

**Step 1.5.2 -- `ConvexBackend` builder API**

Developer-facing: `ConvexBackend::new().with_schema::<(User, Message)>().with_native_functions().run()`.

- Create: `crates/convex_native/src/backend.rs`
- Risk: may need refactoring of `local_backend` to make it embeddable
- Verify: minimal `main()` using builder compiles and starts backend

**Step 1.5.3 -- End-to-end integration test**

Test schema, register functions, start backend, call query + mutation through API, verify results.

- Create: `crates/convex_native/tests/integration_test.rs`
- Verify: query returns data, mutation inserts, subsequent query reads inserted doc

---

### Layer 6: Additional Derive Macros

**Step 1.6.1 -- `#[derive(ConvexEnum)]`** -- simple string enums (Admin -> "admin")

- Create: `crates/convex_macro/src/convex_enum.rs`

**Step 1.6.2 -- `#[derive(ConvexNested)]`** -- embedded objects (no table registration)

- Create: `crates/convex_macro/src/convex_nested.rs`

**Step 1.6.3 -- `#[derive(ConvexUnion)]`** -- tagged unions with `#[convex(tag = "type")]`

- Create: `crates/convex_macro/src/convex_union.rs`

---

### Phase 1 Dependency Graph

```
1.0.1 (crate skeleton)
  |
  +-- 1.0.2 (ToConvex/FromConvex)
  |     +-- 1.0.3 (Id<T>)
  |
  +-- 1.1.1 (ConvexDocument trait)
  |     +-- 1.1.2 (NativeFunctionRegistry)
  |     |     +-- 1.2.4 (#[convex::query])
  |     |     |     +-- 1.2.5 (#[convex::mutation])
  |     |     +-- 1.4.1 (NativeFunctionRunner stub)
  |     |           +-- 1.4.2 (run_function: queries)
  |     |           |     +-- 1.4.3 (run_function: mutations)
  |     |           +-- 1.4.4 (evaluate_schema)
  |     |           +-- 1.4.5 (analyze)
  |     +-- 1.2.1 (derive ConvexDocument basic)
  |     |     +-- 1.2.2 (+ indexes)
  |     |     +-- 1.2.3 (+ Patch, WithId)
  |     +-- 1.1.3 (prelude)
  |
  +-- 1.3.1 (QueryCtx + QueryDb)
  |     +-- 1.3.2 (TypedQueryBuilder)
  |     +-- 1.3.3 (MutationCtx + MutationDb)
  |
  +-- 1.5.1 (wire into local_backend) [needs 1.4.x + 1.3.x]
  |     +-- 1.5.2 (ConvexBackend builder)
  |     +-- 1.5.3 (integration test)
  |
  +-- 1.6.1-1.6.3 (ConvexEnum, ConvexNested, ConvexUnion) [needs 1.0.2]
```

**Parallelizable tracks in Phase 1:**
- Track A: 1.0.1 -> 1.0.2 -> 1.0.3 -> 1.6.1/1.6.2/1.6.3
- Track B: 1.0.1 -> 1.1.1 -> 1.2.1 -> 1.2.2 -> 1.2.3
- Track C: 1.0.1 -> 1.1.1 -> 1.1.2 -> 1.2.4 -> 1.2.5
- Track D: 1.0.1 -> 1.3.1 -> 1.3.2 -> 1.3.3
- Track E: 1.1.2 -> 1.4.1 -> 1.4.2 -> 1.4.3 + 1.4.4 + 1.4.5
- Track F: 1.4.x + 1.3.x -> 1.5.1 -> 1.5.2 -> 1.5.3

---

## Phase 2: Actions, Scheduling, and Typed Sub-calls

**Step 2.1** -- `ActionCtx` with raw sub-call support (`run_query_raw`, `run_mutation_raw`)
- Wraps `ActionCallbacks` trait at `crates/udf/src/action_callbacks.rs`
- Wire into `NativeFunctionRunner` for `UdfType::Action`

**Step 2.2** -- `#[convex::action]` proc macro (reuses native_function.rs)

**Step 2.3** -- Generated typed `XxxArgs` struct per function + `ConvexQueryFunction`/`ConvexMutationFunction` marker traits

**Step 2.4** -- Typed `run_query(fn_ref, TypedArgs) -> TypedResult` on `ActionCtx`

**Step 2.5** -- `Scheduler` API: `ctx.scheduler().run_after(delay, fn_ref, args)`
- Integrates with `VirtualSchedulerModel` from `crates/model/src/scheduled_jobs/`

**Step 2.6** -- `StorageCtx` for file storage in `ActionCtx`

**Step 2.7** -- `#[convex::http_action]` proc macro with method/path attributes

**Step 2.8** -- `ActionCallbacks` integration (weak ref pattern from `InProcessFunctionRunner`)

---

## Phase 3: Distributed Execution

**Step 3.1** -- Protobuf service definition in `crates/pb/` (`FunctionExecutionService`)
**Step 3.2** -- `crates/convex_native_distributed/` crate skeleton
**Step 3.3** -- `FunctionExecutionServer` (worker side gRPC, wraps NativeFunctionRunner)
**Step 3.4** -- `DistributedFunctionRunner` (conductor side, P2C load balancing, retry)
**Step 3.5** -- Binary mode switching: `CONVEX_MODE={standalone,conductor,worker}`
**Step 3.6** -- Distributed integration tests

---

## Phase 4: Production Hardening

**Step 4.1** -- Fastrace span propagation over gRPC
**Step 4.2** -- Per-function latency/error metrics
**Step 4.3** -- Graceful shutdown drain protocol
**Step 4.4** -- Per-function execution timeouts
**Step 4.5** -- Circuit breaker for unhealthy workers
**Step 4.6** -- Index cache warming on startup
**Step 4.7** -- Rolling update with version-aware routing

---

## Phase 5: Advanced Type Features (optional)

**Step 5.1** -- Schema migration tooling (compile-time diff)
**Step 5.2** -- Compile-time index field validation
**Step 5.3** -- Typed vector/text search index support
**Step 5.4** -- Relationship helpers (`get_related`)

---

## Complexity Hotspots

| Step | Risk | Why |
|------|------|-----|
| 1.2.1 | HIGH | Proc macros are hardest to debug. ConvexDocument derive is most complex single piece. |
| 1.2.4 | HIGH | Bridging type-erased registry with typed functions. Handler signature design is critical. |
| 1.4.2 | HIGH | Must faithfully replicate InProcessFunctionRunner's transaction lifecycle. Reference: `crates/function_runner/src/in_process_function_runner.rs:202`. |
| 1.5.1 | MEDIUM | CompositeRunner must implement all 7 FunctionRunner methods. Schema/analyze merging is novel. |
| 1.3.2 | MEDIUM | TypedQueryBuilder to DeveloperQuery translation requires understanding index interval logic. |

## Critical Files

| File | Role |
|------|------|
| `crates/function_runner/src/lib.rs` | `FunctionRunner` trait, `FunctionFinalTransaction`, `FunctionWrites`, `FunctionReads` |
| `crates/function_runner/src/in_process_function_runner.rs` | Reference implementation (template for NativeFunctionRunner) |
| `crates/database/src/bootstrap_model/user_facing.rs` | `UserFacingModel` -- DB ops that QueryDb/MutationDb wrap |
| `crates/convex_macro/src/lib.rs` | Existing proc macro crate to extend |
| `crates/local_backend/src/lib.rs:214` | `make_app()` where function runner is wired in |
| `crates/common/src/schemas/mod.rs` | `DatabaseSchema`, `TableDefinition`, `IndexSchema` |
| `crates/value/src/lib.rs` | `ConvexValue` enum, `ConvexObject`, `ConvexArray` |
| `crates/udf/src/action_callbacks.rs` | `ActionCallbacks` trait (Phase 2) |

## Dev Workflow Per Step

```sh
# After each change
just format-rust

# When a step is ready
just lint-rust
cargo build -p convex_native       # or convex_macro
cargo test -p convex_native        # run unit tests
cargo test -p convex_native "test_name"  # specific test
```
