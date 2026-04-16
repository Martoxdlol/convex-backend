# Convex Native

Framework crates for writing Convex server functions (queries, mutations,
actions) in native Rust. See `native-rust-functions.md` for the design and
`IMPLEMENTATION_PLAN.md` for the phased roadmap.

## Current state

**Phase 1 — Layers 0, 1, 2, 3 and function attribute macros + native
runner (steps 1.0.1 → 1.3.3, 1.2.4 / 1.2.5, and 1.4.1): COMPLETE**
(composite runner + full backend wiring in Phases 1.4.2–1.5 remain)

### What works

- The `convex_native` crate compiles, and `cargo test -p convex_native`
  runs **15 tests** — 6 unit + 9 derive integration — all green.
- `#[derive(ConvexDocument)]` on a struct generates, for `Foo`:
  - `impl ConvexDocument for Foo` (table name, to_convex_object,
    from_convex_object, table_definition with indexes)
  - `pub enum FooField` — one variant per field, `impl FieldReference`
  - `pub enum FooIndex` — one variant per `#[convex(index(...))]`, `impl
    IndexReference` (uninhabited when none declared)
  - `pub struct FooPatch` — every field wrapped in `Option`, `Default`,
    `impl ConvexPatch`
  - `pub struct FooWithId { pub id: Id<Foo>, pub doc: Foo }` with `Deref`
  - `inventory::submit!` of a `TableRegistration` so `NativeSchema::collect()`
    picks it up automatically
- `NativeSchema::collect()` walks the linker-section registrations and
  returns a `DatabaseSchema` populated with every derived type from the
  binary.
- `Id<T: ConvexDocument>` is phantom-typed; `Id<User>` and `Id<Message>`
  are compile-time distinct.

### Consumer surface

Developers write:

```rust
use convex_native::prelude::*;

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub name: String,
    pub email: String,
    pub created_at: f64,
}
```

and get the generated companions for free. They depend on `convex_native`
only; `convex_macro` is re-exported.

### New in Phase 1.4.1 — `NativeFunctionRunner`

- `convex_native::NativeFunctionRunner` wraps an `Arc<NativeFunctionRegistry>`
  and exposes:
  - `from_inventory()` — build from static inventory entries
  - `has_function(name)` / `has_function_of_type(name, UdfType)` — name-based
    dispatch decisions
  - `run_query(name, tx, namespace, args)` / `run_mutation(...)` — execute
    a handler directly against a borrowed `Transaction<Rt>`
- `NativeFunctionRunner` is deliberately standalone and does **not** yet
  implement `function_runner::FunctionRunner`. The adapter that does
  (wrapping a JS runner and delegating unmapped calls) is documented in
  `COMPOSITE_RUNNER.md` — it's not built in-workspace because
  `function_runner` transitively depends on `isolate` (V8), which needs
  `rush install` + build steps to compile. When the full backend build
  lands in a new integration crate, the composite code moves there.

### New in Phase 1.2.4 / 1.2.5 — function attribute macros

- `#[convex::query]` and `#[convex::mutation]` (imported via
  `use convex_native::convex;`).
- Both accept async fns whose first parameter is `ctx: &mut QueryCtx`
  (or `MutationCtx`). Subsequent parameters must be `FromConvex`;
  return values must be `ToConvex + Send`.
- Derive macro now also emits `ToConvex` / `FromConvex` impls for the
  decorated struct, so developers don't need to implement them
  manually.
- Native function dispatch is pinned to
  `runtime::prod::ProdRuntime` (aliased as `convex_native::Rt`) — see
  `registry.rs` module docs for the rationale.
- `NativeFunctionRegistration` carries `name`, `arg_names`, and a
  `HandlerFn::{Query|Mutation}(fn_ptr)` — the runner (Phase 1.4) will
  consume these. `NativeFunctionRegistry::collect()` surfaces all
  registrations with O(1) lookup by name.

### New in Phase 1 Layer 3 (steps 1.3.1–1.3.3)

- `QueryCtx<'tx, RT>` / `QueryDb<'tx, RT>` wrap a
  `database::Transaction<RT>`. `db().get::<T>(Id<T>)` returns
  `Option<T>` parsed through the derived `from_convex_object`.
- `MutationCtx<'tx, RT>` / `MutationDb<'tx, RT>` wrap the same
  transaction and add `insert`, `patch`, `replace`, `delete` — all typed
  by `ConvexDocument` / `ConvexPatch`.
- `TypedQueryBuilder` type-checks against `T::Index` and `T::Field`.
  Terminal methods `.collect()` / `.first()` now actually execute via
  `database::DeveloperQuery`: index-range source when `.with_index()`
  was used, full-table-scan otherwise (filters without an index still
  rejected at runtime for now).

### What doesn't work yet

- **No end-to-end execution yet.** `NativeFunctionRunner` can dispatch
  handlers given a `Transaction<Rt>`, but building that transaction
  and rendering the result as `FunctionOutcome` /
  `FunctionFinalTransaction` is Phase 1.4.2+ TODO work; see
  `COMPOSITE_RUNNER.md` for the full todo list.
- **No backend wiring.** `make_app()` hasn't been updated. The
  `CompositeFunctionRunner` integration shape is documented but not
  in-tree; it belongs in a future `crates/convex_native_backend` crate
  that can depend on `function_runner`.
- **Non-indexed filters.** `.eq()` currently requires
  `.with_index(...)`. Full-table-scan + post-scan filtering is a later
  convenience, not MVP-critical.
- **No function macros.** `#[convex::query]`, `#[convex::mutation]`, and
  `#[convex::action]` are not implemented. Phase 1.2.4 + 1.2.5 + 2.2.
- **No context wrappers.** `QueryCtx`, `MutationCtx`, `ActionCtx` don't
  exist. Phase 1.3.
- **Placeholder handler signature.** `registry::HandlerFn = fn()` until
  the context wrappers pin down the real shape.
- **Document type validation is off.** `table_definition()` emits
  `document_type: None` — i.e. every derived type currently gets an "any"
  schema shape. Enforcing the shape against the struct's fields is
  Phase 1 extension work, not part of the MVP critical path.

## Architecture (today)

```
crates/convex_native/
├── src/
│   ├── lib.rs         -- re-exports, __private module for macro-generated code
│   ├── convert.rs     -- ToConvex / FromConvex
│   ├── id.rs          -- Id<T: ConvexDocument>
│   ├── document.rs    -- ConvexDocument / FieldReference / IndexReference / ConvexPatch
│   ├── schema.rs      -- TableRegistration + NativeSchema::collect()
│   ├── registry.rs    -- NativeFunctionRegistration + NativeFunctionRegistry
│   ├── prelude.rs     -- glob-import target
│   └── ctx/
│       ├── mod.rs
│       ├── query.rs         -- QueryCtx + QueryDb
│       ├── query_builder.rs -- TypedQueryBuilder (typed, unexecuted)
│       └── mutation.rs      -- MutationCtx + MutationDb
└── tests/
    ├── derive_document.rs   -- integration tests for the derive macro
    ├── derive_functions.rs  -- integration tests for the fn attribute macros
    ├── ctx_types.rs         -- compile-time surface tests for ctx wrappers
    └── runner_dispatch.rs   -- NativeFunctionRunner lookup & dispatch tests

crates/convex_macro/
├── src/
│   ├── lib.rs                -- #[proc_macro_derive(ConvexDocument)] entry point
│   ├── convex_document.rs    -- derive implementation
│   ├── (instrument_future, v8_op — pre-existing, unchanged)
```

`inventory::collect!` is the collection backbone for both the schema
(`TableRegistration`) and the function registry
(`NativeFunctionRegistration`). Developers never touch the registration APIs
directly — the derive/attribute macros emit the `submit!` calls.

### Why `::convex_native::__private::...` paths in generated code?

Proc-macro output can't assume what's in scope at the call site. To avoid
forcing users to `use common; use value;` in their own crates, we re-export
the handful of concrete types the generated code needs (`FieldName`,
`ConvexValue`, `ConvexObject`, `TableName`, `TableDefinition`,
`IndexSchema`, `IndexDescriptor`, `FieldPath`, `IndexedFields`) through a
hidden `convex_native::__private` module. The derive emits absolute paths
through that module. This keeps developers' `Cargo.toml` minimal (just
`convex_native`) and frees us to relocate internal types without breaking
downstream callers.

## Development

```sh
cargo check -p convex_native
cargo test -p convex_native
cargo +nightly fmt -p convex_native -p convex_macro
```

## Next up

Per `IMPLEMENTATION_PLAN.md`:

1. **Step 1.3.1** — `QueryCtx` + `QueryDb` (read-only database handle
   wrapping `Transaction<RT>`).
2. **Step 1.3.2** — `TypedQueryBuilder<T>` with typed field/index filters.
3. **Step 1.3.3** — `MutationCtx` + `MutationDb` (read + write).
4. **Step 1.2.4** — `#[convex::query]` proc macro.
5. **Step 1.2.5** — `#[convex::mutation]` proc macro.
6. **Step 1.4.x** — `NativeFunctionRunner` implementing the
   `FunctionRunner` trait.

Agents iterating on this project: please keep this document honest about
what is merged vs what is planned, after each commit.
