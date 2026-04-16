# Convex Native

Framework crates for writing Convex server functions (queries, mutations,
actions) in native Rust. See `native-rust-functions.md` for the design and
`IMPLEMENTATION_PLAN.md` for the phased roadmap.

## Current state

**Phase 1 COMPLETE** (steps 1.0.1 → 1.3.3, 1.2.4 / 1.2.5, 1.4.1,
1.6.1–1.6.3) **plus Phase 2 2.1–2.5 COMPLETE (scheduler surface
defined — backend wiring pending)**. Remaining: query/mutation sub-call
execution (2.4 raw backend integration), scheduler backend (2.5
VirtualSchedulerModel), storage (2.6), HTTP actions (2.7), and the
composite runner end of Phase 1.5.

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

### New in Phase 2.5 — Scheduler (surface)

`MutationCtx::scheduler()` and `ActionCtx::scheduler()` now return a
`Scheduler<'_>` with:

- `run_after<F: ConvexMutationFunction>(delay, marker, args)` — schedule
  a typed mutation.
- `run_action_after<F: ConvexActionFunction>(delay, marker, args)` —
  schedule a typed action.

Both serialize the typed args correctly, then `bail!` at the actual
scheduling step pending `VirtualSchedulerModel` backend integration.
Developers can already write scheduling code against the final API
shape.

### New in Phase 2.3 / 2.4 — Function markers + typed sub-calls

Every `#[convex::query]` / `#[convex::mutation]` / `#[convex::action]`
now also emits:

- `XxxArgs` struct (PascalCase + `Args`): one field per non-`ctx`
  parameter with `ToConvex` / `FromConvex` impls.
- `Xxx` ZST marker (PascalCase of the fn name). Implements the matching
  marker trait — `ConvexQueryFunction` / `ConvexMutationFunction` /
  `ConvexActionFunction` — carrying `type Args`, `type Output`, and
  `fn name()`.

That lets `ActionCtx` offer typed sub-calls:

```rust
let result: Option<User> = ctx.run_query(GetUser, GetUserArgs {
    email: "a@b".into(),
}).await?;

let id: Id<User> = ctx.run_mutation(CreateUser, CreateUserArgs { .. }).await?;

let sent: bool = ctx.run_action(SendEmail, SendEmailArgs { .. }).await?;
```

Today `run_query` / `run_mutation` still `bail!` through the raw
helpers pending backend integration. `run_action` runs end-to-end
because actions don't require a new transaction.

> **Note on API:** the design doc's example uses the function name
> (`ctx.run_query(get_user, …)`) but Rust forbids a `fn` and a `struct`
> with the same name in one scope, so the ZST marker is PascalCase
> (`GetUser`). The function itself remains callable as `get_user(...)`.

### New in Phase 2 foundations — Actions

- `#[convex::action]` attribute macro. Developers write:

  ```rust
  #[convex::action]
  async fn send_email(ctx: &mut ActionCtx, user_id: Id<User>) -> Result<()> { .. }
  ```

  The macro follows the same shape as `#[convex::query]` /
  `#[convex::mutation]` and registers the function under
  `UdfType::Action`.
- `convex_native::ActionCtx<'a, RT>` — context for actions. Unlike
  `QueryCtx` / `MutationCtx`, it does NOT hold a transaction; instead
  it carries an optional `Arc<NativeFunctionRunner>` for sub-calls and
  a namespace.
- `NativeFunctionRunner::run_action(name, namespace, args)` dispatches
  an action end-to-end — **actions with no external I/O or sub-calls
  actually execute today** (see the `action_dispatch_returns_handler_result`
  test). Typed / raw sub-calls (`ctx.run_query_raw` etc.) are still
  stubbed `bail!` pending backend integration (Step 2.4+).

### New in Phase 1.6 — additional derive macros

- `#[derive(ConvexEnum)]` — string-valued unit-only enums. Each variant
  becomes its snake_case name on the wire
  (`Admin` ↔ `"admin"`). `#[convex(rename = "custom")]` overrides per
  variant.
- `#[derive(ConvexNested)]` — structs that round-trip through a
  `ConvexObject` without registering a table. Use for embedded shapes
  like `Address` inside `User`.
- `#[derive(ConvexUnion)]` — tagged unions where each variant is a
  struct-variant with named fields. Configurable discriminant field
  name (`#[convex(tag = "...")]`, default `"type"`) and per-variant
  `rename`. Duplicate tags are compile errors.

All three emit only `ToConvex` / `FromConvex` impls — no inventory
registrations, no schema entries.

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
│   ├── runner.rs            -- NativeFunctionRunner (dispatch)
│   └── ctx/
│       ├── mod.rs
│       ├── query.rs         -- QueryCtx + QueryDb
│       ├── query_builder.rs -- TypedQueryBuilder (typed, executable)
│       ├── mutation.rs      -- MutationCtx + MutationDb
│       └── action.rs        -- ActionCtx (Phase 2 skeleton + dispatch)
└── tests/
    ├── derive_document.rs                  -- integration tests for ConvexDocument
    ├── derive_enums_nested_unions.rs       -- ConvexEnum / ConvexNested / ConvexUnion
    ├── derive_functions.rs                 -- integration tests for fn attribute macros
    ├── ctx_types.rs                        -- compile-time surface tests for ctx wrappers
    └── runner_dispatch.rs                  -- NativeFunctionRunner lookup & dispatch

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
