# Convex Native

Framework crates for writing Convex server functions (queries, mutations,
actions) in native Rust. See `native-rust-functions.md` for the design and
`IMPLEMENTATION_PLAN.md` for the phased roadmap.

## Current state

**Phase 1 — Layer 0, Layer 1, Layer 2 (steps 1.0.1 → 1.2.3): COMPLETE**

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

### What doesn't work yet

- **No runner.** There is no `NativeFunctionRunner` — functions can't be
  called end-to-end. That lands in Phase 1.4.
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
│   └── prelude.rs     -- glob-import target
└── tests/
    └── derive_document.rs  -- integration tests for the derive macro

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
