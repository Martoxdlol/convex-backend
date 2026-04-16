# Convex Native

Framework crates for writing Convex server functions (queries, mutations,
actions) in native Rust. See `native-rust-functions.md` for the design and
`IMPLEMENTATION_PLAN.md` for the phased roadmap.

## Current state

**Phase 1 — Layer 0 + Layer 1 (steps 1.0.1 → 1.1.3): COMPLETE**

The `convex_native` crate skeleton compiles and its unit tests pass. The
following building blocks are in place:

| Module | What it does |
| --- | --- |
| `convex_native::convert` | `ToConvex` / `FromConvex` traits for primitives, `Option`, `Vec`, `BTreeMap<String, _>`. |
| `convex_native::id` | `Id<T: ConvexDocument>` — phantom-typed wrapper over `DeveloperDocumentId`. |
| `convex_native::document` | `ConvexDocument`, `FieldReference`, `IndexReference`, `ConvexPatch` traits. |
| `convex_native::schema` | `TableRegistration` + `NativeSchema::collect()` using `inventory` linker sections. |
| `convex_native::registry` | `NativeFunctionRegistration` + `NativeFunctionRegistry::collect()`. Handler signature is a placeholder (`fn()`) until Phase 1.3 nails down the typed context wrappers. |
| `convex_native::prelude` | Glob-import target (`use convex_native::prelude::*;`). |

No proc macros, no runner, no backend wiring yet — those arrive in Layers
2–5 of Phase 1.

### What works today

- `cargo check -p convex_native` succeeds.
- `cargo test -p convex_native` runs 6 unit tests (conversion round-trips,
  empty schema collection, empty function registry).
- Workspace gained an `inventory = "0.3"` entry and a `convex_native` path
  dependency.

### What doesn't work yet

- There is no way to actually write a native function or document — the
  derive macros (`#[derive(ConvexDocument)]`, `#[convex::query]`,
  `#[convex::mutation]`, `#[convex::action]`) don't exist yet.
- The `HandlerFn` type in `registry.rs` is a placeholder; it will be
  replaced with a typed signature once `QueryCtx` / `MutationCtx` /
  `ActionCtx` exist.
- `NativeFunctionRunner` doesn't exist. Backend wiring comes in Phase 1.5.

## Architecture (today)

```
crates/convex_native/
├── src/
│   ├── lib.rs         -- module declarations + top-level re-exports
│   ├── convert.rs     -- ToConvex / FromConvex
│   ├── id.rs          -- Id<T: ConvexDocument>
│   ├── document.rs    -- ConvexDocument / FieldReference / IndexReference / ConvexPatch
│   ├── schema.rs      -- TableRegistration + NativeSchema::collect()
│   ├── registry.rs    -- NativeFunctionRegistration + NativeFunctionRegistry
│   └── prelude.rs     -- glob-import target
└── Cargo.toml
```

Everything hangs off `inventory::collect!` for the two collection points
(schema + function registry). Developers never call registration APIs
directly — the derive macros and attribute macros (to be added) will emit
`inventory::submit!` calls for them.

## Development

```sh
cargo check -p convex_native
cargo test -p convex_native
cargo +nightly fmt -p convex_native
```

## Next up

Per `IMPLEMENTATION_PLAN.md`:

- **Step 1.2.1** — `#[derive(ConvexDocument)]` basic struct + field enum +
  table registration (in the existing `convex_macro` crate).
- **Step 1.3.1** — `QueryCtx` + `QueryDb` (read-only database handle
  wrapping `Transaction<RT>`).

Agents iterating on this project: please keep this document honest about
what is merged vs what is planned, after each commit.
