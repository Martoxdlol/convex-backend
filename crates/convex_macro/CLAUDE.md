# convex_macro — agent notes

Proc-macro crate powering the developer-facing attribute and
derive macros that `convex_native` re-exports. Read alongside
`../convex_native/CLAUDE.md` (where the re-exports live) and
`../../convex-native/QUICKSTART.md` (the user-visible surface
the macros are emitting for).

## Crate layout

```
src/
├── lib.rs              -- proc-macro entry points + shared helpers
                           (instrument_future, v8_op — pre-existing)
├── convex_document.rs  -- #[derive(ConvexDocument)]
├── convex_enum.rs      -- #[derive(ConvexEnum)]  (string enums)
├── convex_nested.rs    -- #[derive(ConvexNested)] (embedded objects)
├── convex_union.rs     -- #[derive(ConvexUnion)] (tagged unions)
├── cron.rs             -- #[convex::cron(...)]
├── http_action.rs      -- #[convex::http_action(...)]
└── native_function.rs  -- #[convex::query/mutation/action(...)]
```

## Conventions

- **Absolute paths through `::convex_native::__private`.** Emitted
  code MUST use `::convex_native::...` / `::convex_native::__private::...`
  — never `::common::` or `::value::` directly. If you need a new
  type, re-export it through the parent crate's `lib.rs`
  `__private` module first.
- **`rustfmt` strictness on `quote! { ... }` blocks.** Lines over
  ~100 cols inside `quote!` fail the `error_on_line_overflow`
  rustfmt rule. Hand-wrap the offending block rather than letting
  rustfmt "fix" it.
- **Validate at macro expansion, not at runtime.** Index field
  references, cron schedule syntax (via `saffron`), and struct
  shape are all checked at expansion so typos fail the build.
  Prefer `syn::Error::new_spanned(span, message)` over `panic!` so
  errors land on the offending token.
- **PascalCase ZST markers.** `#[convex::query] async fn get_user`
  emits `struct GetUser;` + `struct GetUserArgs { .. }`. The
  function can't share a name with a struct in Rust, so the marker
  is always PascalCase of the fn. Keep this convention consistent
  across query/mutation/action.
- **Inventory-based registration.** Every macro emits an
  `inventory::submit!` entry for its category
  (`TableRegistration`, `NativeFunctionRegistration`,
  `CronRegistration`, etc.). Don't add mutable collection
  alternatives — the whole runtime side depends on the linker
  walking inventory entries at startup.

## Dev workflow

```sh
cargo check -p convex_macro -p convex_native
cargo test -p convex_native      # derive tests run in the parent crate
cargo +nightly fmt -p convex_macro
```

Tests for the macros live in `crates/convex_native/tests/derive_*.rs`
and `tests/function_refs.rs` / `tests/derive_functions.rs` — this
crate has no standalone tests because proc-macros need a consumer
crate to be exercised meaningfully.

## What's shipped vs planned

All of Phase 1.2.1–1.2.5 + 1.6.1–1.6.3 + the attribute macros for
Phase 2.2 / 2.7 / cron are shipped. Known gap: no support for
generic types in `#[derive(ConvexDocument)]` (the macro errors
with a clear message). Components (`defineComponent` equivalent)
have no native analog.
