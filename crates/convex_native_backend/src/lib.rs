//! Integration glue between `convex_native` and the Convex backend's
//! V8-based function runner.
//!
//! [`CompositeFunctionRunner`] implements
//! `function_runner::FunctionRunner<RT>` by wrapping an existing JS
//! runner (typically `InProcessFunctionRunner`) and intercepting
//! calls whose function names are in the native registry:
//!
//! - Queries and mutations dispatch inline against a `Transaction<Rt>` opened
//!   on the owned `Database<RT>`. `existing_writes` is threaded through
//!   `tx.merge_writes` to preserve JS-style multi-UDF batching.
//! - Actions dispatch through
//!   `NativeFunctionRunner::run_action_with_callbacks`, with a
//!   [`BackendCallbacks`] wrapping the JS-side `Arc<dyn ActionCallbacks>`
//!   (cached via `set_action_callbacks` as a `Weak` to avoid a reference
//!   cycle).
//! - HTTP actions and non-native requests delegate to the wrapped JS runner
//!   unchanged.
//! - `evaluate_schema` merges `NativeSchema::collect()` with the JS-side
//!   schema; a table declared in both is a hard error.
//!
//! The adapter is wired into `convex-local-backend` in
//! `crates/local_backend/src/lib.rs`, ahead of `Application::new`,
//! so any `#[convex::query/mutation/action]` linked into the
//! binary at build time is served natively with no per-call
//! configuration.
//!
//! This crate depends on `function_runner` (and transitively on
//! `isolate`), so it only builds after the `npm-packages/`
//! `rush install` + build step has run. `convex_native` itself
//! stays lightweight and has no `isolate` dep — the split exists
//! specifically so framework-only consumers can avoid the V8
//! build cost.

mod callbacks_adapter;
mod composite_runner;

pub use callbacks_adapter::BackendCallbacks;
pub use composite_runner::CompositeFunctionRunner;
