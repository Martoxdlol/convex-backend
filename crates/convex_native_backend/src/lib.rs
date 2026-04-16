//! Integration glue between `convex_native` and the Convex backend's
//! V8-based function runner.
//!
//! `CompositeFunctionRunner<RT>` implements
//! `function_runner::FunctionRunner<RT>` by wrapping an existing JS runner
//! (typically `InProcessFunctionRunner`) and intercepting calls whose function
//! names are in the native registry. Native names that aren't fully
//! wired through yet surface a clear error; every other call falls
//! through to the JS side unchanged.
//!
//! This crate depends on `function_runner` (and transitively on
//! `isolate`), so it can only build in environments with the
//! `npm-packages/` rush install + build step in place. `convex_native`
//! itself stays lightweight and has no isolate dep.

mod callbacks_adapter;
mod composite_runner;

pub use callbacks_adapter::BackendCallbacks;
pub use composite_runner::CompositeFunctionRunner;
