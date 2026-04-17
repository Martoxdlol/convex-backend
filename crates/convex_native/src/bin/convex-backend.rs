//! Agnostic backend binary — the prebuilt image shape from
//! `convex-native/DISTRIBUTED_PLAN.md` Phase 5.
//!
//! Links `convex_native` (which transitively pulls `local_backend`
//! + `convex_native_core`) but carries zero `#[convex::*]`
//! registrations. When booted with `CONVEX_ADMISSION_BIND_ADDR`
//! this is the backend role in the distributed topology; without
//! it, it's the standalone HTTP+WS backend with an empty native
//! registry (every function call falls through to V8).
//!
//! For a worker role, point a deployer-specific binary (see
//! `convex-native/examples/standalone_todo_app`) at this binary's
//! admission port.
fn main() -> anyhow::Result<()> {
    convex_native::run()
}
