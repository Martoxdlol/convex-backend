//! Distributed execution for `convex_native`.
//!
//! Per `convex-native/IMPLEMENTATION_PLAN.md` Phase 3: this crate
//! implements the worker-side gRPC server and conductor-side
//! client generated from
//! `crates/pb/protos/function_execution.proto`.
//!
//! ## Module layout
//!
//! - [`conversions`] — the wire boundary: maps `pb::function_execution::*` ↔
//!   `convex_native::distributed::*` (namespace, args, request, response,
//!   duration, UdfType).
//! - [`server`] — [`FunctionExecutionServer`] implements the tonic service
//!   trait. Dispatches actions via
//!   `NativeFunctionRunner::run_action_with_callbacks` and, when
//!   `.with_database(db)` is wired, queries and mutations inline against a
//!   `Transaction<Rt>` (queries drop the tx; mutations commit via
//!   `commit_with_write_source`).
//! - [`client`] — [`DistributedFunctionRunner`] dispatches over a pool of
//!   workers using Power-of-2-Choices with single-retry failover. Tests use
//!   `MockWorkerClient`.
//! - [`tonic_client`] — [`TonicWorkerClient`] is the real gRPC implementation
//!   of `WorkerClient`.
//! - [`mode`] — env-var parsers (`CONVEX_MODE`, `CONVEX_WORKER_ENDPOINTS`,
//!   `CONVEX_WORKER_BIND_ADDR`) and builder helpers ([`build_worker_server`],
//!   [`build_conductor_runner`]) for Phase 3.5 binary-level wiring.
//! - `examples/worker.rs` + `examples/conductor.rs` are runnable binaries a
//!   deployer can crib from; `tests/examples_smoke.rs` spawns both and asserts
//!   they talk over real gRPC.

pub mod client;
pub mod conversions;
pub mod mode;
pub mod server;
pub mod tonic_client;

pub use client::{
    DistributedFunctionRunner,
    WorkerClient,
};
pub use mode::{
    build_conductor_runner,
    build_worker_server,
    read_mode_from_env,
    read_worker_bind_addr_from_env,
    read_worker_endpoints_from_env,
    serve_worker_with_database,
};
pub use server::FunctionExecutionServer;
pub use tonic_client::TonicWorkerClient;
