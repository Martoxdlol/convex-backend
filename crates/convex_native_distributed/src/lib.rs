//! Distributed execution for `convex_native`.
//!
//! **NOTE — topology change in progress.** Phase 1 of the rework
//! in `convex-native/DISTRIBUTED_PLAN.md` has landed: the worker no
//! longer commits locally, and `ExecuteResponse` carries a
//! `DistributedFinalTx` the backend's Committer will consume in
//! Phase 2. Until Phase 2 lands, the backend still routes native
//! functions through the in-process / composite runner in
//! `local_backend`, so the distributed topology isn't on the main
//! request path yet — but the wire contract it needs is ready.
//!
//! ## What's in here today (post Phase-1)
//!
//! - [`conversions`] — proto ↔ native shape boundary, including the
//!   `FinalTxSummary` ↔ `DistributedFinalTx` pair landed in Phase 1.
//! - [`server::FunctionExecutionServer`] — worker-side tonic service. Phase 1
//!   removed the inline commit; query/mutation handlers now return a
//!   `FinalTxSummary` on the response, and the backend's Committer (Phase 2)
//!   does the durable commit.
//! - [`client::DistributedFunctionRunner`] — worker pool client. Will gain
//!   `impl FunctionRunner<RT>` in Phase 2 so the backend can plug it in where
//!   `InProcessFunctionRunner` sits today.
//! - [`tonic_client::TonicWorkerClient`] — real gRPC transport.
//! - [`mode`] — env-var parsers + server-building helpers. The
//!   `CONVEX_MODE=conductor` path is being removed in Phase 3 (the backend
//!   image replaces the standalone-conductor concept); the `worker` path stays.
//!
//! ## What was removed
//!
//! - `WorkerActionCallbacks` — committed sub-calls on the worker's local
//!   database. Wrong semantics under the new plan; callbacks will route back to
//!   the backend's Committer (Phase 4).
//! - `examples/{conductor,conductor_dispatch,worker_with_functions}.rs` — all
//!   relied on the pure-dispatcher / worker-commits shape.
//! - `tests/{examples_smoke,client_e2e_smoke}.rs` — tested the removed
//!   behaviour end-to-end.

pub mod admission;
pub mod client;
pub mod conversions;
pub mod function_runner_impl;
pub mod mode;
pub mod pool;
pub mod server;
pub mod tonic_client;

pub use client::{
    CapturingConductorLogs,
    ConductorLogSink,
    ConductorMetricsSink,
    ConductorOutcome,
    CountingConductorMetrics,
    DistributedFunctionRunner,
    NoopConductorLogs,
    NoopConductorMetrics,
    WorkerClient,
};
pub use mode::{
    build_conductor_runner,
    build_worker_server,
    read_mode_from_env,
    read_native_workers_from_env,
    read_worker_bind_addr_from_env,
    read_worker_endpoints_from_env,
    serve_worker_with_database,
    serve_worker_with_shutdown,
};
pub use server::FunctionExecutionServer;
pub use tonic_client::TonicWorkerClient;
