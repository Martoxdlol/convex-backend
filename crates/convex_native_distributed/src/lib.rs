//! Distributed execution for `convex_native`.
//!
//! **NOTE — topology change in progress.** The crate as currently
//! shipped implements a "worker commits locally, no backend
//! coordination" shape that is being replaced. See
//! `convex-native/DISTRIBUTED_PLAN.md` for the target architecture.
//! Work is tracked through Phase 1..Phase 7 in that document.
//! Until Phase 1 lands, this crate's mutation path does **not**
//! preserve OCC / subscriptions when used standalone.
//!
//! ## What's in here today (pre-Phase-1)
//!
//! - [`conversions`] — proto ↔ native shape boundary.
//! - [`server::FunctionExecutionServer`] — worker-side tonic
//!   service. Currently commits locally on mutations (will change
//!   in Phase 1 to return `FunctionFinalTransaction` instead).
//! - [`client::DistributedFunctionRunner`] — worker pool client.
//!   Will gain `impl FunctionRunner<RT>` in Phase 2 so the backend
//!   can plug it in where `InProcessFunctionRunner` sits today.
//! - [`tonic_client::TonicWorkerClient`] — real gRPC transport.
//! - [`mode`] — env-var parsers + server-building helpers. The
//!   `CONVEX_MODE=conductor` path is being removed in Phase 3 (the
//!   backend image replaces the standalone-conductor concept); the
//!   `worker` path stays.
//!
//! ## What was removed
//!
//! - `WorkerActionCallbacks` — committed sub-calls on the worker's
//!   local database. Wrong semantics under the new plan; callbacks
//!   will route back to the backend's Committer (Phase 4).
//! - `examples/{conductor,conductor_dispatch,worker_with_functions}.rs` —
//!   all relied on the pure-dispatcher / worker-commits shape.
//! - `tests/{examples_smoke,client_e2e_smoke}.rs` — tested the
//!   removed behaviour end-to-end.

pub mod client;
pub mod conversions;
pub mod mode;
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
    read_worker_bind_addr_from_env,
    read_worker_endpoints_from_env,
    serve_worker_with_database,
    serve_worker_with_shutdown,
};
pub use server::FunctionExecutionServer;
pub use tonic_client::TonicWorkerClient;
