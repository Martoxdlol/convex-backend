//! Distributed execution protocol — tonic-free Rust shapes.
//!
//! Per `IMPLEMENTATION_PLAN.md` Phase 3.
//!
//! The full design in `native-rust-functions.md` §10 calls for a gRPC
//! protocol where a **conductor** dispatches function calls to a pool
//! of identical **worker** binaries, each running the native registry.
//!
//! This module keeps the runner-facing shape visible in one place:
//! [`ConvexMode`] (operating-mode enum parsed from `CONVEX_MODE`),
//! [`ExecuteRequest`] / [`ExecuteResponse`] (the request/response
//! payload shapes), and [`FunctionExecutor`] (the trait a worker
//! implements). The matching protobuf contract lives at
//! `crates/pb/protos/function_execution.proto` and generates
//! `pb::function_execution::{ExecuteRequest, ExecuteResponse,
//! HealthRequest, HealthResponse, FunctionExecutionService}` via
//! `tonic_build`.
//!
//! The tonic-side server/client implementations, conversions
//! between the two shapes, and the CONVEX_MODE env helpers live in
//! `crates/convex_native_distributed/`. Consumers that only need
//! the tonic-free types (e.g. tests, in-process executors, or the
//! composite runner's fallback path) can stay on the types defined
//! here without pulling the `tonic` + generated-code surface into
//! their build.

use std::time::Duration;

use value::{
    ConvexObject,
    ConvexValue,
    TableNamespace,
};

/// Operating mode for a process that embeds the convex-native runner.
///
/// Corresponds to the `CONVEX_MODE` env var described in §10.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConvexMode {
    /// All-in-one: conductor, worker, and database in one process.
    /// This is what the existing local_backend binary does today.
    Standalone,
    /// Conductor only: owns the database, dispatches to remote workers.
    Conductor,
    /// Worker only: executes native functions when called by a conductor.
    Worker,
}

impl ConvexMode {
    /// Parse from env-var string (case-insensitive). Defaults to
    /// `Standalone` on empty/unknown values — callers can enforce a
    /// stricter policy upstream if they want.
    pub fn from_env_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "conductor" => Self::Conductor,
            "worker" => Self::Worker,
            _ => Self::Standalone,
        }
    }

    pub fn is_worker(self) -> bool {
        matches!(self, Self::Worker | Self::Standalone)
    }

    pub fn is_conductor(self) -> bool {
        matches!(self, Self::Conductor | Self::Standalone)
    }
}

/// Request payload a conductor sends to a worker to execute one
/// function. Mirrors the proto message described in §10; kept
/// anyhow/Convex-native for now (no serde-over-gRPC yet).
#[derive(Debug, Clone)]
pub struct ExecuteRequest {
    pub name: String,
    pub namespace: TableNamespace,
    pub args: ConvexObject,
    /// Soft timeout the worker should honor; the conductor enforces a
    /// hard timeout on its side.
    pub timeout: Option<Duration>,
    /// Minimum `registry_version` the worker must be running (Phase
    /// 4.7). The worker rejects older versions with
    /// `tonic::Code::FailedPrecondition`, so during a rolling deploy
    /// the conductor can pin a floor to steer traffic away from
    /// stragglers. `None` means any worker is acceptable.
    pub min_registry_version: Option<String>,
}

/// Response a worker sends back. In the proto version this becomes
/// `FunctionOutcome + FunctionFinalTransaction + usage stats`. Here
/// it's the minimal shape the existing `ActionCtx`-driven path needs.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    pub result: Result<ConvexValue, String>,
}

/// Trait a worker implements to accept remote calls. The distributed
/// crate wires this over gRPC; in tests or in-process flows the
/// composite runner can implement it directly.
#[async_trait::async_trait]
pub trait FunctionExecutor: Send + Sync + 'static {
    async fn execute(&self, req: ExecuteRequest) -> anyhow::Result<ExecuteResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_parses_env_strings() {
        assert_eq!(ConvexMode::from_env_str(""), ConvexMode::Standalone);
        assert_eq!(
            ConvexMode::from_env_str("standalone"),
            ConvexMode::Standalone
        );
        assert_eq!(ConvexMode::from_env_str("CONDUCTOR"), ConvexMode::Conductor);
        assert_eq!(ConvexMode::from_env_str(" worker  "), ConvexMode::Worker);
        assert_eq!(ConvexMode::from_env_str("garbage"), ConvexMode::Standalone);
    }

    #[test]
    fn standalone_is_both_worker_and_conductor() {
        assert!(ConvexMode::Standalone.is_worker());
        assert!(ConvexMode::Standalone.is_conductor());
        assert!(ConvexMode::Worker.is_worker());
        assert!(!ConvexMode::Worker.is_conductor());
        assert!(!ConvexMode::Conductor.is_worker());
        assert!(ConvexMode::Conductor.is_conductor());
    }
}
