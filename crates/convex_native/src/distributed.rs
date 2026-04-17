//! Distributed execution protocol — tonic-free Rust shapes.
//!
//! See `convex-native/DISTRIBUTED_PLAN.md` for the target
//! architecture: the backend process coordinates OCC and
//! subscriptions, and dispatches function execution over gRPC to
//! a pool of identical worker binaries that run the native
//! registry.
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

use common::execution_context::ExecutionContext;
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
    /// Execution context to propagate across the gRPC boundary.
    /// When set, the worker rebuilds a matching `ExecutionContext`
    /// so request-id / execution-id / parent-scheduled-job chains
    /// span both processes — the observable side of design §12.3's
    /// "distributed tracing" hook. `None` means the worker
    /// synthesizes a fresh context (appropriate for internal or
    /// test dispatches).
    pub execution_context: Option<ExecutionContext>,
}

/// Response a worker sends back. In the proto version this becomes
/// `FunctionOutcome + FunctionFinalTransaction + usage stats`. Here
/// it's the minimal shape the existing `ActionCtx`-driven path needs.
///
/// `log_lines` carries the worker's drained `ctx.log()` output
/// in the pretty-string form `LogLine::to_pretty_strings` emits —
/// the conductor can forward them into its own log-streaming path
/// alongside JS log lines without translation. Empty when the
/// handler didn't log anything (the common case) or when the
/// worker isn't configured to drain logs.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    pub result: Result<ConvexValue, String>,
    pub log_lines: Vec<String>,
}

impl ExecuteResponse {
    /// Construct a response carrying no log lines — the previous
    /// shape. Most existing call sites used `ExecuteResponse { result }`
    /// struct literals; this constructor keeps those short.
    pub fn new(result: Result<ConvexValue, String>) -> Self {
        Self {
            result,
            log_lines: Vec::new(),
        }
    }

    /// Builder-style accessor for attaching log lines before
    /// returning.
    pub fn with_log_lines(mut self, lines: Vec<String>) -> Self {
        self.log_lines = lines;
        self
    }
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

    #[test]
    fn mode_parser_is_case_insensitive_and_trimmed() {
        // Operators hand-type these env vars; a rogue space or capital
        // letter should not land them in Standalone by accident.
        for s in [
            "Worker",
            "WORKER",
            "  worker",
            "worker  ",
            "\tworker\n",
            "Worker ",
        ] {
            assert_eq!(
                ConvexMode::from_env_str(s),
                ConvexMode::Worker,
                "should recognise worker in {s:?}",
            );
        }
        for s in ["Conductor", "CONDUCTOR", " conductor "] {
            assert_eq!(
                ConvexMode::from_env_str(s),
                ConvexMode::Conductor,
                "should recognise conductor in {s:?}",
            );
        }
    }

    #[test]
    fn mode_parser_falls_back_to_standalone_for_unknown_values() {
        // Unknown values fall through to Standalone — the default.
        // Pin that so future refactors don't silently flip to an
        // explicit error path.
        for s in ["standalon", "worker-v2", "master", "primary", "1"] {
            assert_eq!(
                ConvexMode::from_env_str(s),
                ConvexMode::Standalone,
                "unknown {s:?} defaults to Standalone",
            );
        }
    }

    #[test]
    fn execute_response_clones_and_debug_formats() {
        // Clone + Debug are derived; callers pattern-match + clone
        // responses across worker/conductor boundaries, so a silent
        // derive drop would break consumers.
        let resp = ExecuteResponse::new(Ok(ConvexValue::Int64(42)));
        let cloned = resp.clone();
        assert!(matches!(&cloned.result, Ok(ConvexValue::Int64(42))));
        assert!(!format!("{resp:?}").is_empty(), "Debug format non-empty");

        let err_resp = ExecuteResponse::new(Err("boom".into()));
        let cloned = err_resp.clone();
        assert_eq!(
            cloned.result.as_ref().err().map(String::as_str),
            Some("boom"),
        );
    }

    #[test]
    fn function_executor_is_object_safe() {
        // The trait is used as `Arc<dyn FunctionExecutor>` by remote
        // dispatch paths. Keep the object-safety assertion as a
        // compile-time canary.
        fn _assert_object_safe(_: std::sync::Arc<dyn FunctionExecutor>) {}
    }
}
