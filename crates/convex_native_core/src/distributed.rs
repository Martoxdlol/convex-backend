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
/// `CONVEX_MODE` env var. See `convex-native/DISTRIBUTED_PLAN.md`
/// for how these map onto the target architecture — under the
/// new plan the standalone "Conductor" role is replaced by the
/// prebuilt backend image, so `Conductor` is kept here only as a
/// rejected legacy value. `Standalone` and `Worker` are both
/// first-class.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConvexMode {
    /// All-in-one: HTTP, database, and native registry in one
    /// process. This is what `convex-local-backend` does by default
    /// and what `STANDALONE.md` documents as the monolith topology.
    Standalone,
    /// Legacy: standalone dispatcher with no database. Removed in
    /// favour of the prebuilt backend image
    /// (`DISTRIBUTED_PLAN.md` Phase 5). Parsing this from the env
    /// is allowed for backwards compatibility; callers are expected
    /// to reject it early (see `local_backend::lib::make_app`).
    Conductor,
    /// Worker only: executes native functions when called by the
    /// backend. The backend ↔ worker wire contract is in
    /// `pb::function_execution`.
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

/// Request payload the backend sends to a worker to execute one
/// function. Mirrors `pb::function_execution::ExecuteRequest` in
/// anyhow/Convex-native form.
#[derive(Debug, Clone)]
pub struct ExecuteRequest {
    pub name: String,
    pub namespace: TableNamespace,
    pub args: ConvexObject,
    /// Soft timeout the worker should honor; the backend enforces
    /// a hard timeout on its side.
    pub timeout: Option<Duration>,
    /// Minimum `registry_version` the worker must be running. The
    /// worker rejects older versions with
    /// `tonic::Code::FailedPrecondition`, so during a rolling deploy
    /// the backend can pin a floor to steer traffic away from
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
    /// Backend-assigned read-snapshot timestamp (substep 2.1 /
    /// Phase 1 proto field `begin_timestamp`). Raw u64 of
    /// `common::types::Timestamp`. Populated by the Phase-2
    /// dispatcher when entering `run_function`; `None` means the
    /// worker should fall back to its own `now_ts_for_reads()`.
    /// Absent for actions (no enclosing tx).
    pub begin_timestamp: Option<u64>,
    /// Substep 2.4 — document updates the backend has staged but
    /// not yet committed. Multi-UDF batched requests use this to
    /// replay earlier UDFs' writes on the worker before running
    /// the handler. Empty on single-UDF requests (the common
    /// case). The worker applies them via
    /// `Transaction::merge_writes`.
    pub existing_writes: Vec<common::document::DocumentUpdateWithPrevTs>,
    /// HTTP-action payload. Set when `udf_type == HttpAction`;
    /// the worker decodes this into a `convex_native_core::http::HttpRequest`,
    /// runs the handler, and returns the response on
    /// `ExecuteResponse.http_response`. `None` for every other
    /// `UdfType`.
    pub http_request: Option<HttpActionRequestPayload>,
    /// Identity the call should run as, pre-serialised through
    /// the `pb::convex_identity::UncheckedIdentity` proto
    /// shape the backend callback path also uses. Empty vec
    /// ⇒ `Identity::system()` on the worker side (matches the
    /// Phase-4 decoder contract). The dispatcher fills this in
    /// from the incoming request's `Identity` so actions /
    /// mutations / HTTP handlers run under the caller's
    /// principal rather than system.
    pub identity: Vec<u8>,
}

/// Native-side mirror of
/// `pb::function_execution::HttpActionRequest`. Carries an HTTP
/// request's wire-ready fields: method, URL, headers, body, and
/// the path the matched route used.
#[derive(Debug, Clone)]
pub struct HttpActionRequestPayload {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: bytes::Bytes,
    pub routed_path: String,
}

/// Native-side mirror of
/// `pb::function_execution::HttpActionResponse`. Populated by
/// the worker after an HTTP-action dispatch; the backend decodes
/// it back into a `convex_native_core::http::HttpResponse`
/// equivalent before streaming bytes to the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpActionResponsePayload {
    pub status: u32,
    pub headers: Vec<(String, String)>,
    pub body: bytes::Bytes,
}

/// Response a worker sends back.
///
/// `log_lines` carries the worker's drained `ctx.log()` output
/// in the pretty-string form `LogLine::to_pretty_strings` emits —
/// the backend can forward them into its own log-streaming path
/// alongside JS log lines without translation. Empty when the
/// handler didn't log anything (the common case) or when the
/// worker isn't configured to drain logs.
///
/// `final_tx` is the Phase-1 snapshot of the worker's transaction
/// (see `FinalTxSummary`). Present for queries + mutations where
/// the handler closed its transaction cleanly; `None` for actions
/// and for handler errors.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    pub result: Result<ConvexValue, String>,
    pub log_lines: Vec<String>,
    pub final_tx: Option<FinalTxSummary>,
    /// HTTP-action response payload. Set when the request was
    /// for `UdfType::HttpAction` and the worker produced a
    /// response; `None` for query/mutation/action paths and for
    /// handler errors on HTTP actions (the error flows on the
    /// `result` channel instead).
    pub http_response: Option<HttpActionResponsePayload>,
}

impl ExecuteResponse {
    /// Construct a response carrying no log lines — the previous
    /// shape. Most existing call sites used `ExecuteResponse { result }`
    /// struct literals; this constructor keeps those short.
    pub fn new(result: Result<ConvexValue, String>) -> Self {
        Self {
            result,
            log_lines: Vec::new(),
            final_tx: None,
            http_response: None,
        }
    }

    /// Builder-style accessor for attaching an HTTP-action
    /// response payload.
    pub fn with_http_response(mut self, http_response: HttpActionResponsePayload) -> Self {
        self.http_response = Some(http_response);
        self
    }

    /// Builder-style accessor for attaching log lines before
    /// returning.
    pub fn with_log_lines(mut self, lines: Vec<String>) -> Self {
        self.log_lines = lines;
        self
    }

    /// Builder-style accessor for attaching the transaction
    /// summary. `FunctionExecutionServer` uses this on the worker
    /// side; client-side callers consume the summary through the
    /// parsed response.
    pub fn with_final_tx(mut self, final_tx: FinalTxSummary) -> Self {
        self.final_tx = Some(final_tx);
        self
    }
}

/// Native-side shape of `function_runner::FunctionFinalTransaction`,
/// mirroring the proto message `pb::function_execution::DistributedFinalTx`
/// 1:1 so the conversions layer can move between them without
/// losing fields.
///
/// Substep 2.2b landed the `index_reads` field, which contains
/// `IntervalSet` — a type that does not implement `PartialEq`.
/// Downstream tests compare the interesting fields individually
/// rather than relying on struct-level equality.
#[derive(Clone, Debug, Default)]
pub struct FinalTxSummary {
    /// Raw u64 representation of the `Timestamp` the worker opened
    /// its transaction at.
    pub begin_timestamp: u64,
    /// Number of coalesced document updates the handler produced.
    /// After substep 2.3 this is redundant with `writes.len()`,
    /// kept for back-compat with the Phase-1 wire message.
    pub writes_count: u64,
    /// Number of read intervals the handler accumulated. Not
    /// "rows read" exactly — it's the count of `ReadSet` intervals,
    /// which the backend uses as a coarse usage signal until
    /// substep 2.2's full read-set wire format lands.
    pub reads_count: u64,
    /// Per-tablet row counts the handler observed. Keys are
    /// `TabletId` strings in their canonical `Display` form
    /// (see `value::InternalId`). The backend feeds this straight
    /// into `Transaction::apply_function_runner_tx`'s
    /// `rows_read_by_tablet` argument, so distributed and
    /// in-process dispatch produce identical usage numbers.
    /// Empty on the error / action paths.
    pub rows_read_by_tablet: std::collections::BTreeMap<String, u64>,
    /// Document updates (insert / replace / delete) the handler
    /// produced, coalesced so each touched document appears at
    /// most once. Matches the `FunctionWrites::updates` shape the
    /// in-process path feeds
    /// `Transaction::apply_function_runner_tx`. Empty on the
    /// error / action paths and when the handler committed no
    /// writes.
    pub writes: Vec<common::document::DocumentUpdateWithPrevTs>,
    /// Substep 2.2a: scalar read-size counters split by user-tablet
    /// vs system-tablet origin. Mirrors
    /// `database::TransactionReadSize`. `None` when the worker
    /// didn't open a transaction (actions, handler errors).
    pub user_tx_size: Option<TxReadSize>,
    pub system_tx_size: Option<TxReadSize>,
    /// Substep 2.2b: indexed reads the handler accumulated — one
    /// entry per (tablet, index). Empty list = the handler opened
    /// a tx but read nothing indexable (and a valid state
    /// distinct from "no final_tx"). Search reads aren't carried
    /// here because native handlers have no tx-level search
    /// surface — vector / text search arrives via the
    /// `NativeActionCallbacks::vector_search` sub-call path and
    /// doesn't accumulate on the worker's `TransactionReadSet`.
    pub index_reads: Vec<IndexReadsSummary>,
    /// Per-invocation observed-determinism flags drained off
    /// the handler's `Observed` handle. The backend writes
    /// these into the resulting `UdfOutcome` so distributed
    /// dispatch produces the same shape the in-process
    /// composite runner does.
    pub observed_identity: bool,
    pub observed_rng: bool,
    pub observed_time: bool,
    /// 32-byte RNG seed the worker's `Observed` was constructed
    /// with. Backend writes into `UdfOutcome::rng_seed` so a
    /// re-execution can replay the same handler-visible
    /// randomness.
    pub rng_seed: [u8; 32],
    /// Wall-clock unix timestamp the worker captured when
    /// `observed_time` flipped — encoded as nanoseconds since
    /// the unix epoch. Zero when the handler didn't observe
    /// time.
    pub unix_timestamp_nanos: u64,
}

/// Native-side mirror of `pb::function_execution::DistributedTxReadSize`
/// (which in turn mirrors `database::TransactionReadSize`). Scalar
/// counters the backend needs for usage tracking.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub struct TxReadSize {
    pub total_document_size: u64,
    pub total_document_count: u64,
}

/// Native-side mirror of `pb::function_execution::DistributedIndexReads`.
/// One entry per (tablet, index) the handler read through. Substep
/// 2.2b of `convex-native/STATUS.md`.
///
/// Kept in a shape close to `database::IndexReads` so
/// `server::summarise_tx` can build one entry per ReadSet index
/// without a lossy projection. Search-index reads are **not**
/// carried here — native handlers don't currently use search; the
/// follow-up substep adds a dedicated shape for those.
#[derive(Clone, Debug)]
pub struct IndexReadsSummary {
    /// `TabletIndexName` carrying the tablet id + index descriptor
    /// the handler read through.
    pub index_name: common::types::TabletIndexName,
    /// Ordered field-path list this index indexes on.
    pub fields: common::bootstrap_model::index::database_index::IndexedFields,
    /// Flat interval set the handler's reads covered. Reuses
    /// `common::interval::IntervalSet` so the conversions layer
    /// can piggy-back on the existing
    /// `IntervalSet <-> Vec<IntervalProto>` impl.
    pub intervals: common::interval::IntervalSet,
}

/// Trait a worker implements to accept remote calls. The
/// `convex_native_distributed` crate wires this over gRPC; in
/// tests or in-process flows a composite runner can implement it
/// directly.
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
        // responses across worker/backend boundaries, so a silent
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
