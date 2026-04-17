//! Worker-side gRPC server. See
//! `convex-native/DISTRIBUTED_PLAN.md` for the target architecture
//! — this module is what the worker process runs.
//!
//! `FunctionExecutionServer` wraps an `Arc<NativeFunctionRunner>`
//! and implements the generated
//! `pb::function_execution::function_execution_service_server::FunctionExecutionService`
//! tonic trait. A worker process instantiates this, binds it to a
//! `tonic::transport::Server`, and serves backend traffic.
//!
//! ## What ships here today (pre-Phase-1)
//!
//! - `health`: fully implemented, reports registry version (derived from the
//!   `convex_native` crate version), `accepts_traffic` (false when the runner
//!   is draining), `registered_functions`, and `in_flight`.
//! - `execute` for `UdfType::Action`: dispatches via
//!   `NativeFunctionRunner::run_action_with_callbacks` with
//!   `NoopCallbacks`. A real `BackendCallbackService` implementation
//!   lands in Phase 4 of `DISTRIBUTED_PLAN.md` so action sub-calls
//!   route back to the backend's Committer.
//! - `execute` for `UdfType::Query` / `UdfType::Mutation`: when the
//!   server was constructed with `.with_database(db)`, dispatches
//!   inline against a fresh `Transaction<Rt>` (queries drop the tx;
//!   mutations commit via `commit_with_write_source`). **Phase 1
//!   removes the inline commit** — mutations will produce a
//!   `FunctionFinalTransaction` in the response and the backend
//!   will commit through its single Committer.
//! - `execute` for `UdfType::HttpAction`: `Unimplemented`. HTTP
//!   actions use a different dispatch path (`HttpRouter`).

use std::sync::Arc;

use async_trait::async_trait;
use common::types::UdfType;
use convex_native::{
    NativeFunctionRunner,
    Rt,
};
use common::types::Timestamp;
use database::Database;
use keybroker::Identity;
use pb::function_execution::{
    self as proto,
    function_execution_service_server::FunctionExecutionService,
};
use tonic::{
    Request,
    Response,
    Status,
};
use usage_tracking::FunctionUsageTracker;

use crate::conversions;

/// Worker-side server. Holds a shared native runner and answers
/// `FunctionExecutionService` RPCs.
///
/// Cheap to clone (it's just `Arc`s) — the generated
/// `FunctionExecutionServiceServer::new(server)` wraps it in a
/// `tonic::service::Routes`.
#[derive(Clone)]
pub struct FunctionExecutionServer {
    native: Arc<NativeFunctionRunner>,
    /// Worker-local database handle. When set, the server can
    /// dispatch queries and mutations inline against a fresh
    /// `Transaction<Rt>`. Without it, query/mutation requests
    /// return `Code::Unimplemented`.
    database: Option<Database<Rt>>,
    /// Registry version reported in `Health`. Defaults to the
    /// `convex_native` crate version; callers can override if they
    /// want a finer-grained tag.
    registry_version: String,
}

impl FunctionExecutionServer {
    pub fn new(native: Arc<NativeFunctionRunner>) -> Self {
        Self {
            native,
            database: None,
            registry_version: convex_native::VERSION.to_string(),
        }
    }

    /// Attach a worker-local database so the server can dispatch
    /// queries and mutations. Today the worker commits locally
    /// against this database because the wire protocol doesn't yet
    /// carry read/write sets back to the backend.
    ///
    /// Phase 1 of `convex-native/DISTRIBUTED_PLAN.md` changes this:
    /// the worker produces a `FunctionFinalTransaction` and the
    /// backend's Committer does the commit. Leaving the database
    /// unattached keeps the query/mutation branches surfacing
    /// `Unimplemented` until Phase 1 lands.
    pub fn with_database(mut self, database: Database<Rt>) -> Self {
        self.database = Some(database);
        self
    }

    /// Override the registry version. Useful for tests and for
    /// per-deploy tagging during rolling updates (Phase 4.7).
    pub fn with_registry_version(mut self, v: impl Into<String>) -> Self {
        self.registry_version = v.into();
        self
    }
}

#[async_trait]
impl FunctionExecutionService for FunctionExecutionServer {
    async fn execute(
        &self,
        request: Request<proto::ExecuteRequest>,
    ) -> Result<Response<proto::ExecuteResponse>, Status> {
        let proto_req = request.into_inner();

        // Version gate (Phase 4.7). If the backend asked for a
        // minimum registry version, confirm we meet it. String
        // comparison is semver-correct for the canonical
        // MAJOR.MINOR.PATCH format used by Cargo versions; pre-release
        // tags are handled by the backend's semver check, not
        // here.
        if let Some(min) = proto_req.min_registry_version.as_deref()
            && !version_at_least(&self.registry_version, min)
        {
            return Err(Status::failed_precondition(format!(
                "worker registry_version {:?} is older than min_registry_version {:?}",
                self.registry_version, min,
            )));
        }

        let (native_req, udf_type) = conversions::from_proto_request(&proto_req)
            .map_err(|e| Status::invalid_argument(format!("decode ExecuteRequest: {e}")))?;

        match udf_type {
            UdfType::Action => {
                // TODO(phase-4): route sub-calls back to the
                // backend via BackendCallbackService so the
                // backend's Committer owns every write the action
                // causes. NoopCallbacks is the placeholder — it
                // bails on every sub-call, which is loud and
                // safe until the proper routing lands. See
                // `convex-native/DISTRIBUTED_PLAN.md` §7.4.
                let callbacks: Arc<dyn convex_native::NativeActionCallbacks> =
                    Arc::new(convex_native::callbacks::NoopCallbacks);
                let log_buffer = convex_native::LogBuffer::new();
                let result = self
                    .native
                    .run_action_with_callbacks_and_log_buffer(
                        &native_req.name,
                        native_req.namespace,
                        native_req.args,
                        callbacks,
                        log_buffer.clone(),
                    )
                    .await;
                let log_lines = log_lines_to_pretty_strings(&log_buffer);
                let native_response = convex_native::distributed::ExecuteResponse::new(
                    result.map_err(|e| e.to_string()),
                )
                .with_log_lines(log_lines);
                let mut proto_resp = conversions::to_proto_response(&native_response);
                proto_resp.served_by_version = Some(self.registry_version.clone());
                Ok(Response::new(proto_resp))
            },
            UdfType::Query | UdfType::Mutation => {
                let Some(database) = self.database.as_ref() else {
                    return Err(Status::unimplemented(
                        "FunctionExecutionServer: query/mutation dispatch requires a worker-local \
                         Database<Rt>. Construct the server with .with_database(db) to enable it.",
                    ));
                };
                let begin_ts = proto_req
                    .begin_timestamp
                    .map(Timestamp::try_from)
                    .transpose()
                    .map_err(|e| {
                        Status::invalid_argument(format!(
                            "ExecuteRequest.begin_timestamp is not a valid Timestamp: {e}"
                        ))
                    })?;
                let dispatch = match udf_type {
                    UdfType::Query => {
                        run_query_inline(&self.native, database, &native_req, begin_ts).await
                    },
                    UdfType::Mutation => {
                        run_mutation_inline(&self.native, database, &native_req, begin_ts).await
                    },
                    _ => unreachable!(),
                };
                let native_response = convex_native::distributed::ExecuteResponse::new(
                    dispatch.result.map_err(|e| e.to_string()),
                )
                .with_log_lines(dispatch.log_lines);
                let mut proto_resp = conversions::to_proto_response(&native_response);
                proto_resp.served_by_version = Some(self.registry_version.clone());
                // Phase 1: worker reports the reads/writes summary back
                // to the backend instead of committing locally. See
                // `convex-native/DISTRIBUTED_PLAN.md` §15 Phase 1.
                proto_resp.final_tx = dispatch.final_tx;
                Ok(Response::new(proto_resp))
            },
            UdfType::HttpAction => Err(Status::unimplemented(
                "FunctionExecutionServer: HTTP actions use the HttpRouter dispatch path, not \
                 FunctionExecutionService.",
            )),
        }
    }

    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        let accepts_traffic = !self.native.is_draining();
        let resp = proto::HealthResponse {
            registry_version: self.registry_version.clone(),
            accepts_traffic,
            registered_functions: self.native.len() as u64,
            in_flight: self.native.in_flight(),
        };
        Ok(Response::new(resp))
    }
}

/// Bundle returned by the query / mutation dispatch helpers.
/// Carries the handler result, drained log lines, and — when the
/// handler closed its transaction cleanly — the `DistributedFinalTx`
/// the worker reports to the backend.
struct InlineDispatch {
    result: anyhow::Result<value::ConvexValue>,
    log_lines: Vec<String>,
    final_tx: Option<proto::DistributedFinalTx>,
}

/// Open a fresh transaction at `begin_ts` (or `now_ts_for_reads()`
/// when the backend didn't supply one — pre-Phase-1 callers), run
/// the native query, then summarise the transaction's read set
/// into a `DistributedFinalTx`. Queries are read-only by design so
/// nothing ever commits.
async fn run_query_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native::distributed::ExecuteRequest,
    begin_ts: Option<Timestamp>,
) -> InlineDispatch {
    let log_buffer = convex_native::LogBuffer::new();
    let mut final_tx: Option<proto::DistributedFinalTx> = None;
    let result = async {
        let ts = match begin_ts {
            Some(ts) => database.now_ts_for_reads().prior_ts(ts)?,
            None => database.now_ts_for_reads(),
        };
        let usage = FunctionUsageTracker::new();
        let mut tx = database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        let value = native
            .run_query_with_log_buffer(
                &req.name,
                &mut tx,
                req.namespace,
                req.args.clone(),
                log_buffer.clone(),
            )
            .await?;
        final_tx = Some(summarise_tx(tx));
        Ok(value)
    }
    .await;
    let log_lines = log_lines_to_pretty_strings(&log_buffer);
    InlineDispatch {
        result,
        log_lines,
        final_tx,
    }
}

/// Open a fresh transaction at `begin_ts` (or `now_ts_for_reads()`
/// when the backend didn't supply one), run the native mutation,
/// and — on handler success — summarise the resulting transaction
/// into a `DistributedFinalTx`. **Phase 1 of `DISTRIBUTED_PLAN.md`
/// removes the inline commit**: the worker no longer touches
/// `Database::commit_with_write_source`. Phase 2 wires the backend
/// to consume the returned `DistributedFinalTx` and commit through
/// its own Committer. On handler error the tx is dropped without
/// reporting.
async fn run_mutation_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native::distributed::ExecuteRequest,
    begin_ts: Option<Timestamp>,
) -> InlineDispatch {
    let log_buffer = convex_native::LogBuffer::new();
    let mut final_tx: Option<proto::DistributedFinalTx> = None;
    let result = async {
        let ts = match begin_ts {
            Some(ts) => database.now_ts_for_reads().prior_ts(ts)?,
            None => database.now_ts_for_reads(),
        };
        let usage = FunctionUsageTracker::new();
        let mut tx = database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        let value = native
            .run_mutation_with_log_buffer(
                &req.name,
                &mut tx,
                req.namespace,
                req.args.clone(),
                log_buffer.clone(),
            )
            .await?;
        final_tx = Some(summarise_tx(tx));
        Ok(value)
    }
    .await;
    let log_lines = log_lines_to_pretty_strings(&log_buffer);
    InlineDispatch {
        result,
        log_lines,
        final_tx,
    }
}

/// Drain a finished transaction into the Phase-1 wire summary.
/// Today only scalars (begin_ts, writes_count, reads_count); Phase
/// 2 grows the `DistributedFinalTx` proto message to carry the full
/// `FunctionReads` + `FunctionWrites` content the backend's
/// Committer needs. Errors from `into_flat()` (nested transaction
/// leftover) are swallowed into a zero writes count — for native
/// handlers the tx is always flat at this point; a future invariant
/// violation would show up as the backend rejecting the response
/// at commit time under Phase 2 regardless.
fn summarise_tx(tx: database::Transaction<Rt>) -> proto::DistributedFinalTx {
    let begin_timestamp: u64 = (*tx.begin_timestamp()).into();
    let (reads, writes) = tx.into_reads_and_writes();
    let reads_count = reads.num_intervals() as u64;
    let writes_count = writes
        .into_flat()
        .map(|flat| flat.into_coalesced_writes().count())
        .unwrap_or(0) as u64;
    proto::DistributedFinalTx {
        begin_timestamp,
        writes_count,
        reads_count,
    }
}

/// Snapshot the buffer and render each line as a plain string.
/// Matches the `repeated string log_lines` field on the proto: one
/// formatted line per entry, ready for the backend to forward
/// into its own log-streaming path without re-parsing.
fn log_lines_to_pretty_strings(buffer: &convex_native::LogBuffer) -> Vec<String> {
    buffer
        .snapshot()
        .into_iter()
        .map(|line| {
            let level = match line.level {
                convex_native::LogLevel::Debug => "DEBUG",
                convex_native::LogLevel::Info => "INFO",
                convex_native::LogLevel::Warn => "WARN",
                convex_native::LogLevel::Error => "ERROR",
            };
            format!("[{level}] {}", line.message)
        })
        .collect()
}

/// Returns true when `have` >= `want` under a simple lexicographic
/// compare of the MAJOR.MINOR.PATCH form. A full `semver` compare
/// belongs on the backend side (that's where the full version
/// set is visible); the worker is just checking its own tag.
fn version_at_least(have: &str, want: &str) -> bool {
    fn parts(s: &str) -> Vec<u64> {
        s.split(['.', '-', '+'])
            .filter_map(|p| p.parse::<u64>().ok())
            .collect()
    }
    parts(have) >= parts(want)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::Duration,
    };

    use convex_native::distributed::ExecuteRequest as NativeExecuteRequest;
    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableNamespace,
    };

    use super::*;

    fn empty_runner() -> Arc<NativeFunctionRunner> {
        Arc::new(NativeFunctionRunner::from_inventory().expect("collect"))
    }

    fn empty_object() -> ConvexObject {
        let f: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        ConvexObject::try_from(f).unwrap()
    }

    #[tokio::test]
    async fn health_reports_registry_version_and_empty_registry() {
        let server = FunctionExecutionServer::new(empty_runner()).with_registry_version("1.2.3");
        let resp = server
            .health(Request::new(proto::HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.registry_version, "1.2.3");
        assert!(resp.accepts_traffic);
        assert_eq!(resp.registered_functions, 0);
        assert_eq!(resp.in_flight, 0);
    }

    #[tokio::test]
    async fn execute_unknown_action_surfaces_runner_error_in_response() {
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "does_not_exist".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
        };
        let proto_req = conversions::to_proto_request(&native, UdfType::Action).unwrap();
        let resp = server
            .execute(Request::new(proto_req))
            .await
            .unwrap()
            .into_inner();
        // Runner returns a user-level error, not a gRPC Status —
        // that's the contract for "handler executed, handler errored".
        let native_resp = conversions::from_proto_response(&resp).unwrap();
        assert!(matches!(native_resp.result, Err(ref m) if m.contains("does_not_exist")));
    }

    #[tokio::test]
    async fn execute_query_without_database_is_unimplemented() {
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "anything".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: Some(Duration::from_millis(100)),
            min_registry_version: None,
            execution_context: None,
        };
        let proto_req = conversions::to_proto_request(&native, UdfType::Query).unwrap();
        let status = server.execute(Request::new(proto_req)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn version_gate_rejects_older_worker() {
        let server = FunctionExecutionServer::new(empty_runner()).with_registry_version("0.1.0");
        let native = NativeExecuteRequest {
            name: "doesnt_matter".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
        };
        let mut proto_req = conversions::to_proto_request(&native, UdfType::Action).unwrap();
        proto_req.min_registry_version = Some("9.9.9".to_string());
        let status = server.execute(Request::new(proto_req)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn version_compare_obeys_major_minor_patch() {
        assert!(version_at_least("1.2.3", "1.2.3"));
        assert!(version_at_least("1.2.4", "1.2.3"));
        assert!(version_at_least("2.0.0", "1.99.99"));
        assert!(!version_at_least("1.2.3", "1.2.4"));
        assert!(!version_at_least("0.1.0", "9.9.9"));
    }

    #[test]
    fn version_compare_handles_prerelease_and_build_suffixes() {
        // The parser splits on `.`, `-`, and `+` and keeps only the
        // numeric fields. `1.2.3-rc.1` therefore compares as
        // `[1, 2, 3, 1]`, and `1.2.3+build.7` as `[1, 2, 3, 7]`.
        // That's intentional — the worker only checks its own tag
        // against a floor; full semver lives on the backend side.
        assert!(version_at_least("1.2.3-rc.1", "1.2.3"));
        assert!(version_at_least("1.2.3+build.7", "1.2.3"));
        assert!(!version_at_least("1.2.3", "1.2.3-rc.1"));
    }

    #[test]
    fn version_compare_treats_empty_as_minimum() {
        // Completely empty / non-numeric strings parse to [], which
        // the `Vec::cmp` lands below every concrete version. Pin
        // that behaviour so a worker that booted with an
        // unreadable version tag loses every floor check.
        assert!(!version_at_least("", "1.0.0"));
        assert!(version_at_least("1.0.0", ""));
        assert!(version_at_least("", ""), "two empties are equal");
    }

    #[tokio::test]
    async fn health_flips_accepts_traffic_when_runner_drains() {
        // `accepts_traffic` is how the backend learns a worker is
        // draining so it can steer new RPCs elsewhere. Pin the flip:
        // before draining → true, after `begin_drain()` → false.
        let runner = empty_runner();
        let server = FunctionExecutionServer::new(runner.clone());

        let resp = server
            .health(Request::new(proto::HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.accepts_traffic, "healthy runner accepts traffic");

        runner.begin_drain();

        let resp = server
            .health(Request::new(proto::HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(
            !resp.accepts_traffic,
            "draining runner stops accepting traffic",
        );
    }

    #[tokio::test]
    async fn execute_http_action_kind_is_unimplemented() {
        // The tonic server doesn't dispatch HTTP actions — those go
        // through the HttpRouter path, not FunctionExecutionService.
        // A caller that mistakenly forwards an HttpAction UdfType
        // must see `Unimplemented`, not a generic error.
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "http_handler".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
        };
        let proto_req = conversions::to_proto_request(&native, UdfType::HttpAction).unwrap();
        let status = server.execute(Request::new(proto_req)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert!(
            status.message().contains("HttpRouter"),
            "error points at the right dispatch path: {}",
            status.message(),
        );
    }
}
