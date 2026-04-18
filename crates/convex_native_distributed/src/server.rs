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
//! ## What ships here today (post Phase-1)
//!
//! - `health`: fully implemented, reports registry version (derived from the
//!   `convex_native_core` crate version), `accepts_traffic` (false when the
//!   runner is draining), `registered_functions`, and `in_flight`.
//! - `execute` for `UdfType::Action`: dispatches via
//!   `NativeFunctionRunner::run_action_with_callbacks` with `NoopCallbacks`. A
//!   real `BackendCallbackService` implementation lands in Phase 4 of
//!   `DISTRIBUTED_PLAN.md` so action sub-calls route back to the backend's
//!   Committer.
//! - `execute` for `UdfType::Query` / `UdfType::Mutation`: when the server was
//!   constructed with `.with_database(db)`, opens a fresh `Transaction<Rt>` at
//!   the backend-supplied `begin_timestamp` (or `now_ts_for_reads()` when
//!   pre-Phase-1 callers skip the field), runs the handler, and returns a
//!   `FinalTxSummary` on the response. **Phase 1 removed the inline commit** —
//!   the worker never calls `commit_with_write_source`; the backend's Committer
//!   owns every durable write in Phase 2.
//! - `execute` for `UdfType::HttpAction`: `Unimplemented`. HTTP actions use a
//!   different dispatch path (`HttpRouter`).

use std::sync::Arc;

use async_trait::async_trait;
use common::types::{
    Timestamp,
    UdfType,
};
use convex_native_core::{
    NativeFunctionRunner,
    Rt,
};
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
    /// `convex_native_core` crate version; callers can override if they
    /// want a finer-grained tag.
    registry_version: String,
    /// Substep 4.4 of `convex-native/DISTRIBUTED_PLAN.md` — URL
    /// of the backend's `BackendCallbackService`. When set,
    /// actions on this worker use a `BackendCallbackClient` to
    /// route sub-calls back to the backend (so
    /// `ctx.run_mutation(...)` commits via the backend's
    /// Committer, not the worker's local database). When unset,
    /// actions fall back to `NoopCallbacks` — every sub-call
    /// bails with a clear error, matching the pre-Phase-4
    /// behaviour. Tests that exercise actions without sub-calls
    /// leave this unset; production workers set it to the
    /// backend's endpoint at registration time.
    backend_callback_endpoint: Option<String>,
}

impl FunctionExecutionServer {
    pub fn new(native: Arc<NativeFunctionRunner>) -> Self {
        Self {
            native,
            database: None,
            registry_version: convex_native_core::VERSION.to_string(),
            backend_callback_endpoint: None,
        }
    }

    /// Attach a worker-local database so the server can open a
    /// `Transaction<Rt>` at the backend-supplied `begin_timestamp`
    /// and run the handler against it.
    ///
    /// Phase 1 of `convex-native/DISTRIBUTED_PLAN.md` ships the
    /// worker as read-only — it returns a `FinalTxSummary` on the
    /// response (see `conversions::final_tx_to_proto`) and the
    /// backend's Committer does the durable commit in Phase 2.
    /// Leaving the database unattached keeps the query/mutation
    /// branches surfacing `Unimplemented` (useful for tests
    /// that only exercise the action path).
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

    /// Substep 4.4: wire the worker's action dispatch path to
    /// send sub-calls (`ctx.run_mutation`, `ctx.scheduler()`,
    /// storage ops) back to the backend's
    /// `BackendCallbackService` at `endpoint`. Without this,
    /// actions still run but every sub-call bails via
    /// `NoopCallbacks`.
    pub fn with_backend_callback_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.backend_callback_endpoint = Some(endpoint.into());
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
                // Substep 4.4: when a backend callback endpoint is
                // configured, route action sub-calls back to the
                // backend's `BackendCallbackService` so every
                // write flows through the backend's Committer
                // (OCC + subscription invalidation preserved).
                // Fall back to `NoopCallbacks` when the endpoint
                // isn't set — matches the pre-Phase-4 behaviour.
                //
                // The callback context (identity,
                // execution_context, component_path) is captured
                // here so sub-calls carry the same principal +
                // trace chain as the enclosing action. The
                // `proto_req.identity` bytes (already in the
                // `pb::convex_identity::UncheckedIdentity` proto
                // shape the backend agreed on) are forwarded
                // verbatim; the backend-side `decode_context`
                // turns them back into a `keybroker::Identity`.
                // Empty bytes map to `Identity::system()` on both
                // sides.
                let callbacks: Arc<dyn convex_native_core::NativeActionCallbacks> =
                    match &self.backend_callback_endpoint {
                        Some(endpoint) => {
                            let execution_context = proto_req.execution_context.clone();
                            let identity_bytes = proto_req.identity.clone().unwrap_or_default();
                            // The component_path string carries
                            // the enclosing action's component
                            // path (slash-separated component
                            // names). Native handlers are all
                            // root-component today, so an empty
                            // string is correct; when component-
                            // scoped native actions land, the
                            // backend's admission handshake will
                            // supply the path string and we'll
                            // forward it here.
                            let component_path_str = String::new();
                            let _ = native_req.namespace;
                            let client =
                                crate::backend_callbacks_client::BackendCallbackClient::connect(
                                    endpoint.clone(),
                                    identity_bytes,
                                    execution_context,
                                    component_path_str,
                                )
                                .await
                                .map_err(|e| {
                                    Status::failed_precondition(format!(
                                        "FunctionExecutionServer: failed to dial backend callback \
                                         endpoint {endpoint:?}: {e}",
                                    ))
                                })?;
                            Arc::new(client)
                        },
                        None => Arc::new(convex_native_core::callbacks::NoopCallbacks),
                    };
                let log_buffer = convex_native_core::LogBuffer::new();
                let action_identity = decode_identity_bytes(&native_req.identity).map_err(|e| {
                    Status::invalid_argument(format!("decode action identity: {e}"))
                })?;
                let result = self
                    .native
                    .run_action_with_callbacks_identity_log_buffer_and_context(
                        &native_req.name,
                        native_req.namespace,
                        native_req.args,
                        callbacks,
                        action_identity,
                        log_buffer.clone(),
                        native_req.execution_context.clone(),
                    )
                    .await;
                let log_lines = log_lines_to_pretty_strings(&log_buffer);
                let native_response = convex_native_core::distributed::ExecuteResponse::new(
                    result.map_err(|e| e.to_string()),
                )
                .with_log_lines(log_lines);
                let mut proto_resp = conversions::to_proto_response(&native_response)
                    .map_err(|e| Status::internal(format!("encode ExecuteResponse: {e}")))?;
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
                // Phase 1: worker reports the reads/writes summary back
                // to the backend instead of committing locally. See
                // `convex-native/DISTRIBUTED_PLAN.md` §15 Phase 1. The
                // summary rides on the native `ExecuteResponse` so the
                // proto encoding — and any future consumer of the
                // native shape — picks it up through `to_proto_response`.
                let mut native_response = convex_native_core::distributed::ExecuteResponse::new(
                    dispatch.result.map_err(|e| e.to_string()),
                )
                .with_log_lines(dispatch.log_lines);
                if let Some(summary) = dispatch.final_tx {
                    native_response = native_response.with_final_tx(summary);
                }
                let mut proto_resp = conversions::to_proto_response(&native_response)
                    .map_err(|e| Status::internal(format!("encode ExecuteResponse: {e}")))?;
                proto_resp.served_by_version = Some(self.registry_version.clone());
                Ok(Response::new(proto_resp))
            },
            UdfType::HttpAction => {
                let http_request = proto_req.http_request.as_ref().ok_or_else(|| {
                    Status::invalid_argument(
                        "ExecuteRequest.http_request is required for UdfType::HttpAction",
                    )
                })?;
                let request = decode_http_request(http_request).map_err(|e| {
                    Status::invalid_argument(format!("decode HttpActionRequest: {e}"))
                })?;
                // Reuse the same callback wiring the action branch
                // builds: HTTP actions can issue sub-calls
                // (run_mutation, scheduler, storage) just like
                // regular actions.
                let callbacks: Arc<dyn convex_native_core::NativeActionCallbacks> =
                    match &self.backend_callback_endpoint {
                        Some(endpoint) => {
                            let execution_context = proto_req.execution_context.clone();
                            let identity_bytes = proto_req.identity.clone().unwrap_or_default();
                            let client =
                                crate::backend_callbacks_client::BackendCallbackClient::connect(
                                    endpoint.clone(),
                                    identity_bytes,
                                    execution_context,
                                    String::new(),
                                )
                                .await
                                .map_err(|e| {
                                    Status::failed_precondition(format!(
                                        "FunctionExecutionServer: failed to dial backend callback \
                                         endpoint {endpoint:?}: {e}",
                                    ))
                                })?;
                            Arc::new(client)
                        },
                        None => Arc::new(convex_native_core::callbacks::NoopCallbacks),
                    };
                let log_buffer = convex_native_core::LogBuffer::new();
                let http_identity = decode_identity_bytes(&native_req.identity).map_err(|e| {
                    Status::invalid_argument(format!("decode http-action identity: {e}"))
                })?;
                let result = self
                    .native
                    .run_http_action_with_callbacks_identity_and_context(
                        &native_req.name,
                        request,
                        callbacks,
                        Some(log_buffer.clone()),
                        http_identity,
                        native_req.execution_context.clone(),
                    )
                    .await;
                let log_lines = log_lines_to_pretty_strings(&log_buffer);
                let mut proto_resp = match result {
                    Ok(http_response) => proto::ExecuteResponse {
                        result: Some(pb::common::FunctionResult {
                            result: Some(pb::common::function_result::Result::JsonPackedValue(
                                "null".to_string(),
                            )),
                        }),
                        user_execution_time: None,
                        served_by_version: None,
                        log_lines,
                        final_tx: None,
                        http_response: Some(encode_http_response(http_response)),
                    },
                    Err(e) => proto::ExecuteResponse {
                        result: Some(pb::common::FunctionResult {
                            result: Some(pb::common::function_result::Result::JsError(
                                pb::common::JsError {
                                    message: Some(e.to_string()),
                                    custom_data: None,
                                    frames: None,
                                },
                            )),
                        }),
                        user_execution_time: None,
                        served_by_version: None,
                        log_lines,
                        final_tx: None,
                        http_response: None,
                    },
                };
                proto_resp.served_by_version = Some(self.registry_version.clone());
                Ok(Response::new(proto_resp))
            },
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
/// handler closed its transaction cleanly — the `FinalTxSummary`
/// the worker reports to the backend. The summary hops through the
/// native `ExecuteResponse` type on its way to the proto; keeping
/// the bundle native here means server.rs never touches the
/// wire-level `DistributedFinalTx` directly.
struct InlineDispatch {
    result: anyhow::Result<value::ConvexValue>,
    log_lines: Vec<String>,
    final_tx: Option<convex_native_core::distributed::FinalTxSummary>,
}

/// Open a fresh transaction at `begin_ts` (or `now_ts_for_reads()`
/// when the backend didn't supply one), run the native query, and
/// summarise the transaction's reads into a `FinalTxSummary`.
/// Queries are read-only by design so nothing ever commits — the
/// summary only matters for subscription tracking on the backend
/// side.
///
/// The summary fires regardless of whether the handler succeeded
/// or errored, as long as the tx was successfully opened. This
/// matches `CompositeFunctionRunner::dispatch_native_inner`'s
/// behaviour (substep 2.5) so distributed + in-process dispatch
/// produce the same `(result, final_tx)` tuple shape.
async fn run_query_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native_core::distributed::ExecuteRequest,
    begin_ts: Option<Timestamp>,
) -> InlineDispatch {
    run_udf_inline(native, database, req, begin_ts, UdfType::Query).await
}

/// Open a fresh transaction at `begin_ts` (or `now_ts_for_reads()`
/// when the backend didn't supply one), run the native mutation,
/// and summarise the transaction into a `FinalTxSummary`. **Phase
/// 1 removed the inline commit**: the worker no longer touches
/// `Database::commit_with_write_source`. The backend's Committer
/// consumes the returned summary (substep 2.6 wires this through
/// `impl FunctionRunner`). Summary-on-error matches
/// `CompositeFunctionRunner` (substep 2.5).
async fn run_mutation_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native_core::distributed::ExecuteRequest,
    begin_ts: Option<Timestamp>,
) -> InlineDispatch {
    run_udf_inline(native, database, req, begin_ts, UdfType::Mutation).await
}

/// Shared query/mutation dispatch body. Factored out in substep
/// 2.5 so the summary-on-error invariant only lives in one place.
async fn run_udf_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native_core::distributed::ExecuteRequest,
    begin_ts: Option<Timestamp>,
    udf_type: UdfType,
) -> InlineDispatch {
    use rand::RngCore;

    let log_buffer = convex_native_core::LogBuffer::new();
    // Seed the per-invocation `Observed` from the worker's runtime
    // RNG so the handler sees a fresh deterministic stream;
    // distributed dispatch carries the seed back in the response so
    // a re-execution can replay it (matches the in-process composite
    // runner's behaviour).
    let mut rng_seed = [0u8; 32];
    rand::rng().fill_bytes(&mut rng_seed);
    let observed: std::sync::Arc<convex_native_core::ctx::query::Observed> = std::sync::Arc::new(
        convex_native_core::ctx::query::Observed::from_seed(rng_seed),
    );
    let prepared = async {
        let ts = match begin_ts {
            Some(ts) => database.now_ts_for_reads().prior_ts(ts)?,
            None => database.now_ts_for_reads(),
        };
        let usage = FunctionUsageTracker::new();
        // Decode the caller's identity bytes if the backend sent
        // them. Empty bytes short-circuit to `Identity::system()`
        // so tests and internal paths that skip the identity
        // field keep the historic behaviour.
        let tx_identity = decode_identity_bytes(&req.identity)?;
        let mut tx = database.begin_with_ts(tx_identity, *ts, usage).await?;
        // Substep 2.4: replay writes the backend staged from
        // earlier UDFs in the same batched request so the handler
        // sees them. Empty on single-UDF calls (the common case).
        if !req.existing_writes.is_empty() {
            tx.merge_writes(req.existing_writes.clone())?;
        }
        anyhow::Ok(tx)
    }
    .await;
    let mut tx = match prepared {
        Ok(tx) => tx,
        Err(setup_err) => {
            // Transaction couldn't be opened — no tx to summarise,
            // no handler to run. Surface the setup error as the
            // handler result so it lands on the proto's
            // `result.js_error` channel.
            let log_lines = log_lines_to_pretty_strings(&log_buffer);
            return InlineDispatch {
                result: Err(setup_err),
                log_lines,
                final_tx: None,
            };
        },
    };
    let result = match udf_type {
        UdfType::Query => {
            native
                .run_query_with_log_buffer_and_observed(
                    &req.name,
                    &mut tx,
                    req.namespace,
                    req.args.clone(),
                    log_buffer.clone(),
                    observed.clone(),
                )
                .await
        },
        UdfType::Mutation => {
            native
                .run_mutation_with_log_buffer_and_observed(
                    &req.name,
                    &mut tx,
                    req.namespace,
                    req.args.clone(),
                    log_buffer.clone(),
                    observed.clone(),
                )
                .await
        },
        _ => unreachable!("run_udf_inline only handles Query/Mutation"),
    };
    let mut summary = summarise_tx(tx);
    summary.observed_identity = observed.identity();
    summary.observed_rng = observed.rng_observed();
    summary.observed_time = observed.unix_timestamp();
    summary.rng_seed = rng_seed;
    summary.unix_timestamp_nanos = if observed.unix_timestamp() {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_nanos()).ok())
            .unwrap_or(0)
    } else {
        0
    };
    let log_lines = log_lines_to_pretty_strings(&log_buffer);
    InlineDispatch {
        result,
        log_lines,
        final_tx: Some(summary),
    }
}

/// Decode the Phase-2 wire identity bytes into a
/// `keybroker::Identity`. Empty bytes map to `Identity::system()`
/// (the dispatcher-side encoder short-circuits
/// `Identity::System` to an empty vec for the common case).
/// Non-empty bytes decode through the
/// `pb::convex_identity::UncheckedIdentity` proto shape.
pub(crate) fn decode_identity_bytes(bytes: &[u8]) -> anyhow::Result<Identity> {
    if bytes.is_empty() {
        return Ok(Identity::system());
    }
    use prost::Message as _;
    let proto = pb::convex_identity::UncheckedIdentity::decode(bytes)
        .map_err(|e| anyhow::anyhow!("decoding identity bytes: {e}"))?;
    keybroker::Identity::from_proto_unchecked(proto)
        .map_err(|e| anyhow::anyhow!("identity proto → Identity: {e}"))
}

/// Drain a finished transaction into the wire summary.
///
/// After substep 2.1 the summary now carries `rows_read_by_tablet`
/// alongside the Phase-1 scalars — the backend's
/// `Transaction::apply_function_runner_tx` consumes that map in
/// the in-process path, and the distributed path has to provide
/// the same information for usage tracking to match. Substeps
/// 2.2 / 2.3 (see `convex-native/STATUS.md`) grow the rest of
/// the content the Committer needs.
///
/// Errors from `into_flat()` (nested transaction leftover) are
/// swallowed into a zero writes count — for native handlers the
/// tx is always flat at this point; a future invariant violation
/// would show up as the backend rejecting the response at commit
/// time under Phase 2 regardless.
fn summarise_tx(tx: database::Transaction<Rt>) -> convex_native_core::distributed::FinalTxSummary {
    let begin_timestamp: u64 = (*tx.begin_timestamp()).into();
    let rows_read_by_tablet = tx
        .stats_by_tablet()
        .iter()
        .map(|(tablet, stats)| (tablet.to_string(), stats.rows_read))
        .collect();
    let (reads, writes) = tx.into_reads_and_writes();
    let reads_count = reads.num_intervals() as u64;
    // Substep 2.2a: pull scalar read-size counters off the
    // TransactionReadSet before moving its interval-set into the
    // flat ReadSet. `usize → u64` is a lossless widening.
    let user_tx_size = convex_native_core::distributed::TxReadSize {
        total_document_size: reads.user_tx_size().total_document_size as u64,
        total_document_count: reads.user_tx_size().total_document_count as u64,
    };
    let system_tx_size = convex_native_core::distributed::TxReadSize {
        total_document_size: reads.system_tx_size().total_document_size as u64,
        total_document_count: reads.system_tx_size().total_document_count as u64,
    };
    // Substep 2.2b: drain the indexed read set into one
    // IndexReadsSummary per (tablet, index). Search-index reads
    // are intentionally skipped here — native handlers don't ship
    // a typed search surface yet.
    let read_set = reads.into_read_set();
    let (indexed, _search) = read_set.consume();
    let index_reads: Vec<convex_native_core::distributed::IndexReadsSummary> = indexed
        .map(|(index_name, index_reads)| {
            // `database::IndexReads` isn't re-exported; reach into
            // the `reads` module directly. `stack_traces` is debug-
            // only (collected under `READ_SET_CAPTURE_BACKTRACES`)
            // and not sent over the wire.
            let database::reads::IndexReads {
                fields,
                intervals,
                stack_traces: _,
            } = index_reads;
            convex_native_core::distributed::IndexReadsSummary {
                index_name,
                fields,
                intervals,
            }
        })
        .collect();
    let writes_vec: Vec<common::document::DocumentUpdateWithPrevTs> = writes
        .into_flat()
        .map(|flat| {
            flat.into_coalesced_writes()
                .map(std::sync::Arc::unwrap_or_clone)
                .collect()
        })
        .unwrap_or_default();
    convex_native_core::distributed::FinalTxSummary {
        begin_timestamp,
        writes_count: writes_vec.len() as u64,
        reads_count,
        rows_read_by_tablet,
        writes: writes_vec,
        user_tx_size: Some(user_tx_size),
        system_tx_size: Some(system_tx_size),
        index_reads,
        // Observed flags + rng_seed + unix_timestamp_nanos are
        // populated by the caller (`run_udf_inline`) once it can
        // drain the per-invocation `Observed` handle. Default
        // here so any pre-Observed callsite still compiles.
        observed_identity: false,
        observed_rng: false,
        observed_time: false,
        rng_seed: [0u8; 32],
        unix_timestamp_nanos: 0,
    }
}

/// Decode a wire-format `HttpActionRequest` into the typed
/// `convex_native_core::http::HttpRequest` shape the handler
/// receives. Method strings parse via `http::Method::from_bytes`;
/// header names + values parse via the standard `http` crate
/// types so invalid bytes surface loudly instead of being
/// silently truncated.
fn decode_http_request(
    req: &proto::HttpActionRequest,
) -> anyhow::Result<convex_native_core::http::HttpRequest> {
    use http::{
        HeaderMap,
        HeaderName,
        HeaderValue,
        Method,
    };
    let method = Method::from_bytes(req.method.as_bytes())
        .map_err(|e| anyhow::anyhow!("invalid HTTP method {:?}: {e}", req.method))?;
    let mut headers = HeaderMap::new();
    for h in &req.headers {
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|e| anyhow::anyhow!("invalid HTTP header name {:?}: {e}", h.name))?;
        let value = HeaderValue::from_str(&h.value)
            .map_err(|e| anyhow::anyhow!("invalid HTTP header value for {}: {e}", h.name))?;
        headers.append(name, value);
    }
    Ok(convex_native_core::http::HttpRequest {
        method,
        url: req.url.clone(),
        headers,
        body: bytes::Bytes::from(req.body.clone()),
        routed_path: req.routed_path.clone(),
    })
}

/// Encode a `convex_native_core::http::HttpResponse` into the
/// wire-format `HttpActionResponse`. Headers are flattened back
/// into `(name, value)` pairs.
fn encode_http_response(resp: convex_native_core::http::HttpResponse) -> proto::HttpActionResponse {
    let headers = resp
        .headers
        .iter()
        .map(|(name, value)| proto::HttpHeader {
            name: name.as_str().to_string(),
            value: value.to_str().unwrap_or_default().to_string(),
        })
        .collect();
    proto::HttpActionResponse {
        status: resp.status as u32,
        headers,
        body: resp.body.to_vec(),
    }
}

/// Snapshot the buffer and render each line as a plain string.
/// Matches the `repeated string log_lines` field on the proto: one
/// formatted line per entry, ready for the backend to forward
/// into its own log-streaming path without re-parsing.
fn log_lines_to_pretty_strings(buffer: &convex_native_core::LogBuffer) -> Vec<String> {
    buffer
        .snapshot()
        .into_iter()
        .map(|line| {
            let level = match line.level {
                convex_native_core::LogLevel::Debug => "DEBUG",
                convex_native_core::LogLevel::Info => "INFO",
                convex_native_core::LogLevel::Warn => "WARN",
                convex_native_core::LogLevel::Error => "ERROR",
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

    use convex_native_core::distributed::ExecuteRequest as NativeExecuteRequest;
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
            begin_timestamp: None,
            existing_writes: Vec::new(),
            http_request: None,
            identity: Vec::new(),
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
            begin_timestamp: None,
            existing_writes: Vec::new(),
            http_request: None,
            identity: Vec::new(),
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
            begin_timestamp: None,
            existing_writes: Vec::new(),
            http_request: None,
            identity: Vec::new(),
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
    async fn execute_http_action_without_http_request_is_invalid_argument() {
        // HTTP actions now dispatch through FunctionExecutionService
        // alongside regular actions (UdfType::HttpAction). A caller
        // forwarding an HttpAction UdfType must also populate
        // `ExecuteRequest.http_request`; missing it surfaces as
        // `InvalidArgument` so clients see the shape contract loudly.
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "http_handler".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
            begin_timestamp: None,
            existing_writes: Vec::new(),
            http_request: None,
            identity: Vec::new(),
        };
        let proto_req = conversions::to_proto_request(&native, UdfType::HttpAction).unwrap();
        let status = server.execute(Request::new(proto_req)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("http_request"),
            "error points at the missing field: {}",
            status.message(),
        );
    }
}
