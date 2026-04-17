//! `impl FunctionRunner<ProdRuntime>` for `DistributedFunctionRunner`
//! — substep 2.6b of `convex-native/DISTRIBUTED_PLAN.md`.
//!
//! After this impl lands, the `local_backend` Composite runner can
//! swap its native branch from the in-process `NativeFunctionRunner`
//! to a remote worker pool via the substep-2.7 env-var. The
//! backend's `ApplicationFunctionRunner` drives `run_function`
//! exactly as before; the worker pool takes the mutation, runs it
//! on a remote process, and returns a
//! `FinalTxSummary → FunctionFinalTransaction` the Committer
//! consumes unchanged.
//!
//! ## What's implemented
//!
//! - `run_function` for `UdfType::Query` / `UdfType::Mutation` — builds the
//!   native `ExecuteRequest` from the backend-side inputs, dispatches via the
//!   P2C client, converts the response into `(Option<FunctionFinalTransaction>,
//!   FunctionOutcome, FunctionUsageStats)` using the substep-2.6a helper and a
//!   minimal `UdfOutcome` builder.
//!
//! ## What's intentionally unimplemented
//!
//! - `run_function` for `UdfType::Action` — native actions still need a
//!   `BackendCallbackService` (Phase 4) so sub-calls route back to the
//!   backend's Committer. Returns a clear error.
//! - `run_function` for `UdfType::HttpAction` — HTTP actions use the
//!   `HttpRouter` dispatch path, not `FunctionExecutionService`.
//! - `analyze` / `evaluate_app_definitions` / `evaluate_component_initializer`
//!   / `evaluate_schema` / `evaluate_auth_config` — all JS-specific. The
//!   distributed runner is native-only; in the backend-image topology
//!   (`DISTRIBUTED_PLAN.md` Phase 5) these methods aren't reached because the
//!   backend ships without V8. In the meantime they return descriptive errors
//!   so the composite runner knows to delegate.
//! - `set_action_callbacks` — no-op. Native actions route sub-calls back to the
//!   backend via Phase 4's `BackendCallbackService`, not via these local
//!   callbacks.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use common::{
    auth::AuthConfig,
    bootstrap_model::components::definition::ComponentDefinitionMetadata,
    components::{
        ComponentDefinitionPath,
        ComponentName,
        Resource,
    },
    errors::JsError,
    execution_context::ExecutionContext,
    log_lines::{
        LogLine,
        LogLines,
    },
    query_journal::QueryJournal,
    runtime::UnixTimestamp,
    schemas::DatabaseSchema,
    types::{
        IndexId,
        RepeatableTimestamp,
        UdfType,
    },
};
use function_runner::{
    server::{
        FunctionMetadata,
        HttpActionMetadata,
    },
    FunctionFinalTransaction,
    FunctionRunner,
    FunctionWrites,
};
use keybroker::Identity;
use model::{
    config::types::ModuleConfig,
    environment_variables::types::{
        EnvVarName,
        EnvVarValue,
    },
    modules::module_versions::{
        AnalyzedModule,
        ModuleSource,
        SourceMap,
    },
    udf_config::types::UdfConfig,
};
use runtime::prod::ProdRuntime;
use sync_types::{
    CanonicalizedModulePath,
    Timestamp,
};
use tokio::sync::mpsc;
use udf::{
    ActionCallbacks,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
    SyscallTrace,
    UdfOutcome,
};
use usage_tracking::{
    FunctionUsageStats,
    FunctionUsageTracker,
};
use value::{
    identifier::Identifier,
    serialized_args_ext::SerializedArgsExt,
    ConvexValue,
    JsonPackedValue,
    TableNamespace,
};

use crate::{
    client::DistributedFunctionRunner,
    conversions,
};

#[async_trait]
impl FunctionRunner<ProdRuntime> for DistributedFunctionRunner {
    async fn run_function(
        &self,
        udf_type: UdfType,
        identity: Identity,
        ts: RepeatableTimestamp,
        existing_writes: FunctionWrites,
        _log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
        function_metadata: Option<FunctionMetadata>,
        _http_action_metadata: Option<HttpActionMetadata>,
        _default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        _in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
        context: ExecutionContext,
    ) -> anyhow::Result<(
        Option<FunctionFinalTransaction>,
        FunctionOutcome,
        FunctionUsageStats,
    )> {
        match udf_type {
            UdfType::Query | UdfType::Mutation => {
                let meta = function_metadata.ok_or_else(|| {
                    anyhow::anyhow!(
                        "DistributedFunctionRunner: function_metadata is required for \
                         Query/Mutation dispatch",
                    )
                })?;
                dispatch_query_or_mutation(
                    self,
                    udf_type,
                    identity,
                    ts,
                    existing_writes,
                    meta,
                    context,
                )
                .await
            },
            UdfType::Action => anyhow::bail!(
                "DistributedFunctionRunner: UdfType::Action dispatch is not yet implemented — \
                 native actions need Phase 4's BackendCallbackService so sub-calls route back to \
                 the backend's Committer. See convex-native/DISTRIBUTED_PLAN.md §7.4.",
            ),
            UdfType::HttpAction => anyhow::bail!(
                "DistributedFunctionRunner: UdfType::HttpAction uses the HttpRouter dispatch \
                 path, not FunctionExecutionService. See convex-native/DISTRIBUTED_PLAN.md §7.5.",
            ),
        }
    }

    async fn analyze(
        &self,
        _udf_config: UdfConfig,
        _modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        _environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        _max_user_heap_size: usize,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>> {
        anyhow::bail!(
            "DistributedFunctionRunner::analyze: native-only runner can't analyze JS modules. \
             Wrap a JS runner (via CompositeFunctionRunner) or ship a JS-only backend in a \
             distributed-native-only deployment (DISTRIBUTED_PLAN.md Phase 5 topology).",
        )
    }

    async fn evaluate_app_definitions(
        &self,
        _app_definition: ModuleConfig,
        _component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        _dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        _user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        _system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        anyhow::bail!(
            "DistributedFunctionRunner::evaluate_app_definitions: native-only runner can't \
             evaluate JS app definitions. Use a composite runner that wraps a JS backend.",
        )
    }

    async fn evaluate_component_initializer(
        &self,
        _evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        _path: ComponentDefinitionPath,
        _definition: ModuleConfig,
        _args: BTreeMap<Identifier, Resource>,
        _name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        anyhow::bail!(
            "DistributedFunctionRunner::evaluate_component_initializer: native-only runner can't \
             evaluate JS component initializers.",
        )
    }

    async fn evaluate_schema(
        &self,
        _schema_bundle: ModuleSource,
        _source_map: Option<SourceMap>,
        _rng_seed: [u8; 32],
        _unix_timestamp: UnixTimestamp,
    ) -> anyhow::Result<DatabaseSchema> {
        anyhow::bail!(
            "DistributedFunctionRunner::evaluate_schema: native-only runner can't evaluate JS \
             schemas. Native schemas come from `NativeSchema::collect()` on the worker; pair that \
             with a JS-capable runner (CompositeFunctionRunner) if JS schema.ts is also present.",
        )
    }

    async fn evaluate_auth_config(
        &self,
        _auth_config_bundle: ModuleSource,
        _source_map: Option<SourceMap>,
        _environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        _explanation: &str,
    ) -> anyhow::Result<AuthConfig> {
        anyhow::bail!(
            "DistributedFunctionRunner::evaluate_auth_config: native-only runner can't evaluate \
             JS auth configs.",
        )
    }

    fn set_action_callbacks(&self, _action_callbacks: Arc<dyn ActionCallbacks>) {
        // Distributed actions route sub-calls back to the backend
        // via Phase 4's BackendCallbackService — not via this
        // per-runner callbacks handle. Deliberately a no-op.
    }
}

/// Dispatch a native query or mutation over gRPC and assemble the
/// backend-consumable outcome triple.
async fn dispatch_query_or_mutation(
    runner: &DistributedFunctionRunner,
    udf_type: UdfType,
    identity: Identity,
    ts: RepeatableTimestamp,
    existing_writes: FunctionWrites,
    meta: FunctionMetadata,
    context: ExecutionContext,
) -> anyhow::Result<(
    Option<FunctionFinalTransaction>,
    FunctionOutcome,
    FunctionUsageStats,
)> {
    let started = Instant::now();
    let (path, arguments, udf_server_version) = meta.path_and_args.clone().consume();
    let function_name = path.udf_path.function_name().to_string();
    let inert_identity = identity.into();

    // Single-object native args encoding. `SerializedArgs` carries
    // a JSON array; native handlers always take one object. The
    // worker decodes with the same contract via
    // `conversions::decode_args`.
    let args_obj = {
        let raw_args = arguments.clone().into_args()?;
        let first = raw_args.into_iter().next().ok_or_else(|| {
            anyhow::anyhow!("DistributedFunctionRunner: {function_name}: missing args object")
        })?;
        let cv: ConvexValue = first.try_into()?;
        match cv {
            ConvexValue::Object(obj) => obj,
            _ => anyhow::bail!(
                "DistributedFunctionRunner: {function_name}: args must be a single object",
            ),
        }
    };

    let begin_timestamp_u64: u64 = (*ts).into();
    let exec_req = convex_native::distributed::ExecuteRequest {
        name: function_name.clone(),
        namespace: TableNamespace::Global,
        args: args_obj,
        timeout: None,
        min_registry_version: None,
        execution_context: Some(context.clone()),
        begin_timestamp: Some(begin_timestamp_u64),
        existing_writes: existing_writes.updates,
    };

    let response = runner.execute(exec_req, udf_type).await.map_err(|status| {
        anyhow::anyhow!(
            "DistributedFunctionRunner::run_function: gRPC dispatch of {function_name} failed: \
             {status}",
        )
    })?;

    let duration = started.elapsed();

    // Build the backend-consumable FunctionFinalTransaction from
    // the worker's summary (substep 2.6a).
    let final_tx = match response.final_tx {
        Some(summary) => Some(conversions::final_tx_summary_to_function_tx(summary)?),
        None => None,
    };

    // Convert the handler's result into `Result<JsonPackedValue,
    // JsError>` the way `CompositeFunctionRunner` does.
    let result = match response.result {
        Ok(v) => Ok(JsonPackedValue::pack(v)),
        Err(msg) => Err(JsError::from_message(msg)),
    };

    // Build a minimal UdfOutcome. Several fields (observed_*, rng)
    // are not carried over the wire yet — defaults here match the
    // in-process path when the handler doesn't touch them. When
    // Phase 4 or a follow-up extends the proto to carry observed
    // flags + rng seed, this constructor grows.
    let log_lines = render_log_lines(response.log_lines);
    let outcome = UdfOutcome {
        path: path.for_logging(),
        arguments,
        identity: inert_identity,
        observed_identity: false,
        rng_seed: [0u8; 32],
        observed_rng: false,
        unix_timestamp: UnixTimestamp::from_nanos(0),
        observed_time: false,
        log_lines,
        audit_log_lines: vec![].into(),
        journal: QueryJournal::new(),
        result,
        syscall_trace: SyscallTrace::new(),
        udf_server_version,
        memory_in_mb: 0,
        user_execution_time: Some(duration),
    };
    let wrapped = match udf_type {
        UdfType::Query => FunctionOutcome::Query(outcome),
        UdfType::Mutation => FunctionOutcome::Mutation(outcome),
        _ => unreachable!("dispatch_query_or_mutation only handles Query/Mutation"),
    };

    // Usage stats — currently empty because the distributed path
    // doesn't accumulate syscall-level usage on the client side.
    // The `rows_read_by_tablet` on `final_tx` carries the
    // table-scoped read counts; per-call syscall usage lands in a
    // follow-up substep.
    let usage_stats = FunctionUsageTracker::new().gather_user_stats();

    Ok((final_tx, wrapped, usage_stats))
}

/// Parse the worker's `"[LEVEL] message"`-shaped lines back into
/// `LogLine` structured form the backend's log-streaming path
/// consumes. Unrecognised prefixes fall through as developer logs
/// at `Info` level.
///
/// The timestamp is taken from `SystemTime::now()` rather than a
/// `Runtime::unix_timestamp()` handle because this path runs on
/// the backend side after the worker has already produced its log
/// output; wall-clock time is close enough for log ordering, and
/// avoids threading a `ProdRuntime` through every call site.
fn render_log_lines(lines: Vec<String>) -> LogLines {
    let now = now_unix_timestamp();
    let parsed: Vec<LogLine> = lines
        .into_iter()
        .map(|line| {
            let (level, message) = parse_level_prefix(&line);
            LogLine::new_developer_log_line(level, vec![message], now)
        })
        .collect();
    parsed.into()
}

fn now_unix_timestamp() -> UnixTimestamp {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    UnixTimestamp::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::MockWorkerClient;

    fn empty_runner() -> DistributedFunctionRunner {
        let worker = MockWorkerClient::new("test-worker", 0);
        DistributedFunctionRunner::new(vec![worker]).unwrap()
    }

    fn test_execution_context() -> ExecutionContext {
        use common::execution_context::{
            ExecutionContext,
            ExecutionId,
            RequestId,
        };
        ExecutionContext::new_from_parts(RequestId::new(), ExecutionId::new(), None, true)
    }

    async fn assert_run_function_err(udf_type: UdfType, needle: &str) {
        let runner = empty_runner();
        let result = runner
            .run_function(
                udf_type,
                Identity::system(),
                RepeatableTimestamp::MIN,
                FunctionWrites { updates: vec![] },
                None,
                None,
                None,
                BTreeMap::new(),
                BTreeMap::new(),
                test_execution_context(),
            )
            .await;
        let err = match result {
            Ok(_) => panic!("expected {udf_type:?} dispatch to error"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains(needle),
            "error must guide the operator toward the right plan step; expected {needle:?} in \
             {msg:?}",
        );
    }

    #[tokio::test]
    async fn run_function_action_returns_phase_4_guidance() {
        // `UdfType::Action` on the distributed runner isn't wired
        // until Phase 4's BackendCallbackService. The error must
        // point the operator at that plan step.
        assert_run_function_err(UdfType::Action, "Phase 4").await;
    }

    #[tokio::test]
    async fn run_function_http_action_points_at_http_router() {
        // HttpAction uses a different dispatch path (HttpRouter);
        // the error steers operators away from trying to wire
        // HttpActions through FunctionExecutionService.
        assert_run_function_err(UdfType::HttpAction, "HttpRouter").await;
    }

    #[tokio::test]
    async fn run_function_query_without_metadata_errors_explicitly() {
        // `run_function` requires `function_metadata` for
        // Query/Mutation so it can extract the canonical path +
        // args. Missing metadata is a programmer error (the
        // caller forgot to supply them) — the error must name
        // the missing argument so the bug is easy to diagnose.
        assert_run_function_err(UdfType::Query, "function_metadata").await;
    }

    #[tokio::test]
    async fn evaluate_schema_returns_clear_error() {
        // The distributed runner is native-only; its JS-focused
        // methods must return descriptive errors so the operator
        // knows to wrap it in a composite runner (or to pair it
        // with a JS-capable backend) instead of trying to evaluate
        // JS directly on the distributed path. `evaluate_schema`
        // is the cheapest to stand up in a test (fewest required
        // arg types), so exercise the "guidance error" contract
        // through that entry point.
        let worker = MockWorkerClient::new("w", 0);
        let runner = DistributedFunctionRunner::new(vec![worker]).unwrap();
        let err = runner
            .evaluate_schema(
                ModuleSource::new("return {};"),
                None,
                [0u8; 32],
                UnixTimestamp::from_nanos(0),
            )
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("native-only runner") && msg.contains("NativeSchema::collect"),
            "error message guides the operator toward the right fix: {msg}",
        );
    }

    // `set_action_callbacks` is tested via its compile-time
    // existence on the trait impl above — building an actual
    // `udf::ActionCallbacks` stub would require implementing a
    // dozen-method trait with no observable side effect here.
    // The substep 2.6b doc comment on `set_action_callbacks`
    // documents the no-op semantics.
}

fn parse_level_prefix(line: &str) -> (common::log_lines::LogLevel, String) {
    use common::log_lines::LogLevel;
    let trimmed = line.trim_start();
    if let Some(rest) = trimmed.strip_prefix("[DEBUG] ") {
        (LogLevel::Debug, rest.to_string())
    } else if let Some(rest) = trimmed.strip_prefix("[INFO] ") {
        (LogLevel::Info, rest.to_string())
    } else if let Some(rest) = trimmed.strip_prefix("[WARN] ") {
        (LogLevel::Warn, rest.to_string())
    } else if let Some(rest) = trimmed.strip_prefix("[ERROR] ") {
        (LogLevel::Error, rest.to_string())
    } else {
        (LogLevel::Info, line.to_string())
    }
}
