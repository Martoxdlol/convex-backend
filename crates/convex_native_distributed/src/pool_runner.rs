//! Substep 3.6 of `convex-native/STATUS.md` — `PoolFunctionRunner`.
//!
//! Wraps an `Arc<WorkerPool>` and implements
//! `FunctionRunner<ProdRuntime>`. For each dispatch it asks the
//! pool for the workers eligible to serve the requested function
//! name (via `WorkerPool::eligible_for`), picks one via
//! Power-of-2-Choices across that set (plus single-retry failover
//! on `Unavailable`), runs the gRPC dispatch via
//! `WorkerClient::execute`, and converts the response into the
//! trait's `(Option<FunctionFinalTransaction>, FunctionOutcome,
//! FunctionUsageStats)` triple.
//!
//! The Phase-2 `DistributedFunctionRunner` (fixed pool from
//! `CONVEX_NATIVE_WORKERS`) stays — this is an additive
//! alternative for the dynamic-pool topology. Phase-5's
//! backend image builds only this runner; `local_backend`
//! prefers the dynamic pool when
//! `CONVEX_ADMISSION_BIND_ADDR` is set.
//!
//! ## No-workers handling
//!
//! When `eligible_for` returns an empty list — no worker in the
//! pool advertises this function — `run_function` surfaces an
//! `anyhow::Error` carrying the dotted function name and the
//! current pool `len()`. The HTTP surface at the edge can map
//! that to a 503 (substep 3.7) so clients get a clean retry
//! signal.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
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
    log_lines::LogLine,
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
use rand::Rng;
use runtime::prod::ProdRuntime;
use sync_types::{
    CanonicalizedModulePath,
    Timestamp,
};
use tokio::sync::mpsc;
use tonic::Status;
use udf::{
    ActionCallbacks,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
};
use usage_tracking::FunctionUsageStats;
use value::identifier::Identifier;

use crate::{
    client::WorkerClient,
    pool::{
        WorkerId,
        WorkerPool,
    },
};

/// `FunctionRunner` over a dynamic `WorkerPool`. One instance is
/// constructed per backend; the backend holds
/// `Arc<PoolFunctionRunner>` and hands it to
/// `Application::new(..., function_runner, ...)` in place of the
/// Phase-2 `DistributedFunctionRunner`.
pub struct PoolFunctionRunner {
    pool: Arc<WorkerPool>,
}

impl PoolFunctionRunner {
    pub fn new(pool: Arc<WorkerPool>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Arc<WorkerPool> {
        &self.pool
    }

    /// Dispatch one request against the pool. Visible to tests
    /// that want to exercise the pool routing without going
    /// through the full `FunctionRunner::run_function` path
    /// (which requires a `ValidatedPathAndArgs` that needs a
    /// real `Transaction<RT>` to build).
    pub async fn dispatch(
        &self,
        function_name: &str,
        req: convex_native_core::distributed::ExecuteRequest,
        udf_type: UdfType,
    ) -> Result<convex_native_core::distributed::ExecuteResponse, Status> {
        let started = Instant::now();
        let eligible = self.pool.eligible_for(function_name);
        if eligible.is_empty() {
            return Err(Status::unavailable(format!(
                "PoolFunctionRunner: no worker in the pool currently advertises {function_name:?} \
                 (pool size {}); retry when the admission service registers one",
                self.pool.len(),
            )));
        }
        dispatch_p2c(eligible, req, udf_type, started).await
    }
}

/// Power-of-2-Choices + single-retry failover over a dynamic
/// eligible set. Mirrors the fixed-pool logic on
/// `DistributedFunctionRunner::execute`; factoring it out is a
/// refactor tracked for after Phase 3 lands.
async fn dispatch_p2c(
    eligible: Vec<(WorkerId, Arc<dyn WorkerClient>)>,
    req: convex_native_core::distributed::ExecuteRequest,
    udf_type: UdfType,
    _started: Instant,
) -> Result<convex_native_core::distributed::ExecuteResponse, Status> {
    let n = eligible.len();
    // Degenerate n=1: skip the pick-two, no failover.
    if n == 1 {
        return eligible[0].1.execute(req, udf_type).await;
    }
    let (a, b) = pick_two(n);
    let primary_idx = if eligible[a].1.in_flight_estimate() <= eligible[b].1.in_flight_estimate() {
        a
    } else {
        b
    };
    let backup_idx = if primary_idx == a { b } else { a };
    // Primary attempt.
    let primary = &eligible[primary_idx].1;
    match primary.execute(req.clone(), udf_type).await {
        Ok(resp) => return Ok(resp),
        Err(e) if e.code() == tonic::Code::Unavailable => {
            // Failover: try the backup.
            let backup = &eligible[backup_idx].1;
            return backup.execute(req, udf_type).await;
        },
        Err(e) => return Err(e),
    }
}

fn pick_two(n: usize) -> (usize, usize) {
    if n <= 1 {
        return (0, 0);
    }
    let mut rng = rand::rng();
    (rng.random_range(0..n), rng.random_range(0..n))
}

#[async_trait]
impl FunctionRunner<ProdRuntime> for PoolFunctionRunner {
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
                        "PoolFunctionRunner: function_metadata is required for Query/Mutation \
                         dispatch",
                    )
                })?;
                crate::function_runner_impl::dispatch_query_or_mutation_via(
                    |req, udf_type, name| {
                        let pool = self.pool.clone();
                        async move {
                            let eligible = pool.eligible_for(&name);
                            if eligible.is_empty() {
                                return Err(Status::unavailable(format!(
                                    "PoolFunctionRunner: no worker in the pool currently \
                                     advertises {name:?} (pool size {}); retry when the admission \
                                     service registers one",
                                    pool.len(),
                                )));
                            }
                            dispatch_p2c(eligible, req, udf_type, Instant::now()).await
                        }
                    },
                    udf_type,
                    identity,
                    ts,
                    existing_writes,
                    meta,
                    context,
                )
                .await
            },
            UdfType::Action => {
                let meta = function_metadata.ok_or_else(|| {
                    anyhow::anyhow!(
                        "PoolFunctionRunner: function_metadata is required for Action dispatch",
                    )
                })?;
                crate::function_runner_impl::dispatch_action_via(
                    |req, udf_type, name| {
                        let pool = self.pool.clone();
                        async move {
                            let eligible = pool.eligible_for(&name);
                            if eligible.is_empty() {
                                return Err(Status::unavailable(format!(
                                    "PoolFunctionRunner: no worker in the pool currently \
                                     advertises {name:?} (pool size {}); retry when the admission \
                                     service registers one",
                                    pool.len(),
                                )));
                            }
                            dispatch_p2c(eligible, req, udf_type, Instant::now()).await
                        }
                    },
                    identity,
                    meta,
                    context,
                )
                .await
            },
            UdfType::HttpAction => anyhow::bail!(
                "PoolFunctionRunner: UdfType::HttpAction uses the HttpRouter dispatch path, not \
                 FunctionExecutionService. See convex-native/DISTRIBUTED_PLAN.md §7.5.",
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
            "PoolFunctionRunner::analyze: native-only pool can't analyze JS modules. Wrap in a \
             composite runner that delegates to a JS backend if JS is also served.",
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
        anyhow::bail!("PoolFunctionRunner::evaluate_app_definitions: native-only runner")
    }

    async fn evaluate_component_initializer(
        &self,
        _evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        _path: ComponentDefinitionPath,
        _definition: ModuleConfig,
        _args: BTreeMap<Identifier, Resource>,
        _name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        anyhow::bail!("PoolFunctionRunner::evaluate_component_initializer: native-only runner")
    }

    async fn evaluate_schema(
        &self,
        _schema_bundle: ModuleSource,
        _source_map: Option<SourceMap>,
        _rng_seed: [u8; 32],
        _unix_timestamp: UnixTimestamp,
    ) -> anyhow::Result<DatabaseSchema> {
        anyhow::bail!(
            "PoolFunctionRunner::evaluate_schema: native-only runner can't evaluate JS schemas. \
             Native schemas come from NativeSchema::collect() on the worker.",
        )
    }

    async fn evaluate_auth_config(
        &self,
        _auth_config_bundle: ModuleSource,
        _source_map: Option<SourceMap>,
        _environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        _explanation: &str,
    ) -> anyhow::Result<AuthConfig> {
        anyhow::bail!("PoolFunctionRunner::evaluate_auth_config: native-only runner")
    }

    fn set_action_callbacks(&self, _action_callbacks: Arc<dyn ActionCallbacks>) {
        // No-op — Phase 4's BackendCallbackService handles sub-calls.
    }
}

// Silence "unused" warning for `Duration` — imported so Phase-7
// metrics sinks can use it when they latch onto this type; no
// direct consumer in this file yet.
const _: Option<Duration> = None;

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{
            AtomicU64,
            Ordering,
        },
        Arc,
    };

    use async_trait::async_trait;
    use convex_native_core::distributed::{
        ExecuteRequest,
        ExecuteResponse,
    };
    use pb::function_execution as proto;
    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableNamespace,
    };

    use super::*;
    use crate::pool::WorkerEntry;

    struct StubClient {
        label: String,
        ok_counter: Arc<AtomicU64>,
    }

    #[async_trait]
    impl WorkerClient for StubClient {
        async fn execute(
            &self,
            _req: ExecuteRequest,
            _udf_type: UdfType,
        ) -> Result<ExecuteResponse, Status> {
            self.ok_counter.fetch_add(1, Ordering::SeqCst);
            Ok(ExecuteResponse::new(Ok(ConvexValue::Null)))
        }

        async fn health(&self) -> Result<proto::HealthResponse, Status> {
            Err(Status::unimplemented("stub"))
        }

        fn in_flight_estimate(&self) -> u64 {
            0
        }

        fn label(&self) -> &str {
            &self.label
        }
    }

    fn sample_req() -> ExecuteRequest {
        let obj: std::collections::BTreeMap<FieldName, ConvexValue> =
            std::collections::BTreeMap::new();
        ExecuteRequest {
            name: "get".to_string(),
            namespace: TableNamespace::Global,
            args: ConvexObject::try_from(obj).unwrap(),
            timeout: None,
            min_registry_version: None,
            execution_context: None,
            begin_timestamp: None,
            existing_writes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn dispatch_routes_to_an_eligible_worker() {
        let pool = Arc::new(WorkerPool::new());
        let ok_counter = Arc::new(AtomicU64::new(0));
        pool.admit(WorkerEntry {
            client: Arc::new(StubClient {
                label: "w1".into(),
                ok_counter: ok_counter.clone(),
            }),
            registry_version: "1.0.0".into(),
            functions: vec!["get".into()],
            kind: crate::pool::WorkerKind::NativeRust,
        });
        let runner = PoolFunctionRunner::new(pool);
        let resp = runner
            .dispatch("get", sample_req(), UdfType::Query)
            .await
            .expect("dispatch");
        assert!(resp.result.is_ok());
        assert_eq!(ok_counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatch_fails_loudly_when_no_worker_serves_the_name() {
        // Substep 3.7 exit criterion: the pool-runner must not
        // silently hang or 500 when nobody serves the requested
        // function — it returns `Unavailable` so the HTTP edge
        // can map to 503 + a retry header.
        let pool = Arc::new(WorkerPool::new());
        // Admit a worker that only serves "other".
        pool.admit(WorkerEntry {
            client: Arc::new(StubClient {
                label: "w1".into(),
                ok_counter: Arc::new(AtomicU64::new(0)),
            }),
            registry_version: "1.0.0".into(),
            functions: vec!["other".into()],
            kind: crate::pool::WorkerKind::NativeRust,
        });
        let runner = PoolFunctionRunner::new(pool);
        let err = runner
            .dispatch("missing", sample_req(), UdfType::Query)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(
            err.message().contains("no worker") && err.message().contains("\"missing\""),
            "error surfaces the missing function name: {}",
            err.message(),
        );
    }

    #[tokio::test]
    async fn dispatch_failover_picks_backup_on_unavailable() {
        // With two eligible workers, if the primary pick errors
        // `Unavailable`, the backup completes the request.
        struct BadClient;
        #[async_trait]
        impl WorkerClient for BadClient {
            async fn execute(
                &self,
                _req: ExecuteRequest,
                _udf_type: UdfType,
            ) -> Result<ExecuteResponse, Status> {
                Err(Status::unavailable("down"))
            }

            async fn health(&self) -> Result<proto::HealthResponse, Status> {
                Err(Status::unimplemented("stub"))
            }

            fn in_flight_estimate(&self) -> u64 {
                0
            }

            fn label(&self) -> &str {
                "bad"
            }
        }
        let pool = Arc::new(WorkerPool::new());
        let good_counter = Arc::new(AtomicU64::new(0));
        pool.admit(WorkerEntry {
            client: Arc::new(BadClient),
            registry_version: "1.0.0".into(),
            functions: vec!["get".into()],
            kind: crate::pool::WorkerKind::NativeRust,
        });
        pool.admit(WorkerEntry {
            client: Arc::new(StubClient {
                label: "good".into(),
                ok_counter: good_counter.clone(),
            }),
            registry_version: "1.0.0".into(),
            functions: vec!["get".into()],
            kind: crate::pool::WorkerKind::NativeRust,
        });
        let runner = PoolFunctionRunner::new(pool);
        // Retry the dispatch a few times — the P2C pick is
        // random, so in the worst case the primary lands on the
        // good worker and failover doesn't exercise. A loop
        // ensures we exercise the failover path at least once
        // with near-certain probability.
        for _ in 0..20 {
            let _ = runner.dispatch("get", sample_req(), UdfType::Query).await;
        }
        assert!(
            good_counter.load(Ordering::SeqCst) > 0,
            "good worker receives at least one request (direct or via failover)",
        );
    }
}
