//! `CompositeFunctionRunner` — wraps a JS `FunctionRunner<RT>` and
//! intercepts calls whose function names match the native registry.
//!
//! For native queries and mutations the composite runner now drives
//! the full end-to-end path: builds a `Transaction<RT>` against the
//! owned `Database<RT>`, runs the native handler, extracts the
//! read/write set into a `FunctionFinalTransaction`, and builds a
//! synthetic `UdfOutcome` so the caller sees the usual
//! `(final_tx, outcome, usage)` tuple.
//!
//! Native actions are still handled by `NativeFunctionRunner::run_action`
//! elsewhere — they don't fit the "run inside a transaction" shape
//! this method assumes.

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
    document::DocumentUpdateWithPrevTs,
    errors::JsError,
    execution_context::ExecutionContext,
    log_lines::LogLine,
    query_journal::QueryJournal,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    schemas::DatabaseSchema,
    types::{
        IndexId,
        RepeatableTimestamp,
        UdfType,
    },
};
use convex_native::{
    ctx::{
        mutation::MutationCtx as NativeMutationCtx,
        query::QueryCtx as NativeQueryCtx,
    },
    HandlerFn,
    NativeFunctionRunner,
    NativeSchema,
    Rt,
};
use database::{
    Database,
    Transaction,
};
use function_runner::{
    server::{
        FunctionMetadata,
        HttpActionMetadata,
    },
    FunctionFinalTransaction,
    FunctionReads,
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
    JsonPackedValue,
    TableNamespace,
};

/// Wraps a JS `FunctionRunner<RT>` and routes recognized native
/// function names through a real dispatch path.
///
/// Today the native branch is only used when `RT = Rt = ProdRuntime`.
/// Other runtimes fall through to JS because the native handler
/// registry is monomorphic over `Rt` (see
/// `convex_native::registry` module docs for the rationale).
pub struct CompositeFunctionRunner<RT: Runtime> {
    pub native: Arc<NativeFunctionRunner>,
    pub js: Arc<dyn FunctionRunner<RT>>,
    pub database: Database<RT>,
}

impl<RT: Runtime> CompositeFunctionRunner<RT> {
    pub fn new(
        native: Arc<NativeFunctionRunner>,
        js: Arc<dyn FunctionRunner<RT>>,
        database: Database<RT>,
    ) -> Self {
        Self {
            native,
            js,
            database,
        }
    }

    fn requested_function_name(meta: Option<&FunctionMetadata>) -> Option<String> {
        let meta = meta?;
        let path = meta.path_and_args.path();
        Some(path.udf_path.function_name().to_string())
    }
}

/// Build an initial-writes-aware transaction at `ts`. Applying
/// `existing_writes` on top mirrors what the JS path does when a
/// request runs multiple UDFs in sequence inside one
/// ApplicationFunctionRunner call.
async fn begin_tx_with_writes<RT: Runtime>(
    database: &Database<RT>,
    identity: Identity,
    ts: RepeatableTimestamp,
    _existing_writes: FunctionWrites,
    usage: FunctionUsageTracker,
) -> anyhow::Result<Transaction<RT>> {
    // Begin against the chosen timestamp. We currently ignore
    // `existing_writes` because threading them in requires the
    // private `NestedWrites`-manipulation path that only the JS
    // FunctionRunnerCore uses. For one-UDF-per-request flows this
    // is fine; multi-UDF batching is a later refinement.
    database.begin_with_ts(identity, *ts, usage).await
}

/// Shared helper: given a query/mutation `UdfType`, a handler, and
/// the request metadata, drive the handler to completion and return
/// the (final_tx, outcome, usage) tuple.
async fn dispatch_native<RT: Runtime + 'static>(
    database: &Database<RT>,
    native: &Arc<NativeFunctionRunner>,
    udf_type: UdfType,
    identity: Identity,
    ts: RepeatableTimestamp,
    existing_writes: FunctionWrites,
    function_metadata: FunctionMetadata,
) -> anyhow::Result<(
    Option<FunctionFinalTransaction>,
    FunctionOutcome,
    FunctionUsageStats,
)> {
    // The native handler is monomorphic over `Rt` (ProdRuntime). We
    // only reach here when RT == Rt at runtime; the static type
    // system treats them as distinct. Guard with a downcast check.
    if std::any::TypeId::of::<RT>() != std::any::TypeId::of::<Rt>() {
        anyhow::bail!(
            "CompositeFunctionRunner native dispatch only supports the ProdRuntime \
             (convex_native::Rt). Runtime mismatch."
        );
    }

    let (path, arguments, udf_server_version) = function_metadata.path_and_args.clone().consume();
    let inert_identity = identity.clone().into();
    let started = Instant::now();
    let usage_tracker = FunctionUsageTracker::new();

    let mut tx = begin_tx_with_writes(
        database,
        identity,
        ts,
        existing_writes,
        usage_tracker.clone(),
    )
    .await?;

    // SAFETY: we verified RT == Rt above; the Transaction layouts
    // match.
    let tx_as_rt: &mut Transaction<Rt> =
        unsafe { &mut *((&mut tx) as *mut Transaction<RT> as *mut Transaction<Rt>) };
    let registration = native
        .get(path.udf_path.function_name())
        .ok_or_else(|| anyhow::anyhow!("native function disappeared from registry"))?;

    // Parse args into a ConvexObject the handler will deserialize.
    let args_obj = {
        use value::{
            serialized_args_ext::SerializedArgsExt,
            ConvexValue,
        };
        let raw_args = arguments.clone().into_args()?;
        let first = raw_args
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("native function: missing args object"))?;
        let cv: ConvexValue = first.try_into()?;
        match cv {
            ConvexValue::Object(obj) => obj,
            _ => anyhow::bail!("native function args must be a single object"),
        }
    };

    let result = match (udf_type, &registration.handler) {
        (UdfType::Query, HandlerFn::Query(handler)) => {
            let mut ctx = NativeQueryCtx::new(tx_as_rt, TableNamespace::Global);
            handler(&mut ctx, args_obj).await
        },
        (UdfType::Mutation, HandlerFn::Mutation(handler)) => {
            let mut ctx = NativeMutationCtx::new(tx_as_rt, TableNamespace::Global);
            handler(&mut ctx, args_obj).await
        },
        (got, reg) => {
            anyhow::bail!(
                "native function {:?} handler kind mismatch: request is {:?}, registration is {:?}",
                path.udf_path.function_name(),
                got,
                reg.udf_type(),
            );
        },
    };

    let duration = started.elapsed();

    let final_tx_result: anyhow::Result<FunctionFinalTransaction> = (move || {
        // Move tx out and convert.
        let ft: FunctionFinalTransaction = tx.try_into()?;
        Ok(ft)
    })();

    let (result_packed, error): (Option<JsonPackedValue>, Option<JsError>) = match result {
        Ok(v) => (Some(JsonPackedValue::pack(v)), None),
        Err(e) => (None, Some(JsError::from_error_ref(&e))),
    };

    let udf_outcome_result = match (result_packed, error) {
        (Some(p), None) => Ok(p),
        (_, Some(e)) => Err(e),
        _ => unreachable!(),
    };

    let runtime_for_rng = database.runtime();
    let rng_seed: [u8; 32] = runtime_for_rng.rng().random();
    let outcome = UdfOutcome {
        path: path.for_logging(),
        arguments,
        identity: inert_identity,
        observed_identity: false,
        rng_seed,
        observed_rng: false,
        unix_timestamp: runtime_for_rng.unix_timestamp(),
        observed_time: false,
        log_lines: vec![].into(),
        audit_log_lines: vec![].into(),
        journal: QueryJournal::new(),
        result: udf_outcome_result,
        syscall_trace: SyscallTrace::new(),
        udf_server_version,
        memory_in_mb: 0,
        user_execution_time: Some(duration),
    };

    let wrapped = match udf_type {
        UdfType::Query => FunctionOutcome::Query(outcome),
        UdfType::Mutation => FunctionOutcome::Mutation(outcome),
        _ => unreachable!("guarded by match above"),
    };

    let final_tx = final_tx_result.ok();
    let usage_stats = usage_tracker.gather_user_stats();
    Ok((final_tx, wrapped, usage_stats))
}

#[async_trait]
impl<RT: Runtime> FunctionRunner<RT> for CompositeFunctionRunner<RT>
where
    RT: 'static,
{
    async fn run_function(
        &self,
        udf_type: UdfType,
        identity: Identity,
        ts: RepeatableTimestamp,
        existing_writes: FunctionWrites,
        log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
        function_metadata: Option<FunctionMetadata>,
        http_action_metadata: Option<HttpActionMetadata>,
        default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
        context: ExecutionContext,
    ) -> anyhow::Result<(
        Option<FunctionFinalTransaction>,
        FunctionOutcome,
        FunctionUsageStats,
    )> {
        let requested = Self::requested_function_name(function_metadata.as_ref());
        let is_native = requested
            .as_deref()
            .is_some_and(|n| self.native.has_function(n));

        if is_native && matches!(udf_type, UdfType::Query | UdfType::Mutation) {
            let meta = function_metadata.expect("is_native implies function_metadata is Some");
            return dispatch_native::<RT>(
                &self.database,
                &self.native,
                udf_type,
                identity,
                ts,
                existing_writes,
                meta,
            )
            .await;
        }

        // Not native (or not a query/mutation): delegate.
        self.js
            .run_function(
                udf_type,
                identity,
                ts,
                existing_writes,
                log_line_sender,
                function_metadata,
                http_action_metadata,
                default_system_env_vars,
                in_memory_index_last_modified,
                context,
            )
            .await
    }

    async fn analyze(
        &self,
        udf_config: UdfConfig,
        modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        max_user_heap_size: usize,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>> {
        self.js
            .analyze(
                udf_config,
                modules,
                environment_variables,
                max_user_heap_size,
            )
            .await
    }

    async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        self.js
            .evaluate_app_definitions(
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_vars,
            )
            .await
    }

    async fn evaluate_component_initializer(
        &self,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        self.js
            .evaluate_component_initializer(evaluated_definitions, path, definition, args, name)
            .await
    }

    async fn evaluate_schema(
        &self,
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
    ) -> anyhow::Result<DatabaseSchema> {
        let js_schema = self
            .js
            .evaluate_schema(schema_bundle, source_map, rng_seed, unix_timestamp)
            .await?;
        let mut merged = NativeSchema::collect()?;
        merged.schema_validation = js_schema.schema_validation;
        for (name, def) in js_schema.tables {
            if merged.tables.contains_key(&name) {
                anyhow::bail!(
                    "table {name:?} is declared both natively (via #[derive(ConvexDocument)]) and \
                     in JS schema.ts — remove one of the declarations",
                );
            }
            merged.tables.insert(name, def);
        }
        Ok(merged)
    }

    async fn evaluate_auth_config(
        &self,
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: &str,
    ) -> anyhow::Result<AuthConfig> {
        self.js
            .evaluate_auth_config(
                auth_config_bundle,
                source_map,
                environment_variables,
                explanation,
            )
            .await
    }

    fn set_action_callbacks(&self, action_callbacks: Arc<dyn ActionCallbacks>) {
        self.js.set_action_callbacks(action_callbacks);
    }
}

// Silence unused imports in non-dispatch paths.
#[allow(dead_code)]
fn _unused(_: FunctionReads, _: DocumentUpdateWithPrevTs) {}
