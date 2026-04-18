//! `CompositeFunctionRunner` — wraps a JS `FunctionRunner<RT>` and
//! intercepts calls whose function names match the native registry.
//!
//! For native queries and mutations the composite runner drives the
//! full end-to-end path: builds a `Transaction<RT>` against the
//! owned `Database<RT>`, runs the native handler, extracts the
//! read/write set into a `FunctionFinalTransaction`, and builds a
//! synthetic `UdfOutcome` so the caller sees the usual
//! `(final_tx, outcome, usage)` tuple.
//!
//! For native actions the composite dispatches through
//! `NativeFunctionRunner::run_action_with_callbacks`, wrapping the
//! cached `Arc<dyn ActionCallbacks>` in a `BackendCallbacks`. Actions
//! don't take a `Transaction`, so `final_tx` is always `None` in the
//! returned tuple.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::{
        Arc,
        Weak,
    },
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
use convex_native_core::{
    ctx::{
        mutation::MutationCtx as NativeMutationCtx,
        query::{
            Observed,
            QueryCtx as NativeQueryCtx,
        },
    },
    HandlerFn,
    LogBuffer,
    LogLevel as NativeLogLevel,
    NativeFunctionRunner,
    NativeLogLine,
    NativeSchema,
    Rt,
};
use database::{
    Database,
    Transaction,
};
use file_storage::FileStorage;
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
use parking_lot::RwLock;
use rand::Rng;
use sync_types::{
    types::SerializedArgs,
    CanonicalizedModulePath,
    Timestamp,
};
use tokio::sync::mpsc;
use udf::{
    ActionCallbacks,
    ActionOutcome,
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
    ConvexObject,
    ConvexValue,
    JsonPackedValue,
    TableNamespace,
};

use crate::callbacks_adapter::BackendCallbacks;

/// Wraps a JS `FunctionRunner<RT>` and routes recognized native
/// function names through a real dispatch path.
///
/// Today the native branch is only used when `RT = Rt = ProdRuntime`.
/// Other runtimes fall through to JS because the native handler
/// registry is monomorphic over `Rt` (see
/// `convex_native_core::registry` module docs for the rationale).
pub struct CompositeFunctionRunner<RT: Runtime> {
    pub native: Arc<NativeFunctionRunner>,
    pub js: Arc<dyn FunctionRunner<RT>>,
    pub database: Database<RT>,
    /// Optional file-storage handle. When set, native actions can
    /// upload raw bytes via `ctx.storage().store(...)`; when `None`,
    /// `storage_store` errors out at dispatch time.
    pub file_storage: Option<FileStorage<RT>>,
    /// Cached `set_action_callbacks` sink. Stored as `Weak` to match the
    /// JS-side `InProcessFunctionRunner` pattern and avoid a reference
    /// cycle with `ApplicationFunctionRunner`. Used to construct a
    /// `BackendCallbacks` when dispatching native actions.
    action_callbacks: Arc<RwLock<Option<Weak<dyn ActionCallbacks>>>>,
    /// Substep 2.7 of `convex-native/DISTRIBUTED_PLAN.md` (env-var
    /// switchover). When set, native Query/Mutation dispatch
    /// routes through this `FunctionRunner<RT>` — typically a
    /// `DistributedFunctionRunner` talking to a pool of remote
    /// workers over gRPC — instead of running against the
    /// in-process `Database<RT>`. Actions still run locally under
    /// Phase 2 (Phase 4 lands the
    /// `BackendCallbackService` so they can be routed too).
    /// When `None`, the composite behaves exactly as before —
    /// native dispatch runs in-process against the owned database.
    remote_native: Option<Arc<dyn FunctionRunner<RT>>>,
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
            file_storage: None,
            action_callbacks: Arc::new(RwLock::new(None)),
            remote_native: None,
        }
    }

    /// Attach a `FileStorage` so native actions can upload raw bytes
    /// via `ctx.storage().store(...)`. Without this, storage calls
    /// from native actions return an error.
    pub fn with_file_storage(mut self, file_storage: FileStorage<RT>) -> Self {
        self.file_storage = Some(file_storage);
        self
    }

    /// Route native Query/Mutation dispatch through `remote`
    /// instead of running against the in-process database.
    /// Substep 2.7 of `convex-native/DISTRIBUTED_PLAN.md`. See
    /// `remote_native` field doc for semantics.
    pub fn with_remote_native_pool(mut self, remote: Arc<dyn FunctionRunner<RT>>) -> Self {
        self.remote_native = Some(remote);
        self
    }

    fn requested_function_name(meta: Option<&FunctionMetadata>) -> Option<String> {
        let meta = meta?;
        let path = meta.path_and_args.path();
        Some(path.udf_path.function_name().to_string())
    }

    /// Resolve the strong `Arc<dyn ActionCallbacks>` from the stored
    /// weak reference. Returns `None` if `set_action_callbacks` hasn't
    /// been called yet or the action runner has been dropped.
    fn resolve_action_callbacks(&self) -> Option<Arc<dyn ActionCallbacks>> {
        self.action_callbacks
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
    }
}

/// Decode the first entry of `SerializedArgs` into a `ConvexObject`.
///
/// Native handlers accept exactly one positional argument — a single
/// object — whereas `SerializedArgs` carries a JSON-encoded array.
/// We pull out the first element, convert it to a `ConvexValue`, and
/// require it to be an object. Used by both the query/mutation and
/// action dispatch paths; `label` is baked into the error message so
/// callers can say "native function" vs "native action".
fn extract_single_object_arg(
    arguments: &SerializedArgs,
    label: &str,
) -> anyhow::Result<ConvexObject> {
    let raw_args = arguments.clone().into_args()?;
    let first = raw_args
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("{label}: missing args object"))?;
    let cv: ConvexValue = first.try_into()?;
    match cv {
        ConvexValue::Object(obj) => Ok(obj),
        _ => anyhow::bail!("{label} args must be a single object"),
    }
}

/// Build an initial-writes-aware transaction at `ts`. Applying
/// `existing_writes` on top mirrors what the JS path does when a
/// request runs multiple UDFs in sequence inside one
/// ApplicationFunctionRunner call (see
/// `function_runner::in_memory_indexes::begin_tx` which calls
/// `tx.merge_writes(existing_writes.updates)` after construction).
async fn begin_tx_with_writes<RT: Runtime>(
    database: &Database<RT>,
    identity: Identity,
    ts: RepeatableTimestamp,
    existing_writes: FunctionWrites,
    usage: FunctionUsageTracker,
) -> anyhow::Result<Transaction<RT>> {
    let mut tx = database.begin_with_ts(identity, *ts, usage).await?;
    if !existing_writes.updates.is_empty() {
        tx.merge_writes(existing_writes.updates)?;
    }
    Ok(tx)
}

/// Shared helper: given a query/mutation `UdfType`, a handler, and
/// the request metadata, drive the handler to completion and return
/// the (final_tx, outcome, usage) tuple.
/// Convert a native `LogBuffer` snapshot into the `common`
/// `LogLine` shape the rest of the backend (log streaming, persistence
/// of action logs) consumes. Level mapping is 1:1; messages wrap into
/// a single-element `Vec<String>` since `NativeLogLine` carries one
/// string, while `LogLineStructured` supports the JS idiom of
/// `console.log(a, b, c)` with multiple messages.
fn drain_log_buffer(buffer: &LogBuffer, now: UnixTimestamp) -> common::log_lines::LogLines {
    let lines = buffer.snapshot();
    let out: Vec<LogLine> = lines
        .into_iter()
        .map(|line| native_line_to_log_line(line, now))
        .collect();
    out.into()
}

fn native_line_to_log_line(line: NativeLogLine, now: UnixTimestamp) -> LogLine {
    let NativeLogLine { level, message } = line;
    let mapped = match level {
        NativeLogLevel::Debug => common::log_lines::LogLevel::Debug,
        NativeLogLevel::Info => common::log_lines::LogLevel::Info,
        NativeLogLevel::Warn => common::log_lines::LogLevel::Warn,
        NativeLogLevel::Error => common::log_lines::LogLevel::Error,
    };
    LogLine::new_developer_log_line(mapped, vec![message], now)
}

async fn dispatch_native_inner<RT: Runtime + 'static>(
    database: &Database<RT>,
    native: &Arc<NativeFunctionRunner>,
    udf_type: UdfType,
    identity: Identity,
    ts: RepeatableTimestamp,
    existing_writes: FunctionWrites,
    function_metadata: FunctionMetadata,
    execution_context: Option<ExecutionContext>,
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
             (convex_native_core::Rt). Runtime mismatch."
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
    let args_obj = extract_single_object_arg(&arguments, "native function")?;

    // Seed the ctx's deterministic RNG from the same bytes we
    // surface on UdfOutcome::rng_seed. Sync / retry paths feed the
    // same seed back in when re-executing; the handler's RNG stream
    // is therefore reproducible.
    let rng_seed: [u8; 32] = database.runtime().rng().random();

    // Share one LogBuffer + Observed-flags handle between the ctx
    // and the post-handler drain so `ctx.log()` output ends up in
    // the UdfOutcome's log_lines and `ctx.auth()` / `ctx.unix_timestamp()`
    // / `ctx.rng_*()` observations land on `observed_identity` /
    // `observed_time` / `observed_rng`.
    let log_buffer = LogBuffer::new();
    let observed: Arc<Observed> = Arc::new(Observed::from_seed(rng_seed));
    let result = match (udf_type, &registration.handler) {
        (UdfType::Query, HandlerFn::Query(handler)) => {
            let mut ctx = NativeQueryCtx::with_log_buffer_and_observed(
                tx_as_rt,
                TableNamespace::Global,
                log_buffer.clone(),
                observed.clone(),
            );
            if let Some(caller_ctx) = execution_context.clone() {
                ctx = ctx.with_execution_context(caller_ctx);
            }
            handler(&mut ctx, args_obj).await
        },
        (UdfType::Mutation, HandlerFn::Mutation(handler)) => {
            let mut ctx = NativeMutationCtx::with_log_buffer_and_observed(
                tx_as_rt,
                TableNamespace::Global,
                log_buffer.clone(),
                observed.clone(),
            );
            if let Some(caller_ctx) = execution_context.clone() {
                ctx = ctx.with_execution_context(caller_ctx);
            }
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
    let now_ts = runtime_for_rng.unix_timestamp();
    let log_lines = drain_log_buffer(&log_buffer, now_ts);
    let outcome = UdfOutcome {
        path: path.for_logging(),
        arguments,
        identity: inert_identity,
        observed_identity: observed.identity(),
        rng_seed,
        observed_rng: observed.rng_observed(),
        unix_timestamp: now_ts,
        observed_time: observed.unix_timestamp(),
        log_lines,
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

/// Dispatch a native `#[convex::action]` through
/// `NativeFunctionRunner::run_action_with_callbacks`. Unlike the
/// query/mutation path we don't open a `Transaction` here — native
/// actions don't take one (their `ActionCtx` carries a callbacks
/// handle, not a tx). Writes happen indirectly via
/// `run_mutation_by_name` / `schedule`, which go back through the
/// wrapped `ActionCallbacks`.
async fn dispatch_native_action<RT: Runtime>(
    database: &Database<RT>,
    native: &Arc<NativeFunctionRunner>,
    action_callbacks: Arc<dyn ActionCallbacks>,
    file_storage: Option<&FileStorage<RT>>,
    identity: Identity,
    function_metadata: FunctionMetadata,
    context: ExecutionContext,
    log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
) -> anyhow::Result<(
    Option<FunctionFinalTransaction>,
    FunctionOutcome,
    FunctionUsageStats,
)> {
    let (path, arguments, udf_server_version) = function_metadata.path_and_args.clone().consume();
    let inert_identity = identity.clone().into();
    let usage_tracker = FunctionUsageTracker::new();

    let args_obj = extract_single_object_arg(&arguments, "native action")?;

    // Pin a read snapshot for the whole action so multiple query
    // sub-calls (typed or by name) observe a consistent view of the
    // database. Mutations commit at a fresh timestamp so this doesn't
    // affect them.
    let action_snapshot_ts = database.now_ts_for_reads();
    let callback_identity = identity.clone();
    let mut callbacks_builder = BackendCallbacks::<RT>::with_native(
        action_callbacks,
        callback_identity,
        context,
        native.clone(),
        database.clone(),
    )
    .with_snapshot_ts(action_snapshot_ts);
    if let Some(fs) = file_storage {
        callbacks_builder = callbacks_builder.with_file_storage(fs.clone());
    }
    let callbacks = Arc::new(callbacks_builder);

    let started = Instant::now();
    let name = path.udf_path.function_name().to_string();
    let log_buffer = LogBuffer::new();
    let result = native
        .run_action_with_callbacks_identity_log_buffer(
            &name,
            TableNamespace::Global,
            args_obj,
            callbacks,
            identity,
            log_buffer.clone(),
        )
        .await;
    let duration = started.elapsed();

    // Stream ctx.log() output through the caller-supplied
    // `log_line_sender` so action logs reach the backend's
    // streaming path (same contract as the JS action runtime).
    // Silently drop when no sender is wired — the action still
    // succeeded.
    if let Some(sender) = &log_line_sender {
        let now = database.runtime().unix_timestamp();
        for line in log_buffer.snapshot() {
            let log_line = native_line_to_log_line(line, now);
            let _ = sender.send(log_line);
        }
    }

    let (result_packed, error): (Option<JsonPackedValue>, Option<JsError>) = match result {
        Ok(v) => (Some(JsonPackedValue::pack(v)), None),
        Err(e) => (None, Some(JsError::from_error_ref(&e))),
    };
    let outcome_result = match (result_packed, error) {
        (Some(p), None) => Ok(p),
        (_, Some(e)) => Err(e),
        _ => unreachable!(),
    };

    let runtime = database.runtime();
    let outcome = ActionOutcome {
        path: path.for_logging(),
        arguments,
        identity: inert_identity,
        unix_timestamp: runtime.unix_timestamp(),
        result: outcome_result,
        syscall_trace: SyscallTrace::new(),
        udf_server_version,
        user_execution_time: Some(duration),
    };

    let usage_stats = usage_tracker.gather_user_stats();
    Ok((None, FunctionOutcome::Action(outcome), usage_stats))
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
            // Substep 2.7: when a remote native pool is configured,
            // route native dispatch through it. The remote pool's
            // `FunctionRunner` impl (DistributedFunctionRunner,
            // substep 2.6b) handles the gRPC round-trip and
            // `FinalTxSummary → FunctionFinalTransaction`
            // conversion. Actions still run locally — Phase 4's
            // BackendCallbackService lands before action dispatch
            // is safe to route.
            if let Some(remote) = self.remote_native.as_ref() {
                return remote
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
                    .await;
            }
            let meta = function_metadata.expect("is_native implies function_metadata is Some");
            return dispatch_native_inner::<RT>(
                &self.database,
                &self.native,
                udf_type,
                identity,
                ts,
                existing_writes,
                meta,
                Some(context.clone()),
            )
            .await;
        }

        if is_native && matches!(udf_type, UdfType::Action) {
            let meta = function_metadata.expect("is_native implies function_metadata is Some");
            let callbacks = self.resolve_action_callbacks().ok_or_else(|| {
                anyhow::anyhow!(
                    "CompositeFunctionRunner: action_callbacks not set — set_action_callbacks \
                     must be invoked before native actions can be dispatched"
                )
            })?;
            return dispatch_native_action::<RT>(
                &self.database,
                &self.native,
                callbacks,
                self.file_storage.as_ref(),
                identity,
                meta,
                context,
                log_line_sender,
            )
            .await;
        }

        // Not native (or not a query/mutation/action): delegate.
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
        // Cache locally so native action dispatch can reach the
        // callbacks without a reference cycle, then forward to the
        // wrapped JS runner so its own action path still works.
        *self.action_callbacks.write() = Some(Arc::downgrade(&action_callbacks));
        self.js.set_action_callbacks(action_callbacks);
    }
}

// Silence unused imports in non-dispatch paths.
#[allow(dead_code)]
fn _unused(_: FunctionReads, _: DocumentUpdateWithPrevTs) {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use value::FieldName;

    use super::*;

    fn serialized_args_from_json(values: Vec<serde_json::Value>) -> SerializedArgs {
        SerializedArgs::from_args(values).expect("encode args")
    }

    fn sample_object_json() -> serde_json::Value {
        // ConvexValue::Object round-trips through the JSON schema used
        // by SerializedArgs; an empty object is the simplest case.
        json!({})
    }

    #[test]
    fn extract_single_object_arg_accepts_one_object() {
        let args = serialized_args_from_json(vec![sample_object_json()]);
        let obj = extract_single_object_arg(&args, "native function").expect("extract");
        let fields: BTreeMap<FieldName, ConvexValue> = obj.into();
        assert!(fields.is_empty(), "round-tripped object keeps its shape");
    }

    #[test]
    fn extract_single_object_arg_preserves_object_fields() {
        let args = serialized_args_from_json(vec![json!({"name": "alice", "count": 7.0})]);
        let obj = extract_single_object_arg(&args, "native function").expect("extract");
        let fields: BTreeMap<FieldName, ConvexValue> = obj.into();
        assert!(fields.contains_key(&"name".parse::<FieldName>().unwrap()));
        assert!(fields.contains_key(&"count".parse::<FieldName>().unwrap()));
    }

    #[test]
    fn extract_single_object_arg_rejects_empty_args() {
        let args = serialized_args_from_json(vec![]);
        let err =
            extract_single_object_arg(&args, "native function").expect_err("empty args must fail");
        assert!(
            format!("{err}").contains("native function: missing args object"),
            "label is baked into the error: {err}",
        );
    }

    #[test]
    fn extract_single_object_arg_rejects_non_object() {
        // First positional arg is a string — the handler contract
        // requires a single object.
        let args = serialized_args_from_json(vec![json!("not-an-object")]);
        let err = extract_single_object_arg(&args, "native action")
            .expect_err("non-object first arg must fail");
        assert!(
            format!("{err}").contains("native action args must be a single object"),
            "error identifies the required shape: {err}",
        );
    }

    #[test]
    fn extract_single_object_arg_rejects_array_first_arg() {
        let args = serialized_args_from_json(vec![json!([1, 2, 3])]);
        assert!(extract_single_object_arg(&args, "native function").is_err());
    }

    #[test]
    fn extract_single_object_arg_takes_first_of_many() {
        // Native handlers only ever see the first arg; extras are
        // silently dropped. This is the same shape dispatch_native
        // passes to the handler, and matches the JS convention of
        // one positional object.
        let args = serialized_args_from_json(vec![json!({"keep": true}), json!({"ignored": true})]);
        let obj = extract_single_object_arg(&args, "native function").expect("extract");
        let fields: BTreeMap<FieldName, ConvexValue> = obj.into();
        assert!(fields.contains_key(&"keep".parse::<FieldName>().unwrap()));
        assert!(!fields.contains_key(&"ignored".parse::<FieldName>().unwrap()));
    }
}
