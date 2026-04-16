# CompositeFunctionRunner — integration reference

This is the adapter that wires `convex_native::NativeFunctionRunner` into
the backend's `function_runner::FunctionRunner` trait. It is **not yet
built in-workspace** because `function_runner` transitively depends on
`isolate` (V8), which requires the `npm-packages/` rush install + build
step to be set up.

When that environment is available (CI, production), the reference
implementation below should live at
`crates/convex_native_backend/src/composite_runner.rs` (or similar) and
be wired into `make_app()` at `crates/local_backend/src/lib.rs:214`.

## Reference implementation

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use common::{
    auth::AuthConfig,
    bootstrap_model::components::definition::ComponentDefinitionMetadata,
    components::{ComponentDefinitionPath, ComponentName, Resource},
    errors::JsError,
    execution_context::ExecutionContext,
    log_lines::LogLine,
    runtime::{Runtime, UnixTimestamp},
    schemas::DatabaseSchema,
    types::{IndexId, RepeatableTimestamp, UdfType},
};
use convex_native::{NativeFunctionRunner, schema::NativeSchema};
use function_runner::{
    FunctionFinalTransaction, FunctionRunner, FunctionWrites,
    server::{FunctionMetadata, HttpActionMetadata},
};
use keybroker::Identity;
use model::{
    config::types::ModuleConfig,
    environment_variables::types::{EnvVarName, EnvVarValue},
    modules::module_versions::{AnalyzedModule, ModuleSource, SourceMap},
    udf_config::types::UdfConfig,
};
use sync_types::{CanonicalizedModulePath, Timestamp};
use tokio::sync::mpsc;
use udf::{ActionCallbacks, EvaluateAppDefinitionsResult, FunctionOutcome};
use usage_tracking::FunctionUsageStats;
use value::identifier::Identifier;

/// Wraps a JS `FunctionRunner` and intercepts native function calls.
pub struct CompositeFunctionRunner<RT: Runtime> {
    pub native: NativeFunctionRunner,
    pub js: Arc<dyn FunctionRunner<RT>>,
}

impl<RT: Runtime> CompositeFunctionRunner<RT> {
    pub fn new(native: NativeFunctionRunner, js: Arc<dyn FunctionRunner<RT>>) -> Self {
        Self { native, js }
    }
}

#[async_trait]
impl<RT: Runtime> FunctionRunner<RT> for CompositeFunctionRunner<RT> {
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
        // Native dispatch path — full FunctionOutcome wiring is TODO.
        // The shape of this work:
        //   1. Snapshot the Database at `ts` (self.db.snapshot(ts)?).
        //   2. Construct a Transaction<RT> from the snapshot +
        //      identity + existing_writes.
        //   3. Invoke NativeFunctionRunner::run_query/run_mutation
        //      against the transaction; capture the ConvexValue result
        //      and, on error, a JsError (we'll need an anyhow-to-JsError
        //      conversion since native code uses anyhow).
        //   4. Extract a FunctionFinalTransaction via
        //      FunctionFinalTransaction::try_from(transaction).
        //   5. Build a UdfOutcome with:
        //        - path = function_metadata.path
        //        - arguments = function_metadata.arguments (as
        //          SerializedArgs)
        //        - identity = identity.inert()
        //        - observed_identity = false (or track if ctx saw it)
        //        - rng_seed = rand::random()
        //        - unix_timestamp = current time
        //        - observed_* = false placeholders initially
        //        - log_lines / audit_log_lines = Vec::new() (wire a
        //          real collector via QueryCtx later)
        //        - journal = QueryJournal::default()
        //        - result = Ok(JsonPackedValue::from_value(value)?)
        //        - syscall_trace = SyscallTrace::default()
        //        - udf_server_version = None
        //        - memory_in_mb = 0
        //        - user_execution_time = Some(elapsed)
        //   6. Return (Some(final_tx), FunctionOutcome::Query/Mutation(udf_outcome),
        //      FunctionUsageStats::default()).
        if let Some(name) = function_metadata
            .as_ref()
            .map(|m| m.path.path.udf_path.function_name())
            && self.native.has_function(name)
        {
            anyhow::bail!(
                "Native dispatch for function {name:?} is registered but \
                 full FunctionOutcome wiring is not yet implemented \
                 (Phase 1.4 TODO)."
            );
        }

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
            .analyze(udf_config, modules, environment_variables, max_user_heap_size)
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
        // Merge native + JS schemas. Native tables collide is a hard
        // error — callers should either declare a table in JS or via
        // #[derive(ConvexDocument)], never both.
        let js_schema = self
            .js
            .evaluate_schema(schema_bundle, source_map, rng_seed, unix_timestamp)
            .await?;
        let mut native_schema = NativeSchema::collect()?;
        native_schema.schema_validation = js_schema.schema_validation;
        for (name, def) in js_schema.tables {
            if native_schema.tables.contains_key(&name) {
                anyhow::bail!(
                    "table {name:?} is declared both natively and in JS \
                     schema.ts — remove one of the declarations"
                );
            }
            native_schema.tables.insert(name, def);
        }
        Ok(native_schema)
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
```

## Wiring into `make_app()`

`crates/local_backend/src/lib.rs` has a `make_app()` function that
constructs the `FunctionRunner`. Once this crate lands it should become:

```rust
let js_runner = Arc::new(InProcessFunctionRunner::new(...)?);
let native_runner = NativeFunctionRunner::from_inventory()?;
let composite = Arc::new(CompositeFunctionRunner::new(native_runner, js_runner));
// ... use `composite` wherever `FunctionRunner` is expected ...
```

## TODO list for full native execution path

1. Build `Transaction<RT>` from a `Database<RT>::snapshot(ts)`
   plus identity/existing_writes — copy the transaction-construction
   dance from `FunctionRunnerCore::run_function_no_retention_check` but
   stripped of V8 setup.
2. Provide a log-line collector accessible to native handlers (likely
   a field on `QueryCtx` / `MutationCtx`).
3. Route `ctx.db().get()`/`insert()` usage through the existing
   `Transaction` read-tracking so `FunctionFinalTransaction::try_from`
   produces a correct read set.
4. Serialize the return value as `JsonPackedValue` for `UdfOutcome`.
5. Handle anyhow errors → JsError translation (the JS path distinguishes
   user-surface errors from system errors via `ErrorMetadata`; native
   errors need the same categorization).
6. Emit usage stats (rows read, bytes read) via
   `tx.usage_tracker.take_stats()`.

Until these pieces land, `run_function` for native names returns a
placeholder error as shown in the reference impl above.
