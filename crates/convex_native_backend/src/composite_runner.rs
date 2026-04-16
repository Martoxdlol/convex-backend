//! `CompositeFunctionRunner` — wraps a JS `FunctionRunner<RT>` and
//! intercepts calls whose function names match the native registry.
//!
//! Today only the "recognize native names" layer is wired: for every
//! other call we delegate to the JS runner unchanged. When the
//! requested function is native, the runner returns a clear error
//! because building the full `FunctionOutcome` / `FunctionFinalTransaction`
//! envelope for a native call still requires work on the transaction
//! plumbing side — see the implementation notes inline.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
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
    NativeFunctionRunner,
    NativeSchema,
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
use sync_types::{
    CanonicalizedModulePath,
    Timestamp,
};
use tokio::sync::mpsc;
use udf::{
    ActionCallbacks,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
};
use usage_tracking::FunctionUsageStats;
use value::identifier::Identifier;

/// Wraps a JS `FunctionRunner<RT>` and routes recognized native
/// function names through a short-circuit that currently errors; all
/// other calls pass straight through.
pub struct CompositeFunctionRunner<RT: Runtime> {
    pub native: Arc<NativeFunctionRunner>,
    pub js: Arc<dyn FunctionRunner<RT>>,
}

impl<RT: Runtime> CompositeFunctionRunner<RT> {
    pub fn new(native: Arc<NativeFunctionRunner>, js: Arc<dyn FunctionRunner<RT>>) -> Self {
        Self { native, js }
    }

    fn requested_function_name(meta: Option<&FunctionMetadata>) -> Option<String> {
        let meta = meta?;
        let path = meta.path_and_args.path();
        Some(path.udf_path.function_name().to_string())
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
        // Intercept native names and surface a clear error. Full
        // FunctionOutcome construction is follow-up work: the
        // composite runner would need to build a Transaction<RT>
        // from the Database snapshot, run the native handler, then
        // render the resulting reads/writes + log lines + return
        // value into an UdfOutcome. That's a non-trivial plumbing
        // exercise tracked separately.
        if let Some(name) = Self::requested_function_name(function_metadata.as_ref()) {
            if self.native.has_function(&name) {
                anyhow::bail!(
                    "function {name:?} is registered natively but native-path dispatch is not \
                     wired through the composite runner yet. See \
                     convex-native/COMPOSITE_RUNNER.md."
                );
            }
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
        // Merge native tables into whatever the JS side produced. Native
        // wins on collision is a hard error — a table declared both in
        // schema.ts and via #[derive(ConvexDocument)] is ambiguous and
        // an operator mistake.
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
