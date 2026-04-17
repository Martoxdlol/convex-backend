#![feature(try_blocks)]
#![feature(try_blocks_heterogeneous)]
#![feature(iterator_try_collect)]
#![feature(coroutines)]
#![feature(exhaustive_patterns)]

use std::{
    self,
    sync::Arc,
    time::Duration,
};

use ::authentication::{
    access_token_auth::NullAccessTokenAuth,
    application_auth::ApplicationAuth,
};
use application::{
    self,
    api::ApplicationApi,
    log_visibility::RedactLogsToClient,
    Application,
    QueryCache,
};
use common::{
    self,
    http::{
        fetch::ProxiedFetchClient,
        RouteMapper,
    },
    knobs::{
        ACTION_USER_TIMEOUT,
        DOCUMENT_RETENTION_RATE_LIMIT,
        UDF_CACHE_MAX_SIZE,
    },
    persistence::Persistence,
    runtime::{
        new_rate_limiter,
        Runtime,
    },
    shutdown::ShutdownSignal,
    types::{
        ConvexOrigin,
        ConvexSite,
        TEST_REGION_NAME,
    },
};
use config::LocalConfig;
use database::Database;
use events::usage::NoOpUsageEventLogger;
use exports::interface::InProcessExportProvider;
use file_storage::{
    FileStorage,
    TransactionalFileStorage,
};
use function_runner::{
    in_process_function_runner::InProcessFunctionRunner,
    server::DeploymentStorage,
    FunctionRunner,
};
use governor::Quota;
use http_client::CachedHttpClient;
use indexing::index_cache::SharedIndexCache;
use model::{
    initialize_application_system_tables,
    virtual_system_mapping,
};
use node_executor::{
    local::LocalNodeExecutor,
    Actions,
};
use runtime::prod::ProdRuntime;
use search::{
    searcher::InProcessSearcher,
    Searcher,
    SegmentTermMetadataFetcher,
};
use serde::Serialize;

pub mod admin;
mod app_metrics;
mod args_structs;
pub mod authentication;
pub mod beacon;
pub mod canonical_urls;
pub mod config;
pub mod custom_headers;
pub mod dashboard;
pub mod deploy_config;
pub mod deploy_config2;
pub mod deployment_info;
pub mod deployment_state;
pub mod environment_variables;
pub mod http_actions;
pub mod log_sinks;
pub mod logs;
pub mod node_action_callbacks;
pub mod parse;
pub mod proxy;
pub mod public_api;
pub mod router;
pub mod scheduling;
pub mod schema;
pub mod snapshot_export;
pub mod snapshot_import;
pub mod storage;
pub mod streaming_export;
pub mod streaming_import;
pub mod subs;
pub const MAX_CONCURRENT_REQUESTS: usize = 128;

#[derive(Clone)]
pub struct LocalAppState {
    // Origin for the server (e.g. http://127.0.0.1:3210, https://demo.convex.cloud)
    pub origin: ConvexOrigin,
    // Origin for the corresponding convex.site (where we serve HTTP) (e.g. http://127.0.0.1:8001, https://crazy-giraffe-123.convex.site)
    pub site_origin: ConvexSite,
    // Name of the instance. (e.g. crazy-giraffe-123)
    pub instance_name: String,
    pub application: Application<ProdRuntime>,
    pub zombify_rx: async_broadcast::Receiver<()>,
}

impl LocalAppState {
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.application.shutdown().await?;

        Ok(())
    }
}

// Contains state needed to serve most http routes. Similar to LocalAppState,
// but uses ApplicationApi instead of Application, which allows it to be used
// in both Backend and Usher.
#[derive(Clone)]
pub struct RouterState {
    pub api: Arc<dyn ApplicationApi>,
    pub runtime: ProdRuntime,
}

#[derive(Serialize)]
pub struct EmptyResponse {}

pub async fn make_app(
    runtime: ProdRuntime,
    config: LocalConfig,
    persistence: Arc<dyn Persistence>,
    zombify_rx: async_broadcast::Receiver<()>,
    preempt_tx: ShutdownSignal,
) -> anyhow::Result<LocalAppState> {
    let key_broker = config.key_broker()?;
    let in_process_searcher = Arc::new(InProcessSearcher::new(runtime.clone())?);
    let searcher: Arc<dyn Searcher> = in_process_searcher.clone();
    // TODO(CX-6572) Separate `SegmentMetadataFetcher` from `SearcherImpl`
    let segment_metadata_fetcher: Arc<dyn SegmentTermMetadataFetcher> = in_process_searcher;
    let (deleted_tablet_sender, deleted_tablet_receiver) = tokio::sync::mpsc::channel(100);
    let usage_event_logger = Arc::new(NoOpUsageEventLogger);
    let database = Database::load(
        persistence.clone(),
        runtime.clone(),
        searcher.clone(),
        preempt_tx.clone(),
        virtual_system_mapping().clone(),
        Some(SharedIndexCache),
        Arc::new(new_rate_limiter(
            runtime.clone(),
            Quota::per_second(*DOCUMENT_RETENTION_RATE_LIMIT),
        )),
        deleted_tablet_sender,
    )
    .await?;
    initialize_application_system_tables(&database).await?;
    let application_storage = Application::initialize_storage(
        runtime.clone(),
        &database,
        config.storage_tag_initializer(),
        config.name(),
    )
    .await?;

    let file_storage = FileStorage {
        transactional_file_storage: TransactionalFileStorage::new(
            runtime.clone(),
            application_storage.files_storage.clone(),
            config.convex_origin_url()?,
        ),
        database: database.clone(),
    };

    let node_process_timeout = *ACTION_USER_TIMEOUT + Duration::from_secs(5);
    let node_executor = Arc::new(LocalNodeExecutor::new(node_process_timeout).await?);
    let actions = Actions::new(
        node_executor,
        config.convex_origin_url()?,
        *ACTION_USER_TIMEOUT,
        runtime.clone(),
    );

    #[cfg(not(debug_assertions))]
    if config.convex_http_proxy.is_none() {
        tracing::warn!(
            "Running without a proxy in release mode -- UDF `fetch` requests are unrestricted!"
        );
    }
    let fetch_client = Arc::new(ProxiedFetchClient::new(
        config.convex_http_proxy.clone(),
        config.name(),
        reqwest::redirect::Policy::none(),
    ));
    let oidc_http_client = CachedHttpClient::new(
        config.convex_http_proxy.clone(),
        config.name(),
        reqwest::redirect::Policy::default(),
    );
    let js_runner: Arc<dyn FunctionRunner<ProdRuntime>> = Arc::new(InProcessFunctionRunner::new(
        config.name().clone(),
        key_broker.function_runner_keybroker(),
        config.convex_origin_url()?,
        runtime.clone(),
        persistence.reader(),
        DeploymentStorage {
            files_storage: application_storage.files_storage.clone(),
            modules_storage: application_storage.modules_storage.clone(),
        },
        database.clone(),
        fetch_client.clone(),
    )?);

    // convex-local-backend is an all-in-one binary: it always runs a
    // full Application (database + HTTP server + composite runner).
    // `CONVEX_MODE` toggles a *second* surface on top of that:
    //
    //   - `standalone` (default): HTTP only.
    //   - `worker`: HTTP + a tonic `FunctionExecutionService` bound to
    //     `CONVEX_WORKER_BIND_ADDR`, so a remote caller can dispatch native calls
    //     to this process while still using the same Database as the HTTP path.
    //     This is a pre-Phase-1 shape; see convex-native/DISTRIBUTED_PLAN.md for
    //     the target architecture (backend coordinates commits; worker stops
    //     committing locally).
    //   - `conductor`: rejected. The standalone-conductor concept is superseded by
    //     the prebuilt backend image described in DISTRIBUTED_PLAN.md Phase 5.
    let convex_mode = convex_native_distributed::read_mode_from_env();
    tracing::info!("convex-local-backend CONVEX_MODE detected: {convex_mode:?}");
    use convex_native_core::distributed::ConvexMode;
    match convex_mode {
        ConvexMode::Standalone | ConvexMode::Worker => {},
        ConvexMode::Conductor => anyhow::bail!(
            "convex-local-backend refuses CONVEX_MODE=conductor: the standalone-conductor concept \
             is removed in favour of the backend image described in \
             convex-native/DISTRIBUTED_PLAN.md. Run in `standalone` or `worker` mode."
        ),
    }

    // Wrap in the composite runner so any statically-registered
    // native functions (#[convex::query/mutation/action]) intercept
    // before the request reaches V8. When the registry is empty the
    // composite is a thin pass-through to the JS runner.
    let native_runner = Arc::new(convex_native_core::NativeFunctionRunner::from_inventory()?);
    tracing::info!(
        "Native function registry: {} registered",
        native_runner.len(),
    );
    // Install the global native-function resolver that
    // `udf::validation` consults at the HTTP / WebSocket / sync entry
    // points. Without this, `ValidatedPathAndArgs::new` can't find
    // any `#[convex::*]` handler that lives only in the binary's
    // `inventory` table — every pure-native deployment's client
    // traffic would 404 with a "run `npx convex dev`" error. See
    // `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md` for the full
    // diagnosis. Idempotent: repeated `make_app` calls in the same
    // process (integration tests) keep the first resolver.
    //
    // Only install when the inventory has entries. A JS-only
    // deployment gets no resolver installed, so the original
    // `missing_or_internal_error` hint ("Did you forget to run `npx
    // convex dev`?") remains accurate — the tailored
    // native-flavoured hint would be misleading there.
    if !native_runner.is_empty() {
        convex_native_backend::install_native_resolver((*native_runner).clone());
    }
    // Note: native schema publication happens AFTER
    // `Application::new` below, because the `SchemaWorker` that
    // validates the pending schema is only started inside
    // `Application::new`. See the block after `Application::new` for
    // the actual call and rationale.
    // Substep 5.2 of `convex-native/DISTRIBUTED_PLAN.md`: when
    // `CONVEX_REFUSE_NATIVE_HANDLERS=1` is set, the backend
    // binary must carry no `#[convex::*]` registrations. The
    // Phase-5 prebuilt image ships with an empty inventory;
    // this env var pins that invariant so a deployer who
    // accidentally links their worker code into the backend
    // image fails loud at boot instead of silently shadowing
    // the remote pool's handlers.
    if convex_native_distributed::read_refuse_native_handlers_from_env() && native_runner.len() > 0
    {
        anyhow::bail!(
            "CONVEX_REFUSE_NATIVE_HANDLERS=1: backend binary has {} native function \
             registration(s) linked in, but the Phase-5 distributed-topology backend is supposed \
             to carry none (workers ship the inventory via WorkerAdmissionService). Drop the \
             `#[convex::*]`-decorated code from your backend's compile graph or unset the env var \
             to override.",
            native_runner.len(),
        );
    }
    // Native-dispatch routing:
    //
    // - Substep 3.8: when `CONVEX_ADMISSION_BIND_ADDR=host:port` is set, spawn a
    //   `WorkerAdmissionService` on that port and use a dynamic
    //   `PoolFunctionRunner` for native dispatch. Takes precedence over
    //   `CONVEX_NATIVE_WORKERS`.
    // - Substep 2.7: when `CONVEX_NATIVE_WORKERS` is set (and
    //   `CONVEX_ADMISSION_BIND_ADDR` isn't), use the fixed-pool
    //   `DistributedFunctionRunner`.
    // - Neither set: keep in-process behaviour (the Phase-2 default), native
    //   Query/Mutation dispatches locally.
    //
    // Actions still run locally until Phase 4's
    // `BackendCallbackService` lands, regardless of which
    // remote-pool mode is active.
    let admission_bind_addr = convex_native_distributed::read_admission_bind_addr_from_env()?;
    let remote_native_pool: Option<Arc<dyn FunctionRunner<ProdRuntime>>> = if let Some(bind_addr) =
        admission_bind_addr
    {
        tracing::info!(
            "CONVEX_ADMISSION_BIND_ADDR={bind_addr:?} — spawning WorkerAdmissionService; native \
             Query/Mutation dispatch will route through the dynamic pool",
        );
        let (pool, admission_handle) =
            convex_native_distributed::admission_server::spawn_admission_server_with_handle(
                bind_addr,
            )
            .await?;
        // Substep 7.4 of `convex-native/DISTRIBUTED_PLAN.md`:
        // when `CONVEX_ADMIN_BIND_ADDR` is also set, mount the
        // admin HTTP router (pool introspection + floor bumps +
        // kind preferences + drain triggers) on the operator-
        // facing port. Loopback-only bind is the expected
        // production shape.
        if let Some(admin_addr) = convex_native_distributed::read_admin_bind_addr_from_env()? {
            tracing::info!("CONVEX_ADMIN_BIND_ADDR={admin_addr:?} — mounting admin HTTP surface",);
            let state = convex_native_distributed::admin_http::AdminState::new(pool.clone())
                .with_admission(admission_handle);
            convex_native_distributed::admin_http::spawn_admin_server(admin_addr, state).await?;
        }
        Some(Arc::new(
            convex_native_distributed::pool_runner::PoolFunctionRunner::new(pool),
        ))
    } else {
        match convex_native_distributed::read_native_workers_from_env()? {
            Some(endpoints) => {
                tracing::info!(
                    "CONVEX_NATIVE_WORKERS configured with {} endpoint(s); native Query/Mutation \
                     dispatch will route through the fixed remote pool",
                    endpoints.len(),
                );
                let clients = convex_native_distributed::build_conductor_runner(&endpoints).await?;
                Some(Arc::new(clients))
            },
            None => None,
        }
    };
    let mut composite = convex_native_backend::CompositeFunctionRunner::new(
        native_runner.clone(),
        js_runner,
        database.clone(),
    )
    .with_file_storage(file_storage.clone());
    if let Some(remote) = remote_native_pool {
        composite = composite.with_remote_native_pool(remote);
    }
    let function_runner: Arc<dyn FunctionRunner<ProdRuntime>> = Arc::new(composite);

    let application = Application::new(
        runtime.clone(),
        database.clone(),
        file_storage.clone(),
        application_storage,
        usage_event_logger,
        key_broker.clone(),
        config.name(),
        Some(TEST_REGION_NAME.clone()),
        function_runner,
        config.convex_origin_url()?,
        config.convex_site_url()?,
        searcher.clone(),
        segment_metadata_fetcher,
        persistence,
        actions,
        Arc::new(RedactLogsToClient::new(config.redact_logs_to_client)),
        Arc::new(ApplicationAuth::new(
            key_broker.clone(),
            Arc::new(NullAccessTokenAuth),
        )),
        QueryCache::new(*UDF_CACHE_MAX_SIZE),
        fetch_client,
        config.local_log_sink.clone(),
        preempt_tx.clone(),
        Arc::new(InProcessExportProvider),
        deleted_tablet_receiver,
        oidc_http_client,
    )
    .await?;

    // Publish the inventory-declared `NativeSchema` into the root
    // component's `_schemas` table and block until the schema is
    // Active + every index has finished backfilling. Idempotent —
    // second boots that see an identical active schema are no-ops.
    //
    // The call blocks on purpose: a native worker that starts
    // answering traffic before its indexes are enabled returns
    // "index X is currently backfilling and not available to query
    // yet" to clients, which looks like a broken deployment.
    // Worker readiness must not lead schema readiness. In Kubernetes
    // and similar, the deployment's readiness probe polls the HTTP
    // server; gating HTTP behind `publish_native_schema` is how we
    // keep the probe from flipping to Ready before indexes are live.
    // For a fresh sqlite DB this costs milliseconds; for larger
    // datasets the wait scales with backfill time.
    //
    // Placement: after `Application::new` (which starts the
    // `SchemaWorker` that validates our pending schema) and before
    // the HTTP server starts accepting connections (main.rs does
    // that *after* `make_app` returns).
    //
    // Without this, native queries that rely on
    // `#[convex(index(...))]` indexes fail with "Index
    // <table>.<name> not found" because the `_indexes` rows are
    // only ever written through `apply_config`, which a pure-native
    // deployment never calls. See
    // `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md` follow-ups.
    convex_native_backend::publish_native_schema(&database).await?;

    let origin = config.convex_origin_url()?;
    let instance_name = config.name();

    if !config.disable_beacon {
        let beacon_future = beacon::start_beacon(
            runtime.clone(),
            database.clone(),
            config.beacon_tag.clone(),
            config.beacon_fields.clone(),
        );
        runtime.spawn_background("beacon_worker", beacon_future);
    }

    // In Worker mode, also expose a tonic `FunctionExecutionService`
    // on `CONVEX_WORKER_BIND_ADDR`. Queries and mutations arriving
    // over gRPC execute inline against the same `Database` the HTTP
    // path uses, so a remote caller and a direct HTTP client see
    // one consistent read timeline. The server drains on the same
    // `zombify_rx` broadcast the HTTP server listens on, so Ctrl-C /
    // `/preempt` stops accepting new RPCs, lets in-flight calls
    // finish, and tears down alongside HTTP.
    if matches!(convex_mode, ConvexMode::Worker) {
        let bind_addr = convex_native_distributed::read_worker_bind_addr_from_env()?;
        tracing::info!(
            "CONVEX_MODE=worker: starting FunctionExecutionService on {bind_addr} ({} native \
             function(s))",
            native_runner.len(),
        );
        let worker_db = database.clone();
        let worker_native = native_runner.clone();
        // Drain on the same broadcast the HTTP server listens on
        // (cloned as `zombify_rx` at the make_app boundary). A single
        // `shutdown_tx.broadcast(())` therefore takes down HTTP, the
        // site proxy, and the worker gRPC server together.
        let mut worker_shutdown_rx = zombify_rx.clone();
        let worker_shutdown = async move {
            let _ = worker_shutdown_rx.recv().await;
        };
        runtime.spawn_background("convex_native_worker", async move {
            if let Err(e) = convex_native_distributed::serve_worker_with_shutdown(
                bind_addr,
                worker_native,
                worker_db,
                worker_shutdown,
            )
            .await
            {
                tracing::error!("convex_native_core worker tonic server exited: {e}");
            }
        });
    }

    let app_state = LocalAppState {
        origin,
        site_origin: config.convex_site_url()?,
        instance_name,
        application,
        zombify_rx,
    };

    Ok(app_state)
}

#[derive(Clone)]
pub struct HttpActionRouteMapper;

impl RouteMapper for HttpActionRouteMapper {
    fn map_route(&self, route: String) -> String {
        // Backend can receive arbitrary HTTP requests, so group all of these
        // under one tag.
        if route.starts_with("/http/") {
            "/http/:user_http_action".into()
        } else {
            route
        }
    }
}
