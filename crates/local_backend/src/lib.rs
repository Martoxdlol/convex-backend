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
pub mod backend_callbacks_wiring;
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
pub mod native_http_dispatch;
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

/// Process-global holder for the native cron driver. The driver
/// owns per-cron tokio tasks; dropping it aborts them, so we
/// park it here for the process lifetime instead of letting it
/// leave scope at the end of `make_app`.
static NATIVE_CRON_DRIVER: std::sync::OnceLock<
    Arc<convex_native_distributed::cron_driver::NativeCronDriver>,
> = std::sync::OnceLock::new();

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
    // Capture the pool alongside the FunctionRunner so the
    // native HTTP dispatcher installed below can consult the
    // pool's by_http_route index for distributed HTTP
    // dispatch (Phase-3+ topology where the backend image
    // carries no native inventory).
    let mut admission_pool: Option<Arc<convex_native_distributed::pool::WorkerPool>> = None;
    // Captured alongside the pool so the cron-driver block below
    // can hand a `NativeCronDriver` back to the admission server
    // for worker-advertised schedules.
    let mut admission_server_handle: Option<
        convex_native_distributed::admission_server::WorkerAdmissionServer,
    > = None;
    // Captured so the cron-driver block below can mount the
    // admin HTTP surface with a live driver attached.
    let mut admin_bind_addr_opt: Option<std::net::SocketAddr> = None;
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
        // Install the pool-backed native-function resolver so
        // `udf::validation` accepts handler names the pool
        // advertises. Without this step the backend binary's
        // empty local inventory means every HTTP / WebSocket
        // request for a worker-only function bails with
        // "Could not find public function — run `npx convex
        // dev`" even though the worker is admitted and
        // dispatch-ready. Idempotent: same `OnceLock`-based
        // install hook as the monolith path.
        if native_runner.is_empty() {
            udf::validation::install_native_function_resolver(Arc::new(
                convex_native_distributed::pool::PoolNativeFunctionResolver::new(pool.clone()),
            ));
        }
        // Substep 7.4 of `convex-native/DISTRIBUTED_PLAN.md`:
        // when `CONVEX_ADMIN_BIND_ADDR` is also set, mount the
        // admin HTTP router (pool introspection + floor bumps +
        // kind preferences + drain triggers) on the operator-
        // facing port. Loopback-only bind is the expected
        // production shape.
        // Defer admin HTTP spawn until after the cron driver
        // setup so `/admin/crons` can see the live driver.
        admin_bind_addr_opt = convex_native_distributed::read_admin_bind_addr_from_env()?;
        // Seed the operator-set floor from the env var. The
        // admin HTTP route (`POST /admin/pool/floor`) can raise
        // / lower it later without a backend restart.
        if let Some(floor) = convex_native_distributed::read_min_registry_version_from_env()? {
            tracing::info!("CONVEX_MIN_REGISTRY_VERSION={floor:?} — seeding pool floor",);
            pool.set_min_registry_version(Some(floor));
        }
        admission_pool = Some(pool.clone());
        admission_server_handle = Some(admission_handle);
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
        // When the admission pool is wired, the composite also
        // needs a name-knowledge oracle so it can mark pool-only
        // function names as "native" and route them through the
        // remote runner. Without this the agnostic-backend shape
        // (empty local inventory, all handlers on the pool) would
        // fall through to the JS branch on every request and bail
        // with "Couldn't find JavaScript module".
        if let Some(pool) = admission_pool.as_ref() {
            let pool_for_oracle = pool.clone();
            composite = composite.with_remote_native_name_oracle(Arc::new(move |name| {
                pool_for_oracle.lookup_function(name).is_some()
            }));
        }
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

    // Install the native cron driver so `#[convex::cron(...)]`
    // registrations actually fire on schedule.
    //
    // Monolith: backend image carries handlers locally → driver
    // dispatches in-process via `InProcessDispatcher`.
    // Distributed: backend has no inventory → driver dispatches
    // via `PoolDispatcher` and the admission server feeds
    // worker-advertised cron entries into the driver on register.
    {
        let cron_jobs = convex_native_distributed::cron_driver::collect_from_inventory()?;
        let monolith_has_crons = !cron_jobs.is_empty();
        let pool_mode = admission_pool.is_some();
        if monolith_has_crons || pool_mode {
            let dispatcher: Arc<dyn convex_native_distributed::cron_driver::CronDispatcher> =
                if let Some(pool) = admission_pool.clone() {
                    tracing::info!(
                        "Native cron driver: {} local job(s) + pool-backed dispatch",
                        cron_jobs.len(),
                    );
                    Arc::new(convex_native_distributed::cron_driver::PoolDispatcher::new(
                        pool,
                    ))
                } else {
                    tracing::info!(
                        "Native cron driver: {} local job(s) — monolith dispatch",
                        cron_jobs.len(),
                    );
                    Arc::new(
                        convex_native_distributed::cron_driver::InProcessDispatcher::new(
                            native_runner.clone(),
                            database.clone(),
                        ),
                    )
                };
            let driver =
                Arc::new(convex_native_distributed::cron_driver::NativeCronDriver::new(dispatcher));
            if monolith_has_crons {
                driver.install(cron_jobs)?;
            }
            if let Some(handle) = admission_server_handle.as_ref() {
                handle.set_cron_driver(driver.clone());
            }
            NATIVE_CRON_DRIVER.set(driver.clone()).ok();
            // Mount the admin HTTP surface with the cron driver
            // attached so `/admin/crons` returns live data.
            if let Some(admin_addr) = admin_bind_addr_opt.take() {
                tracing::info!(
                    "CONVEX_ADMIN_BIND_ADDR={admin_addr:?} — mounting admin HTTP surface",
                );
                let mut state = convex_native_distributed::admin_http::AdminState::new(
                    admission_pool
                        .clone()
                        .expect("admission_pool set with admin"),
                )
                .with_cron_driver(driver);
                if let Some(handle) = admission_server_handle.as_ref() {
                    state = state.with_admission(handle.clone());
                }
                convex_native_distributed::admin_http::spawn_admin_server(admin_addr, state)
                    .await?;
            }
        }
    }
    // Pool-mode admin spawn fallback: when the admission pool is
    // attached but no native crons exist (so the cron block above
    // didn't take the addr), still mount the admin surface for
    // pool introspection / floor / drain.
    if let Some(admin_addr) = admin_bind_addr_opt.take() {
        if let Some(pool) = admission_pool.clone() {
            tracing::info!("CONVEX_ADMIN_BIND_ADDR={admin_addr:?} — mounting admin HTTP surface",);
            let mut state = convex_native_distributed::admin_http::AdminState::new(pool);
            if let Some(handle) = admission_server_handle.as_ref() {
                state = state.with_admission(handle.clone());
            }
            convex_native_distributed::admin_http::spawn_admin_server(admin_addr, state).await?;
        }
    }

    // Install the native HTTP dispatcher so `http_any_method` can
    // short-circuit `(method, path)` pairs that match a
    // `#[convex::http_action]` registration. Without this, pure-
    // native deployments 404 on every HTTP-action request because
    // the JS path requires `_modules` rows that the native boot
    // flow never writes. Install when either the local
    // `HttpRouter` has entries (monolith topology) or a pool is
    // configured (distributed topology — backend image ships no
    // native inventory but the pool's `by_http_route` index
    // points at the workers that do).
    {
        let http_router = Arc::new(convex_native_core::http::HttpRouter::collect()?);
        let should_install = http_router.len() > 0 || admission_pool.is_some();
        if should_install {
            tracing::info!(
                "Native HTTP router: {} local route(s), pool {} — installing dispatcher",
                http_router.len(),
                if admission_pool.is_some() {
                    "attached"
                } else {
                    "not attached"
                },
            );
            let mut dispatcher = native_http_dispatch::NativeHttpDispatcher::new(
                http_router,
                native_runner.clone(),
                application.runner(),
                database.clone(),
                file_storage.clone(),
            );
            if let Some(pool) = admission_pool.clone() {
                dispatcher = dispatcher.with_pool(pool);
            }
            native_http_dispatch::install_native_http_dispatcher(Arc::new(dispatcher));
        }
    }

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

    // Phase-4 backend-callback service. When
    // `CONVEX_BACKEND_CALLBACK_BIND_ADDR` is set, expose a
    // tonic `BackendCallbackService` so remote workers' actions
    // can route sub-calls (`ctx.run_mutation(...)`,
    // `ctx.scheduler()`, file storage) back to the backend's
    // Committer. The component resolver consults the database
    // for non-root component paths; the file-bytes handle wires
    // the streaming `StorageStore` / `StorageGet` paths through
    // `Application`'s file storage.
    if let Some(callback_bind) = convex_native_distributed::read_callback_bind_addr_from_env()? {
        tracing::info!(
            "CONVEX_BACKEND_CALLBACK_BIND_ADDR={callback_bind:?} — spawning BackendCallbackService",
        );
        let action_callbacks: Arc<dyn udf::ActionCallbacks> = application.runner();
        let component_resolver: Arc<
            dyn convex_native_distributed::backend_callbacks_server::ComponentResolver,
        > = Arc::new(backend_callbacks_wiring::ApplicationComponentResolver::new(
            database.clone(),
        ));
        let document_reader: Arc<
            dyn convex_native_distributed::backend_callbacks_server::BackendDocumentReader,
        > = Arc::new(backend_callbacks_wiring::ApplicationDocumentReader::new(
            database.clone(),
        ));
        let file_bytes: Arc<
            dyn convex_native_distributed::backend_callbacks_server::BackendFileBytes,
        > = Arc::new(backend_callbacks_wiring::BackendFileBytesImpl::new(
            file_storage.clone(),
            database.clone(),
        ));
        convex_native_distributed::backend_callbacks_server::spawn_backend_callback_server(
            callback_bind,
            action_callbacks,
            Some(component_resolver),
            Some(file_bytes),
            Some(document_reader),
        )
        .await?;
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

        // When `CONVEX_BACKEND_ENDPOINT=grpc://backend:5678` is set
        // the worker dials the backend's `WorkerAdmissionService`,
        // ships its `FunctionInventory` + `registry_version`, and
        // keeps the stream open so the backend can push drain /
        // floor updates back. Dropping the returned handle retires
        // the worker; we park it in a process-global `OnceLock`
        // so it lives for the lifetime of the worker process.
        //
        // Without this the admission service never learns about
        // the worker, `WorkerPool::eligible_for` returns empty,
        // and every client request bails with "no worker available
        // for function X" — even though the `FunctionExecutionService`
        // above is ready to serve.
        if let Some(backend_endpoint) =
            convex_native_distributed::read_backend_endpoint_from_env()?
        {
            let execute_endpoint = format!("http://{}", bind_addr);
            // Cargo pkg version of the workspace binary. In a real
            // deployment the deployer's own crate version is what
            // the admission envelope should carry; for the
            // all-in-one `convex_native::run()` shape the workspace
            // version is a reasonable stand-in (matches what every
            // `local_backend`-linking worker advertises).
            let registry_version = env!("CARGO_PKG_VERSION").to_string();
            tracing::info!(
                "CONVEX_BACKEND_ENDPOINT={backend_endpoint:?} — registering with admission \
                 service (execute_endpoint={execute_endpoint}, registry_version={registry_version})",
            );
            match convex_native_distributed::admission_client::WorkerRegistration::register(
                backend_endpoint,
                execute_endpoint,
                registry_version,
            )
            .await
            {
                Ok(registration) => {
                    static WORKER_REGISTRATION: std::sync::OnceLock<
                        convex_native_distributed::admission_client::WorkerRegistration,
                    > = std::sync::OnceLock::new();
                    let _ = WORKER_REGISTRATION.set(registration);
                    tracing::info!("Worker admitted into pool");
                },
                Err(e) => {
                    tracing::error!(
                        "Worker admission failed: {e:#}. The worker will still serve \
                         FunctionExecutionService on {bind_addr}, but the backend won't dispatch \
                         to it until registration succeeds.",
                    );
                },
            }
        }
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
