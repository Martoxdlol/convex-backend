//! Batteries-included entry point for convex-native deployers.
//!
//! Re-exports the full developer surface from `convex_native_core`
//! and adds [`run`] — a single async call that parses
//! `LocalConfig` from the process CLI, connects persistence, boots
//! the HTTP service, and blocks until Ctrl-C.
//!
//! ```no_run
//! # async fn _demo() -> anyhow::Result<()> {
//! #[allow(unused_imports)]
//! use my_convex_app as _;   // force `inventory` linkage
//!
//! convex_native::run().await
//! # }
//! ```
//!
//! Deployers who need finer-grained control — custom CLI, embedded
//! usage, or the distributed topology — should reach for
//! `local_backend::make_app` + `convex_native_core` directly. See
//! `convex-native/STANDALONE.md` and `convex-native/DISTRIBUTED_PLAN.md`.

#![allow(clippy::needless_doctest_main)]

pub use convex_native_core::*;

use std::time::Duration;

use clap::Parser;
use cmd_util::env::config_service;
use common::{
    http::ConvexHttpService,
    knobs::HTTP_SERVER_TIMEOUT_DURATION,
    runtime::Runtime,
    shutdown::ShutdownSignal,
    version::SERVER_VERSION_STR,
};
use db_connection::{
    connect_persistence,
    ConnectPersistenceFlags,
};
use futures::{
    future::{
        self,
        Either,
    },
    FutureExt,
};
use local_backend::{
    config::LocalConfig,
    make_app,
    proxy::dev_site_proxy,
    router::router,
    HttpActionRouteMapper,
    MAX_CONCURRENT_REQUESTS,
};
use runtime::prod::ProdRuntime;
use tokio::{
    signal,
    sync::oneshot,
};

/// Parse `LocalConfig` from the process CLI + env, boot the full
/// Convex backend, and serve until Ctrl-C.
///
/// Exactly equivalent to running the upstream `convex-local-backend`
/// binary, with your `#[convex::*]` / `#[derive(ConvexDocument)]`
/// registrations picked up via `inventory`.
///
/// Returns `Ok(())` after graceful shutdown. Fatal errors (DB
/// preempt signal, serve-task failure) surface as `Err`.
pub async fn run() -> anyhow::Result<()> {
    let _guard = config_service();
    let config = LocalConfig::parse();
    run_with_config(config).await
}

/// Same as [`run`] but takes a caller-constructed `LocalConfig`.
/// Use when you parse config yourself or embed the backend inside a
/// larger binary.
pub async fn run_with_config(config: LocalConfig) -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()
        .map_err(|e| anyhow::anyhow!("failed to init tokio: {e}"))?;
    let runtime = ProdRuntime::new(&tokio);
    let runtime_ = runtime.clone();
    runtime.block_on("main", async move { run_server(runtime_, config).await })
}

async fn run_server(runtime: ProdRuntime, config: LocalConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting Convex backend with {} native function(s) registered",
        convex_native_core::NativeFunctionRunner::from_inventory()
            .map(|r| r.len())
            .unwrap_or(0),
    );

    let (preempt_tx, preempt_rx) = oneshot::channel();
    let preempt_signal = ShutdownSignal::new(preempt_tx);
    let (shutdown_tx, shutdown_rx) = async_broadcast::broadcast(1);

    let persistence = connect_persistence(
        config.db,
        &config.db_spec,
        ConnectPersistenceFlags {
            require_ssl: !config.do_not_require_ssl,
            allow_read_only: false,
            skip_index_creation: false,
        },
        &config.name(),
        runtime.clone(),
        preempt_signal.clone(),
    )
    .await?;

    let st = make_app(
        runtime.clone(),
        config.clone(),
        persistence,
        shutdown_rx.clone(),
        preempt_signal.clone(),
    )
    .await?;

    let router = router(st.clone());
    let mut shutdown_rx_ = shutdown_rx.clone();
    let http_service = ConvexHttpService::new(
        router,
        "backend",
        SERVER_VERSION_STR.to_string(),
        MAX_CONCURRENT_REQUESTS,
        *HTTP_SERVER_TIMEOUT_DURATION,
        HttpActionRouteMapper,
    );
    let serve_http_future = http_service.serve(config.http_bind_address(), async move {
        let _ = shutdown_rx_.recv().await;
    });
    let proxy_future = dev_site_proxy(
        config.site_bind_address(),
        config.site_forward_prefix(),
        shutdown_rx,
    );

    let serve_future = future::try_join(serve_http_future, proxy_future).fuse();
    futures::pin_mut!(serve_future);

    let mut force_exit_duration = None;
    futures::select! {
        r = serve_future => {
            r?;
            panic!("Serve future stopped unexpectedly");
        },
        _err = preempt_rx.fuse() => {
            tracing::info!("Received a fatal error. Shutting down immediately");
            force_exit_duration = Some(Duration::from_secs(0));
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        },
        r = signal::ctrl_c().fuse() => {
            tracing::info!("Received Ctrl-C signal");
            r?;
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        },
    }

    let shutdown = async move {
        tracing::info!("Shutdown initiated, draining existing requests");
        serve_future.await?;
        tracing::info!("Shutting down application");
        st.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }
    .fuse();
    futures::pin_mut!(shutdown);

    let mut force_exit_future = match force_exit_duration {
        Some(d) => Either::Left(runtime.wait(d)),
        None => Either::Right(std::future::pending()),
    }
    .fuse();

    loop {
        futures::select! {
            r = shutdown => {
                r?;
                tracing::info!("Server successfully shut down");
                if force_exit_duration.is_none() {
                    break;
                }
            },
            _ = force_exit_future => {
                tracing::info!("Cool-down expired, forcing shutdown");
                break;
            },
            r = signal::ctrl_c().fuse() => {
                r?;
                tracing::warn!("Second Ctrl-C — forcibly shutting down");
                break;
            },
        }
    }

    Ok(())
}
