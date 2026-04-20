//! In-memory `Database<ProdRuntime>` fixture shared across every
//! integration test in this crate.
//!
//! Stands up a real `Database<ProdRuntime>` against sqlite's
//! `:memory:` backend so tests can exercise the Committer,
//! SubscriptionManager, and index paths without any external
//! dependencies. Mirrors
//! `convex_native_distributed::tests::db_fixture::DbFixture`; kept
//! as a library type here so every test file in this crate can
//! share the same wiring.

use std::sync::Arc;

use common::{
    knobs::DOCUMENT_RETENTION_RATE_LIMIT,
    runtime::new_rate_limiter,
    shutdown::ShutdownSignal,
};
use database::Database;
use governor::Quota;
use indexing::index_cache::SharedIndexCache;
use model::virtual_system_mapping;
use runtime::prod::ProdRuntime;
use search::searcher::InProcessSearcher;
use sqlite::SqlitePersistence;

/// In-memory sqlite-backed `Database<ProdRuntime>` test fixture.
///
/// Construct one inside a `#[tokio::test(flavor = "multi_thread")]`
/// body — it reuses the current tokio runtime via
/// `Handle::current()`.
pub struct DbFixture {
    pub database: Database<ProdRuntime>,
    pub runtime: ProdRuntime,
    _preempt_rx: tokio::sync::oneshot::Receiver<anyhow::Error>,
}

impl DbFixture {
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        let runtime = ProdRuntime::from_handle(tokio::runtime::Handle::current());
        let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
        let searcher = Arc::new(InProcessSearcher::new(runtime.clone())?);
        let (preempt_tx, _preempt_rx) = tokio::sync::oneshot::channel();
        let preempt_signal = ShutdownSignal::new(preempt_tx);
        let (deleted_tablet_sender, mut deleted_tablet_rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move { while deleted_tablet_rx.recv().await.is_some() {} });
        let rate_limiter = Arc::new(new_rate_limiter(
            runtime.clone(),
            Quota::per_second(*DOCUMENT_RETENTION_RATE_LIMIT),
        ));
        let database = Database::load(
            persistence,
            runtime.clone(),
            searcher,
            preempt_signal,
            virtual_system_mapping().clone(),
            Some(SharedIndexCache),
            rate_limiter,
            deleted_tablet_sender,
        )
        .await?;
        model::initialize_application_system_tables(&database).await?;
        // NOTE: `publish_native_schema` is deliberately **not**
        // called here. Activating a `DatabaseSchema` with indexes
        // declared requires the `SchemaWorker` + `IndexWorker` +
        // `SearchAndVectorBootstrapWorker` trio, which together
        // pull in the entire `Application` wiring. The fixture
        // keeps boot cheap by leaving the schema pending and
        // relying on full-table scans in the fixture queries —
        // indexes are still exercised at the *declaration* level
        // (compile-time field validation, `NativeSchema::collect`,
        // `BuiltBackend::warmup_plan`) by the tests in
        // `convex_native_core` and at the *admission envelope*
        // level by `convex_native_distributed::admission::tests`.
        // A follow-up `fixture_indexes.rs` module can layer the
        // full workers stack once a test needs to prove
        // `.with_index(...)` dispatch works end-to-end.
        Ok(Self {
            database,
            runtime,
            _preempt_rx,
        })
    }
}
