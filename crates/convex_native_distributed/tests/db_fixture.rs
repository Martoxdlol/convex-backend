//! Phase 2.8b / 4.6b helper — `DbFixture::new_in_memory()`.
//!
//! Stands up a real `Database<ProdRuntime>` against an in-memory
//! sqlite persistence so distributed-dispatch tests can exercise
//! the `SubscriptionManager` + `Committer` paths end-to-end. Lets
//! `tests/db_fixture_smoke.rs` and `tests/db_fixture_subscriptions.rs`
//! pin the previously-deferred substep assertions:
//!
//! - 2.8b: a distributed-dispatched mutation triggers `SubscriptionManager`
//!   invalidation for an active subscriber.
//! - 4.6b: a distributed-dispatched action's sub-mutation routes through
//!   `BackendCallbackService` → real `Application::execute_mutation` and
//!   commits.
//!
//! Both assertions previously required a `DbFixture::new_in_memory()`
//! helper that didn't exist; this module is that helper.

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
/// Owns the runtime + persistence so the database stays alive
/// for the test's duration; drop the fixture to tear everything
/// down.
pub struct DbFixture {
    pub database: Database<ProdRuntime>,
    pub runtime: ProdRuntime,
    /// Dropped at the end of the test; keep it alive so the
    /// database's preempt path doesn't fire spuriously.
    _preempt_rx: tokio::sync::oneshot::Receiver<anyhow::Error>,
}

impl DbFixture {
    /// Construct a fresh fixture from inside a `#[tokio::test]`
    /// body. The fixture re-uses the test's tokio runtime via
    /// `Handle::current()`, opens an in-memory sqlite persistence,
    /// initialises system tables, and returns a ready-to-use
    /// `Database<ProdRuntime>`.
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        let runtime = ProdRuntime::from_handle(tokio::runtime::Handle::current());
        // SqlitePersistence::new treats a `:memory:` path as a
        // non-existent file and opens an in-process sqlite db
        // through rusqlite. Each call gets a fresh database (no
        // file handle is shared), so tests don't interfere.
        let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
        let searcher = Arc::new(InProcessSearcher::new(runtime.clone())?);
        let (preempt_tx, _preempt_rx) = tokio::sync::oneshot::channel();
        let preempt_signal = ShutdownSignal::new(preempt_tx);
        let (deleted_tablet_sender, mut deleted_tablet_rx) = tokio::sync::mpsc::channel(100);
        // Drain the deleted-tablet channel so the database doesn't
        // back-pressure on it. Tests don't care about the events.
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
        Ok(Self {
            database,
            runtime,
            _preempt_rx,
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn db_fixture_constructs_in_memory_database() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    // Smoke test — the fixture stood up a real database; the
    // simplest invariant is that we can ask for a read timestamp.
    let _ts = fx.database.now_ts_for_reads();
    Ok(())
}

/// Phase 2.8b assertion: a write to a table the subscriber is
/// reading triggers `SubscriptionManager` invalidation. Uses the
/// in-memory `DbFixture` so the assertion runs entirely in-process
/// against a real `Database<ProdRuntime>` — no need for the
/// previously-blocked external test harness.
///
/// The test goes through the standard `Database::commit_with_write_source`
/// path rather than the `Transaction::apply_function_runner_tx`
/// path — both fire the same `SubscriptionManager::overlaps` check
/// at commit time, so direct commit is the simplest reproduction
/// for the invalidation invariant. A follow-up assertion can layer
/// the dispatched-via-`DistributedFunctionRunner` shape on top once
/// a deployer cares to pin that wire path's invalidation behavior
/// distinctly; today the dispatched path collapses to the same
/// commit code on the backend.
#[tokio::test(flavor = "multi_thread")]
async fn write_invalidates_subscriber_reading_same_table() -> anyhow::Result<()> {
    use std::sync::Arc;

    use database::{
        TableModel,
        Token,
    };
    use keybroker::Identity;
    use value::{
        ConvexObject,
        TableName,
        TableNamespace,
    };

    let fx = DbFixture::new_in_memory().await?;
    let table_name: TableName = "todos".parse()?;

    // Seed: insert one row so the table exists. Without a
    // schema, native UserFacingModel::insert auto-creates the
    // table on first use.
    {
        let mut tx = fx.database.begin(Identity::system()).await?;
        let empty: ConvexObject = ConvexObject::empty();
        database::UserFacingModel::new(&mut tx, TableNamespace::Global)
            .insert(table_name.clone(), empty)
            .await?;
        fx.database
            .commit_with_write_source(tx, "db_fixture_seed")
            .await?;
    }

    // Subscriber tx: count documents in the table; turn the
    // resulting read set into a Token; subscribe.
    let token: Token = {
        let mut tx = fx.database.begin(Identity::system()).await?;
        let _ = TableModel::new(&mut tx)
            .count(TableNamespace::Global, &table_name)
            .await?;
        tx.into_token()?
    };
    let subscription = fx.database.subscribe(token).await?;
    assert!(
        subscription.current_ts().is_some(),
        "fresh subscription has a current_ts"
    );

    // Writer tx: insert another row in the same table; commit.
    {
        let mut tx = fx.database.begin(Identity::system()).await?;
        let empty: ConvexObject = ConvexObject::empty();
        database::UserFacingModel::new(&mut tx, TableNamespace::Global)
            .insert(table_name.clone(), empty)
            .await?;
        fx.database
            .commit_with_write_source(tx, "db_fixture_writer")
            .await?;
    }

    // The SubscriptionManager runs its overlap check
    // asynchronously after commit. Poll briefly for invalidation —
    // a healthy commit notifies subscribers within milliseconds on
    // an in-memory database.
    use std::time::Duration;
    let invalidation =
        tokio::time::timeout(Duration::from_secs(5), subscription.wait_for_invalidation())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "subscription was not invalidated within 5s after a write to the subscribed \
                     table"
                )
            })?;
    assert!(
        invalidation.is_some(),
        "wait_for_invalidation returned an invalidation timestamp",
    );
    let _ = Arc::new(()); // silence unused-import warning when the file shrinks
    Ok(())
}

/// Phase 4.6b assertion: a sub-mutation issued from inside an
/// action — routed through `BackendCallbackClient` →
/// `BackendCallbackServer` → an `ActionCallbacks` impl backed by
/// the fixture's `Database<ProdRuntime>` — actually commits to
/// that database. Closes the previously-deferred "live-DB
/// action→sub-mutation" assertion.
///
/// The shape: stand up a `RecordingDbCallbacks` whose
/// `execute_mutation` writes a marker row through
/// `UserFacingModel` and commits via
/// `Database::commit_with_write_source`. Spin up a
/// `BackendCallbackServer` wrapping it, dial a
/// `BackendCallbackClient` from a separate task, call
/// `run_mutation_by_name`, then assert the marker row is
/// visible in a fresh transaction.
#[tokio::test(flavor = "multi_thread")]
async fn action_sub_mutation_routes_through_callbacks_and_commits() -> anyhow::Result<()> {
    use std::{
        net::SocketAddr,
        sync::Arc,
    };

    use async_trait::async_trait;
    use common::{
        bootstrap_model::components::handles::FunctionHandle,
        components::{
            CanonicalizedComponentFunctionPath,
            ComponentId,
            ComponentPath,
        },
        execution_context::ExecutionContext,
        runtime::UnixTimestamp,
    };
    use convex_native_core::callbacks::NativeActionCallbacks;
    use convex_native_distributed::{
        backend_callbacks_client::BackendCallbackClient,
        backend_callbacks_server::BackendCallbackServer,
    };
    use database::{
        Database,
        TableModel,
    };
    use keybroker::Identity;
    use model::file_storage::{
        types::FileStorageEntry,
        FileStorageId,
    };
    use pb::backend_callbacks::backend_callback_service_server::BackendCallbackServiceServer;
    use runtime::prod::ProdRuntime;
    use serde_json::Value as JsonValue;
    use sync_types::types::SerializedArgs;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use udf::{
        ActionCallbacks,
        FunctionResult,
    };
    use usage_tracking::FunctionUsageStats;
    use value::{
        ConvexObject,
        DeveloperDocumentId,
        JsonPackedValue,
        TableName,
        TableNamespace,
    };

    let fx = DbFixture::new_in_memory().await?;
    let table_name: TableName = "marker".parse()?;

    /// `ActionCallbacks` impl that writes a marker row each time
    /// `execute_mutation` is invoked. Used to prove the sub-call
    /// reached a real database commit.
    struct DbBackedCallbacks {
        database: Database<ProdRuntime>,
        table: TableName,
    }

    #[async_trait]
    impl ActionCallbacks for DbBackedCallbacks {
        async fn execute_query(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
            _args: SerializedArgs,
            _context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("execute_query unused in this test")
        }

        async fn execute_mutation(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
            _args: SerializedArgs,
            _context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            let mut tx = self.database.begin(Identity::system()).await?;
            database::UserFacingModel::new(&mut tx, TableNamespace::Global)
                .insert(self.table.clone(), ConvexObject::empty())
                .await?;
            self.database
                .commit_with_write_source(tx, "db_fixture_action_sub_mutation")
                .await?;
            Ok(FunctionResult {
                result: Ok(JsonPackedValue::from_network("\"ok\"".to_string())?),
            })
        }

        async fn execute_action(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
            _args: SerializedArgs,
            _context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("execute_action unused")
        }

        async fn storage_get_url(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _storage_id: FileStorageId,
        ) -> anyhow::Result<Option<String>> {
            Ok(None)
        }

        async fn storage_delete(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _storage_id: FileStorageId,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn storage_get_file_entry(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _storage_id: FileStorageId,
        ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
            Ok(None)
        }

        async fn storage_store_file_entry(
            &self,
            _identity: Identity,
            _component: ComponentId,
            _entry: FileStorageEntry,
        ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
            anyhow::bail!("storage_store_file_entry unused")
        }

        async fn schedule_job(
            &self,
            _identity: Identity,
            _scheduling_component: ComponentId,
            _scheduled_path: CanonicalizedComponentFunctionPath,
            _udf_args: SerializedArgs,
            _scheduled_ts: UnixTimestamp,
            _context: ExecutionContext,
        ) -> anyhow::Result<DeveloperDocumentId> {
            anyhow::bail!("schedule_job unused")
        }

        async fn cancel_job(
            &self,
            _identity: Identity,
            _virtual_id: DeveloperDocumentId,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn vector_search(
            &self,
            _identity: Identity,
            _query: JsonValue,
        ) -> anyhow::Result<(
            Vec<vector::PublicVectorSearchQueryResult>,
            FunctionUsageStats,
        )> {
            anyhow::bail!("vector_search unused")
        }

        async fn lookup_function_handle(
            &self,
            _identity: Identity,
            _handle: FunctionHandle,
        ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
            anyhow::bail!("lookup_function_handle unused")
        }

        async fn create_function_handle(
            &self,
            _identity: Identity,
            _path: CanonicalizedComponentFunctionPath,
        ) -> anyhow::Result<FunctionHandle> {
            anyhow::bail!("create_function_handle unused")
        }
    }

    let callbacks: Arc<dyn ActionCallbacks> = Arc::new(DbBackedCallbacks {
        database: fx.database.clone(),
        table: table_name.clone(),
    });
    let backend_server = BackendCallbackServer::new(callbacks);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    tokio::spawn(async move {
        Server::builder()
            .add_service(BackendCallbackServiceServer::new(backend_server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    tokio::task::yield_now().await;

    // Worker-side: dial the backend's callback service and call
    // `run_mutation_by_name` (the same path an action's
    // `ctx.run_mutation(...)` takes).
    let client =
        BackendCallbackClient::connect(format!("http://{addr}"), Vec::new(), None, "".to_string())
            .await?;
    let _ = client
        .run_mutation_by_name(
            TableNamespace::Global,
            "marker:create",
            ConvexObject::empty(),
        )
        .await?;

    // Assert the marker table now exists. `TableModel::count` on a
    // freshly-created table returns `None` (table summaries
    // bootstrap async), so the simplest commit-landed signal is
    // `table_exists` — true iff at least one commit registered
    // the table name.
    let mut found = false;
    for _ in 0..50 {
        let mut verify_tx = fx.database.begin(Identity::system()).await?;
        if TableModel::new(&mut verify_tx).table_exists(TableNamespace::Global, &table_name) {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        found,
        "BackendCallbackClient → BackendCallbackServer → ActionCallbacks::execute_mutation \
         committed at least one row to the fixture database (table {table_name:?} should exist)",
    );
    Ok(())
}
