//! Worker-side `NativeActionCallbacks` adapter.
//!
//! `FunctionExecutionServer` previously installed `NoopCallbacks`
//! for every action, so an action dispatched to a worker that tried
//! `ctx.run_query(...)` / `ctx.run_mutation(...)` / `ctx.scheduler()
//! .run_after(...)` bailed with "no callbacks attached". That
//! made actions with sub-calls a no-op on the distributed path even
//! though the worker has everything it needs.
//!
//! `WorkerActionCallbacks` closes that gap using only the handles
//! the worker already holds:
//!
//! - an `Arc<NativeFunctionRunner>` — for dispatch;
//! - a `Database<Rt>` — for opening fresh transactions and running the
//!   `VirtualSchedulerModel`.
//!
//! It deliberately does *not* fall back to JS-side callbacks the
//! way `convex_native_backend::BackendCallbacks` does — a pure
//! worker process has no JS runtime. Names that aren't in the
//! native registry surface as "no native function registered"
//! errors.

use std::{
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentPath,
    },
    execution_context::{
        ExecutionContext,
        RequestId,
    },
    runtime::{
        Runtime,
        UnixTimestamp,
    },
};
use convex_native::{
    FileMetadata,
    NativeActionCallbacks,
    NativeFunctionRunner,
    Rt,
    StorageId,
};
use database::{
    Database,
    UserFacingModel,
    WriteSource,
};
use keybroker::Identity;
use model::scheduled_jobs::VirtualSchedulerModel;
use sync_types::UdfPath;
use usage_tracking::FunctionUsageTracker;
use value::{
    ConvexArray,
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableName,
    TableNamespace,
};

/// Native-only callbacks that a distributed worker wires into its
/// action dispatch. See module docs for scope.
pub struct WorkerActionCallbacks {
    pub native: Arc<NativeFunctionRunner>,
    pub database: Database<Rt>,
    /// Context used for `schedule` — the caller (server.rs) passes
    /// the one decoded off the proto, so scheduled jobs from inside
    /// an action inherit the conductor's request-id chain.
    pub context: ExecutionContext,
}

impl WorkerActionCallbacks {
    pub fn new(
        native: Arc<NativeFunctionRunner>,
        database: Database<Rt>,
        context: ExecutionContext,
    ) -> Self {
        Self {
            native,
            database,
            context,
        }
    }
}

#[async_trait]
impl NativeActionCallbacks for WorkerActionCallbacks {
    async fn run_query_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let ts = self.database.now_ts_for_reads();
        let usage = FunctionUsageTracker::new();
        let mut tx = self
            .database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        self.native.run_query(name, &mut tx, namespace, args).await
    }

    async fn run_mutation_by_name(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        let ts = self.database.now_ts_for_reads();
        let usage = FunctionUsageTracker::new();
        let mut tx = self
            .database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        let result = self
            .native
            .run_mutation(name, &mut tx, namespace, args)
            .await?;
        self.database
            .commit_with_write_source(tx, WriteSource::system("convex_native_distributed"))
            .await?;
        Ok(result)
    }

    async fn schedule(
        &self,
        namespace: TableNamespace,
        name: &str,
        args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        // Schedules need a transaction — open a short-lived one,
        // record the job through VirtualSchedulerModel, commit.
        let ts = self.database.now_ts_for_reads();
        let usage = FunctionUsageTracker::new();
        let mut tx = self
            .database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        let path = udf_path_for(name)?;
        let arr = args_to_single_arg_array(args)?;
        let now = self.database.runtime().unix_timestamp();
        let target = now + delay;
        let id = VirtualSchedulerModel::new(&mut tx, namespace)
            .schedule(path, arr, target, self.context.clone())
            .await?;
        self.database
            .commit_with_write_source(
                tx,
                WriteSource::system("convex_native_distributed::schedule"),
            )
            .await?;
        Ok(id)
    }

    async fn cancel_scheduled(
        &self,
        namespace: TableNamespace,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        let ts = self.database.now_ts_for_reads();
        let usage = FunctionUsageTracker::new();
        let mut tx = self
            .database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        VirtualSchedulerModel::new(&mut tx, namespace)
            .cancel(id)
            .await?;
        self.database
            .commit_with_write_source(
                tx,
                WriteSource::system("convex_native_distributed::cancel_scheduled"),
            )
            .await?;
        Ok(())
    }

    async fn storage_store(
        &self,
        _namespace: TableNamespace,
        _body: bytes::Bytes,
        _content_type: &str,
    ) -> anyhow::Result<StorageId> {
        anyhow::bail!(
            "WorkerActionCallbacks: storage_store not supported — distributed workers don't own a \
             FileStorage handle"
        )
    }

    async fn storage_get_url(
        &self,
        _namespace: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        anyhow::bail!("WorkerActionCallbacks: storage_get_url not supported on distributed workers")
    }

    async fn storage_delete(
        &self,
        _namespace: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<bool> {
        anyhow::bail!("WorkerActionCallbacks: storage_delete not supported on distributed workers")
    }

    async fn storage_get_metadata(
        &self,
        _namespace: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<Option<FileMetadata>> {
        anyhow::bail!(
            "WorkerActionCallbacks: storage_get_metadata not supported on distributed workers"
        )
    }

    fn unix_timestamp_now(&self) -> UnixTimestamp {
        // The default on the trait returns SystemTime::now(); workers
        // with a Database can honour the runtime clock instead so
        // mocked-clock tests driving through the gRPC path still see
        // the mocked time.
        self.database.runtime().unix_timestamp()
    }

    async fn read_document_at_snapshot(
        &self,
        namespace: TableNamespace,
        table: TableName,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<Option<ConvexObject>> {
        let _ = table;
        let ts = self.database.now_ts_for_reads();
        let usage = FunctionUsageTracker::new();
        let mut tx = self
            .database
            .begin_with_ts(Identity::system(), *ts, usage)
            .await?;
        let maybe = UserFacingModel::new(&mut tx, namespace)
            .get_with_ts(id, None)
            .await?;
        Ok(maybe.map(|(doc, _ts)| {
            let value: common::pii::PII<ConvexObject> = doc.into_value();
            value.0
        }))
    }
}

/// Parse `name` as a UDF path and root it in the default component.
/// Bare identifiers are treated as a default export of module `name`.
fn udf_path_for(name: &str) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
    let udf: UdfPath = name.parse()?;
    Ok(CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: udf.canonicalize(),
    })
}

/// Wrap a single-object `Args` into the `ConvexArray` the scheduler
/// expects (native handlers take exactly one object argument).
fn args_to_single_arg_array(obj: ConvexObject) -> anyhow::Result<ConvexArray> {
    ConvexArray::try_from(vec![ConvexValue::Object(obj)]).map_err(Into::into)
}

/// Convenience constructor for the "no caller context" case — a
/// fresh root context. Server code that has a decoded context from
/// the proto should pass that in instead.
pub fn default_execution_context() -> ExecutionContext {
    ExecutionContext::new_from_parts(RequestId::new(), Default::default(), None, true)
}
