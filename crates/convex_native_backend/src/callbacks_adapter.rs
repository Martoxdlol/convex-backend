//! Bridge `convex_native::NativeActionCallbacks` onto the real
//! `udf::ActionCallbacks` trait the backend hands to the function
//! runner.
//!
//! An instance of `BackendCallbacks` is created per action invocation
//! (it captures the `Identity` + `ExecutionContext` the request is
//! running as) and installed on the ActionCtx before dispatch.

use std::{
    any::TypeId,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use common::{
    components::{
        ComponentId,
        ComponentPath,
    },
    execution_context::ExecutionContext,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    types::RepeatableTimestamp,
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
    Transaction,
    UserFacingModel,
    WriteSource,
};
use file_storage::FileStorage;
use headers::{
    ContentLength,
    ContentType,
};
use keybroker::Identity;
use model::file_storage::FileStorageId;
use sync_types::{
    types::SerializedArgs,
    UdfPath,
};
use udf::ActionCallbacks;
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableName,
    TableNamespace,
};

/// Adapter that implements `NativeActionCallbacks` by delegating to a
/// shared `udf::ActionCallbacks` implementation. The wrapped impl
/// typically comes from the backend's `ApplicationFunctionRunner`.
///
/// When `native` + `database` are provided, `run_query_by_name` /
/// `run_mutation_by_name` short-circuit to the native registry
/// before falling back to the JS `ActionCallbacks` path. This is
/// what makes native-from-native cross-calls by bare identifier
/// (e.g. `"get_user"`) actually land on the native handler instead
/// of trying to load a JS module of the same name.
pub struct BackendCallbacks<RT: Runtime> {
    pub inner: Arc<dyn ActionCallbacks>,
    pub identity: Identity,
    pub context: ExecutionContext,
    /// Native registry to short-circuit when the name is known. Not
    /// wiring this means every sub-call goes through `inner`.
    pub native: Option<Arc<NativeFunctionRunner>>,
    /// Database to open sub-transactions on when dispatching natively.
    pub database: Option<Database<RT>>,
    /// Optional file-storage handle used by `storage_store` to upload
    /// raw bytes directly. When `None`, `storage_store` returns an
    /// error because there's no way to materialise the bytes.
    pub file_storage: Option<FileStorage<RT>>,
    /// Snapshot timestamp shared across native query sub-calls inside
    /// one action dispatch. When `Some`, every `run_query_by_name`
    /// that lands on the native short-circuit opens its transaction
    /// at this fixed timestamp so the action observes one read-time
    /// view across multiple sub-calls (e.g. looking up a user, then
    /// fetching their posts, sees the same world for both).
    ///
    /// Mutations deliberately do **not** honour this — committing at
    /// a stale ts would lose writes — so the timestamp is queries-
    /// only. If a mutation sub-call commits between two queries, the
    /// second query still reads at the pinned snapshot and will not
    /// see the new write.
    pub snapshot_ts: Option<RepeatableTimestamp>,
}

impl<RT: Runtime> BackendCallbacks<RT> {
    /// Construct callbacks that only reach the JS runner. Used for
    /// callers that don't need native cross-call support.
    pub fn new(
        inner: Arc<dyn ActionCallbacks>,
        identity: Identity,
        context: ExecutionContext,
    ) -> Self {
        Self {
            inner,
            identity,
            context,
            native: None,
            database: None,
            file_storage: None,
            snapshot_ts: None,
        }
    }

    /// Construct callbacks that short-circuit known native names to
    /// `native.run_query` / `native.run_mutation` before falling back
    /// to the JS `ActionCallbacks`. Use this variant when dispatching
    /// native actions from the composite runner, so that
    /// `ctx.run_query_by_name("get_user", …)` lands on the native
    /// registration instead of the JS module loader.
    pub fn with_native(
        inner: Arc<dyn ActionCallbacks>,
        identity: Identity,
        context: ExecutionContext,
        native: Arc<NativeFunctionRunner>,
        database: Database<RT>,
    ) -> Self {
        Self {
            inner,
            identity,
            context,
            native: Some(native),
            database: Some(database),
            file_storage: None,
            snapshot_ts: None,
        }
    }

    /// Attach a `FileStorage` handle so `storage_store` uploads raw
    /// bytes directly to the underlying storage backend, returning
    /// a real `StorageId`. Without this, `storage_store` errors.
    pub fn with_file_storage(mut self, file_storage: FileStorage<RT>) -> Self {
        self.file_storage = Some(file_storage);
        self
    }

    /// Pin every native query sub-call to `ts` instead of reading at
    /// the latest timestamp. Set once per action dispatch so multiple
    /// `ctx.run_query(...)` / `ctx.db().get(...)` calls inside one
    /// action see a consistent snapshot. Mutations ignore this by
    /// design (they must commit at a fresh timestamp).
    pub fn with_snapshot_ts(mut self, ts: RepeatableTimestamp) -> Self {
        self.snapshot_ts = Some(ts);
        self
    }
}

/// Run a native query inline against a fresh transaction. Returns
/// `None` when RT ≠ Rt (caller should fall back to the JS path);
/// returns `Some(result)` on native dispatch.
///
/// When `snapshot_ts` is set (typical for sub-calls inside a native
/// action) the transaction opens at that pinned timestamp so every
/// query in the same action observes the same world. When `None`,
/// falls back to `database.now_ts_for_reads()` (latest snapshot) —
/// the shape top-level composite-runner queries use.
///
/// Dropping the transaction discards any reads — queries are
/// read-only by design, so that's the intended behaviour.
async fn try_run_native_query<RT: Runtime + 'static>(
    native: &NativeFunctionRunner,
    database: &Database<RT>,
    identity: &Identity,
    name: &str,
    namespace: TableNamespace,
    args: ConvexObject,
    snapshot_ts: Option<RepeatableTimestamp>,
) -> anyhow::Result<Option<ConvexValue>> {
    if TypeId::of::<RT>() != TypeId::of::<Rt>() {
        return Ok(None);
    }
    let ts = snapshot_ts.unwrap_or_else(|| database.now_ts_for_reads());
    let usage = usage_tracking::FunctionUsageTracker::new();
    let mut tx = database.begin_with_ts(identity.clone(), *ts, usage).await?;
    // SAFETY: TypeId check above ensures RT == Rt.
    let tx_as_rt: &mut Transaction<Rt> =
        unsafe { &mut *((&mut tx) as *mut Transaction<RT> as *mut Transaction<Rt>) };
    let result = native.run_query(name, tx_as_rt, namespace, args).await?;
    Ok(Some(result))
}

/// Run a native mutation inline: open a tx, run the handler, then
/// commit. On success returns the handler's return value; on handler
/// error the tx is dropped without committing.
async fn try_run_native_mutation<RT: Runtime + 'static>(
    native: &NativeFunctionRunner,
    database: &Database<RT>,
    identity: &Identity,
    name: &str,
    namespace: TableNamespace,
    args: ConvexObject,
) -> anyhow::Result<Option<ConvexValue>> {
    if TypeId::of::<RT>() != TypeId::of::<Rt>() {
        return Ok(None);
    }
    let ts = database.now_ts_for_reads();
    let usage = usage_tracking::FunctionUsageTracker::new();
    let mut tx = database.begin_with_ts(identity.clone(), *ts, usage).await?;
    let result = {
        // SAFETY: TypeId check above ensures RT == Rt.
        let tx_as_rt: &mut Transaction<Rt> =
            unsafe { &mut *((&mut tx) as *mut Transaction<RT> as *mut Transaction<Rt>) };
        native.run_mutation(name, tx_as_rt, namespace, args).await?
    };
    database
        .commit_with_write_source(tx, WriteSource::system("convex_native"))
        .await?;
    Ok(Some(result))
}

/// Wrap the single-object `ConvexObject` into the JSON-array shape
/// `SerializedArgs` expects (native handlers accept one object;
/// backend `ActionCallbacks` want an array of JSON values).
fn args_to_serialized(obj: ConvexObject) -> anyhow::Result<SerializedArgs> {
    let arr: ConvexValue = ConvexValue::Object(obj);
    let json: serde_json::Value = arr.into();
    Ok(SerializedArgs::from_args(vec![json])?)
}

/// Build a canonical component function path for a `module:function`
/// name (the JS calling convention). Every JS-fallback call is routed
/// through the root component.
///
/// Native names (bare identifiers like `"get_user"`) are now handled
/// by the native short-circuit in `run_query_by_name` /
/// `run_mutation_by_name`; this function is only reached when the
/// name isn't in the native registry. Typed sub-calls
/// `ctx.run_query(Marker, Args { .. })` bypass name resolution
/// entirely and remain the preferred form for native-to-native
/// cross-calls.
fn path_for(name: &str) -> anyhow::Result<common::components::CanonicalizedComponentFunctionPath> {
    let udf: UdfPath = name.parse()?;
    Ok(common::components::CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: udf.canonicalize(),
    })
}

#[async_trait]
impl<RT: Runtime + 'static> NativeActionCallbacks for BackendCallbacks<RT> {
    async fn run_query_by_name(
        &self,
        ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        // Native short-circuit: if both the registry and a database
        // handle are wired and the name matches a registered query,
        // run it natively and skip the JS module loader entirely.
        if let (Some(native), Some(database)) = (&self.native, &self.database)
            && native.has_function_of_type(name, common::types::UdfType::Query)
            && let Some(v) = try_run_native_query::<RT>(
                native,
                database,
                &self.identity,
                name,
                ns,
                args.clone(),
                self.snapshot_ts,
            )
            .await?
        {
            return Ok(v);
        }

        let path = path_for(name)?;
        let serialized = args_to_serialized(args)?;
        let result = self
            .inner
            .execute_query(
                self.identity.clone(),
                path,
                serialized,
                self.context.clone(),
            )
            .await?;
        match result.result {
            Ok(packed) => packed.unpack(),
            Err(js_error) => Err(anyhow::anyhow!("{js_error}")),
        }
    }

    async fn run_mutation_by_name(
        &self,
        ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        // Native short-circuit: run the mutation inline against a
        // fresh transaction, then commit. The sub-call therefore
        // persists its writes even though it runs outside the
        // enclosing action's transaction (actions don't have one).
        if let (Some(native), Some(database)) = (&self.native, &self.database)
            && native.has_function_of_type(name, common::types::UdfType::Mutation)
            && let Some(v) = try_run_native_mutation::<RT>(
                native,
                database,
                &self.identity,
                name,
                ns,
                args.clone(),
            )
            .await?
        {
            return Ok(v);
        }

        let path = path_for(name)?;
        let serialized = args_to_serialized(args)?;
        let result = self
            .inner
            .execute_mutation(
                self.identity.clone(),
                path,
                serialized,
                self.context.clone(),
            )
            .await?;
        match result.result {
            Ok(packed) => packed.unpack(),
            Err(js_error) => Err(anyhow::anyhow!("{js_error}")),
        }
    }

    async fn schedule(
        &self,
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        let path = path_for(name)?;
        let serialized = args_to_serialized(args)?;
        // Compose "now + delay" against the runtime clock when a
        // Database is wired (so mocked-clock tests see the mocked
        // time), falling back to the wall clock for the
        // JS-only adapter.
        let now = self.unix_timestamp_now();
        let target = now + delay;
        self.inner
            .schedule_job(
                self.identity.clone(),
                ComponentId::Root,
                path,
                serialized,
                target,
                self.context.clone(),
            )
            .await
    }

    fn unix_timestamp_now(&self) -> UnixTimestamp {
        // When a Database handle is wired, use its runtime so tests
        // driving a mocked clock observe the mocked time. Otherwise
        // fall back to the wall clock — callers without a native
        // Database (e.g. JS-only adapters) don't have a Runtime
        // handle to consult.
        if let Some(db) = self.database.as_ref() {
            db.runtime().unix_timestamp()
        } else {
            UnixTimestamp::from_system_time(std::time::SystemTime::now()).unwrap_or_else(|| {
                UnixTimestamp::from_secs_f64(0.0).expect("zero is a valid unix timestamp")
            })
        }
    }

    async fn cancel_scheduled(
        &self,
        _ns: TableNamespace,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        self.inner.cancel_job(self.identity.clone(), id).await
    }

    async fn storage_store(
        &self,
        ns: TableNamespace,
        body: bytes::Bytes,
        content_type: &str,
    ) -> anyhow::Result<StorageId> {
        let file_storage = self.file_storage.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "BackendCallbacks::storage_store requires a FileStorage handle — construct with \
                 .with_file_storage(...) to enable raw-byte uploads from native actions."
            )
        })?;
        // Upload via a single-chunk stream. `FileStorage::store_file`
        // already handles length + content-type + usage tracking.
        let size = body.len();
        let content_length = Some(ContentLength(size as u64));
        let content_type_parsed: Option<ContentType> = if content_type.is_empty() {
            None
        } else {
            Some(content_type.parse()?)
        };
        let stream = futures::stream::once(async move { Ok::<_, anyhow::Error>(body) });
        let usage = usage_tracking::FunctionUsageTracker::new();
        let id = file_storage
            .store_file(
                ns,
                content_length,
                content_type_parsed,
                stream,
                /* expected_sha256 */ None,
                &usage,
            )
            .await?;
        Ok(StorageId(id.to_string()))
    }

    async fn storage_get_url(
        &self,
        _ns: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        let storage_id: FileStorageId = id.0.parse()?;
        self.inner
            .storage_get_url(self.identity.clone(), ComponentId::Root, storage_id)
            .await
    }

    async fn storage_delete(&self, _ns: TableNamespace, id: StorageId) -> anyhow::Result<bool> {
        let storage_id: FileStorageId = id.0.parse()?;
        self.inner
            .storage_delete(self.identity.clone(), ComponentId::Root, storage_id)
            .await?;
        Ok(true)
    }

    async fn storage_get_metadata(
        &self,
        _ns: TableNamespace,
        id: StorageId,
    ) -> anyhow::Result<Option<FileMetadata>> {
        let storage_id: FileStorageId = id.0.parse()?;
        let maybe = self
            .inner
            .storage_get_file_entry(self.identity.clone(), ComponentId::Root, storage_id)
            .await?;
        Ok(maybe.map(|(_component, entry)| FileMetadata {
            content_type: entry.content_type,
            size: entry.size,
            sha256: entry.sha256.as_hex(),
        }))
    }

    async fn read_document_at_snapshot(
        &self,
        namespace: TableNamespace,
        table: TableName,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<Option<ConvexObject>> {
        let _ = table;
        let database = self.database.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "BackendCallbacks::read_document_at_snapshot requires a Database<RT> handle — \
                 construct via with_native(...) to enable native ctx.db() reads from actions."
            )
        })?;
        // Pin to the action's snapshot ts when set so sequential
        // `ctx.db().get(...)` reads inside one action observe a
        // consistent world (matching try_run_native_query /
        // run_query_by_name). Fall back to the latest repeatable ts
        // when no snapshot is pinned.
        let ts = self
            .snapshot_ts
            .unwrap_or_else(|| database.now_ts_for_reads());
        let usage = usage_tracking::FunctionUsageTracker::new();
        let mut tx = database
            .begin_with_ts(self.identity.clone(), *ts, usage)
            .await?;
        let maybe = UserFacingModel::new(&mut tx, namespace)
            .get_with_ts(id, None)
            .await?;
        // Tx is read-only and dropped on return — no commit needed.
        Ok(maybe.map(|(doc, _ts)| {
            let value: common::pii::PII<ConvexObject> = doc.into_value();
            value.0
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use value::FieldName;

    use super::*;

    fn sample_object() -> ConvexObject {
        let mut fields: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        fields.insert(
            "name".parse().unwrap(),
            ConvexValue::try_from("alice".to_string()).unwrap(),
        );
        fields.insert("count".parse().unwrap(), ConvexValue::Int64(7));
        ConvexObject::try_from(fields).unwrap()
    }

    #[test]
    fn args_to_serialized_wraps_object_into_single_element_array() {
        let serialized = args_to_serialized(sample_object()).expect("encode");
        // SerializedArgs round-trips into the raw JSON array; confirm
        // the shape by decoding and inspecting the one entry.
        let parsed: serde_json::Value = serde_json::from_str(serialized.get()).unwrap();
        let arr = parsed.as_array().expect("array");
        assert_eq!(arr.len(), 1, "native handlers take one object arg");
        let obj = arr[0].as_object().expect("object");
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("count"));
    }

    #[test]
    fn path_for_module_colon_function_roots_in_default_component() {
        // UdfPath uses `module:function` syntax (JS convention), not
        // dotted. Native callers of run_query_by_name / etc. need to
        // pass a module-qualified name for cross-module dispatch.
        let p = path_for("users:get").expect("parse");
        assert_eq!(p.component, ComponentPath::root());
        let again = path_for("users:get").expect("parse");
        assert_eq!(p.udf_path, again.udf_path);
    }

    #[test]
    fn path_for_rejects_malformed_input() {
        assert!(path_for("").is_err());
        // A path with an unknown extension is rejected by ModulePath.
        assert!(path_for("users.get").is_err());
    }

    #[test]
    fn path_for_bare_name_parses_as_default_export() {
        // "get_user" (no colon) is interpreted as module "get_user"
        // (`.js` implied) with the default export. This is the JS
        // convention; for native-to-native cross-calls the typed
        // form ctx.run_query(Marker, Args) should be used instead.
        let p = path_for("get_user").expect("parse");
        assert_eq!(p.component, ComponentPath::root());
    }
}
