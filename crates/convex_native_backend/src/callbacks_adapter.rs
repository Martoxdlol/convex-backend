//! Bridge `convex_native::NativeActionCallbacks` onto the real
//! `udf::ActionCallbacks` trait the backend hands to the function
//! runner.
//!
//! An instance of `BackendCallbacks` is created per action invocation
//! (it captures the `Identity` + `ExecutionContext` the request is
//! running as) and installed on the ActionCtx before dispatch.

use std::{
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
};
use convex_native::{
    NativeActionCallbacks,
    StorageId,
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
    TableNamespace,
};

/// Adapter that implements `NativeActionCallbacks` by delegating to a
/// shared `udf::ActionCallbacks` implementation. The wrapped impl
/// typically comes from the backend's `ApplicationFunctionRunner`.
pub struct BackendCallbacks<RT: Runtime> {
    pub inner: Arc<dyn ActionCallbacks>,
    pub identity: Identity,
    pub context: ExecutionContext,
    _rt: std::marker::PhantomData<RT>,
}

impl<RT: Runtime> BackendCallbacks<RT> {
    pub fn new(
        inner: Arc<dyn ActionCallbacks>,
        identity: Identity,
        context: ExecutionContext,
    ) -> Self {
        Self {
            inner,
            identity,
            context,
            _rt: std::marker::PhantomData,
        }
    }
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
/// name (the JS calling convention). Every native-from-action call
/// is routed through the root component today.
///
/// Bare identifiers like `"get_user"` parse as a JS module with the
/// default export — they'll resolve against the JS module loader at
/// the backend side. For native-to-native dispatch, prefer the typed
/// `ctx.run_query(Marker, Args { .. })` form which bypasses path
/// parsing entirely (tracked in task "Fix native→native cross-call
/// name resolution in BackendCallbacks").
fn path_for(name: &str) -> anyhow::Result<common::components::CanonicalizedComponentFunctionPath> {
    let udf: UdfPath = name.parse()?;
    Ok(common::components::CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: udf.canonicalize(),
    })
}

#[async_trait]
impl<RT: Runtime> NativeActionCallbacks for BackendCallbacks<RT> {
    async fn run_query_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
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
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
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
        // The backend wants a UnixTimestamp wall-clock for the
        // scheduled job; compose from "now + delay" using the
        // runtime from the context.
        let now = UnixTimestamp::from_nanos(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        );
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

    async fn cancel_scheduled(
        &self,
        _ns: TableNamespace,
        id: DeveloperDocumentId,
    ) -> anyhow::Result<()> {
        self.inner.cancel_job(self.identity.clone(), id).await
    }

    async fn storage_store(
        &self,
        _ns: TableNamespace,
        _body: bytes::Bytes,
        _content_type: &str,
    ) -> anyhow::Result<StorageId> {
        // Direct byte uploads don't have a clean counterpart on the
        // JS-side ActionCallbacks trait, which expects the caller to
        // have first uploaded the file via the HTTP storage path and
        // only registers the completed entry here. Surface this as
        // "not yet wired" with a clear error rather than silently
        // fabricating a FileStorageEntry.
        anyhow::bail!(
            "BackendCallbacks::storage_store is not implemented — the backend ActionCallbacks \
             trait only takes pre-uploaded FileStorageEntry values. Upload through the HTTP \
             storage API first, then store the resulting id via the schema."
        )
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
