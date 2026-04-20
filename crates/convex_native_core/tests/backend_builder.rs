//! Integration test for the `ConvexBackend` builder.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use convex_native_core::{
    convex,
    ActionCtx,
    ConvexBackend,
    ConvexDocument,
    NativeActionCallbacks,
    Rt,
    StorageId,
    ToConvex,
};
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableNamespace,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "cats")]
#[convex(index(name = "by_color", fields = ["color"]))]
pub struct Cat {
    pub color: String,
}

#[convex::action]
pub async fn ping(_ctx: &mut ActionCtx<'_, Rt>, message: String) -> anyhow::Result<String> {
    Ok(format!("pong:{message}"))
}

struct OkCallbacks;

#[async_trait]
impl NativeActionCallbacks for OkCallbacks {
    async fn run_query_by_name(
        &self,
        _ns: TableNamespace,
        _name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        Ok(ConvexValue::Null)
    }

    async fn run_mutation_by_name(
        &self,
        _ns: TableNamespace,
        _name: &str,
        _args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        Ok(ConvexValue::Null)
    }

    async fn schedule(
        &self,
        _ns: TableNamespace,
        _name: &str,
        _args: ConvexObject,
        _delay: std::time::Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        Ok(DeveloperDocumentId::MIN)
    }

    async fn storage_store(
        &self,
        _ns: TableNamespace,
        _body: Bytes,
        _content_type: &str,
    ) -> anyhow::Result<StorageId> {
        Ok(StorageId("x".into()))
    }

    async fn storage_get_url(
        &self,
        _ns: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    async fn storage_delete(&self, _ns: TableNamespace, _id: StorageId) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn builder_runs_action_end_to_end() {
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_callbacks(Arc::new(OkCallbacks))
        .build()
        .expect("build");

    assert!(built.has_runner());
    assert!(built.has_schema());
    assert!(built.has_http());

    // Schema should have the 'cats' table registered.
    let schema = built.schema.as_ref().unwrap();
    assert!(schema.tables.keys().any(|t| t.to_string() == "cats"));

    let args = PingArgs {
        message: "hi".into(),
    };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    let ret = built
        .run_action("ping", TableNamespace::Global, obj)
        .await
        .expect("run");
    let ConvexValue::String(s) = ret else {
        panic!("expected String")
    };
    assert_eq!(s.as_ref(), "pong:hi");

    // Warmup plan surface: our Cat table has one index, so the plan
    // should contain at least that entry.
    let plan = built.warmup_plan();
    assert!(plan
        .iter()
        .any(|e| matches!(e, convex_native_core::WarmupEntry::DbIndex { .. })));
}
