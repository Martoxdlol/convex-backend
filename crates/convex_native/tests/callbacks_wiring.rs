//! Tests `NativeActionCallbacks` wiring through `ActionCtx`,
//! `Scheduler`, and `StorageCtx`.
//!
//! The tests implement a `MockCallbacks` that captures incoming calls
//! and returns canned results, then verify that typed sub-calls,
//! scheduler calls, and storage calls round-trip through the callback.

use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use convex_native::{
    convex,
    ctx::action::ActionCtx,
    ConvexDocument,
    MutationCtx,
    NativeActionCallbacks,
    NativeFunctionRunner,
    QueryCtx,
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
#[convex(table = "widgets2")]
pub struct Widget2 {
    pub name: String,
}

#[convex::query]
pub async fn count(_ctx: &mut QueryCtx<'_, Rt>, suffix: String) -> anyhow::Result<i64> {
    let _ = suffix;
    Ok(0)
}

#[convex::mutation]
pub async fn bump(_ctx: &mut MutationCtx<'_, Rt>, by: i64) -> anyhow::Result<i64> {
    let _ = by;
    Ok(0)
}

#[convex::action]
pub async fn workflow(ctx: &mut ActionCtx<'_, Rt>, input: String) -> anyhow::Result<String> {
    // Invoke a sub-query and a sub-mutation through the typed markers.
    let qn: i64 = ctx
        .run_query(
            Count,
            CountArgs {
                suffix: input.clone(),
            },
        )
        .await?;
    let mn: i64 = ctx.run_mutation(Bump, BumpArgs { by: qn }).await?;

    // And schedule a future mutation.
    ctx.scheduler()
        .run_after(Duration::from_secs(60), Bump, BumpArgs { by: mn })
        .await?;

    // Return the suffix concatenation as proof of flow.
    Ok(format!("{input}:{qn}:{mn}"))
}

#[derive(Default)]
struct MockState {
    query_calls: Vec<(String, ConvexObject)>,
    mutation_calls: Vec<(String, ConvexObject)>,
    schedule_calls: Vec<(String, Duration)>,
}

struct MockCallbacks {
    state: Mutex<MockState>,
}

#[async_trait]
impl NativeActionCallbacks for MockCallbacks {
    async fn run_query_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.state
            .lock()
            .unwrap()
            .query_calls
            .push((name.to_string(), args));
        Ok(ConvexValue::Int64(42))
    }

    async fn run_mutation_by_name(
        &self,
        _ns: TableNamespace,
        name: &str,
        args: ConvexObject,
    ) -> anyhow::Result<ConvexValue> {
        self.state
            .lock()
            .unwrap()
            .mutation_calls
            .push((name.to_string(), args));
        Ok(ConvexValue::Int64(7))
    }

    async fn schedule(
        &self,
        _ns: TableNamespace,
        name: &str,
        _args: ConvexObject,
        delay: Duration,
    ) -> anyhow::Result<DeveloperDocumentId> {
        self.state
            .lock()
            .unwrap()
            .schedule_calls
            .push((name.to_string(), delay));
        // Return a dummy id — we don't inspect it here.
        Ok(DeveloperDocumentId::MIN)
    }

    async fn storage_store(
        &self,
        _ns: TableNamespace,
        _body: Bytes,
        _content_type: &str,
    ) -> anyhow::Result<StorageId> {
        Ok(StorageId("mock-id".into()))
    }

    async fn storage_get_url(
        &self,
        _ns: TableNamespace,
        _id: StorageId,
    ) -> anyhow::Result<Option<String>> {
        Ok(Some("https://mock/url".into()))
    }

    async fn storage_delete(&self, _ns: TableNamespace, _id: StorageId) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[tokio::test]
async fn action_with_callbacks_drives_typed_sub_calls() {
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    let mock = Arc::new(MockCallbacks {
        state: Mutex::new(MockState::default()),
    });

    let args = WorkflowArgs { input: "go".into() };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };

    let ret = runner
        .run_action_with_callbacks("workflow", TableNamespace::Global, obj, mock.clone())
        .await
        .expect("action ran");

    let ConvexValue::String(s) = ret else {
        panic!("expected String return");
    };
    assert_eq!(s.as_ref(), "go:42:7");

    let state = mock.state.lock().unwrap();
    assert_eq!(state.query_calls.len(), 1, "one sub-query");
    assert_eq!(state.query_calls[0].0, "count");
    assert_eq!(state.mutation_calls.len(), 1, "one sub-mutation");
    assert_eq!(state.mutation_calls[0].0, "bump");
    assert_eq!(state.schedule_calls.len(), 1, "one scheduled job");
    assert_eq!(state.schedule_calls[0].0, "bump");
    assert_eq!(state.schedule_calls[0].1, Duration::from_secs(60));
}

#[tokio::test]
async fn storage_helpers_go_through_callbacks() {
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    // Register a small storage-exercising action.
    #[convex::action]
    pub async fn upload(ctx: &mut ActionCtx<'_, Rt>, body: String) -> anyhow::Result<String> {
        let id = ctx.storage().store(Bytes::from(body), "text/plain").await?;
        let url = ctx.storage().get_url(id.clone()).await?.unwrap_or_default();
        ctx.storage().delete(id).await?;
        Ok(url)
    }
    let mock = Arc::new(MockCallbacks {
        state: Mutex::new(MockState::default()),
    });
    let args = UploadArgs {
        body: "content".into(),
    };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    let ret = runner
        .run_action_with_callbacks("upload", TableNamespace::Global, obj, mock)
        .await
        .unwrap();
    let ConvexValue::String(s) = ret else {
        panic!("expected String")
    };
    assert_eq!(s.as_ref(), "https://mock/url");
}

#[tokio::test]
async fn noop_callbacks_surface_clear_error() {
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    // Run the workflow action with default (noop) callbacks; the
    // sub-query call must surface a clear error.
    let args = WorkflowArgs { input: "x".into() };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    let err = runner
        .run_action("workflow", TableNamespace::Global, obj)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no callbacks attached"));
}
