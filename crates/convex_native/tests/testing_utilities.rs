//! Smoke test for the `convex_native::testing` module.
//!
//! Demonstrates how a developer would unit-test a native action
//! without writing a full `NativeActionCallbacks` implementation.

use std::sync::Arc;

use convex_native::{
    convex,
    testing::{
        args,
        CallRecord,
        TestCallbacks,
    },
    ActionCtx,
    ConvexDocument,
    Id,
    NativeFunctionRunner,
    Rt,
    ToConvex,
};
use value::{
    ConvexObject,
    ConvexValue,
    DeveloperDocumentId,
    TableNamespace,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "tasks")]
pub struct Task {
    pub label: String,
}

#[convex::action]
pub async fn audit(ctx: &mut ActionCtx<'_, Rt>, label: String) -> anyhow::Result<String> {
    let _ret: i64 = ctx
        .run_query(
            CountTasks,
            CountTasksArgs {
                label: label.clone(),
            },
        )
        .await?;
    ctx.run_mutation(LogTask, LogTaskArgs { label }).await?;
    Ok("audit-ok".into())
}

#[convex::query]
pub async fn count_tasks(
    _ctx: &mut convex_native::QueryCtx<'_, Rt>,
    label: String,
) -> anyhow::Result<i64> {
    let _ = label;
    Ok(0)
}

#[convex::mutation]
pub async fn log_task(
    _ctx: &mut convex_native::MutationCtx<'_, Rt>,
    label: String,
) -> anyhow::Result<()> {
    let _ = label;
    Ok(())
}

#[tokio::test]
async fn test_callbacks_drive_an_action_through_its_paces() {
    let (cb, history) = TestCallbacks::new()
        .on_query("count_tasks", |_args| Ok(ConvexValue::Int64(3)))
        .on_mutation("log_task", |_args| Ok(ConvexValue::Null))
        .build();

    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    // Use the args! macro instead of building the object by hand.
    let obj = args! {
        "label" => "cleanup".to_string(),
    };
    let ret = runner
        .run_action_with_callbacks("audit", TableNamespace::Global, obj, cb)
        .await
        .unwrap();
    let ConvexValue::String(s) = ret else {
        panic!("expected String")
    };
    assert_eq!(s.as_ref(), "audit-ok");

    // Exactly one query + one mutation recorded.
    assert_eq!(history.len(), 2);
    assert_eq!(
        history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "count_tasks")),
        1,
    );
    assert_eq!(
        history.count(|r| matches!(r, CallRecord::Mutation { name, .. } if name == "log_task")),
        1,
    );
}

#[convex::action]
pub async fn fetch_task(ctx: &mut ActionCtx<'_, Rt>, id: Id<Task>) -> anyhow::Result<String> {
    // Exercises `ctx.db().get(id)` — the new snapshot-pinned read
    // path. Returns the fetched label so the test can assert on
    // both the reach-through into the callback *and* the
    // object-to-document decoding.
    let maybe = ctx.db().get(id).await?;
    Ok(maybe
        .map(|t: Task| t.label)
        .unwrap_or_else(|| "<missing>".into()))
}

#[tokio::test]
async fn action_ctx_db_get_round_trips_through_test_callbacks() {
    let stored_label = "cleanup-2";
    // `read_document_at_snapshot` returns a ConvexObject shaped as the
    // Task struct's to_convex output — the test stub is simulating
    // what the backend's BackendCallbacks would fetch from a real
    // snapshot-pinned transaction.
    let stored_obj: ConvexObject = match (Task {
        label: stored_label.into(),
    })
    .to_convex()
    .unwrap()
    {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };

    let (cb, history) = TestCallbacks::new()
        .on_doc_read("tasks", move |_id| Ok(Some(stored_obj.clone())))
        .build();
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));

    // Build a Task id — the developer-facing id shape. We use MIN
    // since the test stub doesn't actually dispatch by id value.
    let id: Id<Task> = Id::new(DeveloperDocumentId::MIN);
    let args_obj = args! {
        "id" => id,
    };
    let ret = runner
        .run_action_with_callbacks("fetch_task", TableNamespace::Global, args_obj, cb)
        .await
        .expect("action ran");
    let ConvexValue::String(s) = ret else {
        panic!("expected String return")
    };
    assert_eq!(s.as_ref(), stored_label);

    assert_eq!(
        history.count(|r| matches!(r, CallRecord::DocRead { table, .. } if table == "tasks")),
        1,
        "exactly one tasks read landed on the callback",
    );
}

#[tokio::test]
async fn action_ctx_db_get_returns_none_when_table_unregistered_on_stub() {
    // Default contract: unregistered tables resolve to `None` rather
    // than bailing, so a test that only cares about one table doesn't
    // have to register every table its action touches.
    let (cb, history) = TestCallbacks::new().build();
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));

    let id: Id<Task> = Id::new(DeveloperDocumentId::MIN);
    let ret = runner
        .run_action_with_callbacks(
            "fetch_task",
            TableNamespace::Global,
            args! { "id" => id },
            cb,
        )
        .await
        .expect("action ran");
    let ConvexValue::String(s) = ret else {
        panic!("expected String return")
    };
    assert_eq!(s.as_ref(), "<missing>");
    // Callback still recorded even though no handler was configured.
    assert_eq!(history.len(), 1);
}
