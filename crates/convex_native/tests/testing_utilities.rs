//! Smoke test for the `convex_native::testing` module.
//!
//! Demonstrates how a developer would unit-test a native action
//! without writing a full `NativeActionCallbacks` implementation.

use std::sync::Arc;

use convex_native::{
    convex,
    testing::{
        CallRecord,
        TestCallbacks,
    },
    ActionCtx,
    ConvexDocument,
    NativeFunctionRunner,
    Rt,
    ToConvex,
};
use value::{
    ConvexValue,
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
    let args = AuditArgs {
        label: "cleanup".into(),
    };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
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
