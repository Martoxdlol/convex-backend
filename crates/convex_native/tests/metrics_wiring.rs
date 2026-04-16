//! Tests that the runner records function-level metrics.
//!
//! Per `IMPLEMENTATION_PLAN.md` Phase 4 step 4.2.

use std::sync::Arc;

use common::types::UdfType;
use convex_native::{
    convex,
    ActionCtx,
    ConvexDocument,
    CountingMetrics,
    NativeFunctionRunner,
    Outcome,
    Rt,
    ToConvex,
};
use value::{
    ConvexValue,
    TableNamespace,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "pings")]
pub struct Ping {
    pub note: String,
}

#[convex::action]
pub async fn ok_action(_ctx: &mut ActionCtx<'_, Rt>, echo: String) -> anyhow::Result<String> {
    Ok(echo)
}

#[convex::action]
pub async fn failing_action(_ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    anyhow::bail!("intentional failure")
}

#[convex::action]
pub async fn slow_action(_ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    Ok(())
}

#[tokio::test]
async fn runner_aborts_handlers_past_default_timeout() {
    use value::ConvexObject;

    let runner = Arc::new(
        NativeFunctionRunner::from_inventory()
            .expect("from_inventory")
            .with_default_timeout(std::time::Duration::from_millis(50)),
    );
    let obj =
        ConvexObject::try_from(std::collections::BTreeMap::<value::FieldName, ConvexValue>::new())
            .unwrap();
    let err = runner
        .run_action("slow_action", TableNamespace::Global, obj)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("timed out"), "got: {err}");
}

#[tokio::test]
async fn runner_records_ok_and_err_outcomes() {
    let metrics = Arc::new(CountingMetrics::new());
    let runner = Arc::new(
        NativeFunctionRunner::from_inventory()
            .expect("from_inventory")
            .with_metrics(metrics.clone()),
    );

    // Two successful action invocations.
    for msg in ["a", "b"] {
        let args = OkActionArgs { echo: msg.into() };
        let obj = match args.to_convex().unwrap() {
            ConvexValue::Object(o) => o,
            _ => unreachable!(),
        };
        let _ = runner
            .run_action("ok_action", TableNamespace::Global, obj)
            .await
            .unwrap();
    }

    // One failing action invocation.
    let args = FailingActionArgs {};
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    let _ = runner
        .run_action("failing_action", TableNamespace::Global, obj)
        .await
        .unwrap_err();

    assert_eq!(metrics.count("ok_action", UdfType::Action, Outcome::Ok), 2);
    assert_eq!(
        metrics.count("failing_action", UdfType::Action, Outcome::Err),
        1
    );
    assert_eq!(
        metrics.count("failing_action", UdfType::Action, Outcome::Ok),
        0
    );

    // total_latency is nonzero because we actually awaited futures.
    assert!(
        metrics
            .total_latency("ok_action", UdfType::Action)
            .as_nanos()
            > 0
    );
}
