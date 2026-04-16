//! Tests graceful-shutdown drain on `NativeFunctionRunner`.
//!
//! Per `IMPLEMENTATION_PLAN.md` Phase 4 step 4.3.

use std::{
    sync::Arc,
    time::Duration,
};

use convex_native::{
    convex,
    ActionCtx,
    NativeFunctionRunner,
    Rt,
    ToConvex,
};
use value::{
    ConvexValue,
    TableNamespace,
};

#[convex::action]
pub async fn drain_ok(_ctx: &mut ActionCtx<'_, Rt>, echo: String) -> anyhow::Result<String> {
    Ok(echo)
}

#[tokio::test]
async fn draining_rejects_new_invocations() {
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    // First call succeeds.
    let args = DrainOkArgs { echo: "hi".into() };
    let obj = match args.to_convex().unwrap() {
        ConvexValue::Object(o) => o,
        _ => unreachable!(),
    };
    let _ = runner
        .run_action("drain_ok", TableNamespace::Global, obj.clone())
        .await
        .unwrap();

    assert!(!runner.is_draining());
    runner.begin_drain();
    assert!(runner.is_draining());

    // Any new call is rejected.
    let err = runner
        .run_action("drain_ok", TableNamespace::Global, obj)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("draining"), "got: {err}");
}

#[tokio::test]
async fn await_drain_returns_true_when_no_inflight() {
    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    // in_flight starts at 0 — await_drain should return true immediately.
    let drained = runner.await_drain(Duration::from_millis(100)).await;
    assert!(drained);
    assert_eq!(runner.in_flight(), 0);
}
