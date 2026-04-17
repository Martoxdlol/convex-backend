//! Tests graceful-shutdown drain on `NativeFunctionRunner`.

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

#[convex::action]
pub async fn always_fails(_ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<()> {
    anyhow::bail!("nope")
}

#[tokio::test]
async fn circuit_breaker_opens_after_threshold() {
    use convex_native::{
        CircuitBreaker,
        CircuitBreakerConfig,
    };
    use value::ConvexObject;

    let cb = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 2,
        cooldown: Duration::from_secs(5),
    }));
    let runner = Arc::new(
        NativeFunctionRunner::from_inventory()
            .expect("from_inventory")
            .with_circuit_breaker(cb.clone()),
    );

    let empty_obj = ConvexObject::try_from(std::collections::BTreeMap::<
        value::FieldName,
        value::ConvexValue,
    >::new())
    .unwrap();

    // Two consecutive failures open the breaker.
    for _ in 0..2 {
        let _ = runner
            .run_action("always_fails", TableNamespace::Global, empty_obj.clone())
            .await
            .unwrap_err();
    }

    // Third call is rejected by the breaker (not by the handler).
    let err = runner
        .run_action("always_fails", TableNamespace::Global, empty_obj)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("circuit breaker"), "got: {err}");
    assert!(cb.is_open("always_fails"));
}
