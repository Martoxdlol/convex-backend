//! Coverage for the runner's operational knobs: drain, metrics,
//! circuit breaker, timeout.
//!
//! These features live on `NativeFunctionRunner` and compose
//! through the `dispatch` body on every invocation, so tests here
//! attach the behaviour to the fixture app's real handlers and
//! watch the observable effect. The distributed topology inherits
//! this behaviour through the worker's own `NativeFunctionRunner`,
//! so these assertions don't need mirrors on the distributed side.

use std::{
    sync::Arc,
    time::Duration,
};

use common::types::UdfType;
use convex_native_core::{
    __private::{
        ConvexObject,
        ConvexValue,
        FieldName,
    },
    callbacks::NoopCallbacks,
    circuit_breaker::{
        CircuitBreaker,
        CircuitBreakerConfig,
    },
    metrics::{
        CountingMetrics,
        Outcome,
    },
    NativeActionCallbacks,
    NativeFunctionRunner,
};
use convex_native_integration_tests::db_fixture::DbFixture;
use keybroker::Identity;
use usage_tracking::FunctionUsageTracker;
use value::TableNamespace;

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

fn args(pairs: &[(&str, ConvexValue)]) -> ConvexObject {
    let mut map = std::collections::BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k.parse::<FieldName>().unwrap(), v.clone());
    }
    ConvexObject::try_from(map).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_sink_records_ok_and_err_outcomes() -> anyhow::Result<()> {
    let metrics = Arc::new(CountingMetrics::new());
    let runner =
        Arc::new(NativeFunctionRunner::from_inventory()?.with_metrics(metrics.clone() as _));
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);

    // Successful action dispatch — one OK recorded.
    runner
        .run_action_with_callbacks(
            "echo_action",
            TableNamespace::Global,
            args(&[("message", ConvexValue::try_from("m".to_string())?)]),
            callbacks.clone(),
        )
        .await?;

    // Failing mutation dispatch — one Err recorded.
    let fx = DbFixture::new_in_memory().await?;
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let _ = runner
        .run_mutation(
            "always_bad_request",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await
        .expect_err("mutation errors");

    assert_eq!(
        metrics.count("echo_action", UdfType::Action, Outcome::Ok),
        1,
        "CountingMetrics should record the successful action dispatch",
    );
    assert_eq!(
        metrics.count("always_bad_request", UdfType::Mutation, Outcome::Err),
        1,
        "CountingMetrics should record the failed mutation dispatch",
    );

    // total_latency is a separate accumulator on CountingMetrics;
    // a regression that forgot to update it in NativeMetricsSink::record
    // would leave it at zero even when `count` incremented. Pin a
    // non-zero latency for the successful action dispatch.
    let latency = metrics.total_latency("echo_action", UdfType::Action);
    assert!(
        latency > Duration::ZERO,
        "echo_action's total_latency should accumulate something; got {latency:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn in_flight_counter_increments_during_handler_and_drops_after() -> anyhow::Result<()> {
    // NativeFunctionRunner::in_flight tracks the number of
    // currently-executing handlers. The existing drain test
    // only reads in_flight() == 0 post-completion. This test
    // wraps the runner's action invocation in a task that pauses
    // (sleep_forever is annotated timeout_ms=100 — it'll abort
    // on its own) and reads the counter while the handler is
    // suspended, then asserts it drops back to 0 after the task
    // finishes. A regression that stopped bumping the counter on
    // enter would leave the drain path silently stuck.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);

    assert_eq!(runner.in_flight(), 0, "idle runner has no in-flight work");
    let r2 = runner.clone();
    let cb2 = callbacks.clone();
    let join = tokio::spawn(async move {
        let _ = r2
            .run_action_with_callbacks(
                "sleep_forever",
                TableNamespace::Global,
                ConvexObject::empty(),
                cb2,
            )
            .await;
    });

    // Poll briefly until the counter registers the enqueued call.
    let mut seen_in_flight = false;
    for _ in 0..20 {
        if runner.in_flight() >= 1 {
            seen_in_flight = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        seen_in_flight,
        "in_flight should increment while the handler is executing",
    );
    let _ = join.await;
    assert_eq!(
        runner.in_flight(),
        0,
        "counter must drop back to 0 after the task finishes",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_state_refuses_new_invocations() -> anyhow::Result<()> {
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    runner.begin_drain();
    assert!(runner.is_draining());
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let err = runner
        .run_action_with_callbacks(
            "echo_action",
            TableNamespace::Global,
            args(&[("message", ConvexValue::try_from("x".to_string())?)]),
            callbacks,
        )
        .await
        .expect_err("draining runner rejects new calls");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("draining"),
        "expected draining-rejection error; got: {msg}",
    );
    // In-flight counter stays at 0 because the call never entered
    // the runner.
    assert_eq!(runner.in_flight(), 0);
    // `await_drain` returns true immediately when nothing is in
    // flight.
    assert!(runner.await_drain(Duration::from_millis(100)).await);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn circuit_breaker_stays_closed_below_threshold() -> anyhow::Result<()> {
    // Companion to circuit_breaker_opens_after_threshold_failures:
    // with failure_threshold=3, two failing calls must leave the
    // breaker closed (i.e. a third call reaches the handler and
    // errors with the handler's message, not the breaker's
    // short-circuit message). Guards against an off-by-one that
    // would trip the breaker too eagerly.
    let breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 3,
        cooldown: Duration::from_secs(30),
    }));
    let runner =
        Arc::new(NativeFunctionRunner::from_inventory()?.with_circuit_breaker(breaker.clone()));
    let fx = DbFixture::new_in_memory().await?;
    for _ in 0..2 {
        let usage = FunctionUsageTracker::new();
        let mut tx = fx
            .database
            .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
            .await?;
        let _ = runner
            .run_mutation(
                "always_bad_request",
                &mut tx,
                TableNamespace::Global,
                ConvexObject::empty(),
            )
            .await
            .expect_err("handler-level error");
    }
    // Third call — still below threshold. Handler runs (produces
    // the same bad_request error), *not* the breaker's short-circuit.
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let err = runner
        .run_mutation(
            "always_bad_request",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await
        .expect_err("third call still errors at handler level");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains("circuit breaker"),
        "breaker must be closed below threshold; got breaker error: {msg}",
    );
    assert!(
        msg.contains("deliberately broken"),
        "third call should surface the handler's own error, not the breaker; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn circuit_breaker_opens_after_threshold_failures() -> anyhow::Result<()> {
    let breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 2,
        cooldown: Duration::from_secs(30),
    }));
    let runner =
        Arc::new(NativeFunctionRunner::from_inventory()?.with_circuit_breaker(breaker.clone()));
    let fx = DbFixture::new_in_memory().await?;

    // Two failing calls → breaker opens.
    for _ in 0..2 {
        let usage = FunctionUsageTracker::new();
        let mut tx = fx
            .database
            .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
            .await?;
        let _ = runner
            .run_mutation(
                "always_bad_request",
                &mut tx,
                TableNamespace::Global,
                ConvexObject::empty(),
            )
            .await
            .expect_err("call 1/2 fails");
    }
    // Third call — open breaker short-circuits before the handler
    // even runs, producing the breaker's own error string.
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let err = runner
        .run_mutation(
            "always_bad_request",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await
        .expect_err("open breaker short-circuits");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("circuit breaker"),
        "expected circuit-breaker error; got: {msg}",
    );
    Ok(())
}
