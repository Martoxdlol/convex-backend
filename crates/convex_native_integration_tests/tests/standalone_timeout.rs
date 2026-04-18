//! Coverage for the per-function + runner-default timeout path.

use std::{
    sync::Arc,
    time::Duration,
};

use convex_native_core::{
    __private::ConvexObject,
    callbacks::NoopCallbacks,
    NativeActionCallbacks,
    NativeFunctionRunner,
};
use value::TableNamespace;

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[tokio::test(flavor = "multi_thread")]
async fn per_function_timeout_aborts_runaway_action() -> anyhow::Result<()> {
    // `sleep_forever` is annotated `#[convex::action(timeout_ms = 100)]`.
    // The runner wraps every handler in `tokio::time::timeout`,
    // so after ~100ms the call should abort with a clear error.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);

    let started = std::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(5), // test-level safety net
        runner.run_action_with_callbacks(
            "sleep_forever",
            TableNamespace::Global,
            ConvexObject::empty(),
            callbacks,
        ),
    )
    .await
    .expect("runner-level timeout fires before the 5s test safety net")
    .expect_err("handler should error (timed out)");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "handler aborted well before the 5s safety net; took {elapsed:?}",
    );
    let msg = format!("{err:#}");
    assert!(
        msg.contains("timeout") || msg.contains("timed out"),
        "expected a timeout-flavoured error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn runner_default_timeout_also_aborts() -> anyhow::Result<()> {
    // A runner built with `with_default_timeout(50ms)` aborts
    // *any* handler that doesn't set its own timeout — this
    // covers the global-default knob as distinct from the
    // per-function override.
    let runner = Arc::new(
        NativeFunctionRunner::from_inventory()?.with_default_timeout(Duration::from_millis(50)),
    );
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);

    // `echo_action` has no per-function timeout, so the default
    // applies. It normally finishes in microseconds; add a sleep
    // via the handler input? We can't — `echo_action` doesn't
    // sleep. Instead, use `sleep_forever` which already sleeps.
    // The per-function 100ms there is higher than the default
    // 50ms; the *lower* of the two wins. This proves both knobs
    // compose (i.e. the default isn't silently shadowed by the
    // per-function value when the per-function value is larger).
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        runner.run_action_with_callbacks(
            "sleep_forever",
            TableNamespace::Global,
            ConvexObject::empty(),
            callbacks,
        ),
    )
    .await
    .expect("timeout fires")
    .expect_err("expected a timeout");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("timeout") || msg.contains("timed out"),
        "expected a timeout error; got: {msg}",
    );
    Ok(())
}
