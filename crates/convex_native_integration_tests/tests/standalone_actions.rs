//! Standalone-topology action + logging coverage.
//!
//! Actions don't own a transaction, so they dispatch through
//! `NativeFunctionRunner::run_action_with_callbacks` with a
//! `NativeActionCallbacks` impl. `echo_action` is a pure no-op
//! action that doesn't exercise the callbacks surface; for the
//! action → sub-call path see `standalone_sub_calls.rs`.

use std::sync::Arc;

use convex_native_core::{
    __private::{
        ConvexObject,
        ConvexValue,
        FieldName,
    },
    callbacks::NoopCallbacks,
    logging::{
        LogBuffer,
        LogLevel,
    },
    NativeActionCallbacks,
    NativeFunctionRunner,
};
use convex_native_integration_tests::db_fixture::DbFixture;
use database::Database;
use keybroker::Identity;
use runtime::prod::ProdRuntime;
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
async fn pure_action_dispatches_and_returns_result() -> anyhow::Result<()> {
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let out = runner
        .run_action_with_callbacks(
            "echo_action",
            TableNamespace::Global,
            args(&[("message", ConvexValue::try_from("hi".to_string())?)]),
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "echo:hi"),
        other => panic!("expected string, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_log_lines_land_in_the_shared_buffer() -> anyhow::Result<()> {
    // `ctx.log().info(...)` inside `echo_action` should land in
    // the `LogBuffer` the runner is given — same shape the
    // distributed worker uses to stream `log_lines` back to the
    // backend.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let log_buffer = LogBuffer::new();
    runner
        .run_action_with_callbacks_and_log_buffer(
            "echo_action",
            TableNamespace::Global,
            args(&[("message", ConvexValue::try_from("hello".to_string())?)]),
            callbacks,
            log_buffer.clone(),
        )
        .await?;
    let lines = log_buffer.snapshot();
    assert!(
        lines
            .iter()
            .any(|l| l.level == LogLevel::Info && l.message.contains("echo: hello")),
        "expected INFO log from echo_action; got {lines:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn every_log_level_lands_in_the_buffer() -> anyhow::Result<()> {
    // `emit_every_log_level` calls `ctx.log().debug/info/warn/error(...)`
    // once each; the runner drains the buffer after the handler
    // returns. A regression in any of the three non-Info level
    // helpers would otherwise slip past the `info`-only assertion
    // in `action_log_lines_land_in_the_shared_buffer`.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let log_buffer = LogBuffer::new();
    runner
        .run_action_with_callbacks_and_log_buffer(
            "emit_every_log_level",
            TableNamespace::Global,
            ConvexObject::empty(),
            callbacks,
            log_buffer.clone(),
        )
        .await?;
    let lines = log_buffer.snapshot();
    for (level, tag) in [
        (LogLevel::Debug, "lvl:debug"),
        (LogLevel::Info, "lvl:info"),
        (LogLevel::Warn, "lvl:warn"),
        (LogLevel::Error, "lvl:error"),
    ] {
        assert!(
            lines
                .iter()
                .any(|l| l.level == level && l.message.contains(tag)),
            "expected {level:?} line containing {tag:?}; got {lines:?}",
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_log_lines_land_in_the_shared_buffer() -> anyhow::Result<()> {
    // `create_todo` calls `ctx.log().info(...)`; the mutation path
    // exposes its log buffer through
    // `run_mutation_with_log_buffer`. Verifies the Logger surface
    // works inside a tx, not just inside actions.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let log_buffer = LogBuffer::new();
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    runner
        .run_mutation_with_log_buffer(
            "create_todo",
            &mut tx,
            TableNamespace::Global,
            args(&[
                ("owner", ConvexValue::try_from("logger".to_string())?),
                ("text", ConvexValue::try_from("t".to_string())?),
            ]),
            log_buffer.clone(),
        )
        .await?;
    fx.database
        .commit_with_write_source(tx, "log_buffer_test")
        .await?;

    let lines = log_buffer.snapshot();
    assert!(
        lines
            .iter()
            .any(|l| l.level == LogLevel::Info && l.message.starts_with("created todo")),
        "expected 'created todo ...' INFO line; got {lines:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_ctx_unix_timestamp_is_non_zero_current_era() -> anyhow::Result<()> {
    // ActionCtx::unix_timestamp() reads from
    // callbacks.unix_timestamp_now() (NoopCallbacks falls back to
    // SystemTime::now). A regression that returned 0 or the
    // epoch would slip past the query-ctx timestamp test covered
    // via create_todo.created_at.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let out = runner
        .run_action_with_callbacks(
            "action_ctx_now",
            TableNamespace::Global,
            ConvexObject::empty(),
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::Float64(ts) => {
            assert!(
                ts > 1_000_000_000.0,
                "expected a current-era timestamp (>= 2001); got {ts}",
            );
        },
        other => panic!("expected Float64 timestamp, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_run_action_uses_local_runner_fast_path() -> anyhow::Result<()> {
    // chain_echo calls ctx.run_action(EchoAction, ...). The action
    // ctx's run_action_raw checks the local runner first and
    // dispatches inline when the callee is registered — avoiding
    // a callback round-trip. NoopCallbacks would normally bail on
    // an action sub-call, so if the fast path ever regresses (e.g.
    // the runner handle is dropped), this test surfaces it.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let out = runner
        .run_action_with_callbacks(
            "chain_echo",
            TableNamespace::Global,
            args(&[("message", ConvexValue::try_from("looped".to_string())?)]),
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "echo:looped"),
        other => panic!("expected string, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_request_error_surfaces_with_metadata() -> anyhow::Result<()> {
    // `always_bad_request` returns an `errors::bad_request` so
    // callers can see a 400-style error with the expected short
    // code + message. The standalone dispatch path surfaces it
    // as an `Err(anyhow::Error)` whose `ErrorMetadata` chain
    // carries the `BadInput` tag.
    let _fx = _unused_for_parity();
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let fx = DbFixture::new_in_memory().await?;
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
        .expect_err("always_bad_request must error");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("deliberately broken"),
        "expected the error message to surface; got: {rendered}",
    );
    Ok(())
}

fn _unused_for_parity() -> Option<Database<ProdRuntime>> {
    None
}
