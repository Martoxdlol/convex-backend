//! Coverage for `ctx.scheduler()` and `ctx.storage()` via
//! `TestCallbacks`. Both APIs flow through
//! `NativeActionCallbacks`, so the stub captures every call in
//! `CallRecord`.

use std::sync::Arc;

use convex_native_core::{
    __private::{
        ConvexObject,
        ConvexValue,
    },
    testing::{
        CallRecord,
        TestCallbacks,
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

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_run_after_reaches_callbacks() -> anyhow::Result<()> {
    let (callbacks, history) = TestCallbacks::new().build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    runner
        .run_action_with_callbacks(
            "schedule_follow_up",
            TableNamespace::Global,
            convex_native_core::testing::args! {},
            callbacks,
        )
        .await?;
    let scheduled = history.count(|r| matches!(r, CallRecord::Schedule { .. }));
    assert_eq!(scheduled, 1);
    let sched_names: Vec<String> = history
        .snapshot()
        .into_iter()
        .filter_map(|r| match r {
            CallRecord::Schedule { name, .. } => Some(name),
            _ => None,
        })
        .collect();
    assert_eq!(sched_names, vec!["nightly_cleanup".to_string()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_under_noop_callbacks_bails_with_guided_error() -> anyhow::Result<()> {
    // Companion to action_scheduler_under_noop_callbacks_bails_with_guided_error:
    // NoopCallbacks::storage_store bails with
    // "no callbacks attached — cannot store in file storage".
    // full_storage_flow exercises it as the very first call, so
    // the handler dies on the first storage hop. A regression
    // that returned a stub Ok from the no-op branch would let
    // callers silently lose their file uploads.
    use convex_native_core::callbacks::NoopCallbacks;
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let err = runner
        .run_action_with_callbacks(
            "full_storage_flow",
            TableNamespace::Global,
            convex_native_core::testing::args! {
                "content_type" => "image/png".to_string(),
            },
            callbacks,
        )
        .await
        .expect_err("storage under NoopCallbacks must fail loudly");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("no callbacks attached") || msg.contains("cannot store"),
        "expected guided NoopCallbacks storage error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_scheduler_under_noop_callbacks_bails_with_guided_error() -> anyhow::Result<()> {
    // NoopCallbacks' default schedule impl bails with
    // "no callbacks attached — cannot schedule {name:?}". A
    // regression that returned Ok from the no-op branch would
    // let unit tests silently pass while handlers tried to
    // schedule jobs that never got persisted. Pin the guided
    // error under NoopCallbacks.
    use convex_native_core::callbacks::NoopCallbacks;
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let err = runner
        .run_action_with_callbacks(
            "schedule_follow_up",
            TableNamespace::Global,
            ConvexObject::empty(),
            callbacks,
        )
        .await
        .expect_err("scheduling under NoopCallbacks must fail loudly");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("no callbacks attached") || msg.contains("cannot schedule"),
        "expected guided NoopCallbacks error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_scheduler_run_action_at_reaches_callbacks() -> anyhow::Result<()> {
    // Fourth and last scheduler entry: run_action_at
    // (action-kind target + absolute timestamp). Complements
    // run_after / run_at / run_action_after. Each entry has
    // independent type-parameter constraints and delegation
    // shapes — this one routes through run_action_after under
    // the hood but still requires its own wrapper body. Pin the
    // delay computation lands the expected "internal_action"
    // schedule entry.
    let (callbacks, history) = TestCallbacks::new().build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    runner
        .run_action_with_callbacks(
            "schedule_action_at_absolute_time",
            TableNamespace::Global,
            convex_native_core::testing::args! {},
            callbacks,
        )
        .await?;
    let scheduled = history
        .snapshot()
        .into_iter()
        .filter_map(|r| match r {
            CallRecord::Schedule { name, delay } => Some((name, delay)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].0, "internal_action");
    let delay = scheduled[0].1;
    assert!(
        delay <= std::time::Duration::from_secs(121),
        "run_action_at with timestamp = now+120 should compute a delay <= 121s; got {delay:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_scheduler_run_at_reaches_callbacks_with_computed_delay() -> anyhow::Result<()> {
    // Scheduler::run_at on the action-ctx side computes the
    // delay as (timestamp - callbacks.unix_timestamp_now()) and
    // delegates to run_after. TestCallbacks' NoopCallbacks
    // fallback uses SystemTime::now, so the recorded delay
    // should land between 0s and ~3600s (the fixture schedules
    // one hour out; the test-level clock can drift a handful of
    // milliseconds at most between the handler's timestamp read
    // and the callback's now read).
    let (callbacks, history) = TestCallbacks::new().build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    runner
        .run_action_with_callbacks(
            "schedule_at_absolute_time_action",
            TableNamespace::Global,
            convex_native_core::testing::args! {},
            callbacks,
        )
        .await?;
    let scheduled = history
        .snapshot()
        .into_iter()
        .filter_map(|r| match r {
            CallRecord::Schedule { name, delay } => Some((name, delay)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].0, "nightly_cleanup");
    let delay = scheduled[0].1;
    assert!(
        delay <= std::time::Duration::from_secs(3601),
        "run_at with timestamp = now+3600 should compute a delay <= 3601s; got {delay:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn action_scheduler_run_action_after_reaches_callbacks() -> anyhow::Result<()> {
    // Mirror of scheduler_run_after_reaches_callbacks, but
    // through the *run_action_after* entry point on the
    // action-ctx scheduler. A regression that wired only the
    // mutation typed entry point into the callbacks layer would
    // pass the run_after test while silently dropping every
    // action schedule_by_name.
    let (callbacks, history) = TestCallbacks::new().build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    runner
        .run_action_with_callbacks(
            "schedule_follow_up_action",
            TableNamespace::Global,
            convex_native_core::testing::args! {},
            callbacks,
        )
        .await?;
    let scheduled = history
        .snapshot()
        .into_iter()
        .filter_map(|r| match r {
            CallRecord::Schedule { name, delay } => Some((name, delay)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].0, "internal_action");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_store_get_url_metadata_delete_all_flow() -> anyhow::Result<()> {
    let (callbacks, history) = TestCallbacks::new()
        .with_storage_url(Some("https://files/test"))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let got = runner
        .run_action_with_callbacks(
            "full_storage_flow",
            TableNamespace::Global,
            convex_native_core::testing::args! { "content_type" => "image/png".to_string() },
            callbacks,
        )
        .await?;
    // `full_storage_flow` returns the url from `get_url`.
    match got {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "https://files/test"),
        ConvexValue::Null => panic!("expected a url; got Null"),
        other => panic!("expected string, got {other:?}"),
    }
    assert_eq!(
        history.count(|r| matches!(r, CallRecord::StorageStore { .. })),
        1,
    );
    assert_eq!(
        history.count(|r| matches!(r, CallRecord::StorageGetUrl { .. })),
        1,
    );
    assert_eq!(
        history.count(|r| matches!(r, CallRecord::StorageGetMetadata { .. })),
        1,
    );
    assert_eq!(
        history.count(|r| matches!(r, CallRecord::StorageDelete { .. })),
        1,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_get_url_returns_none_when_builder_opts_out() -> anyhow::Result<()> {
    // `TestCallbacks::new().with_storage_url(None)` configures
    // the stub to return `None` from `storage_get_url`.
    // `full_storage_flow` propagates that Option through to its
    // own return value, so the handler comes back as `Null`.
    // Pins the "no url minted" path, distinct from the default-
    // url path covered by storage_store_get_url_metadata_delete_all_flow.
    let (callbacks, _history) = TestCallbacks::new().with_storage_url(None).build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let out = runner
        .run_action_with_callbacks(
            "full_storage_flow",
            TableNamespace::Global,
            convex_native_core::testing::args! { "content_type" => "image/png".to_string() },
            callbacks,
        )
        .await?;
    assert!(
        matches!(out, ConvexValue::Null),
        "with_storage_url(None) should propagate through storage_get_url; got {out:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_get_metadata_round_trips_content_type() -> anyhow::Result<()> {
    // The existing full_storage_flow test pins the *call* landed
    // in history but discards the returned FileMetadata. The
    // TestCallbacks default stub returns a canned metadata with
    // content_type=Some("application/octet-stream"). Round-trip
    // the accessor through the action ctx to prove the Option<
    // FileMetadata> wire shape is wired correctly — a regression
    // collapsing Some to None would pass the call-count test but
    // break any deployer relying on get_metadata values.
    let (callbacks, _history) = TestCallbacks::new().build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let out = runner
        .run_action_with_callbacks(
            "storage_metadata_probe",
            TableNamespace::Global,
            convex_native_core::testing::args! {
                "content_type" => "image/webp".to_string(),
            },
            callbacks,
        )
        .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(
            s.to_string(),
            "application/octet-stream",
            "TestCallbacks' default FileMetadata content_type round-trips through the action",
        ),
        ConvexValue::Null => panic!("metadata content_type was lost across the callbacks boundary"),
        other => panic!("expected string or null, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_scheduler_writes_job_to_transaction() -> anyhow::Result<()> {
    // The mutation-ctx scheduler writes through `VirtualSchedulerModel`
    // onto the mutation's own transaction, so the scheduled job
    // commits atomically with the mutation. Run `schedule_from_mutation`
    // against the fixture's live DB and assert the handler returns a
    // non-empty job id — the id-round-trip proves the write landed
    // on the tx before commit.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let out = runner
        .run_mutation(
            "schedule_from_mutation",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await?;
    fx.database
        .commit_with_write_source(tx, "scheduler_mutation_test")
        .await?;
    let id = match out {
        ConvexValue::String(s) => s.to_string(),
        other => panic!("expected scheduled id string, got {other:?}"),
    };
    assert!(
        !id.is_empty(),
        "scheduler returned an empty id — the schedule path did not mint one",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_scheduler_cancel_is_idempotent() -> anyhow::Result<()> {
    // `MutationScheduler::cancel(id)` routes through
    // `VirtualSchedulerModel::cancel` on the mutation's own tx.
    // `schedule_then_cancel` schedules once and cancels twice —
    // the second cancel must succeed (idempotent) because real
    // caller code can race the scheduler worker firing the job.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let out = runner
        .run_mutation(
            "schedule_then_cancel",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await?;
    fx.database
        .commit_with_write_source(tx, "scheduler_cancel_test")
        .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "ok"),
        other => panic!("expected 'ok', got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_scheduler_run_at_accepts_absolute_timestamp() -> anyhow::Result<()> {
    // MutationScheduler::run_at converts the absolute timestamp
    // into a delay against the *runtime* clock (not SystemTime::now
    // — so mocked-runtime tests stay deterministic) before
    // delegating to run_after. This test just asserts the call
    // succeeds and mints an id; deeper delay-math coverage lives
    // in convex_native_core::ctx::scheduler::tests.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let out = runner
        .run_mutation(
            "schedule_at_absolute_time",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await?;
    fx.database
        .commit_with_write_source(tx, "scheduler_run_at_test")
        .await?;
    match out {
        ConvexValue::String(s) => assert!(!s.is_empty(), "job id must be non-empty"),
        other => panic!("expected string id, got {other:?}"),
    }
    Ok(())
}
