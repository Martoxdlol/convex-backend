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
