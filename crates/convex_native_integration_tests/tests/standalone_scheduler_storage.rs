//! Coverage for `ctx.scheduler()` and `ctx.storage()` via
//! `TestCallbacks`. Both APIs flow through
//! `NativeActionCallbacks`, so the stub captures every call in
//! `CallRecord`.

use std::sync::Arc;

use convex_native_core::{
    __private::ConvexValue,
    testing::{
        CallRecord,
        TestCallbacks,
    },
    NativeActionCallbacks,
    NativeFunctionRunner,
};
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
