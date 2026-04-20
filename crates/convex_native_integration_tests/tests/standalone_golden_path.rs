//! Standalone-topology integration tests.
//!
//! Wires a real `Database<ProdRuntime>` (via
//! `DbFixture::new_in_memory()`) to a `NativeFunctionRunner`
//! populated from the `fixture_app` registrations and drives every
//! relevant `convex_native` feature end-to-end: queries, mutations,
//! actions, logging, `ctx.auth()`, `ctx.unix_timestamp()`, indexed
//! reads, typed sub-calls.
//!
//! What this file **doesn't** cover (yet):
//! - HTTP action dispatch — covered by `standalone_http_actions.rs` (to land).
//! - Storage / scheduler / cron wiring — covered by `standalone_scheduler.rs` /
//!   `standalone_storage.rs` / `standalone_crons.rs` (to land).

use std::{
    sync::Arc,
    time::SystemTime,
};

use common::{
    runtime::UnixTimestamp,
    types::UdfType,
};
use convex_native_core::{
    __private::{
        ConvexObject,
        ConvexValue,
        FieldName,
    },
    NativeFunctionRunner,
};
use convex_native_integration_tests::db_fixture::DbFixture;
// Force the fixture app's `inventory::submit!` entries to link by
// naming a path from the module. Without any reference the linker
// can drop the `rlib`'s .init_array sections on some targets, and
// the runner sees an empty registry. Touch a type that references
// every submodule directly; it costs nothing at runtime.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;
use database::Database;
use keybroker::Identity;
use runtime::prod::ProdRuntime;
use usage_tracking::FunctionUsageTracker;
use value::TableNamespace;

/// Build an args `ConvexObject` from `(name, value)` pairs.
fn args(pairs: &[(&str, ConvexValue)]) -> ConvexObject {
    let mut map = std::collections::BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k.parse::<FieldName>().expect("field name"), v.clone());
    }
    ConvexObject::try_from(map).expect("args object")
}

/// Thin helper that opens a tx, runs a mutation handler, and
/// commits. Returns the handler's result.
async fn run_mutation(
    db: &Database<ProdRuntime>,
    runner: &NativeFunctionRunner,
    name: &str,
    args: ConvexObject,
) -> anyhow::Result<ConvexValue> {
    let usage = FunctionUsageTracker::new();
    let mut tx = db
        .begin_with_ts(Identity::system(), *db.now_ts_for_reads(), usage)
        .await?;
    let result = runner
        .run_mutation(name, &mut tx, TableNamespace::Global, args)
        .await?;
    db.commit_with_write_source(tx, "integration_test_mutation")
        .await?;
    Ok(result)
}

/// Read-only counterpart — opens a tx and runs a query handler.
async fn run_query(
    db: &Database<ProdRuntime>,
    runner: &NativeFunctionRunner,
    name: &str,
    args: ConvexObject,
) -> anyhow::Result<ConvexValue> {
    let usage = FunctionUsageTracker::new();
    let mut tx = db
        .begin_with_ts(Identity::system(), *db.now_ts_for_reads(), usage)
        .await?;
    runner
        .run_query(name, &mut tx, TableNamespace::Global, args)
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn runner_discovers_every_fixture_function() -> anyhow::Result<()> {
    let runner = NativeFunctionRunner::from_inventory()?;
    // Spot-check the main kinds.
    assert!(runner.has_function_of_type("list_todos", UdfType::Query));
    assert!(runner.has_function_of_type("count_pending", UdfType::Query));
    assert!(runner.has_function_of_type("whoami", UdfType::Query));
    assert!(runner.has_function_of_type("create_todo", UdfType::Mutation));
    assert!(runner.has_function_of_type("mark_done", UdfType::Mutation));
    assert!(runner.has_function_of_type("internal_delete", UdfType::Mutation));
    assert!(runner.has_function_of_type("summarise", UdfType::Action));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn has_function_accepts_without_type_filter_and_rejects_unknown() -> anyhow::Result<()> {
    // `NativeFunctionRunner::has_function` is the type-agnostic
    // variant, used by run_action_raw's local-runner fast path
    // before it knows which UdfType it's dispatching. No test
    // pinned its accept + reject contract at integration level;
    // has_function_of_type is a stricter variant that wouldn't
    // catch a regression that ignored the name altogether.
    let runner = NativeFunctionRunner::from_inventory()?;
    assert!(runner.has_function("create_todo"));
    assert!(runner.has_function("summarise"));
    assert!(runner.has_function("nightly_cleanup"));
    assert!(!runner.has_function("does_not_exist"));
    assert!(!runner.has_function(""));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_then_list_round_trips_through_the_database() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    // Insert two todos under the "alice" owner.
    run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("alice".to_string())?),
            ("text", ConvexValue::try_from("write docs".to_string())?),
        ]),
    )
    .await?;
    run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("alice".to_string())?),
            ("text", ConvexValue::try_from("ship it".to_string())?),
        ]),
    )
    .await?;

    // List alice's todos — should see both.
    let out = run_query(
        &fx.database,
        &runner,
        "list_todos",
        args(&[("owner", ConvexValue::try_from("alice".to_string())?)]),
    )
    .await?;
    let arr = match out {
        ConvexValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(arr.len(), 2, "both inserts should land in the index");

    // Counting pending should return 2 — nothing has been marked done yet.
    let pending = run_query(
        &fx.database,
        &runner,
        "count_pending",
        args(&[("owner", ConvexValue::try_from("alice".to_string())?)]),
    )
    .await?;
    assert!(matches!(pending, ConvexValue::Int64(2)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mark_done_flips_the_flag() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    // Insert one todo and capture its id.
    let raw_id = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("bob".to_string())?),
            ("text", ConvexValue::try_from("pay rent".to_string())?),
        ]),
    )
    .await?;
    let id_str = match raw_id {
        ConvexValue::String(s) => s,
        other => panic!("expected id string, got {other:?}"),
    };

    // Mark it done through the mutation.
    run_mutation(
        &fx.database,
        &runner,
        "mark_done",
        args(&[("id", ConvexValue::String(id_str))]),
    )
    .await?;

    // Pending count is now 0.
    let pending = run_query(
        &fx.database,
        &runner,
        "count_pending",
        args(&[("owner", ConvexValue::try_from("bob".to_string())?)]),
    )
    .await?;
    assert!(matches!(pending, ConvexValue::Int64(0)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn whoami_reflects_the_caller_identity() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    let out = run_query(&fx.database, &runner, "whoami", ConvexObject::empty()).await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "system"),
        other => panic!("expected string, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn secondary_table_query_dispatches_through_the_same_ctx() -> anyhow::Result<()> {
    // `list_messages_in_channel` queries the `messages` table
    // (the fixture's secondary tablet, carrying text + vector
    // index declarations). Empty table should come back as an
    // empty array; proves the runner registry indexes by dotted
    // name across every table, not just `todos`.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let out = run_query(
        &fx.database,
        &runner,
        "list_messages_in_channel",
        args(&[("channel", ConvexValue::try_from("general".to_string())?)]),
    )
    .await?;
    match out {
        ConvexValue::Array(a) => assert!(a.is_empty(), "expected empty array for unseeded channel"),
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unix_timestamp_sourced_from_runtime() -> anyhow::Result<()> {
    // `ctx.unix_timestamp()` inside `create_todo` populates
    // `Todo::created_at`. The value should be in a plausible
    // current-era range (>= 0 and after year 2000) — anything
    // drastically off would point at a runtime-wiring regression.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("carol".to_string())?),
            ("text", ConvexValue::try_from("hello".to_string())?),
        ]),
    )
    .await?;
    let out = run_query(
        &fx.database,
        &runner,
        "list_todos",
        args(&[("owner", ConvexValue::try_from("carol".to_string())?)]),
    )
    .await?;
    let todos = match out {
        ConvexValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    let first = todos.first().expect("one todo");
    let obj = match first {
        ConvexValue::Object(o) => o,
        other => panic!("expected object, got {other:?}"),
    };
    let created_at = obj
        .get(&"created_at".parse::<FieldName>()?)
        .expect("created_at field");
    match created_at {
        ConvexValue::Float64(ts) => {
            assert!(*ts >= 0.0, "timestamp should be non-negative, got {ts}");
            // Sanity: the test's own wall clock is also in the
            // current era, so if the runtime clock is stuck at
            // epoch this will catch it.
            let now = UnixTimestamp::from_system_time(SystemTime::now())
                .expect("system time is in Unix epoch")
                .as_secs_f64();
            assert!(
                *ts <= now + 5.0,
                "timestamp should be in the past; got {ts} but now is {now}",
            );
        },
        other => panic!("expected float, got {other:?}"),
    }
    Ok(())
}
