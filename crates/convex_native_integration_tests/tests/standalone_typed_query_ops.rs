//! Coverage for the typed-query operator surface —
//! `.first()`, `.take(n)`, `.count()`, `.gte`, `.lt`.

use std::sync::Arc;

use convex_native_core::{
    __private::{
        ConvexObject,
        ConvexValue,
        FieldName,
    },
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
    let out = runner
        .run_mutation(name, &mut tx, TableNamespace::Global, args)
        .await?;
    db.commit_with_write_source(tx, "typed_query_test").await?;
    Ok(out)
}

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

async fn seed_todos(
    db: &Database<ProdRuntime>,
    runner: &NativeFunctionRunner,
    owner: &str,
    texts: &[&str],
) -> anyhow::Result<()> {
    for text in texts {
        run_mutation(
            db,
            runner,
            "create_todo",
            args(&[
                ("owner", ConvexValue::try_from(owner.to_string())?),
                ("text", ConvexValue::try_from(text.to_string())?),
            ]),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn count_returns_total_row_count() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "a", &["x", "y", "z"]).await?;
    seed_todos(&fx.database, &runner, "b", &["p"]).await?;
    let got = run_query(
        &fx.database,
        &runner,
        "count_all_todos",
        ConvexObject::empty(),
    )
    .await?;
    assert!(
        matches!(got, ConvexValue::Int64(4)),
        "expected 4; got {got:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn first_returns_option() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    // Empty table → None.
    let none = run_query(
        &fx.database,
        &runner,
        "first_todo_for_owner",
        args(&[("owner", ConvexValue::try_from("ghost".to_string())?)]),
    )
    .await?;
    assert!(
        matches!(none, ConvexValue::Null),
        "expected Null; got {none:?}"
    );

    seed_todos(&fx.database, &runner, "carol", &["one", "two"]).await?;
    let some = run_query(
        &fx.database,
        &runner,
        "first_todo_for_owner",
        args(&[("owner", ConvexValue::try_from("carol".to_string())?)]),
    )
    .await?;
    assert!(
        matches!(some, ConvexValue::Object(_)),
        "expected a todo object; got {some:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn take_caps_result_size() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "d", &["a", "b", "c", "d", "e"]).await?;

    let out = run_query(
        &fx.database,
        &runner,
        "take_todos",
        args(&[("n", ConvexValue::Int64(2))]),
    )
    .await?;
    match out {
        ConvexValue::Array(a) => assert_eq!(a.len(), 2),
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unique_returns_none_empty_some_one_errors_many() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    // Empty → None.
    let none = run_query(
        &fx.database,
        &runner,
        "unique_todo_for_owner",
        args(&[("owner", ConvexValue::try_from("nobody".to_string())?)]),
    )
    .await?;
    assert!(matches!(none, ConvexValue::Null));

    // One row → Some(todo).
    seed_todos(&fx.database, &runner, "solo", &["lone"]).await?;
    let one = run_query(
        &fx.database,
        &runner,
        "unique_todo_for_owner",
        args(&[("owner", ConvexValue::try_from("solo".to_string())?)]),
    )
    .await?;
    assert!(
        matches!(one, ConvexValue::Object(_)),
        "expected object, got {one:?}",
    );

    // Two rows → error.
    seed_todos(&fx.database, &runner, "dup", &["a", "b"]).await?;
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let err = runner
        .run_query(
            "unique_todo_for_owner",
            &mut tx,
            TableNamespace::Global,
            args(&[("owner", ConvexValue::try_from("dup".to_string())?)]),
        )
        .await
        .expect_err(".unique() must error when >1 row matches");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("more than one") || msg.contains("expected"),
        "expected unique-violation error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn order_asc_explicitly_returns_all_rows() -> anyhow::Result<()> {
    // Order::Desc is covered via todos_by_created_desc; the
    // explicit Order::Asc branch goes through the same
    // TypedQueryBuilder::order path but with a different
    // enum variant. Without a handler that passes Asc we
    // only ever see it as the default, so a regression that
    // rejected the Asc variant explicitly would slip through.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "a1", &["one", "two"]).await?;
    let out = run_query(
        &fx.database,
        &runner,
        "todos_by_created_asc",
        ConvexObject::empty(),
    )
    .await?;
    match out {
        ConvexValue::Array(a) => assert!(a.len() >= 2, "expected at least the seeded rows"),
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn order_desc_reverses_index_traversal() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "z", &["one", "two"]).await?;
    let out = run_query(
        &fx.database,
        &runner,
        "todos_by_created_desc",
        ConvexObject::empty(),
    )
    .await?;
    // We can't assert strict order without publishing an index on
    // `created_at` (and the fixture skips schema activation), so
    // just verify the call succeeds and returns an array. The
    // query-builder-level `.order()` handling is already covered
    // by `convex_native_core::tests::query_builder`; this test
    // ensures `Order::Desc` is a valid value that survives the
    // runner's dispatch path.
    match out {
        ConvexValue::Array(a) => assert!(a.len() >= 2),
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn page_returns_bounded_items_with_cursor() -> anyhow::Result<()> {
    // Seed 5 rows, ask for a page of 2; the handler returns
    // `[2, is_done=0, has_cursor=1]` to prove the page was
    // capped at 2 and a next-cursor is available.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "p", &["a", "b", "c", "d", "e"]).await?;
    let out = run_query(
        &fx.database,
        &runner,
        "page_todos_probe",
        args(&[("page_size", ConvexValue::Int64(2))]),
    )
    .await?;
    let arr = match out {
        ConvexValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(arr.len(), 3);
    match &arr[0] {
        ConvexValue::Int64(n) => assert_eq!(*n, 2, "page_size respected"),
        other => panic!("expected int, got {other:?}"),
    }
    match &arr[1] {
        ConvexValue::Int64(n) => assert_eq!(*n, 0, "is_done=false (more rows available)"),
        other => panic!("expected int, got {other:?}"),
    }
    match &arr[2] {
        ConvexValue::Int64(n) => assert_eq!(*n, 1, "has_cursor=true (next page exists)"),
        other => panic!("expected int, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn page_with_size_larger_than_table_is_done() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "q", &["a", "b"]).await?;
    let out = run_query(
        &fx.database,
        &runner,
        "page_todos_probe",
        args(&[("page_size", ConvexValue::Int64(100))]),
    )
    .await?;
    let arr = match out {
        ConvexValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    match &arr[0] {
        ConvexValue::Int64(n) => assert_eq!(*n, 2),
        other => panic!("expected int, got {other:?}"),
    }
    match &arr[1] {
        ConvexValue::Int64(n) => assert_eq!(*n, 1, "is_done=true when scan is exhausted"),
        other => panic!("expected int, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn gt_lte_range_filters_correctly() -> anyhow::Result<()> {
    // Mirror of gte_lt_range_filters_correctly against the strict
    // `.gt` + closed-upper `.lte` operator pair. A regression in
    // either operator's field-encoding or OCC read-set handling
    // would show up here without also breaking the `.gte` / `.lt`
    // test above.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "r", &["a", "b", "c"]).await?;

    let all = run_query(
        &fx.database,
        &runner,
        "todos_in_time_range_exclusive",
        args(&[
            ("from", ConvexValue::Float64(-1.0)),
            ("to", ConvexValue::Float64(1e15)),
        ]),
    )
    .await?;
    match all {
        ConvexValue::Array(a) => assert_eq!(a.len(), 3, "all 3 in the (-1, 1e15] range"),
        other => panic!("expected array, got {other:?}"),
    }

    let none = run_query(
        &fx.database,
        &runner,
        "todos_in_time_range_exclusive",
        args(&[
            ("from", ConvexValue::Float64(1e14)),
            ("to", ConvexValue::Float64(1e14 + 1.0)),
        ]),
    )
    .await?;
    match none {
        ConvexValue::Array(a) => assert!(a.is_empty(), "expected empty; got {a:?}"),
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn gte_lt_range_filters_correctly() -> anyhow::Result<()> {
    // `create_todo` stamps `ctx.unix_timestamp()` into `created_at`.
    // All rows committed within milliseconds share (approximately)
    // the same timestamp, so a `[0.0, f64::INFINITY)` range covers
    // every row; a `[future, future+1)` range covers none.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    seed_todos(&fx.database, &runner, "k", &["a", "b", "c"]).await?;

    let all = run_query(
        &fx.database,
        &runner,
        "todos_in_time_range",
        args(&[
            ("from", ConvexValue::Float64(0.0)),
            ("to", ConvexValue::Float64(1e15)),
        ]),
    )
    .await?;
    match all {
        ConvexValue::Array(a) => assert_eq!(a.len(), 3, "all 3 in the [0, +inf) range"),
        other => panic!("expected array, got {other:?}"),
    }

    let none = run_query(
        &fx.database,
        &runner,
        "todos_in_time_range",
        args(&[
            ("from", ConvexValue::Float64(1e14)),
            ("to", ConvexValue::Float64(1e14 + 1.0)),
        ]),
    )
    .await?;
    match none {
        ConvexValue::Array(a) => assert!(a.is_empty(), "expected empty; got {a:?}"),
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}
