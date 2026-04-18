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
