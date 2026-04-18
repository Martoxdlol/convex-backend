//! Coverage for `ctx.db()` single-doc read APIs —
//! `get` / `exists` / `normalize_id`.

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
    db.commit_with_write_source(tx, "db_api_test").await?;
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

#[tokio::test(flavor = "multi_thread")]
async fn get_then_exists_on_real_id() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    let id = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("eve".to_string())?),
            ("text", ConvexValue::try_from("x".to_string())?),
        ]),
    )
    .await?;
    let id_str = match &id {
        ConvexValue::String(s) => s.to_string(),
        other => panic!("expected id, got {other:?}"),
    };

    let got = run_query(
        &fx.database,
        &runner,
        "get_todo",
        args(&[("id", ConvexValue::try_from(id_str.clone())?)]),
    )
    .await?;
    assert!(
        matches!(got, ConvexValue::Object(_)),
        "get_todo on a real id returns Some(obj); got {got:?}",
    );

    let exists = run_query(
        &fx.database,
        &runner,
        "todo_exists",
        args(&[("id", ConvexValue::try_from(id_str)?)]),
    )
    .await?;
    assert!(matches!(exists, ConvexValue::Boolean(true)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn normalize_id_rejects_garbage() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let out = run_query(
        &fx.database,
        &runner,
        "normalize_todo_id",
        args(&[("raw", ConvexValue::try_from("not-an-id".to_string())?)]),
    )
    .await?;
    assert!(
        matches!(out, ConvexValue::Boolean(false)),
        "normalize_id rejects garbage; got {out:?}",
    );
    Ok(())
}
