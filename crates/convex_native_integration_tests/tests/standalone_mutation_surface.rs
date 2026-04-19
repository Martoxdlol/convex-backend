//! Coverage for the mutation-side db API —
//! `replace`, `delete`, `patch`.

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
    db.commit_with_write_source(tx, "mutation_surface_test")
        .await?;
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
async fn replace_overwrites_document_in_place() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    let id = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("fran".to_string())?),
            ("text", ConvexValue::try_from("draft".to_string())?),
        ]),
    )
    .await?;
    let id_str = match &id {
        ConvexValue::String(s) => s.to_string(),
        other => panic!("expected id, got {other:?}"),
    };

    run_mutation(
        &fx.database,
        &runner,
        "replace_todo",
        args(&[
            ("id", ConvexValue::try_from(id_str.clone())?),
            ("owner", ConvexValue::try_from("fran".to_string())?),
            ("text", ConvexValue::try_from("final".to_string())?),
            ("done", ConvexValue::Boolean(true)),
        ]),
    )
    .await?;

    let got = run_query(
        &fx.database,
        &runner,
        "get_todo",
        args(&[("id", ConvexValue::try_from(id_str)?)]),
    )
    .await?;
    let obj = match got {
        ConvexValue::Object(o) => o,
        other => panic!("expected object, got {other:?}"),
    };
    let done = obj.get(&"done".parse::<FieldName>()?).unwrap();
    assert!(
        matches!(done, ConvexValue::Boolean(true)),
        "replace_todo flipped `done` to true; got {done:?}",
    );
    let text = obj.get(&"text".parse::<FieldName>()?).unwrap();
    match text {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "final"),
        other => panic!("expected 'final', got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn internal_delete_removes_document() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    let id = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("gus".to_string())?),
            ("text", ConvexValue::try_from("x".to_string())?),
        ]),
    )
    .await?;
    let id_str = match &id {
        ConvexValue::String(s) => s.to_string(),
        other => panic!("expected id, got {other:?}"),
    };

    run_mutation(
        &fx.database,
        &runner,
        "internal_delete",
        args(&[("id", ConvexValue::try_from(id_str.clone())?)]),
    )
    .await?;

    let got = run_query(
        &fx.database,
        &runner,
        "get_todo",
        args(&[("id", ConvexValue::try_from(id_str)?)]),
    )
    .await?;
    assert!(
        matches!(got, ConvexValue::Null),
        "internal_delete should remove the row; get_todo returned {got:?}",
    );
    Ok(())
}
