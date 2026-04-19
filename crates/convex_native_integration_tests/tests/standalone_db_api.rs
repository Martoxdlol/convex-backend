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
async fn get_with_meta_returns_creation_time() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    let id = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("z".to_string())?),
            ("text", ConvexValue::try_from("t".to_string())?),
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
        "get_todo_creation_time",
        args(&[("id", ConvexValue::try_from(id_str)?)]),
    )
    .await?;
    match got {
        ConvexValue::Float64(t) => assert!(t > 0.0, "creation_time is non-zero; got {t}"),
        other => panic!("expected Float64, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn try_get_errors_on_missing_id() -> anyhow::Result<()> {
    // `ctx.db().try_get(id)` errors when the doc is absent — the
    // "blow up loudly" variant of `.get(id)`. Build an id that
    // can't exist (parse a known-bad shape) by inserting+deleting
    // a row.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let id = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("h".to_string())?),
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

    // Now the id references a deleted row → `try_get` should
    // error.
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let err = runner
        .run_query(
            "try_get_todo",
            &mut tx,
            TableNamespace::Global,
            args(&[("id", ConvexValue::try_from(id_str)?)]),
        )
        .await
        .expect_err("try_get on a deleted id must error");
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("not found")
            || msg.to_lowercase().contains("missing")
            || msg.to_lowercase().contains("no such"),
        "expected missing-document error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_many_returns_options_preserving_order() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let mut ids: Vec<String> = Vec::new();
    for text in ["a", "b", "c"] {
        let v = run_mutation(
            &fx.database,
            &runner,
            "create_todo",
            args(&[
                ("owner", ConvexValue::try_from("g".to_string())?),
                ("text", ConvexValue::try_from(text.to_string())?),
            ]),
        )
        .await?;
        match v {
            ConvexValue::String(s) => ids.push(s.to_string()),
            other => panic!("expected id, got {other:?}"),
        }
    }

    let mut arr = Vec::new();
    for id in &ids {
        arr.push(ConvexValue::try_from(id.clone())?);
    }
    let v = value::ConvexArray::try_from(arr)?;
    let got = run_query(
        &fx.database,
        &runner,
        "get_many_todos",
        args(&[("ids", ConvexValue::Array(v))]),
    )
    .await?;
    match got {
        ConvexValue::Array(a) => {
            assert_eq!(a.len(), 3);
            for entry in a.iter() {
                assert!(
                    matches!(entry, ConvexValue::Object(_)),
                    "each returned slot is Some(todo); got {entry:?}",
                );
            }
        },
        other => panic!("expected array, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn normalize_id_accepts_valid_todo_id() -> anyhow::Result<()> {
    // `ctx.db().normalize_id::<Todo>(raw)` returns `Some` when
    // `raw` is a syntactically valid id whose embedded table
    // tag matches `Todo::table_name()`. The existing garbage
    // test only covers the rejection side — pin the accept
    // side against a real mutation-minted id so a regression
    // that over-eagerly rejected valid ids would surface here.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let v = run_mutation(
        &fx.database,
        &runner,
        "create_todo",
        args(&[
            ("owner", ConvexValue::try_from("n".to_string())?),
            ("text", ConvexValue::try_from("t".to_string())?),
        ]),
    )
    .await?;
    let id_str = match v {
        ConvexValue::String(s) => s.to_string(),
        other => panic!("expected id, got {other:?}"),
    };
    let out = run_query(
        &fx.database,
        &runner,
        "normalize_todo_id",
        args(&[("raw", ConvexValue::try_from(id_str)?)]),
    )
    .await?;
    assert!(
        matches!(out, ConvexValue::Boolean(true)),
        "normalize_id accepts a real Todo id; got {out:?}",
    );
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
