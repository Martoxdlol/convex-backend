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
async fn writes_are_visible_within_the_same_mutation_tx() -> anyhow::Result<()> {
    // A mutation that inserts a row and then reads it back in
    // the same `ctx.db()` sees the inserted row — the framework's
    // transactional-consistency contract. A regression here would
    // show up as `insert_then_read` returning `false`.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let got = run_mutation(
        &fx.database,
        &runner,
        "insert_then_read",
        args(&[("owner", ConvexValue::try_from("i".to_string())?)]),
    )
    .await?;
    assert!(
        matches!(got, ConvexValue::Boolean(true)),
        "expected true (inserted row read back in same tx); got {got:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn option_arg_decodes_both_some_and_none() -> anyhow::Result<()> {
    // The #[convex::mutation] macro generates an args struct
    // whose Option<String> fields decode absent-or-Null values
    // into None and present-as-String into Some. Both branches
    // live in the macro expansion's FromConvex impl. Round-trip
    // echo_optional_owner through both shapes.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let some = run_mutation(
        &fx.database,
        &runner,
        "echo_optional_owner",
        args(&[("owner", ConvexValue::try_from("alice".to_string())?)]),
    )
    .await?;
    match some {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "alice"),
        other => panic!("expected alice, got {other:?}"),
    }
    // Omit the arg entirely — the macro-generated FromConvex
    // should treat missing Option<T> args as None.
    let none = run_mutation(
        &fx.database,
        &runner,
        "echo_optional_owner",
        ConvexObject::empty(),
    )
    .await?;
    match none {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "none"),
        other => panic!("expected 'none', got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_struct_arg_round_trips_through_convex_value() -> anyhow::Result<()> {
    // echo_metadata takes `Metadata` (a ConvexNested struct with
    // an embedded ConvexEnum field) as an arg. The macro-
    // generated args struct routes it through the nested
    // FromConvex impl — a code path none of the existing tests
    // exercise at the arg layer. A regression that mis-wired
    // nested decoding would silently flatten or corrupt the
    // Priority enum variant.
    use std::collections::BTreeMap;

    use value::FieldName;
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let mut nested: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    nested.insert("source".parse()?, ConvexValue::try_from("web".to_string())?);
    nested.insert(
        "priority".parse()?,
        ConvexValue::try_from("high".to_string())?,
    );
    let mut arg_map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    arg_map.insert(
        "md".parse()?,
        ConvexValue::Object(ConvexObject::try_from(nested)?),
    );
    let out = run_mutation(
        &fx.database,
        &runner,
        "echo_metadata",
        ConvexObject::try_from(arg_map)?,
    )
    .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "web/High"),
        other => panic!("expected 'web/High', got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn convex_union_as_arg_decodes_both_variants() -> anyhow::Result<()> {
    // Companion to query_returning_convex_union_round_trips_via_runner:
    // the arg side of ConvexUnion, where the macro-generated args
    // struct decodes the tag + body from ConvexValue::Object. A
    // regression that mis-routed variants by tag would pass the
    // return-type test but corrupt incoming arg values.
    use std::collections::BTreeMap;

    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    // Email variant → expect "email:x@y"
    let mut email_inner: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    email_inner.insert("kind".parse()?, ConvexValue::try_from("email".to_string())?);
    email_inner.insert("to".parse()?, ConvexValue::try_from("x@y".to_string())?);
    let mut arg_map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    arg_map.insert(
        "note".parse()?,
        ConvexValue::Object(ConvexObject::try_from(email_inner)?),
    );
    let out = run_mutation(
        &fx.database,
        &runner,
        "echo_notification",
        ConvexObject::try_from(arg_map)?,
    )
    .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "email:x@y"),
        other => panic!("expected email, got {other:?}"),
    }

    // Push variant → expect "push:tok"
    let mut push_inner: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    push_inner.insert("kind".parse()?, ConvexValue::try_from("push".to_string())?);
    push_inner.insert("token".parse()?, ConvexValue::try_from("tok".to_string())?);
    let mut arg_map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    arg_map.insert(
        "note".parse()?,
        ConvexValue::Object(ConvexObject::try_from(push_inner)?),
    );
    let out = run_mutation(
        &fx.database,
        &runner,
        "echo_notification",
        ConvexObject::try_from(arg_map)?,
    )
    .await?;
    match out {
        ConvexValue::String(s) => assert_eq!(s.to_string(), "push:tok"),
        other => panic!("expected push, got {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn query_returning_convex_union_round_trips_via_runner() -> anyhow::Result<()> {
    // fetch_notification returns Notification — a ConvexUnion
    // variant. The runner's response serialisation calls the
    // generated ToConvex::to_convex on the return value; this
    // test pins that the response tag/body survive into the
    // dispatched ConvexValue. A regression that mis-serialised
    // ConvexUnion on return would pass the derive-level
    // round-trip test but break every deployer whose query
    // returns a tagged union.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    for (kind, expected_tag) in [("email", "email"), ("anything_else", "push")] {
        let out = run_query(
            &fx.database,
            &runner,
            "fetch_notification",
            args(&[("kind", ConvexValue::try_from(kind.to_string())?)]),
        )
        .await?;
        let obj = match out {
            ConvexValue::Object(o) => o,
            other => panic!("expected object, got {other:?}"),
        };
        let tag = obj
            .get(&"kind".parse::<FieldName>()?)
            .expect("tag field present");
        match tag {
            ConvexValue::String(s) => assert_eq!(s.to_string(), expected_tag),
            other => panic!("expected tag string, got {other:?}"),
        }
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
