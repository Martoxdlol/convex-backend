//! Tests `NativeFunctionRunner` dispatch against the inventory.
//!
//! We can exercise name-based lookup, udf-type classification, and error
//! paths (unknown function, wrong kind) without standing up a real
//! `Database<Rt>`. End-to-end execution of a handler against a real
//! transaction will land alongside the full backend wiring (see
//! `convex-native/COMPOSITE_RUNNER.md`).

use common::types::UdfType;
use convex_native_core::{
    convex,
    ActionCtx,
    ConvexDocument,
    MutationCtx,
    NativeFunctionRunner,
    QueryCtx,
    Rt,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "tiles")]
pub struct Tile {
    pub x: i64,
    pub y: i64,
}

#[convex::query]
pub async fn tile_query(_ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<i64> {
    Ok(7)
}

#[convex::mutation]
pub async fn tile_mutation(_ctx: &mut MutationCtx<'_, Rt>, _x: i64) -> anyhow::Result<()> {
    Ok(())
}

#[convex::action]
pub async fn tile_action(_ctx: &mut ActionCtx<'_, Rt>, _name: String) -> anyhow::Result<i64> {
    Ok(0)
}

#[test]
fn runner_finds_registered_functions() {
    let runner = NativeFunctionRunner::from_inventory().expect("from_inventory");
    assert!(runner.has_function("tile_query"));
    assert!(runner.has_function("tile_mutation"));
    assert!(runner.has_function("tile_action"));
    assert!(!runner.has_function("nonexistent"));
    assert!(runner.has_function_of_type("tile_query", UdfType::Query));
    assert!(runner.has_function_of_type("tile_mutation", UdfType::Mutation));
    assert!(runner.has_function_of_type("tile_action", UdfType::Action));
    assert!(!runner.has_function_of_type("tile_query", UdfType::Mutation));
    // Every listed function should be reachable via `get`.
    let names: Vec<_> = runner.iter().map(|r| r.name).collect();
    assert!(names.contains(&"tile_query"));
    assert!(names.contains(&"tile_mutation"));
    assert!(names.contains(&"tile_action"));
}

#[tokio::test]
async fn action_dispatch_returns_handler_result() {
    use std::{
        collections::BTreeMap,
        sync::Arc,
    };

    use convex_native_core::__private::{
        ConvexObject,
        ConvexValue,
        FieldName,
    };

    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    let mut args: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    args.insert(
        "_name".parse::<FieldName>().unwrap(),
        ConvexValue::try_from("hi".to_string()).unwrap(),
    );
    let obj = ConvexObject::try_from(args).unwrap();
    let got = runner
        .run_action("tile_action", value::TableNamespace::Global, obj)
        .await
        .expect("run_action");
    assert!(matches!(got, ConvexValue::Int64(0)));
}

#[tokio::test]
async fn dispatch_kind_mismatch_errors() {
    use std::{
        collections::BTreeMap,
        sync::Arc,
    };

    use convex_native_core::__private::{
        ConvexObject,
        FieldName,
    };

    let runner = Arc::new(NativeFunctionRunner::from_inventory().expect("from_inventory"));
    let args = ConvexObject::try_from(BTreeMap::<FieldName, _>::new()).unwrap();
    // tile_query is a query — dispatching as action must fail.
    let err = runner
        .run_action("tile_query", value::TableNamespace::Global, args)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("is not an action"));
}
