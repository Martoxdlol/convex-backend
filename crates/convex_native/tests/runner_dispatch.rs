//! Tests `NativeFunctionRunner` dispatch against the inventory.
//!
//! We can exercise name-based lookup, udf-type classification, and error
//! paths (unknown function, wrong kind) without standing up a real
//! `Database<Rt>`. End-to-end execution of a handler against a real
//! transaction will land alongside the full backend wiring (see
//! `convex-native/COMPOSITE_RUNNER.md`).

use common::types::UdfType;
use convex_native::{
    convex,
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

#[test]
fn runner_finds_registered_functions() {
    let runner = NativeFunctionRunner::from_inventory().expect("from_inventory");
    assert!(runner.has_function("tile_query"));
    assert!(runner.has_function("tile_mutation"));
    assert!(!runner.has_function("nonexistent"));
    assert!(runner.has_function_of_type("tile_query", UdfType::Query));
    assert!(runner.has_function_of_type("tile_mutation", UdfType::Mutation));
    assert!(!runner.has_function_of_type("tile_query", UdfType::Mutation));
    // Every listed function should be reachable via `get`.
    let names: Vec<_> = runner.iter().map(|r| r.name).collect();
    assert!(names.contains(&"tile_query"));
    assert!(names.contains(&"tile_mutation"));
}
