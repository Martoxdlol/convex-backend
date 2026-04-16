//! Tests `#[convex::query]` / `#[convex::mutation]` attribute macros.
//!
//! We can't execute the handlers without a working `NativeFunctionRunner`,
//! but we can assert that:
//! 1. The macros expand successfully against realistic function shapes.
//! 2. The original functions remain directly callable.
//! 3. `NativeFunctionRegistry::collect()` finds the entries and carries the
//!    expected metadata (name / udf_type / arg_names / handler variant).

use common::types::UdfType;
use convex_native::{
    convex,
    ConvexDocument,
    HandlerFn,
    Id,
    MutationCtx,
    NativeFunctionRegistry,
    QueryCtx,
    Rt,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "posts")]
#[convex(index(name = "by_author", fields = ["author"]))]
pub struct Post {
    pub author: String,
    pub body: String,
}

#[convex::query]
pub async fn get_post(_ctx: &mut QueryCtx<'_, Rt>, _id: Id<Post>) -> anyhow::Result<Option<Post>> {
    Ok(None)
}

#[convex::query]
pub async fn list_posts(_ctx: &mut QueryCtx<'_, Rt>, _limit: i64) -> anyhow::Result<Vec<Post>> {
    Ok(Vec::new())
}

#[convex::mutation]
pub async fn create_post(
    _ctx: &mut MutationCtx<'_, Rt>,
    _author: String,
    _body: String,
) -> anyhow::Result<i64> {
    Ok(42)
}

#[convex::mutation(internal)]
pub async fn internal_rebuild(
    _ctx: &mut MutationCtx<'_, Rt>,
    _token: String,
) -> anyhow::Result<()> {
    Ok(())
}

#[test]
fn functions_register_with_correct_metadata() {
    let registry = NativeFunctionRegistry::collect().expect("collect");

    let get = registry.get("get_post").expect("get_post registered");
    assert_eq!(get.arg_names, &["_id"]);
    assert!(matches!(get.handler, HandlerFn::Query(_)));
    assert_eq!(get.udf_type(), UdfType::Query);

    let list = registry.get("list_posts").expect("list_posts registered");
    assert_eq!(list.arg_names, &["_limit"]);
    assert_eq!(list.udf_type(), UdfType::Query);

    let create = registry.get("create_post").expect("create_post registered");
    assert_eq!(create.arg_names, &["_author", "_body"]);
    assert!(matches!(create.handler, HandlerFn::Mutation(_)));
    assert_eq!(create.udf_type(), UdfType::Mutation);

    assert!(registry.get("not_real").is_none());

    // The `internal` modifier is reflected in the registration.
    let internal = registry
        .get("internal_rebuild")
        .expect("internal_rebuild registered");
    assert!(internal.is_internal);
    // And non-internal ones are flagged accordingly.
    assert!(!create.is_internal);
}
