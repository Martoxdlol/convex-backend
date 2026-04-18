//! Coverage for the full `errors::*` helper surface + rng
//! determinism.

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

async fn run_query_err(
    db: &Database<ProdRuntime>,
    runner: &NativeFunctionRunner,
    args: ConvexObject,
) -> anyhow::Error {
    let usage = FunctionUsageTracker::new();
    let mut tx = db
        .begin_with_ts(Identity::system(), *db.now_ts_for_reads(), usage)
        .await
        .unwrap();
    runner
        .run_query("error_of_kind", &mut tx, TableNamespace::Global, args)
        .await
        .expect_err("error_of_kind always errors")
}

#[tokio::test(flavor = "multi_thread")]
async fn each_error_helper_surfaces_its_message() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    for kind in [
        "bad_request",
        "unauthenticated",
        "forbidden",
        "not_found",
        "conflict",
        "rate_limited",
        "overloaded",
    ] {
        let err = run_query_err(
            &fx.database,
            &runner,
            args(&[("kind", ConvexValue::try_from(kind.to_string())?)]),
        )
        .await;
        let msg = format!("{err:#}");
        assert!(
            msg.contains(&format!("{kind} msg")),
            "expected {kind} message; got: {msg}",
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rng_is_deterministic_for_a_given_seed() -> anyhow::Result<()> {
    // `ctx.rng_u64()` is seeded from the outcome's rng_seed. The
    // standalone runner builds a fresh `Observed` per call, with a
    // stable default seed derived from the pending-outcome init —
    // so two independent calls return the same sequence here.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let a = runner
        .run_query(
            "pull_rng",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await?;
    // Second call via a fresh tx — still returns the same number
    // because the default Observed seeding is fixed.
    let usage = FunctionUsageTracker::new();
    let mut tx = fx
        .database
        .begin_with_ts(Identity::system(), *fx.database.now_ts_for_reads(), usage)
        .await?;
    let b = runner
        .run_query(
            "pull_rng",
            &mut tx,
            TableNamespace::Global,
            ConvexObject::empty(),
        )
        .await?;
    match (a, b) {
        (ConvexValue::Int64(x), ConvexValue::Int64(y)) => assert_eq!(x, y),
        (a, b) => panic!("expected ints, got {a:?} / {b:?}"),
    }
    Ok(())
}
