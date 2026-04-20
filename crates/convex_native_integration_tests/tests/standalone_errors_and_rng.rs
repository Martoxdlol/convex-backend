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

#[tokio::test(flavor = "multi_thread")]
async fn mutation_ctx_rng_u64_is_deterministic() -> anyhow::Result<()> {
    // QueryCtx::rng_u64 is pinned by rng_is_deterministic_for_a_given_seed.
    // MutationCtx has a separate impl block forwarding to the
    // same Observed PRNG — exercise it through
    // pull_rng_mutation to make sure the mutation-ctx delegation
    // isn't dropped.
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    async fn call(
        db: &Database<ProdRuntime>,
        runner: &NativeFunctionRunner,
    ) -> anyhow::Result<i64> {
        let usage = FunctionUsageTracker::new();
        let mut tx = db
            .begin_with_ts(Identity::system(), *db.now_ts_for_reads(), usage)
            .await?;
        let v = runner
            .run_mutation(
                "pull_rng_mutation",
                &mut tx,
                TableNamespace::Global,
                ConvexObject::empty(),
            )
            .await?;
        db.commit_with_write_source(tx, "rng_mutation_test").await?;
        match v {
            ConvexValue::Int64(n) => Ok(n),
            other => anyhow::bail!("expected int, got {other:?}"),
        }
    }

    let a = call(&fx.database, &runner).await?;
    let b = call(&fx.database, &runner).await?;
    assert_eq!(
        a, b,
        "mutation-ctx rng_u64 must be deterministic across calls"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rng_fill_is_deterministic_and_non_empty() -> anyhow::Result<()> {
    // Companion to `rng_is_deterministic_for_a_given_seed` but for
    // the byte-buffer surface `ctx.rng_fill(&mut buf)`. Two
    // independent calls with the framework's default Observed
    // seeding must produce the same 16-byte hex string, and it
    // must not be the all-zero sentinel (which would signal the
    // fill path silently did nothing).
    let fx = DbFixture::new_in_memory().await?;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);

    async fn call(
        db: &Database<ProdRuntime>,
        runner: &NativeFunctionRunner,
    ) -> anyhow::Result<String> {
        let usage = FunctionUsageTracker::new();
        let mut tx = db
            .begin_with_ts(Identity::system(), *db.now_ts_for_reads(), usage)
            .await?;
        let v = runner
            .run_query(
                "pull_rng_bytes",
                &mut tx,
                TableNamespace::Global,
                ConvexObject::empty(),
            )
            .await?;
        match v {
            ConvexValue::String(s) => Ok(s.to_string()),
            other => anyhow::bail!("expected string, got {other:?}"),
        }
    }

    let a = call(&fx.database, &runner).await?;
    let b = call(&fx.database, &runner).await?;
    assert_eq!(a, b, "rng_fill must be deterministic across calls");
    assert_eq!(a.len(), 32, "16 bytes → 32 hex chars");
    assert_ne!(
        a,
        "0".repeat(32),
        "rng_fill produced all zeros — likely a no-op",
    );
    Ok(())
}
