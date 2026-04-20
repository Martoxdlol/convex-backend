use convex_native::{
    convex,
    ActionCtx,
    Rt,
};

use crate::app::queries::{
    CountPending,
    CountPendingArgs,
};

/// Action that fans out to a sub-query. Actions sit outside the OCC
/// transaction so they can make external calls, schedule jobs, etc.
#[convex::action]
pub async fn summarise(ctx: &mut ActionCtx<'_, Rt>, owner: String) -> anyhow::Result<i64> {
    let pending = ctx
        .run_query(CountPending, CountPendingArgs { owner })
        .await?;
    Ok(pending)
}
