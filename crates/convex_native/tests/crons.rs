//! Tests `#[convex::cron(...)]` registration.

use convex_native::{
    convex,
    ConvexDocument,
    CronRegistry,
    MutationCtx,
    Rt,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "cron_docs")]
pub struct CronDoc {
    pub tag: String,
}

#[convex::mutation(internal)]
pub async fn nightly_cleanup(_ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<()> {
    Ok(())
}

// The cron attribute attaches to a placeholder item. Its identifier
// is only to give the attribute a slot to sit on.
#[convex::cron(
    name = "nightly-cleanup",
    schedule = "0 3 * * *",
    target = "nightly_cleanup"
)]
#[allow(dead_code)]
fn _nightly_cleanup_cron() {}

#[convex::cron(
    name = "heartbeat",
    schedule = "*/5 * * * *",
    target = "nightly_cleanup"
)]
#[allow(dead_code)]
fn _heartbeat_cron() {}

#[test]
fn crons_are_collected() {
    let registry = CronRegistry::collect().expect("collect");
    assert!(registry.len() >= 2);
    let nightly = registry
        .lookup("nightly-cleanup")
        .expect("nightly-cleanup registered");
    assert_eq!(nightly.schedule, "0 3 * * *");
    assert_eq!(nightly.target, "nightly_cleanup");
    assert_eq!(nightly.target_kind, "mutation");
    let hb = registry.lookup("heartbeat").expect("heartbeat");
    assert_eq!(hb.schedule, "*/5 * * * *");
}
