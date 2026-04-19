//! Distributed-topology admission coverage.
//!
//! Worker registration sends a `RegistrationEnvelope` containing
//! the output of `collect_inventory()` — functions, schema, HTTP
//! routes, crons. Under the fixture app that envelope is
//! non-empty and hashes deterministically across successive
//! calls. This file pins that contract against the fixture's
//! inventory so a regression in either `collect_inventory` or the
//! `inventory` linkage shows up as a test failure, not a silent
//! "worker registered with empty payload" in production.

use convex_native_distributed::admission;

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[test]
fn collected_inventory_includes_every_fixture_piece() -> anyhow::Result<()> {
    let (inv, _hash) = admission::collect_inventory()?;

    // Functions: the fixture defines at least one of each kind.
    let names: Vec<&str> = inv.functions.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"list_todos"));
    assert!(names.contains(&"count_pending"));
    assert!(names.contains(&"whoami"));
    assert!(names.contains(&"create_todo"));
    assert!(names.contains(&"mark_done"));
    assert!(names.contains(&"internal_delete"));
    assert!(names.contains(&"summarise"));
    assert!(names.contains(&"echo_action"));
    assert!(names.contains(&"always_bad_request"));
    // HTTP routes: POST /api/ping.
    assert!(
        inv.routes
            .iter()
            .any(|r| r.method == "POST" && r.path == "/api/ping"),
        "expected POST /api/ping route in envelope; got {:?}",
        inv.routes,
    );
    // Crons: nightly-cleanup.
    assert!(
        inv.crons.iter().any(|c| c.name == "nightly-cleanup"),
        "expected nightly-cleanup cron in envelope; got {:?}",
        inv.crons,
    );
    // Schema: non-empty JSON payload.
    assert!(
        inv.schema
            .as_ref()
            .map(|s| !s.schema_json.is_empty())
            .unwrap_or(false),
        "expected a non-empty schema_json in envelope",
    );
    Ok(())
}

#[test]
fn inventory_hash_is_stable_across_successive_collections() -> anyhow::Result<()> {
    let (_, h1) = admission::collect_inventory()?;
    let (_, h2) = admission::collect_inventory()?;
    assert_eq!(
        h1, h2,
        "collect_inventory is non-deterministic — a rolling-update would re-register every worker \
         on every heartbeat",
    );
    Ok(())
}

#[test]
fn cron_schedule_and_kind_survive_into_envelope() -> anyhow::Result<()> {
    // Confirm the wire CronRegistration carries each fixture cron's
    // full shape — name, schedule, handler, and the mutation/action
    // kind the native cron driver branches on. A regression that
    // silently dropped `kind` would still pass the "envelope
    // contains nightly-cleanup" test above but would break
    // action-kind dispatch in production.
    let (inv, _) = admission::collect_inventory()?;

    let nightly = inv
        .crons
        .iter()
        .find(|c| c.name == "nightly-cleanup")
        .expect("nightly-cleanup cron in envelope");
    assert_eq!(nightly.schedule, "0 3 * * *");
    assert_eq!(nightly.handler, "nightly_cleanup");
    assert_eq!(nightly.kind, "mutation");

    let hourly = inv
        .crons
        .iter()
        .find(|c| c.name == "hourly-probe")
        .expect("hourly-probe cron in envelope");
    assert_eq!(hourly.schedule, "0 * * * *");
    assert_eq!(hourly.handler, "internal_action");
    assert_eq!(
        hourly.kind, "action",
        "action-kind cron must round-trip into the envelope's kind field",
    );
    Ok(())
}

#[test]
fn internal_modifier_survives_into_envelope() -> anyhow::Result<()> {
    let (inv, _) = admission::collect_inventory()?;
    let internal = inv
        .functions
        .iter()
        .find(|f| f.name == "internal_delete")
        .expect("internal_delete registered");
    assert!(
        internal.is_internal,
        "the `internal` modifier should propagate into the envelope so the backend can refuse \
         external calls to it; got {internal:?}",
    );
    let external = inv
        .functions
        .iter()
        .find(|f| f.name == "create_todo")
        .expect("create_todo registered");
    assert!(
        !external.is_internal,
        "create_todo is a regular mutation; is_internal must be false",
    );

    // `internal` must propagate through every UdfType kind —
    // the fixture exercises all three (query / mutation / action)
    // so a regression that only wired the flag on one variant
    // fails here.
    let internal_query = inv
        .functions
        .iter()
        .find(|f| f.name == "internal_count_all")
        .expect("internal_count_all query registered");
    assert!(
        internal_query.is_internal,
        "internal modifier must propagate on queries; got {internal_query:?}",
    );
    let internal_action = inv
        .functions
        .iter()
        .find(|f| f.name == "internal_action")
        .expect("internal_action registered");
    assert!(
        internal_action.is_internal,
        "internal modifier must propagate on actions; got {internal_action:?}",
    );
    Ok(())
}
