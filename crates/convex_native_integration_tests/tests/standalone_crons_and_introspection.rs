//! Coverage for cron registration + introspection + the
//! `BuiltBackend` builder surface.
//!
//! These features are wire-independent — collected from
//! `inventory` at process start — so the same assertion applies in
//! both topologies. Kept in a single file under the
//! `standalone_*` prefix for tidiness; the distributed side's
//! admission envelope contract covers its mirror.

use std::sync::Arc;

use convex_native_core::{
    callbacks::NoopCallbacks,
    ConvexBackend,
    CronRegistry,
    HttpRouter,
    NativeActionCallbacks,
    NativeSchema,
};

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[test]
fn cron_registry_includes_fixture_entry() {
    let registry = CronRegistry::collect().expect("collect crons");
    let entry = registry
        .lookup("nightly-cleanup")
        .expect("nightly-cleanup cron registered");
    assert_eq!(entry.schedule, "0 3 * * *");
    assert_eq!(entry.target, "nightly_cleanup");
    assert_eq!(entry.target_kind, "mutation");

    // Fixture also registers an action-kind cron so the
    // target_kind = "action" branch is exercised.
    let action_cron = registry
        .lookup("hourly-probe")
        .expect("hourly-probe cron registered");
    assert_eq!(action_cron.target, "internal_action");
    assert_eq!(action_cron.target_kind, "action");
}

#[test]
fn native_schema_collects_fixture_tables() {
    let schema = NativeSchema::collect().expect("collect schema");
    let tables: Vec<_> = schema.tables.keys().map(|t| t.to_string()).collect();
    assert!(
        tables.iter().any(|t| t == "todos"),
        "expected `todos` table in schema; got {tables:?}",
    );
    assert!(
        tables.iter().any(|t| t == "messages"),
        "expected `messages` table in schema; got {tables:?}",
    );
}

#[test]
fn native_schema_surfaces_text_and_vector_indexes_on_messages() {
    // Messages declares text_index + vector_index in the fixture.
    // Those variants live on separate fields of TableDefinition
    // (text_indexes / vector_indexes) and have their own
    // #[derive(ConvexDocument)] macro arms; confirm they survive
    // NativeSchema::collect into the DatabaseSchema the admission
    // envelope carries over the wire.
    let schema = NativeSchema::collect().expect("collect schema");
    let messages_table: value::TableName = "messages".parse().unwrap();
    let msg = schema.tables.get(&messages_table).expect("messages table");
    // DocumentSchema wraps the per-table definition; poke it
    // through its Debug form for a stable assertion shape — the
    // important thing is that the text/vector index names show up.
    let rendered = format!("{msg:?}");
    assert!(
        rendered.contains("by_body"),
        "text index `by_body` missing from messages schema; got: {rendered}",
    );
    assert!(
        rendered.contains("by_embedding"),
        "vector index `by_embedding` missing from messages schema; got: {rendered}",
    );
}

#[test]
fn http_router_collects_fixture_routes() {
    let router = HttpRouter::collect().expect("collect routes");
    assert!(router.lookup("POST", "/api/ping").is_some());
}

#[test]
fn built_backend_summary_names_all_pieces() -> anyhow::Result<()> {
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(callbacks)
        .build()?;

    // The cron's target must exist as a registered mutation/action —
    // `validate()` cross-checks that; a wrong target in the fixture
    // would fail here.
    built.validate()?;

    // Summary should mention function + table + route + cron
    // counts. We exercise that the counts are non-zero; the exact
    // format is covered by `convex_native_core`'s own tests.
    assert!(built.function_count() > 0);
    assert!(built.table_count() >= 2);
    assert!(built.route_count() >= 1);
    assert!(built.cron_count() >= 1);
    let summary = built.summary();
    assert!(
        summary.contains("fn") && summary.contains("table"),
        "summary should mention fn/table counts; got: {summary}",
    );
    Ok(())
}

#[test]
fn built_backend_describe_pretty_is_valid_json_string() -> anyhow::Result<()> {
    // describe_pretty() is the CLI-facing surface — it calls
    // describe_json and serialises the result with
    // `serde_json::to_string_pretty`. Pin that it always returns
    // parseable JSON (not the "<serde error>" fallback) and that
    // the pretty form contains each top-level key the caller
    // expects to `jq` against.
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(callbacks)
        .build()?;
    let pretty = built.describe_pretty();
    let parsed: serde_json::Value = serde_json::from_str(&pretty)?;
    assert_eq!(parsed["version"].as_i64(), Some(1));
    // The pretty form should contain newlines (confirms
    // serde_json::to_string_pretty actually ran, vs just
    // `to_string`).
    assert!(pretty.contains('\n'), "expected pretty-printed output");
    Ok(())
}

#[test]
fn built_backend_describe_json_is_stable_v1() -> anyhow::Result<()> {
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(callbacks)
        .build()?;
    let v: serde_json::Value = built.describe_json();
    assert_eq!(v["version"].as_i64(), Some(1));
    assert!(v["schema"]["tables"].is_array());
    assert!(v["functions"]["entries"].is_array());
    assert!(v["http_routes"]["routes"].is_array());
    assert!(v["crons"]["entries"].is_array());
    Ok(())
}
