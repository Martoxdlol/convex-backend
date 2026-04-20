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
    NativeFunctionRegistry,
    NativeSchema,
};

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[test]
fn function_registration_udf_type_reflects_handler_kind() -> anyhow::Result<()> {
    // NativeFunctionRegistration::udf_type() reads the UdfType
    // off the HandlerFn enum. The fixture registers one handler
    // per kind (Query / Mutation / Action / HttpAction). A
    // regression that mis-bucketed handlers (e.g. returned
    // Mutation for every shape) would silently break every
    // dev-tooling + admission consumer that branches on type.
    use common::types::UdfType;
    let reg = NativeFunctionRegistry::collect()?;
    assert_eq!(reg.get("count_pending").unwrap().udf_type(), UdfType::Query);
    assert_eq!(
        reg.get("create_todo").unwrap().udf_type(),
        UdfType::Mutation
    );
    assert_eq!(reg.get("summarise").unwrap().udf_type(), UdfType::Action);
    assert_eq!(
        reg.get("__http::POST:/api/ping").unwrap().udf_type(),
        UdfType::HttpAction,
    );
    Ok(())
}

#[test]
fn function_registration_surfaces_timeout_and_arg_names() -> anyhow::Result<()> {
    // NativeFunctionRegistration carries arg_names and timeout_ms
    // populated by the macro. The fixture's sleep_forever has
    // timeout_ms = 100 and zero args; create_todo has timeout_ms
    // = 0 (default) and ("owner", "text"). Pin both so a
    // regression in the macro expansion (e.g. losing the
    // timeout_ms attribute) fails here instead of silently in a
    // deployer's per-function timeout config.
    let reg = NativeFunctionRegistry::collect()?;
    let sleep = reg.get("sleep_forever").expect("registered");
    assert_eq!(
        sleep.timeout_ms, 100,
        "per-function timeout survives registration"
    );
    assert!(sleep.arg_names.is_empty(), "sleep_forever takes no args");

    let create = reg.get("create_todo").expect("registered");
    assert_eq!(create.timeout_ms, 0, "no per-function timeout means 0");
    assert_eq!(
        create.arg_names,
        &["owner", "text"],
        "arg_names preserve declaration order",
    );
    Ok(())
}

#[test]
fn function_registry_iter_len_get_cover_fixture_handlers() -> anyhow::Result<()> {
    // NativeFunctionRegistry::iter / .len() / .get() form the
    // dev-tooling surface parallel to HttpRouter's. A regression
    // that lost a handler from by_name (for example a double-
    // insert overwriting an earlier registration) would still
    // pass NativeFunctionRunner::has_function via the runner's
    // own map — the registry-level iterator is the independent
    // observer. Pin a representative handler per kind.
    let reg = NativeFunctionRegistry::collect()?;
    assert!(!reg.is_empty());
    assert!(
        reg.len() >= 15,
        "fixture registers well over a dozen handlers; got {}",
        reg.len(),
    );
    assert!(
        reg.get("create_todo").is_some(),
        "by-name lookup finds create_todo"
    );
    assert!(reg.get("summarise").is_some(), "action handler in registry");
    assert!(
        reg.get("does_not_exist").is_none(),
        "unknown name resolves to None"
    );
    let iter_names: Vec<&'static str> = reg.iter().map(|f| f.name).collect();
    assert!(
        iter_names.iter().any(|n| *n == "create_todo"),
        "iter surfaces create_todo; got {iter_names:?}",
    );
    Ok(())
}

#[test]
fn cron_registry_iter_surfaces_every_fixture_entry() {
    // CronRegistry::iter is the dev-tooling entry point for
    // enumerating every declared cron without naming them
    // individually. A regression that short-circuited the
    // iterator (e.g. returned an empty vec or stopped after
    // the first) would pass lookup-by-name tests while silently
    // breaking the operator surface. Pin both fixture entries
    // land in iter().
    let registry = CronRegistry::collect().expect("collect crons");
    let names: Vec<&'static str> = registry.iter().map(|c| c.name).collect();
    assert!(
        names.contains(&"nightly-cleanup"),
        "iter missing nightly-cleanup; got {names:?}",
    );
    assert!(
        names.contains(&"hourly-probe"),
        "iter missing hourly-probe; got {names:?}",
    );
}

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
    for expected in ["todos", "messages", "attachments"] {
        assert!(
            tables.iter().any(|t| t == expected),
            "expected `{expected}` table in schema; got {tables:?}",
        );
    }
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
    // .len() / .is_empty() / .iter() are part of the HttpRouter
    // public surface alongside .lookup(). A regression that
    // mis-wired the count or the iterator body would break dev
    // tooling (convex dev) but slip past the lookup-only
    // assertion above.
    let count = router.len();
    assert!(
        count >= 3,
        "at least ping + echo + pending routes; got {count}"
    );
    assert!(!router.is_empty());
    let route_names: Vec<&'static str> = router.iter().map(|r| r.name).collect();
    assert!(
        route_names.contains(&"__http::POST:/api/ping"),
        "iter() surface should include ping; got {route_names:?}",
    );
}

#[test]
fn built_backend_counts_match_collected_counterparts() -> anyhow::Result<()> {
    // function_count / table_count / route_count / cron_count
    // delegate to the collected registries + schema. Pin that
    // each count matches the free-fn collector's equivalent.
    // A regression that returned 0 or a stale count would slip
    // past the summary test (which only asserts ">= non-zero").
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        .with_callbacks(callbacks)
        .build()?;
    assert_eq!(
        built.function_count(),
        NativeFunctionRegistry::collect()?.len(),
        "function_count should equal registry.len()",
    );
    assert_eq!(
        built.route_count(),
        HttpRouter::collect()?.len(),
        "route_count should equal router.len()",
    );
    assert_eq!(
        built.table_count(),
        NativeSchema::collect()?.tables.len(),
        "table_count should equal schema.tables.len()",
    );
    // Cron count — the registry exposes iter/lookup; len isn't
    // on the public API so compare against the iter count.
    let cron_iter_count = CronRegistry::collect()?.iter().count();
    assert_eq!(
        built.cron_count(),
        cron_iter_count,
        "cron_count should equal iter().count()",
    );
    Ok(())
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
fn built_backend_warmup_plan_reflects_schema_opt_in() -> anyhow::Result<()> {
    // BuiltBackend::warmup_plan() delegates to
    // plan_warmup(schema) when schema was opted in, and returns
    // an empty vec when it wasn't. The direct plan_warmup(schema)
    // path is covered by the schema-evolution tests; this pins
    // the BuiltBackend-level wrapper so a regression that
    // returned an empty plan even with schema opted in wouldn't
    // fall out of the direct-path test.
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);

    let opted_in = ConvexBackend::new()
        .with_native_schema()
        .with_callbacks(callbacks.clone())
        .build()?;
    assert!(
        !opted_in.warmup_plan().is_empty(),
        "with_native_schema opt-in must surface a non-empty warmup plan",
    );

    let opted_out = ConvexBackend::new().with_callbacks(callbacks).build()?;
    assert!(
        opted_out.warmup_plan().is_empty(),
        "no schema opt-in must yield an empty warmup plan; got {:?}",
        opted_out.warmup_plan(),
    );
    Ok(())
}

#[test]
fn built_backend_with_no_opts_produces_empty_piece_counts() -> anyhow::Result<()> {
    // ConvexBackend::new().build() — no with_* called — must
    // succeed and produce a BuiltBackend with zero counts
    // across every collected piece. Deployers that only want
    // ctx.db() + a logger (e.g. a CLI that reads schema from
    // elsewhere) rely on this shape. A regression that made any
    // with_* mandatory at build time would surface here.
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let built = ConvexBackend::new().with_callbacks(callbacks).build()?;
    assert_eq!(built.function_count(), 0);
    assert_eq!(built.table_count(), 0);
    assert_eq!(built.route_count(), 0);
    assert_eq!(built.cron_count(), 0);
    // validate() is a no-op when nothing is opted in — proves the
    // cross-check body doesn't trip on missing pieces.
    built.validate()?;
    Ok(())
}

#[test]
fn built_backend_without_callbacks_still_builds_and_validates() -> anyhow::Result<()> {
    // ConvexBackend::build should succeed without an explicit
    // with_callbacks call — BuiltBackend defaults to
    // NoopCallbacks. Deployers writing unit tests that only
    // introspect the schema / function list shouldn't have to
    // fabricate callbacks. A regression that made with_callbacks
    // mandatory at build time would break every existing unit
    // test in the ecosystem.
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_crons()
        // deliberately no .with_callbacks(...)
        .build()?;
    built.validate()?;
    assert!(built.function_count() > 0);
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
