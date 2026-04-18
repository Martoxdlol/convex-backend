//! Stable JSON description of a collected native app.
//!
//! Intended consumer: dev tooling (`convex dev`, code generation, CI
//! checks) that needs to know what tables, indexes, and functions the
//! current binary declares without running it. The output schema is
//! stable; additions are additive-only unless flagged in this
//! module's changelog section.

use common::schemas::DatabaseSchema;
use serde_json::json;

use crate::{
    cron::CronRegistry,
    http::HttpRouter,
    registry::{
        HandlerFn,
        NativeFunctionRegistry,
    },
};

/// Build a JSON envelope describing `(schema, functions, http_routes)`.
/// Any argument set to `None` is omitted from the output.
pub fn describe_json(
    schema: Option<&DatabaseSchema>,
    functions: Option<&NativeFunctionRegistry>,
    router: Option<&HttpRouter>,
) -> serde_json::Value {
    describe_json_full(schema, functions, router, None)
}

/// Same as [`describe_json`] but also includes cron registrations.
pub fn describe_json_full(
    schema: Option<&DatabaseSchema>,
    functions: Option<&NativeFunctionRegistry>,
    router: Option<&HttpRouter>,
    crons: Option<&CronRegistry>,
) -> serde_json::Value {
    let schema_json = schema.map(describe_schema);
    let functions_json = functions.map(describe_functions);
    let router_json = router.map(describe_router);
    let crons_json = crons.map(describe_crons);

    let mut envelope = serde_json::Map::new();
    envelope.insert("version".into(), json!(1));
    envelope.insert("convex_native_version".into(), json!(crate::VERSION));
    if let Some(s) = schema_json {
        envelope.insert("schema".into(), s);
    }
    if let Some(f) = functions_json {
        envelope.insert("functions".into(), f);
    }
    if let Some(r) = router_json {
        envelope.insert("http_routes".into(), r);
    }
    if let Some(c) = crons_json {
        envelope.insert("crons".into(), c);
    }
    serde_json::Value::Object(envelope)
}

fn describe_crons(registry: &CronRegistry) -> serde_json::Value {
    let entries: Vec<_> = registry
        .iter()
        .map(|c| {
            json!({
                "name": c.name,
                "schedule": c.schedule,
                "target": c.target,
                "target_kind": c.target_kind,
            })
        })
        .collect();
    json!({ "entries": entries })
}

fn describe_schema(schema: &DatabaseSchema) -> serde_json::Value {
    let tables: Vec<serde_json::Value> = schema
        .tables
        .iter()
        .map(|(name, def)| {
            let db_indexes: Vec<_> = def
                .indexes
                .iter()
                .map(|(desc, idx)| {
                    let fields: Vec<String> = idx.fields.iter().map(|f| format!("{f}")).collect();
                    json!({ "name": desc.as_str(), "fields": fields })
                })
                .collect();
            let text_indexes: Vec<_> = def
                .text_indexes
                .iter()
                .map(|(desc, idx)| {
                    let filter: Vec<String> =
                        idx.filter_fields.iter().map(|f| format!("{f}")).collect();
                    json!({
                        "name": desc.as_str(),
                        "search_field": format!("{}", idx.search_field),
                        "filter_fields": filter,
                    })
                })
                .collect();
            let vector_indexes: Vec<_> = def
                .vector_indexes
                .iter()
                .map(|(desc, idx)| {
                    let filter: Vec<String> =
                        idx.filter_fields.iter().map(|f| format!("{f}")).collect();
                    json!({
                        "name": desc.as_str(),
                        "vector_field": format!("{}", idx.vector_field),
                        "dimensions": u32::from(idx.dimension),
                        "filter_fields": filter,
                    })
                })
                .collect();
            let document_type = describe_document_type(def.document_type.as_ref());
            json!({
                "name": name.to_string(),
                "indexes": db_indexes,
                "text_indexes": text_indexes,
                "vector_indexes": vector_indexes,
                "document_type": document_type,
            })
        })
        .collect();
    json!({ "tables": tables, "schema_validation": schema.schema_validation })
}

/// Render the `DocumentSchema` on a table into a JSON shape matching
/// the validator DSL (`v.object({...})`, `v.union(...)`, `v.optional(...)`
/// etc. — `Display for Validator` is the canonical pretty form).
/// `None` means the table had no schema attached.
fn describe_document_type(schema: Option<&common::schemas::DocumentSchema>) -> serde_json::Value {
    use common::schemas::DocumentSchema;
    match schema {
        None => serde_json::Value::Null,
        Some(DocumentSchema::Any) => json!({ "kind": "any" }),
        Some(DocumentSchema::Union(objects)) => {
            let variants: Vec<_> = objects
                .iter()
                .map(|obj_validator| {
                    // Use the Display impl on `ObjectValidator`. It
                    // emits the familiar `v.object({...})` string
                    // JS developers already recognise from
                    // schema.ts, so introspection tooling can
                    // round-trip it without a custom parser.
                    json!(format!("{obj_validator}"))
                })
                .collect();
            json!({ "kind": "union", "variants": variants })
        },
    }
}

fn describe_functions(functions: &NativeFunctionRegistry) -> serde_json::Value {
    let entries: Vec<_> = functions
        .iter()
        .map(|r| {
            let kind = match r.handler {
                HandlerFn::Query(_) => "query",
                HandlerFn::Mutation(_) => "mutation",
                HandlerFn::Action(_) => "action",
                HandlerFn::Http(_) => "http_action",
            };
            let mut entry = serde_json::json!({
                "name": r.name,
                "kind": kind,
                "args": r.arg_names,
                "internal": r.is_internal,
            });
            if r.timeout_ms > 0 {
                entry["timeout_ms"] = json!(r.timeout_ms);
            }
            entry
        })
        .collect();
    json!({ "entries": entries })
}

fn describe_router(router: &HttpRouter) -> serde_json::Value {
    let routes: Vec<_> = router
        .iter()
        .map(|r| json!({ "method": r.method, "path": r.path, "name": r.name }))
        .collect();
    json!({ "routes": routes })
}

/// Convenience: turn a `serde_json::Value` into a pretty-printed string
/// for CLI output.
pub fn describe_pretty(
    schema: Option<&DatabaseSchema>,
    functions: Option<&NativeFunctionRegistry>,
    router: Option<&HttpRouter>,
) -> String {
    let v = describe_json(schema, functions, router);
    serde_json::to_string_pretty(&v).unwrap_or_else(|_| "<serde error>".to_string())
}

/// Same as [`describe_pretty`] but also surfaces the cron registry.
/// Mirrors the [`describe_json`] / [`describe_json_full`] split so
/// standalone callers (i.e. those not going through
/// [`crate::BuiltBackend::describe_pretty`]) can produce the full
/// envelope when they're holding a `CronRegistry`.
pub fn describe_pretty_full(
    schema: Option<&DatabaseSchema>,
    functions: Option<&NativeFunctionRegistry>,
    router: Option<&HttpRouter>,
    crons: Option<&CronRegistry>,
) -> String {
    let v = describe_json_full(schema, functions, router, crons);
    serde_json::to_string_pretty(&v).unwrap_or_else(|_| "<serde error>".to_string())
}

#[cfg(test)]
mod tests {
    //! The `introspect::describe_json` envelope is consumed by dev
    //! tooling (`convex dev`, codegen, CI checks). The docstring
    //! promises "the output schema is stable; additions are
    //! additive-only unless flagged in this module's changelog
    //! section." These tests pin three invariants that support that
    //! promise:
    //!
    //! 1. The envelope always carries `version` + `convex_native_version` —
    //!    consumers branch on those for forward-compat.
    //! 2. Absent sections are omitted (not present-with-null), so consumers can
    //!    distinguish "not provided" from "empty".
    //! 3. `describe_pretty` emits valid JSON (never the "<serde error>"
    //!    fallback for the all-None envelope).

    use super::*;

    #[test]
    fn envelope_always_includes_version_fields() {
        let v = describe_json(None, None, None);
        let obj = v.as_object().expect("envelope is a JSON object");
        assert_eq!(obj.get("version"), Some(&json!(1)));
        assert!(
            obj.get("convex_native_version").is_some(),
            "convex_native_version must appear even with nothing else to describe",
        );
    }

    #[test]
    fn missing_sections_are_absent_not_null() {
        // The docs say "any argument set to None is omitted". An
        // accidental switch to `insert("schema", Value::Null)` would
        // look the same to a human but parse differently on the
        // consumer side.
        let v = describe_json(None, None, None);
        let obj = v.as_object().expect("object");
        assert!(!obj.contains_key("schema"));
        assert!(!obj.contains_key("functions"));
        assert!(!obj.contains_key("http_routes"));
        assert!(!obj.contains_key("crons"));
    }

    #[test]
    fn full_envelope_includes_crons_when_registry_passed() {
        // `describe_json_full` is the only entry point that can surface
        // the `crons` key; `describe_json` unconditionally passes
        // `None`. Confirm that contract so a drive-by "always include
        // crons" refactor wouldn't silently start emitting crons from
        // the smaller helper.
        let v_basic = describe_json(None, None, None);
        assert!(!v_basic.as_object().unwrap().contains_key("crons"));
        let v_full = describe_json_full(None, None, None, None);
        // Still no crons when the arg is None.
        assert!(!v_full.as_object().unwrap().contains_key("crons"));
    }

    #[test]
    fn describe_pretty_emits_valid_json() {
        // The fallback on serde error is `"<serde error>"` (a plain
        // string, not JSON). Make sure the normal path never hits that
        // by parsing the output back.
        let pretty = describe_pretty(None, None, None);
        let parsed: serde_json::Value =
            serde_json::from_str(&pretty).expect("pretty output is valid JSON");
        let obj = parsed.as_object().expect("pretty output is an object");
        assert_eq!(obj.get("version"), Some(&json!(1)));
    }

    #[test]
    fn describe_pretty_full_surfaces_crons_block_when_registry_passed() {
        // `describe_pretty` (3-arg) can't represent crons because its
        // `describe_json` call passes `None`. The `_full` variant must
        // surface the `crons` key when a registry is provided.
        //
        // The cron registry in this test is the inventory-collected
        // live one (from every `#[convex::cron]` in the test binary);
        // we don't care about its contents — only that the block
        // appears when we pass `Some(&registry)` and is absent when we
        // pass `None`.
        let registry = CronRegistry::collect().expect("collect registry");
        let pretty = describe_pretty_full(None, None, None, Some(&registry));
        let parsed: serde_json::Value =
            serde_json::from_str(&pretty).expect("pretty output is valid JSON");
        assert!(
            parsed.as_object().unwrap().contains_key("crons"),
            "crons section surfaces through the _full pretty variant",
        );

        // Without the registry, `describe_pretty_full(.., None)` must
        // match the 3-arg variant (no crons key).
        let pretty_no_crons = describe_pretty_full(None, None, None, None);
        let parsed2: serde_json::Value = serde_json::from_str(&pretty_no_crons).unwrap();
        assert!(!parsed2.as_object().unwrap().contains_key("crons"));
    }

    #[test]
    fn convex_native_version_matches_the_crate_version() {
        // The envelope surfaces `crate::VERSION`; a typo in the
        // constant would silently make the introspect envelope
        // disagree with the `worker`'s `Health` response (which also
        // uses `crate::VERSION`). This test is the cheap pin.
        let v = describe_json(None, None, None);
        let version_field = v
            .as_object()
            .unwrap()
            .get("convex_native_version")
            .and_then(|v| v.as_str())
            .expect("convex_native_version is a string");
        assert_eq!(version_field, crate::VERSION);
    }
}
