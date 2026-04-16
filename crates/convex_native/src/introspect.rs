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
    let schema_json = schema.map(describe_schema);
    let functions_json = functions.map(describe_functions);
    let router_json = router.map(describe_router);

    let mut envelope = serde_json::Map::new();
    envelope.insert("version".into(), json!(1));
    if let Some(s) = schema_json {
        envelope.insert("schema".into(), s);
    }
    if let Some(f) = functions_json {
        envelope.insert("functions".into(), f);
    }
    if let Some(r) = router_json {
        envelope.insert("http_routes".into(), r);
    }
    serde_json::Value::Object(envelope)
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
            json!({
                "name": name.to_string(),
                "indexes": db_indexes,
                "text_indexes": text_indexes,
                "vector_indexes": vector_indexes,
            })
        })
        .collect();
    json!({ "tables": tables, "schema_validation": schema.schema_validation })
}

fn describe_functions(functions: &NativeFunctionRegistry) -> serde_json::Value {
    let entries: Vec<_> = functions
        .iter()
        .map(|r| {
            let kind = match r.handler {
                HandlerFn::Query(_) => "query",
                HandlerFn::Mutation(_) => "mutation",
                HandlerFn::Action(_) => "action",
            };
            json!({
                "name": r.name,
                "kind": kind,
                "args": r.arg_names,
            })
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
