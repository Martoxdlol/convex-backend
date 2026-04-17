//! Smoke test for `convex_native::introspect::describe_json`.

use std::sync::Arc;

use convex_native::{
    convex,
    ActionCtx,
    ConvexBackend,
    ConvexDocument,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    MutationCtx,
    NoopCallbacks,
    QueryCtx,
    Rt,
};

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "intro_users")]
#[convex(index(name = "by_name", fields = ["name"]))]
pub struct IntroUser {
    pub name: String,
}

#[convex::query]
pub async fn intro_q(_ctx: &mut QueryCtx<'_, Rt>) -> anyhow::Result<i64> {
    Ok(0)
}

#[convex::mutation]
pub async fn intro_m(_ctx: &mut MutationCtx<'_, Rt>) -> anyhow::Result<()> {
    Ok(())
}

#[convex::action]
pub async fn intro_a(_ctx: &mut ActionCtx<'_, Rt>) -> anyhow::Result<bool> {
    Ok(true)
}

#[convex::http_action(method = "GET", path = "/intro/health")]
#[allow(dead_code)]
async fn intro_http(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    _req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    Ok(HttpResponse::new(200))
}

#[test]
fn describe_json_lists_schema_functions_and_routes() {
    let built = ConvexBackend::new()
        .with_native_functions()
        .with_native_schema()
        .with_http_routes()
        .with_callbacks(Arc::new(NoopCallbacks))
        .build()
        .unwrap();

    let v = built.describe_json();
    let obj = v.as_object().expect("object");
    assert_eq!(obj["version"], serde_json::json!(1));

    // Schema contains our table.
    let tables = obj["schema"]["tables"].as_array().unwrap();
    assert!(tables.iter().any(|t| t["name"] == "intro_users"));

    // Functions list includes every kind.
    let fns = obj["functions"]["entries"].as_array().unwrap();
    let has = |name: &str, kind: &str| fns.iter().any(|f| f["name"] == name && f["kind"] == kind);
    assert!(has("intro_q", "query"));
    assert!(has("intro_m", "mutation"));
    assert!(has("intro_a", "action"));

    // Routes include our HTTP action.
    let routes = obj["http_routes"]["routes"].as_array().unwrap();
    assert!(routes
        .iter()
        .any(|r| r["method"] == "GET" && r["path"] == "/intro/health"));

    // Pretty-print works.
    let s = built.describe_pretty();
    assert!(s.contains("intro_users"));
}

#[test]
fn describe_json_surfaces_document_type_validator() {
    // Since `#[derive(ConvexDocument)]` now emits
    // `document_type: Some(DocumentSchema::Union(...))` by default,
    // introspection should surface it. Tooling that compares the
    // deployed schema against the binary's declared shape relies
    // on this being non-null.
    let built = ConvexBackend::new()
        .with_native_schema()
        .with_callbacks(Arc::new(NoopCallbacks))
        .build()
        .unwrap();
    let v = built.describe_json();
    let tables = v["schema"]["tables"].as_array().unwrap();
    let users = tables
        .iter()
        .find(|t| t["name"] == "intro_users")
        .expect("intro_users table present");
    let doc_type = &users["document_type"];
    assert_eq!(doc_type["kind"], "union");
    let variants = doc_type["variants"].as_array().unwrap();
    assert_eq!(variants.len(), 1, "single-shape struct -> one variant");
    let rendered = variants[0].as_str().unwrap();
    assert!(
        rendered.contains("name") && rendered.contains("v.string()"),
        "variant renders the object validator: {rendered}",
    );
}
