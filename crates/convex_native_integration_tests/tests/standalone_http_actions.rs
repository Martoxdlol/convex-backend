//! Standalone-topology HTTP action coverage.
//!
//! `#[convex::http_action(method, path)]` registers a handler
//! under the synthetic name `__http::METHOD:path` (see the
//! `convex_macro::http_action` expansion). The
//! `NativeFunctionRunner::run_http_action` entry point dispatches
//! by that name and threads the `HttpRequest` → `HttpResponse`
//! shapes through.

use std::sync::Arc;

use bytes::Bytes;
use convex_native_core::{
    __private::ConvexValue,
    http::HttpRequest,
    logging::LogBuffer,
    testing::TestCallbacks,
    NativeActionCallbacks,
    NativeFunctionRunner,
};
use http::{
    HeaderMap,
    Method,
};

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

#[tokio::test(flavor = "multi_thread")]
async fn http_action_dispatches_and_round_trips_body() -> anyhow::Result<()> {
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let request = HttpRequest {
        method: Method::POST,
        url: "http://example.invalid/api/ping".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::from_static(b"hello"),
        routed_path: "/api/ping".to_string(),
    };
    let resp = runner
        .run_http_action("__http::POST:/api/ping", request)
        .await?;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, Bytes::from_static(b"pong:hello"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_action_lookup_miss_surfaces_error() -> anyhow::Result<()> {
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let request = HttpRequest {
        method: Method::POST,
        url: "http://example.invalid/api/missing".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        routed_path: "/api/missing".to_string(),
    };
    let err = runner
        .run_http_action("__http::POST:/api/missing", request)
        .await
        .expect_err("unknown HTTP action should error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("does not exist") || msg.contains("not found") || msg.contains("__http::"),
        "expected lookup-miss error; got: {msg}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn json_body_and_header_round_trip() -> anyhow::Result<()> {
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let mut headers = HeaderMap::new();
    headers.insert("X-Via", "tests".parse()?);
    let request = HttpRequest {
        method: Method::POST,
        url: "http://example.invalid/api/echo".to_string(),
        headers,
        body: Bytes::from_static(b"{\"name\":\"alice\"}"),
        routed_path: "/api/echo".to_string(),
    };
    let resp = runner
        .run_http_action("__http::POST:/api/echo", request)
        .await?;
    assert_eq!(resp.status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&resp.body)?;
    assert_eq!(
        parsed,
        serde_json::json!({"hello": "alice", "via": "tests"})
    );
    assert_eq!(
        resp.headers.get("Content-Type").unwrap().to_str().unwrap(),
        "application/json",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_action_sub_calls_native_query_via_callbacks() -> anyhow::Result<()> {
    // `HttpActionCtx::run_query(...)` routes through the attached
    // `NativeActionCallbacks`. Stub the `count_pending` query with
    // a canned value and confirm the handler picks it up and
    // echoes it back in the response body — proves the HTTP ctx
    // exposes the full sub-call surface (not just req/resp types).
    let (callbacks, _history) = TestCallbacks::new()
        .on_query("count_pending", |_args| Ok(ConvexValue::Int64(9)))
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let mut headers = HeaderMap::new();
    headers.insert("X-Owner", "alice".parse()?);
    let request = HttpRequest {
        method: Method::GET,
        url: "http://example.invalid/api/pending".to_string(),
        headers,
        body: Bytes::new(),
        routed_path: "/api/pending".to_string(),
    };
    let resp = runner
        .run_http_action_with_callbacks(
            "__http::GET:/api/pending",
            request,
            callbacks,
            Some(LogBuffer::new()),
        )
        .await?;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, Bytes::from_static(b"9"));
    Ok(())
}

#[test]
fn http_router_collects_registered_routes() {
    // `HttpRouter::collect()` enumerates inventory-submitted
    // `#[convex::http_action]`s. The fixture registers
    // `POST /api/ping`; pin that so a future fixture edit can't
    // silently drop it.
    let router = convex_native_core::http::HttpRouter::collect().expect("collect");
    let got = router.lookup("POST", "/api/ping");
    assert!(got.is_some(), "expected POST /api/ping in router");
    assert_eq!(got.unwrap().name, "__http::POST:/api/ping");
}
