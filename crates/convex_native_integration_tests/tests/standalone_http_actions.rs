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

#[tokio::test(flavor = "multi_thread")]
async fn http_action_sub_mutation_reaches_callbacks() -> anyhow::Result<()> {
    // Sub-query path is covered; the sub-mutation path is a
    // distinct code path on `HttpActionCtx` (routes through
    // `NativeActionCallbacks::run_mutation_by_name` rather
    // than `run_query_by_name`). Stub `create_todo` to return a
    // canned id and assert the handler returns it back in the
    // response body.
    let (callbacks, _history) = TestCallbacks::new()
        .on_mutation("create_todo", |_args| {
            Ok(ConvexValue::try_from("stub-id".to_string()).unwrap())
        })
        .build();
    let callbacks: Arc<dyn NativeActionCallbacks> = callbacks;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let mut headers = HeaderMap::new();
    headers.insert("X-Owner", "alice".parse()?);
    let request = HttpRequest {
        method: Method::POST,
        url: "http://example.invalid/api/create".to_string(),
        headers,
        body: Bytes::from_static(b"hi"),
        routed_path: "/api/create".to_string(),
    };
    let resp = runner
        .run_http_action_with_callbacks(
            "__http::POST:/api/create",
            request,
            callbacks,
            Some(LogBuffer::new()),
        )
        .await?;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, Bytes::from_static(b"stub-id"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_ctx_auth_reports_system_when_system_identity_forwarded() -> anyhow::Result<()> {
    // Complement to http_ctx_auth_defaults_to_anonymous_without_identity:
    // run_http_action_with_callbacks_and_identity(Identity::system())
    // threads the identity through HttpActionCtx::with_identity —
    // the handler's ctx.auth() should see System. A regression in
    // the identity-forward wiring would leave the handler seeing
    // Unknown even when the caller supplied a principal.
    use convex_native_core::callbacks::NoopCallbacks;
    use keybroker::Identity;
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let callbacks: Arc<dyn NativeActionCallbacks> = Arc::new(NoopCallbacks);
    let request = HttpRequest {
        method: Method::GET,
        url: "http://example.invalid/api/whoami-http".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        routed_path: "/api/whoami-http".to_string(),
    };
    let resp = runner
        .run_http_action_with_callbacks_and_identity(
            "__http::GET:/api/whoami-http",
            request,
            callbacks,
            Some(LogBuffer::new()),
            Identity::system(),
        )
        .await?;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, Bytes::from_static(b"system"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_ctx_auth_defaults_to_anonymous_without_identity() -> anyhow::Result<()> {
    // The HTTP-action ctx holds identity through its own builder
    // (HttpActionCtx::with_identity) parallel to query/mutation
    // ctxs. run_http_action with no identity hooks must expose
    // an Unknown(None) identity — which whoami_http reports as
    // "anonymous". Pins the accessor wiring; a regression that
    // accidentally routed through system identity would report
    // "system".
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let request = HttpRequest {
        method: Method::GET,
        url: "http://example.invalid/api/whoami-http".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        routed_path: "/api/whoami-http".to_string(),
    };
    let resp = runner
        .run_http_action("__http::GET:/api/whoami-http", request)
        .await?;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, Bytes::from_static(b"anonymous"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_method_registers_and_dispatches() -> anyhow::Result<()> {
    // The fixture otherwise only uses GET / POST, so this test
    // exercises the macro's method-string expansion for the
    // DELETE verb. The synthesised name embeds the method
    // verbatim, so a regression in the method-string handling
    // would break registration / lookup for any verb beyond the
    // two common ones.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let request = HttpRequest {
        method: Method::DELETE,
        url: "http://example.invalid/api/item".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        routed_path: "/api/item".to_string(),
    };
    let resp = runner
        .run_http_action("__http::DELETE:/api/item", request)
        .await?;
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty());
    // Router-side lookup must also find it.
    let router = convex_native_core::http::HttpRouter::collect()?;
    assert!(router.lookup("DELETE", "/api/item").is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn redirect_handler_returns_location_header() -> anyhow::Result<()> {
    // HttpResponse::redirect is pinned at the builder layer by
    // http_response_builders.rs. Driving it through the runner
    // surfaces any regression in the response serialisation path
    // that might strip headers or mis-encode the status. Assert
    // the handler's 302 + Location survive end-to-end.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let request = HttpRequest {
        method: Method::GET,
        url: "http://example.invalid/api/goto".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        routed_path: "/api/goto".to_string(),
    };
    let resp = runner
        .run_http_action("__http::GET:/api/goto", request)
        .await?;
    assert_eq!(resp.status, 302);
    assert_eq!(
        resp.headers.get("Location").unwrap().to_str().unwrap(),
        "/home",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn path_remainder_returns_routed_path() -> anyhow::Result<()> {
    // `HttpRequest::path_remainder()` aliases to `routed_path` —
    // what the router hands the handler after matching. Asserting
    // the accessor round-trips the supplied value keeps the
    // handler-visible contract stable even if the internal storage
    // shape shifts.
    let runner = Arc::new(NativeFunctionRunner::from_inventory()?);
    let request = HttpRequest {
        method: Method::GET,
        url: "http://example.invalid/api/remainder".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        routed_path: "/api/remainder".to_string(),
    };
    let resp = runner
        .run_http_action("__http::GET:/api/remainder", request)
        .await?;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, Bytes::from_static(b"remainder=/api/remainder"));
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
