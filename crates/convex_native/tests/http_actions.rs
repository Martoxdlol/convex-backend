//! Tests `#[convex::http_action]` registration + request/response types.

use bytes::Bytes;
use convex_native::{
    convex,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    HttpRouter,
    Rt,
};
use http::Method;

#[convex::http_action(method = "GET", path = "/api/health")]
#[allow(dead_code)]
async fn health_check(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    _req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    Ok(HttpResponse::json(200, serde_json::json!({"status": "ok"})))
}

#[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
#[allow(dead_code)]
async fn stripe_webhook(
    _ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let _ = req.body_bytes();
    Ok(HttpResponse::new(204))
}

#[test]
fn http_routes_register_with_correct_metadata() {
    let router = HttpRouter::collect().expect("collect");
    assert!(router.len() >= 2);

    let health = router.lookup("GET", "/api/health").expect("health route");
    assert_eq!(health.method, "GET");
    assert_eq!(health.path, "/api/health");

    let stripe = router
        .lookup("POST", "/api/webhooks/stripe")
        .expect("stripe route");
    assert_eq!(stripe.path, "/api/webhooks/stripe");

    // Casing is preserved as uppercase.
    assert!(router.lookup("post", "/api/webhooks/stripe").is_none());
}

#[test]
fn http_request_helpers() {
    use http::HeaderMap;
    let mut headers = HeaderMap::new();
    headers.insert("X-Test", "value".parse().unwrap());
    let req = HttpRequest {
        method: Method::GET,
        url: "/hello".into(),
        headers,
        body: Bytes::from_static(b"hello"),
        routed_path: "leaf".into(),
    };
    assert_eq!(req.header("X-Test"), Some("value"));
    assert_eq!(req.path_remainder(), "leaf");
    assert_eq!(req.body_text().unwrap(), "hello");
}

#[test]
fn http_response_builders() {
    let resp = HttpResponse::json(201, serde_json::json!({"ok": true}));
    assert_eq!(resp.status, 201);
    assert_eq!(
        resp.headers.get("Content-Type").unwrap().to_str().unwrap(),
        "application/json"
    );
    let parsed: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(parsed["ok"], serde_json::Value::Bool(true));

    let redirect = HttpResponse::redirect(302, "/login");
    assert_eq!(redirect.status, 302);
    assert_eq!(
        redirect.headers.get("Location").unwrap().to_str().unwrap(),
        "/login"
    );

    // Plain-text response sets Content-Type and copies the body
    // verbatim. Accepts both &str and String via `impl Into<String>`.
    let text = HttpResponse::text(200, "hello world");
    assert_eq!(text.status, 200);
    assert_eq!(
        text.headers.get("Content-Type").unwrap().to_str().unwrap(),
        "text/plain; charset=utf-8"
    );
    assert_eq!(&text.body[..], b"hello world");

    let owned = HttpResponse::text(500, String::from("oops"));
    assert_eq!(owned.status, 500);
    assert_eq!(&owned.body[..], b"oops");
}
