//! Coverage for the `HttpResponse` builders on
//! `convex_native_core::http`. Wire-independent — the same types
//! back every HTTP-action response on both topologies.

use bytes::Bytes;
use convex_native_core::http::HttpResponse;

#[test]
fn new_returns_empty_response_with_status() {
    let r = HttpResponse::new(204);
    assert_eq!(r.status, 204);
    assert!(r.body.is_empty());
    assert!(r.headers.get("Content-Type").is_none());
}

#[test]
fn text_sets_plain_content_type_and_body() {
    let r = HttpResponse::text(200, "hello");
    assert_eq!(r.status, 200);
    assert_eq!(r.body, Bytes::from_static(b"hello"));
    assert_eq!(
        r.headers.get("Content-Type").unwrap().to_str().unwrap(),
        "text/plain; charset=utf-8",
    );
}

#[test]
fn json_sets_json_content_type_and_serialised_body() {
    let r = HttpResponse::json(201, serde_json::json!({"ok": true}));
    assert_eq!(r.status, 201);
    assert_eq!(
        r.headers.get("Content-Type").unwrap().to_str().unwrap(),
        "application/json",
    );
    let parsed: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(parsed, serde_json::json!({"ok": true}));
}

#[test]
fn redirect_sets_location_header() {
    let r = HttpResponse::redirect(302, "/login");
    assert_eq!(r.status, 302);
    assert_eq!(
        r.headers.get("Location").unwrap().to_str().unwrap(),
        "/login",
    );
}

#[test]
fn with_header_and_with_body_chain_cleanly() -> anyhow::Result<()> {
    let r = HttpResponse::new(200)
        .with_header("X-Test", "yes")?
        .with_body(Bytes::from_static(b"payload"));
    assert_eq!(r.body, Bytes::from_static(b"payload"));
    assert_eq!(r.headers.get("X-Test").unwrap().to_str().unwrap(), "yes");
    Ok(())
}
