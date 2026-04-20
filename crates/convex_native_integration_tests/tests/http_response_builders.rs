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

#[test]
fn with_header_rejects_invalid_header_name() {
    // HttpResponse::with_header wraps http::header::HeaderName
    // parsing, which rejects whitespace / control chars / CRLF.
    // A regression that silently accepted malformed names would
    // create header-injection footguns. Pin the reject contract.
    let err = HttpResponse::new(200)
        .with_header("bad header\r\nEvil: x", "ok")
        .expect_err("CRLF in header name should be rejected");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("invalid") || msg.contains("header") || msg.contains("parse"),
        "expected invalid-header-name error; got: {msg}",
    );
}

#[test]
fn multiple_with_header_calls_accumulate() -> anyhow::Result<()> {
    // Chaining .with_header(...) twice should leave both headers
    // on the response. A regression that replaced existing
    // headers on each call (or failed to store the second one)
    // would only land the last set name.
    let r = HttpResponse::new(204)
        .with_header("X-First", "one")?
        .with_header("X-Second", "two")?;
    assert_eq!(r.headers.get("X-First").unwrap().to_str().unwrap(), "one");
    assert_eq!(r.headers.get("X-Second").unwrap().to_str().unwrap(), "two");
    Ok(())
}
