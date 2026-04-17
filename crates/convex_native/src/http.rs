//! HTTP action surface — request, response, context, registration.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.7.
//!
//! Handlers look like:
//!
//! ```ignore
//! #[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
//! async fn stripe_webhook(
//!     ctx: &mut HttpActionCtx,
//!     req: HttpRequest,
//! ) -> Result<HttpResponse> { .. }
//! ```
//!
//! Today the dispatch surface compiles and registers the route with
//! `inventory`. Running the handler end-to-end is routed the same way
//! as actions via the `NativeFunctionRunner`, which means a no-sub-call
//! HTTP handler can be executed directly. Real HTTP serving integration
//! lands alongside the backend wiring.

use std::{
    collections::BTreeMap,
    sync::Arc,
};

use bytes::Bytes;
use common::runtime::Runtime;
use http::{
    HeaderMap,
    Method,
};
use serde::de::DeserializeOwned;
use value::TableNamespace;

use crate::{
    ctx::action::ActionCtx,
    runner::NativeFunctionRunner,
};

/// Incoming HTTP request.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Portion of the path *after* the matched route prefix.
    pub routed_path: String,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    pub fn path_remainder(&self) -> &str {
        &self.routed_path
    }

    pub fn body_bytes(&self) -> &Bytes {
        &self.body
    }

    pub fn body_text(&self) -> anyhow::Result<String> {
        String::from_utf8(self.body.to_vec())
            .map_err(|e| anyhow::anyhow!("body is not valid UTF-8: {e}"))
    }

    pub fn body_json<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        serde_json::from_slice::<T>(&self.body)
            .map_err(|e| anyhow::anyhow!("body is not valid JSON for the target type: {e}"))
    }
}

/// Outgoing HTTP response.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    /// Build a JSON response. Serializes `value` and sets
    /// `Content-Type: application/json`.
    pub fn json(status: u16, value: serde_json::Value) -> Self {
        let body = serde_json::to_vec(&value).unwrap_or_default();
        let mut resp = Self::new(status);
        resp.headers
            .insert("Content-Type", "application/json".parse().unwrap());
        resp.body = Bytes::from(body);
        resp
    }

    /// Build a plain-text response. Sets
    /// `Content-Type: text/plain; charset=utf-8` and copies `body`
    /// into the response bytes. Accepts anything that converts to a
    /// `String` (both `&str` and `String`).
    pub fn text(status: u16, body: impl Into<String>) -> Self {
        let s = body.into();
        let mut resp = Self::new(status);
        resp.headers
            .insert("Content-Type", "text/plain; charset=utf-8".parse().unwrap());
        resp.body = Bytes::from(s);
        resp
    }

    /// Build a 30x redirect response.
    pub fn redirect(status: u16, location: &str) -> Self {
        let mut resp = Self::new(status);
        resp.headers.insert(
            "Location",
            location.parse().unwrap_or_else(|_| "/".parse().unwrap()),
        );
        resp
    }

    pub fn with_header(mut self, name: &str, value: &str) -> anyhow::Result<Self> {
        self.headers.insert(
            http::HeaderName::from_bytes(name.as_bytes())?,
            http::HeaderValue::from_str(value)?,
        );
        Ok(self)
    }

    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }
}

/// HTTP-action context. Wraps `ActionCtx` and exposes the same
/// sub-call / scheduler / storage APIs.
pub struct HttpActionCtx<'a, RT: Runtime> {
    inner: ActionCtx<'a, RT>,
}

impl<'a, RT: Runtime> HttpActionCtx<'a, RT> {
    pub fn new(runner: Option<Arc<NativeFunctionRunner>>, namespace: TableNamespace) -> Self {
        Self {
            inner: ActionCtx::new(runner, namespace),
        }
    }

    /// Construct with explicit backend callbacks.
    pub fn with_callbacks(
        runner: Option<Arc<NativeFunctionRunner>>,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
        namespace: TableNamespace,
    ) -> Self {
        Self {
            inner: ActionCtx::with_callbacks(runner, callbacks, namespace),
        }
    }

    /// Same as [`with_callbacks`] plus an external log buffer.
    pub fn with_callbacks_and_log_buffer(
        runner: Option<Arc<NativeFunctionRunner>>,
        callbacks: Arc<dyn crate::callbacks::NativeActionCallbacks>,
        namespace: TableNamespace,
        log_buffer: crate::logging::LogBuffer,
    ) -> Self {
        Self {
            inner: ActionCtx::with_callbacks_and_log_buffer(
                runner, callbacks, namespace, log_buffer,
            ),
        }
    }

    /// Attach the enclosing request's `ExecutionContext` — mirrors
    /// `ActionCtx::with_execution_context`.
    pub fn with_execution_context(
        mut self,
        execution_context: common::execution_context::ExecutionContext,
    ) -> Self {
        self.inner = self.inner.with_execution_context(execution_context);
        self
    }

    /// Delegate — borrow the enclosing request's `ExecutionContext`,
    /// if any.
    pub fn execution_context(&self) -> Option<&common::execution_context::ExecutionContext> {
        self.inner.execution_context()
    }

    /// Delegate — logger.
    pub fn log(&self) -> crate::logging::Logger<'_> {
        self.inner.log()
    }

    /// Delegate — run a query by name (untyped path).
    pub async fn run_query_raw(
        &mut self,
        name: &str,
        args: value::ConvexObject,
    ) -> anyhow::Result<value::ConvexValue> {
        self.inner.run_query_raw(name, args).await
    }

    /// Delegate — run a mutation by name (untyped path).
    pub async fn run_mutation_raw(
        &mut self,
        name: &str,
        args: value::ConvexObject,
    ) -> anyhow::Result<value::ConvexValue> {
        self.inner.run_mutation_raw(name, args).await
    }

    /// Delegate — run a typed sub-call query.
    pub async fn run_query<F: crate::function_ref::ConvexQueryFunction>(
        &mut self,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        self.inner.run_query(marker, args).await
    }

    /// Delegate — run a typed sub-call mutation.
    pub async fn run_mutation<F: crate::function_ref::ConvexMutationFunction>(
        &mut self,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        self.inner.run_mutation(marker, args).await
    }

    /// Delegate — run a typed sub-call action.
    pub async fn run_action<F: crate::function_ref::ConvexActionFunction>(
        &mut self,
        marker: F,
        args: F::Args,
    ) -> anyhow::Result<F::Output> {
        self.inner.run_action(marker, args).await
    }

    /// Delegate — get the storage handle.
    pub fn storage(&mut self) -> crate::ctx::storage::StorageCtx<'_> {
        self.inner.storage()
    }

    /// Delegate — get the scheduler handle.
    pub fn scheduler(&mut self) -> crate::ctx::scheduler::Scheduler<'_> {
        self.inner.scheduler()
    }
}

/// Collected by `inventory` — one per `#[convex::http_action]`.
pub struct HttpRouteRegistration {
    pub method: &'static str,
    pub path: &'static str,
    /// Registry-key under which the handler is also stored as an
    /// action with the same dispatch shape. The generated macro
    /// synthesizes a name like `__http::POST:/api/stripe`.
    pub name: &'static str,
}

inventory::collect!(HttpRouteRegistration);

/// Lookup/iteration over collected HTTP route registrations.
pub struct HttpRouter {
    routes: BTreeMap<(String, String), &'static HttpRouteRegistration>,
}

impl HttpRouter {
    pub fn collect() -> anyhow::Result<Self> {
        let mut routes = BTreeMap::new();
        for r in inventory::iter::<HttpRouteRegistration> {
            let key = (r.method.to_string(), r.path.to_string());
            if routes.insert(key.clone(), r).is_some() {
                anyhow::bail!(
                    "duplicate HTTP route registration for {} {}",
                    r.method,
                    r.path,
                );
            }
        }
        Ok(Self { routes })
    }

    /// Exact-match lookup.
    pub fn lookup(&self, method: &str, path: &str) -> Option<&'static HttpRouteRegistration> {
        self.routes
            .get(&(method.to_string(), path.to_string()))
            .copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = &'static HttpRouteRegistration> + '_ {
        self.routes.values().copied()
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use http::{
        HeaderMap,
        HeaderValue,
        Method,
    };
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    fn request_with_body(body: impl Into<Bytes>) -> HttpRequest {
        let mut headers = HeaderMap::new();
        headers.insert("x-test", HeaderValue::from_static("abc"));
        HttpRequest {
            method: Method::GET,
            url: "http://example.com/foo".into(),
            headers,
            body: body.into(),
            routed_path: "/foo".into(),
        }
    }

    #[test]
    fn request_header_returns_string_value_or_none() {
        let req = request_with_body(Bytes::new());
        assert_eq!(req.header("x-test"), Some("abc"));
        // Header lookup is case-insensitive in `http::HeaderMap`.
        assert_eq!(req.header("X-Test"), Some("abc"));
        assert_eq!(req.header("missing"), None);
    }

    #[test]
    fn request_body_text_roundtrips_utf8() {
        let req = request_with_body("hello world".as_bytes().to_vec());
        assert_eq!(req.body_text().unwrap(), "hello world");
    }

    #[test]
    fn request_body_text_rejects_non_utf8() {
        // 0xFF is never valid in UTF-8.
        let req = request_with_body(vec![0xFFu8, 0x00]);
        let err = req.body_text().expect_err("non-UTF-8 must fail");
        assert!(
            format!("{err}").contains("not valid UTF-8"),
            "error surfaces the cause: {err}",
        );
    }

    #[test]
    fn request_body_json_deserialises_into_target_type() {
        #[derive(Deserialize)]
        struct Payload {
            name: String,
            count: u32,
        }
        let req = request_with_body(b"{\"name\":\"alice\",\"count\":7}".to_vec());
        let parsed: Payload = req.body_json().expect("decode");
        assert_eq!(parsed.name, "alice");
        assert_eq!(parsed.count, 7);
    }

    #[test]
    fn request_body_json_rejects_malformed_input() {
        let req = request_with_body(b"not-json".to_vec());
        assert!(
            req.body_json::<serde_json::Value>().is_err(),
            "malformed JSON must not decode",
        );
    }

    #[test]
    fn request_path_remainder_exposes_the_routed_suffix() {
        // path_remainder is the sub-path after the matched route prefix.
        // For HttpRouter dispatch this is what user handlers see.
        let req = request_with_body(Bytes::new());
        assert_eq!(req.path_remainder(), "/foo");
    }

    #[test]
    fn response_new_has_empty_headers_and_body() {
        let resp = HttpResponse::new(204);
        assert_eq!(resp.status, 204);
        assert!(resp.headers.is_empty());
        assert!(resp.body.is_empty());
    }

    #[test]
    fn response_json_sets_content_type_and_serialises_body() {
        let resp = HttpResponse::json(201, json!({"ok": true, "n": 1}));
        assert_eq!(resp.status, 201);
        assert_eq!(
            resp.headers
                .get("Content-Type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json"),
        );
        let body_str = std::str::from_utf8(&resp.body).expect("utf-8");
        // Field order is stable in serde_json.
        assert!(body_str.contains("\"ok\":true"));
        assert!(body_str.contains("\"n\":1"));
    }

    #[test]
    fn response_redirect_sets_location_header() {
        let resp = HttpResponse::redirect(302, "/next");
        assert_eq!(resp.status, 302);
        assert_eq!(
            resp.headers.get("Location").and_then(|v| v.to_str().ok()),
            Some("/next"),
        );
    }

    #[test]
    fn response_with_header_adds_the_header() {
        let resp = HttpResponse::new(200)
            .with_header("x-extra", "one")
            .expect("header");
        assert_eq!(
            resp.headers.get("x-extra").and_then(|v| v.to_str().ok()),
            Some("one"),
        );
    }

    #[test]
    fn response_with_header_rejects_invalid_name() {
        // Control characters aren't valid in header names.
        let err = HttpResponse::new(200)
            .with_header("bad name", "x")
            .expect_err("must reject");
        let _ = err;
    }

    #[test]
    fn response_with_body_replaces_existing_body() {
        let resp = HttpResponse::new(200)
            .with_body("first")
            .with_body("second");
        assert_eq!(&resp.body[..], b"second");
    }
}
