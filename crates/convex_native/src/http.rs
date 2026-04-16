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
