//! Native HTTP action dispatch for `#[convex::http_action]` handlers
//! registered via `convex_native_core::HttpRouter`.
//!
//! Integrates with the backend's HTTP entry point (`http_any_method`
//! in `http_actions.rs`) by consulting a process-global
//! `NativeHttpDispatcher` before delegating to the JS/isolate
//! action path. When the incoming `(method, path)` matches a native
//! registration the request body is drained, a
//! `convex_native_core::http::HttpRequest` is built, the handler
//! runs via `NativeFunctionRunner::run_http_action_with_callbacks`
//! with a `BackendCallbacks` that routes sub-calls (`run_query`,
//! `run_mutation`, `run_action`, `storage`, `scheduler`) back
//! through the backend's Committer + file storage, and the
//! resulting `HttpResponse` is encoded as an
//! `udf::HttpActionResponseHead` + one body chunk.
//!
//! When no native route matches, `try_dispatch_native` returns
//! `Ok(Some(request))` so the caller can fall through to the
//! JS-based `application.execute_http_action` path with the
//! unconsumed body.
//!
//! ## Wire-up
//!
//! `local_backend::make_app` calls
//! [`install_native_http_dispatcher`] after `Application::new` so
//! the dispatcher can borrow the action-callbacks handle, the
//! database, and the file-storage handle. The install is
//! idempotent; a second call is a silent no-op.
//!
//! The dispatcher is a process-global `OnceLock` to avoid
//! plumbing an extra `Arc` through `RouterState` (which is also
//! consumed by the Usher binary — threading a native dispatcher
//! there would spread native-runtime knowledge into a crate that
//! shouldn't care).

use std::sync::{
    Arc,
    OnceLock,
};

use anyhow::Context;
use bytes::Bytes;
use common::{
    execution_context::ExecutionContext,
    types::FunctionCaller,
    RequestId,
};
use convex_native_backend::BackendCallbacks;
use convex_native_core::{
    callbacks::NativeActionCallbacks,
    http::{
        HttpRequest as NativeHttpRequest,
        HttpResponse as NativeHttpResponse,
        HttpRouter,
    },
    LogBuffer,
    NativeFunctionRunner,
};
use database::Database;
use file_storage::FileStorage;
use futures::StreamExt;
use http::{
    HeaderMap,
    HeaderName,
    HeaderValue,
    StatusCode,
};
use keybroker::Identity;
use runtime::prod::ProdRuntime;
use udf::{
    ActionCallbacks,
    HttpActionRequest,
    HttpActionResponseHead,
    HttpActionResponsePart,
    HttpActionResponseStreamer,
    HTTP_ACTION_BODY_LIMIT,
};

/// Holds everything needed to dispatch a native HTTP action:
/// the collected `HttpRouter`, the `NativeFunctionRunner` that
/// owns the handler fn pointers, and the backend handles the
/// resulting `BackendCallbacks` need (action callbacks, database,
/// file storage) so sub-calls commit through the backend's
/// Committer.
pub struct NativeHttpDispatcher {
    router: Arc<HttpRouter>,
    native_runner: Arc<NativeFunctionRunner>,
    action_callbacks: Arc<dyn ActionCallbacks>,
    database: Database<ProdRuntime>,
    file_storage: FileStorage<ProdRuntime>,
}

impl NativeHttpDispatcher {
    pub fn new(
        router: Arc<HttpRouter>,
        native_runner: Arc<NativeFunctionRunner>,
        action_callbacks: Arc<dyn ActionCallbacks>,
        database: Database<ProdRuntime>,
        file_storage: FileStorage<ProdRuntime>,
    ) -> Self {
        Self {
            router,
            native_runner,
            action_callbacks,
            database,
            file_storage,
        }
    }

    pub fn has_route(&self, method: &str, path: &str) -> bool {
        self.router.lookup(method, path).is_some()
    }

    /// Run the matched native handler end-to-end: decode the
    /// request, build a native ctx with `BackendCallbacks`, call
    /// `run_http_action_with_callbacks`, and push the response
    /// through `streamer`. Returns `Err` only on infrastructure
    /// failures (body exceeds the limit, invalid status code
    /// produced by the handler); handler-level errors are
    /// encoded as a 500 response so the HTTP client still sees a
    /// well-formed reply.
    pub async fn dispatch(
        &self,
        method: &str,
        path: &str,
        request: HttpActionRequest,
        identity: Identity,
        request_id: RequestId,
        streamer: HttpActionResponseStreamer,
    ) -> anyhow::Result<()> {
        let registration = self
            .router
            .lookup(method, path)
            .ok_or_else(|| anyhow::anyhow!("native HTTP dispatch called with no matching route"))?;

        let native_request = self.build_native_request(request, path).await?;
        let context = ExecutionContext::new(request_id, &FunctionCaller::HttpEndpoint);
        let callbacks = self.build_callbacks(identity, context);
        let log_buffer = LogBuffer::new();
        let result = self
            .native_runner
            .run_http_action_with_callbacks(
                registration.name,
                native_request,
                callbacks,
                Some(log_buffer.clone()),
            )
            .await;

        match result {
            Ok(response) => push_response(response, streamer).await?,
            Err(e) => push_error_response(e, streamer).await?,
        }
        Ok(())
    }

    async fn build_native_request(
        &self,
        request: HttpActionRequest,
        routed_path: &str,
    ) -> anyhow::Result<NativeHttpRequest> {
        let HttpActionRequest { head, body } = request;
        let body_bytes = match body {
            Some(stream) => collect_body(stream).await?,
            None => Bytes::new(),
        };
        Ok(NativeHttpRequest {
            method: head.method,
            url: head.url.to_string(),
            headers: head.headers,
            body: body_bytes,
            routed_path: routed_path.to_string(),
        })
    }

    fn build_callbacks(
        &self,
        identity: Identity,
        context: ExecutionContext,
    ) -> Arc<dyn NativeActionCallbacks> {
        let snapshot_ts = self.database.now_ts_for_reads();
        let callbacks = BackendCallbacks::with_native(
            self.action_callbacks.clone(),
            identity,
            context,
            self.native_runner.clone(),
            self.database.clone(),
        )
        .with_file_storage(self.file_storage.clone())
        .with_snapshot_ts(snapshot_ts);
        Arc::new(callbacks)
    }
}

/// Drain the request body into a single `Bytes`, capped at
/// `HTTP_ACTION_BODY_LIMIT`. Matches the JS action path's body
/// cap so a native handler can't read a larger payload than the
/// rest of the system expects.
async fn collect_body(
    mut stream: futures::stream::BoxStream<'static, anyhow::Result<Bytes>>,
) -> anyhow::Result<Bytes> {
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if buf.len() + chunk.len() > HTTP_ACTION_BODY_LIMIT {
            anyhow::bail!("HTTP action request body exceeds {HTTP_ACTION_BODY_LIMIT} bytes");
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

/// Encode a successful `HttpResponse` into the streamer's
/// `(Head, BodyChunk)` pair. Order matches the JS path so
/// downstream streaming code (which peeks `Head` first) doesn't
/// care which runtime served the request.
async fn push_response(
    response: NativeHttpResponse,
    mut streamer: HttpActionResponseStreamer,
) -> anyhow::Result<()> {
    let NativeHttpResponse {
        status,
        headers,
        body,
    } = response;
    let status_code = StatusCode::from_u16(status)
        .with_context(|| format!("native http action returned invalid status {status}"))?;
    let head = HttpActionResponseHead {
        status: status_code,
        headers,
    };
    streamer
        .send_part(HttpActionResponsePart::Head(head))?
        .map_err(|e| anyhow::anyhow!("sending response head: {e}"))?;
    if !body.is_empty() {
        streamer
            .send_part(HttpActionResponsePart::BodyChunk(body))?
            .map_err(|e| anyhow::anyhow!("sending response body: {e}"))?;
    }
    Ok(())
}

/// Encode a handler-level error as a 500. The streamer doesn't
/// expose an error channel, so wrapping the message in a 500
/// keeps the client from hanging on a missing head.
async fn push_error_response(
    err: anyhow::Error,
    mut streamer: HttpActionResponseStreamer,
) -> anyhow::Result<()> {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    let head = HttpActionResponseHead {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        headers,
    };
    streamer
        .send_part(HttpActionResponsePart::Head(head))?
        .map_err(|e| anyhow::anyhow!("sending error head: {e}"))?;
    let body = Bytes::from(format!("native http action error: {err:#}"));
    streamer
        .send_part(HttpActionResponsePart::BodyChunk(body))?
        .map_err(|e| anyhow::anyhow!("sending error body: {e}"))?;
    Ok(())
}

static NATIVE_HTTP_DISPATCHER: OnceLock<Arc<NativeHttpDispatcher>> = OnceLock::new();

/// Install the process-global `NativeHttpDispatcher`. Idempotent:
/// a second call is a no-op and returns `false`. Called from
/// `make_app` after `Application::new` so the dispatcher can
/// borrow the action-callbacks handle, database, and file-storage
/// clones.
pub fn install_native_http_dispatcher(dispatcher: Arc<NativeHttpDispatcher>) -> bool {
    NATIVE_HTTP_DISPATCHER.set(dispatcher).is_ok()
}

/// Read the installed `NativeHttpDispatcher`, if any. Returns
/// `None` when no dispatcher is installed (JS-only deployments
/// where no `#[convex::http_action]` was linked in).
pub fn get_native_http_dispatcher() -> Option<Arc<NativeHttpDispatcher>> {
    NATIVE_HTTP_DISPATCHER.get().cloned()
}

/// Try to dispatch natively. When the installed dispatcher
/// matches the incoming `(method, path)`, this runs the handler
/// and returns `Ok(None)`. Otherwise returns
/// `Ok(Some(request))` so the caller can fall through to the JS
/// `application.execute_http_action` path with the unconsumed
/// body.
pub async fn try_dispatch_native(
    method: &str,
    path: &str,
    request: HttpActionRequest,
    identity: Identity,
    request_id: RequestId,
    streamer: HttpActionResponseStreamer,
) -> anyhow::Result<Option<HttpActionRequest>> {
    let Some(dispatcher) = get_native_http_dispatcher() else {
        return Ok(Some(request));
    };
    if !dispatcher.has_route(method, path) {
        return Ok(Some(request));
    }
    dispatcher
        .dispatch(method, path, request, identity, request_id, streamer)
        .await?;
    Ok(None)
}
