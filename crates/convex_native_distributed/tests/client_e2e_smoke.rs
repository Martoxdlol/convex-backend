//! End-to-end smoke test: a registered `#[convex::action]` driven
//! through a real `TonicWorkerClient` over real TCP/gRPC.
//!
//! Previously called out in `convex-native/STATUS.md`:
//!
//! > What's missing is a scripted test that boots the backend with a
//! > registered `#[convex::query]` and drives it through the
//! > websocket or HTTP client path.
//!
//! A full HTTP/WebSocket-level test would boot `convex-local-backend`,
//! which pulls in the V8-backed `isolate` crate and therefore needs
//! `rush install` under `npm-packages/` before `cargo build` succeeds.
//! The gap that that test would close is "do registered native handlers
//! dispatch cleanly when a real client talks to the backend over the
//! wire?" — and the answer lives the same whether the wire is
//! WebSocket/HTTP or gRPC: the native registry, dispatch, and response
//! encoding have to agree end-to-end.
//!
//! This test drives an action end-to-end through the
//! `convex_native_distributed` gRPC surface (the same surface
//! `convex-local-backend`'s `CONVEX_MODE=worker` exposes) with a
//! `TonicWorkerClient`:
//!
//! 1. Register a `#[convex::action]` that consumes typed args and returns a
//!    typed result.
//! 2. Spawn `FunctionExecutionServer` on an ephemeral port, backed by the
//!    inventory-collected native registry.
//! 3. Connect a `TonicWorkerClient` to it.
//! 4. Encode arguments with the action's generated `XxxArgs` struct, round-trip
//!    through the real wire format, decode the response back into the typed
//!    output.
//!
//! If any piece in the pipeline breaks — proto conversions, registry
//! lookup, action dispatch, response envelope, tonic wiring — this
//! test fails at the layer closest to the break.

use std::{
    collections::BTreeMap,
    sync::Arc,
};

use common::types::UdfType;
use convex_native::{
    convex,
    distributed::ExecuteRequest,
    ActionCtx,
    FromConvex,
    NativeFunctionRunner,
    Rt,
    ToConvex,
};
use convex_native_distributed::{
    FunctionExecutionServer,
    TonicWorkerClient,
    WorkerClient,
};
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use value::{
    ConvexObject,
    ConvexValue,
    FieldName,
    TableNamespace,
};

/// Action registered for the e2e smoke test. Deliberately named with
/// a `_smoke` suffix so it doesn't collide with identically-named
/// actions in sibling test binaries (every integration-test file
/// shares one global inventory table per cargo test binary).
#[convex::action]
pub async fn echo_label_smoke(
    _ctx: &mut ActionCtx<'_, Rt>,
    label: String,
) -> anyhow::Result<String> {
    // Return a deterministic shape the test can pin: the original
    // input + a prefix so we can tell a successful dispatch from a
    // spurious default value.
    Ok(format!("echo:{label}"))
}

/// Spawn a `FunctionExecutionServer` on an ephemeral port. Returns
/// the endpoint URL as `http://127.0.0.1:PORT` so the caller can
/// hand it straight to `TonicWorkerClient::connect`.
async fn spawn_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let native = Arc::new(NativeFunctionRunner::from_inventory().expect("inventory"));
    let server = FunctionExecutionServer::new(native);
    tokio::spawn(async move {
        Server::builder()
            .add_service(FunctionExecutionServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server loop");
    });
    // Yield once so the OS has a moment to accept the first connect
    // the client is about to issue — matches the pattern in
    // `multi_worker.rs::spawn_worker`.
    tokio::task::yield_now().await;
    format!("http://{addr}")
}

#[tokio::test]
async fn action_dispatches_end_to_end_through_real_grpc_client() {
    let endpoint = spawn_server().await;
    let client = TonicWorkerClient::connect(endpoint)
        .await
        .expect("connect client");

    // Encode args through the same typed pipeline the macro emits
    // for typed sub-calls: the generated `EchoLabelSmokeArgs` struct
    // produces a ConvexObject, the server decodes it back, the
    // handler receives a typed `String`.
    let args_obj: ConvexObject = match (EchoLabelSmokeArgs {
        label: "e2e".into(),
    })
    .to_convex()
    .expect("to_convex")
    {
        ConvexValue::Object(o) => o,
        _ => panic!("args serialise to an object"),
    };

    let resp = client
        .execute(
            ExecuteRequest {
                name: "echo_label_smoke".to_string(),
                namespace: TableNamespace::Global,
                args: args_obj,
                timeout: None,
                min_registry_version: None,
            },
            UdfType::Action,
        )
        .await
        .expect("execute succeeds at the transport layer");

    // Handler returned a real value, not an error. If the native
    // registry hadn't been populated, `result` would be an `Err` with
    // "no native function registered" — that's a regression we
    // explicitly want to catch.
    let ok_value = resp.result.expect("handler returned a value");
    let decoded: String = String::from_convex(ok_value).expect("decode");
    assert_eq!(
        decoded, "echo:e2e",
        "handler output round-trips through the wire unchanged",
    );
}

#[tokio::test]
async fn execute_surfaces_unknown_function_error_cleanly() {
    // Symmetric check: when the name isn't registered, the wire
    // path still works — the client gets back a well-formed
    // ExecuteResponse with `result: Err(...)`. Previously a
    // mis-registered native would have manifested as a transport
    // error, which is much harder to diagnose at the caller.
    let endpoint = spawn_server().await;
    let client = TonicWorkerClient::connect(endpoint)
        .await
        .expect("connect client");

    let empty: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    let resp = client
        .execute(
            ExecuteRequest {
                name: "definitely_not_registered".to_string(),
                namespace: TableNamespace::Global,
                args: ConvexObject::try_from(empty).expect("empty"),
                timeout: None,
                min_registry_version: None,
            },
            UdfType::Action,
        )
        .await
        .expect("transport succeeds even on handler miss");

    match resp.result {
        Err(msg) => {
            assert!(
                msg.contains("definitely_not_registered"),
                "error message names the missing function: {msg}",
            );
        },
        Ok(v) => panic!("expected handler miss, got {v:?}"),
    }
}
