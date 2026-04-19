//! Distributed-topology HTTP action coverage.
//!
//! Builds an `ExecuteRequest` with a populated `http_request`
//! payload, dispatches through `DistributedFunctionRunner` over
//! real tonic, worker decodes the proto into a native `HttpRequest`,
//! runs the handler, encodes the response into `http_response`,
//! backend decodes it.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
};

use common::types::UdfType;
use convex_native_core::{
    distributed::{
        ExecuteRequest,
        HttpActionRequestPayload,
    },
    NativeFunctionRunner,
};
use convex_native_distributed::{
    DistributedFunctionRunner,
    FunctionExecutionServer,
    TonicWorkerClient,
};
use convex_native_integration_tests::db_fixture::DbFixture;
use database::Database;
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use runtime::prod::ProdRuntime;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use value::{
    ConvexObject,
    FieldName,
    TableNamespace,
};

// Force fixture app inventory entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

fn empty_args() -> ConvexObject {
    let map: BTreeMap<FieldName, value::ConvexValue> = BTreeMap::new();
    ConvexObject::try_from(map).unwrap()
}

async fn spawn_worker(db: Database<ProdRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let native = Arc::new(NativeFunctionRunner::from_inventory().unwrap());
    let server = FunctionExecutionServer::new(native)
        .with_database(db)
        .with_registry_version("integration-1.0.0");
    tokio::spawn(async move {
        Server::builder()
            .add_service(FunctionExecutionServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn http_action_round_trips_request_response_over_the_wire() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            ExecuteRequest {
                name: "__http::POST:/api/ping".to_string(),
                namespace: TableNamespace::Global,
                args: empty_args(),
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: None,
                existing_writes: Vec::new(),
                http_request: Some(HttpActionRequestPayload {
                    method: "POST".to_string(),
                    url: "http://example.invalid/api/ping".to_string(),
                    headers: vec![("content-type".to_string(), "text/plain".to_string())],
                    body: bytes::Bytes::from_static(b"world"),
                    routed_path: "/api/ping".to_string(),
                }),
                identity: Vec::new(),
            },
            UdfType::HttpAction,
        )
        .await?;
    let http = resp
        .http_response
        .expect("HttpAction response must carry an http_response payload");
    assert_eq!(http.status, 200);
    assert_eq!(http.body, bytes::Bytes::from_static(b"pong:world"));
    assert!(
        resp.final_tx.is_none(),
        "HTTP actions don't open a top-level tx",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_action_json_body_and_header_round_trip_over_the_wire() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            ExecuteRequest {
                name: "__http::POST:/api/echo".to_string(),
                namespace: TableNamespace::Global,
                args: empty_args(),
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: None,
                existing_writes: Vec::new(),
                http_request: Some(HttpActionRequestPayload {
                    method: "POST".to_string(),
                    url: "http://example.invalid/api/echo".to_string(),
                    headers: vec![
                        ("content-type".to_string(), "application/json".to_string()),
                        ("x-via".to_string(), "wire".to_string()),
                    ],
                    body: bytes::Bytes::from_static(b"{\"name\":\"bob\"}"),
                    routed_path: "/api/echo".to_string(),
                }),
                identity: Vec::new(),
            },
            UdfType::HttpAction,
        )
        .await?;
    let http = resp
        .http_response
        .expect("HttpAction response must carry an http_response payload");
    assert_eq!(http.status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&http.body)?;
    assert_eq!(parsed, serde_json::json!({"hello": "bob", "via": "wire"}));
    let ct = http
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.as_str())
        .expect("Content-Type header should be set by HttpResponse::json");
    assert_eq!(ct, "application/json");
    Ok(())
}
