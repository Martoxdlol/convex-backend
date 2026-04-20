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

#[tokio::test(flavor = "multi_thread")]
async fn delete_method_dispatches_over_the_wire() -> anyhow::Result<()> {
    // Standalone test covers the DELETE verb registration +
    // dispatch. Mirroring it over the wire confirms the proto
    // `method` field survives the transport intact — a
    // regression that uppercase-normalised the method (e.g.
    // "Delete" / "delete" → "DELETE" only on send, leaving the
    // receiver looking up "Delete") would break every
    // non-GET/POST handler in the distributed topology.
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "__http::DELETE:/api/item".to_string(),
                namespace: TableNamespace::Global,
                args: empty_args(),
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: None,
                existing_writes: Vec::new(),
                http_request: Some(HttpActionRequestPayload {
                    method: "DELETE".to_string(),
                    url: "http://example.invalid/api/item".to_string(),
                    headers: vec![],
                    body: bytes::Bytes::new(),
                    routed_path: "/api/item".to_string(),
                }),
                identity: Vec::new(),
            },
            UdfType::HttpAction,
        )
        .await?;
    let http = resp
        .http_response
        .expect("HttpAction response must carry an http_response payload");
    assert_eq!(http.status, 204);
    assert!(http.body.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn http_action_sub_mutation_routes_through_backend_callbacks_over_the_wire(
) -> anyhow::Result<()> {
    // The HTTP-action branch of FunctionExecutionServer shares
    // its BackendCallbackClient setup with the action branch
    // (same `with_backend_callback_endpoint` knob). Close the
    // distributed coverage on it by driving `create_via_http`
    // (which does `ctx.run_mutation_raw("create_todo", ...)`)
    // through a worker + backend-callback server and asserting
    // the response body carries the stub id.
    use std::{
        collections::BTreeMap,
        sync::Arc,
    };

    use async_trait::async_trait;
    use common::{
        bootstrap_model::components::handles::FunctionHandle,
        components::{
            CanonicalizedComponentFunctionPath,
            ComponentId,
            ComponentPath,
        },
        execution_context::ExecutionContext,
        runtime::UnixTimestamp,
    };
    use convex_native_distributed::backend_callbacks_server::BackendCallbackServer;
    use keybroker::Identity;
    use model::file_storage::{
        types::FileStorageEntry,
        FileStorageId,
    };
    use pb::backend_callbacks::backend_callback_service_server::BackendCallbackServiceServer;
    use serde_json::Value as JsonValue;
    use sync_types::types::SerializedArgs;
    use udf::{
        ActionCallbacks,
        FunctionResult,
    };
    use usage_tracking::FunctionUsageStats;
    use value::{
        DeveloperDocumentId,
        JsonPackedValue,
    };
    use vector::PublicVectorSearchQueryResult;

    struct StubMutationCallbacks;

    #[async_trait]
    impl ActionCallbacks for StubMutationCallbacks {
        async fn execute_query(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("unused")
        }

        async fn execute_mutation(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            Ok(FunctionResult {
                result: Ok(JsonPackedValue::from_network(
                    "\"http-stub-id\"".to_string(),
                )?),
            })
        }

        async fn execute_action(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("unused")
        }

        async fn storage_get_url(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageId,
        ) -> anyhow::Result<Option<String>> {
            Ok(None)
        }

        async fn storage_delete(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageId,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn storage_get_file_entry(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageId,
        ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
            Ok(None)
        }

        async fn storage_store_file_entry(
            &self,
            _: Identity,
            _: ComponentId,
            _: FileStorageEntry,
        ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
            anyhow::bail!("unused")
        }

        async fn schedule_job(
            &self,
            _: Identity,
            _: ComponentId,
            _: CanonicalizedComponentFunctionPath,
            _: SerializedArgs,
            _: UnixTimestamp,
            _: ExecutionContext,
        ) -> anyhow::Result<DeveloperDocumentId> {
            anyhow::bail!("unused")
        }

        async fn cancel_job(&self, _: Identity, _: DeveloperDocumentId) -> anyhow::Result<()> {
            Ok(())
        }

        async fn vector_search(
            &self,
            _: Identity,
            _: JsonValue,
        ) -> anyhow::Result<(Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)> {
            anyhow::bail!("unused")
        }

        async fn lookup_function_handle(
            &self,
            _: Identity,
            _: FunctionHandle,
        ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
            anyhow::bail!("unused")
        }

        async fn create_function_handle(
            &self,
            _: Identity,
            _: CanonicalizedComponentFunctionPath,
        ) -> anyhow::Result<FunctionHandle> {
            anyhow::bail!("unused")
        }
    }

    let cb_listener = TcpListener::bind("127.0.0.1:0").await?;
    let cb_addr = cb_listener.local_addr()?;
    let cb_server = BackendCallbackServer::new(Arc::new(StubMutationCallbacks));
    tokio::spawn(async move {
        Server::builder()
            .add_service(BackendCallbackServiceServer::new(cb_server))
            .serve_with_incoming(TcpListenerStream::new(cb_listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;

    // Worker wired with the backend-callback endpoint.
    let fx = DbFixture::new_in_memory().await?;
    let worker_listener = TcpListener::bind("127.0.0.1:0").await?;
    let worker_addr = worker_listener.local_addr()?;
    let native = Arc::new(NativeFunctionRunner::from_inventory()?);
    let server = FunctionExecutionServer::new(native)
        .with_database(fx.database.clone())
        .with_backend_callback_endpoint(format!("http://{cb_addr}"))
        .with_registry_version("integration-1.0.0");
    tokio::spawn(async move {
        Server::builder()
            .add_service(FunctionExecutionServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(worker_listener))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;

    let client = TonicWorkerClient::connect(format!("http://{worker_addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let mut args: BTreeMap<FieldName, value::ConvexValue> = BTreeMap::new();
    let _ = &mut args; // silence unused_mut when args stays empty below
    let resp = runner
        .execute(
            ExecuteRequest {
                name: "__http::POST:/api/create".to_string(),
                namespace: TableNamespace::Global,
                args: ConvexObject::try_from(args)?,
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: None,
                existing_writes: Vec::new(),
                http_request: Some(HttpActionRequestPayload {
                    method: "POST".to_string(),
                    url: "http://example.invalid/api/create".to_string(),
                    headers: vec![("x-owner".to_string(), "alice".to_string())],
                    body: bytes::Bytes::from_static(b"body text"),
                    routed_path: "/api/create".to_string(),
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
    assert_eq!(http.body, bytes::Bytes::from_static(b"http-stub-id"));
    Ok(())
}
