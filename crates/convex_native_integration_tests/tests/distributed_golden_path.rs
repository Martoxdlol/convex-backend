//! Distributed-topology integration tests.
//!
//! Stands up a real `FunctionExecutionServer` with a
//! `Database<ProdRuntime>` attached (in-memory sqlite) + a
//! `TonicWorkerClient` dialing it over an ephemeral TCP port +
//! a `DistributedFunctionRunner`. Drives the fixture app's
//! queries / mutations through that wire end-to-end and asserts
//! observable behaviour at the response level (handler result,
//! `FinalTxSummary` shape, error propagation).
//!
//! What this file covers today:
//! - Query dispatch over the wire against seeded data.
//! - Mutation dispatch — the worker runs the handler against a
//!   `Transaction<Rt>` and returns a populated `DistributedFinalTx` (the
//!   backend is responsible for committing it; the test applies the summary to
//!   its own `Database` to close the loop).
//! - Handler-error propagation verbatim through tonic.
//!
//! Out of scope here (covered by follow-up files):
//! - Action sub-calls through `BackendCallbackServer`.
//! - HTTP action dispatch.
//! - Admission + pool churn (already covered by
//!   `convex_native_distributed::tests::admission_churn`).

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
};

use common::types::UdfType;
use convex_native_core::{
    distributed::ExecuteRequest,
    NativeFunctionRunner,
};
use convex_native_distributed::{
    DistributedFunctionRunner,
    FunctionExecutionServer,
    TonicWorkerClient,
};
use convex_native_integration_tests::db_fixture::DbFixture;
use database::Database;
use keybroker::Identity;
use pb::function_execution::function_execution_service_server::FunctionExecutionServiceServer;
use runtime::prod::ProdRuntime;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use usage_tracking::FunctionUsageTracker;
use value::{
    ConvexObject,
    ConvexValue,
    FieldName,
    TableNamespace,
};

// Force the fixture app's `inventory::submit!` entries to link.
#[allow(dead_code)]
type _ForceLink = convex_native_integration_tests::fixture_app::Todo;

/// Build an args `ConvexObject` from `(name, value)` pairs.
fn args(pairs: &[(&str, ConvexValue)]) -> ConvexObject {
    let mut map: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k.parse::<FieldName>().expect("field name"), v.clone());
    }
    ConvexObject::try_from(map).expect("args object")
}

fn empty_object() -> ConvexObject {
    ConvexObject::empty()
}

/// Spin up a worker-side `FunctionExecutionServer` backed by the
/// fixture `Database<ProdRuntime>` on an ephemeral TCP port.
/// Returns the bound address so the caller can build a
/// `TonicWorkerClient` against it.
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

/// Seed a todo row directly on the fixture's database — skips the
/// mutation handler so the seed doesn't depend on the feature
/// under test.
async fn seed_todo(
    db: &Database<ProdRuntime>,
    owner: &str,
    text: &str,
    done: bool,
) -> anyhow::Result<()> {
    use std::collections::BTreeMap;

    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableName,
    };
    let mut fields: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    fields.insert("owner".parse()?, ConvexValue::try_from(owner.to_string())?);
    fields.insert("text".parse()?, ConvexValue::try_from(text.to_string())?);
    fields.insert("done".parse()?, ConvexValue::Boolean(done));
    fields.insert("created_at".parse()?, ConvexValue::Float64(0.0));
    fields.insert("metadata".parse()?, ConvexValue::Null);
    let obj = ConvexObject::try_from(fields)?;
    let usage = FunctionUsageTracker::new();
    let mut tx = db
        .begin_with_ts(Identity::system(), *db.now_ts_for_reads(), usage)
        .await?;
    let table: TableName = "todos".parse()?;
    database::UserFacingModel::new(&mut tx, TableNamespace::Global)
        .insert(table, obj)
        .await?;
    db.commit_with_write_source(tx, "distributed_test_seed")
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn query_dispatches_over_the_wire_and_sees_database_state() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    seed_todo(&fx.database, "zoe", "one", false).await?;
    seed_todo(&fx.database, "zoe", "two", true).await?;
    seed_todo(&fx.database, "eve", "three", false).await?;

    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            ExecuteRequest {
                name: "list_todos".to_string(),
                namespace: TableNamespace::Global,
                args: args(&[("owner", ConvexValue::try_from("zoe".to_string())?)]),
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: Some(u64::from(*fx.database.now_ts_for_reads())),
                existing_writes: Vec::new(),
                http_request: None,
                identity: Vec::new(),
            },
            UdfType::Query,
        )
        .await?;
    let value = resp.result.expect("query succeeds");
    let arr = match value {
        ConvexValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(
        arr.len(),
        2,
        "query only sees zoe's rows, not eve's: got {}",
        arr.len(),
    );
    assert!(
        resp.final_tx.is_some(),
        "a query that successfully opens a tx returns a FinalTxSummary the backend can \
         OCC-validate",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_dispatch_returns_final_tx_summary() -> anyhow::Result<()> {
    // Worker runs `create_todo` against its tx, returns the
    // `DistributedFinalTx` summary; the backend's Committer is
    // responsible for the actual commit. Here we assert the wire
    // shape + non-empty writes count.
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            ExecuteRequest {
                name: "create_todo".to_string(),
                namespace: TableNamespace::Global,
                args: args(&[
                    ("owner", ConvexValue::try_from("alice".to_string())?),
                    ("text", ConvexValue::try_from("ship it".to_string())?),
                ]),
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: Some(u64::from(*fx.database.now_ts_for_reads())),
                existing_writes: Vec::new(),
                http_request: None,
                identity: Vec::new(),
            },
            UdfType::Mutation,
        )
        .await?;
    let value = resp.result.expect("mutation succeeds");
    match value {
        ConvexValue::String(s) => {
            assert!(!s.is_empty(), "mutation returns the inserted id");
        },
        other => panic!("expected id string, got {other:?}"),
    }
    let final_tx = resp.final_tx.expect("mutation returns FinalTxSummary");
    assert!(
        !final_tx.writes.is_empty(),
        "create_todo produces at least one write in the summary (first-insert also creates the \
         table + metadata rows)",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_function_errors_verbatim_through_the_wire() -> anyhow::Result<()> {
    let fx = DbFixture::new_in_memory().await?;
    let addr = spawn_worker(fx.database.clone()).await;
    let client = TonicWorkerClient::connect(format!("http://{addr}")).await?;
    let runner = DistributedFunctionRunner::new(vec![client])?;

    let resp = runner
        .execute(
            ExecuteRequest {
                name: "does_not_exist".to_string(),
                namespace: TableNamespace::Global,
                args: empty_object(),
                timeout: None,
                min_registry_version: None,
                execution_context: None,
                begin_timestamp: Some(u64::from(*fx.database.now_ts_for_reads())),
                existing_writes: Vec::new(),
                http_request: None,
                identity: Vec::new(),
            },
            UdfType::Query,
        )
        .await?;
    assert!(
        matches!(&resp.result, Err(m) if m.contains("does_not_exist")),
        "handler-level error surfaces verbatim: {:?}",
        resp.result,
    );
    Ok(())
}
