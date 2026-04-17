//! Worker-side gRPC server. Phase 3.3 of
//! `convex-native/IMPLEMENTATION_PLAN.md`.
//!
//! `FunctionExecutionServer` wraps an `Arc<NativeFunctionRunner>`
//! and implements the generated
//! `pb::function_execution::function_execution_service_server::FunctionExecutionService`
//! tonic trait. A worker process instantiates this, binds it to a
//! `tonic::transport::Server`, and serves conductor traffic.
//!
//! ## What ships here
//!
//! - `health`: fully implemented, reports registry version (derived from the
//!   `convex_native` crate version), `accepts_traffic` (false when the runner
//!   is draining), `registered_functions`, and `in_flight`.
//! - `execute` for `UdfType::Action`: fully implemented via
//!   `NativeFunctionRunner::run_action_with_callbacks`. Until the worker is
//!   wired to a real `ActionCallbacks`, actions use
//!   `convex_native::callbacks::NoopCallbacks`, so any sub-call from the action
//!   body errors at the callback boundary.
//! - `execute` for `UdfType::Query` / `UdfType::Mutation`: when the server was
//!   constructed with `.with_database(db)`, dispatches inline against a fresh
//!   `Transaction<Rt>` (queries drop the tx; mutations commit via
//!   `commit_with_write_source`). Without the database handle the branch
//!   returns `Code::Unimplemented` so the conductor learns the worker wasn't
//!   provisioned for query/mutation traffic. This matches the "pure worker"
//!   model where the worker owns its own Database and commits locally — the
//!   wire protocol doesn't carry read/write sets back to the conductor today.
//! - `execute` for `UdfType::HttpAction`: likewise `Unimplemented`. HTTP
//!   actions use a different dispatch path anyway (`HttpRouter`).

use std::sync::Arc;

use async_trait::async_trait;
use common::types::UdfType;
use convex_native::{
    NativeFunctionRunner,
    Rt,
};
use database::{
    Database,
    WriteSource,
};
use keybroker::Identity;
use pb::function_execution::{
    self as proto,
    function_execution_service_server::FunctionExecutionService,
};
use tonic::{
    Request,
    Response,
    Status,
};
use usage_tracking::FunctionUsageTracker;

use crate::conversions;

/// Worker-side server. Holds a shared native runner and answers
/// `FunctionExecutionService` RPCs.
///
/// Cheap to clone (it's just `Arc`s) — the generated
/// `FunctionExecutionServiceServer::new(server)` wraps it in a
/// `tonic::service::Routes`.
#[derive(Clone)]
pub struct FunctionExecutionServer {
    native: Arc<NativeFunctionRunner>,
    /// Worker-local database handle. When set, the server can
    /// dispatch queries and mutations inline against a fresh
    /// `Transaction<Rt>`. Without it, query/mutation requests
    /// return `Code::Unimplemented`.
    database: Option<Database<Rt>>,
    /// Registry version reported in `Health`. Defaults to the
    /// `convex_native` crate version; callers can override if they
    /// want a finer-grained tag.
    registry_version: String,
}

impl FunctionExecutionServer {
    pub fn new(native: Arc<NativeFunctionRunner>) -> Self {
        Self {
            native,
            database: None,
            registry_version: convex_native::VERSION.to_string(),
        }
    }

    /// Attach a worker-local database so the server can dispatch
    /// queries and mutations. The conductor is still the one that
    /// chose the worker — the worker just commits locally because
    /// the wire protocol doesn't carry read/write sets back to the
    /// conductor.
    ///
    /// This model fits the "pure worker" topology where each
    /// worker owns its own Database. If you want the conductor-
    /// commits topology instead, keep the server without a database
    /// and the query/mutation branches will surface `Unimplemented`.
    pub fn with_database(mut self, database: Database<Rt>) -> Self {
        self.database = Some(database);
        self
    }

    /// Override the registry version. Useful for tests and for
    /// per-deploy tagging during rolling updates (Phase 4.7).
    pub fn with_registry_version(mut self, v: impl Into<String>) -> Self {
        self.registry_version = v.into();
        self
    }
}

#[async_trait]
impl FunctionExecutionService for FunctionExecutionServer {
    async fn execute(
        &self,
        request: Request<proto::ExecuteRequest>,
    ) -> Result<Response<proto::ExecuteResponse>, Status> {
        let proto_req = request.into_inner();

        // Version gate (Phase 4.7). If the conductor asked for a
        // minimum registry version, confirm we meet it. String
        // comparison is semver-correct for the canonical
        // MAJOR.MINOR.PATCH format used by Cargo versions; pre-release
        // tags are handled by the conductor's semver check, not
        // here.
        if let Some(min) = proto_req.min_registry_version.as_deref()
            && !version_at_least(&self.registry_version, min)
        {
            return Err(Status::failed_precondition(format!(
                "worker registry_version {:?} is older than min_registry_version {:?}",
                self.registry_version, min,
            )));
        }

        let (native_req, udf_type) = conversions::from_proto_request(&proto_req)
            .map_err(|e| Status::invalid_argument(format!("decode ExecuteRequest: {e}")))?;

        match udf_type {
            UdfType::Action => {
                let callbacks = Arc::new(convex_native::callbacks::NoopCallbacks);
                let result = self
                    .native
                    .run_action_with_callbacks(
                        &native_req.name,
                        native_req.namespace,
                        native_req.args,
                        callbacks,
                    )
                    .await;
                let native_response = convex_native::distributed::ExecuteResponse {
                    result: result.map_err(|e| e.to_string()),
                };
                let mut proto_resp = conversions::to_proto_response(&native_response);
                proto_resp.served_by_version = Some(self.registry_version.clone());
                Ok(Response::new(proto_resp))
            },
            UdfType::Query | UdfType::Mutation => {
                let Some(database) = self.database.as_ref() else {
                    return Err(Status::unimplemented(
                        "FunctionExecutionServer: query/mutation dispatch requires a worker-local \
                         Database<Rt>. Construct the server with .with_database(db) to enable it.",
                    ));
                };
                let result = match udf_type {
                    UdfType::Query => run_query_inline(&self.native, database, &native_req).await,
                    UdfType::Mutation => {
                        run_mutation_inline(&self.native, database, &native_req).await
                    },
                    _ => unreachable!(),
                };
                let native_response = convex_native::distributed::ExecuteResponse {
                    result: result.map_err(|e| e.to_string()),
                };
                let mut proto_resp = conversions::to_proto_response(&native_response);
                proto_resp.served_by_version = Some(self.registry_version.clone());
                Ok(Response::new(proto_resp))
            },
            UdfType::HttpAction => Err(Status::unimplemented(
                "FunctionExecutionServer: HTTP actions use the HttpRouter dispatch path, not \
                 FunctionExecutionService.",
            )),
        }
    }

    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        let accepts_traffic = !self.native.is_draining();
        let resp = proto::HealthResponse {
            registry_version: self.registry_version.clone(),
            accepts_traffic,
            registered_functions: self.native.len() as u64,
            in_flight: self.native.in_flight(),
        };
        Ok(Response::new(resp))
    }
}

/// Open a fresh transaction, run the native query, drop the tx.
/// Queries are read-only by design — no commit needed.
async fn run_query_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native::distributed::ExecuteRequest,
) -> anyhow::Result<value::ConvexValue> {
    let ts = database.now_ts_for_reads();
    let usage = FunctionUsageTracker::new();
    let mut tx = database
        .begin_with_ts(Identity::system(), *ts, usage)
        .await?;
    let result = native
        .run_query(&req.name, &mut tx, req.namespace, req.args.clone())
        .await?;
    Ok(result)
}

/// Open a fresh transaction, run the native mutation, commit on
/// handler success. On handler error the tx is dropped without
/// committing.
async fn run_mutation_inline(
    native: &Arc<NativeFunctionRunner>,
    database: &Database<Rt>,
    req: &convex_native::distributed::ExecuteRequest,
) -> anyhow::Result<value::ConvexValue> {
    let ts = database.now_ts_for_reads();
    let usage = FunctionUsageTracker::new();
    let mut tx = database
        .begin_with_ts(Identity::system(), *ts, usage)
        .await?;
    let result = native
        .run_mutation(&req.name, &mut tx, req.namespace, req.args.clone())
        .await?;
    database
        .commit_with_write_source(tx, WriteSource::system("convex_native_distributed"))
        .await?;
    Ok(result)
}

/// Returns true when `have` >= `want` under a simple lexicographic
/// compare of the MAJOR.MINOR.PATCH form. A full `semver` compare
/// belongs on the conductor side (that's where the full version
/// set is visible); the worker is just checking its own tag.
fn version_at_least(have: &str, want: &str) -> bool {
    fn parts(s: &str) -> Vec<u64> {
        s.split(|c| c == '.' || c == '-' || c == '+')
            .filter_map(|p| p.parse::<u64>().ok())
            .collect()
    }
    parts(have) >= parts(want)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::Duration,
    };

    use convex_native::distributed::ExecuteRequest as NativeExecuteRequest;
    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableNamespace,
    };

    use super::*;

    fn empty_runner() -> Arc<NativeFunctionRunner> {
        Arc::new(NativeFunctionRunner::from_inventory().expect("collect"))
    }

    fn empty_object() -> ConvexObject {
        let f: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        ConvexObject::try_from(f).unwrap()
    }

    #[tokio::test]
    async fn health_reports_registry_version_and_empty_registry() {
        let server = FunctionExecutionServer::new(empty_runner()).with_registry_version("1.2.3");
        let resp = server
            .health(Request::new(proto::HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.registry_version, "1.2.3");
        assert!(resp.accepts_traffic);
        assert_eq!(resp.registered_functions, 0);
        assert_eq!(resp.in_flight, 0);
    }

    #[tokio::test]
    async fn execute_unknown_action_surfaces_runner_error_in_response() {
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "does_not_exist".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: None,
            min_registry_version: None,
        };
        let proto_req = conversions::to_proto_request(&native, UdfType::Action).unwrap();
        let resp = server
            .execute(Request::new(proto_req))
            .await
            .unwrap()
            .into_inner();
        // Runner returns a user-level error, not a gRPC Status —
        // that's the contract for "handler executed, handler errored".
        let native_resp = conversions::from_proto_response(&resp).unwrap();
        assert!(matches!(native_resp.result, Err(ref m) if m.contains("does_not_exist")));
    }

    #[tokio::test]
    async fn execute_query_without_database_is_unimplemented() {
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "anything".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: Some(Duration::from_millis(100)),
            min_registry_version: None,
        };
        let proto_req = conversions::to_proto_request(&native, UdfType::Query).unwrap();
        let status = server.execute(Request::new(proto_req)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn version_gate_rejects_older_worker() {
        let server = FunctionExecutionServer::new(empty_runner()).with_registry_version("0.1.0");
        let native = NativeExecuteRequest {
            name: "doesnt_matter".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: None,
            min_registry_version: None,
        };
        let mut proto_req = conversions::to_proto_request(&native, UdfType::Action).unwrap();
        proto_req.min_registry_version = Some("9.9.9".to_string());
        let status = server.execute(Request::new(proto_req)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn version_compare_obeys_major_minor_patch() {
        assert!(version_at_least("1.2.3", "1.2.3"));
        assert!(version_at_least("1.2.4", "1.2.3"));
        assert!(version_at_least("2.0.0", "1.99.99"));
        assert!(!version_at_least("1.2.3", "1.2.4"));
        assert!(!version_at_least("0.1.0", "9.9.9"));
    }
}
