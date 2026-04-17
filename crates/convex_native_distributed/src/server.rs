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
//! - `execute` for `UdfType::Query` / `UdfType::Mutation`: returns a clear
//!   `Unimplemented` gRPC status. Queries/mutations need a `Transaction<Rt>`
//!   built against a worker-local `Database<Rt>`; threading that into the
//!   server belongs in a follow-up commit (the worker process will own its own
//!   Database).
//! - `execute` for `UdfType::HttpAction`: likewise `Unimplemented`. HTTP
//!   actions use a different dispatch path anyway (`HttpRouter`).

use std::sync::Arc;

use async_trait::async_trait;
use common::types::UdfType;
use convex_native::NativeFunctionRunner;
use pb::function_execution::{
    self as proto,
    function_execution_service_server::FunctionExecutionService,
};
use tonic::{
    Request,
    Response,
    Status,
};

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
    /// Registry version reported in `Health`. Defaults to the
    /// `convex_native` crate version; callers can override if they
    /// want a finer-grained tag.
    registry_version: String,
}

impl FunctionExecutionServer {
    pub fn new(native: Arc<NativeFunctionRunner>) -> Self {
        Self {
            native,
            registry_version: convex_native::VERSION.to_string(),
        }
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
            UdfType::Query | UdfType::Mutation => Err(Status::unimplemented(
                "FunctionExecutionServer: query/mutation dispatch requires a worker-local \
                 Database<Rt>. Wire one via a follow-up commit; see IMPLEMENTATION_PLAN.md Phase \
                 3.3.",
            )),
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
    async fn execute_query_is_unimplemented_until_database_wiring() {
        let server = FunctionExecutionServer::new(empty_runner());
        let native = NativeExecuteRequest {
            name: "anything".to_string(),
            namespace: TableNamespace::Global,
            args: empty_object(),
            timeout: Some(Duration::from_millis(100)),
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
