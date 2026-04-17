//! Conductor-side client. Phase 3.4 of
//! `convex-native/IMPLEMENTATION_PLAN.md`.
//!
//! `DistributedFunctionRunner` holds a pool of `WorkerClient`s —
//! one per worker endpoint — and steers each `execute` call to the
//! one that looks least busy. The load balancer is Power-of-2-Choices
//! (P2C): pick two workers at random, compare their in-flight
//! estimates, send to the lower. On an `Unavailable` gRPC error the
//! conductor retries against the other chosen worker, then gives up.
//!
//! The `WorkerClient` trait is the seam for testing: the real
//! implementation (`TonicWorkerClient`, added in a follow-up
//! commit) wraps a `FunctionExecutionServiceClient` and tracks
//! in-flight locally; tests use the in-memory `MockWorkerClient`
//! below.

use std::sync::{
    atomic::{
        AtomicU64,
        Ordering,
    },
    Arc,
};

use async_trait::async_trait;
use common::types::UdfType;
use convex_native::distributed::{
    ExecuteRequest,
    ExecuteResponse,
};
use pb::function_execution as proto;
use tonic::Status;

/// A single worker the conductor can dispatch to. Implementations
/// are `Arc`-shared.
#[async_trait]
pub trait WorkerClient: Send + Sync {
    /// Execute one function on the worker. Returns `Ok(response)`
    /// when the worker produced an `ExecuteResponse` (including
    /// handler-level errors surfaced via
    /// `ExecuteResponse.result = Err(...)`); returns `Err(status)`
    /// for transport / gRPC-level failures the conductor may want
    /// to retry.
    async fn execute(
        &self,
        req: ExecuteRequest,
        udf_type: UdfType,
    ) -> Result<ExecuteResponse, Status>;

    /// Fresh health probe. Used by the conductor's readiness and
    /// version-gating paths.
    async fn health(&self) -> Result<proto::HealthResponse, Status>;

    /// Cheap local estimate of outstanding requests. Consumed by
    /// the P2C load balancer; no network round-trip.
    fn in_flight_estimate(&self) -> u64;
}

/// Strategy the runner uses to pick a worker for each request.
/// Exposed so tests can swap in deterministic choices.
pub trait Chooser: Send + Sync {
    /// Return two worker indices in `[0, n)`. The runner sends the
    /// request to whichever has the lower `in_flight_estimate()`.
    /// The same index may be returned twice — the runner degrades
    /// gracefully (no double-send).
    fn pick_two(&self, n: usize) -> (usize, usize);
}

/// Default `rand`-based P2C chooser.
pub struct RandomChooser;

impl Chooser for RandomChooser {
    fn pick_two(&self, n: usize) -> (usize, usize) {
        use rand::Rng;
        let mut rng = rand::rng();
        if n <= 1 {
            return (0, 0);
        }
        (rng.random_range(0..n), rng.random_range(0..n))
    }
}

/// Conductor-side function runner. Keeps an `Arc<dyn WorkerClient>`
/// per worker and routes each call via the chosen `Chooser`.
pub struct DistributedFunctionRunner {
    workers: Vec<Arc<dyn WorkerClient>>,
    chooser: Arc<dyn Chooser>,
    retries: u32,
}

impl DistributedFunctionRunner {
    /// Construct with the default `RandomChooser` and one retry
    /// on transient errors.
    pub fn new(workers: Vec<Arc<dyn WorkerClient>>) -> anyhow::Result<Self> {
        if workers.is_empty() {
            anyhow::bail!("DistributedFunctionRunner requires at least one worker");
        }
        Ok(Self {
            workers,
            chooser: Arc::new(RandomChooser),
            retries: 1,
        })
    }

    /// Replace the chooser. Tests use this to make dispatch
    /// deterministic.
    pub fn with_chooser(mut self, chooser: Arc<dyn Chooser>) -> Self {
        self.chooser = chooser;
        self
    }

    /// Set the number of retries on `Unavailable` errors. Default 1.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Power-of-2-choices: pick two indices, send to the one with
    /// fewer in-flight requests. On `Unavailable` retry against
    /// the other, then bail.
    pub async fn execute(
        &self,
        req: ExecuteRequest,
        udf_type: UdfType,
    ) -> Result<ExecuteResponse, Status> {
        let n = self.workers.len();
        let (i, j) = self.chooser.pick_two(n);
        let (primary, backup) = self.rank(i, j);

        let mut last_err: Option<Status> = None;
        let mut attempts_left = self.retries + 1;
        for idx in [primary, backup] {
            if attempts_left == 0 {
                break;
            }
            attempts_left -= 1;
            let worker = &self.workers[idx];
            match worker.execute(req.clone(), udf_type).await {
                Ok(resp) => return Ok(resp),
                Err(e) if e.code() == tonic::Code::Unavailable => {
                    last_err = Some(e);
                    continue;
                },
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Status::unavailable("DistributedFunctionRunner: all workers failed")
        }))
    }

    /// Return `(less_busy, more_busy)`. Ties break toward `a`.
    fn rank(&self, a: usize, b: usize) -> (usize, usize) {
        if a == b {
            return (a, a);
        }
        let ia = self.workers[a].in_flight_estimate();
        let ib = self.workers[b].in_flight_estimate();
        if ia <= ib {
            (a, b)
        } else {
            (b, a)
        }
    }
}

/// In-memory mock client for tests. Records every call so
/// assertions can inspect routing behaviour.
pub struct MockWorkerClient {
    pub name: &'static str,
    /// Atomic so `in_flight_estimate` doesn't require a mutex.
    pub in_flight: AtomicU64,
    /// Closure producing a response (or error) per call. The
    /// closure takes a reference to the request so tests can
    /// assert on the routed value.
    pub handler:
        Box<dyn Fn(&ExecuteRequest, UdfType) -> Result<ExecuteResponse, Status> + Send + Sync>,
    pub call_count: AtomicU64,
}

impl MockWorkerClient {
    pub fn new(name: &'static str, initial_in_flight: u64) -> Arc<Self> {
        Arc::new(Self {
            name,
            in_flight: AtomicU64::new(initial_in_flight),
            handler: Box::new(|_, _| Ok(default_ok_response())),
            call_count: AtomicU64::new(0),
        })
    }

    pub fn with_handler<F>(name: &'static str, initial_in_flight: u64, handler: F) -> Arc<Self>
    where
        F: Fn(&ExecuteRequest, UdfType) -> Result<ExecuteResponse, Status> + Send + Sync + 'static,
    {
        Arc::new(Self {
            name,
            in_flight: AtomicU64::new(initial_in_flight),
            handler: Box::new(handler),
            call_count: AtomicU64::new(0),
        })
    }

    pub fn calls(&self) -> u64 {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl WorkerClient for MockWorkerClient {
    async fn execute(
        &self,
        req: ExecuteRequest,
        udf_type: UdfType,
    ) -> Result<ExecuteResponse, Status> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        (self.handler)(&req, udf_type)
    }

    async fn health(&self) -> Result<proto::HealthResponse, Status> {
        Ok(proto::HealthResponse {
            registry_version: "test".to_string(),
            accepts_traffic: true,
            registered_functions: 0,
            in_flight: self.in_flight.load(Ordering::SeqCst),
        })
    }

    fn in_flight_estimate(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }
}

fn default_ok_response() -> ExecuteResponse {
    ExecuteResponse {
        result: Ok(value::ConvexValue::Null),
    }
}

/// Deterministic chooser for tests.
pub struct FixedChooser(pub (usize, usize));

impl Chooser for FixedChooser {
    fn pick_two(&self, _n: usize) -> (usize, usize) {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use value::{
        ConvexObject,
        ConvexValue,
        FieldName,
        TableNamespace,
    };

    use super::*;

    fn req() -> ExecuteRequest {
        let obj: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
        ExecuteRequest {
            name: "get_user".to_string(),
            namespace: TableNamespace::Global,
            args: ConvexObject::try_from(obj).unwrap(),
            timeout: None,
        }
    }

    #[tokio::test]
    async fn empty_worker_set_rejected_at_construction() {
        assert!(DistributedFunctionRunner::new(vec![]).is_err());
    }

    #[tokio::test]
    async fn p2c_routes_to_less_busy_worker() {
        // Worker A is loaded, worker B is idle.
        let a = MockWorkerClient::new("A", 10);
        let b = MockWorkerClient::new("B", 0);
        let runner = DistributedFunctionRunner::new(vec![a.clone(), b.clone()])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))));
        runner.execute(req(), UdfType::Action).await.unwrap();
        assert_eq!(a.calls(), 0, "loaded worker should be skipped");
        assert_eq!(b.calls(), 1);
    }

    #[tokio::test]
    async fn p2c_degrades_when_chooser_returns_same_index() {
        let a = MockWorkerClient::new("A", 0);
        let b = MockWorkerClient::new("B", 0);
        let runner = DistributedFunctionRunner::new(vec![a.clone(), b.clone()])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 0))));
        runner.execute(req(), UdfType::Action).await.unwrap();
        // Both picks are index 0, so only A runs; no double-send.
        assert_eq!(a.calls(), 1);
        assert_eq!(b.calls(), 0);
    }

    #[tokio::test]
    async fn unavailable_failover_retries_backup_worker() {
        // Primary errors Unavailable; backup succeeds.
        let a =
            MockWorkerClient::with_handler("A", 0, |_, _| Err(Status::unavailable("A is down")));
        let b = MockWorkerClient::new("B", 5);
        let runner = DistributedFunctionRunner::new(vec![a.clone(), b.clone()])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))));
        // A has lower in_flight (0 vs 5) so it's primary; on its
        // Unavailable error we retry B.
        let resp = runner.execute(req(), UdfType::Action).await.unwrap();
        assert!(resp.result.is_ok());
        assert_eq!(a.calls(), 1);
        assert_eq!(b.calls(), 1);
    }

    #[tokio::test]
    async fn non_unavailable_error_bubbles_up_without_retry() {
        // Primary errors InvalidArgument; we must NOT retry.
        let a = MockWorkerClient::with_handler("A", 0, |_, _| {
            Err(Status::invalid_argument("bad request"))
        });
        let b = MockWorkerClient::new("B", 5);
        let runner = DistributedFunctionRunner::new(vec![a.clone(), b.clone()])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))));
        let err = runner.execute(req(), UdfType::Action).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(a.calls(), 1);
        assert_eq!(b.calls(), 0);
    }

    #[tokio::test]
    async fn both_workers_unavailable_returns_last_error() {
        let a = MockWorkerClient::with_handler("A", 0, |_, _| Err(Status::unavailable("A down")));
        let b = MockWorkerClient::with_handler("B", 0, |_, _| Err(Status::unavailable("B down")));
        let runner = DistributedFunctionRunner::new(vec![a.clone(), b.clone()])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))));
        let err = runner.execute(req(), UdfType::Action).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(a.calls(), 1);
        assert_eq!(b.calls(), 1);
    }

    #[test]
    fn conversions_module_round_trip_check() {
        // Sanity-check that the client and the conversions module
        // agree on the same types — any refactor that breaks the
        // seam between them would fail here at compile time too.
        let native = req();
        let encoded =
            crate::conversions::to_proto_request(&native, UdfType::Action).expect("encode");
        assert_eq!(encoded.name, "get_user");
    }
}
