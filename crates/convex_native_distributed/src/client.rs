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
//! The `WorkerClient` trait is the seam for testing. The real
//! implementation — [`crate::tonic_client::TonicWorkerClient`] —
//! wraps a `FunctionExecutionServiceClient<Channel>` and tracks
//! in-flight locally. Tests use the in-memory `MockWorkerClient`
//! below.

use std::{
    sync::{
        atomic::{
            AtomicU64,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        Instant,
    },
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

    /// Stable identifier for metrics / log labels. Real clients
    /// return the endpoint URL; mock clients return their test name.
    /// Default falls back to a constant so older implementors don't
    /// get broken when this is added.
    fn label(&self) -> &str {
        "unknown"
    }
}

/// Observability hook for the conductor side of the dispatch path.
///
/// Called once per `DistributedFunctionRunner::execute` — after
/// every attempt has finished (whether a failover retry was needed
/// or not). Implementors wire this into Prometheus / OpenTelemetry
/// / tracing the way their deployment's metrics stack expects.
///
/// The [`ConductorOutcome::RetryAttempted`] marker lets dashboards
/// separate "first attempt succeeded" from "backup salvaged the
/// call" so operators can tell the cluster is degrading before
/// requests actually start failing.
pub trait ConductorMetricsSink: Send + Sync + 'static {
    /// Report one completed dispatch. `worker` is the endpoint of
    /// the worker the final attempt landed on (or `None` when no
    /// worker was reachable — e.g. every attempt returned
    /// `Unavailable`). `latency` spans the entire `execute(...)`
    /// call, including any failover retry.
    fn record(
        &self,
        worker: Option<&str>,
        udf_type: UdfType,
        outcome: ConductorOutcome,
        latency: Duration,
    );
}

/// Outcome of one conductor-side `execute(...)` dispatch.
///
/// Mirrors the `native_funrun_request_*` metric families named in
/// `native-rust-functions.md` §12.3: successes on first try,
/// successes after a failover retry, and transport / gRPC-level
/// failures the conductor couldn't recover from.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConductorOutcome {
    /// First-attempt success. The handler returned a value (or a
    /// handler-level `ExecuteResponse::Err(...)`; the conductor
    /// doesn't distinguish handler errors from handler successes at
    /// this layer — both mean "the worker received and processed
    /// the request").
    Ok,
    /// First attempt failed with `Unavailable`; the backup picked
    /// up and returned a response. Operators want to alert on this
    /// rising before a user-visible failure mode appears.
    Retried,
    /// Every attempt failed at the transport / gRPC layer; the
    /// caller received an `Err(Status)`. `Status::code()` is
    /// carried through the sink for label construction (e.g.
    /// `status.code()` as a Prometheus label).
    Error(tonic::Code),
}

/// Default discards everything — the baseline wiring for deployments
/// that don't (yet) care about conductor-side metrics.
pub struct NoopConductorMetrics;

impl ConductorMetricsSink for NoopConductorMetrics {
    fn record(&self, _: Option<&str>, _: UdfType, _: ConductorOutcome, _: Duration) {}
}

/// Observability hook for forwarding worker-side `ctx.log()` output
/// into the conductor's own log-streaming path.
///
/// `FunctionExecutionServer` on the worker snapshots the ctx's
/// `LogBuffer` after every handler invocation and writes the
/// rendered lines into `ExecuteResponse::log_lines` (see
/// `function_execution.proto`). Without a sink the conductor
/// receives those lines but has nowhere to route them; wiring one
/// lets deployments forward them into syslog / Loki / fluentd /
/// whatever their log stack is.
///
/// Called once per completed `execute(...)` with the successful
/// response's log lines. No-op for transport failures (no response
/// means no worker-side logs to forward).
pub trait ConductorLogSink: Send + Sync + 'static {
    /// Forward the log lines a worker emitted for one completed
    /// request. `worker` is the endpoint label the request landed on.
    /// `lines` is the `"[LEVEL] message"`-shaped vec the worker
    /// wrote into `ExecuteResponse::log_lines`.
    fn forward(&self, worker: &str, udf_type: UdfType, lines: Vec<String>);
}

/// Default discards every log line.
pub struct NoopConductorLogs;

impl ConductorLogSink for NoopConductorLogs {
    fn forward(&self, _: &str, _: UdfType, _: Vec<String>) {}
}

/// In-memory capture sink — tests assert on the captured lines.
#[derive(Default)]
pub struct CapturingConductorLogs {
    inner: std::sync::Mutex<Vec<(String, UdfType, Vec<String>)>>,
}

impl CapturingConductorLogs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<(String, UdfType, Vec<String>)> {
        self.inner.lock().unwrap().clone()
    }
}

impl ConductorLogSink for CapturingConductorLogs {
    fn forward(&self, worker: &str, udf_type: UdfType, lines: Vec<String>) {
        self.inner
            .lock()
            .unwrap()
            .push((worker.to_string(), udf_type, lines));
    }
}

/// In-memory counter sink for tests. Records every call so
/// assertions can verify routing + outcome attribution.
#[derive(Default)]
pub struct CountingConductorMetrics {
    inner: std::sync::Mutex<CountingConductorInner>,
}

#[derive(Default)]
struct CountingConductorInner {
    /// `(worker_endpoint_or_none, udf_type, outcome) -> count`. Using
    /// `Option<String>` for the endpoint keeps the "no worker
    /// reachable" state distinguishable from any specific worker's
    /// bucket.
    calls: std::collections::BTreeMap<(Option<String>, UdfType, ConductorOutcome), u64>,
    total_latency: std::collections::BTreeMap<(Option<String>, UdfType), Duration>,
}

impl CountingConductorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn count(&self, worker: Option<&str>, udf_type: UdfType, outcome: ConductorOutcome) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .calls
            .get(&(worker.map(|s| s.to_string()), udf_type, outcome))
            .copied()
            .unwrap_or(0)
    }

    pub fn total_latency(&self, worker: Option<&str>, udf_type: UdfType) -> Duration {
        self.inner
            .lock()
            .unwrap()
            .total_latency
            .get(&(worker.map(|s| s.to_string()), udf_type))
            .copied()
            .unwrap_or_default()
    }
}

// `BTreeMap` keys need `Ord`. Order between variants is irrelevant
// for counting semantics — we just need a total order for map keys.
impl PartialOrd for ConductorOutcome {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConductorOutcome {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let key = |o: &ConductorOutcome| -> (u8, u16) {
            match o {
                ConductorOutcome::Ok => (0, 0),
                ConductorOutcome::Retried => (1, 0),
                ConductorOutcome::Error(c) => (2, *c as u16),
            }
        };
        key(self).cmp(&key(other))
    }
}

impl ConductorMetricsSink for CountingConductorMetrics {
    fn record(
        &self,
        worker: Option<&str>,
        udf_type: UdfType,
        outcome: ConductorOutcome,
        latency: Duration,
    ) {
        let worker_owned = worker.map(|s| s.to_string());
        let mut inner = self.inner.lock().unwrap();
        *inner
            .calls
            .entry((worker_owned.clone(), udf_type, outcome))
            .or_insert(0) += 1;
        let total = inner
            .total_latency
            .entry((worker_owned, udf_type))
            .or_default();
        *total += latency;
    }
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
    /// When true, an `Unavailable` from the primary worker retries
    /// once against the backup. When false, the primary's error
    /// bubbles up immediately. Defaults to `true`. P2C semantics
    /// cap this at one retry — trying a third worker would pick
    /// another random pair and defeat the point of the load-balancing
    /// decision, so multi-retry isn't modelled at this layer.
    failover: bool,
    /// Applied to every `ExecuteRequest` that doesn't already set
    /// one. Phase 4.7 rolling-update floor: during a deploy the
    /// operator pins a minimum `registry_version` here so the
    /// conductor routes around older workers. Workers that don't
    /// meet the floor reject with `tonic::Code::FailedPrecondition`.
    min_registry_version: Option<String>,
    /// Observability hook. Defaults to [`NoopConductorMetrics`];
    /// deployments that care about `native_funrun_*` metrics
    /// swap in their own sink via [`with_metrics`].
    metrics: Arc<dyn ConductorMetricsSink>,
    /// Log-forwarding hook. Defaults to [`NoopConductorLogs`];
    /// deployments wire their log stack through
    /// [`with_log_sink`] to forward worker `ctx.log()` output.
    log_sink: Arc<dyn ConductorLogSink>,
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
            failover: true,
            min_registry_version: None,
            metrics: Arc::new(NoopConductorMetrics),
            log_sink: Arc::new(NoopConductorLogs),
        })
    }

    /// Attach a metrics sink. Called once per `execute(...)` with
    /// the final outcome + latency. See [`ConductorMetricsSink`].
    pub fn with_metrics(mut self, metrics: Arc<dyn ConductorMetricsSink>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Attach a log-forwarding sink. Called once per successful
    /// `execute(...)` with the worker's `ExecuteResponse::log_lines`.
    /// See [`ConductorLogSink`].
    pub fn with_log_sink(mut self, log_sink: Arc<dyn ConductorLogSink>) -> Self {
        self.log_sink = log_sink;
        self
    }

    /// Replace the chooser. Tests use this to make dispatch
    /// deterministic.
    pub fn with_chooser(mut self, chooser: Arc<dyn Chooser>) -> Self {
        self.chooser = chooser;
        self
    }

    /// Enable or disable single-retry failover on `Unavailable`.
    /// Defaults to enabled. When disabled, the primary worker's
    /// error bubbles up without trying the backup — useful for
    /// clients that want strict primary-only semantics or that
    /// handle their own retry policy upstream.
    pub fn with_failover(mut self, enabled: bool) -> Self {
        self.failover = enabled;
        self
    }

    /// Pin a minimum `registry_version` every dispatch must meet
    /// (Phase 4.7 rolling-update routing). Workers that don't meet
    /// the floor reject with `Code::FailedPrecondition`, which
    /// propagates here so the operator can catch a half-deployed
    /// cluster. Per-call `ExecuteRequest::min_registry_version`
    /// still overrides this floor when set.
    pub fn with_min_registry_version(mut self, v: impl Into<String>) -> Self {
        self.min_registry_version = Some(v.into());
        self
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Snapshot each worker's `(label, in_flight_estimate())` — the
    /// shape behind the `native_funrun_in_flight_per_worker` metric
    /// named in `convex-native/native-rust-functions.md` §12.3.
    /// Callers typically render these onto a gauge so operators can
    /// see the P2C load distribution across the pool in one view.
    pub fn in_flight_per_worker(&self) -> Vec<(String, u64)> {
        self.workers
            .iter()
            .map(|w| (w.label().to_string(), w.in_flight_estimate()))
            .collect()
    }

    /// Power-of-2-choices: pick two indices, send to the one with
    /// fewer in-flight requests. On `Unavailable` retry against
    /// the other, then bail.
    pub async fn execute(
        &self,
        mut req: ExecuteRequest,
        udf_type: UdfType,
    ) -> Result<ExecuteResponse, Status> {
        let started = Instant::now();

        // Apply the conductor-level rolling-update floor when the
        // request doesn't already pin a minimum.
        if req.min_registry_version.is_none()
            && let Some(floor) = &self.min_registry_version
        {
            req.min_registry_version = Some(floor.clone());
        }

        let n = self.workers.len();
        let (i, j) = self.chooser.pick_two(n);
        let (primary, backup) = self.rank(i, j);

        let attempts: &[usize] = if self.failover && primary != backup {
            &[primary, backup]
        } else {
            // Either failover is disabled or the chooser degenerated
            // to the same worker twice — no point in a second try.
            std::slice::from_ref(&primary)
        };

        let mut last_err: Option<Status> = None;
        let mut unavailable_retried = false;
        for &idx in attempts {
            let worker = &self.workers[idx];
            match worker.execute(req.clone(), udf_type).await {
                Ok(resp) => {
                    let outcome = if unavailable_retried {
                        ConductorOutcome::Retried
                    } else {
                        ConductorOutcome::Ok
                    };
                    self.metrics
                        .record(Some(worker.label()), udf_type, outcome, started.elapsed());
                    if !resp.log_lines.is_empty() {
                        self.log_sink
                            .forward(worker.label(), udf_type, resp.log_lines.clone());
                    }
                    return Ok(resp);
                },
                Err(e) if e.code() == tonic::Code::Unavailable => {
                    unavailable_retried = true;
                    last_err = Some(e);
                    continue;
                },
                Err(e) => {
                    self.metrics.record(
                        Some(worker.label()),
                        udf_type,
                        ConductorOutcome::Error(e.code()),
                        started.elapsed(),
                    );
                    return Err(e);
                },
            }
        }
        let err = last_err.unwrap_or_else(|| {
            Status::unavailable("DistributedFunctionRunner: all workers failed")
        });
        // Every attempt failed transport-side — no single "final
        // worker" to attribute to, so omit the worker label.
        self.metrics.record(
            None,
            udf_type,
            ConductorOutcome::Error(err.code()),
            started.elapsed(),
        );
        Err(err)
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

    fn label(&self) -> &str {
        self.name
    }
}

fn default_ok_response() -> ExecuteResponse {
    ExecuteResponse::new(Ok(value::ConvexValue::Null))
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
            min_registry_version: None,
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
    async fn failover_disabled_bubbles_primary_error_without_retry() {
        let a = MockWorkerClient::with_handler("A", 0, |_, _| Err(Status::unavailable("A down")));
        let b = MockWorkerClient::new("B", 5);
        let runner = DistributedFunctionRunner::new(vec![a.clone(), b.clone()])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))))
            .with_failover(false);
        let err = runner.execute(req(), UdfType::Action).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(a.calls(), 1);
        assert_eq!(
            b.calls(),
            0,
            "backup must not be tried when failover disabled"
        );
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

    #[tokio::test]
    async fn metrics_sink_records_ok_outcome_against_the_attributed_worker() {
        // A routine `Ok` dispatch records against the worker that
        // actually handled the call (the backup label for workers
        // in this test is "B"), tagged as `Ok` — `Retried` is
        // reserved for the "primary Unavailable, backup salvaged"
        // path.
        let a = MockWorkerClient::new("A", 10);
        let b = MockWorkerClient::new("B", 0);
        let metrics = Arc::new(CountingConductorMetrics::new());
        let runner = DistributedFunctionRunner::new(vec![a, b])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))))
            .with_metrics(metrics.clone());
        runner.execute(req(), UdfType::Action).await.unwrap();
        assert_eq!(
            metrics.count(Some("B"), UdfType::Action, ConductorOutcome::Ok),
            1
        );
        assert_eq!(
            metrics.count(Some("A"), UdfType::Action, ConductorOutcome::Ok),
            0,
            "loaded worker never saw the request",
        );
        assert_eq!(
            metrics.count(Some("B"), UdfType::Action, ConductorOutcome::Retried),
            0,
            "first-attempt success is not tagged Retried",
        );
    }

    #[tokio::test]
    async fn metrics_sink_records_retried_when_backup_salvages() {
        // Primary returns Unavailable, backup succeeds → the final
        // record should be against the backup's label and tagged
        // `Retried` so dashboards can alert before the error rate
        // climbs.
        let primary = MockWorkerClient::with_handler("A", 0, |_, _| {
            Err(Status::unavailable("simulated primary outage"))
        });
        let backup = MockWorkerClient::with_handler("B", 1, |_, _| Ok(default_ok_response()));
        let metrics = Arc::new(CountingConductorMetrics::new());
        let runner = DistributedFunctionRunner::new(vec![primary, backup])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))))
            .with_metrics(metrics.clone());
        runner.execute(req(), UdfType::Action).await.unwrap();
        assert_eq!(
            metrics.count(Some("B"), UdfType::Action, ConductorOutcome::Retried),
            1,
            "backup-salvaged call surfaces as Retried",
        );
    }

    #[tokio::test]
    async fn metrics_sink_records_error_when_every_attempt_fails_transport() {
        // All workers `Unavailable` → the final record has no worker
        // attribution (no single "final attempt worker" landed the
        // response) and is tagged `Error(Unavailable)`.
        let a = MockWorkerClient::with_handler("A", 0, |_, _| Err(Status::unavailable("down")));
        let b = MockWorkerClient::with_handler("B", 0, |_, _| Err(Status::unavailable("down")));
        let metrics = Arc::new(CountingConductorMetrics::new());
        let runner = DistributedFunctionRunner::new(vec![a, b])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))))
            .with_metrics(metrics.clone());
        assert!(runner.execute(req(), UdfType::Action).await.is_err());
        assert_eq!(
            metrics.count(
                None,
                UdfType::Action,
                ConductorOutcome::Error(tonic::Code::Unavailable),
            ),
            1,
            "final record when every attempt failed has no worker label",
        );
    }

    #[tokio::test]
    async fn log_sink_receives_worker_log_lines_on_successful_dispatch() {
        // The worker would normally render its LogBuffer into
        // `"[LEVEL] message"` strings. Here the mock short-circuits
        // that by returning an `ExecuteResponse` with log_lines set,
        // which is the same wire shape. The sink should capture
        // them verbatim, keyed by the worker label.
        let handler_a = |_: &ExecuteRequest, _: UdfType| {
            Ok(ExecuteResponse::new(Ok(value::ConvexValue::Null))
                .with_log_lines(vec!["[INFO] hello from a".to_string()]))
        };
        let a = MockWorkerClient::with_handler("A", 0, handler_a);
        let b = MockWorkerClient::new("B", 10);
        let sink = Arc::new(CapturingConductorLogs::new());
        let runner = DistributedFunctionRunner::new(vec![a, b])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))))
            .with_log_sink(sink.clone());
        runner.execute(req(), UdfType::Action).await.unwrap();
        let snap = sink.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].0, "A");
        assert_eq!(snap[0].2, vec!["[INFO] hello from a"]);
    }

    #[tokio::test]
    async fn log_sink_is_silent_when_worker_returns_no_log_lines() {
        // Most handlers don't log — the sink must NOT be called for
        // every request, so deployments that only forward actual
        // log output don't eat a function-call overhead on every
        // dispatch.
        let a = MockWorkerClient::new("A", 0);
        let sink = Arc::new(CapturingConductorLogs::new());
        let runner = DistributedFunctionRunner::new(vec![a])
            .unwrap()
            .with_log_sink(sink.clone());
        runner.execute(req(), UdfType::Action).await.unwrap();
        assert!(sink.snapshot().is_empty());
    }

    #[tokio::test]
    async fn in_flight_per_worker_snapshots_label_and_count_for_every_worker() {
        // Dashboard-shaped gauge: one entry per worker, ordered as
        // they were handed to `DistributedFunctionRunner::new`.
        // `MockWorkerClient::label()` returns the `name` passed at
        // construction; the in-flight count is whatever `initial_in_flight`
        // was set to (atomic-read, no side effects).
        let a = MockWorkerClient::new("A", 3);
        let b = MockWorkerClient::new("B", 7);
        let runner = DistributedFunctionRunner::new(vec![a, b]).unwrap();
        let snap = runner.in_flight_per_worker();
        assert_eq!(snap, vec![("A".to_string(), 3), ("B".to_string(), 7)]);
    }

    #[tokio::test]
    async fn metrics_sink_records_error_with_specific_code_on_non_retryable_failure() {
        // A non-`Unavailable` error bubbles up on the first attempt.
        // The record should carry the worker that actually failed
        // (not `None`) and the exact gRPC code so dashboards can
        // separate `InvalidArgument` from `Internal`.
        let a = MockWorkerClient::with_handler("A", 0, |_, _| {
            Err(Status::invalid_argument("bad args"))
        });
        let b = MockWorkerClient::new("B", 10);
        let metrics = Arc::new(CountingConductorMetrics::new());
        let runner = DistributedFunctionRunner::new(vec![a, b])
            .unwrap()
            .with_chooser(Arc::new(FixedChooser((0, 1))))
            .with_metrics(metrics.clone());
        assert!(runner.execute(req(), UdfType::Action).await.is_err());
        assert_eq!(
            metrics.count(
                Some("A"),
                UdfType::Action,
                ConductorOutcome::Error(tonic::Code::InvalidArgument),
            ),
            1,
        );
    }
}
