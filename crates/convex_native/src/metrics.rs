//! Pluggable per-function metrics sink.
//!
//! Per `IMPLEMENTATION_PLAN.md` Phase 4 step 4.2.
//!
//! The `NativeFunctionRunner` exposes hooks to report function-level
//! latency and outcome (ok vs err) without pulling a specific metrics
//! backend into the crate. Implementors can wire this to `prometheus`,
//! `fastrace`, `tracing`, or whatever the surrounding binary uses.

use std::time::Duration;

use common::types::UdfType;

/// Outcome of a single native function invocation, passed to
/// [`NativeMetricsSink::record`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Ok,
    Err,
}

/// Plug-in metrics sink. The default [`NoopMetrics`] discards
/// everything; the backend adapter can install an `Arc<dyn
/// NativeMetricsSink>` that emits to Prometheus or equivalent.
pub trait NativeMetricsSink: Send + Sync + 'static {
    fn record(&self, name: &str, udf_type: UdfType, outcome: Outcome, latency: Duration);
}

/// Default discards everything.
pub struct NoopMetrics;

impl NativeMetricsSink for NoopMetrics {
    fn record(&self, _: &str, _: UdfType, _: Outcome, _: Duration) {}
}

/// Counter-style in-memory sink useful for tests / dev dashboards.
#[derive(Default)]
pub struct CountingMetrics {
    inner: parking_lot::Mutex<CountingInner>,
}

#[derive(Default)]
struct CountingInner {
    calls: std::collections::BTreeMap<(String, UdfType, Outcome), u64>,
    total_latency: std::collections::BTreeMap<(String, UdfType), Duration>,
}

impl CountingMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of calls to `name` with given kind + outcome.
    pub fn count(&self, name: &str, udf_type: UdfType, outcome: Outcome) -> u64 {
        self.inner
            .lock()
            .calls
            .get(&(name.to_string(), udf_type, outcome))
            .copied()
            .unwrap_or(0)
    }

    /// Total accumulated latency for `name`.
    pub fn total_latency(&self, name: &str, udf_type: UdfType) -> Duration {
        self.inner
            .lock()
            .total_latency
            .get(&(name.to_string(), udf_type))
            .copied()
            .unwrap_or_default()
    }
}

impl NativeMetricsSink for CountingMetrics {
    fn record(&self, name: &str, udf_type: UdfType, outcome: Outcome, latency: Duration) {
        let mut inner = self.inner.lock();
        *inner
            .calls
            .entry((name.to_string(), udf_type, outcome))
            .or_insert(0) += 1;
        let total = inner
            .total_latency
            .entry((name.to_string(), udf_type))
            .or_default();
        *total += latency;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counting_metrics_tracks_outcome_and_latency() {
        let m = CountingMetrics::new();
        m.record(
            "foo",
            UdfType::Query,
            Outcome::Ok,
            Duration::from_millis(10),
        );
        m.record("foo", UdfType::Query, Outcome::Ok, Duration::from_millis(5));
        m.record(
            "foo",
            UdfType::Query,
            Outcome::Err,
            Duration::from_millis(1),
        );
        assert_eq!(m.count("foo", UdfType::Query, Outcome::Ok), 2);
        assert_eq!(m.count("foo", UdfType::Query, Outcome::Err), 1);
        assert_eq!(m.count("foo", UdfType::Mutation, Outcome::Ok), 0);
        assert_eq!(
            m.total_latency("foo", UdfType::Query),
            Duration::from_millis(16),
        );
    }

    #[test]
    fn counting_metrics_segregates_by_function_name() {
        // Two calls with the same kind + outcome but different names
        // must be tallied independently — otherwise a noisy function
        // would corrupt another function's stats.
        let m = CountingMetrics::new();
        m.record("foo", UdfType::Query, Outcome::Ok, Duration::ZERO);
        m.record("bar", UdfType::Query, Outcome::Ok, Duration::ZERO);
        assert_eq!(m.count("foo", UdfType::Query, Outcome::Ok), 1);
        assert_eq!(m.count("bar", UdfType::Query, Outcome::Ok), 1);
        assert_eq!(m.count("missing", UdfType::Query, Outcome::Ok), 0);
    }

    #[test]
    fn counting_metrics_segregates_latency_by_udf_type() {
        // Same function name, different kinds. Latency buckets must
        // stay separate so the "query vs mutation" distinction stays
        // observable in dashboards.
        let m = CountingMetrics::new();
        m.record(
            "foo",
            UdfType::Query,
            Outcome::Ok,
            Duration::from_millis(10),
        );
        m.record(
            "foo",
            UdfType::Mutation,
            Outcome::Ok,
            Duration::from_millis(20),
        );
        assert_eq!(
            m.total_latency("foo", UdfType::Query),
            Duration::from_millis(10),
        );
        assert_eq!(
            m.total_latency("foo", UdfType::Mutation),
            Duration::from_millis(20),
        );
    }

    #[test]
    fn count_and_total_latency_return_zero_for_unseen_keys() {
        // Readers rely on "never recorded → zero" rather than
        // panicking or returning a sentinel. Pin that so a refactor
        // toward `Option<u64>` return types can't land silently.
        let m = CountingMetrics::new();
        assert_eq!(m.count("never-called", UdfType::Action, Outcome::Ok), 0);
        assert_eq!(
            m.total_latency("never-called", UdfType::Action),
            Duration::ZERO,
        );
    }

    #[test]
    fn noop_metrics_never_panics() {
        // The default sink discards everything; exercising a few
        // calls at least proves it doesn't panic under normal use.
        let n = NoopMetrics;
        n.record("a", UdfType::Query, Outcome::Ok, Duration::from_secs(1));
        n.record("b", UdfType::Action, Outcome::Err, Duration::ZERO);
    }

    #[test]
    fn metrics_sink_is_object_safe() {
        // The trait is used as `Arc<dyn NativeMetricsSink>` by the
        // runner. If someone accidentally adds a generic method (or
        // `Self: Sized` bound) the trait stops being object-safe and
        // the runner wiring fails to compile. Constructing a
        // boxed-dyn here is the cheap compile-time guard for that.
        let _sink: std::sync::Arc<dyn NativeMetricsSink> =
            std::sync::Arc::new(CountingMetrics::new());
        let _sink: std::sync::Arc<dyn NativeMetricsSink> = std::sync::Arc::new(NoopMetrics);
    }

    #[test]
    fn outcome_ord_places_ok_before_err() {
        // The derived `Ord` matters because `CountingMetrics` keys
        // by `Outcome` in a `BTreeMap`. A reorder (e.g. putting Err
        // before Ok in the enum) changes iteration order and would
        // silently break consumers that rely on it for dashboard
        // stability.
        assert!(Outcome::Ok < Outcome::Err);
    }
}
