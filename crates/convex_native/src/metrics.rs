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
            .or_insert_with(Duration::default);
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
}
