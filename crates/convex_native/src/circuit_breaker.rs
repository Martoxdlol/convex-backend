//! Per-function circuit breaker.
//!
//! Per `IMPLEMENTATION_PLAN.md` Phase 4 step 4.5.
//!
//! Simple one-shot breaker: each function gets a window of allowed
//! consecutive failures; past that it opens and rejects new calls for
//! `cooldown` before letting one probe call through (half-open). A
//! success in half-open closes the breaker; a failure reopens it.
//!
//! This is intentionally minimal — Convex-backend's production
//! circuit-breaker flavor (tracking error rate across a rolling
//! window) can replace this later. The wiring point is
//! `NativeFunctionRunner::with_circuit_breaker(...)`.

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{
        Duration,
        Instant,
    },
};

#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures allowed before opening the breaker.
    pub failure_threshold: u32,
    /// How long the breaker stays open before allowing a probe call.
    pub cooldown: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown: Duration::from_secs(30),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum State {
    Closed,
    Open { opened_at: Instant },
    HalfOpen,
}

#[derive(Default)]
struct FnState {
    consecutive_failures: u32,
    state: Option<State>,
}

impl FnState {
    fn closed() -> Self {
        Self {
            consecutive_failures: 0,
            state: Some(State::Closed),
        }
    }
}

/// Plug-in circuit breaker. Use
/// `NativeFunctionRunner::with_circuit_breaker` to install one.
pub struct CircuitBreaker {
    cfg: CircuitBreakerConfig,
    inner: Mutex<BTreeMap<String, FnState>>,
}

impl CircuitBreaker {
    pub fn new(cfg: CircuitBreakerConfig) -> Self {
        Self {
            cfg,
            inner: Mutex::new(BTreeMap::new()),
        }
    }

    /// Check before dispatching a call. Returns `Err` if the breaker
    /// is open and still inside the cooldown window. If the cooldown
    /// has elapsed, transitions to half-open and lets this call
    /// through.
    pub fn before_call(&self, name: &str) -> anyhow::Result<()> {
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(name.to_string()).or_insert_with(FnState::closed);
        match entry.state {
            Some(State::Closed) | None => Ok(()),
            Some(State::HalfOpen) => {
                // Another call is already probing — reject.
                anyhow::bail!("circuit breaker for {name:?} is half-open (probe in flight)")
            },
            Some(State::Open { opened_at }) => {
                if opened_at.elapsed() >= self.cfg.cooldown {
                    // Cooldown elapsed; let this one probe through.
                    entry.state = Some(State::HalfOpen);
                    Ok(())
                } else {
                    anyhow::bail!(
                        "circuit breaker for {name:?} is open (cooldown: {:?})",
                        self.cfg.cooldown - opened_at.elapsed(),
                    )
                }
            },
        }
    }

    /// Report the outcome of a completed call. Advances the breaker
    /// state machine.
    pub fn after_call(&self, name: &str, success: bool) {
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(name.to_string()).or_insert_with(FnState::closed);
        if success {
            entry.consecutive_failures = 0;
            entry.state = Some(State::Closed);
        } else {
            entry.consecutive_failures += 1;
            if entry.consecutive_failures >= self.cfg.failure_threshold
                || matches!(entry.state, Some(State::HalfOpen))
            {
                entry.state = Some(State::Open {
                    opened_at: Instant::now(),
                });
            }
        }
    }

    /// Test hook: what's the current state of this function's breaker?
    #[doc(hidden)]
    pub fn is_open(&self, name: &str) -> bool {
        matches!(
            self.inner.lock().unwrap().get(name).and_then(|s| s.state),
            Some(State::Open { .. }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_threshold_failures() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            cooldown: Duration::from_secs(1),
        });
        for _ in 0..3 {
            cb.before_call("foo").unwrap();
            cb.after_call("foo", false);
        }
        assert!(cb.is_open("foo"));
        assert!(cb.before_call("foo").is_err());
    }

    #[test]
    fn closes_after_success_in_half_open() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_millis(1),
        });
        // Trip the breaker.
        cb.before_call("bar").unwrap();
        cb.after_call("bar", false);
        assert!(cb.is_open("bar"));

        // Cooldown passes.
        std::thread::sleep(Duration::from_millis(5));

        // Probe call allowed.
        cb.before_call("bar").unwrap();
        // A second call before the probe completes is rejected.
        assert!(cb.before_call("bar").is_err());
        // Probe succeeds → closed.
        cb.after_call("bar", true);
        assert!(!cb.is_open("bar"));
        cb.before_call("bar").unwrap();
    }
}
