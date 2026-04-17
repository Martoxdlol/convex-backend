//! Log-line collection exposed to native function bodies.
//!
//! The canonical LogLine type lives in `common::log_lines::LogLine` and
//! is heavy (adjustable log levels, structured payloads, audit
//! variants). The native surface wraps a thin in-memory buffer that a
//! developer can push messages into; the runner drains this buffer
//! after the function returns and surfaces it through the backend's
//! usual log-streaming path.
//!
//! The shape deliberately matches `console.log / .info / .warn / .error`
//! from the JS side so porting code between runtimes is a rename, not
//! a rethink.

use std::sync::Arc;

use parking_lot::Mutex;

/// Severity of a single log line.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// One captured log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeLogLine {
    pub level: LogLevel,
    pub message: String,
}

/// Shared append-only buffer. Cheap to `Clone` — both the context
/// wrappers and the runner hold one `Arc<LogBuffer>` and the runner
/// snapshots the contents after the handler returns.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<Vec<NativeLogLine>>>,
    min_level: LogLevel,
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            min_level: LogLevel::Debug,
        }
    }
}

impl LogBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a minimum severity. Lines below `level` are
    /// silently dropped.
    pub fn with_min_level(level: LogLevel) -> Self {
        Self {
            inner: Arc::default(),
            min_level: level,
        }
    }

    /// Current minimum severity filter.
    pub fn min_level(&self) -> LogLevel {
        self.min_level
    }

    pub fn push(&self, line: NativeLogLine) {
        if line.level < self.min_level {
            return;
        }
        self.inner.lock().push(line);
    }

    pub fn snapshot(&self) -> Vec<NativeLogLine> {
        self.inner.lock().clone()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

/// Borrowed handle obtained via `ctx.log()`. Writes into the shared
/// [`LogBuffer`].
pub struct Logger<'a> {
    pub(crate) buffer: &'a LogBuffer,
}

impl<'a> Logger<'a> {
    /// Construct a logger against a caller-owned buffer. Normally
    /// developers don't build this directly — they go through
    /// `ctx.log()`.
    pub fn new(buffer: &'a LogBuffer) -> Self {
        Self { buffer }
    }

    pub fn debug(&self, msg: impl Into<String>) {
        self.buffer.push(NativeLogLine {
            level: LogLevel::Debug,
            message: msg.into(),
        });
    }

    pub fn info(&self, msg: impl Into<String>) {
        self.buffer.push(NativeLogLine {
            level: LogLevel::Info,
            message: msg.into(),
        });
    }

    pub fn warn(&self, msg: impl Into<String>) {
        self.buffer.push(NativeLogLine {
            level: LogLevel::Warn,
            message: msg.into(),
        });
    }

    pub fn error(&self, msg: impl Into<String>) {
        self.buffer.push(NativeLogLine {
            level: LogLevel::Error,
            message: msg.into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logger_pushes_each_level_into_buffer() {
        let buffer = LogBuffer::new();
        let logger = Logger { buffer: &buffer };
        logger.debug("d");
        logger.info("i");
        logger.warn("w");
        logger.error("e");
        let snap = buffer.snapshot();
        assert_eq!(snap.len(), 4);
        assert_eq!(snap[0].level, LogLevel::Debug);
        assert_eq!(snap[3].message, "e");
    }

    #[test]
    fn min_level_filters_out_lower_severities() {
        let buffer = LogBuffer::with_min_level(LogLevel::Warn);
        let logger = Logger::new(&buffer);
        logger.debug("d");
        logger.info("i");
        logger.warn("w");
        logger.error("e");
        let snap = buffer.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].level, LogLevel::Warn);
        assert_eq!(snap[1].level, LogLevel::Error);
    }

    #[test]
    fn new_buffer_starts_empty() {
        let buffer = LogBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert!(buffer.snapshot().is_empty());
    }

    #[test]
    fn len_tracks_push_and_clear_resets() {
        let buffer = LogBuffer::new();
        let logger = Logger::new(&buffer);
        logger.info("1");
        logger.info("2");
        logger.info("3");
        assert_eq!(buffer.len(), 3);
        assert!(!buffer.is_empty());
        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn clones_share_underlying_buffer() {
        // `LogBuffer` wraps the storage in `Arc`, so a clone must see
        // writes made through the original handle (and vice-versa).
        // This pins the "cheap clone + shared state" contract the
        // runner relies on: the ctx wrappers and the runner each hold
        // a `LogBuffer` and the runner snapshots at the end.
        let a = LogBuffer::new();
        let b = a.clone();
        Logger::new(&a).info("via a");
        Logger::new(&b).info("via b");
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 2);
        let snap = b.snapshot();
        assert_eq!(snap[0].message, "via a");
        assert_eq!(snap[1].message, "via b");
    }

    #[test]
    fn snapshot_is_a_copy_not_a_live_view() {
        // Snapshots freeze the current contents; subsequent pushes
        // must not retroactively appear in an older snapshot.
        let buffer = LogBuffer::new();
        Logger::new(&buffer).info("before");
        let frozen = buffer.snapshot();
        Logger::new(&buffer).info("after");
        assert_eq!(frozen.len(), 1);
        assert_eq!(frozen[0].message, "before");
        assert_eq!(buffer.len(), 2, "underlying buffer kept growing");
    }

    #[test]
    fn min_level_accessor_reports_configured_floor() {
        // Confirms `with_min_level(...)` actually lands in the field
        // returned by `min_level()` — a quiet getter can silently
        // drift from the constructor if someone refactors the
        // internal field name.
        let default = LogBuffer::new();
        assert_eq!(default.min_level(), LogLevel::Debug);
        let warn_only = LogBuffer::with_min_level(LogLevel::Warn);
        assert_eq!(warn_only.min_level(), LogLevel::Warn);
    }
}
