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
}
