/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! In-memory ring buffer of recent log lines, fed by a `tracing` layer.
//!
//! This exists so Spice Cloud Connect can answer a `GetLogs` control
//! message: a standalone `spiced` has no pod / kube API to read logs from,
//! so it serves the most recent lines of its own log output instead. The
//! capture layer is installed alongside — never in place of — the terminal
//! `fmt` layer, so console logging is unaffected; it is only installed when
//! Cloud Connect is configured for the instance (see `init_tracing`).
//!
//! The buffer is bounded: it keeps at most `capacity` of the most recent
//! formatted lines, discarding the oldest. Memory is therefore constant and
//! there is no disk I/O.

use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use tracing_subscriber::fmt::MakeWriter;

/// Default number of log lines retained in the ring buffer. At a typical
/// ~200 bytes/line this caps the buffer around a few MiB.
pub(crate) const DEFAULT_CAPACITY: usize = 10_000;

/// Process-global handle to the installed ring buffer. Set once by
/// [`install`] from `init_tracing` (the tracing subscriber is frozen at
/// startup via `set_global_default`, so the buffer must be wired in then).
/// `None` until installed — i.e. when Cloud Connect is not configured.
static CAPTURE: OnceLock<LogRingBuffer> = OnceLock::new();

/// A cloneable handle to the bounded log ring buffer. Clones share the same
/// underlying storage (`Arc`). Doubles as the `tracing` `MakeWriter` that
/// feeds it.
#[derive(Clone)]
pub(crate) struct LogRingBuffer {
    inner: Arc<Mutex<VecDeque<String>>>,
    capacity: usize,
}

impl LogRingBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            // A poisoned lock only means a formatter panicked mid-write; the
            // guard below recovers rather than propagating, so capacity 1 is
            // never actually hit in practice.
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity.max(1)))),
            capacity: capacity.max(1),
        }
    }

    /// Append one already-formatted log record (may include a trailing
    /// newline). Evicts the oldest lines once `capacity` is exceeded.
    fn push(&self, line: String) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            // Recover from a poisoned lock: a captured log line is best-effort
            // and must never take down the runtime.
            Err(poisoned) => poisoned.into_inner(),
        };
        while guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(line);
    }

    /// Return the most recent `n` lines concatenated in chronological order.
    /// `n == 0` (or larger than the buffer) returns everything retained.
    /// Each stored record already carries its own trailing newline, so the
    /// records are joined verbatim.
    pub(crate) fn tail(&self, n: usize) -> String {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let take_from = if n == 0 {
            0
        } else {
            guard.len().saturating_sub(n)
        };
        guard.iter().skip(take_from).cloned().collect()
    }
}

/// Per-event writer handed to the `fmt` layer. Accumulates the formatted
/// bytes for a single event and commits them to the ring buffer on drop
/// (the `fmt` layer creates a fresh writer per event and drops it once the
/// record is fully written).
pub(crate) struct RingWriter {
    ring: LogRingBuffer,
    buf: Vec<u8>,
}

impl io::Write for RingWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for RingWriter {
    fn drop(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        // Formatted records are UTF-8; lossy conversion is a defensive
        // fallback that can't fail rather than an expected path.
        let line = String::from_utf8_lossy(&self.buf).into_owned();
        self.ring.push(line);
    }
}

impl<'a> MakeWriter<'a> for LogRingBuffer {
    type Writer = RingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        RingWriter {
            ring: self.clone(),
            buf: Vec::new(),
        }
    }
}

/// Install the process-global ring buffer (idempotent) and return a handle.
/// Called from `init_tracing` only when Cloud Connect is configured, so the
/// capture layer's `MakeWriter` and the later `GetLogs` reader share one
/// buffer.
pub(crate) fn install(capacity: usize) -> LogRingBuffer {
    CAPTURE.get_or_init(|| LogRingBuffer::new(capacity)).clone()
}

/// Return the installed ring buffer, if `install` has run. The Cloud Connect
/// runtime handle uses this to serve `GetLogs`.
pub(crate) fn handle() -> Option<LogRingBuffer> {
    CAPTURE.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn tail_returns_most_recent_lines() {
        let ring = LogRingBuffer::new(100);
        for i in 0..5 {
            ring.push(format!("line {i}\n"));
        }
        assert_eq!(ring.tail(2), "line 3\nline 4\n");
        // n == 0 returns everything.
        assert_eq!(ring.tail(0), "line 0\nline 1\nline 2\nline 3\nline 4\n");
        // Asking for more than retained returns everything, not a panic.
        assert_eq!(ring.tail(999), "line 0\nline 1\nline 2\nline 3\nline 4\n");
    }

    #[test]
    fn buffer_is_bounded_and_evicts_oldest() {
        let ring = LogRingBuffer::new(3);
        for i in 0..10 {
            ring.push(format!("line {i}\n"));
        }
        // Only the last 3 survive.
        assert_eq!(ring.tail(0), "line 7\nline 8\nline 9\n");
    }

    #[test]
    fn ring_writer_commits_on_drop() {
        let ring = LogRingBuffer::new(10);
        {
            let mut w = ring.make_writer();
            w.write_all(b"2026-01-01 INFO target: hello\n")
                .expect("write to ring writer");
        } // drop commits.
        assert_eq!(ring.tail(1), "2026-01-01 INFO target: hello\n");
    }
}
