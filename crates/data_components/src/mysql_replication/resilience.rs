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

//! Reconnect/backoff policy for the binlog stream: `MySQL`-specific
//! transient-vs-fatal classification over the workspace's shared
//! [`RetryBackoff`] strategy.

use std::time::Duration;

use util::retry_strategy::{Backoff as _, BackoffMethod, RetryBackoff, RetryBackoffBuilder};

/// Defaults picked to tolerate short network blips without being
/// user-visibly disruptive; the stream reconnects indefinitely.
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Jittered exponential backoff for the reconnect loop, with the next delay
/// peekable before sleeping so reconnect log lines can report it.
pub struct StreamBackoff {
    inner: RetryBackoff,
    pending: Option<Duration>,
}

impl StreamBackoff {
    #[must_use]
    pub fn default_for_stream() -> Self {
        Self {
            inner: RetryBackoffBuilder::new()
                .method(BackoffMethod::Exponential)
                .base_interval(DEFAULT_INITIAL_BACKOFF)
                .max_duration(Some(DEFAULT_MAX_BACKOFF))
                .randomization_factor(0.2)
                .build(),
            pending: None,
        }
    }

    /// The delay the next [`Self::wait`] will sleep for. Idempotent until
    /// that wait happens.
    pub fn next_delay(&mut self) -> Duration {
        if self.pending.is_none() {
            // `None` only occurs when a max-retries budget is exhausted,
            // which this strategy doesn't set.
            self.pending = Some(self.inner.next_duration().unwrap_or(DEFAULT_MAX_BACKOFF));
        }
        self.pending.unwrap_or(DEFAULT_MAX_BACKOFF)
    }

    pub async fn wait(&mut self) {
        let delay = self.next_delay();
        tokio::time::sleep(delay).await;
        self.pending = None;
    }

    pub fn reset(&mut self) {
        self.inner.reset();
        self.pending = None;
    }
}

/// `MySQL` server error: the replica asked for a binlog position the server no
/// longer has (`ER_SOURCE_FATAL_ERROR_READING_BINLOG` — raised when binary
/// logs were purged past the requested position). Fatal, with a dedicated
/// recovery path (`invalid_position_behavior`).
pub const ER_SOURCE_FATAL_ERROR_READING_BINLOG: u16 = 1236;

/// Classify a `mysql_async::Error` as transient (worth reconnecting) or
/// fatal (propagate to the user).
///
/// IO-path errors are transient. Server errors are fatal (auth, privilege,
/// purged binlog) except a small set of connection-lifecycle codes emitted
/// around server restarts and connection churn.
#[must_use]
pub fn is_transient_mysql(err: &mysql_async::Error) -> bool {
    match err {
        mysql_async::Error::Io(_) => true,
        mysql_async::Error::Server(server) => {
            matches!(
                server.code,
                // ER_CON_COUNT_ERROR: too many connections — clears as churn drains.
                1040
                // ER_SERVER_SHUTDOWN / ER_NORMAL_SHUTDOWN: restart in progress.
                | 1053 | 1077
                // ER_ABORTING_CONNECTION / ER_NEW_ABORTING_CONNECTION /
                // ER_CONNECTION_KILLED: this connection was torn down server-side.
                | 1152 | 1184 | 1927
            )
        }
        other => is_transient_by_display(&other.to_string()),
    }
}

/// String-heuristic fallback for error types without structured codes
/// (driver-level teardown states).
fn is_transient_by_display(msg: &str) -> bool {
    const TRANSIENT_MARKERS: &[&str] = &[
        "connection closed",
        "connection reset",
        "connection refused",
        "broken pipe",
        "unexpected eof",
        "unexpected end of file",
        "temporarily unavailable",
        "timed out",
        "timeout",
        "network is unreachable",
        "host unreachable",
        "pool was disconnected",
    ];
    let lower = msg.to_ascii_lowercase();
    TRANSIENT_MARKERS.iter().any(|m| lower.contains(m))
}

/// `true` when the server rejected our requested binlog position (purged
/// binary logs). Gets the dedicated `invalid_position_behavior` handling
/// rather than the generic fatal path.
#[must_use]
pub fn is_purged_position_error(err: &mysql_async::Error) -> bool {
    matches!(
        err,
        mysql_async::Error::Server(s) if s.code == ER_SOURCE_FATAL_ERROR_READING_BINLOG
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_error(code: u16, message: &str) -> mysql_async::Error {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: message.to_string(),
            state: "HY000".to_string(),
        })
    }

    #[test]
    fn backoff_delay_is_stable_until_waited_and_resets() {
        let mut b = StreamBackoff::default_for_stream();
        let first = b.next_delay();
        assert_eq!(first, b.next_delay(), "peek must be idempotent");
        assert!(
            first <= DEFAULT_MAX_BACKOFF,
            "delay {first:?} must respect the cap"
        );
        b.reset();
        let after_reset = b.next_delay();
        assert!(
            after_reset <= DEFAULT_INITIAL_BACKOFF * 2,
            "reset must return to the base interval range, got {after_reset:?}"
        );
    }

    #[test]
    fn shutdown_and_kill_codes_are_transient() {
        for code in [1040u16, 1053, 1077, 1152, 1184, 1927] {
            assert!(
                is_transient_mysql(&server_error(code, "connection lifecycle")),
                "code {code} must be transient"
            );
        }
    }

    #[test]
    fn auth_and_privilege_errors_are_fatal() {
        // ER_ACCESS_DENIED_ERROR / ER_SPECIFIC_ACCESS_DENIED_ERROR (missing
        // REPLICATION SLAVE) must surface to the operator, not retry forever.
        for code in [1045u16, 1227] {
            assert!(
                !is_transient_mysql(&server_error(code, "denied")),
                "code {code} must be fatal"
            );
        }
    }

    #[test]
    fn purged_binlog_position_is_fatal_and_specifically_detected() {
        let err = server_error(
            ER_SOURCE_FATAL_ERROR_READING_BINLOG,
            "Could not find first log file name in binary log index file",
        );
        assert!(!is_transient_mysql(&err));
        assert!(is_purged_position_error(&err));
        assert!(!is_purged_position_error(&server_error(1045, "denied")));
    }

    #[test]
    fn io_errors_are_transient() {
        let io = mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        )));
        assert!(is_transient_mysql(&io));
    }
}
