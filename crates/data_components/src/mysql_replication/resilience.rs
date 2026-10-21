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
/// purged binlog) except a set of connection-lifecycle, HA failover, and
/// socket communication codes emitted around server restarts and failovers.
///
/// Reference: `MySQL` Server Error Message Reference
/// <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>
/// Reference: `MySQL` Client Error Message Reference
/// <https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html>
#[must_use]
pub fn is_transient_mysql(err: &mysql_async::Error) -> bool {
    match err {
        mysql_async::Error::Io(_)
        | mysql_async::Error::Driver(
            mysql_async::DriverError::ConnectionClosed | mysql_async::DriverError::PoolDisconnected,
        ) => true,
        mysql_async::Error::Server(server) => {
            if server.code == 1290 {
                // ER_OPTION_PREVENTS_STATEMENT (1290): only retry if the option is read_only / super_read_only during HA failovers.
                // Other persistent server options (e.g. --secure-file-priv) are permanent failures and should remain fatal.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_option_prevents_statement
                let msg = server.message.to_ascii_lowercase();
                return msg.contains("read_only") || msg.contains("read-only");
            }
            matches!(
                server.code,
                // ER_CON_COUNT_ERROR (1040): Too many connections — clears as churn drains.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_con_count_error
                1040
                // Server shutdown / restart lifecycle in progress:
                // ER_SERVER_SHUTDOWN (1053): Server shutdown in progress.
                // ER_NORMAL_SHUTDOWN (1077): Normal shutdown.
                // ER_GOT_SIGNAL (1078): Got signal; aborting.
                // ER_SHUTDOWN_COMPLETE (1079): Shutdown complete.
                // ER_FORCING_CLOSE (1080): Forcing close of thread/connection.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_server_shutdown
                | 1053 | 1077 | 1078 | 1079 | 1080
                // Connection termination server-side:
                // ER_ABORTING_CONNECTION (1152) / ER_NEW_ABORTING_CONNECTION (1184): Connection aborted.
                // ER_CONNECTION_KILLED (1927, MariaDB): Connection was killed.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_aborting_connection
                | 1152 | 1184 | 1927
                // Socket / network communication packet timeouts and interrupts:
                // ER_NET_READ_ERROR (1158): Error reading communication packets.
                // ER_NET_READ_INTERRUPTED (1159): Timeout reading communication packets.
                // ER_NET_ERROR_ON_WRITE (1160): Error writing communication packets.
                // ER_NET_WRITE_INTERRUPTED (1161): Timeout writing communication packets.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_net_read_error
                | 1158 | 1159 | 1160 | 1161
                // Transient transaction lock timeouts and deadlocks:
                // ER_LOCK_WAIT_TIMEOUT (1205): Lock wait timeout exceeded; retryable.
                // ER_LOCK_DEADLOCK (1213): Deadlock found when trying to get lock; retryable.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_lock_wait_timeout
                | 1205 | 1213
                // ER_QUERY_INTERRUPTED (1317): Query execution was interrupted.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_query_interrupted
                | 1317
                // Server/proxy connection loss error codes (emitted in server error packets by MySQL proxies / middleware):
                // CR_SERVER_GONE_ERROR (2006): MySQL server has gone away.
                // CR_SERVER_LOST (2013): Lost connection to MySQL server during query.
                // https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html
                | 2006 | 2013
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
    fn server_shutdown_and_restart_codes_are_transient() {
        // Test all server shutdown / restart lifecycle error codes:
        // ER_SERVER_SHUTDOWN (1053), ER_NORMAL_SHUTDOWN (1077), ER_GOT_SIGNAL (1078),
        // ER_SHUTDOWN_COMPLETE (1079), ER_FORCING_CLOSE (1080).
        for (code, name) in [
            (1053u16, "ER_SERVER_SHUTDOWN"),
            (1077, "ER_NORMAL_SHUTDOWN"),
            (1078, "ER_GOT_SIGNAL"),
            (1079, "ER_SHUTDOWN_COMPLETE"),
            (1080, "ER_FORCING_CLOSE"),
        ] {
            assert!(
                is_transient_mysql(&server_error(code, name)),
                "error code {code} ({name}) must be classified as transient"
            );
        }
    }

    #[test]
    fn connection_termination_and_kill_codes_are_transient() {
        // Test connection count exhaustion and server-side connection aborts/kills:
        // ER_CON_COUNT_ERROR (1040), ER_ABORTING_CONNECTION (1152),
        // ER_NEW_ABORTING_CONNECTION (1184), ER_CONNECTION_KILLED (1927).
        for (code, name) in [
            (1040u16, "ER_CON_COUNT_ERROR"),
            (1152, "ER_ABORTING_CONNECTION"),
            (1184, "ER_NEW_ABORTING_CONNECTION"),
            (1927, "ER_CONNECTION_KILLED"),
        ] {
            assert!(
                is_transient_mysql(&server_error(code, name)),
                "error code {code} ({name}) must be classified as transient"
            );
        }
    }

    #[test]
    fn network_and_socket_packet_codes_are_transient() {
        // Test packet read/write errors and timeouts:
        // ER_NET_READ_ERROR (1158), ER_NET_READ_INTERRUPTED (1159),
        // ER_NET_ERROR_ON_WRITE (1160), ER_NET_WRITE_INTERRUPTED (1161).
        for (code, name) in [
            (1158u16, "ER_NET_READ_ERROR"),
            (1159, "ER_NET_READ_INTERRUPTED"),
            (1160, "ER_NET_ERROR_ON_WRITE"),
            (1161, "ER_NET_WRITE_INTERRUPTED"),
        ] {
            assert!(
                is_transient_mysql(&server_error(code, name)),
                "error code {code} ({name}) must be classified as transient"
            );
        }
    }

    #[test]
    fn lock_deadlock_and_failover_codes_are_transient() {
        // Test lock wait timeout, deadlock, and query interruption:
        // ER_LOCK_WAIT_TIMEOUT (1205), ER_LOCK_DEADLOCK (1213), ER_QUERY_INTERRUPTED (1317).
        for (code, name) in [
            (1205u16, "ER_LOCK_WAIT_TIMEOUT"),
            (1213, "ER_LOCK_DEADLOCK"),
            (1317, "ER_QUERY_INTERRUPTED"),
        ] {
            assert!(
                is_transient_mysql(&server_error(code, name)),
                "error code {code} ({name}) must be classified as transient"
            );
        }

        // ER_OPTION_PREVENTS_STATEMENT (1290) is only transient when message indicates read_only / super_read_only:
        assert!(is_transient_mysql(&server_error(
            1290,
            "The MySQL server is running with the --read-only option so it cannot execute this statement"
        )));
        assert!(is_transient_mysql(&server_error(
            1290,
            "The MySQL server is running with the --super-read-only option so it cannot execute this statement"
        )));
        // Persistent option prevents statement is fatal:
        assert!(!is_transient_mysql(&server_error(
            1290,
            "The MySQL server is running with the --secure-file-priv option so it cannot execute this statement"
        )));
    }

    #[test]
    fn proxy_connection_loss_codes_are_transient() {
        // Test proxy/middleware connection loss codes (2006, 2013) that can arrive in server error packets:
        for (code, name) in [(2006u16, "CR_SERVER_GONE_ERROR"), (2013, "CR_SERVER_LOST")] {
            assert!(
                is_transient_mysql(&server_error(code, name)),
                "error code {code} ({name}) must be classified as transient"
            );
        }
    }

    #[test]
    fn auth_privilege_and_schema_errors_are_fatal() {
        // Permanent / fatal server error codes that should NOT retry indefinitely:
        // ER_ACCESS_DENIED_ERROR (1045), ER_BAD_DB_ERROR (1049),
        // ER_NO_SUCH_TABLE (1146), ER_SPECIFIC_ACCESS_DENIED_ERROR (1227).
        for (code, name) in [
            (1045u16, "ER_ACCESS_DENIED_ERROR"),
            (1049, "ER_BAD_DB_ERROR"),
            (1146, "ER_NO_SUCH_TABLE"),
            (1227, "ER_SPECIFIC_ACCESS_DENIED_ERROR"),
        ] {
            assert!(
                !is_transient_mysql(&server_error(code, name)),
                "error code {code} ({name}) must be fatal"
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

    #[test]
    fn driver_errors_are_transient() {
        assert!(is_transient_mysql(&mysql_async::Error::Driver(
            mysql_async::DriverError::ConnectionClosed
        )));
        assert!(is_transient_mysql(&mysql_async::Error::Driver(
            mysql_async::DriverError::PoolDisconnected
        )));
    }
}
