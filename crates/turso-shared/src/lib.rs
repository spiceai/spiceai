/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::time::Duration;

/// SQL used to begin a concurrent write transaction in Turso/libsql.
///
/// This should be used when issuing write transactions that may contend with
/// other writers and are expected to be retried on conflict. It relies on
/// libsql's MVCC support and is only valid when the database is configured
/// with `PRAGMA journal_mode = 'mvcc'` (see [`JOURNAL_MODE_SQL_LITERAL`]).
pub const BEGIN_CONCURRENT_SQL: &str = "BEGIN CONCURRENT";

/// SQL used to begin a standard transaction.
///
/// Use this for transactions that do not require `BEGIN CONCURRENT` semantics
/// (for example, when MVCC journal mode is not enabled, or when the client
/// handles concurrency differently).
pub const BEGIN_TRANSACTION_SQL: &str = "BEGIN TRANSACTION";

/// SQL used to commit either a concurrent or standard transaction.
pub const COMMIT_SQL: &str = "COMMIT";

/// SQL literal for configuring libsql/Turso to use MVCC journal mode.
///
/// This is typically passed to `PRAGMA journal_mode` before using
/// [`BEGIN_CONCURRENT_SQL`] so that concurrent write transactions can rely on
/// MVCC semantics. The value includes SQL single-quoting.
pub const JOURNAL_MODE_SQL_LITERAL: &str = "'mvcc'";

/// Default maximum number of attempts for retrying a concurrent write
/// transaction after retryable conflicts.
pub const DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS: u32 = 4;

const _: () = assert!(DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS > 0);

/// Base delay in milliseconds used by [`retry_backoff_delay`] for
/// Fibonacci backoff between concurrent write retries.
pub const DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS: u64 = 10;

/// Fibonacci backoff delay for retry `attempt` (1-based), with equal jitter
/// applied via [`apply_equal_jitter`].
///
/// The backoff base is `DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS * fib(attempt)`, where
/// `fib` walks the Fibonacci sequence `1, 1, 2, 3, 5, 8, …` (see
/// [`fib_backoff_multiplier`]); the returned delay is then randomized within
/// `[base / 2, base]` so that many writers contending on the same row — a Turso
/// `BEGIN CONCURRENT` MVCC commit conflict or a `SQLite` `SQLITE_BUSY` — do not all wake
/// on the same backoff boundary and re-collide (a retry thundering herd).
///
/// Fibonacci grows by the golden ratio (≈1.618×) per attempt rather than doubling, so
/// retries ramp through more, smaller steps before reaching a long wait. Under moderate
/// write contention that converges sooner than exponential backoff while still bounding
/// the worst-case delay.
#[must_use]
pub fn retry_backoff_delay(attempt: u32) -> Duration {
    let base_ms =
        DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS.saturating_mul(fib_backoff_multiplier(attempt));

    apply_equal_jitter(Duration::from_millis(base_ms))
}

/// Fibonacci multiplier for retry `attempt`: `1, 1, 2, 3, 5, 8, …` for
/// `attempt = 0, 1, 2, 3, 4, 5, …` respectively.
///
/// Uses saturating arithmetic so a runaway attempt count pins the multiplier at
/// [`u64::MAX`] instead of overflowing — the final delay still saturates without
/// panicking, the same safety the previous exponential shift-clamp provided.
fn fib_backoff_multiplier(attempt: u32) -> u64 {
    let mut prev: u64 = 1;
    let mut curr: u64 = 1;
    for _ in 0..attempt {
        let next = prev.saturating_add(curr);
        prev = curr;
        curr = next;
    }
    prev
}

/// Apply *equal jitter* to a backoff `delay`: keep half of it as a floor and
/// randomize the other half, returning a value in `[delay / 2, delay]`.
///
/// Shared by both metastore backends' retry paths — [`retry_backoff_delay`] (the
/// write-conflict retry common to Turso `BEGIN CONCURRENT` and `SQLite` `SQLITE_BUSY`)
/// and the `SQLite` connection-setup retry — so concurrent retriers spread across the
/// backoff window instead of forming a thundering herd on the same boundary. Delays
/// with no meaningful jitter window (`< 2 ms`) are returned unchanged.
#[must_use]
pub fn apply_equal_jitter(delay: Duration) -> Duration {
    let total_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    let half = total_ms / 2;
    if half == 0 {
        return delay;
    }
    Duration::from_millis(half + rand::random_range(0..=half))
}

#[must_use]
pub fn is_retryable_write_conflict_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    // SQLite engine-level busy/locked indicators (rusqlite / WAL).
    message.contains("sqlite_busy")
        || message.contains("sqlite_locked")
        || message.contains("database is busy")
        || message.contains("database is locked")
        // Turso BEGIN CONCURRENT MVCC raises this on commit when another
        // transaction has already written to overlapping rows. Cayenne's
        // existing retry-on-conflict loops (in `commit_inlined_mutation`,
        // `commit_on_conflict_deletions`, snapshot publish, etc.) need to
        // back off and retry these the same way they do for sqlite_busy,
        // otherwise sustained-writes workloads against Turso panic on the
        // first cross-transaction conflict instead of converging.
        || message.contains("write-write conflict")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_delay_stays_within_equal_jitter_window_for_attempts_1_through_5() {
        // Fibonacci multipliers for attempts 1..=5 are 1, 2, 3, 5, 8.
        let fib_multipliers = [1_u64, 2, 3, 5, 8];
        for (attempt, &multiplier) in (1_u32..=5).zip(fib_multipliers.iter()) {
            let base = Duration::from_millis(DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS * multiplier);
            // Equal jitter: every sample must land in [base / 2, base].
            for _ in 0..256 {
                let delay = retry_backoff_delay(attempt);
                assert!(
                    delay >= base / 2 && delay <= base,
                    "attempt {attempt}: {delay:?} outside [{:?}, {base:?}]",
                    base / 2,
                );
            }
        }
    }

    #[test]
    fn retry_backoff_delay_for_attempt_zero_uses_base_delay_window() {
        let base = Duration::from_millis(DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS);
        for _ in 0..256 {
            let delay = retry_backoff_delay(0);
            assert!(delay >= base / 2 && delay <= base);
        }
    }

    #[test]
    fn retry_backoff_delay_does_not_panic_for_large_attempt() {
        // Fibonacci overflows u64 well before attempt 200; saturating arithmetic
        // must pin the delay instead of panicking.
        let delay = retry_backoff_delay(200);
        assert!(delay > Duration::ZERO);
    }

    #[test]
    fn fib_backoff_multiplier_follows_fibonacci_sequence() {
        // attempt = 0, 1, 2, 3, … maps to 1, 1, 2, 3, 5, 8, … (so attempt 0 and 1
        // both keep the base delay, matching the previous exponential behavior).
        let expected = [1_u64, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89];
        for (attempt, &multiplier) in expected.iter().enumerate() {
            let attempt = u32::try_from(attempt).expect("small index");
            assert_eq!(fib_backoff_multiplier(attempt), multiplier, "attempt {attempt}");
        }
    }

    #[test]
    fn fib_backoff_multiplier_saturates_without_overflow() {
        // Fibonacci passes u64::MAX around attempt 93; far beyond that it must pin
        // at u64::MAX rather than wrapping or panicking.
        assert_eq!(fib_backoff_multiplier(200), u64::MAX);
    }

    #[test]
    fn apply_equal_jitter_stays_in_window_and_actually_varies() {
        let base = Duration::from_millis(100);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..512 {
            let delay = apply_equal_jitter(base);
            assert!(
                delay >= Duration::from_millis(50) && delay <= base,
                "{delay:?} outside [50ms, 100ms]"
            );
            seen.insert(delay);
        }
        // The whole point is to spread retriers — a constant would defeat it.
        assert!(seen.len() > 1, "expected jitter to vary across calls");
    }

    #[test]
    fn apply_equal_jitter_passes_through_subjitter_delays() {
        assert_eq!(
            apply_equal_jitter(Duration::from_millis(0)),
            Duration::from_millis(0)
        );
        assert_eq!(
            apply_equal_jitter(Duration::from_millis(1)),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn is_retryable_write_conflict_message_positive_cases() {
        let messages = [
            "SQLITE_BUSY: database is locked",
            "sqlite_locked while committing transaction",
            "Database is BUSY, please retry",
            "Some prefix SQLITE_BUSY some suffix",
            "some SQLITE_LOCKED error",
            "database is locked by another transaction",
            // Turso BEGIN CONCURRENT commit-time MVCC failure.
            "Failed to commit transaction: Write-write conflict",
            "write-write conflict",
            "Write-Write Conflict",
        ];

        for message in messages {
            assert!(
                is_retryable_write_conflict_message(message),
                "expected message to be retryable: {message}"
            );
        }
    }

    #[test]
    fn is_retryable_write_conflict_message_negative_cases() {
        let messages = [
            "syntax error near 'select'",
            "unique constraint failed",
            "connection timeout",
            "disk I/O error",
            "permission denied",
            "unexpected SQLITE error without conflict indicators",
            "resource is unlocked",
        ];

        for message in messages {
            assert!(
                !is_retryable_write_conflict_message(message),
                "expected message to NOT be retryable: {message}"
            );
        }
    }
}
