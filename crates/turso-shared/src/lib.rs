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

pub const BEGIN_CONCURRENT_SQL: &str = "BEGIN CONCURRENT";
pub const BEGIN_TRANSACTION_SQL: &str = "BEGIN TRANSACTION";
pub const COMMIT_SQL: &str = "COMMIT";
pub const MVCC_JOURNAL_MODE_VALUE: &str = "'mvcc'";

pub const DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS: u32 = 4;
pub const DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS: u64 = 10;

#[must_use]
pub fn retry_backoff_delay(attempt: u32) -> Duration {
    Duration::from_millis(
        DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS * (1_u64 << attempt.saturating_sub(1)),
    )
}

#[must_use]
pub fn is_retryable_write_conflict_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("sqlite_busy")
        || message.contains("sqlite_locked")
        || message.contains("database is busy")
        || message.contains("database is locked")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_delay_grows_exponentially_for_attempts_1_through_5() {
        for attempt in 1_u32..=5 {
            let delay = retry_backoff_delay(attempt);
            let expected_ms =
                DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS * (1_u64 << attempt.saturating_sub(1));
            assert_eq!(delay, Duration::from_millis(expected_ms));
        }
    }

    #[test]
    fn retry_backoff_delay_for_attempt_zero_uses_base_delay() {
        let delay = retry_backoff_delay(0);
        let expected_ms = DEFAULT_CONCURRENT_RETRY_BASE_DELAY_MS;
        assert_eq!(delay, Duration::from_millis(expected_ms));
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
