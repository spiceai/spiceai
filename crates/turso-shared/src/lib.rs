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
pub const WAL_JOURNAL_MODE_VALUE: &str = "'WAL'";

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
        || message.contains("busy")
        || message.contains("locked")
}
