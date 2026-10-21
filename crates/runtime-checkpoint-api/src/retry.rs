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

//! Retrying a sidecar write that lost a race with another writer.
//!
//! Sidecar writes share the accelerator's connection pool with the accelerator's own
//! CDC-apply transactions, so they contend. How that contention is *reported* differs
//! per engine, which is why the classifier below is a string heuristic rather than a
//! typed match: `rusqlite`, `Turso`, `DuckDB` and `tokio-postgres` each phrase it
//! differently.

use std::time::Duration;

use crate::CheckpointError;

/// Retries for a sidecar write contending with another writer, on top of the initial
/// attempt. Bounded and short: paired with [`UPSERT_MAX_RETRY_DELAY`] the worst-case
/// added latency stays well under one checkpoint/commit interval, and a persistent
/// conflict just retries on the next interval anyway.
pub const UPSERT_MAX_RETRIES: usize = 4;

/// Per-attempt cap on the `FibonacciBackoffBuilder` delay for sidecar upsert retries.
///
/// The shared Fibonacci schedule starts at 1s, far longer than a transient writer
/// hand-off needs, so clamp each delay to keep the whole retry budget (~4 x 100ms)
/// short relative to the commit interval.
pub const UPSERT_MAX_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Whether a sidecar write failure is a transient lock/contention error worth retrying
/// rather than surfacing.
///
/// Deliberately a string heuristic over the boxed engine error, mirroring the reconnect
/// classifier in `data_components::mysql_replication::resilience`. Slight over-matching
/// is harmless: retries are bounded, so a misclassified non-lock error only costs a few
/// short sleeps before it is returned unchanged.
///
/// The `DuckDB` markers matter because its transaction manager is optimistic — it
/// reports a write-write conflict instead of blocking, so two sidecar writers touching
/// the same row surface `TransactionContext Error: Conflict on update!` rather than
/// serializing. Sidecar writers take the pool's write gate with `read()` and so do not
/// exclude each other; only a file swap takes it exclusively.
#[must_use]
pub fn is_retryable_lock_error(err: &CheckpointError) -> bool {
    const MARKERS: &[&str] = &[
        "database is locked",
        "database table is locked",
        "sqlite_busy",
        "sqlite_locked",
        "deadlock",
        // DuckDB's optimistic concurrency control.
        "conflict on update",
        "transactioncontext error",
        "write-write conflict",
    ];
    let msg = err.to_string().to_ascii_lowercase();
    MARKERS.iter().any(|marker| msg.contains(marker))
}

/// Runs a sidecar write, retrying a transient write conflict a bounded number of times.
///
/// # Errors
///
/// Returns the attempt's error once it is not retryable, or once the retry budget is
/// spent.
pub async fn retry_on_write_conflict<F, Fut>(
    dataset_name: &str,
    attempt: F,
) -> Result<(), CheckpointError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<(), CheckpointError>>,
{
    use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

    let backoff = FibonacciBackoffBuilder::new()
        .max_retries(Some(UPSERT_MAX_RETRIES))
        .max_duration(Some(UPSERT_MAX_RETRY_DELAY))
        .build();

    retry(backoff, || async {
        attempt().await.map_err(|e| {
            if is_retryable_lock_error(&e) {
                tracing::debug!(
                    dataset = %dataset_name,
                    error = %e,
                    "sidecar offset upsert hit a transient accelerator write conflict"
                );
                RetryError::transient(e)
            } else {
                RetryError::permanent(e)
            }
        })
    })
    .await
}
