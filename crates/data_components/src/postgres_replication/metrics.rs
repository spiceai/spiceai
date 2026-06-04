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

//! Runtime metrics for the Postgres logical-replication path.
//!
//! Modeled after [`crate::dynamodb`]'s streams metrics: a lightweight
//! `Collector` with `AtomicU64` counters and an `RwLock<SystemTime>` watermark,
//! wrapped in a read-only `Metrics` handle. The connector exposes these via
//! `MetricsProvider` so they flow through OpenTelemetry as observables named
//! `dataset_postgres_<metric_spec_name>`, for example
//! `dataset_postgres_replication_lag_ms`.

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::SystemTime;

/// Mutable collector used by the replication stream to record events.
///
/// Created once per dataset on the `Postgres` connector, shared with the
/// running stream, and snapshot-read by the `MetricsProvider`.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    // WAL change counters (monotonic).
    wal_inserts_total: AtomicU64,
    wal_updates_total: AtomicU64,
    wal_deletes_total: AtomicU64,
    wal_truncates_total: AtomicU64,
    wal_transactions_total: AtomicU64,

    // Bootstrap progress.
    bootstrap_rows_total: AtomicU64,
    bootstrap_rows_expected: AtomicU64, // 0 = unknown; set from extended schema inference
    bootstrap_complete: AtomicU64,      // 0 = running/not-started, 1 = done

    // LSN position.
    confirmed_flush_lsn: AtomicU64, // last LSN we acknowledged to the server
    server_wal_end_lsn: AtomicU64,  // latest WAL end reported by the server (keepalive)

    // Errors.
    wal_decode_errors_total: AtomicU64,
    schema_mismatch_errors_total: AtomicU64,
    replication_recv_errors_total: AtomicU64,
    /// Number of times the stream reconnected after a transient failure.
    /// A non-zero value with no user-visible error just means the network
    /// wobbled and we recovered.
    replication_reconnects_total: AtomicU64,

    // Watermark: commit time of the most-recent transaction we've ingested.
    // Used to compute `replication_lag_ms = now - watermark`.
    last_commit_seen_at: RwLock<Option<SystemTime>>,
}

impl MetricsCollector {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc_insert(&self) {
        self.wal_inserts_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_update(&self) {
        self.wal_updates_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_delete(&self) {
        self.wal_deletes_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_truncate(&self) {
        self.wal_truncates_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_transaction(&self) {
        self.wal_transactions_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_bootstrap_rows(&self, n: u64) {
        self.bootstrap_rows_total.fetch_add(n, Ordering::Relaxed);
    }
    pub fn mark_bootstrap_complete(&self) {
        self.bootstrap_complete.store(1, Ordering::Relaxed);
    }

    /// Set the estimated total rows to bootstrap (from extended schema inference).
    /// `0` leaves the total unknown. Enables a progress fraction during the snapshot.
    pub fn set_bootstrap_rows_expected(&self, n: u64) {
        self.bootstrap_rows_expected.store(n, Ordering::Relaxed);
    }
    #[must_use]
    pub fn bootstrap_rows_expected(&self) -> u64 {
        self.bootstrap_rows_expected.load(Ordering::Relaxed)
    }
    /// Bootstrap progress as a percent (0–100), or `None` when the expected total
    /// is unknown. Clamped to 100 since the source estimate is approximate.
    #[must_use]
    pub fn bootstrap_progress_percent(&self) -> Option<u64> {
        let expected = self.bootstrap_rows_expected.load(Ordering::Relaxed);
        if expected == 0 {
            return None;
        }
        let total = self.bootstrap_rows_total.load(Ordering::Relaxed);
        Some((total.saturating_mul(100) / expected).min(100))
    }

    pub fn set_confirmed_flush_lsn(&self, lsn: u64) {
        // Monotonic CAS — never regress.
        let mut current = self.confirmed_flush_lsn.load(Ordering::Relaxed);
        while lsn > current {
            match self.confirmed_flush_lsn.compare_exchange(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn set_server_wal_end(&self, lsn: u64) {
        let mut current = self.server_wal_end_lsn.load(Ordering::Relaxed);
        while lsn > current {
            match self.server_wal_end_lsn.compare_exchange(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn record_commit_watermark(&self, at: SystemTime) {
        if let Ok(mut guard) = self.last_commit_seen_at.write() {
            *guard = Some(at);
        }
    }

    pub fn inc_decode_error(&self) {
        self.wal_decode_errors_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_schema_mismatch_error(&self) {
        self.schema_mismatch_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_recv_error(&self) {
        self.replication_recv_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_reconnect(&self) {
        self.replication_reconnects_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Read-only snapshot interface used by the `MetricsProvider` callbacks.
///
/// Cheaply cloneable (`Arc` inside).
#[derive(Debug, Clone)]
pub struct Metrics {
    collector: Arc<MetricsCollector>,
}

impl Metrics {
    #[must_use]
    pub fn new(collector: Arc<MetricsCollector>) -> Self {
        Self { collector }
    }

    #[must_use]
    pub fn wal_inserts_total(&self) -> u64 {
        self.collector.wal_inserts_total.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn wal_updates_total(&self) -> u64 {
        self.collector.wal_updates_total.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn wal_deletes_total(&self) -> u64 {
        self.collector.wal_deletes_total.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn wal_truncates_total(&self) -> u64 {
        self.collector.wal_truncates_total.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn wal_transactions_total(&self) -> u64 {
        self.collector
            .wal_transactions_total
            .load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn bootstrap_rows_total(&self) -> u64 {
        self.collector.bootstrap_rows_total.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn bootstrap_complete(&self) -> u64 {
        self.collector.bootstrap_complete.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn bootstrap_rows_expected(&self) -> u64 {
        self.collector.bootstrap_rows_expected()
    }
    /// Bootstrap progress percent (0–100), or `None` when the expected total is unknown.
    #[must_use]
    pub fn bootstrap_progress_percent(&self) -> Option<u64> {
        self.collector.bootstrap_progress_percent()
    }

    #[must_use]
    pub fn confirmed_flush_lsn(&self) -> u64 {
        self.collector.confirmed_flush_lsn.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn server_wal_end_lsn(&self) -> u64 {
        self.collector.server_wal_end_lsn.load(Ordering::Relaxed)
    }

    /// Bytes between the server's reported WAL end and our last confirmed
    /// flush LSN. Returns 0 if the server hasn't reported yet or if we're
    /// ahead of the last-seen server position (can happen with stale atomic reads).
    #[must_use]
    pub fn replication_lag_bytes(&self) -> u64 {
        let server = self.server_wal_end_lsn();
        let confirmed = self.confirmed_flush_lsn();
        server.saturating_sub(confirmed)
    }

    /// Milliseconds since the most recent transaction we've ingested.
    /// Returns `None` before the first commit is seen.
    #[must_use]
    pub fn replication_lag_ms(&self) -> Option<u64> {
        let watermark = self
            .collector
            .last_commit_seen_at
            .read()
            .ok()?
            .as_ref()
            .copied()?;
        SystemTime::now()
            .duration_since(watermark)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
    }

    #[must_use]
    pub fn wal_decode_errors_total(&self) -> u64 {
        self.collector
            .wal_decode_errors_total
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn schema_mismatch_errors_total(&self) -> u64 {
        self.collector
            .schema_mismatch_errors_total
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn replication_recv_errors_total(&self) -> u64 {
        self.collector
            .replication_recv_errors_total
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn replication_reconnects_total(&self) -> u64 {
        self.collector
            .replication_reconnects_total
            .load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment_monotonically() {
        let c = MetricsCollector::new();
        c.inc_insert();
        c.inc_insert();
        c.inc_update();
        c.inc_delete();
        c.inc_truncate();
        c.inc_transaction();
        let m = Metrics::new(c);
        assert_eq!(m.wal_inserts_total(), 2);
        assert_eq!(m.wal_updates_total(), 1);
        assert_eq!(m.wal_deletes_total(), 1);
        assert_eq!(m.wal_truncates_total(), 1);
        assert_eq!(m.wal_transactions_total(), 1);
    }

    #[test]
    fn bootstrap_progress_tracks_expected() {
        let c = MetricsCollector::new();
        let m = Metrics::new(Arc::clone(&c));
        // Unknown until an expected total is set.
        assert_eq!(m.bootstrap_progress_percent(), None);

        c.set_bootstrap_rows_expected(200);
        c.add_bootstrap_rows(50);
        assert_eq!(m.bootstrap_rows_expected(), 200);
        assert_eq!(m.bootstrap_progress_percent(), Some(25));

        // The estimate can be exceeded; progress clamps at 100.
        c.add_bootstrap_rows(1000);
        assert_eq!(m.bootstrap_progress_percent(), Some(100));
    }

    #[test]
    fn confirmed_flush_lsn_never_regresses() {
        let c = MetricsCollector::new();
        c.set_confirmed_flush_lsn(100);
        c.set_confirmed_flush_lsn(50); // should be ignored
        c.set_confirmed_flush_lsn(200);
        let m = Metrics::new(c);
        assert_eq!(m.confirmed_flush_lsn(), 200);
    }

    #[test]
    fn lag_bytes_saturates_on_reorder() {
        let c = MetricsCollector::new();
        c.set_confirmed_flush_lsn(300);
        c.set_server_wal_end(200); // stale read
        let m = Metrics::new(c);
        // saturating_sub → 0 rather than wrap-around
        assert_eq!(m.replication_lag_bytes(), 0);
    }

    #[test]
    fn lag_bytes_reports_delta() {
        let c = MetricsCollector::new();
        c.set_confirmed_flush_lsn(100);
        c.set_server_wal_end(500);
        let m = Metrics::new(c);
        assert_eq!(m.replication_lag_bytes(), 400);
    }

    #[test]
    fn lag_ms_is_none_before_first_commit() {
        let c = MetricsCollector::new();
        let m = Metrics::new(c);
        assert!(m.replication_lag_ms().is_none());
    }

    #[test]
    fn lag_ms_reports_after_watermark_set() {
        let c = MetricsCollector::new();
        c.record_commit_watermark(SystemTime::now() - std::time::Duration::from_millis(150));
        let m = Metrics::new(c);
        let lag = m.replication_lag_ms().expect("lag set");
        // Should be at least ~150ms; allow slack.
        assert!(lag >= 140, "expected ≥140ms lag, got {lag}");
    }

    #[test]
    fn bootstrap_tracking() {
        let c = MetricsCollector::new();
        c.add_bootstrap_rows(10);
        c.add_bootstrap_rows(5);
        let m = Metrics::new(Arc::clone(&c));
        assert_eq!(m.bootstrap_rows_total(), 15);
        assert_eq!(m.bootstrap_complete(), 0);
        c.mark_bootstrap_complete();
        assert_eq!(m.bootstrap_complete(), 1);
    }
}
