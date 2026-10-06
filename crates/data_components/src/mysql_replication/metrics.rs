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

//! Lock-free counters/gauges updated by the replication stream and read by
//! the connector's OpenTelemetry `MetricsProvider`. Mirrors
//! `postgres_replication::metrics` with `MySQL` vocabulary (binlog position
//! instead of LSN).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

/// Shared collector; the stream holds an `Arc` and updates it as it
/// processes events.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    truncates: AtomicU64,
    transactions: AtomicU64,
    bootstrap_rows: AtomicU64,
    /// `u64::MAX` = no estimate available; distinguishes "unknown" from a
    /// known-empty source table.
    bootstrap_rows_expected: AtomicU64,
    bootstrap_complete: AtomicU64,
    /// `1` when the stream is positioning by GTID auto-positioning
    /// (failover-safe), `0` for file+offset. Set once at stream start.
    gtid_enabled: AtomicU64,
    /// Byte offset of the most recently committed (acked) binlog position.
    committed_pos: AtomicU64,
    /// Numeric suffix of the committed binlog file (`binlog.000042` → 42);
    /// `0` until the first commit.
    committed_file_ordinal: AtomicU64,
    /// Byte offset of the source's binlog head, from the periodic
    /// `SHOW BINARY LOG STATUS` poll; 0 until first polled.
    source_head_pos: AtomicU64,
    /// Numeric suffix of the source's head binlog file; 0 until first polled.
    source_head_file_ordinal: AtomicU64,
    /// Bytes between the source head and the stream's resume position when
    /// both are in the same binlog file; `u64::MAX` = unknown (different
    /// files, or not yet polled).
    lag_bytes: AtomicU64,
    /// Millis-since-epoch of the newest source commit applied; 0 = none yet.
    last_commit_unix_ms: AtomicU64,
    decode_errors: AtomicU64,
    schema_mismatch_errors: AtomicU64,
    recv_errors: AtomicU64,
    reconnects: AtomicU64,
    checkpoint_persists: AtomicU64,
    checkpoint_persist_errors: AtomicU64,
    /// Shared-binlog membership liveness: `1` while this dataset is an attached
    /// member of a shared dump (`super::shared`), `0` once detached.
    /// Initialized to `1` — a member starts attached to its group. A detached
    /// member holds the shared resume position back, so this is the
    /// unambiguous signal for *which* dataset stalled the group.
    member_attached: AtomicU64,
    /// Cumulative seconds the shared dump's pump spent blocked delivering
    /// committed changes into this dataset's channel because its sink was not
    /// draining. The pump reads the dump socket for the whole group, so this is
    /// also the time the socket went undrained — the source aborts the dump once
    /// that passes its `net_write_timeout`.
    member_send_stalled_seconds: AtomicU64,
}

impl MetricsCollector {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bootstrap_rows_expected: AtomicU64::new(u64::MAX),
            lag_bytes: AtomicU64::new(u64::MAX),
            // A member starts attached to its shared dump; the shared path
            // flips this to 0 on detach.
            member_attached: AtomicU64::new(1),
            ..Self::default()
        })
    }

    pub fn inc_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_truncate(&self) {
        self.truncates.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_transaction(&self) {
        self.transactions.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_bootstrap_rows(&self, n: u64) {
        self.bootstrap_rows.fetch_add(n, Ordering::Relaxed);
    }
    pub fn mark_bootstrap_complete(&self) {
        self.bootstrap_complete.store(1, Ordering::Relaxed);
    }
    pub fn set_bootstrap_rows_expected(&self, n: u64) {
        self.bootstrap_rows_expected.store(n, Ordering::Relaxed);
    }
    pub fn set_gtid_enabled(&self, enabled: bool) {
        self.gtid_enabled
            .store(u64::from(enabled), Ordering::Relaxed);
    }

    /// Record the acked (committed-to-sidecar-visible) binlog position.
    pub fn set_committed_position(&self, file_ordinal: u64, pos: u64) {
        self.committed_file_ordinal
            .store(file_ordinal, Ordering::Relaxed);
        self.committed_pos.store(pos, Ordering::Relaxed);
    }

    /// Record the source's binlog head from the periodic status poll, plus
    /// the byte lag versus the stream's resume position when computable
    /// (`None` = head and resume are in different binlog files).
    pub fn set_source_head(&self, file_ordinal: u64, pos: u64, lag_bytes: Option<u64>) {
        self.source_head_file_ordinal
            .store(file_ordinal, Ordering::Relaxed);
        self.source_head_pos.store(pos, Ordering::Relaxed);
        self.lag_bytes
            .store(lag_bytes.unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Record the source commit timestamp of the newest applied transaction,
    /// for the replication-lag signal.
    pub fn record_commit_watermark(&self, at: SystemTime) {
        if let Ok(d) = at.duration_since(std::time::UNIX_EPOCH) {
            let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
            self.last_commit_unix_ms.store(ms, Ordering::Relaxed);
        }
    }

    pub fn inc_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_schema_mismatch_error(&self) {
        self.schema_mismatch_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_recv_error(&self) {
        self.recv_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_checkpoint_persist(&self) {
        self.checkpoint_persists.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_checkpoint_persist_error(&self) {
        self.checkpoint_persist_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Mark this dataset an attached member of a shared binlog dump.
    pub fn mark_member_attached(&self) {
        self.member_attached.store(1, Ordering::Relaxed);
    }

    /// Mark this dataset detached from the shared dump (its floor is now held,
    /// pinning the shared resume position).
    pub fn mark_member_detached(&self) {
        self.member_attached.store(0, Ordering::Relaxed);
    }

    /// Accrue another stall interval spent waiting on this dataset's channel.
    pub fn add_send_stalled(&self, secs: u64) {
        self.member_send_stalled_seconds
            .fetch_add(secs, Ordering::Relaxed);
    }
}

/// Read handle for the connector's `MetricsProvider` callbacks.
#[derive(Clone, Debug)]
pub struct Metrics {
    collector: Arc<MetricsCollector>,
}

impl Metrics {
    #[must_use]
    pub fn new(collector: Arc<MetricsCollector>) -> Self {
        Self { collector }
    }

    #[must_use]
    pub fn inserts_total(&self) -> u64 {
        self.collector.inserts.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn updates_total(&self) -> u64 {
        self.collector.updates.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn deletes_total(&self) -> u64 {
        self.collector.deletes.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn truncates_total(&self) -> u64 {
        self.collector.truncates.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn transactions_total(&self) -> u64 {
        self.collector.transactions.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn bootstrap_rows_total(&self) -> u64 {
        self.collector.bootstrap_rows.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn bootstrap_complete(&self) -> u64 {
        self.collector.bootstrap_complete.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn gtid_enabled(&self) -> u64 {
        self.collector.gtid_enabled.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn bootstrap_rows_expected(&self) -> Option<u64> {
        match self
            .collector
            .bootstrap_rows_expected
            .load(Ordering::Relaxed)
        {
            u64::MAX => None,
            v => Some(v),
        }
    }
    #[must_use]
    pub fn committed_binlog_pos(&self) -> u64 {
        self.collector.committed_pos.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn committed_binlog_file_ordinal(&self) -> u64 {
        self.collector
            .committed_file_ordinal
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn source_head_pos(&self) -> u64 {
        self.collector.source_head_pos.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn source_head_file_ordinal(&self) -> u64 {
        self.collector
            .source_head_file_ordinal
            .load(Ordering::Relaxed)
    }

    /// Bytes of binlog between the source head and the stream's resume
    /// position; `None` when unknown (head not yet polled, or the two
    /// positions are in different binlog files).
    #[must_use]
    pub fn replication_lag_bytes(&self) -> Option<u64> {
        match self.collector.lag_bytes.load(Ordering::Relaxed) {
            u64::MAX => None,
            v => Some(v),
        }
    }

    /// Milliseconds between now and the newest applied source commit
    /// timestamp; `None` until the first transaction commits. Binlog event
    /// timestamps have 1-second granularity, so treat sub-second values as
    /// noise.
    #[must_use]
    pub fn replication_lag_ms(&self) -> Option<u64> {
        let committed = self.collector.last_commit_unix_ms.load(Ordering::Relaxed);
        if committed == 0 {
            return None;
        }
        let now_ms = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())?;
        Some(now_ms.saturating_sub(committed))
    }

    #[must_use]
    pub fn decode_errors_total(&self) -> u64 {
        self.collector.decode_errors.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn schema_mismatch_errors_total(&self) -> u64 {
        self.collector
            .schema_mismatch_errors
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn recv_errors_total(&self) -> u64 {
        self.collector.recv_errors.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn reconnects_total(&self) -> u64 {
        self.collector.reconnects.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn checkpoint_persists_total(&self) -> u64 {
        self.collector.checkpoint_persists.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn checkpoint_persist_errors_total(&self) -> u64 {
        self.collector
            .checkpoint_persist_errors
            .load(Ordering::Relaxed)
    }
    /// `1` while attached to the shared dump, `0` once detached from it.
    #[must_use]
    pub fn member_attached(&self) -> u64 {
        self.collector.member_attached.load(Ordering::Relaxed)
    }
    /// Cumulative seconds the shared dump's pump waited on this dataset's
    /// channel, i.e. seconds the dump socket went undrained on its behalf.
    #[must_use]
    pub fn member_send_stalled_seconds_total(&self) -> u64 {
        self.collector
            .member_send_stalled_seconds
            .load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_rows_expected_distinguishes_unknown_from_zero() {
        let collector = MetricsCollector::new();
        let metrics = Metrics::new(Arc::clone(&collector));
        assert_eq!(metrics.bootstrap_rows_expected(), None);
        collector.set_bootstrap_rows_expected(0);
        assert_eq!(metrics.bootstrap_rows_expected(), Some(0));
    }

    #[test]
    fn lag_is_none_before_first_commit() {
        let metrics = Metrics::new(MetricsCollector::new());
        assert_eq!(metrics.replication_lag_ms(), None);
    }

    #[test]
    fn lag_reflects_commit_watermark() {
        let collector = MetricsCollector::new();
        let metrics = Metrics::new(Arc::clone(&collector));
        collector.record_commit_watermark(SystemTime::now() - std::time::Duration::from_secs(5));
        let lag = metrics.replication_lag_ms().expect("lag after watermark");
        assert!((4_000..60_000).contains(&lag), "lag {lag}ms out of range");
    }
}
