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
    atomic::{AtomicBool, AtomicU64, Ordering},
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
    bootstrap_rows_expected: AtomicU64, // valid only when `bootstrap_rows_expected_known`
    bootstrap_rows_expected_known: AtomicBool, // false = no estimate yet (distinct from `0`)
    bootstrap_complete: AtomicU64,      // 0 = running/not-started, 1 = done

    // LSN position.
    confirmed_flush_lsn: AtomicU64, // last LSN we acknowledged to the server
    server_wal_end_lsn: AtomicU64,  // latest WAL end reported by the server (keepalive)

    // Reader-task time accounting (microseconds), to split "blocked waiting on the
    // source socket" (input wait) from "our decode/build" (processing) — the
    // source-vs-us discriminator for a reader/delivery-bound pipeline.
    reader_input_wait_micros_total: AtomicU64,
    reader_processing_micros_total: AtomicU64,

    // Schema evolution (stream-time, from pgoutput Relation messages).
    /// Widening schema changes adopted into the working schema.
    schema_evolutions_total: AtomicU64,
    /// Schema changes detected but NOT adopted: incompatible/non-widening
    /// changes (terminal error) or changes ignored under `block`.
    schema_evolution_rejections_total: AtomicU64,

    // Errors.
    wal_decode_errors_total: AtomicU64,
    schema_mismatch_errors_total: AtomicU64,
    replication_recv_errors_total: AtomicU64,
    /// Number of times the stream reconnected after a transient failure.
    /// A non-zero value with no user-visible error just means the network
    /// wobbled and we recovered.
    replication_reconnects_total: AtomicU64,
    /// Cumulative milliseconds the stream was disconnected across all reconnects
    /// (drop → successful resume, including backoff). Paired with
    /// `replication_reconnects_total`, this quantifies the DURATION cost of a
    /// reconnect storm — during that time no changes are delivered and lag grows,
    /// and Postgres replays from the held floor on resume.
    replication_disconnected_ms_total: AtomicU64,
    /// The replication slot this dataset is a member of (for shared slots, several
    /// datasets share one). Lets the analysis join the per-dataset view to the
    /// authoritative per-slot backlog and show grouping. Set at (shared) attach.
    slot_name: RwLock<Option<String>>,
    /// Why this dataset's acceleration was rebuilt from the source instead of
    /// resumed, as [`super::RebuildCause::label`], or `None` when it resumed.
    ///
    /// A rebuild is a full re-read nobody asked for, and the causes call for
    /// different responses — a restored source, a repointed endpoint, a broken
    /// sidecar, a slot lifecycle problem. Reporting only "rebuilt" would make
    /// them indistinguishable without scraping log text. Set at attach; a member
    /// classifies once, so this is a label rather than a counter.
    rebuild_cause: RwLock<Option<&'static str>>,
    /// Cumulative seconds the shared-slot pump spent blocked trying to deliver
    /// committed changes into this member's mailbox because its sink was not
    /// draining. Non-zero means downstream backpressure stalled the pump (and
    /// therefore every other member on the slot); the server connection itself
    /// stays alive throughout. Only ever set for shared-slot datasets.
    member_send_stalled_seconds_total: AtomicU64,
    /// Cumulative microseconds the shared-slot pump spent `await`ing this
    /// member's bounded mailbox while delivering committed changes. Unlike
    /// `member_send_stalled_seconds_total` (which only ticks after a full
    /// `MEMBER_SEND_STALL_WARN` interval elapses), this accrues the *full*
    /// per-commit wait, including sub-second waits. The pump already subtracts
    /// this wait from `reader_processing_micros_total` at the source (so that
    /// counter stays decode-only, not inflated by downstream back-pressure);
    /// this counter exports the subtracted amount so the waterfall can attribute
    /// it to apply back-pressure rather than lose it. Only set for shared slots.
    member_send_wait_micros_total: AtomicU64,
    /// Envelopes the shared-slot pump published to this member as distinct
    /// units of work. Compared against `wal_transactions_total` (source
    /// transactions), the ratio is the coalescing factor the accelerator's apply
    /// loop actually sees. Only set for shared slots.
    member_envelopes_delivered_total: AtomicU64,
    /// Source transactions folded into an envelope the pump was still holding,
    /// before it crossed the member boundary at all.
    member_envelope_eager_merges_total: AtomicU64,
    /// Source transactions folded into an envelope already sitting unclaimed in
    /// this member's mailbox. This is the back-pressure-driven half of
    /// coalescing: it rises when the sink is not keeping up, which is exactly
    /// when collapsing envelopes matters. Split from
    /// `member_envelope_eager_merges_total` so the two stages can be attributed
    /// separately.
    member_envelope_mailbox_merges_total: AtomicU64,
    /// Times a transaction could NOT be folded into this member's unclaimed
    /// mailbox tail because a configured bound refused it — the per-envelope row
    /// limit or the mailbox byte budget — rather than because the envelopes were
    /// not foldable at all. The mailbox bounds ship deliberately low (folding is
    /// a back-pressure absorber, not a throughput lever), so a persistently
    /// rising value here alongside a rising
    /// `member_envelope_mailbox_merges_total` is the evidence that raising
    /// `SPICE_POSTGRES_CDC_MAX_BACKPRESSURE_ROWS_PER_ENVELOPE` /
    /// `SPICE_POSTGRES_CDC_MAX_MAILBOX_BYTES` would absorb more. A flat zero
    /// means the bounds are not binding and there is nothing to tune.
    member_mailbox_coalesce_limited_total: AtomicU64,

    // Shared-slot membership liveness. `member_attached` is `1` while this
    // dataset is an attached member of its shared replication slot and `0` once
    // it has detached — a detached member freezes its ack floor and pins WAL
    // retention for the WHOLE shared slot until it rejoins or spiced restarts
    // (#11644). `member_attached_known` gates observation to shared-slot datasets
    // only: it stays `false` for a dedicated (non-shared) slot, whose single
    // consumer has no member-detach concept, so the metric reports no series
    // there rather than a misleading constant `0`/`1`.
    member_attached: AtomicU64,
    member_attached_known: AtomicBool,

    // Watermark: commit time of the most-recent transaction we've ingested.
    // Used to compute `replication_lag_ms = now - watermark`.
    last_commit_seen_at: RwLock<Option<SystemTime>>,
}

impl MetricsCollector {
    #[must_use]
    pub fn new() -> Arc<Self> {
        // `member_attached` stays "unknown" (series absent) until the shared-slot path
        // calls `mark_member_attached`; a dedicated (non-shared) slot never does, so it
        // reports no membership series rather than a misleading constant.
        Arc::new(Self::default())
    }

    /// Record which replication slot this member belongs to (shared-slot grouping).
    pub fn set_slot_name(&self, slot: String) {
        // Recover through poisoning so an unrelated panic can't leave the `slot` label
        // permanently unset (which would break shared-slot grouping in the analysis).
        let mut guard = self
            .slot_name
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(slot);
    }

    /// Record why this dataset's acceleration is being rebuilt rather than
    /// resumed. Called once at attach, only on the rebuild path.
    pub fn set_rebuild_cause(&self, cause: &'static str) {
        // Recover through poisoning, as `set_slot_name` does: an unrelated panic must
        // not leave a rebuild unattributed in the analysis.
        let mut guard = self
            .rebuild_cause
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(cause);
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

    /// Set the estimated total rows to bootstrap (from schema inference).
    /// Marks the estimate as known, so a count of `0` is a valid value (a known-empty
    /// source table) rather than being conflated with "no estimate available".
    pub fn set_bootstrap_rows_expected(&self, n: u64) {
        self.bootstrap_rows_expected.store(n, Ordering::Relaxed);
        // Release pairs with the Acquire load in `bootstrap_rows_expected()`: once a
        // reader (the metrics callback, a different thread) observes the flag as `true`,
        // the value store above is guaranteed visible — never a stale default `0`.
        self.bootstrap_rows_expected_known
            .store(true, Ordering::Release);
    }
    /// The estimated bootstrap row total, or `None` when no estimate is available
    /// (schema inference surfaced no row count for the source). `Some(0)` is a
    /// known-empty source table — deliberately distinct from `None`.
    #[must_use]
    pub fn bootstrap_rows_expected(&self) -> Option<u64> {
        // Acquire pairs with the Release store in `set_bootstrap_rows_expected()` so the
        // value load below never observes a stale default once the flag reads `true`.
        if self.bootstrap_rows_expected_known.load(Ordering::Acquire) {
            Some(self.bootstrap_rows_expected.load(Ordering::Relaxed))
        } else {
            None
        }
    }
    /// Bootstrap progress as a percent (0–100), or `None` when the expected total is
    /// unknown. A known-empty source table (`Some(0)`) reports `Some(100)` — the
    /// snapshot is trivially complete. Clamped to 100 since the estimate is approximate.
    #[must_use]
    pub fn bootstrap_progress_percent(&self) -> Option<u64> {
        let expected = self.bootstrap_rows_expected()?;
        if expected == 0 {
            return Some(100);
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

    /// Add to the reader input-wait accumulator (time blocked awaiting the next
    /// event from the source socket). High relative to processing ⇒ source/network
    /// can't deliver fast enough (source-bound).
    pub fn add_reader_input_wait_micros(&self, us: u64) {
        self.reader_input_wait_micros_total
            .fetch_add(us, Ordering::Relaxed);
    }
    /// Add to the reader processing accumulator (decode + batch-build after a
    /// socket event). High relative to input-wait ⇒ our decode/build is the limiter.
    pub fn add_reader_processing_micros(&self, us: u64) {
        self.reader_processing_micros_total
            .fetch_add(us, Ordering::Relaxed);
    }

    pub fn record_commit_watermark(&self, at: SystemTime) {
        if let Ok(mut guard) = self.last_commit_seen_at.write() {
            *guard = Some(at);
        }
    }

    pub fn inc_schema_evolution(&self) {
        self.schema_evolutions_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_schema_evolution_rejected(&self) {
        self.schema_evolution_rejections_total
            .fetch_add(1, Ordering::Relaxed);
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
    /// Add to the cumulative member-send-stalled seconds counter (shared slot).
    pub fn add_send_stalled(&self, secs: u64) {
        self.member_send_stalled_seconds_total
            .fetch_add(secs, Ordering::Relaxed);
    }
    /// Add microseconds the shared-slot pump spent `await`ing this member's
    /// mailbox during commit delivery (shared slot). The pump subtracts the
    /// same amount from `reader_processing_micros_total` at the source, so that
    /// counter stays decode-only; this exports the subtracted wait for
    /// attribution.
    pub fn add_member_send_wait_micros(&self, us: u64) {
        self.member_send_wait_micros_total
            .fetch_add(us, Ordering::Relaxed);
    }
    /// Count an envelope published to this member as its own unit of work
    /// (shared slot).
    pub fn inc_envelope_delivered(&self) {
        self.member_envelopes_delivered_total
            .fetch_add(1, Ordering::Relaxed);
    }
    /// Count a source transaction folded into an envelope the pump was still
    /// holding back (shared slot).
    pub fn inc_envelope_merged_eager(&self) {
        self.member_envelope_eager_merges_total
            .fetch_add(1, Ordering::Relaxed);
    }
    /// Count a source transaction folded into an unclaimed envelope already in
    /// this member's mailbox (shared slot).
    pub fn inc_envelope_merged_mailbox(&self) {
        self.member_envelope_mailbox_merges_total
            .fetch_add(1, Ordering::Relaxed);
    }
    /// Count a fold refused by a configured mailbox bound (shared slot) — the
    /// signal that raising that bound would coalesce more.
    pub fn inc_mailbox_coalesce_limited(&self) {
        self.member_mailbox_coalesce_limited_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_reconnect(&self) {
        self.replication_reconnects_total
            .fetch_add(1, Ordering::Relaxed);
    }
    /// Add elapsed disconnected time (ms) for a completed reconnect (drop → resume).
    pub fn add_disconnected_ms(&self, ms: u64) {
        self.replication_disconnected_ms_total
            .fetch_add(ms, Ordering::Relaxed);
    }

    /// Mark this dataset as an attached member of its shared replication slot
    /// (fresh join or in-process rejoin). Also flips the "known" flag so the
    /// metric begins reporting — a dataset on a dedicated (non-shared) slot never
    /// calls this, so its series stays absent rather than a misleading `0`.
    pub fn mark_member_attached(&self) {
        self.member_attached.store(1, Ordering::Relaxed);
        // Release pairs with the Acquire load in `member_attached()` so a reader
        // that observes `known == true` also sees the value store above.
        self.member_attached_known.store(true, Ordering::Release);
    }

    /// Mark this dataset as DETACHED from its shared replication slot: its ack
    /// floor is now frozen and pins WAL retention for the whole slot until it
    /// rejoins or spiced restarts (#11644).
    pub fn mark_member_detached(&self) {
        self.member_attached.store(0, Ordering::Relaxed);
        self.member_attached_known.store(true, Ordering::Release);
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
    pub fn bootstrap_rows_expected(&self) -> Option<u64> {
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

    #[must_use]
    pub fn reader_input_wait_micros_total(&self) -> u64 {
        self.collector
            .reader_input_wait_micros_total
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn reader_processing_micros_total(&self) -> u64 {
        self.collector
            .reader_processing_micros_total
            .load(Ordering::Relaxed)
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
    pub fn schema_evolutions_total(&self) -> u64 {
        self.collector
            .schema_evolutions_total
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn schema_evolution_rejections_total(&self) -> u64 {
        self.collector
            .schema_evolution_rejections_total
            .load(Ordering::Relaxed)
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
    #[must_use]
    pub fn replication_disconnected_ms_total(&self) -> u64 {
        self.collector
            .replication_disconnected_ms_total
            .load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn slot_name(&self) -> Option<String> {
        // Recover through poisoning: an unrelated panic must not permanently drop the
        // `slot` label (which would break shared-slot grouping in the analysis). The
        // guarded String is not corrupted by another thread's panic.
        self.collector
            .slot_name
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// Why this dataset's acceleration was rebuilt, or `None` when it resumed.
    #[must_use]
    pub fn rebuild_cause(&self) -> Option<&'static str> {
        // Recover through poisoning, as `slot_name` does.
        *self
            .collector
            .rebuild_cause
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    #[must_use]
    pub fn member_send_stalled_seconds_total(&self) -> u64 {
        self.collector
            .member_send_stalled_seconds_total
            .load(Ordering::Relaxed)
    }

    /// Shared-slot membership liveness: `Some(1)` while attached, `Some(0)` once
    /// detached (ack floor frozen, WAL pinned for the whole slot — #11644), or
    /// `None` for a dedicated (non-shared) slot, which has no member-detach
    /// concept and reports no series. Callers observe only on `Some` so an absent
    /// series means "not applicable", never a misleading `0`.
    #[must_use]
    pub fn member_attached(&self) -> Option<u64> {
        // Acquire pairs with the Release store in `mark_member_{attached,detached}`
        // so the value load never observes a stale default once `known` is true.
        if self.collector.member_attached_known.load(Ordering::Acquire) {
            Some(self.collector.member_attached.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    #[must_use]
    pub fn member_send_wait_micros_total(&self) -> u64 {
        self.collector
            .member_send_wait_micros_total
            .load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn member_envelopes_delivered_total(&self) -> u64 {
        self.collector
            .member_envelopes_delivered_total
            .load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn member_envelope_eager_merges_total(&self) -> u64 {
        self.collector
            .member_envelope_eager_merges_total
            .load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn member_envelope_mailbox_merges_total(&self) -> u64 {
        self.collector
            .member_envelope_mailbox_merges_total
            .load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn member_mailbox_coalesce_limited_total(&self) -> u64 {
        self.collector
            .member_mailbox_coalesce_limited_total
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
        // Unknown until an expected total is set: both the estimate and the progress
        // percent read as `None` (never a misleading `0`).
        assert_eq!(m.bootstrap_rows_expected(), None);
        assert_eq!(m.bootstrap_progress_percent(), None);

        c.set_bootstrap_rows_expected(200);
        c.add_bootstrap_rows(50);
        assert_eq!(m.bootstrap_rows_expected(), Some(200));
        assert_eq!(m.bootstrap_progress_percent(), Some(25));

        // The estimate can be exceeded; progress clamps at 100.
        c.add_bootstrap_rows(1000);
        assert_eq!(m.bootstrap_progress_percent(), Some(100));
    }

    #[test]
    fn bootstrap_empty_table_is_distinct_from_unknown() {
        // A known-empty source table (expected `0`) is a complete bootstrap, not
        // "unknown" — `0` must not be conflated with an absent estimate.
        let c = MetricsCollector::new();
        let m = Metrics::new(Arc::clone(&c));
        c.set_bootstrap_rows_expected(0);
        assert_eq!(m.bootstrap_rows_expected(), Some(0));
        assert_eq!(m.bootstrap_progress_percent(), Some(100));
    }

    #[test]
    fn member_attached_reports_only_for_shared_slot_members() {
        let c = MetricsCollector::new();
        let m = Metrics::new(Arc::clone(&c));
        // A dedicated (non-shared) slot never marks membership: no series, so an
        // absent value means "not applicable" — never a misleading `0`.
        assert_eq!(m.member_attached(), None);

        // A shared-slot member: attached on join, 0 on detach (ack floor frozen /
        // WAL pinned), back to 1 on rejoin.
        c.mark_member_attached();
        assert_eq!(m.member_attached(), Some(1));
        c.mark_member_detached();
        assert_eq!(m.member_attached(), Some(0));
        c.mark_member_attached();
        assert_eq!(m.member_attached(), Some(1));
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
    fn schema_evolution_counters_increment() {
        let c = MetricsCollector::new();
        c.inc_schema_evolution();
        c.inc_schema_evolution();
        c.inc_schema_evolution_rejected();
        let m = Metrics::new(c);
        assert_eq!(m.schema_evolutions_total(), 2);
        assert_eq!(m.schema_evolution_rejections_total(), 1);
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
