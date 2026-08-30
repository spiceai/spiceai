/*
Copyright 2025-2026 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Post-write maintenance scheduling state and protected-snapshot maintenance
//! triggers.
//!
//! Holds the coalescing [`PostWriteMaintenance`] debounce state plus the
//! count/age-based [`SnapshotMaintenanceTrigger`] evaluation over the protected
//! snapshot set (with bounded, de-duplicated warning emission via
//! [`BoundedFifoSet`]). The provider drives these from its write path.

use super::column_stats::ColumnStatsAccumulator;
use crate::bounded_fifo::BoundedFifoSet;
use parking_lot::Mutex as ParkingMutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Cap on the number of distinct protected-snapshot warning keys retained, to
/// bound the dedup set used by age-based maintenance warnings.
pub(crate) const PROTECTED_SNAPSHOT_AGE_WARNING_KEY_LIMIT: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotMaintenanceTrigger {
    ProtectedSnapshotCount {
        protected_snapshot_count: usize,
        trigger_count: usize,
    },
    ProtectedSnapshotAge {
        protected_snapshot_count: usize,
        oldest_snapshot_age: Duration,
        trigger_age: Duration,
    },
    SmallFileCount {
        number_picker_candidate_files: usize,
        compaction_trigger_files: usize,
    },
}

fn should_warn_protected_snapshot_age(
    warning_keys: &ParkingMutex<BoundedFifoSet>,
    snapshot_id: &str,
    warning_kind: &'static str,
) -> bool {
    warning_keys
        .lock()
        .insert_new(format!("{warning_kind}:{snapshot_id}"))
}

fn protected_snapshot_age(
    warning_keys: &ParkingMutex<BoundedFifoSet>,
    snapshot_id: &str,
    now: SystemTime,
) -> Option<Duration> {
    // Protected snapshot ids are generated as UUIDv7 values by
    // `commit_on_conflict_publish`; imported or future ids that
    // do not preserve that invariant are ignored for age-triggered maintenance
    // and still participate in count-triggered maintenance.
    let Ok(snapshot_uuid) = uuid::Uuid::parse_str(snapshot_id) else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "invalid_uuid") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot id is not a valid UUID; ignoring it for age-based maintenance"
            );
        }
        return None;
    };
    let Some(timestamp) = snapshot_uuid.get_timestamp() else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "missing_uuid_timestamp") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot id does not contain a UUID timestamp; ignoring it for age-based maintenance"
            );
        }
        return None;
    };
    let (seconds, nanos) = timestamp.to_unix();
    let Some(snapshot_time) = UNIX_EPOCH.checked_add(Duration::new(seconds, nanos)) else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "timestamp_overflow") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot timestamp overflowed SystemTime; ignoring it for age-based maintenance"
            );
        }
        return None;
    };
    if let Ok(age) = now.duration_since(snapshot_time) {
        Some(age)
    } else {
        if should_warn_protected_snapshot_age(warning_keys, snapshot_id, "future_timestamp") {
            tracing::warn!(
                snapshot_id,
                "Cayenne protected snapshot timestamp is in the future; ignoring it for age-based maintenance"
            );
        }
        None
    }
}

fn oldest_protected_snapshot_age(
    warning_keys: &ParkingMutex<BoundedFifoSet>,
    protected_snapshots: &HashMap<String, i64>,
    now: SystemTime,
) -> Option<Duration> {
    protected_snapshots
        .keys()
        .filter_map(|snapshot_id| protected_snapshot_age(warning_keys, snapshot_id, now))
        .max()
}

pub(crate) fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn protected_snapshot_maintenance_trigger(
    warning_keys: &ParkingMutex<BoundedFifoSet>,
    protected_snapshots: &HashMap<String, i64>,
    trigger_count: usize,
    trigger_age: Option<Duration>,
    now: SystemTime,
) -> Option<SnapshotMaintenanceTrigger> {
    let protected_snapshot_count = protected_snapshots.len();
    let trigger_count = trigger_count.max(1);
    if protected_snapshot_count >= trigger_count {
        return Some(SnapshotMaintenanceTrigger::ProtectedSnapshotCount {
            protected_snapshot_count,
            trigger_count,
        });
    }

    // Age parsing only runs below the count trigger; above it, the cheaper
    // count trigger short-circuits before scanning snapshot ids.
    let trigger_age = trigger_age?;
    let oldest_snapshot_age =
        oldest_protected_snapshot_age(warning_keys, protected_snapshots, now)?;
    if oldest_snapshot_age >= trigger_age {
        Some(SnapshotMaintenanceTrigger::ProtectedSnapshotAge {
            protected_snapshot_count,
            oldest_snapshot_age,
            trigger_age,
        })
    } else {
        None
    }
}

#[derive(Default)]
pub(crate) struct PostWriteMaintenanceState {
    pub(crate) stats: Option<Arc<ColumnStatsAccumulator>>,
    pub(crate) refresh_listing: bool,
    /// Set when the writer wants retention filters applied. Coalesces — multiple
    /// writes scheduling retention collapse to one scan per debounce window.
    pub(crate) retention_requested: bool,
    /// Net change to the live row count across the coalesced writes
    /// (`inserted - superseded - deleted`). Accumulated alongside `stats` and
    /// applied as a [`RowCountUpdate::Delta`] when the stats are persisted.
    pub(crate) live_rows_delta: i64,
    /// How many separate deltas were folded into `live_rows_delta`, carried
    /// with the state so a drain can retire exactly the ones it persisted from
    /// [`PostWriteMaintenance::outstanding_live_rows_deltas`]. Zero when no
    /// delta has been queued since the last drain.
    pub(crate) live_rows_delta_count: u64,
    /// Set when a write wants the metastore WAL drained but has no other
    /// maintenance to contribute. Every pass ends in a WAL checkpoint, so this
    /// only has to keep the pass from being skipped as empty. Coalesces with the
    /// rest of the debounce window: a burst of writes drains the WAL once.
    pub(crate) wal_checkpoint_requested: bool,
}

impl PostWriteMaintenanceState {
    pub(crate) fn is_empty(&self) -> bool {
        self.stats.is_none()
            && !self.refresh_listing
            && !self.retention_requested
            && self.live_rows_delta == 0
            && !self.wal_checkpoint_requested
    }
}

/// What one retention pass did.
///
/// `Deferred` is not a failure and not an empty delete: the pass declined to run because
/// materializing the inline corpus while a staged inline-conflict tombstone, a mem-tier
/// seal shadow, or an append finalization is in flight would resurrect a superseded row
/// or serve a live one twice. Keeping it distinct from `Deleted(0)` is what lets each
/// caller pick its own retry — the background loop has a debounce to retry after, the
/// synchronous drain does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetentionPass {
    Deleted(u64),
    /// Deferred behind something that publishes within milliseconds — an unpublished
    /// inline-conflict tombstone or a staged append mid-finalize. Worth re-arming: the
    /// next debounce almost certainly finds the window closed.
    DeferredTransient,
    /// Deferred behind a mem-tier seal shadow, which is NOT transient: it stands until
    /// the next full checkpoint — 10s by default, and indefinitely when the age trigger
    /// is off and the size gate is unmet. Re-arming would poll at the debounce interval
    /// for that whole span, dragging `run_maintenance_state`'s metastore vacuum and WAL
    /// checkpoint tail along on every pass. The checkpoint that clears a shadow arms
    /// retention itself (see `checkpoint_mem_tier_inner`), so waiting for it costs one
    /// checkpoint of latency and no polling.
    DeferredUntilCheckpoint,
}

pub(crate) enum RetentionFailureAction {
    Requeue,
    ReturnError,
}

#[derive(Default)]
pub(crate) struct PostWriteMaintenance {
    pub(crate) state: ParkingMutex<PostWriteMaintenanceState>,
    pub(crate) scheduled: AtomicBool,
    /// How many queued live-row deltas the persisted `num_rows` does not
    /// include yet.
    pub(crate) outstanding_live_rows_deltas: AtomicU64,
}

impl PostWriteMaintenance {
    /// Whether a live-row delta has been queued that the persisted `num_rows`
    /// does not yet include.
    ///
    /// The rows themselves are visible to scans the moment the write commits;
    /// the count that describes them lands later, on the maintenance task. Any
    /// reader deciding whether the maintained count is a *provably exact* live
    /// count has to treat that gap as drift, because the in-memory proxies for
    /// it (resident inline rows, mem-tier tombstones) are cleared by a
    /// checkpoint that does not drain this queue.
    ///
    /// Errs toward reporting drift. A compaction that re-baselines the count
    /// from the corpus already covers a delta queued before it, but does not
    /// drain the queue, so this keeps reporting until the maintenance pass
    /// applies that delta — briefly conservative, and the safe direction.
    pub(crate) fn has_unapplied_live_rows_delta(&self) -> bool {
        self.outstanding_live_rows_deltas.load(Ordering::Acquire) > 0
    }

    /// Count one delta being folded into the queued state.
    ///
    /// Call with [`Self::state`] held, so this and the `live_rows_delta` it
    /// accounts for become visible to a drain together.
    pub(crate) fn record_queued_live_rows_delta(&self) {
        self.outstanding_live_rows_deltas
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Retire the `count` deltas a persist folded into `num_rows`.
    ///
    /// A count rather than a high-water mark, so a drain can only ever retire
    /// what it actually persisted. A drain whose persist abandoned its update
    /// retires nothing, and its deltas stay outstanding — otherwise a later
    /// drain's success would declare the count exact over a gap it never
    /// filled, which is the same `Exact`-and-short answer this gate exists to
    /// refuse. Being a count also makes it order-independent: two drains that
    /// finish out of order retire the same total either way.
    pub(crate) fn retire_applied_live_rows_deltas(&self, count: u64) {
        if count == 0 {
            return;
        }
        // `saturating_sub` for the arithmetic alone: every retirement pairs
        // with recorded deltas, but wrapping here would read as a permanently
        // outstanding queue and strand the table on `Inexact`.
        let _ = self.outstanding_live_rows_deltas.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |outstanding| Some(outstanding.saturating_sub(count)),
        );
    }
}

#[cfg(test)]
mod post_write_maintenance_tests {
    use super::PostWriteMaintenance;

    /// A table that has never queued a delta has nothing outstanding, so its
    /// maintained count starts eligible to be served `Exact`.
    #[test]
    fn a_fresh_queue_has_nothing_outstanding() {
        let maintenance = PostWriteMaintenance::default();
        assert!(!maintenance.has_unapplied_live_rows_delta());
    }

    /// The signal spans the whole window between a commit queueing its delta
    /// and the persist that folds it into `num_rows`.
    #[test]
    fn a_queued_delta_stays_outstanding_until_it_is_applied() {
        let maintenance = PostWriteMaintenance::default();

        maintenance.record_queued_live_rows_delta();
        assert!(maintenance.has_unapplied_live_rows_delta());

        maintenance.retire_applied_live_rows_deltas(1);
        assert!(!maintenance.has_unapplied_live_rows_delta());
    }

    /// Maintenance coalesces: one drain persists every delta queued since the
    /// last one, and retiring that many clears them all.
    #[test]
    fn one_drain_retires_every_delta_it_coalesced() {
        let maintenance = PostWriteMaintenance::default();

        maintenance.record_queued_live_rows_delta();
        maintenance.record_queued_live_rows_delta();
        assert!(maintenance.has_unapplied_live_rows_delta());

        maintenance.retire_applied_live_rows_deltas(2);
        assert!(!maintenance.has_unapplied_live_rows_delta());
    }

    /// A drain whose persist abandoned its update retires nothing. A later
    /// drain that succeeds must not clear the gap that left, or the count is
    /// served `Exact` while short by the abandoned delta — the very answer
    /// this gate refuses.
    #[test]
    fn a_later_success_does_not_cover_an_abandoned_delta() {
        let maintenance = PostWriteMaintenance::default();

        maintenance.record_queued_live_rows_delta();
        // Its drain's persist failed, so nothing is retired for it.
        maintenance.record_queued_live_rows_delta();
        maintenance.retire_applied_live_rows_deltas(1);

        assert!(maintenance.has_unapplied_live_rows_delta());
    }

    /// Two drains can finish out of order. Retiring a count rather than a
    /// high-water mark makes the total the same either way, so neither
    /// re-opens a window the other closed.
    #[test]
    fn out_of_order_drains_retire_the_same_total() {
        let maintenance = PostWriteMaintenance::default();

        maintenance.record_queued_live_rows_delta();
        maintenance.record_queued_live_rows_delta();

        maintenance.retire_applied_live_rows_deltas(1);
        assert!(maintenance.has_unapplied_live_rows_delta());
        maintenance.retire_applied_live_rows_deltas(1);

        assert!(!maintenance.has_unapplied_live_rows_delta());
    }

    /// A write that lands while a drain is in flight is not covered by it, so
    /// the signal re-arms rather than staying clear.
    #[test]
    fn a_write_after_a_drain_re_arms_the_signal() {
        let maintenance = PostWriteMaintenance::default();

        maintenance.record_queued_live_rows_delta();
        maintenance.retire_applied_live_rows_deltas(1);

        maintenance.record_queued_live_rows_delta();
        assert!(maintenance.has_unapplied_live_rows_delta());
    }

    /// Retiring more than is outstanding must not wrap: a wrapped counter
    /// reads as a permanently outstanding queue and strands the table on
    /// `Inexact` for the life of the process.
    #[test]
    fn retiring_more_than_is_outstanding_does_not_wrap() {
        let maintenance = PostWriteMaintenance::default();

        maintenance.record_queued_live_rows_delta();
        maintenance.retire_applied_live_rows_deltas(5);

        assert!(!maintenance.has_unapplied_live_rows_delta());
    }
}
