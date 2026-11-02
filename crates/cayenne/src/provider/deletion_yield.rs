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

//! How many rows a key tombstone hides, as the last reclaim of the deletion
//! index measured it.
//!
//! The bake and the whole-table reclaim are triggered by the deletion index's
//! size, and each rewrites data to fold the tombstones in. That is worth its cost
//! only while the tombstones hide rows. An upsert whose existence filter answered
//! "maybe" for a key that was never stored records a tombstone that hides
//! nothing, and on a table that only appends, those are the only tombstones it
//! has: the index fills with them at the rate the filter errs, and every reclaim
//! re-encodes data to remove none of its rows.
//!
//! So the trigger is read as rows hidden rather than tombstones held. Each
//! reclaim measures the rows its deletion filters removed against the tombstones
//! it took out of the index, and the next trigger is scaled by that yield: a table
//! whose reclaims remove a row per tombstone triggers as before, and one whose
//! reclaims remove nothing waits for up to [`MAX_BACKOFF`] times as many
//! tombstones. The bound keeps the index from growing without limit, and every
//! reclaim measures again, so a table that starts deleting for real is back on
//! its configured trigger after one reclaim.
//!
//! A reclaim is not the only rewrite that removes the rows tombstones hide. A
//! subset merge of protected snapshots, and a small-file rewrite of the current
//! snapshot, read their inputs through the same deletion filters and leave the
//! superseded rows out of their output, but they keep the tombstones: a
//! tombstone may still hide a row in a file they did not rewrite. Under steady
//! upserts those merges remove nearly every row before the reclaim that takes
//! the tombstones out of the index reaches them, and a reclaim that counted only
//! its own rows would read real upserts as tombstones that hide nothing. So the
//! rows such a rewrite removes are carried to the next reclaim and counted as
//! that reclaim's yield.
//!
//! The carried count lives in memory, so it misses the rows removed before the
//! process started. A reclaim that takes out a tombstone loaded at open could
//! read its yield low for that reason alone, so until the index no longer holds
//! a tombstone at or below the highest sequence loaded at open, a reclaim
//! records no yield and the one before it stands. Tombstones recorded after open
//! always carry higher sequences, so once none at or below it remain, none
//! return.

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};

/// The yield of a reclaim that removed a row for every tombstone, in rows per
/// thousand tombstones. Also the yield a table starts at, which keeps its
/// configured trigger until a reclaim says otherwise.
const FULL_YIELD: u32 = 1_000;

/// The most a low yield multiplies the configured trigger by.
pub(crate) const MAX_BACKOFF: u32 = 16;

/// Marks that no tombstone loaded at open can still be held.
const NO_TOMBSTONES_FROM_OPEN: i64 = i64::MIN;

/// The last measured yield of one table's deletion-index reclaims.
#[derive(Debug)]
pub(crate) struct DeletionIndexYield {
    rows_per_thousand: AtomicU32,
    /// Rows rewrites that keep their tombstones removed since the last reclaim
    /// that took tombstones out of the index.
    rows_removed_before_reclaim: AtomicU64,
    /// The highest delete sequence the index held when the table opened, while
    /// a tombstone at or below it may still be held; otherwise
    /// [`NO_TOMBSTONES_FROM_OPEN`].
    tombstones_from_open_through: AtomicI64,
}

impl Default for DeletionIndexYield {
    fn default() -> Self {
        Self {
            rows_per_thousand: AtomicU32::new(FULL_YIELD),
            rows_removed_before_reclaim: AtomicU64::new(0),
            tombstones_from_open_through: AtomicI64::new(NO_TOMBSTONES_FROM_OPEN),
        }
    }
}

impl DeletionIndexYield {
    /// Record that the index loaded at open held tombstones up to delete sequence
    /// `max_delete_seq`, whose earlier removals this process never counted.
    pub(crate) fn record_tombstones_from_open(&self, max_delete_seq: Option<i64>) {
        self.tombstones_from_open_through.store(
            max_delete_seq.unwrap_or(NO_TOMBSTONES_FROM_OPEN),
            Ordering::Relaxed,
        );
    }

    /// The highest delete sequence of the tombstones loaded at open, while one of
    /// them may still be held.
    #[must_use]
    pub(crate) fn tombstones_from_open_through(&self) -> Option<i64> {
        let through = self.tombstones_from_open_through.load(Ordering::Relaxed);
        (through != NO_TOMBSTONES_FROM_OPEN).then_some(through)
    }

    /// Record that the index no longer holds a tombstone loaded at open.
    pub(crate) fn forget_tombstones_from_open(&self) {
        self.tombstones_from_open_through
            .store(NO_TOMBSTONES_FROM_OPEN, Ordering::Relaxed);
    }

    /// Record a committed rewrite whose deletion filters removed `rows` rows and
    /// which left their tombstones in the index: the next reclaim counts them.
    pub(crate) fn record_rows_removed_before_reclaim(&self, rows: u64) {
        self.rows_removed_before_reclaim
            .fetch_add(rows, Ordering::Relaxed);
    }

    /// Record a reclaim that removed `rows_removed` rows and took
    /// `tombstones_reclaimed` tombstones out of the index. Its yield also counts the
    /// rows rewrites removed since the last reclaim (see
    /// [`Self::record_rows_removed_before_reclaim`]). A reclaim that took none out
    /// measured nothing: the yield stays as it was, and the rows removed before it
    /// carry to the next.
    ///
    /// A tombstone can hide more than one row, so the yield is capped at a row per
    /// tombstone: more than that cannot make a reclaim more urgent than the
    /// configured trigger already does. The cap also bounds the error of carrying
    /// rows whose tombstones this reclaim left in the index.
    pub(crate) fn record(&self, rows_removed: u64, tombstones_reclaimed: u64) {
        if tombstones_reclaimed == 0 {
            return;
        }
        let rows_removed = rows_removed
            .saturating_add(self.rows_removed_before_reclaim.swap(0, Ordering::Relaxed));
        let per_thousand = rows_removed
            .saturating_mul(u64::from(FULL_YIELD))
            .checked_div(tombstones_reclaimed)
            .unwrap_or(u64::MAX)
            .min(u64::from(FULL_YIELD));
        self.rows_per_thousand.store(
            u32::try_from(per_thousand).unwrap_or(FULL_YIELD),
            Ordering::Relaxed,
        );
    }

    /// Record a reclaim that took `tombstones_reclaimed` tombstones out of the
    /// index but could not count the rows they hid. The yield stays as it was; the
    /// rows removed before it belonged to the tombstones it took out, so they are
    /// not carried to the next reclaim.
    pub(crate) fn record_unmeasured(&self, tombstones_reclaimed: u64) {
        if tombstones_reclaimed > 0 {
            self.rows_removed_before_reclaim.store(0, Ordering::Relaxed);
        }
    }

    /// Rows removed per thousand tombstones by the last reclaim that measured any.
    #[must_use]
    pub(crate) fn rows_per_thousand(&self) -> u32 {
        self.rows_per_thousand.load(Ordering::Relaxed)
    }

    /// The deletion-index size at which `trigger` rows' worth of tombstones are
    /// held: `trigger` at full yield, up to [`MAX_BACKOFF`] times it as the yield
    /// falls.
    #[must_use]
    pub(crate) fn scale_trigger(&self, trigger: usize) -> usize {
        let trigger = u64::try_from(trigger).unwrap_or(u64::MAX);
        let bound = trigger.saturating_mul(u64::from(MAX_BACKOFF));
        let scaled = trigger
            .saturating_mul(u64::from(FULL_YIELD))
            .checked_div(u64::from(self.rows_per_thousand()))
            .unwrap_or(bound)
            .min(bound);
        usize::try_from(scaled).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::{DeletionIndexYield, MAX_BACKOFF};

    const TRIGGER: usize = 50_000;

    #[test]
    fn a_new_table_keeps_its_configured_trigger() {
        assert_eq!(
            DeletionIndexYield::default().scale_trigger(TRIGGER),
            TRIGGER
        );
    }

    #[test]
    fn reclaims_that_remove_a_row_per_tombstone_keep_the_trigger() {
        let yield_ = DeletionIndexYield::default();
        yield_.record(50_000, 50_000);
        assert_eq!(yield_.scale_trigger(TRIGGER), TRIGGER);
        // A tombstone hiding several superseded versions is still full yield.
        yield_.record(150_000, 50_000);
        assert_eq!(yield_.scale_trigger(TRIGGER), TRIGGER);
    }

    #[test]
    fn reclaims_that_remove_nothing_back_off_to_the_bound() {
        let yield_ = DeletionIndexYield::default();
        yield_.record(0, 50_000);
        assert_eq!(
            yield_.scale_trigger(TRIGGER),
            TRIGGER * usize::try_from(MAX_BACKOFF).expect("small constant")
        );
    }

    #[test]
    fn a_partial_yield_scales_the_trigger_inversely() {
        let yield_ = DeletionIndexYield::default();
        yield_.record(25_000, 50_000);
        assert_eq!(yield_.scale_trigger(TRIGGER), 2 * TRIGGER);
    }

    #[test]
    fn real_deletes_restore_the_trigger_after_one_reclaim() {
        let yield_ = DeletionIndexYield::default();
        yield_.record(0, 50_000);
        yield_.record(50_000, 50_000);
        assert_eq!(yield_.scale_trigger(TRIGGER), TRIGGER);
    }

    #[test]
    fn a_reclaim_that_took_no_tombstones_measures_nothing() {
        let yield_ = DeletionIndexYield::default();
        yield_.record(0, 50_000);
        yield_.record(10, 0);
        assert_eq!(yield_.rows_per_thousand(), 0);
    }

    #[test]
    fn rows_merges_removed_before_a_reclaim_count_toward_its_yield() {
        let yield_ = DeletionIndexYield::default();
        yield_.record_rows_removed_before_reclaim(288_000);
        yield_.record_rows_removed_before_reclaim(15_000);
        yield_.record(1_500, 304_877);
        assert_eq!(yield_.rows_per_thousand(), 998);
        assert_eq!(yield_.scale_trigger(TRIGGER), 50_100);
        // Counted once: the next reclaim starts from its own rows.
        yield_.record(0, 50_000);
        assert_eq!(yield_.rows_per_thousand(), 0);
    }

    #[test]
    fn rows_removed_before_a_reclaim_that_took_no_tombstones_carry_to_the_next() {
        let yield_ = DeletionIndexYield::default();
        yield_.record_rows_removed_before_reclaim(25_000);
        yield_.record(0, 0);
        yield_.record(0, 50_000);
        assert_eq!(yield_.scale_trigger(TRIGGER), 2 * TRIGGER);
    }

    #[test]
    fn an_unmeasured_reclaim_keeps_the_yield_and_drops_the_carried_rows() {
        let yield_ = DeletionIndexYield::default();
        yield_.record(25_000, 50_000);
        yield_.record_rows_removed_before_reclaim(50_000);
        yield_.record_unmeasured(50_000);
        assert_eq!(yield_.scale_trigger(TRIGGER), 2 * TRIGGER);
        yield_.record(0, 50_000);
        assert_eq!(yield_.rows_per_thousand(), 0);
    }

    #[test]
    fn tombstones_from_open_are_tracked_until_forgotten() {
        let yield_ = DeletionIndexYield::default();
        assert_eq!(yield_.tombstones_from_open_through(), None);
        yield_.record_tombstones_from_open(None);
        assert_eq!(yield_.tombstones_from_open_through(), None);
        yield_.record_tombstones_from_open(Some(42));
        assert_eq!(yield_.tombstones_from_open_through(), Some(42));
        yield_.forget_tombstones_from_open();
        assert_eq!(yield_.tombstones_from_open_through(), None);
    }
}
