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

use std::sync::atomic::{AtomicU32, Ordering};

/// The yield of a reclaim that removed a row for every tombstone, in rows per
/// thousand tombstones. Also the yield a table starts at, which keeps its
/// configured trigger until a reclaim says otherwise.
const FULL_YIELD: u32 = 1_000;

/// The most a low yield multiplies the configured trigger by.
pub(crate) const MAX_BACKOFF: u32 = 16;

/// The last measured yield of one table's deletion-index reclaims.
#[derive(Debug)]
pub(crate) struct DeletionIndexYield {
    rows_per_thousand: AtomicU32,
}

impl Default for DeletionIndexYield {
    fn default() -> Self {
        Self {
            rows_per_thousand: AtomicU32::new(FULL_YIELD),
        }
    }
}

impl DeletionIndexYield {
    /// Record a reclaim that removed `rows_removed` rows and took
    /// `tombstones_reclaimed` tombstones out of the index. A reclaim that took none
    /// out measured nothing and leaves the yield as it was.
    ///
    /// A tombstone can hide more than one row, so the yield is capped at a row per
    /// tombstone: more than that cannot make a reclaim more urgent than the
    /// configured trigger already does.
    pub(crate) fn record(&self, rows_removed: u64, tombstones_reclaimed: u64) {
        if tombstones_reclaimed == 0 {
            return;
        }
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
}
