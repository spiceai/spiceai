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

//! How a primary-key table records the delete of a row: fixed by
//! `deletion_mode: key` / `position`, or chosen by `auto` as the table grows.
//!
//! The two modes reclaim their deletion state differently, and that is what the
//! choice is about. A position-mode table reclaims by rewriting its whole current
//! snapshot, folding in every protected snapshot and every tombstone, which leaves
//! one compact snapshot, an empty deletion index and a bloom keyset. That is the
//! cheapest state to scan and to hold in memory, and the rewrite costs time in
//! proportion to the table. A key-mode table reclaims with the seq-prefix bake,
//! which rewrites only the settled prefix of its protected snapshots — cheap at any
//! size, but it leaves the table in several snapshots behind a deletion filter.
//!
//! `auto` therefore keeps a table in position mode while it is small enough that
//! rewriting it whole reads faster than the bake
//! ([`AutoDeletionMode::max_position_bytes_for`]), and moves it to key mode once it
//! is not.
//!
//! A table whose rows sit in its current snapshot — which is where every
//! whole-table rewrite leaves them — needs that rewrite whatever its mode: the bake
//! folds only protected snapshots, so it cannot apply a key delete to a row in the
//! current snapshot, and its clean-prefix check then keeps it from pruning any
//! delete at or below that row's sequence. So an `auto` table on key deletes whose
//! current snapshot holds rows reclaims by rewriting itself whole too, paced to a
//! small share of the time ([`RECLAIM_SHARE_DENOMINATOR`]) because such a table can
//! be any size; one whose rows are all in protected snapshots leaves them to the
//! bake.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::metadata::{DeletionMode, ResolvedDeletionMode};

/// An `auto` table on key deletes takes at most this fraction (one tenth) of the
/// time rewriting itself whole: such a reclaim waits, the deletion index growing
/// past its trigger meanwhile, until 9 times the recent whole-table reclaims'
/// duration has passed since the previous one ended. Over the deletion index's
/// memory ceiling it does not wait.
///
/// The pace bounds what a table of any size can spend on whole-table rewrites
/// when the bake cannot reach its rows. It costs reads in the meantime, because
/// the index grows past the trigger: a 1M-row table under 8,000 upserts a second
/// read at a 10 ms P99 rewriting at every trigger, with a rewrite running 4.3% of
/// the time, and at 16–19 ms paced to a fiftieth. A tenth leaves such a table
/// unpaced and still bounds a large one. A position-delete table, which `auto`
/// keeps small, is not paced.
pub(crate) const RECLAIM_SHARE_DENOMINATOR: u32 = 10;

/// How a primary-key table records the delete of a row right now.
#[derive(Clone, Debug)]
pub(crate) enum PkDeletionMode {
    /// Key deletes: `deletion_mode: key`, or `auto` on a table it does not adapt.
    Key,
    /// Position deletes, configured explicitly with `deletion_mode: position`.
    Position,
    /// `deletion_mode: auto` choosing between the two as the table grows.
    Auto(Arc<AutoDeletionMode>),
}

impl PkDeletionMode {
    /// Resolve the configured `mode` for one table.
    ///
    /// `adapts` says whether `auto` may choose position deletes for this table at
    /// all: it may not for a table whose rows live in RAM (`mode: memory`), a
    /// partitioned table, a table with a datalake tier, or one on object storage,
    /// which keep key deletes. An `auto` table starts on position deletes until it
    /// is sized ([`AutoDeletionMode::size_at_open`]).
    pub(crate) fn for_table(
        mode: DeletionMode,
        has_primary_key: bool,
        adapts: bool,
        target_file_size_bytes: usize,
    ) -> Self {
        match mode.resolved(has_primary_key) {
            ResolvedDeletionMode::Key => Self::Key,
            ResolvedDeletionMode::Position => Self::Position,
            ResolvedDeletionMode::Adaptive if !adapts => Self::Key,
            ResolvedDeletionMode::Adaptive => Self::Auto(Arc::new(AutoDeletionMode::new(
                AutoDeletionMode::max_position_bytes_for(target_file_size_bytes),
            ))),
        }
    }

    /// Whether the table records position deletes where a row's position is
    /// known (and reclaims by rewriting itself), rather than key deletes only.
    pub(crate) fn uses_positions(&self) -> bool {
        match self {
            Self::Key => false,
            Self::Position => true,
            Self::Auto(auto) => auto.uses_positions(),
        }
    }

    /// The `auto` state, when this table's mode is `auto`.
    pub(crate) fn auto(&self) -> Option<&Arc<AutoDeletionMode>> {
        match self {
            Self::Auto(auto) => Some(auto),
            Self::Key | Self::Position => None,
        }
    }
}

/// `deletion_mode: auto` on a primary-key table: position deletes while the table
/// is small enough to rewrite whole, key deletes once it is not.
///
/// While on position deletes, every rewrite of the table — the whole-table
/// reclaim and the protected-snapshot merges — runs alongside writers, as on a
/// key-delete table (see `CayenneTableProvider::position_rewrites_exclude_writers`).
///
/// The switch goes one way. A table that outgrows the bound stays on key deletes
/// for the life of this provider; the next open sizes it again.
#[derive(Debug)]
pub(crate) struct AutoDeletionMode {
    /// Whether the table still records position deletes.
    position: AtomicBool,
    /// Set by a check that found the table past its bound where it cannot take
    /// `compaction_lock`. The next compaction trigger performs the switch.
    switch_requested: AtomicBool,
    /// See [`Self::max_position_bytes`].
    max_position_bytes: u64,
    /// See [`Self::reclaim_due`].
    reclaims: Mutex<ReclaimPace>,
    /// Whether a whole-table reclaim has committed while the table was on
    /// position deletes (see [`Self::was_kept_compacted`]).
    kept_compacted: AtomicBool,
}

#[derive(Debug, Default)]
struct ReclaimPace {
    /// When the previous whole-table reclaim ended; `None` before the first.
    last_end: Option<Instant>,
    /// How long a whole-table reclaim takes: the mean of the last one and the
    /// estimate before it, so one slow pass does not stall the next for long.
    busy: Duration,
}

/// The largest bound `auto` keeps position deletes within, whatever the target file
/// size: the size up to which they were measured to read faster (see
/// [`AutoDeletionMode::max_position_bytes_for`]).
pub(crate) const MAX_POSITION_BYTES: u64 = 8 * 1024 * 1024;

impl AutoDeletionMode {
    /// A table at most `max_position_bytes` on-disk bytes keeps position deletes.
    /// It starts on them; [`Self::size_at_open`] moves it to key deletes when the
    /// table is past the bound, or cannot be sized, when it is opened.
    pub(crate) fn new(max_position_bytes: u64) -> Self {
        Self {
            position: AtomicBool::new(true),
            switch_requested: AtomicBool::new(false),
            max_position_bytes,
            reclaims: Mutex::new(ReclaimPace::default()),
            kept_compacted: AtomicBool::new(false),
        }
    }

    /// Choose the starting mode for a table of `table_bytes` on-disk bytes (`None`
    /// when it could not be sized), before anything has written to it. Not a
    /// switch: nothing has recorded a position yet.
    pub(crate) fn size_at_open(&self, table_bytes: Option<u64>) {
        if !table_bytes.is_some_and(|bytes| self.fits(bytes)) {
            self.position.store(false, Ordering::Release);
        }
    }

    /// Record that a whole-table reclaim committed while the table was on
    /// position deletes.
    pub(crate) fn record_kept_compacted(&self) {
        self.kept_compacted.store(true, Ordering::Release);
    }

    /// Whether the table was kept fully compacted at some point: a whole-table
    /// reclaim committed while it was on position deletes. A table that outgrew
    /// the bound during its first load never was.
    pub(crate) fn was_kept_compacted(&self) -> bool {
        self.kept_compacted.load(Ordering::Acquire)
    }

    /// The bound for a table whose target file size is `target_file_size_bytes`:
    /// 1/32 of it, and at most [`MAX_POSITION_BYTES`] (8 MiB, the default's 1/32).
    ///
    /// Under one writer upserting 8,000 keys a second, against key deletes the
    /// table's bake could serve, position deletes read faster with 1.8 MB, 4.8 MB
    /// and 7.8 MB of table data (P99 scans 5–12% and P99 point lookups 9–16%
    /// faster, 15–30% more reads, writes even), and slower at the P99 with 11.4 MB
    /// and 16 MB (scans 20–25%). The bound is measured against what a whole-table
    /// rewrite reads, which includes the upserts it is about to fold, so 1/32 of the
    /// default target keeps position deletes inside the range where they won; the
    /// target it derives from follows the storage the table is on.
    /// A larger target does not raise the bound past what was measured.
    pub(crate) fn max_position_bytes_for(target_file_size_bytes: usize) -> u64 {
        u64::try_from(target_file_size_bytes / 32)
            .unwrap_or(u64::MAX)
            .min(MAX_POSITION_BYTES)
    }

    /// Largest on-disk size, in bytes, at which the table keeps position deletes.
    pub(crate) fn max_position_bytes(&self) -> u64 {
        self.max_position_bytes
    }

    /// Whether the table records position deletes.
    pub(crate) fn uses_positions(&self) -> bool {
        self.position.load(Ordering::Acquire)
    }

    /// Whether a table of `table_bytes` on-disk bytes is within the bound.
    pub(crate) fn fits(&self, table_bytes: u64) -> bool {
        table_bytes <= self.max_position_bytes
    }

    /// Whether a whole-table reclaim may start at `now` with the table's reclaims
    /// still taking at most one [`RECLAIM_SHARE_DENOMINATOR`]th of the time: the
    /// first always may, and each later one once the time since the previous one
    /// ended is at least `RECLAIM_SHARE_DENOMINATOR - 1` times a reclaim's
    /// duration.
    pub(crate) fn reclaim_due(&self, now: Instant) -> bool {
        let pace = self.reclaims.lock();
        pace.last_end.is_none_or(|last_end| {
            now.saturating_duration_since(last_end)
                >= pace.busy.saturating_mul(RECLAIM_SHARE_DENOMINATOR - 1)
        })
    }

    /// Record a whole-table reclaim that took `busy` and ended at `ended`.
    pub(crate) fn record_reclaim(&self, busy: Duration, ended: Instant) {
        let mut pace = self.reclaims.lock();
        pace.busy = if pace.last_end.is_some() {
            pace.busy.saturating_add(busy) / 2
        } else {
            busy
        };
        pace.last_end = Some(ended);
    }

    /// Forget the pace, so the next reclaim is due at once: lets a test run
    /// reclaims back to back.
    #[cfg(test)]
    pub(crate) fn forget_reclaim_pace(&self) {
        *self.reclaims.lock() = ReclaimPace::default();
    }

    /// Ask the next compaction trigger to move the table to key deletes. Returns
    /// `true` when this call raised the request (the table was still on position
    /// deletes and no request was pending).
    pub(crate) fn request_switch_to_key(&self) -> bool {
        self.uses_positions() && !self.switch_requested.swap(true, Ordering::AcqRel)
    }

    /// Whether a switch to key deletes is pending.
    pub(crate) fn switch_requested(&self) -> bool {
        self.switch_requested.load(Ordering::Acquire)
    }

    /// Move the table to key deletes. Returns `true` when this call switched it.
    pub(crate) fn switch_to_key(&self) -> bool {
        self.switch_requested.store(false, Ordering::Release);
        self.position.swap(false, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: usize = 256 * 1024 * 1024;
    const BOUND: u64 = 8 * 1024 * 1024;

    /// An `auto` table opened at `table_bytes` (`None`: could not be sized).
    fn opened(table_bytes: Option<u64>) -> AutoDeletionMode {
        let auto = AutoDeletionMode::new(BOUND);
        auto.size_at_open(table_bytes);
        auto
    }

    #[test]
    fn the_bound_is_a_thirty_second_of_the_target_file_size_up_to_8_mib() {
        assert_eq!(AutoDeletionMode::max_position_bytes_for(TARGET), BOUND);
        assert_eq!(
            AutoDeletionMode::max_position_bytes_for(64 * 1024 * 1024),
            2 * 1024 * 1024
        );
        assert_eq!(AutoDeletionMode::max_position_bytes_for(0), 0);
        // A larger target, such as an object store's, does not raise it.
        assert_eq!(
            AutoDeletionMode::max_position_bytes_for(1024 * 1024 * 1024),
            BOUND
        );
    }

    #[test]
    fn a_table_starts_on_position_deletes_only_within_the_bound() {
        assert!(opened(Some(0)).uses_positions());
        assert!(opened(Some(BOUND)).uses_positions());
        assert!(!opened(Some(BOUND + 1)).uses_positions());
        assert!(
            !opened(None).uses_positions(),
            "an unknown size must start on key deletes"
        );
    }

    #[test]
    fn the_switch_to_key_is_one_way_and_reported_once() {
        let auto = opened(Some(1));
        assert!(auto.request_switch_to_key());
        assert!(
            !auto.request_switch_to_key(),
            "a pending request is not raised twice"
        );
        assert!(auto.switch_requested());
        assert!(auto.switch_to_key());
        assert!(!auto.uses_positions());
        assert!(!auto.switch_requested());
        assert!(!auto.switch_to_key(), "a second switch changes nothing");
        assert!(
            !auto.request_switch_to_key(),
            "a key-mode table has nothing to switch"
        );
    }

    #[test]
    fn a_table_is_kept_compacted_only_once_a_reclaim_recorded_it() {
        let auto = opened(Some(1));
        assert!(!auto.was_kept_compacted());
        auto.record_kept_compacted();
        assert!(auto.was_kept_compacted());
    }

    #[test]
    fn the_first_reclaim_is_due_at_once() {
        let auto = opened(Some(1));
        assert!(auto.reclaim_due(Instant::now()));
    }

    #[test]
    fn a_reclaim_waits_until_reclaims_take_at_most_a_tenth_of_the_time() {
        let auto = opened(Some(1));
        let ended = Instant::now();
        auto.record_reclaim(Duration::from_millis(100), ended);
        // 9 × 100 ms after it ended, and not a moment before.
        assert!(!auto.reclaim_due(ended + Duration::from_millis(899)));
        assert!(auto.reclaim_due(ended + Duration::from_millis(900)));
    }

    #[test]
    fn one_slow_reclaim_moves_the_pace_by_half() {
        let auto = opened(Some(1));
        let first = Instant::now();
        auto.record_reclaim(Duration::from_millis(100), first);
        let second = first + Duration::from_secs(10);
        // A 2.1 s pass after a 100 ms one: the estimate is their mean, 1.1 s.
        auto.record_reclaim(Duration::from_millis(2_100), second);
        assert!(!auto.reclaim_due(second + Duration::from_millis(9_899)));
        assert!(auto.reclaim_due(second + Duration::from_millis(9_900)));
    }

    #[test]
    fn fits_is_inclusive_of_the_bound() {
        let auto = opened(Some(0));
        assert!(auto.fits(BOUND));
        assert!(!auto.fits(BOUND + 1));
    }

    #[test]
    fn for_table_resolves_each_configuration() {
        let resolve = |mode, has_pk, adapts| {
            PkDeletionMode::for_table(mode, has_pk, adapts, TARGET).uses_positions()
        };
        // Explicit modes ignore eligibility.
        assert!(!resolve(DeletionMode::Key, true, true));
        assert!(resolve(DeletionMode::Position, true, true));
        // `auto` on an eligible primary-key table starts on position deletes, until
        // it is sized.
        assert!(resolve(DeletionMode::Auto, true, true));
        // `auto` on a table it does not adapt keeps key deletes.
        assert!(!resolve(DeletionMode::Auto, true, false));
        assert!(matches!(
            PkDeletionMode::for_table(DeletionMode::Auto, true, false, TARGET),
            PkDeletionMode::Key
        ));
        let PkDeletionMode::Auto(auto) =
            PkDeletionMode::for_table(DeletionMode::Auto, true, true, TARGET)
        else {
            panic!("an eligible primary-key table is auto");
        };
        assert_eq!(auto.max_position_bytes(), BOUND);
    }
}
