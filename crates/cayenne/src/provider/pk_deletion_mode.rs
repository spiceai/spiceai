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
//! current snapshot holds rows reclaims by rewriting itself whole too; one whose
//! rows are all in protected snapshots leaves them to the bake. Every whole-table
//! reclaim of an `auto` table, on either kind of delete, is paced to a small share
//! of the time ([`RECLAIM_SHARE_DENOMINATOR`]).
//!
//! The pace makes the size bound a rate bound too. A table small enough for
//! position deletes, written fast enough that its first whole-table rewrite takes
//! more than its share of the time, would be held back by the pace on every
//! rewrite after it, so that rewrite is discarded and the table keeps key deletes
//! ([`AutoDeletionMode::rewrite_keeps_key_deletes`]). Its rows are then all still
//! in protected snapshots, where the bake serves it as it serves an explicit
//! `key` table. A table whose rows already sit in its current snapshot keeps the
//! paced rewrite, which is all that reaches them.
//!
//! On a host with too few cores the same first rewrite keeps key deletes however
//! long it took (`CpuBudget::cayenne_keeps_small_tables_compacted`). In a Linux
//! virtual machine held to four CPUs, a 200K-row table under 2,000 upserts every
//! 250 ms rewrote itself in about 190 ms, a few percent of the time, so the pace
//! never held it back, yet on position deletes its P99 write was 4% above key
//! deletes' and its reads were mixed. Four performance cores of a bare-metal Arm
//! host showed no such loss, and on eight its writes were even and its reads
//! faster, but a small host is often a virtual machine.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::metadata::{DeletionMode, ResolvedDeletionMode};

/// An `auto` table takes at most this fraction (one tenth) of the time rewriting
/// itself whole, on position or key deletes, counted over each reclaim and the
/// wait after it. After each whole-table reclaim the next one waits, the deletion
/// index growing past its trigger meanwhile, until 9 times that reclaim's
/// duration has passed since it ended, so each reclaim and its wait are at most a
/// tenth rewriting however the durations change from one reclaim to the next. A
/// reclaim's duration is known only when it ends, so a window that ends just after
/// a long reclaim, before its wait, can show more than a tenth. Over the deletion
/// index's memory ceiling it does not wait.
///
/// On key deletes the pace bounds what a table of any size can spend on
/// whole-table rewrites when the bake cannot reach its rows. On position deletes
/// it bounds a small table under a high write rate on a small host. At 32,000
/// upserts a second on four CPUs, a 250K-row table rewrote itself every 1.8 s for
/// 236 ms, 13% of the time, and its write P99 was 16% above key deletes'. Paced to
/// a tenth, its write P99 was even with key deletes', but its read P99s ranged
/// from 9% faster to 11% slower, against 15–22% faster unpaced. Such a table now
/// keeps key deletes from the start ([`AutoDeletionMode::rewrite_keeps_key_deletes`]),
/// and the pace is left to tables whose rows its rewrite has already compacted.
/// The pace costs reads while it holds a reclaim back, because the index grows
/// past the trigger: a 1M-row table under 8,000 upserts a second read at a 10 ms
/// P99 rewriting at every trigger, with a rewrite running 4.3% of the time, and at
/// 16–19 ms paced to a fiftieth. A tenth leaves that table unpaced, as it does
/// every position-delete table measured at 8,000 upserts a second on 16 cores,
/// which rewrote for 44–145 ms every ~6.25 s.
pub(crate) const RECLAIM_SHARE_DENOMINATOR: u32 = 10;

/// The shortest whole-table rewrite that can move an `auto` table to key deletes
/// for its write rate ([`AutoDeletionMode::rewrite_keeps_key_deletes`]). A shorter
/// rewrite holds writes up for less than that whatever the rate, and over a few
/// milliseconds the share it is measured as would be noise. The rewrite that
/// crowded out writes — 250K rows at 32,000 upserts a second on four CPUs — took
/// 236 ms. The 200K-row table on 16 cores took 70 ms, against a 6.25 s interval.
pub(crate) const MIN_PACED_REWRITE: Duration = Duration::from_millis(100);

/// How `deletion_mode: auto`'s write-rate rule treats one table
/// ([`AutoDeletionMode::rewrite_keeps_key_deletes`]), pinned by a test whose
/// subject is one regime (`CayenneTableProvider::pin_auto_write_rate_rule`). The
/// rule times the table's first whole-table rewrite, and in a debug build that
/// time depends on the machine and its load. `#[doc(hidden)]`: not a stable API.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoWriteRateRule {
    /// The rule as `spiced` runs it: the rewrite's duration against the table's
    /// write rate.
    Measured,
    /// No rewrite outpaces the writes: the table keeps position deletes until it
    /// outgrows its bound.
    Never,
    /// Every first whole-table rewrite of rows all in protected snapshots
    /// outpaces the writes: it is discarded, and the table keeps key deletes.
    Always,
}

/// The state behind [`AutoWriteRateRule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteRateRule {
    /// A rewrite shorter than `floor` never counts ([`MIN_PACED_REWRITE`] outside
    /// this crate's unit tests).
    Measured {
        floor: Duration,
    },
    Never,
    Always,
}

/// Why a first whole-table rewrite of an `auto` table keeps it on key deletes
/// ([`AutoDeletionMode::rewrite_keeps_key_deletes`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyDeletesReason {
    /// The rewrite took more than a [`RECLAIM_SHARE_DENOMINATOR`]th of
    /// `interval`: the time since the table's previous whole-table reclaim ended
    /// or, before the first, since its first delete.
    WriteRate { interval: Duration },
    /// The host has `cores` CPU cores, too few to keep a table compacted whole
    /// alongside its writes (`CpuBudget::cayenne_keeps_small_tables_compacted`).
    Host { cores: usize },
}

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
    /// When a compaction pass first found a delete in the table's index since it
    /// was opened (see [`Self::rewrite_keeps_key_deletes`]).
    first_delete_at: Mutex<Option<Instant>>,
    /// See [`Self::rewrite_keeps_key_deletes`]. In this crate's unit tests the rule
    /// applies only where a test asks for it: their tables rewrite in times that
    /// depend on the machine and its load.
    write_rate_rule: Mutex<WriteRateRule>,
    /// The CPU cores of a host too small to keep a table compacted whole
    /// alongside its writes (`CpuBudget::cayenne_keeps_small_tables_compacted`),
    /// or `None` on a host that can. Every host can in this crate's unit tests,
    /// unless a test says otherwise.
    small_host_cores: Mutex<Option<usize>>,
}

#[derive(Debug, Default)]
struct ReclaimPace {
    /// When the previous whole-table reclaim ended; `None` before the first.
    last_end: Option<Instant>,
    /// How long the previous whole-table reclaim took.
    busy: Duration,
}

/// The largest bound `auto` keeps position deletes within, whatever the target file
/// size: the size up to which they were measured to read faster (see
/// [`AutoDeletionMode::max_position_bytes_for`]).
pub(crate) const MAX_POSITION_BYTES: u64 = 8 * 1024 * 1024;

/// The CPU cores of this host when they are too few to keep a table compacted
/// whole alongside its writes, as `deletion_mode: auto` does on a larger one.
fn small_host_cores() -> Option<usize> {
    let budget = cpu_budget::cpu_budget();
    (!budget.cayenne_keeps_small_tables_compacted()).then_some(budget.cores())
}

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
            first_delete_at: Mutex::new(None),
            write_rate_rule: Mutex::new(if cfg!(test) {
                WriteRateRule::Never
            } else {
                WriteRateRule::Measured {
                    floor: MIN_PACED_REWRITE,
                }
            }),
            small_host_cores: Mutex::new(if cfg!(test) { None } else { small_host_cores() }),
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

    /// Whether the table was kept fully compacted since this provider opened it: a
    /// whole-table reclaim committed while it was on position deletes. A table that
    /// outgrew the bound during its first load never was, and neither, until such a
    /// reclaim, was one an earlier load compacted: the metastore records no kind of
    /// commit to tell that from an overwrite.
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
    /// and 16 MB (scans 20–25%), sizes measured once compaction had re-encoded the
    /// table. The bound is measured against what a whole-table rewrite reads, which
    /// includes the upserts it is about to fold and freshly written files, encoded
    /// lightly until compaction re-encodes them (in that harness about twice their
    /// compacted size). So it never admits a table past the size where position
    /// deletes won, and a freshly loaded table qualifies only up to about half of
    /// it; the target it derives from follows the storage the table is on.
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
    /// ended is at least `RECLAIM_SHARE_DENOMINATOR - 1` times that reclaim's
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
        pace.busy = busy;
        pace.last_end = Some(ended);
    }

    /// Forget the pace, so the next reclaim is due at once: lets a test run
    /// reclaims back to back (via [`crate::CayenneTableProvider::forget_auto_reclaim_pace`]).
    pub(crate) fn forget_reclaim_pace(&self) {
        *self.reclaims.lock() = ReclaimPace::default();
    }

    /// Note that the table's deletion index holds a delete at `now`. The first such
    /// note starts the interval its first whole-table reclaim is measured against.
    pub(crate) fn note_delete(&self, now: Instant) {
        self.first_delete_at.lock().get_or_insert(now);
    }

    /// When [`Self::note_delete`] was first called, if it has been.
    #[cfg(test)]
    pub(crate) fn first_delete_noted(&self) -> Option<Instant> {
        *self.first_delete_at.lock()
    }

    /// Whether a whole-table rewrite of a table whose rows are all still in
    /// protected snapshots, which started at `started` and took `took`, should be
    /// discarded and the table kept on key deletes, and why. The bake reaches such
    /// rows, so key deletes serve the table whenever rewriting it whole would cost
    /// its writes:
    ///
    /// - The rewrite takes more than one [`RECLAIM_SHARE_DENOMINATOR`]th of the
    ///   time at the table's write rate: more than `RECLAIM_SHARE_DENOMINATOR - 1`
    ///   times less than the interval since the previous whole-table reclaim
    ///   ended or, before the first, since the table's first delete. The pace
    ///   would hold every such rewrite back.
    /// - The host has too few cores to keep the table compacted whole alongside its
    ///   writes (`small_host_cores`), whatever the rewrite took.
    ///
    /// Otherwise a rewrite shorter than [`MIN_PACED_REWRITE`] never counts, and the
    /// write rate needs a delete to measure against. A test may pin the answer
    /// ([`Self::pin_write_rate_rule`]).
    pub(crate) fn rewrite_keeps_key_deletes(
        &self,
        started: Instant,
        took: Duration,
    ) -> Option<KeyDeletesReason> {
        let interval = self
            .reclaims
            .lock()
            .last_end
            .or_else(|| *self.first_delete_at.lock())
            .map(|since| started.saturating_duration_since(since));
        match *self.write_rate_rule.lock() {
            WriteRateRule::Never => None,
            WriteRateRule::Always => Some(KeyDeletesReason::WriteRate {
                interval: interval.unwrap_or_default(),
            }),
            WriteRateRule::Measured { floor } => {
                if let Some(cores) = *self.small_host_cores.lock() {
                    return Some(KeyDeletesReason::Host { cores });
                }
                let interval = interval?;
                (took >= floor && took.saturating_mul(RECLAIM_SHARE_DENOMINATOR - 1) > interval)
                    .then_some(KeyDeletesReason::WriteRate { interval })
            }
        }
    }

    /// Treat the host as one of `cores` CPU cores, too few to keep a table
    /// compacted whole alongside its writes, or (`None`) as one with enough.
    #[cfg(test)]
    pub(crate) fn on_small_host(&self, cores: Option<usize>) {
        *self.small_host_cores.lock() = cores;
    }

    /// Pin how [`Self::rewrite_keeps_key_deletes`] answers, for a test whose subject
    /// is one regime.
    pub(crate) fn pin_write_rate_rule(&self, rule: AutoWriteRateRule) {
        *self.write_rate_rule.lock() = match rule {
            AutoWriteRateRule::Measured => WriteRateRule::Measured {
                floor: MIN_PACED_REWRITE,
            },
            AutoWriteRateRule::Never => WriteRateRule::Never,
            AutoWriteRateRule::Always => WriteRateRule::Always,
        };
    }

    /// Measure rewrites of any duration against the table's write rate, starting
    /// the interval at `first_delete_at`: lets a test reach the share comparison
    /// with a table that rewrites in milliseconds.
    #[cfg(test)]
    pub(crate) fn measure_write_rate_from(&self, first_delete_at: Option<Instant>) {
        *self.write_rate_rule.lock() = WriteRateRule::Measured {
            floor: Duration::ZERO,
        };
        *self.first_delete_at.lock() = first_delete_at;
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

    /// An `auto` table opened at `table_bytes` (`None`: could not be sized),
    /// with the write-rate rule as `spiced` runs it.
    fn opened(table_bytes: Option<u64>) -> AutoDeletionMode {
        let auto = AutoDeletionMode::new(BOUND);
        auto.size_at_open(table_bytes);
        auto.pin_write_rate_rule(AutoWriteRateRule::Measured);
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
    fn a_slower_reclaim_waits_nine_times_its_own_duration() {
        let auto = opened(Some(1));
        // A 100 ms reclaim from 0 ms, then a 2.1 s one from the moment the next is
        // due (1,000 ms): it ends at 3,100 ms.
        let start = Instant::now();
        auto.record_reclaim(
            Duration::from_millis(100),
            start + Duration::from_millis(100),
        );
        assert!(auto.reclaim_due(start + Duration::from_millis(1_000)));
        let second_end = start + Duration::from_millis(3_100);
        auto.record_reclaim(Duration::from_millis(2_100), second_end);
        // The third waits 9 × 2.1 s = 18.9 s, to 22 s: 2.2 s of rewriting in 22 s,
        // a tenth. Paced from the two reclaims' mean (1.1 s), it would have been
        // due at 13 s, with rewriting 16.9% of the time.
        assert!(!auto.reclaim_due(second_end + Duration::from_millis(18_899)));
        assert!(auto.reclaim_due(second_end + Duration::from_millis(18_900)));
    }

    #[test]
    fn a_rewrite_keeps_key_deletes_when_it_takes_over_a_tenth_of_the_interval() {
        let auto = opened(Some(1));
        let first_delete = Instant::now();
        auto.note_delete(first_delete);
        // A later delete does not move the start of the interval.
        auto.note_delete(first_delete + Duration::from_secs(1));
        let started = first_delete + Duration::from_millis(1_800);
        // 9 × 200 ms = 1.8 s is not more than the 1.8 s interval: within its share.
        assert_eq!(
            auto.rewrite_keeps_key_deletes(started, Duration::from_millis(200)),
            None
        );
        // 9 × 201 ms is: past its share, measured against 1.8 s.
        assert_eq!(
            auto.rewrite_keeps_key_deletes(started, Duration::from_millis(201)),
            Some(KeyDeletesReason::WriteRate {
                interval: Duration::from_millis(1_800)
            })
        );
    }

    #[test]
    fn a_rewrite_is_measured_from_the_previous_reclaim_once_there_is_one() {
        let auto = opened(Some(1));
        let first_delete = Instant::now();
        auto.note_delete(first_delete);
        let previous_end = first_delete + Duration::from_secs(60);
        auto.record_reclaim(Duration::from_millis(100), previous_end);
        let started = previous_end + Duration::from_secs(1);
        // Against the minute since the first delete it would be within its share;
        // against the second since the previous reclaim it is not.
        assert_eq!(
            auto.rewrite_keeps_key_deletes(started, Duration::from_millis(150)),
            Some(KeyDeletesReason::WriteRate {
                interval: Duration::from_secs(1)
            })
        );
    }

    #[test]
    fn a_short_rewrite_or_one_with_nothing_to_measure_against_never_outpaces_writes() {
        let auto = opened(Some(1));
        let started = Instant::now();
        // No delete yet: nothing to measure against.
        assert_eq!(
            auto.rewrite_keeps_key_deletes(started, Duration::from_secs(10)),
            None
        );
        auto.note_delete(started);
        // Below the floor, however short the interval.
        assert_eq!(
            auto.rewrite_keeps_key_deletes(
                started,
                MIN_PACED_REWRITE.saturating_sub(Duration::from_millis(1))
            ),
            None
        );
        assert_eq!(
            auto.rewrite_keeps_key_deletes(started, MIN_PACED_REWRITE),
            Some(KeyDeletesReason::WriteRate {
                interval: Duration::ZERO
            })
        );
    }

    #[test]
    fn a_first_rewrite_on_a_small_host_keeps_key_deletes_however_long_it_took() {
        let auto = opened(Some(1));
        let started = Instant::now();
        auto.note_delete(
            started
                .checked_sub(Duration::from_secs(60))
                .expect("a minute ago is representable"),
        );
        // Well within its share of a minute, on a host with enough cores.
        assert_eq!(
            auto.rewrite_keeps_key_deletes(started, Duration::from_millis(150)),
            None
        );
        auto.on_small_host(Some(4));
        for took in [Duration::from_millis(1), Duration::from_millis(150)] {
            assert_eq!(
                auto.rewrite_keeps_key_deletes(started, took),
                Some(KeyDeletesReason::Host { cores: 4 }),
                "a rewrite that took {took:?}"
            );
        }
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
