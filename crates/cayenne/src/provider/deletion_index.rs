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

//! Immutable deletion index with bloom-filter prefilter.
//!
//! [`DeletionIndex`] (Int64 PK) and [`KeyDeletionIndex`] (composite-key PK) are the
//! frozen snapshots that scans probe at query time. Each entry fuses the two
//! sequence numbers visibility needs — the highest *delete* sequence and, for keys
//! re-inserted by an upsert, the highest *insert* sequence — into one
//! [`TombstoneEntry`], so the scan hot path answers "is this row deleted, and was
//! it re-inserted after that?" with a **single** probe.
//!
//! # Layered storage: ordered frozen runs + a small active tier
//!
//! Entries live in `LayeredRuns`: an ordered set of frozen **runs** (oldest
//! first) over one small mutable **active** tier — the in-memory analogue of an
//! LSM run cascade over a memtable:
//!
//! - **runs**: a `Vec<Arc<RunData>>`. Each run is a frozen
//!   `Arc<std::collections::HashMap>` (`SwissTable`) tagged with its maximum
//!   delete sequence (`max_delete_seq`) plus its own bloom filter. Frozen — never
//!   mutated after construction — so publishing a new generation shares the runs
//!   by `Arc` clone, and probing one run costs 1–2 cache lines.
//! - **active**: a small persistent [`im::HashMap`] holding writes since the last
//!   freeze. `O(1)` to snapshot per publish (structural sharing).
//!
//! A write goes to `active`; when `active` crosses
//! `max(`[`DELTA_MERGE_MIN`]`, base/4)` entries it **freezes** into a new run,
//! and once the run count exceeds `MAX_FROZEN_RUNS` the adjacent pair with the
//! smallest combined size **folds** together (size-tiering — the large base run
//! stays stable while the recent runs a probe touches stay shallow and
//! cache-resident). The `base/4` freeze ladder keeps total copy work per key
//! logarithmic in the final index size, and generations pinned by long scans
//! share the previous runs by `Arc`.
//!
//! A probe fuses `active` with the *applicable* runs, per-side max:
//! - **main-scan** (cutoff `None`): every run is fused, behind the global bloom
//!   fast-reject (the base run's bloom *is* the global one).
//! - **protected-snapshot** (cutoff `Some(S)`): runs with `max_delete_seq <= S`
//!   are skipped wholesale (no delete in them can be newer than `S`), and the
//!   remaining recent runs are gated by their own per-run blooms — so a protected
//!   probe never touches the large global bloom and its cost tracks the recent
//!   (small) runs, not the total accumulated tombstone count.
//!
//! The seq-prefix bake ([`prune_deletes_at_or_below`](DeletionIndex::prune_deletes_at_or_below))
//! drops deletes at or below a cutoff sequence once they are baked into the data
//! files, shrinking the runs.
//!
//! The bloom filters are keyed on *deletion* membership (a single cache line per
//! probe). Entries holding only an insert record (which occur after compaction
//! purges delete files while insert records remain in the catalog) are not
//! represented in the bloom and are reported as absent by [`get`] — exactly the
//! visibility semantics of the original two-index design.
//!
//! [`get`]: DeletionIndex::get
//!
//! # Build then publish
//!
//! All map mutation happens before the index is wrapped in an `Arc`/`ArcSwap`.
//! Construct via [`DeletionIndex::from_maps`] / [`DeletionIndex::empty`] (and the
//! matching [`KeyDeletionIndex`] constructors), publish through `ArcSwap`, and treat
//! the published index as immutable. To apply a write, build a new index with
//! [`extend_max_deletes`](DeletionIndex::extend_max_deletes) (delete-only writes) or
//! [`extend_max_conflicts`](DeletionIndex::extend_max_conflicts) (upsert conflicts,
//! which record a delete and a re-insert in one pass) and store the
//! `Arc<DeletionIndex>` back into the swap cell. Readers always see a fully-built
//! snapshot and never block.
//!
//! The bloom filter is the one deliberate exception to "frozen": it is shared
//! across generations behind an `Arc` and extended in place with relaxed atomic
//! stores. Older pinned generations may observe bits for keys added after they
//! were published — a safe superset that can only add false positives (caught by
//! the map probe), never false negatives. See [`SplitBlockBloomFilter`] for the
//! memory-ordering contract and [`extend_max_deletes`](DeletionIndex::extend_max_deletes)
//! for the rebuild policy.

use hash_index::{
    PrehashedBuildHasher, SplitBlockBloomFilter, XxHash3BuildHasher, hash_key_128, hash_key_i64,
};
use im::HashMap as PersistentHashMap;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::{Arc, LazyLock};

/// Bloom filter capacity floor: keep some signal even for empty / tiny sets so that the
/// "probably-not-present" path stays useful when a fresh index is constructed.
const MIN_BLOOM_CAPACITY: usize = 64;

/// Delta-size floor below which a merge into the base is never triggered.
/// Small in tests so layering (merge, pinned-generation, and counter behavior
/// across merges) is exercised by ordinary unit tests without inserting tens of
/// thousands of keys.
#[cfg(not(test))]
const DELTA_MERGE_MIN: usize = 16_384;
#[cfg(test)]
const DELTA_MERGE_MIN: usize = 64;

/// Delta size at which the delta is folded into a fresh frozen base.
///
/// `max(DELTA_MERGE_MIN, base/4)`: the floor avoids re-copying tiny bases on
/// every publish, and the `base/4` ladder makes merge cadence geometric — each
/// key is copied `O(log N)` times over the index's lifetime — while capping the
/// delta at a quarter of the base so it stays a shallow, cache-resident HAMT.
fn delta_merge_threshold(base_len: usize) -> usize {
    (base_len / 4).max(DELTA_MERGE_MIN)
}

/// Hasher for the Int64 index maps. `im::HashMap`/`std::collections::HashMap`
/// default to `RandomState` (SipHash-1-3), which showed up as
/// `im::nodes::hamt::hash_key` in executor profiles of changes-mode ingest —
/// every probe was paying a `SipHash` walk on top of the seeded-XXH3 bloom hash.
/// [`XxHash3BuildHasher`] uses the same seeded XXH3-64 as [`hash_key_i64`], so
/// map and bloom hashing now agree at a fraction of `SipHash`'s per-key cost.
pub type DeletionIndexHasher = XxHash3BuildHasher;

/// Bloom capacity for a deletion set of `len` keys: sized with 2x headroom so the
/// filter stays at or below its design false-positive rate for the whole window
/// between rebuilds. The previous policy sized the filter exactly and rebuilt
/// only when the entry count crossed `2x` capacity, which let the effective
/// bits-per-key halve before each rebuild — at millions of deletions the filter
/// spent long windows saturated, sending most not-deleted probes through to the
/// hash map.
fn bloom_capacity_for(len: usize) -> usize {
    len.saturating_mul(2).max(MIN_BLOOM_CAPACITY)
}

/// Keys swept per chunk by the batched probes ([`DeletionIndex::get_batch`] /
/// [`KeyDeletionIndex::get_batch`]). Sized so one chunk's working set (key
/// slice / hash buffer plus the candidate-position buffer) stays L1/L2
/// resident: candidate positions produced by the bloom sweep are still in
/// cache when the tier walk consumes them, while the chunk is large enough
/// that each pass runs as a long branch-light loop the out-of-order window
/// can overlap loads across.
const BATCH_SWEEP_CHUNK: usize = 2048;

/// Sentinel for "no sequence recorded" inside [`TombstoneEntry`]. Catalog
/// sequence numbers are non-negative, so `i64::MIN` can never collide with a
/// real sequence; packing both fields as raw `i64` keeps the per-entry payload
/// at 16 bytes (an `Option<i64>` pair would double it — at tens of millions of
/// entries that is hundreds of MiB).
const SEQUENCE_ABSENT: i64 = i64::MIN;

/// Raw fused map value: the highest delete and insert sequence numbers recorded
/// for one primary key, with [`SEQUENCE_ABSENT`] marking an unset side. Exposed
/// (read-only) so callers iterating [`iter_entries`](DeletionIndex::iter_entries)
/// can project the side they need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneEntry {
    delete_seq: i64,
    insert_seq: i64,
}

impl TombstoneEntry {
    const EMPTY: Self = Self {
        delete_seq: SEQUENCE_ABSENT,
        insert_seq: SEQUENCE_ABSENT,
    };

    /// Highest delete sequence recorded for this key, if any.
    #[inline]
    #[must_use]
    pub fn delete_sequence(&self) -> Option<i64> {
        (self.delete_seq != SEQUENCE_ABSENT).then_some(self.delete_seq)
    }

    /// Highest insert (upsert re-insertion) sequence recorded for this key, if any.
    #[inline]
    #[must_use]
    pub fn insert_sequence(&self) -> Option<i64> {
        (self.insert_seq != SEQUENCE_ABSENT).then_some(self.insert_seq)
    }
}

/// Deletion state returned by a successful probe: the key **has** a recorded
/// deletion, plus the re-insertion sequence if an upsert wrote the key back.
///
/// Visibility under upsert semantics: the row is visible iff
/// `insert_sequence > delete_sequence` (re-inserted after the delete).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tombstone {
    /// Highest delete sequence recorded for the key.
    pub delete_sequence: i64,
    /// Highest insert (upsert re-insertion) sequence for the key, if any.
    pub insert_sequence: Option<i64>,
}

impl Tombstone {
    #[inline]
    fn from_entry(entry: &TombstoneEntry) -> Option<Self> {
        entry.delete_sequence().map(|delete_sequence| Self {
            delete_sequence,
            insert_sequence: entry.insert_sequence(),
        })
    }
}

/// Outcome of merging one addition against a key's current effective entry.
#[derive(Clone, Copy)]
struct MergeOutcome {
    /// The merged entry to store.
    entry: TombstoneEntry,
    /// The key had no entry in either tier before this addition.
    new_key: bool,
    /// The delete side transitioned from absent to present (drives the bloom
    /// update and `delete_len`).
    new_delete: bool,
    /// The insert side transitioned from absent to present (drives `insert_len`).
    new_insert: bool,
}

/// Merge `delete_seq`/`insert_seq` (either possibly [`SEQUENCE_ABSENT`]) into
/// `current`, taking the per-side max.
#[inline]
fn merged_entry(current: Option<TombstoneEntry>, delete_seq: i64, insert_seq: i64) -> MergeOutcome {
    let new_key = current.is_none();
    let mut entry = current.unwrap_or(TombstoneEntry::EMPTY);
    let mut new_delete = false;
    let mut new_insert = false;
    if delete_seq != SEQUENCE_ABSENT {
        if entry.delete_seq == SEQUENCE_ABSENT {
            new_delete = true;
            entry.delete_seq = delete_seq;
        } else if delete_seq > entry.delete_seq {
            entry.delete_seq = delete_seq;
        }
    }
    if insert_seq != SEQUENCE_ABSENT {
        if entry.insert_seq == SEQUENCE_ABSENT {
            new_insert = true;
            entry.insert_seq = insert_seq;
        } else if insert_seq > entry.insert_seq {
            entry.insert_seq = insert_seq;
        }
    }
    MergeOutcome {
        entry,
        new_key,
        new_delete,
        new_insert,
    }
}

// =============================================================================
// Seq-partitioned runs core (rank-1: probe cost independent of accumulated K)
// =============================================================================

/// Maximum frozen runs kept before a fold merges the smallest adjacent pair.
/// Size-tiering keeps most runs small + the base stable, so a protected probe
/// still touches only the recent (small, applicable) runs.
#[cfg(not(test))]
const MAX_FROZEN_RUNS: usize = 8;
/// Small in tests so fold/run-skip behaviour is exercised without huge inputs.
#[cfg(test)]
const MAX_FROZEN_RUNS: usize = 3;

/// Cap on the freeze threshold: a frozen run never exceeds ~this many entries, so
/// the recent runs (and their per-run blooms) stay small + cache-resident no
/// matter how large the accumulated base grows. Without it the threshold is
/// `base/4`, making recent runs scale with the base — which bounded the v1.1
/// protected-probe win at steady state.
const FREEZE_CAP: usize = 262_144;

/// One frozen run in [`LayeredRuns`]: an immutable entry map plus the per-run
/// maximum delete sequence. A protected-snapshot probe with cutoff `Some(S)`
/// skips a run wholesale when `max_delete_seq <= S` — no entry in it can carry a
/// delete newer than `S` — so probe cost tracks the recent (small) runs instead
/// of every accumulated tombstone. `SEQUENCE_ABSENT` when the run holds no
/// deletion (insert-only), which never satisfies `> S`, so it is always skipped
/// under a threshold (correctly: it can't apply a deletion).
#[derive(Debug)]
struct RunData<K, S>
where
    K: Copy + Eq + Hash,
    S: BuildHasher,
{
    map: Arc<HashMap<K, TombstoneEntry, S>>,
    max_delete_seq: i64,
    /// Bloom over this run's deletion keys (insert-only entries excluded, like the
    /// global bloom). A probe skips this run's map GET when the key is absent —
    /// and a protected probe, which already skips the base run by `max_delete_seq`,
    /// can therefore avoid the large global bloom entirely and consult only the
    /// recent runs' (smaller) blooms (v1.1).
    bloom: Arc<SplitBlockBloomFilter>,
}

/// Seq-partitioned deletion-index core: an ordered set of frozen `runs` (oldest
/// first, each tagged with its max delete sequence) plus a mutable `active` tier.
/// A probe fuses `active` with the runs that could carry a delete newer than the
/// caller's threshold, per-side max. Unthresholded (main-scan) probes fuse ALL
/// runs (the full fused value over every write); protected-snapshot probes
/// (`Some(S)`) skip runs with `max_delete_seq <= S`. `active` is always fused
/// (small; its values are bounded by the same `> S` comparison the caller applies
/// in `tombstone_visible`).
///
/// EQUIVALENCE of the threshold skip: for `S=None`, fuse-over-all = per-side max
/// over every write. For `S=Some`, `(max-over-applicable > S) <=> (true-max > S)`:
/// a skipped run has `max_delete_seq <= S` so this key's delete there is `<= S`
/// (can't be the witness for `> S`); an included run with the true max gives it.
/// Only `Ignore`-mode branches carry `Some(S)` (no `(Apply,Some)` ctor), so
/// insert-side values in skipped runs are irrelevant.
#[derive(Debug, Clone)]
struct LayeredRuns<K, S>
where
    K: Copy + Eq + Hash,
    S: BuildHasher + Default + Clone,
{
    /// Frozen tiers, oldest first. Fused per-side-max at read; shared via `Arc`.
    runs: Vec<Arc<RunData<K, S>>>,
    /// Mutable tier: writes since the last freeze, per-side-max WITHIN active but
    /// NOT pre-fused with the runs (so the runs stay independently seq-skippable).
    active: PersistentHashMap<K, TombstoneEntry, S>,
    bloom: Arc<SplitBlockBloomFilter>,
    /// Global monotonic max delete sequence (over runs + active). Feeds the
    /// protected-snapshot install-skip + compaction fence; recompute on removal.
    max_sequence_number: Option<i64>,
    /// Global min/max deleted key (conservative superset; i64 PK-range pruning).
    min_deleted_key: Option<K>,
    max_deleted_key: Option<K>,
    /// Distinct-key counts across runs + active (a key in several tiers counts
    /// once); maintained by a fuse-check on extend.
    entry_count: usize,
    delete_count: usize,
    insert_count: usize,
    bloom_capacity: usize,
}

impl<K, S> LayeredRuns<K, S>
where
    K: Copy + Eq + Hash + Ord,
    S: BuildHasher + Default + Clone,
{
    fn empty() -> Self {
        Self {
            runs: Vec::new(),
            active: PersistentHashMap::default(),
            bloom: Arc::new(SplitBlockBloomFilter::new(MIN_BLOOM_CAPACITY)),
            max_sequence_number: None,
            min_deleted_key: None,
            max_deleted_key: None,
            entry_count: 0,
            delete_count: 0,
            insert_count: 0,
            bloom_capacity: MIN_BLOOM_CAPACITY,
        }
    }

    /// Bulk-build: deletions + insert records fold into a single frozen base run.
    fn from_iters(
        deleted: impl ExactSizeIterator<Item = (K, i64)>,
        insert_records: impl Iterator<Item = (K, i64)>,
        bloom_hash_of: impl Fn(&K) -> u64,
    ) -> Self {
        let capacity = bloom_capacity_for(deleted.len());
        let bloom = SplitBlockBloomFilter::new(capacity);
        let delete_count = deleted.len();
        let mut max_sequence_number = None;
        let mut min_deleted_key: Option<K> = None;
        let mut max_deleted_key: Option<K> = None;
        let mut run_max_delete_seq = SEQUENCE_ABSENT;
        let mut map: HashMap<K, TombstoneEntry, S> =
            HashMap::with_capacity_and_hasher(delete_count, S::default());
        for (key, delete_seq) in deleted {
            bloom.insert(bloom_hash_of(&key));
            if max_sequence_number.is_none_or(|max| delete_seq > max) {
                max_sequence_number = Some(delete_seq);
            }
            if delete_seq > run_max_delete_seq {
                run_max_delete_seq = delete_seq;
            }
            if min_deleted_key.is_none_or(|min| key < min) {
                min_deleted_key = Some(key);
            }
            if max_deleted_key.is_none_or(|max| key > max) {
                max_deleted_key = Some(key);
            }
            map.insert(
                key,
                TombstoneEntry {
                    delete_seq,
                    insert_seq: SEQUENCE_ABSENT,
                },
            );
        }
        let mut insert_count = 0;
        for (key, insert_seq) in insert_records {
            insert_count += 1;
            let outcome = merged_entry(map.get(&key).copied(), SEQUENCE_ABSENT, insert_seq);
            map.insert(key, outcome.entry);
        }
        let entry_count = map.len();
        let bloom = Arc::new(bloom);
        let runs = if entry_count == 0 {
            Vec::new()
        } else {
            vec![Arc::new(RunData {
                map: Arc::new(map),
                max_delete_seq: run_max_delete_seq,
                // The base run holds every deletion, so its bloom IS the global one.
                bloom: Arc::clone(&bloom),
            })]
        };
        Self {
            runs,
            active: PersistentHashMap::default(),
            bloom,
            max_sequence_number,
            min_deleted_key,
            max_deleted_key,
            entry_count,
            delete_count,
            insert_count,
            bloom_capacity: capacity,
        }
    }

    /// Fuse `active` + `runs` (filtered by `applicable`) for `key`, per-side max.
    #[inline]
    fn fuse(
        active: &PersistentHashMap<K, TombstoneEntry, S>,
        runs: &[Arc<RunData<K, S>>],
        key: &K,
        applicable: impl Fn(&RunData<K, S>) -> bool,
    ) -> Option<TombstoneEntry> {
        let mut acc = active.get(key).copied();
        for run in runs {
            if !applicable(run) {
                continue;
            }
            if let Some(e) = run.map.get(key) {
                acc = Some(merged_entry(acc, e.delete_seq, e.insert_seq).entry);
            }
        }
        acc
    }

    /// Tombstone for `key` (its bloom `hash` precomputed by the wrapper), gated by
    /// the per-run blooms: a run is skipped when it can't carry an applicable
    /// delete (`max_delete_seq <= S`) or when its bloom misses `key`. The wrapper
    /// applies the global bloom as the fast-reject for `None` (main-scan) probes;
    /// protected (`Some(S)`) probes never touch the large global bloom — their
    /// applicable runs are the recent, small ones (v1.1).
    ///
    /// Per-run blooms have no false negatives, so this returns exactly the
    /// per-side-max fusion over the applicable runs + active — identical
    /// visibility to fusing every applicable run directly; the blooms only skip
    /// GETs that would miss.
    #[inline]
    fn tombstone_of(&self, key: &K, hash: u64, min_delete_seq: Option<i64>) -> Option<Tombstone> {
        // Reuse the per-side-max fold (`fuse`) with the run-skip gate:
        // - None (main scan): the wrapper already applied the global bloom, so
        //   every run is included (re-checking per-run blooms here — the base
        //   run's bloom IS the global one — would regress the hot path ~+30%).
        // - Some(S) (protected): skip runs that can't carry an applicable delete
        //   or whose per-run bloom misses, never touching the large global bloom.
        Self::fuse(&self.active, &self.runs, key, |run| match min_delete_seq {
            None => true,
            Some(s) => run.max_delete_seq > s && run.bloom.might_contain(hash),
        })
        .as_ref()
        .and_then(Tombstone::from_entry)
    }

    /// All effective entries, each key once with its fully-fused (all-runs +
    /// active) value. O(N) into a temp map — used by listing-time consumers that
    /// scan every entry anyway (e.g. `tombstone_exclusion_filter`).
    fn iter_entries(&self) -> impl Iterator<Item = (K, TombstoneEntry)> {
        let mut fused: HashMap<K, TombstoneEntry, S> =
            HashMap::with_capacity_and_hasher(self.entry_count, S::default());
        for run in &self.runs {
            for (key, entry) in run.map.iter() {
                let m = merged_entry(fused.get(key).copied(), entry.delete_seq, entry.insert_seq);
                fused.insert(*key, m.entry);
            }
        }
        for (key, entry) in self.active.iter() {
            let m = merged_entry(fused.get(key).copied(), entry.delete_seq, entry.insert_seq);
            fused.insert(*key, m.entry);
        }
        fused.into_iter()
    }

    /// Core of the extend methods. Distinct-key counters and the bloom/range are
    /// maintained against the FUSED state (active + all runs); the write goes to
    /// `active` only (per-side-max within active), then freeze/fold run.
    fn extend(
        &self,
        additions: impl Iterator<Item = (K, i64, i64)>,
        bloom_hash_of: impl Fn(&K) -> u64,
    ) -> Self {
        let mut active = self.active.clone();
        let mut runs = self.runs.clone();
        let mut max_sequence_number = self.max_sequence_number;
        let mut min_deleted_key = self.min_deleted_key;
        let mut max_deleted_key = self.max_deleted_key;
        let mut entry_count = self.entry_count;
        let mut delete_count = self.delete_count;
        let mut insert_count = self.insert_count;
        let mut new_delete_hashes: Vec<u64> = Vec::with_capacity(additions.size_hint().0);
        for (key, delete_seq, insert_seq) in additions {
            debug_assert_ne!(
                delete_seq, SEQUENCE_ABSENT,
                "real sequences are non-negative"
            );
            // Counter/bloom/range flags vs the FUSED pre-state (active + all runs).
            let fused = Self::fuse(&active, &runs, &key, |_| true);
            let outcome = merged_entry(fused, delete_seq, insert_seq);
            if outcome.new_key {
                entry_count += 1;
            }
            if outcome.new_delete {
                delete_count += 1;
                new_delete_hashes.push(bloom_hash_of(&key));
                if min_deleted_key.is_none_or(|min| key < min) {
                    min_deleted_key = Some(key);
                }
                if max_deleted_key.is_none_or(|max| key > max) {
                    max_deleted_key = Some(key);
                }
            }
            if outcome.new_insert {
                insert_count += 1;
            }
            if max_sequence_number.is_none_or(|max| outcome.entry.delete_seq > max) {
                max_sequence_number = Some(outcome.entry.delete_seq);
            }
            // Write to ACTIVE only (merge within active; runs stay un-fused so
            // they remain independently seq-skippable). Skip stale no-ops against
            // the FUSED pre-state (active + all runs), not just active: an
            // addition already covered by an older run must not be copied into the
            // hot `active` tier — it would bloat active and trigger premature
            // freezes/folds without changing the fused value.
            if fused != Some(outcome.entry) {
                let active_current = active.get(&key).copied();
                let active_merged = merged_entry(active_current, delete_seq, insert_seq).entry;
                active.insert(key, active_merged);
            }
        }

        // Bloom policy (global over runs+active deletions) — mirrors the old core.
        let (bloom, bloom_capacity) = if delete_count > self.bloom_capacity {
            let new_capacity = bloom_capacity_for(delete_count);
            let fresh = SplitBlockBloomFilter::new(new_capacity);
            for run in &runs {
                for (key, entry) in run.map.iter() {
                    if entry.delete_seq != SEQUENCE_ABSENT {
                        fresh.insert(bloom_hash_of(key));
                    }
                }
            }
            for (key, entry) in active.iter() {
                if entry.delete_seq != SEQUENCE_ABSENT {
                    fresh.insert(bloom_hash_of(key));
                }
            }
            (Arc::new(fresh), new_capacity)
        } else {
            for hash in new_delete_hashes {
                self.bloom.insert(hash);
            }
            (Arc::clone(&self.bloom), self.bloom_capacity)
        };

        // Freeze: active → newest frozen run once it crosses the threshold.
        let total_run_entries: usize = runs.iter().map(|r| r.map.len()).sum();
        if active.len() >= delta_merge_threshold(total_run_entries).min(FREEZE_CAP) {
            let mut map: HashMap<K, TombstoneEntry, S> =
                HashMap::with_capacity_and_hasher(active.len(), S::default());
            let mut run_max = SEQUENCE_ABSENT;
            // Size the per-run bloom by the DELETION-key count, not total entries:
            // only deletion keys are inserted below, so sizing by `active.len()`
            // (which includes insert-only entries) over-allocates the bloom on
            // insert-heavy runs and hurts the cache residency this refactor exists
            // to protect. Mirrors the global bloom's `bloom_capacity_for(delete…)`.
            let active_deletes = active
                .iter()
                .filter(|(_, e)| e.delete_seq != SEQUENCE_ABSENT)
                .count();
            let run_bloom = SplitBlockBloomFilter::new(bloom_capacity_for(active_deletes.max(1)));
            for (key, entry) in active.iter() {
                if entry.delete_seq != SEQUENCE_ABSENT {
                    if entry.delete_seq > run_max {
                        run_max = entry.delete_seq;
                    }
                    run_bloom.insert(bloom_hash_of(key));
                }
                map.insert(*key, *entry);
            }
            runs.push(Arc::new(RunData {
                map: Arc::new(map),
                max_delete_seq: run_max,
                bloom: Arc::new(run_bloom),
            }));
            active = PersistentHashMap::default();
        }

        // Fold: while over the cap, merge the adjacent pair with the smallest
        // combined size. Adjacent keeps each run's seq-range coherent (so the
        // `max_delete_seq` skip stays meaningful); smallest-pair keeps the large
        // base stable and consolidates the small recent runs among themselves
        // (size-tiering) — so the runs a protected probe touches stay small + cache-
        // resident regardless of how much the base accumulates.
        while runs.len() > MAX_FROZEN_RUNS {
            let mut at = 0;
            let mut smallest = usize::MAX;
            for i in 0..runs.len() - 1 {
                let combined = runs[i].map.len().saturating_add(runs[i + 1].map.len());
                if combined < smallest {
                    smallest = combined;
                    at = i;
                }
            }
            let newer = runs.remove(at + 1);
            let older = runs.remove(at);
            let mut map: HashMap<K, TombstoneEntry, S> = HashMap::with_capacity_and_hasher(
                older.map.len().saturating_add(newer.map.len()),
                S::default(),
            );
            // Build the merged run's bloom in the same passes that build the map:
            // the merged key carries a delete iff either side did, so inserting
            // each side's delete keys yields the union (no extra pass over the map).
            // Size the merged bloom by the DELETION-key count (only deletions are
            // inserted below), not the total entry count — the sum over-counts a
            // key deleted in both runs but stays far tighter than counting
            // insert-only entries, keeping the recent-run blooms cache-resident.
            let merged_deletes = older
                .map
                .iter()
                .filter(|(_, e)| e.delete_seq != SEQUENCE_ABSENT)
                .count()
                + newer
                    .map
                    .iter()
                    .filter(|(_, e)| e.delete_seq != SEQUENCE_ABSENT)
                    .count();
            let bloom = SplitBlockBloomFilter::new(bloom_capacity_for(merged_deletes.max(1)));
            for (key, entry) in older.map.iter() {
                map.insert(*key, *entry);
                if entry.delete_seq != SEQUENCE_ABSENT {
                    bloom.insert(bloom_hash_of(key));
                }
            }
            for (key, entry) in newer.map.iter() {
                let m = merged_entry(map.get(key).copied(), entry.delete_seq, entry.insert_seq);
                map.insert(*key, m.entry);
                if entry.delete_seq != SEQUENCE_ABSENT {
                    bloom.insert(bloom_hash_of(key));
                }
            }
            let max_delete_seq = older.max_delete_seq.max(newer.max_delete_seq);
            runs.insert(
                at,
                Arc::new(RunData {
                    map: Arc::new(map),
                    max_delete_seq,
                    bloom: Arc::new(bloom),
                }),
            );
        }

        Self {
            runs,
            active,
            bloom,
            max_sequence_number,
            min_deleted_key,
            max_deleted_key,
            entry_count,
            delete_count,
            insert_count,
            bloom_capacity,
        }
    }

    fn approx_bytes(&self, base_entry_bytes: usize, delta_entry_bytes: usize) -> usize {
        let run_bytes: usize = self
            .runs
            .iter()
            .map(|r| r.map.len().saturating_mul(base_entry_bytes))
            .sum();
        run_bytes.saturating_add(self.active.len().saturating_mul(delta_entry_bytes))
    }

    /// Drop every recorded **deletion** whose `delete_seq <= cutoff`, returning a
    /// fresh core with the surviving entries consolidated into a single frozen
    /// base run, a bloom rebuilt over the surviving deletion keys, and all
    /// derived counters/ranges recomputed.
    ///
    /// This is the deletion-index half of a seq-prefix compaction
    /// (`max_sequence <= T` files baked into a consolidated file): once the
    /// rewrite has physically applied every deletion at or below `cutoff`, those
    /// tombstones are dead and must be removed so they stop costing probe work
    /// and memory — but deletions with `delete_seq > cutoff` MUST be retained,
    /// because they still apply to the higher-than-`T` files the new snapshot
    /// references in place.
    ///
    /// ## What survives, per entry (key fused across every tier first)
    ///
    /// - **delete side** is kept iff `delete_seq > cutoff`; a delete at or below
    ///   `cutoff` is cleared (it was baked into the consolidated file).
    /// - **insert side** is always preserved: an upsert re-insertion above the
    ///   cutoff still resolves visibility against any surviving `> cutoff` delete,
    ///   and an insert-only entry is harmless to keep. An entry that loses its
    ///   delete side and has no insert record is dropped entirely.
    ///
    /// ## Why a single consolidated base run (no per-run tag surgery)
    ///
    /// Re-tagging the surviving runs after stripping individual sub-`cutoff`
    /// delete sides would desync each run's `max_delete_seq` (the basis of the
    /// protected-probe skip), forcing a full per-run rebuild anyway. A seq-prefix
    /// bake removes the *bulk* of accumulated deletions by construction, so the
    /// `> cutoff` remainder is small; folding it into one fresh base run (active
    /// empty) — exactly the shape [`from_iters`](Self::from_iters) builds — is
    /// both simplest-correct and cheap. The single run's tag is its true max
    /// surviving delete seq, so subsequent protected probes skip it precisely.
    fn prune_deletes_at_or_below(&self, cutoff: i64, bloom_hash_of: impl Fn(&K) -> u64) -> Self {
        // Fuse every key's sides across all tiers (runs + active), taking the
        // per-side max — identical to a full-index probe — then apply the cutoff
        // to the fused delete side. Fusing BEFORE the cutoff is essential: a key
        // could have a sub-cutoff delete in one run and a `> cutoff` delete in
        // another; the max wins and the entry correctly survives.
        let mut fused: HashMap<K, TombstoneEntry, S> = HashMap::default();
        for run in &self.runs {
            for (key, entry) in run.map.iter() {
                let m = merged_entry(fused.get(key).copied(), entry.delete_seq, entry.insert_seq);
                fused.insert(*key, m.entry);
            }
        }
        for (key, entry) in self.active.iter() {
            let m = merged_entry(fused.get(key).copied(), entry.delete_seq, entry.insert_seq);
            fused.insert(*key, m.entry);
        }

        let mut survivors: HashMap<K, TombstoneEntry, S> =
            HashMap::with_capacity_and_hasher(fused.len(), S::default());
        let mut max_sequence_number: Option<i64> = None;
        let mut min_deleted_key: Option<K> = None;
        let mut max_deleted_key: Option<K> = None;
        let mut delete_count = 0_usize;
        let mut insert_count = 0_usize;
        let mut run_max_delete_seq = SEQUENCE_ABSENT;
        // Surviving deletion key-hashes, to size + fill a fresh bloom in one pass
        // (the kept set may have shrunk well below the old capacity).
        let mut delete_hashes: Vec<u64> = Vec::new();

        for (key, entry) in &fused {
            let delete_seq = if entry.delete_seq != SEQUENCE_ABSENT && entry.delete_seq > cutoff {
                entry.delete_seq
            } else {
                SEQUENCE_ABSENT
            };
            let insert_seq = entry.insert_seq;
            if delete_seq == SEQUENCE_ABSENT && insert_seq == SEQUENCE_ABSENT {
                continue; // fully dead: delete baked, no re-insertion to track.
            }
            if delete_seq != SEQUENCE_ABSENT {
                delete_count += 1;
                delete_hashes.push(bloom_hash_of(key));
                if delete_seq > run_max_delete_seq {
                    run_max_delete_seq = delete_seq;
                }
                if max_sequence_number.is_none_or(|m| delete_seq > m) {
                    max_sequence_number = Some(delete_seq);
                }
                if min_deleted_key.is_none_or(|m| *key < m) {
                    min_deleted_key = Some(*key);
                }
                if max_deleted_key.is_none_or(|m| *key > m) {
                    max_deleted_key = Some(*key);
                }
            }
            if insert_seq != SEQUENCE_ABSENT {
                insert_count += 1;
            }
            survivors.insert(
                *key,
                TombstoneEntry {
                    delete_seq,
                    insert_seq,
                },
            );
        }

        let bloom_capacity = bloom_capacity_for(delete_count);
        let bloom = SplitBlockBloomFilter::new(bloom_capacity);
        for hash in &delete_hashes {
            bloom.insert(*hash);
        }
        let bloom = Arc::new(bloom);

        let entry_count = survivors.len();
        let runs = if entry_count == 0 {
            Vec::new()
        } else {
            vec![Arc::new(RunData {
                map: Arc::new(survivors),
                max_delete_seq: run_max_delete_seq,
                // Single consolidated run holds every surviving deletion, so its
                // bloom IS the global one (mirrors `from_iters`' base run).
                bloom: Arc::clone(&bloom),
            })]
        };

        Self {
            runs,
            active: PersistentHashMap::default(),
            bloom,
            max_sequence_number,
            min_deleted_key,
            max_deleted_key,
            entry_count,
            delete_count,
            insert_count,
            bloom_capacity,
        }
    }
}

// =============================================================================
// Int64 primary keys
// =============================================================================

/// Frozen deletion index for tables with a single-column Int64 primary key.
///
/// Entries are layered across a frozen flat base and a small delta (see the
/// [module docs](self)), fronted by a bloom filter keyed on deletion
/// membership. The bloom filter's bit array is sized for `bloom_capacity`
/// deletion keys with 2x headroom; in-capacity extends update the shared
/// filter in place and a full O(N) rebuild only happens when the deletion
/// count outgrows the sized capacity, keeping amortized writer cost at O(K)
/// per call (K = number of additions) — see
/// [`extend_max_deletes`](Self::extend_max_deletes) for the full argument.
#[derive(Debug, Clone)]
pub struct DeletionIndex {
    core: LayeredRuns<i64, DeletionIndexHasher>,
}

impl Default for DeletionIndex {
    fn default() -> Self {
        Self::empty()
    }
}

impl DeletionIndex {
    /// An empty deletion index. Probes always miss; bloom filter still allocated at the
    /// minimum capacity so size-0 indexes don't degrade once the index is extended.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            core: LayeredRuns::empty(),
        }
    }

    /// Process-wide shared empty index. Use this instead of
    /// `Arc::new(Self::empty())` from per-scan hot paths — the bloom
    /// allocation is amortized across every caller. See
    /// `apply_partial_filter_empty_alloc` bench.
    #[must_use]
    pub fn shared_empty() -> Arc<Self> {
        static EMPTY: LazyLock<Arc<DeletionIndex>> =
            LazyLock::new(|| Arc::new(DeletionIndex::empty()));
        Arc::clone(&EMPTY)
    }

    /// Build a frozen index from an owned `HashMap` of `pk -> delete_sequence`
    /// with no insert records.
    #[must_use]
    pub fn from_map(deleted: HashMap<i64, i64>) -> Self {
        Self::from_maps(deleted, HashMap::new())
    }

    /// Build a frozen index from `pk -> delete_sequence` plus
    /// `pk -> insert_sequence` maps (the catalog load path persists the two
    /// sides separately).
    #[must_use]
    pub fn from_maps(deleted: HashMap<i64, i64>, insert_records: HashMap<i64, i64>) -> Self {
        Self {
            core: LayeredRuns::from_iters(deleted.into_iter(), insert_records.into_iter(), |pk| {
                hash_key_i64(*pk)
            }),
        }
    }

    /// Build a frozen index from an `Arc<HashMap>` of deletions (clones the map).
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<i64, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Total number of fused entries (keys with a deletion, an insert record,
    /// or both).
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.entry_count
    }

    /// Whether the index has no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.entry_count == 0
    }

    /// Number of keys with a recorded deletion.
    #[must_use]
    pub fn delete_len(&self) -> usize {
        self.core.delete_count
    }

    /// Number of keys with a recorded insert (upsert re-insertion).
    #[must_use]
    pub fn insert_len(&self) -> usize {
        self.core.insert_count
    }

    /// Whether any key has a recorded deletion. Scan fast paths skip the
    /// filter entirely when this is `false` (insert-only entries never affect
    /// visibility).
    #[must_use]
    pub fn has_deletions(&self) -> bool {
        self.core.delete_count > 0
    }

    /// Approximate resident bytes for memory accounting. Base entries are
    /// flat-table slots (`i64 -> (i64, i64)` payload plus `SwissTable` load
    /// factor and control bytes); delta entries carry HAMT node/bitmap/Arc
    /// overhead. Shared structure retained by older reader-pinned generations
    /// is intentionally not charged to the latest snapshot again.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_I64_BASE_ENTRY_BYTES: usize = 32;
        const APPROX_I64_DELTA_ENTRY_BYTES: usize = 80;
        self.core
            .approx_bytes(APPROX_I64_BASE_ENTRY_BYTES, APPROX_I64_DELTA_ENTRY_BYTES)
    }

    /// Highest **delete** sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.core.max_sequence_number
    }

    /// Closed `[min,max]` of all **deleted** PK values in this index, or `None`
    /// when no key has a recorded deletion (`delete_len() == 0`).
    ///
    /// This is a sound *superset* of the PKs the filter could remove: every PK
    /// with a tombstone lies within it (including deletions below a protected
    /// snapshot's `min_delete_seq_to_apply` cutoff that the visibility check
    /// would skip — the range is intentionally NOT narrowed by sequence, which
    /// keeps it a conservative superset of the applicable-by-sequence subset).
    /// A scan window proven disjoint from this range therefore contains no
    /// deletable PK, so the deletion filter can be skipped for that branch /
    /// batch without dropping a live row or keeping a deleted one. See
    /// `CayenneTableProvider::branch_int64_pk_range` and the
    /// `Int64PkDeletionFilterStream` per-batch fast path.
    #[must_use]
    pub fn deleted_key_range(&self) -> Option<(i64, i64)> {
        Some((self.core.min_deleted_key?, self.core.max_deleted_key?))
    }

    /// Bloom-filter check against deletion membership. Returns `false` if the key
    /// definitely has no recorded deletion; `true` if it might (and a
    /// [`get`](Self::get) is required to confirm).
    #[inline]
    #[must_use]
    pub fn might_contain(&self, pk: i64) -> bool {
        self.core.bloom.might_contain(hash_key_i64(pk))
    }

    /// Bloom-prefiltered lookup. Returns the key's [`Tombstone`] if `pk` has a
    /// recorded deletion, `None` otherwise.
    ///
    /// Keys holding only an insert record are reported as `None`: the bloom is
    /// keyed on deletion membership, and visibility treats "no deletion" and
    /// "insert-only" identically. One probe answers both the deletion and the
    /// re-insertion question.
    ///
    /// # Inlining trade-off (measured)
    ///
    /// Keeping the tier walk fully inlined here is a deliberate choice that
    /// favors present-key probes over bloom rejects. With the walk inlined,
    /// out-of-cache present-key probes overlap their base-map misses across
    /// loop iterations (memory-level parallelism): 92µs vs 143µs per 8192
    /// probes at 1M entries when the walk is forced out of line. The cost is
    /// the inverse effect on the bloom-reject path, whose tight probe loop no
    /// longer unrolls as densely (~3.7ns → ~6ns per rejected probe; composite
    /// keys are unaffected). Changes-mode scans — the workload this index is
    /// hot in — are present-key heavy, so the walk stays inlined. Callers
    /// probing a whole batch should prefer [`get_batch`](Self::get_batch),
    /// which sweeps the bloom across the batch first and walks only the
    /// survivors — winning on both paths.
    #[inline]
    #[must_use]
    pub fn get(&self, pk: i64) -> Option<Tombstone> {
        self.get_with_min_seq(pk, None)
    }

    /// Like [`get`](Self::get) but applies the protected-snapshot cutoff `S` at
    /// probe time: frozen runs whose `max_delete_seq <= S` are skipped, so a
    /// protected-snapshot scan walks only the recent (cache-resident) runs
    /// rather than the full accumulated deletion set. `None` fuses all runs
    /// (main scan), byte-identical to [`get`](Self::get).
    #[inline]
    #[must_use]
    pub fn get_with_min_seq(&self, pk: i64, min_delete_seq: Option<i64>) -> Option<Tombstone> {
        let hash = hash_key_i64(pk);
        // The global bloom covers every run's deletion keys and is a safe
        // superset (never a false negative), so a miss means the key has no
        // tombstone at any sequence: `None` for any cutoff, including a protected
        // `Some(S)` probe. Apply the fast-reject unconditionally so protected
        // scans don't walk every frozen run per row for keys that were never
        // deleted (the common scan case). A bloom HIT still falls through to
        // `tombstone_of`, which applies the `Some(S)` cutoff exactly as before.
        if !self.core.bloom.might_contain(hash) {
            return None;
        }
        self.core.tombstone_of(&pk, hash, min_delete_seq)
    }

    /// Batched bloom-prefiltered lookup: invokes `on_hit(index, tombstone)`
    /// for every `pks[index]` that has a recorded deletion, in ascending
    /// `index` order. Exactly equivalent to calling [`get`](Self::get) on each
    /// element and reporting the `Some` results — keys holding only an insert
    /// record are reported absent, per the same contract.
    ///
    /// # Why a batch entry point
    ///
    /// [`get`](Self::get) fuses the bloom probe with the tier walk per key, so
    /// on a bloom hit the delta/base map walk (an out-of-cache load at scale)
    /// serializes against the *next* key's bloom probe. This method
    /// restructures the loop into the column-sweep form the read-side
    /// `KeyBasedDeletionFilterExec` already uses (b3 sub-lever 2):
    ///
    /// 1. a tight bloom sweep over a chunk of keys — branch-light, one
    ///    independent 32-byte block load per key that the out-of-order window
    ///    overlaps across iterations — collecting candidate positions;
    /// 2. the delta→base tier walk only for the surviving candidates (bloom
    ///    false positives resolve to "absent" here, preserving per-key `get`
    ///    results).
    ///
    /// Keys are processed in [`BATCH_SWEEP_CHUNK`]-sized chunks so the
    /// candidate buffer is still cache-hot when pass 2 consumes it.
    pub fn get_batch(&self, pks: &[i64], mut on_hit: impl FnMut(usize, Tombstone)) {
        // The bloom is keyed on deletion membership only: with no recorded
        // deletions every probe would miss (insert-only entries probe as
        // absent), so skip the sweep entirely.
        if self.core.delete_count == 0 {
            return;
        }
        // Carry each survivor's bloom hash forward to pass 2 so `hash_key_i64`
        // runs exactly once per key: pass 2 reuses the hash computed during the
        // sweep rather than recomputing it for every bloom-hit candidate.
        let mut candidates: Vec<(u32, u64)> = Vec::with_capacity(BATCH_SWEEP_CHUNK.min(pks.len()));
        for (chunk_index, chunk) in pks.chunks(BATCH_SWEEP_CHUNK).enumerate() {
            let chunk_base = chunk_index * BATCH_SWEEP_CHUNK;
            candidates.clear();
            // Pass 1: bloom sweep — no tier walk in the loop body.
            for (i, &pk) in (0_u32..).zip(chunk.iter()) {
                let hash = hash_key_i64(pk);
                if self.core.bloom.might_contain(hash) {
                    candidates.push((i, hash));
                }
            }
            // Pass 2: tier walk for the bloom survivors only.
            for &(i, hash) in &candidates {
                if let Some(tombstone) = self.core.tombstone_of(&chunk[i as usize], hash, None) {
                    on_hit(chunk_base + i as usize, tombstone);
                }
            }
        }
    }

    /// Iterate all effective entries (for callers that need a full walk, e.g.
    /// benches; project sides via [`TombstoneEntry::delete_sequence`] /
    /// [`TombstoneEntry::insert_sequence`]). Each key yields exactly once with
    /// its newest value.
    pub fn iter_entries(&self) -> impl Iterator<Item = (i64, TombstoneEntry)> + '_ {
        self.core.iter_entries()
    }

    /// Build a new index from `self`'s entries plus delete-only `additions`
    /// (`pk -> delete_sequence`), taking the per-key max sequence on conflict.
    /// Used by writers to publish a new snapshot via `ArcSwap::store`.
    ///
    /// # Performance
    ///
    /// Additions land in the small delta tier (`O(log delta)` persistent-map
    /// inserts that share structure with reader-pinned generations); the
    /// frozen base is shared by `Arc` and only re-copied when the delta
    /// crosses [`delta_merge_threshold`] — a geometric cadence that amortizes
    /// to `O(log N)` total copy work per key. The bloom filter is *shared*
    /// with the parent index behind an `Arc`: keys gaining their first
    /// deletion are inserted into it in place (relaxed atomic stores, O(K), no
    /// copy of the bit array — at millions of entries a per-call copy would be
    /// a multi-megabyte memcpy on every CDC batch). A full O(N) bloom rebuild
    /// only happens when the deletion count outgrows the capacity the filter
    /// was sized for — 2x the deletion count at build time
    /// ([`bloom_capacity_for`]) — so the rebuild cadence is geometric and the
    /// amortized bloom cost stays O(K) per call.
    ///
    /// Sharing the filter means older pinned generations observe bits for keys
    /// added after they were published. That is a safe superset: extra bits can
    /// only add false positives (caught by the map probe), never false
    /// negatives. Writers are serialized by the per-table write lock, which
    /// keeps the `bloom_capacity` bookkeeping consistent across generations.
    ///
    /// **Why this matters**: a previous revision rebuilt the bloom from scratch on
    /// every extend call, which is the dominant cost (10K entries ≈ 10K hash
    /// ops ≈ ~1 ms per call before any map update work is counted).
    /// On high-rate upsert/delete workloads (each producing a small `additions`
    /// batch but operating on a deletion cache that grows over time), the wasted
    /// bloom rebuild work compounds — and is the root cause of the ingestion
    /// regression that prompted this fix.
    #[must_use]
    pub fn extend_max_deletes(&self, additions: impl IntoIterator<Item = (i64, i64)>) -> Self {
        Self {
            core: self.core.extend(
                additions
                    .into_iter()
                    .map(|(pk, delete_seq)| (pk, delete_seq, SEQUENCE_ABSENT)),
                |pk| hash_key_i64(*pk),
            ),
        }
    }

    /// Build a new index recording an upsert-conflict batch: every key in
    /// `keys` gains a deletion at `delete_sequence` **and** a re-insertion at
    /// `insert_sequence`, each merged with per-key max. One pass over the map
    /// replaces the previous two-index double extend.
    #[must_use]
    pub fn extend_max_conflicts(
        &self,
        keys: impl IntoIterator<Item = i64>,
        delete_sequence: i64,
        insert_sequence: i64,
    ) -> Self {
        Self {
            core: self.core.extend(
                keys.into_iter()
                    .map(|pk| (pk, delete_sequence, insert_sequence)),
                |pk| hash_key_i64(*pk),
            ),
        }
    }

    /// Build a new index with every **deletion** at or below `cutoff` removed,
    /// retaining deletions with `delete_seq > cutoff` and all insert records.
    ///
    /// Used by a seq-prefix compaction after the rewrite has physically applied
    /// every deletion with `delete_seq <= cutoff` (= `T`): those tombstones are
    /// now dead, while `> cutoff` tombstones still apply to the higher-than-`T`
    /// files the new snapshot references in place. The bloom and all derived
    /// counters/ranges are recomputed over the survivors. See
    /// [`LayeredRuns::prune_deletes_at_or_below`] for the per-entry semantics.
    #[must_use]
    pub fn prune_deletes_at_or_below(&self, cutoff: i64) -> Self {
        Self {
            core: self
                .core
                .prune_deletes_at_or_below(cutoff, |pk| hash_key_i64(*pk)),
        }
    }
}

// =============================================================================
// Composite / non-integer primary keys
// =============================================================================

/// Splits a 128-bit key hash into its map identity and the (independent)
/// 64 bits fed to the bloom filter. Using disjoint halves keeps the bloom's
/// block/bit selection uncorrelated with the map's bucket selection (the maps
/// consume the low half via [`PrehashedBuildHasher`]).
#[inline]
fn bloom_half(hash: u128) -> u64 {
    (hash >> 64) as u64
}

/// Frozen deletion index for tables with a composite or non-integer primary key.
/// Probe keys are the byte-encoded form produced by `arrow_row::RowConverter`.
///
/// Entries are keyed by the **seeded XXH3-128 hash of the key bytes**
/// ([`hash_key_128`]) rather than the bytes themselves: the hash is the key's
/// identity and the bytes are not retained. This fixes the per-entry footprint
/// at 32 bytes regardless of composite-key width, removes byte comparisons
/// from probes, and (via [`PrehashedBuildHasher`]) lets the maps reuse the
/// hash's own entropy instead of re-hashing per probe — one hash computation
/// serves the bloom filter and the maps together.
///
/// Two distinct keys colliding under XXH3-128 would share a tombstone; at one
/// billion keys the birthday bound puts that below ~1e-20 — orders of
/// magnitude under hardware error rates. (A 64-bit hash would NOT be safe as
/// identity at these cardinalities: ~0.3% collision odds at the same scale.)
///
/// See [`DeletionIndex`] for the fused-entry, layered-storage, bloom-capacity,
/// and shared-filter contracts.
#[derive(Debug, Clone)]
pub struct KeyDeletionIndex {
    core: LayeredRuns<u128, PrehashedBuildHasher>,
}

impl Default for KeyDeletionIndex {
    fn default() -> Self {
        Self::empty()
    }
}

impl KeyDeletionIndex {
    /// An empty index.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            core: LayeredRuns::empty(),
        }
    }

    /// Process-wide shared empty index. Use this instead of
    /// `Arc::new(Self::empty())` from per-scan hot paths.
    #[must_use]
    pub fn shared_empty() -> Arc<Self> {
        static EMPTY: LazyLock<Arc<KeyDeletionIndex>> =
            LazyLock::new(|| Arc::new(KeyDeletionIndex::empty()));
        Arc::clone(&EMPTY)
    }

    /// Build a frozen index from an owned `HashMap` of `pk_bytes ->
    /// delete_sequence` with no insert records.
    #[must_use]
    pub fn from_map(deleted: HashMap<Box<[u8]>, i64>) -> Self {
        Self::from_maps(deleted, HashMap::new())
    }

    /// Build a frozen index from `pk_bytes -> delete_sequence` plus
    /// `pk_bytes -> insert_sequence` maps (the catalog load path persists the
    /// two sides separately).
    #[must_use]
    pub fn from_maps(
        deleted: HashMap<Box<[u8]>, i64>,
        insert_records: HashMap<Box<[u8]>, i64>,
    ) -> Self {
        Self {
            core: LayeredRuns::from_iters(
                deleted
                    .into_iter()
                    .map(|(key, seq)| (hash_key_128(&key), seq)),
                insert_records
                    .into_iter()
                    .map(|(key, seq)| (hash_key_128(&key), seq)),
                |key_hash| bloom_half(*key_hash),
            ),
        }
    }

    /// Build a frozen index from an `Arc<HashMap>` of deletions (clones the map).
    #[must_use]
    pub fn from_arc_map(map: &Arc<HashMap<Box<[u8]>, i64>>) -> Self {
        Self::from_map((**map).clone())
    }

    /// Total number of fused entries (keys with a deletion, an insert record,
    /// or both).
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.entry_count
    }

    /// Number of keys with a recorded deletion.
    #[must_use]
    pub fn delete_len(&self) -> usize {
        self.core.delete_count
    }

    /// Number of keys with a recorded insert (upsert re-insertion).
    #[must_use]
    pub fn insert_len(&self) -> usize {
        self.core.insert_count
    }

    /// Whether any key has a recorded deletion. See
    /// [`DeletionIndex::has_deletions`].
    #[must_use]
    pub fn has_deletions(&self) -> bool {
        self.core.delete_count > 0
    }

    /// Approximate resident bytes for memory accounting. Base entries are
    /// flat-table slots (`u128 -> (i64, i64)` payload plus `SwissTable` load
    /// factor and control bytes); delta entries carry HAMT node/bitmap/Arc
    /// overhead. Key bytes are not retained (hash-keyed identity), so the
    /// estimate no longer depends on composite-key width. Shared structure
    /// retained by older reader-pinned generations is intentionally not
    /// charged to the latest snapshot again.
    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        const APPROX_KEY_BASE_ENTRY_BYTES: usize = 40;
        const APPROX_KEY_DELTA_ENTRY_BYTES: usize = 88;
        self.core
            .approx_bytes(APPROX_KEY_BASE_ENTRY_BYTES, APPROX_KEY_DELTA_ENTRY_BYTES)
    }

    /// Whether the index has no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.entry_count == 0
    }

    /// Highest **delete** sequence number in this index, if any.
    #[must_use]
    pub fn max_sequence_number(&self) -> Option<i64> {
        self.core.max_sequence_number
    }

    // NOTE: there is deliberately NO `deleted_key_range()` here. The core's
    // `min_deleted_key`/`max_deleted_key` are over the XXH3-128 *hash* of the
    // PK bytes (the original bytes are not retained — see the module docs), so a
    // min/max over them is meaningless for PK value-range pruning and could
    // wrongly report a hash-window as disjoint from a scan's value window. The
    // composite-PK read-tax is instead attacked by the batch-level bloom sweep
    // in `KeyBasedDeletionFilterStream` (b3 sub-lever 2), which is sound because
    // `might_contain` has no false negatives.

    /// Bloom-filter check against deletion membership; see
    /// [`DeletionIndex::might_contain`].
    #[inline]
    #[must_use]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.core.bloom.might_contain(bloom_half(hash_key_128(key)))
    }

    /// Bloom-prefiltered lookup. Returns the key's [`Tombstone`] if it has a
    /// recorded deletion, `None` otherwise. See [`DeletionIndex::get`] for the
    /// insert-only-entry contract.
    ///
    /// One XXH3-128 computation serves both the bloom check (high half) and
    /// the map probes (full hash as identity, low half as bucket entropy via
    /// [`PrehashedBuildHasher`]) — the key bytes are never re-hashed or
    /// compared.
    #[inline]
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Tombstone> {
        self.get_with_min_seq(key, None)
    }

    /// Like [`get`](Self::get) but applies the protected-snapshot cutoff `S` at
    /// probe time (skips frozen runs with `max_delete_seq <= S`). `None` fuses
    /// all runs (main scan), byte-identical to [`get`](Self::get).
    #[inline]
    #[must_use]
    pub fn get_with_min_seq(&self, key: &[u8], min_delete_seq: Option<i64>) -> Option<Tombstone> {
        let key_hash = hash_key_128(key);
        let bloom_hash = bloom_half(key_hash);
        // Global bloom is a safe superset over every run's deletion keys, so a
        // miss means no tombstone at any sequence — `None` for any cutoff,
        // including a protected `Some(S)` probe. Apply the fast-reject
        // unconditionally so protected scans skip the per-run walk for
        // never-deleted keys (a HIT still applies the `Some(S)` cutoff below).
        if !self.core.bloom.might_contain(bloom_hash) {
            return None;
        }
        self.core
            .tombstone_of(&key_hash, bloom_hash, min_delete_seq)
    }

    /// Batched bloom-prefiltered lookup over row-encoded keys: invokes
    /// `on_hit(index, tombstone)` for every key (0-based position in `keys`)
    /// that has a recorded deletion, in ascending `index` order. Exactly
    /// equivalent to calling [`get`](Self::get) on each key and reporting the
    /// `Some` results — insert-only entries are reported absent, per the same
    /// contract.
    ///
    /// Mirrors [`DeletionIndex::get_batch`]: a chunked XXH3-128 hash pass
    /// feeds a tight bloom sweep (high hash half), and the delta→base tier
    /// walk runs only for the surviving candidates. The full 128-bit hash is
    /// retained per chunk, so survivors probe the maps by hash identity
    /// without re-hashing the key bytes — one hash computation per key, same
    /// as [`get`](Self::get).
    pub fn get_batch<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        mut on_hit: impl FnMut(usize, Tombstone),
    ) {
        // Deletion-keyed bloom: with no recorded deletions every probe would
        // miss (insert-only entries probe as absent), so skip the sweep.
        if self.core.delete_count == 0 {
            return;
        }
        let mut keys = keys.into_iter();
        let mut hashes: Vec<u128> = Vec::with_capacity(BATCH_SWEEP_CHUNK);
        let mut candidates: Vec<u32> = Vec::with_capacity(BATCH_SWEEP_CHUNK);
        let mut chunk_base = 0_usize;
        loop {
            hashes.clear();
            hashes.extend(
                keys.by_ref()
                    .take(BATCH_SWEEP_CHUNK)
                    .map(|k| hash_key_128(k.as_ref())),
            );
            if hashes.is_empty() {
                break;
            }
            candidates.clear();
            // Pass 1: bloom sweep over the chunk's hashes — no tier walk in
            // the loop body.
            for (i, &key_hash) in (0_u32..).zip(hashes.iter()) {
                if self.core.bloom.might_contain(bloom_half(key_hash)) {
                    candidates.push(i);
                }
            }
            // Pass 2: tier walk for the bloom survivors only (probed by the
            // retained hash identity; false positives resolve to "absent").
            for &i in &candidates {
                if let Some(tombstone) = self.core.tombstone_of(
                    &hashes[i as usize],
                    bloom_half(hashes[i as usize]),
                    None,
                ) {
                    on_hit(chunk_base + i as usize, tombstone);
                }
            }
            chunk_base += hashes.len();
        }
    }

    /// Iterate all effective entries, keyed by the XXH3-128 hash of the
    /// original key bytes. Each key yields exactly once with its newest value.
    pub fn iter_entries(&self) -> impl Iterator<Item = (u128, TombstoneEntry)> + '_ {
        self.core.iter_entries()
    }

    /// Build a new index from `self`'s entries plus delete-only `additions`,
    /// taking the per-key max sequence on conflict. Keys are borrowed — the
    /// index stores their XXH3-128 hash, never the bytes.
    ///
    /// See [`DeletionIndex::extend_max_deletes`] for the layering,
    /// amortization, and shared-filter safety argument.
    #[must_use]
    pub fn extend_max_deletes<K: AsRef<[u8]>>(
        &self,
        additions: impl IntoIterator<Item = (K, i64)>,
    ) -> Self {
        Self {
            core: self.core.extend(
                additions.into_iter().map(|(key, delete_seq)| {
                    (hash_key_128(key.as_ref()), delete_seq, SEQUENCE_ABSENT)
                }),
                |key_hash| bloom_half(*key_hash),
            ),
        }
    }

    /// Build a new index recording an upsert-conflict batch; see
    /// [`DeletionIndex::extend_max_conflicts`].
    #[must_use]
    pub fn extend_max_conflicts<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        delete_sequence: i64,
        insert_sequence: i64,
    ) -> Self {
        Self {
            core: self.core.extend(
                keys.into_iter()
                    .map(|key| (hash_key_128(key.as_ref()), delete_sequence, insert_sequence)),
                |key_hash| bloom_half(*key_hash),
            ),
        }
    }

    /// Build a new index with every **deletion** at or below `cutoff` removed;
    /// see [`DeletionIndex::prune_deletes_at_or_below`].
    #[must_use]
    pub fn prune_deletes_at_or_below(&self, cutoff: i64) -> Self {
        Self {
            core: self
                .core
                .prune_deletes_at_or_below(cutoff, |key_hash| bloom_half(*key_hash)),
        }
    }
}

#[cfg(test)]
mod tests {
    // Random u64 test keys are intentionally bit-cast to i64 (wrap is fine for keys).
    #![allow(clippy::cast_possible_wrap)]
    use super::*;

    fn byte_key(value: u64) -> Box<[u8]> {
        value.to_be_bytes().to_vec().into_boxed_slice()
    }

    fn delete_seq_of(idx: &DeletionIndex, pk: i64) -> Option<i64> {
        idx.get(pk).map(|t| t.delete_sequence)
    }

    fn key_delete_seq_of(idx: &KeyDeletionIndex, key: &[u8]) -> Option<i64> {
        idx.get(key).map(|t| t.delete_sequence)
    }

    // ---- Differential oracle for the seq-partitioned runs core (rank-1) ----

    /// Per-side max into the naive reference (`SEQUENCE_ABSENT` = unset side).
    fn naive_merge(
        naive: &mut std::collections::BTreeMap<i64, (i64, i64)>,
        key: i64,
        d: i64,
        i: i64,
    ) {
        let e = naive
            .entry(key)
            .or_insert((SEQUENCE_ABSENT, SEQUENCE_ABSENT));
        if d != SEQUENCE_ABSENT && (e.0 == SEQUENCE_ABSENT || d > e.0) {
            e.0 = d;
        }
        if i != SEQUENCE_ABSENT && (e.1 == SEQUENCE_ABSENT || i > e.1) {
            e.1 = i;
        }
    }

    /// Replicates `filter_exec::is_pk_visible_*` + `tombstone_visible` so the
    /// oracle validates the exact visibility the scan path computes.
    fn oracle_visible(t: Option<Tombstone>, apply: bool, min_delete_seq: Option<i64>) -> bool {
        match t {
            None => true,
            Some(t) => {
                if min_delete_seq.is_some_and(|m| t.delete_sequence <= m) {
                    return true;
                }
                if apply {
                    t.insert_sequence.is_some_and(|i| i > t.delete_sequence)
                } else {
                    false
                }
            }
        }
    }

    fn naive_tombstone(entry: Option<&(i64, i64)>) -> Option<Tombstone> {
        entry.and_then(|(d, i)| {
            (*d != SEQUENCE_ABSENT).then_some(Tombstone {
                delete_sequence: *d,
                insert_sequence: (*i != SEQUENCE_ABSENT).then_some(*i),
            })
        })
    }

    #[test]
    fn layered_runs_matches_naive_reference_under_random_ops() {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rng = || {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            s.wrapping_mul(0x2545_F491_4F6C_DD1D)
        };
        let key_space = 80i64;

        for trial in 0..50u64 {
            let mut idx: LayeredRuns<i64, DeletionIndexHasher> = LayeredRuns::empty();
            let mut naive: std::collections::BTreeMap<i64, (i64, i64)> =
                std::collections::BTreeMap::new();
            let mut seq: i64 = 0;
            let ops = 40 + rng() % 80;

            for _ in 0..ops {
                match rng() % 12 {
                    0 => {
                        // Re-bootstrap via from_iters (deduped, mirroring from_maps).
                        let mut dmap: std::collections::HashMap<i64, i64> =
                            std::collections::HashMap::new();
                        let mut imap: std::collections::HashMap<i64, i64> =
                            std::collections::HashMap::new();
                        let n = rng() % 40;
                        for _ in 0..n {
                            let key = (rng() as i64).rem_euclid(key_space);
                            seq += 1;
                            dmap.entry(key)
                                .and_modify(|e| *e = (*e).max(seq))
                                .or_insert(seq);
                            if rng() % 3 == 0 {
                                seq += 1;
                                imap.entry(key)
                                    .and_modify(|e| *e = (*e).max(seq))
                                    .or_insert(seq);
                            }
                        }
                        naive.clear();
                        for (k, v) in &dmap {
                            naive_merge(&mut naive, *k, *v, SEQUENCE_ABSENT);
                        }
                        for (k, v) in &imap {
                            naive_merge(&mut naive, *k, SEQUENCE_ABSENT, *v);
                        }
                        idx = LayeredRuns::from_iters(
                            dmap.iter().map(|(k, v)| (*k, *v)),
                            imap.iter().map(|(k, v)| (*k, *v)),
                            |pk| hash_key_i64(*pk),
                        );
                    }
                    1..=6 => {
                        // Delete burst.
                        let n = 1 + rng() % 8;
                        let mut adds = Vec::new();
                        for _ in 0..n {
                            let key = (rng() as i64).rem_euclid(key_space);
                            seq += 1;
                            adds.push((key, seq, SEQUENCE_ABSENT));
                            naive_merge(&mut naive, key, seq, SEQUENCE_ABSENT);
                        }
                        idx = idx.extend(adds.into_iter(), |pk| hash_key_i64(*pk));
                    }
                    7 => {
                        // Seq-prefix prune: drop deletions at or below a cutoff
                        // (the seq-prefix-compaction bake). Mirror the same cut on
                        // the naive reference: clear a delete side <= cutoff, keep
                        // the insert side, drop now-empty entries.
                        let cutoff = match rng() % 5 {
                            0 => -1,       // no-op floor (no real seq is <= -1)
                            1 => 0,        // boundary
                            2 => seq,      // drop everything recorded so far
                            3 => i64::MAX, // drop all deletions
                            // A mid-range cutoff in [0, seq]. `rng() % _` keeps the
                            // operand non-negative, so the i64 conversion is exact.
                            _ => {
                                i64::try_from(rng() % (u64::try_from(seq.max(0)).unwrap_or(0) + 1))
                                    .unwrap_or(0)
                            }
                        };
                        idx = idx.prune_deletes_at_or_below(cutoff, |pk| hash_key_i64(*pk));
                        for (d, _) in naive.values_mut() {
                            if *d != SEQUENCE_ABSENT && *d <= cutoff {
                                *d = SEQUENCE_ABSENT;
                            }
                        }
                        naive.retain(|_, (d, i)| *d != SEQUENCE_ABSENT || *i != SEQUENCE_ABSENT);
                    }
                    _ => {
                        // Upsert conflicts; sometimes inject an OLD delete seq
                        // (mirrors extend_max_conflicts grouping by an existing
                        // tombstone's delete_sequence — breaks tier monotonicity).
                        let n = 1 + rng() % 6;
                        let mut adds = Vec::new();
                        for _ in 0..n {
                            let key = (rng() as i64).rem_euclid(key_space);
                            let d = if seq > 5 && rng() % 4 == 0 {
                                1 + (rng() as i64).rem_euclid(seq)
                            } else {
                                seq += 1;
                                seq
                            };
                            seq += 1;
                            let ins = seq;
                            adds.push((key, d, ins));
                            naive_merge(&mut naive, key, d, ins);
                        }
                        idx = idx.extend(adds.into_iter(), |pk| hash_key_i64(*pk));
                    }
                }

                // ---- invariants vs the naive reference ----
                let nonempty: std::collections::BTreeMap<i64, (i64, i64)> = naive
                    .iter()
                    .filter(|(_, (d, i))| *d != SEQUENCE_ABSENT || *i != SEQUENCE_ABSENT)
                    .map(|(k, v)| (*k, *v))
                    .collect();
                assert_eq!(
                    idx.entry_count,
                    nonempty.len(),
                    "trial {trial}: entry_count"
                );
                assert_eq!(
                    idx.delete_count,
                    nonempty
                        .values()
                        .filter(|(d, _)| *d != SEQUENCE_ABSENT)
                        .count(),
                    "trial {trial}: delete_count"
                );
                assert_eq!(
                    idx.insert_count,
                    nonempty
                        .values()
                        .filter(|(_, i)| *i != SEQUENCE_ABSENT)
                        .count(),
                    "trial {trial}: insert_count"
                );
                assert_eq!(
                    idx.max_sequence_number,
                    nonempty
                        .values()
                        .filter_map(|(d, _)| (*d != SEQUENCE_ABSENT).then_some(*d))
                        .max(),
                    "trial {trial}: max_sequence_number"
                );
                let del_keys: Vec<i64> = nonempty
                    .iter()
                    .filter_map(|(k, (d, _))| (*d != SEQUENCE_ABSENT).then_some(*k))
                    .collect();
                let naive_range = del_keys.iter().min().map(|mn| {
                    (
                        *mn,
                        *del_keys.iter().max().expect("non-empty: min() was Some"),
                    )
                });
                let idx_range = idx
                    .min_deleted_key
                    .map(|mn| (mn, idx.max_deleted_key.expect("max set when min is Some")));
                assert_eq!(idx_range, naive_range, "trial {trial}: deleted_key_range");

                // iter_entries: each key exactly once, fully fused.
                let mut iter_map: std::collections::BTreeMap<i64, (i64, i64)> =
                    std::collections::BTreeMap::new();
                for (k, e) in idx.iter_entries() {
                    assert!(
                        iter_map.insert(k, (e.delete_seq, e.insert_seq)).is_none(),
                        "trial {trial}: iter_entries duplicate key {k}"
                    );
                }
                assert_eq!(iter_map, nonempty, "trial {trial}: iter_entries");

                // Run-skipping invariant: visibility matches under every cutoff,
                // for present and absent keys. (Apply,Some) is skipped — it never
                // occurs (no such ctor) and insert-side fusion isn't promised
                // under a cutoff.
                let cutoffs = [
                    None,
                    Some(-1),
                    Some(0),
                    Some(seq / 2),
                    Some(seq),
                    Some(i64::MAX),
                ];
                for probe in -2..=key_space + 2 {
                    let naive_t = naive_tombstone(naive.get(&probe));
                    for s_cut in cutoffs {
                        for apply in [false, true] {
                            if apply && s_cut.is_some() {
                                continue;
                            }
                            let core_t = idx.tombstone_of(&probe, hash_key_i64(probe), s_cut);
                            assert_eq!(
                                oracle_visible(core_t, apply, s_cut),
                                oracle_visible(naive_t, apply, s_cut),
                                "trial {trial} key {probe} cutoff {s_cut:?} apply {apply}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn from_map_then_get() {
        let mut map = HashMap::new();
        map.insert(100, 1);
        map.insert(200, 2);
        map.insert(300, 3);
        let idx = DeletionIndex::from_map(map);

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.delete_len(), 3);
        assert_eq!(idx.insert_len(), 0);
        assert_eq!(idx.max_sequence_number(), Some(3));
        assert_eq!(delete_seq_of(&idx, 100), Some(1));
        assert_eq!(delete_seq_of(&idx, 200), Some(2));
        assert_eq!(delete_seq_of(&idx, 300), Some(3));
        assert_eq!(idx.get(100).and_then(|t| t.insert_sequence), None);
        assert_eq!(idx.get(400), None);
    }

    // ---- Seq-prefix prune (phase 3: drop deletions at or below T) ----

    #[test]
    fn prune_drops_at_or_below_cutoff_keeps_above() {
        // Deletions at seqs 1,2,3,4. Bake everything <= 2 (the seq-prefix cutoff).
        let idx = DeletionIndex::from_map(HashMap::from([(10, 1), (20, 2), (30, 3), (40, 4)]));
        let pruned = idx.prune_deletes_at_or_below(2);

        // <= cutoff are gone; > cutoff survive with unchanged seqs.
        assert_eq!(delete_seq_of(&pruned, 10), None, "seq 1 baked");
        assert_eq!(
            delete_seq_of(&pruned, 20),
            None,
            "seq 2 baked (boundary <=)"
        );
        assert_eq!(delete_seq_of(&pruned, 30), Some(3), "seq 3 survives");
        assert_eq!(delete_seq_of(&pruned, 40), Some(4), "seq 4 survives");

        // Derived counters / range / max all recomputed over survivors only.
        assert_eq!(pruned.delete_len(), 2);
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned.insert_len(), 0);
        assert_eq!(pruned.max_sequence_number(), Some(4));
        assert_eq!(pruned.deleted_key_range(), Some((30, 40)));

        // The bloom is rebuilt: a baked key now misses the membership filter,
        // and a survivor still probes present (no false negative).
        assert!(!pruned.might_contain(10), "baked key dropped from bloom");
        assert!(pruned.might_contain(30), "survivor retained in bloom");
    }

    #[test]
    fn prune_keeps_insert_records_when_delete_side_dropped() {
        // Upsert conflict: key 7 deleted at seq 2, re-inserted at seq 5. A prune
        // at cutoff 3 bakes the delete (<= 3) but the re-insertion (5) must stay,
        // so the row is correctly VISIBLE (insert_seq > delete_seq, delete gone).
        let idx = DeletionIndex::empty().extend_max_conflicts([7_i64], 2, 5);
        assert_eq!(
            idx.get(7).map(|t| (t.delete_sequence, t.insert_sequence)),
            Some((2, Some(5)))
        );

        let pruned = idx.prune_deletes_at_or_below(3);
        // Delete side cleared; the entry survives as insert-only (so it is still
        // consulted, and the membership bloom no longer reports it as deleted).
        assert_eq!(delete_seq_of(&pruned, 7), None, "baked delete cleared");
        assert_eq!(pruned.delete_len(), 0);
        assert_eq!(pruned.insert_len(), 1, "re-insertion retained");
        assert_eq!(pruned.len(), 1);
        assert_eq!(
            pruned.max_sequence_number(),
            None,
            "no surviving delete seq"
        );
        assert_eq!(pruned.deleted_key_range(), None);
        assert!(
            !pruned.might_contain(7),
            "insert-only key absent from delete bloom"
        );
    }

    #[test]
    fn prune_above_cutoff_conflict_keeps_both_sides() {
        // Same conflict but delete (8) and insert (9) are BOTH above the cutoff:
        // the whole entry must be preserved verbatim.
        let idx = DeletionIndex::empty().extend_max_conflicts([7_i64], 8, 9);
        let pruned = idx.prune_deletes_at_or_below(5);
        assert_eq!(
            pruned
                .get(7)
                .map(|t| (t.delete_sequence, t.insert_sequence)),
            Some((8, Some(9))),
            "delete and insert both > cutoff survive unchanged"
        );
        assert_eq!(pruned.delete_len(), 1);
        assert_eq!(pruned.insert_len(), 1);
    }

    #[test]
    fn prune_fuses_across_tiers_before_cutoff() {
        // A key's delete can live below the cutoff in an old tier and ABOVE it in
        // a newer one. The per-key MAX (the fused value, == what a read sees) is
        // what the cutoff applies to, so the key must survive with its max seq.
        // Force two tiers: a frozen base (from_map) plus a later extend.
        let idx = DeletionIndex::from_map(HashMap::from([(5, 2)])) // base: key 5 @ seq 2
            .extend_max_deletes([(5, 9)]); // newer delete for key 5 @ seq 9
        assert_eq!(delete_seq_of(&idx, 5), Some(9), "fused max before prune");

        let pruned = idx.prune_deletes_at_or_below(4);
        assert_eq!(
            delete_seq_of(&pruned, 5),
            Some(9),
            "fused max (9) is > cutoff (4), so the key survives despite an \
             obsolete sub-cutoff delete in an older tier"
        );
        assert_eq!(pruned.delete_len(), 1);
    }

    #[test]
    fn prune_to_empty_yields_no_deletions() {
        let idx = DeletionIndex::from_map(HashMap::from([(1, 1), (2, 2), (3, 3)]));
        let pruned = idx.prune_deletes_at_or_below(i64::MAX);
        assert!(!pruned.has_deletions());
        assert_eq!(pruned.len(), 0);
        assert_eq!(pruned.delete_len(), 0);
        assert_eq!(pruned.max_sequence_number(), None);
        assert_eq!(pruned.deleted_key_range(), None);
        assert_eq!(pruned.get(1), None);
        assert!(!pruned.might_contain(1));
    }

    #[test]
    fn prune_noop_below_all_sequences_preserves_everything() {
        let idx = DeletionIndex::from_map(HashMap::from([(1, 5), (2, 6)])).extend_max_conflicts(
            [3_i64],
            7,
            8,
        );
        // Cutoff below every recorded delete seq: nothing baked.
        let pruned = idx.prune_deletes_at_or_below(0);
        assert_eq!(pruned.delete_len(), idx.delete_len());
        assert_eq!(pruned.insert_len(), idx.insert_len());
        assert_eq!(pruned.len(), idx.len());
        assert_eq!(pruned.max_sequence_number(), idx.max_sequence_number());
        assert_eq!(delete_seq_of(&pruned, 1), Some(5));
        assert_eq!(delete_seq_of(&pruned, 2), Some(6));
        assert_eq!(
            pruned
                .get(3)
                .map(|t| (t.delete_sequence, t.insert_sequence)),
            Some((7, Some(8)))
        );
    }

    #[test]
    fn prune_composite_key_index() {
        // KeyDeletionIndex (composite/non-integer PK) parity check.
        let idx = KeyDeletionIndex::from_map(HashMap::from([(byte_key(10), 1), (byte_key(20), 3)]));
        let pruned = idx.prune_deletes_at_or_below(2);
        assert_eq!(
            key_delete_seq_of(&pruned, &byte_key(10)),
            None,
            "seq 1 baked"
        );
        assert_eq!(
            key_delete_seq_of(&pruned, &byte_key(20)),
            Some(3),
            "seq 3 survives"
        );
        assert_eq!(pruned.delete_len(), 1);
        assert_eq!(pruned.max_sequence_number(), Some(3));
        assert!(!pruned.might_contain(&byte_key(10)));
        assert!(pruned.might_contain(&byte_key(20)));
    }

    #[test]
    fn prune_then_extend_round_trips() {
        // After a prune the consolidated base run must still accept new deletes
        // and fuse them correctly (the prune output is a valid index to extend).
        let idx = DeletionIndex::from_map(HashMap::from([(1, 1), (2, 2), (3, 5)]));
        let pruned = idx.prune_deletes_at_or_below(3); // keeps only key 3 @ 5
        assert_eq!(pruned.delete_len(), 1);

        let grown = pruned.extend_max_deletes([(2, 7), (9, 8)]);
        assert_eq!(delete_seq_of(&grown, 3), Some(5), "survivor intact");
        assert_eq!(delete_seq_of(&grown, 2), Some(7), "re-deleted post-prune");
        assert_eq!(delete_seq_of(&grown, 9), Some(8), "new delete");
        assert_eq!(grown.delete_len(), 3);
        assert_eq!(grown.max_sequence_number(), Some(8));
    }

    // [b3 sub-lever 1] The deleted-key min/max range is the plan-time gate that
    // lets a PK-disjoint branch skip the deletion filter. It must (a) track the
    // true min/max over the delete side, (b) never include insert-only keys,
    // and (c) be `None` when there are no deletions.
    #[test]
    fn deleted_key_range_tracks_min_max_int64() {
        let idx = DeletionIndex::from_map(HashMap::from([(100, 1), (300, 3), (50, 2)]));
        assert_eq!(idx.deleted_key_range(), Some((50, 300)));

        // Extending with deletes outside the range widens it monotonically.
        let grown = idx.extend_max_deletes([(10, 4), (400, 5)]);
        assert_eq!(grown.deleted_key_range(), Some((10, 400)));

        // Empty index has no range.
        assert_eq!(DeletionIndex::empty().deleted_key_range(), None);
    }

    #[test]
    fn deleted_key_range_ignores_insert_only() {
        // Insert-only key (no deletion) must NOT enter the deleted-key range,
        // mirroring `delete_len() == 0`.
        let idx = DeletionIndex::from_maps(HashMap::new(), HashMap::from([(7_i64, 70_i64)]));
        assert_eq!(idx.delete_len(), 0);
        assert_eq!(idx.deleted_key_range(), None);
    }

    #[test]
    fn deleted_key_range_after_delete_of_insert_only() {
        // An insert-only key gains a range entry only once it is actually
        // deleted.
        let idx = DeletionIndex::from_maps(HashMap::new(), HashMap::from([(7_i64, 70_i64)]));
        assert_eq!(idx.deleted_key_range(), None);
        let after = idx.extend_max_deletes([(7_i64, 80_i64)]);
        assert_eq!(after.deleted_key_range(), Some((7, 7)));
    }

    #[test]
    fn empty_index_probes_to_none() {
        let idx = DeletionIndex::empty();
        assert!(idx.is_empty());
        assert!(!idx.has_deletions());
        assert_eq!(idx.max_sequence_number(), None);
        assert_eq!(idx.get(42), None);
    }

    #[test]
    fn extend_max_deletes_takes_higher_sequence() {
        let mut map = HashMap::new();
        map.insert(100, 5);
        let idx = DeletionIndex::from_map(map);

        let next = idx.extend_max_deletes([(100, 3), (200, 7)]);
        assert_eq!(next.max_sequence_number(), Some(7));
        assert_eq!(delete_seq_of(&next, 100), Some(5));
        assert_eq!(delete_seq_of(&next, 200), Some(7));

        let after = next.extend_max_deletes([(100, 10)]);
        assert_eq!(after.max_sequence_number(), Some(10));
        assert_eq!(delete_seq_of(&after, 100), Some(10));
    }

    #[test]
    fn conflicts_record_delete_and_insert() {
        let idx = DeletionIndex::empty();
        let next = idx.extend_max_conflicts([1, 2, 3], 10, 11);

        assert_eq!(next.len(), 3);
        assert_eq!(next.delete_len(), 3);
        assert_eq!(next.insert_len(), 3);
        assert_eq!(
            next.max_sequence_number(),
            Some(10),
            "insert seqs must not raise the delete max"
        );
        for pk in [1, 2, 3] {
            let tombstone = next.get(pk).expect("conflict key must have a tombstone");
            assert_eq!(tombstone.delete_sequence, 10);
            assert_eq!(tombstone.insert_sequence, Some(11));
        }
    }

    #[test]
    fn conflicts_merge_max_per_side() {
        let idx = DeletionIndex::empty();
        let first = idx.extend_max_conflicts([42], 10, 11);
        // An older conflict must not lower either side.
        let second = first.extend_max_conflicts([42], 5, 6);
        let tombstone = second.get(42).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 10);
        assert_eq!(tombstone.insert_sequence, Some(11));
        assert_eq!(second.len(), 1);
        assert_eq!(second.delete_len(), 1);
        assert_eq!(second.insert_len(), 1);

        // A newer conflict raises both sides.
        let third = second.extend_max_conflicts([42], 20, 21);
        let tombstone = third.get(42).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 20);
        assert_eq!(tombstone.insert_sequence, Some(21));
    }

    #[test]
    fn insert_only_entries_are_invisible_to_get() {
        // Post-compaction state: delete files purged, insert records remain.
        let inserts: HashMap<i64, i64> = (0..10).map(|pk| (pk, pk + 100)).collect();
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);

        assert_eq!(idx.len(), 10);
        assert_eq!(idx.delete_len(), 0);
        assert_eq!(idx.insert_len(), 10);
        assert!(!idx.has_deletions());
        assert_eq!(idx.max_sequence_number(), None);
        for pk in 0..10 {
            assert_eq!(
                idx.get(pk),
                None,
                "insert-only pk={pk} must probe as absent"
            );
        }
    }

    #[test]
    fn from_maps_fuses_overlapping_keys() {
        let deleted: HashMap<i64, i64> = HashMap::from([(1, 10), (2, 20)]);
        let inserts: HashMap<i64, i64> = HashMap::from([(2, 21), (3, 30)]);
        let idx = DeletionIndex::from_maps(deleted, inserts);

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.delete_len(), 2);
        assert_eq!(idx.insert_len(), 2);
        assert_eq!(idx.max_sequence_number(), Some(20));

        let lone_delete = idx.get(1).expect("tombstone");
        assert_eq!(lone_delete.delete_sequence, 10);
        assert_eq!(lone_delete.insert_sequence, None);

        let fused = idx.get(2).expect("tombstone");
        assert_eq!(fused.delete_sequence, 20);
        assert_eq!(fused.insert_sequence, Some(21));

        assert_eq!(idx.get(3), None, "insert-only key must probe as absent");
    }

    #[test]
    fn delete_after_insert_only_enters_bloom() {
        // An existing insert-only entry gaining its first deletion must become
        // probeable (exercises the bloom insert on the occupied-entry path).
        let inserts: HashMap<i64, i64> = HashMap::from([(7, 70)]);
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);
        assert_eq!(idx.get(7), None);

        let next = idx.extend_max_deletes([(7, 80)]);
        let tombstone = next.get(7).expect("tombstone after delete");
        assert_eq!(tombstone.delete_sequence, 80);
        assert_eq!(tombstone.insert_sequence, Some(70));
        assert_eq!(next.delete_len(), 1);
        assert_eq!(
            next.len(),
            1,
            "delete of an insert-only key must not add an entry"
        );
    }

    #[test]
    fn bloom_rejects_most_misses() {
        let mut map = HashMap::new();
        for i in 0..100_i64 {
            map.insert(i * 2, i);
        }
        let idx = DeletionIndex::from_map(map);

        let mut rejects = 0;
        for i in 0..100 {
            let odd = i * 2 + 1;
            if !idx.might_contain(odd) {
                rejects += 1;
            }
        }
        assert!(
            rejects > 80,
            "expected bloom to reject most non-deleted keys, got {rejects}"
        );
    }

    /// Reference implementation for the `get_batch` equivalence tests: the
    /// scalar per-row probe the batch sweep replaces.
    fn per_row_gets_int64(idx: &DeletionIndex, pks: &[i64]) -> Vec<Option<Tombstone>> {
        pks.iter().map(|&pk| idx.get(pk)).collect()
    }

    /// Collect `get_batch` results into per-row form, asserting the callback
    /// contract along the way (strictly ascending indices, each at most once,
    /// all in bounds).
    fn batch_gets_int64(idx: &DeletionIndex, pks: &[i64]) -> Vec<Option<Tombstone>> {
        let mut out: Vec<Option<Tombstone>> = vec![None; pks.len()];
        let mut last: Option<usize> = None;
        idx.get_batch(pks, |i, tombstone| {
            assert!(i < pks.len(), "callback index out of bounds: {i}");
            assert!(
                last.is_none_or(|prev| prev < i),
                "callback indices must be strictly ascending: {last:?} then {i}"
            );
            last = Some(i);
            out[i] = Some(tombstone);
        });
        out
    }

    fn per_row_gets_key(idx: &KeyDeletionIndex, keys: &[Box<[u8]>]) -> Vec<Option<Tombstone>> {
        keys.iter().map(|key| idx.get(key)).collect()
    }

    fn batch_gets_key(idx: &KeyDeletionIndex, keys: &[Box<[u8]>]) -> Vec<Option<Tombstone>> {
        let mut out: Vec<Option<Tombstone>> = vec![None; keys.len()];
        let mut last: Option<usize> = None;
        idx.get_batch(keys.iter().map(AsRef::as_ref), |i, tombstone| {
            assert!(i < keys.len(), "callback index out of bounds: {i}");
            assert!(
                last.is_none_or(|prev| prev < i),
                "callback indices must be strictly ascending: {last:?} then {i}"
            );
            last = Some(i);
            out[i] = Some(tombstone);
        });
        out
    }

    /// `get_batch` must report exactly what per-row [`DeletionIndex::get`]
    /// reports, across a layered index (frozen base + delta + insert-only +
    /// fused conflict entries) and every batch shape the apply path produces.
    #[test]
    fn get_batch_matches_per_row_get_int64() {
        // Base tier: even keys 0..200 deleted at seq 1; key 4 also carries an
        // insert record (re-inserted at seq 50); key 1000 is insert-only.
        let deleted: HashMap<i64, i64> = (0..100).map(|i| (i * 2, 1)).collect();
        let inserts: HashMap<i64, i64> = HashMap::from([(4, 50), (1000, 70)]);
        let base = DeletionIndex::from_maps(deleted, inserts);
        // Delta tier: a small extend (below the test merge floor) so probes
        // exercise the delta→base walk, plus fused conflicts and extreme keys.
        let idx = base
            .extend_max_deletes([(3, 7), (i64::MIN, 9), (i64::MAX, 11)])
            .extend_max_conflicts([5_i64, 4], 20, 21);

        let shapes: &[&[i64]] = &[
            &[],                             // empty batch
            &[2],                            // single row, hit
            &[9999],                         // single row, miss
            &[0, 2, 4, 6, 8],                // all-hit
            &[1, 7, 9, 8887, 9999],          // all-miss (odd / out of range)
            &[0, 1, 2, 7, 4, 9, 3, 5],       // alternating hit/miss + delta hits
            &[2, 2, 9999, 2],                // duplicate keys in one batch
            &[i64::MIN, -1, 0, 1, i64::MAX], // extreme key values
            &[1000, 4, 5],                   // insert-only (absent) vs fused entries
        ];
        for pks in shapes {
            assert_eq!(
                batch_gets_int64(&idx, pks),
                per_row_gets_int64(&idx, pks),
                "get_batch diverged from per-row get for {pks:?}"
            );
        }

        // Spot-check the fused-entry payloads survive the batch path intact.
        let fused = batch_gets_int64(&idx, &[5, 4, 1000]);
        assert_eq!(
            fused[0],
            Some(Tombstone {
                delete_sequence: 20,
                insert_sequence: Some(21)
            })
        );
        // Key 4: insert record at 50 (base) fused with conflict insert at 21
        // (delta) — per-side max keeps 50; conflict delete at 20 wins over 1.
        assert_eq!(
            fused[1],
            Some(Tombstone {
                delete_sequence: 20,
                insert_sequence: Some(50)
            })
        );
        assert_eq!(fused[2], None, "insert-only key must probe as absent");
    }

    /// Batches longer than one sweep chunk must keep global (not chunk-local)
    /// indices and stay equivalent to per-row probes across the boundary.
    #[test]
    fn get_batch_spans_chunk_boundaries_int64() {
        let deleted: HashMap<i64, i64> = (0..3000).map(|i| (i * 3, i + 1)).collect();
        let idx = DeletionIndex::from_map(deleted);

        // 5000 keys = 2 full chunks + a partial tail at BATCH_SWEEP_CHUNK=2048.
        let pks: Vec<i64> = (0..5000).collect();
        assert!(pks.len() > 2 * BATCH_SWEEP_CHUNK);
        assert_eq!(batch_gets_int64(&idx, &pks), per_row_gets_int64(&idx, &pks));
    }

    /// Empty and insert-only indexes take the `delete_count == 0` early-out;
    /// it must agree with per-row probes (all absent).
    #[test]
    fn get_batch_empty_and_insert_only_indexes() {
        let pks: Vec<i64> = (0..100).collect();

        let empty = DeletionIndex::empty();
        assert_eq!(batch_gets_int64(&empty, &pks), vec![None; pks.len()]);

        let insert_only =
            DeletionIndex::from_maps(HashMap::new(), (0..50).map(|i| (i, i + 1)).collect());
        assert_eq!(batch_gets_int64(&insert_only, &pks), vec![None; pks.len()]);

        let empty_key_idx = KeyDeletionIndex::empty();
        let keys: Vec<Box<[u8]>> = (0..100_u64).map(byte_key).collect();
        assert_eq!(
            batch_gets_key(&empty_key_idx, &keys),
            vec![None; keys.len()]
        );
    }

    /// Composite-key equivalence: layered index, varying key widths (including
    /// the empty key), duplicates, and a chunk-spanning batch.
    #[test]
    fn get_batch_matches_per_row_get_composite() {
        let deleted: HashMap<Box<[u8]>, i64> = (0..100_u64).map(|i| (byte_key(i * 2), 1)).collect();
        let inserts: HashMap<Box<[u8]>, i64> = HashMap::from([(byte_key(4), 50)]);
        let base = KeyDeletionIndex::from_maps(deleted, inserts);
        let wide_key: Box<[u8]> = vec![7_u8; 24].into_boxed_slice(); // composite-width key
        let empty_key: Box<[u8]> = Vec::new().into_boxed_slice();
        let idx = base
            .extend_max_deletes([(wide_key.clone(), 7), (empty_key.clone(), 9)])
            .extend_max_conflicts([byte_key(6)], 20, 21);

        let shapes: Vec<Vec<Box<[u8]>>> = vec![
            vec![],                                             // empty batch
            vec![byte_key(2)],                                  // single hit
            vec![byte_key(3)],                                  // single miss
            (0..10_u64).map(|i| byte_key(i * 2)).collect(),     // all-hit
            (0..10_u64).map(|i| byte_key(i * 2 + 1)).collect(), // all-miss
            vec![
                byte_key(0),
                byte_key(1),
                wide_key,
                byte_key(3),
                empty_key,
                byte_key(6),
            ], // alternating + width mix + fused entry
            vec![byte_key(2), byte_key(2), byte_key(3), byte_key(2)], // duplicates
        ];
        for keys in &shapes {
            assert_eq!(
                batch_gets_key(&idx, keys),
                per_row_gets_key(&idx, keys),
                "composite get_batch diverged from per-row get"
            );
        }

        // Chunk-spanning batch (2 full chunks + tail).
        let many: Vec<Box<[u8]>> = (0..5000_u64).map(byte_key).collect();
        assert!(many.len() > 2 * BATCH_SWEEP_CHUNK);
        assert_eq!(batch_gets_key(&idx, &many), per_row_gets_key(&idx, &many));
    }

    #[test]
    fn key_index_basic() {
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        let key1: Box<[u8]> = vec![1, 2, 3].into_boxed_slice();
        let key2: Box<[u8]> = vec![4, 5, 6].into_boxed_slice();
        map.insert(key1.clone(), 1);
        map.insert(key2.clone(), 2);

        let idx = KeyDeletionIndex::from_map(map);
        assert_eq!(idx.max_sequence_number(), Some(2));
        assert_eq!(key_delete_seq_of(&idx, &key1), Some(1));
        assert_eq!(key_delete_seq_of(&idx, &key2), Some(2));
        assert_eq!(idx.get(&[7, 8, 9]), None);
    }

    #[test]
    fn key_index_extend_max_deletes() {
        let key1: Box<[u8]> = vec![1, 2].into_boxed_slice();
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        map.insert(key1.clone(), 5);
        let idx = KeyDeletionIndex::from_map(map);

        let next = idx.extend_max_deletes([(key1.clone(), 3)]);
        assert_eq!(next.max_sequence_number(), Some(5));
        assert_eq!(key_delete_seq_of(&next, &key1), Some(5));

        let after = next.extend_max_deletes([(key1.clone(), 10)]);
        assert_eq!(after.max_sequence_number(), Some(10));
        assert_eq!(key_delete_seq_of(&after, &key1), Some(10));
    }

    #[test]
    fn key_index_conflicts_and_insert_only_fusion() {
        let idx = KeyDeletionIndex::empty();
        let next = idx.extend_max_conflicts([byte_key(1), byte_key(2)], 10, 11);
        assert_eq!(next.delete_len(), 2);
        assert_eq!(next.insert_len(), 2);
        let tombstone = next.get(&byte_key(1)).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 10);
        assert_eq!(tombstone.insert_sequence, Some(11));

        // Insert-only entries from the catalog load path probe as absent until deleted.
        let loaded =
            KeyDeletionIndex::from_maps(HashMap::new(), HashMap::from([(byte_key(9), 90_i64)]));
        assert_eq!(loaded.get(&byte_key(9)), None);
        assert!(!loaded.has_deletions());
        let deleted = loaded.extend_max_deletes([(byte_key(9), 95)]);
        let tombstone = deleted.get(&byte_key(9)).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 95);
        assert_eq!(tombstone.insert_sequence, Some(90));
    }

    #[test]
    fn key_index_pinned_generation_is_unaffected_by_extends() {
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        for value in 0_u16..512 {
            map.insert(
                value.to_be_bytes().to_vec().into_boxed_slice(),
                i64::from(value),
            );
        }
        let idx = KeyDeletionIndex::from_map(map);

        let pinned_reader_generation = idx.clone();
        let next =
            idx.extend_max_deletes([(999_u16.to_be_bytes().to_vec().into_boxed_slice(), 999)]);

        // The pinned generation never sees the new key's entry; the new
        // generation sees both old and new entries.
        assert_eq!(
            pinned_reader_generation.get(&999_u16.to_be_bytes()),
            None,
            "pinned generation must not observe later extends"
        );
        assert_eq!(
            key_delete_seq_of(&pinned_reader_generation, &0_u16.to_be_bytes()),
            Some(0)
        );
        assert_eq!(key_delete_seq_of(&next, &0_u16.to_be_bytes()), Some(0));
        assert_eq!(key_delete_seq_of(&next, &999_u16.to_be_bytes()), Some(999));
    }

    #[test]
    fn key_index_identity_is_content_based_not_allocation_based() {
        // Hash-keyed identity: equal key bytes from different allocations
        // resolve to the same entry.
        let key: Box<[u8]> = vec![9, 9].into_boxed_slice();
        let mut map: HashMap<Box<[u8]>, i64> = HashMap::new();
        map.insert(key, 1);
        let idx = KeyDeletionIndex::from_map(map);

        let replacement_key: Box<[u8]> = vec![9, 9].into_boxed_slice();
        let next = idx.extend_max_deletes([(replacement_key, 5)]);

        assert_eq!(next.len(), 1, "equal bytes must merge into one entry");
        assert_eq!(key_delete_seq_of(&idx, &[9, 9]), Some(1));
        assert_eq!(key_delete_seq_of(&next, &[9, 9]), Some(5));
    }

    // -------------------------------------------------------------------------
    // Regression tests for the incremental bloom-update path.
    //
    // A previous revision rebuilt the bloom filter from scratch on every
    // extend call (iterating ALL entries and re-hashing them). On high-rate
    // upsert/delete workloads this turned every per-row cache update into
    // O(N) work, where N is the cumulative deletion-cache size. The
    // cumulative effect across M writes is O(M*N), which is the root cause
    // of the ingestion regression the user reported (~200% on upsert-heavy
    // workloads with growing deletion sets).
    //
    // The fix rebuilds the bloom only when the deletion count outgrows the
    // sized capacity (which carries 2x headroom, so the cadence is geometric
    // / amortized O(K)) and inserts into the shared filter in place in
    // between. These tests exercise both code paths and verify correctness
    // across many extend cycles.
    // -------------------------------------------------------------------------

    #[test]
    fn extend_many_small_batches_preserves_all_entries() {
        // Simulates many small upserts each adding a single new PK to the
        // cache — the exact pattern that exposed the O(N²) regression. With
        // the test-sized DELTA_MERGE_MIN this also crosses several base
        // merges, covering the layered path end to end.
        let mut idx = DeletionIndex::empty();
        let n = 1024;
        for pk in 0_i64..n {
            idx = idx.extend_max_deletes([(pk, pk + 1)]);
        }
        assert_eq!(i64::try_from(idx.len()).expect("len fits in i64"), n);
        for pk in 0_i64..n {
            assert_eq!(
                delete_seq_of(&idx, pk),
                Some(pk + 1),
                "missing entry for pk={pk} after {n} incremental extends",
            );
        }
        // A key never inserted must not be reported as present.
        assert_eq!(idx.get(n + 100), None);
    }

    #[test]
    fn extend_rebuilds_bloom_with_headroom() {
        // Verify the bloom is rebuilt when the deletion count crosses the
        // sized capacity, and that the new filter takes 2x headroom so it
        // never runs past its design FPR between rebuilds (geometric
        // amortization).
        let mut idx = DeletionIndex::empty();
        assert_eq!(idx.core.bloom_capacity, MIN_BLOOM_CAPACITY);

        // 64 deletions fit the minimum capacity exactly: no rebuild.
        for pk in 0..64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert_eq!(idx.len(), 64);
        assert_eq!(
            idx.core.bloom_capacity, MIN_BLOOM_CAPACITY,
            "no rebuild expected before exceeding the sized capacity"
        );

        // The 65th deletion crosses the sized capacity: rebuild with headroom.
        idx = idx.extend_max_deletes([(64, 1)]);
        assert_eq!(idx.len(), 65);
        assert_eq!(
            idx.core.bloom_capacity, 130,
            "rebuild must size the new bloom for 2x the deletion count"
        );

        // Keep growing to force another rebuild cycle at the next boundary.
        for pk in 65..200 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert!(
            idx.core.bloom_capacity >= 200,
            "bloom_capacity must stay ahead of {} deletions, got {}",
            idx.delete_len(),
            idx.core.bloom_capacity,
        );

        // Every inserted key probes positive.
        for pk in 0..200 {
            assert_eq!(
                delete_seq_of(&idx, pk),
                Some(1),
                "missing pk={pk} after rebuilds"
            );
        }
    }

    #[test]
    fn bloom_rebuild_sizes_by_deletion_count_not_total_entries() {
        // Insert-only entries must not count against the bloom capacity: the
        // filter is keyed on deletion membership.
        let inserts: HashMap<i64, i64> = (0..1000).map(|pk| (pk, 1)).collect();
        let idx = DeletionIndex::from_maps(HashMap::new(), inserts);
        assert_eq!(idx.core.bloom_capacity, MIN_BLOOM_CAPACITY);

        // 64 deletions still fit the minimum capacity despite 1000 entries.
        let mut grown = idx;
        for pk in 0..64 {
            grown = grown.extend_max_deletes([(pk, 2)]);
        }
        assert_eq!(grown.core.bloom_capacity, MIN_BLOOM_CAPACITY);
        assert_eq!(grown.delete_len(), 64);
        assert_eq!(grown.len(), 1000);
    }

    #[test]
    fn extend_preserves_max_sequence_under_repeated_updates() {
        // Same PK updated many times — every extend should preserve the max
        // sequence seen so far. Tests the occupied-entry path.
        let mut idx = DeletionIndex::empty();
        idx = idx.extend_max_deletes([(42, 100)]);
        idx = idx.extend_max_deletes([(42, 50)]); // older write, should not override
        idx = idx.extend_max_deletes([(42, 200)]); // newer write, takes max
        idx = idx.extend_max_deletes([(42, 150)]); // older write, should not override
        assert_eq!(delete_seq_of(&idx, 42), Some(200));
        assert_eq!(idx.len(), 1, "no new entry should have been added");
    }

    #[test]
    fn key_index_extend_many_small_batches_preserves_all_entries() {
        // Same regression case for byte-keyed (composite-PK) tables.
        let mut idx = KeyDeletionIndex::empty();
        let n = 256_usize;
        for i in 0..n {
            let key = byte_key(i as u64);
            idx = idx.extend_max_deletes([(key, i64::try_from(i).expect("i fits in i64") + 1)]);
        }
        assert_eq!(idx.len(), n);
        for i in 0..n {
            let key = byte_key(i as u64);
            assert_eq!(
                key_delete_seq_of(&idx, &key),
                Some(i64::try_from(i).expect("i fits in i64") + 1),
                "missing entry for key i={i} after {n} incremental extends",
            );
        }
    }

    #[test]
    fn extend_batch_only_pays_for_new_keys() {
        // When all additions are duplicates (already present), no new bloom
        // inserts should happen — verified indirectly by checking the
        // bloom_capacity is unchanged and queries still work.
        let mut map = HashMap::new();
        for pk in 0..32 {
            map.insert(pk, 1_i64);
        }
        let idx = DeletionIndex::from_map(map);
        let initial_cap = idx.core.bloom_capacity;

        // Extend with all-duplicate keys (different seq, but occupied path).
        let next = idx.extend_max_deletes((0..32).map(|pk| (pk, 2_i64)));
        assert_eq!(next.core.bloom_capacity, initial_cap);
        assert_eq!(next.len(), 32);
        for pk in 0..32 {
            assert_eq!(
                delete_seq_of(&next, pk),
                Some(2),
                "max-sequence update lost for pk={pk}"
            );
        }
    }

    #[test]
    fn extend_shares_bloom_until_rebuild() {
        // The common path must share the parent's filter (no per-call copy of
        // the bit array); a rebuild must allocate a fresh one.
        let idx = DeletionIndex::from_map((0..32).map(|pk| (pk, 1_i64)).collect());

        let extended = idx.extend_max_deletes([(100, 2)]);
        assert!(
            Arc::ptr_eq(&idx.core.bloom, &extended.core.bloom),
            "in-capacity extend must share the parent's bloom filter"
        );

        // Old generation sees a superset: the new key's bits are visible, but
        // a map probe correctly reports it absent from the old generation.
        assert!(idx.might_contain(100));
        assert_eq!(idx.get(100), None);
        assert_eq!(delete_seq_of(&extended, 100), Some(2));

        // Grow past the sized capacity (64): the rebuild must not alias the
        // shared filter.
        let mut grown = extended;
        for pk in 200..280 {
            grown = grown.extend_max_deletes([(pk, 3)]);
        }
        assert!(
            !Arc::ptr_eq(&idx.core.bloom, &grown.core.bloom),
            "rebuild must allocate a fresh filter"
        );
        assert_eq!(delete_seq_of(&grown, 100), Some(2));
        assert_eq!(delete_seq_of(&grown, 250), Some(3));
    }

    // -------------------------------------------------------------------------
    // Layered-storage tests (frozen runs + active). DELTA_MERGE_MIN is 64 under
    // cfg(test), so ordinary insert counts cross freeze boundaries.
    // -------------------------------------------------------------------------

    #[test]
    fn active_freezes_into_run_at_threshold() {
        // Single-key extends accumulate in `active` until the freeze threshold,
        // then freeze into a new frozen run (O(1), no fold).
        let mut idx = DeletionIndex::empty();
        for pk in 0..63_i64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        assert_eq!(idx.core.active.len(), 63, "active below threshold");
        assert_eq!(idx.core.runs.len(), 0);

        idx = idx.extend_max_deletes([(63, 1)]);
        assert_eq!(idx.core.active.len(), 0, "active frozen into a run");
        assert_eq!(idx.core.runs.len(), 1);
        assert_eq!(idx.core.runs[0].map.len(), 64);

        // Everything probeable through both tiers' lifetimes.
        for pk in 0..64 {
            assert_eq!(delete_seq_of(&idx, pk), Some(1), "missing pk={pk}");
        }
        assert_eq!(idx.len(), 64);
    }

    #[test]
    fn run_key_update_lands_in_active_and_wins() {
        // Force keys into a frozen run, then update one: the merged entry lands
        // in `active` and overrides the run on probes, without changing counts.
        let idx = DeletionIndex::from_map((0..100).map(|pk| (pk, 10_i64)).collect());
        assert_eq!(idx.core.runs.len(), 1);
        assert_eq!(idx.core.runs[0].map.len(), 100);
        assert_eq!(idx.core.active.len(), 0);

        let updated = idx.extend_max_conflicts([5], 20, 21);
        assert_eq!(updated.core.runs[0].map.len(), 100, "run stays frozen");
        assert_eq!(updated.core.active.len(), 1, "update lands in active");
        assert_eq!(updated.len(), 100, "no new entry for an existing key");
        assert_eq!(updated.delete_len(), 100);
        assert_eq!(updated.insert_len(), 1);

        let tombstone = updated.get(5).expect("tombstone");
        assert_eq!(tombstone.delete_sequence, 20, "active overrides the run");
        assert_eq!(tombstone.insert_sequence, Some(21));

        // A stale update is a no-op and must not grow active.
        let stale = updated.extend_max_deletes([(5, 15)]);
        assert_eq!(
            stale.core.active.len(),
            1,
            "no-op update must not re-insert"
        );
        assert_eq!(stale.get(5).expect("tombstone").delete_sequence, 20);
    }

    #[test]
    fn pinned_generation_survives_freeze() {
        // A reader pinning a pre-freeze generation must keep its exact view
        // while the writer accumulates + freezes active into new runs.
        let mut idx = DeletionIndex::empty();
        for pk in 0..50_i64 {
            idx = idx.extend_max_deletes([(pk, 1)]);
        }
        let pinned = idx.clone();
        assert_eq!(pinned.core.runs.len(), 0, "pinned generation is pre-freeze");

        // Cross the freeze threshold.
        for pk in 50..130_i64 {
            idx = idx.extend_max_deletes([(pk, 2)]);
        }
        assert!(
            !idx.core.runs.is_empty(),
            "writer must have frozen active into a run"
        );
        assert_eq!(
            pinned.core.runs.len(),
            0,
            "pinned generation unaffected by the writer's freeze"
        );

        // Pinned view: first 50 keys only.
        for pk in 0..50 {
            assert_eq!(delete_seq_of(&pinned, pk), Some(1));
        }
        assert_eq!(pinned.get(75), None, "pinned view must not see later keys");
        assert_eq!(pinned.len(), 50);

        // New view: everything.
        for pk in 0..50 {
            assert_eq!(delete_seq_of(&idx, pk), Some(1));
        }
        for pk in 50..130 {
            assert_eq!(delete_seq_of(&idx, pk), Some(2));
        }
        assert_eq!(idx.len(), 130);
    }

    #[test]
    fn counters_consistent_across_merges() {
        // Interleave fresh keys, repeat updates, and conflicts across several
        // merge boundaries; counters must match a straightforward model.
        let mut idx = DeletionIndex::empty();
        let mut model: HashMap<i64, (i64, Option<i64>)> = HashMap::new();

        for round in 0..6_i64 {
            for slot in 0..40_i64 {
                let pk = round * 30 + slot; // overlapping ranges → repeat keys
                let delete_seq = round + 1;
                let insert_seq = round + 2;
                idx = idx.extend_max_conflicts([pk], delete_seq, insert_seq);
                let entry = model.entry(pk).or_insert((SEQUENCE_ABSENT, None));
                entry.0 = entry.0.max(delete_seq);
                entry.1 = Some(
                    entry
                        .1
                        .map_or(insert_seq, |existing: i64| existing.max(insert_seq)),
                );
            }
        }

        assert_eq!(idx.len(), model.len());
        assert_eq!(idx.delete_len(), model.len());
        assert_eq!(idx.insert_len(), model.len());
        for (pk, (delete_seq, insert_seq)) in &model {
            let tombstone = idx.get(*pk).expect("tombstone");
            assert_eq!(tombstone.delete_sequence, *delete_seq, "pk={pk}");
            assert_eq!(tombstone.insert_sequence, *insert_seq, "pk={pk}");
        }
    }

    #[test]
    fn iter_entries_yields_latest_values_without_duplicates() {
        // Keys split across a frozen run and active (recent update of a run
        // key + a brand-new key): iteration must yield each key once with its
        // newest value.
        let idx = DeletionIndex::from_map((0..100).map(|pk| (pk, 1_i64)).collect());
        let idx = idx.extend_max_deletes([(5, 50), (200, 60)]);
        assert!(
            idx.core.active.len() >= 2,
            "updates must be active-resident"
        );

        let collected: HashMap<i64, TombstoneEntry> = idx.iter_entries().collect();
        assert_eq!(collected.len(), 101);
        assert_eq!(
            idx.iter_entries().count(),
            101,
            "no duplicate keys in iteration"
        );
        assert_eq!(collected[&5].delete_sequence(), Some(50), "delta wins");
        assert_eq!(collected[&200].delete_sequence(), Some(60));
        assert_eq!(collected[&6].delete_sequence(), Some(1), "base preserved");
    }
}
