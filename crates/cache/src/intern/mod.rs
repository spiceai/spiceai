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

//! A content-keyed, weakly-held pool of shared allocations.
//!
//! Lives in this crate because the cache is what retains the values worth
//! sharing: a schema is minted per plan and a table set per query, but only a
//! cached entry keeps either past the query that made it. Interning anywhere
//! higher would hash values that are about to be dropped.
//!
//! The problem it solves is the same wherever it appears: something builds a
//! fresh allocation per operation whose *contents* are one of a handful of
//! shapes, and something else retains one of those per item. N retained items
//! then hold N copies of one shape. Handing each retainer the pool's copy
//! instead leaves one allocation and N pointers.
//!
//! Rows are [`Weak`], so the pool never keeps a value alive on its own: the
//! bytes belong to whichever holders are using them, and when the last one goes
//! the row becomes a tombstone that the next sweep drops. That is what makes it
//! safe to hold without a bound — there is no eviction policy to get wrong,
//! because the pool is not what is holding the memory.
//!
//! Interning is by *content*, resolved with `Eq` rather than by hash alone, so
//! two values that collide are never conflated. [`Internable::content_hash`]
//! only has to be a good bucket key.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

/// Number of independently-locked shards.
///
/// Sharded because the content-equality check is a deep comparison held under
/// the lock; see the module docs. Fixed rather than CPU-derived because the
/// pool is touched once per retained item, not per row of data, so the count
/// only needs to keep unrelated shapes off a single lock.
pub(crate) const SHARDS: usize = 16;

/// Attempts a shard tolerates before sweeping its tombstones.
///
/// A sweep walks the whole shard, so it is amortised across attempts rather
/// than run on each one.
pub(crate) const SWEEP_INTERVAL: usize = 256;

/// How many times larger than its live contents a collection may be before a
/// sweep reallocates it.
///
/// `retain` removes rows but never gives back the capacity they occupied, so
/// without this a burst of short-lived shapes would leave every shard holding
/// its peak allocation for the process lifetime — which is exactly what a
/// weakly-held pool promises not to do. Shrinking only past a slack factor
/// keeps a steady-state workload from reallocating on every sweep.
const CAPACITY_SLACK: usize = 4;

/// A value that came out of an [`Interner`], and so is shared with every other
/// holder of the same content.
///
/// The point is what it cannot be built from: there is no way to make one from
/// an arbitrary `Arc<T>`, only by interning. A cache entry that stores its
/// schema and table set as `Interned` therefore cannot hold a private copy, so
/// the weigher's decision not to charge for them stays true no matter how many
/// constructors the entry grows. Before this, that decision rested on every
/// call site remembering to intern — the class of mistake CLAUDE.md's
/// *Trait evolution & wrapper delegation* section is about, where the wiring
/// compiles and silently no-ops.
#[derive(Debug)]
pub struct Interned<T>(Arc<T>);

impl<T> Interned<T> {
    /// The shared allocation, for a caller that needs the `Arc` itself.
    #[must_use]
    pub fn arc(&self) -> Arc<T> {
        Arc::clone(&self.0)
    }
}

impl<T> Clone for Interned<T> {
    // Derived `Clone` would demand `T: Clone`; sharing the `Arc` is the point.
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> std::ops::Deref for Interned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> AsRef<T> for Interned<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

/// A value the pool can share: comparable by content, and able to say how much
/// memory it occupies.
///
/// Implemented for foreign types (`Schema`, `HashSet<TableReference>`), which is
/// why it lives beside the pool rather than beside its users — the orphan rule
/// allows a local trait on a foreign type, but not the reverse.
pub trait Internable: Eq + Send + Sync + 'static {
    /// Hashes the value's contents. Must agree with `Eq`: values that compare
    /// equal must hash equal. The reverse need not hold — a collision costs one
    /// extra comparison, not a wrong answer.
    fn content_hash<H: Hasher>(&self, state: &mut H);

    /// The deep size of the value, in bytes: the struct plus everything it owns
    /// through a pointer. Reported so the memory the pool shares stays visible
    /// to whoever stopped charging for it per item.
    fn deep_size(&self) -> usize;
}

/// What the pool holds, and what it has done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InternerStats {
    /// Rows currently held — one per distinct value.
    pub rows: usize,
    /// Total deep size of the values those rows point at.
    ///
    /// This is the memory that per-item accounting no longer charges. The pool
    /// holds these values weakly, so the bytes belong to whichever holders keep
    /// them alive; reporting them here is what keeps the saving visible rather
    /// than invisible.
    pub value_bytes: usize,
    /// The pool's own retained allocation: its hash-map slots and bucket
    /// vectors, measured from capacity rather than from live rows so that
    /// capacity left behind by a past burst is visible.
    pub self_bytes: usize,
    /// Interns that collapsed a *distinct* allocation onto the shared one.
    ///
    /// This is the only counter that evidences the pool doing its job: each one
    /// is a duplicate that existed a moment ago and does not now.
    pub collapsed: u64,
    /// Interns whose caller already held the shared allocation.
    ///
    /// Content-equal *and* pointer-equal, so there was no duplicate to remove.
    /// Counted apart from [`Self::collapsed`] because lumping the two together
    /// would let a pool that collapses nothing look busy: a caller re-interning
    /// a value it already had would register as a hit.
    pub already_shared: u64,
    /// Interns that adopted a value the pool had not seen.
    pub misses: u64,
}

/// One row: a weak handle plus the size of the value it points at.
///
/// The size is kept alongside so a sweep can discount a dead row without
/// upgrading it — by then there is nothing left to measure.
pub(crate) struct Row<T> {
    pub(crate) value: Weak<T>,
    pub(crate) value_size: usize,
}

pub(crate) struct Shard<T> {
    /// Content hash -> candidate rows. A bucket holds more than one row only on
    /// a hash collision, which content equality then resolves.
    pub(crate) buckets: HashMap<u64, Vec<Row<T>>>,
    /// Interning attempts since this shard was last swept.
    since_sweep: usize,
}

// Derived `Default` would demand `T: Default`, which the interned value need
// not be.
impl<T> Default for Shard<T> {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            since_sweep: 0,
        }
    }
}

impl<T> Shard<T> {
    /// Drops rows whose last holder is gone, and any bucket left empty, then
    /// returns the capacity the survivors no longer need.
    fn sweep(&mut self) {
        self.buckets.retain(|_, candidates| {
            candidates.retain(|row| row.value.strong_count() > 0);
            if candidates.capacity() > candidates.len().saturating_mul(CAPACITY_SLACK) {
                candidates.shrink_to_fit();
            }
            !candidates.is_empty()
        });
        if self.buckets.capacity() > self.buckets.len().saturating_mul(CAPACITY_SLACK) {
            self.buckets.shrink_to_fit();
        }
        self.since_sweep = 0;
    }

    /// Rows this shard holds, live and dead alike.
    ///
    /// Test-only: production reads what the pool *shares*, via
    /// [`Self::live_counts`]. This is the figure that distinguishes a sweep
    /// which reclaimed from one which merely found nothing live.
    #[cfg(test)]
    pub(crate) fn retained_rows(&self) -> usize {
        self.buckets.values().map(Vec::len).sum()
    }

    /// Live rows and the bytes of the values they point at, counted without
    /// mutating anything.
    ///
    /// Reporting must not be what reclaims: a metrics reader that sweeps makes
    /// the pool's memory depend on whether telemetry is enabled, and makes two
    /// reads taken moments apart disagree for reasons that have nothing to do
    /// with the workload. [`Interner::sweep`] is the mutator.
    fn live_counts(&self) -> (usize, usize) {
        let mut rows = 0;
        let mut value_bytes = 0;
        for candidates in self.buckets.values() {
            for row in candidates {
                if row.value.strong_count() > 0 {
                    rows += 1;
                    value_bytes += row.value_size;
                }
            }
        }
        (rows, value_bytes)
    }

    /// The pool's own retained allocation for this shard: its hash-map slots and
    /// its bucket vectors.
    ///
    /// Measured from capacity rather than from the live-row count, so capacity a
    /// past burst left behind is visible rather than reported as zero.
    pub(crate) fn self_bytes(&self) -> usize {
        self.buckets.capacity() * (size_of::<u64>() + size_of::<Vec<Row<T>>>())
            + self
                .buckets
                .values()
                .map(|candidates| candidates.capacity() * size_of::<Row<T>>())
                .sum::<usize>()
    }
}

/// A pool of shared `Arc<T>` allocations. See the module docs.
///
/// The hasher is a type parameter so that the collision path — where one bucket
/// holds two genuinely different values — can be exercised by a test that
/// forces every value into the same bucket. Production uses the default.
pub struct Interner<T, S = ahash::RandomState> {
    pub(crate) shards: Box<[Mutex<Shard<T>>]>,
    hasher: S,
    collapsed: AtomicU64,
    already_shared: AtomicU64,
    misses: AtomicU64,
}

impl<T: Internable, S: BuildHasher> std::fmt::Debug for Interner<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("Interner")
            .field("rows", &stats.rows)
            .field("value_bytes", &stats.value_bytes)
            .finish_non_exhaustive()
    }
}

impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Interner<T> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_hasher(ahash::RandomState::new())
    }
}

impl<T, S: BuildHasher> Interner<T, S> {
    #[must_use]
    pub fn with_hasher(hasher: S) -> Self {
        let mut shards = Vec::with_capacity(SHARDS);
        shards.resize_with(SHARDS, || Mutex::new(Shard::default()));

        Self {
            shards: shards.into_boxed_slice(),
            hasher,
            collapsed: AtomicU64::new(0),
            already_shared: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Reclaims every shard's tombstones and the capacity they left behind.
    ///
    /// Interning sweeps the shard it touches, which is enough while a shard
    /// keeps seeing traffic — but a shard that goes quiet after a burst would
    /// otherwise hold its dead rows indefinitely, since nothing else would ever
    /// look at it. The runtime's cache-maintenance loop drives this on its own
    /// schedule, so the pool shrinks when its holders disappear rather than
    /// when they return.
    ///
    /// Deliberately not driven by anything that observes the pool: tying
    /// reclamation to metrics collection would make the pool's memory depend on
    /// whether telemetry is enabled.
    pub fn sweep(&self) {
        for shard in &self.shards {
            shard.lock().sweep();
        }
    }




    /// A snapshot of what the pool holds, excluding rows whose holders are gone.
    ///
    /// Read-only. It does not sweep, so observing the pool never changes it:
    /// reclamation is [`Self::sweep`]'s job and runs on its own schedule, which
    /// keeps the pool's memory independent of whether anything is watching it.
    #[must_use]
    pub fn stats(&self) -> InternerStats {
        let mut rows = 0;
        let mut value_bytes = 0;
        let mut self_bytes = 0;
        for shard in &self.shards {
            let shard = shard.lock();
            let (live_rows, live_bytes) = shard.live_counts();
            rows += live_rows;
            value_bytes += live_bytes;
            self_bytes += shard.self_bytes();
        }

        InternerStats {
            rows,
            value_bytes,
            self_bytes,
            collapsed: self.collapsed.load(Ordering::Relaxed),
            already_shared: self.already_shared.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

impl<T: Internable, S: BuildHasher> Interner<T, S> {
    /// Returns the pool's shared copy of `value`'s content, adopting `value`
    /// itself if no equal value is held yet.
    ///
    /// The result always compares equal to `value`; only its identity may
    /// differ. Callers that hold many items over one shape should intern once
    /// and reuse the result: this hashes the full contents, which is cheap per
    /// retained item but not per row of data.
    ///
    /// Takes the value by value because that is how it is meant to be used —
    /// hand over the copy you were about to store, keep the canonical one.
    #[must_use]
    pub fn intern(&self, value: Arc<T>) -> Interned<T> {
        Interned(self.intern_arc(value))
    }

    /// The shared allocation itself, for the few callers that need an `Arc`
    /// rather than the proof-carrying [`Interned`] — rewriting a batch's schema
    /// in place, for one.
    #[must_use]
    pub fn intern_arc(&self, value: Arc<T>) -> Arc<T> {
        let hash = self.hash_of(&value);
        // The remainder is below SHARDS and so always fits; the fallback keeps
        // this total rather than resting on that reasoning.
        let shard_index = usize::try_from(hash % SHARDS as u64).unwrap_or(0);
        let mut shard = self.shards[shard_index].lock();

        shard.since_sweep += 1;
        if shard.since_sweep >= SWEEP_INTERVAL {
            shard.sweep();
        }

        if let Some(candidates) = shard.buckets.get(&hash) {
            for row in candidates {
                if let Some(existing) = row.value.upgrade() {
                    // Pointer equality first. It answers the same question as
                    // the deep compare whenever it is true, and it is true on a
                    // hot path: a caller that interns a value it already got
                    // from this pool — which every re-store of an unchanged
                    // shape does — would otherwise pay a full content walk,
                    // holding the shard lock, to learn what one pointer compare
                    // settles. It also separates "a duplicate was collapsed"
                    // from "the caller already had the shared copy"; only the
                    // former is a saving, and counting both as one number would
                    // make a pool that dedupes nothing look effective.
                    if Arc::ptr_eq(&existing, &value) {
                        self.already_shared.fetch_add(1, Ordering::Relaxed);
                        return existing;
                    }
                    // Content equality, not merely a matching hash: two distinct
                    // values that collide must never be conflated.
                    if existing.as_ref() == value.as_ref() {
                        self.collapsed.fetch_add(1, Ordering::Relaxed);
                        return existing;
                    }
                }
            }
        }

        let value_size = value.deep_size();
        shard.buckets.entry(hash).or_default().push(Row {
            value: Arc::downgrade(&value),
            value_size,
        });
        self.misses.fetch_add(1, Ordering::Relaxed);

        value
    }

    fn hash_of(&self, value: &T) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        value.content_hash(&mut hasher);
        hasher.finish()
    }
}

pub mod schema;
pub mod table_set;

/// Reclaims tombstones across every process-wide pool this crate owns.
///
/// Driven by [`crate::Caching::run_pending_maintenance`]: a pool only reclaims
/// a shard that interning happens to touch, so a shard that goes quiet after a
/// burst would otherwise hold its dead rows for the process lifetime.
pub fn sweep_all() {
    schema::sweep();
    table_set::sweep();
}
