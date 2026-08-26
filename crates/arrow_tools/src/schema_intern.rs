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

//! A process-wide, weakly-held pool of shared [`Schema`] allocations.
//!
//! A schema is immutable and is usually identical across every item in a
//! collection, yet each item holds its own copy: two runs of the same SQL, a
//! batch and the stream that carries it, and a table and a `SELECT *` over it
//! all produce distinct [`Schema`] allocations. Wherever many small items are
//! retained together that duplication dominates — a cached 0-row result over a
//! 200-column table is almost entirely schema.
//!
//! [`SchemaInterner::intern`] collapses those copies onto one allocation per
//! distinct schema *content*.
//!
//! # Every row is a `Weak`
//!
//! The pool must never be what keeps a schema alive: a dropped table or a
//! retired query shape would otherwise be pinned for the process lifetime.
//! Every row holds a [`Weak`], so the last real holder dropping its `Arc` frees
//! the schema and leaves only a tombstone, which the next sweep reclaims.
//!
//! That is also why the pool needs no eviction policy and never declines to
//! intern. A row costs a `Weak` and a hash-map slot; the schema it points at is
//! memory some holder is keeping alive regardless. Refusing to intern would
//! trade that row for a fully duplicated schema — strictly worse — so while any
//! holder remains, staying interned is always the better outcome. The pool
//! shrinks on its own as schemas fall out of use.
//!
//! # Why not an existing cache or interner
//!
//! **A cache is the wrong shape, not merely heavier.** A cache exists to evict
//! under pressure, and eviction is precisely what must not happen here:
//! dropping a live row frees nothing, because its holders keep the schema
//! alive, and only makes the next caller for that schema allocate a duplicate.
//! What this needs is weak values and no eviction at all, which is close to the
//! inverse of what a cache provides.
//!
//! **An interner crate cannot return what Arrow requires.** The established
//! ones hand back their own smart pointer over the interned value. Arrow's API
//! is `SchemaRef = Arc<Schema>`: `RecordBatch::with_schema`,
//! `TableProvider::schema`, and every batch rewritten here take a real
//! `Arc<Schema>`, so a foreign pointer type cannot be passed to any of them.
//! That rules them out on the type, not on preference.
//!
//! **A concurrent map would replace the shard array, not the difficulty.** The
//! intricate parts — weak rows, resolving a bucket by content rather than by
//! hash, and sweeping tombstones — all follow from holding values weakly, and
//! would remain over any map. Sharding a `HashMap` behind mutexes is the small
//! part, so it is kept here rather than taking a dependency in a crate this low
//! in the graph.
//!
//! # Why the pool is sharded
//!
//! The content-equality check runs while the shard's lock is held, and it is a
//! deep comparison: every field, its type, its nullability, and its metadata
//! map. Hashing happens before the lock is taken, but that comparison cannot.
//! A single lock would therefore serialise 200-column comparisons across every
//! unrelated table in the process.
//!
//! # Accounting
//!
//! Holders do not charge interned schemas to their own per-item memory
//! budgets: one allocation shared by every item is not a per-item cost, and
//! charging it per item is what made a cached 0-row result over a wide table
//! look 40 KB heavier than it was. The schema bytes are instead reported here,
//! in one place, by [`SchemaInterner::stats`] — so the memory stays visible
//! without every item paying for it.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use parking_lot::Mutex;

/// Number of independently-locked shards.
///
/// Sharded because the content-equality check is a deep comparison held under
/// the lock; see the module docs. Fixed rather than CPU-derived because the
/// pool is touched once per retained item, not per row of data, so the count
/// only needs to keep unrelated tables off a single lock.
const SHARDS: usize = 16;

/// Attempts a shard tolerates before sweeping its tombstones.
///
/// A sweep walks the whole shard, so it is amortised across attempts rather
/// than run on each one.
const SWEEP_INTERVAL: usize = 256;

/// What the pool holds, and what it has done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InternerStats {
    /// Rows currently held — one per distinct schema.
    pub rows: usize,
    /// Total deep size of the schemas those rows point at.
    ///
    /// This is the memory that per-item accounting no longer charges. The pool
    /// holds these schemas weakly, so the bytes belong to whichever holders
    /// keep them alive; reporting them here is what keeps the saving visible
    /// rather than invisible.
    pub schema_bytes: usize,
    /// The pool's own retained allocation: its hash-map slots and bucket
    /// vectors, measured from capacity rather than from live rows so that
    /// capacity left behind by a past burst is visible.
    pub self_bytes: usize,
    /// Interned schemas served from an existing row.
    pub hits: u64,
    /// Interned schemas that created a new row.
    pub misses: u64,
}

/// The deep size of an Arrow [`Schema`]: the struct, the fields it owns, and
/// its key-value metadata.
///
/// `arrow-schema` exposes `Fields::size` and `Field::size` but no
/// `Schema::size`, so the metadata map is charged here the same way
/// `Field::size` charges its own — by slots plus the bytes each string owns.
#[must_use]
pub fn schema_deep_size(schema: &Schema) -> usize {
    size_of::<Schema>()
        + schema.fields().size()
        + schema.metadata.capacity() * size_of::<(String, String)>()
        + schema
            .metadata
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>()
}

/// One row: a weak handle plus the size of the schema it points at.
///
/// The size is kept alongside so a sweep can discount a dead row without
/// upgrading it — by then there is nothing left to measure.
struct Row {
    schema: Weak<Schema>,
    schema_size: usize,
}

#[derive(Default)]
struct Shard {
    /// Content hash -> candidate rows. A bucket holds more than one row only on
    /// a hash collision, which content equality then resolves.
    buckets: HashMap<u64, Vec<Row>>,
    /// Interning attempts since this shard was last swept.
    since_sweep: usize,
}

/// How many times larger than its live contents a collection may be before a
/// sweep reallocates it.
///
/// `retain` removes rows but never gives back the capacity they occupied, so
/// without this a burst of short-lived query shapes would leave every shard
/// holding its peak allocation for the process lifetime — which is exactly what
/// a weakly-held pool promises not to do. Shrinking only past a slack factor
/// keeps a steady-state workload from reallocating on every sweep.
const CAPACITY_SLACK: usize = 4;

impl Shard {
    /// Drops rows whose last holder is gone, and any bucket left empty, then
    /// returns the capacity the survivors no longer need.
    fn sweep(&mut self) {
        self.buckets.retain(|_, candidates| {
            candidates.retain(|row| row.schema.strong_count() > 0);
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
    /// [`Self::live_counts`] answers what the pool *shares*; this answers what
    /// it is *holding on to*, which is what tells a test whether a sweep
    /// actually reclaimed anything rather than merely reporting no live rows.
    fn retained_rows(&self) -> usize {
        self.buckets.values().map(Vec::len).sum()
    }

    /// Live rows and the bytes of the schemas they point at, counted without
    /// mutating anything.
    ///
    /// Reporting must not be what reclaims: a metrics reader that sweeps makes
    /// the pool's memory depend on whether telemetry is enabled, and makes two
    /// reads taken moments apart disagree for reasons that have nothing to do
    /// with the workload. [`SchemaInterner::sweep`] is the mutator.
    fn live_counts(&self) -> (usize, usize) {
        let mut rows = 0;
        let mut schema_bytes = 0;
        for candidates in self.buckets.values() {
            for row in candidates {
                if row.schema.strong_count() > 0 {
                    rows += 1;
                    schema_bytes += row.schema_size;
                }
            }
        }
        (rows, schema_bytes)
    }

    /// The pool's own retained allocation for this shard: its hash-map slots and
    /// its bucket vectors.
    ///
    /// Measured from capacity rather than from the live-row count, so capacity a
    /// past burst left behind is visible rather than reported as zero.
    fn self_bytes(&self) -> usize {
        self.buckets.capacity() * (size_of::<u64>() + size_of::<Vec<Row>>())
            + self
                .buckets
                .values()
                .map(|candidates| candidates.capacity() * size_of::<Row>())
                .sum::<usize>()
    }
}

/// A pool of shared [`Schema`] allocations. See the module docs.
///
/// The hasher is a type parameter so that the collision path — where one bucket
/// holds two genuinely different schemas — can be exercised by a test that
/// forces every schema into the same bucket. Production uses the default.
pub struct SchemaInterner<S = ahash::RandomState> {
    shards: Box<[Mutex<Shard>]>,
    hasher: S,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<S: BuildHasher> std::fmt::Debug for SchemaInterner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("SchemaInterner")
            .field("rows", &stats.rows)
            .field("schema_bytes", &stats.schema_bytes)
            .finish_non_exhaustive()
    }
}

impl Default for SchemaInterner {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaInterner {
    #[must_use]
    pub fn new() -> Self {
        Self::with_hasher(ahash::RandomState::new())
    }
}

impl<S: BuildHasher> SchemaInterner<S> {
    #[must_use]
    pub fn with_hasher(hasher: S) -> Self {
        let mut shards = Vec::with_capacity(SHARDS);
        shards.resize_with(SHARDS, || Mutex::new(Shard::default()));

        Self {
            shards: shards.into_boxed_slice(),
            hasher,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Returns the pool's shared copy of `schema`'s content, adopting `schema`
    /// itself if no equal schema is held yet.
    ///
    /// The result always compares equal to `schema`; only its identity may
    /// differ. Callers that hold many items over one schema should intern once
    /// and reuse the result: this hashes the schema's full contents, which is
    /// cheap per retained item but not per row of data.
    ///
    /// Takes the schema by value because that is how it is meant to be used —
    /// hand over the copy you were about to store, keep the canonical one.
    #[must_use]
    pub fn intern(&self, schema: SchemaRef) -> SchemaRef {
        let hash = self.hash_of(&schema);
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
                if let Some(existing) = row.schema.upgrade() {
                    // Content equality, not merely a matching hash: two distinct
                    // schemas that collide must never be conflated, and
                    // `Schema`'s `Eq` covers its metadata as well as its fields.
                    if existing.as_ref() == schema.as_ref() {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return existing;
                    }
                }
            }
        }

        let schema_size = schema_deep_size(&schema);
        shard.buckets.entry(hash).or_default().push(Row {
            schema: Arc::downgrade(&schema),
            schema_size,
        });
        self.misses.fetch_add(1, Ordering::Relaxed);

        schema
    }

    /// Replaces every batch's schema with the pool's shared copy, in place.
    ///
    /// Interning hashes a schema's full contents, so a batch whose schema is
    /// the *same allocation* as the previous batch's reuses that lookup rather
    /// than hashing again. Only the previous batch is remembered, so a slice
    /// that alternates between two schemas hashes at every change — correct,
    /// but not free. The case this is for is the common one: batches of a
    /// single result almost always share one pointer, making it one hash for
    /// the whole slice.
    ///
    /// Each batch is interned by its *own* content, never coerced onto a
    /// neighbour's schema: two batches that genuinely differ stay different.
    pub fn intern_batch_schemas(&self, batches: &mut [RecordBatch]) {
        let mut last: Option<(SchemaRef, SchemaRef)> = None;

        for batch in batches {
            let interned = match &last {
                Some((seen, interned)) if Arc::ptr_eq(seen, batch.schema_ref()) => {
                    Arc::clone(interned)
                }
                _ => {
                    let schema = Arc::clone(batch.schema_ref());
                    let interned = self.intern(Arc::clone(&schema));
                    last = Some((schema, Arc::clone(&interned)));
                    interned
                }
            };

            if Arc::ptr_eq(batch.schema_ref(), &interned) {
                continue;
            }

            // The interned schema is content-equal to the batch's own, so this
            // cannot fail; on the impossible path keep the batch as it stands
            // rather than losing data over an optimisation.
            match batch.clone().with_schema(interned) {
                Ok(reschemad) => *batch = reschemad,
                Err(e) => {
                    tracing::debug!(
                        "Retaining a batch's own schema, which did not match its interned copy: {e}"
                    );
                }
            }
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
    /// reclamation is [`Self::sweep`]'s job and runs on its own schedule,
    /// which keeps the pool's memory independent of whether anything is
    /// watching it.
    #[must_use]
    pub fn stats(&self) -> InternerStats {
        let mut rows = 0;
        let mut schema_bytes = 0;
        let mut self_bytes = 0;
        for shard in &self.shards {
            let shard = shard.lock();
            let (live_rows, live_bytes) = shard.live_counts();
            rows += live_rows;
            schema_bytes += live_bytes;
            self_bytes += shard.self_bytes();
        }

        InternerStats {
            rows,
            schema_bytes,
            self_bytes,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    fn hash_of(&self, schema: &Schema) -> u64 {
        self.hasher.hash_one(schema)
    }
}

static GLOBAL: LazyLock<SchemaInterner> = LazyLock::new(SchemaInterner::new);

/// The process-wide pool.
#[must_use]
pub fn global() -> &'static SchemaInterner {
    &GLOBAL
}

/// Reclaims tombstones across the process-wide pool. See
/// [`SchemaInterner::sweep`].
pub fn sweep() {
    GLOBAL.sweep();
}

/// Interns `schema` in the process-wide pool. See [`SchemaInterner::intern`].
#[must_use]
pub fn intern(schema: SchemaRef) -> SchemaRef {
    GLOBAL.intern(schema)
}

/// Interns every batch's schema in the process-wide pool, in place. See
/// [`SchemaInterner::intern_batch_schemas`].
pub fn intern_batch_schemas(batches: &mut [RecordBatch]) {
    GLOBAL.intern_batch_schemas(batches);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use std::collections::HashMap;
    use std::hash::Hasher;

    fn schema_of(width: usize) -> SchemaRef {
        Arc::new(Schema::new(
            (0..width)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn equal_schemas_collapse_onto_one_allocation() {
        let interner = SchemaInterner::new();
        let (a, b) = (schema_of(8), schema_of(8));

        assert!(!Arc::ptr_eq(&a, &b), "the two inputs start out distinct");

        let ia = interner.intern(Arc::clone(&a));
        let ib = interner.intern(Arc::clone(&b));

        assert!(
            Arc::ptr_eq(&ia, &ib),
            "two equal schemas must intern to the same allocation"
        );
        assert_eq!(ia.as_ref(), a.as_ref(), "interning preserves content");
    }

    /// The whole point of interning is that a holder can keep the result and
    /// drop its own copy; that is only sound if the two are content-equal.
    #[test]
    fn interning_preserves_field_order_and_nullability() {
        let interner = SchemaInterner::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, false),
            Field::new("a", DataType::Int64, true),
        ]));

        let shared = interner.intern(Arc::clone(&schema));

        assert_eq!(shared.as_ref(), schema.as_ref());
        assert_eq!(shared.fields()[0].name(), "b", "order is preserved");
        assert!(!shared.fields()[0].is_nullable());
        assert!(shared.fields()[1].is_nullable());
    }

    #[test]
    fn schemas_differing_only_in_metadata_are_not_conflated() {
        let interner = SchemaInterner::new();
        let bare = schema_of(4);
        let annotated = Arc::new(
            Schema::new(bare.fields().clone())
                .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
        );

        let ia = interner.intern(Arc::clone(&bare));
        let ib = interner.intern(Arc::clone(&annotated));

        assert!(
            !Arc::ptr_eq(&ia, &ib),
            "schema metadata distinguishes two schemas and must not be collapsed"
        );
        assert!(ia.metadata().is_empty());
        assert_eq!(ib.metadata().get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn schemas_differing_only_in_field_metadata_are_not_conflated() {
        let interner = SchemaInterner::new();
        let plain = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let tagged = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int64, true).with_metadata(HashMap::from([(
                "embedding".to_string(),
                "true".to_string(),
            )])),
        ]));

        assert!(
            !Arc::ptr_eq(
                &interner.intern(Arc::clone(&plain)),
                &interner.intern(Arc::clone(&tagged))
            ),
            "field-level metadata distinguishes two schemas"
        );
    }

    #[test]
    fn schemas_differing_only_in_nullability_are_not_conflated() {
        let interner = SchemaInterner::new();
        let nullable = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let required = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));

        assert!(!Arc::ptr_eq(
            &interner.intern(Arc::clone(&nullable)),
            &interner.intern(Arc::clone(&required))
        ));
    }

    #[test]
    fn a_dropped_schema_leaves_no_live_row() {
        let interner = SchemaInterner::new();
        {
            let schema = schema_of(4);
            let shared = interner.intern(Arc::clone(&schema));
            assert_eq!(interner.stats().rows, 1);
            drop(shared);
            drop(schema);
        }

        let stats = interner.stats();
        assert_eq!(stats.rows, 0, "the pool must not keep a schema alive");
        assert_eq!(stats.schema_bytes, 0, "its bytes are discounted with it");
    }

    /// The pool holds `Weak`s, so a row must never be what keeps a schema
    /// reachable — otherwise a retired query shape is pinned for the process
    /// lifetime.
    #[test]
    fn the_pool_does_not_keep_a_schema_alive() {
        let interner = SchemaInterner::new();
        let weak = {
            let schema = schema_of(4);
            let shared = interner.intern(Arc::clone(&schema));
            let weak = Arc::downgrade(&shared);
            drop(shared);
            drop(schema);
            weak
        };

        assert!(
            weak.upgrade().is_none(),
            "no strong reference may survive in the pool"
        );
    }

    /// A schema re-interned after its predecessor died must get a fresh row
    /// rather than resurrect a tombstone.
    #[test]
    fn a_schema_can_be_reinterned_after_its_row_dies() {
        let interner = SchemaInterner::new();
        drop(interner.intern(schema_of(4)));
        assert_eq!(interner.stats().rows, 0);

        let revived = schema_of(4);
        let shared = interner.intern(Arc::clone(&revived));

        assert_eq!(shared.as_ref(), revived.as_ref());
        assert_eq!(interner.stats().rows, 1);
    }

    /// Interning sweeps the shard it touches every `SWEEP_INTERVAL` attempts,
    /// so a shard under sustained traffic must not grow a tombstone per attempt.
    #[test]
    fn tombstones_do_not_accumulate_under_sustained_interning() {
        let interner = SchemaInterner::new();
        // Enough attempts that every shard crosses its sweep threshold.
        let attempts = SWEEP_INTERVAL * SHARDS * 2;
        for width in 0..attempts {
            drop(interner.intern(schema_of(width % 32)));
        }

        // Raw retained rows, not `stats()`: that counts only *live* rows and so
        // reads zero whether or not the tombstones were ever reclaimed.
        let retained: usize = interner
            .shards
            .iter()
            .map(|shard| shard.lock().retained_rows())
            .sum();
        assert!(
            retained < attempts / 4,
            "tombstones must be reclaimed as interning proceeds, {retained} retained of {attempts} attempts"
        );
        assert_eq!(interner.stats().rows, 0, "nothing is still held");
    }

    /// Observing the pool must not be what reclaims it. If `stats()` swept, the
    /// pool's memory would depend on whether telemetry happened to be enabled,
    /// and two reads moments apart would disagree for reasons unrelated to the
    /// workload.
    #[test]
    fn stats_does_not_mutate_the_pool() {
        let interner = SchemaInterner::new();
        for width in 0..8 {
            drop(interner.intern(schema_of(width)));
        }

        let retained = || -> usize {
            interner
                .shards
                .iter()
                .map(|s| s.lock().retained_rows())
                .sum()
        };
        let before = retained();
        assert!(before > 0, "the dropped schemas must leave rows to reclaim");

        let first = interner.stats();
        let second = interner.stats();

        assert_eq!(retained(), before, "stats() must not reclaim anything");
        assert_eq!(first, second, "two reads with no work between must agree");
        assert_eq!(first.rows, 0, "and it must still report only live rows");
    }

    #[test]
    fn a_burst_does_not_leave_its_capacity_behind() {
        let interner = SchemaInterner::new();
        for width in 0..512 {
            drop(interner.intern(schema_of(width)));
        }

        // Read straight from the shards to capture the peak: `stats()` counts
        // only live rows, so it reports zero here whether or not the capacity
        // behind them was ever given back.
        let retained = || -> usize {
            interner
                .shards
                .iter()
                .map(|shard| shard.lock().self_bytes())
                .sum()
        };
        // The bucket map specifically. Dropping empty buckets frees their
        // vectors on its own, so only this distinguishes giving the map's own
        // capacity back from merely emptying it.
        let map_capacity = || -> usize {
            interner
                .shards
                .iter()
                .map(|shard| shard.lock().buckets.capacity())
                .sum()
        };

        let (peak_bytes, peak_capacity) = (retained(), map_capacity());
        assert!(peak_bytes > 0, "the burst must leave capacity behind");
        assert!(peak_capacity > 0, "the burst must grow the bucket maps");

        interner.sweep();

        assert_eq!(interner.stats().rows, 0, "every row's holder is gone");
        assert!(
            retained() * 2 < peak_bytes,
            "the pool must give back what the burst left, held {} of a {peak_bytes}-byte peak",
            retained()
        );
        assert!(
            map_capacity() * 2 < peak_capacity,
            "the bucket maps must give their capacity back, not just empty themselves: {} of {peak_capacity} slots",
            map_capacity()
        );
    }

    /// Interning only sweeps the shard it touches, so a pool that goes quiet
    /// after a burst would hold its dead rows until traffic returned. The
    /// explicit sweep is what the runtime's cache-maintenance loop drives.
    #[test]
    fn an_explicit_sweep_reclaims_without_further_interning() {
        let interner = SchemaInterner::new();
        // Fewer than SWEEP_INTERVAL, so no intern call can have swept.
        for width in 0..8 {
            drop(interner.intern(schema_of(width)));
        }

        interner.sweep();

        // Counted without `stats()`, which reports only live rows and so would
        // read zero even if `sweep()` had reclaimed nothing.
        let live: usize = interner
            .shards
            .iter()
            .map(|shard| shard.lock().retained_rows())
            .sum();
        assert_eq!(
            live, 0,
            "a sweep must reclaim dead rows with no new traffic"
        );
    }

    #[test]
    fn stats_report_what_the_pool_points_at() {
        let interner = SchemaInterner::new();
        let narrow = schema_of(4);
        let wide = schema_of(200);
        let _narrow = interner.intern(Arc::clone(&narrow));
        let _wide = interner.intern(Arc::clone(&wide));

        let stats = interner.stats();
        assert_eq!(stats.rows, 2);
        assert_eq!(
            stats.schema_bytes,
            schema_deep_size(&narrow) + schema_deep_size(&wide),
            "reported bytes are the deep sizes of the live schemas"
        );
        assert!(
            stats.self_bytes < stats.schema_bytes,
            "the pool's own bookkeeping is far smaller than what it deduplicates"
        );
    }

    #[test]
    fn repeated_interning_reports_hits_not_new_rows() {
        let interner = SchemaInterner::new();
        let held: Vec<SchemaRef> = (0..64).map(|_| interner.intern(schema_of(8))).collect();

        let stats = interner.stats();
        assert_eq!(stats.rows, 1, "64 equal schemas occupy one row");
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 63);
        assert!(held.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])));
    }

    /// Every schema lands in one bucket, so `intern` must resolve candidates by
    /// content rather than trusting the hash.
    #[derive(Default, Clone)]
    struct CollideEverything;

    impl BuildHasher for CollideEverything {
        type Hasher = ConstantHasher;
        fn build_hasher(&self) -> ConstantHasher {
            ConstantHasher
        }
    }

    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }
        fn write(&mut self, _: &[u8]) {}
    }

    /// The content-equality check in `intern` is what stops a hash collision
    /// from serving one schema in place of another. With a real hasher a
    /// collision is unreachable, so this drives the same code path with a
    /// hasher that collides everything.
    #[test]
    fn colliding_schemas_are_kept_distinct_by_content() {
        let interner = SchemaInterner::with_hasher(CollideEverything);

        let narrow = schema_of(4);
        let wide = schema_of(9);
        let annotated = Arc::new(
            Schema::new(narrow.fields().clone())
                .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
        );

        let i_narrow = interner.intern(Arc::clone(&narrow));
        let i_wide = interner.intern(Arc::clone(&wide));
        let i_annotated = interner.intern(Arc::clone(&annotated));

        assert_eq!(i_narrow.as_ref(), narrow.as_ref(), "content is preserved");
        assert_eq!(i_wide.as_ref(), wide.as_ref());
        assert_eq!(i_annotated.as_ref(), annotated.as_ref());

        assert!(!Arc::ptr_eq(&i_narrow, &i_wide));
        assert!(
            !Arc::ptr_eq(&i_narrow, &i_annotated),
            "a collision must not let metadata be dropped"
        );

        let stats = interner.stats();
        assert_eq!(stats.rows, 3, "all three share a bucket but stay distinct");
        assert_eq!(stats.misses, 3);

        // Re-interning still finds the right candidate within the bucket.
        assert!(Arc::ptr_eq(&interner.intern(schema_of(9)), &i_wide));
        assert_eq!(interner.stats().hits, 1);
    }

    #[test]
    #[expect(
        clippy::needless_collect,
        reason = "every thread must be spawned before any is joined; consuming the iterator lazily would join each thread as it is created, making the test sequential and defeating what it checks"
    )]
    fn concurrent_interning_converges_on_one_allocation() {
        let interner = Arc::new(SchemaInterner::new());
        let results: Vec<SchemaRef> = std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    let interner = Arc::clone(&interner);
                    scope.spawn(move || {
                        (0..64)
                            .map(|_| interner.intern(schema_of(16)))
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .flat_map(|h| h.join().expect("thread"))
                .collect()
        });

        assert!(
            results.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])),
            "every thread must observe the same allocation"
        );
        assert_eq!(interner.stats().rows, 1);
    }
}
