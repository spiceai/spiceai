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

use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::size_of;
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};

use super::{Internable, Interner};
#[cfg(test)]
use super::{SHARDS, SWEEP_INTERVAL};

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

impl Internable for Schema {
    /// `Schema`'s own `Hash` covers its fields and its metadata, field-level
    /// metadata included, which is exactly the content `Eq` compares. Keying on
    /// it is therefore safe: two schemas that hash alike and compare equal are
    /// interchangeable to any holder.
    fn content_hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self, state);
    }

    fn deep_size(&self) -> usize {
        schema_deep_size(self)
    }
}

/// A pool of shared [`Schema`] allocations. See [`Interner`].
pub type SchemaInterner<S = ahash::RandomState> = Interner<Schema, S>;

impl<S: BuildHasher> Interner<Schema, S> {
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
                    let interned = self.intern_arc(Arc::clone(&schema));
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
}

static GLOBAL: LazyLock<SchemaInterner> = LazyLock::new(SchemaInterner::new);

/// The process-wide pool.
#[must_use]
pub fn global() -> &'static SchemaInterner {
    &GLOBAL
}

/// Reclaims tombstones across the process-wide pool. See [`Interner::sweep`].
pub fn sweep() {
    GLOBAL.sweep();
}

/// Interns `schema` in the process-wide pool. See [`Interner::intern`].
#[must_use]
pub fn intern(schema: SchemaRef) -> super::Interned<Schema> {
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
    use arrow::datatypes::{DataType, Field};
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

        let ia = interner.intern(Arc::clone(&a)).arc();
        let ib = interner.intern(Arc::clone(&b)).arc();

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

        let shared = interner.intern(Arc::clone(&schema)).arc();

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

        let ia = interner.intern(Arc::clone(&bare)).arc();
        let ib = interner.intern(Arc::clone(&annotated)).arc();

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
                &interner.intern(Arc::clone(&plain)).arc(),
                &interner.intern(Arc::clone(&tagged)).arc()
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
            &interner.intern(Arc::clone(&nullable)).arc(),
            &interner.intern(Arc::clone(&required)).arc()
        ));
    }

    #[test]
    fn a_dropped_schema_leaves_no_live_row() {
        let interner = SchemaInterner::new();
        {
            let schema = schema_of(4);
            let shared = interner.intern(Arc::clone(&schema)).arc();
            assert_eq!(interner.stats().rows, 1);
            drop(shared);
            drop(schema);
        }

        let stats = interner.stats();
        assert_eq!(stats.rows, 0, "the pool must not keep a schema alive");
        assert_eq!(stats.value_bytes, 0, "its bytes are discounted with it");
    }

    /// The pool holds `Weak`s, so a row must never be what keeps a schema
    /// reachable — otherwise a retired query shape is pinned for the process
    /// lifetime.
    #[test]
    fn the_pool_does_not_keep_a_schema_alive() {
        let interner = SchemaInterner::new();
        let weak = {
            let schema = schema_of(4);
            let shared = interner.intern(Arc::clone(&schema)).arc();
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
        let shared = interner.intern(Arc::clone(&revived)).arc();

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
        let _narrow = interner.intern(Arc::clone(&narrow)).arc();
        let _wide = interner.intern(Arc::clone(&wide)).arc();

        let stats = interner.stats();
        assert_eq!(stats.rows, 2);
        assert_eq!(
            stats.value_bytes,
            schema_deep_size(&narrow) + schema_deep_size(&wide),
            "reported bytes are the deep sizes of the live schemas"
        );
        assert!(
            stats.self_bytes < stats.value_bytes,
            "the pool's own bookkeeping is far smaller than what it deduplicates"
        );
    }

    #[test]
    fn repeated_interning_collapses_duplicates_into_one_row() {
        let interner = SchemaInterner::new();
        let held: Vec<SchemaRef> = (0..64)
            .map(|_| interner.intern(schema_of(8)).arc())
            .collect();

        let stats = interner.stats();
        assert_eq!(stats.rows, 1, "64 equal schemas occupy one row");
        assert_eq!(stats.misses, 1, "only the first was unseen");
        assert_eq!(
            stats.collapsed, 63,
            "each of the other 63 was a distinct allocation that got collapsed"
        );
        assert_eq!(
            stats.already_shared, 0,
            "every caller arrived with its own allocation, none pre-shared"
        );
        assert!(held.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])));
    }

    /// The two kinds of hit must not be conflated: re-interning the allocation
    /// the pool already handed back saves nothing, while interning a distinct
    /// but equal allocation is the duplicate this pool exists to remove. A
    /// single "hit" counter would let a pool that collapses nothing look busy.
    #[test]
    fn a_re_intern_of_the_shared_copy_is_not_counted_as_a_collapse() {
        let interner = SchemaInterner::new();
        let shared = interner.intern(schema_of(8)).arc();
        assert_eq!(interner.stats().misses, 1);

        // Hand back the very allocation the pool returned.
        let again = interner.intern(Arc::clone(&shared)).arc();
        assert!(Arc::ptr_eq(&again, &shared));

        let stats = interner.stats();
        assert_eq!(
            stats.already_shared, 1,
            "the caller already held the shared copy"
        );
        assert_eq!(
            stats.collapsed, 0,
            "nothing was collapsed — no duplicate existed"
        );

        // A separately-built equal schema *is* a collapse.
        drop(interner.intern(schema_of(8)));
        let stats = interner.stats();
        assert_eq!(stats.collapsed, 1, "a distinct allocation was collapsed");
        assert_eq!(stats.already_shared, 1, "unchanged by the collapse");
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

        let i_narrow = interner.intern(Arc::clone(&narrow)).arc();
        let i_wide = interner.intern(Arc::clone(&wide)).arc();
        let i_annotated = interner.intern(Arc::clone(&annotated)).arc();

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
        assert!(Arc::ptr_eq(&interner.intern(schema_of(9)).arc(), &i_wide));
        assert_eq!(interner.stats().collapsed, 1);
    }

    #[test]
    #[expect(
        clippy::needless_collect,
        reason = "every thread must be spawned before any is joined; consuming the iterator lazily would join each thread as it is created, making the test sequential and defeating what it checks"
    )]
    fn concurrent_interning_converges_on_one_allocation() {
        let interner = Arc::new(SchemaInterner::new());
        let results: Vec<SchemaRef> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let interner = Arc::clone(&interner);
                    scope.spawn(move || {
                        (0..64)
                            .map(|_| interner.intern(schema_of(16)).arc())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
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
