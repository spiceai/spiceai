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
//! Nothing upstream shares an Arrow schema: two runs of the same SQL, a
//! `SELECT *` and the table it reads, and a batch and the stream carrying it
//! all produce distinct allocations. So a cache holding many entries over one
//! query shape holds one copy of that shape's schema per entry.
//!
//! What that costs is not primarily memory — a per-entry weigher already
//! charges it, so it is bounded and evicted like anything else. It is *budget
//! occupancy*: a 0-row result over a 200-column table charges roughly 26 KB of
//! schema against 644 bytes of everything else, so the cache spends its budget
//! on duplicate schemas instead of on results. Interning frees that budget for
//! payload, which is worth about an order of magnitude more small responses
//! cached in the same memory.
//!
//! # Representation
//!
//! One slot per content hash, holding a [`Weak`]:
//!
//! * **Weak**, so the pool is never what keeps a schema alive. The last real
//!   holder dropping its `Arc` frees the schema and leaves a tombstone that the
//!   next sweep of that slot reclaims. Nothing else is needed to bound it, and
//!   no eviction policy is wanted: dropping a live entry would free nothing —
//!   its holders keep the schema alive — and only make the next caller allocate
//!   a duplicate.
//! * **One slot, not a bucket of candidates.** A hit is still verified by
//!   content, because a 64-bit hash can in principle collide and returning the
//!   wrong schema would be a correctness bug. But a collision between two *live*
//!   schemas is resolved by simply declining to share the second one, rather
//!   than by keeping a list. Interning is an optimisation, so declining costs a
//!   duplicate allocation in a case that is astronomically rare, and it keeps
//!   the structure a flat map.
//!
//! # Accounting
//!
//! [`Interned::owned`] tells a holder whether it is the entry that put the
//! schema in the pool. A per-item budget charges the schema exactly when it is
//! — so one charge is made per *distinct* schema, however many items come to
//! share it, and a workload whose schemas are all distinct charges every one of
//! them, which is what it did before any of this existed.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};

use arrow_schema::{Schema, SchemaRef};
use parking_lot::Mutex;

/// Number of independently-locked shards.
///
/// The content check that resolves a hit is a deep comparison — every field,
/// its type, its nullability and its metadata — and it is held under the lock.
/// One lock would serialise 200-column comparisons across unrelated tables.
/// Fixed rather than CPU-derived: the pool is touched once per stored item, not
/// per row, so the count only has to keep unrelated tables apart.
const SHARDS: usize = 16;

/// Misses a shard tolerates before it sweeps its tombstones.
///
/// Counted on misses only. A hit adds no slot, so nothing has grown and there
/// is nothing new to reclaim; sweeping on hits would walk the map under the
/// lock in the steady state, which is the case that is almost all hits.
const SWEEP_INTERVAL: usize = 64;

/// The outcome of interning a schema.
pub struct Interned {
    /// The schema to use — the pool's copy, which may be the one passed in.
    pub schema: SchemaRef,
    /// Whether this caller is the one that placed `schema` in the pool.
    ///
    /// A holder that charges a per-item memory budget should charge the schema
    /// exactly when this is `true`: the pool holds only a `Weak`, so this
    /// caller's `Arc` is what keeps the allocation alive, and every later
    /// holder is merely pointing at it. See the module docs.
    pub owned: bool,
}

#[derive(Default)]
struct Shard {
    slots: HashMap<u64, Weak<Schema>>,
    since_sweep: usize,
}

impl Shard {
    /// Drops slots whose schema is gone, and gives back the capacity they held.
    fn sweep(&mut self) {
        self.slots.retain(|_, weak| weak.strong_count() > 0);
        // `retain` frees no capacity, so a burst of short-lived shapes would
        // otherwise leave every shard at its peak allocation for the process
        // lifetime — which is what holding schemas weakly is meant to avoid.
        self.slots.shrink_to_fit();
        self.since_sweep = 0;
    }
}

/// A pool of shared [`Schema`] allocations. See the module docs.
pub struct SchemaInterner<S = ahash::RandomState> {
    shards: Box<[Mutex<Shard>]>,
    hasher: S,
    /// Schemas that collapsed onto one already in the pool.
    collapsed: AtomicU64,
    /// Schemas the pool adopted, having not seen them before.
    adopted: AtomicU64,
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
    /// The hasher is a parameter so a test can force every schema into one
    /// slot; with a real hasher the collision path is unreachable.
    #[must_use]
    pub fn with_hasher(hasher: S) -> Self {
        let mut shards = Vec::with_capacity(SHARDS);
        shards.resize_with(SHARDS, || Mutex::new(Shard::default()));

        Self {
            shards: shards.into_boxed_slice(),
            hasher,
            collapsed: AtomicU64::new(0),
            adopted: AtomicU64::new(0),
        }
    }

    /// Returns the pool's copy of `schema`'s content, adopting `schema` itself
    /// if nothing equal is held.
    ///
    /// The result always compares equal to `schema`; only its identity may
    /// differ. Hashes the schema's full contents, so a caller storing many
    /// items over one shape should intern once and reuse the result.
    #[must_use]
    pub fn intern(&self, schema: SchemaRef) -> Interned {
        let hash = self.hasher.hash_one(&schema);
        // The remainder is below SHARDS and always fits; the fallback keeps
        // this total rather than resting on that.
        let index = usize::try_from(hash % SHARDS as u64).unwrap_or(0);
        let mut shard = self.shards[index].lock();

        if let Some(existing) = shard.slots.get(&hash).and_then(Weak::upgrade) {
            // Verified by content, never by the hash alone: returning a
            // different schema that merely collided would be a correctness bug.
            if existing.as_ref() == schema.as_ref() {
                self.collapsed.fetch_add(1, Ordering::Relaxed);
                return Interned {
                    schema: existing,
                    owned: false,
                };
            }

            // A live schema already occupies this slot. Rather than keep a list
            // of candidates for a case a 64-bit hash makes vanishingly rare,
            // decline to share this one — it stays exactly as correct, just
            // unshared, and this caller owns it.
            return Interned {
                schema,
                owned: true,
            };
        }

        shard.since_sweep += 1;
        if shard.since_sweep >= SWEEP_INTERVAL {
            shard.sweep();
        }
        shard.slots.insert(hash, Arc::downgrade(&schema));
        self.adopted.fetch_add(1, Ordering::Relaxed);

        Interned {
            schema,
            owned: true,
        }
    }

    /// Schemas that collapsed onto one already held, cumulative.
    ///
    /// Read against [`Self::adopted`], this says whether interning is earning
    /// its place: adoptions without collapses means schemas are arriving
    /// distinct and nothing is being shared.
    #[must_use]
    pub fn collapsed(&self) -> u64 {
        self.collapsed.load(Ordering::Relaxed)
    }

    /// Schemas the pool adopted, cumulative.
    #[must_use]
    pub fn adopted(&self) -> u64 {
        self.adopted.load(Ordering::Relaxed)
    }

    /// Slots currently held, live and dead alike.
    #[must_use]
    pub fn slots(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().slots.len())
            .sum()
    }
}

static GLOBAL: LazyLock<SchemaInterner> = LazyLock::new(SchemaInterner::new);

/// The process-wide pool.
#[must_use]
pub fn global() -> &'static SchemaInterner {
    &GLOBAL
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
        assert!(!Arc::ptr_eq(&a, &b), "the inputs start out distinct");

        let first = interner.intern(a);
        let second = interner.intern(b);

        assert!(Arc::ptr_eq(&first.schema, &second.schema));
        assert!(first.owned, "the first caller put it in the pool");
        assert!(!second.owned, "the second only points at it");
        assert_eq!(interner.collapsed(), 1);
        assert_eq!(interner.adopted(), 1);
    }

    #[test]
    fn interning_preserves_the_schema_exactly() {
        let interner = SchemaInterner::new();
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("b", DataType::Utf8, false),
                Field::new("a", DataType::Int64, true),
            ])
            .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
        );

        let shared = interner.intern(Arc::clone(&schema));

        assert_eq!(shared.schema.as_ref(), schema.as_ref());
        assert_eq!(shared.schema.fields()[0].name(), "b", "order preserved");
        assert!(!shared.schema.fields()[0].is_nullable());
        assert_eq!(
            shared.schema.metadata().get("k").map(String::as_str),
            Some("v")
        );
    }

    /// Metadata distinguishes two schemas, at both levels. Collapsing on fields
    /// alone would silently serve a schema stripped of what a caller put on it.
    #[test]
    fn schemas_differing_only_in_metadata_are_not_conflated() {
        let interner = SchemaInterner::new();
        let bare = schema_of(4);
        let table_meta = Arc::new(
            Schema::new(bare.fields().clone())
                .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
        );
        let field_meta = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Int64, true).with_metadata(HashMap::from([(
                "embedding".to_string(),
                "true".to_string(),
            )])),
        ]));
        let plain_one_col = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Int64,
            true,
        )]));

        let a = interner.intern(bare);
        let b = interner.intern(table_meta);
        let c = interner.intern(field_meta);
        let d = interner.intern(plain_one_col);

        assert!(!Arc::ptr_eq(&a.schema, &b.schema), "table metadata differs");
        assert!(!Arc::ptr_eq(&c.schema, &d.schema), "field metadata differs");
        assert!(a.schema.metadata().is_empty());
        assert_eq!(c.schema.fields()[0].metadata().len(), 1);
    }

    #[test]
    fn schemas_differing_only_in_nullability_are_not_conflated() {
        let interner = SchemaInterner::new();
        let nullable = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let required = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));

        assert!(!Arc::ptr_eq(
            &interner.intern(nullable).schema,
            &interner.intern(required).schema
        ));
    }

    /// The pool holds `Weak`s, so a row must never be what keeps a schema
    /// reachable — otherwise a retired query shape is pinned for the process
    /// lifetime.
    #[test]
    fn the_pool_does_not_keep_a_schema_alive() {
        let interner = SchemaInterner::new();
        let weak = {
            let shared = interner.intern(schema_of(4));
            let weak = Arc::downgrade(&shared.schema);
            drop(shared);
            weak
        };

        assert!(weak.upgrade().is_none(), "no strong reference may survive");
    }

    /// A schema whose holders are gone must not be served to the next caller.
    #[test]
    fn a_dead_slot_is_replaced_rather_than_served() {
        let interner = SchemaInterner::new();
        drop(interner.intern(schema_of(4)));

        let revived = interner.intern(schema_of(4));

        assert!(
            revived.owned,
            "the previous holder is gone, so this caller adopts the schema"
        );
        assert_eq!(interner.adopted(), 2, "both were adoptions, not a collapse");
        assert_eq!(interner.collapsed(), 0);
    }

    #[test]
    fn tombstones_do_not_accumulate_under_sustained_interning() {
        let interner = SchemaInterner::new();
        let attempts = SWEEP_INTERVAL * SHARDS * 4;
        for width in 0..attempts {
            drop(interner.intern(schema_of(width % 64)));
        }

        assert!(
            interner.slots() < attempts / 4,
            "dead slots must be reclaimed as interning proceeds, {} of {attempts} held",
            interner.slots()
        );
    }

    /// Every schema lands in one slot, so a collision between two live schemas
    /// is reachable. The pool must never serve one in place of the other; it
    /// declines to share the second instead.
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

    #[test]
    fn a_collision_declines_to_share_rather_than_serving_the_wrong_schema() {
        let interner = SchemaInterner::with_hasher(CollideEverything);
        let narrow = schema_of(4);
        let wide = schema_of(9);

        let first = interner.intern(Arc::clone(&narrow));
        let second = interner.intern(Arc::clone(&wide));

        assert_eq!(
            second.schema.as_ref(),
            wide.as_ref(),
            "the colliding caller must get its OWN schema back, never the other one"
        );
        assert!(Arc::ptr_eq(&second.schema, &wide));
        assert!(
            second.owned,
            "it is unshared, so it is charged like any unshared schema"
        );
        assert!(first.owned);

        // The occupant still shares normally.
        let again = interner.intern(schema_of(4));
        assert!(Arc::ptr_eq(&again.schema, &first.schema));
        assert!(!again.owned);
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
                            .map(|_| interner.intern(schema_of(16)).schema)
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
    }
}
