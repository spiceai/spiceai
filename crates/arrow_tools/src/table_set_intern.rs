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

//! A process-wide pool of shared input-table sets.
//!
//! A cached query result carries the set of tables its query read, so a later
//! write to any of them can invalidate it. That set is rebuilt for every query
//! — `get_logical_plan_input_tables` walks the plan and collects into a fresh
//! `HashSet` — so a workload of many point lookups over one table leaves one
//! private copy of the same one-element set per cached entry, names included.
//!
//! Measured on a v2.2.1 runtime over 100,000 distinct point lookups, comparing
//! a cache-enabled run against an identical cache-disabled one: giving the table
//! a 4,000-char name cost **+4,063 bytes per entry**, which is the whole name
//! copied per entry.
//!
//! This lives beside [`crate::intern`] rather than beside its user in the cache
//! crate because [`Internable`] is local here: the orphan rule allows a local
//! trait on the foreign `HashSet<TableReference>`, but would not allow the
//! cache crate to write the same impl.

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::{Arc, LazyLock};

use datafusion::sql::TableReference;

use crate::intern::{Internable, Interner};

/// The set a cached result carries, as the cache stores it.
pub type TableSet = HashSet<TableReference>;

/// Bytes an `Arc<T>` allocation costs beyond `T` itself: the strong and weak
/// counts that sit in front of the value.
const ARC_HEADER_BYTES: usize = 2 * size_of::<usize>();

/// The heap a [`TableReference`]'s name parts own, excluding the enum itself,
/// which the containing collection charges.
///
/// Each part is its own `Arc<str>` allocation, so each carries a header as well
/// as its characters.
#[must_use]
pub fn table_reference_heap_size(table_ref: &TableReference) -> usize {
    let parts: &[&Arc<str>] = match table_ref {
        TableReference::Bare { table } => &[table],
        TableReference::Partial { schema, table } => &[schema, table],
        TableReference::Full {
            catalog,
            schema,
            table,
        } => &[catalog, schema, table],
    };

    parts.iter().map(|part| ARC_HEADER_BYTES + part.len()).sum()
}

/// The deep size of an input-table set: the `HashSet` itself, its slots, and
/// the name each entry owns.
#[must_use]
pub fn table_set_deep_size(tables: &TableSet) -> usize {
    size_of::<TableSet>()
        + tables.capacity() * size_of::<TableReference>()
        + tables.iter().map(table_reference_heap_size).sum::<usize>()
}

impl Internable for TableSet {
    /// `HashSet` has no `Hash` impl, because its iteration order is not stable
    /// and the obvious element-by-element hash would therefore not be
    /// well-defined. Fold the elements commutatively instead, so that two sets
    /// with the same members hash alike whichever order they happen to iterate
    /// in.
    ///
    /// `wrapping_add` rather than `xor`: xor lets a pair of equal element
    /// hashes cancel to zero, and while a *set* cannot hold duplicates, two
    /// different sets whose members pair up that way would then collide
    /// needlessly. The length is mixed in so sets that differ only by a member
    /// hashing to zero stay apart.
    ///
    /// None of this has to be strong. `HashSet` *does* implement `Eq` as set
    /// equality, and the pool resolves every candidate with `==`, so a
    /// collision costs one extra comparison rather than a wrong answer.
    fn content_hash<H: Hasher>(&self, state: &mut H) {
        let mut combined: u64 = 0;
        for table in self {
            // A fixed-key hasher, so the fold is stable within a run: the
            // pool's own `BuildHasher` supplies the randomness, and it is
            // applied to the result below.
            let mut element = DefaultHasher::new();
            table.hash(&mut element);
            combined = combined.wrapping_add(element.finish());
        }
        state.write_u64(combined);
        state.write_usize(self.len());
    }

    fn deep_size(&self) -> usize {
        table_set_deep_size(self)
    }
}

/// A pool of shared input-table sets.
pub type TableSetInterner<S = ahash::RandomState> = Interner<TableSet, S>;

static GLOBAL: LazyLock<TableSetInterner> = LazyLock::new(TableSetInterner::new);

/// The process-wide pool.
#[must_use]
pub fn global() -> &'static TableSetInterner {
    &GLOBAL
}

/// Reclaims tombstones across the process-wide pool. See [`Interner::sweep`].
pub fn sweep() {
    GLOBAL.sweep();
}

/// Interns `tables` in the process-wide pool. See [`Interner::intern`].
#[must_use]
pub fn intern(tables: Arc<TableSet>) -> Arc<TableSet> {
    GLOBAL.intern(tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> Arc<TableSet> {
        Arc::new(names.iter().map(|n| TableReference::bare(*n)).collect())
    }

    /// Every part of a qualified name is its own allocation and must be charged.
    #[test]
    fn a_table_reference_is_charged_for_every_name_part() {
        let bare = TableReference::bare("t".repeat(32));
        let full = TableReference::full("c".repeat(32), "s".repeat(32), "t".repeat(32));

        assert!(
            table_reference_heap_size(&full) >= table_reference_heap_size(&bare) + 64,
            "a catalog- and schema-qualified name must cost more than a bare one, got {} vs {}",
            table_reference_heap_size(&full),
            table_reference_heap_size(&bare)
        );
    }

    /// The property the fold exists for: insertion order must not change the key.
    #[test]
    fn a_set_hashes_the_same_whatever_order_it_was_built_in() {
        let interner = TableSetInterner::new();
        let forwards = set(&["alpha", "bravo", "charlie"]);
        let backwards = set(&["charlie", "bravo", "alpha"]);
        assert!(
            !Arc::ptr_eq(&forwards, &backwards),
            "the two sets must start as distinct allocations for this to prove anything"
        );

        let first = interner.intern(forwards);
        let second = interner.intern(backwards);
        assert!(
            Arc::ptr_eq(&first, &second),
            "sets with the same members must collapse regardless of build order"
        );
    }

    /// Different contents must not be conflated, however the fold behaves.
    #[test]
    fn sets_with_different_members_stay_separate() {
        let interner = TableSetInterner::new();
        let orders = interner.intern(set(&["orders"]));
        let customers = interner.intern(set(&["customers"]));
        assert!(!Arc::ptr_eq(&orders, &customers));
        assert_eq!(interner.stats().rows, 2);
    }

    /// A set is more than its members: `{a}` and `{a, b}` must differ even
    /// though the first's members are a subset of the second's.
    #[test]
    fn a_subset_does_not_collapse_onto_its_superset() {
        let interner = TableSetInterner::new();
        let one = interner.intern(set(&["orders"]));
        let two = interner.intern(set(&["orders", "customers"]));
        assert!(!Arc::ptr_eq(&one, &two));
    }

    /// The saving is reported, since nothing charges for it per entry any more.
    #[test]
    fn the_pool_reports_the_bytes_it_shares() {
        let interner = TableSetInterner::new();
        let held = interner.intern(set(&["a_table_with_a_reasonably_long_name"]));
        let stats = interner.stats();
        assert_eq!(stats.rows, 1);
        assert_eq!(stats.value_bytes, table_set_deep_size(&held));
    }

    /// Weakly held: when the last holder goes, a sweep reclaims the row.
    #[test]
    fn a_row_is_reclaimed_once_its_last_holder_is_gone() {
        let interner = TableSetInterner::new();
        drop(interner.intern(set(&["transient"])));
        interner.sweep();
        assert_eq!(
            interner.stats().rows,
            0,
            "a set nothing holds any more must not keep the pool alive"
        );
    }
}
