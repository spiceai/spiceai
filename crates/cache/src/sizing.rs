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

//! Deep-size helpers shared by the crate's [`crate::Sizeable`] implementations.
//!
//! `max_size` is enforced by a weigher that is handed only the cached *value* —
//! moka's `weigher` and the Pingora backend's `insert` both work from it alone —
//! so every allocation the value reaches through a pointer is invisible to the
//! budget unless it is counted here. Counting only what a value owns inline
//! makes `max_size` a bound on array bytes rather than on the memory the cache
//! holds: a 0-row query result contributes no array bytes at all, so 100% of
//! its real cost went unbilled and the byte budget could never evict it.
//!
//! What this produces is an estimate, not an exact figure. Three deliberate
//! imprecisions:
//!
//! * An allocation reached through an `Arc` from two entries is charged to
//!   both. Sharing is generally not observable from a weigher, and
//!   over-charging evicts sooner, which is the safe direction for a budget.
//!   Schemas are the deliberate exception: they are interned, so one allocation
//!   backs every entry of the same shape, and charging it per entry would bill
//!   a wide schema once for every entry that merely points at it.
//!   [`arrow_tools::schema_intern`] counts those bytes once instead.
//! * Collection slots are charged as `len`- or `capacity`-times-entry-size,
//!   which omits a hash table's control bytes and a `Vec`'s spare capacity.
//! * [`ENTRY_OVERHEAD_BYTES`] is a flat allowance, not a measurement.
//!
//! Exactness is not what `max_size` needs. What it needs — and what the
//! pre-fix accounting did not give it — is a figure *proportional to what the
//! entry holds*, so that a budget expressed in bytes constrains how many
//! entries fit under it.

use std::collections::HashSet;
use std::hash::BuildHasher;
use std::mem::size_of;
use std::sync::Arc;

use datafusion::sql::TableReference;

/// Bytes charged to every cache entry for the store's own per-entry bookkeeping.
///
/// A weigher cannot see what the store allocates around the value it is
/// weighing — moka's entry record and its two LRU deque nodes, or the Pingora
/// engine's node and metadata shard slot — so this is an *allowance* covering
/// them, not a measurement of either store's internals. It is deliberately one
/// documented constant rather than a per-engine figure with false precision.
///
/// Its job is to keep a stream of individually tiny entries from being free:
/// without it, `max_size` cannot bound a workload of high-cardinality 0-row
/// results at all, however accurately the rest of this module counts.
pub(crate) const ENTRY_OVERHEAD_BYTES: usize = 256;

/// Bytes an `Arc<T>` allocation costs beyond `T` itself: the strong and weak
/// counts that sit in front of the value.
pub(crate) const ARC_HEADER_BYTES: usize = 2 * size_of::<usize>();

/// The heap an `Arc<T>` owns: its two reference counts plus the `T` behind them.
///
/// Callers add this on top of the pointer, which their own `size_of::<Self>()`
/// already covers.
pub(crate) const fn arc_heap_size<T>() -> usize {
    ARC_HEADER_BYTES + size_of::<T>()
}

/// The bytes a [`TableReference`]'s name parts own on the heap, excluding the
/// enum itself — the caller charges that through its containing collection.
///
/// Each part is its own `Arc<str>` allocation, so each carries a header as well
/// as its characters.
pub(crate) fn table_reference_heap_size(table_ref: &TableReference) -> usize {
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

/// Deep size of the input-table set every cached result carries for invalidation.
///
/// A fresh set is allocated per query by
/// [`crate::get_logical_plan_input_tables`], so this is per-entry cost rather
/// than something amortised across the cache.
pub(crate) fn table_refs_size<S: BuildHasher>(tables: &HashSet<TableReference, S>) -> usize {
    size_of::<HashSet<TableReference, S>>()
        + tables.capacity() * size_of::<TableReference>()
        + tables.iter().map(table_reference_heap_size).sum::<usize>()
}

/// The heap a `Vec<String>` owns — its slots plus the bytes each string owns —
/// excluding the outer `Vec` struct, which its container already charges.
pub(crate) fn string_vec_heap_size(strings: &[String]) -> usize {
    std::mem::size_of_val(strings)
        + strings
            .iter()
            .map(std::string::String::capacity)
            .sum::<usize>()
}

/// The heap a `Vec<Vec<f32>>` owns, excluding the outer `Vec` struct itself.
pub(crate) fn f32_vectors_heap_size(vectors: &[Vec<f32>]) -> usize {
    std::mem::size_of_val(vectors)
        + vectors
            .iter()
            .map(|vector| vector.capacity() * size_of::<f32>())
            .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn an_empty_table_set_still_costs_its_container() {
        let empty: HashSet<TableReference> = HashSet::new();
        assert!(
            table_refs_size(&empty) >= size_of::<HashSet<TableReference>>(),
            "an empty set is still an allocation the entry holds"
        );
    }

    /// The pre-fix accounting read `vectors.len() * first.len()`, which is wrong
    /// for a ragged batch — the shape a base64/float mix or a failed embedding
    /// can produce.
    #[test]
    fn ragged_vectors_are_charged_per_vector() {
        let ragged = vec![vec![0.0_f32; 1], vec![0.0_f32; 1_024]];
        let uniform_by_first = ragged.len() * ragged[0].len() * size_of::<f32>();

        assert!(
            f32_vectors_heap_size(&ragged) > 1_024 * size_of::<f32>(),
            "the long vector must be charged in full, got {}",
            f32_vectors_heap_size(&ragged)
        );
        assert!(
            f32_vectors_heap_size(&ragged) > uniform_by_first,
            "charging every vector the first one's length under-counts a ragged batch"
        );
    }
}
