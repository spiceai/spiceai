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
//! Three deliberate imprecisions, all in the over-counting direction, which is
//! the safe one for a bound:
//!
//! * An allocation reached through an `Arc` from two entries is charged to
//!   both. Sharing is not observable from a weigher.
//! * Collection capacity is charged as `capacity * size_of::<Entry>()`, which
//!   omits a hash table's control bytes and rounding. This matches how
//!   `arrow_schema::Field::size` charges its own metadata map, so a schema is
//!   sized the same way whoever asks.
//! * [`ENTRY_OVERHEAD_BYTES`] is a flat allowance, not a measurement.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::mem::size_of;

use arrow::datatypes::Schema;
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

/// Deep size of an Arrow [`Schema`], including the fields it owns and its
/// key-value metadata.
///
/// `arrow-schema` exposes `Fields::size` and `Field::size` but no `Schema::size`,
/// so the metadata map is charged here the same way `Field::size` charges its own.
pub(crate) fn schema_size(schema: &Schema) -> usize {
    size_of::<Schema>() + schema.fields().size() + string_map_size(&schema.metadata)
}

/// Deep size of a `HashMap<String, String>`: its slots plus the bytes each
/// string owns.
pub(crate) fn string_map_size<S: BuildHasher>(map: &HashMap<String, String, S>) -> usize {
    map.capacity() * size_of::<(String, String)>()
        + map
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>()
}

/// The bytes a [`TableReference`]'s name parts own on the heap, excluding the
/// enum itself — the caller charges that through its containing collection.
pub(crate) fn table_reference_heap_size(table_ref: &TableReference) -> usize {
    match table_ref {
        TableReference::Bare { table } => table.len(),
        TableReference::Partial { schema, table } => schema.len() + table.len(),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => catalog.len() + schema.len() + table.len(),
    }
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

/// Deep size of a `Vec<String>`: its slots plus the bytes each string owns.
pub(crate) fn string_vec_size(strings: &[String]) -> usize {
    size_of::<Vec<String>>()
        + std::mem::size_of_val(strings)
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
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn a_wider_schema_is_charged_more_than_a_narrow_one() {
        let narrow = Schema::new(
            (0..4)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        );
        let wide = Schema::new(
            (0..200)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        );

        assert!(
            schema_size(&wide) > 20 * schema_size(&narrow),
            "a 200-column schema must be charged far more than a 4-column one, got {} vs {}",
            schema_size(&wide),
            schema_size(&narrow)
        );
    }

    #[test]
    fn a_schema_is_charged_for_its_metadata() {
        let bare = Schema::new(vec![Field::new("col", DataType::Int64, true)]);
        let annotated = bare.clone().with_metadata(HashMap::from([(
            "spice.origin".to_string(),
            "x".repeat(4_096),
        )]));

        assert!(
            schema_size(&annotated) >= schema_size(&bare) + 4_096,
            "schema metadata must be charged, got {} vs {}",
            schema_size(&annotated),
            schema_size(&bare)
        );
    }

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
