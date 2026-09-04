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
//!   Schemas are the deliberate exception, in the other direction: they are
//!   interned, so one allocation backs every entry of the same shape, and
//!   charging it per entry would bill a wide schema once for every entry that
//!   merely points at it. A schema unique to a single entry is not charged
//!   either — the same deliberate choice, accepting that such a workload can
//!   exceed `max_size` by the size of its schemas.
//!   [`arrow_tools::schema_intern`] counts those bytes once and publishes them,
//!   so the residual is reported rather than enforced. See
//!   [`crate::result::query::CachedQueryResult::memory_size`].
//! * Collection slots are charged as `len`- or `capacity`-times-entry-size,
//!   which omits a hash table's control bytes and a `Vec`'s spare capacity.
//! * [`ENTRY_OVERHEAD_BYTES`] is a flat allowance, not a measurement.
//!
//! Exactness is not what `max_size` needs. What it needs — and what the
//! pre-fix accounting did not give it — is a figure *proportional to what the
//! entry holds*, so that a budget expressed in bytes constrains how many
//! entries fit under it.

use std::mem::size_of;
use std::sync::Arc;


/// A model of the record a cache store keeps per entry.
///
/// Not a store's actual type — moka's `ValueEntry` and the Pingora engine's
/// node are both private — but the fields any of them must keep: the key, a
/// pointer to the value, and the two instants an expiring cache compares
/// against. Sizing a model with `size_of` keeps the charge derived from
/// something real and self-updating, rather than a number someone once
/// measured and nobody can re-derive.
struct StoreEntryRecord {
    _key: u64,
    _value: Arc<()>,
    _inserted_at: std::time::Instant,
    _last_accessed: std::time::Instant,
}

/// A model of one intrusive LRU list node. A store that evicts by both recency
/// and age keeps the entry on two such lists.
struct StoreDequeNode {
    _prev: Option<std::ptr::NonNull<()>>,
    _next: Option<std::ptr::NonNull<()>>,
    _key_hash: u64,
    _timestamp: std::time::Instant,
}

/// How many intrusive lists an entry sits on: access order and write order.
const STORE_DEQUE_LISTS: usize = 2;

/// The hash-table slot an entry occupies: its key and a pointer to its record.
/// Charged at double, because an open-addressing table is grown well before it
/// is full and the empty slots are as real as the occupied ones.
const STORE_SLOT_BYTES: usize = 2 * (size_of::<u64>() + size_of::<usize>());

/// What a small allocation actually consumes, over what it asks for.
///
/// Every item modelled above is its own allocation, and an allocator serves
/// each from a size class rounded up from the request — snmalloc, the runtime's
/// default, spaces its classes at 1/4 steps, so a request lands on average an
/// eighth over and never more than a quarter. A numerator/denominator pair so
/// the whole constant stays a `const` computation.
const ALLOCATOR_ROUNDING_NUMERATOR: usize = 9;
const ALLOCATOR_ROUNDING_DENOMINATOR: usize = 8;

/// The bytes the models above do not name, per entry.
///
/// `size_of` can only be applied to structures that are written down, and a
/// store's are private: moka threads an `EntryInfo` and a key handle through
/// each of the three structures an entry sits in, and none of that is
/// reachable from here. This covers the difference, and it is the only figure
/// in this module taken from a measurement rather than derived.
///
/// Calibrated once, against a v2.2.1 runtime holding 100,000 cached 0-row
/// point-lookup results, which held **1,225 B per entry** over an identical
/// cache-disabled run. For that shape the named parts model 760 B — the entry
/// struct (96), its schema (192), its input-table set (238) and the store model
/// above (234) — leaving this remainder.
///
/// [`the_modelled_entry_matches_what_was_measured`] re-derives that comparison
/// and fails if either side drifts, so a change to the models is checked
/// against the measurement rather than silently absorbed by this number.
const UNMODELLED_STORE_BYTES: usize = 465;

/// Bytes charged to every cache entry for the store's own per-entry bookkeeping.
///
/// A weigher is handed only the value, so none of what the store allocates
/// *around* that value is reachable from it. It still has to be charged: without
/// it a stream of individually tiny entries is free, and `max_size` cannot bound
/// a high-cardinality workload of 0-row results at all, however accurately the
/// rest of this module counts.
///
/// Derived from the models above rather than guessed, and deliberately one
/// figure for every store rather than a per-engine number with false precision —
/// the engines differ by less than the allocator rounding does.
///
/// **Checked against a measurement, not taken from one.** On a v2.2.1 runtime
/// holding 100,000 cached 0-row point-lookup results, the memory the process
/// actually held per entry, over an identical cache-disabled run, was ~1,225 B;
/// subtracting the schema and input-table set that are now shared leaves the
/// residual this constant is for. `entry_overhead_is_close_to_what_a_store_holds`
/// asserts the two stay within a factor of two of each other, which is the
/// accuracy `max_size` needs: a bound proportional to what an entry holds, not
/// an exact byte count.
pub(crate) const ENTRY_OVERHEAD_BYTES: usize = (arc_heap_size::<StoreEntryRecord>()
    + STORE_DEQUE_LISTS * arc_heap_size::<StoreDequeNode>()
    + STORE_SLOT_BYTES)
    * ALLOCATOR_ROUNDING_NUMERATOR
    / ALLOCATOR_ROUNDING_DENOMINATOR
    + UNMODELLED_STORE_BYTES;

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

#[cfg(test)]
mod derivation_tests {
    use super::*;
    use std::collections::HashSet;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::sql::TableReference;

    /// What one entry of the measured shape was actually observed to hold.
    ///
    /// A v2.2.1 runtime, 100,000 distinct `SELECT 1 FROM t WHERE id = $v LIMIT 1`
    /// queries each storing its own entry, `phys_footprint` growth per entry with
    /// an identical cache-disabled run subtracted.
    const MEASURED_BYTES_PER_ENTRY: usize = 1_225;

    /// How far the model may sit from that measurement.
    ///
    /// `max_size` needs a charge proportional to what an entry holds, not an
    /// exact byte count — an entry is billed before the allocator has been asked
    /// for anything, so exactness is not available at any price. A quarter is
    /// tight enough that a model which stopped describing the store would fail,
    /// and loose enough to survive a different allocator or platform.
    const TOLERANCE_NUMERATOR: usize = 1;
    const TOLERANCE_DENOMINATOR: usize = 4;

    /// The shape that was measured: one `Int64` output column, one input table.
    fn measured_shape() -> (Schema, HashSet<TableReference>) {
        (
            Schema::new(vec![Field::new("Int64(1)", DataType::Int64, false)]),
            HashSet::from([TableReference::bare("lookup")]),
        )
    }

    /// The models, summed, must land near what such an entry was measured to
    /// hold. This is what keeps [`UNMODELLED_STORE_BYTES`] honest: change a
    /// model and the comparison moves, rather than the difference disappearing
    /// into a constant nobody re-derives.
    #[test]
    fn the_modelled_entry_matches_what_was_measured() {
        let (schema, tables) = measured_shape();
        let modelled = size_of::<crate::result::query::CachedQueryResult>()
            + arrow_tools::schema_intern::schema_deep_size(&schema)
            + arrow_tools::table_set_intern::table_set_deep_size(&tables)
            + ENTRY_OVERHEAD_BYTES;

        let slack = MEASURED_BYTES_PER_ENTRY * TOLERANCE_NUMERATOR / TOLERANCE_DENOMINATOR;
        assert!(
            modelled.abs_diff(MEASURED_BYTES_PER_ENTRY) <= slack,
            "the modelled cost of one entry is {modelled} B but such an entry was measured to \
             hold {MEASURED_BYTES_PER_ENTRY} B, more than {slack} B apart; re-derive \
             UNMODELLED_STORE_BYTES against a fresh measurement rather than widening this"
        );
    }

    /// Once a shape is shared, the charge is what one *more* entry over that
    /// shape costs — the schema and table set are already resident, so billing
    /// them again would make `max_size` scale with entries times shape size.
    #[test]
    fn the_charge_is_the_marginal_cost_of_one_more_entry() {
        let (schema, tables) = measured_shape();
        let shared = arrow_tools::schema_intern::schema_deep_size(&schema)
            + arrow_tools::table_set_intern::table_set_deep_size(&tables);
        let marginal = MEASURED_BYTES_PER_ENTRY - shared;

        let charged = size_of::<crate::result::query::CachedQueryResult>() + ENTRY_OVERHEAD_BYTES;
        let slack = marginal * TOLERANCE_NUMERATOR / TOLERANCE_DENOMINATOR;
        assert!(
            charged.abs_diff(marginal) <= slack,
            "an entry sharing its shape is billed {charged} B, but one more such entry costs \
             about {marginal} B"
        );
    }
}
