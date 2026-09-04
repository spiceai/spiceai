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
//!   [`crate::intern::schema`] counts those bytes once and publishes them,
//!   so the residual is reported rather than enforced. See
//!   [`crate::result::query::CachedQueryResult::memory_size`].
//! * Collection slots are charged as `len`- or `capacity`-times-entry-size,
//!   which omits a hash table's control bytes and a `Vec`'s spare capacity.
//! * [`ENTRY_OVERHEAD_BYTES`] is one flat per-entry allowance calibrated
//!   against a measurement, not a model of any particular store.
//!
//! Exactness is not what `max_size` needs. What it needs — and what the
//! pre-fix accounting did not give it — is a figure *proportional to what the
//! entry holds*, so that a budget expressed in bytes constrains how many
//! entries fit under it.

use std::mem::size_of;


/// Bytes charged to every cache entry for the store's own per-entry bookkeeping.
///
/// A weigher is handed only the value, so nothing the store allocates *around*
/// that value is reachable from it: moka's entry record, the two intrusive
/// lists an entry sits on, its key handle, its hash-table slot, and the
/// allocator's rounding on each of those. It still has to be charged, or a
/// stream of individually tiny entries is free and `max_size` cannot bound a
/// high-cardinality workload of 0-row results at all.
///
/// **A flat allowance, calibrated once against a measurement.** An earlier
/// version of this derived most of it with `size_of` over invented structs
/// modelling moka's internals; that was worse than useless, because moka's
/// types are private so the models could not track them, and the fitted
/// remainder was two thirds of the total anyway. One honest number beats
/// arithmetic that looks derived and is not.
///
/// The measurement: a `spiced` holding 100,000 cached 0-row point-lookup
/// results gave up ~1,225 B per entry over an identical cache-disabled run
/// (`phys_footprint`, macOS/arm64, snmalloc). Of that, the entry struct and its
/// batch vector account for ~136 B and the shared schema and input-table set
/// for ~430 B across *all* entries, leaving this as what one more entry costs.
///
/// It is a per-entry cost of the store and the allocator, so it moves with
/// platform, allocator and moka version. `results_cache_growth` in the runtime
/// crate is what keeps it honest: it fills a real cache and asserts the
/// reported total stays within a factor of the live heap, so a platform where
/// this is badly wrong fails there rather than silently over- or under-billing.
pub(crate) const ENTRY_OVERHEAD_BYTES: usize = 700;

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

