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

//! Machine-checked array indices for the sharded cache.
//!
//! Every fixed-size array this cache subscripts is subscripted by a number
//! derived from a cache key: [`shard_index`] picks the shard mutex and the
//! touch buffer on every get, insert and remove, and [`sketch_cell_index`]
//! picks the `TinyLFU` counter cell on every admission, under that mutex.
//! Neither constrains the key it is given, so an out-of-range result is a
//! panic reached by an ordinary cache lookup — and one no test can rule out by
//! sampling `u64`s.
//!
//! So the bounds are postconditions
//! [Verus](https://github.com/verus-lang/verus) discharges for every input
//! rather than properties the reader is asked to see. What they pin is the
//! agreement between a reduction and the array it indexes. The proofs hold for
//! any non-zero `NUM_SHARDS` or [`SKETCH_WIDTH`], and stop holding the moment
//! a reduction is expressed as something other than the constant its array is
//! sized by — a hand-rolled mask, say, left behind by a change to the shard
//! count.
//!
//! This is the executable encoding, not a model of it: the functions below are
//! the ones the cache calls. Verus reads the `verus!` block; a normal
//! `cargo build` erases the specifications and compiles the bodies as ordinary
//! Rust. `cargo verus focus` re-checks the proofs.
//!
//! Deliberately not verified here: the byte-budget arithmetic. The weight
//! counters are atomics written under concurrency, and Verus discharges
//! sequential reasoning only, so a proof about them would say less than it
//! looks like it says. The segment caps (`window_cap` / `protected_cap`) are
//! pure and could carry one, but the property worth stating — that the two
//! caps fit inside `max_weight` — is false at the degenerate budgets of 0 and
//! 1 byte, where the `.max(1)` floors each cap above the whole budget. A
//! proof would have to assert something weaker than the reader would assume
//! from seeing it there.

use vstd::prelude::*;

verus! {

/// Number of shards. Keys map to shard `key % NUM_SHARDS`.
pub const NUM_SHARDS: usize = 16;

/// Rows in the `TinyLFU` count-min sketch.
pub const SKETCH_DEPTH: usize = 4;

/// Counters per sketch row.
///
/// A power of two, so the `% SKETCH_WIDTH` below is the bitmask the compiler
/// emits for it — the bound, however, does not depend on that and holds for
/// any non-zero width.
pub const SKETCH_WIDTH: usize = 4096;

/// Maps a cache key to its shard.
///
/// The result subscripts `ShardedCache::shards` and
/// `ShardedCache::touch_buffers`, both `[_; NUM_SHARDS]`.
#[inline]
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "only the low bits of the key select a shard, and the bound below is proved for whatever the cast yields"
)]
pub fn shard_index(key: u64) -> (idx: usize)
    ensures
        idx < NUM_SHARDS,
{
    // Only the low bits of the key select a shard, so the truncating cast on a
    // 32-bit target loses nothing the modulo would have kept.
    (key as usize) % NUM_SHARDS
}

/// Maps a mixed key and a sketch row to its counter cell.
///
/// The result subscripts the sketch's flat `SKETCH_DEPTH * SKETCH_WIDTH`
/// counter array. `mix` is the row-seeded hash of the key: it is unconstrained
/// on purpose, because the bound must not rest on the quality — or the
/// stability — of the mixing function.
#[inline]
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "only the low bits of the mix select a column, and the bound below is proved for whatever the cast yields"
)]
pub fn sketch_cell_index(mix: u64, row: usize) -> (idx: usize)
    requires
        row < SKETCH_DEPTH,
    ensures
        idx < SKETCH_DEPTH * SKETCH_WIDTH,
{
    row * SKETCH_WIDTH + (mix as usize) % SKETCH_WIDTH
}

} // verus!

#[cfg(test)]
mod tests {
    use super::{NUM_SHARDS, SKETCH_DEPTH, SKETCH_WIDTH, shard_index, sketch_cell_index};

    #[test]
    fn shard_index_is_the_key_modulo_the_shard_count() {
        assert_eq!(shard_index(0), 0);
        assert_eq!(shard_index(17), 1);
        assert_eq!(shard_index(u64::MAX), NUM_SHARDS - 1);
    }

    #[test]
    fn sketch_cell_index_packs_rows_without_overlapping() {
        // Each row owns a contiguous, disjoint span of the flat counter array.
        for row in 0..SKETCH_DEPTH {
            assert_eq!(sketch_cell_index(0, row), row * SKETCH_WIDTH);
            assert_eq!(
                sketch_cell_index(u64::MAX, row),
                row * SKETCH_WIDTH + SKETCH_WIDTH - 1
            );
        }
    }

    /// The proofs hold for any non-zero width, but the reductions only compile
    /// to a mask while these are powers of two. A width that is not would put a
    /// division on the get and admission paths and nothing else would say so.
    #[test]
    fn the_reduced_widths_are_powers_of_two() {
        assert!(NUM_SHARDS.is_power_of_two());
        assert!(SKETCH_WIDTH.is_power_of_two());
    }
}
