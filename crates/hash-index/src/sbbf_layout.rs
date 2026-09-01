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

//! Machine-checked block selection for [`crate::SplitBlockBloomFilter`].
//!
//! [`block_index`] returns an index used to subscript the filter's block
//! vector, so an out-of-range result is a panic on a lock-free read path. The
//! bound is not obvious by inspection: it rests on the multiply-shift range
//! reduction cancelling exactly, across a `u128` widening and a truncating cast
//! back to `usize`. Here the bound is a postcondition the
//! [Verus](https://github.com/verus-lang/verus) verifier discharges for every
//! `(num_blocks, hash)` pair, rather than for the pairs a test happens to pick.
//!
//! Verus reads the `verus!` block; a normal `cargo build` erases the
//! specifications and compiles the function bodies as ordinary Rust, so this
//! module needs no verifier to build. `cargo verus focus` re-checks it.

use vstd::prelude::*;

verus! {

/// Selects the block for `hash` by multiply-shift range reduction over the
/// high 32 bits, leaving the low 32 bits independent for the in-block bit
/// positions.
///
/// The multiply widens to `u128` so block counts beyond `2^32` (a >128 GiB
/// filter) cannot overflow the product. The `ensures` clause below is the
/// property the subscript depends on: the double shift leaves the result
/// within block range, so the cast back to `usize` cannot truncate.
///
/// `num_blocks` of 0 has no block to select and yields 0;
/// [`crate::SplitBlockBloomFilter::new`] always allocates at least one.
pub fn block_index(num_blocks: usize, hash: u64) -> (index: usize)
    ensures
        num_blocks >= 1 ==> index < num_blocks,
{
    if num_blocks == 0 {
        return 0;
    }
    let hi: u128 = u128::from(hash >> 32);
    let len: u128 = num_blocks as u128;

    // The high half is what the reduction ranges over: at most 2^32 - 1.
    assert(hash >> 32 <= 0xffff_ffffu64) by (bit_vector);
    // So the product is under 2^96 and cannot overflow the u128 it widens to.
    assert(hi * len <= 0xffff_ffffu128 * len) by (nonlinear_arith)
        requires hi <= 0xffff_ffffu128;
    assert(0xffff_ffffu128 * len <= 0xffff_ffffu128 * 0x1_0000_0000_0000_0000u128)
        by (nonlinear_arith)
        requires len <= 0xffff_ffff_ffff_ffffu128;

    let product: u128 = hi * len;

    // Shifting the 32 bits back off divides by exactly what the reduction
    // multiplied in, so the quotient lands strictly below `num_blocks`.
    assert(product >> 32 == product / 0x1_0000_0000u128) by (bit_vector);
    assert(product / 0x1_0000_0000u128 < len) by (nonlinear_arith)
        requires product <= 0xffff_ffffu128 * len, len >= 1;

    (product >> 32) as usize
}

}
