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

//! Machine-checked encodings a run's correctness rests on, as the verified
//! functions [`crate::tiered`] and [`crate::encode`] call.
//!
//! - [`posting`] and [`decode_posting`]: a row is stored as
//!   `position * files + file`. For every in-range row the posting fits 63 bits and decodes back
//!   to the row, and postings are strictly increasing in `(position, file)`
//!   order, so renumbering a run's files into a merged run (adding an offset to
//!   each file, [`lemma_renumbering_keeps_order`]) keeps its postings sorted.
//! - [`lone_slot`], [`offset_slot`], [`slot_is_lone`], [`slot_offset`]: a
//!   word's slot holds its only posting, or [`MULTI`] and an offset into the
//!   run's postings; the two forms are never confused and each decodes back.
//! - [`fold_word`]: the word of a key whose fields fit 8 bytes is their bytes
//!   as one big-endian integer. It never overflows, and two value byte strings
//!   of the same length have the same word only when they are the same bytes
//!   ([`lemma_word_is_injective`]), so keys whose encodings differ never share
//!   a word. That is a property of the encoded bytes: the encoding itself
//!   gives some distinct float values one encoding (see `encode`).
//!
//! Verus reads the `verus!` block; a normal `cargo build` erases the
//! specifications and compiles the bodies as ordinary Rust. `cargo verus
//! focus` re-checks the proofs.

use vstd::prelude::*;

verus! {

/// Row positions a posting can hold: below `2^40`.
pub const POSITION_LIMIT: u64 = 0x100_0000_0000;

/// Files a run can cover: `2^23`, so a posting stays below `2^63`.
pub const FILE_LIMIT: u64 = 0x80_0000;

/// A slot with this bit set holds an offset; one without it is a posting.
pub const MULTI: u32 = 0x8000_0000;

/// The posting of row `position` of file `file`, in a run of `files` files.
#[must_use]
pub fn posting(position: u64, file: u64, files: u64) -> (result: u64)
    requires
        position < POSITION_LIMIT,
        file < files,
        files <= FILE_LIMIT,
    ensures
        result == position * files + file,
        result < 0x8000_0000_0000_0000u64,
{
    proof {
        assert(position * files + file < POSITION_LIMIT * FILE_LIMIT) by (nonlinear_arith)
            requires
                position < POSITION_LIMIT,
                file < files,
                files <= FILE_LIMIT,
        ;
        assert(POSITION_LIMIT * FILE_LIMIT == 0x8000_0000_0000_0000u64) by (compute);
    }
    position * files + file
}

/// The `(file, position)` of a posting in a run of `files` files.
#[must_use]
pub fn decode_posting(posting: u64, files: u64) -> (result: (u64, u64))
    requires
        files >= 1,
    ensures
        result.0 == posting % files,
        result.1 == posting / files,
        result.0 < files,
{
    (posting % files, posting / files)
}

/// A posting decodes back to the row it was made from.
pub proof fn lemma_posting_round_trips(position: u64, file: u64, files: u64)
    requires
        position < POSITION_LIMIT,
        file < files,
        files <= FILE_LIMIT,
    ensures
        (position * files + file) % (files as int) == file,
        (position * files + file) / (files as int) == position,
{
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse(
        (position * files + file) as int,
        files as int,
        position as int,
        file as int,
    );
}

/// Postings are strictly increasing in `(position, file)` order.
pub proof fn lemma_posting_order(p1: u64, f1: u64, p2: u64, f2: u64, files: u64)
    requires
        f1 < files,
        f2 < files,
        p1 < p2 || (p1 == p2 && f1 < f2),
    ensures
        p1 * files + f1 < p2 * files + f2,
{
    assert(p1 * files + f1 < p2 * files + f2) by (nonlinear_arith)
        requires
            f1 < files,
            f2 < files,
            p1 < p2 || (p1 == p2 && f1 < f2),
    ;
}

/// Conversely, a smaller posting is an earlier `(position, file)` row.
pub proof fn lemma_posting_order_reflects(p1: u64, f1: u64, p2: u64, f2: u64, files: u64)
    requires
        f1 < files,
        f2 < files,
        p1 * files + f1 < p2 * files + f2,
    ensures
        p1 < p2 || (p1 == p2 && f1 < f2),
{
    if p2 < p1 || (p1 == p2 && f2 <= f1) {
        if p2 < p1 || f2 < f1 {
            lemma_posting_order(p2, f2, p1, f1, files);
        }
        assert(false);
    }
}

/// A run's postings stay sorted when its files are renumbered into a merged
/// run: file `f` becomes `f + offset`, of `merged` files.
pub proof fn lemma_renumbering_keeps_order(
    p1: u64,
    f1: u64,
    p2: u64,
    f2: u64,
    files: u64,
    offset: u64,
    merged: u64,
)
    requires
        f1 < files,
        f2 < files,
        f1 + offset < merged,
        f2 + offset < merged,
        p1 * files + f1 < p2 * files + f2,
    ensures
        p1 * merged + (f1 + offset) < p2 * merged + (f2 + offset),
{
    lemma_posting_order_reflects(p1, f1, p2, f2, files);
    lemma_posting_order(p1, (f1 + offset) as u64, p2, (f2 + offset) as u64, merged);
}

/// The slot of a word whose only posting is `posting`.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "`posting` is below `MULTI` (2^31), which `requires` guarantees"
)]
pub fn lone_slot(posting: u64) -> (slot: u32)
    requires
        posting < MULTI as u64,
    ensures
        slot as u64 == posting,
        slot & MULTI == 0,
{
    let slot = posting as u32;
    assert(slot & 0x8000_0000u32 == 0) by (bit_vector)
        requires
            slot == posting as u32,
            posting < 0x8000_0000u64,
    ;
    slot
}

/// The slot of a word whose postings start at `offset` in the run's postings.
#[must_use]
pub fn offset_slot(offset: u32) -> (slot: u32)
    requires
        offset < MULTI,
    ensures
        slot & MULTI != 0,
        slot & 0x7FFF_FFFFu32 == offset,
{
    let slot = offset | MULTI;
    assert(slot & 0x8000_0000u32 != 0 && slot & 0x7FFF_FFFFu32 == offset) by (bit_vector)
        requires
            slot == offset | 0x8000_0000u32,
            offset < 0x8000_0000u32,
    ;
    slot
}

/// Whether `slot` is a word's only posting rather than an offset.
#[must_use]
pub fn slot_is_lone(slot: u32) -> (lone: bool)
    ensures
        lone == (slot & MULTI == 0),
{
    slot & MULTI == 0
}

/// The postings offset an offset slot holds.
#[must_use]
pub fn slot_offset(slot: u32) -> (offset: u32)
    ensures
        offset == slot & 0x7FFF_FFFFu32,
        offset < MULTI,
{
    let offset = slot & 0x7FFF_FFFF;
    assert(offset < 0x8000_0000u32) by (bit_vector)
        requires
            offset == slot & 0x7FFF_FFFFu32,
    ;
    offset
}

/// `s` as a big-endian integer.
pub open spec fn be(s: Seq<u8>) -> nat
    decreases s.len(),
{
    if s.len() == 0 {
        0
    } else {
        be(s.drop_last()) * 256 + s.last() as nat
    }
}

/// `256^n`.
pub open spec fn base(n: nat) -> nat
    decreases n,
{
    if n == 0 {
        1
    } else {
        base((n - 1) as nat) * 256
    }
}

proof fn lemma_be_below_base(s: Seq<u8>)
    ensures
        be(s) < base(s.len()),
    decreases s.len(),
{
    if s.len() > 0 {
        let init = s.drop_last();
        lemma_be_below_base(init);
        let (b, bi, l) = (be(init), base(init.len()), s.last() as nat);
        assert(b * 256 + l < bi * 256) by (nonlinear_arith)
            requires
                b < bi,
                l < 256,
        ;
    }
}

proof fn lemma_base_eight()
    ensures
        base(8) == 0x1_0000_0000_0000_0000nat,
{
    reveal_with_fuel(base, 9);
}

proof fn lemma_base_monotone(m: nat, n: nat)
    requires
        m <= n,
    ensures
        base(m) <= base(n),
    decreases n,
{
    if m < n {
        lemma_base_monotone(m, (n - 1) as nat);
    }
}

/// Two byte strings of one length with the same big-endian value are equal.
pub proof fn lemma_word_is_injective(a: Seq<u8>, b: Seq<u8>)
    requires
        a.len() == b.len(),
        be(a) == be(b),
    ensures
        a == b,
    decreases a.len(),
{
    if a.len() > 0 {
        let (ai, bi) = (a.drop_last(), b.drop_last());
        let (x, y, la, lb) = (be(ai), be(bi), a.last() as nat, b.last() as nat);
        assert(x == y && la == lb) by (nonlinear_arith)
            requires
                x * 256 + la == y * 256 + lb,
                la < 256,
                lb < 256,
        ;
        lemma_word_is_injective(ai, bi);
        assert(a =~= ai.push(a.last()));
        assert(b =~= bi.push(b.last()));
    } else {
        assert(a =~= b);
    }
}

/// The word of value bytes that fit 8 bytes: their big-endian integer.
#[must_use]
#[expect(
    clippy::cast_lossless,
    reason = "the verifier follows `as` casts; a byte always widens exactly"
)]
pub fn fold_word(bytes: &[u8]) -> (word: u64)
    requires
        bytes@.len() <= 8,
    ensures
        word as nat == be(bytes@),
{
    let mut word: u64 = 0;
    let mut i: usize = 0;
    while i < bytes.len()
        invariant
            i <= bytes@.len(),
            bytes@.len() <= 8,
            word as nat == be(bytes@.subrange(0, i as int)),
        decreases bytes@.len() - i,
    {
        proof {
            let prefix = bytes@.subrange(0, i as int);
            let next = bytes@.subrange(0, i as int + 1);
            assert(next.drop_last() =~= prefix);
            assert(next.last() == bytes@[i as int]);
            lemma_be_below_base(next);
            lemma_base_monotone(next.len(), 8);
            lemma_base_eight();
        }
        word = word * 256 + bytes[i] as u64;
        i += 1;
    }
    assert(bytes@.subrange(0, i as int) =~= bytes@);
    word
}

} // verus!
