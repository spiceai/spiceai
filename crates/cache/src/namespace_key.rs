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

//! Machine-checked namespace prefix for results-cache keys.
//!
//! [`namespace_key_prefix`] is the byte stream
//! [`crate::key::CacheKey::as_raw_key_in_namespace`] hashes before the
//! payload. Two requests whose `(namespace_tag, namespace_id)` differ must
//! not produce the same stream after any payload is appended — otherwise a
//! principal can be served another principal's cached rows as a fresh hit.
//! The length prefix is what rules out the collision
//! `(tag=1, id="abc")` vs `(tag=1, id="a")` plus a payload starting `"bc"`.
//!
//! That bound is not obvious by inspection of the hasher writes, and a
//! test can only sample pairs. Here it is a postcondition
//! [Verus](https://github.com/verus-lang/verus) discharges for every
//! `(tag, id, payload)` triple.
//!
//! This is the executable encoding, not a model of it: the function below
//! is what the cache key path calls. Verus reads the `verus!` block; a
//! normal `cargo build` erases the specifications and compiles the body
//! as ordinary Rust. `cargo verus focus` re-checks the proof.

use vstd::prelude::*;

verus! {

/// Little-endian encoding of a `u64`, matching
/// [`u64::to_le_bytes`](u64::to_le_bytes).
pub open spec fn spec_u64_to_le_bytes(n: u64) -> Seq<u8> {
    seq![
        ((n >> 0u64) & 0xffu64) as u8,
        ((n >> 8u64) & 0xffu64) as u8,
        ((n >> 16u64) & 0xffu64) as u8,
        ((n >> 24u64) & 0xffu64) as u8,
        ((n >> 32u64) & 0xffu64) as u8,
        ((n >> 40u64) & 0xffu64) as u8,
        ((n >> 48u64) & 0xffu64) as u8,
        ((n >> 56u64) & 0xffu64) as u8,
    ]
}

/// `[tag][id.len() as u64 LE][id...]` — the stream hashed before the payload.
pub open spec fn spec_namespace_key_prefix(tag: u8, id: Seq<u8>) -> Seq<u8> {
    seq![tag] + spec_u64_to_le_bytes(id.len() as u64) + id
}

proof fn lemma_u64_to_le_bytes_len(n: u64)
    ensures
        spec_u64_to_le_bytes(n).len() == 8,
{
}

proof fn lemma_low_byte_in_range(n: u64, shift: u64)
    requires
        shift <= 56,
    ensures
        ((n >> shift) & 0xffu64) < 0x100u64,
{
    assert(((n >> shift) & 0xffu64) < 0x100u64) by (bit_vector)
        requires
            shift <= 56,
    ;
}

proof fn lemma_u8_cast_of_low_byte_is_injective(x: u64, y: u64)
    requires
        x < 0x100u64,
        y < 0x100u64,
        (x as u8) == (y as u8),
    ensures
        x == y,
{
    assert(x == y) by (bit_vector)
        requires
            x < 0x100u64,
            y < 0x100u64,
            (x as u8) == (y as u8),
    ;
}

proof fn lemma_masked_bytes_determine_u64(left: u64, right: u64)
    requires
        ((left >> 0u64) & 0xffu64) == ((right >> 0u64) & 0xffu64),
        ((left >> 8u64) & 0xffu64) == ((right >> 8u64) & 0xffu64),
        ((left >> 16u64) & 0xffu64) == ((right >> 16u64) & 0xffu64),
        ((left >> 24u64) & 0xffu64) == ((right >> 24u64) & 0xffu64),
        ((left >> 32u64) & 0xffu64) == ((right >> 32u64) & 0xffu64),
        ((left >> 40u64) & 0xffu64) == ((right >> 40u64) & 0xffu64),
        ((left >> 48u64) & 0xffu64) == ((right >> 48u64) & 0xffu64),
        ((left >> 56u64) & 0xffu64) == ((right >> 56u64) & 0xffu64),
    ensures
        left == right,
{
    assert(left == right) by (bit_vector)
        requires
            ((left >> 0u64) & 0xffu64) == ((right >> 0u64) & 0xffu64),
            ((left >> 8u64) & 0xffu64) == ((right >> 8u64) & 0xffu64),
            ((left >> 16u64) & 0xffu64) == ((right >> 16u64) & 0xffu64),
            ((left >> 24u64) & 0xffu64) == ((right >> 24u64) & 0xffu64),
            ((left >> 32u64) & 0xffu64) == ((right >> 32u64) & 0xffu64),
            ((left >> 40u64) & 0xffu64) == ((right >> 40u64) & 0xffu64),
            ((left >> 48u64) & 0xffu64) == ((right >> 48u64) & 0xffu64),
            ((left >> 56u64) & 0xffu64) == ((right >> 56u64) & 0xffu64),
    ;
}

proof fn lemma_u64_to_le_bytes_injective(left: u64, right: u64)
    ensures
        spec_u64_to_le_bytes(left) == spec_u64_to_le_bytes(right) ==> left == right,
{
    if spec_u64_to_le_bytes(left) == spec_u64_to_le_bytes(right) {
        lemma_low_byte_in_range(left, 0u64);
        lemma_low_byte_in_range(right, 0u64);
        lemma_u8_cast_of_low_byte_is_injective((left >> 0u64) & 0xffu64, (right >> 0u64) & 0xffu64);
        lemma_low_byte_in_range(left, 8u64);
        lemma_low_byte_in_range(right, 8u64);
        lemma_u8_cast_of_low_byte_is_injective((left >> 8u64) & 0xffu64, (right >> 8u64) & 0xffu64);
        lemma_low_byte_in_range(left, 16u64);
        lemma_low_byte_in_range(right, 16u64);
        lemma_u8_cast_of_low_byte_is_injective(
            (left >> 16u64) & 0xffu64,
            (right >> 16u64) & 0xffu64,
        );
        lemma_low_byte_in_range(left, 24u64);
        lemma_low_byte_in_range(right, 24u64);
        lemma_u8_cast_of_low_byte_is_injective(
            (left >> 24u64) & 0xffu64,
            (right >> 24u64) & 0xffu64,
        );
        lemma_low_byte_in_range(left, 32u64);
        lemma_low_byte_in_range(right, 32u64);
        lemma_u8_cast_of_low_byte_is_injective(
            (left >> 32u64) & 0xffu64,
            (right >> 32u64) & 0xffu64,
        );
        lemma_low_byte_in_range(left, 40u64);
        lemma_low_byte_in_range(right, 40u64);
        lemma_u8_cast_of_low_byte_is_injective(
            (left >> 40u64) & 0xffu64,
            (right >> 40u64) & 0xffu64,
        );
        lemma_low_byte_in_range(left, 48u64);
        lemma_low_byte_in_range(right, 48u64);
        lemma_u8_cast_of_low_byte_is_injective(
            (left >> 48u64) & 0xffu64,
            (right >> 48u64) & 0xffu64,
        );
        lemma_low_byte_in_range(left, 56u64);
        lemma_low_byte_in_range(right, 56u64);
        lemma_u8_cast_of_low_byte_is_injective(
            (left >> 56u64) & 0xffu64,
            (right >> 56u64) & 0xffu64,
        );
        lemma_masked_bytes_determine_u64(left, right);
    }
}

proof fn lemma_namespace_key_prefix_len(tag: u8, id: Seq<u8>)
    ensures
        spec_namespace_key_prefix(tag, id).len() == 9 + id.len(),
{
    lemma_u64_to_le_bytes_len(id.len() as u64);
}

proof fn lemma_concat_index_left<A>(a: Seq<A>, b: Seq<A>, i: int)
    requires
        0 <= i < a.len(),
    ensures
        (a + b)[i] == a[i],
{
}

proof fn lemma_concat_index_right<A>(a: Seq<A>, b: Seq<A>, i: int)
    requires
        0 <= i < b.len(),
    ensures
        (a + b)[a.len() + i] == b[i],
{
}

proof fn lemma_namespace_key_prefix_tag(tag: u8, id: Seq<u8>)
    ensures
        spec_namespace_key_prefix(tag, id)[0] == tag,
{
    lemma_namespace_key_prefix_len(tag, id);
}

proof fn lemma_namespace_key_prefix_len_byte(tag: u8, id: Seq<u8>, i: int)
    requires
        0 <= i < 8,
    ensures
        spec_namespace_key_prefix(tag, id)[i + 1] == spec_u64_to_le_bytes(id.len() as u64)[i],
{
    lemma_namespace_key_prefix_len(tag, id);
}

proof fn lemma_namespace_key_prefix_id_byte(tag: u8, id: Seq<u8>, i: int)
    requires
        0 <= i < id.len(),
    ensures
        spec_namespace_key_prefix(tag, id)[i + 9] == id[i],
{
    lemma_namespace_key_prefix_len(tag, id);
}

proof fn lemma_reveal_len_bytes(tag: u8, id: Seq<u8>)
    ensures
        spec_namespace_key_prefix(tag, id)[1] == spec_u64_to_le_bytes(id.len() as u64)[0],
        spec_namespace_key_prefix(tag, id)[2] == spec_u64_to_le_bytes(id.len() as u64)[1],
        spec_namespace_key_prefix(tag, id)[3] == spec_u64_to_le_bytes(id.len() as u64)[2],
        spec_namespace_key_prefix(tag, id)[4] == spec_u64_to_le_bytes(id.len() as u64)[3],
        spec_namespace_key_prefix(tag, id)[5] == spec_u64_to_le_bytes(id.len() as u64)[4],
        spec_namespace_key_prefix(tag, id)[6] == spec_u64_to_le_bytes(id.len() as u64)[5],
        spec_namespace_key_prefix(tag, id)[7] == spec_u64_to_le_bytes(id.len() as u64)[6],
        spec_namespace_key_prefix(tag, id)[8] == spec_u64_to_le_bytes(id.len() as u64)[7],
{
    lemma_namespace_key_prefix_len_byte(tag, id, 0);
    lemma_namespace_key_prefix_len_byte(tag, id, 1);
    lemma_namespace_key_prefix_len_byte(tag, id, 2);
    lemma_namespace_key_prefix_len_byte(tag, id, 3);
    lemma_namespace_key_prefix_len_byte(tag, id, 4);
    lemma_namespace_key_prefix_len_byte(tag, id, 5);
    lemma_namespace_key_prefix_len_byte(tag, id, 6);
    lemma_namespace_key_prefix_len_byte(tag, id, 7);
}

/// Distinct `(tag, id)` pairs cannot produce equal byte streams after any
/// payloads are appended. That is the collision class the length prefix
/// exists to prevent: without it, `(tag=1, id="abc")` and `(tag=1, id="a")`
/// plus a payload starting `"bc"` are the same stream.
pub proof fn lemma_namespace_key_prefix_unambiguous(
    tag1: u8,
    id1: Seq<u8>,
    payload1: Seq<u8>,
    tag2: u8,
    id2: Seq<u8>,
    payload2: Seq<u8>,
)
    requires
        id1.len() <= 0xffff_ffff_ffff_ffff,
        id2.len() <= 0xffff_ffff_ffff_ffff,
    ensures
        (spec_namespace_key_prefix(tag1, id1) + payload1 == spec_namespace_key_prefix(tag2, id2)
            + payload2) ==> tag1 == tag2 && id1 =~= id2 && payload1 =~= payload2,
        (tag1 != tag2 || !(id1 =~= id2)) ==> spec_namespace_key_prefix(tag1, id1) + payload1
            != spec_namespace_key_prefix(tag2, id2) + payload2,
{
    let e1 = spec_namespace_key_prefix(tag1, id1);
    let e2 = spec_namespace_key_prefix(tag2, id2);
    let s1 = e1 + payload1;
    let s2 = e2 + payload2;
    lemma_namespace_key_prefix_len(tag1, id1);
    lemma_namespace_key_prefix_len(tag2, id2);
    if s1 == s2 {
        lemma_namespace_key_prefix_tag(tag1, id1);
        lemma_namespace_key_prefix_tag(tag2, id2);
        lemma_concat_index_left(e1, payload1, 0);
        lemma_concat_index_left(e2, payload2, 0);
        assert(s1[0] == tag1);
        assert(s2[0] == tag2);
        assert(tag1 == tag2);

        lemma_reveal_len_bytes(tag1, id1);
        lemma_reveal_len_bytes(tag2, id2);
        lemma_concat_index_left(e1, payload1, 1);
        lemma_concat_index_left(e1, payload1, 2);
        lemma_concat_index_left(e1, payload1, 3);
        lemma_concat_index_left(e1, payload1, 4);
        lemma_concat_index_left(e1, payload1, 5);
        lemma_concat_index_left(e1, payload1, 6);
        lemma_concat_index_left(e1, payload1, 7);
        lemma_concat_index_left(e1, payload1, 8);
        lemma_concat_index_left(e2, payload2, 1);
        lemma_concat_index_left(e2, payload2, 2);
        lemma_concat_index_left(e2, payload2, 3);
        lemma_concat_index_left(e2, payload2, 4);
        lemma_concat_index_left(e2, payload2, 5);
        lemma_concat_index_left(e2, payload2, 6);
        lemma_concat_index_left(e2, payload2, 7);
        lemma_concat_index_left(e2, payload2, 8);
        assert(spec_u64_to_le_bytes(id1.len() as u64) =~= spec_u64_to_le_bytes(id2.len() as u64));
        lemma_u64_to_le_bytes_injective(id1.len() as u64, id2.len() as u64);
        assert(id1.len() == id2.len());

        assert forall|i: int|
            #![trigger id1[i]]
            0 <= i < id1.len() ==> s1[i + 9] == id1[i] && s2[i + 9] == id2[i] by {
            if 0 <= i && i < id1.len() {
                lemma_namespace_key_prefix_id_byte(tag1, id1, i);
                lemma_namespace_key_prefix_id_byte(tag2, id2, i);
                lemma_concat_index_left(e1, payload1, i + 9);
                lemma_concat_index_left(e2, payload2, i + 9);
            }
        }
        assert(id1 =~= id2);
        assert(e1.len() == e2.len());
        assert(s1.len() == e1.len() + payload1.len());
        assert(s2.len() == e2.len() + payload2.len());
        assert(payload1.len() == payload2.len());
        assert forall|i: int|
            #![trigger payload1[i]]
            0 <= i < payload1.len() ==> payload1[i] == payload2[i] by {
            if 0 <= i && i < payload1.len() {
                lemma_concat_index_right(e1, payload1, i);
                lemma_concat_index_right(e2, payload2, i);
            }
        }
        assert(payload1 =~= payload2);
    }
}

/// Low byte of `n` after shifting `shift` bits. Isolated so the
/// `as u8` truncation is next to the mask that makes it lossless.
fn le_byte(n: u64, shift: u64) -> (b: u8)
    requires
        shift <= 56,
    ensures
        b == ((n >> shift) & 0xffu64) as u8,
{
    ((n >> shift) & 0xffu64) as u8
}

/// Encodes `(namespace_tag, namespace_id)` as
/// `[tag][id.len() as u64 LE][id...]`.
///
/// This is the byte stream [`crate::key::CacheKey::as_raw_key_in_namespace`]
/// writes before hashing the payload. The `ensures` clause pins the body
/// to [`spec_namespace_key_prefix`]; [`lemma_namespace_key_prefix_unambiguous`]
/// is the anti-collision property that lemma discharges for every payload.
#[must_use]
pub fn namespace_key_prefix(tag: u8, id: &[u8]) -> (prefix: Vec<u8>)
    ensures
        prefix@ == spec_namespace_key_prefix(tag, id@),
{
    let mut prefix: Vec<u8> = Vec::new();
    let len: u64 = id.len() as u64;

    proof {
        assert(id.len() as int == id@.len());
        assert(len == id@.len() as u64);
        assert(id@.len() <= 0xffff_ffff_ffff_ffff);
    }

    prefix.push(tag);
    prefix.push(le_byte(len, 0u64));
    prefix.push(le_byte(len, 8u64));
    prefix.push(le_byte(len, 16u64));
    prefix.push(le_byte(len, 24u64));
    prefix.push(le_byte(len, 32u64));
    prefix.push(le_byte(len, 40u64));
    prefix.push(le_byte(len, 48u64));
    prefix.push(le_byte(len, 56u64));

    proof {
        lemma_u64_to_le_bytes_len(len);
        assert(prefix@ == seq![tag] + spec_u64_to_le_bytes(len));
    }

    let mut i: usize = 0;
    while i < id.len()
        invariant
            i <= id.len(),
            len == id@.len() as u64,
            prefix@ == seq![tag] + spec_u64_to_le_bytes(len) + id@.subrange(0, i as int),
        decreases id.len() - i,
    {
        prefix.push(id[i]);
        proof {
            assert(id@.subrange(0, i as int + 1) =~= id@.subrange(0, i as int) + seq![id@[i
                as int]]);
        }
        i += 1;
    }

    proof {
        assert(id@.subrange(0, id@.len() as int) =~= id@);
    }

    prefix
}

} // verus!

#[cfg(test)]
mod tests {
    use super::namespace_key_prefix;

    #[test]
    fn prefix_is_tag_then_le_length_then_id() {
        let prefix = namespace_key_prefix(1, b"abc");
        let mut expected = vec![1_u8];
        expected.extend_from_slice(&3_u64.to_le_bytes());
        expected.extend_from_slice(b"abc");
        assert_eq!(prefix, expected);
    }

    #[test]
    fn empty_id_still_carries_a_zero_length() {
        let prefix = namespace_key_prefix(0, b"");
        let mut expected = vec![0_u8];
        expected.extend_from_slice(&0_u64.to_le_bytes());
        assert_eq!(prefix, expected);
        // Public (tag 0) and system (tag 2) share an empty id; the tag is
        // what keeps their streams distinct.
        assert_ne!(namespace_key_prefix(0, b""), namespace_key_prefix(2, b""));
    }

    #[test]
    fn length_prefix_blocks_the_documented_payload_collision() {
        // Without a length, these two streams are equal — the collision
        // class the comment on `as_raw_key_in_namespace` names.
        let mut naive_left = vec![1_u8];
        naive_left.extend_from_slice(b"abc");
        let mut naive_right = vec![1_u8];
        naive_right.extend_from_slice(b"a");
        naive_right.extend_from_slice(b"bc");
        assert_eq!(
            naive_left, naive_right,
            "the naive concatenation is the collision the length prefix exists to prevent"
        );

        let left = namespace_key_prefix(1, b"abc");
        let mut right = namespace_key_prefix(1, b"a");
        right.extend_from_slice(b"bc");
        assert_ne!(left, right);
    }
}
