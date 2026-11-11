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

//! Machine-checked prefix-freedom of the compound key encoding.
//!
//! Two key tuples whose columns encode differently must never encode to the
//! same bytes. The encoding frames each column in three steps: a
//! variable-length value is escaped (`00` → `01 01`, `01` → `01 02`) and
//! terminated by `00`; a nullable column is `00` for NULL or `01` followed by
//! the value; and a key is its columns' encodings concatenated. The lemmas
//! below prove, for every input, that each step keeps the set of encodings
//! prefix-free, so the concatenation adds no collisions: it keeps every
//! distinction the columns' encodings make. Which values one column's encoding
//! tells apart is the encoder's choice; floats give `-0.0` and `0.0`, and
//! every NaN, one encoding each.
//!
//! That is a property of the encoding, not of a lookup: an index stores a key
//! that does not fit 8 bytes as a 64-bit hash of these bytes (see
//! `word_proof`), which two keys can share, and the query's own filter drops
//! the other key's rows. The framing makes that hash the only place two keys
//! whose columns encode differently can collide.
//!
//! [`escape_value_into`] is the executable escape [`crate::encode`] writes
//! every string and binary value with, and its postcondition is the
//! specification the lemmas are about. A value with no byte to escape (the
//! common case) is copied whole and terminated, which [`lemma_escape_plain`]
//! proves is its escape; any other value goes through [`escape_into`] byte by
//! byte.
//!
//! Verus reads the `verus!` block; a normal `cargo build` erases the
//! specifications and compiles the body as ordinary Rust. `cargo verus focus`
//! re-checks the proofs.

use vstd::prelude::*;

verus! {

/// Whether `x` is a prefix of `y` (possibly equal).
pub open spec fn is_prefix(x: Seq<u8>, y: Seq<u8>) -> bool {
    x.len() <= y.len() && y.subrange(0, x.len() as int) == x
}

/// The escape of one byte.
pub open spec fn escape_byte(b: u8) -> Seq<u8> {
    if b == 0 {
        seq![1u8, 1u8]
    } else if b == 1 {
        seq![1u8, 2u8]
    } else {
        seq![b]
    }
}

/// A variable-length value: every byte escaped, then the `00` terminator.
pub open spec fn escape(s: Seq<u8>) -> Seq<u8>
    decreases s.len(),
{
    if s.len() == 0 {
        seq![0u8]
    } else {
        escape_byte(s[0]) + escape(s.subrange(1, s.len() as int))
    }
}

/// A nullable column: `00` for NULL, or `01` then the value's encoding.
pub open spec fn nullable(value: Option<Seq<u8>>) -> Seq<u8> {
    match value {
        None => seq![0u8],
        Some(v) => seq![1u8] + v,
    }
}

proof fn lemma_escape_nonempty(s: Seq<u8>)
    ensures
        escape(s).len() >= 1,
        s.len() == 0 ==> escape(s) == seq![0u8],
        s.len() > 0 ==> escape(s)[0] != 0u8,
        s.len() > 0 ==> escape(s).len() >= 2,
    decreases s.len(),
{
    if s.len() > 0 {
        lemma_escape_nonempty(s.subrange(1, s.len() as int));
        assert(escape(s) == escape_byte(s[0]) + escape(s.subrange(1, s.len() as int)));
    }
}

/// The escape is prefix-free: an escaped value that prefixes another is it.
pub proof fn lemma_escape_prefix_free(a: Seq<u8>, b: Seq<u8>)
    requires
        is_prefix(escape(a), escape(b)),
    ensures
        a == b,
    decreases a.len(),
{
    lemma_escape_nonempty(a);
    lemma_escape_nonempty(b);
    if a.len() == 0 {
        // escape(a) is [00]; escape(b) starts with 00 only when b is empty.
        assert(escape(b)[0] == escape(a)[0]);
        if b.len() > 0 {
            assert(false);
        }
        assert(a =~= b);
    } else if b.len() == 0 {
        // escape(a) has at least two bytes; escape(b) has one.
        assert(false);
    } else {
        let (ra, rb) = (a.subrange(1, a.len() as int), b.subrange(1, b.len() as int));
        let (ea, eb) = (escape_byte(a[0]), escape_byte(b[0]));
        assert(escape(a) == ea + escape(ra));
        assert(escape(b) == eb + escape(rb));
        lemma_escape_nonempty(ra);
        lemma_escape_nonempty(rb);
        // The first escaped bytes agree, which fixes the first input byte.
        assert(escape(b).subrange(0, escape(a).len() as int) == escape(a));
        assert(escape(a)[0] == ea[0]);
        assert(escape(b)[0] == eb[0]);
        assert(ea[0] == eb[0]) by {
            assert(escape(b).subrange(0, escape(a).len() as int)[0] == escape(b)[0]);
        }
        if ea.len() == 2 {
            assert(escape(a)[1] == ea[1]);
            assert(escape(b).subrange(0, escape(a).len() as int)[1] == escape(b)[1]);
            assert(eb.len() == 2);
            assert(escape(b)[1] == eb[1]);
        }
        assert(a[0] == b[0]);
        assert(ea == eb);
        // Strip the shared escaped byte; the rest is again a prefix.
        let k = ea.len() as int;
        assert(escape(a).subrange(k, escape(a).len() as int) == escape(ra));
        assert(escape(b).subrange(k, escape(b).len() as int) == escape(rb));
        assert(is_prefix(escape(ra), escape(rb))) by {
            assert(escape(rb).subrange(0, escape(ra).len() as int)
                == escape(b).subrange(k, k + escape(ra).len()));
            assert(escape(b).subrange(k, k + escape(ra).len())
                == escape(b).subrange(0, escape(a).len() as int).subrange(k, escape(a).len() as int));
        }
        lemma_escape_prefix_free(ra, rb);
        assert(a =~= seq![a[0]] + ra);
        assert(b =~= seq![b[0]] + rb);
    }
}

/// Fixed-width values of one width are prefix-free: same length, so a
/// prefix is the whole value.
pub proof fn lemma_fixed_width_prefix_free(a: Seq<u8>, b: Seq<u8>)
    requires
        a.len() == b.len(),
        is_prefix(a, b),
    ensures
        a == b,
{
    assert(b.subrange(0, a.len() as int) =~= b);
}

/// The nullable wrapper keeps a prefix-free code prefix-free.
pub proof fn lemma_nullable_prefix_free(
    code: spec_fn(Seq<u8>) -> bool,
    a: Option<Seq<u8>>,
    b: Option<Seq<u8>>,
)
    requires
        forall|x: Seq<u8>, y: Seq<u8>| code(x) && code(y) && is_prefix(x, y) ==> x == y,
        a is Some ==> code(a->0),
        b is Some ==> code(b->0),
        is_prefix(nullable(a), nullable(b)),
    ensures
        a == b,
{
    let (na, nb) = (nullable(a), nullable(b));
    assert(nb.subrange(0, na.len() as int)[0] == nb[0]);
    match (a, b) {
        (None, None) => {},
        (None, Some(_)) => { assert(na[0] == 0u8 && nb[0] == 1u8); },
        (Some(_), None) => { assert(na.len() >= 1 && nb.len() == 1); assert(na[0] == 1u8 && nb[0] == 0u8); },
        (Some(x), Some(y)) => {
            assert(na.subrange(1, na.len() as int) =~= x);
            assert(nb.subrange(1, nb.len() as int) =~= y);
            assert(is_prefix(x, y)) by {
                assert(y.subrange(0, x.len() as int)
                    =~= nb.subrange(0, na.len() as int).subrange(1, na.len() as int));
            }
        },
    }
}

/// Concatenating two prefix-free codes gives a prefix-free code: column by
/// column, a compound key that prefixes another is it.
pub proof fn lemma_concat_prefix_free(
    first: spec_fn(Seq<u8>) -> bool,
    rest: spec_fn(Seq<u8>) -> bool,
    x1: Seq<u8>,
    x2: Seq<u8>,
    y1: Seq<u8>,
    y2: Seq<u8>,
)
    requires
        forall|u: Seq<u8>, v: Seq<u8>| first(u) && first(v) && is_prefix(u, v) ==> u == v,
        forall|u: Seq<u8>, v: Seq<u8>| rest(u) && rest(v) && is_prefix(u, v) ==> u == v,
        first(x1),
        first(y1),
        rest(x2),
        rest(y2),
        is_prefix(x1 + x2, y1 + y2),
    ensures
        x1 == y1,
        x2 == y2,
{
    let (x, y) = (x1 + x2, y1 + y2);
    // The shorter first column prefixes the longer: both are prefixes of y.
    if x1.len() <= y1.len() {
        assert(is_prefix(x1, y1)) by {
            assert(y1.subrange(0, x1.len() as int) =~= y.subrange(0, x1.len() as int));
            assert(x1 =~= x.subrange(0, x1.len() as int));
            assert(y.subrange(0, x1.len() as int) =~= y.subrange(0, x.len() as int).subrange(0, x1.len() as int));
        }
    } else {
        assert(is_prefix(y1, x1)) by {
            assert(x1.subrange(0, y1.len() as int) =~= x.subrange(0, y1.len() as int));
            assert(y1 =~= y.subrange(0, y1.len() as int));
            assert(y.subrange(0, y1.len() as int) =~= y.subrange(0, x.len() as int).subrange(0, y1.len() as int));
        }
    }
    assert(x1 == y1);
    assert(is_prefix(x2, y2)) by {
        let k = x1.len() as int;
        assert(x2 =~= x.subrange(k, x.len() as int));
        assert(y2 =~= y.subrange(k, y.len() as int));
        assert(y2.subrange(0, x2.len() as int) =~= y.subrange(0, x.len() as int).subrange(k, x.len() as int));
    }
}

/// Whether no byte of `s` needs escaping: every byte is above `01`.
pub open spec fn plain(s: Seq<u8>) -> bool {
    forall|i: int| 0 <= i < s.len() ==> s[i] >= 2u8
}

/// A value with no byte to escape escapes to itself and the terminator.
pub proof fn lemma_escape_plain(s: Seq<u8>)
    requires
        plain(s),
    ensures
        escape(s) == s + seq![0u8],
    decreases s.len(),
{
    if s.len() == 0 {
        assert(escape(s) =~= s + seq![0u8]);
    } else {
        let rest = s.subrange(1, s.len() as int);
        assert(plain(rest));
        lemma_escape_plain(rest);
        assert(escape(s) == escape_byte(s[0]) + escape(rest));
        assert(escape_byte(s[0]) == seq![s[0]]);
        assert(escape(s) =~= s + seq![0u8]);
    }
}

/// Whether no byte of `value` needs escaping.
fn is_plain(value: &[u8]) -> (result: bool)
    ensures
        result == plain(value@),
{
    let mut i: usize = 0;
    while i < value.len()
        invariant
            i <= value.len(),
            forall|j: int| 0 <= j < i ==> value@[j] >= 2u8,
        decreases value.len() - i,
    {
        if value[i] < 2 {
            return false;
        }
        i += 1;
    }
    true
}

/// Append the escape of `value` to `out`, verified against [`escape`]: the
/// value copied whole and terminated when no byte needs escaping, and
/// otherwise escaped byte by byte.
#[inline]
pub fn escape_value_into(value: &[u8], out: &mut Vec<u8>)
    ensures
        final(out)@ == old(out)@ + escape(value@),
{
    if is_plain(value) {
        proof {
            lemma_escape_plain(value@);
        }
        out.extend_from_slice(value);
        out.push(0u8);
        assert(out@ =~= old(out)@ + (value@ + seq![0u8]));
    } else {
        escape_into(value, out);
    }
}

/// Append the escape of `value` to `out` one byte at a time, verified against
/// [`escape`].
pub fn escape_into(value: &[u8], out: &mut Vec<u8>)
    ensures
        final(out)@ == old(out)@ + escape(value@),
{
    let mut i: usize = 0;
    assert(value@.subrange(0, value@.len() as int) =~= value@);
    while i < value.len()
        invariant
            i <= value.len(),
            out@ + escape(value@.subrange(i as int, value@.len() as int))
                == old(out)@ + escape(value@),
        decreases value.len() - i,
    {
        let b = value[i];
        let ghost before = out@;
        let ghost tail = value@.subrange(i as int, value@.len() as int);
        assert(tail.len() > 0);
        assert(tail[0] == b);
        assert(tail.subrange(1, tail.len() as int) =~= value@.subrange(i + 1, value@.len() as int));
        assert(escape(tail) == escape_byte(b) + escape(tail.subrange(1, tail.len() as int)));
        if b == 0 {
            out.push(1u8);
            out.push(1u8);
        } else if b == 1 {
            out.push(1u8);
            out.push(2u8);
        } else {
            out.push(b);
        }
        assert(out@ =~= before + escape_byte(b));
        i += 1;
    }
    assert(value@.subrange(i as int, value@.len() as int) =~= Seq::<u8>::empty());
    out.push(0u8);
}

} // verus!
