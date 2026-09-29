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

//! LEB128 varints and delta-encoded posting runs.

/// Append `value` as an LEB128 varint.
#[inline]
pub fn put(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Read a varint at `*at`, advancing past it. `None` when the bytes end
/// inside it or it overflows a `u64`.
#[inline]
pub fn get(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *bytes.get(*at)?;
        *at += 1;
        let low = u64::from(byte & 0x7f);
        if shift >= u64::BITS || (shift > 0 && low >> (u64::BITS - shift) != 0) {
            return None;
        }
        value |= low << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
}

/// Append ascending, distinct `postings` as varints of their gaps (the first
/// as its gap from zero).
pub fn put_postings(out: &mut Vec<u8>, postings: &[u64]) {
    debug_assert!(postings.windows(2).all(|w| w[0] < w[1]));
    let mut previous = 0;
    for &posting in postings {
        put(out, posting - previous);
        previous = posting;
    }
}

/// Call `f` with each posting of a run written by [`put_postings`].
pub fn for_each_posting(run: &[u8], mut f: impl FnMut(u64)) {
    let mut posting = 0_u64;
    let mut gap = 0_u64;
    let mut shift = 0;
    for &byte in run {
        gap |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            posting += gap;
            f(posting);
            gap = 0;
            shift = 0;
        } else {
            shift += 7;
        }
    }
}

/// Append `postings` like [`put_postings`], but with the first posting stored
/// as its zigzag-encoded difference from `base` (which may be larger), so a
/// run whose first posting is near its predecessor's takes one or two bytes.
/// Postings must be at most `u64::MAX >> 1`.
pub fn put_postings_from(out: &mut Vec<u8>, base: u64, postings: &[u64]) {
    debug_assert!(postings.windows(2).all(|w| w[0] < w[1]));
    let Some((&first, rest)) = postings.split_first() else {
        return;
    };
    put(
        out,
        zigzag(first.cast_signed().wrapping_sub(base.cast_signed())),
    );
    let mut previous = first;
    for &posting in rest {
        put(out, posting - previous);
        previous = posting;
    }
}

/// The first posting of a run written by [`put_postings_from`] with `base`.
#[must_use]
pub fn first_posting_from(run: &[u8], base: u64) -> Option<u64> {
    let mut at = 0;
    let delta = unzigzag(get(run, &mut at)?);
    Some(base.cast_signed().wrapping_add(delta).cast_unsigned())
}

/// Call `f` with each posting of a run written by [`put_postings_from`].
pub fn for_each_posting_from(run: &[u8], base: u64, mut f: impl FnMut(u64)) {
    let mut at = 0;
    let Some(delta) = get(run, &mut at).map(unzigzag) else {
        return;
    };
    let first = base.cast_signed().wrapping_add(delta).cast_unsigned();
    f(first);
    // The rest are gaps; `for_each_posting` yields their running sum.
    for_each_posting(&run[at..], |offset| f(first + offset));
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)).cast_unsigned()
}

fn unzigzag(value: u64) -> i64 {
    (value >> 1).cast_signed() ^ -((value & 1).cast_signed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip_at_every_width() {
        let values = [
            0,
            1,
            127,
            128,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX >> 1,
            u64::MAX,
        ];
        let mut out = Vec::new();
        for &v in &values {
            put(&mut out, v);
        }
        let mut at = 0;
        for &v in &values {
            assert_eq!(get(&out, &mut at), Some(v));
        }
        assert_eq!(at, out.len());
        assert_eq!(get(&out, &mut at), None, "reading past the end");
        // Truncated and overflowing encodings are rejected, not misread.
        assert_eq!(get(&[0x80], &mut 0), None);
        assert_eq!(get(&[0xff; 11], &mut 0), None);
    }

    #[test]
    fn based_runs_round_trip_above_and_below_the_base() {
        for base in [0, 5, 1 << 40, u64::MAX >> 1] {
            for postings in [
                vec![0_u64, 3, 9],
                vec![4],
                vec![(1 << 40) + 1, (1 << 41)],
                vec![u64::MAX >> 1],
            ] {
                let mut run = Vec::new();
                put_postings_from(&mut run, base, &postings);
                assert_eq!(first_posting_from(&run, base), postings.first().copied());
                let mut got = Vec::new();
                for_each_posting_from(&run, base, |p| got.push(p));
                assert_eq!(got, postings, "base {base}");
            }
        }
    }

    #[test]
    fn posting_runs_round_trip() {
        let postings = [0, 1, 2, 300, 70_000, 1 << 40, (1 << 62) + 5];
        let mut run = Vec::new();
        put_postings(&mut run, &postings);
        let mut got = Vec::new();
        for_each_posting(&run, |p| got.push(p));
        assert_eq!(got, postings);
    }
}
