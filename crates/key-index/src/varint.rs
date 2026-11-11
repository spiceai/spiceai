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

//! LEB128 varints and delta-encoded posting lists.

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

/// Append a posting list: the number of `postings`, then each of them,
/// ascending and distinct, as its gap from the one before (the first as its
/// gap from zero), all varints.
pub fn put_postings(out: &mut Vec<u8>, postings: &[u64]) {
    debug_assert!(postings.windows(2).all(|w| w[0] < w[1]));
    put(out, postings.len() as u64);
    let mut previous = 0;
    for &posting in postings {
        put(out, posting - previous);
        previous = posting;
    }
}

/// A gap that does not decode, or postings that overflow a `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Malformed;

/// Reads a posting list [`put_postings`] wrote, posting by posting.
#[derive(Debug, Clone)]
pub struct PostingList<'a> {
    bytes: &'a [u8],
    /// Where the next gap starts.
    at: usize,
    /// Postings the list's count gives it.
    count: u64,
    /// Postings not yet read.
    left: u64,
    /// The posting read last, or zero.
    last: u64,
}

impl<'a> PostingList<'a> {
    /// The list that starts at `at` in `bytes`, or `None` when its count does
    /// not decode.
    #[inline]
    pub fn at(bytes: &'a [u8], mut at: usize) -> Option<Self> {
        let count = get(bytes, &mut at)?;
        Some(Self {
            bytes,
            at,
            count,
            left: count,
            last: 0,
        })
    }

    /// The number of postings the list's count gives it, read or not.
    #[inline]
    pub fn postings(&self) -> u64 {
        self.count
    }

    /// Where the bytes read so far end: the end of the list once every
    /// posting has been read.
    #[inline]
    pub fn end(&self) -> usize {
        self.at
    }
}

impl Iterator for PostingList<'_> {
    /// The next posting, or [`Malformed`], after which the list ends.
    type Item = Result<u64, Malformed>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let Some(posting) =
            get(self.bytes, &mut self.at).and_then(|gap| self.last.checked_add(gap))
        else {
            self.left = 0;
            return Some(Err(Malformed));
        };
        self.last = posting;
        Some(Ok(posting))
    }
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
    fn posting_lists_round_trip() {
        let postings = [0, 1, 2, 300, 70_000, 1 << 40, (1 << 62) + 5];
        let mut bytes = vec![0xff];
        put_postings(&mut bytes, &postings);
        let mut list = PostingList::at(&bytes, 1).expect("count");
        assert_eq!(list.postings(), postings.len() as u64);
        let got: Vec<u64> = list.by_ref().map(|p| p.expect("posting")).collect();
        assert_eq!(got, postings);
        assert_eq!(list.end(), bytes.len());
        // A list that ends early, or overflows, is reported, then ends.
        let truncated = PostingList::at(&bytes[..bytes.len() - 1], 1).expect("count");
        assert_eq!(truncated.last(), Some(Err(Malformed)));
        let overflowing = [
            2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 1,
        ];
        let got: Vec<_> = PostingList::at(&overflowing, 0).expect("count").collect();
        assert_eq!(got, [Ok(u64::MAX), Err(Malformed)]);
        assert!(
            PostingList::at(&[0x80], 0).is_none(),
            "a count that does not decode"
        );
    }
}
