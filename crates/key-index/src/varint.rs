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
    fn posting_runs_round_trip() {
        let postings = [0, 1, 2, 300, 70_000, 1 << 40, (1 << 62) + 5];
        let mut run = Vec::new();
        put_postings(&mut run, &postings);
        let mut at = 0;
        let mut posting = 0;
        let got: Vec<u64> = std::iter::from_fn(|| {
            posting += get(&run, &mut at)?;
            Some(posting)
        })
        .collect();
        assert_eq!(got, postings);
    }
}
