/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016-2025 The chrono-tz contributors

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

use core::cmp::Ordering;

/// An implementation of binary search on indices only
/// that does not require slices to be constructed. Mirrors
/// the semantics of binary_search_by in the standard library.
pub fn binary_search<F>(mut start: usize, mut end: usize, mut f: F) -> Result<usize, usize>
where
    F: FnMut(usize) -> Ordering,
{
    loop {
        let mid = start + (end - start) / 2;
        if mid == end {
            return Err(start);
        }
        match f(mid) {
            Ordering::Less => start = mid + 1,
            Ordering::Greater => end = mid,
            Ordering::Equal => return Ok(mid),
        }
    }
}

#[test]
fn test_binary_search() {
    assert_eq!(binary_search(0, 5000, |x| x.cmp(&1337)), Ok(1337));
    assert_eq!(binary_search(0, 5000, |x| x.cmp(&9000)), Err(5000));
    assert_eq!(binary_search(30, 50, |x| x.cmp(&42)), Ok(42));
    assert_eq!(binary_search(300, 500, |x| x.cmp(&42)), Err(300));
    assert_eq!(
        binary_search(0, 500, |x| if x < 42 {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        Err(42)
    );
}
