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

//! Reading the `from` of a dataset or catalog: the connector it names and the
//! path or URL after it.

/// Helper function that finds the position and length of the first delimiter ('://', ':', or '/')
#[must_use]
pub fn find_first_delimiter(from: &str) -> Option<(usize, usize)> {
    // Find the earliest occurrence of each delimiter
    let colon_slash_slash = from.find("://");
    let colon = from.find(':');
    let slash = from.find('/');

    // Get the position and length of the first delimiter
    match (colon_slash_slash, colon, slash) {
        (Some(css), Some(c), Some(s)) => {
            let min_pos = css.min(c).min(s);
            Some(if min_pos == css {
                (css, 3)
            } else {
                (min_pos, 1)
            })
        }
        (Some(css), Some(c), None) => Some(if css < c { (css, 3) } else { (c, 1) }),
        (Some(css), None, Some(s)) => Some(if css < s { (css, 3) } else { (s, 1) }),
        (None, Some(c), Some(s)) => Some(if c < s { (c, 1) } else { (s, 1) }),
        (Some(css), None, None) => Some((css, 3)),
        (None, Some(c), None) => Some((c, 1)),
        (None, None, Some(s)) => Some((s, 1)),
        (None, None, None) => None,
    }
}
