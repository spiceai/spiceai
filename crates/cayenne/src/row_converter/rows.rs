/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Owned and borrowed views over encoded row bytes.
//!
//! Equality, ordering, and hashing are defined purely over the raw encoded bytes — the row
//! format is normalized so that byte comparison equals logical comparison of the values that
//! produced the rows (when both come from the same converter).

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A collection of encoded rows, stored as one contiguous buffer with per-row offsets.
///
/// Row `i` occupies `buffer[offsets[i]..offsets[i + 1]]`.
#[derive(Debug)]
pub struct Rows {
    pub(crate) buffer: Vec<u8>,
    pub(crate) offsets: Vec<usize>,
}

impl Rows {
    /// Borrow row `i`.
    ///
    /// # Panics
    /// Panics if `row` is out of bounds.
    #[must_use]
    pub fn row(&self, row: usize) -> Row<'_> {
        assert!(row + 1 < self.offsets.len(), "row index out of bounds");
        let start = self.offsets[row];
        let end = self.offsets[row + 1];
        Row {
            data: &self.buffer[start..end],
        }
    }

    /// The number of rows.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Iterate over the rows in order.
    #[must_use]
    pub fn iter(&self) -> RowsIter<'_> {
        RowsIter {
            rows: self,
            start: 0,
            end: self.num_rows(),
        }
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = Row<'a>;
    type IntoIter = RowsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A double-ended iterator over the rows of a [`Rows`].
#[derive(Debug)]
pub struct RowsIter<'a> {
    rows: &'a Rows,
    start: usize,
    end: usize,
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }
        let row = self.rows.row(self.start);
        self.start += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.end - self.start;
        (len, Some(len))
    }
}

impl ExactSizeIterator for RowsIter<'_> {}

impl DoubleEndedIterator for RowsIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }
        self.end -= 1;
        Some(self.rows.row(self.end))
    }
}

/// A borrowed, encoded row.
///
/// Compared, ordered, and hashed by its raw bytes.
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    data: &'a [u8],
}

impl<'a> Row<'a> {
    /// The row's raw encoded bytes.
    #[must_use]
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Detach an owned copy of this row.
    #[must_use]
    pub fn owned(&self) -> OwnedRow {
        OwnedRow {
            data: self.data.into(),
        }
    }
}

impl PartialEq for Row<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for Row<'_> {}

impl PartialOrd for Row<'_> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Row<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(other.data)
    }
}

impl Hash for Row<'_> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl AsRef<[u8]> for Row<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

/// An owned, encoded row that can be moved and stored freely (e.g. as a hash-set key).
///
/// Holds only the bytes of a single row. Compared, ordered, and hashed by those bytes.
#[derive(Debug, Clone)]
pub struct OwnedRow {
    data: Box<[u8]>,
}

impl OwnedRow {
    /// Borrow this owned row as a [`Row`].
    #[must_use]
    pub fn row(&self) -> Row<'_> {
        Row { data: &self.data }
    }
}

impl PartialEq for OwnedRow {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for OwnedRow {}

impl PartialOrd for OwnedRow {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OwnedRow {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }
}

impl Hash for OwnedRow {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl AsRef<[u8]> for OwnedRow {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
