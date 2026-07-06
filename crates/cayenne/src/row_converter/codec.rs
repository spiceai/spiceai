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

//! Version-agnostic framing engine and the per-column codec extension point.
//!
//! The framing here — how a row's column encodings are laid out, how offsets are tracked, and
//! the null sentinel convention — is shared by every format version. Per-column encoding and
//! decoding is delegated to a [`ColumnCodec`], so a new version only supplies a new set of
//! column codecs (usually reusing most of the previous version's).
//!
//! Portions are derived from Apache Arrow's `arrow-row` crate (Apache-2.0), whose byte layout
//! this reproduces.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow_schema::{ArrowError, SortOptions};

use super::SortField;
use super::rows::{Row, Rows};

/// Encodes and decodes a single column to and from the row format.
///
/// Each codec instance is built for one [`SortField`] and captures that field's
/// [`SortOptions`] and data type. This is the extension point for the format: optimizing a
/// data type means writing a new codec and routing that type to it, leaving every other
/// column codec untouched.
pub(crate) trait ColumnCodec: Debug + Send + Sync {
    /// Contribute the per-row encoded length of `array` to `tracker`.
    fn append_lengths(&self, array: &dyn Array, tracker: &mut LengthTracker);

    /// Encode `array` into `data`, advancing each entry of `offsets[1..]` past the bytes
    /// written for that row.
    fn encode(&self, data: &mut [u8], offsets: &mut [usize], array: &dyn Array);

    /// Decode this column from `rows`, advancing each slice past the bytes consumed.
    fn decode(&self, rows: &mut [&[u8]], validate_utf8: bool) -> Result<ArrayRef, ArrowError>;
}

/// The shared row-encoding engine: a schema plus one [`ColumnCodec`] per column.
///
/// Wrapped by [`super::RowConverter`]; the enum variant records the format version, while this
/// struct holds the built codecs and performs the version-agnostic framing.
#[derive(Debug)]
pub struct RowCodec {
    fields: Arc<[SortField]>,
    codecs: Vec<Box<dyn ColumnCodec>>,
}

impl RowCodec {
    /// Build the per-column codecs for `fields` using a version-specific `build` function.
    pub(crate) fn new(
        fields: Vec<SortField>,
        build: impl Fn(&SortField) -> Result<Box<dyn ColumnCodec>, ArrowError>,
    ) -> Result<Self, ArrowError> {
        let codecs = fields.iter().map(&build).collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            fields: fields.into(),
            codecs,
        })
    }

    pub(crate) fn convert_columns(&self, columns: &[ArrayRef]) -> Result<Rows, ArrowError> {
        if columns.len() != self.fields.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Incorrect number of arrays provided to RowConverter, expected {} got {}",
                self.fields.len(),
                columns.len()
            )));
        }
        for column in columns.iter().skip(1) {
            if column.len() != columns[0].len() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "RowConverter columns must all have the same length, expected {} got {}",
                    columns[0].len(),
                    column.len()
                )));
            }
        }
        for (column, field) in columns.iter().zip(self.fields.iter()) {
            if !column.data_type().equals_datatype(&field.data_type) {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "RowConverter column schema mismatch, expected {} got {}",
                    field.data_type,
                    column.data_type()
                )));
            }
        }

        let num_rows = match columns.first() {
            Some(c) => c.len(),
            None => 0,
        };
        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.push(0usize);

        let mut tracker = LengthTracker::new(num_rows);
        for (column, codec) in columns.iter().zip(&self.codecs) {
            codec.append_lengths(column.as_ref(), &mut tracker);
        }
        let total = tracker.extend_offsets(0, &mut offsets);
        let mut buffer = vec![0u8; total];

        for (column, codec) in columns.iter().zip(&self.codecs) {
            codec.encode(
                buffer.as_mut_slice(),
                offsets.as_mut_slice(),
                column.as_ref(),
            );
        }

        debug_assert_eq!(offsets.last().copied(), Some(buffer.len()));
        debug_assert!(
            offsets.windows(2).all(|w| w[0] <= w[1]),
            "offsets must be monotonic"
        );

        Ok(Rows { buffer, offsets })
    }

    pub(crate) fn convert_rows<'a>(
        &self,
        rows: impl IntoIterator<Item = Row<'a>>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        let mut slices: Vec<&[u8]> = rows.into_iter().map(|row| row.data()).collect();
        let mut columns = Vec::with_capacity(self.codecs.len());
        for codec in &self.codecs {
            columns.push(codec.decode(&mut slices, true)?);
        }
        Ok(columns)
    }
}

/// The null sentinel byte: `0` when nulls sort first, `0xFF` when they sort last.
#[inline]
pub(crate) fn null_sentinel(options: SortOptions) -> u8 {
    if options.nulls_first { 0 } else { 0xFF }
}

/// Tracks per-row encoded lengths, materializing a per-row vector only once a variable-length
/// column is added.
pub(crate) enum LengthTracker {
    /// Every row has the same `length`.
    Fixed { length: usize, num_rows: usize },
    /// Row `i` has length `lengths[i] + fixed_length`.
    Variable {
        fixed_length: usize,
        lengths: Vec<usize>,
    },
}

impl LengthTracker {
    pub(crate) fn new(num_rows: usize) -> Self {
        Self::Fixed {
            length: 0,
            num_rows,
        }
    }

    /// Add a column whose every row encodes to `new_length` bytes.
    pub(crate) fn push_fixed(&mut self, new_length: usize) {
        match self {
            LengthTracker::Fixed { length, .. } => *length += new_length,
            LengthTracker::Variable { fixed_length, .. } => *fixed_length += new_length,
        }
    }

    /// Add a column whose row `i` encodes to `new_lengths.nth(i)` bytes.
    pub(crate) fn push_variable(&mut self, new_lengths: impl ExactSizeIterator<Item = usize>) {
        match self {
            LengthTracker::Fixed { length, .. } => {
                *self = LengthTracker::Variable {
                    fixed_length: *length,
                    lengths: new_lengths.collect(),
                }
            }
            LengthTracker::Variable { lengths, .. } => {
                assert_eq!(lengths.len(), new_lengths.len());
                lengths
                    .iter_mut()
                    .zip(new_lengths)
                    .for_each(|(length, new_length)| *length += new_length);
            }
        }
    }

    /// Initialize `offsets` (shifted down by one row) from the tracked lengths, returning the
    /// total encoded byte length.
    pub(crate) fn extend_offsets(&self, initial_offset: usize, offsets: &mut Vec<usize>) -> usize {
        match self {
            LengthTracker::Fixed { length, num_rows } => {
                offsets.extend((0..*num_rows).map(|i| initial_offset + i * length));
                initial_offset + num_rows * length
            }
            LengthTracker::Variable {
                fixed_length,
                lengths,
            } => {
                let mut acc = initial_offset;
                offsets.extend(lengths.iter().map(|length| {
                    let current = acc;
                    acc += length + fixed_length;
                    current
                }));
                acc
            }
        }
    }
}
