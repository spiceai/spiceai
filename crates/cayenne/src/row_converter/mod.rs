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

//! A Cayenne-owned, **versioned** row-format converter.
//!
//! Cayenne encodes composite / non-`Int64` primary keys into a comparable byte
//! sequence and **persists those bytes durably** — as the `row_key` column of on-disk
//! deletion-vector files, as `cayenne_insert_record.pk_bytes` in the metastore, and inside
//! inlined-delete blobs. On read (after a restart or compaction) Cayenne re-encodes fresh
//! keys and byte-compares them against the stored bytes.
//!
//! This encoding is a copy of the format produced by Apache Arrow's `arrow-row` crate.
//! Arrow explicitly documents that *"the encoding of the row format may change from release
//! to release"* — it is designed as a transient sort/compare representation, not a storage
//! format. Vendoring it here decouples Cayenne's durable data from arrow-rs's version cadence:
//! a future arrow bump can no longer silently change the bytes Cayenne persists.
//!
//! # Versioning
//!
//! [`RowConverter`] is an enum whose variant is the wire-format version. [`RowConverter::new`]
//! always builds the current version; older data is read by constructing the matching variant
//! (see [`RowConverter::with_version`] and [`RowFormatVersion`]). [`RowFormatVersion::V1`] is
//! **byte-identical** to `arrow-row` 58.3.0, so all previously-persisted data reads unchanged
//! (it is implicitly version 1).
//!
//! # Extensibility
//!
//! Version-agnostic framing (offsets, null sentinels, column concatenation) lives in
//! [`codec::RowCodec`]; per-column behavior lives behind the [`codec::ColumnCodec`] trait. A
//! future version that optimizes a single data type adds one small codec and routes that type
//! to it in a new `build_codec`, delegating every unchanged type to the previous version —
//! it never re-implements the whole encoder/decoder.
//!
//! Only the Arrow types reachable as a Cayenne primary key are supported (all integer/float
//! primitives, `Boolean`, the byte/string family including views, dates, times, timestamps at
//! every unit, and decimals). Unsupported types return [`arrow_schema::ArrowError::NotYetImplemented`].

mod codec;
mod rows;
mod v1;

#[cfg(test)]
mod tests;

use arrow::array::ArrayRef;
use arrow_schema::{ArrowError, DataType};

pub use arrow_schema::SortOptions;

pub use codec::RowCodec;
pub use rows::{OwnedRow, Row, Rows, RowsIter};

/// The wire-format version of a set of encoded row bytes.
///
/// The version determines how bytes are decoded, so older persisted data stays readable by
/// selecting the version that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowFormatVersion {
    /// Byte-compatible with Apache Arrow `arrow-row` 58.3.0.
    V1,
}

impl RowFormatVersion {
    /// The version used for all newly-encoded data.
    pub const CURRENT: Self = Self::V1;

    /// A stable integer identifier, suitable for persisting alongside encoded bytes.
    #[must_use]
    pub const fn id(self) -> u16 {
        match self {
            Self::V1 => 1,
        }
    }

    /// The version for a previously-persisted [`id`](Self::id), or `None` if unknown.
    #[must_use]
    pub const fn from_id(id: u16) -> Option<Self> {
        match id {
            1 => Some(Self::V1),
            _ => None,
        }
    }
}

/// Configures the data type and sort order of one column of the row encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    pub(crate) options: SortOptions,
    pub(crate) data_type: DataType,
}

impl SortField {
    /// Create a field for `data_type` with default [`SortOptions`] (ascending, nulls first).
    #[must_use]
    pub fn new(data_type: DataType) -> Self {
        Self::new_with_options(data_type, SortOptions::default())
    }

    /// Create a field for `data_type` with explicit [`SortOptions`].
    #[must_use]
    pub fn new_with_options(data_type: DataType, options: SortOptions) -> Self {
        Self { options, data_type }
    }
}

/// A versioned, row-oriented encoder for a fixed sequence of [`SortField`] columns.
///
/// Each variant is a wire-format version. Construct with [`RowConverter::new`] (current
/// version) or [`RowConverter::with_version`] (to read data written by an older version).
#[derive(Debug)]
pub enum RowConverter {
    /// The [`RowFormatVersion::V1`] converter — byte-compatible with `arrow-row` 58.3.0.
    Version1(RowCodec),
}

impl RowConverter {
    /// Create a converter for the current format version ([`RowFormatVersion::CURRENT`]).
    ///
    /// # Errors
    /// Returns [`ArrowError::NotYetImplemented`] if any field's data type is not a supported
    /// primary-key type.
    pub fn new(fields: Vec<SortField>) -> Result<Self, ArrowError> {
        Self::with_version(RowFormatVersion::CURRENT, fields)
    }

    /// Create a converter for a specific format `version`.
    ///
    /// # Errors
    /// Returns [`ArrowError::NotYetImplemented`] if any field's data type is not supported by
    /// that version.
    pub fn with_version(
        version: RowFormatVersion,
        fields: Vec<SortField>,
    ) -> Result<Self, ArrowError> {
        match version {
            RowFormatVersion::V1 => Ok(Self::Version1(RowCodec::new(fields, v1::build_codec)?)),
        }
    }

    /// The format version of this converter.
    #[must_use]
    pub fn version(&self) -> RowFormatVersion {
        match self {
            Self::Version1(_) => RowFormatVersion::V1,
        }
    }

    fn codec(&self) -> &RowCodec {
        match self {
            Self::Version1(codec) => codec,
        }
    }

    /// Encode `columns` into a [`Rows`] of comparable byte sequences.
    ///
    /// # Errors
    /// Returns an error if `columns` does not match the schema this converter was built with.
    pub fn convert_columns(&self, columns: &[ArrayRef]) -> Result<Rows, ArrowError> {
        self.codec().convert_columns(columns)
    }

    /// Decode previously-encoded `rows` back into their column arrays.
    ///
    /// Cayenne does not decode at runtime; this exists for round-trip validation and future use.
    ///
    /// # Errors
    /// Returns an error if the row bytes are malformed for this converter's schema.
    pub fn convert_rows<'a>(
        &self,
        rows: impl IntoIterator<Item = Row<'a>>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        self.codec().convert_rows(rows)
    }
}
