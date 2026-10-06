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

//! Secondary indexes over compound Arrow keys: immutable sorted runs, one per
//! write, that a table's readers probe for the row positions holding a key.
//!
//! # Keys
//!
//! [`KeyEncoder`] turns a row's key columns into a byte string that is
//! prefix-free per column, so concatenating a compound key's columns adds no
//! collisions: key tuples whose columns encode differently never encode to the
//! same bytes. Each column's encoding tells apart every two values except
//! floats, where `-0.0` and `0.0`, and every NaN, share an encoding by design.
//! Byte order is the SQL order of the key tuple, NULLs first, except for those
//! floats: that one NaN encoding sorts above `+∞`. Index lookups compare
//! encodings only for equality, so a lookup for one of those floats also
//! returns rows holding the others, which its filter drops if the query tells
//! them apart. Arrow's row format is not used. `escape_proof` machine-checks the
//! prefix-freedom of the encoding's specification, and the escape the encoder
//! writes every string and binary value with is verified against that
//! specification.
//!
//! An index stores a 64-bit word per key ([`KeyEncoder::key_word`]): the
//! key's encoded bytes when its fields are fixed-width and fit 8 bytes, so
//! keys with distinct encodings have distinct words, and otherwise a 64-bit
//! hash of the encoded key. An index answers candidate rows that every query
//! still filters, so two keys sharing a word cost only extra rows read.
//!
//! # Structures
//!
//! - [`tiered::IndexRun`]: an immutable run over the files one write produced:
//!   its keys' words, ascending, each with its rows' positions.
//! - [`tiered::TieredIndex`]: a secondary index layered by storage tier: one
//!   run per written file set, merged by size tier, with per-file coverage so
//!   a reader uses the index only for the files a live run covers.

mod encode;
mod escape_proof;
pub mod persist;
pub mod tiered;
mod varint;
mod word_proof;

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub use encode::{BoundKeyColumns, KeyEncoder, KeyField, can_key, key_type};

use snafu::Snafu;

/// Errors raised while encoding keys or updating an index.
#[derive(Debug, Snafu, PartialEq, Eq)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// A key column type has no key encoding.
    #[snafu(display(
        "Failed to encode an index key: the column type {data_type} is not supported."
    ))]
    UnsupportedType {
        /// The rejected Arrow type.
        data_type: String,
    },

    /// A different number of columns was bound than the key has fields.
    #[snafu(display(
        "Failed to encode an index key: expected {expected} key columns but received {received}"
    ))]
    ColumnCount {
        /// The key's fields.
        expected: usize,
        /// The columns bound.
        received: usize,
    },

    /// A bound column's type is not its key field's.
    #[snafu(display(
        "Failed to encode an index key: key column {index} is {found} but the key declares {declared}"
    ))]
    ColumnType {
        /// The column's position in the key.
        index: usize,
        /// The bound array's type.
        found: arrow_schema::DataType,
        /// The key field's type.
        declared: arrow_schema::DataType,
    },

    /// The bound columns do not all have the same number of rows.
    #[snafu(display(
        "Failed to encode an index key: key column {index} has {rows} rows but key column 0 has {expected}"
    ))]
    ColumnLength {
        /// The column's position in the key.
        index: usize,
        /// Its rows.
        rows: usize,
        /// The first column's rows.
        expected: usize,
    },

    /// A column of a non-nullable key field holds NULL.
    #[snafu(display(
        "Failed to encode an index key: key column {index} is declared non-nullable but holds NULL"
    ))]
    UnexpectedNull {
        /// The column's position in the key.
        index: usize,
    },
}

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;
