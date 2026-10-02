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
//! order-preserving (byte order is the SQL order of the key tuple, NULLs
//! first) and prefix-free per column, so the concatenation of a compound key's
//! columns is injective: two distinct key tuples never encode to the same
//! bytes. Arrow's row format is not used. `escape_proof` machine-checks the
//! prefix-freedom of the encoding's specification, and the escape the encoder
//! writes every string and binary value with is verified against that
//! specification.
//!
//! An index stores a 64-bit word per key ([`KeyEncoder::key_word`]): the
//! key's own bytes when its fields are fixed-width and fit 8 bytes, so
//! distinct keys have distinct words, and otherwise a 64-bit hash of the
//! encoded key. An index answers candidate rows that every query still
//! filters, so two keys sharing a word cost only extra rows read.
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
mod tests;

pub use encode::{BoundKeyColumns, KeyEncoder, KeyField};

/// One row's encoded key, owned.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedKey(Vec<u8>);

impl EncodedKey {
    /// The key of `row` of `bound`.
    #[must_use]
    pub fn from_row(bound: &BoundKeyColumns<'_>, row: usize) -> Self {
        let mut bytes = Vec::new();
        bound.encode_row(row, &mut bytes);
        Self(bytes)
    }

    /// A key already in [`KeyEncoder`]'s encoding.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The encoded bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

use snafu::Snafu;

/// Errors raised while encoding keys or updating an index.
#[derive(Debug, Snafu, PartialEq, Eq)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// A key column type has no order-preserving encoding.
    #[snafu(display(
        "Failed to encode an index key: the column type {data_type} is not supported."
    ))]
    UnsupportedType {
        /// The rejected Arrow type.
        data_type: String,
    },

    /// The bound columns do not match the encoder's key fields.
    #[snafu(display("Failed to encode an index key: {reason}"))]
    ColumnMismatch {
        /// What did not match.
        reason: String,
    },
}

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;
