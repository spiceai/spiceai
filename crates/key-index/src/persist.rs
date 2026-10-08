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

//! The byte format of a persisted [`IndexRun`](crate::tiered::IndexRun).
//!
//! A run is already flat — its key words, one slot per word (its only
//! posting, or an offset into the postings), and the postings — so it
//! persists as those arrays, framed. Its Bloom filter is rebuilt from the
//! words on load rather than stored, so it can never disagree with them:
//!
//! ```text
//! magic "CIDX" | version | kind | body | checksum
//! body (run): encoding u64 | files u32 | (len u32 | name)* | rows u64
//!             | words u64 | u64 * words | u32 * words (slots)
//!             | postings u64 | postings bytes
//! ```
//!
//! `encoding` is the [`KeyEncoder::word_identity`](crate::KeyEncoder::word_identity)
//! of the encoder that built the run; an index publishes only runs of its own.
//!
//! All integers are little-endian. The trailing checksum is
//! [`hash_index::hash_key_bytes_oneshot`] over everything before it, so a torn or
//! corrupt file is rejected rather than read; a caller should treat any error
//! as a missing file and index its files again.

use snafu::{Snafu, ensure};

const MAGIC: u32 = u32::from_le_bytes(*b"CIDX");
/// Bumped on any change to the byte layout.
pub const VERSION: u32 = 2;
pub(crate) const KIND_RUN: u32 = 2;

/// Why persisted bytes were rejected.
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    /// The bytes end before the structure they describe does.
    #[snafu(display("Failed to read the persisted index: the file is truncated."))]
    Truncated,
    /// Not an index file, or one of another version or kind.
    #[snafu(display(
        "Failed to read the persisted index: expected an index of kind {kind} at version {VERSION}; the file is of another format."
    ))]
    Format {
        /// The kind expected.
        kind: u32,
    },
    /// The checksum does not match the contents.
    #[snafu(display(
        "Failed to read the persisted index: its checksum does not match its contents."
    ))]
    Checksum,
    /// Lengths or offsets inside the file are inconsistent.
    #[snafu(display("Failed to read the persisted index: its internal offsets are inconsistent."))]
    Corrupt,
}

/// Result alias for this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn header(out: &mut Vec<u8>, kind: u32) {
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&kind.to_le_bytes());
}

pub(crate) fn seal(out: &mut Vec<u8>) {
    let checksum = hash_index::hash_key_bytes_oneshot(out);
    out.extend_from_slice(&checksum.to_le_bytes());
}

/// The body of `bytes`, after checking the frame and checksum.
pub(crate) fn open(bytes: &[u8], kind: u32) -> Result<Reader<'_>> {
    ensure!(bytes.len() >= 20, TruncatedSnafu);
    let (body, checksum) = bytes.split_at(bytes.len() - 8);
    let mut reader = Reader(body);
    ensure!(
        reader.u32()? == MAGIC && reader.u32()? == VERSION && reader.u32()? == kind,
        FormatSnafu { kind }
    );
    let expected = u64::from_le_bytes(checksum.try_into().map_err(|_| Error::Truncated)?);
    ensure!(
        hash_index::hash_key_bytes_oneshot(body) == expected,
        ChecksumSnafu
    );
    Ok(reader)
}

#[derive(Clone, Copy)]
pub(crate) struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub(crate) fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        ensure!(self.0.len() >= len, TruncatedSnafu);
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(head)
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().map_err(|_| Error::Truncated)?,
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        let bytes = self.bytes(8)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| Error::Truncated)?,
        ))
    }

    pub(crate) fn len(&mut self) -> Result<usize> {
        usize::try_from(self.u64()?).map_err(|_| Error::Corrupt)
    }

    /// `count` little-endian `u32`s.
    pub(crate) fn u32s(&mut self, count: usize) -> Result<Vec<u32>> {
        let bytes = self.bytes(count.checked_mul(4).ok_or(Error::Corrupt)?)?;
        Ok(bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|&c| u32::from_le_bytes(c))
            .collect())
    }

    /// `count` little-endian `u64`s.
    pub(crate) fn u64s(&mut self, count: usize) -> Result<Vec<u64>> {
        let bytes = self.bytes(count.checked_mul(8).ok_or(Error::Corrupt)?)?;
        Ok(bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|&c| u64::from_le_bytes(c))
            .collect())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub(crate) fn put_u32s(out: &mut Vec<u8>, values: &[u32]) {
    out.reserve(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

pub(crate) fn put_u64s(out: &mut Vec<u8>, values: &[u64]) {
    out.reserve(values.len() * 8);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}
