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

//! The byte accounting every read in this crate shares, and the compression ratio check
//! every decoder of remote data shares: HTTP bodies, files read from object stores, and
//! archive entries.

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion_execution::memory_pool::MemoryReservation;
use snafu::prelude::*;

use crate::{
    CompressionRatioSnafu, DECODE_BLOCK_BYTES, Error, MemoryLimitSnafu, RATIO_CHECK_FLOOR_BYTES,
    Result, TooLargeSnafu,
};

/// How much a read may hold once decoded.
///
/// Every read is bounded. A response that carries data is charged to the query memory
/// pool, which `runtime.query.memory_limit` sizes, so it is held to the same budget as
/// any other buffer a query holds. A response that never carries data (a token, a
/// schema, an error) is capped at a fixed size instead.
#[derive(Clone, Copy, Debug)]
pub enum Limit<'a> {
    /// Charge every byte the read holds to this reservation, and refuse the body once
    /// the pool cannot grant more. On success the reservation keeps the charge for the
    /// returned bytes, so the caller holds it for as long as it holds them; on failure
    /// the charge is returned to the pool.
    Reserve(&'a MemoryReservation),
    /// Refuse a body whose decoded size passes this many bytes, for responses that are
    /// never bulk data.
    Size(usize),
}

impl Limit<'static> {
    /// The limit for a metadata or token response: [`crate::MAX_METADATA_BODY_BYTES`].
    pub const METADATA: Self = Self::Size(crate::MAX_METADATA_BODY_BYTES);
}

/// The bytes one read has charged to its [`Limit`].
///
/// Charges are made before the bytes are allocated, and a read that fails returns
/// what it charged when this is dropped; [`Held::keep`] leaves the charge with the
/// reservation for the caller.
pub(crate) struct Held<'a> {
    limit: Limit<'a>,
    /// Bytes charged to a [`Limit::Reserve`] reservation by this read.
    reserved: usize,
    kept: bool,
}

impl<'a> Held<'a> {
    pub(crate) fn new(limit: Limit<'a>) -> Self {
        Self {
            limit,
            reserved: 0,
            kept: false,
        }
    }

    /// Account for `bytes` more held in memory, before they are allocated.
    pub(crate) fn grow(&mut self, bytes: usize) -> Result<()> {
        if let Limit::Reserve(reservation) = self.limit {
            reservation.try_grow(bytes).context(MemoryLimitSnafu)?;
            self.reserved += bytes;
        }
        Ok(())
    }

    /// Account for `bytes` no longer held.
    pub(crate) fn shrink(&mut self, bytes: usize) {
        if let Limit::Reserve(reservation) = self.limit {
            let bytes = bytes.min(self.reserved);
            reservation.shrink(bytes);
            self.reserved -= bytes;
        }
    }

    /// Refuse a decoded size past a [`Limit::Size`] cap.
    pub(crate) fn check_size(&self, decoded: usize) -> Result<()> {
        if let Limit::Size(max_bytes) = self.limit {
            ensure!(decoded <= max_bytes, TooLargeSnafu { limit: max_bytes });
        }
        Ok(())
    }

    /// Leave what this read charged with the reservation, for the bytes it returns.
    pub(crate) fn keep(mut self) {
        self.kept = true;
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        if !self.kept
            && let Limit::Reserve(reservation) = self.limit
        {
            reservation.shrink(self.reserved);
        }
    }
}

/// A compression format whose decoded output this crate checks, and how far real data
/// in it expands.
///
/// Each threshold sits at least 3.8x above the most compressible real data measured in
/// that codec, and below what a compression bomb in it reaches: gzip and deflate cannot
/// expand much past 1,000:1, while zstd, brotli, xz and bzip2 reach tens of thousands to
/// one. One number cannot serve both, so each codec has its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Gzip,
    Deflate,
    Brotli,
    Zstd,
    Xz,
    Bzip2,
}

impl Codec {
    /// The most an input in this codec may expand, decoded bytes per compressed byte,
    /// once it has decoded past [`RATIO_CHECK_FLOOR_BYTES`].
    #[must_use]
    pub const fn max_ratio(self) -> usize {
        match self {
            Self::Gzip | Self::Deflate => 400,
            Self::Brotli | Self::Zstd | Self::Xz | Self::Bzip2 => 2_000,
        }
    }

    /// The codec's name as a refusal states it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
            Self::Brotli => "brotli",
            Self::Zstd => "zstd",
            Self::Xz => "xz",
            Self::Bzip2 => "bzip2",
        }
    }
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a compressed input is, which decides how a refusal describes it and its fix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    /// A response an origin sent: its fix is at the origin.
    Response,
    /// A file read from storage: its fix is how the file is written.
    File,
    /// A log entry of an archive, checked against the entry's compressed size.
    Log,
}

/// The compression ratio check over one compressed input: the bytes received for it and
/// the bytes decoded from it so far.
///
/// Once the input has decoded past [`RATIO_CHECK_FLOOR_BYTES`], it is refused as soon
/// as it has expanded more than its codec's [`Codec::max_ratio`] times the bytes
/// received. The check runs on every decoded block, so a refused input has produced at
/// most one block more than the larger of the floor and the ratio times the bytes
/// received. Clones share the counts, so one clone can count the input while another
/// checks the output.
#[derive(Clone, Debug)]
pub struct Expansion {
    codec: Codec,
    input: Input,
    received: Arc<AtomicUsize>,
    decoded: Arc<AtomicUsize>,
}

impl Expansion {
    /// A check over an `input` compressed with `codec`.
    #[must_use]
    pub fn new(codec: Codec, input: Input) -> Self {
        Self {
            codec,
            input,
            received: Arc::new(AtomicUsize::new(0)),
            decoded: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Count `bytes` more of the compressed input as received.
    pub fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Count `bytes` more decoded from the input.
    ///
    /// # Errors
    ///
    /// [`Error::CompressionRatio`] once the input has expanded past its codec's ratio.
    pub fn decoded(&self, bytes: usize) -> Result<()> {
        let decoded = self.decoded.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let received = self.received.load(Ordering::Relaxed);
        ensure!(
            decoded <= RATIO_CHECK_FLOOR_BYTES
                || decoded <= received.saturating_mul(self.codec.max_ratio()),
            CompressionRatioSnafu {
                codec: self.codec,
                input: self.input,
                decoded,
                received,
            }
        );
        Ok(())
    }

    /// The refusal this input has earned so far, if it has expanded past its codec's
    /// ratio: for a caller whose parser turns a failed read into text, so it can still
    /// tell a compression bomb from a malformed input.
    #[must_use]
    pub fn refused(&self) -> Option<Error> {
        let decoded = self.decoded.load(Ordering::Relaxed);
        let received = self.received.load(Ordering::Relaxed);
        (decoded > RATIO_CHECK_FLOOR_BYTES
            && decoded > received.saturating_mul(self.codec.max_ratio()))
        .then_some(Error::CompressionRatio {
            codec: self.codec,
            input: self.input,
            decoded,
            received,
        })
    }

    /// `reader`, counting every byte read from it as compressed input received.
    pub fn count_input<R: Read>(&self, reader: R) -> CountInput<R> {
        CountInput {
            inner: reader,
            expansion: self.clone(),
        }
    }

    /// `reader`, a decoder of the input, failing a read once the input has expanded past
    /// its codec's ratio. The failure is an [`io::Error`] that [`refusal`] recovers the
    /// [`Error::CompressionRatio`] from.
    pub fn check_output<R: Read>(&self, reader: R) -> CheckOutput<R> {
        CheckOutput {
            inner: reader,
            expansion: self.clone(),
        }
    }
}

/// A reader that counts what is read from it as compressed input. See
/// [`Expansion::count_input`].
pub struct CountInput<R> {
    inner: R,
    expansion: Expansion,
}

impl<R: Read> Read for CountInput<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.expansion.received(n);
        Ok(n)
    }
}

/// A decoder whose reads fail once its input expands past its codec's ratio. See
/// [`Expansion::check_output`].
pub struct CheckOutput<R> {
    inner: R,
    expansion: Expansion,
}

impl<R: Read> Read for CheckOutput<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.expansion.decoded(n).map_err(into_io)?;
        Ok(n)
    }
}

/// A refusal carried through an [`io::Error`], so a refused read fails whatever reads
/// it.
#[must_use]
pub fn into_io(error: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// The refusal an [`io::Error`] carries, when a check in this crate refused the data.
#[must_use]
pub fn refusal(error: &io::Error) -> Option<&Error> {
    error.get_ref()?.downcast_ref::<Error>()
}

/// The refusal anywhere in `error`'s chain of sources, carried directly or inside an
/// [`io::Error`], so a caller can tell a refused input from a failed one however many
/// layers wrapped it.
#[must_use]
pub fn find_refusal<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Error> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        if let Some(refused) = error.downcast_ref::<Error>() {
            return Some(refused);
        }
        if let Some(refused) = error.downcast_ref::<io::Error>().and_then(refusal) {
            return Some(refused);
        }
        current = error.source();
    }
    None
}

/// Read `reader` to its end under `limit`, for data that must be held whole.
///
/// The data is read a block at a time, each block charged before it is allocated, and
/// joined once at the end into an allocation of exactly its size; the join briefly holds
/// the data twice, which is charged too. A read that fails partway returns what it
/// charged.
///
/// # Errors
///
/// [`Error::TooLarge`] or [`Error::MemoryLimit`] when the data breaks `limit`, the
/// [`Error::CompressionRatio`] a [`CheckOutput`] in `reader` raised, and [`Error::Io`]
/// when reading or decoding it fails.
pub fn read_to_end(mut reader: impl Read, limit: Limit<'_>) -> Result<Vec<u8>> {
    let mut held = Held::new(limit);
    let mut blocks: Vec<Vec<u8>> = Vec::new();
    let mut decoded = 0usize;
    loop {
        held.grow(DECODE_BLOCK_BYTES)?;
        let mut block = vec![0u8; DECODE_BLOCK_BYTES];
        let mut filled = 0;
        while filled < block.len() {
            match reader.read(&mut block[filled..]) {
                Ok(0) => break,
                Ok(n) => {
                    filled += n;
                    held.check_size(decoded + filled)?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(from_io(error)),
            }
        }
        decoded += filled;
        let ended = filled < block.len();
        block.truncate(filled);
        blocks.push(block);
        if ended {
            break;
        }
    }

    // Charged at block capacity while reading, and held at exactly the data's size
    // once it is joined: the join is charged before it allocates, and the blocks are
    // released after it.
    let charged_for_blocks = blocks.len() * DECODE_BLOCK_BYTES;
    let out = if blocks.len() == 1 {
        let mut only = blocks.pop().unwrap_or_default();
        only.shrink_to_fit();
        held.shrink(charged_for_blocks - only.len());
        only
    } else {
        held.grow(decoded)?;
        let mut out = Vec::with_capacity(decoded);
        for block in blocks.drain(..) {
            out.extend_from_slice(&block);
        }
        held.shrink(charged_for_blocks);
        out
    };
    held.keep();
    Ok(out)
}

/// The error a failed read in [`read_to_end`] reports: the refusal a [`CheckOutput`] in
/// the reader raised, or the read failure itself.
fn from_io(error: io::Error) -> Error {
    if refusal(&error).is_none() {
        return Error::Io { source: error };
    }
    let Some(inner) = error.into_inner() else {
        return Error::Io {
            source: io::Error::other("the read was refused"),
        };
    };
    match inner.downcast::<Error>() {
        Ok(refused) => *refused,
        // `refusal` found this crate's `Error` inside, so the downcast does not fail.
        Err(other) => Error::Io {
            source: io::Error::other(other),
        },
    }
}
