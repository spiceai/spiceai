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

//! The byte accounting every read in this crate shares, for HTTP bodies and for data read
//! through [`std::io::Read`] (object-store files, archive entries), and the compression
//! ratio check on compressed HTTP bodies.

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion_execution::memory_pool::MemoryReservation;
use snafu::prelude::*;

use crate::{
    CompressionRatioSnafu, DECODE_BLOCK_BYTES, Error, MAX_COMPRESSION_RATIO, MemoryLimitSnafu,
    RATIO_CHECK_FLOOR_BYTES, Result, TooLargeSnafu,
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

/// The compression ratio check over one compressed HTTP body: the bytes received for it
/// and the bytes decoded from it so far.
///
/// Once the input has decoded past [`RATIO_CHECK_FLOOR_BYTES`], it is refused as soon
/// as it has expanded more than [`MAX_COMPRESSION_RATIO`] times the bytes received. The
/// check runs on every decoded block, so a refused input has produced at most one block
/// more than the larger of the floor and the ratio times the bytes received. Clones
/// share the counts, so one clone can count the input while another checks the output.
#[derive(Clone, Debug)]
pub(crate) struct Expansion {
    codec: &'static str,
    received: Arc<AtomicUsize>,
    decoded: Arc<AtomicUsize>,
}

impl Expansion {
    /// A check over an input compressed with `codec`, which names it in a refusal.
    pub(crate) fn new(codec: &'static str) -> Self {
        Self {
            codec,
            received: Arc::new(AtomicUsize::new(0)),
            decoded: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Count `bytes` more of the compressed input as received.
    pub(crate) fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Count `bytes` more decoded from the input.
    ///
    /// # Errors
    ///
    /// [`Error::CompressionRatio`] once the input has expanded past the ratio.
    pub(crate) fn decoded(&self, bytes: usize) -> Result<()> {
        let decoded = self.decoded.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let received = self.received.load(Ordering::Relaxed);
        ensure!(
            decoded <= RATIO_CHECK_FLOOR_BYTES
                || decoded <= received.saturating_mul(MAX_COMPRESSION_RATIO),
            CompressionRatioSnafu {
                codec: self.codec,
                decoded,
                received,
            }
        );
        Ok(())
    }
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
/// [`Error::TooLarge`] or [`Error::MemoryLimit`] when the data breaks `limit`, and
/// [`Error::Io`] when reading or decoding it fails.
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
                Err(source) => return Err(Error::Io { source }),
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
