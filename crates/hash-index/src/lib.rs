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

//! High-Performance Hash Index for Arrow-based Accelerators
//!
//! This crate provides a sharded hash index optimized for point lookups
//! in Arrow-based data accelerators. The index is designed to be the fastest
//! possible solution for row lookups when a primary key is specified.
//!
//! # Design Goals
//!
//! 1. **Lock-Free Reads**: 256-shard design minimizes contention
//! 2. **Cache-Friendly**: Linear probing with prefetching for batch lookups
//! 3. **Zero-Copy Keys**: Direct access to Arrow array buffers
//! 4. **Bloom Filter**: Optional fast negative lookup filtering
//!
//! # Performance
//!
//! - Single-threaded: ~275M point lookups/sec
//! - 8-threaded: ~27M concurrent ops/sec
//! - Batch (1000 keys): ~300M elements/sec
//!
//! # Architecture
//!
//! The hash index uses a 256-shard design where each shard is an independent
//! linear probing hash table protected by a [`parking_lot::RwLock`]. This provides:
//!
//! - Minimal contention even with 64+ cores
//! - Fast read locks (~10ns uncontended)
//! - Backward-shift deletion for correctness

#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod bloom;
mod extract;
mod index;

#[cfg(test)]
mod tests;

pub use bloom::{BatchBloomFilter, BloomFilter};
pub use extract::{
    KeyExtractor, PrimitiveKeyExtractor, RowConverterKeyExtractor, Utf8KeyExtractor,
};
pub use index::{
    HashIndex, HashIndexBuilder, NUM_SHARDS, RowLocation, hash_key, hash_key_bytes, index_threshold,
};

use snafu::prelude::*;

/// Errors that can occur during hash index operations.
#[derive(Debug, Snafu)]
pub enum Error {
    /// The key column was not found in the schema.
    #[snafu(display("Key column '{column}' not found in schema"))]
    KeyColumnNotFound {
        /// The name of the column that was not found.
        column: String,
    },

    /// The key type is not supported for indexing.
    #[snafu(display("Unsupported key type: {data_type}"))]
    UnsupportedKeyType {
        /// The Arrow data type that is not supported.
        data_type: String,
    },

    /// Arrow error during key extraction.
    #[snafu(display("Arrow error: {source}"))]
    Arrow {
        /// The underlying Arrow error.
        source: arrow::error::ArrowError,
    },

    /// Duplicate key detected during index build.
    #[snafu(display("Duplicate key detected"))]
    DuplicateKey,
}

/// Result type for hash index operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
