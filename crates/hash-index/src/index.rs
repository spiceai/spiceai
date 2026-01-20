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

//! High-performance sharded hash index for Arrow-based accelerators.
//!
//! This module provides a lock-free hash index optimized for read-heavy workloads
//! with the following characteristics:
//!
//! - **256-shard design**: Minimal contention even with 64+ cores
//! - **Parking-lot `RwLock`**: Fast read locks (~10ns uncontended)
//! - **Linear probing**: Cache-friendly, simple, fast
//! - **Bloom filter**: Optional fast negative lookups
//! - **Batch prefetch**: Optimized for multiple key lookups
//!
//! # Performance
//!
//! Single-threaded: ~275M point lookups/sec\
//! 8-threaded concurrent: ~27M ops/sec\
//! Batch (1000 keys): 300M elements/sec
//!
//! # Platform Requirements
//!
//! This module requires at least a 64-bit platform. All `u64 -> usize` casts
//! are safe because the Spice runtime requires `usize` to be at least 64 bits.

// Spice runtime requires at least 64-bit pointer size. These casts are always safe.
#![allow(clippy::cast_possible_truncation)]

// Compile-time assertion: require at least 64-bit pointer size (8 bytes).
// This is a build-time check that prevents compiling on unsupported platforms.
const _: () = assert!(
    size_of::<usize>() >= 8,
    "hash-index requires a 64-bit platform (usize must be at least 8 bytes)"
);

use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow::array::RecordBatch;
use parking_lot::RwLock;
use snafu::OptionExt;
use twox_hash::XxHash3_64;

use crate::bloom::BloomFilter;
use crate::extract::create_key_extractor;
use crate::{IndexOverflowSnafu, Result};

/// Fixed seed for deterministic hashing across instances.
const HASH_SEED: u64 = 0x5370_6963_6541_4920; // "SpiceAI " in hex

/// Number of shards for concurrent access.
///
/// Using a power of 2 enables efficient bitwise masking for shard selection.
/// This value also serves as the threshold multiplier for indexing decisions.
pub const NUM_SHARDS: usize = 256;
const SHARD_MASK: u64 = (NUM_SHARDS - 1) as u64;

/// Calculates the threshold row count below which indexing is not beneficial.
///
/// The threshold is calculated as `NUM_SHARDS × parallelism`. For small tables
/// below this threshold, linear scans are faster than index lookups due to
/// the overhead of index construction and maintenance.
///
/// # Arguments
///
/// * `parallelism` - The number of parallel threads (e.g., from `DataFusion`'s
///   `target_partitions` setting)
///
/// # Example
///
/// With parallelism=8: threshold = 256 × 8 = 2,048 rows\
/// With parallelism=64: threshold = 256 × 64 = 16,384 rows
#[inline]
#[must_use]
pub const fn index_threshold(parallelism: usize) -> usize {
    NUM_SHARDS * parallelism
}

/// Computes a 64-bit hash for a key using XXH3.
///
/// This is the primary hash function used throughout the index.
#[inline]
pub fn hash_key<K: Hash>(key: &K) -> u64 {
    use std::hash::Hasher;

    let mut hasher = XxHash3_64::with_seed(HASH_SEED);
    key.hash(&mut hasher);
    hasher.finish()
}

/// Computes hash from raw byte slices (for composite keys).
#[inline]
#[must_use]
pub fn hash_key_bytes(parts: &[&[u8]]) -> u64 {
    use std::hash::Hasher;

    let mut hasher = XxHash3_64::with_seed(HASH_SEED);
    for part in parts {
        hasher.write(part);
    }
    hasher.finish()
}

/// Location of a row in the accelerator's storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowLocation {
    /// Partition index.
    pub partition: u32,
    /// Batch index within the partition.
    pub batch: u32,
    /// Row index within the batch.
    pub row: u32,
}

impl RowLocation {
    /// Creates a new row location.
    #[inline]
    #[must_use]
    pub const fn new(partition: u32, batch: u32, row: u32) -> Self {
        Self {
            partition,
            batch,
            row,
        }
    }

    /// Creates a simple location with partition 0.
    #[inline]
    #[must_use]
    pub const fn simple(batch: u32, row: u32) -> Self {
        Self::new(0, batch, row)
    }
}

/// A high-performance sharded hash index.
///
/// Uses 256 shards to minimize lock contention, with per-shard linear probing
/// hash tables and optional bloom filter for fast negative lookups.
pub struct HashIndex {
    shards: Box<[Shard; NUM_SHARDS]>,
    len: AtomicUsize,
    key_columns: Vec<String>,
    bloom: Option<RwLock<BloomFilter>>,
}

/// Builder for constructing a `HashIndex`.
pub struct HashIndexBuilder {
    key_columns: Vec<String>,
    expected_rows: usize,
    allow_duplicates: bool,
    use_bloom_filter: bool,
    min_rows_threshold: usize,
}

impl HashIndexBuilder {
    /// Creates a new builder with the specified key columns.
    #[must_use]
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            key_columns,
            expected_rows: 0,
            allow_duplicates: false,
            use_bloom_filter: true,
            min_rows_threshold: 0,
        }
    }

    /// Sets the expected number of rows for capacity planning.
    #[must_use]
    pub fn with_expected_rows(mut self, rows: usize) -> Self {
        self.expected_rows = rows;
        self
    }

    /// Allows duplicate keys (last write wins).
    #[must_use]
    pub fn allow_duplicates(mut self, allow: bool) -> Self {
        self.allow_duplicates = allow;
        self
    }

    /// Enables or disables the bloom filter.
    #[must_use]
    pub fn with_bloom_filter(mut self, enabled: bool) -> Self {
        self.use_bloom_filter = enabled;
        self
    }

    /// Sets the minimum row threshold below which no index will be built.
    ///
    /// Use [`index_threshold`] to calculate an appropriate value based on
    /// the parallelism setting (e.g., `DataFusion`'s `target_partitions`).
    ///
    /// If the total row count is below this threshold, [`try_build`] returns
    /// `None` instead of building an index, as linear scans would be faster.
    #[must_use]
    pub fn with_min_rows_threshold(mut self, threshold: usize) -> Self {
        self.min_rows_threshold = threshold;
        self
    }

    /// Builds the hash index from the given partitions.
    ///
    /// This method always builds an index regardless of row count. Use
    /// [`try_build`] if you want to skip index creation for small tables.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails for any batch.
    pub fn build(self, partitions: &[Vec<RecordBatch>]) -> Result<HashIndex> {
        self.build_internal(partitions)
    }

    /// Builds a hash index only if the row count exceeds the minimum threshold.
    ///
    /// Returns `None` if the total row count is below [`with_min_rows_threshold`].
    /// This allows callers to fall back to linear scans for small tables where
    /// index overhead isn't worthwhile.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails for any batch.
    pub fn try_build(self, partitions: &[Vec<RecordBatch>]) -> Result<Option<HashIndex>> {
        let total_rows: usize = partitions
            .iter()
            .flat_map(|p| p.iter())
            .map(RecordBatch::num_rows)
            .sum();

        if total_rows < self.min_rows_threshold {
            tracing::debug!(
                total_rows,
                threshold = self.min_rows_threshold,
                "Skipping index creation: row count below threshold"
            );
            return Ok(None);
        }

        self.build_internal(partitions).map(Some)
    }

    fn build_internal(self, partitions: &[Vec<RecordBatch>]) -> Result<HashIndex> {
        let start_time = Instant::now();

        let total_rows: usize = partitions
            .iter()
            .flat_map(|p| p.iter())
            .map(RecordBatch::num_rows)
            .sum();

        let num_partitions = partitions.len();
        let num_batches: usize = partitions.iter().map(Vec::len).sum();
        let key_columns_display = self.key_columns.join(", ");

        tracing::info!(
            rows = total_rows,
            partitions = num_partitions,
            batches = num_batches,
            key_columns = %key_columns_display,
            "Building hash index"
        );

        let capacity = self.expected_rows.max(total_rows).max(1024);
        let per_shard = (capacity / NUM_SHARDS).max(16);

        let shards: Vec<Shard> = (0..NUM_SHARDS).map(|_| Shard::new(per_shard)).collect();
        let shards: Box<[Shard; NUM_SHARDS]> = shards.try_into().unwrap_or_else(|_| unreachable!());

        let bloom = if self.use_bloom_filter {
            Some(RwLock::new(BloomFilter::new(total_rows.max(16))))
        } else {
            None
        };

        let index = HashIndex {
            shards,
            len: AtomicUsize::new(0),
            key_columns: self.key_columns.clone(),
            bloom,
        };

        // Progress tracking for large indexes
        // Use a max threshold (1M rows) to ensure more frequent updates for extremely large indexes
        let progress_interval = if total_rows > 1_000_000 {
            // Log every 10% or 1M rows, whichever is smaller
            (total_rows / 10).min(1_000_000)
        } else if total_rows > 100_000 {
            total_rows / 5 // Log every 20%
        } else {
            usize::MAX // Don't log progress for small tables
        };
        let mut rows_processed: usize = 0;
        let mut last_progress_log: usize = 0;

        // Insert all entries
        for (partition_idx, partition) in partitions.iter().enumerate() {
            for (batch_idx, batch) in partition.iter().enumerate() {
                if batch.num_rows() == 0 {
                    continue;
                }

                let extractor = create_key_extractor(batch, &self.key_columns)?;

                for row in 0..extractor.len() {
                    let Some(hash) = extractor.hash_key(row) else {
                        continue; // Skip null keys
                    };

                    let partition_u32 =
                        u32::try_from(partition_idx)
                            .ok()
                            .context(IndexOverflowSnafu {
                                context: "partition",
                                value: partition_idx,
                            })?;
                    let batch_u32 = u32::try_from(batch_idx).ok().context(IndexOverflowSnafu {
                        context: "batch",
                        value: batch_idx,
                    })?;
                    let row_u32 = u32::try_from(row).ok().context(IndexOverflowSnafu {
                        context: "row",
                        value: row,
                    })?;

                    let location = RowLocation::new(partition_u32, batch_u32, row_u32);

                    if self.allow_duplicates {
                        index.insert_or_replace(hash, location);
                    } else {
                        match index.insert(hash, location) {
                            InsertResult::Inserted => {}
                            InsertResult::HashCollision(existing_loc) => {
                                // Hash collision detected. Verify if keys are actually equal
                                // by comparing their raw byte representations.
                                let existing_batch = &partitions[existing_loc.partition as usize]
                                    [existing_loc.batch as usize];
                                let existing_extractor =
                                    create_key_extractor(existing_batch, &self.key_columns)?;

                                let current_key_bytes = extractor.key_bytes(row);
                                let existing_key_bytes =
                                    existing_extractor.key_bytes(existing_loc.row as usize);

                                // Note: Null keys are filtered at line 301-303 (Skip null keys),
                                // so key_bytes() should never return None here. If it does,
                                // treat it as a hash collision (different keys) to be safe.
                                match (current_key_bytes, existing_key_bytes) {
                                    (Some(current), Some(existing)) if current == existing => {
                                        // Same key bytes = true duplicate key
                                        return Err(crate::Error::DuplicateKey);
                                    }
                                    _ => {
                                        // Different key bytes (or null) = hash collision.
                                        // Per DATA CORRECTNESS principles: lookups must never fail to find
                                        // existing data. Since our hash table doesn't support chaining,
                                        // we must fail the index build rather than silently drop keys.
                                        return Err(crate::Error::HashCollision { hash });
                                    }
                                }
                            }
                        }
                    }

                    rows_processed += 1;
                }

                // Log progress for large indexes
                if rows_processed - last_progress_log >= progress_interval {
                    let percent = (rows_processed * 100) / total_rows;
                    let elapsed = start_time.elapsed();
                    // Cast precision loss is acceptable for logging display purposes
                    #[expect(clippy::cast_precision_loss)]
                    let rows_per_sec_f64 = if elapsed.as_secs_f64() > 0.0 {
                        rows_processed as f64 / elapsed.as_secs_f64()
                    } else {
                        0.0
                    };
                    // Truncation/sign loss acceptable: rows/sec won't exceed u64::MAX
                    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let rows_per_sec = rows_per_sec_f64 as u64;
                    tracing::info!(
                        progress_percent = percent,
                        rows_indexed = rows_processed,
                        rows_per_sec = rows_per_sec,
                        "Hash index build progress"
                    );
                    last_progress_log = rows_processed;
                }
            }
        }

        let elapsed = start_time.elapsed();
        let indexed_entries = index.len();
        // Cast precision loss is acceptable for logging display purposes
        #[expect(clippy::cast_precision_loss)]
        let rows_per_sec_f64 = if elapsed.as_secs_f64() > 0.0 {
            indexed_entries as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        // Truncation/sign loss acceptable: rows/sec won't exceed u64::MAX
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let rows_per_sec = rows_per_sec_f64 as u64;

        // Estimate memory usage: each shard has slots, each slot is 16 bytes (hash + location)
        let memory_bytes = index.memory_usage_bytes();
        #[expect(clippy::cast_precision_loss)]
        let memory_mb = memory_bytes as f64 / 1_048_576.0;

        // Truncation/sign loss acceptable: elapsed_ms won't exceed u64::MAX for practical builds
        #[expect(clippy::cast_possible_truncation)]
        let elapsed_ms = elapsed.as_millis() as u64;

        tracing::info!(
            entries = indexed_entries,
            elapsed_ms = elapsed_ms,
            rows_per_sec = rows_per_sec,
            memory_bytes = memory_bytes,
            memory_mb = format!("{memory_mb:.2}"),
            bloom_filter = self.use_bloom_filter,
            "Hash index build complete"
        );

        // Record OTel metrics when the metrics feature is enabled
        #[cfg(feature = "metrics")]
        {
            let dimensions = &[];
            telemetry::track_hash_index_build(dimensions);
            telemetry::track_hash_index_build_duration(elapsed, dimensions);
            // usize to u64 never truncates on 64-bit, and widens safely on 32-bit
            telemetry::track_hash_index_entries(indexed_entries as u64, dimensions);
            telemetry::track_hash_index_memory_bytes(memory_bytes as u64, dimensions);
        }

        Ok(index)
    }
}

/// A single shard of the hash index.
struct Shard {
    table: RwLock<ShardTable>,
}

/// Internal table structure for a shard.
struct ShardTable {
    slots: Vec<Slot>,
    mask: usize,
    len: usize,
}

/// Sentinel value used to mark empty slots in the hash table.
/// We use `u64::MAX` as the empty marker because it is an extremely unlikely hash
/// value in practice, so very few real hashes need to be remapped away from it.
const EMPTY_SLOT_SENTINEL: u64 = u64::MAX;

/// Normalizes a hash value to avoid collision with the empty-slot sentinel.
/// If the hash equals `EMPTY_SLOT_SENTINEL` (`u64::MAX`), it is mapped to `u64::MAX - 1`.
/// This means hashes of `u64::MAX` and `u64::MAX - 1` are not distinguished, but such
/// values are vanishingly rare for our hash functions in practice.
/// Using `u64::MAX` as the sentinel makes this collision extremely unlikely compared to
/// using `0`, which is a much more common hash value and would cause more frequent
/// remapping and potential collisions.
#[inline]
const fn normalize_hash(hash: u64) -> u64 {
    if hash == EMPTY_SLOT_SENTINEL {
        u64::MAX - 1
    } else {
        hash
    }
}

/// A slot in the hash table.
#[derive(Clone, Copy)]
struct Slot {
    /// The normalized hash value. `EMPTY_SLOT_SENTINEL` (`u64::MAX`) indicates an empty slot.
    /// Any actual hash that equals `u64::MAX` is remapped to `u64::MAX - 1` via `normalize_hash()`.
    hash: u64,
    location: RowLocation,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            hash: EMPTY_SLOT_SENTINEL,
            location: RowLocation::new(0, 0, 0),
        }
    }
}

impl Shard {
    fn new(initial_capacity: usize) -> Self {
        let capacity = initial_capacity.max(16).next_power_of_two();
        Self {
            table: RwLock::new(ShardTable {
                slots: vec![Slot::default(); capacity],
                mask: capacity - 1,
                len: 0,
            }),
        }
    }

    #[inline]
    fn get(&self, hash: u64) -> Option<RowLocation> {
        let table = self.table.read();
        table.get(hash)
    }

    fn insert(&self, hash: u64, location: RowLocation) -> InsertResult {
        let mut table = self.table.write();
        table.insert(hash, location)
    }

    fn insert_or_replace(&self, hash: u64, location: RowLocation) {
        let mut table = self.table.write();
        table.insert_or_replace(hash, location);
    }

    fn remove(&self, hash: u64) -> Option<RowLocation> {
        let mut table = self.table.write();
        table.remove(hash)
    }

    fn len(&self) -> usize {
        self.table.read().len
    }

    fn clear(&self) {
        let mut table = self.table.write();
        table.slots.fill(Slot::default());
        table.len = 0;
    }

    fn capacity(&self) -> usize {
        self.table.read().slots.len()
    }
}

/// Result of an insert operation into the hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    /// Successfully inserted.
    Inserted,
    /// Hash collision detected. Contains the location of the existing entry.
    HashCollision(RowLocation),
}

impl ShardTable {
    #[inline]
    fn get(&self, hash: u64) -> Option<RowLocation> {
        if self.len == 0 {
            return None;
        }

        let mut idx = (hash as usize) & self.mask;
        let mut probes = 0;

        loop {
            let slot = unsafe { self.slots.get_unchecked(idx) };

            if slot.hash == hash {
                return Some(slot.location);
            }

            if slot.hash == EMPTY_SLOT_SENTINEL {
                return None;
            }

            probes += 1;
            if probes > self.slots.len() {
                return None;
            }

            idx = (idx + 1) & self.mask;
        }
    }

    fn insert(&mut self, hash: u64, location: RowLocation) -> InsertResult {
        if self.len * 4 >= self.slots.len() * 3 {
            self.grow();
        }

        let mut idx = (hash as usize) & self.mask;

        loop {
            let slot = &mut self.slots[idx];

            if slot.hash == EMPTY_SLOT_SENTINEL {
                slot.hash = hash;
                slot.location = location;
                self.len += 1;
                return InsertResult::Inserted;
            }

            if slot.hash == hash {
                return InsertResult::HashCollision(slot.location);
            }

            idx = (idx + 1) & self.mask;
        }
    }

    fn insert_or_replace(&mut self, hash: u64, location: RowLocation) {
        if self.len * 4 >= self.slots.len() * 3 {
            self.grow();
        }

        let mut idx = (hash as usize) & self.mask;

        loop {
            let slot = &mut self.slots[idx];

            if slot.hash == EMPTY_SLOT_SENTINEL {
                slot.hash = hash;
                slot.location = location;
                self.len += 1;
                return;
            }

            if slot.hash == hash {
                slot.location = location;
                return;
            }

            idx = (idx + 1) & self.mask;
        }
    }

    fn remove(&mut self, hash: u64) -> Option<RowLocation> {
        if self.len == 0 {
            return None;
        }

        let mut idx = (hash as usize) & self.mask;
        let start_idx = idx;

        loop {
            let slot = &self.slots[idx];

            if slot.hash == hash {
                let location = slot.location;
                // Backward shift deletion to maintain probe chains
                self.backward_shift_delete(idx);
                self.len -= 1;
                return Some(location);
            }

            if slot.hash == EMPTY_SLOT_SENTINEL {
                return None;
            }

            idx = (idx + 1) & self.mask;
            if idx == start_idx {
                return None;
            }
        }
    }

    /// Backward shift deletion to maintain linear probing correctness.
    fn backward_shift_delete(&mut self, mut empty_idx: usize) {
        self.slots[empty_idx] = Slot::default();

        let mut current_idx = (empty_idx + 1) & self.mask;

        loop {
            let slot = self.slots[current_idx];
            if slot.hash == EMPTY_SLOT_SENTINEL {
                break;
            }

            let ideal_idx = (slot.hash as usize) & self.mask;

            // Check if this slot should be shifted
            let should_shift = if current_idx >= empty_idx {
                ideal_idx <= empty_idx || ideal_idx > current_idx
            } else {
                ideal_idx <= empty_idx && ideal_idx > current_idx
            };

            if should_shift {
                self.slots[empty_idx] = slot;
                self.slots[current_idx] = Slot::default();
                empty_idx = current_idx;
            }

            current_idx = (current_idx + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let new_capacity = (self.slots.len() * 2).max(16);
        let mut new_slots = vec![Slot::default(); new_capacity];
        let new_mask = new_capacity - 1;

        for slot in &self.slots {
            if slot.hash != EMPTY_SLOT_SENTINEL {
                let mut idx = (slot.hash as usize) & new_mask;
                loop {
                    if new_slots[idx].hash == EMPTY_SLOT_SENTINEL {
                        new_slots[idx] = *slot;
                        break;
                    }
                    idx = (idx + 1) & new_mask;
                }
            }
        }

        self.slots = new_slots;
        self.mask = new_mask;
    }
}

impl HashIndex {
    /// Creates a new empty hash index.
    #[must_use]
    pub fn new(key_columns: Vec<String>) -> Self {
        Self::with_capacity(key_columns, 1024)
    }

    /// Creates a new hash index with expected capacity.
    #[must_use]
    pub fn with_capacity(key_columns: Vec<String>, expected_rows: usize) -> Self {
        let per_shard = (expected_rows / NUM_SHARDS).max(16);
        let shards: Vec<Shard> = (0..NUM_SHARDS).map(|_| Shard::new(per_shard)).collect();

        Self {
            shards: shards.try_into().unwrap_or_else(|_| unreachable!()),
            len: AtomicUsize::new(0),
            key_columns,
            bloom: None,
        }
    }

    /// Creates a new hash index with bloom filter enabled.
    #[must_use]
    pub fn with_bloom_filter(key_columns: Vec<String>, expected_items: usize) -> Self {
        let per_shard = (expected_items / NUM_SHARDS).max(16);
        let shards: Vec<Shard> = (0..NUM_SHARDS).map(|_| Shard::new(per_shard)).collect();

        Self {
            shards: shards.try_into().unwrap_or_else(|_| unreachable!()),
            len: AtomicUsize::new(0),
            key_columns,
            bloom: Some(RwLock::new(BloomFilter::new(expected_items))),
        }
    }

    /// Creates a builder for constructing a hash index.
    #[must_use]
    pub fn builder(key_columns: Vec<String>) -> HashIndexBuilder {
        HashIndexBuilder::new(key_columns)
    }

    #[inline]
    fn shard(&self, hash: u64) -> &Shard {
        let shard_idx = ((hash >> 56) ^ (hash >> 48) ^ hash) & SHARD_MASK;
        // SAFETY: shard_idx is always < NUM_SHARDS (256) due to SHARD_MASK
        unsafe { self.shards.get_unchecked(shard_idx as usize) }
    }

    /// Returns the key columns.
    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if bloom filter is enabled.
    pub fn has_bloom_filter(&self) -> bool {
        self.bloom.is_some()
    }

    /// Looks up by pre-computed hash.
    #[inline]
    pub fn get_by_hash(&self, hash: u64) -> Option<RowLocation> {
        let hash = normalize_hash(hash);
        // Fast path: bloom filter check
        if let Some(bloom) = &self.bloom
            && !bloom.read().might_contain(hash)
        {
            return None;
        }
        self.shard(hash).get(hash)
    }

    /// Looks up by key.
    #[inline]
    pub fn get<K: Hash>(&self, key: &K) -> Option<RowLocation> {
        let hash = hash_key(key);
        self.get_by_hash(hash)
    }

    /// Batch lookup with prefetching.
    pub fn get_batch(&self, hashes: &[u64]) -> Vec<Option<RowLocation>> {
        const PREFETCH_DISTANCE: usize = 8;

        let mut results = Vec::with_capacity(hashes.len());
        for (i, &hash) in hashes.iter().enumerate() {
            if i + PREFETCH_DISTANCE < hashes.len() {
                let _shard = self.shard(hashes[i + PREFETCH_DISTANCE]);
            }
            results.push(self.get_by_hash(hash));
        }

        results
    }

    /// Checks if index might contain hash (bloom filter only).
    #[inline]
    pub fn might_contain(&self, hash: u64) -> bool {
        let hash = normalize_hash(hash);
        match &self.bloom {
            Some(bloom) => bloom.read().might_contain(hash),
            None => true,
        }
    }

    /// Checks if index contains hash.
    #[inline]
    pub fn contains(&self, hash: u64) -> bool {
        self.get_by_hash(hash).is_some()
    }

    /// Inserts a new entry.
    ///
    /// Returns `InsertResult::Inserted` if the entry was successfully inserted,
    /// or `InsertResult::HashCollision(existing_location)` if an entry with the
    /// same hash already exists.
    pub fn insert(&self, hash: u64, location: RowLocation) -> InsertResult {
        let hash = normalize_hash(hash);
        let result = self.shard(hash).insert(hash, location);
        if matches!(result, InsertResult::Inserted) {
            self.len.fetch_add(1, Ordering::Relaxed);
            if let Some(bloom) = &self.bloom {
                bloom.write().insert(hash);
            }
        }
        result
    }

    /// Inserts or replaces an entry.
    pub fn insert_or_replace(&self, hash: u64, location: RowLocation) {
        let hash = normalize_hash(hash);
        let shard = self.shard(hash);
        let old_len = shard.len();
        shard.insert_or_replace(hash, location);
        let new_len = shard.len();
        if new_len > old_len {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(bloom) = &self.bloom {
            bloom.write().insert(hash);
        }
    }

    /// Removes an entry.
    pub fn remove(&self, hash: u64) -> Option<RowLocation> {
        let hash = normalize_hash(hash);
        let result = self.shard(hash).remove(hash);
        if result.is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
        result
    }

    /// Returns the estimated memory usage in bytes.
    ///
    /// This includes the hash table slots across all shards and the bloom filter
    /// if enabled. The actual memory usage may be slightly higher due to allocator
    /// overhead and struct padding.
    #[must_use]
    pub fn memory_usage_bytes(&self) -> usize {
        // Each slot is 16 bytes (8 byte hash + 8 byte RowLocation which packs into 8 bytes)
        const SLOT_SIZE: usize = 16;

        let mut total: usize = 0;

        // Sum up all shard capacities
        for shard in self.shards.iter() {
            total += shard.capacity() * SLOT_SIZE;
        }

        // Add bloom filter memory if present
        if let Some(bloom) = &self.bloom {
            // Bloom filter uses 1 bit per entry, roughly
            total += bloom.read().memory_usage_bytes();
        }

        total
    }

    /// Clears all entries.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            shard.clear();
        }
        self.len.store(0, Ordering::Relaxed);
        if let Some(bloom) = &self.bloom {
            bloom.write().clear();
        }
    }

    /// Rebuilds the index from partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails for any batch.
    pub fn rebuild(&self, partitions: &[Vec<RecordBatch>]) -> Result<()> {
        self.clear();

        for (partition_idx, partition) in partitions.iter().enumerate() {
            for (batch_idx, batch) in partition.iter().enumerate() {
                if batch.num_rows() == 0 {
                    continue;
                }

                let extractor = create_key_extractor(batch, &self.key_columns)?;

                for row in 0..extractor.len() {
                    let Some(hash) = extractor.hash_key(row) else {
                        continue;
                    };

                    let partition_u32 =
                        u32::try_from(partition_idx)
                            .ok()
                            .context(IndexOverflowSnafu {
                                context: "partition",
                                value: partition_idx,
                            })?;
                    let batch_u32 = u32::try_from(batch_idx).ok().context(IndexOverflowSnafu {
                        context: "batch",
                        value: batch_idx,
                    })?;
                    let row_u32 = u32::try_from(row).ok().context(IndexOverflowSnafu {
                        context: "row",
                        value: row,
                    })?;

                    let location = RowLocation::new(partition_u32, batch_u32, row_u32);

                    self.insert_or_replace(hash, location);
                }
            }
        }

        Ok(())
    }

    /// Adds entries from new batches.
    ///
    /// # Errors
    ///
    /// Returns an error if key extraction fails for any batch, or if indices overflow u32.
    pub fn add_batches(
        &self,
        partition_idx: u32,
        starting_batch_idx: u32,
        batches: &[RecordBatch],
    ) -> Result<()> {
        for (batch_offset, batch) in batches.iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }

            let extractor = create_key_extractor(batch, &self.key_columns)?;
            let batch_offset_u32 =
                u32::try_from(batch_offset)
                    .ok()
                    .context(IndexOverflowSnafu {
                        context: "batch_offset",
                        value: batch_offset,
                    })?;
            let batch_idx =
                starting_batch_idx
                    .checked_add(batch_offset_u32)
                    .context(IndexOverflowSnafu {
                        context: "batch",
                        value: starting_batch_idx as usize + batch_offset,
                    })?;

            for row in 0..extractor.len() {
                let Some(hash) = extractor.hash_key(row) else {
                    continue;
                };

                let row_u32 = u32::try_from(row).ok().context(IndexOverflowSnafu {
                    context: "row",
                    value: row,
                })?;

                let location = RowLocation::new(partition_idx, batch_idx, row_u32);

                self.insert_or_replace(hash, location);
            }
        }

        Ok(())
    }
}

// Safety: All internal state is either atomic or protected by locks
unsafe impl Send for HashIndex {}
unsafe impl Sync for HashIndex {}

impl Clone for HashIndex {
    fn clone(&self) -> Self {
        let shards: Vec<Shard> = self
            .shards
            .iter()
            .map(|s| {
                let table = s.table.read();
                Shard {
                    table: RwLock::new(ShardTable {
                        slots: table.slots.clone(),
                        mask: table.mask,
                        len: table.len,
                    }),
                }
            })
            .collect();

        Self {
            shards: shards.try_into().unwrap_or_else(|_| unreachable!()),
            len: AtomicUsize::new(self.len.load(Ordering::Relaxed)),
            key_columns: self.key_columns.clone(),
            bloom: self.bloom.as_ref().map(|b| RwLock::new(b.read().clone())),
        }
    }
}

impl std::fmt::Debug for HashIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashIndex")
            .field("len", &self.len())
            .field("key_columns", &self.key_columns)
            .field("bloom_filter", &self.bloom.is_some())
            .finish_non_exhaustive()
    }
}
