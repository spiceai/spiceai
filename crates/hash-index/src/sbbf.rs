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

//! Split-block bloom filter for cache-efficient negative lookups.
//!
//! A split-block bloom filter (SBBF — the design used by Apache Parquet and
//! Impala) confines every key to a single 256-bit block: the high 32 bits of
//! the key's hash select the block and the low 32 bits derive eight bit
//! positions inside it, one per 32-bit word. A probe therefore touches exactly
//! one 32-byte, cache-line-aligned block — at most a single cache miss — where
//! a classic bloom filter with `k` hash functions ([`BloomFilter`]) takes up to
//! `k` dependent cache misses per probe.
//!
//! [`BloomFilter`]: crate::BloomFilter
//!
//! # Concurrency
//!
//! [`insert`](SplitBlockBloomFilter::insert) takes `&self`: words are
//! [`AtomicU32`] and bits are set with relaxed `fetch_or`. This lets a writer
//! extend a filter that is concurrently probed by readers pinning older
//! snapshots of the indexed data. A reader that observes bits from a newer
//! generation sees a superset of its own generation's keys, which can only add
//! false positives — never false negatives — so probes stay correct without
//! locking or copy-on-write of the bit array. Relaxed atomic loads compile to
//! plain loads on x86-64 and aarch64, so the probe path costs the same as a
//! non-atomic implementation.
//!
//! # False positive rate
//!
//! Sized at 16 bits per item ([`SplitBlockBloomFilter::new`]), the FPR at
//! design capacity is ≈0.04%. Unlike a classic bloom filter the FPR degrades
//! steeply once fill exceeds capacity, so callers should size for projected
//! growth and rebuild before the item count crosses the sized capacity.

use std::array;
use std::sync::atomic::{AtomicU32, Ordering};

/// Per-word salt constants from the Parquet split-block bloom filter spec.
/// Each odd constant maps the low 32 hash bits to an independent bit position
/// within one 32-bit word of the block.
const SALT: [u32; 8] = [
    0x47b6_137b,
    0x4497_4d91,
    0x8824_ad5b,
    0xa2b7_289d,
    0x7054_95c7,
    0x2df1_424b,
    0x9efc_4947,
    0x5c6b_fb31,
];

/// Bits allocated per expected item. 16 bits/item gives ≈0.04% FPR at design
/// capacity — comfortably below the classic filter's 0.82% at 10 bits/item —
/// while keeping the filter ~2 bytes per key, negligible next to the indexes
/// it fronts.
const BITS_PER_ITEM: usize = 16;

/// One 256-bit filter block. `align(32)` keeps a block from straddling two
/// cache lines, preserving the one-miss-per-probe property.
#[derive(Debug, Default)]
#[repr(align(32))]
struct Block([AtomicU32; 8]);

/// A cache-efficient split-block bloom filter with lock-free concurrent
/// inserts.
///
/// If [`might_contain`](Self::might_contain) returns `false`, the item is
/// definitely not in the set. If it returns `true`, the item might be (check
/// the backing structure). See the [module docs](self) for the block layout
/// and concurrency contract.
#[derive(Debug)]
pub struct SplitBlockBloomFilter {
    blocks: Vec<Block>,
}

impl SplitBlockBloomFilter {
    /// Creates a filter sized for `expected_items` at 16 bits per item.
    ///
    /// Always allocates at least one block, so the filter is usable (and
    /// rejects misses) even when sized for zero items.
    #[must_use]
    pub fn new(expected_items: usize) -> Self {
        let num_blocks = expected_items
            .saturating_mul(BITS_PER_ITEM)
            .div_ceil(256)
            .max(1);
        Self {
            blocks: (0..num_blocks).map(|_| Block::default()).collect(),
        }
    }

    /// Number of items this filter was sized for at the design FPR.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.blocks.len() * 256 / BITS_PER_ITEM
    }

    /// Memory used by the bit array, in bytes.
    #[must_use]
    pub fn memory_usage_bytes(&self) -> usize {
        self.blocks.len() * size_of::<Block>()
    }

    /// Selects the block for `hash` via multiply-shift range reduction over
    /// the high 32 bits, leaving the low 32 bits independent for the in-block
    /// bit positions. The multiply widens to `u128` so block counts beyond
    /// `2^32` (a >128 GiB filter) cannot overflow the product; the double
    /// shift keeps the result within block range, so the cast cannot truncate.
    #[inline]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "range reduction bounds the result to the block count"
    )]
    fn block_index(&self, hash: u64) -> usize {
        ((u128::from(hash >> 32) * self.blocks.len() as u128) >> 32) as usize
    }

    /// Computes the eight per-word bit masks for `hash` from its low 32 bits.
    #[inline]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "intentional truncation to the low 32 bits"
    )]
    fn masks(hash: u64) -> [u32; 8] {
        let h = hash as u32;
        array::from_fn(|i| 1_u32 << (h.wrapping_mul(SALT[i]) >> 27))
    }

    /// Inserts a hash into the filter.
    ///
    /// Takes `&self`: bits are set with relaxed atomic `fetch_or`, so a writer
    /// may insert while readers probe concurrently (readers observe a safe
    /// superset; see the [module docs](self)). Callers that require a reader
    /// to see the bits for a specific key must publish the surrounding
    /// structure with release/acquire ordering (e.g. `ArcSwap::store`), as the
    /// deletion index does.
    #[inline]
    pub fn insert(&self, hash: u64) {
        let block = &self.blocks[self.block_index(hash)];
        let masks = Self::masks(hash);
        for (word, mask) in block.0.iter().zip(masks) {
            word.fetch_or(mask, Ordering::Relaxed);
        }
    }

    /// Checks whether a hash might be in the filter.
    ///
    /// Returns `false` if the item is definitely not present; `true` if it
    /// might be (possible false positive). Touches exactly one 32-byte block.
    #[inline]
    #[must_use]
    pub fn might_contain(&self, hash: u64) -> bool {
        let block = &self.blocks[self.block_index(hash)];
        let masks = Self::masks(hash);
        block
            .0
            .iter()
            .zip(masks)
            .all(|(word, mask)| word.load(Ordering::Relaxed) & mask == mask)
    }
}

impl Clone for SplitBlockBloomFilter {
    /// Snapshots the bit array with relaxed loads. Cloning concurrently with
    /// inserts yields some consistent superset/subset of in-flight bits, which
    /// is safe for bloom semantics (per-bit monotonicity); clones intended to
    /// capture specific keys must be taken after those inserts are published.
    fn clone(&self) -> Self {
        Self {
            blocks: self
                .blocks
                .iter()
                .map(|block| {
                    Block(array::from_fn(|i| {
                        AtomicU32::new(block.0[i].load(Ordering::Relaxed))
                    }))
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash_key;

    #[test]
    fn insert_then_contains() {
        let filter = SplitBlockBloomFilter::new(100);
        for key in [12345_i64, 67890, 11111, -1, 0, i64::MAX] {
            filter.insert(hash_key(&key));
        }
        for key in [12345_i64, 67890, 11111, -1, 0, i64::MAX] {
            assert!(filter.might_contain(hash_key(&key)), "missing key {key}");
        }
    }

    #[test]
    fn no_false_negatives_even_when_overfilled() {
        // 4x over design capacity: FPR degrades but false negatives must not occur.
        let filter = SplitBlockBloomFilter::new(1_000);
        for key in 0_i64..4_000 {
            filter.insert(hash_key(&key));
        }
        for key in 0_i64..4_000 {
            assert!(filter.might_contain(hash_key(&key)), "missing key {key}");
        }
    }

    #[test]
    fn false_positive_rate_at_capacity() {
        let n = 100_000_i64;
        #[expect(clippy::cast_sign_loss, reason = "n is positive")]
        let filter = SplitBlockBloomFilter::new(n as usize);
        for key in 0..n {
            filter.insert(hash_key(&key));
        }

        let mut false_positives = 0_u32;
        for key in n..(2 * n) {
            if filter.might_contain(hash_key(&key)) {
                false_positives += 1;
            }
        }
        // Design FPR at 16 bits/item is ≈0.04%; allow generous margin.
        let fpr = f64::from(false_positives) / 1_000.0; // percent of 100k probes
        assert!(fpr < 0.5, "FPR too high at design capacity: {fpr:.3}%");
    }

    #[test]
    fn zero_capacity_is_usable() {
        let filter = SplitBlockBloomFilter::new(0);
        assert!(filter.capacity() >= 1);
        filter.insert(hash_key(&42_i64));
        assert!(filter.might_contain(hash_key(&42_i64)));
        assert!(!filter.might_contain(hash_key(&43_i64)));
    }

    #[test]
    fn clone_snapshots_bits() {
        let filter = SplitBlockBloomFilter::new(100);
        filter.insert(hash_key(&1_i64));
        let snapshot = filter.clone();
        filter.insert(hash_key(&2_i64));

        assert!(snapshot.might_contain(hash_key(&1_i64)));
        assert!(filter.might_contain(hash_key(&2_i64)));
        assert!(!snapshot.might_contain(hash_key(&2_i64)));
    }

    #[test]
    fn shared_insert_is_a_superset_for_old_readers() {
        // Simulates the deletion-index generation model: an old reader keeps
        // probing the same filter while a writer inserts new keys. Old keys
        // stay present (no false negatives ever).
        let filter = std::sync::Arc::new(SplitBlockBloomFilter::new(1_000));
        for key in 0_i64..500 {
            filter.insert(hash_key(&key));
        }
        let reader = std::sync::Arc::clone(&filter);
        for key in 500_i64..1_000 {
            filter.insert(hash_key(&key));
        }
        for key in 0_i64..500 {
            assert!(reader.might_contain(hash_key(&key)), "missing key {key}");
        }
    }

    #[test]
    fn memory_usage_matches_sizing() {
        let filter = SplitBlockBloomFilter::new(1_000);
        // 1000 items * 16 bits = 16,000 bits = 2,000 bytes, rounded up to
        // 32-byte blocks.
        let usage = filter.memory_usage_bytes();
        assert!(usage >= 2_000, "memory usage too low: {usage}");
        assert!(usage <= 2_048, "memory usage too high: {usage}");
    }
}
