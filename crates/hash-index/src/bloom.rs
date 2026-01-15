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

//! Bloom filter implementation for fast negative lookups.
//!
//! This module provides a simple but efficient bloom filter that can be used
//! to quickly determine if a key is definitely NOT in the index, avoiding
//! more expensive hash table probes.
//!
//! # Design
//!
//! The bloom filter uses:
//! - Multiple hash functions derived from a single 64-bit hash
//! - Block-aligned bit storage for cache efficiency
//! - SIMD-friendly bit manipulation where possible
//!
//! # False Positive Rate
//!
//! For a bloom filter with `m` bits and `k` hash functions, with `n` items:
//! - FPR ≈ (1 - e^(-kn/m))^k
//! - With 10 bits per item and k=7: FPR ≈ 0.82%
//! - With 8 bits per item and k=5: FPR ≈ 2.2%
//!
//! We default to 10 bits per item with k=7 hash functions.

/// Default number of bits per item for the bloom filter.
const DEFAULT_BITS_PER_ITEM: usize = 10;

/// Default number of hash functions.
const DEFAULT_NUM_HASHES: usize = 7;

/// A space-efficient bloom filter for fast negative lookups.
///
/// The bloom filter provides O(1) lookup with a small false positive rate.
/// If `might_contain` returns `false`, the item is definitely not in the set.
/// If it returns `true`, the item might be in the set (check the hash table).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit storage (using u64 blocks for efficiency).
    bits: Vec<u64>,
    /// Number of hash functions to use.
    num_hashes: usize,
    /// Total number of bits (for masking).
    num_bits: usize,
}

impl BloomFilter {
    /// Creates a new bloom filter sized for the expected number of items.
    ///
    /// Uses default parameters (10 bits/item, 7 hash functions) for ~0.82% FPR.
    #[must_use]
    pub fn new(expected_items: usize) -> Self {
        Self::with_params(expected_items, DEFAULT_BITS_PER_ITEM, DEFAULT_NUM_HASHES)
    }

    /// Creates a new bloom filter with custom parameters.
    ///
    /// # Parameters
    /// - `expected_items`: Expected number of items to insert
    /// - `bits_per_item`: Number of bits per item (higher = lower FPR, more memory)
    /// - `num_hashes`: Number of hash functions (optimal ≈ 0.693 × `bits_per_item`)
    #[must_use]
    pub fn with_params(expected_items: usize, bits_per_item: usize, num_hashes: usize) -> Self {
        let num_bits = (expected_items * bits_per_item).max(64);
        // Round up to next multiple of 64 for block alignment
        let num_bits = (num_bits + 63) & !63;
        let num_blocks = num_bits / 64;

        Self {
            bits: vec![0; num_blocks],
            num_hashes,
            num_bits,
        }
    }

    /// Creates an empty bloom filter with zero capacity.
    ///
    /// This is useful as a placeholder; it will always return `true` from `might_contain`.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bits: Vec::new(),
            num_hashes: 0,
            num_bits: 0,
        }
    }

    /// Returns true if the bloom filter is empty (has no capacity).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    /// Returns the number of bits in the bloom filter.
    #[must_use]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Returns the estimated memory usage in bytes.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        self.bits.len() * 8
    }

    /// Inserts a hash into the bloom filter.
    #[inline]
    pub fn insert(&mut self, hash: u64) {
        if self.bits.is_empty() {
            return;
        }

        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(hash, i);
            let block_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            self.bits[block_idx] |= 1 << bit_offset;
        }
    }

    /// Checks if a hash might be in the bloom filter.
    ///
    /// Returns `false` if the item is definitely not in the set.
    /// Returns `true` if the item might be in the set (possible false positive).
    #[inline]
    #[must_use]
    pub fn might_contain(&self, hash: u64) -> bool {
        if self.bits.is_empty() {
            // Empty bloom filter - always return true to fall through to hash table
            return true;
        }

        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(hash, i);
            let block_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            if self.bits[block_idx] & (1 << bit_offset) == 0 {
                return false;
            }
        }

        true
    }

    /// Clears all bits in the bloom filter.
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }

    /// Computes the bit index for the i-th hash function.
    ///
    /// Uses double hashing: h(i) = h1 + i * h2
    /// where h1 = low 32 bits and h2 = high 32 bits of the hash.
    #[inline]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "intentional truncation to 32 bits"
    )]
    fn get_bit_index(&self, hash: u64, i: usize) -> usize {
        // Truncation is intentional - we only need the low/high 32 bits
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;
        let combined = h1.wrapping_add((i as u32).wrapping_mul(h2));
        (combined as usize) % self.num_bits
    }
}

/// A bloom filter optimized for batch operations.
///
/// This variant pre-computes bit indices for faster batch checking.
#[derive(Debug, Clone)]
pub struct BatchBloomFilter {
    inner: BloomFilter,
}

impl BatchBloomFilter {
    /// Creates a new batch bloom filter.
    #[must_use]
    pub fn new(expected_items: usize) -> Self {
        Self {
            inner: BloomFilter::new(expected_items),
        }
    }

    /// Inserts multiple hashes into the bloom filter.
    pub fn insert_batch(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            self.inner.insert(hash);
        }
    }

    /// Checks multiple hashes against the bloom filter.
    ///
    /// Returns a vector of booleans indicating which hashes might be present.
    #[must_use]
    pub fn might_contain_batch(&self, hashes: &[u64]) -> Vec<bool> {
        hashes
            .iter()
            .map(|&h| self.inner.might_contain(h))
            .collect()
    }

    /// Returns hashes that might be in the filter (filters out definite negatives).
    ///
    /// This is useful for batch lookups where you want to skip definitely-missing keys.
    #[must_use]
    pub fn filter_candidates(&self, hashes: &[u64]) -> Vec<u64> {
        hashes
            .iter()
            .copied()
            .filter(|&h| self.inner.might_contain(h))
            .collect()
    }

    /// Provides access to the underlying bloom filter.
    #[must_use]
    pub fn inner(&self) -> &BloomFilter {
        &self.inner
    }

    /// Provides mutable access to the underlying bloom filter.
    pub fn inner_mut(&mut self) -> &mut BloomFilter {
        &mut self.inner
    }
}

impl From<BloomFilter> for BatchBloomFilter {
    fn from(inner: BloomFilter) -> Self {
        Self { inner }
    }
}

#[cfg(test)]
#[expect(
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::needless_range_loop
)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_lookup() {
        let mut bloom = BloomFilter::new(100);

        // Insert some hashes
        bloom.insert(12345);
        bloom.insert(67890);
        bloom.insert(11111);

        // Inserted items should be found
        assert!(bloom.might_contain(12345));
        assert!(bloom.might_contain(67890));
        assert!(bloom.might_contain(11111));
    }

    #[test]
    fn test_definite_negatives() {
        let mut bloom = BloomFilter::new(100);

        // Insert specific hashes
        bloom.insert(1);
        bloom.insert(2);
        bloom.insert(3);

        // Count false positives for unseen items
        let mut false_positives = 0;
        for i in 1000..2000 {
            if bloom.might_contain(i) {
                false_positives += 1;
            }
        }

        // With 100 items capacity and only 3 inserted,
        // false positive rate should be very low
        assert!(
            false_positives < 50,
            "Too many false positives: {false_positives}"
        );
    }

    #[test]
    fn test_false_positive_rate() {
        let n = 10_000;
        let mut bloom = BloomFilter::new(n);

        // Insert n items
        for i in 0..n as u64 {
            bloom.insert(i * 2); // Even numbers only
        }

        // Check false positive rate for odd numbers
        let mut false_positives = 0;
        let test_count = 10_000;
        for i in 0..test_count {
            let odd = (i * 2 + 1) as u64;
            if bloom.might_contain(odd) {
                false_positives += 1;
            }
        }

        let fpr = (false_positives as f64) / (test_count as f64) * 100.0;

        // With 10 bits/item and 7 hashes, FPR should be ~0.82%
        // Allow some margin for randomness
        assert!(fpr < 2.0, "FPR too high: {fpr:.2}%");
    }

    #[test]
    fn test_empty_bloom_filter() {
        let bloom = BloomFilter::empty();

        // Empty bloom filter should always return true
        assert!(bloom.might_contain(12345));
        assert!(bloom.might_contain(0));
        assert!(bloom.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut bloom = BloomFilter::new(100);

        bloom.insert(12345);
        assert!(bloom.might_contain(12345));

        bloom.clear();

        // After clear, might_contain should return false for everything
        // (unless it's a false positive, but 12345 was specifically inserted before)
        // Note: might_contain could still return true due to other bit patterns
        // but for a cleared bloom filter with nothing inserted, it should be false
        let mut found_any = false;
        for i in 0..100 {
            if bloom.might_contain(i) {
                found_any = true;
                break;
            }
        }
        // Cleared bloom filter should not find anything
        assert!(!found_any);
    }

    #[test]
    fn test_batch_operations() {
        let mut batch_bloom = BatchBloomFilter::new(100);

        let hashes: Vec<u64> = (0..50).collect();
        batch_bloom.insert_batch(&hashes);

        // Check batch lookup
        let check_hashes: Vec<u64> = (0..100).collect();
        let results = batch_bloom.might_contain_batch(&check_hashes);

        // First 50 should definitely be true
        for i in 0..50 {
            assert!(results[i], "Hash {i} should be found");
        }
    }

    #[test]
    fn test_filter_candidates() {
        let mut batch_bloom = BatchBloomFilter::new(100);

        // Insert even numbers
        let hashes: Vec<u64> = (0..50).map(|i| i * 2).collect();
        batch_bloom.insert_batch(&hashes);

        // Filter candidates should include all inserted items
        let candidates = batch_bloom.filter_candidates(&[0, 1, 2, 3, 4, 5]);

        // Even numbers should definitely be in candidates
        assert!(candidates.contains(&0));
        assert!(candidates.contains(&2));
        assert!(candidates.contains(&4));
    }

    #[test]
    fn test_memory_usage() {
        let bloom = BloomFilter::new(1000);

        // 1000 items * 10 bits = 10,000 bits = 1,250 bytes
        // Rounded up to 64-bit blocks = ~1,280 bytes
        let usage = bloom.memory_usage();
        assert!(usage >= 1250, "Memory usage too low: {usage}");
        assert!(usage <= 2000, "Memory usage too high: {usage}");
    }
}
