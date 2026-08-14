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

//! Candidate approximate-membership filters for the primary-key existence
//! index, measured against the shipping [`PkBloom`](super::pk_index::PkBloom)
//! in `benches/pk_filter.rs`.
//!
//! Nothing here is wired into a query or write path yet. The point is to
//! measure, at the shapes Cayenne actually uses, before choosing.
//!
//! # The two call sites want different filters
//!
//! * The **resident PK existence index** is grown key-by-key by the CDC apply
//!   and probed on every upsert, so it must accept inserts after construction.
//!   That rules out the static constructions (ribbon, XOR, binary fuse), which
//!   solve for the whole key set at build time. [`BlockedBloom`](BlockedBloom) is the
//!   candidate here: same "insert then probe" contract as today's bloom, but
//!   every probe of one key lands in a single cache line.
//! * The **per-file cold-tier blooms** are built once at promotion from a known
//!   key set, serialized into the manifest, and thereafter only probed. A
//!   static filter fits exactly, and buys roughly a third of the space at the
//!   same false-positive rate. Not implemented yet — the benchmark's build/probe
//!   lanes are shaped so one can be dropped in beside these.
//!
//! # Why a blocked bloom might win
//!
//! A classic bloom scatters its `k` probes across the whole bit array, so a
//! filter larger than L2 costs up to `k` cache misses per operation. A blocked
//! bloom hashes once to choose a block the size of a cache line, then sets all
//! `k` bits inside it: one miss per operation regardless of `k`, at the cost of
//! a slightly worse false-positive rate for the same bits/key, because keys
//! landing in an unlucky block share its bits.

use twox_hash::XxHash3_64;

/// Block width is a measured variable, not a constant: a "cache line" is 64
/// bytes on x86-64 and 128 on Apple silicon, so the same 512-bit block is a
/// whole line on one and half a line on the other. The benchmark instantiates
/// 256, 512 and 1024 bits and lets the host answer.

/// Probes per key. Matches the shipping bloom's `PK_BLOOM_NUM_HASHES`, so the
/// benchmark compares layouts rather than two different accuracy targets.
const NUM_PROBES: u32 = 7;

/// One XXH3-64 of the key, split into the values the probes need.
///
/// XXH3 reads the key in wide chunks, where the shipping filter's FNV-1a walks
/// it one byte at a time with a dependent multiply per byte. For a 16-byte
/// composite key that is the difference between a couple of instructions and a
/// serial chain of thirty-two.
///
/// Uses the ONE-SHOT entry point, not the streaming `Hasher` trait. For a
/// 16-byte key the streaming path's buffer management costs more than the hash
/// itself and more than the two FNV passes it is meant to replace — measured at
/// ~20 ns/probe versus ~12 ns for the shipping filter, which inverts the result
/// this arm exists to establish.
#[inline]
fn hash_key(key: &[u8]) -> u64 {
    XxHash3_64::oneshot(key)
}

/// A cache-line-blocked bloom filter over primary keys.
///
/// Same contract as the shipping [`PkBloom`](super::pk_index::PkBloom) — insert
/// any time, no removal, no false negatives — with the probes for one key
/// confined to a single 512-bit block.
#[derive(Debug, Clone)]
pub struct BlockedBloom<const BLOCK_BITS: usize> {
    /// `num_blocks * Self::BLOCK_WORDS` words, one block per `BLOCK_BITS`.
    words: Vec<u64>,
    /// `num_blocks - 1`; the block count is a power of two so selection masks.
    block_mask: u64,
    inserted_keys: usize,
}

impl<const BLOCK_BITS: usize> BlockedBloom<BLOCK_BITS> {
    const BLOCK_WORDS: usize = BLOCK_BITS / 64;
    /// Allocate the largest power-of-two block count whose bits fit `target_bits`
    /// (minimum one block), matching `PkBloom::with_num_bits_pow2`'s sizing rule
    /// so the two are compared at equal memory.
    #[must_use]
    pub fn with_num_bits_pow2(target_bits: usize) -> Self {
        let want_blocks = (target_bits / BLOCK_BITS).max(1);
        let num_blocks = 1usize << want_blocks.ilog2();
        Self {
            words: vec![0u64; num_blocks * Self::BLOCK_WORDS],
            block_mask: u64::try_from(num_blocks - 1).unwrap_or(0),
            inserted_keys: 0,
        }
    }

    /// Right-size for `expected_keys` at ~10 bits/key, capped at `max_bytes`.
    #[must_use]
    pub fn with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        let want_bits = expected_keys.saturating_mul(10);
        let cap_bits = max_bytes.saturating_mul(8).max(BLOCK_BITS);
        Self::with_num_bits_pow2(want_bits.min(cap_bits))
    }

    /// The block a key belongs to, and the per-probe bit positions within it.
    ///
    /// The block index comes from the high bits and the probe positions from the
    /// low bits, so two keys sharing a block still scatter inside it.
    #[inline]
    fn locate(&self, key: &[u8]) -> (usize, [u32; NUM_PROBES as usize]) {
        let hash = hash_key(key);
        let block = usize::try_from((hash >> 32) & self.block_mask).unwrap_or(0);
        // Two independent-enough streams from the low half, combined
        // Kirsch-Mitzenmacher style, then taken modulo the block's bit count.
        let h1 = hash as u32;
        let h2 = ((hash >> 16) as u32) | 1;
        let mut bits = [0u32; NUM_PROBES as usize];
        for (i, slot) in bits.iter_mut().enumerate() {
            let i = u32::try_from(i).unwrap_or(0);
            *slot = h1.wrapping_add(i.wrapping_mul(h2)) % u32::try_from(BLOCK_BITS).unwrap_or(512);
        }
        (block, bits)
    }

    /// Record `key`. Idempotent, and never removable — the no-false-negative
    /// guarantee the upsert path depends on holds only while that stays true.
    pub fn insert(&mut self, key: &[u8]) {
        let (block, bits) = self.locate(key);
        let base = block * Self::BLOCK_WORDS;
        for bit in bits {
            let word = base + usize::try_from(bit >> 6).unwrap_or(0);
            self.words[word] |= 1u64 << (bit & 63);
        }
        self.inserted_keys = self.inserted_keys.saturating_add(1);
    }

    /// Whether `key` may have been inserted. False positives are possible;
    /// false negatives are not.
    #[must_use]
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let (block, bits) = self.locate(key);
        let base = block * Self::BLOCK_WORDS;
        for bit in bits {
            let word = base + usize::try_from(bit >> 6).unwrap_or(0);
            if self.words[word] & (1u64 << (bit & 63)) == 0 {
                return false;
            }
        }
        true
    }

    /// Resident bytes of the bit array, for the size comparison.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.words.len() * 8
    }

    /// Keys inserted so far, for observability and FPR estimation.
    #[must_use]
    pub fn inserted_keys(&self) -> usize {
        self.inserted_keys
    }
}

/// Split-block bloom filter (Putze, Sanders & Singler; the design Impala uses
/// and the Parquet spec standardises).
///
/// A block is eight `u32` lanes — 256 bits — and a key sets **exactly one bit in
/// each lane**, the lane's bit chosen by multiplying the key's low word by that
/// lane's odd salt and taking the top five bits of the product. That shape is
/// what makes it the SIMD candidate:
///
/// * the eight lane masks are one vector multiply, one shift and one shift-left
///   — `vpmulld`/`vpsrld`/`vpsllvd` on AVX2, two 128-bit `mul`/`ushl` pairs on
///   NEON — with no dependency between lanes;
/// * a probe is `(block & mask) == mask` over the whole vector, so there is no
///   per-probe branch to mispredict, unlike the early-exit loop the other arms
///   run;
/// * 256 bits is 32 bytes, so a block never straddles a cache line on either a
///   64- or a 128-byte line.
///
/// Written as plain array code rather than intrinsics: LLVM vectorises the
/// fixed-length loops, and it stays portable across the x86-64 CI hosts and the
/// aarch64 development ones. Whether it actually vectorises here is the point of
/// measuring it.
///
/// One structural difference from the other arms: it sets eight bits, not seven,
/// because one per lane is what the construction is. The size/FPR report is what
/// makes that comparable.
#[derive(Debug, Clone)]
pub struct SplitBlockBloom {
    blocks: Vec<[u32; 8]>,
    block_mask: u64,
    inserted_keys: usize,
}

/// The Parquet/Impala salts: eight odd constants with well-spread bit patterns,
/// so the lanes' chosen bits are independent enough.
const SPLIT_BLOCK_SALT: [u32; 8] = [
    0x47b6_137b,
    0x4497_4d91,
    0x8824_ad5b,
    0xa2b7_289d,
    0x7054_95c7,
    0x2df1_424b,
    0x9efc_4947,
    0x5c6b_fb31,
];

impl SplitBlockBloom {
    /// Allocate the largest power-of-two block count whose bits fit
    /// `target_bits`, matching the other arms' sizing rule.
    #[must_use]
    pub fn with_num_bits_pow2(target_bits: usize) -> Self {
        let want_blocks = (target_bits / 256).max(1);
        let num_blocks = 1usize << want_blocks.ilog2();
        Self {
            blocks: vec![[0u32; 8]; num_blocks],
            block_mask: u64::try_from(num_blocks - 1).unwrap_or(0),
            inserted_keys: 0,
        }
    }

    /// Right-size for `expected_keys` at ~10 bits/key, capped at `max_bytes`.
    #[must_use]
    pub fn with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        let want_bits = expected_keys.saturating_mul(10);
        let cap_bits = max_bytes.saturating_mul(8).max(256);
        Self::with_num_bits_pow2(want_bits.min(cap_bits))
    }

    /// The eight per-lane masks for a key. Fixed length and branch-free, which
    /// is what lets this compile to vector instructions.
    #[inline]
    fn lane_masks(key_low: u32) -> [u32; 8] {
        let mut masks = [0u32; 8];
        for (mask, salt) in masks.iter_mut().zip(SPLIT_BLOCK_SALT) {
            // Top five bits of the product pick one of the lane's 32 bits.
            *mask = 1u32 << (key_low.wrapping_mul(salt) >> 27);
        }
        masks
    }

    #[inline]
    fn block_of(&self, hash: u64) -> usize {
        usize::try_from((hash >> 32) & self.block_mask).unwrap_or(0)
    }

    /// Record `key`.
    pub fn insert(&mut self, key: &[u8]) {
        let hash = hash_key(key);
        let index = self.block_of(hash);
        let masks = Self::lane_masks(hash as u32);
        let block = &mut self.blocks[index];
        for (lane, mask) in block.iter_mut().zip(masks) {
            *lane |= mask;
        }
        self.inserted_keys = self.inserted_keys.saturating_add(1);
    }

    /// Whether `key` may have been inserted.
    ///
    /// Folds over all eight lanes rather than exiting on the first miss: the
    /// branch costs more than the seven remaining ANDs, and the fold is what
    /// vectorises.
    #[must_use]
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let hash = hash_key(key);
        let index = self.block_of(hash);
        let masks = Self::lane_masks(hash as u32);
        let block = &self.blocks[index];
        let mut present = true;
        for (lane, mask) in block.iter().zip(masks) {
            present &= (*lane & mask) == mask;
        }
        present
    }

    /// Resident bytes of the block array.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.blocks.len() * 32
    }
}

/// The shipping bloom's structure with XXH3 in place of FNV-1a.
///
/// A control, not a proposal: it isolates how much of any measured difference
/// is the hash function and how much is the memory layout. Without it, a
/// blocked-bloom win could be entirely the hash.
#[derive(Debug, Clone)]
pub struct ScatteredBloomXxh3 {
    bits: Vec<u64>,
    bit_mask: u64,
    inserted_keys: usize,
}

impl ScatteredBloomXxh3 {
    /// Allocate the largest power-of-two bit count `<= target_bits`, matching
    /// the shipping filter's sizing exactly so only the hash differs.
    #[must_use]
    pub fn with_num_bits_pow2(target_bits: usize) -> Self {
        let num_bits: usize = 1usize << target_bits.max(64).ilog2();
        let words = (num_bits / 64).max(1);
        Self {
            bits: vec![0u64; words],
            bit_mask: u64::try_from(num_bits.saturating_sub(1)).unwrap_or(u64::MAX),
            inserted_keys: 0,
        }
    }

    #[inline]
    fn probe_bits(key: &[u8]) -> impl Iterator<Item = u64> {
        let hash = hash_key(key);
        let h1 = hash;
        // Force odd so successive probes stride across the whole bit space,
        // exactly as the shipping filter does.
        let h2 = hash.rotate_left(32) | 1;
        (0..NUM_PROBES).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)))
    }

    /// Record `key`; see [`BlockedBloom::insert`].
    pub fn insert(&mut self, key: &[u8]) {
        for hash in Self::probe_bits(key) {
            let bit = hash & self.bit_mask;
            let word = usize::try_from(bit >> 6).unwrap_or(0);
            self.bits[word] |= 1u64 << (bit & 63);
        }
        self.inserted_keys = self.inserted_keys.saturating_add(1);
    }

    /// Whether `key` may have been inserted; see [`BlockedBloom::maybe_contains`].
    #[must_use]
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        for hash in Self::probe_bits(key) {
            let bit = hash & self.bit_mask;
            let word = usize::try_from(bit >> 6).unwrap_or(0);
            if self.bits[word] & (1u64 << (bit & 63)) == 0 {
                return false;
            }
        }
        true
    }

    /// Resident bytes of the bit array.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }
}

/// The shipping [`PkBloom`](super::pk_index::PkBloom), wrapped so the benchmark
/// measures the filter that is actually in production rather than a copy of it
/// that can drift. A wrapper rather than widening `pk_index`'s visibility: the
/// baseline is measured, not exported.
pub struct ShippingBloom(super::pk_index::PkBloom);

impl ShippingBloom {
    /// Allocate the shipping filter at the largest power-of-two bit count
    /// `<= target_bits`.
    #[must_use]
    pub fn with_num_bits_pow2(target_bits: usize) -> Self {
        Self(super::pk_index::PkBloom::with_num_bits_pow2(target_bits))
    }

    /// Right-size the shipping filter for `expected_keys`, capped at `max_bytes`.
    #[must_use]
    pub fn with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        Self(super::pk_index::PkBloom::with_expected_keys(
            expected_keys,
            max_bytes,
        ))
    }

    /// Record `key` in the shipping filter.
    pub fn insert(&mut self, key: &[u8]) {
        self.0.insert(key);
    }

    /// Whether `key` may have been inserted, per the shipping filter.
    #[must_use]
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        self.0.maybe_contains(key)
    }

    /// Resident bytes of the shipping filter's bit array.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.0.bits.len() * 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one invariant the upsert path depends on: an inserted key must never
    /// probe as absent, or a real conflict is missed and a duplicate live row is
    /// written.
    #[test]
    fn blocked_bloom_has_no_false_negatives() {
        let keys: Vec<[u8; 16]> = (0..50_000u128).map(u128::to_le_bytes).collect();
        let mut filter = BlockedBloom::<512>::with_expected_keys(keys.len(), usize::MAX);
        for key in &keys {
            filter.insert(key);
        }
        for key in &keys {
            assert!(
                filter.maybe_contains(key),
                "inserted key probed as absent -- a false negative breaks upsert conflict detection"
            );
        }
        assert_eq!(filter.inserted_keys(), keys.len());
    }

    #[test]
    fn scattered_xxh3_has_no_false_negatives() {
        let keys: Vec<[u8; 16]> = (0..50_000u128).map(u128::to_le_bytes).collect();
        let mut filter = ScatteredBloomXxh3::with_num_bits_pow2(keys.len() * 10);
        for key in &keys {
            filter.insert(key);
        }
        for key in &keys {
            assert!(filter.maybe_contains(key), "false negative");
        }
    }

    /// A blocked bloom trades some accuracy for locality. The test is that the
    /// trade is bounded *relative to the scattered filter at the same size* —
    /// an absolute rate would only be measuring the sizing rule below, which
    /// dominates it.
    #[test]
    fn blocked_bloom_false_positive_rate_stays_near_the_scattered_one() {
        let inserted: Vec<[u8; 16]> = (0..100_000u128).map(u128::to_le_bytes).collect();
        let bits = inserted.len() * 10;
        let mut blocked = BlockedBloom::<512>::with_num_bits_pow2(bits);
        let mut scattered = ScatteredBloomXxh3::with_num_bits_pow2(bits);
        for key in &inserted {
            blocked.insert(key);
            scattered.insert(key);
        }
        assert_eq!(
            blocked.size_bytes(),
            scattered.size_bytes(),
            "the arms must be the same size for their rates to be comparable"
        );

        let absent: Vec<[u8; 16]> = (1_000_000..1_100_000u128).map(u128::to_le_bytes).collect();
        let rate = |hits: usize| {
            #[expect(clippy::cast_precision_loss, reason = "ratio of two small counts")]
            let r = hits as f64 / absent.len() as f64;
            r
        };
        let blocked_fpr = rate(absent.iter().filter(|k| blocked.maybe_contains(*k)).count());
        let scattered_fpr = rate(
            absent
                .iter()
                .filter(|k| scattered.maybe_contains(*k))
                .count(),
        );
        assert!(
            blocked_fpr <= scattered_fpr * 3.0 + 0.01,
            "blocked {blocked_fpr:.4} is more than 3x the scattered {scattered_fpr:.4} at equal size"
        );
    }

    /// Pins a property of the SHIPPING filter that its own doc comment
    /// contradicts: `with_expected_keys` asks for 10 bits/key and then rounds the
    /// bit count DOWN to a power of two, so the delivered size is between 5.0 and
    /// 10.0 bits/key depending on where the key count falls. At the bottom of
    /// that range the false-positive rate is an order of magnitude worse than the
    /// "~1%" the comment claims, because `k = 7` is tuned for the 10 it does not
    /// get. A false positive is safe here by design, so this costs work, not
    /// correctness -- but it is worth knowing before any filter is judged against
    /// its documented accuracy.
    #[test]
    fn shipping_expected_keys_sizing_rounds_down_to_a_power_of_two() {
        // 100_000 keys asks for 1_000_000 bits and receives 2^19 = 524_288.
        let filter = ShippingBloom::with_expected_keys(100_000, usize::MAX);
        assert_eq!(filter.size_bytes() * 8, 524_288);

        let inserted: Vec<[u8; 16]> = (0..100_000u128).map(u128::to_le_bytes).collect();
        let mut filter = ShippingBloom::with_expected_keys(inserted.len(), usize::MAX);
        for key in &inserted {
            filter.insert(key);
        }
        let absent: Vec<[u8; 16]> = (1_000_000..1_100_000u128).map(u128::to_le_bytes).collect();
        #[expect(clippy::cast_precision_loss, reason = "ratio of two small counts")]
        let fpr = absent.iter().filter(|k| filter.maybe_contains(*k)).count() as f64
            / absent.len() as f64;
        assert!(
            fpr > 0.05,
            "expected the documented ~1% to be missed by a wide margin at 5.24 bits/key, got {fpr:.4}"
        );
    }

    /// Both sizing paths must land on a power-of-two block count, or the mask
    /// selection silently addresses the wrong block.
    #[test]
    fn blocked_bloom_block_count_is_a_power_of_two() {
        for bits in [512usize, 1_000, 100_000, 1 << 20] {
            let filter = BlockedBloom::<512>::with_num_bits_pow2(bits);
            let blocks = filter.words.len() / BLOCK_WORDS;
            assert!(blocks.is_power_of_two(), "{bits} bits -> {blocks} blocks");
            assert_eq!(u64::try_from(blocks - 1).unwrap_or(0), filter.block_mask);
        }
    }
}
