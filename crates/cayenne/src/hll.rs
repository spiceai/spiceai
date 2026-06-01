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

//! A small, mergeable [`HyperLogLog`] cardinality sketch maintained incrementally
//! on the Cayenne write path, plus a [`NdvSketches`] container that holds one
//! sketch per (integer) column and (de)serializes them for the metastore.
//!
//! Why a purpose-built sketch: `DataFusion`'s `approx_distinct` `HyperLogLog` is
//! private to its aggregate executor and not importable, and no other HLL crate
//! is in the dependency graph. The estimate only needs to be accurate enough to
//! *size distributed joins* on sparse integer keys (e.g. CDC `o_custkey` spanning
//! ~1e9 with ~1M distinct), so a standard register-array HLL at precision
//! [`PRECISION`] (≈1.6% standard error) is more than sufficient.
//!
//! Mergeability is the reason this is incremental rather than recomputed: HLL is
//! a register-wise max, so a write's sketch folds into the metastore aggregate
//! exactly the way min/max already do, and slices accumulate across writes with
//! no full-table rescan. Deletes are not represented (HLL can't remove elements),
//! which leaves the NDV a *superset* under deletes — the safe direction for join
//! sizing (it only loosens the effective-max range). Reset on table overwrite.

use std::collections::BTreeMap;

/// Number of register-index bits. `m = 2^PRECISION` registers. At 12 bits that
/// is 4096 one-byte registers (4 KiB) per column with ~1.6% standard error
/// (`1.04 / sqrt(m)`) — ample to distinguish ~1M / ~15M / ~60M distinct keys and
/// to keep the per-column metastore blob small enough to merge on every commit.
const PRECISION: u8 = 12;

/// Serialization format version for [`NdvSketches`]. Bump on any layout change so
/// older blobs are skipped rather than misread.
const SKETCH_FORMAT_VERSION: u8 = 1;

/// A `HyperLogLog` sketch over 64-bit hashed values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperLogLog {
    /// Index bits (`m = 1 << precision`). Stored so a deserialized sketch only
    /// merges with another of the same precision.
    precision: u8,
    /// `m` one-byte registers holding the max observed rank per bucket.
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// Create an empty sketch at the default [`PRECISION`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_precision(PRECISION)
    }

    #[must_use]
    fn with_precision(precision: u8) -> Self {
        let m = 1usize << precision;
        Self {
            precision,
            registers: vec![0u8; m],
        }
    }

    /// Number of registers, `m = 2^precision`.
    fn m(&self) -> usize {
        1usize << self.precision
    }

    /// Fold a precomputed 64-bit hash into the sketch.
    ///
    /// The top `precision` bits select the register; the remaining bits' leading
    /// zero count (+1) is the rank. With a 64-bit hash the max meaningful rank is
    /// `64 - precision + 1`, so no large-range correction is needed.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "idx < 2^precision and rank <= 64 - precision + 1 are both small and in range"
    )]
    pub fn add_hash(&mut self, hash: u64) {
        let p = u32::from(self.precision);
        let idx = (hash >> (64 - p)) as usize;
        // Place the remaining (64 - p) bits at the top so leading_zeros counts
        // only them; an all-zero remainder yields the capped max rank.
        let remaining = hash << p;
        let rank = (remaining.leading_zeros() + 1).min(64 - p + 1) as u8;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    /// Add a raw integer value (sign-extended to `i128` so all integer widths
    /// share one stable hashing path).
    pub fn add_i128(&mut self, value: i128) {
        // Explicit byte hashing (not `Hash`) so the mapping is stable across
        // runs and builds — the sketch is persisted and merged over time.
        let hash = hash_index::hash_key_bytes(&[&value.to_le_bytes()]);
        self.add_hash(hash);
    }

    /// Merge another sketch into this one (register-wise max). No-op on a
    /// precision mismatch (treated as incompatible rather than panicking).
    pub fn merge(&mut self, other: &Self) {
        if self.precision != other.precision || self.registers.len() != other.registers.len() {
            tracing::warn!(
                "HyperLogLog::merge: precision/size mismatch ({}/{} vs {}/{}); skipping",
                self.precision,
                self.registers.len(),
                other.precision,
                other.registers.len(),
            );
            return;
        }
        for (dst, src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *src > *dst {
                *dst = *src;
            }
        }
    }

    /// Estimate the distinct cardinality.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "register/zero counts <= 2^18 are exact in f64; the estimate is rounded and clamped >= 0 before the u64 cast"
    )]
    pub fn estimate(&self) -> u64 {
        let m = self.m();
        let m_f = m as f64;

        let mut sum = 0.0_f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum += 2.0_f64.powi(-i32::from(r));
            if r == 0 {
                zeros += 1;
            }
        }

        // alpha_m constant (Flajolet et al.).
        let alpha = match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m_f),
        };

        let raw = alpha * m_f * m_f / sum;

        // Small-range correction: linear counting when registers are mostly empty.
        let estimate = if raw <= 2.5 * m_f && zeros > 0 {
            m_f * (m_f / zeros as f64).ln()
        } else {
            raw
        };

        // 64-bit hashes make the large-range (2^32) correction unnecessary.
        estimate.round().max(0.0) as u64
    }

    /// True if no value has been added.
    fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-column NDV sketches, keyed by column index in the table schema. Only
/// integer columns (join-key candidates) get a sketch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NdvSketches {
    columns: BTreeMap<u32, HyperLogLog>,
}

impl NdvSketches {
    /// Create an empty container with no per-column sketches.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// True if no column has a sketch.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get (creating if absent) the sketch for `column_index`.
    pub fn entry(&mut self, column_index: u32) -> &mut HyperLogLog {
        self.columns.entry(column_index).or_default()
    }

    /// The estimated distinct count for `column_index`, if a sketch exists and is
    /// non-empty.
    #[must_use]
    pub fn estimate(&self, column_index: u32) -> Option<u64> {
        let hll = self.columns.get(&column_index)?;
        if hll.is_empty() {
            return None;
        }
        Some(hll.estimate())
    }

    /// Merge `other` into `self` (per-column register-wise max; union of columns).
    pub fn merge(&mut self, other: &Self) {
        for (idx, hll) in &other.columns {
            self.columns
                .entry(*idx)
                .and_modify(|existing| existing.merge(hll))
                .or_insert_with(|| hll.clone());
        }
    }

    /// Serialize to a compact blob:
    /// `[version u8][precision u8][num_columns u32][ (col_idx u32, registers[m]) * ]`.
    /// Empty (no-column) sketch sets serialize to `None` so callers can store SQL
    /// `NULL`.
    #[must_use]
    pub fn serialize(&self) -> Option<Vec<u8>> {
        // Drop empty sketches so we don't persist all-zero registers.
        let present: Vec<(&u32, &HyperLogLog)> =
            self.columns.iter().filter(|(_, h)| !h.is_empty()).collect();
        if present.is_empty() {
            return None;
        }
        let precision = present[0].1.precision;
        let m = 1usize << precision;
        let mut out = Vec::with_capacity(2 + 4 + present.len() * (4 + m));
        out.push(SKETCH_FORMAT_VERSION);
        out.push(precision);
        out.extend_from_slice(
            &u32::try_from(present.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for (idx, hll) in present {
            // Skip columns whose precision differs from the header's (shouldn't
            // happen — all columns use PRECISION).
            if hll.registers.len() != m {
                continue;
            }
            out.extend_from_slice(&idx.to_le_bytes());
            out.extend_from_slice(&hll.registers);
        }
        Some(out)
    }

    /// Deserialize a blob produced by [`Self::serialize`]. Returns `None` on a
    /// version mismatch or malformed input (callers fall back to no NDV).
    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 6 {
            return None;
        }
        let version = bytes[0];
        if version != SKETCH_FORMAT_VERSION {
            return None;
        }
        let precision = bytes[1];
        if precision == 0 || precision > 18 {
            return None;
        }
        let m = 1usize << precision;
        let num_columns = u32::from_le_bytes(bytes[2..6].try_into().ok()?) as usize;
        let mut offset = 6usize;
        let mut columns = BTreeMap::new();
        for _ in 0..num_columns {
            if offset + 4 + m > bytes.len() {
                return None;
            }
            let idx = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
            offset += 4;
            let registers = bytes[offset..offset + m].to_vec();
            offset += m;
            columns.insert(
                idx,
                HyperLogLog {
                    precision,
                    registers,
                },
            );
        }
        Some(Self { columns })
    }

    /// Merge a serialized blob into `self` in place. Convenience for the persist
    /// path, mirroring `merge_serialized_stats` for min/max.
    pub fn merge_serialized(&mut self, existing_blob: &[u8]) {
        if let Some(existing) = Self::deserialize(existing_blob) {
            self.merge(&existing);
        }
    }
}

#[cfg(test)]
#[expect(clippy::cast_precision_loss)]
mod tests {
    use super::*;

    fn build(distinct: u64) -> HyperLogLog {
        let mut hll = HyperLogLog::new();
        for v in 0..distinct {
            hll.add_i128(i128::from(v));
        }
        hll
    }

    fn within_error(estimate: u64, actual: u64, rel: f64) -> bool {
        let diff = (estimate as f64 - actual as f64).abs();
        diff <= rel * actual as f64
    }

    #[test]
    fn empty_estimate_is_zero() {
        assert_eq!(HyperLogLog::new().estimate(), 0);
        assert!(HyperLogLog::new().is_empty());
    }

    #[test]
    fn small_cardinality_linear_counting_is_accurate() {
        for &n in &[1u64, 5, 50, 500] {
            let est = build(n).estimate();
            assert!(
                within_error(est, n, 0.10),
                "n={n} est={est} outside 10% (small range / linear counting)"
            );
        }
    }

    #[test]
    fn large_cardinality_within_standard_error() {
        // ~1.6% standard error at PRECISION=12; allow generous slack for the test.
        for &n in &[100_000u64, 1_000_000] {
            let est = build(n).estimate();
            assert!(within_error(est, n, 0.05), "n={n} est={est} outside 5%");
        }
    }

    #[test]
    fn duplicates_do_not_inflate() {
        let mut hll = HyperLogLog::new();
        for _ in 0..10_000 {
            hll.add_i128(42);
        }
        let est = hll.estimate();
        assert!(
            est <= 3,
            "duplicate-only sketch estimated {est}, expected ~1"
        );
    }

    #[test]
    fn merge_equals_union() {
        // Two disjoint halves merged ≈ full set.
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for v in 0..500_000i128 {
            a.add_i128(v);
        }
        for v in 500_000..1_000_000i128 {
            b.add_i128(v);
        }
        a.merge(&b);
        let est = a.estimate();
        assert!(
            within_error(est, 1_000_000, 0.05),
            "merged est={est} outside 5% of 1,000,000"
        );
    }

    #[test]
    fn merge_idempotent_on_same_sketch() {
        // Re-merging the same sketch (e.g. an inline-checkpoint re-persist) must
        // not change the estimate.
        let a = build(200_000);
        let before = a.estimate();
        let mut merged = a.clone();
        merged.merge(&a);
        assert_eq!(merged.estimate(), before);
    }

    #[test]
    fn sketches_serialize_roundtrip_and_merge() {
        let mut s = NdvSketches::new();
        for v in 0..100_000i128 {
            s.entry(2).add_i128(v);
            s.entry(5).add_i128(v * 7);
        }
        let blob = s.serialize().expect("non-empty");
        let back = NdvSketches::deserialize(&blob).expect("roundtrip");
        assert_eq!(s, back);
        assert!(within_error(
            back.estimate(2).expect("col 2 estimate present"),
            100_000,
            0.05
        ));
        // Column with no sketch -> None.
        assert_eq!(back.estimate(9), None);

        // merge_serialized accumulates a second disjoint half into column 2.
        let mut s2 = NdvSketches::new();
        for v in 100_000..200_000i128 {
            s2.entry(2).add_i128(v);
        }
        s2.merge_serialized(&blob);
        assert!(
            within_error(
                s2.estimate(2).expect("col 2 estimate present"),
                200_000,
                0.05
            ),
            "merged column 2 est={:?}",
            s2.estimate(2)
        );
    }

    #[test]
    fn deserialize_rejects_bad_input() {
        assert!(NdvSketches::deserialize(&[]).is_none());
        assert!(NdvSketches::deserialize(&[99, 12, 0, 0, 0, 0]).is_none()); // bad version
        let mut s = NdvSketches::new();
        s.entry(0).add_i128(1);
        let mut blob = s.serialize().expect("serialize non-empty sketch");
        blob.truncate(blob.len() - 10); // corrupt length
        assert!(NdvSketches::deserialize(&blob).is_none());
    }

    #[test]
    fn empty_sketches_serialize_to_none() {
        let s = NdvSketches::new();
        assert!(s.serialize().is_none());
        let mut s2 = NdvSketches::new();
        // touch a column but add nothing -> still empty registers
        let _ = s2.entry(3);
        assert!(s2.serialize().is_none());
    }
}
