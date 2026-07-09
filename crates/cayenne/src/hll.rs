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
//! sketch per NDV-tracked column (integer, string, or temporal) and (de)serializes
//! them for the metastore.
//!
//! Why a purpose-built sketch: `DataFusion`'s `approx_distinct` `HyperLogLog` is
//! private to its aggregate executor and not importable, and no other HLL crate
//! is in the dependency graph. The estimate only needs to be accurate enough to
//! *size distributed joins and group-bys* on keys whose distinct count diverges
//! sharply from their min/max range (e.g. CDC `o_custkey` spanning ~1e9 with ~1M
//! distinct, or string group keys like `n_name`), so a standard register-array
//! HLL at precision [`PRECISION`] (≈1.6% standard error) is more than sufficient.
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

    /// Add a raw byte value (e.g. a UTF-8 string or binary key). Uses the same
    /// stable, explicit byte hashing as [`add_i128`](Self::add_i128) so the
    /// mapping is consistent across runs and builds for the persisted sketch.
    pub fn add_bytes(&mut self, value: &[u8]) {
        let hash = hash_index::hash_key_bytes(&[value]);
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
/// NDV-tracked columns (integers, strings, temporal) get a sketch.
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
    /// Empty (all-zero) columns are dropped; an all-empty set returns `None` so
    /// callers can store SQL `NULL`.
    #[must_use]
    pub fn serialize(&self) -> Option<Vec<u8>> {
        // Non-empty columns only (don't persist all-zero registers).
        let mut cols: Vec<(&u32, &HyperLogLog)> =
            self.columns.iter().filter(|(_, h)| !h.is_empty()).collect();
        if cols.is_empty() {
            return None;
        }
        // The header carries one precision; keep only columns that match it (all
        // do in practice — every sketch is built at `PRECISION`).
        let precision = cols[0].1.precision;
        let m = 1usize << precision;
        cols.retain(|(_, h)| h.registers.len() == m);
        if cols.is_empty() {
            return None;
        }

        let mut out = Vec::with_capacity(6 + cols.len() * (4 + m));
        out.push(SKETCH_FORMAT_VERSION);
        out.push(precision);
        out.extend_from_slice(&u32::try_from(cols.len()).unwrap_or(u32::MAX).to_le_bytes());
        for (idx, hll) in cols {
            out.extend_from_slice(&idx.to_le_bytes());
            out.extend_from_slice(&hll.registers);
        }
        Some(out)
    }

    /// Parse a serialized blob's header and yield each column as
    /// `(col_idx, register_slice)` **borrowing** `bytes` — the single zero-copy
    /// reader shared by [`deserialize`](Self::deserialize) (which copies each
    /// slice into an owned sketch) and [`merge_serialized`](Self::merge_serialized)
    /// (which folds each slice in place, no allocation). Returns `None` on a
    /// malformed header or a payload too short for `num_columns` records, so both
    /// consumers treat a bad blob as "no columns" — one all-or-nothing validation
    /// instead of two hand-rolled parsers.
    fn parse_columns(bytes: &[u8]) -> Option<(u8, impl Iterator<Item = (u32, &[u8])>)> {
        if bytes.len() < 6 || bytes[0] != SKETCH_FORMAT_VERSION {
            return None;
        }
        let precision = bytes[1];
        if precision == 0 || precision > 18 {
            return None;
        }
        let record = 4 + (1usize << precision);
        let num_columns = u32::from_le_bytes(bytes[2..6].try_into().ok()?) as usize;
        let body = bytes.get(6..)?;
        // A body too short for all declared columns is malformed.
        if body.len() < num_columns.checked_mul(record)? {
            return None;
        }
        let columns = body.chunks_exact(record).take(num_columns).map(|rec| {
            // rec.len() == record == 4 + m by construction, so the leading 4
            // bytes are the column index and the rest is the register slice.
            let idx = u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]);
            (idx, &rec[4..])
        });
        Some((precision, columns))
    }

    /// Deserialize a blob produced by [`Self::serialize`]. Returns `None` on a
    /// version mismatch or malformed input (callers fall back to no NDV).
    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        let (precision, columns) = Self::parse_columns(bytes)?;
        let columns = columns
            .map(|(idx, registers)| {
                (
                    idx,
                    HyperLogLog {
                        precision,
                        registers: registers.to_vec(),
                    },
                )
            })
            .collect();
        Some(Self { columns })
    }

    /// Merge a serialized blob into `self` in place (register-wise union),
    /// mirroring `merge_serialized_stats` for min/max.
    ///
    /// Allocation-free: folds each column's register bytes straight from the blob
    /// slice into the per-column accumulator, rather than materializing a
    /// transient [`NdvSketches`] via [`deserialize`](Self::deserialize) (a fresh
    /// 4 KiB `Vec` + `BTreeMap` node per column plus a second pass). The
    /// `ndv_cumulative_rebuild` bench measures this ~16-20x faster than
    /// deserialize-then-[`merge`](Self::merge) over many files — and this is the
    /// write-time aggregate-merge path, so the win applies on every commit.
    ///
    /// A malformed blob is a no-op (matching `deserialize` → `None`); a column
    /// whose register width doesn't match the accumulator's is skipped (mirrors
    /// [`HyperLogLog::merge`](HyperLogLog::merge)'s precision guard).
    pub fn merge_serialized(&mut self, existing_blob: &[u8]) {
        let Some((_precision, columns)) = Self::parse_columns(existing_blob) else {
            return;
        };
        for (idx, src) in columns {
            let hll = self.entry(idx);
            if hll.registers.len() != src.len() {
                continue;
            }
            // Register-wise max in a single pass; `(*dst).max(*s)` autovectorizes
            // to packed unsigned-max (`pmaxub`/`vpmaxub` on x86, `umax` on NEON) —
            // see the `ndv_cumulative_rebuild` bench.
            for (dst, s) in hll.registers.iter_mut().zip(src) {
                *dst = (*dst).max(*s);
            }
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
    fn add_bytes_counts_distinct_strings() {
        let mut hll = HyperLogLog::new();
        for v in 0..10_000u64 {
            // Distinct strings; repeat each a few times to confirm dedup.
            let s = format!("name-{v}");
            for _ in 0..3 {
                hll.add_bytes(s.as_bytes());
            }
        }
        let est = hll.estimate();
        assert!(
            within_error(est, 10_000, 0.05),
            "string est={est} outside 5% of 10,000"
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
    fn merge_serialized_equals_deserialize_then_merge() {
        // Multi-column base accumulator (the "existing aggregate").
        let mut base = NdvSketches::new();
        for c in 0..4u32 {
            let h = base.entry(c);
            for v in 0..1_000i128 {
                h.add_i128(v + i128::from(c) * 7);
            }
        }
        // Incoming blob overlaps some columns, extends the ranges, adds col 4.
        let mut incoming = NdvSketches::new();
        for c in 0..5u32 {
            let h = incoming.entry(c);
            for v in 500..1_500i128 {
                h.add_i128(v + i128::from(c) * 7);
            }
        }
        let blob = incoming.serialize().expect("non-empty");

        // Path A: allocation-free merge_serialized (the code under test).
        let mut via_slice = base.clone();
        via_slice.merge_serialized(&blob);

        // Path B: deserialize then register-wise merge (the prior behavior).
        let mut via_deserialize = base.clone();
        via_deserialize.merge(&NdvSketches::deserialize(&blob).expect("roundtrip"));

        assert_eq!(
            via_slice, via_deserialize,
            "allocation-free merge must equal deserialize-then-merge register-for-register"
        );
    }

    #[test]
    fn merge_serialized_ignores_malformed_blob() {
        let mut base = NdvSketches::new();
        base.entry(1).add_i128(42);
        let before = base.clone();
        // Too short, bad version, and truncated payload are all no-ops.
        base.merge_serialized(&[]);
        base.merge_serialized(&[99, 12, 0, 0, 0, 0]);
        let mut good = NdvSketches::new();
        good.entry(1).add_i128(7);
        let mut truncated = good.serialize().expect("serialize");
        truncated.truncate(truncated.len() - 10);
        base.merge_serialized(&truncated);
        assert_eq!(
            base, before,
            "a malformed blob must leave the accumulator unchanged"
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
