/*
Copyright 2025-2026 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Primary-key existence index for upsert/insert conflict detection.
//!
//! Holds the in-memory keyset cache ([`CachedPkKeyset`]) and its bounded
//! Bloom-filter fallback ([`PkBloom`]) behind the [`CachedPkIndex`] enum, plus
//! the sidecar (de)serialization for persisting the bloom across restarts. The
//! provider maintains and probes these; the per-row location is recorded as a
//! [`RowLocation`].

use arrow_row::OwnedRow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Approximate per-entry `HashMap` control/allocation overhead used for the
// cache budget. The exact value is allocator-dependent, so keep this estimate
// centralized with `approx_pk_keyset_entry_bytes`.
const PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES: usize = 16;

pub(crate) fn approx_pk_keyset_entry_bytes(key: &OwnedRow) -> usize {
    key.as_ref().len()
        + std::mem::size_of::<RowLocation>()
        + PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES
}

/// Approximate resident bytes a captured-file path adds to the keyset's
/// `captured_files` set: the heap string (counted once per file — the `Arc<str>`
/// is shared with the keyset's `FilePositioned` values) plus the fat pointer and
/// `HashSet` slot overhead.
pub(crate) fn approx_captured_file_bytes(path: &str) -> usize {
    path.len() + std::mem::size_of::<Arc<str>>() + PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES
}

/// Where a primary key's current row version lives. The upsert path uses this to
/// decide how to tombstone the prior version: a `FilePositioned` entry can be
/// tombstoned by a per-file position deletion vector (pushed into the Vortex
/// scan, page-skippable); everything else falls back to a key-based deletion
/// vector applied above the scan. A single table can hold a mix.
#[derive(Debug, Clone)]
pub(crate) enum RowLocation {
    /// Row lives in the inline memtable; tombstoned by an inlined-data rewrite.
    Inlined,
    /// Row lives in a Vortex file but its file-local position is unknown — a
    /// cold-rebuilt keyset entry, or any entry under `deletion_mode: key`.
    /// Tombstoned by a key-based deletion vector.
    FileUnlocated,
    /// Row lives at a known `(file path, file-local position)`, captured by the
    /// `row_idx()` read-back under `deletion_mode: position`. Tombstoned by a
    /// per-file position deletion vector (`Selection::ExcludeRoaring`). The
    /// `file_path` `Arc` is shared across all rows in the same file, so the
    /// per-entry cost is one pointer + the `u64` position.
    FilePositioned { file_path: Arc<str>, position: u64 },
}

pub(crate) struct CachedPkKeyset {
    pub(crate) keys: HashMap<OwnedRow, RowLocation>,
    pub(crate) approx_bytes: usize,
    /// Data files whose rows have already had their `(key -> file-local
    /// position)` captured by the `deletion_mode: position` read-back, so the
    /// capture pass can skip them. Reset whenever the keyset is rebuilt (e.g.
    /// after compaction), which is exactly when the file set changes.
    pub(crate) captured_files: HashSet<Arc<str>>,
}

impl CachedPkKeyset {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: HashMap::with_capacity(capacity),
            approx_bytes: 0,
            captured_files: HashSet::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn insert(&mut self, key: OwnedRow, location: RowLocation) {
        let entry_bytes = approx_pk_keyset_entry_bytes(&key);
        match self.keys.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(location);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.approx_bytes = self.approx_bytes.saturating_add(entry_bytes);
                entry.insert(location);
            }
        }
    }
}

/// Number of hash probes for [`PkBloom`]. Seven keeps the false-positive rate
/// near 1% at the ~10 bits/key fill level; the bloom is sized to the whole byte
/// budget, so at realistic fills the rate is far lower.
const PK_BLOOM_NUM_HASHES: u32 = 7;

/// Seeded FNV-1a-64. Dependency-free and adequate for a Bloom filter; two
/// independent seeds feed the Kirsch–Mitzenmacher double-hashing scheme below.
fn pk_bloom_hash(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Bounded Bloom filter of live primary keys.
///
/// Used as the existence index for **`OnConflict::Upsert`** tables whose exact
/// keyset would exceed the configured byte budget (`pk_keyset_cache_max_bytes`).
/// Sized to the budget, it stays resident and is maintained incrementally,
/// avoiding the O(total-rows) full keyset rebuild on every CDC batch.
///
/// Correctness invariants:
/// - **No false negatives** as long as every inserted key is added and keys are
///   never removed — so a real upsert conflict is never missed.
/// - A **false positive** yields a redundant key-based delete tombstone, which
///   masks no older version (none exists) and is harmless under upsert.
/// - Only valid for upsert. `DoNothing` needs an exact answer (a false positive
///   would wrongly drop a genuinely new row), so those tables keep the exact path.
pub(crate) struct PkBloom {
    pub(crate) bits: Vec<u64>,
    /// `num_bits - 1`; `num_bits` is a power of two so indexing masks instead of mods.
    pub(crate) bit_mask: u64,
    /// Keys inserted (observability + false-positive-rate estimation).
    pub(crate) inserted_keys: usize,
}

impl PkBloom {
    /// Allocate a bloom whose bit array fits within `budget_bytes`, using the
    /// largest power-of-two bit count that does not exceed the budget.
    pub(crate) fn with_byte_budget(budget_bytes: usize) -> Self {
        Self::with_num_bits_pow2(budget_bytes.saturating_mul(8))
    }

    /// Right-size a bloom for `expected_keys` (~10 bits/key, ~1% FPR), never
    /// exceeding `max_bytes`. Used when persisting a compaction checkpoint so the
    /// sidecar stays small rather than the full byte budget.
    pub(crate) fn with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        let want_bits = expected_keys.saturating_mul(10);
        let cap_bits = max_bytes.saturating_mul(8).max(64);
        Self::with_num_bits_pow2(want_bits.min(cap_bits))
    }

    /// Allocate with the largest power-of-two bit count `<= target_bits` (min 64).
    pub(crate) fn with_num_bits_pow2(target_bits: usize) -> Self {
        let num_bits: usize = 1usize << target_bits.max(64).ilog2();
        let words = (num_bits / 64).max(1);
        Self {
            bits: vec![0u64; words],
            bit_mask: u64::try_from(num_bits.saturating_sub(1)).unwrap_or(u64::MAX),
            inserted_keys: 0,
        }
    }

    /// Serialize as `bit_mask(8) | inserted_keys(8) | num_words(8) | words(8·W)`,
    /// little-endian.
    pub(crate) fn serialize_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bit_mask.to_le_bytes());
        out.extend_from_slice(
            &u64::try_from(self.inserted_keys)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        out.extend_from_slice(&u64::try_from(self.bits.len()).unwrap_or(0).to_le_bytes());
        for word in &self.bits {
            out.extend_from_slice(&word.to_le_bytes());
        }
    }

    /// Inverse of [`serialize_into`]. Returns `None` on any length/format mismatch
    /// so a corrupt sidecar safely falls back to a full keyset rebuild.
    pub(crate) fn deserialize_from(bytes: &[u8]) -> Option<Self> {
        let bit_mask = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
        let inserted_keys = u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?);
        let num_words =
            usize::try_from(u64::from_le_bytes(bytes.get(16..24)?.try_into().ok()?)).ok()?;
        // Reject impossible word counts before allocating.
        if num_words == 0 || num_words > bytes.len().saturating_sub(24) / 8 {
            return None;
        }
        // `num_bits` must be a power of two and consistent with `bit_mask`.
        let num_bits = u64::try_from(num_words).ok()?.checked_mul(64)?;
        if num_bits != bit_mask.checked_add(1)? || !num_bits.is_power_of_two() {
            return None;
        }
        let mut bits = Vec::with_capacity(num_words);
        let mut offset = 24usize;
        for _ in 0..num_words {
            let end = offset.checked_add(8)?;
            bits.push(u64::from_le_bytes(bytes.get(offset..end)?.try_into().ok()?));
            offset = end;
        }
        Some(Self {
            bits,
            bit_mask,
            inserted_keys: usize::try_from(inserted_keys).unwrap_or(0),
        })
    }

    pub(crate) fn probe_bits(key: &[u8]) -> impl Iterator<Item = u64> {
        let h1 = pk_bloom_hash(key, 0x517c_c1b7_2722_0a95);
        // Force odd so successive probes stride across the whole bit space.
        let h2 = pk_bloom_hash(key, 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PK_BLOOM_NUM_HASHES).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)))
    }

    pub(crate) fn insert(&mut self, key: &[u8]) {
        for hash in Self::probe_bits(key) {
            let bit = hash & self.bit_mask;
            let word = usize::try_from(bit >> 6).unwrap_or(0);
            self.bits[word] |= 1u64 << (bit & 63);
        }
        self.inserted_keys = self.inserted_keys.saturating_add(1);
    }

    pub(crate) fn maybe_contains(&self, key: &[u8]) -> bool {
        for hash in Self::probe_bits(key) {
            let bit = hash & self.bit_mask;
            let word = usize::try_from(bit >> 6).unwrap_or(0);
            if self.bits[word] & (1u64 << (bit & 63)) == 0 {
                return false;
            }
        }
        true
    }
}

/// Magic ("CPKB") + version for the persisted PK-index bloom sidecar. Bumping
/// the version invalidates older sidecars (they deserialize to `None` → safe
/// full-scan fallback).
const PK_INDEX_SIDECAR_MAGIC: u32 = 0x4350_4b42;
const PK_INDEX_SIDECAR_VERSION: u32 = 1;
/// Upper bound on the persisted PK-index blob. Extreme-cardinality tables skip
/// persistence (and fall back to a runtime rebuild) to bound the metastore and
/// snapshot footprint. The bloom is right-sized (~10 bits/key), so this caps the
/// covered live-key count at roughly 200M.
pub(crate) const PK_INDEX_PERSIST_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Serialize a checkpoint: `magic | version | snapshot_id_len | snapshot_id | bloom`.
pub(crate) fn serialize_pk_bloom_sidecar(bloom: &PkBloom, snapshot_id: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&PK_INDEX_SIDECAR_MAGIC.to_le_bytes());
    out.extend_from_slice(&PK_INDEX_SIDECAR_VERSION.to_le_bytes());
    let snapshot_bytes = snapshot_id.as_bytes();
    out.extend_from_slice(
        &u64::try_from(snapshot_bytes.len())
            .unwrap_or(0)
            .to_le_bytes(),
    );
    out.extend_from_slice(snapshot_bytes);
    bloom.serialize_into(&mut out);
    out
}

/// Inverse of [`serialize_pk_bloom_sidecar`]; returns `None` on any
/// magic/version/length mismatch so a corrupt or stale-format sidecar falls back
/// to the full keyset rebuild.
pub(crate) fn deserialize_pk_bloom_sidecar(bytes: &[u8]) -> Option<(PkBloom, String)> {
    let magic = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?);
    let version = u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?);
    if magic != PK_INDEX_SIDECAR_MAGIC || version != PK_INDEX_SIDECAR_VERSION {
        return None;
    }
    let snapshot_len =
        usize::try_from(u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?)).ok()?;
    let snapshot_end = 16usize.checked_add(snapshot_len)?;
    let snapshot_id = std::str::from_utf8(bytes.get(16..snapshot_end)?)
        .ok()?
        .to_string();
    let bloom = PkBloom::deserialize_from(bytes.get(snapshot_end..)?)?;
    Some((bloom, snapshot_id))
}

/// Cached primary-key existence index for upsert/insert conflict detection.
///
/// Tables keep an [`Exact`](Self::Exact) keyset while it fits the byte budget;
/// upsert tables that exceed it fall back to a bounded [`Bloom`](Self::Bloom)
/// (see [`PkBloom`]) instead of dropping the cache and rebuilding from a
/// full-table scan every batch.
pub(crate) enum CachedPkIndex {
    Exact(CachedPkKeyset),
    Bloom(PkBloom),
}

impl CachedPkIndex {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Exact(keyset) => keyset.len(),
            Self::Bloom(bloom) => bloom.inserted_keys,
        }
    }

    /// Approximate resident bytes for memory accounting: the exact keyset's
    /// running byte tally, or the bloom's fixed bit-array size.
    pub(crate) fn approx_bytes(&self) -> usize {
        match self {
            Self::Exact(keyset) => keyset.approx_bytes,
            Self::Bloom(bloom) => bloom.bits.len().saturating_mul(8),
        }
    }
}

/// Borrowed view of a [`CachedPkIndex`] handed to per-batch validation.
pub(crate) enum PkExistenceRef<'a> {
    Exact(&'a HashMap<OwnedRow, RowLocation>),
    Bloom(&'a PkBloom),
}
