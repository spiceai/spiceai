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

use crate::row_converter::OwnedRow;
use hash_index::{PrehashedBuildHasher, hash_key_128};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Seeded XXH3-128 digest of a primary key's `RowConverter`-encoded bytes — the
/// key's identity throughout the upsert conflict path.
///
/// The per-row conflict loop probes three maps (in-batch dedup, cross-batch
/// `incoming_keys`, and the `existing_keys` keyset). Hashing the `OwnedRow`
/// bytes ONCE into this digest and keying every map on it — fronted by
/// [`PrehashedBuildHasher`], which reuses the digest's own entropy rather than
/// re-hashing — collapses those probes onto a single hash pass, mirroring
/// [`KeyDeletionIndex`](super::deletion_index::KeyDeletionIndex). It replaces the
/// standard-library `SipHash` default, which showed up on apply-side flamegraphs
/// and streamed (slowly) for composite PKs whose encoding is long.
///
/// Two distinct keys colliding under XXH3-128 would share one map slot; at one
/// billion keys the birthday bound puts that below ~1e-20 — orders of magnitude
/// under hardware error rates, and the same identity trade the key-based
/// deletion index already makes. (A 64-bit digest would NOT be safe as identity
/// at these cardinalities: ~0.3% collision odds at the same scale.)
#[inline]
pub(crate) fn pk_digest(key: &OwnedRow) -> u128 {
    hash_key_128(key.as_ref())
}

/// A set of primary-key [`OwnedRow`]s identified by their [`pk_digest`] and
/// fronted by [`PrehashedBuildHasher`]. Presents a `HashSet`-like API while
/// keying on the 128-bit digest, so the per-apply accumulators
/// (`incoming_keys` / `kept_keys` / bloom-MISS keys) share the conflict loop's
/// single hash pass. The `OwnedRow` is retained (as the map value) because
/// downstream consumers — the keyset insert, bloom rebuild, deletion lists, and
/// shard routing — need the raw key bytes, never the digest.
#[derive(Debug, Clone, Default)]
pub(crate) struct PkDigestSet {
    inner: HashMap<u128, OwnedRow, PrehashedBuildHasher>,
}

impl PkDigestSet {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashMap::with_capacity_and_hasher(capacity, PrehashedBuildHasher),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Whether a key with this precomputed digest is present — lets the conflict
    /// loop reuse the digest it already computed for the row.
    #[inline]
    pub(crate) fn contains_digest(&self, digest: u128) -> bool {
        self.inner.contains_key(&digest)
    }

    /// Insert `key` under a precomputed `digest` (the loop's single hash pass).
    /// `digest` MUST equal `pk_digest(&key)`.
    #[inline]
    pub(crate) fn insert_with_digest(&mut self, digest: u128, key: OwnedRow) {
        debug_assert_eq!(
            digest,
            pk_digest(&key),
            "insert_with_digest called with a digest that does not match the key"
        );
        self.inner.insert(digest, key);
    }

    /// Iterate the retained `OwnedRow`s (arbitrary order, like a `HashSet`).
    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &OwnedRow> {
        self.inner.values()
    }

    /// Merge every key of `other` into `self`, consuming it and reusing its
    /// stored digests — no re-hashing.
    #[inline]
    pub(crate) fn absorb(&mut self, other: PkDigestSet) {
        self.inner.extend(other.inner);
    }

    /// Copy every key of `other` into `self`, reusing its stored digests.
    pub(crate) fn extend_ref(&mut self, other: &PkDigestSet) {
        self.inner.reserve(other.inner.len());
        for (&digest, key) in &other.inner {
            self.inner.insert(digest, key.clone());
        }
    }

    /// Iterate `(digest, key)` pairs, so a consumer rebuilding another
    /// digest-keyed structure reuses the stored digest instead of re-hashing.
    #[inline]
    pub(crate) fn iter_with_digest(&self) -> impl Iterator<Item = (u128, &OwnedRow)> {
        self.inner.iter().map(|(&digest, key)| (digest, key))
    }
}

/// Value stored per key in the digest-keyed [`CachedPkKeyset`]: the raw PK
/// [`OwnedRow`] (retained because bloom rebuild, shard routing, and byte
/// accounting need the bytes) alongside its current [`RowLocation`].
#[derive(Debug, Clone)]
struct PkKeysetEntry {
    row: OwnedRow,
    location: RowLocation,
}

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
    /// PK existence map, keyed by [`pk_digest`] and fronted by
    /// [`PrehashedBuildHasher`]. Private so every access routes through a method
    /// that hashes the key consistently; the retained [`OwnedRow`] lives in each
    /// entry's value.
    keys: HashMap<u128, PkKeysetEntry, PrehashedBuildHasher>,
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
            keys: HashMap::with_capacity_and_hasher(capacity, PrehashedBuildHasher),
            approx_bytes: 0,
            captured_files: HashSet::new(),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Insert `key` -> `location`, overwriting an existing entry's location.
    /// Hashes `key` once via [`pk_digest`]; callers with a precomputed digest
    /// should use [`Self::insert_with_digest`] instead.
    pub(crate) fn insert(&mut self, key: OwnedRow, location: RowLocation) {
        self.insert_with_digest(pk_digest(&key), key, location);
    }

    /// Insert `key` -> `location` under a precomputed `digest` (which MUST equal
    /// `pk_digest(&key)`), overwriting an existing entry's location. Lets the
    /// per-apply recording paths reuse the digest already stored in a
    /// [`PkDigestSet`] instead of re-hashing.
    pub(crate) fn insert_with_digest(
        &mut self,
        digest: u128,
        key: OwnedRow,
        location: RowLocation,
    ) {
        debug_assert_eq!(
            digest,
            pk_digest(&key),
            "insert_with_digest called with a digest that does not match the key"
        );
        match self.keys.entry(digest) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().location = location;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.approx_bytes = self
                    .approx_bytes
                    .saturating_add(approx_pk_keyset_entry_bytes(&key));
                entry.insert(PkKeysetEntry { row: key, location });
            }
        }
    }

    /// Insert `key` -> `location` only when the key is ABSENT, preserving an
    /// existing entry's `RowLocation` (unlike [`Self::insert`], which overwrites).
    /// One hash lookup via `entry`, updating `approx_bytes` only for the new key.
    /// Used by the mem-tier fold, which must not clobber a durable-scan location
    /// with `FileUnlocated`.
    pub(crate) fn insert_if_absent(&mut self, key: OwnedRow, location: RowLocation) {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.keys.entry(pk_digest(&key)) {
            self.approx_bytes = self
                .approx_bytes
                .saturating_add(approx_pk_keyset_entry_bytes(&key));
            entry.insert(PkKeysetEntry { row: key, location });
        }
    }

    /// Whether a key with this precomputed digest is present — lets a caller
    /// holding the digest (e.g. from a [`PkDigestSet`]) skip re-hashing.
    #[inline]
    pub(crate) fn contains_digest(&self, digest: u128) -> bool {
        self.keys.contains_key(&digest)
    }

    /// The [`RowLocation`] for a key with this precomputed digest — the hot-path
    /// existence probe, reusing the conflict loop's single hash pass.
    #[inline]
    pub(crate) fn location_by_digest(&self, digest: u128) -> Option<&RowLocation> {
        self.keys.get(&digest).map(|entry| &entry.location)
    }

    /// Mutable access to a key's [`RowLocation`] (position-capture upgrade).
    #[inline]
    pub(crate) fn location_mut(&mut self, key: &OwnedRow) -> Option<&mut RowLocation> {
        self.keys
            .get_mut(&pk_digest(key))
            .map(|entry| &mut entry.location)
    }

    /// Iterate every retained key's [`OwnedRow`] bytes (bloom rebuild, sharding).
    #[inline]
    pub(crate) fn rows(&self) -> impl Iterator<Item = &OwnedRow> {
        self.keys.values().map(|entry| &entry.row)
    }

    /// Iterate every entry's [`RowLocation`] immutably (test/inspection).
    #[cfg(test)]
    pub(crate) fn locations(&self) -> impl Iterator<Item = &RowLocation> {
        self.keys.values().map(|entry| &entry.location)
    }

    /// Mutable iterator over every entry's [`RowLocation`] (the
    /// `Inlined -> FileUnlocated` flip after an inline checkpoint).
    pub(crate) fn locations_mut(&mut self) -> impl Iterator<Item = &mut RowLocation> {
        self.keys.values_mut().map(|entry| &mut entry.location)
    }

    /// Consume the keyset into `(key, location)` pairs (the shard split).
    pub(crate) fn into_entries(self) -> impl Iterator<Item = (OwnedRow, RowLocation)> {
        self.keys
            .into_values()
            .map(|entry| (entry.row, entry.location))
    }
}

/// Routing seed for PK-shard assignment — distinct from the bloom's hashing seeds
/// so shard placement is independent of bloom bit positions.
const PK_SHARD_SEED: u64 = 0x243f_6a88_85a3_08d3;

/// Map a primary key to one of `n` shards by hashing its `RowConverter`-encoded
/// `OwnedRow` bytes.
///
/// THE shard key is defined as the `OwnedRow` byte representation — NEVER the
/// big-endian i64 encoding the tombstone delete-lists use. Every routing site
/// (write/validate routing, per-shard keyset/bloom, derived tombstone lists) must
/// hash this same byte string, or the same logical key routes to two shards,
/// splitting its version history and breaking last-writer-wins. `n <= 1` is the
/// unsharded fast path and always returns shard 0.
#[inline]
pub(crate) fn shard_of_pk(owned_row_bytes: &[u8], n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    let bucket = pk_bloom_hash(owned_row_bytes, PK_SHARD_SEED) % n as u64;
    usize::try_from(bucket).unwrap_or(0)
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

    /// Deserialize ONE bloom from the front of `bytes`, returning it and the
    /// number of bytes it consumed — so several blooms can be read back-to-back
    /// from a sharded sidecar (the bloom is self-describing via its `num_words`).
    fn deserialize_from_prefix(bytes: &[u8]) -> Option<(Self, usize)> {
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
        Some((
            Self {
                bits,
                bit_mask,
                inserted_keys: usize::try_from(inserted_keys).unwrap_or(0),
            },
            offset,
        ))
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
/// Bumped to 2 for the sharded PK-index rollout: the sidecar now carries a bloom
/// COUNT prefix and N serialized blooms (one per mem-tier shard) instead of a
/// single bloom. A version-1 (single-bloom) sidecar deserializes to `None` →
/// safe full keyset rebuild (the designed stale-format fallback), so an upgrade
/// across the bump simply rebuilds the index once.
const PK_INDEX_SIDECAR_VERSION: u32 = 2;
/// Upper bound on the persisted PK-index blob. Extreme-cardinality tables skip
/// persistence (and fall back to a runtime rebuild) to bound the metastore and
/// snapshot footprint. The bloom is right-sized (~10 bits/key), so this caps the
/// covered live-key count at roughly 200M.
pub(crate) const PK_INDEX_PERSIST_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Serialize a sharded checkpoint:
/// `magic | version | snapshot_id_len | snapshot_id | bloom_count | bloom* `.
/// `blooms` carries one entry per mem-tier shard (one element at the default N=1).
fn serialize_pk_blooms_sidecar(blooms: &[PkBloom], snapshot_id: &str) -> Vec<u8> {
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
    out.extend_from_slice(&u64::try_from(blooms.len()).unwrap_or(0).to_le_bytes());
    for bloom in blooms {
        bloom.serialize_into(&mut out);
    }
    out
}

/// Single-bloom convenience over [`serialize_pk_blooms_sidecar`] (the persist path
/// produces one combined-snapshot bloom; the sharded blooms are rebuilt at load).
pub(crate) fn serialize_pk_bloom_sidecar(bloom: &PkBloom, snapshot_id: &str) -> Vec<u8> {
    serialize_pk_blooms_sidecar(std::slice::from_ref(bloom), snapshot_id)
}

/// Inverse of [`serialize_pk_blooms_sidecar`]; returns `None` on any
/// magic/version/length/count mismatch so a corrupt or stale-format sidecar
/// (including every version-1 single-bloom sidecar) falls back to the full keyset
/// rebuild.
fn deserialize_pk_blooms_sidecar(bytes: &[u8]) -> Option<(Vec<PkBloom>, String)> {
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
    let count_end = snapshot_end.checked_add(8)?;
    let count = usize::try_from(u64::from_le_bytes(
        bytes.get(snapshot_end..count_end)?.try_into().ok()?,
    ))
    .ok()?;
    let mut rest = bytes.get(count_end..)?;
    // Reject an impossible bloom count before allocating: each bloom is
    // self-describing and consumes >= 32 bytes (24-byte header + >= one 8-byte
    // word), so a `count` larger than the remaining bytes can encode means a
    // corrupt/truncated sidecar — return None (clean rebuild) rather than risk a
    // huge `with_capacity` allocation. Same guard idiom as `deserialize_from_prefix`.
    if count > rest.len() / 32 {
        return None;
    }
    let mut blooms = Vec::with_capacity(count);
    for _ in 0..count {
        let (bloom, consumed) = PkBloom::deserialize_from_prefix(rest)?;
        blooms.push(bloom);
        rest = rest.get(consumed..)?;
    }
    Some((blooms, snapshot_id))
}

/// Single-bloom convenience over [`deserialize_pk_blooms_sidecar`]: returns the
/// first bloom (the n==1 reload path). A multi-bloom sidecar with count != 1 is
/// rejected so the n==1 reader never silently uses a sharded sidecar.
pub(crate) fn deserialize_pk_bloom_sidecar(bytes: &[u8]) -> Option<(PkBloom, String)> {
    let (mut blooms, snapshot_id) = deserialize_pk_blooms_sidecar(bytes)?;
    if blooms.len() != 1 {
        return None;
    }
    Some((blooms.remove(0), snapshot_id))
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

/// The per-shard PK existence index — the sharded analog of [`CachedPkIndex`]
/// (§2.3c). A key is owned by `shard_of_pk(OwnedRow bytes)` (§3.5), the SAME
/// routing the tier append + reads use, so a key's existence entry co-locates
/// with its segments and a shard validates only against its own keys. Either
/// all-exact (one keyset per shard) or all-bloom (one bloom per shard), matching
/// the source index's path.
pub(crate) enum ShardedPkIndex {
    Exact(Box<[CachedPkKeyset]>),
    Bloom(Box<[PkBloom]>),
}

impl ShardedPkIndex {
    /// Partition an exact keyset into `n` per-shard keysets by `shard_of_pk` on
    /// each key's `OwnedRow` bytes (§3.5). Bloom-path indices are built sharded at
    /// load time instead — a combined bloom can't be partitioned (its keys are
    /// unrecoverable), so per-shard blooms are constructed by routing keys to N
    /// blooms during `load_existing_keyset` / `try_load_persisted_pk_index`.
    pub(crate) fn from_exact(keyset: CachedPkKeyset, n: usize) -> Self {
        let n = n.max(1);
        let mut shards: Vec<CachedPkKeyset> =
            (0..n).map(|_| CachedPkKeyset::with_capacity(0)).collect();
        // The position-delete capture set is table-global; every shard needs the
        // complete skip set so its read-back doesn't re-capture a covered file.
        // Captured before `into_entries` consumes the keyset below.
        let captured_files = keyset.captured_files.clone();
        for (key, loc) in keyset.into_entries() {
            let s = shard_of_pk(key.as_ref(), n);
            // Route through CachedPkKeyset::insert — the single source of truth for
            // approx_bytes (per-key approx_pk_keyset_entry_bytes) — so each shard's
            // byte tally is exact for variable-length/composite PKs, not an even
            // split of the source total. The per-shard sums then add back up to the
            // unsharded keyset's bytes with no integer-division undercount.
            shards[s].insert(key, loc);
        }
        for shard in &mut shards {
            shard.captured_files.clone_from(&captured_files);
        }
        Self::Exact(shards.into_boxed_slice())
    }

    pub(crate) fn shard_count(&self) -> usize {
        match self {
            Self::Exact(s) => s.len(),
            Self::Bloom(s) => s.len(),
        }
    }

    /// Borrowed existence view for shard `i`, handed to that shard's validation.
    pub(crate) fn existence_ref(&self, i: usize) -> PkExistenceRef<'_> {
        match self {
            Self::Exact(keysets) => PkExistenceRef::Exact(&keysets[i]),
            Self::Bloom(blooms) => PkExistenceRef::Bloom(&blooms[i]),
        }
    }

    /// Approximate resident bytes across all shards, for memory accounting.
    pub(crate) fn approx_bytes(&self) -> usize {
        match self {
            Self::Exact(keysets) => keysets
                .iter()
                .map(|k| k.approx_bytes)
                .fold(0, usize::saturating_add),
            Self::Bloom(blooms) => blooms
                .iter()
                .map(|b| b.bits.len().saturating_mul(8))
                .fold(0, usize::saturating_add),
        }
    }

    /// Record `keys` into ONE shard's existence view (Phase 6 — the bloom-split
    /// insert performed UNDER `mem_tier_publish_locks[shard]`). Every key in
    /// `keys` MUST belong to `shard` (it is the validated/kept key set of that
    /// shard's own sub-batch, already routed by `shard_of_pk`); inserting them
    /// here keyed on the SAME `shard` index keeps a key's existence entry
    /// co-located with its segments. Inserting under the shard lock makes the
    /// bloom INSERT atomic with the segment swap, so a later same-apply HIT-path
    /// validation against this shard observes the prior MISS-path appends (the
    /// §3.4 / Review-4 HOLE-3 intra-apply-dup window is closed jointly by this
    /// insert and the per-apply `incoming_keys` set).
    ///
    /// NOTE: unlike `record_pk_keys_with_location`, this intentionally does NOT do
    /// per-insert over-budget exact→bloom conversion. The sharded path recomputes
    /// the keyset byte tally ONCE after all per-shard appends (recompute-once), so a
    /// shard never converts exact→bloom mid-life — a deliberate divergence, not an
    /// oversight.
    pub(crate) fn record_keys_in_shard(
        &mut self,
        shard: usize,
        keys: &PkDigestSet,
        location: &RowLocation,
    ) {
        match self {
            Self::Exact(keysets) => {
                if let Some(keyset) = keysets.get_mut(shard) {
                    // Reuse each key's stored digest — the keyset is digest-keyed,
                    // so this avoids re-hashing every recorded key.
                    for (digest, key) in keys.iter_with_digest() {
                        keyset.insert_with_digest(digest, key.clone(), location.clone());
                    }
                }
            }
            Self::Bloom(blooms) => {
                if let Some(bloom) = blooms.get_mut(shard) {
                    for key in keys.iter() {
                        bloom.insert(key.as_ref());
                    }
                }
            }
        }
    }
}

/// Borrowed view of a [`CachedPkIndex`] handed to per-batch validation. The
/// `Exact` variant borrows the whole [`CachedPkKeyset`] so the conflict loop can
/// probe it by precomputed digest ([`CachedPkKeyset::location_by_digest`]).
pub(crate) enum PkExistenceRef<'a> {
    Exact(&'a CachedPkKeyset),
    Bloom(&'a PkBloom),
}
