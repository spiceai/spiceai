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
use hash_index::{PrehashedBuildHasher, hash_key_128, hash_key_bytes};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

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
    /// Sequence of the last commit that wrote this key's live row (0 = unknown,
    /// treated as "older than any transaction"). Used by transaction per-key OCC:
    /// a read-footprint / write-set key whose stored `sequence` exceeds the
    /// transaction's begin high-water was modified after the transaction began →
    /// conflict. Stamped by `record_sequence` on every committed write and by the
    /// keyset rebuild's end-of-scan high-water.
    sequence: i64,
}

// Approximate per-entry `HashMap` control/allocation overhead used for the
// cache budget. The exact value is allocator-dependent, so keep this estimate
// centralized with `approx_pk_keyset_entry_bytes`.
const PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES: usize = 16;

pub(crate) fn approx_pk_keyset_entry_bytes(key: &OwnedRow) -> usize {
    // Charge what the map actually stores per entry: the u128 digest key, the
    // whole `PkKeysetEntry` (the `OwnedRow` fat pointer, `RowLocation`, and the
    // OCC `sequence`), and the key's heap bytes. An estimate that drops any of
    // those bounds the cache at a fraction of its believed size - a
    // counting-allocator measurement puts the real per-entry cost at 1.6-3.5x
    // a key-plus-location-only figure.
    key.as_ref().len()
        + std::mem::size_of::<u128>()
        + std::mem::size_of::<PkKeysetEntry>()
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

/// Outcome of [`CachedPkKeyset::try_insert_with_digest`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PkKeysetInsertOutcome {
    /// The key was already present; its location was updated in place.
    Updated,
    /// The key was new and inserted within `max_bytes`.
    Inserted,
    /// The key was new but inserting it would exceed `max_bytes`; the keyset is
    /// unchanged and the caller should fall back to a bloom filter.
    OverBudget,
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
                entry.insert(PkKeysetEntry {
                    row: key,
                    location,
                    sequence: 0,
                });
            }
        }
    }

    /// Update `key`'s location if it is already present, else insert it if doing
    /// so keeps `approx_bytes` within `max_bytes` — one hash lookup via `entry`
    /// (vs. a separate `contains_digest` probe followed by `insert_with_digest`'s
    /// own lookup), and clones `key` only on the insert branch. The hot path on
    /// re-touched PKs (e.g. CDC updates) is "present", where this does neither a
    /// second hash nor an allocation.
    pub(crate) fn try_insert_with_digest(
        &mut self,
        digest: u128,
        key: &OwnedRow,
        location: RowLocation,
        max_bytes: usize,
    ) -> PkKeysetInsertOutcome {
        debug_assert_eq!(
            digest,
            pk_digest(key),
            "try_insert_with_digest called with a digest that does not match the key"
        );
        match self.keys.entry(digest) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().location = location;
                PkKeysetInsertOutcome::Updated
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let entry_bytes = approx_pk_keyset_entry_bytes(key);
                if self.approx_bytes.saturating_add(entry_bytes) > max_bytes {
                    return PkKeysetInsertOutcome::OverBudget;
                }
                self.approx_bytes = self.approx_bytes.saturating_add(entry_bytes);
                entry.insert(PkKeysetEntry {
                    row: key.clone(),
                    location,
                    sequence: 0,
                });
                PkKeysetInsertOutcome::Inserted
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
            entry.insert(PkKeysetEntry {
                row: key,
                location,
                sequence: 0,
            });
        }
    }

    /// The [`RowLocation`] for a key with this precomputed digest — the hot-path
    /// existence probe, reusing the conflict loop's single hash pass.
    #[inline]
    pub(crate) fn location_by_digest(&self, digest: u128) -> Option<&RowLocation> {
        self.keys.get(&digest).map(|entry| &entry.location)
    }

    /// The stored commit sequence for a key with this precomputed digest, or
    /// `None` if the key is absent. Drives per-key transaction OCC re-check.
    #[inline]
    pub(crate) fn sequence_by_digest(&self, digest: u128) -> Option<i64> {
        self.keys.get(&digest).map(|entry| entry.sequence)
    }

    /// Stamp a present key's last-commit sequence (monotonic max). Called after a
    /// committed write records the key. No-op if the key is absent (over budget /
    /// bloomed) — those tables fall back to per-table OCC.
    pub(crate) fn record_sequence(&mut self, digest: u128, sequence: i64) {
        if let Some(entry) = self.keys.get_mut(&digest) {
            entry.sequence = entry.sequence.max(sequence);
        }
    }

    /// Raise every entry's sequence to at least `sequence` — the end-of-scan
    /// high-water after a full keyset rebuild, so any key that might have been
    /// modified concurrently during the rebuild is conservatively treated as
    /// changed (over-abort, never a missed conflict).
    pub(crate) fn stamp_all_sequences_min(&mut self, sequence: i64) {
        for entry in self.keys.values_mut() {
            entry.sequence = entry.sequence.max(sequence);
        }
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
///
/// Changing this invalidates every persisted bloom. That is handled, not
/// forbidden: it moves [`SCATTERED_PROBE_FINGERPRINT`], which is what a reader
/// checks before trusting a blob's bits.
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

/// Magic ("CPKF") and version framing every serialized [`PkBloom`]. The magic
/// occupies the leading 4 bytes, where an unframed (pre-v1) bloom keeps the low
/// half of `bit_mask` — and a `bit_mask` is always `2^n - 1`, which the magic is
/// not, so the two shapes are told apart deterministically rather than by the
/// consistency checks in [`PkBloom::deserialize_from_prefix`] happening to
/// reject one.
const PK_BLOOM_FRAME_MAGIC: u32 = 0x4350_4b46;
/// Version of the framed layout. A reader rejects any other value (→ `None` →
/// the caller's conservative fallback), so a rollback across a future bump
/// degrades to extra work, never to wrong answers.
const PK_BLOOM_FRAME_VERSION_SCATTERED: u32 = 1;

/// Frame version for the split-block layout: a key sets one bit in each of a
/// 256-bit block's eight `u32` lanes, so a probe touches one cache line and
/// carries no per-probe branch.
///
/// A second VERSION rather than a second magic, because the version is what
/// tells a reader how to parse the body — words for scattered, blocks for this
/// — and the fingerprint then says whether the probes that filled it match the
/// ones this binary would use.
const PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK: u32 = 2;
/// Bytes the frame prepends to the bloom body: magic, version, fingerprint.
const PK_BLOOM_FRAME_HEADER_LEN: usize = 16;

/// Identity of everything that decides WHERE a key's bits land: the hash
/// function and its seeds, the double-hashing scheme, the probe count
/// [`PK_BLOOM_NUM_HASHES`], and the bit/word layout [`PkBloom::insert`] uses.
///
/// A bloom's bits are only meaningful to a reader that derives probes the same
/// way. Change any leg and the bits sit where the old function put them while
/// probes read where the new one looks — uncorrelated, so a key that IS present
/// can probe as absent. On the cold-tier path that false negative means no
/// supersede tombstone, and so a duplicate live row.
///
/// It is derived rather than declared, so that it cannot be forgotten the way a
/// hand-bumped version can: it folds [`PK_BLOOM_NUM_HASHES`] together with what
/// a fixed sample WRITES and what that same sample then READS BACK. That covers
/// the seeds, the double-hashing scheme, the body of [`pk_bloom_hash`], and both
/// copies of the bit/word arithmetic — legs no declared constant would catch,
/// since none of them is a constant.
///
/// Reading back is not redundant with the bits. [`PkBloom::insert`] and
/// [`PkBloom::maybe_contains`] each spell out the mask/word/bit mapping
/// themselves rather than sharing one helper, so a change to the READ side alone
/// leaves every written bit — and a write-only fingerprint — exactly where it
/// was, while breaking the agreement between them. Folding the answers to a
/// fixed set of present and absent probes is what closes that.
///
/// What it does not offer is a proof. The sample is a fixed set of keys, so a
/// change that happens to leave all of their bits and answers identical — one
/// conditioned on a key length or byte pattern the sample never takes — goes
/// unnoticed. The probe count is folded in on its own for that reason, being the
/// leg likeliest to collide. Nor does the value decompose: a mismatch reports
/// two `u64`s, not which leg moved.
/// Fingerprint of the SCATTERED probe function, and of the split-block one.
///
/// One per layout, because which probes filled a bloom now depends on which
/// layout wrote it: a single global would mean a binary that writes split-block
/// could not validate a scattered blob, and every filter already on disk is
/// scattered.
///
/// Both samples are built through a layout-EXPLICIT constructor, never
/// [`PkBloom::with_num_bits_pow2`], which follows this process's configured
/// write version. A fingerprint that moved with an environment variable would
/// make the same bytes valid in one process and rejected in another.
static SCATTERED_PROBE_FINGERPRINT: LazyLock<u64> =
    LazyLock::new(|| probe_fingerprint_of(PkBloom::scattered_with_num_bits_pow2(512)));

static SPLIT_BLOCK_PROBE_FINGERPRINT: LazyLock<u64> =
    LazyLock::new(|| probe_fingerprint_of(PkBloom::split_block_with_num_bits_pow2(512)));

/// The fingerprint of `sample`'s layout: fill it with fixed keys, then fold the
/// probe count, the sizing field, every resulting bit, and the answers to a set
/// of keys that were never inserted.
///
/// Keep the scattered arm's folded bytes exactly as they are. Their value is
/// pinned by [`LEGACY_PK_BLOOM_PROBE_FINGERPRINT`], which every unframed bloom
/// on disk is validated against — change what goes into the fold and every one
/// of them stops being readable.
fn probe_fingerprint_of(mut sample: PkBloom) -> u64 {
    // Fixed keys spanning empty, short, word-boundary, and long lengths, and
    // both extremes of the byte range: a leg conditioned on one shape of key is
    // only caught if the sample takes that shape.
    const FILLED: [&[u8]; 8] = [
        b"",
        b"0",
        b"\xff\x00",
        b"\x7f\x80\x01",
        b"cayenne\x00",
        b"cayenne-pk-bloom",
        b"cayenne-pk-bloom-probe-fingerprint-sample-key-0123456789abcdef",
        &[0xff; 33],
    ];
    // Never inserted. Their answers are almost all `false`, and each one is a
    // separate chance to notice a read side that no longer looks where the write
    // side put the bits.
    const ABSENT: [&[u8]; 4] = [b"\x01", b"absent", b"cayenne-pk-bloo", &[0x00; 17]];

    for key in FILLED {
        sample.insert(key);
    }
    let mut folded = Vec::new();
    match &sample.repr {
        PkBloomRepr::Scattered { bits, bit_mask } => {
            // Exactly the bytes the pre-frame fold produced, in that order:
            // `LEGACY_PK_BLOOM_PROBE_FINGERPRINT` pins this value, and every
            // unframed bloom on disk is validated against it.
            folded.extend_from_slice(&PK_BLOOM_NUM_HASHES.to_le_bytes());
            folded.extend_from_slice(&bit_mask.to_le_bytes());
            for word in bits {
                folded.extend_from_slice(&word.to_le_bytes());
            }
        }
        PkBloomRepr::SplitBlock { blocks, block_mask } => {
            // This layout's OWN probe count -- one bit per lane. Folding the
            // scattered `PK_BLOOM_NUM_HASHES` here would tie the two together,
            // so retuning the scattered probe count would invalidate every
            // persisted split-block filter whose probes had not changed.
            let lanes = u32::try_from(SPLIT_BLOCK_SALT.len()).unwrap_or(0);
            folded.extend_from_slice(&lanes.to_le_bytes());
            folded.extend_from_slice(&block_mask.to_le_bytes());
            for block in blocks {
                for lane in block {
                    folded.extend_from_slice(&lane.to_le_bytes());
                }
            }
        }
    }
    for key in FILLED.iter().chain(ABSENT.iter()) {
        folded.push(u8::from(sample.maybe_contains(key)));
    }
    // The crate's byte-fingerprint primitive (as used by the WAL checksum and
    // the file digest), not `pk_bloom_hash` — folding the sample with the same
    // function that filled it would let a change to that function move the bits
    // and the fold in step.
    hash_key_bytes(&[&folded])
}

/// The frame version and fingerprint this filter serializes as.
fn frame_of(repr: &PkBloomRepr) -> (u32, u64) {
    match repr {
        PkBloomRepr::Scattered { .. } => (
            PK_BLOOM_FRAME_VERSION_SCATTERED,
            *SCATTERED_PROBE_FINGERPRINT,
        ),
        PkBloomRepr::SplitBlock { .. } => (
            PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK,
            *SPLIT_BLOCK_PROBE_FINGERPRINT,
        ),
    }
}
/// The [`SCATTERED_PROBE_FINGERPRINT`] of the probe function that wrote the
/// unframed blooms — every bloom persisted before the frame existed.
///
/// FROZEN. It names one specific historical probe function, so a change to that
/// function must NOT be followed by updating this constant: the divergence is
/// exactly what stops the new reader from trusting bits the old probes placed.
/// Once the two differ, unframed blobs are rejected (→ exact-scan fallback) and
/// this constant is inert.
///
/// It grandfathers ONE transition — the blooms already on disk — rather than
/// establishing that old formats are read forever. A future
/// [`PK_BLOOM_FRAME_VERSION`] bump rejects its predecessor outright, the same
/// way [`PK_INDEX_SIDECAR_VERSION`] already treats its own. The asymmetry is
/// deliberate: rejecting a cold-file bloom costs an exact scan of a table's
/// whole cold tier on every keyset rebuild until those files are re-promoted,
/// which is worth a one-off compatibility branch in a way rebuilding one
/// sidecar checkpoint is not.
const LEGACY_PK_BLOOM_PROBE_FINGERPRINT: u64 = 0x242b_5f72_35cc_ed37;

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
    repr: PkBloomRepr,
    /// Keys inserted (observability + false-positive-rate estimation).
    pub(crate) inserted_keys: usize,
}

/// The two on-disk layouts a [`PkBloom`] can hold.
///
/// A filter's bits are the output of a specific (hash, probe-derivation, layout)
/// triple, so these are not interchangeable: bits written by one can only be
/// probed by the same one. Both are carried because persisted filters outlive a
/// deployment -- a v2 blob keeps being read by the v2 arm until whatever wrote it
/// is rewritten.
enum PkBloomRepr {
    /// Version 2. Seven probes scattered across the whole bit array, addressed by
    /// two FNV-1a passes over the key.
    Scattered {
        bits: Vec<u64>,
        /// `num_bits - 1`; `num_bits` is a power of two so indexing masks instead of mods.
        bit_mask: u64,
    },
    /// Version 3. One XXH3 selects a 256-bit block and the key sets exactly one
    /// bit in each of its eight `u32` lanes, so a probe touches one cache line and
    /// carries no per-probe branch.
    SplitBlock {
        blocks: Vec<[u32; 8]>,
        /// `num_blocks - 1`; the block count is a power of two.
        block_mask: u64,
    },
}

/// Bits per split-block block: one 256-bit block, eight `u32` lanes.
const SPLIT_BLOCK_BITS: usize = 256;

/// The Parquet/Impala salts. Eight odd constants with well-spread bit patterns,
/// so each lane's chosen bit is independent of its neighbours'.
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

impl PkBloom {
    /// Allocate a bloom whose bit array fits within `budget_bytes`.
    pub(crate) fn with_byte_budget(budget_bytes: usize) -> Self {
        Self::with_num_bits_pow2(budget_bytes.saturating_mul(8))
    }

    /// Right-size a bloom for `expected_keys` at ~10 bits/key, never exceeding
    /// `max_bytes`.
    ///
    /// Version 3 rounds the bit count UP to the next power of two; version 2
    /// rounds DOWN, which is what it has always done. That round-down is why the
    /// documented "~1% FPR" was not what the filter delivered: asking for 10
    /// bits/key and taking the largest power of two below it lands anywhere from
    /// 5.0 to 10.0 bits/key, and `PK_BLOOM_NUM_HASHES` is tuned for the 10. At
    /// 100K keys the measured rate is 12.1% rounding down and 0.76% rounding up,
    /// for the same code and the same request.
    ///
    /// Rounding up is safe for existing data even though it changes sizing:
    /// `bit_mask` is persisted per blob and restored on read, so a filter already
    /// on disk keeps probing against its own stored size whatever this returns.
    pub(crate) fn with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        let want_bits = expected_keys.saturating_mul(10);
        let cap_bits = max_bytes.saturating_mul(8).max(64);
        // Round UP, then clamp -- never past the caller's byte ceiling.
        let rounded = want_bits.checked_next_power_of_two().unwrap_or(want_bits);
        Self::with_num_bits_pow2(rounded.min(cap_bits))
    }

    /// Allocate with the largest power-of-two bit count `<= target_bits` (min 64).
    ///
    /// New filters are split-block. The scattered layout remains fully readable
    /// — every filter already on disk is one — but nothing writes it any more.
    /// Reverting that decision is a binary rollback rather than a setting, and
    /// deliberately so: an older binary rejects a split-block frame outright and
    /// falls back to the exact path, where a runtime switch would leave the
    /// filters it had already written in place and still being probed.
    pub(crate) fn with_num_bits_pow2(target_bits: usize) -> Self {
        Self::split_block_with_num_bits_pow2(target_bits)
    }

    /// The scattered layout at the PRE-FRAME sizing (round down), which is the
    /// shape every legacy bloom on disk has.
    #[cfg(test)]
    fn scattered_with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        let want_bits = expected_keys.saturating_mul(10);
        let cap_bits = max_bytes.saturating_mul(8).max(64);
        Self::scattered_with_num_bits_pow2(want_bits.min(cap_bits))
    }

    /// The scattered layout, which nothing writes any more.
    ///
    /// Retained for the fingerprint sample that validates legacy blooms, and for
    /// the tests that build the shape those blooms have.
    fn scattered_with_num_bits_pow2(target_bits: usize) -> Self {
        let num_bits: usize = 1usize << target_bits.max(64).ilog2();
        let words = (num_bits / 64).max(1);
        Self {
            repr: PkBloomRepr::Scattered {
                bits: vec![0u64; words],
                bit_mask: u64::try_from(num_bits.saturating_sub(1)).unwrap_or(u64::MAX),
            },
            inserted_keys: 0,
        }
    }

    /// The split-block layout, which every new filter uses.
    fn split_block_with_num_bits_pow2(target_bits: usize) -> Self {
        let want_blocks = (target_bits / SPLIT_BLOCK_BITS).max(1);
        let num_blocks = 1usize << want_blocks.ilog2();
        Self {
            repr: PkBloomRepr::SplitBlock {
                blocks: vec![[0u32; 8]; num_blocks],
                block_mask: u64::try_from(num_blocks.saturating_sub(1)).unwrap_or(0),
            },
            inserted_keys: 0,
        }
    }

    /// Resident bytes of the bit array, whichever layout backs it.
    pub(crate) fn size_bytes(&self) -> usize {
        match &self.repr {
            PkBloomRepr::Scattered { bits, .. } => bits.len() * 8,
            PkBloomRepr::SplitBlock { blocks, .. } => blocks.len() * 32,
        }
    }

    /// The frame version this filter serializes as.
    #[cfg(test)]
    pub(crate) fn frame_version(&self) -> u32 {
        frame_of(&self.repr).0
    }

    pub(crate) fn probe_bits(key: &[u8]) -> impl Iterator<Item = u64> {
        let h1 = pk_bloom_hash(key, 0x517c_c1b7_2722_0a95);
        // Force odd so successive probes stride across the whole bit space.
        let h2 = pk_bloom_hash(key, 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PK_BLOOM_NUM_HASHES).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)))
    }

    /// The block index and the eight per-lane masks for a key, for the
    /// split-block layout. Fixed length and branch-free so it vectorises.
    #[inline]
    fn split_block_locate(block_mask: u64, key: &[u8]) -> (usize, [u32; 8]) {
        // The crate's existing one-shot XXH3-64, the same primitive the WAL
        // checksum and the frame fingerprint use. One-shot rather than the
        // streaming `Hasher`: for a 16-byte key the streaming path's setup
        // costs more than the hash, and more than the two FNV passes it
        // replaces.
        let hash = hash_index::hash_key_bytes_oneshot(key);
        let block = usize::try_from((hash >> 32) & block_mask).unwrap_or(0);
        // The low half, deliberately: the high half already chose the block, so
        // the lanes draw on bits the block selection did not consume.
        let low = u32::try_from(hash & u64::from(u32::MAX)).unwrap_or(0);
        let mut masks = [0u32; 8];
        for (mask, salt) in masks.iter_mut().zip(SPLIT_BLOCK_SALT) {
            // Top five bits of the product pick one of the lane's 32 bits.
            *mask = 1u32 << (low.wrapping_mul(salt) >> 27);
        }
        (block, masks)
    }

    pub(crate) fn insert(&mut self, key: &[u8]) {
        match &mut self.repr {
            PkBloomRepr::Scattered { bits, bit_mask } => {
                for hash in Self::probe_bits(key) {
                    let bit = hash & *bit_mask;
                    let word = usize::try_from(bit >> 6).unwrap_or(0);
                    bits[word] |= 1u64 << (bit & 63);
                }
            }
            PkBloomRepr::SplitBlock { blocks, block_mask } => {
                let (index, masks) = Self::split_block_locate(*block_mask, key);
                let block = &mut blocks[index];
                for (lane, mask) in block.iter_mut().zip(masks) {
                    *lane |= mask;
                }
            }
        }
        self.inserted_keys = self.inserted_keys.saturating_add(1);
    }

    pub(crate) fn maybe_contains(&self, key: &[u8]) -> bool {
        match &self.repr {
            PkBloomRepr::Scattered { bits, bit_mask } => {
                for hash in Self::probe_bits(key) {
                    let bit = hash & *bit_mask;
                    let word = usize::try_from(bit >> 6).unwrap_or(0);
                    if bits[word] & (1u64 << (bit & 63)) == 0 {
                        return false;
                    }
                }
                true
            }
            PkBloomRepr::SplitBlock { blocks, block_mask } => {
                let (index, masks) = Self::split_block_locate(*block_mask, key);
                let block = &blocks[index];
                // Fold every lane rather than exiting on the first miss: the
                // branch costs more than the remaining ANDs, and the fold is
                // what vectorises.
                let mut present = true;
                for (lane, mask) in block.iter().zip(masks) {
                    present &= (*lane & mask) == mask;
                }
                present
            }
        }
    }

    /// Serialize as `magic(4) | version(4) | probe_fingerprint(8) | body`,
    /// little-endian, where the body is
    /// `bit_mask(8) | inserted_keys(8) | num_words(8) | words(8·W)` for the
    /// scattered layout and
    /// `block_mask(8) | inserted_keys(8) | num_blocks(8) | blocks(32·B)` for
    /// split-block.
    ///
    /// The header states what a reader needs in order to know it may probe
    /// these bits at all: which layout wrote them (the version) and which probe
    /// function placed them (the fingerprint).
    pub(crate) fn serialize_into(&self, out: &mut Vec<u8>) {
        let (version, fingerprint) = frame_of(&self.repr);
        out.extend_from_slice(&PK_BLOOM_FRAME_MAGIC.to_le_bytes());
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&fingerprint.to_le_bytes());
        let inserted = u64::try_from(self.inserted_keys).unwrap_or(u64::MAX);
        match &self.repr {
            PkBloomRepr::Scattered { bits, bit_mask } => {
                out.extend_from_slice(&bit_mask.to_le_bytes());
                out.extend_from_slice(&inserted.to_le_bytes());
                out.extend_from_slice(&u64::try_from(bits.len()).unwrap_or(0).to_le_bytes());
                for word in bits {
                    out.extend_from_slice(&word.to_le_bytes());
                }
            }
            PkBloomRepr::SplitBlock { blocks, block_mask } => {
                out.extend_from_slice(&block_mask.to_le_bytes());
                out.extend_from_slice(&inserted.to_le_bytes());
                out.extend_from_slice(&u64::try_from(blocks.len()).unwrap_or(0).to_le_bytes());
                for block in blocks {
                    for lane in block {
                        out.extend_from_slice(&lane.to_le_bytes());
                    }
                }
            }
        }
    }

    /// Deserialize ONE bloom from the front of `bytes`, returning it and the
    /// number of bytes it consumed — so several blooms can be read back-to-back
    /// from a sharded sidecar (each is self-describing via its count field).
    ///
    /// Reads every layout this build knows, whatever it writes: a bloom already
    /// on disk can only be probed by the layout that filled it, and there is no
    /// converting one into another — a bloom is lossy, so its members cannot be
    /// enumerated and re-inserted.
    fn deserialize_from_prefix(bytes: &[u8]) -> Option<(Self, usize)> {
        Self::deserialize_from_prefix_probed_by(bytes, *SCATTERED_PROBE_FINGERPRINT)
    }

    /// [`Self::deserialize_from_prefix`] against an explicit scattered
    /// fingerprint. Production passes the compiled-in value.
    ///
    /// The parameter exists for the UNFRAMED arm, which no serialized-byte
    /// mutation can reach: an unframed bloom records no fingerprint, so its
    /// rejection turns on this binary's own value having moved away from
    /// [`LEGACY_PK_BLOOM_PROBE_FINGERPRINT`] — not on anything in the blob. The
    /// framed arms need no such seam; a test mutates the recorded fingerprint
    /// in the bytes and goes through [`Self::from_bytes`].
    fn deserialize_from_prefix_probed_by(
        bytes: &[u8],
        scattered_fingerprint: u64,
    ) -> Option<(Self, usize)> {
        let magic = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?);
        if magic != PK_BLOOM_FRAME_MAGIC {
            // Unframed: persisted before the header existed, so it records
            // nothing about what filled it. Its bits are probeable only while
            // this binary still derives scattered probes exactly as that code
            // did. Always the scattered layout — the frame predates any other.
            if scattered_fingerprint != LEGACY_PK_BLOOM_PROBE_FINGERPRINT {
                return None;
            }
            return Self::parse_scattered_body(bytes, 0);
        }

        let version = u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?);
        let written_by = u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?);
        // The version says how to read the body; the fingerprint says whether
        // the probes that filled it are the ones this binary would use. An
        // unknown version or a mismatched fingerprint is `None` — the caller's
        // conservative fallback — never a probe of bits this binary cannot
        // place.
        match version {
            PK_BLOOM_FRAME_VERSION_SCATTERED => {
                if written_by != scattered_fingerprint {
                    return None;
                }
                Self::parse_scattered_body(bytes, PK_BLOOM_FRAME_HEADER_LEN)
            }
            PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK => {
                if written_by != *SPLIT_BLOCK_PROBE_FINGERPRINT {
                    return None;
                }
                Self::parse_split_block_body(bytes, PK_BLOOM_FRAME_HEADER_LEN)
            }
            _ => None,
        }
    }

    /// `bit_mask(8) | inserted_keys(8) | num_words(8) | words(8·W)` at
    /// `header_len`, returning the total bytes consumed including the header.
    fn parse_scattered_body(bytes: &[u8], header_len: usize) -> Option<(Self, usize)> {
        let body = bytes.get(header_len..)?;
        let bit_mask = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
        let inserted_keys = u64::from_le_bytes(body.get(8..16)?.try_into().ok()?);
        let num_words =
            usize::try_from(u64::from_le_bytes(body.get(16..24)?.try_into().ok()?)).ok()?;
        // Reject impossible word counts before allocating.
        if num_words == 0 || num_words > body.len().saturating_sub(24) / 8 {
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
            bits.push(u64::from_le_bytes(body.get(offset..end)?.try_into().ok()?));
            offset = end;
        }
        Some((
            Self {
                repr: PkBloomRepr::Scattered { bits, bit_mask },
                inserted_keys: usize::try_from(inserted_keys).unwrap_or(0),
            },
            header_len + offset,
        ))
    }

    /// `block_mask(8) | inserted_keys(8) | num_blocks(8) | blocks(32·B)` at
    /// `header_len`, returning the total bytes consumed including the header.
    fn parse_split_block_body(bytes: &[u8], header_len: usize) -> Option<(Self, usize)> {
        let body = bytes.get(header_len..)?;
        let block_mask = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
        let inserted_keys = u64::from_le_bytes(body.get(8..16)?.try_into().ok()?);
        let num_blocks =
            usize::try_from(u64::from_le_bytes(body.get(16..24)?.try_into().ok()?)).ok()?;
        // Reject impossible block counts before allocating.
        if num_blocks == 0 || num_blocks > body.len().saturating_sub(24) / 32 {
            return None;
        }
        // The block count is a power of two, and `block_mask` selects within it.
        if !num_blocks.is_power_of_two()
            || u64::try_from(num_blocks.saturating_sub(1)).ok()? != block_mask
        {
            return None;
        }
        let mut blocks = Vec::with_capacity(num_blocks);
        let mut offset = 24usize;
        for _ in 0..num_blocks {
            let mut block = [0u32; 8];
            for lane in &mut block {
                let end = offset.checked_add(4)?;
                *lane = u32::from_le_bytes(body.get(offset..end)?.try_into().ok()?);
                offset = end;
            }
            blocks.push(block);
        }
        Some((
            Self {
                repr: PkBloomRepr::SplitBlock { blocks, block_mask },
                inserted_keys: usize::try_from(inserted_keys).unwrap_or(0),
            },
            header_len + offset,
        ))
    }

    /// Serialize this bloom standalone (the [`Self::serialize_into`] frame with
    /// no sidecar wrapper around it) for embedding one bloom per cold-tier
    /// manifest row (`ColdTierFile::pk_bloom`).
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.serialize_into(&mut out);
        out
    }

    /// Inverse of [`Self::to_bytes`]: parse ONE bloom from `bytes`, ignoring any
    /// trailing bytes. Returns `None` on a corrupt/short frame, an unknown
    /// format version, or bits this binary's probe function did not place, so
    /// the caller falls back to the exact cold scan.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        Self::deserialize_from_prefix(bytes).map(|(bloom, _)| bloom)
    }
}

/// Upper bound on ONE cold file's persisted PK bloom. A file whose right-sized
/// bloom (~10 bits/key) would exceed this is stored with no bloom (`None`), so
/// the keyset rebuild falls back to the exact cold scan for the whole table
/// rather than bloating the manifest/snapshot. ~32 MiB covers ~26M keys/file;
/// promotion additionally row-caps output files so they stay under this budget.
pub(crate) const COLD_PK_BLOOM_PER_FILE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Table-global cold-tier PK existence view: one [`PkBloom`] per live cold file
/// (from the `cayenne_cold_tier_file` manifest), probed at CDC-upsert
/// conflict-detection time so a re-ingested cold-resident key records a
/// supersede tombstone WITHOUT scanning the cold object store.
///
/// No false negatives (a live cold key is never missed); a false positive is a
/// harmless redundant key-delete under upsert. Never consulted for `DoNothing`
/// (a false positive would wrongly drop a genuinely new row). Blooms stay a
/// list, not a union, because each is right-sized to its file's key count.
pub(crate) struct ColdPkExistence {
    blooms: Vec<PkBloom>,
}

impl ColdPkExistence {
    pub(crate) fn new(blooms: Vec<PkBloom>) -> Self {
        Self { blooms }
    }

    /// `true` if `key` MAY live in any cold file (definitely absent when every
    /// file's bloom misses — blooms have no false negatives).
    pub(crate) fn maybe_contains(&self, key: &[u8]) -> bool {
        self.blooms.iter().any(|b| b.maybe_contains(key))
    }

    /// Approximate resident bytes across all per-file blooms, for logging.
    pub(crate) fn approx_bytes(&self) -> usize {
        self.blooms
            .iter()
            .map(PkBloom::size_bytes)
            .fold(0, usize::saturating_add)
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
///
/// Framing the blooms themselves did NOT need a bump: each bloom states its own
/// layout, so a version-2 sidecar full of unframed blooms still reads (no forced
/// rebuild on upgrade), and a version-2 sidecar full of framed blooms is
/// rejected deterministically by an older binary — its `num_words` check reads
/// this bloom's `bit_mask` as a word count, which no frame length can satisfy.
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
    // self-describing and consumes >= 32 bytes (an unframed bloom's 24-byte body
    // + >= one 8-byte word; a framed one is 16 bytes larger still), so a `count`
    // larger than the remaining bytes can encode means a
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
            Self::Bloom(bloom) => bloom.size_bytes(),
        }
    }

    /// Representation name for the `kind` dimension of
    /// `cayenne_pk_index_discard_total`.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Exact(_) => "exact",
            Self::Bloom(_) => "bloom",
        }
    }
}

/// One committed key batch held while a PK existence index was checked out of
/// its cache. Carries the recorded [`RowLocation`] and commit sequence verbatim
/// so replaying it into the restored index is byte-identical to having recorded
/// it directly.
struct PendingPkKeyBatch {
    keys: PkDigestSet,
    location: RowLocation,
    sequence: i64,
}

/// Divisor applied to a PK cache's byte budget to bound its pending-key log.
/// The log is transient (it lives only while an index is checked out) and holds
/// the same per-key payload as the cache, so a quarter keeps the worst case a
/// fraction of the index it protects.
const PENDING_PK_KEYS_BUDGET_DIVISOR: usize = 4;

/// Keys committed by other writers while a PK existence index was checked out
/// of its cache for validation.
///
/// A validation stream takes the shared index out of its cache cell and holds it
/// for the whole lazily-consumed stream, so a writer that commits during that
/// window finds the cell empty. Recording its keys there is a no-op, and an
/// existence entry that never lands leaves the index a strict UNDER-approximation
/// of the live rows — a later upsert probes the restored index, misses, and
/// classifies the key as new, so it emits no supersede and the table ends up with
/// two live rows for one primary key. (Over-approximation is the safe direction:
/// a stale-present entry only costs a redundant tombstone.)
///
/// The keys are therefore held here instead, and are both:
/// - merged into the index when it is restored, so the cache regains every key, and
/// - probed by the in-flight validation that holds the checked-out index, so a key
///   committed mid-stream is not read as a new primary key by the writer whose
///   snapshot predates it.
///
/// Bounded by a byte cap. Past it the log stops recording, its snapshot reports
/// [`PendingPkExistence::is_incomplete`], and the restore discards the index
/// (forcing an authoritative rebuild) rather than caching one that is silently
/// missing keys.
#[derive(Default)]
pub(crate) struct PendingPkKeys {
    /// How many indexes are currently checked out over this cache. Distinguishes
    /// "checked out" from "no cache at all": a cold cache needs no log, because the
    /// next validation rebuilds the index from the table and sees every committed
    /// key. Normally 0 or 1 (writers are serialized by the table write lock); see
    /// [`Self::begin_checkout`] for what a second one means.
    outstanding: usize,
    batches: Vec<PendingPkKeyBatch>,
    approx_bytes: usize,
    /// The log hit its byte cap and stopped recording, so it no longer holds every
    /// key committed during the checkout.
    overflowed: bool,
    /// The cache was invalidated while the index was checked out, so the index
    /// describes a table state that has since been superseded (a DELETE, a
    /// compaction, a recovery) and must not be cached when it comes back.
    invalidated: bool,
}

impl PendingPkKeys {
    /// Byte cap for a log protecting a cache with `cache_budget` bytes.
    pub(crate) fn budget_from_cache_budget(cache_budget: usize) -> usize {
        cache_budget / PENDING_PK_KEYS_BUDGET_DIVISOR
    }

    /// Open a checkout window: keys committed from here until [`Self::end_checkout`]
    /// are recorded. Any residue from an abandoned checkout is dropped — that index
    /// was never stored back, so the next validation rebuilds from the table and
    /// already sees those keys.
    ///
    /// Opening a SECOND window while one is outstanding puts two independently-aged
    /// indexes over one cache: each was read at a different point, so whichever is
    /// stored last silently reverts the other's keys. Neither is trustworthy, so both
    /// are marked for discard and the cache goes cold — one rebuild instead of a
    /// cache that answers "absent" for a live key. Writers are serialized by the
    /// table write lock, so this is a backstop, not a routine path.
    pub(crate) fn begin_checkout(&mut self) {
        if self.outstanding == 0 {
            self.overflowed = false;
            self.invalidated = false;
        } else {
            self.invalidated = true;
        }
        self.batches.clear();
        self.approx_bytes = 0;
        self.outstanding = self.outstanding.saturating_add(1);
    }

    /// Report that the cache was invalidated while an index was checked out, so the
    /// restore drops that index instead of resurrecting a superseded one. A no-op
    /// when nothing is checked out — there is then no index in flight, and the flag
    /// would otherwise leak into the next checkout.
    pub(crate) fn invalidate(&mut self) {
        if self.outstanding == 0 {
            return;
        }
        self.invalidated = true;
        self.batches.clear();
        self.approx_bytes = 0;
    }

    /// Hold one committed key batch. A no-op when no index is checked out, or once
    /// the log has stopped recording. Callers read [`Self::approx_bytes`] around the
    /// call to account the change.
    pub(crate) fn record(
        &mut self,
        keys: &PkDigestSet,
        location: &RowLocation,
        sequence: i64,
        max_bytes: usize,
    ) {
        if self.outstanding == 0 || self.overflowed || self.invalidated {
            return;
        }
        let batch_bytes = keys
            .iter()
            .map(approx_pk_keyset_entry_bytes)
            .fold(0, usize::saturating_add);
        if self.approx_bytes.saturating_add(batch_bytes) > max_bytes {
            // Stop holding keys rather than grow without bound, and release the ones
            // already held: the index this log protects is now unrecoverable either
            // way, and `end_checkout` reports that so it is discarded instead of
            // being stored back missing entries.
            self.overflowed = true;
            self.batches.clear();
            self.approx_bytes = 0;
            return;
        }
        self.approx_bytes = self.approx_bytes.saturating_add(batch_bytes);
        self.batches.push(PendingPkKeyBatch {
            keys: keys.clone(),
            location: location.clone(),
            sequence,
        });
    }

    /// Close the checkout window and hand back everything committed during it. With
    /// several windows outstanding every one of them reports a discard, and the flags
    /// only reset once the last closes.
    pub(crate) fn end_checkout(&mut self) -> RestoredPkKeys {
        let restored = RestoredPkKeys {
            batches: std::mem::take(&mut self.batches),
            discard_index: self.overflowed || self.invalidated,
            overflowed: self.overflowed,
            invalidated: self.invalidated,
        };
        self.approx_bytes = 0;
        self.outstanding = self.outstanding.saturating_sub(1);
        if self.outstanding == 0 {
            self.overflowed = false;
            self.invalidated = false;
        }
        restored
    }

    /// Existence view over the keys held so far, for the validation that holds the
    /// checked-out index. `None` when nothing was committed during this checkout —
    /// the overwhelmingly common case, which costs one uncontended lock.
    pub(crate) fn existence(&self) -> Option<PendingPkExistence> {
        if self.batches.is_empty() && !self.overflowed {
            return None;
        }
        let capacity = self
            .batches
            .iter()
            .map(|batch| batch.keys.len())
            .fold(0, usize::saturating_add);
        let mut locations: HashMap<u128, RowLocation, PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(capacity, PrehashedBuildHasher);
        // Later batches win: a key committed twice during the checkout lives where
        // its most recent commit put it.
        for batch in &self.batches {
            for (digest, _) in batch.keys.iter_with_digest() {
                locations.insert(digest, batch.location.clone());
            }
        }
        Some(PendingPkExistence {
            locations,
            incomplete: self.overflowed,
        })
    }

    /// Bytes currently held, for memory accounting.
    pub(crate) fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }
}

/// Keys committed while an index was checked out, handed to the restore.
pub(crate) struct RestoredPkKeys {
    batches: Vec<PendingPkKeyBatch>,
    discard_index: bool,
    /// Which of the two independent conditions forced the discard, for the
    /// `cayenne_pk_index_discard_total` reason dimension. The two have different
    /// remedies — a log that outgrew its cap is a sizing/representation problem,
    /// an invalidation is a compaction/snapshot-rewrite problem — and only one
    /// counter can tell them apart, so the flags are carried separately rather
    /// than collapsed into `discard_index`.
    overflowed: bool,
    invalidated: bool,
}

impl RestoredPkKeys {
    /// Whether the index that was checked out must be dropped rather than cached:
    /// keys committed during the checkout went unheld (the log hit its cap), or the
    /// cache was invalidated while the index was out. Caching it either way would
    /// answer "absent" for a live key, which reads as a new primary key.
    pub(crate) fn index_must_be_discarded(&self) -> bool {
        self.discard_index
    }

    /// Why the index must be dropped, as the `reason` dimension of
    /// `cayenne_pk_index_discard_total`. `None` when it may be cached.
    pub(crate) fn discard_reason(&self) -> Option<&'static str> {
        match (self.overflowed, self.invalidated) {
            (true, true) => Some("overflowed_and_invalidated"),
            (true, false) => Some("overflowed"),
            (false, true) => Some("invalidated"),
            (false, false) => None,
        }
    }

    /// Replay every held batch, oldest first, so a key committed twice ends on its
    /// most recent location and sequence.
    pub(crate) fn batches(&self) -> impl Iterator<Item = (&PkDigestSet, &RowLocation, i64)> {
        self.batches
            .iter()
            .map(|batch| (&batch.keys, &batch.location, batch.sequence))
    }
}

/// Snapshot of a [`PendingPkKeys`] log handed to per-batch validation alongside
/// the checked-out index, so an index miss can still see a concurrent commit.
pub(crate) struct PendingPkExistence {
    locations: HashMap<u128, RowLocation, PrehashedBuildHasher>,
    incomplete: bool,
}

impl PendingPkExistence {
    /// Where a key committed during this checkout lives, or `None` if no such key
    /// was recorded.
    pub(crate) fn location_by_digest(&self, digest: u128) -> Option<&RowLocation> {
        self.locations.get(&digest)
    }

    /// Whether keys committed during this checkout went unrecorded, so a miss here
    /// does not prove the key is absent from the table.
    pub(crate) fn is_incomplete(&self) -> bool {
        self.incomplete
    }
}

/// Streaming, budget-bounded builder for the cold PK-index rebuild
/// (`load_existing_pk_index`). Routes every scanned key to its shard
/// (`shard_of_pk`) as it arrives and, when a byte budget is set (upsert tables,
/// where a bloom is a sound existence answer), degrades the accumulating exact
/// keysets to bounded per-shard blooms the moment the total would exceed it.
///
/// One pass, memory bounded by `max_bytes`: the rebuild never materializes an
/// over-budget exact keyset only to re-shard it (`ShardedPkIndex::from_exact`)
/// or throw it away for blooms — the O(table-rows) second pass and unbounded
/// allocation that stalled the first CDC apply after a large initial snapshot.
///
/// `max_bytes: None` never degrades — `on_conflict: do-nothing` needs an exact
/// answer (a bloom false positive would wrongly drop a genuinely new row).
pub(crate) struct BoundedShardedPkIndexBuilder {
    /// Exact per-shard keysets; drained into `blooms` on budget overflow.
    shards: Vec<CachedPkKeyset>,
    /// `Some` once degraded: bounded per-shard blooms (upsert-only superset).
    blooms: Option<Vec<PkBloom>>,
    /// Total byte budget across all shards; `None` = exact answer required.
    max_bytes: Option<usize>,
    /// Live rows the scan feeding this builder is expected to yield, when the plan
    /// could report it. Sizes the blooms a degrade produces: without it the degrade
    /// can only take the whole budget split, which on a large table over-allocates
    /// by an order of magnitude.
    expected_keys: Option<usize>,
}

impl BoundedShardedPkIndexBuilder {
    pub(crate) fn new(shard_count: usize, max_bytes: Option<usize>) -> Self {
        let n = shard_count.max(1);
        Self {
            shards: (0..n)
                .map(|_| CachedPkKeyset::with_capacity(1024))
                .collect(),
            blooms: None,
            max_bytes,
            expected_keys: None,
        }
    }

    /// Tell the builder how many live rows the scan will yield, so a degrade can
    /// right-size its blooms instead of claiming the whole byte budget.
    #[must_use]
    pub(crate) fn with_expected_keys(mut self, expected_keys: Option<usize>) -> Self {
        self.expected_keys = expected_keys;
        self
    }

    /// Shard routing in exact mode (bloom mode routes by `blooms.len()` at the
    /// call site — `shards` is drained once degraded).
    #[inline]
    fn shard_of(&self, key: &OwnedRow) -> usize {
        shard_of_pk(key.as_ref(), self.shards.len())
    }

    /// Keys retained so far. In bloom mode this is the inserted-key count
    /// (observability only — blooms retain no entries).
    pub(crate) fn len(&self) -> usize {
        match &self.blooms {
            Some(blooms) => blooms.iter().map(|b| b.inserted_keys).sum(),
            None => self.shards.iter().map(CachedPkKeyset::len).sum(),
        }
    }

    /// Insert `key`, overwriting an existing entry's location (the scan-order
    /// override semantics of `CachedPkKeyset::insert`).
    pub(crate) fn insert(&mut self, key: OwnedRow, location: RowLocation) {
        if let Some(blooms) = &mut self.blooms {
            let s = shard_of_pk(key.as_ref(), blooms.len());
            blooms[s].insert(key.as_ref());
            return;
        }
        let s = self.shard_of(&key);
        self.shards[s].insert(key, location);
        self.degrade_if_over_budget();
    }

    /// Insert `key` only when absent, preserving an existing entry's location
    /// (`CachedPkKeyset::insert_if_absent` — the mem-tier fold semantics). In
    /// bloom mode a plain bloom insert (idempotent superset).
    pub(crate) fn insert_if_absent(&mut self, key: OwnedRow, location: RowLocation) {
        if let Some(blooms) = &mut self.blooms {
            let s = shard_of_pk(key.as_ref(), blooms.len());
            blooms[s].insert(key.as_ref());
            return;
        }
        let s = self.shard_of(&key);
        self.shards[s].insert_if_absent(key, location);
        self.degrade_if_over_budget();
    }

    /// Raise every retained entry's OCC sequence to at least `sequence` (the
    /// end-of-scan high-water; see `CachedPkKeyset::stamp_all_sequences_min`).
    /// No-op once degraded — bloomed tables fall back to per-table OCC.
    pub(crate) fn stamp_all_sequences_min(&mut self, sequence: i64) {
        if self.blooms.is_none() {
            for shard in &mut self.shards {
                shard.stamp_all_sequences_min(sequence);
            }
        }
    }

    fn degrade_if_over_budget(&mut self) {
        let Some(max_bytes) = self.max_bytes else {
            return;
        };
        let total: usize = self
            .shards
            .iter()
            .map(|k| k.approx_bytes)
            .fold(0, usize::saturating_add);
        if total <= max_bytes {
            return;
        }
        // Shard keysets are already routed by `shard_of_pk`, so shard i's keys
        // drain into bloom i directly. Each bloom gets an even split of the
        // budget; `with_byte_budget` floors every bloom at 64 bits, so a
        // pathologically small split (under 8 bytes per shard) still allocates
        // a usable filter, overshooting the budget by at most 8 bytes per
        // shard rather than degrading to an always-positive zero-bit bloom.
        //
        // Prefer sizing for the rows the scan will actually yield. Taking the whole
        // budget split is blind to cardinality and over-allocates by an order of
        // magnitude on a large table: measured at SF1000, a rebuilt `order_line`
        // filter landed at ~113 bits per key against the ~10 `with_expected_keys`
        // targets, which is where ~110 GB of resident memory went. The budget split
        // stays the CEILING, and `with_expected_keys` rounds the bit count UP to a
        // power of two, so the round-up supplies the slack for rows committed while
        // the scan runs. No hint (the plan could not report a row count) keeps the
        // previous behaviour.
        let n = self.shards.len();
        let per_shard_budget = max_bytes / n;
        // Floor the hint by what this shard must hold RIGHT NOW. The hint is a live
        // count sampled elsewhere, so mid-load it can lag far behind the keys already
        // in hand, and sizing a filter below its own contents is the one way this can
        // do real harm: a filter that small answers "present" for nearly everything,
        // which pushes every incoming row onto the full validation path.
        let per_shard_expected = self.expected_keys.map(|keys| keys.div_ceil(n));
        let mut blooms: Vec<PkBloom> = self
            .shards
            .iter()
            .map(|keyset| match per_shard_expected {
                Some(expected) => {
                    PkBloom::with_expected_keys(expected.max(keyset.len()), per_shard_budget)
                }
                None => PkBloom::with_byte_budget(per_shard_budget),
            })
            .collect();
        for (shard, bloom) in self.shards.drain(..).zip(&mut blooms) {
            for key in shard.rows() {
                bloom.insert(key.as_ref());
            }
        }
        tracing::debug!(
            max_bytes,
            shards = n,
            "cold PK-index rebuild exceeded the keyset byte budget; continuing into bounded per-shard blooms"
        );
        self.blooms = Some(blooms);
    }

    /// Finish the build. `Exact` shards keep their per-key locations and OCC
    /// stamps; `Bloom` is the bounded existence superset.
    pub(crate) fn finish(self) -> ShardedPkIndex {
        match self.blooms {
            Some(blooms) => ShardedPkIndex::Bloom(blooms.into_boxed_slice()),
            None => ShardedPkIndex::Exact(self.shards.into_boxed_slice()),
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
    /// Bloom load in milli-bits-per-inserted-key, summed across shards; `None` for
    /// an exact index. This is the quantity that governs a bloom's false-positive
    /// rate: bits are only ever SET, so every insert raises the load and no deletion
    /// lowers it. `PkBloom` is sized for ~10 bits/key, so `10_000` here is the design
    /// point and a falling number is a filter saturating toward useless.
    pub(crate) fn bloom_load_milli(&self) -> Option<u64> {
        let Self::Bloom(blooms) = self else {
            return None;
        };
        let bits: u64 = blooms
            .iter()
            .map(|b| (b.size_bytes() as u64).saturating_mul(8))
            .fold(0, u64::saturating_add);
        let keys: u64 = blooms
            .iter()
            .map(|b| b.inserted_keys as u64)
            .fold(0, u64::saturating_add);
        Some(bits.saturating_mul(1000) / keys.max(1))
    }

    /// Keys inserted across every shard bloom; `None` for an exact index.
    pub(crate) fn bloom_inserted_keys(&self) -> Option<u64> {
        let Self::Bloom(blooms) = self else {
            return None;
        };
        Some(
            blooms
                .iter()
                .map(|b| b.inserted_keys as u64)
                .fold(0, u64::saturating_add),
        )
    }

    /// Representation name for the `kind` dimension of
    /// `cayenne_pk_index_discard_total`. A sharded index is wholly one or the
    /// other — degrading converts every shard — so one name covers it.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Exact(_) => "exact",
            Self::Bloom(_) => "bloom",
        }
    }

    /// Partition an exact keyset into `n` per-shard keysets by `shard_of_pk` on
    /// each key's `OwnedRow` bytes (§3.5). Bloom-path indices are built sharded at
    /// load time instead — a combined bloom can't be partitioned (its keys are
    /// unrecoverable), so per-shard blooms are constructed by routing keys to N
    /// blooms during `load_existing_pk_index` / `try_load_persisted_pk_index`.
    pub(crate) fn from_exact(keyset: CachedPkKeyset, n: usize) -> Self {
        let n = n.max(1);
        // At n == 1 every key routes to shard 0, so wrap the keyset directly —
        // re-inserting each entry would be an O(rows) pass for nothing (the
        // persisted-checkpoint load on the serial path takes this branch).
        if n == 1 {
            return Self::Exact(vec![keyset].into_boxed_slice());
        }
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
                .map(PkBloom::size_bytes)
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
    /// per-insert over-budget exact→bloom conversion — the `Exact`/`Bloom` variant
    /// is table-global, so one shard cannot convert while its siblings are still
    /// appending under their own publish locks. The sharded path instead recomputes
    /// the tally ONCE after all per-shard appends and enforces the budget there
    /// (step 6 of `validate_and_append_sharded`, via
    /// [`ShardedPkIndex::degrade_to_blooms`]; upsert tables only, for the reasons
    /// documented at that call site).
    pub(crate) fn record_keys_in_shard(
        &mut self,
        shard: usize,
        keys: &PkDigestSet,
        location: &RowLocation,
    ) {
        match self {
            Self::Exact(keysets) => {
                if let Some(keyset) = keysets.get_mut(shard) {
                    // Reuse each key's stored digest and fold the presence
                    // check into the insert itself (`max_bytes: usize::MAX`
                    // so `OverBudget` never fires — this path recomputes the
                    // byte tally once after all shards, per the doc above) —
                    // one hash lookup per key, and no clone on the (common,
                    // re-touched-PK) present branch.
                    for (digest, key) in keys.iter_with_digest() {
                        let _ = keyset.try_insert_with_digest(
                            digest,
                            key,
                            location.clone(),
                            usize::MAX,
                        );
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
    /// Insert with the byte budget enforced DURING the loop, returning whether
    /// the index stayed inside it.
    ///
    /// The previous shape inserted every key with `usize::MAX` and left the
    /// caller to reconcile afterwards, which made the budget a trim rather than
    /// an admission control: the peak is `batch_keys x entry_bytes` with no
    /// ceiling, and each entry retains a cloned `OwnedRow` alongside its digest,
    /// location and sequence. At SF-1000 a heap profile attributed ~14.5 GiB to
    /// this path against a 256 MiB per-table default — 58x the budget, and the
    /// budget was doing exactly what it was written to do, just too late.
    ///
    /// The tally is re-read every [`BUDGET_RECHECK_KEYS`] keys rather than per
    /// key: `approx_bytes` is O(shards) over cached per-keyset totals, so it is
    /// cheap but not free, and a bound that only has to stop unbounded growth
    /// does not need to be exact. Overshoot is therefore bounded by one chunk
    /// instead of one batch.
    ///
    /// Returning `false` rather than degrading here keeps the policy with the
    /// caller: an upsert table can fall back to blooms (a false positive is a
    /// harmless redundant delete) while `DoNothing` needs exactness and must
    /// drop the index instead.
    pub(crate) fn record_keys_bounded(
        &mut self,
        keys: &PkDigestSet,
        location: &RowLocation,
        max_bytes: usize,
    ) -> bool {
        /// Keys between budget re-reads. Small enough that a wide batch cannot
        /// overshoot far, large enough to keep the sum out of the hot loop.
        const BUDGET_RECHECK_KEYS: usize = 512;

        let n = self.shard_count();
        match self {
            Self::Exact(keysets) => {
                let tally = |keysets: &[CachedPkKeyset]| {
                    keysets
                        .iter()
                        .map(|k| k.approx_bytes)
                        .fold(0, usize::saturating_add)
                };
                if tally(keysets) > max_bytes {
                    return false;
                }
                let mut since_check = 0usize;
                for (digest, key) in keys.iter_with_digest() {
                    let shard = shard_of_pk(key.as_ref(), n);
                    if let Some(keyset) = keysets.get_mut(shard) {
                        // Still `usize::MAX` per insert: the per-keyset cap would
                        // bound one shard, and the budget being enforced here is
                        // the table-global one across all of them.
                        let _ = keyset.try_insert_with_digest(
                            digest,
                            key,
                            location.clone(),
                            usize::MAX,
                        );
                    }
                    since_check = since_check.saturating_add(1);
                    if since_check >= BUDGET_RECHECK_KEYS {
                        since_check = 0;
                        if tally(keysets) > max_bytes {
                            return false;
                        }
                    }
                }
                tally(keysets) <= max_bytes
            }
            // Blooms are allocated at a fixed size, so recording into them
            // cannot grow the index past its budget.
            Self::Bloom(blooms) => {
                for key in keys.iter() {
                    let shard = shard_of_pk(key.as_ref(), n);
                    if let Some(bloom) = blooms.get_mut(shard) {
                        bloom.insert(key.as_ref());
                    }
                }
                true
            }
        }
    }

    /// Record every key of a batch into an already-degraded bloom index.
    ///
    /// MUST be called after [`Self::degrade_to_blooms`] when the degrade was
    /// triggered by [`Self::record_keys_bounded`] returning `false`. That stops
    /// at the budget, so the keys after the stop were never inserted, and
    /// `degrade_to_blooms` only converts what the keysets already hold — leaving
    /// the rest of the batch absent from the bloom.
    ///
    /// An absent key is a FALSE NEGATIVE. Under upsert that reads as "this PK is
    /// new" and writes a duplicate live row, which is the one failure the bloom
    /// fallback is documented never to cause (a false POSITIVE is merely a
    /// redundant delete). The single-keyset path has always re-inserted the full
    /// batch after converting; this is the sharded equivalent.
    ///
    /// Cheap and unconditional: blooms are fixed-size, so re-inserting keys
    /// already present costs a hash and a few bit sets, and no memory.
    pub(crate) fn record_keys_after_degrade(&mut self, keys: &PkDigestSet) {
        let n = self.shard_count();
        match self {
            Self::Bloom(blooms) => {
                for key in keys.iter() {
                    let shard = shard_of_pk(key.as_ref(), n);
                    if let Some(bloom) = blooms.get_mut(shard) {
                        bloom.insert(key.as_ref());
                    }
                }
            }
            // Not degraded, so `record_keys_bounded` admitted the whole batch
            // and there is nothing to backfill.
            Self::Exact(_) => {}
        }
    }

    /// Convert every exact shard keyset into a byte-budgeted bloom (no-op on
    /// an already-bloomed index). The budget backstop for the maintained
    /// index: exact keysets grow with every recorded key, and a caller whose
    /// running total exceeds its byte budget degrades here instead of growing
    /// unbounded. Safe only under upsert semantics (a bloom false positive
    /// yields a harmless redundant delete) — the caller gates on that.
    pub(crate) fn degrade_to_blooms(
        &mut self,
        per_shard_max_bytes: usize,
        expected_keys: Option<usize>,
    ) {
        self.degrade_to_blooms_observed(per_shard_max_bytes, expected_keys, |_, _| {});
    }

    /// [`Self::degrade_to_blooms`], reporting each shard as it converts.
    ///
    /// `observe` receives the shard just converted and the exact bytes still
    /// held across every keyset. That the figure falls at each step — rather
    /// than staying at the full exact total until the last shard — is the
    /// difference between releasing as the conversion goes and releasing at the
    /// end, and it is not visible in the post-state the two share. Production
    /// callers pass a no-op.
    fn degrade_to_blooms_observed(
        &mut self,
        per_shard_max_bytes: usize,
        expected_keys: Option<usize>,
        mut observe: impl FnMut(usize, usize),
    ) {
        if let Self::Exact(keysets) = self {
            let n_shards = keysets.len().max(1);
            let mut exact_bytes_held = keysets
                .iter()
                .map(|k| k.approx_bytes)
                .fold(0, usize::saturating_add);
            let blooms: Vec<PkBloom> = keysets
                .iter_mut()
                .enumerate()
                .map(|(shard, keyset)| {
                    // Size per shard for the table's LIVE cardinality when it is
                    // known. Falling back to the conversion-time key count times four
                    // under-sizes badly whenever the degrade fires mid-fill: those
                    // keys are only the ones that FIT the budget, so on a table many
                    // times larger the "4x headroom" is really making up a 10x
                    // shortfall — measured at SF1000 on `order_line` as ~6.4 bits per
                    // key against the ~10 this call targets. The budget split stays
                    // the cap either way.
                    let expected = expected_keys.map_or_else(
                        || keyset.len().saturating_mul(4),
                        |keys| keys.div_ceil(n_shards).max(keyset.len()),
                    );
                    let mut bloom = PkBloom::with_expected_keys(expected, per_shard_max_bytes);
                    for key in keyset.rows() {
                        bloom.insert(key.as_ref());
                    }
                    // Release this shard's exact entries as soon as its bloom
                    // exists. Building every bloom first and dropping the
                    // keysets at the end would peak at exact + blooms together,
                    // which is the worst moment to need extra memory: this
                    // conversion only runs because the budget was already hit.
                    exact_bytes_held = exact_bytes_held.saturating_sub(keyset.approx_bytes);
                    *keyset = CachedPkKeyset::with_capacity(0);
                    observe(shard, exact_bytes_held);
                    bloom
                })
                .collect();
            *self = Self::Bloom(blooms.into_boxed_slice());
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

#[cfg(test)]
mod tests {
    use super::{
        BoundedShardedPkIndexBuilder, COLD_PK_BLOOM_PER_FILE_MAX_BYTES, CachedPkKeyset,
        ColdPkExistence, LEGACY_PK_BLOOM_PROBE_FINGERPRINT, PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK,
        PK_INDEX_SIDECAR_MAGIC, PK_INDEX_SIDECAR_VERSION, PkBloom, PkBloomRepr, PkDigestSet,
        PkKeysetInsertOutcome, RowLocation, SCATTERED_PROBE_FINGERPRINT, ShardedPkIndex,
        approx_pk_keyset_entry_bytes, deserialize_pk_bloom_sidecar, deserialize_pk_blooms_sidecar,
        pk_digest, serialize_pk_blooms_sidecar, shard_of_pk,
    };

    /// Degrading after a mid-batch stop must not lose the rest of the batch.
    ///
    /// `record_keys_bounded` stops once the budget is reached, so the keys after
    /// the stop were never inserted. `degrade_to_blooms` only converts what the
    /// keysets already hold, so those keys would be absent from the bloom — and
    /// an absent key is a FALSE NEGATIVE, which under upsert reads as "this PK
    /// is new" and writes a duplicate live row. The single-keyset path has
    /// always re-inserted the full batch after converting; the sharded path must
    /// match that contract.
    ///
    /// A bloom may answer `true` for a key it never saw; it must never answer
    /// `false` for one it did.
    /// A degrade sizes its blooms for the rows the scan will yield, not for the
    /// whole byte budget.
    ///
    /// Regression: `degrade_if_over_budget` allocated `max_bytes / n` per shard
    /// regardless of cardinality, so a table far smaller than its budget still got a
    /// budget-sized filter. Measured at SF1000 that was ~113 bits per key against a
    /// ~10 target, and ~110 GB of resident memory. The budget split must remain the
    /// ceiling, so the no-hint path is asserted alongside it.
    #[test]
    fn a_degrade_sizes_its_blooms_for_the_expected_key_count() {
        // The budget has to be small enough that these keys exceed it as an exact
        // keyset (~110 B/entry, so ~330 KB) and large enough that its split still
        // dwarfs a right-sized filter: 3k keys at ~10 bits/key rounds to 4 KiB,
        // against a 64 KiB budget split.
        const BUDGET: usize = 64 * 1024;
        const KEYS: usize = 3_000;

        let build = |expected: Option<usize>| {
            let mut builder =
                BoundedShardedPkIndexBuilder::new(1, Some(BUDGET)).with_expected_keys(expected);
            for i in 0..KEYS as u64 {
                builder.insert(owned_key(&key(i)), RowLocation::FileUnlocated);
            }
            builder
        };

        let sized = build(Some(KEYS)).finish();
        let unsized_ = build(None).finish();
        let bytes = |index: &ShardedPkIndex| match index {
            ShardedPkIndex::Bloom(blooms) => blooms.iter().map(PkBloom::size_bytes).sum::<usize>(),
            ShardedPkIndex::Exact(_) => panic!("the budget was exceeded, so it must have degraded"),
        };
        let (sized_bytes, unsized_bytes) = (bytes(&sized), bytes(&unsized_));

        assert!(
            sized_bytes < unsized_bytes / 8,
            "a sized degrade must be far smaller than a budget-sized one: {sized_bytes} vs {unsized_bytes}"
        );
        assert!(
            sized_bytes * 8 >= KEYS * 10,
            "and still at least ~10 bits/key: {sized_bytes} bytes for {KEYS} keys"
        );
        assert!(
            unsized_bytes <= BUDGET,
            "the budget split stays the ceiling: {unsized_bytes} > {BUDGET}"
        );
    }

    #[test]
    fn degrading_after_a_mid_batch_stop_still_records_every_key() {
        let keysets: Vec<CachedPkKeyset> =
            (0..4).map(|_| CachedPkKeyset::with_capacity(0)).collect();
        let mut index = ShardedPkIndex::Exact(keysets.into_boxed_slice());

        let mut keys = PkDigestSet::with_capacity(8_000);
        for i in 0..8_000u64 {
            let k = owned_key(&key(i));
            keys.insert_with_digest(pk_digest(&k), k);
        }
        // Tight enough that the insert stops long before the batch ends.
        let one_entry = approx_pk_keyset_entry_bytes(&owned_key(&key(0)));
        let max_bytes = one_entry.saturating_mul(500);

        let within = index.record_keys_bounded(&keys, &RowLocation::FileUnlocated, max_bytes);
        assert!(
            !within,
            "this batch must exceed the budget for the test to mean anything"
        );

        let per_shard = max_bytes / index.shard_count().max(1);
        index.degrade_to_blooms(per_shard, None);
        index.record_keys_after_degrade(&keys);

        let n = index.shard_count();
        match &index {
            ShardedPkIndex::Bloom(blooms) => {
                for k in keys.iter() {
                    let shard = shard_of_pk(k.as_ref(), n);
                    assert!(
                        blooms[shard].maybe_contains(k.as_ref()),
                        "every key in the batch must survive degradation; a false negative \
                         here is a duplicate live row under upsert"
                    );
                }
            }
            ShardedPkIndex::Exact(_) => panic!("degrade_to_blooms must leave a bloom index"),
        }
    }

    /// The budget must stop growth DURING the insert, not after it.
    ///
    /// The previous shape recorded the whole batch with `usize::MAX` and
    /// reconciled afterwards, so the peak was `batch_keys x entry_bytes` with no
    /// ceiling regardless of the configured budget — the mechanism behind ~14.5
    /// GiB against a 256 MiB default at SF-1000. This asserts the index stops
    /// near the budget rather than at the end of the batch.
    #[test]
    fn a_batch_over_the_budget_stops_inside_it_not_after_it() {
        let keysets: Vec<CachedPkKeyset> =
            (0..4).map(|_| CachedPkKeyset::with_capacity(0)).collect();
        let mut index = ShardedPkIndex::Exact(keysets.into_boxed_slice());

        // Far more keys than the budget admits, in one batch.
        let mut keys = PkDigestSet::with_capacity(20_000);
        for i in 0..20_000u64 {
            let k = owned_key(&key(i));
            keys.insert_with_digest(pk_digest(&k), k);
        }
        let one_entry = approx_pk_keyset_entry_bytes(&owned_key(&key(0)));
        // Room for ~1000 entries; the batch is 20x that.
        let max_bytes = one_entry.saturating_mul(1000);

        let within = index.record_keys_bounded(&keys, &RowLocation::FileUnlocated, max_bytes);
        assert!(
            !within,
            "a batch this far over budget must report over-budget"
        );

        let held = index.approx_bytes();
        // Overshoot is bounded by one BUDGET_RECHECK_KEYS chunk (512 entries),
        // not by the batch. Without the in-loop check this would be all 20,000.
        let ceiling = max_bytes.saturating_add(one_entry.saturating_mul(512 + 4));
        assert!(
            held <= ceiling,
            "index held {held} bytes, over the {ceiling}-byte chunk-bounded ceiling \
             (budget {max_bytes}); the budget is being applied after the batch, not during it"
        );
    }

    /// Each shard's exact entries are released as its bloom is built, so exact
    /// and blooms are never both fully resident.
    ///
    /// The post-state alone cannot show this — an implementation that built
    /// every bloom first and dropped the keysets at the end reaches the same
    /// one. What separates them is the exact bytes still held at each step, so
    /// the conversion is run through its observation hook: releasing as it goes
    /// steps the figure down per shard and reaches zero on the last, while
    /// releasing at the end would report the full exact total until then.
    #[test]
    fn degrading_releases_each_shard_as_it_converts() {
        const SHARDS: usize = 4;

        let keysets: Vec<CachedPkKeyset> = (0..SHARDS)
            .map(|_| CachedPkKeyset::with_capacity(0))
            .collect();
        let mut index = ShardedPkIndex::Exact(keysets.into_boxed_slice());

        let mut keys = PkDigestSet::with_capacity(4096);
        for i in 0..4096u64 {
            let k = owned_key(&key(i));
            keys.insert_with_digest(pk_digest(&k), k);
        }
        assert!(index.record_keys_bounded(&keys, &RowLocation::FileUnlocated, usize::MAX));
        let exact_bytes = index.approx_bytes();
        assert!(
            exact_bytes > 0,
            "the exact index should hold something to release"
        );

        // (shard converted, exact bytes still held) after each conversion.
        let mut steps: Vec<(usize, usize)> = Vec::new();
        index.degrade_to_blooms_observed(64 * 1024, None, |shard, still_held| {
            steps.push((shard, still_held));
        });

        assert_eq!(
            steps.iter().map(|(shard, _)| *shard).collect::<Vec<_>>(),
            (0..SHARDS).collect::<Vec<_>>(),
            "every shard converts, in order"
        );
        let mut previously_held = exact_bytes;
        for (shard, still_held) in &steps {
            assert!(
                *still_held < previously_held,
                "shard {shard} was converted without releasing its exact entries: \
                 {still_held} bytes still held, unchanged from {previously_held}"
            );
            previously_held = *still_held;
        }
        assert_eq!(
            previously_held, 0,
            "the last conversion must leave no exact entries behind"
        );

        match &index {
            ShardedPkIndex::Bloom(blooms) => {
                assert_eq!(blooms.len(), SHARDS, "every shard converts");
            }
            ShardedPkIndex::Exact(_) => panic!("degrade_to_blooms must leave a bloom index"),
        }
        assert!(
            index.approx_bytes() < exact_bytes,
            "the bloom index must be smaller than the exact one it replaced"
        );
    }

    /// `record_keys_bounded` routes each key to its `shard_of_pk` shard, and
    /// `degrade_to_blooms` converts over-budget exact keysets into blooms with
    /// no false negatives — the budget backstop for the maintained index.
    /// `usize::MAX` here isolates routing from the budget, which has its own
    /// tests below.
    #[test]
    fn record_keys_routes_and_degrades_to_blooms_without_false_negatives() {
        let keysets: Vec<CachedPkKeyset> =
            (0..4).map(|_| CachedPkKeyset::with_capacity(0)).collect();
        let mut index = ShardedPkIndex::Exact(keysets.into_boxed_slice());

        let mut keys = PkDigestSet::with_capacity(32);
        for i in 0..32u64 {
            let k = owned_key(&key(i));
            keys.insert_with_digest(pk_digest(&k), k);
        }
        assert!(index.record_keys_bounded(&keys, &RowLocation::FileUnlocated, usize::MAX));
        match &index {
            ShardedPkIndex::Exact(keysets) => {
                let total: usize = keysets.iter().map(CachedPkKeyset::len).sum();
                assert_eq!(total, 32, "every recorded key must land in some shard");
                for (shard, keyset) in keysets.iter().enumerate() {
                    for k in keyset.rows() {
                        assert_eq!(
                            shard_of_pk(k.as_ref(), 4),
                            shard,
                            "keys must be routed to their shard_of_pk shard"
                        );
                    }
                }
            }
            ShardedPkIndex::Bloom(_) => panic!("recording alone must not degrade"),
        }

        index.degrade_to_blooms(1024, None);
        match &index {
            ShardedPkIndex::Bloom(blooms) => {
                for i in 0..32u64 {
                    let k = owned_key(&key(i));
                    let shard = shard_of_pk(k.as_ref(), 4);
                    assert!(
                        blooms[shard].maybe_contains(k.as_ref()),
                        "degrading must not lose any key (no false negatives)"
                    );
                }
            }
            ShardedPkIndex::Exact(_) => panic!("degrade must convert to blooms"),
        }
    }
    use crate::row_converter::Row;

    fn key(n: u64) -> [u8; 8] {
        n.to_be_bytes()
    }

    fn owned_key(bytes: &[u8]) -> super::OwnedRow {
        Row::from_encoded(bytes).owned()
    }

    #[test]
    fn entry_estimate_charges_the_digest_the_entry_struct_and_the_key() {
        // Pins the figure every keyset byte budget is computed from (rationale on
        // `approx_pk_keyset_entry_bytes`). Asserted as a concrete number rather
        // than re-derived from the same `size_of`s the function adds, which would
        // restate the implementation and pass no matter what it charged: on 64-bit
        // an 8-byte key costs 8 + 16 (u128 digest) + 56 (`PkKeysetEntry`) + 16
        // (map slot) = 96. `sharded_table_splits_the_keyset_budget_between_the_two_caches`
        // sizes its key count against this, so changing the estimate must fail
        // HERE rather than silently widen that test's budget window.
        assert_eq!(approx_pk_keyset_entry_bytes(&owned_key(&key(7))), 96);
    }

    #[test]
    fn bounded_builder_stays_exact_within_budget() {
        let mut builder = BoundedShardedPkIndexBuilder::new(4, Some(1 << 20));
        for n in 0..100u64 {
            builder.insert(owned_key(&key(n)), RowLocation::FileUnlocated);
        }
        assert_eq!(builder.len(), 100);
        let ShardedPkIndex::Exact(shards) = builder.finish() else {
            panic!("within-budget build must stay exact");
        };
        assert_eq!(shards.len(), 4);
        // Every key must sit in exactly the shard `shard_of_pk` routes it to.
        for n in 0..100u64 {
            let k = owned_key(&key(n));
            let s = shard_of_pk(k.as_ref(), 4);
            assert!(
                shards[s].location_by_digest(pk_digest(&k)).is_some(),
                "key {n} missing from its routed shard {s}"
            );
        }
        let total: usize = shards.iter().map(CachedPkKeyset::len).sum();
        assert_eq!(total, 100, "no key may land in two shards");
    }

    #[test]
    fn bounded_builder_degrades_to_blooms_over_budget_without_false_negatives() {
        // Budget below even a handful of entries: the build must degrade
        // mid-stream and keep accepting keys, never exceeding the budget.
        let budget = approx_pk_keyset_entry_bytes(&owned_key(&key(0))) * 8;
        let mut builder = BoundedShardedPkIndexBuilder::new(4, Some(budget));
        for n in 0..1000u64 {
            builder.insert(owned_key(&key(n)), RowLocation::FileUnlocated);
        }
        let ShardedPkIndex::Bloom(blooms) = builder.finish() else {
            panic!("over-budget upsert build must degrade to blooms");
        };
        assert_eq!(blooms.len(), 4);
        // No false negatives — including the keys inserted BEFORE the degrade.
        for n in 0..1000u64 {
            let k = owned_key(&key(n));
            let s = shard_of_pk(k.as_ref(), 4);
            assert!(
                blooms[s].maybe_contains(k.as_ref()),
                "key {n} lost across the exact->bloom degrade"
            );
        }
    }

    #[test]
    fn bounded_builder_without_budget_never_degrades() {
        // `on_conflict: do-nothing` requires an exact answer; `max_bytes: None`
        // must keep building exact keysets regardless of size.
        let mut builder = BoundedShardedPkIndexBuilder::new(2, None);
        for n in 0..1000u64 {
            builder.insert(owned_key(&key(n)), RowLocation::FileUnlocated);
        }
        assert!(
            matches!(builder.finish(), ShardedPkIndex::Exact(_)),
            "unbudgeted build must stay exact"
        );
    }

    #[test]
    fn bounded_builder_insert_if_absent_preserves_location() {
        let mut builder = BoundedShardedPkIndexBuilder::new(1, None);
        let k = owned_key(&key(7));
        builder.insert(
            k.clone(),
            RowLocation::FilePositioned {
                file_path: "f".into(),
                position: 3,
            },
        );
        builder.insert_if_absent(k.clone(), RowLocation::FileUnlocated);
        let ShardedPkIndex::Exact(shards) = builder.finish() else {
            panic!("exact expected");
        };
        assert!(
            matches!(
                shards[0].location_by_digest(pk_digest(&k)),
                Some(RowLocation::FilePositioned { .. })
            ),
            "insert_if_absent must not clobber a durable-scan location"
        );
    }

    #[test]
    fn split_block_filter_round_trips_and_has_no_false_negatives() {
        // Build a v3 filter directly, independent of the env var, so the test
        // covers the layout rather than the process's write setting.
        let mut bloom = PkBloom {
            repr: PkBloomRepr::SplitBlock {
                blocks: vec![[0u32; 8]; 4096],
                block_mask: 4095,
            },
            inserted_keys: 0,
        };
        let keys: Vec<[u8; 16]> = (0..20_000u128).map(u128::to_le_bytes).collect();
        for key in &keys {
            bloom.insert(key);
        }
        assert_eq!(bloom.frame_version(), PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK);

        let restored = PkBloom::from_bytes(&bloom.to_bytes()).expect("v3 round-trips");
        assert_eq!(restored.frame_version(), PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK);
        assert_eq!(restored.size_bytes(), bloom.size_bytes());
        assert_eq!(restored.inserted_keys, bloom.inserted_keys);
        for key in &keys {
            assert!(
                restored.maybe_contains(key),
                "false negative after round-trip -- a missed conflict writes a duplicate live row"
            );
        }
    }

    /// The property the whole versioning scheme rests on: a v2 reader must reject
    /// a v3 blob every time, not merely usually. A v2 `bit_mask` is always
    /// `2^k - 1`, and the v3 magic is not, so the `num_bits == bit_mask + 1` check
    /// can never accept one.
    #[test]
    fn a_v2_reader_deterministically_rejects_a_v3_blob() {
        let mut bloom = PkBloom {
            repr: PkBloomRepr::SplitBlock {
                blocks: vec![[0u32; 8]; 64],
                block_mask: 63,
            },
            inserted_keys: 0,
        };
        for i in 0..500u128 {
            bloom.insert(&i.to_le_bytes());
        }
        let bytes = bloom.to_bytes();

        // Exactly the v2 acceptance test, applied to a v3 frame.
        let bit_mask = u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes"));
        let num_words = u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes"));
        let num_bits = num_words * 64;
        assert_ne!(
            num_bits,
            bit_mask.wrapping_add(1),
            "a v3 frame must fail the v2 consistency check"
        );
        assert!(
            !(bit_mask.wrapping_add(1)).is_power_of_two(),
            "the v3 magic must not look like a v2 bit mask"
        );
    }

    /// Both layouts must survive the sharded sidecar, since a table may be
    /// checkpointed by one binary and reopened by another.
    #[test]
    fn a_sidecar_round_trips_either_layout() {
        for split in [false, true] {
            let mut bloom = if split {
                PkBloom {
                    repr: PkBloomRepr::SplitBlock {
                        blocks: vec![[0u32; 8]; 128],
                        block_mask: 127,
                    },
                    inserted_keys: 0,
                }
            } else {
                // Explicitly scattered: the general constructor builds
                // split-block now, so this arm would otherwise test the same
                // layout twice and cover nothing.
                PkBloom::scattered_with_num_bits_pow2(1 << 16)
            };
            let keys: Vec<[u8; 16]> = (0..2_000u128).map(u128::to_le_bytes).collect();
            for key in &keys {
                bloom.insert(key);
            }
            let bytes = serialize_pk_blooms_sidecar(std::slice::from_ref(&bloom), "snap-1");
            let (restored, snapshot) =
                deserialize_pk_blooms_sidecar(&bytes).expect("sidecar round-trips");
            assert_eq!(snapshot, "snap-1");
            assert_eq!(restored.len(), 1);
            assert_eq!(restored[0].frame_version(), bloom.frame_version());
            for key in &keys {
                assert!(
                    restored[0].maybe_contains(key),
                    "false negative (split={split})"
                );
            }
        }
    }

    /// `with_expected_keys` must round the bit count UP.
    ///
    /// Asserted on the constructor's own output, not on two hand-sized filters:
    /// a test that only shows "more bits means fewer false positives" stays
    /// green if the round-up is deleted, which makes it no test of this change.
    #[test]
    fn with_expected_keys_rounds_the_bit_count_up() {
        // 100K keys asks for 1,000,000 bits. Rounding down takes 2^19 and gives
        // 5.24 bits/key; rounding up takes 2^20 and gives the ~10 the caller
        // asked for.
        let filter = PkBloom::with_expected_keys(100_000, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        assert_eq!(
            filter.size_bytes() * 8,
            1 << 20,
            "asked for 10 bits/key and must not receive 5.24"
        );

        // The ceiling still wins: never round up past what the caller allows.
        let capped = PkBloom::with_expected_keys(100_000, 64 * 1024);
        assert!(
            capped.size_bytes() <= 64 * 1024,
            "rounding up must not breach max_bytes, got {}",
            capped.size_bytes()
        );
    }

    /// And the accuracy that sizing buys, measured against the round-down the
    /// pre-frame constructor performed.
    #[test]
    fn round_up_sizing_beats_round_down_at_the_same_request() {
        // Scattered keys, as a hashed composite key is in practice.
        let mix = |i: u128| i.wrapping_mul(0x9e37_79b9_7f4a_7c15_9e37_79b9_7f4a_7c15);
        let keys: Vec<[u8; 16]> = (0..100_000u128).map(|i| mix(i).to_le_bytes()).collect();
        let absent: Vec<[u8; 16]> = (1_000_000..1_100_000u128)
            .map(|i| mix(i).to_le_bytes())
            .collect();

        let fpr_at = |bits: usize| {
            let mut bloom = PkBloom::with_num_bits_pow2(bits);
            for key in &keys {
                bloom.insert(key);
            }
            #[expect(clippy::cast_precision_loss, reason = "ratio of two small counts")]
            let rate = absent.iter().filter(|k| bloom.maybe_contains(*k)).count() as f64
                / absent.len() as f64;
            rate
        };

        let want = keys.len() * 10;
        let rounded_down = fpr_at(want);
        let rounded_up = fpr_at(want.next_power_of_two());
        assert!(
            rounded_up * 5.0 < rounded_down,
            "rounding up should be far more accurate for the same request: \
             down={rounded_down:.4} up={rounded_up:.4}"
        );
    }

    /// The legacy layout is much less accurate for SEQUENTIAL keys than for
    /// well-distributed ones, at identical size and load — and a monotonic
    /// integer primary key, the most ordinary shape a CDC table has, is exactly
    /// the bad case. This is a large part of what the split-block layout buys.
    ///
    /// The cause is the hash. FNV-1a's last operation is a multiply, which
    /// propagates bits leftward, so its LOW bits are the least diffused — and
    /// `maybe_contains` indexes with `hash & bit_mask`, i.e. precisely those bits.
    /// The split-block layout hashes with XXH3 instead and does not have the
    /// weakness, which this pins as a difference between the two layouts rather
    /// than an abstract claim about hash quality.
    #[test]
    fn sequential_keys_punish_the_scattered_hash_but_not_split_block() {
        let sequential: Vec<[u8; 16]> = (0..100_000u128).map(u128::to_le_bytes).collect();
        let absent: Vec<[u8; 16]> = (1_000_000..1_100_000u128).map(u128::to_le_bytes).collect();

        let measure = |mut bloom: PkBloom| {
            for key in &sequential {
                bloom.insert(key);
            }
            #[expect(clippy::cast_precision_loss, reason = "ratio of two small counts")]
            let rate = absent.iter().filter(|k| bloom.maybe_contains(*k)).count() as f64
                / absent.len() as f64;
            rate
        };

        let bits = (sequential.len() * 10).next_power_of_two();
        let v2 = measure(PkBloom::scattered_with_num_bits_pow2(bits));
        let blocks = bits / super::SPLIT_BLOCK_BITS;
        let v3 = measure(PkBloom {
            repr: PkBloomRepr::SplitBlock {
                blocks: vec![[0u32; 8]; blocks],
                block_mask: u64::try_from(blocks - 1).expect("fits"),
            },
            inserted_keys: 0,
        });

        assert!(
            v3 * 4.0 < v2,
            "the scattered hash should be markedly worse on sequential keys: \
             scattered={v2:.4} split_block={v3:.4}"
        );
    }

    #[test]
    fn pk_bloom_to_from_bytes_round_trips() {
        let mut bloom = PkBloom::with_expected_keys(1000, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        for n in 0..1000u64 {
            bloom.insert(&key(n));
        }
        let bytes = bloom.to_bytes();
        let restored = PkBloom::from_bytes(&bytes).expect("round-trips");

        // No false negatives: every inserted key must still be reported present.
        for n in 0..1000u64 {
            assert!(
                restored.maybe_contains(&key(n)),
                "restored bloom dropped an inserted key {n}"
            );
        }
        assert_eq!(
            restored.size_bytes(),
            bloom.size_bytes(),
            "bit layout preserved"
        );
        assert_eq!(restored.inserted_keys, bloom.inserted_keys);
    }

    #[test]
    fn pk_bloom_from_bytes_rejects_corrupt_input() {
        assert!(PkBloom::from_bytes(&[]).is_none(), "empty input");
        assert!(
            PkBloom::from_bytes(&[0u8; 4]).is_none(),
            "shorter than the header"
        );
        // A valid frame with its trailing words truncated must be rejected, not
        // silently half-parsed.
        let mut bloom = PkBloom::with_expected_keys(64, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        bloom.insert(&key(7));
        let mut bytes = bloom.to_bytes();
        bytes.truncate(bytes.len() - 8);
        assert!(PkBloom::from_bytes(&bytes).is_none(), "truncated frame");
    }

    /// The pre-frame serialization: `bit_mask | inserted_keys | num_words |
    /// words`, with no header. Every bloom persisted before the frame existed
    /// looks exactly like this, so the tests below build the legacy shape rather
    /// than asserting against a byte string nothing produces any more.
    fn unframed_bytes(bloom: &PkBloom) -> Vec<u8> {
        let PkBloomRepr::Scattered { bits, bit_mask } = &bloom.repr else {
            panic!("the pre-frame format is scattered only; the frame predates any other layout")
        };
        let mut out = Vec::new();
        out.extend_from_slice(&bit_mask.to_le_bytes());
        out.extend_from_slice(
            &u64::try_from(bloom.inserted_keys)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        out.extend_from_slice(&u64::try_from(bits.len()).unwrap_or(0).to_le_bytes());
        for word in bits {
            out.extend_from_slice(&word.to_le_bytes());
        }
        out
    }

    /// Trunk's pre-frame reader, as an OLD binary would run it: the consistency
    /// checks alone, with no notion of a magic. Used to prove which shapes such
    /// a binary accepts and which it turns down — the rollback direction.
    fn pre_frame_reader_accepts(bytes: &[u8]) -> bool {
        let Some(bit_mask) = bytes.get(0..8).map(|b| {
            let mut w = [0u8; 8];
            w.copy_from_slice(b);
            u64::from_le_bytes(w)
        }) else {
            return false;
        };
        let Some(num_words) = bytes.get(16..24).map(|b| {
            let mut w = [0u8; 8];
            w.copy_from_slice(b);
            u64::from_le_bytes(w)
        }) else {
            return false;
        };
        let available = u64::try_from(bytes.len().saturating_sub(24) / 8).unwrap_or(u64::MAX);
        if num_words == 0 || num_words > available {
            return false;
        }
        let Some(num_bits) = num_words.checked_mul(64) else {
            return false;
        };
        Some(num_bits) == bit_mask.checked_add(1) && num_bits.is_power_of_two()
    }

    /// A filter in the pre-frame SCATTERED shape, which nothing writes any more
    /// but every legacy blob on disk has. The tests below are about that shape
    /// specifically, so they must name it rather than take whatever the current
    /// writer produces.
    fn scattered_sample_bloom() -> PkBloom {
        let mut bloom =
            PkBloom::scattered_with_expected_keys(100, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        for n in 0..100u64 {
            bloom.insert(&key(n));
        }
        bloom
    }

    /// A bloom persisted before the frame existed stays readable, so upgrading
    /// does not force every table through the exact cold scan / keyset rebuild.
    ///
    /// If this fails because [`PK_BLOOM_NUM_HASHES`], the hash seeds, or the
    /// bit layout changed, that is the design working: those blooms are no
    /// longer probeable and MUST be rejected. Assert the rejection here — do
    /// NOT update [`LEGACY_PK_BLOOM_PROBE_FINGERPRINT`], which names the
    /// historical probe function and is frozen.
    #[test]
    fn pk_bloom_reads_an_unframed_legacy_blob() {
        let bloom = scattered_sample_bloom();
        let legacy = unframed_bytes(&bloom);
        assert_eq!(
            *SCATTERED_PROBE_FINGERPRINT, LEGACY_PK_BLOOM_PROBE_FINGERPRINT,
            "the probe function moved; unframed blooms are no longer probeable — see this test's doc"
        );

        let restored = PkBloom::from_bytes(&legacy).expect("legacy blob still parses");
        assert_eq!(restored.size_bytes(), bloom.size_bytes());
        assert_eq!(restored.inserted_keys, bloom.inserted_keys);
        for n in 0..100u64 {
            assert!(
                restored.maybe_contains(&key(n)),
                "legacy blob lost inserted key {n}"
            );
        }
    }

    /// The hazard this framing exists for: bits placed by a DIFFERENT probe
    /// function must not be probed by this one. A live key would probe as
    /// absent, no supersede tombstone would be recorded, and the row would
    /// duplicate.
    #[test]
    fn pk_bloom_rejects_bits_a_different_probe_function_placed() {
        let bloom = scattered_sample_bloom();

        // Framed: the writer recorded a fingerprint that is not this binary's.
        // Through the production reader — the recorded value lives in the bytes,
        // so the rejection needs no seam.
        let mut framed = bloom.to_bytes();
        framed[8..16].copy_from_slice(&SCATTERED_PROBE_FINGERPRINT.wrapping_add(1).to_le_bytes());
        assert!(
            PkBloom::from_bytes(&framed).is_none(),
            "a framed blob written by another probe function must be rejected"
        );
        // Unframed: nothing is recorded, so the rejection turns on this binary's
        // own fingerprint having moved — which only the seam can simulate.
        assert!(
            PkBloom::deserialize_from_prefix_probed_by(
                &unframed_bytes(&bloom),
                SCATTERED_PROBE_FINGERPRINT.wrapping_add(1)
            )
            .is_none(),
            "an unframed blob must be rejected once the probe function has moved"
        );
    }

    #[test]
    fn pk_bloom_rejects_an_unknown_frame_version() {
        let mut bytes = scattered_sample_bloom().to_bytes();
        bytes[4..8].copy_from_slice(&(PK_BLOOM_FRAME_VERSION_SPLIT_BLOCK + 1).to_le_bytes());
        assert!(
            PkBloom::from_bytes(&bytes).is_none(),
            "a future frame version must fall back, not be parsed as this one"
        );
    }

    /// The rollback direction: a binary predating the frame rejects a framed
    /// blob outright rather than probing it, and does so deterministically —
    /// the magic sits where it reads `bit_mask`, and a `bit_mask` is always
    /// `2^n - 1`.
    #[test]
    fn a_pre_frame_reader_rejects_a_framed_blob() {
        let bloom = scattered_sample_bloom();
        let framed = bloom.to_bytes();
        assert!(
            pre_frame_reader_accepts(&unframed_bytes(&bloom)),
            "control: the pre-frame reader accepts the shape it wrote"
        );
        assert!(
            !pre_frame_reader_accepts(&framed),
            "a pre-frame reader must reject a framed blob"
        );
        let leading = u64::from_le_bytes(
            framed[0..8]
                .try_into()
                .expect("frame is longer than 8 bytes"),
        );
        assert!(
            !leading.wrapping_add(1).is_power_of_two(),
            "the frame's leading word must never look like a bit_mask"
        );
    }

    /// A sidecar written before the blooms were framed still loads: the frame
    /// is per-bloom and self-describing, so the sidecar version did not move
    /// and no upgrade pays a full keyset rebuild.
    #[test]
    fn pk_bloom_sidecar_reads_unframed_blooms() {
        let bloom = scattered_sample_bloom();
        let mut legacy_sidecar = Vec::new();
        legacy_sidecar.extend_from_slice(&PK_INDEX_SIDECAR_MAGIC.to_le_bytes());
        legacy_sidecar.extend_from_slice(&PK_INDEX_SIDECAR_VERSION.to_le_bytes());
        legacy_sidecar.extend_from_slice(&8u64.to_le_bytes());
        legacy_sidecar.extend_from_slice(b"snap-old");
        legacy_sidecar.extend_from_slice(&1u64.to_le_bytes());
        legacy_sidecar.extend_from_slice(&unframed_bytes(&bloom));

        let (restored, snapshot) =
            deserialize_pk_bloom_sidecar(&legacy_sidecar).expect("legacy sidecar still parses");
        assert_eq!(snapshot, "snap-old");
        for n in 0..100u64 {
            assert!(
                restored.maybe_contains(&key(n)),
                "legacy sidecar lost inserted key {n}"
            );
        }
    }

    /// Blooms read back-to-back from a sharded sidecar: framing changed each
    /// bloom's length, so the consumed-bytes accounting has to move with it.
    #[test]
    fn pk_bloom_sidecar_reads_framed_blooms_back_to_back() {
        let mut evens = PkBloom::with_expected_keys(100, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        let mut odds = PkBloom::with_expected_keys(200, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        for n in 0..100u64 {
            if n % 2 == 0 {
                evens.insert(&key(n));
            } else {
                odds.insert(&key(n));
            }
        }
        let bytes = serialize_pk_blooms_sidecar(&[evens, odds], "snap-sharded");

        let (blooms, snapshot) =
            deserialize_pk_blooms_sidecar(&bytes).expect("sharded sidecar round-trips");
        assert_eq!(snapshot, "snap-sharded");
        assert_eq!(blooms.len(), 2, "both blooms must be read");
        for n in 0..100u64 {
            let shard = usize::from(n % 2 != 0);
            assert!(
                blooms[shard].maybe_contains(&key(n)),
                "sharded sidecar lost inserted key {n}"
            );
        }
    }

    #[test]
    fn cold_pk_existence_unions_per_file_blooms() {
        // File A holds evens, file B holds odds — different right-sized blooms.
        let mut a = PkBloom::with_expected_keys(500, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        let mut b = PkBloom::with_expected_keys(500, COLD_PK_BLOOM_PER_FILE_MAX_BYTES);
        for n in 0..1000u64 {
            if n % 2 == 0 {
                a.insert(&key(n));
            } else {
                b.insert(&key(n));
            }
        }
        let existence = ColdPkExistence::new(vec![a, b]);

        // No false negatives across the union for keys in either file.
        for n in 0..1000u64 {
            assert!(
                existence.maybe_contains(&key(n)),
                "union dropped key {n} present in one of the files"
            );
        }
        // A definitely-absent key is (almost surely) reported absent — the union
        // must not blanket-accept. Probe a sparse range far from the inserted set
        // to keep the false-positive odds negligible for the test.
        let absent_reported_present = (10_000_000u64..10_001_000)
            .filter(|&n| existence.maybe_contains(&key(n)))
            .count();
        assert!(
            absent_reported_present < 50,
            "union false-positive rate far too high: {absent_reported_present}/1000"
        );
        assert!(existence.approx_bytes() > 0);
    }

    #[test]
    fn cold_pk_existence_empty_never_contains() {
        let existence = ColdPkExistence::new(Vec::new());
        assert!(!existence.maybe_contains(&key(1)));
        assert_eq!(existence.approx_bytes(), 0);
    }

    #[test]
    fn try_insert_with_digest_on_new_key_inserts_and_grows_budget() {
        let mut keyset = CachedPkKeyset::with_capacity(4);
        let row = owned_key(b"pk-a");
        let digest = pk_digest(&row);
        let entry_bytes = approx_pk_keyset_entry_bytes(&row);

        let outcome = keyset.try_insert_with_digest(digest, &row, RowLocation::Inlined, usize::MAX);

        assert_eq!(outcome, PkKeysetInsertOutcome::Inserted);
        assert_eq!(keyset.len(), 1);
        assert_eq!(
            keyset.approx_bytes, entry_bytes,
            "a new key must grow approx_bytes by exactly its own entry cost"
        );
        assert!(
            matches!(
                keyset.location_by_digest(digest),
                Some(RowLocation::Inlined)
            ),
            "the inserted key must be retrievable at its stored location"
        );
    }

    #[test]
    fn try_insert_with_digest_on_present_key_updates_location_without_growing_budget() {
        let mut keyset = CachedPkKeyset::with_capacity(4);
        let row = owned_key(b"pk-b");
        let digest = pk_digest(&row);

        let first = keyset.try_insert_with_digest(digest, &row, RowLocation::Inlined, usize::MAX);
        assert_eq!(first, PkKeysetInsertOutcome::Inserted);
        let bytes_after_insert = keyset.approx_bytes;

        // Re-touch the SAME key (the CDC-update hot path this fix targets) with a
        // different location.
        let second =
            keyset.try_insert_with_digest(digest, &row, RowLocation::FileUnlocated, usize::MAX);

        assert_eq!(second, PkKeysetInsertOutcome::Updated);
        assert_eq!(
            keyset.len(),
            1,
            "the present-path update must not duplicate the entry"
        );
        assert_eq!(
            keyset.approx_bytes, bytes_after_insert,
            "updating an already-present key's location must not grow approx_bytes"
        );
        assert!(
            matches!(
                keyset.location_by_digest(digest),
                Some(RowLocation::FileUnlocated)
            ),
            "the update must overwrite the stored location"
        );
    }

    #[test]
    fn try_insert_with_digest_over_budget_leaves_keyset_unchanged() {
        let mut keyset = CachedPkKeyset::with_capacity(4);
        let row = owned_key(b"pk-c");
        let digest = pk_digest(&row);
        // One byte short of what this key needs, so the vacant branch must
        // refuse the insert rather than exceed the budget.
        let max_bytes = approx_pk_keyset_entry_bytes(&row) - 1;

        let outcome = keyset.try_insert_with_digest(digest, &row, RowLocation::Inlined, max_bytes);

        assert_eq!(outcome, PkKeysetInsertOutcome::OverBudget);
        assert_eq!(
            keyset.len(),
            0,
            "an over-budget key must not be inserted into the keyset"
        );
        assert_eq!(
            keyset.approx_bytes, 0,
            "a refused insert must not have mutated approx_bytes"
        );
        assert!(
            keyset.location_by_digest(digest).is_none(),
            "an over-budget key must not be retrievable afterward"
        );
    }
}
