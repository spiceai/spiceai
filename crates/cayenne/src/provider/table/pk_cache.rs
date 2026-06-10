//! Primary-key keyset/bloom caches and tombstone delta tracking.
//!
//! [`CachedPkIndex`] is the PK existence index for conflict detection: an exact
//! keyset ([`CachedPkKeyset`], with per-key [`RowLocation`]s) while it fits the
//! byte budget, or a bounded [`PkBloom`] for over-budget upsert tables. Guarded
//! by the `pk_keyset_cache` mutex and accounted against the query memory pool
//! via `table_memory`. Includes the persisted bloom sidecar checkpoint
//! (`persist_pk_bloom_checkpoint` / `try_load_persisted_pk_index`), the
//! `deletion_mode: position` capture pass, the cold keyset rebuild
//! (`load_existing_keyset`), and [`PendingTombstoneDeltas`] — the queue feeding
//! the inline-cache delta path.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use arrow::array::Array;

use super::{
    Arc, CatalogError, CatalogResult, CayenneDeletionSink, CayenneTableProvider, DeletionIndex,
    Error, HashMap, HashSet, KeyDeletionIndex, ListingTableUrl, ObjectStoreExt, OnConflict,
    OwnedRow, PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES, PkDeletionStrategyWithCache,
    RecordBatch, Result, RowConverter, SendableRecordBatchStream, SnapshotScanListingRequest,
    SortField, TryStreamExt, VecDeque,
};

pub(super) fn approx_pk_keyset_entry_bytes(key: &OwnedRow) -> usize {
    key.as_ref().len()
        + std::mem::size_of::<RowLocation>()
        + PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES
}

/// Approximate resident bytes a captured-file path adds to the keyset's
/// `captured_files` set: the heap string (counted once per file — the `Arc<str>`
/// is shared with the keyset's `FilePositioned` values) plus the fat pointer and
/// `HashSet` slot overhead.
pub(super) fn approx_captured_file_bytes(path: &str) -> usize {
    path.len() + std::mem::size_of::<Arc<str>>() + PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES
}

/// Where a primary key's current row version lives. The upsert path uses this to
/// decide how to tombstone the prior version: a `FilePositioned` entry can be
/// tombstoned by a per-file position deletion vector (pushed into the Vortex
/// scan, page-skippable); everything else falls back to a key-based deletion
/// vector applied above the scan. A single table can hold a mix.
#[derive(Debug, Clone)]
pub(super) enum RowLocation {
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

pub(super) struct CachedPkKeyset {
    pub(super) keys: HashMap<OwnedRow, RowLocation>,
    pub(super) approx_bytes: usize,
    /// Data files whose rows have already had their `(key -> file-local
    /// position)` captured by the `deletion_mode: position` read-back, so the
    /// capture pass can skip them. Reset whenever the keyset is rebuilt (e.g.
    /// after compaction), which is exactly when the file set changes.
    pub(super) captured_files: HashSet<Arc<str>>,
}

impl CachedPkKeyset {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: HashMap::with_capacity(capacity),
            approx_bytes: 0,
            captured_files: HashSet::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(super) fn insert(&mut self, key: OwnedRow, location: RowLocation) {
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
pub(super) const PK_BLOOM_NUM_HASHES: u32 = 7;

/// Seeded FNV-1a-64. Dependency-free and adequate for a Bloom filter; two
/// independent seeds feed the Kirsch–Mitzenmacher double-hashing scheme below.
pub(super) fn pk_bloom_hash(bytes: &[u8], seed: u64) -> u64 {
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
pub(super) struct PkBloom {
    pub(super) bits: Vec<u64>,
    /// `num_bits - 1`; `num_bits` is a power of two so indexing masks instead of mods.
    pub(super) bit_mask: u64,
    /// Keys inserted (observability + false-positive-rate estimation).
    pub(super) inserted_keys: usize,
}

impl PkBloom {
    /// Allocate a bloom whose bit array fits within `budget_bytes`, using the
    /// largest power-of-two bit count that does not exceed the budget.
    pub(super) fn with_byte_budget(budget_bytes: usize) -> Self {
        Self::with_num_bits_pow2(budget_bytes.saturating_mul(8))
    }

    /// Right-size a bloom for `expected_keys` (~10 bits/key, ~1% FPR), never
    /// exceeding `max_bytes`. Used when persisting a compaction checkpoint so the
    /// sidecar stays small rather than the full byte budget.
    pub(super) fn with_expected_keys(expected_keys: usize, max_bytes: usize) -> Self {
        let want_bits = expected_keys.saturating_mul(10);
        let cap_bits = max_bytes.saturating_mul(8).max(64);
        Self::with_num_bits_pow2(want_bits.min(cap_bits))
    }

    /// Allocate with the largest power-of-two bit count `<= target_bits` (min 64).
    pub(super) fn with_num_bits_pow2(target_bits: usize) -> Self {
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
    pub(super) fn serialize_into(&self, out: &mut Vec<u8>) {
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
    pub(super) fn deserialize_from(bytes: &[u8]) -> Option<Self> {
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

    pub(super) fn probe_bits(key: &[u8]) -> impl Iterator<Item = u64> {
        let h1 = pk_bloom_hash(key, 0x517c_c1b7_2722_0a95);
        // Force odd so successive probes stride across the whole bit space.
        let h2 = pk_bloom_hash(key, 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PK_BLOOM_NUM_HASHES).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)))
    }

    pub(super) fn insert(&mut self, key: &[u8]) {
        for hash in Self::probe_bits(key) {
            let bit = hash & self.bit_mask;
            let word = usize::try_from(bit >> 6).unwrap_or(0);
            self.bits[word] |= 1u64 << (bit & 63);
        }
        self.inserted_keys = self.inserted_keys.saturating_add(1);
    }

    pub(super) fn maybe_contains(&self, key: &[u8]) -> bool {
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
pub(super) const PK_INDEX_SIDECAR_MAGIC: u32 = 0x4350_4b42;
pub(super) const PK_INDEX_SIDECAR_VERSION: u32 = 1;
/// Upper bound on the persisted PK-index blob. Extreme-cardinality tables skip
/// persistence (and fall back to a runtime rebuild) to bound the metastore and
/// snapshot footprint. The bloom is right-sized (~10 bits/key), so this caps the
/// covered live-key count at roughly 200M.
pub(super) const PK_INDEX_PERSIST_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Serialize a checkpoint: `magic | version | snapshot_id_len | snapshot_id | bloom`.
pub(super) fn serialize_pk_bloom_sidecar(bloom: &PkBloom, snapshot_id: &str) -> Vec<u8> {
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
pub(super) fn deserialize_pk_bloom_sidecar(bytes: &[u8]) -> Option<(PkBloom, String)> {
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
pub(super) enum CachedPkIndex {
    Exact(CachedPkKeyset),
    Bloom(PkBloom),
}

impl CachedPkIndex {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Exact(keyset) => keyset.len(),
            Self::Bloom(bloom) => bloom.inserted_keys,
        }
    }

    /// Approximate resident bytes for memory accounting: the exact keyset's
    /// running byte tally, or the bloom's fixed bit-array size.
    pub(super) fn approx_bytes(&self) -> usize {
        match self {
            Self::Exact(keyset) => keyset.approx_bytes,
            Self::Bloom(bloom) => bloom.bits.len().saturating_mul(8),
        }
    }
}

/// Borrowed view of a [`CachedPkIndex`] handed to per-batch validation.
pub(super) enum PkExistenceRef<'a> {
    Exact(&'a HashMap<OwnedRow, RowLocation>),
    Bloom(&'a PkBloom),
}

#[derive(Default)]
pub(super) struct InlinedDeletionMaps {
    pub(super) int64_pk: HashMap<i64, i64>,
    pub(super) row_keys: HashMap<Box<[u8]>, i64>,
}

/// One published inline tombstone's removal effect, recorded so the inline-cache
/// delta path can apply it to the structurally-shared base entries WITHOUT a
/// structural epoch bump + full corpus re-read (cycle-5 TASK 1).
///
/// A published tombstone only ever REMOVES rows from the cached view — it hides
/// the prior inline copy of an upserted PK whose entry `sequence_number <=
/// delete_sequence`. Removal can never invalidate a *retained* entry (the same
/// soundness as pruning-under-deletes), so re-filtering the base entries against
/// just these keys is sound. The keys are exactly the ones in hand at publish
/// (`PreparedOnConflictDeletionPublish::deleted_pk_i64` / `deleted_row_keys`), so
/// no metastore read is needed to build the removal.
pub(super) struct TombstoneDelta {
    /// Monotonic queue sequence (`tombstone_delta_seq` at publish). Globally
    /// unique and never reset, so an `InlinedCache` records the highest delta it
    /// has applied (`tombstone_delta_seq`) and the delta path applies exactly the
    /// deltas with `seq > base.tombstone_delta_seq`.
    pub(super) seq: u64,
    /// The tombstone's `delete_sequence`. An entry's row is removed iff its PK is
    /// in this delta AND the entry `sequence_number <= delete_sequence` (mirrors
    /// `filter_inlined_batch_for_deletions`: keep iff `data_sequence > delete_sequence`).
    pub(super) delete_sequence: i64,
    /// Deleted Int64 PKs (for `Int64Pk` tables). Empty for composite-key tables.
    pub(super) int64_pk: Vec<i64>,
    /// Deleted encoded row-keys (for `RowConverterBased` tables). Empty for
    /// `Int64Pk` tables.
    pub(super) row_keys: Vec<Box<[u8]>>,
}

impl TombstoneDelta {
    /// Approximate heap footprint, used to bound the pending-delta queue.
    pub(super) fn approx_keys(&self) -> usize {
        self.int64_pk.len() + self.row_keys.len()
    }
}

/// Cap on the pending tombstone-delta queue (cycle-5 TASK 1). When EITHER the
/// number of queued deltas OR the total queued keys exceeds these, the next
/// inline-cache miss falls back to a FULL rebuild (which reads the whole corpus
/// plus the full deletion maps, so it captures every tombstone) and resets the
/// queue baseline. This bounds both the queue's memory and the per-miss
/// re-filter work between checkpoints, while keeping the delta path on the
/// common per-batch single-tombstone case. A checkpoint clears the queue
/// entirely, so in steady state it stays far below these caps.
pub(super) const MAX_PENDING_TOMBSTONE_DELTAS: usize = 256;
pub(super) const MAX_PENDING_TOMBSTONE_DELTA_KEYS: usize = 1_000_000;

/// Queue of published-but-not-yet-baked tombstone removals plus the live
/// monotonic sequence counter (cycle-5 TASK 1). Guarded by a single
/// `ParkingMutex` shared across writer clones; mutated only under that lock so
/// the `seq` and the `deltas` stay consistent.
#[derive(Default)]
pub(super) struct PendingTombstoneDeltas {
    /// Monotonic sequence; the value of the most recently enqueued delta. A new
    /// delta is assigned `seq + 1`. Never reset (so seqs are globally unique even
    /// across a queue drain).
    pub(super) seq: u64,
    /// Deltas pending application to the inline-cache base, ordered by `seq`
    /// ascending. Drained from the front once a stored cache has baked them in.
    pub(super) deltas: VecDeque<TombstoneDelta>,
    /// Running sum of `deltas[..].approx_keys()` for the O(1) cap check.
    pub(super) total_keys: usize,
}

impl PendingTombstoneDeltas {
    /// Enqueue a published tombstone's removal and return its assigned sequence.
    pub(super) fn push(
        &mut self,
        delete_sequence: i64,
        int64_pk: Vec<i64>,
        row_keys: Vec<Box<[u8]>>,
    ) -> u64 {
        self.seq += 1;
        let delta = TombstoneDelta {
            seq: self.seq,
            delete_sequence,
            int64_pk,
            row_keys,
        };
        self.total_keys += delta.approx_keys();
        self.deltas.push_back(delta);
        self.seq
    }

    /// `true` when the queue has outgrown either cap and the next miss should
    /// full-rebuild instead of delta-extend.
    pub(super) fn over_cap(&self) -> bool {
        self.deltas.len() > MAX_PENDING_TOMBSTONE_DELTAS
            || self.total_keys > MAX_PENDING_TOMBSTONE_DELTA_KEYS
    }

    /// Drop deltas with `seq <= applied_through` from the front — they are
    /// provably baked into a cache stored with `tombstone_delta_seq >=
    /// applied_through`, which is the base every future miss extends. Safe under
    /// concurrent populates because the queue is monotonic and a stale store only
    /// triggers a (correct) miss-and-recompute, and any delta above
    /// `applied_through` is retained.
    pub(super) fn drain_through(&mut self, applied_through: u64) {
        while let Some(front) = self.deltas.front() {
            if front.seq <= applied_through {
                self.total_keys = self.total_keys.saturating_sub(front.approx_keys());
                self.deltas.pop_front();
            } else {
                break;
            }
        }
    }

    /// Snapshot the deltas with `seq > base_seq` into a single
    /// [`InlinedDeletionMaps`] (the merged removal to apply to the base entries),
    /// returning `(removal_map, max_seq_in_queue)`. The max seq is the queue's
    /// current `seq` (even when no new delta exists), so a cache built from this
    /// records that it is current through the whole queue.
    pub(super) fn removal_above(&self, base_seq: u64) -> (InlinedDeletionMaps, u64) {
        let mut maps = InlinedDeletionMaps::default();
        // Deltas are stored seq-ascending (monotonic `push_back`), so the ones
        // with `seq > base_seq` are a suffix at the back — iterate from the back
        // and stop at the first `seq <= base_seq` so this is O(new deltas).
        for delta in self.deltas.iter().rev() {
            if delta.seq <= base_seq {
                break;
            }
            for &pk in &delta.int64_pk {
                maps.int64_pk
                    .entry(pk)
                    .and_modify(|seq| *seq = (*seq).max(delta.delete_sequence))
                    .or_insert(delta.delete_sequence);
            }
            for key in &delta.row_keys {
                maps.row_keys
                    .entry(key.clone())
                    .and_modify(|seq| *seq = (*seq).max(delta.delete_sequence))
                    .or_insert(delta.delete_sequence);
            }
        }
        (maps, self.seq)
    }
}

impl CayenneTableProvider {
    pub(super) fn take_cached_pk_index(&self) -> Option<CachedPkIndex> {
        self.pk_keyset_cache.lock().take()
    }

    /// Whether this table may fall back to a bounded bloom existence filter when
    /// its exact keyset exceeds the budget. Only safe for `Upsert`: a bloom false
    /// positive yields a harmless redundant delete under upsert, but would wrongly
    /// drop a genuinely new row under `DoNothing` semantics.
    pub(super) fn upsert_bloom_eligible(&self) -> bool {
        matches!(self.table_metadata.on_conflict, Some(OnConflict::Upsert(_)))
    }

    /// Build a bloom existence filter over `keyset`'s keys, sized to `max_bytes`.
    pub(super) fn bloom_from_keyset(keyset: &CachedPkKeyset, max_bytes: usize) -> PkBloom {
        let mut bloom = PkBloom::with_byte_budget(max_bytes);
        for key in keyset.keys.keys() {
            bloom.insert(key.as_ref());
        }
        bloom
    }

    pub(super) fn store_cached_pk_index(&self, index: CachedPkIndex) {
        let max_bytes = self.context.pk_keyset_cache_max_bytes();
        let to_store = match index {
            CachedPkIndex::Exact(keyset) if keyset.approx_bytes > max_bytes => {
                if self.upsert_bloom_eligible() {
                    // Convert the over-budget exact keyset to a bounded bloom so
                    // subsequent CDC batches skip the full-table keyset rebuild.
                    tracing::debug!(
                        table = self.table_metadata.table_name.as_str(),
                        key_count = keyset.len(),
                        approx_bytes = keyset.approx_bytes,
                        max_bytes,
                        "Converting over-budget primary-key keyset to a bounded bloom existence filter"
                    );
                    CachedPkIndex::Bloom(Self::bloom_from_keyset(&keyset, max_bytes))
                } else {
                    // DoNothing needs exact answers; drop and rebuild next batch
                    // rather than risk a bloom false positive dropping a new row.
                    tracing::debug!(
                        table = self.table_metadata.table_name.as_str(),
                        key_count = keyset.len(),
                        approx_bytes = keyset.approx_bytes,
                        max_bytes,
                        "Skipping primary-key keyset cache because it exceeds the configured byte budget"
                    );
                    *self.pk_keyset_cache.lock() = None;
                    self.table_memory.set_keyset_bytes(0);
                    return;
                }
            }
            other => other,
        };

        let bytes = to_store.approx_bytes();
        *self.pk_keyset_cache.lock() = Some(to_store);
        self.table_memory.set_keyset_bytes(bytes);
    }

    pub(crate) fn clear_cached_pk_keyset(&self) {
        *self.pk_keyset_cache.lock() = None;
        self.table_memory.set_keyset_bytes(0);
    }

    /// Rewrite every cached keyset entry from `RowLocation::Inlined` to
    /// `RowLocation::FileUnlocated`. Called after an inline checkpoint has
    /// flushed the memtable to files.
    ///
    /// Only the `Exact` keyset carries per-key `RowLocation`s; the `Bloom` index
    /// emits key-based deletes to both lists, so no change is needed.
    pub(super) fn flip_inlined_keyset_entries_to_file_unlocated(&self) {
        let mut guard = self.pk_keyset_cache.lock();
        if let Some(CachedPkIndex::Exact(keyset)) = guard.as_mut() {
            for location in keyset.keys.values_mut() {
                if matches!(location, RowLocation::Inlined) {
                    *location = RowLocation::FileUnlocated;
                }
            }
        }
    }

    pub(super) fn record_pk_keys_with_location(
        &self,
        keys: &HashSet<OwnedRow>,
        location: &RowLocation,
    ) {
        if keys.is_empty() {
            return;
        }

        let max_bytes = self.context.pk_keyset_cache_max_bytes();
        let mut guard = self.pk_keyset_cache.lock();
        // Take ownership so an over-budget Exact keyset can be replaced by a
        // bloom without a borrow conflict; the index is restored before return.
        let Some(mut index) = guard.take() else {
            return;
        };

        let mut convert_to_bloom = false;
        match &mut index {
            CachedPkIndex::Bloom(bloom) => {
                for key in keys {
                    bloom.insert(key.as_ref());
                }
            }
            CachedPkIndex::Exact(keyset) => {
                // Existence-only insert. Under `deletion_mode: position`, real
                // `(file, position)` for File rows is captured separately by the
                // row_idx() read-back, which upgrades these to `FilePositioned`.
                for key in keys {
                    if !keyset.keys.contains_key(key)
                        && keyset
                            .approx_bytes
                            .saturating_add(approx_pk_keyset_entry_bytes(key))
                            > max_bytes
                    {
                        convert_to_bloom = true;
                        break;
                    }
                    keyset.insert(key.clone(), location.clone());
                }
            }
        }

        if convert_to_bloom {
            if self.upsert_bloom_eligible() {
                let mut bloom = match &index {
                    CachedPkIndex::Exact(keyset) => Self::bloom_from_keyset(keyset, max_bytes),
                    CachedPkIndex::Bloom(_) => PkBloom::with_byte_budget(max_bytes),
                };
                for key in keys {
                    bloom.insert(key.as_ref());
                }
                tracing::debug!(
                    table = self.table_metadata.table_name.as_str(),
                    incoming_key_count = keys.len(),
                    max_bytes,
                    "Converting over-budget primary-key keyset to a bounded bloom existence filter on incremental update"
                );
                index = CachedPkIndex::Bloom(bloom);
            } else {
                tracing::debug!(
                    table = self.table_metadata.table_name.as_str(),
                    incoming_key_count = keys.len(),
                    max_bytes,
                    "Clearing primary-key keyset cache because the write would exceed the byte budget"
                );
                // `guard` already holds None from the take() above.
                self.table_memory.set_keyset_bytes(0);
                return;
            }
        }

        let bytes = index.approx_bytes();
        *guard = Some(index);
        self.table_memory.set_keyset_bytes(bytes);
    }

    pub(crate) fn record_inlined_pk_keys(&self, keys: &HashSet<OwnedRow>) {
        self.record_pk_keys_with_location(keys, &RowLocation::Inlined);
    }

    pub(crate) fn record_file_pk_keys(&self, keys: &HashSet<OwnedRow>) {
        self.record_pk_keys_with_location(keys, &RowLocation::FileUnlocated);
    }

    /// Whether this table should capture file-local row positions for upsert
    /// deletes: a PK table (`Int64Pk`/`RowConverterBased`) whose resolved
    /// [`DeletionMode`] is `Position`. PK-less tables use the `PositionBased`
    /// strategy directly and never reach this read-back.
    pub(super) fn should_capture_positions(&self) -> bool {
        !self.pk_deletion_strategy.is_position_based()
            && self.context.deletion_mode().resolved(true).is_position()
    }

    /// Force a synchronous position-capture pass now, if this table is in
    /// `deletion_mode: position`. The post-write maintenance loop normally runs
    /// this asynchronously; this entry point lets a caller (or a test) capture
    /// eagerly and deterministically. A no-op for key-mode / PK-less tables.
    ///
    /// # Errors
    ///
    /// Returns an error if listing the snapshot's data files, resolving the
    /// object store, or the per-file position read-back scan fails.
    pub async fn run_position_capture(&self) -> CatalogResult<()> {
        if self.should_capture_positions() {
            self.capture_new_file_positions().await
        } else {
            Ok(())
        }
    }

    /// Bytes this table currently reserves against the query memory pool for its
    /// off-pool resident state (PK keyset + deletion indexes). Exposed
    /// for observability and for memory-accounting correctness tests.
    #[must_use]
    pub fn accounted_memory_bytes(&self) -> usize {
        self.table_memory.reserved_bytes()
    }

    pub(super) fn refresh_deletion_memory_accounting(&self) {
        self.table_memory
            .set_deletion_bytes(self.pk_deletion_strategy.approx_resident_bytes());
    }

    /// Best-effort write-time read-back (`deletion_mode: position`): scan newly
    /// written data files and upgrade their keyset entries from `FileUnlocated`
    /// to `FilePositioned`, so a later upsert of those keys tombstones the prior
    /// version by position (page-skipped inside the Vortex scan) rather than by
    /// key (re-evaluated above the scan). Safe to run async and best-effort: a
    /// row whose position has not yet been captured simply falls back to a
    /// key-based delete, which is always correct.
    pub(super) async fn capture_new_file_positions(&self) -> CatalogResult<()> {
        let Some(pk_indices) =
            self.primary_key_indices()
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Position capture: failed to resolve primary key indices: {err}"
                    ),
                })?
        else {
            return Ok(());
        };
        if pk_indices.is_empty() {
            return Ok(());
        }
        let converter = self.build_pk_converter(&pk_indices).map_err(|err| {
            CatalogError::InvalidOperationNoSource {
                message: format!("Position capture: failed to build PK converter: {err}"),
            }
        })?;
        let pk_column_names: Vec<String> = self.table_metadata.primary_key.clone();

        // Enumerate current snapshot data files. The object-store location of
        // each PartitionedFile is exactly the key the scan-time access-plan
        // provider looks up, so position vectors written under these keys apply.
        let ctx = self.create_session_context();
        let state = ctx.state();
        let snapshot_id = self.get_current_snapshot_id();
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &snapshot_id,
        );
        let table_url = ListingTableUrl::parse(&snapshot_dir_url).map_err(|err| {
            CatalogError::InvalidOperationNoSource {
                message: format!("Position capture: invalid snapshot URL: {err}"),
            }
        })?;
        let options = Self::create_listing_options(
            self.context.file_format(),
            &self.pk_deletion_strategy,
            state.config(),
        );
        let scan_schema = Self::snapshot_scan_schema(&self.table_metadata.schema, &options);
        let listed = self
            .list_files_for_snapshot_scan(&SnapshotScanListingRequest {
                state: &state,
                table_url: &table_url,
                options: &options,
                partition_filters: &[],
                data_filters: &[],
                snapshot_id: &snapshot_id,
                limit: None,
                scan_schema,
            })
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Position capture: failed to list snapshot files: {err}"),
            })?;
        let object_store = state
            .runtime_env()
            .object_store(&table_url)
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Position capture: failed to resolve object store: {err}"),
            })?;

        // Snapshot the already-captured set. Only the exact keyset tracks
        // positions; a bloom/None keyset means everything stays key-based.
        let already_captured: HashSet<Arc<str>> = {
            let guard = self.pk_keyset_cache.lock();
            match guard.as_ref() {
                Some(CachedPkIndex::Exact(keyset)) => keyset.captured_files.clone(),
                _ => return Ok(()),
            }
        };

        // Minimal sink for the per-file read-back scan (needs only table
        // metadata; no protected snapshots, no write lock — the caller path
        // already serializes writes).
        let sink = CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            &[],
            self.pk_deletion_strategy.clone(),
            Arc::clone(&self.table_memory),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            Vec::new(),
            Arc::clone(self.context.runtime_env()),
            None,
            Arc::clone(&self.seq_allocator),
        );

        for file_group in &listed.file_groups {
            for partitioned_file in file_group.iter() {
                let file_path: Arc<str> =
                    Arc::from(partitioned_file.object_meta.location.to_string());
                if already_captured.contains(&file_path) {
                    continue;
                }
                let entries = sink
                    .scan_file_for_all_positions(
                        &file_path,
                        &object_store,
                        &pk_column_names,
                        &converter,
                    )
                    .await
                    .map_err(|err| CatalogError::InvalidOperationNoSource {
                        message: format!("Position capture: read-back scan failed: {err}"),
                    })?;

                // Re-lock to publish: upgrade existing keyset entries in place
                // (no byte-budget change — `RowLocation` is a fixed-size enum).
                // Entries should already exist (recorded as `FileUnlocated` by
                // `record_file_pk_keys`); a missing one is skipped defensively.
                let mut guard = self.pk_keyset_cache.lock();
                if let Some(CachedPkIndex::Exact(keyset)) = guard.as_mut() {
                    for (key, position) in entries {
                        if let Some(location) = keyset.keys.get_mut(&key) {
                            *location = RowLocation::FilePositioned {
                                file_path: Arc::clone(&file_path),
                                position,
                            };
                        }
                    }
                    // Track the file-path string + set slot now held resident.
                    if keyset.captured_files.insert(Arc::clone(&file_path)) {
                        keyset.approx_bytes = keyset
                            .approx_bytes
                            .saturating_add(approx_captured_file_bytes(&file_path));
                    }
                }
            }
        }

        // Republish the keyset's resident size: the capture pass grew
        // `captured_files`, which is part of the keyset's accounted footprint.
        let keyset_bytes = self
            .pk_keyset_cache
            .lock()
            .as_ref()
            .map_or(0, CachedPkIndex::approx_bytes);
        self.table_memory.set_keyset_bytes(keyset_bytes);

        Ok(())
    }

    /// Returns the column indices for the configured primary key, if any.
    pub(super) fn primary_key_indices(&self) -> Result<Option<Vec<usize>>> {
        if self.table_metadata.primary_key.is_empty() {
            return Ok(None);
        }

        let mut indices = Vec::with_capacity(self.table_metadata.primary_key.len());
        for pk_col in &self.table_metadata.primary_key {
            let idx =
                self.table_metadata
                    .schema
                    .index_of(pk_col)
                    .map_err(|_| Error::DataValidation {
                        table: self.table_metadata.table_name.clone(),
                        message: format!("Primary key column '{pk_col}' not found in schema"),
                    })?;
            indices.push(idx);
        }

        Ok(Some(indices))
    }

    /// Build a `RowConverter` for the primary key columns.
    pub(super) fn build_pk_converter(&self, pk_indices: &[usize]) -> Result<RowConverter> {
        let mut sort_fields = Vec::with_capacity(pk_indices.len());
        for idx in pk_indices {
            let field = self.table_metadata.schema.field(*idx);
            sort_fields.push(SortField::new(field.data_type().clone()));
        }

        Ok(RowConverter::new(sort_fields)?)
    }

    /// Build the existing keyset (primary key bytes -> row location) for append-mode inserts.
    ///
    /// This method scans BOTH the main listing table AND any protected snapshots to build
    /// a complete keyset of all existing primary keys.
    ///
    /// This method respects ALL deletion caches based on `pk_deletion_strategy`:
    /// - `Int64Pk`: Uses the atomically-published Int64 PK deletion snapshot
    /// - `RowConverterBased`: Uses the atomically-published row-key deletion snapshot
    /// - `PositionBased`: Uses `cached_deleted_row_ids` (no primary key)
    ///
    /// Rows marked as deleted are excluded unless they were re-inserted with a higher
    /// sequence number (upsert semantics).
    pub(super) async fn load_existing_keyset(
        &self,
        pk_indices: &[usize],
        converter: &RowConverter,
    ) -> Result<CachedPkKeyset> {
        // Wait-free Arc::clone — the inner HashMap is shared, not cloned,
        // so the scan does not pay an O(N) String + i64 clone per call.
        let protected_snapshots = self.protected_snapshots.load_full();

        let ctx = self.create_session_context();
        // Only read PK columns - no need to load all columns for keyset building
        let pk_projection = pk_indices.to_vec();

        // Scan the current snapshot directly from its listed Vortex files.
        let current_snapshot_id = self.get_current_snapshot_id();
        let scan_plan = self
            .create_snapshot_scan_plan(
                &ctx.state(),
                &current_snapshot_id,
                Some(&pk_projection),
                &[],
                None,
            )
            .await?;

        // Load the deletion caches based on pk_deletion_strategy.
        // Note: PositionBased strategy is never used here since it implies no primary key,
        // and this function is only called for tables with primary keys.
        // ArcSwap loads are wait-free; the resulting `Arc<...Index>` is an immutable
        // snapshot of the deletion state at this instant.
        let deleted_pk_i64: Option<Arc<DeletionIndex>> = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => Some(Arc::clone(&deletion_snapshot.load_full().tombstones)),
            _ => None,
        };

        let deleted_row_keys: Option<Arc<KeyDeletionIndex>> = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => Some(Arc::clone(&deletion_snapshot.load_full().tombstones)),
            _ => None,
        };

        let mut keyset = CachedPkKeyset::with_capacity(1024);
        let mut row_id_base: i64 = 0;

        // After projection, batch columns are at indices 0..pk_indices.len()
        let projected_pk_indices: Vec<usize> = (0..pk_indices.len()).collect();

        // Process main listing table batches with the FULL deletion filter (no insert_records).
        // This mirrors scan()'s apply_deletion_filter() which uses all deletions without
        // insert_records when protected snapshots exist.
        // min_delete_seq_threshold=None means ALL deletions apply.
        let main_stream = datafusion_physical_plan::execute_stream(scan_plan, ctx.task_ctx())?;
        Self::process_stream_into_keyset(
            main_stream,
            &self.pk_deletion_strategy,
            pk_indices,
            converter,
            &projected_pk_indices,
            deleted_pk_i64.as_deref(),
            deleted_row_keys.as_deref(),
            None, // all deletions apply to main listing table
            &self.table_metadata.table_name,
            &mut keyset,
            &mut row_id_base,
        )
        .await?;

        // Process each protected snapshot with a PARTIAL deletion filter.
        // Only deletions with seq > max_delete_seq_at_creation apply, mirroring
        // scan()'s apply_partial_deletion_filter().
        for (snapshot_id, max_delete_seq_at_creation) in protected_snapshots.iter() {
            let snapshot_plan = self
                .create_snapshot_scan_plan(
                    &ctx.state(),
                    snapshot_id,
                    Some(&pk_projection),
                    &[],
                    None,
                )
                .await?;

            let snapshot_stream =
                datafusion_physical_plan::execute_stream(snapshot_plan, ctx.task_ctx())?;

            Self::process_stream_into_keyset(
                snapshot_stream,
                &self.pk_deletion_strategy,
                pk_indices,
                converter,
                &projected_pk_indices,
                deleted_pk_i64.as_deref(),
                deleted_row_keys.as_deref(),
                Some(*max_delete_seq_at_creation), // only deletions with seq > threshold apply
                &self.table_metadata.table_name,
                &mut keyset,
                &mut row_id_base,
            )
            .await?;
        }

        if self.cached_inlined_row_count() > 0 {
            let inlined_batches = self.read_inlined_batches().await?;
            self.process_visible_inlined_batches_into_keyset(
                &inlined_batches,
                pk_indices,
                converter,
                &mut keyset,
            )?;
        }

        Ok(keyset)
    }

    // ---- Phase 3: persist/checkpoint the PK existence index across restarts ----
    // Persisted in the metastore (`cayenne_pk_index`) so it is captured by
    // metastore snapshots — letting both a restart AND a node bootstrapped from a
    // snapshot skip the O(total-rows) full keyset rebuild. Works uniformly for
    // local and object-store tables.

    /// Insert one batch's primary keys (no deletion filter — a superset is safe
    /// for the upsert bloom; deleted keys only cost a harmless false positive)
    /// into `bloom`. `pk_col_indices` are the PK columns' positions in `batch`.
    pub(super) fn insert_batch_pks_into_bloom(
        batch: &RecordBatch,
        pk_col_indices: &[usize],
        converter: &RowConverter,
        bloom: &mut PkBloom,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let pk_columns: Vec<_> = pk_col_indices
            .iter()
            .map(|idx| Arc::clone(batch.column(*idx)))
            .collect();
        let rows = converter.convert_columns(&pk_columns)?;
        for row_idx in 0..batch.num_rows() {
            bloom.insert(rows.row(row_idx).as_ref());
        }
        Ok(())
    }

    /// Build a right-sized bloom of the PK values in a single snapshot (used to
    /// checkpoint the freshly-compacted current snapshot, which contains exactly
    /// the live rows and no deletions).
    pub(super) async fn build_snapshot_pk_bloom(
        &self,
        snapshot_id: &str,
        pk_indices: &[usize],
        converter: &RowConverter,
        expected_keys: usize,
        max_bytes: usize,
    ) -> Result<PkBloom> {
        let ctx = self.create_session_context();
        let pk_projection = pk_indices.to_vec();
        let projected_pk_indices: Vec<usize> = (0..pk_indices.len()).collect();
        let mut bloom = PkBloom::with_expected_keys(expected_keys, max_bytes);

        let scan_plan = self
            .create_snapshot_scan_plan(&ctx.state(), snapshot_id, Some(&pk_projection), &[], None)
            .await?;
        let mut stream = datafusion_physical_plan::execute_stream(scan_plan, ctx.task_ctx())?;
        while let Some(batch) = stream.next().await {
            Self::insert_batch_pks_into_bloom(
                &batch?,
                &projected_pk_indices,
                converter,
                &mut bloom,
            )?;
        }
        Ok(bloom)
    }

    /// Fold the post-checkpoint delta — every protected snapshot and inline entry
    /// (all created after the checkpoint, since compaction clears both) — into a
    /// bloom loaded from the sidecar, making it a superset of all current keys.
    pub(super) async fn extend_bloom_with_protected_and_inline(
        &self,
        pk_indices: &[usize],
        converter: &RowConverter,
        bloom: &mut PkBloom,
    ) -> Result<()> {
        let protected_snapshots = self.protected_snapshots.load_full();
        let ctx = self.create_session_context();
        let pk_projection = pk_indices.to_vec();
        let projected_pk_indices: Vec<usize> = (0..pk_indices.len()).collect();

        for (snapshot_id, _max_delete_seq) in protected_snapshots.iter() {
            let scan_plan = self
                .create_snapshot_scan_plan(
                    &ctx.state(),
                    snapshot_id,
                    Some(&pk_projection),
                    &[],
                    None,
                )
                .await?;
            let mut stream = datafusion_physical_plan::execute_stream(scan_plan, ctx.task_ctx())?;
            while let Some(batch) = stream.next().await {
                Self::insert_batch_pks_into_bloom(
                    &batch?,
                    &projected_pk_indices,
                    converter,
                    bloom,
                )?;
            }
        }

        if self.cached_inlined_row_count() > 0 {
            let inlined_batches = self.read_inlined_batches().await?;
            for batch in &inlined_batches {
                // Inlined batches carry the full table schema, so use pk_indices directly.
                Self::insert_batch_pks_into_bloom(batch, pk_indices, converter, bloom)?;
            }
        }
        Ok(())
    }

    /// Persist a PK-index bloom checkpoint for the just-compacted snapshot.
    /// Best-effort: any failure only means the next restart pays a full scan.
    pub(super) async fn persist_pk_bloom_checkpoint(&self, snapshot_id: &str, total_rows: u64) {
        if let Err(err) = self
            .try_persist_pk_bloom_checkpoint(snapshot_id, total_rows)
            .await
        {
            tracing::debug!(
                table = self.table_metadata.table_name.as_str(),
                error = %err,
                "Failed to persist PK-index bloom checkpoint; restart will rebuild from a full scan"
            );
        }
    }

    pub(super) async fn try_persist_pk_bloom_checkpoint(
        &self,
        snapshot_id: &str,
        total_rows: u64,
    ) -> Result<()> {
        let Some(pk_indices) = self.primary_key_indices()? else {
            return Ok(());
        };
        let converter = self.build_pk_converter(&pk_indices)?;
        // Size the persistence bloom against the *persist* budget, not the (much
        // larger, up to 8 GiB) in-memory keyset budget. A checkpoint bloom whose
        // serialized form exceeds `PK_INDEX_PERSIST_MAX_BYTES` is discarded below
        // anyway, so building it at the in-memory budget would let compaction
        // allocate — and potentially OOM on — a blob it will never store.
        let max_bytes = self
            .context
            .pk_keyset_cache_max_bytes()
            .min(PK_INDEX_PERSIST_MAX_BYTES);
        let expected_keys = usize::try_from(total_rows).unwrap_or(usize::MAX);
        let bloom = self
            .build_snapshot_pk_bloom(
                snapshot_id,
                &pk_indices,
                &converter,
                expected_keys,
                max_bytes,
            )
            .await?;

        let bytes = serialize_pk_bloom_sidecar(&bloom, snapshot_id);
        // Bound the metastore/snapshot footprint: extreme-cardinality tables skip
        // persistence and fall back to a runtime rebuild on restart/bootstrap.
        if bytes.len() > PK_INDEX_PERSIST_MAX_BYTES {
            tracing::debug!(
                table = self.table_metadata.table_name.as_str(),
                blob_bytes = bytes.len(),
                max_bytes = PK_INDEX_PERSIST_MAX_BYTES,
                "Skipping PK-index checkpoint persistence: blob exceeds the persist budget"
            );
            return Ok(());
        }

        self.catalog
            .upsert_pk_index(&self.table_metadata.table_id, snapshot_id, &bytes)
            .await
            .map_err(|source| Error::Catalog { source })?;
        tracing::debug!(
            table = self.table_metadata.table_name.as_str(),
            snapshot_id,
            keys = bloom.inserted_keys,
            blob_bytes = bytes.len(),
            "Persisted PK-index bloom checkpoint to the metastore"
        );
        Ok(())
    }

    /// Try to reconstruct the PK existence index from the persisted sidecar,
    /// skipping the full-table keyset scan. Returns `None` (→ caller falls back to
    /// the full `load_existing_keyset`) unless the table is upsert-eligible, the
    /// sidecar exists and validates, AND its checkpoint snapshot still equals the
    /// current snapshot (guaranteeing the bloom covers the full current snapshot —
    /// upsert tables only add sequence-tagged protected/inline data after a
    /// checkpoint, never rewriting the current snapshot except via compaction,
    /// which re-persists). The post-checkpoint delta is folded in to keep the
    /// no-false-negative invariant.
    pub(super) async fn try_load_persisted_pk_index(
        &self,
        pk_indices: &[usize],
        converter: &RowConverter,
    ) -> Result<Option<CachedPkIndex>> {
        if !self.upsert_bloom_eligible() {
            return Ok(None);
        }
        let Some((checkpoint_snapshot, bytes)) = self
            .catalog
            .get_pk_index(&self.table_metadata.table_id)
            .await
            .map_err(|source| Error::Catalog { source })?
        else {
            return Ok(None);
        };
        // Defensive read-side bound mirroring the write-side persist cap: a
        // corrupted or manually-modified metastore row could carry an oversized
        // `index_blob` that would drive a large allocation in
        // `deserialize_pk_bloom_sidecar` before we could fall back. Fail closed to
        // the full rebuild when it exceeds the persist budget.
        if bytes.len() > PK_INDEX_PERSIST_MAX_BYTES {
            tracing::debug!(
                table = self.table_metadata.table_name.as_str(),
                blob_bytes = bytes.len(),
                max_bytes = PK_INDEX_PERSIST_MAX_BYTES,
                "Persisted PK-index blob exceeds the persist budget; rebuilding keyset"
            );
            return Ok(None);
        }
        // Gate on the snapshot tag: the bloom covers the full current snapshot
        // only if nothing rewrote it since the checkpoint (compaction re-persists).
        if checkpoint_snapshot != self.get_current_snapshot_id() {
            return Ok(None);
        }
        let Some((mut bloom, blob_snapshot)) = deserialize_pk_bloom_sidecar(&bytes) else {
            return Ok(None);
        };
        // Defense in depth: the metastore `checkpoint_snapshot` column and the
        // snapshot id embedded in the blob are written together, so they should
        // always agree. But if a row is ever inconsistent/corrupt such that the
        // column matches the current snapshot while the blob was produced for a
        // different snapshot, trusting it could admit Bloom false negatives and
        // break upsert correctness. Fail closed to the full rebuild on mismatch.
        if blob_snapshot != checkpoint_snapshot {
            tracing::debug!(
                table = self.table_metadata.table_name.as_str(),
                checkpoint_snapshot = checkpoint_snapshot.as_str(),
                blob_snapshot = blob_snapshot.as_str(),
                "PK-index sidecar snapshot mismatch (metastore column vs blob); rebuilding keyset"
            );
            return Ok(None);
        }
        self.extend_bloom_with_protected_and_inline(pk_indices, converter, &mut bloom)
            .await?;
        tracing::debug!(
            table = self.table_metadata.table_name.as_str(),
            checkpoint_snapshot = checkpoint_snapshot.as_str(),
            "Loaded PK-index bloom checkpoint; skipped full-table keyset rebuild"
        );
        Ok(Some(CachedPkIndex::Bloom(bloom)))
    }

    pub(super) fn process_visible_inlined_batches_into_keyset(
        &self,
        batches: &[RecordBatch],
        pk_indices: &[usize],
        converter: &RowConverter,
        keyset: &mut CachedPkKeyset,
    ) -> Result<()> {
        for batch in batches {
            let pk_columns: Vec<_> = pk_indices
                .iter()
                .map(|idx| Arc::clone(batch.column(*idx)))
                .collect();
            let rows = converter.convert_columns(&pk_columns)?;

            for row_index in 0..batch.num_rows() {
                if pk_columns.iter().any(|column| column.is_null(row_index)) {
                    return Err(Error::DataValidation {
                        table: self.table_metadata.table_name.clone(),
                        message: format!(
                            "Null primary key encountered in inlined data for table {}",
                            self.table_metadata.table_name,
                        ),
                    });
                }
                keyset.insert(rows.row(row_index).owned(), RowLocation::Inlined);
            }
        }

        Ok(())
    }

    /// Process a record batch stream and add visible keys to the keyset.
    ///
    /// Filters out deleted rows using the provided deletion maps. No `insert_records` are
    /// used — visibility is determined solely by whether a deletion exists for the key.
    ///
    /// `min_delete_seq_threshold`: When `Some(threshold)`, only deletions with
    /// `seq > threshold` are considered (for protected snapshots). When `None`, all
    /// deletions apply (for the main listing table). This avoids building filtered
    /// `HashMap` copies per snapshot — each row is checked with a single O(1) lookup.
    ///
    /// Keys from later batches override earlier ones in the keyset, which is correct
    /// because protected snapshots contain data inserted at higher sequence numbers.
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn process_stream_into_keyset(
        mut stream: SendableRecordBatchStream,
        pk_deletion_strategy: &PkDeletionStrategyWithCache,
        pk_indices: &[usize],
        converter: &RowConverter,
        projected_pk_indices: &[usize],
        deleted_pk_i64: Option<&DeletionIndex>,
        deleted_row_keys: Option<&KeyDeletionIndex>,
        min_delete_seq_threshold: Option<i64>,
        table_name: &str,
        keyset: &mut CachedPkKeyset,
        row_id_base: &mut i64,
    ) -> Result<()> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let pk_columns: Vec<_> = projected_pk_indices
                .iter()
                .map(|idx| Arc::clone(batch.column(*idx)))
                .collect();

            let rows = converter.convert_columns(&pk_columns)?;

            // For Int64Pk strategy, get the PK column as Int64Array for efficient lookup
            let int64_pk_array: Option<&arrow::array::Int64Array> =
                if pk_deletion_strategy.is_int64_pk() && pk_indices.len() == 1 {
                    batch.column(0).as_any().downcast_ref()
                } else {
                    None
                };

            for row_idx in 0..batch.num_rows() {
                // Check if row is deleted based on pk_deletion_strategy.
                // For main batches (threshold=None): all deletions apply.
                // For protected snapshots (threshold=Some(T)): only deletions with seq > T apply.
                let is_deleted = match pk_deletion_strategy {
                    PkDeletionStrategyWithCache::Int64Pk { .. } => {
                        if let (Some(pk_array), Some(deleted_pks)) =
                            (int64_pk_array, deleted_pk_i64)
                        {
                            let pk_value = pk_array.value(row_idx);
                            match deleted_pks.get(pk_value) {
                                None => false, // not deleted (bloom-prefiltered)
                                Some(tombstone) => match min_delete_seq_threshold {
                                    None => true, // all deletions apply
                                    Some(threshold) => tombstone.delete_sequence > threshold,
                                },
                            }
                        } else {
                            false
                        }
                    }
                    PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                        if let Some(deleted_keys) = deleted_row_keys {
                            let key = rows.row(row_idx);
                            match deleted_keys.get(key.as_ref()) {
                                None => false, // not deleted (bloom-prefiltered)
                                Some(tombstone) => match min_delete_seq_threshold {
                                    None => true, // all deletions apply
                                    Some(threshold) => tombstone.delete_sequence > threshold,
                                },
                            }
                        } else {
                            false
                        }
                    }
                    PkDeletionStrategyWithCache::PositionBased { .. } => {
                        unreachable!("PositionBased strategy should not reach load_existing_keyset")
                    }
                };

                if is_deleted {
                    continue;
                }

                // Enforce non-null primary key values
                let has_null = pk_columns.iter().any(|col| col.is_null(row_idx));
                if has_null {
                    return Err(Error::DataValidation {
                        table: table_name.to_string(),
                        message: format!(
                            "Null primary key encountered in existing data for table {table_name}",
                        ),
                    });
                }

                let key = rows.row(row_idx).owned();

                // Insert or update the key in the keyset.
                // Keys from protected snapshots may override keys from the main listing table
                // because protected snapshots contain data inserted at higher sequence numbers.
                // This is expected behavior for upserts.
                //
                // Cold rebuild cannot cheaply assign file-local positions: this scan
                // unions all files, so scan order != per-file position. The entry is
                // `FileUnlocated` and falls back to key-based deletes until a later
                // write/compaction row_idx() read-back upgrades it to `FilePositioned`.
                keyset.insert(key, RowLocation::FileUnlocated);
            }

            *row_id_base += i64::try_from(batch.num_rows()).map_err(|_| Error::Internal {
                table: table_name.to_string(),
                message: "Batch row count exceeds i64::MAX; cannot compute row_id_base".to_string(),
            })?;
        }

        Ok(())
    }
}
