//! Deletion-vector serialization, on-conflict deletion staging/publish, and PK deletion snapshots.
//!
//! Covers the tagged tombstone blob format ([`tombstone_format`]), the coherent
//! per-scan [`PkDeletionSnapshot`], inlined-mutation commit/publish
//! (`commit_inlined_data_durable` → `publish_inlined_mutation` under
//! `scan_state_lock.write()`), and the staged CDC-upsert pair:
//! [`CayenneTableProvider::prepare_on_conflict_deletions_for_staged_snapshot`]
//! (Stage A — durable writes + sequence reservation, no visibility change) and
//! [`CayenneTableProvider::publish_prepared_on_conflict_deletions`] (Stage B —
//! in-memory publish, called with `listing_fence.write()` held by the caller).
//! Protected-snapshot publishes go through `scan_state_lock.write()`.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use arrow::array::Array;
use datafusion_catalog::Session;

use super::{
    Arc, BTreeMap, BinaryArray, CatalogError, CatalogResult, CayenneDeletionSink,
    CayenneTableProvider, Cow, DataType, DeletionIdentifier, DeletionIndex,
    DeletionVectorWriteResult, DeletionVectorWriteSpec, DeletionVectorWriter, Error, Expr, Field,
    HashMap, HashSet, InlinedData, InlinedDataRewrite, InlinedDelete, InlinedDurableCommit,
    Instant, Int64PkDeletionSnapshot, KeyDeletionIndex, ObjectStoreExt, OnConflictDeletionUpdate,
    OnConflictDeletions, OnConflictUpdate, Ordering, PkDeletionStrategyWithCache,
    PositionDeletionVector, PreparedOnConflictDeletionPublish, RecordBatch, Result, RoaringBitmap,
    RowConverter, RowConverterDeletionSnapshot, SnapshotSequenceCommit, TryStreamExt,
    record_cayenne_write_phase,
};

/// Serialize one or more `RecordBatch`es to Arrow IPC stream bytes.
pub(super) fn serialize_batches_to_ipc(
    batches: &[RecordBatch],
) -> std::result::Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    if let Some(first) = batches.first() {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, first.schema_ref())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserialize Arrow IPC bytes back to a `RecordBatch`.
pub(super) fn deserialize_ipc_to_batch(
    ipc_bytes: &[u8],
) -> std::result::Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)?;
    reader.collect()
}

/// Tombstone payload format discriminator (cycle-5 TASK 2a).
///
/// The byte is a PREFIX on the `delete_ipc` blob. It can never collide with a
/// legacy (pre-cycle-5) blob: those are a raw Arrow IPC *stream*, whose first
/// byte is the IPC continuation marker `0xFF` (`CONTINUATION_MARKER = [0xff; 4]`
/// in arrow-ipc, written for the non-legacy V5 stream format we use). So a
/// deserializer that sees a leading `0xFF` routes to the legacy uncompressed-IPC
/// reader, keeping on-disk `published = 0` tombstones written by an older binary
/// readable across an in-place upgrade.
pub(super) mod tombstone_format {
    /// Packed raw big-endian `i64` keys, no Arrow framing. Used for `Int64Pk`
    /// tables: the keys are already 8-byte BE, so the blob is `[TAG][key0..key1..]`
    /// — 8 bytes/key with ZERO schema/offset/padding overhead (vs Arrow IPC's
    /// ~hundreds of bytes of schema header + 4-byte offset per key + 64-byte
    /// alignment padding). This both shrinks the WAL frame written under the
    /// Stage-A `BEGIN IMMEDIATE` and removes the IPC decode on the read path.
    // `pub` (not `pub(super)`) so that `table::tests` can inspect format tags.
    pub const PACKED_I64: u8 = 0x00;
    /// LZ4_FRAME-compressed Arrow IPC of the `row_key` `BinaryArray`. Used for
    /// composite-key (`RowConverterBased`) tables, whose encoded row-keys share
    /// prefixes and compress well. Decompression is automatic in the IPC reader.
    // `pub` (not `pub(super)`) so that `table::tests` can inspect format tags.
    pub const COMPRESSED_IPC: u8 = 0x01;
}

pub(super) fn deserialize_delete_keys_from_ipc(
    ipc_bytes: &[u8],
) -> std::result::Result<Vec<Box<[u8]>>, arrow::error::ArrowError> {
    // Empty blob (never written today, but defensive) → no keys.
    let Some((&tag, rest)) = ipc_bytes.split_first() else {
        return Ok(Vec::new());
    };

    match tag {
        // cycle-5 TASK 2a: packed raw BE i64 keys, no Arrow framing.
        tombstone_format::PACKED_I64 => {
            if rest.len() % 8 != 0 {
                return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                    "Packed-i64 tombstone payload length {} is not a multiple of 8",
                    rest.len()
                )));
            }
            Ok(rest
                .chunks_exact(8)
                .map(|chunk| chunk.to_vec().into_boxed_slice())
                .collect())
        }
        // cycle-5 TASK 2a: LZ4-compressed Arrow IPC (composite keys).
        tombstone_format::COMPRESSED_IPC => deserialize_delete_keys_from_arrow_ipc(rest),
        // Legacy (pre-cycle-5) blob: a bare uncompressed Arrow IPC stream whose
        // first byte is the `0xFF` continuation marker. `tag` is that first byte,
        // so decode the WHOLE original slice (tag included).
        _ => deserialize_delete_keys_from_arrow_ipc(ipc_bytes),
    }
}

/// Decode the `row_key` `BinaryArray` Arrow IPC stream (uncompressed legacy OR
/// LZ4-compressed; the IPC reader auto-detects the batch compression) into the
/// raw key byte vectors.
pub(super) fn deserialize_delete_keys_from_arrow_ipc(
    ipc_bytes: &[u8],
) -> std::result::Result<Vec<Box<[u8]>>, arrow::error::ArrowError> {
    let batches = deserialize_ipc_to_batch(ipc_bytes)?;
    let mut row_keys = Vec::new();

    for batch in batches {
        let Some(row_key_array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
        else {
            return Err(arrow::error::ArrowError::CastError(
                "Expected BinaryArray for inlined delete row_key column".to_string(),
            ));
        };

        row_keys.reserve(row_key_array.len());
        for row_index in 0..row_key_array.len() {
            if !row_key_array.is_null(row_index) {
                row_keys.push(row_key_array.value(row_index).to_vec().into_boxed_slice());
            }
        }
    }

    Ok(row_keys)
}

/// Serialize raw delete-identifier byte keys into the compact `delete_ipc` blob
/// that [`deserialize_delete_keys_from_ipc`] reads back.
///
/// Used by the on-conflict upsert path to write an inline tombstone
/// (`cayenne_inlined_delete`) that hides the prior inline copy of an upserted
/// PK, instead of re-decoding and rewriting the entire inline corpus.
///
/// cycle-5 TASK 2a — the encoding is chosen by `is_int64_pk` to minimise the
/// multi-MB-per-batch WAL traffic (and the autocheckpoint cadence it drives,
/// see TASK 2b) that the prior plain-Arrow-IPC encoding caused on heavy-upsert
/// tables like `stock`:
/// - **`Int64Pk`** (`is_int64_pk == true`): each key is the PK's 8-byte BE
///   encoding (`build_pk_deletion_row_keys` + `CayenneTableProvider::row_key_to_i64`),
///   so the keys are packed RAW behind a 1-byte tag — no Arrow schema header, no
///   per-key offset, no alignment padding. Roughly halves the blob vs Arrow IPC
///   for ~180K keys AND drops the IPC decode on the read path.
/// - **`RowConverterBased`** (composite keys): Arrow IPC of the `row_key`
///   `BinaryArray` (column 0), now LZ4_FRAME-compressed (the codec is already
///   compiled into `arrow-ipc`). Encoded composite row-keys share prefixes, so
///   LZ4 shrinks the blob materially with negligible CPU.
pub(super) fn serialize_delete_keys_to_ipc(
    keys: &[Box<[u8]>],
    is_int64_pk: bool,
) -> std::result::Result<Vec<u8>, arrow::error::ArrowError> {
    if is_int64_pk {
        // Packed raw BE i64 keys behind the format tag. The keys are produced by
        // `build_pk_deletion_row_keys` as exactly 8 BE bytes for an Int64Pk
        // table; assert that contract so a stray non-8-byte key fails loudly
        // here rather than silently corrupting the packed stream.
        let mut out = Vec::with_capacity(1 + keys.len() * 8);
        out.push(tombstone_format::PACKED_I64);
        for key in keys {
            if key.len() != 8 {
                return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                    "Int64Pk tombstone key must be 8 bytes, got {}",
                    key.len()
                )));
            }
            out.extend_from_slice(key);
        }
        return Ok(out);
    }

    let array = BinaryArray::from_iter_values(keys.iter().map(std::convert::AsRef::as_ref));
    let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
        "row_key",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])?;

    // LZ4_FRAME-compress the IPC stream. Metadata defaults to V5 (compression
    // requires V5+), so this is always valid for our writer.
    let write_options = arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))?;
    let mut out = Vec::new();
    out.push(tombstone_format::COMPRESSED_IPC);
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
            &mut out,
            batch.schema_ref(),
            write_options,
        )?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(out)
}

#[derive(Clone)]
pub(super) enum PkDeletionSnapshot {
    PositionBased,
    Int64Pk { tombstones: Arc<DeletionIndex> },
    RowConverterBased { tombstones: Arc<KeyDeletionIndex> },
}

impl PkDeletionSnapshot {
    pub(super) fn has_deletions(&self) -> bool {
        match self {
            Self::PositionBased => false,
            Self::Int64Pk { tombstones } => tombstones.has_deletions(),
            Self::RowConverterBased { tombstones } => tombstones.has_deletions(),
        }
    }

    pub(super) fn with_mem_tier_tombstones(
        &self,
        mem_tier: &crate::provider::mem_tier::MemTier,
    ) -> Self {
        match self {
            Self::PositionBased => Self::PositionBased,
            Self::Int64Pk { tombstones } => {
                if mem_tier.tombstones.int64_pk.is_empty() {
                    return self.clone();
                }

                let updated = tombstones.extend_max_deletes(
                    mem_tier
                        .tombstones
                        .int64_pk
                        .iter()
                        .map(|(&pk, &delete_sequence)| (pk, delete_sequence)),
                );
                Self::Int64Pk {
                    tombstones: Arc::new(updated),
                }
            }
            Self::RowConverterBased { tombstones } => {
                if mem_tier.tombstones.row_keys.is_empty() {
                    return self.clone();
                }

                let updated = tombstones.extend_max_deletes(
                    mem_tier
                        .tombstones
                        .row_keys
                        .iter()
                        .map(|(key, &delete_sequence)| (key.as_ref(), delete_sequence)),
                );
                Self::RowConverterBased {
                    tombstones: Arc::new(updated),
                }
            }
        }
    }

    /// The highest delete sequence reflected in THIS coherent snapshot.
    ///
    /// Because every deletion update builds an extended index followed by a single
    /// atomic `deletion_snapshot.store(...)`, a snapshot obtained from one load
    /// reflects all deletions up to this value. Deriving the compaction fence
    /// from the same snapshot (rather than a second, independently loaded
    /// snapshot's `max_sequence_number()`) is required for correctness — see
    /// `compact_protected_snapshots_subset`.
    pub(super) fn max_sequence_number(&self) -> Option<i64> {
        match self {
            Self::PositionBased => None,
            Self::Int64Pk { tombstones } => tombstones.max_sequence_number(),
            Self::RowConverterBased { tombstones } => tombstones.max_sequence_number(),
        }
    }
}

pub(super) fn pk_deletion_snapshot_for_strategy(
    strategy: &PkDeletionStrategyWithCache,
) -> PkDeletionSnapshot {
    match strategy {
        PkDeletionStrategyWithCache::PositionBased { .. } => PkDeletionSnapshot::PositionBased,
        PkDeletionStrategyWithCache::Int64Pk {
            deletion_snapshot, ..
        } => {
            let snapshot = deletion_snapshot.load_full();
            PkDeletionSnapshot::Int64Pk {
                tombstones: Arc::clone(&snapshot.tombstones),
            }
        }
        PkDeletionStrategyWithCache::RowConverterBased {
            deletion_snapshot, ..
        } => {
            let snapshot = deletion_snapshot.load_full();
            PkDeletionSnapshot::RowConverterBased {
                tombstones: Arc::clone(&snapshot.tombstones),
            }
        }
    }
}

pub(super) struct ProtectedSnapshotScan<'a> {
    pub(super) state: &'a dyn Session,
    pub(super) projection: Option<&'a Vec<usize>>,
    pub(super) filters: &'a [Expr],
    pub(super) limit: Option<usize>,
    pub(super) pk_indices_in_projection: &'a [usize],
    pub(super) protected_snapshots: Arc<HashMap<String, i64>>,
    pub(super) deletion_snapshot: &'a PkDeletionSnapshot,
}

pub(super) struct PreparedProtectedSnapshotUpdate {
    pub(super) expected: Arc<HashMap<String, i64>>,
    pub(super) updated: Arc<HashMap<String, i64>>,
}

/// Tier-0 size ceiling for protected-snapshot leveling, in bytes (8 MiB).
///
/// Runs at or below this size are tier 0; each subsequent tier is
/// [`PROTECTED_TIER_GROWTH`]× larger. Function-scoped constant for now —
/// promote to a `CayenneContext` config knob when the policy stabilizes.
pub(super) const PROTECTED_TIER_BASE_BYTES: u64 = 8 * 1024 * 1024;

/// Geometric growth factor between protected-snapshot size tiers.
pub(super) const PROTECTED_TIER_GROWTH: u64 = 8;

/// Hard cap on the number of runs consolidated in a single fast merge pass,
/// bounding the per-pass read/write amplification regardless of how many
/// same-tier runs have accumulated.
pub(super) const PROTECTED_MERGE_MAX_WIDTH: usize = 32;

/// Classify a protected snapshot's on-disk byte size into an LSM-style size
/// tier. Tier 0 covers everything up to `base_bytes`; each higher tier covers
/// up to `growth`× the previous tier's ceiling.
///
/// Pure and total: returns 0 for `bytes <= base_bytes` or a degenerate
/// `growth <= 1`, and saturates (stops climbing) on multiplication overflow so
/// arbitrarily large inputs map to the top representable tier rather than
/// panicking.
pub(super) fn protected_snapshot_size_tier(bytes: u64, base_bytes: u64, growth: u64) -> u32 {
    if bytes <= base_bytes || growth <= 1 {
        return 0;
    }
    let mut ceiling = base_bytes;
    let mut tier: u32 = 0;
    loop {
        match ceiling.checked_mul(growth) {
            Some(next) => ceiling = next,
            // Overflow: this is the largest tier we can represent; stop here.
            None => return tier,
        }
        tier += 1;
        if bytes <= ceiling {
            return tier;
        }
    }
}

/// Select which same-size protected snapshots to consolidate this pass.
///
/// LSM-style leveling: assign every input to a size tier, then pick the
/// **lowest** tier that has accumulated at least `min_runs` runs and merge
/// those (oldest-first, capped at `max_width`). Merging only within a tier
/// bounds write amplification to O(log N) per byte (a run is rewritten only
/// when it levels up), while the `min_runs` threshold bounds read amplification
/// by keeping at most `min_runs - 1` un-merged runs per tier. The large
/// carried-forward run sits alone in a high tier and is rewritten rarely
/// instead of on every pass.
///
/// `inputs` is `(snapshot_id, deletion_threshold, bytes)`. Returns the selected
/// `(snapshot_id, deletion_threshold)` pairs oldest-first (input order is
/// assumed oldest-first, i.e. `UUIDv7` lexical order), or empty if no tier has
/// `>= min_runs` runs.
pub(super) fn select_protected_snapshot_merge_tier(
    inputs: &[(String, i64, u64)],
    min_runs: usize,
    max_width: usize,
    base_bytes: u64,
    growth: u64,
) -> Vec<(String, i64)> {
    if inputs.len() < 2 || min_runs < 2 {
        // A merge needs at least two runs, and a floor below 2 is meaningless.
        return Vec::new();
    }

    // Group input indices by tier, preserving oldest-first order within a tier.
    let mut tiers: std::collections::BTreeMap<u32, Vec<usize>> = std::collections::BTreeMap::new();
    for (idx, (_, _, bytes)) in inputs.iter().enumerate() {
        let tier = protected_snapshot_size_tier(*bytes, base_bytes, growth);
        tiers.entry(tier).or_default().push(idx);
    }

    // BTreeMap iterates tiers in ascending order, so the first qualifying tier
    // is the lowest one.
    for (_tier, indices) in tiers {
        if indices.len() >= min_runs {
            return indices
                .into_iter()
                .take(max_width.max(2))
                .map(|i| {
                    let (id, threshold, _) = &inputs[i];
                    (id.clone(), *threshold)
                })
                .collect();
        }
    }

    Vec::new()
}

/// Write shape — encoder fan-out cap and size estimate — for the
/// subset-merge output (see `compact_protected_snapshots_subset`).
///
/// Pure so the position-vs-key parallel decision is unit-testable: when
/// `keeps_positions_serial` (the table carries position-scoped deletes —
/// either a PK table whose resolved `deletion_mode` is `position`, or a
/// PK-less table on the legacy `PositionBased` strategy), the merge keeps the
/// serial single-writer shape `(1, None)`; otherwise it passes the session's
/// `target_partitions` and the selected tier's total bytes so
/// `snapshot_shard_count` sizes a parallel encoder fan-out
/// (`floor(bytes / target_file_size)`, min 1, capped by write concurrency
/// and the global encode budget).
///
/// Note: output FILE COUNT is not a proxy for this decision — a single
/// serial writer still rolls multiple files when the merged output exceeds
/// the target file size. This function is the authoritative, testable gate.
pub(super) const fn subset_merge_write_shape(
    keeps_positions_serial: bool,
    session_target_partitions: usize,
    total_input_bytes: u64,
) -> (usize, Option<u64>) {
    if keeps_positions_serial {
        (1, None)
    } else {
        // Clamp to >= 1, matching the defensive treatment of
        // `target_partitions` elsewhere (e.g. vortex `format.rs` and the
        // runtime builder treat 0 as invalid): a zeroed session config must
        // not propagate a 0 cap into the write path.
        let cap = if session_target_partitions == 0 {
            1
        } else {
            session_target_partitions
        };
        (cap, Some(total_input_bytes))
    }
}

impl CayenneTableProvider {
    pub(super) fn adjust_cached_inlined_row_count(&self, delta: i64) {
        let _ =
            self.inlined_row_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(if delta >= 0 {
                        current.saturating_add(delta)
                    } else {
                        current.saturating_sub(delta.saturating_abs())
                    })
                });
    }

    pub(super) fn rewritten_inlined_data_entry(
        source: &InlinedData,
        batches: &[RecordBatch],
        record_count: usize,
    ) -> Result<InlinedData> {
        let data_ipc = serialize_batches_to_ipc(batches)?;

        Ok(InlinedData {
            inlined_id: source.inlined_id.clone(),
            table_id: source.table_id.clone(),
            partition_key: source.partition_key.clone(),
            data_ipc,
            record_count: i64::try_from(record_count).unwrap_or(i64::MAX),
            sequence_number: source.sequence_number,
            created_at: source.created_at.clone(),
        })
    }

    pub(super) fn filter_inlined_batch_for_pk_deletions(
        &self,
        batch: RecordBatch,
        deleted_pk_i64: &HashSet<i64>,
        deleted_row_keys: &HashSet<Box<[u8]>>,
    ) -> Result<(Option<RecordBatch>, usize)> {
        if batch.num_rows() == 0 {
            return Ok((None, 0));
        }

        let pk_indices = &self.pk_column_indices;
        if pk_indices.is_empty() {
            return Ok((Some(batch), 0));
        }

        let mut keep_mask = Vec::with_capacity(batch.num_rows());
        let mut removed_rows = 0_usize;

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                if deleted_pk_i64.is_empty() {
                    return Ok((Some(batch), 0));
                }

                let pk_array = batch
                    .column(pk_indices[0])
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| Error::DataValidation {
                        table: self.table_metadata.table_name.clone(),
                        message: "Int64 primary key column has unexpected type".to_string(),
                    })?;

                for row_index in 0..batch.num_rows() {
                    if pk_array.is_null(row_index) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let should_delete = deleted_pk_i64.contains(&pk_array.value(row_index));
                    keep_mask.push(!should_delete);
                    removed_rows += usize::from(should_delete);
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                if deleted_row_keys.is_empty() {
                    return Ok((Some(batch), 0));
                }

                // Reuse the table's cached RowConverter when available — building
                // a fresh one revalidates each SortField.
                let owned_converter;
                let converter: &RowConverter = if let Some(c) = self.pk_row_converter.as_deref() {
                    c
                } else {
                    owned_converter = self.build_pk_converter(pk_indices)?;
                    &owned_converter
                };

                let pk_columns: Vec<_> = pk_indices
                    .iter()
                    .map(|idx| Arc::clone(batch.column(*idx)))
                    .collect();
                let rows = converter.convert_columns(&pk_columns)?;

                for row_index in 0..batch.num_rows() {
                    if pk_columns.iter().any(|column| column.is_null(row_index)) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let should_delete = deleted_row_keys.contains(rows.row(row_index).as_ref());
                    keep_mask.push(!should_delete);
                    removed_rows += usize::from(should_delete);
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => return Ok((Some(batch), 0)),
        }

        if removed_rows == 0 {
            return Ok((Some(batch), 0));
        }
        if removed_rows == batch.num_rows() {
            return Ok((None, removed_rows));
        }

        let filter_array = arrow::array::BooleanArray::from(keep_mask);
        let filtered_batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;
        Ok((Some(filtered_batch), removed_rows))
    }

    pub(super) async fn build_inlined_data_rewrite_for_pk_keys(
        &self,
        deleted_pk_i64: &[i64],
        deleted_row_keys: &[Box<[u8]>],
    ) -> Result<InlinedDataRewrite> {
        let deleted_pk_i64: HashSet<i64> = deleted_pk_i64.iter().copied().collect();
        let deleted_row_keys: HashSet<Box<[u8]>> = deleted_row_keys.iter().cloned().collect();
        if deleted_pk_i64.is_empty() && deleted_row_keys.is_empty() {
            return Ok(InlinedDataRewrite::default());
        }

        // Use the generation-keyed cache to avoid a second metastore round-trip
        // and IPC re-decode on every upsert. The batches in each entry are
        // already deletion-map-filtered, so we skip that step here.
        let view = self.cached_inlined_view().await?;
        if view.is_empty() {
            return Ok(InlinedDataRewrite::default());
        }

        let mut rewrite = InlinedDataRewrite::default();

        for entry in view.iter() {
            // `entry.batches` are already deletion-map filtered; count visible rows.
            let original_rows: usize = entry.batches.iter().map(RecordBatch::num_rows).sum();
            let mut rewritten_batches = Vec::with_capacity(entry.batches.len());
            let mut remaining_rows = 0_usize;
            let mut entry_removed_rows = 0_usize;

            for batch in &entry.batches {
                let (filtered_batch, removed_rows) = self.filter_inlined_batch_for_pk_deletions(
                    batch.clone(),
                    &deleted_pk_i64,
                    &deleted_row_keys,
                )?;
                entry_removed_rows += removed_rows;
                if let Some(batch) = filtered_batch {
                    remaining_rows += batch.num_rows();
                    rewritten_batches.push(batch);
                }
            }

            if entry_removed_rows == 0 {
                continue;
            }

            rewrite.removed_rows += original_rows.saturating_sub(remaining_rows);
            if remaining_rows == 0 {
                rewrite
                    .deleted_inlined_ids
                    .push(entry.envelope.inlined_id.clone());
            } else {
                rewrite
                    .updated_data
                    .push(Self::rewritten_inlined_data_entry(
                        &entry.envelope,
                        &rewritten_batches,
                        remaining_rows,
                    )?);
            }
        }

        // This is the O(corpus) inline-rewrite fallback (still live on the
        // inline-insert path). Count it only when it actually removed superseded
        // inline rows, so the tombstone-vs-rewrite ratio (paired with
        // `track_cayenne_inline_tombstone_write`) reflects real rewrite work.
        if rewrite.removed_rows > 0 {
            telemetry::track_cayenne_inline_rewrite_fallback(&[telemetry::KeyValue::new(
                "table",
                self.table_metadata.table_name.clone(),
            )]);
        }

        Ok(rewrite)
    }

    /// Update the in-memory PK deletion cache to immediately hide file-backed
    /// rows that have been superseded by inlined data.
    pub(super) fn update_file_deletion_cache(
        &self,
        deleted_pk_i64: &[i64],
        deleted_row_keys: &[Box<[u8]>],
        delete_sequence: i64,
    ) {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => {
                if deleted_pk_i64.is_empty() {
                    return;
                }
                let current = deletion_snapshot.load_full();
                let updated = current
                    .tombstones
                    .extend_max_deletes(deleted_pk_i64.iter().map(|&pk| (pk, delete_sequence)));
                deletion_snapshot.store(Arc::new(Int64PkDeletionSnapshot::from_index(updated)));
                self.refresh_deletion_memory_accounting();
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => {
                if deleted_row_keys.is_empty() {
                    return;
                }
                let current = deletion_snapshot.load_full();
                let updated = current
                    .tombstones
                    .extend_max_deletes(deleted_row_keys.iter().map(|key| (key, delete_sequence)));
                deletion_snapshot
                    .store(Arc::new(RowConverterDeletionSnapshot::from_index(updated)));
                self.refresh_deletion_memory_accounting();
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based tables don't support upserts.
            }
        }
    }

    /// After `checkpoint_inlined_data` flushes inline rows to a Vortex file at
    /// `flush_sequence`, walk the supplied PKs and upgrade any pre-existing
    /// delete-only tombstone (`insert_seq=None`) to record `insert_seq=flush_sequence`.
    ///
    /// Without this upgrade, listing-time pruning via
    /// `vortex_key_delete_pushdown_filter` and the runtime
    /// `Int64PkDeletionFilterExec` keep treating those PKs as fully hidden and
    /// drop the brand-new checkpoint file's rows. The upgrade is also persisted
    /// to the catalog as paired insert records so the same state is rebuilt on
    /// restart.
    pub(super) async fn upgrade_tombstones_for_flushed_pks(
        &self,
        flushed_pks: &[i64],
        flush_sequence: i64,
    ) -> Result<()> {
        if flushed_pks.is_empty() {
            return Ok(());
        }
        let PkDeletionStrategyWithCache::Int64Pk {
            deletion_snapshot, ..
        } = &self.pk_deletion_strategy
        else {
            return Ok(());
        };

        let current = deletion_snapshot.load_full();
        // Group PKs needing an upgrade by their existing delete_sequence so we
        // can do one `extend_max_conflicts` call per group.
        let mut by_delete_seq: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        for &pk in flushed_pks {
            if let Some(t) = current.tombstones.get(pk)
                && t.insert_sequence.is_none()
            {
                by_delete_seq.entry(t.delete_sequence).or_default().push(pk);
            }
        }
        if by_delete_seq.is_empty() {
            return Ok(());
        }

        let insert_pk_bytes: Vec<Vec<u8>> = by_delete_seq
            .values()
            .flatten()
            .map(|pk| pk.to_be_bytes().to_vec())
            .collect();

        self.catalog
            .commit_on_conflict_deletions(
                Vec::new(),
                &self.table_metadata.table_id,
                insert_pk_bytes,
                flush_sequence,
                None,
            )
            .await
            .map_err(|err| Error::Catalog { source: err })?;

        let mut updated = current.tombstones.as_ref().clone();
        for (delete_seq, pks) in &by_delete_seq {
            updated =
                updated.extend_max_conflicts(pks.iter().copied(), *delete_seq, flush_sequence);
        }
        deletion_snapshot.store(Arc::new(Int64PkDeletionSnapshot::from_index(updated)));
        self.refresh_deletion_memory_accounting();
        Ok(())
    }

    pub(super) async fn commit_inlined_data_mutation(
        &self,
        rewrite: InlinedDataRewrite,
        data: Vec<InlinedData>,
        appended_rows: usize,
        assigned_sequence: Option<i64>,
    ) -> CatalogResult<()> {
        let Some(commit) = self
            .commit_inlined_data_durable(rewrite, data, assigned_sequence)
            .await?
        else {
            return Ok(());
        };
        self.publish_inlined_mutation(appended_rows, commit.removed_rows, commit.published_seq);
        Ok(())
    }

    /// Durably commit an inlined-data mutation to the catalog WITHOUT publishing
    /// the in-memory visibility change. Returns `Some(InlinedDurableCommit)`
    /// when a commit occurred, or `None` when there was nothing to commit.
    ///
    /// `assigned_sequence` (lever B2) is the sequence to stamp on appended `data`
    /// rows — it MUST be `Some` whenever `data` is non-empty (reserved by the
    /// caller from the in-memory allocator, strictly above any paired
    /// `delete_seq`). It is ignored for rewrite-/delete-only mutations (empty
    /// `data`), which append no rows and therefore consume no sequence.
    pub(super) async fn commit_inlined_data_durable(
        &self,
        rewrite: InlinedDataRewrite,
        data: Vec<InlinedData>,
        assigned_sequence: Option<i64>,
    ) -> CatalogResult<Option<InlinedDurableCommit>> {
        if rewrite.is_empty() && data.is_empty() {
            return Ok(None);
        }

        // Appended rows require a caller-reserved sequence; a rewrite-/delete-only
        // mutation appends nothing and the value is unused by the catalog impl.
        let stamp_sequence = if data.is_empty() {
            assigned_sequence.unwrap_or(0)
        } else {
            assigned_sequence.ok_or_else(|| CatalogError::InvalidOperationNoSource {
                message: format!(
                    "internal: inline append for table {} has no reserved sequence (lever B2)",
                    self.table_metadata.table_name
                ),
            })?
        };

        let removed_rows = rewrite.removed_rows;
        let published_seq = self
            .catalog
            .commit_inlined_mutation(
                &self.table_metadata.table_id,
                rewrite.updated_data,
                rewrite.deleted_inlined_ids,
                data,
                stamp_sequence,
            )
            .await?;

        Ok(Some(InlinedDurableCommit {
            removed_rows: i64::try_from(removed_rows).unwrap_or(i64::MAX),
            published_seq,
        }))
    }

    /// Publish a previously-durable inlined mutation into the in-memory caches:
    /// adjust the cached live-row count and bump `inlined_generation` so the
    /// next scan rebuilds the inlined view.
    pub(super) fn publish_inlined_mutation(
        &self,
        appended_rows: usize,
        removed_rows: i64,
        published_seq: Option<i64>,
    ) {
        let appended_rows = i64::try_from(appended_rows).unwrap_or(i64::MAX);
        self.adjust_cached_inlined_row_count(appended_rows.saturating_sub(removed_rows));
        // Advance the visibility watermark BEFORE bumping the generation: the
        // generation bump's `Release` store (paired with the Acquire load in
        // `read_inlined_batches`) publishes the watermark store, so a scan that
        // observes the new generation also observes the advanced watermark and
        // includes the freshly appended entry.
        if let Some(seq) = published_seq {
            self.published_inlined_seq.fetch_max(seq, Ordering::Release);
        }
        // Structural vs append-only provenance for the incremental inline cache:
        // a mutation that rewrote or removed any existing inline entry
        // (`removed_rows > 0`, which implies non-empty `updated_data` /
        // `deleted_inlined_ids`) can retroactively change an already-materialized
        // view, so it must bump the structural epoch and force the next miss to
        // full-rebuild. A pure append (`removed_rows == 0`) only adds new entries
        // above the corpus max and is delta-safe, so it bumps the generation
        // alone — letting `populate_inlined_cache` extend the cached view with
        // just the appended rows. See the `InlinedCache` contract.
        if removed_rows != 0 {
            self.bump_inlined_structural_epoch();
        } else {
            self.bump_inlined_generation();
        }
    }

    /// Invalidate the inline cache by advancing `inlined_generation`.
    ///
    /// The `Release` store is paired with the `Acquire` load in
    /// `cached_inlined_view` / `read_inlined_batches`, so any state written
    /// before this call (a durable inline tombstone, an advanced watermark) is
    /// visible to a scan that observes the new generation. The next inline read
    /// misses the cache and rebuilds from the metastore.
    ///
    /// This is the **append-only** invalidation: the structural epoch is left
    /// unchanged, so the next miss may extend the cached view with the delta
    /// instead of rebuilding it. Use [`Self::bump_inlined_structural_epoch`] for
    /// any mutation that can change an already-materialized entry (rewrite,
    /// removal, tombstone, checkpoint, overwrite, recovery).
    pub(super) fn bump_inlined_generation(&self) {
        self.inlined_generation.fetch_add(1, Ordering::Release);
    }

    /// Invalidate the inline cache AND mark the change structural so the next
    /// miss full-rebuilds rather than taking the append-only delta path.
    ///
    /// Advances `inlined_structural_epoch` (which no `InlinedCache` view will
    /// match) and then `inlined_generation`. The structural-epoch store is
    /// published by the same `Release` chain as the generation bump, so a scan
    /// that observes the new generation also observes the advanced structural
    /// epoch (and on a miss correctly chooses the full rebuild). Must be used by
    /// every mutation that can retroactively change an already-materialized
    /// entry: an inline rewrite/removal, a newly published tombstone (whose
    /// re-filter can hide rows in older cached entries), a checkpoint clear, an
    /// overwrite that wipes the inline tables, and open-time orphan recovery.
    pub(super) fn bump_inlined_structural_epoch(&self) {
        self.inlined_structural_epoch
            .fetch_add(1, Ordering::Release);
        self.inlined_generation.fetch_add(1, Ordering::Release);
    }

    /// Convert typed PK values into raw key bytes for deletion vector writing.
    ///
    /// For `Int64Pk` tables, encodes each i64 as big-endian bytes.
    /// For `RowConverterBased` tables, passes through the already-encoded row keys.
    /// Position-based tables don't support upserts and return an empty vec.
    pub(super) fn build_pk_deletion_row_keys<'keys>(
        &self,
        deleted_pk_i64: &[i64],
        deleted_row_keys: Cow<'keys, [Box<[u8]>]>,
    ) -> Cow<'keys, [Box<[u8]>]> {
        match &self.pk_deletion_strategy {
            // Int64 PK tables re-derive keys from the i64 PKs and ignore the
            // encoded row keys, so this allocates only when it must.
            PkDeletionStrategyWithCache::Int64Pk { .. } => Cow::Owned(
                deleted_pk_i64
                    .iter()
                    .map(|&pk| pk.to_be_bytes().to_vec().into_boxed_slice())
                    .collect(),
            ),
            // RowConverter tables reuse the caller's keys verbatim: a borrowed
            // slice stays borrowed (no clone), an owned Vec is moved through.
            PkDeletionStrategyWithCache::RowConverterBased { .. } => deleted_row_keys,
            PkDeletionStrategyWithCache::PositionBased { .. } => Cow::Owned(Vec::new()),
        }
    }

    /// Write key-based deletion vectors to disk and commit them to the catalog.
    ///
    /// This is the shared mechanical step used by both the snapshot upsert path
    /// ([`Self::apply_on_conflict_deletions`]) and the inline upsert path
    /// ([`Self::persist_file_deletions_after_inlined_insert`]). It handles:
    ///
    /// 1. Building deletion vector specs from raw row keys
    /// 2. Writing deletion vector files via [`DeletionVectorWriter`]
    /// 3. Committing delete files + optional insert records to the catalog
    pub(super) async fn write_and_commit_deletion_vectors(
        &self,
        delete_sequence: i64,
        row_keys: Vec<Box<[u8]>>,
        insert_pk_bytes: Vec<Vec<u8>>,
        insert_sequence: i64,
    ) -> CatalogResult<Option<Vec<DeletionVectorWriteResult>>> {
        let Some(results) = self
            .write_key_deletion_vectors(delete_sequence, row_keys)
            .await?
        else {
            return Ok(None);
        };

        if results.is_empty() {
            return Ok(None);
        }

        let delete_files: Vec<crate::metadata::DeleteFile> =
            results.iter().map(|r| r.delete_file.clone()).collect();
        self.catalog
            .commit_on_conflict_deletions(
                delete_files,
                &self.table_metadata.table_id,
                insert_pk_bytes,
                insert_sequence,
                None,
            )
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Failed to commit deletion vectors: {err}"),
            })?;

        Ok(Some(results))
    }

    pub(super) async fn write_key_deletion_vectors(
        &self,
        delete_sequence: i64,
        row_keys: Vec<Box<[u8]>>,
    ) -> CatalogResult<Option<Vec<DeletionVectorWriteResult>>> {
        if row_keys.is_empty() {
            return Ok(None);
        }

        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;
        let writer = DeletionVectorWriter::new(&temp_metadata);

        let specs = vec![DeletionVectorWriteSpec::new_key_based(row_keys)];
        let results = writer.write(specs).await?;

        if results.is_empty() {
            return Ok(None);
        }

        Ok(Some(results))
    }

    pub(super) async fn commit_mem_tier_checkpoint_metadata(
        &self,
        snapshot: &crate::provider::mem_tier::MemTier,
        target_snapshot_id: &str,
        snapshot_sequence: i64,
    ) -> CatalogResult<OnConflictUpdate> {
        let mut delete_files: Vec<crate::metadata::DeleteFile> = Vec::new();
        let mut insert_pk_bytes: Vec<Vec<u8>> = Vec::new();

        let deletion_update = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => {
                if snapshot.tombstones.int64_pk.is_empty() {
                    OnConflictDeletionUpdate::None
                } else {
                    let mut grouped: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
                    for (&pk, &delete_sequence) in &snapshot.tombstones.int64_pk {
                        grouped.entry(delete_sequence).or_default().push(pk);
                        insert_pk_bytes.push(pk.to_be_bytes().to_vec());
                    }

                    let current = deletion_snapshot.load_full();
                    let mut updated = current.tombstones.as_ref().clone();
                    for (delete_sequence, pks) in grouped {
                        let row_keys = pks
                            .iter()
                            .map(|pk| pk.to_be_bytes().to_vec().into_boxed_slice())
                            .collect::<Vec<_>>();
                        if let Some(results) = self
                            .write_key_deletion_vectors(delete_sequence, row_keys)
                            .await?
                        {
                            delete_files
                                .extend(results.iter().map(|result| result.delete_file.clone()));
                        }
                        updated = updated.extend_max_conflicts(
                            pks.iter().copied(),
                            delete_sequence,
                            snapshot_sequence,
                        );
                    }

                    OnConflictDeletionUpdate::Int64Pk(Arc::new(
                        Int64PkDeletionSnapshot::from_index(updated),
                    ))
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => {
                if snapshot.tombstones.row_keys.is_empty() {
                    OnConflictDeletionUpdate::None
                } else {
                    let mut grouped: BTreeMap<i64, Vec<Box<[u8]>>> = BTreeMap::new();
                    for (key, &delete_sequence) in &snapshot.tombstones.row_keys {
                        grouped
                            .entry(delete_sequence)
                            .or_default()
                            .push(key.clone());
                        insert_pk_bytes.push(key.as_ref().to_vec());
                    }

                    let current = deletion_snapshot.load_full();
                    let mut updated = current.tombstones.as_ref().clone();
                    for (delete_sequence, row_keys) in grouped {
                        if let Some(results) = self
                            .write_key_deletion_vectors(delete_sequence, row_keys.clone())
                            .await?
                        {
                            delete_files
                                .extend(results.iter().map(|result| result.delete_file.clone()));
                        }
                        updated = updated.extend_max_conflicts(
                            row_keys.iter().map(|key| &**key),
                            delete_sequence,
                            snapshot_sequence,
                        );
                    }

                    OnConflictDeletionUpdate::RowConverter(Arc::new(
                        RowConverterDeletionSnapshot::from_index(updated),
                    ))
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => OnConflictDeletionUpdate::None,
        };

        self.catalog
            .commit_on_conflict_deletions(
                delete_files,
                &self.table_metadata.table_id,
                insert_pk_bytes,
                snapshot_sequence,
                Some(SnapshotSequenceCommit {
                    snapshot_id: target_snapshot_id.to_string(),
                    sequence_number: snapshot_sequence,
                }),
            )
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!(
                    "Failed to commit in-memory CDC checkpoint metadata for table {}: {err}",
                    self.table_metadata.table_name
                ),
            })?;

        Ok(OnConflictUpdate::from_deletion_update(deletion_update))
    }

    /// Durably write the deletion vectors for a CDC upsert and reserve the new
    /// protected snapshot's sequence, WITHOUT publishing any visibility change.
    /// The returned [`PreparedOnConflictDeletionPublish`] is published later (on
    /// the backgrounded finalize) under one listing fence by
    /// [`Self::publish_prepared_on_conflict_deletions`].
    ///
    /// # Inline-conflict batches stage inert (Option D)
    ///
    /// A batch that replaces *inlined* rows DOES stage. Its inline tombstone is
    /// written here durably with `published = false` at a `delete_sequence`
    /// reserved below `snapshot_sequence`, and stays INERT — the read filter
    /// (`load_inlined_deletion_maps`) skips unpublished tombstones — until this
    /// snapshot finalizes. This is why the staged inline tombstone cannot cause
    /// the transient vanish a naive stage would: an inline-cache rebuild (which a
    /// concurrent same-table inline INSERT can trigger via the
    /// `inlined_generation` bump) running before finalize reads the tombstone but,
    /// seeing `published = false`, does NOT hide the old inline row, so the PK
    /// stays visible (old value) the whole staged window. `publish_prepared_on_conflict_deletions`
    /// activates the tombstone at finalize — under the listing fence, AFTER the
    /// replacement files are moved into the target snapshot — by recording it in
    /// `inlined_locally_published` (in-memory activation; the durable
    /// `published = 1` flip is DEFERRED into `pending_durable_tombstone_flips`,
    /// cycle-4 b1★), so the old row is
    /// hidden exactly when — and never before — the replacement becomes visible.
    /// A global watermark cannot achieve this (advance ⇒ HIDE polarity: a
    /// concurrent same-table inline INSERT, or an out-of-order detached finalize,
    /// advances any monotonic floor past this tombstone's `delete_sequence`,
    /// activating it early). `pending_inline_tombstones` is bumped so the inline
    /// checkpoint defers during the staged window (it would otherwise flush the
    /// old row to a file and clear the inert tombstone — a resurrect).
    ///
    /// # Sequence ordering (correctness-critical)
    ///
    /// All sequences are reserved here, at STAGE time, from the shared monotone
    /// allocator (`reserve_sequences_local`, lever B2, backed by the durable
    /// `current_sequence_number` high-water) — never re-read at finalize. The
    /// `snapshot_sequence` (the protected-snapshot deletion threshold) is the
    /// HIGHEST reserved number, so it is strictly above the `delete_sequence`
    /// (shared by the file `DeleteFile` AND the inline tombstone) and every
    /// previously-committed delete sequence (the reservation advances the counter
    /// past them). A `delete_sequence` is reserved whenever this batch has file
    /// key deletions OR inlined deletions, since the inline tombstone needs one
    /// `>=` the old inline row's sequence (to hide it) and `<` the replacement
    /// snapshot's sequence (so the replacement survives).
    ///
    /// Because the protected snapshot's rows apply only deletions with
    /// `delete_seq > snapshot_sequence` (see `process_stream_into_keyset`), the
    /// replacement rows staged into this snapshot are immune to both this batch's
    /// own conflict deletes and every pre-existing tombstone — they can neither
    /// resurface an old version nor vanish. The inline tombstone hides only inline
    /// ENTRIES (never file rows), and the replacement is a FILE row, so applying
    /// the tombstone never touches the replacement.
    pub(crate) async fn prepare_on_conflict_deletions_for_staged_snapshot(
        &self,
        on_conflict_deletions: OnConflictDeletions,
        target_snapshot_id: String,
    ) -> CatalogResult<PreparedOnConflictDeletionPublish> {
        // Capture the superseded-row count BEFORE destructuring re-encodes the
        // deletions: at validation time each superseded row is counted exactly
        // once (position OR file-i64 OR file-row-key OR inlined), so this is the
        // correct live-row-delta input. See `PreparedOnConflictDeletionPublish::superseded`.
        let superseded = on_conflict_deletions.total_superseded();
        let OnConflictDeletions {
            delete_specs,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        } = on_conflict_deletions;

        // Inline-bearing batches now STAGE inert (Option D): the tombstone is
        // written below with `published = false` and flipped at finalize. See
        // the function doc for why this is vanish-free where a global watermark
        // is not.
        let has_key_deletions = !deleted_pk_i64.is_empty() || !deleted_row_keys.is_empty();
        let has_inlined_deletions =
            !deleted_inlined_pk_i64.is_empty() || !deleted_inlined_row_keys.is_empty();

        // Reserve a `delete_sequence` (below `snapshot_sequence`) whenever the
        // batch has file key deletions OR inlined deletions — the inline
        // tombstone shares it with the file `DeleteFile` (one "hide the prior
        // version at this PK" intent). `insert_sequence` (the re-insertion
        // record) is only needed for file key deletions.
        //   - key deletions:    [delete, insert, snapshot]            (3)
        //   - inline-only:       [delete,         snapshot]            (2)
        //   - neither:                            [snapshot]           (1)
        let sequence_count = if has_key_deletions {
            3
        } else if has_inlined_deletions {
            2
        } else {
            1
        };
        // Stage-A metastore write #1 (sequence reservation). Instrumented as its
        // own phase (`stage_seq_reserve`) so the per-batch metastore-writer time
        // the OLAP-lag audit attributed to "unaccounted" is now visible. This
        // txn stays SEPARATE from the catalog-metadata commit below because the
        // reserved sequences are needed to write the deletion-vector FILES (an
        // object-store write) BEFORE the metadata commit — folding it in would
        // hold the single SQLite writer across object-store I/O, serializing
        // every other table behind this one's DV upload.
        let seq_reserve_start = Instant::now();
        // Lever B2: serve the reserved block from the in-memory allocator. This
        // hits the metastore writer only ~1/`SEQ_RESERVE_BLOCK` batches, so the
        // `stage_seq_reserve` phase below now reads ~0 ms except on a refill.
        let base = self
            .reserve_sequences_local(sequence_count)
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!(
                    "Failed to reserve sequence numbers for staged on-conflict commit: {err}"
                ),
            })?;
        record_cayenne_write_phase(
            self.table_metadata.table_name.as_str(),
            "stage_seq_reserve",
            seq_reserve_start,
        );

        let (delete_sequence, insert_sequence, snapshot_sequence) = if has_key_deletions {
            (Some(base), Some(base + 1), base + 2)
        } else if has_inlined_deletions {
            (Some(base), None, base + 1)
        } else {
            (None, None, base)
        };

        let position_sequence = delete_sequence.unwrap_or(snapshot_sequence);
        let (mut delete_files, position_deletions) = self
            .write_position_deletion_vectors_for_staged_on_conflict(delete_specs, position_sequence)
            .await?;

        let mut committed_deleted_row_keys = Vec::new();
        let insert_pk_bytes = if let Some(delete_sequence) = delete_sequence {
            let row_keys = self
                .build_pk_deletion_row_keys(&deleted_pk_i64, Cow::Owned(deleted_row_keys))
                .into_owned();
            let insert_pk_bytes: Vec<Vec<u8>> =
                row_keys.iter().map(|key| key.as_ref().to_vec()).collect();
            if let Some(results) = self
                .write_key_deletion_vectors(delete_sequence, row_keys)
                .await?
            {
                delete_files.extend(results.iter().map(|result| result.delete_file.clone()));
                committed_deleted_row_keys = results
                    .into_iter()
                    .find_map(|result| match result.identifiers {
                        DeletionIdentifier::KeyBased(keys) => Some(keys),
                        DeletionIdentifier::PositionBased { .. } => None,
                    })
                    .unwrap_or_default();
            }
            insert_pk_bytes
        } else {
            Vec::new()
        };

        // Build the INERT (`published = false`) inline tombstone payload for any
        // inlined rows this upsert replaces, WITHOUT writing it yet — it is
        // folded into the on-conflict deletion transaction below so the two
        // durable metastore writes share one SQLite-writer acquisition.
        // `delete_sequence` is `Some` here whenever `has_inlined_deletions` (see
        // the reservation above), and is strictly below `snapshot_sequence`, so
        // the tombstone hides the old inline copy (entry seq < delete_sequence)
        // but never the file replacement (snapshot rows carry seq >
        // snapshot_sequence > delete_sequence, and the tombstone is consulted
        // only against inline ENTRIES regardless). The tombstone stays inert
        // until `publish_prepared_on_conflict_deletions` flips it at finalize.
        let inline_tombstone = if has_inlined_deletions {
            let delete_sequence = delete_sequence.ok_or_else(|| {
                CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "internal: staged inline tombstone for table {} reserved no delete sequence",
                        self.table_metadata.table_name
                    ),
                }
            })?;
            self.build_staged_inline_tombstone(
                &deleted_inlined_pk_i64,
                &deleted_inlined_row_keys,
                delete_sequence,
                false,
            )?
        } else {
            None
        };
        let tombstone_delete_count = inline_tombstone
            .as_ref()
            .map(|tombstone| tombstone.delete_count);

        // b1★ (cycle-4): drain the durable `published = 1` flips owed for
        // PREVIOUSLY-finalized tombstones (whose Stage-B activated them in memory
        // but deferred their durable flip). Snapshot the queue BEFORE the commit so
        // a concurrent finalize that pushes a new id between here and the post-
        // commit cleanup is NOT erroneously cleared. These ride the SAME folded
        // `BEGIN IMMEDIATE` below, so the deferred flips cost no extra writer
        // acquisition. On commit success they are removed from
        // `inlined_locally_published` (the durable flag now matches, so the
        // in-memory override is no longer needed) and from the pending queue. On
        // failure they stay queued and re-defer to the next batch (or the
        // maintenance drain).
        let drained_flips: Vec<String> =
            std::mem::take(&mut *self.pending_durable_tombstone_flips.lock());

        // Stage-A metastore write #2 (FOLDED): commit the on-conflict deletion
        // metadata (delete files + insert records + protected-snapshot sequence)
        // AND the inline tombstone INSERT AND the deferred flips in ONE
        // transaction. Previously these were two transactions
        // (`commit_on_conflict_deletions` then `add_inlined_delete`), each
        // acquiring the process-wide SQLite writer separately. Instrumented as
        // `stage_tombstone_prepare` (the audit's name for "the OTHER per-batch
        // metastore write transactions"). Statement order is preserved: delete
        // files → insert records → snapshot sequence → tombstone INSERT →
        // deferred flips.
        let tombstone_prepare_start = Instant::now();
        let commit_result = self
            .catalog
            .commit_on_conflict_deletions_with_tombstone(
                delete_files,
                &self.table_metadata.table_id,
                insert_pk_bytes,
                insert_sequence.unwrap_or(snapshot_sequence),
                Some(SnapshotSequenceCommit {
                    snapshot_id: target_snapshot_id.clone(),
                    sequence_number: snapshot_sequence,
                }),
                inline_tombstone,
                &drained_flips,
            )
            .await;
        let inlined_delete_id = match commit_result {
            Ok(id) => {
                // Commit succeeded: the drained flips are now durably published, so
                // drop their in-memory visibility overrides. (Their pending-queue
                // entries were already removed by the `mem::take` above.)
                if !drained_flips.is_empty() {
                    let mut guard = self.inlined_locally_published.lock();
                    for flipped in &drained_flips {
                        guard.remove(flipped);
                    }
                }
                id
            }
            Err(err) => {
                // Commit failed: re-defer the drained flips so they ride the next
                // batch (or the maintenance drain). Their in-memory overrides stay
                // in place, so readers continue to apply the tombstones. Preserve
                // FIFO order with any flips a concurrent finalize enqueued meanwhile.
                if !drained_flips.is_empty() {
                    let mut guard = self.pending_durable_tombstone_flips.lock();
                    let mut requeued = drained_flips;
                    requeued.append(&mut guard);
                    *guard = requeued;
                }
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Failed to commit staged on-conflict metadata and snapshot sequence: {err}"
                    ),
                });
            }
        };
        record_cayenne_write_phase(
            self.table_metadata.table_name.as_str(),
            "stage_tombstone_prepare",
            tombstone_prepare_start,
        );

        // Tombstone bookkeeping that previously lived in `add_inlined_tombstone`:
        // bump `pending_inline_tombstones` so the inline checkpoint defers for the
        // staged window, plus the trace/telemetry for the write. Gated on the
        // returned id (which is `Some` exactly when a tombstone row was written),
        // identical to the pre-fold `if id.is_some()` guard.
        if let (Some(_), Some(delete_count)) = (&inlined_delete_id, tombstone_delete_count) {
            self.pending_inline_tombstones
                .fetch_add(1, Ordering::AcqRel);
            self.record_inline_tombstone_written(
                delete_count,
                delete_sequence.unwrap_or(snapshot_sequence),
                false,
            );
        }

        Ok(PreparedOnConflictDeletionPublish {
            target_snapshot_id,
            snapshot_sequence,
            delete_sequence,
            insert_sequence,
            deleted_pk_i64,
            deleted_row_keys: committed_deleted_row_keys,
            // cycle-5 TASK 1: carry the INLINE tombstone keys (moved — only
            // borrowed by `build_staged_inline_tombstone` above) so the finalize's
            // removal delta hides exactly the cached inline rows this tombstone
            // covers. One of the two is empty per PK strategy.
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
            position_deletions,
            inlined_delete_id,
            superseded,
        })
    }

    pub(super) async fn write_position_deletion_vectors_for_staged_on_conflict(
        &self,
        delete_specs: HashMap<Arc<str>, Vec<u64>>,
        sequence_number: i64,
    ) -> CatalogResult<(Vec<crate::metadata::DeleteFile>, HashMap<String, Vec<u32>>)> {
        if delete_specs.is_empty() {
            return Ok((Vec::new(), HashMap::new()));
        }

        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = sequence_number;
        let writer = DeletionVectorWriter::new(&temp_metadata);

        let mut specs = Vec::with_capacity(delete_specs.len());
        let mut position_deletions = HashMap::with_capacity(delete_specs.len());
        for (file_path, incoming_row_ids) in delete_specs {
            let mut row_ids = Vec::with_capacity(incoming_row_ids.len());
            for id in incoming_row_ids {
                let id32 = u32::try_from(id).map_err(|_| {
                    CatalogError::InvalidOperationNoSource {
                        message: format!(
                            "Cannot stage CDC upsert for table {} because row id {id} in file {file_path} exceeds u32::MAX; compact the table before retrying",
                            self.table_metadata.table_name
                        ),
                    }
                })?;
                row_ids.push(id32);
            }
            row_ids.sort_unstable();
            row_ids.dedup();

            if row_ids.is_empty() {
                continue;
            }

            let file_path = file_path.to_string();
            let writer_row_ids = row_ids.iter().copied().map(u64::from).collect();
            specs.push(DeletionVectorWriteSpec::new_position_based_sorted(
                file_path.clone(),
                writer_row_ids,
            ));
            position_deletions.insert(file_path, row_ids);
        }

        if specs.is_empty() {
            return Ok((Vec::new(), HashMap::new()));
        }

        let results = writer.write(specs).await?;
        let delete_files = results
            .into_iter()
            .map(|result| result.delete_file)
            .collect();

        Ok((delete_files, position_deletions))
    }

    /// Drain any deferred durable `published = 1` tombstone flips (cycle-4 b1★)
    /// outside the staged-batch fold. The fold
    /// (`commit_on_conflict_deletions_with_tombstone`) normally piggybacks these
    /// onto the NEXT batch's transaction, but a table that goes IDLE (no further
    /// CDC) would otherwise never converge the durable flag — leaving the in-
    /// memory `inlined_locally_published` override carrying visibility
    /// indefinitely (correct, but the durable state would only heal on reopen via
    /// `publish_orphan_inlined_deletes`). This bounds the convergence: the
    /// background maintenance tick (`run_maintenance_state`) calls it so an idle
    /// table's owed flips persist within a maintenance debounce.
    ///
    /// Each flip is an idempotent single-row autocommit `UPDATE` (no held writer
    /// txn). Successful ids are removed from BOTH the pending queue and the in-
    /// memory override; a failed id stays queued for the next drain. Best-effort:
    /// errors are logged, never propagated — the orphan sweep is the crash
    /// backstop and the next drain retries.
    pub(super) async fn drain_pending_durable_tombstone_flips(&self) {
        let drained: Vec<String> =
            std::mem::take(&mut *self.pending_durable_tombstone_flips.lock());
        if drained.is_empty() {
            return;
        }
        let mut published = Vec::new();
        let mut failed = Vec::new();
        for inlined_id in drained {
            match self
                .catalog
                .mark_inlined_delete_published(&self.table_metadata.table_id, &inlined_id)
                .await
            {
                Ok(()) => published.push(inlined_id),
                Err(err) => {
                    tracing::warn!(
                        table = %self.table_metadata.table_name,
                        inlined_id,
                        error = %err,
                        "Deferred tombstone flip drain failed; re-queueing for the next maintenance tick"
                    );
                    failed.push(inlined_id);
                }
            }
        }
        if !published.is_empty() {
            let mut guard = self.inlined_locally_published.lock();
            for flipped in &published {
                guard.remove(flipped);
            }
        }
        if !failed.is_empty() {
            // Re-queue ahead of anything a concurrent finalize enqueued meanwhile.
            let mut guard = self.pending_durable_tombstone_flips.lock();
            let mut requeued = failed;
            requeued.append(&mut guard);
            *guard = requeued;
        }
    }

    pub(crate) fn publish_prepared_on_conflict_deletions(
        &self,
        mut prepared: PreparedOnConflictDeletionPublish,
    ) -> CatalogResult<()> {
        let snapshot_sequence = prepared.snapshot_sequence;
        self.publish_staged_position_deletion_cache(prepared.position_deletions);

        // cycle-5 TASK 1: capture this tombstone's INLINE removal — the keys the
        // inline tombstone hides (`deleted_inlined_*`), at `delete_sequence` — so
        // the inline-cache delta path removes exactly the old inline rows this
        // upsert supersedes, WITHOUT a structural rebuild. These are MOVED out of
        // `prepared` (they feed nothing else), and deliberately NOT the
        // file-deletion `deleted_pk_i64`/`deleted_row_keys`: a file-conflict
        // deletion never matches a cached inline row, so using the file keys would
        // leave the old inline copy visible (a transient duplicate). Only captured
        // when an inline tombstone was actually written (`inlined_delete_id`).
        let tombstone_removal = if prepared.inlined_delete_id.is_some() {
            prepared.delete_sequence.map(|delete_sequence| {
                (
                    delete_sequence,
                    std::mem::take(&mut prepared.deleted_inlined_pk_i64),
                    std::mem::take(&mut prepared.deleted_inlined_row_keys),
                )
            })
        } else {
            None
        };

        if let (Some(delete_sequence), Some(insert_sequence)) =
            (prepared.delete_sequence, prepared.insert_sequence)
        {
            self.publish_staged_key_deletion_cache(
                &prepared.deleted_pk_i64,
                prepared.deleted_row_keys,
                delete_sequence,
                insert_sequence,
            )?;
        }

        // b1★ (cycle-4) + cycle-5 TASK 1: activate the inline tombstone IN MEMORY
        // and DEFER its durable `published = 1` flip (Stage-B is writer-free).
        // Runs under the same held listing fence as the protected-snapshot rcu
        // below and AFTER the replacement files were moved into the snapshot, so a
        // scan observes the tombstone applied exactly when the replacement is in
        // the listing — never before (no transient vanish, no transient duplicate).
        //
        //  1. Record the id in `inlined_locally_published` so the read filter
        //     (`load_inlined_deletion_maps`) applies this tombstone immediately,
        //     even though it is still durably `published = 0`. This is what makes
        //     the deferred durable flip safe to defer WITHOUT leaving the old
        //     inline row + its file replacement both visible, AND is what makes a
        //     FULL rebuild (sentinel/over-cap) apply the tombstone.
        //  2. Enqueue the id in `pending_durable_tombstone_flips` so the durable
        //     flip rides the NEXT staged batch's Stage-A folded `BEGIN IMMEDIATE`
        //     transaction (or the idle-table maintenance drain) — Stage-B itself
        //     issues no `UPDATE`.
        //  3. cycle-5 TASK 1 — DELTA-capable cache invalidation. A published
        //     tombstone only ever REMOVES rows from the cached view (it hides the
        //     prior inline copy of an upserted PK; `filter_inlined_batch_for_deletions`
        //     keeps a row iff `data_sequence > delete_sequence`). Removal can never
        //     invalidate a *retained* base entry, so instead of bumping the
        //     STRUCTURAL epoch (which forced a full corpus rebuild on EVERY upsert
        //     batch — bench #4: 16,471 full rebuilds), enqueue the removal in
        //     `pending_tombstone_deltas` and bump ONLY the generation. The next
        //     miss's `extend_inlined_cache_delta` re-filters the reused base
        //     entries against just these keys. Over-cap or sentinel ⇒ a full
        //     rebuild (the safety fallback), which still applies the tombstone via
        //     (1). The push happens-before the generation bump's `Release` store,
        //     so a scan observing the new generation also observes the delta.
        //  4. Decrement `pending_inline_tombstones`. This is SAFE to do now even
        //     though the durable flip is pending: the inline checkpoint that the
        //     counter gates (`checkpoint_inlined_data`) reads visible rows via
        //     `load_inlined_deletion_maps`, which now consults
        //     `inlined_locally_published`, so a checkpoint flush correctly EXCLUDES
        //     this tombstone's old row even before the durable flip lands (the
        //     checkpoint's `clear_inlined_data_and_deletes` then drops the
        //     tombstone row and its in-memory entries, see
        //     `clear_inlined_metadata_after_checkpoint`).
        if let Some(inlined_id) = prepared.inlined_delete_id.clone() {
            self.inlined_locally_published
                .lock()
                .insert(inlined_id.clone());
            self.pending_durable_tombstone_flips.lock().push(inlined_id);
            if let Some((delete_sequence, int64_pk, row_keys)) = tombstone_removal {
                self.pending_tombstone_deltas
                    .lock()
                    .push(delete_sequence, int64_pk, row_keys);
            }
            // Generation-only bump (NOT structural): the removal is delta-applied
            // from `pending_tombstone_deltas`. `bump_inlined_generation`'s
            // `Release` store publishes both the queue push above and the
            // `inlined_locally_published` insert to any scan that loads the new
            // generation with `Acquire`.
            self.bump_inlined_generation();
            self.pending_inline_tombstones
                .fetch_sub(1, Ordering::AcqRel);
        }

        // Threshold = the snapshot's OWN allocated `sequence_number` (reserved in
        // `prepare_on_conflict_deletions_for_staged_snapshot` and persisted to
        // `cayenne_snapshot_sequence` in the same catalog commit). This MUST equal the
        // value `load_protected_snapshots` reloads on restart so the partial-deletion
        // filter (`delete_seq > threshold`) is reload-stable. A live max-delete-sequence
        // read (the deletion snapshot's `max_sequence_number()`) is NOT used: with
        // pipelined finalization an
        // unrelated deletion can land between this snapshot's sequence allocation and
        // this (backgrounded) publish, raising the global max past `snapshot_sequence`
        // and skipping a delete the reloaded threshold would apply.
        self.protected_snapshots.rcu(|current| {
            let mut new_map = (**current).clone();
            new_map.insert(prepared.target_snapshot_id.clone(), snapshot_sequence);
            Arc::new(new_map)
        });

        tracing::debug!(
            table = self.table_metadata.table_name.as_str(),
            snapshot_id = prepared.target_snapshot_id,
            snapshot_sequence,
            "Published staged on-conflict snapshot"
        );

        Ok(())
    }

    pub(super) fn publish_staged_position_deletion_cache(
        &self,
        position_deletions: HashMap<String, Vec<u32>>,
    ) {
        if position_deletions.is_empty() {
            return;
        }

        let cached_deleted_row_ids = self.pk_deletion_strategy.position_cache();
        let current = cached_deleted_row_ids.load_full();
        let mut updated = (*current).clone();

        for (file_path, row_ids) in position_deletions {
            let mut bitmap = current
                .get(&file_path)
                .map_or_else(RoaringBitmap::new, |deletion_vector| {
                    deletion_vector.to_bitmap()
                });
            bitmap.extend(row_ids);
            updated.insert(file_path, Arc::new(PositionDeletionVector::new(bitmap)));
        }

        cached_deleted_row_ids.store(Arc::new(updated));
        self.refresh_deletion_memory_accounting();
    }

    pub(super) fn publish_staged_key_deletion_cache(
        &self,
        deleted_pk_i64: &[i64],
        deleted_row_keys: Vec<Box<[u8]>>,
        delete_sequence: i64,
        insert_sequence: i64,
    ) -> CatalogResult<()> {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => {
                let current = deletion_snapshot.load_full();
                let updated = current.tombstones.extend_max_conflicts(
                    deleted_pk_i64.iter().copied(),
                    delete_sequence,
                    insert_sequence,
                );
                deletion_snapshot.store(Arc::new(Int64PkDeletionSnapshot::from_index(updated)));
                self.refresh_deletion_memory_accounting();
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => {
                let current = deletion_snapshot.load_full();
                let updated = current.tombstones.extend_max_conflicts(
                    deleted_row_keys,
                    delete_sequence,
                    insert_sequence,
                );
                deletion_snapshot
                    .store(Arc::new(RowConverterDeletionSnapshot::from_index(updated)));
                self.refresh_deletion_memory_accounting();
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Cannot publish staged key deletions for position-based table {}",
                        self.table_metadata.table_name
                    ),
                });
            }
        }

        Ok(())
    }

    /// Apply deletion vectors generated by on-conflict (upsert) handling.
    ///
    /// Not supported for Position-based tables (no PK) that doesn't support upserts
    ///
    /// This function handles three independently-timed sub-phases:
    /// 1. **Inline-deletion** (`on_conflict_inlined_delete`): for PKs whose prior
    ///    copy lives in the inline memtable, write a small inline tombstone
    ///    (`add_inlined_delete`) carrying `delete_sequence`. The scan-time read
    ///    filter (`filter_inlined_batch_for_deletions`) then hides any inline row
    ///    at that PK whose entry `sequence_number <= delete_sequence`. This
    ///    replaces the previous O(corpus) approach that re-decoded and rewrote
    ///    the ENTIRE inline corpus on every conflicting batch.
    /// 2. **Position-delete** (`on_conflict_position_delete`): one per-file
    ///    position vector per source file for located conflict rows.
    /// 3. **Key-deletion-vector** (`on_conflict_key_delete`): a key-based
    ///    `DeleteFile` for unlocated / bloom-fallback file rows, plus the paired
    ///    re-insertion record so the replacement row stays visible.
    ///
    /// The appropriate in-memory deletion-cache update is returned (not stored)
    /// so it can be committed atomically with the protected-snapshot publish
    /// under `scan_state_lock`.
    ///
    /// # Sequence ordering (correctness-critical)
    ///
    /// A single `delete_sequence` (`= base`, the first reserved number) is shared
    /// by the inline tombstone (phase 1) and the file `DeleteFile` (phase 3): they
    /// are one logical "hide the prior version at this PK" operation. The
    /// replacement rows are written by the caller into a Vortex snapshot whose own
    /// sequence is allocated *after* this function returns (see
    /// `write_new_snapshot_after_validation` in `mutation_writer.rs`, which
    /// reserves the snapshot sequence via `reserve_sequences_local` only
    /// after this returns), so the replacement file rows always carry a
    /// strictly-higher data sequence (`> base`) and are never hidden by
    /// `delete_sequence`.
    ///
    /// Inline tombstones are consulted ONLY by `filter_inlined_batch_for_deletions`
    /// against inline entries (never against file rows). Every conflicting inline
    /// entry is a prior-published copy whose `sequence_number` was assigned from
    /// the same monotonic `current_sequence_number` counter at an earlier insert,
    /// so it is strictly below the `base` reserved here. The tombstone therefore
    /// hides exactly the old inline copy and nothing newer.
    ///
    /// Following Iceberg's sequence-based ordering model where deletes are tracked by
    /// PK value + sequence number for proper ordering of concurrent operations.
    pub(crate) async fn apply_on_conflict_deletions(
        &self,
        on_conflict_deletions: OnConflictDeletions,
    ) -> CatalogResult<OnConflictUpdate> {
        let OnConflictDeletions {
            delete_specs,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        } = on_conflict_deletions;

        let has_file_key_deletions = !deleted_pk_i64.is_empty() || !deleted_row_keys.is_empty();
        let has_file_deletions = !delete_specs.is_empty() || has_file_key_deletions;
        let has_inlined_deletions =
            !deleted_inlined_pk_i64.is_empty() || !deleted_inlined_row_keys.is_empty();

        if !has_file_deletions && !has_inlined_deletions {
            return Ok(OnConflictUpdate::none());
        }

        // Reserve the delete sequence ONCE, up front. It is shared by the inline
        // tombstone (phase 1) and the file `DeleteFile` (phase 3) — both express
        // the same "hide the prior version at this PK" intent, so a single
        // sequence keeps them consistent. When file key-deletions are present we
        // reserve a second, strictly-higher `insert_sequence` for the paired
        // re-insertion record (so the replacement file row is not filtered out by
        // the `DeleteFile` during scans). `reserve_sequence_numbers` batches both
        // into one writer-lock acquisition on the serialized metastore.
        //
        // The sequence is reserved BEFORE any caller allocates the replacement
        // snapshot's sequence, guaranteeing `delete_sequence < snapshot_sequence`
        // (see the sequence-ordering note above). Even in the inline-only case
        // (no file deletions) we MUST reserve one sequence here: the previous
        // inline-rewrite path needed none, but the tombstone needs a
        // `delete_sequence` that is (a) >= the old inline row's sequence so the
        // tombstone hides it, and (b) < the replacement snapshot's sequence so
        // the new file row stays visible. Reserving here, before the caller's
        // `increment_sequence_number`, satisfies both.
        let reserve_count = if has_file_key_deletions { 2 } else { 1 };
        // Lever B2: same in-memory allocator as the staged path, so the sync
        // last-resort path and the staged path share one monotone source.
        let base = self
            .reserve_sequences_local(reserve_count)
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Failed to reserve sequence numbers for on-conflict: {err}"),
            })?;
        let delete_sequence = base;

        // --- Phase 1: inline-deletion handling -----------------------------
        // Durably write an inline tombstone for each PK whose prior copy is in
        // the inline memtable. Bumping `inlined_generation` is deferred to the
        // publish step (under `scan_state_lock`) so the tombstone becomes
        // visible atomically with the deletion-cache + protected-snapshot flips.
        let inlined_tombstone_written = if has_inlined_deletions {
            let phase_start = Instant::now();
            // Synchronous (last-resort) on-conflict path: this resolution
            // publishes immediately under the held write guard, so the tombstone
            // is written ALREADY-published (`published = true`) — there is no
            // staged window in which it could be observed inert. The staged
            // (pipelined) path writes `published = false` and flips it at
            // finalize; see `prepare_on_conflict_deletions_for_staged_snapshot`.
            let written = self
                .add_inlined_tombstone(
                    &deleted_inlined_pk_i64,
                    &deleted_inlined_row_keys,
                    delete_sequence,
                    true,
                )
                .await?
                .is_some();
            record_cayenne_write_phase(
                &self.table_metadata.table_name,
                "on_conflict_inlined_delete",
                phase_start,
            );
            written
        } else {
            false
        };

        // Position-based deletions for located conflict rows (deletion_mode:
        // position): write one per-file position vector per source file and
        // publish them to the position cache. These tombstone the prior version
        // at its exact (file, position), so they need no insert-record/sequence
        // bookkeeping — a re-inserted PK lands in a different file that carries
        // no position tombstone for it (self-scoping merge-on-read semantics).
        if !delete_specs.is_empty() {
            let phase_start = Instant::now();
            let position_specs: HashMap<String, Vec<u64>> = delete_specs
                .into_iter()
                .map(|(path, positions)| (path.to_string(), positions))
                .collect();
            // Persist via a deletion sink. The sink's `pk_deletion_strategy` is a
            // clone, but its caches are `Arc<ArcSwap<…>>` so the position-cache
            // publish writes through to this provider's live cache. No protected-
            // snapshot tables are needed (persist only touches table_metadata +
            // catalog + the position cache); write_lock=None because the upsert
            // write path already holds it.
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
            sink.persist_position_based_deletions(position_specs)
                .await
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Failed to persist position-based on-conflict deletions: {err}"
                    ),
                })?;
            record_cayenne_write_phase(
                &self.table_metadata.table_name,
                "on_conflict_position_delete",
                phase_start,
            );
        }

        // Key-based deletions for unlocated / bloom-fallback FILE rows. A batch
        // with only inline and/or position deletes has no key lists, so skip the
        // key-vector path — but still report whether an inline tombstone (phase 1)
        // was written so the publish step bumps the inline generation. The single
        // `delete_sequence` reserved up front is already consumed by the inline
        // tombstone (phase 1) in that case; no `insert_sequence` was reserved
        // (`reserve_count` was 1), so it is computed only below where it is used.
        if !has_file_key_deletions {
            return Ok(
                OnConflictUpdate::none().with_inlined_tombstone_written(inlined_tombstone_written)
            );
        }

        // Reaching here means `has_file_key_deletions` is true, so `reserve_count`
        // was 2 and the second reserved sequence (`base + 1`) is a real, allocated
        // sequence number. Compute it only now (it is unused on the inline/position-
        // only early-return above) and use `checked_add` to fail safely instead of
        // overflow-panicking (debug) or wrapping (release) at the i64 ceiling.
        let insert_sequence =
            base.checked_add(1)
                .ok_or_else(|| CatalogError::InvalidOperationNoSource {
                    message: "sequence-number counter overflowed i64 reserving an on-conflict insert sequence".to_string(),
                })?;

        let phase_start = Instant::now();
        let row_keys = self
            .build_pk_deletion_row_keys(&deleted_pk_i64, Cow::Owned(deleted_row_keys))
            .into_owned();
        let insert_pk_bytes: Vec<Vec<u8>> =
            row_keys.iter().map(|key| key.as_ref().to_vec()).collect();

        let Some(results) = self
            .write_and_commit_deletion_vectors(
                delete_sequence,
                row_keys,
                insert_pk_bytes,
                insert_sequence,
            )
            .await?
        else {
            record_cayenne_write_phase(
                &self.table_metadata.table_name,
                "on_conflict_key_delete",
                phase_start,
            );
            return Ok(
                OnConflictUpdate::none().with_inlined_tombstone_written(inlined_tombstone_written)
            );
        };
        record_cayenne_write_phase(
            &self.table_metadata.table_name,
            "on_conflict_key_delete",
            phase_start,
        );

        // Build the appropriate deletion-cache update based on deletion strategy.
        // This follows Iceberg's pattern where deletes are tracked by PK + sequence number.
        // For upserts, we also update insert records so the new row isn't filtered out.
        // The update is returned (not stored) so it can be committed atomically
        // with the protected-snapshot publish under `scan_state_lock`.
        let update = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => {
                // Record the deletion and the re-insertion in ONE fused-index
                // pass and one ArcSwap store, so readers never observe
                // mismatched generations. Writers are serialised by the
                // per-table write lock so the load+rebuild+store sequence is
                // race-free.
                let current = deletion_snapshot.load_full();
                let updated = current.tombstones.extend_max_conflicts(
                    deleted_pk_i64.iter().copied(),
                    delete_sequence,
                    insert_sequence,
                );
                tracing::debug!(
                    "Prepared Int64 PK deletion cache update with {} deleted keys (seq={}) and {} insert records (seq={}) for table {}",
                    updated.delete_len(),
                    delete_sequence,
                    updated.insert_len(),
                    insert_sequence,
                    self.table_metadata.table_name
                );

                OnConflictDeletionUpdate::Int64Pk(Arc::new(Int64PkDeletionSnapshot::from_index(
                    updated,
                )))
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => {
                // Consume `results` to take owned `Box<[u8]>` keys. The branch
                // is invariantly the sole `KeyBased` producer (one key-based spec
                // built above, `results` non-empty by the `else` early-return on
                // `write_and_commit_deletion_vectors` returning `None`).
                let written_keys: Vec<Box<[u8]>> = results
                    .into_iter()
                    .find_map(|r| match r.identifiers {
                        DeletionIdentifier::KeyBased(keys) => Some(keys),
                        DeletionIdentifier::PositionBased { .. } => None,
                    })
                    .ok_or_else(|| CatalogError::InvalidOperationNoSource {
                        message: "RowConverterBased on-conflict deletion did not produce a key-based write result".to_string(),
                    })?;
                let current = deletion_snapshot.load_full();
                let updated = current.tombstones.extend_max_conflicts(
                    written_keys,
                    delete_sequence,
                    insert_sequence,
                );
                tracing::debug!(
                    "Prepared RowConverter deletion cache update with {} deleted keys (seq={}) and {} insert records (seq={}) for table {}",
                    updated.delete_len(),
                    delete_sequence,
                    updated.insert_len(),
                    insert_sequence,
                    self.table_metadata.table_name
                );

                OnConflictDeletionUpdate::RowConverter(Arc::new(
                    RowConverterDeletionSnapshot::from_index(updated),
                ))
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based tables have no PKs and don't support upserts, so
                // this branch should never be reached. Fail safely with a
                // structured error instead of panicking if a higher-level routing
                // bug ever calls it.
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "apply_on_conflict_deletions called for position-based strategy on table {}",
                        self.table_metadata.table_name
                    ),
                });
            }
        };

        Ok(OnConflictUpdate::from_deletion_update(update)
            .with_inlined_tombstone_written(inlined_tombstone_written))
    }

    /// Durably write an inline tombstone (`cayenne_inlined_delete`) that hides
    /// the prior inline copy of each upserted PK at scan time.
    ///
    /// Instead of re-decoding and rewriting the entire inline corpus per
    /// conflicting batch (the previous `build_inlined_data_rewrite_for_pk_keys` →
    /// `commit_inlined_data_durable` path, O(corpus)), we append one small delete
    /// blob keyed by `delete_sequence`. The scan-time filter
    /// (`filter_inlined_batch_for_deletions`) then hides any inline row at one of
    /// these PKs whose entry `sequence_number <= delete_sequence`.
    ///
    /// The keys are serialized to the tagged `delete_ipc` encoding that
    /// `load_inlined_deletion_maps` / `deserialize_delete_keys_from_ipc` read
    /// back (see [`tombstone_format`]): for `Int64Pk` tables each PK is its
    /// 8-byte big-endian encoding (`build_pk_deletion_row_keys` +
    /// `row_key_to_i64`) packed raw with no Arrow framing; for
    /// `RowConverterBased` tables the already-encoded `arrow_row` key bytes are
    /// written as an LZ4-compressed single-column `BinaryArray` IPC stream.
    ///
    /// `published` selects the durable activation state the tombstone is written
    /// with: the synchronous on-conflict path passes `true` (it publishes
    /// immediately under the held write guard, so there is no staged window),
    /// while the staged (pipelined) path passes `false` and flips the flag to
    /// `true` at finalize via `MetadataCatalog::mark_inlined_delete_published`.
    /// The read filter (`load_inlined_deletion_maps`) applies a tombstone only
    /// when its flag is `true`, so a `published = false` tombstone is inert until
    /// its owning snapshot publishes.
    ///
    /// Returns `Some(inlined_id)` when a tombstone row was written (so the caller
    /// can later flip its flag and defers the `inlined_generation` bump to the
    /// publish step), `None` when there was nothing to write or the table is
    /// position-based (no inline filtering). Does NOT bump the generation or
    /// touch any in-memory cache: visibility is flipped by the publish step.
    pub(super) async fn add_inlined_tombstone(
        &self,
        deleted_inlined_pk_i64: &[i64],
        deleted_inlined_row_keys: &[Box<[u8]>],
        delete_sequence: i64,
        published: bool,
    ) -> CatalogResult<Option<String>> {
        let Some(tombstone) = self.build_staged_inline_tombstone(
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
            delete_sequence,
            published,
        )?
        else {
            return Ok(None);
        };
        let delete_count = tombstone.delete_count;

        let inlined_id = self.catalog.add_inlined_delete(tombstone).await?;

        self.record_inline_tombstone_written(delete_count, delete_sequence, published);

        Ok(Some(inlined_id))
    }

    /// Build the `InlinedDelete` payload for an Option-D inline tombstone, or
    /// `None` when there is nothing to hide (no keys, or a position-based table
    /// that never applies inline deletion filtering). Pure: serializes the keys
    /// to IPC but performs NO metastore write — the caller persists it either
    /// via [`MetadataCatalog::add_inlined_delete`] (`add_inlined_tombstone`) or,
    /// on the staged-upsert hot path, folded into the on-conflict deletion
    /// transaction via
    /// [`MetadataCatalog::commit_on_conflict_deletions_with_tombstone`].
    pub(super) fn build_staged_inline_tombstone(
        &self,
        deleted_inlined_pk_i64: &[i64],
        deleted_inlined_row_keys: &[Box<[u8]>],
        delete_sequence: i64,
        published: bool,
    ) -> CatalogResult<Option<InlinedDelete>> {
        // Position-based tables have no PK and never apply inline deletion
        // filtering (`load_inlined_deletion_maps` returns empty for them), so a
        // tombstone would be inert. Defensive: this path is not reached for them.
        if self.pk_deletion_strategy.is_position_based() {
            return Ok(None);
        }

        let row_keys = self.build_pk_deletion_row_keys(
            deleted_inlined_pk_i64,
            Cow::Borrowed(deleted_inlined_row_keys),
        );
        if row_keys.is_empty() {
            return Ok(None);
        }

        let delete_count = i64::try_from(row_keys.len()).unwrap_or(i64::MAX);
        // cycle-5 TASK 2a: pack Int64Pk keys raw (no Arrow framing), LZ4 the
        // composite-key IPC. `build_pk_deletion_row_keys` above produced 8-byte
        // BE keys for an Int64Pk table, matching the packed-i64 encoding.
        let delete_ipc =
            serialize_delete_keys_to_ipc(&row_keys, self.pk_deletion_strategy.is_int64_pk())
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Failed to serialize inline tombstone keys for table {}: {err}",
                        self.table_metadata.table_name
                    ),
                })?;

        Ok(Some(InlinedDelete {
            inlined_id: String::new(),
            table_id: self.table_metadata.table_id.clone(),
            delete_ipc,
            delete_count,
            sequence_number: delete_sequence,
            created_at: String::new(),
            published,
        }))
    }

    /// Trace + telemetry for a written inline tombstone, shared by the
    /// `add_inlined_delete` path and the folded staged-upsert transaction path.
    pub(super) fn record_inline_tombstone_written(
        &self,
        delete_count: i64,
        delete_sequence: i64,
        published: bool,
    ) {
        tracing::debug!(
            table = %self.table_metadata.table_name,
            keys = delete_count,
            delete_sequence,
            published,
            "Wrote inline tombstone for upserted PK(s)"
        );

        // Confirm the cheap tombstone path was taken (vs the O(corpus) inline
        // rewrite fallback below). Counts one tombstone write plus the number of
        // keys hidden, dimensioned by table; pair with the rewrite-fallback
        // counter in `build_inlined_data_rewrite_for_pk_keys` to observe the
        // tombstone-vs-rewrite ratio.
        telemetry::track_cayenne_inline_tombstone_write(
            u64::try_from(delete_count).unwrap_or(u64::MAX),
            &[telemetry::KeyValue::new(
                "table",
                self.table_metadata.table_name.clone(),
            )],
        );
    }

    /// Synchronously store a deferred on-conflict deletion-cache update into the
    /// live deletion cache. MUST be called while holding
    /// `scan_state_lock.write()` when paired with a protected-snapshot publish
    /// so concurrent scans observe both changes atomically.
    pub(super) fn commit_on_conflict_deletion_update(&self, update: OnConflictDeletionUpdate) {
        match update {
            OnConflictDeletionUpdate::None => {}
            OnConflictDeletionUpdate::Int64Pk(snapshot) => {
                if let PkDeletionStrategyWithCache::Int64Pk {
                    deletion_snapshot, ..
                } = &self.pk_deletion_strategy
                {
                    deletion_snapshot.store(snapshot);
                }
                self.refresh_deletion_memory_accounting();
            }
            OnConflictDeletionUpdate::RowConverter(snapshot) => {
                if let PkDeletionStrategyWithCache::RowConverterBased {
                    deletion_snapshot, ..
                } = &self.pk_deletion_strategy
                {
                    deletion_snapshot.store(snapshot);
                }
                self.refresh_deletion_memory_accounting();
            }
        }
    }

    pub(super) fn publish_on_conflict_update(&self, update: OnConflictUpdate) {
        let OnConflictUpdate {
            deletion_update,
            inlined_tombstone_written,
        } = update;
        // An inline tombstone was durably written (hide the prior inline copy of
        // an upserted PK instead of rewriting the whole inline corpus). Bump the
        // STRUCTURAL epoch so the next scan FULL-rebuilds `inlined_cache` and
        // `load_inlined_deletion_maps` picks up the new tombstone — a published
        // tombstone re-filters cached base entries, so the append-only delta path
        // would be unsound here. This runs under `scan_state_lock.write()` in
        // every caller of `publish_on_conflict_update`, so the tombstone's
        // visibility flips atomically with the deletion-cache update and the
        // protected-snapshot publish — a scan sees either the old inline row or
        // the new file row, never both and never neither.
        if inlined_tombstone_written {
            self.bump_inlined_structural_epoch();
        }
        self.commit_on_conflict_deletion_update(deletion_update);
    }

    pub(super) fn prepare_protected_snapshot_update(
        &self,
        snapshot_id: &str,
        threshold: i64,
    ) -> PreparedProtectedSnapshotUpdate {
        let expected = self.protected_snapshots.load_full();
        let mut updated = (*expected).clone();
        updated.insert(snapshot_id.to_string(), threshold);
        PreparedProtectedSnapshotUpdate {
            expected,
            updated: Arc::new(updated),
        }
    }

    pub(super) fn try_commit_prepared_protected_snapshot(
        &self,
        prepared: PreparedProtectedSnapshotUpdate,
    ) -> bool {
        let PreparedProtectedSnapshotUpdate { expected, updated } = prepared;
        let previous = self
            .protected_snapshots
            .compare_and_swap(&expected, updated);
        Arc::ptr_eq(&expected, &previous)
    }

    /// Insert a protected-snapshot entry while holding `scan_state_lock.write()`
    /// only for the atomic store. The map clone happens before the guard is
    /// acquired; if another publisher wins the race, rebuild and retry.
    pub(super) async fn commit_protected_snapshot_with_scan_lock(
        &self,
        snapshot_id: &str,
        threshold: i64,
    ) {
        loop {
            let prepared = self.prepare_protected_snapshot_update(snapshot_id, threshold);
            let _view_guard = self.scan_state_lock.write().await;
            if self.try_commit_prepared_protected_snapshot(prepared) {
                return;
            }
        }
    }

    /// Atomically publish on-conflict inlined/deletion updates and, when
    /// `protected_snapshot_id` is set, the protected-snapshot entry for a newly
    /// written snapshot — all under a single `scan_state_lock.write()` guard.
    ///
    /// Only the synchronous in-memory commits run under the guard; all durable
    /// I/O (deletion vectors, sequence records) is performed by the caller
    /// beforehand, so the write lock is held for microseconds. When there is no
    /// protected snapshot to publish and the scan-visible views are unchanged
    /// (the hot pure-append case) the guard is skipped entirely.
    pub(crate) async fn commit_on_conflict_publish(
        &self,
        update: OnConflictUpdate,
        protected_snapshot: Option<(&str, i64)>,
    ) {
        if protected_snapshot.is_none() && update.is_empty() {
            return;
        }
        let Some((snapshot_id, threshold)) = protected_snapshot else {
            let _view_guard = self.scan_state_lock.write().await;
            self.publish_on_conflict_update(update);
            return;
        };

        // Threshold = this snapshot's OWN allocated sequence number, matching
        // `load_protected_snapshots` so scans are reload-stable.
        let mut update = Some(update);
        loop {
            let prepared = self.prepare_protected_snapshot_update(snapshot_id, threshold);
            let _view_guard = self.scan_state_lock.write().await;
            if self.try_commit_prepared_protected_snapshot(prepared) {
                let update = update.take().unwrap_or_else(OnConflictUpdate::none);
                self.publish_on_conflict_update(update);
                return;
            }
        }
    }

    /// Persist file-backed PK deletion vectors to disk for durability.
    ///
    /// Called during an inline upsert after the replacement data has been
    /// durably committed but BEFORE the in-memory deletion cache is published
    /// (by [`Self::update_file_deletion_cache`] under `scan_state_lock`
    /// inside [`Self::try_inline_batches_with_inlined_deletions`]). This method
    /// writes the durable deletion vectors and commits them to the catalog so
    /// that the deletions survive a restart; it does not touch the in-memory
    /// cache, so its ordering relative to the cache publish is irrelevant.
    pub(crate) async fn persist_file_deletions_after_inlined_insert(
        &self,
        deleted_pk_i64: &[i64],
        deleted_row_keys: &[Box<[u8]>],
        delete_sequence: i64,
    ) -> CatalogResult<()> {
        let has_file_deletions = !deleted_pk_i64.is_empty() || !deleted_row_keys.is_empty();

        if !has_file_deletions {
            return Ok(());
        }

        let row_keys = self
            .build_pk_deletion_row_keys(deleted_pk_i64, Cow::Borrowed(deleted_row_keys))
            .into_owned();

        // Commit delete files only — no insert records (inline data bypasses
        // the deletion filter, so no protected insert sequence is needed).
        self.write_and_commit_deletion_vectors(delete_sequence, row_keys, vec![], 0)
            .await?;

        Ok(())
    }
}
