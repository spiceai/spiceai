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

//! On-conflict (upsert / do-nothing) write-path machinery.
//!
//! Holds the per-batch primary-key validation pipeline
//! ([`OnConflictValidationStream`], [`OnConflictContext`]), the prepared
//! deletion/update publishes ([`OnConflictDeletions`], [`OnConflictUpdate`],
//! [`PreparedOnConflictDeletionPublish`]), the inline-cache tombstone deltas
//! ([`TombstoneDelta`], [`PendingTombstoneDeltas`]), and the protected-snapshot
//! scan/update plumbing. The provider drives these from its insert/delete path.

use super::delete::CayenneDeletionSink;
use super::pk_index::{
    CachedPkIndex, PendingPkExistence, PkDigestSet, PkExistenceRef, ShardedPkIndex,
};
use crate::metadata::InlinedData;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;

use crate::row_converter::RowConverter;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::execution::TaskContext;
use datafusion_catalog::Session;
use datafusion_expr::Expr;
use datafusion_physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_table_providers::util::on_conflict::OnConflict;
use parking_lot::Mutex as ParkingMutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use super::deletion_index::{DeletionIndex, KeyDeletionIndex};
use super::deletion_strategy::PkDeletionStrategyWithCache;
use super::table::{
    CayenneTableProvider, InlinedDeletionMaps, OnConflictExt, UpsertOptions,
    record_cayenne_write_phase,
};

/// Prepared deletion metadata and process-local visibility state for a staged upsert.
pub struct PreparedOnConflictDeletionPublish {
    pub(crate) durable_payload: Option<PreparedOnConflictDurablePayload>,
    pub(crate) cleanup_armed: bool,
    pub(crate) pending_inline_tombstone_owned: bool,
    pub(crate) table: CayenneTableProvider,
    pub(crate) publish_as_protected_snapshot: bool,
    pub(crate) target_snapshot_id: String,
    pub(crate) snapshot_sequence: i64,
    pub(crate) delete_sequence: Option<i64>,
    pub(crate) insert_sequence: Option<i64>,
    pub(crate) deleted_pk_i64: Vec<i64>,
    pub(crate) deleted_row_keys: Vec<Box<[u8]>>,
    /// PKs whose prior copy was INLINE (the ones the inline tombstone hides),
    /// kept separate from the file-deletion `deleted_pk_i64`/`deleted_row_keys`
    /// above (cycle-5 TASK 1). At finalize these — at `delete_sequence` — are the
    /// removal applied to the inline-cache base via `pending_tombstone_deltas`, so
    /// they MUST be the inline keys (the tombstone's keys), NOT the file keys: a
    /// file-conflict deletion never matches a cached inline row, so using the file
    /// keys would fail to hide the old inline copy (a transient duplicate). Empty
    /// when the batch replaced no inline rows (then `inlined_delete_id` is `None`
    /// and no removal is enqueued). One of the two is always empty per PK strategy.
    pub(crate) deleted_inlined_pk_i64: Vec<i64>,
    pub(crate) deleted_inlined_row_keys: Vec<Box<[u8]>>,
    pub(crate) position_deletions: HashMap<String, Vec<u32>>,
    /// `inlined_id` of the inline tombstone this staged upsert wrote with
    /// `published = false` (Option D), or `None` when the batch replaced no
    /// inlined rows. At finalize (`publish_prepared_on_conflict_deletions`, under
    /// the listing fence, after the replacement files are moved into the snapshot)
    /// the tombstone is activated IN MEMORY (recorded in
    /// `inlined_locally_published` so the read filter applies it immediately) and
    /// its durable `published = 1` flip is DEFERRED into
    /// `pending_durable_tombstone_flips` — the cycle-4 b1★ Stage-B-writer-free
    /// path. `pending_inline_tombstones` is decremented there. Carrying the exact
    /// id (rather than re-deriving from keys) makes both the in-memory activation
    /// and the later durable flip target precisely THIS tombstone — never a
    /// later-staged tombstone for the same PK.
    pub(crate) inlined_delete_id: Option<String>,
    /// Count of existing rows superseded by this upsert, captured from
    /// [`OnConflictDeletions::total_superseded`] at validation time. This is the
    /// authoritative live-row-delta input: it must NOT be recomputed from the
    /// fields above, because `deleted_pk_i64` and `deleted_row_keys` carry the
    /// SAME `Int64Pk` deletions in two encodings (i64 + committed byte keys), so
    /// summing their lengths double-counts, and neither captures `position_deletions`.
    pub(crate) superseded: usize,
}

pub(crate) struct PreparedOnConflictDurablePayload {
    pub(crate) table_id: String,
    pub(crate) delete_files: Vec<crate::metadata::DeleteFile>,
    pub(crate) insert_pk_bytes: Vec<Vec<u8>>,
    pub(crate) inline_tombstone: Option<crate::metadata::InlinedDelete>,
    pub(crate) pending_durable_flips: Vec<String>,
}

impl PreparedOnConflictDeletionPublish {
    /// The commit sequence this staged upsert publishes under. An on-conflict
    /// append carries no `append_sequence`, so this is the value its validated
    /// primary keys must be stamped with for per-key optimistic concurrency.
    #[must_use]
    pub fn snapshot_sequence(&self) -> i64 {
        self.snapshot_sequence
    }

    /// Return the exact deletion-vector paths owned by abort cleanup.
    pub fn cleanup_paths(&self) -> Vec<std::path::PathBuf> {
        self.durable_payload
            .as_ref()
            .map_or_else(Vec::new, |payload| {
                payload
                    .delete_files
                    .iter()
                    .map(|file| std::path::PathBuf::from(&file.path))
                    .collect()
            })
    }

    /// Mark the durable metadata committed and disarm destructive abort cleanup.
    pub fn mark_catalog_committed(&mut self) {
        self.cleanup_armed = false;
        self.pending_inline_tombstone_owned = false;
    }

    /// Relinquish process-local bookkeeping without deleting physical files.
    ///
    /// Used when a shared transaction's durable outcome is mixed or cannot be
    /// read. The top-level WAL remains authoritative for restart recovery, so
    /// deleting staged vectors would be unsafe, but counters and deferred flips
    /// owned by this process must still be restored before the value is dropped.
    pub fn retain_files_for_wal_recovery(&mut self) {
        if let Some(payload) = self.durable_payload.as_mut() {
            self.table.restore_aborted_inline_tombstone_bookkeeping(
                &mut self.pending_inline_tombstone_owned,
                &mut payload.pending_durable_flips,
            );
        } else {
            let mut no_pending_flips = Vec::new();
            self.table.restore_aborted_inline_tombstone_bookkeeping(
                &mut self.pending_inline_tombstone_owned,
                &mut no_pending_flips,
            );
        }
        self.cleanup_armed = false;
    }

    /// Disarm abort cleanup when recovery proves that an ambiguously completed
    /// shared transaction committed this payload. Exact path matching is used:
    /// an unrelated later catalog row must never retain this batch's files.
    pub(crate) fn mark_catalog_committed_if_paths_match(
        &mut self,
        committed_paths: &std::collections::HashSet<String>,
    ) -> bool {
        let Some(payload) = self.durable_payload.as_ref() else {
            return true;
        };
        if payload
            .delete_files
            .iter()
            .all(|file| committed_paths.contains(&file.path))
        {
            self.mark_catalog_committed();
            true
        } else {
            false
        }
    }
}

impl Drop for PreparedOnConflictDeletionPublish {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        if let Some(payload) = self.durable_payload.as_mut() {
            self.table.restore_aborted_inline_tombstone_bookkeeping(
                &mut self.pending_inline_tombstone_owned,
                &mut payload.pending_durable_flips,
            );
        } else {
            let mut no_pending_flips = Vec::new();
            self.table.restore_aborted_inline_tombstone_bookkeeping(
                &mut self.pending_inline_tombstone_owned,
                &mut no_pending_flips,
            );
        }
        let paths = self.cleanup_paths();
        if paths.is_empty() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                super::delete::cleanup_uncommitted_delete_paths(&paths).await;
            });
        } else {
            std::thread::spawn(move || {
                for path in paths {
                    match std::fs::remove_file(path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => tracing::warn!(
                            %error,
                            "Failed to clean uncommitted deletion-vector file"
                        ),
                    }
                }
            });
        }
    }
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
pub(crate) struct TombstoneDelta {
    /// Monotonic queue sequence (`tombstone_delta_seq` at publish). Globally
    /// unique and never reset, so an `InlinedCache` records the highest delta it
    /// has applied (`tombstone_delta_seq`) and the delta path applies exactly the
    /// deltas with `seq > base.tombstone_delta_seq`.
    pub(crate) seq: u64,
    /// The tombstone's `delete_sequence`. An entry's row is removed iff its PK is
    /// in this delta AND the entry `sequence_number <= delete_sequence` (mirrors
    /// `filter_inlined_batch_for_deletions`: keep iff `data_sequence > delete_sequence`).
    pub(crate) delete_sequence: i64,
    /// Deleted Int64 PKs (for `Int64Pk` tables). Empty for composite-key tables.
    pub(crate) int64_pk: Vec<i64>,
    /// Deleted encoded row-keys (for `RowConverterBased` tables). Empty for
    /// `Int64Pk` tables.
    pub(crate) row_keys: Vec<Box<[u8]>>,
}

impl TombstoneDelta {
    /// Approximate heap footprint, used to bound the pending-delta queue.
    fn approx_keys(&self) -> usize {
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
const MAX_PENDING_TOMBSTONE_DELTAS: usize = 256;
const MAX_PENDING_TOMBSTONE_DELTA_KEYS: usize = 1_000_000;

/// Queue of published-but-not-yet-baked tombstone removals plus the live
/// monotonic sequence counter (cycle-5 TASK 1). Guarded by a single
/// `ParkingMutex` shared across writer clones; mutated only under that lock so
/// the `seq` and the `deltas` stay consistent.
#[derive(Default)]
pub(crate) struct PendingTombstoneDeltas {
    /// Monotonic sequence; the value of the most recently enqueued delta. A new
    /// delta is assigned `seq + 1`. Never reset (so seqs are globally unique even
    /// across a queue drain).
    pub(crate) seq: u64,
    /// Deltas pending application to the inline-cache base, ordered by `seq`
    /// ascending. Drained from the front once a stored cache has baked them in.
    deltas: VecDeque<TombstoneDelta>,
    /// Running sum of `deltas[..].approx_keys()` for the O(1) cap check.
    total_keys: usize,
}

impl PendingTombstoneDeltas {
    /// Enqueue a published tombstone's removal and return its assigned sequence.
    pub(crate) fn push(
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
    pub(crate) fn over_cap(&self) -> bool {
        self.deltas.len() > MAX_PENDING_TOMBSTONE_DELTAS
            || self.total_keys > MAX_PENDING_TOMBSTONE_DELTA_KEYS
    }

    /// Drop deltas with `seq <= applied_through` from the front — they are
    /// provably baked into a cache stored with `tombstone_delta_seq >=
    /// applied_through`, which is the base every future miss extends. Safe under
    /// concurrent populates because the queue is monotonic and a stale store only
    /// triggers a (correct) miss-and-recompute, and any delta above
    /// `applied_through` is retained.
    pub(crate) fn drain_through(&mut self, applied_through: u64) {
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
    pub(crate) fn removal_above(&self, base_seq: u64) -> (InlinedDeletionMaps, u64) {
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

#[derive(Default)]
pub(crate) struct ExtractedPrimaryKeys {
    pub(crate) int64_pk: Vec<i64>,
    pub(crate) row_keys: Vec<Box<[u8]>>,
}

#[derive(Default)]
pub(crate) struct InlinedDataRewrite {
    pub(crate) updated_data: Vec<InlinedData>,
    pub(crate) deleted_inlined_ids: Vec<String>,
    pub(crate) removed_rows: usize,
}

impl InlinedDataRewrite {
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.updated_data.is_empty() && self.deleted_inlined_ids.is_empty()
    }
}

pub(crate) struct InlineAwareDeletionSink {
    pub(crate) table: CayenneTableProvider,
    pub(crate) file_sink: CayenneDeletionSink,
    pub(crate) filters: Vec<Expr>,
}

/// `true` when the delete targets every row — an empty filter list, or every
/// filter being the always-true literal `true` (a TRUNCATE / `DELETE … WHERE
/// TRUE`, which the CDC truncate path emits as `vec![lit(true)]`).
pub(crate) fn is_delete_all(filters: &[Expr]) -> bool {
    filters.iter().all(|filter| {
        matches!(
            filter,
            Expr::Literal(datafusion_common::ScalarValue::Boolean(Some(true)), _)
        )
    })
}

/// Taints the maintained live row count's exactness around a user `DELETE`.
///
/// A delete tombstones rows the persisted `num_rows` still counts, and nothing
/// re-derives that count — `cached_table_statistics_for_optimizer` only *masks*
/// the drift while `has_pending_deletions()` holds. Any path that folds the
/// tombstone (compaction, overwrite, datalake promotion, the seq-prefix bake)
/// drops that mask, and one that does not also re-baseline the count with
/// [`RowCountUpdate::Set`] leaves it served `Exact` over a stale value — which a
/// distributed `COUNT(*)` can substitute into its result. Tainting exactness at
/// delete time makes the mask no longer the only thing standing between a stale
/// count and an `Exact` answer, for every fold path at once.
///
/// The count itself is deliberately left alone rather than decremented: the
/// deleted total spans tiers the persisted count does not uniformly include (a
/// delete-all also purges the mem tier), so subtracting it can under-count. An
/// over-count served `Inexact` is a planner estimate; an under-count that a later
/// `Set` has not yet corrected would be a wrong answer.
///
/// [`RowCountUpdate::Set`]: super::column_stats::RowCountUpdate::Set
pub(crate) struct RowCountExactnessTaintingDeletionSink {
    pub(crate) table: CayenneTableProvider,
    pub(crate) inner: Arc<dyn DeletionSink>,
}

#[async_trait]
impl DeletionSink for RowCountExactnessTaintingDeletionSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Taint BEFORE the inner delete, which publishes durably and cannot be
        // undone. Taint-then-delete can only leave the count conservative (a
        // delete that removes nothing, errors, or is cancelled costs the metadata
        // `COUNT(*)` fast path until the next full rewrite); delete-then-taint
        // leaves the *unsafe* residue — a cancellation, crash, or failed
        // statistics write between the two, after which the tombstone is durable
        // while `num_rows_exact` still claims the stale count is the live one, and
        // a later fold un-masks it as `Exact`. This mirrors
        // `PkKeysetInvalidatingDeletionSink`'s unconditional pre-delete
        // `mark_pk_keyset_occ_degraded`, and for the same reason: on this path the
        // conservative direction is free and the optimistic one is a wrong answer.
        self.table.taint_persisted_row_count_exactness().await;
        self.inner.delete_from(context).await
    }
}

pub(crate) struct PkKeysetInvalidatingDeletionSink {
    pub(crate) table: CayenneTableProvider,
    pub(crate) inner: Arc<dyn DeletionSink>,
    /// The delete request's filters, needed to recognize a delete-all so the
    /// mem-tier can be purged alongside the inner sink's file-side work.
    pub(crate) filters: Vec<Expr>,
}

#[async_trait]
impl DeletionSink for PkKeysetInvalidatingDeletionSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        self.table.mark_maintained_aggregates_stale();
        // Degrade per-key OCC BEFORE the inner delete runs. `self.inner.delete_from`
        // draws the delete sequence and (for an upsert table) leaves the deleted
        // keys stale-present in the Exact keyset with their pre-delete stamps, and
        // it acquires + releases the table `write_lock` INTERNALLY. If the flag were
        // set only afterward, a transaction commit could acquire `write_lock` in the
        // window between the inner delete releasing it and this flag write, run
        // `transaction_has_conflict` against a non-degraded keyset, trust a
        // stale-present stamp, and resurrect a just-deleted key (a missed conflict).
        // Setting the flag first (a `Release` store) orders it ahead of any commit
        // that can observe the delete's effects. It is set unconditionally here
        // (before we know the deleted count): degrading on a zero-row delete only
        // costs a conservative per-table fallback until the next rebuild, never a
        // missed conflict. A `DoNothing` table's post-delete `clear_cached_pk_keyset`
        // below resets the flag and rebuilds exact; an upsert table keeps the
        // stale-superset keyset and stays degraded until its next rebuild.
        self.table.mark_pk_keyset_occ_degraded();
        let mut deleted = self.inner.delete_from(context).await?;

        // Delete-all (TRUNCATE / `DELETE … WHERE TRUE`): the inner sink records
        // `(file, file-local position)` deletes, and rows resident in the
        // in-memory tier live in no file — so nothing tombstones them and they
        // stay visible. A table with no primary key reaches this sink for every
        // delete (`pk_deletion_strategy` is `PositionBased` exactly then), and in
        // `mode: memory` the mem-tier is the permanent store, so without this the
        // table can never be emptied. Mirrors `InlineAwareDeletionSink` below,
        // which covers the key-based arm. Skipped for filtered deletes, whose
        // predicate cannot be evaluated against the tier here.
        //
        // `purge_mem_tier_all` requires the table `write_lock`, which the inner
        // sink takes and releases internally, so acquire it here rather than
        // nesting. Purge before the `deleted > 0` bookkeeping below: on a table
        // whose rows are *only* in the mem-tier the inner count is 0, and the
        // cached scan statistics still need invalidating once the purge changes
        // the visible row count.
        if is_delete_all(&self.filters) {
            let _guard = self.table.write_lock.lock().await;
            let purged = self
                .table
                .purge_mem_tier_all()
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            deleted = deleted.saturating_add(purged);
        }

        if deleted > 0 {
            // Keyset clear-on-delete avoidance (cycle-4 incremental lever).
            //
            // This is a FILTER-based DELETE (`DELETE … WHERE <predicate>`), so
            // the deleted PK set is NOT enumerable at this call site — only the
            // count is. We therefore cannot surgically `remove` keys from the
            // Exact keyset here. But for an `Upsert` table we do not need to:
            // leaving a deleted key STALE-PRESENT in the existence index only
            // ever produces a redundant key-based delete tombstone on a later
            // re-insert of that PK, which masks no prior version (none exists)
            // and is harmless — exactly the false-positive invariant documented
            // on `PkBloom` (see `provider::pk_index::PkBloom`) and exercised on the upsert
            // existence path in `apply_on_conflict_to_batch` (both the Exact arm
            // at ~6106 and the Bloom arm at ~6159 keep the row and emit at most a
            // no-op delete). So for upsert tables we SKIP the clear entirely and
            // keep the stale-superset index — eliminating the O(live-rows)
            // `load_existing_pk_index` cold rebuild the next CDC insert batch would
            // otherwise pay (measured 277 ms × 244 = 68 s/600 s on `new_order`).
            //
            // `DoNothing` tables need an EXACT answer (a stale-present entry would
            // wrongly DROP a genuinely new row at `apply_on_conflict_to_batch`
            // ~6105), and their keys are not enumerable on this filter path, so
            // they keep the conservative full clear and rebuild next batch.
            // `upsert_bloom_eligible()` is precisely "is this an `Upsert` table".
            // Upsert tables keep the stale-superset keyset (already degraded before
            // the delete above, so its stale stamps are never trusted until the
            // next rebuild); `DoNothing` tables need exactness, so clear and rebuild
            // next batch (which also resets the degraded flag).
            if !self.table.upsert_bloom_eligible() {
                self.table.clear_cached_pk_keyset();
            }
            // Drop the per-file stats `CayenneTableProvider::collect_scan_file_statistics`
            // caches. Without this, a follow-up `COUNT(*)` (or any other stats-driven
            // query) is served the row count we computed *before* this delete added
            // its rows to the position-based deletion vector, so the count is stale —
            // see `tests/position_based_deletion_test.rs::test_position_based_sequential_deletes`.
            // (Independent of the keyset: always invalidate so counts stay fresh.)
            self.table.invalidate_scan_file_statistics();
        }
        Ok(deleted)
    }
}

#[async_trait]
impl DeletionSink for InlineAwareDeletionSink {
    async fn delete_from(
        &self,
        _context: Arc<TaskContext>,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let _write_guard = self.table.write_lock.lock().await;
        self.table.mark_maintained_aggregates_stale();

        let (inline_rewrite, inlined_deleted) = self
            .table
            .prepare_inlined_rows_matching_filters(&self.filters)
            .await?;
        let mut prepared_file_delete = self.file_sink.prepare_delete().await?;
        let file_deleted = prepared_file_delete
            .as_ref()
            .map_or(0, super::delete::PreparedDeletionPublish::deleted_count);

        if !inline_rewrite.is_empty() || prepared_file_delete.is_some() {
            let delete_files = prepared_file_delete
                .as_ref()
                .map_or_else(Vec::new, |prepared| prepared.delete_files().to_vec());
            if let Err(error) = self
                .table
                .metadata_catalog()
                .commit_delete_files_with_inlined_rewrite(
                    delete_files,
                    self.table.table_id(),
                    inline_rewrite.updated_data.clone(),
                    inline_rewrite.deleted_inlined_ids.clone(),
                )
                .await
            {
                return Err(Box::new(error));
            }
            if let Some(prepared) = &mut prepared_file_delete {
                prepared.mark_catalog_committed();
            }
            if let Some(prepared) = prepared_file_delete {
                prepared.publish()?;
            }
            if !inline_rewrite.is_empty() {
                self.table.publish_inlined_rewrite(&inline_rewrite);
            }
        }

        let mut deleted = inlined_deleted.checked_add(file_deleted).ok_or_else(|| {
            Box::new(datafusion_common::DataFusionError::Execution(
                "Deleted row count overflowed u64".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // Delete-all (TRUNCATE / `DELETE … WHERE TRUE`): the file/inline sink
        // above tombstones only durable file rows and catalog-inlined data, and
        // cannot enumerate keys — so un-checkpointed rows still resident in the
        // in-memory CDC mem-tier survive and keep showing up in scans (#11987).
        // Discard them wholesale here, under the `write_lock` held above so no
        // concurrent CDC apply mutates the tier. Skipped for per-key deletes,
        // which land their own key tombstones across every tier.
        if is_delete_all(&self.filters) {
            let purged = self
                .table
                .purge_mem_tier_all()
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            deleted = deleted.saturating_add(purged);
        }

        if deleted > 0 {
            // Keyset clear-on-delete avoidance (cycle-4 incremental lever) — see
            // the detailed rationale on `PkKeysetInvalidatingDeletionSink::delete_from`.
            // This is a FILTER-based DELETE, so the deleted PK set is not in hand
            // here. For an `Upsert` table a stale-present existence entry only
            // yields a harmless redundant delete on a later re-insert (the
            // `PkBloom` false-positive invariant, see `provider::pk_index::PkBloom`), so we SKIP
            // the clear and avoid the O(live-rows) `load_existing_pk_index` rebuild
            // the next insert batch would pay. `DoNothing` tables need exactness
            // (a stale entry would wrongly drop a new row) and keep the full clear.
            if self.table.upsert_bloom_eligible() {
                // Upsert stale-superset keyset: retained deleted keys keep their
                // pre-delete per-key OCC stamps — degrade to the per-table
                // fallback until rebuild (see the twin site in
                // `PkKeysetInvalidatingDeletionSink::delete_from`).
                self.table.mark_pk_keyset_occ_degraded();
            } else {
                self.table.clear_cached_pk_keyset();
            }
            if file_deleted > 0 && self.table.pk_deletion_strategy.is_position_based() {
                self.table.clear_scan_file_statistics_cache();
            }
        }

        Ok(deleted)
    }
}

pub(crate) struct BatchValidationResult {
    pub(crate) filtered_batch: Option<RecordBatch>,
    /// Per-file position deletes for located conflict rows: file path -> deleted
    /// file-local row positions. Empty unless `deletion_mode: position`.
    pub(crate) delete_specs: Vec<(Arc<str>, Vec<u64>)>,
    pub(crate) kept_keys: PkDigestSet,
    /// File-backed Int64 PK values being deleted (for `Int64Pk` strategy).
    pub(crate) deleted_pk_i64: Vec<i64>,
    /// File-backed row key bytes being deleted (for `RowConverterBased` strategy).
    pub(crate) deleted_row_keys: Vec<Box<[u8]>>,
    /// Inlined Int64 PK values being deleted.
    pub(crate) deleted_inlined_pk_i64: Vec<i64>,
    /// Inlined row key bytes being deleted.
    pub(crate) deleted_inlined_row_keys: Vec<Box<[u8]>>,
    /// Count of reinsert-over-tombstone resurrections among the keys above; see
    /// [`OnConflictDeletions::reinserted_over_tombstone`].
    pub(crate) reinserted_over_tombstone: usize,
}

pub(crate) struct PreparedInsertStream {
    pub(crate) stream: SendableRecordBatchStream,
    post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
    may_have_on_conflict_deletions: bool,
}

/// Stream wrapper that enforces the non-null primary-key invariant without
/// performing conflict detection. `pk_conflict_detection: none` disables only
/// the existence lookup; it must not make invalid rows writable.
pub(crate) struct PrimaryKeyValidationStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    pk_indices: Vec<usize>,
    table_name: String,
}

impl PrimaryKeyValidationStream {
    pub(crate) fn new(
        inner: SendableRecordBatchStream,
        pk_indices: Vec<usize>,
        table_name: String,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            pk_indices,
            table_name,
        }
    }
}

impl futures::Stream for PrimaryKeyValidationStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if this
                    .pk_indices
                    .iter()
                    .any(|&index| batch.column(index).null_count() > 0)
                {
                    Poll::Ready(Some(Err(datafusion_common::DataFusionError::Execution(
                        format!(
                            "Data validation failed for table '{}': Primary key values must be non-null",
                            this.table_name
                        ),
                    ))))
                } else {
                    Poll::Ready(Some(Ok(batch)))
                }
            }
            other => other,
        }
    }
}

impl RecordBatchStream for PrimaryKeyValidationStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl PreparedInsertStream {
    pub(crate) fn immediate(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            post_validation: Arc::new(ParkingMutex::new(Some(PostValidationState::default()))),
            may_have_on_conflict_deletions: false,
        }
    }

    pub(crate) fn deferred(
        stream: SendableRecordBatchStream,
        post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
        may_have_on_conflict_deletions: bool,
    ) -> Self {
        Self {
            stream,
            post_validation,
            may_have_on_conflict_deletions,
        }
    }

    pub(crate) fn post_validation(&self) -> Arc<ParkingMutex<Option<PostValidationState>>> {
        Arc::clone(&self.post_validation)
    }

    #[must_use]
    pub(crate) const fn may_have_on_conflict_deletions(&self) -> bool {
        self.may_have_on_conflict_deletions
    }
}

/// Prepared sharded insert (the N>1 in-memory CDC path, §2.3c/§5 Phase 3).
///
/// Unlike [`PreparedInsertStream`], the stream is the RAW decoded upstream — NOT
/// wrapped in an [`OnConflictValidationStream`] — because the sharded path runs
/// the on-conflict validation PER SHARD after splitting each batch by
/// `shard_of_pk`. The pre-apply existence snapshot is carried as a
/// [`ShardedPkIndex`] (one existence view per shard), so a shard validates only
/// against its own keys (a key's whole history is confined to one shard, §3.1).
///
/// The single-shard (`n == 1`) path never uses this — it takes the existing
/// `prepare_stream_for_insert` flow unchanged, keeping N=1 byte-identical.
pub(crate) struct PreparedShardedInsertStream {
    /// Raw decoded upstream stream (no validation wrapper).
    pub(crate) stream: SendableRecordBatchStream,
    /// PK column indices (in the stream's schema) for the shard split + validate.
    pub(crate) pk_indices: Vec<usize>,
    /// The PK existence converter, reused across the apply's batches. An `Arc`
    /// so it can be the table's cached `pk_row_converter` (zero per-apply rebuild)
    /// for composite PKs, or a freshly built one for `Int64` PKs (no cache).
    pub(crate) converter: Arc<RowConverter>,
    /// Pre-apply per-shard existence snapshot. `None` when conflict detection is
    /// off (`pk_conflict_detection: none`) or the source trusts uniqueness — the
    /// drain then appends every row with no validation, mirroring the immediate
    /// path.
    pub(crate) sharded_index: Option<ShardedPkIndex>,
    /// The resolved on-conflict behavior for this table.
    pub(crate) on_conflict: OnConflict,
}

#[derive(Default)]
pub(crate) struct OnConflictDeletions {
    /// Per-file position deletes: file path -> deleted file-local row positions.
    /// Routed to the position-vector write path; empty unless `deletion_mode: position`.
    pub(crate) delete_specs: HashMap<Arc<str>, Vec<u64>>,
    /// Deleted file-backed Int64 PK values (for `Int64Pk` strategy).
    pub(crate) deleted_pk_i64: Vec<i64>,
    /// Deleted file-backed row keys (for `RowConverterBased` strategy).
    pub(crate) deleted_row_keys: Vec<Box<[u8]>>,
    /// Deleted inlined Int64 PK values.
    pub(crate) deleted_inlined_pk_i64: Vec<i64>,
    /// Deleted inlined row keys.
    pub(crate) deleted_inlined_row_keys: Vec<Box<[u8]>>,
    /// How many of the keys in the lists above are reinsert-over-tombstone
    /// resurrections (a key ABSENT from the visible existence index that still
    /// carried a pending DELETE tombstone) rather than supersedes of a live row.
    /// These are pushed to the key-lists ONLY to drive the reinsert-marker
    /// machinery (reserve an `insert_sequence`, write the insert-record); they
    /// replace no live row, so `total_superseded` excludes them from the
    /// live-row delta. Counted ONCE per resurrected key even though the key is
    /// pushed to both a file and an inline list. See `apply_on_conflict_to_batch`.
    pub(crate) reinserted_over_tombstone: usize,
}

impl OnConflictDeletions {
    /// Total number of existing rows superseded (deleted) by this upsert across
    /// all strategies, each superseded row counted exactly ONCE. Used to net the
    /// live row count: an upsert that replaces N existing rows adds
    /// `inserted - N` live rows, not `inserted`.
    ///
    /// Counting a row once requires accounting for the deliberate dual encoding
    /// of `FilePositioned` conflicts under the `Int64Pk`/`RowConverterBased`
    /// strategies: `apply_on_conflict_to_batch` pushes BOTH a per-file position
    /// delete (`delete_specs`, masked inside the Vortex scan) AND a key-based
    /// twin (`deleted_pk_i64`/`deleted_row_keys`) covering the paths position
    /// vectors never reach (e.g. the in-memory CDC tier's merge-on-read
    /// tombstones, built from the key lists only — see `build_mem_tombstones`).
    /// Summing all five collections would count those rows twice. The key-based
    /// total already counts every dual-encoded row once, so position deletes
    /// contribute only the EXCESS beyond the key-based total — exactly the
    /// conflicts with no key twin (the `PositionBased` strategy, whose key
    /// lists stay empty).
    ///
    /// Reinsert-over-tombstone resurrections (`reinserted_over_tombstone`) are
    /// subtracted from BOTH the file-key and inline-key totals: a resurrected
    /// key sits in both a file and an inline list (to drive the reinsert marker)
    /// but supersedes no live row, so it must not inflate the live-row delta. A
    /// table uses a single PK strategy, so the count's keys live in exactly one
    /// of the `i64`/row-key list pairs and `saturating_sub` never underflows.
    pub(crate) fn total_superseded(&self) -> usize {
        let position_deletes = self.delete_specs.values().map(Vec::len).sum::<usize>();
        let reinserts = self.reinserted_over_tombstone;
        let file_key_deletes =
            (self.deleted_pk_i64.len() + self.deleted_row_keys.len()).saturating_sub(reinserts);
        let inline_key_deletes = (self.deleted_inlined_pk_i64.len()
            + self.deleted_inlined_row_keys.len())
        .saturating_sub(reinserts);
        file_key_deletes + position_deletes.saturating_sub(file_key_deletes) + inline_key_deletes
    }
}

/// `apply_on_conflict_deletions` performs all durable deletion-vector and
/// inlined-data rewrite I/O but returns the computed in-memory visibility
/// updates instead of storing them, so the stores can be committed
/// synchronously — together with the protected snapshot publish — under a
/// single `scan_state_lock.write()`. This keeps the scan-excluding guard held
/// for microseconds rather than across durable writes.
pub(crate) struct OnConflictUpdate {
    pub(crate) deletion_update: OnConflictDeletionUpdate,
    /// Set when `apply_on_conflict_deletions` durably wrote an inline tombstone
    /// (via `add_inlined_delete`) to hide the prior inline copy of an upserted
    /// PK. Publishing must then bump `inlined_generation` (under
    /// `scan_state_lock`) so the next scan rebuilds the inline view and observes
    /// the tombstone atomically with the deletion-cache + protected-snapshot
    /// flips. A tombstone only adds a hide-marker — it appends no inline DATA
    /// rows and changes no row count — so unlike the previous inline-rewrite
    /// path there is no visibility watermark to advance.
    pub(crate) inlined_tombstone_written: bool,
}

impl OnConflictUpdate {
    pub(crate) fn none() -> Self {
        Self {
            deletion_update: OnConflictDeletionUpdate::None,
            inlined_tombstone_written: false,
        }
    }

    pub(crate) fn from_deletion_update(deletion_update: OnConflictDeletionUpdate) -> Self {
        Self {
            deletion_update,
            inlined_tombstone_written: false,
        }
    }

    pub(crate) fn with_inlined_tombstone_written(mut self, written: bool) -> Self {
        self.inlined_tombstone_written = written;
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.deletion_update, OnConflictDeletionUpdate::None)
            && !self.inlined_tombstone_written
    }
}

/// A replayable deletion-index delta, folded into the LIVE index under `rcu`
/// at publish time.
///
/// Carrying the operations — rather than a snapshot prebuilt from a `load` at
/// prepare time — is what makes the on-conflict publish lost-update-safe:
/// `commit_on_conflict_deletion_update` re-applies the delta against whatever
/// index is live when it commits, so a concurrent compaction prune or
/// delete-sink add (which serialize on a DIFFERENT lock than the on-conflict
/// finalize) can never be clobbered by storing a snapshot built off a stale
/// load. The `extend_max_*` folds are per-key max, so replaying the delta over
/// concurrent changes is order-independent.
pub(crate) struct Int64DeletionDelta {
    /// `(delete_sequence, pks)` groups folded via `extend_max_deletes`.
    pub(crate) pure: Vec<Int64DeleteGroup>,
    /// `(delete_sequence, pks, insert_sequence)` groups folded via `extend_max_conflicts`.
    pub(crate) reinsert: Vec<Int64ReinsertGroup>,
}

/// One `extend_max_deletes` group: a delete sequence and the int64 PKs deleted at it.
type Int64DeleteGroup = (i64, Vec<i64>);
/// One `extend_max_conflicts` group: delete sequence, int64 PKs, and the re-insert sequence.
type Int64ReinsertGroup = (i64, Vec<i64>, i64);
/// Key-based counterpart of [`Int64DeleteGroup`].
type RowKeyDeleteGroup = (i64, Vec<Box<[u8]>>);
/// Key-based counterpart of [`Int64ReinsertGroup`].
type RowKeyReinsertGroup = (i64, Vec<Box<[u8]>>, i64);

/// Key-based counterpart to [`Int64DeletionDelta`].
pub(crate) struct RowKeyDeletionDelta {
    /// `(delete_sequence, keys)` groups folded via `extend_max_deletes`.
    pub(crate) pure: Vec<RowKeyDeleteGroup>,
    /// `(delete_sequence, keys, insert_sequence)` groups folded via `extend_max_conflicts`.
    pub(crate) reinsert: Vec<RowKeyReinsertGroup>,
}

pub(crate) enum OnConflictDeletionUpdate {
    /// No key-based deletion-cache change (pure position deletes or no deletes).
    None,
    /// `Int64Pk` deletion delta to fold into the live index.
    Int64Pk(Int64DeletionDelta),
    /// `RowConverterBased` deletion delta to fold into the live index.
    RowConverter(RowKeyDeletionDelta),
}

#[derive(Clone)]
pub(crate) enum PkDeletionSnapshot {
    PositionBased,
    Int64Pk { tombstones: Arc<DeletionIndex> },
    RowConverterBased { tombstones: Arc<KeyDeletionIndex> },
}

/// PK membership of a mem-tier checkpoint's flushed corpus (the visible inline +
/// tier rows being encoded into the new snapshot), keyed by deletion strategy.
/// Splits the tier's tombstones at durable-commit time: a tombstoned key WITH a
/// corpus row was re-inserted after its delete and must carry the reinsert
/// marker so the flushed row stays visible; a tombstoned key WITHOUT one is a
/// pure delete and must be committed delete-only (a phantom reinsert marker
/// would resurrect older durable copies on the main scan path). See
/// `commit_mem_tier_checkpoint_metadata`.
pub(crate) enum CheckpointCorpusKeys {
    Int64(HashSet<i64>),
    RowKeys(HashSet<Box<[u8]>>),
    None,
}

impl CheckpointCorpusKeys {
    pub(crate) fn contains_i64(&self, pk: i64) -> bool {
        matches!(self, Self::Int64(keys) if keys.contains(&pk))
    }

    pub(crate) fn contains_row_key(&self, key: &[u8]) -> bool {
        matches!(self, Self::RowKeys(keys) if keys.contains(key))
    }
}

impl PkDeletionSnapshot {
    /// Identity of the inner index allocation, for the merged-scan memo key.
    /// `None` for `PositionBased` (no index; the merge is a no-op there anyway).
    pub(crate) fn index_ptr(&self) -> Option<usize> {
        match self {
            Self::PositionBased => None,
            Self::Int64Pk { tombstones } => Some(Arc::as_ptr(tombstones) as usize),
            Self::RowConverterBased { tombstones } => Some(Arc::as_ptr(tombstones) as usize),
        }
    }

    pub(crate) fn has_deletions(&self) -> bool {
        match self {
            Self::PositionBased => false,
            Self::Int64Pk { tombstones } => tombstones.has_deletions(),
            Self::RowConverterBased { tombstones } => tombstones.has_deletions(),
        }
    }

    /// Count of keys with a live deletion in this snapshot — the per-query
    /// merge-on-read probe scales with this, so the seq-prefix bake is triggered
    /// on it (see `BAKE_DELETION_INDEX_TRIGGER`). `0` for `PositionBased` (those
    /// tombstones are file-scoped, never seq-tagged, and are out of the bake's
    /// scope).
    pub(crate) fn delete_len(&self) -> usize {
        match self {
            Self::PositionBased => 0,
            Self::Int64Pk { tombstones } => tombstones.delete_len(),
            Self::RowConverterBased { tombstones } => tombstones.delete_len(),
        }
    }

    /// Count of re-insert records in this snapshot — keys whose tombstone is
    /// superseded by a later insert.
    ///
    /// Its ratio to [`Self::delete_len`] is how much of the index is dead
    /// weight: in an upsert workload most tombstones are immediately superseded,
    /// so a high ratio means the index's size is carrying history the probe no
    /// longer needs. `0` for `PositionBased`, matching `delete_len`.
    pub(crate) fn insert_len(&self) -> usize {
        match self {
            Self::PositionBased => 0,
            Self::Int64Pk { tombstones } => tombstones.insert_len(),
            Self::RowConverterBased { tombstones } => tombstones.insert_len(),
        }
    }

    /// Merge a mem-tier tombstone map into this file-side snapshot — the scan
    /// path passes the cross-shard UNION (`ShardedMemTier::union_tombstones`). At
    /// N==1 the union is shard 0's tombstone map.
    pub(crate) fn with_mem_tier_tombstones_map(
        &self,
        tombstones: &crate::provider::mem_tier::InMemTombstones,
    ) -> Self {
        match self {
            Self::PositionBased => Self::PositionBased,
            Self::Int64Pk { tombstones: file } => {
                if tombstones.int64_pk.is_empty() {
                    return self.clone();
                }

                let updated = file.extend_max_deletes(
                    tombstones
                        .int64_pk
                        .iter()
                        .map(|(&pk, &delete_sequence)| (pk, delete_sequence)),
                );
                Self::Int64Pk {
                    tombstones: Arc::new(updated),
                }
            }
            Self::RowConverterBased { tombstones: file } => {
                if tombstones.row_keys.is_empty() {
                    return self.clone();
                }

                let updated = file.extend_max_deletes(
                    tombstones
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
    /// from the same snapshot (rather than a second, independent
    /// `get_max_delete_sequence()` load) is required for correctness — see
    /// `compact_protected_snapshots_subset`.
    pub(crate) fn max_sequence_number(&self) -> Option<i64> {
        match self {
            Self::PositionBased => None,
            Self::Int64Pk { tombstones } => tombstones.max_sequence_number(),
            Self::RowConverterBased { tombstones } => tombstones.max_sequence_number(),
        }
    }
}

pub(crate) fn pk_deletion_snapshot_for_strategy(
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

pub(crate) struct ProtectedSnapshotScan<'a> {
    pub(crate) state: &'a dyn Session,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) filters: &'a [Expr],
    pub(crate) limit: Option<usize>,
    pub(crate) pk_indices_in_projection: &'a [usize],
    pub(crate) protected_snapshots: Arc<HashMap<String, i64>>,
    pub(crate) deletion_snapshot: &'a PkDeletionSnapshot,
    /// View-typed read schema so protected-snapshot scans match the main file
    /// scan in the union (see `viewify_read_schema`).
    pub(crate) read_schema: SchemaRef,
}

pub(crate) struct PreparedProtectedSnapshotUpdate {
    pub(crate) expected: Arc<HashMap<String, i64>>,
    pub(crate) updated: Arc<HashMap<String, i64>>,
}

#[derive(Default)]
pub(crate) struct PostValidationState {
    pub(crate) on_conflict_deletions: OnConflictDeletions,
    pub(crate) validated_keys: PkDigestSet,
}

/// Aggregate result of one sharded in-memory CDC apply
/// ([`CayenneTableProvider::validate_and_append_sharded`]).
pub(crate) struct ShardedApplyResult {
    /// The single shared per-apply epoch (§3.4 Fix 1), stamped IDENTICALLY on
    /// every shard's segment this apply — NOT a max across shards. Used for the
    /// slot-deferral receipt; the all-shards-atomic Phase 5 checkpoint reconciles
    /// durable coverage on this one axis.
    pub(crate) epoch: u64,
    /// Existing rows superseded across all shards (each counted once), for the
    /// live-row-count net.
    pub(crate) superseded: u64,
    /// Union of every shard's on-conflict deletions (keys disjoint across shards).
    pub(crate) on_conflict_deletions: OnConflictDeletions,
    /// Union of every shard's validated (kept) keys.
    pub(crate) validated_keys: PkDigestSet,
}

pub(crate) struct OnConflictContext<'a> {
    pub(crate) pk_indices: &'a [usize],
    pub(crate) converter: &'a RowConverter,
    pub(crate) on_conflict: &'a OnConflict,
    pub(crate) upsert_options: &'a UpsertOptions,
    pub(crate) existing: PkExistenceRef<'a>,
    /// Keys committed by other writers since `existing` was checked out of its
    /// cache, which `existing` therefore cannot know about (see
    /// [`PendingPkKeys`](super::pk_index::PendingPkKeys)).
    /// Consulted on an `existing` miss so a key committed mid-validation is not
    /// classified as a new primary key. `None` when nothing was committed during
    /// this checkout — the common case.
    pub(crate) pending: Option<&'a PendingPkExistence>,
    pub(crate) incoming_keys: &'a PkDigestSet,
}

pub(crate) struct OnConflictValidationStream {
    pub(crate) table: CayenneTableProvider,
    pub(crate) inner: SendableRecordBatchStream,
    schema: SchemaRef,
    pub(crate) pk_indices: Vec<usize>,
    pub(crate) converter: RowConverter,
    pub(crate) on_conflict: OnConflict,
    pub(crate) upsert_options: UpsertOptions,
    existing_keys: Option<CachedPkIndex>,
    pub(crate) incoming_keys: PkDigestSet,
    pub(crate) kept_keys: PkDigestSet,
    pub(crate) delete_specs: HashMap<Arc<str>, Vec<u64>>,
    pub(crate) deleted_pk_i64: Vec<i64>,
    pub(crate) deleted_row_keys: Vec<Box<[u8]>>,
    pub(crate) deleted_inlined_pk_i64: Vec<i64>,
    pub(crate) deleted_inlined_row_keys: Vec<Box<[u8]>>,
    reinserted_over_tombstone: usize,
    post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
    /// Whether the validation keyset is stored back into the table's shared PK
    /// index cache when the stream finishes. `true` for the ordinary write path
    /// (the keyset was taken from the shared cache and is returned). `false` for
    /// off-lock conditional-commit staging, which validates against a **private**
    /// keyset without holding `write_lock` — storing it back would clobber a
    /// concurrent ordinary writer's cache update and drop committed keys.
    store_back: bool,
    finalized: bool,
}

impl OnConflictValidationStream {
    #[expect(
        clippy::too_many_arguments,
        reason = "distinct stream-construction inputs; grouping them into a struct would not aid clarity"
    )]
    pub(crate) fn new(
        table: CayenneTableProvider,
        inner: SendableRecordBatchStream,
        pk_indices: Vec<usize>,
        converter: RowConverter,
        existing_keys: CachedPkIndex,
        on_conflict: OnConflict,
        post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
        store_back: bool,
    ) -> Self {
        let schema = inner.schema();
        let upsert_options = on_conflict.get_upsert_options();
        Self {
            table,
            inner,
            schema,
            pk_indices,
            converter,
            on_conflict,
            upsert_options,
            existing_keys: Some(existing_keys),
            incoming_keys: PkDigestSet::with_capacity(1024),
            kept_keys: PkDigestSet::with_capacity(1024),
            delete_specs: HashMap::new(),
            deleted_pk_i64: Vec::new(),
            deleted_row_keys: Vec::new(),
            deleted_inlined_pk_i64: Vec::new(),
            deleted_inlined_row_keys: Vec::new(),
            reinserted_over_tombstone: 0,
            post_validation,
            store_back,
            finalized: false,
        }
    }

    fn process_batch(
        &mut self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let existing_index = self.existing_keys.as_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "On-conflict validation for table {} was polled after finalization",
                self.table.table_name()
            ))
        })?;
        let existing = match existing_index {
            CachedPkIndex::Exact(keyset) => PkExistenceRef::Exact(keyset),
            CachedPkIndex::Bloom(bloom) => PkExistenceRef::Bloom(bloom),
        };

        // Snapshot per batch, not per stream: `existing` was checked out before the
        // first batch, and this stream is consumed lazily as the encode runs, so a
        // concurrent writer can commit a key between two batches of it.
        let pending = if self.store_back {
            self.table.pending_pk_existence()
        } else {
            // Off-lock staging validates against a private keyset it just built —
            // it holds no checkout, so the log is another writer's business.
            None
        };

        let mut ctx = OnConflictContext {
            pk_indices: &self.pk_indices,
            converter: &self.converter,
            on_conflict: &self.on_conflict,
            upsert_options: &self.upsert_options,
            existing,
            pending: pending.as_ref(),
            incoming_keys: &self.incoming_keys,
        };

        let validation_start = Instant::now();
        let validation_result = self.table.apply_on_conflict_to_batch(batch, &mut ctx);
        record_cayenne_write_phase(
            self.table.table_name(),
            "apply_on_conflict_validation",
            validation_start,
        );

        let BatchValidationResult {
            filtered_batch,
            delete_specs: batch_delete_specs,
            kept_keys,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
            reinserted_over_tombstone,
        } = validation_result.map_err(datafusion_common::DataFusionError::from)?;

        for (file_path, rows) in batch_delete_specs {
            self.delete_specs.entry(file_path).or_default().extend(rows);
        }

        self.deleted_pk_i64.extend(deleted_pk_i64);
        self.deleted_row_keys.extend(deleted_row_keys);
        self.deleted_inlined_pk_i64.extend(deleted_inlined_pk_i64);
        self.deleted_inlined_row_keys
            .extend(deleted_inlined_row_keys);
        self.reinserted_over_tombstone += reinserted_over_tombstone;

        self.incoming_keys.extend_ref(&kept_keys);
        self.kept_keys.absorb(kept_keys);

        Ok(filtered_batch)
    }

    fn store_existing_keyset(&mut self) {
        let existing_keys = self.existing_keys.take();
        // Off-lock staging validates against a private keyset and must never
        // publish it to the shared cache (see `store_back`). Drop it instead.
        if self.store_back
            && let Some(existing_keys) = existing_keys
        {
            self.table.store_cached_pk_index(existing_keys);
        }
    }

    fn finish_success(&mut self) {
        if self.finalized {
            return;
        }

        self.store_existing_keyset();
        let post_validation = PostValidationState {
            on_conflict_deletions: OnConflictDeletions {
                delete_specs: std::mem::take(&mut self.delete_specs),
                deleted_pk_i64: std::mem::take(&mut self.deleted_pk_i64),
                deleted_row_keys: std::mem::take(&mut self.deleted_row_keys),
                deleted_inlined_pk_i64: std::mem::take(&mut self.deleted_inlined_pk_i64),
                deleted_inlined_row_keys: std::mem::take(&mut self.deleted_inlined_row_keys),
                reinserted_over_tombstone: self.reinserted_over_tombstone,
            },
            validated_keys: std::mem::take(&mut self.kept_keys),
        };
        *self.post_validation.lock() = Some(post_validation);
        self.finalized = true;
    }

    fn finish_after_error(&mut self) {
        if self.finalized {
            return;
        }

        self.store_existing_keyset();
        self.finalized = true;
    }
}

impl Unpin for OnConflictValidationStream {}

impl futures::Stream for OnConflictValidationStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finalized {
            return Poll::Ready(None);
        }

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.finish_success();
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(err))) => {
                    this.finish_after_error();
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(batch))) => match this.process_batch(batch) {
                    Ok(Some(filtered_batch)) => return Poll::Ready(Some(Ok(filtered_batch))),
                    Ok(None) => {}
                    Err(err) => {
                        this.finish_after_error();
                        return Poll::Ready(Some(Err(err)));
                    }
                },
            }
        }
    }
}

impl RecordBatchStream for OnConflictValidationStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
