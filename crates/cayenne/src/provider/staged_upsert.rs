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

//! Optimistic primary-key writes staged in unpublished snapshots.
//!
//! Validation uses a private keyset without holding the table write lock. Each
//! statement resolves repeated keys against its complete input. Commit holds
//! the write lock while checking the transaction's read footprint and write-set
//! against a token captured before its gate read. Conflicting writes fail with
//! [`Error::WriteConflict`].
//!
//! Snapshot visibility and deletion metadata commit in one catalog transaction.
//! The protected snapshot's sequence exceeds its deletion sequence so replacement
//! rows survive their own tombstones. The listing fence covers publication of
//! the matching in-memory views. Rollback discards unpublished files.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;

use super::Error;
use super::Result;
use super::append_stage::{StagedAppend, ValidationScope};
use super::column_stats::ColumnStatsAccumulator;
use super::delta_encoding::WritePolicy;
use super::on_conflict::{OnConflictDeletions, PostValidationState};
use super::pk_index::PkDigestSet;
use super::table::CayenneTableProvider;

/// Optimistic-concurrency token for an off-lock transaction write.
///
/// Captured at transaction **begin** (before the gate read) under a brief
/// `write_lock` hold, and re-checked at commit under `write_lock`. It is the
/// per-table sequence high-water plus a "no staging append in flight" bit —
/// together, everything that changes key liveness on the table either advances
/// the sequence or sets the staging bit, so an unchanged token at commit proves
/// no intervening commit touched the table (see the module docs).
#[derive(Debug, Clone, Copy)]
pub struct TransactionWriteToken {
    /// The table's sequence high-water at capture (`allocator.next - 1`).
    stage_seq: i64,
    /// Whether no pipelined staging append was in flight at capture. A staging
    /// append's Stage-B finalize publishes without drawing a sequence, so it is
    /// covered by this bit rather than by `stage_seq`.
    staging_clean: bool,
}

impl TransactionWriteToken {
    /// The table's begin sequence high-water (per-table; each table's allocator
    /// is independent).
    #[must_use]
    pub fn stage_seq(&self) -> i64 {
        self.stage_seq
    }

    /// Whether no staging append was in flight at capture.
    #[must_use]
    pub fn staging_clean(&self) -> bool {
        self.staging_clean
    }
}

/// A staged upsert: the replacement rows have been written to a fresh snapshot
/// directory, but no catalog visibility change has been made yet. The rows are
/// published (or discarded) at [`Self::commit`] / [`Self::rollback`].
pub struct CayenneStagedUpsert {
    table: CayenneTableProvider,
    /// Optimistic-concurrency token captured at transaction begin, re-checked at
    /// commit against the table's live sequence high-water.
    token: TransactionWriteToken,
    new_snapshot_id: String,
    /// The prior versions this upsert supersedes, captured at validation time
    /// and applied under the fence at commit. Taken (`mem::take`) on commit.
    on_conflict_deletions: OnConflictDeletions,
    validated_keys: PkDigestSet,
    stats: Arc<ColumnStatsAccumulator>,
    row_count: u64,
    superseded: usize,
}

impl std::fmt::Debug for CayenneStagedUpsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneStagedUpsert")
            .field("table", &self.table.table_name())
            .field("new_snapshot_id", &self.new_snapshot_id)
            .field("row_count", &self.row_count)
            .field("superseded", &self.superseded)
            .finish_non_exhaustive()
    }
}

impl CayenneStagedUpsert {
    fn new(
        table: CayenneTableProvider,
        token: TransactionWriteToken,
        staged: StagedAppend,
    ) -> Self {
        Self {
            table,
            token,
            new_snapshot_id: staged.new_snapshot_id,
            on_conflict_deletions: staged.on_conflict_deletions,
            validated_keys: staged.validated_keys,
            stats: staged.stats,
            row_count: staged.row_count,
            superseded: staged.superseded,
        }
    }

    /// Number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The write-set (kept keys) of this staged upsert — union'd into the
    /// commit-time per-key OCC re-check.
    pub(crate) fn validated_keys(&self) -> &PkDigestSet {
        &self.validated_keys
    }

    /// Whether committing publishes nothing: no row was staged, so no stored
    /// row is superseded either.
    pub(crate) fn publishes_nothing(&self) -> bool {
        self.row_count == 0
            && self.on_conflict_deletions.delete_specs.is_empty()
            && self.on_conflict_deletions.total_superseded() == 0
            && self.on_conflict_deletions.reinserted_over_tombstone == 0
    }

    /// Prepare this staged upsert's durable payload for an atomic multi-table
    /// commit: reserve sequences + write deletion-vector files (no metastore
    /// commit). The caller holds the table `write_lock`; it applies the payload
    /// via [`PreparedTxnCommit::apply_in_txn`] inside a shared transaction that
    /// fuses all written tables, then publishes with [`PreparedTxnCommit::finish`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table needs the non-composable inline-tombstone or
    /// position-deletion publish path (rejected for multi-table), if the staged
    /// snapshot directory cannot be synced, or if reserving sequences / writing
    /// deletion vectors fails.
    pub async fn prepare_commit(mut self) -> Result<PreparedTxnCommit> {
        // Directory barrier for the staged rows, before the shared transaction
        // makes them visible. The non-transactional publish takes this barrier
        // in `record_written_snapshot_sequence`; the fused path commits the
        // snapshot-sequence row inside the caller's multi-table transaction and
        // so never reaches that helper — without the barrier here a power loss
        // can leave the catalog referencing a snapshot whose directory entries
        // were never flushed. Nothing after `stage_upsert_data` adds files to
        // this directory (the on-conflict deletion vectors are written under the
        // *current* snapshot's `_deletes/`), so the barrier is complete here.
        self.table
            .sync_local_snapshot_dir(&self.new_snapshot_id)
            .await?;

        let on_conflict_deletions = std::mem::take(&mut self.on_conflict_deletions);
        // Reserve sequences + write deletion-vector files + build the inline
        // tombstone, WITHOUT committing the catalog metadata — `defer_catalog_commit`
        // carries them in the returned publish's `durable_payload` for application
        // in the shared multi-table transaction. This composes inline-mode
        // deletions (unlike the earlier bespoke path, which rejected them).
        let mut publish = self
            .table
            .prepare_on_conflict_deletions_for_staged_snapshot(
                on_conflict_deletions,
                self.new_snapshot_id.clone(),
                true,
            )
            .await?;
        // `defer_catalog_commit` couples two concerns for the append path: it
        // both defers the durable catalog write AND suppresses the in-memory
        // protected-snapshot promotion (appends publish into the current
        // snapshot, not a protected one). A staged UPSERT still needs the
        // protected-snapshot promotion — the durable side is recorded by
        // `apply_prepared_on_conflict_in_txn` — so re-enable just the in-memory
        // promotion, which `finish` performs via `publish_prepared_on_conflict_deletions`.
        publish.publish_as_protected_snapshot = true;
        Ok(PreparedTxnCommit {
            table: self.table,
            new_snapshot_id: self.new_snapshot_id,
            validated_keys: self.validated_keys,
            stats: self.stats,
            row_count: self.row_count,
            superseded: self.superseded,
            publish,
        })
    }

    /// Publish the staged rows, making the upsert visible to readers.
    ///
    /// Acquires `write_lock` now (staging ran off-lock) and re-checks the
    /// transaction's per-key read footprint + write-set first, aborting with
    /// [`Error::WriteConflict`] (staged dir cleaned up) if any of those keys was
    /// committed after the transaction began (or, when per-key state is
    /// unavailable, if the table's high-water moved at all). An owned commit task
    /// retains that lock through atomic catalog publication and cache updates.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriteConflict`] on a lost OCC race (retryable), or an
    /// error if applying the deletions, reserving the sequence, or the durable
    /// snapshot-sequence write fails. Uncommitted staged files are removed on error.
    pub async fn commit(
        self,
        footprint: std::collections::HashSet<u128>,
        footprint_complete: bool,
    ) -> Result<u64> {
        // Acquire the write lock now (staging ran off-lock), then re-check the
        // transaction's read footprint + write-set per-key before touching
        // anything visible: if any of those keys was committed after this
        // transaction began, it must abort and retry. Keys not in the footprint
        // are unaffected, so disjoint-key transactions commit concurrently.
        let write_guard = self.table.write_lock_arc().lock_owned().await;
        self.table.ensure_no_incomplete_write().await?;
        let current_high_water = self.table.sequence_high_water().await;
        if !self.token.staging_clean
            || self.table.transaction_has_conflict(
                self.token.stage_seq,
                &footprint,
                footprint_complete,
                &self.validated_keys,
                current_high_water,
            )
        {
            drop(write_guard);
            cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
            return Err(Error::WriteConflict {
                table: self.table.table_name().to_string(),
            });
        }

        super::append_commit::PreparedResolvedAppend {
            context: Arc::clone(self.table.context()),
            table: self.table,
            _write_guard: write_guard,
            snapshot_id: self.new_snapshot_id,
            rows: self.row_count,
            stats: self.stats,
            deletions: self.on_conflict_deletions,
            validated_keys: self.validated_keys,
            superseded: self.superseded,
            into_empty_table: false,
        }
        .commit()
        .await
    }

    /// Discard the staged upsert and remove its staged snapshot directory.
    ///
    /// The catalog was never touched, so this only cleans the orphan directory
    /// (best-effort; object-store orphans are pruned by the next successful
    /// snapshot cleanup cycle).
    ///
    /// # Errors
    ///
    /// Infallible today; returns `Result` for symmetry with the other staged
    /// lifecycles and to allow future durable-cleanup steps.
    pub async fn rollback(self) -> Result<()> {
        // Staging held no lock and the catalog was never touched, so this just
        // removes the orphan directory.
        cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
        Ok(())
    }
}

/// A staged upsert prepared for atomic commit inside a caller-owned
/// `MetastoreTransaction` (multi-table fusion). Sequences are reserved and
/// deletion-vector files written at [`CayenneStagedUpsert::prepare_commit`]; the
/// durable catalog write happens in [`Self::apply_in_txn`] and the in-memory
/// visibility flip in [`Self::finish`], after the shared transaction commits.
pub struct PreparedTxnCommit {
    table: CayenneTableProvider,
    new_snapshot_id: String,
    validated_keys: PkDigestSet,
    stats: Arc<ColumnStatsAccumulator>,
    row_count: u64,
    superseded: usize,
    /// The deferred on-conflict publish (trunk's `durable_payload` model): its
    /// `durable_payload` carries the delete-vector files, protected-snapshot
    /// sequence, inline tombstone, and deferred flips, applied in the shared
    /// transaction by [`Self::apply_in_txn`]. Its `Drop` cleans up the staged
    /// deletion-vector files unless [`Self::mark_committed`] disarms it.
    publish: super::on_conflict::PreparedOnConflictDeletionPublish,
}

impl PreparedTxnCommit {
    /// The written table's id (participants are committed in canonical id order).
    #[must_use]
    pub fn table_id(&self) -> &str {
        self.table.table_id()
    }

    /// The written table's provider — the multi-table orchestrator holds its
    /// visibility + listing-fence locks and reads its catalog for the shared txn.
    pub(crate) fn provider(&self) -> &CayenneTableProvider {
        &self.table
    }

    /// Rows to be published on commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Append this table's deferred on-conflict payload (delete files + snapshot
    /// sequence + inline tombstone + deferred flips) to the caller-owned
    /// transaction, plus the durable write-back markers. No `begin`/`commit`.
    ///
    /// # Errors
    ///
    /// Returns the first statement failure; the caller rolls back the shared
    /// transaction and aborts the whole multi-table commit.
    pub async fn apply_in_txn(
        &mut self,
        catalog: &crate::CayenneCatalog,
        txn: &mut dyn crate::metastore::MetastoreTransaction,
    ) -> crate::catalog::CatalogResult<()> {
        catalog
            .apply_prepared_on_conflict_in_txn(txn, &mut self.publish)
            .await?;
        // Durable federated write-back (#11838): on a durable-write-back table,
        // durably mark the written PKs (their `OwnedRow` encodings) in the SAME
        // commit transaction so the delivery worker reconciles them to the
        // source. A non-write-back table never marks.
        if self.table.is_durable_write_back() {
            let dirty_pk_bytes: Vec<Vec<u8>> = self
                .validated_keys
                .iter()
                .map(|row| row.as_ref().to_vec())
                .collect();
            if !dirty_pk_bytes.is_empty() {
                catalog
                    .mark_dirty_keys_in_txn(
                        txn,
                        self.table.table_id(),
                        &dirty_pk_bytes,
                        self.publish.snapshot_sequence,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// The staged snapshot this commit publishes.
    pub(crate) fn snapshot_id(&self) -> &str {
        &self.new_snapshot_id
    }

    /// The sequence the staged snapshot commits under.
    pub(crate) fn snapshot_sequence(&self) -> i64 {
        self.publish.snapshot_sequence
    }

    /// Keep this table's staged files and refuse its writes until it is
    /// reloaded, after a shared commit whose outcome could not be read back.
    pub(crate) fn retain_after_unknown_outcome(&mut self) {
        self.publish.retain_files_for_wal_recovery();
        self.table
            .publication_outcome_unknown()
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Disarm the publish's destructive abort cleanup after the shared
    /// transaction has durably committed (mirrors the sync path's
    /// post-commit `mark_catalog_committed`).
    pub(crate) fn mark_committed(&mut self) {
        self.publish.mark_catalog_committed();
    }

    /// Flip in-memory visibility after the shared transaction committed. Must run
    /// under the table's held listing fence; best-effort per the same crash
    /// contract as [`super::overwrite::PreparedOverwrite::finish`] (a crash before
    /// finish reconstructs the same state from the catalog on reopen).
    ///
    /// # Errors
    ///
    /// Returns an error only if swapping the in-memory deletion caches fails.
    pub fn finish(self) -> Result<u64> {
        let sequence = self.publish.snapshot_sequence;
        // Taken before the publish below, which makes this transaction's staged
        // rows visible while the `num_rows` delta describing them only reaches
        // the maintenance queue afterwards.
        let reserved_live_rows_delta = self.table.reserve_live_rows_delta();
        self.table
            .publish_prepared_on_conflict_deletions(self.publish);
        // The fused transaction publish just made this transaction's staged rows
        // visible; mark maintained aggregates stale under the same held listing
        // fence so an IVM query cannot serve a Fresh aggregate that omits them.
        // The non-fused `commit_on_conflict_publish` path already does this; the
        // fused path (the only live transaction commit path) previously did not,
        // so aggregate queries served pre-transaction state as Fresh until an
        // unrelated CDC event re-fed the registry. `None` marks stale
        // (conservative base-scan fallback); feeding the staged batches as
        // retraction+insert deltas for incremental maintenance is a follow-up.
        self.table.feed_staged_ivm_under_fence(None);
        // The rows are visible now, so the claim survives from here.
        let published_live_rows_delta = reserved_live_rows_delta.published();
        let retention_requested = self.table.has_retention_delete_filters();
        let live_rows_delta = i64::try_from(self.row_count)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(self.superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(Arc::clone(&self.stats)),
            false,
            retention_requested,
            live_rows_delta,
            published_live_rows_delta,
        );
        if retention_requested {
            self.table.clear_cached_pk_keyset();
        } else {
            self.table
                .record_file_pk_keys(&self.validated_keys, sequence);
        }
        Ok(self.row_count)
    }

    /// Discard a prepared-but-uncommitted table (the shared transaction was not
    /// committed): the publish's `Drop` removes its staged deletion-vector files
    /// (still armed — `mark_committed` was never called); this also removes the
    /// unreferenced staged replacement-snapshot directory.
    pub async fn rollback(self) {
        cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
    }
}

impl CayenneTableProvider {
    /// Capture the optimistic-concurrency token for a transaction write.
    ///
    /// Must be called at transaction **begin**, before the gate read — a commit
    /// landing between the gate read and staging would otherwise be invisible to
    /// the staged validation while making the gate verdict stale (a lost update).
    /// Holds `write_lock` only long enough to read the sequence high-water and
    /// the staging-append flag (the documented soundness condition on
    /// `sequence_high_water`).
    pub async fn transaction_write_token(&self) -> TransactionWriteToken {
        let _guard = self.write_lock_arc().lock_owned().await;
        let stage_seq = self.sequence_high_water().await;
        let staging_clean = !self.has_inflight_staging_appends();
        TransactionWriteToken {
            stage_seq,
            staging_clean,
        }
    }

    /// Stage a primary-key upsert **off-lock** for a transaction: validation +
    /// encode run without `write_lock`; the guard is acquired and `token`
    /// re-checked at [`CayenneStagedUpsert::commit`]. The caller must have
    /// captured `token` via [`Self::transaction_write_token`] before the
    /// transaction's gate read.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is partitioned (unsupported for the MVP) or
    /// if writing the staged data fails.
    pub async fn begin_staged_upsert_occ(
        &self,
        token: TransactionWriteToken,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<CayenneStagedUpsert> {
        let staged = self.stage_upsert_data(data, target_partitions).await?;
        Ok(CayenneStagedUpsert::new(
            self.clone_for_write(),
            token,
            staged,
        ))
    }

    /// Validate the incoming rows for PK conflicts (off-lock: private keyset, no
    /// shared-cache take/store) and encode them into a fresh, unreferenced
    /// snapshot directory.
    async fn stage_upsert_data(
        &self,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<StagedAppend> {
        // Partitioned tables publish across partitions; their visibility flip
        // cannot be a single protected-snapshot publish. Out of scope for the MVP.
        if self.metadata().partition_column.is_some() {
            return Err(Error::Unsupported {
                operation: "staged upsert for partitioned Cayenne tables",
            });
        }

        if let Some(resolver) = self.key_resolver()? {
            return self
                .stage_resolved_append(
                    data,
                    resolver,
                    super::overwrite::WriteShape {
                        target_size_bytes: self.target_file_size_bytes(),
                        target_partitions,
                        write_policy: WritePolicy::DELTA,
                    },
                    ValidationScope::Optimistic,
                )
                .await;
        }

        let prepared = self.prepare_stream_for_insert_offlock(data).await?;
        let post_validation = prepared.post_validation();

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let target_size_bytes = self.target_file_size_bytes();

        let (row_count, _writer_ops, stats) = match self
            .write_to_snapshot(
                prepared.stream,
                target_size_bytes,
                &new_snapshot_id,
                target_partitions,
                // Unknown size (the validation stream is consumed lazily); shard
                // across the full write concurrency, matching `begin_staged_append`.
                None,
                WritePolicy::DELTA,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                cleanup_orphan_snapshot_dir(self, &new_snapshot_id).await;
                return Err(e);
            }
        };

        // On-conflict deletions are computed by the validation stream as it is
        // consumed by `write_to_snapshot` above, so take them only now. A table
        // without a primary key (or with conflict detection disabled) yields the
        // default (empty) state — a plain insert published as a protected snapshot.
        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = post_validation.lock().take().unwrap_or_default();
        let superseded = on_conflict_deletions.total_superseded();

        Ok(StagedAppend {
            new_snapshot_id,
            on_conflict_deletions,
            validated_keys,
            stats,
            row_count,
            superseded,
        })
    }
}

/// Best-effort removal of an unreferenced staged snapshot directory.
///
/// The catalog never referenced the directory (no `cayenne_snapshot_sequence`
/// row, `current_snapshot_id` unchanged), so leaving it is safe; object stores
/// (S3) have no atomic "remove dir" and are left to the next successful
/// snapshot-cleanup cycle, mirroring [`super::overwrite::PreparedOverwrite::rollback`].
pub(super) async fn cleanup_orphan_snapshot_dir(table: &CayenneTableProvider, snapshot_id: &str) {
    if table.table_path().starts_with("s3://") {
        return;
    }
    let snapshot_dir = table.snapshot_dir_path_for(snapshot_id);
    match tokio::fs::remove_dir_all(&snapshot_dir).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(
                "Failed to clean up staged snapshot dir {} for table {}: {e}",
                snapshot_dir.display(),
                table.table_name()
            );
        }
    }
}
