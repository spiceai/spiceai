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

//! Staged **upsert** lifecycle for `CayenneTableProvider` (Analytical Replica
//! dual-apply).
//!
//! [`super::staging_wal::CayenneStagedAppend`] stages append-only writes; it
//! rejects primary-key / on-conflict tables. This module adds the PK-aware
//! counterpart: a write into a table with a primary key and `on_conflict`
//! upsert is staged **invisibly** and published (or discarded) at a later
//! moment — the point at which the dual-apply driver has an ack (or a failure)
//! from the federated source.
//!
//! It is a split of the synchronous on-conflict path
//! ([`CayenneTableProvider::write_new_snapshot_after_validation`]) at the
//! listing fence:
//!
//! - [`CayenneTableProvider::begin_staged_upsert`] validates the incoming rows
//!   for PK conflicts and writes them into a fresh, **unreferenced** snapshot
//!   directory (`{table_id}/{new_snapshot}/`). Nothing durable in the catalog
//!   is touched, so the rows are invisible to every reader and there is nothing
//!   to compensate on abort. The captured [`OnConflictDeletions`] (which prior
//!   versions this write supersedes) rides the returned handle.
//! - [`CayenneStagedUpsert::commit`] runs the fenced publish: apply the
//!   on-conflict deletions, reserve the protected-snapshot sequence (strictly
//!   above the delete sequence, so the staged rows survive their own conflict
//!   deletes), durably record it, then flip the deletion caches and the
//!   protected snapshot in one listing-fence write. After this the rows are
//!   visible.
//! - [`CayenneStagedUpsert::rollback`] discards the staged snapshot directory.
//!
//! Sequence stamping is deferred by construction: the sync path already
//! reserves every sequence at publish time, and the protected snapshot's
//! deletion threshold is its own (highest) sequence, so a commit that lands
//! between stage and publish never mis-orders the staged rows.
//!
//! The stage is deliberately **not** durable: the federated source is the
//! system of record and CDC (`refresh_mode: changes`) is the crash backstop, so
//! a crash before commit simply drops the staged directory (recovered, if the
//! source committed, by CDC replay).

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::OwnedMutexGuard;

use super::Error;
use super::Result;
use super::column_stats::ColumnStatsAccumulator;
use super::delta_encoding::WriteClass;
use super::on_conflict::{OnConflictDeletions, PostValidationState};
use super::pk_index::PkDigestSet;
use super::table::CayenneTableProvider;

/// A staged upsert: the replacement rows have been written to a fresh snapshot
/// directory, but no catalog visibility change has been made yet.
///
/// The handle owns the table's write guard, so concurrent writers on the same
/// table block until it is either committed via [`Self::commit`] or discarded
/// via [`Self::rollback`] (or dropped — see the note on `Drop`).
pub struct CayenneStagedUpsert {
    table: CayenneTableProvider,
    write_guard: Option<OwnedMutexGuard<()>>,
    new_snapshot_id: String,
    /// The prior versions this upsert supersedes, captured at validation time
    /// and applied under the fence at commit. Taken (`mem::take`) on commit.
    on_conflict_deletions: OnConflictDeletions,
    validated_keys: PkDigestSet,
    stats: Arc<ColumnStatsAccumulator>,
    row_count: u64,
    /// Existing rows replaced by this upsert (for the live-row-count delta),
    /// captured before `on_conflict_deletions` is consumed.
    superseded: usize,
}

impl std::fmt::Debug for CayenneStagedUpsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneStagedUpsert")
            .field("table", &self.table.table_name())
            .field("new_snapshot_id", &self.new_snapshot_id)
            .field("row_count", &self.row_count)
            .field("superseded", &self.superseded)
            .field("has_write_guard", &self.write_guard.is_some())
            .finish_non_exhaustive()
    }
}

impl CayenneStagedUpsert {
    /// Number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Publish the staged rows, making the upsert visible to readers.
    ///
    /// Mirrors the fenced tail of
    /// [`CayenneTableProvider::write_new_snapshot_after_validation`]: apply the
    /// on-conflict deletions, reserve the protected-snapshot sequence, record it
    /// durably, then flip the deletion caches + protected snapshot under one
    /// listing-fence write.
    ///
    /// # Errors
    ///
    /// Returns an error if applying the deletions, reserving the sequence, or the
    /// durable snapshot-sequence write fails. The staged snapshot directory is
    /// left in place on error so the caller can retry the commit or roll back.
    pub async fn commit(mut self) -> Result<u64> {
        // visibility lock then listing fence — the same order as
        // `apply_under_barrier` and the sync on-conflict publish.
        let _visibility = self.table.visibility_lock_arc().lock_owned().await;
        let _fence = self.table.lock_listing_fence_write_owned().await;

        let on_conflict_deletions = std::mem::take(&mut self.on_conflict_deletions);
        let update = self.table.apply_on_conflict_deletions(on_conflict_deletions).await?;

        // Reserve the snapshot sequence AFTER `apply_on_conflict_deletions` (which
        // reserves the lower delete/insert sequences), so the protected snapshot's
        // deletion threshold is strictly above them and the staged rows survive
        // their own conflict deletes.
        let new_sequence = self.table.reserve_sequences_local(1).await?;
        self.table
            .record_written_snapshot_sequence(&self.new_snapshot_id, new_sequence)
            .await?;
        // Atomically publish the deletion-cache update and the protected snapshot
        // so a concurrent scan never sees the new rows without the deletes that
        // hide their prior versions.
        self.table
            .commit_on_conflict_publish(update, Some((&self.new_snapshot_id, new_sequence)))
            .await;

        let retention_requested = self.table.has_retention_delete_filters();
        let live_rows_delta = i64::try_from(self.row_count)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(self.superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(Arc::clone(&self.stats)),
            // The publish above already refreshed listing visibility; only stats
            // / retention / row-count maintenance is scheduled here.
            false,
            retention_requested,
            live_rows_delta,
        );
        if retention_requested {
            self.table.clear_cached_pk_keyset();
        } else {
            self.table.record_file_pk_keys(&self.validated_keys);
        }

        Ok(self.row_count)
        // `_fence`, `_visibility`, and `self.write_guard` drop here, in that order.
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
        // Hold the write guard across cleanup so another writer cannot start a
        // commit while the staged directory is mid-deletion.
        let _write_guard = self.write_guard;
        cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
        Ok(())
    }
}

impl CayenneTableProvider {
    /// Stage a primary-key upsert without making the new rows visible.
    ///
    /// Validates the incoming stream for PK conflicts and writes the rows into a
    /// fresh snapshot directory, returning a [`CayenneStagedUpsert`] that the
    /// caller commits (on federated-source ack) or rolls back (on source
    /// failure). See the module documentation for the full lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error if the table has an incomplete write, is partitioned
    /// (unsupported for the staged-upsert MVP), or if writing the staged data
    /// fails.
    pub async fn begin_staged_upsert(
        &self,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<CayenneStagedUpsert> {
        let write_guard = self.write_lock_arc().lock_owned().await;
        self.ensure_no_incomplete_write().await?;

        // Partitioned tables publish across partitions; their visibility flip
        // cannot be a single protected-snapshot publish. Out of scope for the MVP
        // (§10 conditional Phase 3).
        if self.metadata().partition_column.is_some() {
            return Err(Error::Unsupported {
                operation: "staged upsert for partitioned Cayenne tables",
            });
        }

        let prepared = self.prepare_stream_for_insert(data).await?;
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
                WriteClass::Delta,
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
        // default (empty) state — a plain insert published as a protected
        // snapshot.
        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = post_validation.lock().take().unwrap_or_default();
        let superseded = on_conflict_deletions.total_superseded();

        Ok(CayenneStagedUpsert {
            table: self.clone_for_write(),
            write_guard: Some(write_guard),
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
async fn cleanup_orphan_snapshot_dir(table: &CayenneTableProvider, snapshot_id: &str) {
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
