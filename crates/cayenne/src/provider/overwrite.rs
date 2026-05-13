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

//! Overwrite-mode two-phase commit lifecycle for `CayenneTableProvider`.
//!
//! For issue #10125 (synchronize Cayenne partition commits across partitions),
//! the per-partition write path needs to be decomposable so that a coordinator
//! can stage every participating partition independently and then commit them
//! all atomically by batching their catalog mutations into one
//! `MetastoreTransaction`.
//!
//! [`PreparedOverwrite`] is the overwrite-side counterpart to
//! [`super::staging_wal::PreparedStagedAppend`]:
//!
//! - [`CayenneTableProvider::begin_overwrite`] writes the new snapshot's data
//!   to disk in a fresh snapshot directory and returns a `PreparedOverwrite`
//!   handle. The catalog is untouched.
//! - [`PreparedOverwrite::apply_in_txn`] flips the catalog
//!   `current_snapshot_id` pointer (and clears delete files, insert records,
//!   snapshot sequences) inside the caller's [`MetastoreTransaction`].
//! - [`PreparedOverwrite::finish`] updates the table's in-memory state to
//!   match the new catalog state and triggers old-snapshot GC.
//! - [`PreparedOverwrite::rollback`] discards the staged snapshot directory.
//!
//! The legacy one-shot overwrite (`CayenneDataSink::write_all_overwrite`) is
//! reimplemented in terms of this lifecycle by opening its own
//! single-partition transaction.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::OwnedMutexGuard;

use super::Result;
use super::table::{CayenneTableProvider, ColumnStatsAccumulator};
use crate::CayenneCatalog;
use crate::catalog::CatalogResult;
use crate::metastore::MetastoreTransaction;

/// A prepared overwrite: data has been written to a new snapshot directory
/// but the catalog still points at the old snapshot.
///
/// See the module-level documentation for the full lifecycle. The handle owns
/// the table's write guard, so concurrent writers on the same table block
/// until this handle is either committed via
/// [`Self::apply_in_txn`] + [`Self::finish`] or dropped/rolled back.
pub struct PreparedOverwrite {
    table: CayenneTableProvider,
    write_guard: Option<OwnedMutexGuard<()>>,
    new_snapshot_id: String,
    row_count: u64,
    write_stats_acc: Arc<ColumnStatsAccumulator>,
}

impl std::fmt::Debug for PreparedOverwrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedOverwrite")
            .field("table", &self.table.table_name())
            .field("new_snapshot_id", &self.new_snapshot_id)
            .field("row_count", &self.row_count)
            .field("has_write_guard", &self.write_guard.is_some())
            .finish()
    }
}

impl PreparedOverwrite {
    /// Number of rows that have been written into the new snapshot.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The new snapshot id that [`Self::apply_in_txn`] will publish.
    #[must_use]
    pub fn new_snapshot_id(&self) -> &str {
        &self.new_snapshot_id
    }

    /// The table id whose catalog pointer this overwrite will flip.
    #[must_use]
    pub fn table_id(&self) -> &str {
        self.table.table_id()
    }

    /// Apply the catalog mutation for this overwrite inside the caller's
    /// transaction.
    ///
    /// Executes the same SQL batch as
    /// [`crate::MetadataCatalog::commit_compaction`] (delete files cleared,
    /// insert records cleared, snapshot sequences cleared, snapshot pointer
    /// updated) but against `txn` instead of opening a new transaction.
    ///
    /// The caller owns the transaction lifecycle: this method does not
    /// commit, roll back, or retry. Cross-partition coordinators batch every
    /// participating partition's `apply_in_txn` inside one shared transaction
    /// so the pointer flips happen atomically.
    ///
    /// Single-partition callers can use [`Self::apply_owned_txn`] instead,
    /// which goes through the trait-based [`crate::MetadataCatalog::commit_compaction`]
    /// (own transaction, retry-on-conflict, no concrete-catalog dependency).
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL batch fails against the borrowed
    /// transaction. UUID validation errors are surfaced as
    /// `CatalogError::InvalidOperationNoSource`.
    pub async fn apply_in_txn(
        &self,
        catalog: &CayenneCatalog,
        txn: &mut dyn MetastoreTransaction,
    ) -> CatalogResult<()> {
        catalog
            .commit_compaction_in_txn(txn, self.table_id(), &self.new_snapshot_id)
            .await
    }

    /// Apply the catalog mutation by opening a dedicated single-partition
    /// transaction.
    ///
    /// Convenience for callers that don't need to batch with other partitions
    /// (e.g. [`super::sink::CayenneDataSink::write_all`] in overwrite mode).
    /// Delegates to [`crate::MetadataCatalog::commit_compaction`] which opens
    /// its own transaction with retry-on-conflict. The retry semantics match
    /// the pre-issue-#10125 behavior exactly.
    ///
    /// # Errors
    ///
    /// Returns any error surfaced by the catalog's `commit_compaction`.
    pub async fn apply_owned_txn(&self) -> CatalogResult<()> {
        self.table
            .catalog()
            .commit_compaction(self.table_id(), &self.new_snapshot_id)
            .await
    }

    /// Publish the new snapshot in memory after the caller's transaction has
    /// committed.
    ///
    /// Performs the bookkeeping that `CayenneDataSink::write_all_overwrite`
    /// did inline before this lifecycle existed:
    ///
    /// - Update the in-memory `current_snapshot_id` to match the catalog.
    /// - Clear all deletion caches (the new snapshot has no pending deletions).
    /// - Atomically swap the in-memory `ListingTable` to the new snapshot
    ///   (under [`CayenneTableProvider::listing_fence`] write — §6.4).
    /// - Trigger background cleanup of old snapshot directories.
    /// - Clear inlined data, inlined deletes, and table-level statistics that
    ///   were tied to the old snapshot.
    /// - Persist the new statistics accumulator.
    ///
    /// Failures inside the bookkeeping steps are logged as warnings; the
    /// visibility flip itself has already been observed by readers via the
    /// catalog pointer, so the return value reflects success of the whole
    /// commit.
    ///
    /// # Errors
    ///
    /// Returns an error if updating the in-memory snapshot id or swapping the
    /// listing table fails. Other steps are best-effort.
    pub async fn finish(self) -> Result<u64> {
        self.table.update_current_snapshot_id(&self.new_snapshot_id)?;

        if let Err(e) = self.table.clear_all_deletion_caches() {
            tracing::warn!(
                "Failed to clear deletion caches after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }

        self.table
            .update_listing_table_for_snapshot(&self.new_snapshot_id)
            .await?;

        self.table
            .trigger_old_snapshot_cleanup(&self.new_snapshot_id)
            .await;

        if let Err(e) = self
            .table
            .catalog()
            .clear_inlined_data(self.table.table_id())
            .await
        {
            tracing::warn!(
                "Failed to clear inlined data after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }
        if let Err(e) = self
            .table
            .catalog()
            .clear_inlined_deletes(self.table.table_id())
            .await
        {
            tracing::warn!(
                "Failed to clear inlined deletes after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }
        // Clear the prior statistics row before upserting so a zero-row
        // overwrite leaves no stats at all (rather than stale stats that
        // describe rows the overwrite just deleted). `persist_table_stats`
        // is a no-op when the accumulator is empty, so the clear is what
        // actually removes the stale row in that case.
        if let Err(e) = self
            .table
            .catalog()
            .clear_table_statistics(self.table.table_id())
            .await
        {
            tracing::warn!(
                "Failed to clear table statistics after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }
        self.table.persist_table_stats(&self.write_stats_acc).await;

        // Drop the write guard last so all visibility-related updates happen
        // under exclusive table access.
        let _ = self.write_guard;
        Ok(self.row_count)
    }

    /// Discard a prepared overwrite that has not been committed.
    ///
    /// Removes the new snapshot directory on disk (best-effort). The catalog
    /// is unaffected: either `apply_in_txn` was never called, or the caller's
    /// `MetastoreTransaction` was rolled back without committing.
    ///
    /// # Errors
    ///
    /// Currently returns `Ok` even if the directory removal fails (logged as
    /// a warning) — the orphan directory will be cleaned by the next
    /// `trigger_old_snapshot_cleanup` cycle on a successful overwrite.
    pub async fn rollback(self) -> Result<()> {
        let _ = self.write_guard;

        // Best-effort cleanup of the new snapshot directory. Object stores
        // (S3) don't have a single "remove dir" call; we leave object-store
        // cleanup to `trigger_old_snapshot_cleanup` on a subsequent
        // successful overwrite, which prunes any snapshot dir not referenced
        // by the catalog.
        let table_path = self.table.table_path();
        if !table_path.starts_with("s3://") {
            let new_snapshot_dir = self.table.snapshot_dir_path_for(&self.new_snapshot_id);
            match tokio::fs::remove_dir_all(&new_snapshot_dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to clean up new snapshot dir {} after overwrite rollback for table {}: {e}",
                        new_snapshot_dir.display(),
                        self.table.table_name()
                    );
                }
            }
        }
        Ok(())
    }
}

impl CayenneTableProvider {
    /// Stage an overwrite without committing it.
    ///
    /// Writes the input stream into a fresh `<table_id>/<new_snapshot>/`
    /// directory and returns a [`PreparedOverwrite`] handle. The catalog is
    /// untouched; the caller must complete the commit via
    /// [`PreparedOverwrite::apply_in_txn`] (inside a transaction the caller
    /// owns) + [`PreparedOverwrite::finish`], or abandon it via
    /// [`PreparedOverwrite::rollback`].
    ///
    /// Acquires the per-table write lock for the duration of the prepared
    /// state, so concurrent writes (inserts, overwrites, sort rewrites) block
    /// until this handle is finished or rolled back.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot directory cannot be created, the
    /// stream write fails, or directory sync fails.
    pub async fn begin_overwrite(
        &self,
        data: SendableRecordBatchStream,
    ) -> Result<PreparedOverwrite> {
        let write_guard = self.write_lock_arc().lock_owned().await;

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let is_s3 = self.table_path().starts_with("s3://");

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        let target_size_bytes = self.target_file_size_bytes();
        let (row_count, _files_written, write_stats_acc) = self
            .write_to_snapshot(data, target_size_bytes, &new_snapshot_id)
            .await?;

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::sync_snapshot_dir(&snapshot_dir).await?;
        }

        Ok(PreparedOverwrite {
            table: self.clone_for_write(),
            write_guard: Some(write_guard),
            new_snapshot_id,
            row_count,
            write_stats_acc,
        })
    }
}
