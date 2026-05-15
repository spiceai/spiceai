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

//! Staging Write-Ahead Log (WAL) for crash-safe staged appends.
//!
//! When a Cayenne table performs a staged append, data files are first written
//! to a `_staging/` directory and then moved to the active snapshot. The move
//! is **not** atomic as a batch (individual renames are atomic on local FS, but
//! the loop over files is not).
//!
//! The staging WAL bridges this gap:
//!
//! 1. After all data files are written to `_staging/`, a `_wal.json` file is
//!    created that records which files need to be moved and to which snapshot.
//! 2. The files are then moved to the snapshot directory.
//! 3. On success, the WAL file is removed.
//!
//! If the process crashes during step 2, the WAL file survives and is detected
//! on the next table open or write attempt, alerting the operator to a potentially inconsistent
//! state.
//!
//! # Two-phase commit lifecycle
//!
//! For cross-partition coordination (issue #10125), the staged-append surface is
//! split into a three-phase lifecycle:
//!
//! - [`CayenneStagedAppend::prepare`] writes the staging WAL and returns a
//!   [`PreparedStagedAppend`] receipt.
//! - [`PreparedStagedAppend::apply_under_barrier`] performs the file move,
//!   WAL removal, and listing-table refresh that make the staged rows visible.
//!   For single-partition use the receipt acquires the listing-table write lock
//!   itself; the cross-partition coordinator (future work) will own the lock
//!   externally.
//! - [`PreparedStagedAppend::apply_in_txn`] is reserved for the overwrite path,
//!   where the visibility flip is a catalog pointer mutation inside a shared
//!   [`crate::metastore::MetastoreTransaction`]. For the append lifecycle it is
//!   a no-op.
//! - [`PreparedStagedAppend::finish`] releases the write guard and returns the
//!   row count.
//!
//! The legacy one-shot [`CayenneStagedAppend::commit`] is reimplemented in terms
//! of this lifecycle and remains observably identical to the previous behavior.

use super::Result;
use super::constants::{STAGING_DIR_NAME, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME};
use super::table::CayenneTableProvider;
use crate::metastore::MetastoreTransaction;
use crate::provider::Error;
use datafusion::execution::SendableRecordBatchStream;
use futures::TryStreamExt;
use object_store::path::Path as ObjectStorePath;
use tokio::sync::OwnedMutexGuard;

/// Coordinates staged writes and the staging WAL lifecycle for a Cayenne table.
///
/// This struct supports two usage patterns:
/// - Granular orchestration via `write_wal`, `move_staged_files`,
///   `remove_wal`, and `refresh_listing_table`
/// - One-shot orchestration via `finalize_staged_write`
/// - Two-phase commit via [`CayenneStagedAppend::prepare`] →
///   [`PreparedStagedAppend::apply_under_barrier`] → [`PreparedStagedAppend::finish`].
///   The two-phase API is what the cross-partition coordinator uses; for single-partition
///   callers it is equivalent to [`CayenneStagedAppend::commit`].
///
/// `begin_staged_append` returns this handle after writing data into `_staging/`
/// so external consumers can synchronize writes and call `commit` only when ready.
pub struct CayenneStagedAppend {
    table: CayenneTableProvider,
    write_guard: Option<OwnedMutexGuard<()>>,
    row_count: u64,
}

impl std::fmt::Debug for CayenneStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneStagedAppend")
            .field("table", &self.table.table_name())
            .field("has_write_guard", &self.write_guard.is_some())
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl CayenneStagedAppend {
    pub(crate) fn from_staged_append(
        table: CayenneTableProvider,
        write_guard: OwnedMutexGuard<()>,
        row_count: u64,
    ) -> Self {
        Self {
            table,
            write_guard: Some(write_guard),
            row_count,
        }
    }

    pub(crate) fn from_existing_staging(table: CayenneTableProvider) -> Self {
        Self {
            table,
            write_guard: None,
            row_count: 0,
        }
    }

    /// Returns the number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Writes the staging WAL for the current `_staging/` files.
    ///
    /// # Errors
    ///
    /// Returns an error if writing the WAL file fails.
    pub async fn write_wal(&self) -> Result<()> {
        self.table.write_staging_wal().await
    }

    /// Moves staged files into the current snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if moving the staged files fails.
    pub async fn move_staged_files(&self) -> Result<()> {
        self.table.move_files_to_current_snapshot().await
    }

    /// Removes the staging WAL after a successful move.
    ///
    /// # Errors
    ///
    /// Returns an error if removing the WAL file fails.
    pub async fn remove_wal(&self) -> Result<()> {
        self.table.remove_staging_wal().await
    }

    /// Refreshes the listing table so newly committed files become visible.
    ///
    /// # Errors
    ///
    /// Returns an error if refreshing the listing table fails.
    pub async fn refresh_listing_table(&self) -> Result<()> {
        self.table.refresh_listing_table().await
    }

    /// Executes the full WAL finalize sequence in order.
    ///
    /// # Errors
    ///
    /// Returns an error if any step in the finalize sequence (write WAL, move files,
    /// remove WAL, or refresh listing table) fails.
    pub async fn finalize_staged_write(&self) -> Result<()> {
        self.write_wal().await?;
        self.move_staged_files().await?;
        self.remove_wal().await?;
        self.refresh_listing_table().await?;
        Ok(())
    }

    /// Commits the staged append, making the new rows visible to readers.
    ///
    /// Equivalent to [`Self::prepare`] → [`PreparedStagedAppend::apply_under_barrier`]
    /// → [`PreparedStagedAppend::finish`], run sequentially. Single-partition
    /// callers should use this; the cross-partition coordinator drives the
    /// three phases explicitly so it can hold a cross-partition barrier across
    /// every prepared partition's `apply_under_barrier` call.
    ///
    /// # Errors
    ///
    /// Returns an error if the finalize sequence fails.
    pub async fn commit(self) -> Result<u64> {
        let prepared = self.prepare().await?;
        prepared.apply_under_barrier().await?;
        prepared.finish().await
    }

    /// Prepare the staged append for commit.
    ///
    /// Writes the staging WAL — a durable record of the intent to move the
    /// already-staged files into the current snapshot directory. After this
    /// returns, the caller owns the lifecycle: it must either complete the
    /// commit via [`PreparedStagedAppend::apply_under_barrier`] (and then
    /// [`PreparedStagedAppend::finish`]) or [`PreparedStagedAppend::rollback`]
    /// the receipt. Dropping the receipt without finishing leaves the WAL on
    /// disk and will block subsequent writes via
    /// [`CayenneTableProvider::ensure_no_incomplete_write`].
    ///
    /// # Errors
    ///
    /// Returns an error if writing the staging WAL fails.
    pub async fn prepare(self) -> Result<PreparedStagedAppend> {
        self.table.write_staging_wal().await?;
        Ok(PreparedStagedAppend {
            table: self.table,
            write_guard: self.write_guard,
            row_count: self.row_count,
        })
    }

    /// Discards the staged append and removes any staged files.
    ///
    /// # Errors
    ///
    /// Returns an error if clearing the staging directory fails.
    pub async fn rollback(self) -> Result<()> {
        // Hold the per-table write guard until after the staging directory is
        // cleared. Dropping the guard first would let another writer acquire
        // the lock mid-cleanup and transiently observe an `IncompleteWrite`
        // or leftover WAL.
        let _write_guard = self.write_guard;
        self.table.clear_staging_dir().await
        // _write_guard drops here, after cleanup completes.
    }
}

/// A staged append that has been [prepared](CayenneStagedAppend::prepare) for
/// commit.
///
/// Holds the staging WAL on disk and the per-table write guard. Completing the
/// commit is a two-step dance:
///
/// 1. [`Self::apply_under_barrier`] (append path) or [`Self::apply_in_txn`]
///    (overwrite path, future work) performs the visibility flip.
/// 2. [`Self::finish`] releases the guard and returns the row count.
///
/// Dropping a `PreparedStagedAppend` without calling `finish` or `rollback`
/// leaves the staging WAL on disk; the next write attempt will fail at
/// [`CayenneTableProvider::ensure_no_incomplete_write`].
pub struct PreparedStagedAppend {
    table: CayenneTableProvider,
    write_guard: Option<OwnedMutexGuard<()>>,
    row_count: u64,
}

impl std::fmt::Debug for PreparedStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStagedAppend")
            .field("table", &self.table.table_name())
            .field("has_write_guard", &self.write_guard.is_some())
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl PreparedStagedAppend {
    /// Returns the number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Apply the staged write under the caller's append-side barrier.
    ///
    /// Performs, in order: move staged files into the current snapshot
    /// directory; remove the staging WAL; refresh the in-memory listing table.
    /// The WAL is removed *before* the listing-table refresh to preserve the
    /// existing crash-safety invariant ("WAL absent ⇒ files moved
    /// successfully"); a crash between WAL removal and listing refresh leaves
    /// the data on disk and is self-healing on the next listing refresh
    /// trigger.
    ///
    /// Single-partition path. Acquires this partition's `listing_fence` for
    /// write internally. The cross-partition append coordinator (#10125 step
    /// 6) uses [`Self::apply_under_held_barrier`] instead so it can hold the
    /// fences on every participating partition for one shared barrier window.
    ///
    /// # Errors
    ///
    /// Returns an error if moving the staged files, removing the WAL, or
    /// refreshing the listing table fails.
    pub async fn apply_under_barrier(&self) -> Result<()> {
        // Hold the listing fence for the entire move + WAL removal + listing
        // swap sequence. Without this, `CayenneTableProvider::scan()` (which
        // holds `listing_fence.read()` across DataFusion's listing call) can
        // interleave with the move and observe a torn directory snapshot.
        let _fence = self.table.lock_listing_fence_write_owned().await;
        self.table.move_files_to_current_snapshot().await?;
        self.table.remove_staging_wal().await?;
        self.table.refresh_listing_table_under_held_fence()?;
        Ok(())
    }

    /// Apply the staged write ASSUMING the caller already holds this
    /// partition's `listing_fence` for write.
    ///
    /// Same observable effect as [`Self::apply_under_barrier`] but skips the
    /// internal fence acquisition. Used by the cross-partition append
    /// coordinator (#10125 step 6), which locks fences on every participating
    /// partition (sorted to keep concurrent coordinators deadlock-free) for
    /// the duration of one barrier window, calls this method on each, and
    /// releases the fences together. Readers going through `scan()` either
    /// see the pre-barrier state on every partition or the post-barrier
    /// state on every partition.
    ///
    /// # Errors
    ///
    /// Returns an error if moving the staged files, removing the WAL, or
    /// reconstructing the listing table fails.
    pub async fn apply_under_held_barrier(&self) -> Result<()> {
        self.table.move_files_to_current_snapshot().await?;
        self.table.remove_staging_wal().await?;
        self.table.refresh_listing_table_under_held_fence()?;
        Ok(())
    }

    /// Returns the partition's catalog `table_id`. Used by the cross-partition
    /// coordinator to populate the top-level WAL.
    #[must_use]
    pub fn table_id(&self) -> &str {
        self.table.table_id()
    }

    /// Returns this partition's absolute staging-WAL path, used by the
    /// cross-partition coordinator to record what the top-level WAL refers
    /// to.
    #[must_use]
    pub fn staging_wal_path(&self) -> std::path::PathBuf {
        self.table.staging_wal_path_for_recovery()
    }

    /// Acquire this partition's listing fence for write, returning an owned
    /// guard the coordinator holds across the cross-partition barrier.
    pub async fn lock_listing_fence_write_owned(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.table.lock_listing_fence_write_owned().await
    }

    /// Apply the staged write inside the caller's metastore transaction.
    ///
    /// Reserved for the cross-partition overwrite path (future work for issue
    /// #10125), where the visibility flip is a catalog pointer mutation that
    /// must be batched with other partitions' mutations inside a shared
    /// [`MetastoreTransaction`]. The append-mode lifecycle has no catalog
    /// mutation, so this is a no-op today; it is exposed now to fix the API
    /// shape that the coordinator will consume.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog mutation fails. The append-mode
    /// implementation never returns an error.
    #[expect(
        clippy::unused_async,
        reason = "API symmetry / forward-compat — see body"
    )]
    pub async fn apply_in_txn(&self, _txn: &mut dyn MetastoreTransaction) -> Result<()> {
        // Async kept for API symmetry with `PreparedOverwrite::apply_in_txn`
        // and for forward-compat: any future append-mode catalog mutation
        // would live here and need `.await`.
        Ok(())
    }

    /// Finish a prepared append after [`Self::apply_under_barrier`] or
    /// [`Self::apply_in_txn`] has succeeded.
    ///
    /// Releases the per-table write guard and returns the row count. For the
    /// append path, all visibility work has already happened in
    /// `apply_under_barrier`; this is purely a typestate transition that makes
    /// the `Drop` of the write guard explicit.
    ///
    /// # Errors
    ///
    /// Currently infallible. Returns a `Result` for symmetry with future
    /// extensions (e.g. publishing per-partition statistics inside `finish`)
    /// and for forward-compatibility with the cross-partition coordinator.
    #[expect(
        clippy::unused_async,
        reason = "API symmetry / forward-compat — see body"
    )]
    pub async fn finish(self) -> Result<u64> {
        // Async kept so a future cross-partition coordinator can call
        // `prep.finish().await` uniformly without callers having to know
        // whether finish is sync or async for this mode.
        let _ = self.write_guard;
        Ok(self.row_count)
    }

    /// Discards the prepared append.
    ///
    /// Clears the staging directory (which removes the WAL along with the
    /// staged files). Use this when, after [`CayenneStagedAppend::prepare`]
    /// returns successfully, downstream coordination determines the commit
    /// must not proceed (e.g. another partition's prepare failed).
    ///
    /// # Errors
    ///
    /// Returns an error if clearing the staging directory fails.
    pub async fn rollback(self) -> Result<()> {
        // Same ordering rationale as `CayenneStagedAppend::rollback`: hold
        // the write guard until after the staging directory is cleared so
        // other writers can't transiently observe a leftover WAL between
        // guard release and cleanup.
        let _write_guard = self.write_guard;
        self.table.clear_staging_dir().await
        // _write_guard drops here.
    }
}

/// Staging WAL (Write-Ahead Log) entry.
///
/// Written to `_staging/_wal.json` after all data files are staged but before
/// the move-to-snapshot operation begins. Records the intent so that an
/// interrupted move can be detected on the next table open.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagingWal {
    /// The table this WAL entry belongs to.
    pub table_name: String,
    /// The snapshot directory the staged files should be moved to.
    pub target_snapshot: String,
    /// Names of the data files in the staging directory.
    pub staged_files: Vec<String>,
    /// ISO-8601 timestamp when this WAL entry was created.
    pub created_at: String,
}

impl CayenneTableProvider {
    /// Create a staging WAL handle for data already written to `_staging/`.
    pub(crate) fn staged_append_for_existing_staging(&self) -> CayenneStagedAppend {
        CayenneStagedAppend::from_existing_staging(self.clone_for_write())
    }

    /// Stage an append into Cayenne without making the new rows visible.
    ///
    /// This path supports append-only semantics and returns a handle that allows
    /// callers to commit or roll back once external coordination succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the table has an incomplete write, uses a non-position-based
    /// deletion strategy, has pending deletions, the incoming stream contains upsert or
    /// on-conflict writes, or if writing the staged data fails.
    pub async fn begin_staged_append(
        &self,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<CayenneStagedAppend> {
        let write_guard = self.write_lock_arc().lock_owned().await;

        self.ensure_no_incomplete_write().await?;

        if !self.pk_deletion_strategy().is_position_based() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne tables with primary-key deletion handling",
            });
        }

        if self.has_pending_deletions() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne tables with pending deletions",
            });
        }

        let prepared_insert = self.prepare_stream_for_insert(data).await?;

        if !prepared_insert.on_conflict_deletions.is_empty() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne upsert or on-conflict writes",
            });
        }

        self.clear_staging_dir().await?;

        let (row_count, _writer_ops, _stats_acc) = self
            .write_to_snapshot(
                prepared_insert.stream,
                self.target_file_size_bytes(),
                STAGING_DIR_NAME,
                target_partitions,
            )
            .await?;

        Ok(CayenneStagedAppend::from_staged_append(
            self.clone_for_write(),
            write_guard,
            row_count,
        ))
    }

    /// Write the staging WAL file that records the pending move operation.
    ///
    /// The WAL is written after all data files have been staged but before any
    /// files are moved to the snapshot directory. It records the list of staged
    /// files and the target snapshot so that an interrupted move can be detected
    /// on the next table open.
    ///
    /// # Layout
    ///
    /// The WAL file is placed at `{table_path}/{table_id}/_staging/_wal.json`
    /// (local FS) or at the corresponding S3 key.
    pub(crate) async fn write_staging_wal(&self) -> Result<()> {
        let current_snapshot = self.get_current_snapshot_id()?;

        if self.table_path().starts_with("s3://") {
            self.write_staging_wal_s3(&current_snapshot).await
        } else {
            self.write_staging_wal_local(&current_snapshot).await
        }
    }

    /// Write the staging WAL on local filesystem.
    ///
    /// Crash-safe: writes to `_wal.json.tmp`, fsyncs the temp file, atomically
    /// renames to `_wal.json`, then fsyncs the parent (staging) directory so
    /// the rename itself is durable across power loss. Without the parent
    /// fsync, a power failure between `rename` and the next dirty-page
    /// writeback could leave the WAL file's inode on disk but unreachable via
    /// the directory, defeating the `ensure_no_incomplete_write` check.
    async fn write_staging_wal_local(&self, target_snapshot: &str) -> Result<()> {
        let staging_dir =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);

        // Collect staged file names (exclude the WAL file itself and any
        // leftover tmp file from a prior interrupted attempt).
        let mut staged_files = Vec::new();
        let mut entries = tokio::fs::read_dir(&staging_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name != STAGING_WAL_FILENAME && name != STAGING_WAL_TMP_FILENAME {
                    staged_files.push(name);
                }
            }
        }

        let wal = StagingWal {
            table_name: self.table_name().to_string(),
            target_snapshot: target_snapshot.to_string(),
            staged_files,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
        let tmp_path = staging_dir.join(STAGING_WAL_TMP_FILENAME);
        let content = serde_json::to_string_pretty(&wal).map_err(|e| Error::Internal {
            table: self.table_name().to_string(),
            message: format!("Failed to serialize staging WAL: {e}"),
        })?;

        // Step 1: write to tmp file + fsync the file contents.
        tokio::fs::write(&tmp_path, content.as_bytes()).await?;
        let tmp_file = tokio::fs::File::open(&tmp_path).await?;
        tmp_file.sync_all().await?;
        drop(tmp_file);

        // Step 2: atomic rename. POSIX rename within the same directory is
        // atomic and replaces any existing target. If the rename fails, do a
        // best-effort cleanup of the tmp file so we don't leave junk behind.
        if let Err(e) = tokio::fs::rename(&tmp_path, &wal_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(e.into());
        }

        // Step 3: fsync the parent dir so the rename is durable across a
        // power-loss restart. Without this, the WAL's directory entry can be
        // lost even though the file's data is on disk — and that's exactly
        // the case `ensure_no_incomplete_write` is designed to catch.
        let dir = tokio::fs::File::open(&staging_dir).await?;
        if let Err(e) = dir.sync_all().await {
            // Directory fsync is best-effort: on some filesystems / OSes it
            // is a no-op anyway. Log the failure but don't abort — the WAL
            // file itself is already fsync'd and renamed.
            tracing::warn!(
                "Failed to fsync staging WAL parent dir {}: {e}",
                staging_dir.display(),
            );
        }

        tracing::debug!(
            "Wrote staging WAL for table {} with {} file(s) targeting snapshot {target_snapshot}",
            self.table_name(),
            wal.staged_files.len(),
        );

        Ok(())
    }

    /// Write the staging WAL on S3.
    ///
    /// Devil's advocate (S3 side of the uniform durability contract):
    /// On local FS we now write to `_wal.json.tmp`, fsync, atomic rename to
    /// `_wal.json`, then fsync the parent dir. This guarantees a reader of
    /// the final key sees either a complete, fsynced document or nothing.
    ///
    /// The current S3 implementation does a direct `put` of the final key.
    /// While a single small-object `put` on S3 is atomic from the reader's
    /// perspective, there is no "tmp object" phase and no equivalent of the
    /// "parent directory fsync" that protects the directory entry.
    ///
    /// In practice this is acceptable because:
    /// - The content is a small JSON blob (low chance of partial write).
    /// - The data files the WAL references are uploaded *before* the WAL is
    ///   written; a crash before the WAL appears means the next writer will
    ///   simply not see a WAL and will clean the orphaned staging files.
    /// - `ensure_no_incomplete_write` currently returns `IncompleteWrite`
    ///   (manual recovery required); automated recovery is noted as future work.
    ///
    /// A future improvement (to reach full parity with the local-FS hardening)
    /// would be to write the JSON to a `_wal.json.tmp` object key first, then
    /// `put`/`copy` to the final key, and have readers explicitly ignore any
    /// `.tmp` WAL object. This would make a torn WAL JSON impossible to observe
    /// on S3 as well.
    async fn write_staging_wal_s3(&self, target_snapshot: &str) -> Result<()> {
        let config = self.require_object_store()?;

        let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? else {
            return Ok(());
        };

        // List staged files (exclude the WAL file itself).
        let objects: Vec<_> = config
            .store
            .list(Some(&staging_prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list staging objects for WAL",
                table: self.table_name().to_string(),
                source: e,
            })?;

        let staged_files: Vec<String> = objects
            .iter()
            .filter_map(|meta| {
                let name = meta
                    .location
                    .as_ref()
                    .strip_prefix(staging_prefix.as_ref())
                    .unwrap_or(meta.location.as_ref());
                if name == STAGING_WAL_FILENAME || name == STAGING_WAL_TMP_FILENAME {
                    None
                } else {
                    Some(name.to_string())
                }
            })
            .collect();

        let wal = StagingWal {
            table_name: self.table_name().to_string(),
            target_snapshot: target_snapshot.to_string(),
            staged_files,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let content = serde_json::to_string_pretty(&wal).map_err(|e| Error::Internal {
            table: self.table_name().to_string(),
            message: format!("Failed to serialize staging WAL: {e}"),
        })?;

        // Write to a temporary object first, then to the final key.
        // This mirrors the local-FS tmp+rename pattern and guarantees that
        // any reader looking for STAGING_WAL_FILENAME sees either a complete,
        // previously-written document or nothing at all (never a torn/partial JSON).
        let tmp_key = ObjectStorePath::from(format!(
            "{}{STAGING_WAL_TMP_FILENAME}",
            staging_prefix.as_ref()
        ));
        let final_key =
            ObjectStorePath::from(format!("{}{STAGING_WAL_FILENAME}", staging_prefix.as_ref()));

        // Phase 1: write content to the tmp key (atomic for small objects on S3).
        config
            .store
            .put(&tmp_key, content.clone().into())
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "write staging WAL (tmp)",
                table: self.table_name().to_string(),
                source: e,
            })?;

        // Phase 2: atomically publish to the final key (another put; on S3 this
        // replaces any previous version atomically from the reader's perspective).
        config
            .store
            .put(&final_key, content.into())
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "write staging WAL (final)",
                table: self.table_name().to_string(),
                source: e,
            })?;

        // Best-effort cleanup of the tmp object. If this fails, the next writer
        // will overwrite it anyway, and readers only ever look for the final key.
        let _ = config.store.delete(&tmp_key).await;

        tracing::debug!(
            "Wrote staging WAL (S3) for table {} with {} file(s) targeting snapshot {target_snapshot}",
            self.table_name(),
            wal.staged_files.len(),
        );

        Ok(())
    }

    /// Remove the staging WAL file after a successful move.
    ///
    /// This signals that all staged files have been moved successfully. If this
    /// removal fails, the WAL is stale (files already moved) and will be detected
    /// as a false positive on next open — harmless but logged.
    ///
    /// On local filesystems the parent staging directory is fsync'd after a
    /// successful removal so the unlink is durable across power loss. Without
    /// this, a crash between `unlink` and the next dirty-page writeback could
    /// leave the WAL directory entry intact, causing `ensure_no_incomplete_write`
    /// to spuriously block writes after a clean commit.
    pub(crate) async fn remove_staging_wal(&self) -> Result<()> {
        if self.table_path().starts_with("s3://") {
            let config = self.require_object_store()?;
            if let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? {
                let final_key = ObjectStorePath::from(format!(
                    "{}{STAGING_WAL_FILENAME}",
                    staging_prefix.as_ref()
                ));
                let tmp_key = ObjectStorePath::from(format!(
                    "{}{STAGING_WAL_TMP_FILENAME}",
                    staging_prefix.as_ref()
                ));

                // Best-effort delete of the final WAL key — if it doesn't exist, that's fine.
                match config.store.delete(&final_key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        tracing::warn!(
                            "Failed to remove staging WAL (S3) for table {}: {e}",
                            self.table_name(),
                        );
                    }
                }

                // Also best-effort delete any stray tmp WAL object (e.g., left behind
                // after a crash during write_staging_wal_s3 or after automated recovery).
                // This keeps the staging prefix clean and prevents future recovery
                // logic or listing from seeing confusing tmp objects.
                match config.store.delete(&tmp_key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        tracing::warn!(
                            "Failed to remove staging WAL tmp object (S3) for table {}: {e}",
                            self.table_name(),
                        );
                    }
                }
            }
        } else {
            let staging_dir =
                Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);
            let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
            let mut removed = false;
            match tokio::fs::remove_file(&wal_path).await {
                Ok(()) => {
                    removed = true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to remove staging WAL for table {}: {e}",
                        self.table_name(),
                    );
                }
            }

            // Make the unlink durable so a later `ensure_no_incomplete_write`
            // doesn't spuriously see the WAL after a power-loss restart.
            // Best-effort: matches the partitioned_wal and write_staging_wal_local
            // patterns. If the staging dir no longer exists (a concurrent
            // clear_staging_dir removed it), skip silently.
            if removed
                && let Ok(dir) = tokio::fs::File::open(&staging_dir).await
                && let Err(e) = dir.sync_all().await
            {
                tracing::warn!(
                    "Failed to fsync staging dir {} after WAL removal for table {}: {e}",
                    staging_dir.display(),
                    self.table_name(),
                );
            }
        }
        Ok(())
    }

    /// Ensure no incomplete write is pending before starting a new write.
    ///
    /// Checks for a leftover staging WAL, which indicates a previous staged
    /// append was interrupted during the file-move phase. Returns an error to
    /// block further writes until the inconsistency is resolved.
    ///
    /// # Errors
    ///
    /// Returns [`Error::IncompleteWrite`] if a staging WAL file is found.
    pub(crate) async fn ensure_no_incomplete_write(&self) -> Result<()> {
        let wal = if self.table_path().starts_with("s3://") {
            self.read_staging_wal_s3().await
        } else {
            self.read_staging_wal_local().await
        };

        if let Some((wal, wal_location)) = wal {
            let table_name = self.table_name().to_string();

            // Audit: every file the WAL claims must be reachable — either
            // present in `_staging/` (so we can move it) or already present
            // in the target snapshot directory (so the previous commit's
            // move loop got that far before the crash). If any WAL-listed
            // file is missing from BOTH locations, automated recovery would
            // silently lose data, so refuse and require manual operator
            // intervention.
            //
            // This separates the benign "crash between rename and WAL
            // removal" (every file already in target snapshot, staging is
            // empty, recovery is just a WAL unlink) from "filesystem-level
            // corruption that lost staged files" (file in neither location).
            // Only the former should self-heal.
            //
            // Pre-recovery audit (local FS): every file the WAL claims must be
            // reachable — either present in `_staging/` or already in the target
            // snapshot. If any file is missing from both, refuse recovery to
            // avoid silent data loss.
            if !self.table_path().starts_with("s3://") && !wal.staged_files.is_empty() {
                let staging_dir = Self::snapshot_dir_path(
                    self.table_path(),
                    self.table_id(),
                    STAGING_DIR_NAME,
                );
                let target_dir = self.get_current_snapshot_id().ok().map(|snapshot_id| {
                    Self::snapshot_dir_path(self.table_path(), self.table_id(), &snapshot_id)
                });

                let mut missing_files: Vec<String> = Vec::new();
                for staged_file in &wal.staged_files {
                    let in_staging = tokio::fs::metadata(staging_dir.join(staged_file))
                        .await
                        .is_ok();
                    let in_target = match &target_dir {
                        Some(dir) => tokio::fs::metadata(dir.join(staged_file)).await.is_ok(),
                        None => false,
                    };
                    if !in_staging && !in_target {
                        missing_files.push(staged_file.clone());
                    }
                }

                if !missing_files.is_empty() {
                    tracing::error!(
                        table = table_name.as_str(),
                        wal_location = %wal_location,
                        missing_count = missing_files.len(),
                        total_files = wal.staged_files.len(),
                        "Incomplete staged append references files missing from both staging and target snapshot; refusing automated recovery"
                    );
                    let sample: Vec<&str> =
                        missing_files.iter().take(3).map(String::as_str).collect();
                    return Err(Error::IncompleteWrite {
                        table: table_name,
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery aborted because {} of those file(s) are missing from both '_staging/' and the target snapshot — e.g. {sample:?}. This indicates genuine data loss (filesystem corruption or external interference). Manual resolution is required. The WAL file is located at '{wal_location}'.",
                            wal.staged_files.len(),
                            wal.target_snapshot,
                            wal.created_at,
                            missing_files.len(),
                        ),
                    });
                }
            } else if self.table_path().starts_with("s3://") && !wal.staged_files.is_empty() {
                // Pre-recovery audit (S3): symmetric to the local-FS audit.
                // List the staging prefix and the target snapshot prefix (cheap
                // list operations). Every file listed in the WAL must appear in
                // at least one of those prefixes. If any file is missing from
                // both, refuse recovery to avoid promoting a snapshot that has
                // lost data (e.g., partial multipart upload that was never
                // completed or was cleaned up externally).
                let config = match self.require_object_store() {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };

                let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME).ok().flatten() else {
                    // Can't even determine staging prefix — refuse.
                    return Err(Error::IncompleteWrite {
                        table: table_name.clone(),
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}'. Could not determine S3 staging prefix for pre-recovery audit. Manual resolution required.",
                            wal.staged_files.len(),
                            wal.target_snapshot
                        ),
                    });
                };

                let target_prefix = self.get_current_snapshot_id().ok().and_then(|snapshot_id| {
                    self.snapshot_object_store_prefix(&snapshot_id).ok().flatten()
                });

                // Collect reachable filenames from staging and target (best-effort).
                let mut reachable: std::collections::HashSet<String> = std::collections::HashSet::new();

                // List staging
                if let Ok(objects) = config.store.list(Some(&staging_prefix)).try_collect::<Vec<_>>().await {
                    for meta in objects {
                        if let Some(rel) = meta.location.as_ref().strip_prefix(staging_prefix.as_ref()) {
                            if rel != STAGING_WAL_FILENAME && rel != STAGING_WAL_TMP_FILENAME {
                                reachable.insert(rel.to_string());
                            }
                        }
                    }
                }

                // List target (if known)
                if let Some(tp) = &target_prefix {
                    if let Ok(objects) = config.store.list(Some(tp)).try_collect::<Vec<_>>().await {
                        for meta in objects {
                            if let Some(rel) = meta.location.as_ref().strip_prefix(tp.as_ref()) {
                                reachable.insert(rel.to_string());
                            }
                        }
                    }
                }

                let mut missing_files: Vec<String> = Vec::new();
                for staged_file in &wal.staged_files {
                    if !reachable.contains(staged_file) {
                        missing_files.push(staged_file.clone());
                    }
                }

                if !missing_files.is_empty() {
                    tracing::error!(
                        table = table_name.as_str(),
                        wal_location = %wal_location,
                        missing_count = missing_files.len(),
                        total_files = wal.staged_files.len(),
                        "Incomplete staged append (S3) references files missing from both staging and target snapshot; refusing automated recovery"
                    );
                    let sample: Vec<&str> = missing_files.iter().take(3).map(String::as_str).collect();
                    return Err(Error::IncompleteWrite {
                        table: table_name,
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery aborted because {} of those file(s) are missing from both the staging prefix and the target snapshot on S3 — e.g. {sample:?}. This may indicate a partial multipart upload that was never completed or external interference. Manual resolution is required. The WAL file is located at '{wal_location}'.",
                            wal.staged_files.len(),
                            wal.target_snapshot,
                            wal.created_at,
                            missing_files.len(),
                        ),
                    });
                }
            }

            // Best-effort automated recovery:
            // Re-drive the move of any remaining files listed in the WAL from
            // staging into the target snapshot, then remove the WAL.
            // This turns most "IncompleteWrite" situations into self-healing
            // events instead of requiring manual operator intervention.
            //
            // The move logic is idempotent (files already in the target are
            // skipped or harmlessly re-copied on S3). If the target snapshot
            // no longer exists (very old WAL after many compactions) or the
            // move fails irrecoverably, we still return IncompleteWrite.
            tracing::warn!(
                table = table_name.as_str(),
                wal_location = %wal_location,
                target_snapshot = %wal.target_snapshot,
                staged_files = wal.staged_files.len(),
                "Incomplete staged append detected — attempting automated recovery"
            );

            match self.move_files_to_current_snapshot().await {
                Ok(()) => {
                    // Move succeeded (or was a no-op). Remove the WAL.
                    self.remove_staging_wal().await.ok();
                    tracing::info!(
                        table = table_name.as_str(),
                        "Automated recovery from incomplete write succeeded; table is now writable"
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        table = table_name.as_str(),
                        error = %e,
                        "Automated recovery from incomplete write failed — manual intervention required"
                    );
                    return Err(Error::IncompleteWrite {
                        table: table_name,
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery was attempted but failed ({}). Manual resolution is required. The WAL file is located at '{wal_location}'.",
                            wal.staged_files.len(),
                            wal.target_snapshot,
                            wal.created_at,
                            e
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Read the staging WAL from local filesystem, if present.
    /// Returns the WAL data and the absolute path to the WAL file.
    async fn read_staging_wal_local(&self) -> Option<(StagingWal, String)> {
        let staging_dir =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);
        let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
        let location = wal_path.to_string_lossy().to_string();
        match tokio::fs::read_to_string(&wal_path).await {
            Ok(content) => match serde_json::from_str::<StagingWal>(&content) {
                Ok(wal) => Some((wal, location)),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse staging WAL for table {}: {e}",
                        self.table_name(),
                    );
                    None
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                tracing::warn!(
                    "Failed to read staging WAL for table {}: {e}",
                    self.table_name(),
                );
                None
            }
        }
    }

    /// Read the staging WAL from S3, if present.
    /// Returns the WAL data and the S3 key of the WAL file.
    async fn read_staging_wal_s3(&self) -> Option<(StagingWal, String)> {
        let config = self.require_object_store().ok()?;
        let staging_prefix = self.snapshot_object_store_prefix(STAGING_DIR_NAME).ok()??;
        let wal_key =
            ObjectStorePath::from(format!("{}{STAGING_WAL_FILENAME}", staging_prefix.as_ref()));
        let location = wal_key.to_string();
        match config.store.get(&wal_key).await {
            Ok(result) => {
                let bytes = result.bytes().await.ok()?;
                let wal = serde_json::from_slice::<StagingWal>(&bytes).ok()?;
                Some((wal, location))
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => {
                tracing::warn!(
                    "Failed to read staging WAL (S3) for table {}: {e}",
                    self.table_name(),
                );
                None
            }
        }
    }
}
