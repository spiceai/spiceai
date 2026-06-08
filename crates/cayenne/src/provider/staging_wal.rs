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
//! to a `_staging/` directory and then moved to the target snapshot. The target
//! can be either the active snapshot or a protected snapshot that is published
//! later. The move is **not** atomic as a batch (individual renames are atomic
//! on local FS, but the loop over files is not).
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
//! - [`PreparedStagedAppend::finish`] completes the typestate transition and
//!   returns the row count.
//!
//! The legacy one-shot [`CayenneStagedAppend::commit`] is reimplemented in terms
//! of this lifecycle and remains observably identical to the previous behavior.

use super::PartitionedWal;
use super::Result;
use super::constants::{STAGING_DIR_NAME, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME};
use super::table::CayenneTableProvider;
use crate::metastore::MetastoreTransaction;
use crate::provider::Error;
use datafusion::execution::SendableRecordBatchStream;
use futures::TryStreamExt;
use object_store::path::Path as ObjectStorePath;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
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
    staging_snapshot_id: String,
    target_snapshot_id: String,
    target_kind: StagingWalTargetKind,
    row_count: u64,
}

impl std::fmt::Debug for CayenneStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneStagedAppend")
            .field("table", &self.table.table_name())
            .field("has_write_guard", &self.write_guard.is_some())
            .field("staging_snapshot_id", &self.staging_snapshot_id)
            .field("target_snapshot_id", &self.target_snapshot_id)
            .field("target_kind", &self.target_kind)
            .field("row_count", &self.row_count)
            .finish()
    }
}

/// RAII guard for the in-flight staging-append registration taken out at the
/// start of [`CayenneStagedAppend::prepare`].
///
/// `prepare` must register the append as in-flight BEFORE awaiting the WAL write
/// so a concurrent recovery pass cannot treat the not-yet-durable WAL as a crash
/// leftover. If that `await` is cancelled (the `prepare` future is dropped),
/// neither the method's error path nor [`PreparedStagedAppend`]'s `Drop` runs —
/// so without this guard the registration would leak forever, permanently
/// blocking compaction (`has_inflight_staging_appends`) and skewing recovery
/// cleanup. The guard unregisters on drop and is [disarmed](Self::disarm) once a
/// `PreparedStagedAppend` has taken over the registration (its own `Drop` then
/// owns the unregister).
struct InflightStagingAppendGuard<'a> {
    table: &'a CayenneTableProvider,
    staging_snapshot_id: &'a str,
    armed: bool,
}

impl<'a> InflightStagingAppendGuard<'a> {
    /// Register `staging_snapshot_id` as in-flight and return an armed guard.
    fn register(table: &'a CayenneTableProvider, staging_snapshot_id: &'a str) -> Self {
        table.register_inflight_staging_append(staging_snapshot_id);
        Self {
            table,
            staging_snapshot_id,
            armed: true,
        }
    }

    /// Hand the registration off to a constructed `PreparedStagedAppend` so the
    /// guard no longer unregisters on drop.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for InflightStagingAppendGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.table
                .unregister_inflight_staging_append(self.staging_snapshot_id);
        }
    }
}

impl CayenneStagedAppend {
    pub(crate) fn from_staged_append_in(
        table: CayenneTableProvider,
        write_guard: Option<OwnedMutexGuard<()>>,
        staging_snapshot_id: String,
        row_count: u64,
    ) -> Self {
        let target_snapshot_id = table.get_current_snapshot_id();
        Self::from_staged_append_to_snapshot(
            table,
            write_guard,
            staging_snapshot_id,
            target_snapshot_id,
            StagingWalTargetKind::CurrentSnapshot,
            row_count,
        )
    }

    pub(crate) fn from_staged_append_to_snapshot(
        table: CayenneTableProvider,
        write_guard: Option<OwnedMutexGuard<()>>,
        staging_snapshot_id: String,
        target_snapshot_id: String,
        target_kind: StagingWalTargetKind,
        row_count: u64,
    ) -> Self {
        Self {
            table,
            write_guard,
            staging_snapshot_id,
            target_snapshot_id,
            target_kind,
            row_count,
        }
    }

    /// Returns the number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the local filesystem path to this append's staging WAL.
    #[must_use]
    pub fn staging_wal_path(&self) -> std::path::PathBuf {
        self.table
            .staging_wal_path_for_recovery_for(&self.staging_snapshot_id)
    }

    /// Writes the staging WAL for the current `_staging/` files.
    ///
    /// # Errors
    ///
    /// Returns an error if writing the WAL file fails.
    pub async fn write_wal(&self) -> Result<()> {
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .write_staging_wal_for(&self.staging_snapshot_id)
                .await
        } else {
            self.table
                .write_staging_wal_for_target(
                    &self.staging_snapshot_id,
                    &self.target_snapshot_id,
                    self.target_kind,
                )
                .await
        }
    }

    /// Moves staged files into the configured target snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if moving the staged files fails.
    pub async fn move_staged_files(&self) -> Result<()> {
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .move_staged_files_to_current_snapshot(&self.staging_snapshot_id)
                .await
        } else {
            self.table
                .move_staged_files_to_snapshot(&self.staging_snapshot_id, &self.target_snapshot_id)
                .await
        }
    }

    /// Removes the staging WAL after a successful move.
    ///
    /// # Errors
    ///
    /// Returns an error if removing the WAL file fails.
    pub async fn remove_wal(&self) -> Result<()> {
        self.table
            .remove_staging_wal_for(&self.staging_snapshot_id)
            .await
    }

    /// Publishes current snapshot file changes so newly committed files become visible.
    ///
    pub async fn refresh_listing_table(&self) {
        self.table.publish_current_snapshot_files_changed().await;
    }

    /// Executes the full WAL finalize sequence in order.
    ///
    /// # Errors
    ///
    /// Returns an error if any fallible step in the finalize sequence (write WAL, move files,
    /// or remove WAL) fails.
    pub async fn finalize_staged_write(&self) -> Result<()> {
        use super::table::record_cayenne_write_phase;

        // Carve the single `publish` phase into its four cost centers so the
        // metastore WAL, lock contention, object-store moves, and final commit can
        // be attributed separately. Under concurrent writes the `publish_lock_wait`
        // bucket (acquiring the visibility lock + listing fence) is the one that
        // grows: publishes serialize across the table, so a high lock-wait vs a
        // high move/commit distinguishes lock-bound from work-bound finalization.
        let wal_start = Instant::now();
        self.write_wal().await?;
        record_cayenne_write_phase(self.table.table_name(), "publish_wal_write", wal_start);

        let lock_start = Instant::now();
        let _visibility_guard = self.table.visibility_lock_arc().lock_owned().await;
        let _fence = self.table.lock_listing_fence_write_owned().await;
        record_cayenne_write_phase(self.table.table_name(), "publish_lock_wait", lock_start);

        let move_start = Instant::now();
        self.move_staged_files().await?;
        record_cayenne_write_phase(self.table.table_name(), "publish_move_files", move_start);

        let commit_start = Instant::now();
        self.remove_wal().await?;
        self.table
            .publish_current_snapshot_files_changed_under_held_fence();
        self.table.mark_maintained_aggregates_stale();
        record_cayenne_write_phase(self.table.table_name(), "publish_commit", commit_start);
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
    /// Writes the staging WAL: a durable record of the intent to move the
    /// already-staged files into the configured target snapshot directory. After this
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
        // Register the in-flight append BEFORE its WAL becomes discoverable on
        // disk. Recovery treats any committed WAL whose id is not in the
        // in-flight set as a crash leftover; writing the WAL first opened a
        // window where a concurrent recovery pass could "recover" — move and
        // delete — an append that was still being prepared.
        //
        // The guard reverts the registration on every early exit that does NOT
        // hand it to a `PreparedStagedAppend`: a WAL-write error AND cancellation
        // of this future mid-`await` (when neither the error path below nor
        // `PreparedStagedAppend::drop` would run, which would otherwise leak the
        // entry forever and permanently block compaction/recovery cleanup). It is
        // disarmed once the receipt is built, handing the unregister to its `Drop`.
        let inflight_guard =
            InflightStagingAppendGuard::register(&self.table, &self.staging_snapshot_id);
        let wal_write = if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .write_staging_wal_for(&self.staging_snapshot_id)
                .await
        } else {
            self.table
                .write_staging_wal_for_target(
                    &self.staging_snapshot_id,
                    &self.target_snapshot_id,
                    self.target_kind,
                )
                .await
        };
        // On a WAL-write error the `?` returns early here and `inflight_guard`
        // drops, reverting the registration.
        wal_write?;
        // WAL is durable; hand the in-flight registration to the receipt, whose
        // `Drop` now owns the unregister.
        inflight_guard.disarm();
        Ok(PreparedStagedAppend {
            table: self.table,
            staging_snapshot_id: self.staging_snapshot_id,
            target_snapshot_id: self.target_snapshot_id,
            target_kind: self.target_kind,
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
        self.table
            .clear_staging_snapshot_dir(&self.staging_snapshot_id)
            .await
        // _write_guard drops here, after cleanup completes.
    }
}

/// A staged append that has been [prepared](CayenneStagedAppend::prepare) for
/// commit.
///
/// Holds the staging WAL on disk. Completing the commit is a two-step dance:
///
/// 1. [`Self::apply_under_barrier`] (append path) or [`Self::apply_in_txn`]
///    (overwrite path, future work) performs the visibility flip.
/// 2. [`Self::finish`] returns the row count.
///
/// Dropping a `PreparedStagedAppend` without calling `finish` or `rollback`
/// leaves the staging WAL on disk; the next write attempt will fail at
/// [`CayenneTableProvider::ensure_no_incomplete_write`].
pub struct PreparedStagedAppend {
    table: CayenneTableProvider,
    staging_snapshot_id: String,
    target_snapshot_id: String,
    target_kind: StagingWalTargetKind,
    row_count: u64,
}

impl std::fmt::Debug for PreparedStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStagedAppend")
            .field("table", &self.table.table_name())
            .field("staging_snapshot_id", &self.staging_snapshot_id)
            .field("target_snapshot_id", &self.target_snapshot_id)
            .field("target_kind", &self.target_kind)
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl Drop for PreparedStagedAppend {
    fn drop(&mut self) {
        self.table
            .unregister_inflight_staging_append(&self.staging_snapshot_id);
    }
}

impl PreparedStagedAppend {
    /// Returns the number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    async fn lock_current_snapshot_for_apply(&self) -> Option<OwnedMutexGuard<()>> {
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            Some(self.table.write_lock_arc().lock_owned().await)
        } else {
            None
        }
    }

    fn try_lock_current_snapshot_for_held_barrier(&self) -> Result<Option<OwnedMutexGuard<()>>> {
        if self.target_kind != StagingWalTargetKind::CurrentSnapshot {
            return Ok(None);
        }

        self.table.write_lock_arc().try_lock_owned().map(Some).map_err(|_| Error::Internal {
            table: self.table.table_name().to_string(),
            message: "Failed to acquire write_lock while applying a current-snapshot staged append under a held listing fence".to_string(),
        })
    }

    fn mark_inflight_complete(&self) {
        self.table
            .unregister_inflight_staging_append(&self.staging_snapshot_id);
        if !self.table.has_inflight_staging_appends() {
            self.table
                .staging_wal_present()
                .store(false, Ordering::Release);
            self.table
                .staging_may_have_files()
                .store(false, Ordering::Release);
        }
    }

    fn ensure_current_snapshot_target_unchanged(&self) -> Result<()> {
        if self.target_kind != StagingWalTargetKind::CurrentSnapshot {
            return Ok(());
        }

        let current_snapshot_id = self.table.get_current_snapshot_id();
        if current_snapshot_id == self.target_snapshot_id {
            return Ok(());
        }

        Err(Error::IncompleteWrite {
            table: self.table.table_name().to_string(),
            message: format!(
                "Refusing to apply staged append WAL '{}' because it targets current snapshot '{}', but the current snapshot is now '{}'. Manual resolution is required.",
                self.staging_wal_path().display(),
                self.target_snapshot_id,
                current_snapshot_id,
            ),
        })
    }

    /// Apply the staged write under the caller's append-side barrier.
    ///
    /// Performs, in order: move staged files into the target snapshot
    /// directory; remove the staging WAL; invalidate the list-files cache for
    /// current-snapshot targets. Current-snapshot targets hold `write_lock`
    /// while moving files so background compaction cannot interleave with the
    /// snapshot directory mutation.
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
    /// Returns an error if moving the staged files or removing the WAL fails.
    pub async fn apply_under_barrier(&self) -> Result<()> {
        let _write_guard = self.lock_current_snapshot_for_apply().await;
        let _visibility_guard = self.table.visibility_lock_arc().lock_owned().await;
        // Hold the listing fence for the entire move + WAL removal + listing
        // swap sequence. Without this, `CayenneTableProvider::scan()` (which
        // holds `listing_fence.read()` across DataFusion's listing call) can
        // interleave with the move and observe a torn directory snapshot.
        let _fence = self.table.lock_listing_fence_write_owned().await;
        self.ensure_current_snapshot_target_unchanged()?;
        self.table
            .move_staged_files_to_snapshot(&self.staging_snapshot_id, &self.target_snapshot_id)
            .await?;
        self.table
            .remove_staging_wal_for(&self.staging_snapshot_id)
            .await?;
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .publish_current_snapshot_files_changed_under_held_fence();
        }
        self.table.mark_maintained_aggregates_stale();
        Ok(())
    }

    /// Apply the staged write ASSUMING the caller already holds this
    /// partition's `listing_fence` for write.
    ///
    /// Same observable effect as [`Self::apply_under_barrier`] but skips the
    /// internal fence acquisition. For current-snapshot targets, this method
    /// still requires `write_lock` to protect against background compaction.
    /// It attempts a non-blocking acquisition because the caller already holds
    /// the listing fence. Used by the cross-partition append
    /// coordinator (#10125 step 6), which locks fences on every participating
    /// partition (sorted to keep concurrent coordinators deadlock-free) for
    /// the duration of one barrier window, calls this method on each, and
    /// releases the fences together. Readers going through `scan()` either
    /// see the pre-barrier state on every partition or the post-barrier
    /// state on every partition.
    ///
    /// # Errors
    ///
    /// Returns an error if moving the staged files or removing the WAL fails.
    pub async fn apply_under_held_barrier(&self) -> Result<()> {
        let _write_guard = self.try_lock_current_snapshot_for_held_barrier()?;
        self.ensure_current_snapshot_target_unchanged()?;
        self.table
            .move_staged_files_to_snapshot(&self.staging_snapshot_id, &self.target_snapshot_id)
            .await?;
        self.table
            .remove_staging_wal_for(&self.staging_snapshot_id)
            .await?;
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .publish_current_snapshot_files_changed_under_held_fence();
        }
        self.table.mark_maintained_aggregates_stale();
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
        self.table
            .staging_wal_path_for_recovery_for(&self.staging_snapshot_id)
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
    /// Returns the row count. For the append path, all visibility work has
    /// already happened in `apply_under_barrier`; this is purely a typestate
    /// transition for callers that drive the staged lifecycle explicitly.
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
        self.mark_inflight_complete();
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
        self.table
            .clear_staging_snapshot_dir(&self.staging_snapshot_id)
            .await?;
        self.mark_inflight_complete();
        Ok(())
    }
}

/// Staging WAL (Write-Ahead Log) entry.
///
/// Written to `_staging/<id>/_wal.json` after all data files are staged but
/// before the move-to-snapshot operation begins. Records the intent so that
/// an interrupted move can be detected on the next table open.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagingWal {
    /// The table this WAL entry belongs to.
    pub table_name: String,
    /// The snapshot directory the staged files should be moved to.
    pub target_snapshot: String,
    /// Whether `target_snapshot` is the table's active snapshot or a protected
    /// replacement snapshot that will be published separately after recovery or finalize.
    #[serde(default)]
    pub target_kind: StagingWalTargetKind,
    /// Names of the data files in the staging directory.
    pub staged_files: Vec<String>,
    /// ISO-8601 timestamp when this WAL entry was created.
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StagingWalTargetKind {
    #[default]
    CurrentSnapshot,
    ProtectedSnapshot,
}

#[derive(Debug)]
struct LocatedStagingWal {
    staging_snapshot_id: String,
    wal: StagingWal,
    location: String,
}

impl CayenneTableProvider {
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
        let lock_wait_start = Instant::now();
        let write_guard = self.write_lock_arc().lock_owned().await;
        let lock_wait_elapsed = lock_wait_start.elapsed();
        if lock_wait_elapsed > Duration::from_millis(10) {
            tracing::debug!(
                table = self.table_name(),
                duration_ms = lock_wait_elapsed.as_millis(),
                "Cayenne write lock acquisition exceeded threshold in begin_staged_append"
            );
        }

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

        if prepared_insert.may_have_on_conflict_deletions() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne upsert or on-conflict writes",
            });
        }

        let staging_snapshot_id = Self::new_staging_snapshot_id();
        self.clear_staging_snapshot_dir(&staging_snapshot_id)
            .await?;

        self.staging_may_have_files().store(true, Ordering::Release);

        let (row_count, _writer_ops, _stats_acc) = match self
            .write_to_snapshot(
                prepared_insert.stream,
                self.target_file_size_bytes(),
                &staging_snapshot_id,
                target_partitions,
                // The prepared insert is a lazily-consumed stream of unknown
                // size; shard across the full write concurrency (prior behavior).
                None,
                super::delta_encoding::WriteClass::Delta,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                if let Err(cleanup_err) =
                    self.clear_staging_snapshot_dir(&staging_snapshot_id).await
                {
                    tracing::warn!(
                        "Failed to clean staging dir after staged append write error for table {}: {cleanup_err}",
                        self.table_name(),
                    );
                }
                return Err(e);
            }
        };

        Ok(CayenneStagedAppend::from_staged_append_in(
            self.clone_for_write(),
            Some(write_guard),
            staging_snapshot_id,
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
    /// The WAL file is placed at `{table_path}/{table_id}/_staging/<id>/_wal.json`
    /// (local FS) or at the corresponding S3 key.
    pub(crate) async fn write_staging_wal_for(&self, staging_snapshot_id: &str) -> Result<()> {
        let current_snapshot = self.get_current_snapshot_id();

        self.write_staging_wal_for_target(
            staging_snapshot_id,
            &current_snapshot,
            StagingWalTargetKind::CurrentSnapshot,
        )
        .await
    }

    pub(crate) async fn write_staging_wal_for_target(
        &self,
        staging_snapshot_id: &str,
        target_snapshot: &str,
        target_kind: StagingWalTargetKind,
    ) -> Result<()> {
        if self.table_path().starts_with("s3://") {
            self.write_staging_wal_s3(staging_snapshot_id, target_snapshot, target_kind)
                .await?;
        } else {
            self.write_staging_wal_local(staging_snapshot_id, target_snapshot, target_kind)
                .await?;
        }
        self.staging_wal_present().store(true, Ordering::Release);
        Ok(())
    }

    /// Write the staging WAL on local filesystem.
    async fn write_staging_wal_local(
        &self,
        staging_snapshot_id: &str,
        target_snapshot: &str,
        target_kind: StagingWalTargetKind,
    ) -> Result<()> {
        let staging_dir =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), staging_snapshot_id);
        Self::ensure_snapshot_dir_exists(&staging_dir).await?;

        // Collect staged data file names (exclude WAL bookkeeping files).
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
            target_kind,
            staged_files,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
        let tmp_path = staging_dir.join(STAGING_WAL_TMP_FILENAME);
        // Compact serialization: this WAL is a machine-only marker written on
        // every staged append. Pretty-printing roughly doubles the byte size
        // and adds CPU time for whitespace formatting — both pure overhead on
        // the ingestion hot path. The JSON parser is whitespace-tolerant, so
        // legacy pretty-printed WALs from older builds still load correctly.
        let content = serde_json::to_string(&wal).map_err(|e| Error::Internal {
            table: self.table_name().to_string(),
            message: format!("Failed to serialize staging WAL: {e}"),
        })?;

        // Single open + write + fsync, keeping the fd through to the sync.
        // The previous revision called `tokio::fs::write` (which opens,
        // writes, drops the fd) and then re-opened the file to call
        // `sync_all` — paying an extra `open(2)` per WAL write on every
        // staged append. Replacing the two opens with one is a small but
        // real per-ingestion saving on the local-FS hot path.
        //
        // Ordering tier (`fsync_tier::ordering_sync_tokio_file`), not
        // `sync_all`: on macOS BOTH std `sync_all` and `sync_data` are
        // `fcntl(F_FULLFSYNC)` (~4-5 ms full drive-cache flush, measured),
        // while plain `fsync(2)` is ~66 µs — and plain fsync is the macOS
        // tier SQLite/DuckDB/Postgres default to. On Linux the helper is
        // `fdatasync`, which flushes the WAL bytes + the size metadata needed
        // to read them. Full-platter durability is not load-bearing here:
        // losing this WAL record in a power-loss window only orphans staging
        // files that recovery (`ensure_no_incomplete_write`) audits and
        // discards — and the metastore's own visibility commits are SQLite
        // `synchronous=NORMAL` (no fullfsync), so a stronger barrier on this
        // marker file could not raise end-to-end durability anyway. See
        // `provider/fsync_tier.rs` for the measurements and rationale.
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
        file.write_all(content.as_bytes()).await?;
        super::fsync_tier::ordering_sync_tokio_file(&file).await?;
        drop(file);

        if let Err(e) = tokio::fs::rename(&tmp_path, &wal_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(Error::IoError { source: e });
        }

        // fsync the staging directory so that the directory entry for the newly
        // written WAL file (and any data files previously written to this staging
        // dir by `write_to_snapshot`) are durably persisted. This completes the
        // "prepare" phase durability: the staging WAL record that lists the files
        // to be moved is only considered durably written after its own directory
        // entry is safe. Because the final file is published by rename, the read
        // path never observes a half-written WAL from this writer.
        Self::sync_snapshot_dir(&staging_dir).await?;

        tracing::debug!(
            "Wrote staging WAL for table {} with {} file(s) targeting snapshot {target_snapshot}",
            self.table_name(),
            wal.staged_files.len(),
        );

        Ok(())
    }

    /// Write the staging WAL on S3.
    async fn write_staging_wal_s3(
        &self,
        staging_snapshot_id: &str,
        target_snapshot: &str,
        target_kind: StagingWalTargetKind,
    ) -> Result<()> {
        let config = self.require_object_store()?;

        let Some(staging_prefix) = self.snapshot_object_store_prefix(staging_snapshot_id)? else {
            return Ok(());
        };

        // List staged data files (exclude WAL bookkeeping objects).
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
            target_kind,
            staged_files,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        // Compact serialization: see `write_staging_wal_local` for the
        // rationale; the S3 case has the same trade-offs plus a smaller PUT
        // payload (fewer bytes billed) and faster network upload.
        let content = serde_json::to_string(&wal).map_err(|e| Error::Internal {
            table: self.table_name().to_string(),
            message: format!("Failed to serialize staging WAL: {e}"),
        })?;

        let wal_key =
            ObjectStorePath::from(format!("{}{STAGING_WAL_FILENAME}", staging_prefix.as_ref()));
        config
            .store
            .put(&wal_key, content.into())
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "write staging WAL",
                table: self.table_name().to_string(),
                source: e,
            })?;

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
    pub(crate) async fn remove_staging_wal_for(&self, staging_snapshot_id: &str) -> Result<()> {
        if self.table_path().starts_with("s3://") {
            let config = self.require_object_store()?;
            if let Some(staging_prefix) = self.snapshot_object_store_prefix(staging_snapshot_id)? {
                let wal_key = ObjectStorePath::from(format!(
                    "{}{STAGING_WAL_FILENAME}",
                    staging_prefix.as_ref()
                ));
                // Best-effort delete — if the key doesn't exist, that's fine.
                match config.store.delete(&wal_key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        if !self.has_inflight_staging_appends() {
                            self.staging_wal_present().store(false, Ordering::Release);
                            self.staging_may_have_files()
                                .store(false, Ordering::Release);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to remove staging WAL (S3) for table {}: {e}",
                            self.table_name(),
                        );
                        // leave flag true so next ensure will retry the check
                    }
                }
            }
        } else {
            let staging_dir =
                Self::snapshot_dir_path(self.table_path(), self.table_id(), staging_snapshot_id);
            let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
            let removed = match tokio::fs::remove_file(&wal_path).await {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true, // already gone = success state
                Err(e) => {
                    tracing::warn!(
                        "Failed to remove staging WAL for table {}: {e}",
                        self.table_name(),
                    );
                    false
                }
            };

            if removed {
                if !self.has_inflight_staging_appends() {
                    self.staging_wal_present().store(false, Ordering::Release);
                    self.staging_may_have_files()
                        .store(false, Ordering::Release);
                }
                // Durability: after removing the WAL marker (the "commit success" signal),
                // fsync the staging directory so the unlink is persisted. A crash without
                // this sync could make the removal non-durable, causing a false-positive
                // "incomplete write" detection on the next open even though the data move
                // succeeded and was synced. This completes the "WAL absent = durably
                // committed" contract for local FS staged appends (symmetric to the
                // sync after data file moves).
                if let Err(e) = Self::sync_snapshot_dir(&staging_dir).await {
                    tracing::warn!(
                        "Failed to sync staging dir after WAL removal for table {}: {e} (data is safe; may see stale WAL on restart)",
                        self.table_name(),
                    );
                    // Non-fatal: data files are already durable. A lingering WAL is conservative.
                }
                match tokio::fs::remove_dir(&staging_dir).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => tracing::debug!(
                        "Failed to remove empty staging dir for table {}: {e}",
                        self.table_name(),
                    ),
                }
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
        if !self.staging_wal_present().load(Ordering::Acquire)
            && !self.staging_may_have_files().load(Ordering::Acquire)
        {
            if self.table_path().starts_with("s3://") {
                return Ok(());
            }

            let staging_root =
                Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);
            let mut entries = match tokio::fs::read_dir(&staging_root).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(Error::IoError { source: e }),
            };
            if entries.next_entry().await?.is_none() {
                return Ok(());
            }
        }

        // Exclude the pipelined staged-commit finalize while recovering. The
        // Stage-B finalize (`apply_under_held_barrier`) moves staged files out
        // of `_staging/` under `visibility_lock` WITHOUT `write_lock`, while
        // this recovery runs under `write_lock` only — with no shared lock,
        // recovery can read a WAL written milliseconds ago by a still-running
        // finalize and "recover" (move + delete) the staging entries out from
        // under it (observed live: ENOENT mid-finalize → the changes stream
        // died permanently with "manual intervention required"). Taking the
        // visibility lock makes recovery and finalize mutually exclusive.
        // Lock order is safe: every caller of this function holds `write_lock`
        // (or runs single-threaded at provider open), matching the staged
        // publish order `write_lock → visibility_lock`; the finalize task
        // never takes `write_lock`, so no inversion exists.
        let _visibility = self.visibility_lock_arc().lock_owned().await;

        let mut located_wals = self.read_staging_wals().await?;
        // Sort by the staging snapshot id rather than `wal.created_at`. The
        // staging snapshot id is derived from `Uuid::now_v7()` (see
        // `CayenneTableProvider::new_staging_snapshot_id`), which is
        // monotonic in ms-precision creation time AND strictly unique even
        // when two partitions race within the same millisecond. Sorting by
        // the RFC3339 string in `created_at` is ms-precision (or coarser on
        // some platforms) and admits ties — under contention the first-
        // failure short-circuit at `:898` could otherwise abandon a later-
        // tied recovery candidate. UUID v7's encoded ordering also resists
        // small clock skew across partitions.
        located_wals
            .sort_by(|left, right| left.staging_snapshot_id.cmp(&right.staging_snapshot_id));

        let mut recovered_current_any = false;
        for located_wal in located_wals {
            if self.staging_append_is_inflight(&located_wal.staging_snapshot_id) {
                continue;
            }

            let wal = located_wal.wal;
            let wal_location = located_wal.location;
            let staging_snapshot_id = located_wal.staging_snapshot_id;
            let table_name = self.table_name().to_string();

            // If this per-partition incomplete write belongs to a cross-partition
            // commit, carry the commit id through every operator-facing recovery
            // error so related partition failures can be correlated.
            let mut extra = String::new();
            if let Ok(all_wals) =
                PartitionedWal::read_all_in(std::path::Path::new(self.table_path())).await
            {
                for (partitioned_wal, _) in all_wals {
                    if partitioned_wal
                        .partitions
                        .iter()
                        .any(|entry| entry.table_id == self.table_id())
                    {
                        extra = format!(
                            " (part of cross-partition commit {})",
                            partitioned_wal.commit_id
                        );
                        break;
                    }
                }
            }

            let current_snapshot = self.get_current_snapshot_id();
            if wal.target_kind == StagingWalTargetKind::CurrentSnapshot
                && current_snapshot != wal.target_snapshot
            {
                return Err(Error::IncompleteWrite {
                    table: table_name,
                    message: format!(
                        "A previous write was interrupted while moving {} file(s) to '{}' (started at {}), but the current snapshot is now '{}'. Automated recovery refused to avoid moving staged files into the wrong snapshot. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                        wal.staged_files.len(),
                        wal.target_snapshot,
                        wal.created_at,
                        current_snapshot,
                    ),
                });
            }

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
            if !self.table_path().starts_with("s3://") && !wal.staged_files.is_empty() {
                let staging_dir = Self::snapshot_dir_path(
                    self.table_path(),
                    self.table_id(),
                    &staging_snapshot_id,
                );
                let target_dir = Self::snapshot_dir_path(
                    self.table_path(),
                    self.table_id(),
                    &wal.target_snapshot,
                );

                let mut missing_files: Vec<String> = Vec::new();
                for staged_file in &wal.staged_files {
                    let in_staging = tokio::fs::metadata(staging_dir.join(staged_file))
                        .await
                        .is_ok();
                    let in_target = tokio::fs::metadata(target_dir.join(staged_file))
                        .await
                        .is_ok();
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
                            "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery aborted because {} of those file(s) are missing from both '_staging/' and the target snapshot — e.g. {sample:?}. This indicates genuine data loss (filesystem corruption or external interference). Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot,
                            wal.created_at,
                            missing_files.len(),
                        ),
                    });
                }
            } else if self.table_path().starts_with("s3://") && !wal.staged_files.is_empty() {
                // Pre-recovery audit (S3): symmetric to the local-FS audit.
                // List the staging prefix and the target snapshot prefix. Every
                // WAL-listed file must appear in at least one of those prefixes.
                let config = match self.require_object_store() {
                    Ok(config) => config,
                    Err(e) => return Err(e),
                };

                let Some(staging_prefix) = self
                    .snapshot_object_store_prefix(&staging_snapshot_id)
                    .ok()
                    .flatten()
                else {
                    return Err(Error::IncompleteWrite {
                        table: table_name.clone(),
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}'. Could not determine S3 staging prefix for pre-recovery audit. Manual resolution required.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot
                        ),
                    });
                };

                let target_prefix = self
                    .snapshot_object_store_prefix(&wal.target_snapshot)
                    .ok()
                    .flatten();

                let mut reachable: std::collections::HashSet<String> =
                    std::collections::HashSet::new();

                if let Ok(objects) = config
                    .store
                    .list(Some(&staging_prefix))
                    .try_collect::<Vec<_>>()
                    .await
                {
                    for meta in objects {
                        if let Some(rel) =
                            meta.location.as_ref().strip_prefix(staging_prefix.as_ref())
                            && rel != STAGING_WAL_FILENAME
                            && rel != STAGING_WAL_TMP_FILENAME
                        {
                            reachable.insert(rel.to_string());
                        }
                    }
                }

                if let Some(target_prefix) = &target_prefix
                    && let Ok(objects) = config
                        .store
                        .list(Some(target_prefix))
                        .try_collect::<Vec<_>>()
                        .await
                {
                    for meta in objects {
                        if let Some(rel) =
                            meta.location.as_ref().strip_prefix(target_prefix.as_ref())
                        {
                            reachable.insert(rel.to_string());
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
                    let sample: Vec<&str> =
                        missing_files.iter().take(3).map(String::as_str).collect();
                    return Err(Error::IncompleteWrite {
                        table: table_name,
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery aborted because {} of those file(s) are missing from both the staging prefix and the target snapshot on S3 — e.g. {sample:?}. This may indicate a partial multipart upload that was never completed or external interference. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot,
                            wal.created_at,
                            missing_files.len(),
                        ),
                    });
                }
            }

            tracing::warn!(
                table = table_name.as_str(),
                wal_location = %wal_location,
                target_snapshot = %wal.target_snapshot,
                staged_files = wal.staged_files.len(),
                "Incomplete staged append detected — attempting automated recovery"
            );

            match self
                .move_staged_files_to_snapshot(&staging_snapshot_id, &wal.target_snapshot)
                .await
            {
                Ok(()) => {
                    if let Err(e) = self.remove_staging_wal_for(&staging_snapshot_id).await {
                        tracing::error!(
                            table = table_name.as_str(),
                            error = %e,
                            "Automated recovery moved staged files but failed to remove the staging WAL"
                        );
                        return Err(Error::IncompleteWrite {
                            table: table_name,
                            message: format!(
                                "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery moved the staged files, but failed to remove the WAL ({}). Refusing writes until the stale WAL is removed manually. The WAL file is located at '{wal_location}'.{extra}",
                                wal.staged_files.len(),
                                wal.target_snapshot,
                                wal.created_at,
                                e
                            ),
                        });
                    }
                    tracing::info!(
                        table = table_name.as_str(),
                        "Automated recovery from incomplete write succeeded; table is now writable"
                    );
                    if wal.target_kind == StagingWalTargetKind::CurrentSnapshot {
                        recovered_current_any = true;
                    }
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
                            "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Automated recovery was attempted but failed ({}). Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot,
                            wal.created_at,
                            e
                        ),
                    });
                }
            }
        }

        if recovered_current_any {
            self.publish_current_snapshot_files_changed().await;
        }

        // WAL absent, or only process-local in-flight WALs remain. When no
        // in-flight append is known, clear any orphan pre-WAL staging files
        // and correct the flags so future writes take the fast path. Unparseable
        // committed WALs are errors above; only uncommitted tmp WALs are ignored.
        //
        // Cleanup is per-entry (`clear_orphan_staging_dirs`), never a whole-root
        // delete: an append registering concurrently with this pass must not
        // lose its staged files (the whole-root variant destroyed a pipelined
        // finalize's staging dir mid-move — observed live as a permanent
        // changes-stream failure).
        if !self.has_inflight_staging_appends() {
            self.clear_orphan_staging_dirs().await?;
            self.staging_wal_present().store(false, Ordering::Release);
        }
        Ok(())
    }

    async fn read_staging_wals(&self) -> Result<Vec<LocatedStagingWal>> {
        if self.table_path().starts_with("s3://") {
            self.read_staging_wals_s3().await
        } else {
            self.read_staging_wals_local().await
        }
    }

    async fn read_staging_wals_local(&self) -> Result<Vec<LocatedStagingWal>> {
        let mut wals = Vec::new();
        let staging_root =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);
        let top_level_wal = staging_root.join(STAGING_WAL_FILENAME);
        match tokio::fs::try_exists(&top_level_wal).await {
            Ok(true) => {
                let location = top_level_wal.to_string_lossy().to_string();
                return Err(Error::IncompleteWrite {
                    table: self.table_name().to_string(),
                    message: format!(
                        "Found unsupported top-level staging WAL at '{location}'. Cayenne staged appends now use isolated '_staging/<id>/' directories. Manual resolution is required."
                    ),
                });
            }
            Ok(false) => {}
            Err(e) => return Err(Error::IoError { source: e }),
        }

        let mut entries = match tokio::fs::read_dir(&staging_root).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(wals),
            Err(e) => return Err(Error::IoError { source: e }),
        };

        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let child = entry.file_name().to_string_lossy().to_string();
            let staging_snapshot_id = format!("{STAGING_DIR_NAME}/{child}");
            if let Some(wal) = self.read_staging_wal_local_at(&staging_snapshot_id).await? {
                wals.push(wal);
            }
        }

        Ok(wals)
    }

    async fn read_staging_wal_local_at(
        &self,
        staging_snapshot_id: &str,
    ) -> Result<Option<LocatedStagingWal>> {
        let staging_dir =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), staging_snapshot_id);
        let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
        let location = wal_path.to_string_lossy().to_string();
        match tokio::fs::read_to_string(&wal_path).await {
            Ok(content) => match serde_json::from_str::<StagingWal>(&content) {
                Ok(wal) => Ok(Some(LocatedStagingWal {
                    staging_snapshot_id: staging_snapshot_id.to_string(),
                    wal,
                    location,
                })),
                Err(e) => Err(Error::IncompleteWrite {
                    table: self.table_name().to_string(),
                    message: format!(
                        "Found unreadable staging WAL at '{location}': {e}. Refusing writes to avoid ignoring a possibly committed staged append. Manual resolution is required."
                    ),
                }),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::IoError { source: e }),
        }
    }

    async fn read_staging_wals_s3(&self) -> Result<Vec<LocatedStagingWal>> {
        let config = self.require_object_store()?;
        let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? else {
            return Ok(Vec::new());
        };
        let objects: Vec<_> = config
            .store
            .list(Some(&staging_prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list staging WALs",
                table: self.table_name().to_string(),
                source: e,
            })?;

        let mut wals = Vec::new();
        for meta in objects {
            let Some(relative) = meta.location.as_ref().strip_prefix(staging_prefix.as_ref())
            else {
                continue;
            };
            let staging_snapshot_id = if relative == STAGING_WAL_FILENAME {
                return Err(Error::IncompleteWrite {
                    table: self.table_name().to_string(),
                    message: format!(
                        "Found unsupported top-level staging WAL at '{}'. Cayenne staged appends now use isolated '_staging/<id>/' prefixes. Manual resolution is required.",
                        meta.location,
                    ),
                });
            } else if let Some(child) = relative.strip_suffix(&format!("/{STAGING_WAL_FILENAME}")) {
                format!("{STAGING_DIR_NAME}/{child}")
            } else {
                continue;
            };

            let location = meta.location.to_string();
            let result =
                config
                    .store
                    .get(&meta.location)
                    .await
                    .map_err(|e| Error::ObjectStore {
                        operation: "read staging WAL",
                        table: self.table_name().to_string(),
                        source: e,
                    })?;
            let bytes = result.bytes().await.map_err(|e| Error::ObjectStore {
                operation: "read staging WAL",
                table: self.table_name().to_string(),
                source: e,
            })?;
            let wal = serde_json::from_slice::<StagingWal>(&bytes).map_err(|e| {
                Error::IncompleteWrite {
                    table: self.table_name().to_string(),
                    message: format!(
                        "Found unreadable staging WAL at '{location}': {e}. Refusing writes to avoid ignoring a possibly committed staged append. Manual resolution is required."
                    ),
                }
            })?;
            wals.push(LocatedStagingWal {
                staging_snapshot_id,
                wal,
                location,
            });
        }

        Ok(wals)
    }
}
