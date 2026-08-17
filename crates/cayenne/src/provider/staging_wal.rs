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
use super::on_conflict::{PostValidationState, PreparedOnConflictDeletionPublish};
use super::table::{CayenneTableProvider, PreparedAppendSnapshotPublish};
use crate::metadata::SnapshotFile;
use crate::metastore::MetastoreTransaction;
use crate::provider::Error;
use arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectStorePath;
use std::sync::Arc;
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
    source_snapshot_id: Option<String>,
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
            .field("source_snapshot_id", &self.source_snapshot_id)
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
        Self {
            table,
            write_guard,
            staging_snapshot_id,
            source_snapshot_id: None,
            target_snapshot_id,
            target_kind: StagingWalTargetKind::CurrentSnapshot,
            row_count,
        }
    }

    pub(crate) fn from_staged_append_to_snapshot(
        table: CayenneTableProvider,
        write_guard: Option<OwnedMutexGuard<()>>,
        staging_snapshot_id: String,
        source_snapshot_id: String,
        target_snapshot_id: String,
        target_kind: StagingWalTargetKind,
        row_count: u64,
    ) -> Self {
        Self {
            table,
            write_guard,
            staging_snapshot_id,
            source_snapshot_id: Some(source_snapshot_id),
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
        // Empty-table probe (see `maybe_install_warm_pk_caches`): a staged
        // append can be a fresh table's very first write, and its committed
        // keys must land in live caches for the warm-index invariant to hold.
        self.table.maybe_install_warm_pk_caches().await;
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
            // Retain the per-table write guard through finalize ONLY for the
            // deferred / protected-snapshot path (the cross-partition coordinator
            // in `begin_deferred_snapshot_append` and the on-conflict deferred
            // publish), which finalizes via `apply_under_held_barrier` and holds
            // the lock across the coordinated commit. A single-table
            // current-snapshot append finalizes via `finish` → `apply_under_barrier`,
            // which RE-ACQUIRES `write_lock` in `lock_current_snapshot_for_apply`:
            // keeping the guard here would self-deadlock that re-acquire and block
            // any concurrent staged append. Dropping it (it stays held through this
            // method's WAL write, then releases when `self` is consumed) mirrors the
            // pre-refactor behavior and the `write_cdc_pipelined` guard handling.
            write_guard: if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
                None
            } else {
                self.write_guard
            },
            staging_snapshot_id: self.staging_snapshot_id,
            source_snapshot_id: self.source_snapshot_id,
            target_snapshot_id: self.target_snapshot_id,
            target_kind: self.target_kind,
            row_count: self.row_count,
            // Default: no incremental IVM feed. The write path attaches captured
            // batches via `set_ivm_feed_batches` only for IVM tables.
            ivm_feed_batches: None,
            prepared_on_conflict: None,
            deferred_manifest: None,
            validated_file_keys: None,
            append_sequence: None,
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
    write_guard: Option<OwnedMutexGuard<()>>,
    staging_snapshot_id: String,
    source_snapshot_id: Option<String>,
    target_snapshot_id: String,
    target_kind: StagingWalTargetKind,
    row_count: u64,
    /// IVM feed: the insert `RecordBatches` captured at Stage A, present ONLY when
    /// this table has a registered maintained aggregate AND the write is
    /// incrementally feedable (set by the write path; `None` for non-IVM tables —
    /// the common case, zero cost — or when the write must fall back to a full
    /// rebuild). Consumed under the publish fence by
    /// [`CayenneTableProvider::feed_staged_ivm_under_fence`]: `Some` feeds the
    /// registry incrementally, `None` marks it stale (if IVM is registered).
    ivm_feed_batches: Option<Arc<Vec<RecordBatch>>>,
    prepared_on_conflict: Option<PreparedOnConflictDeletionPublish>,
    deferred_manifest: Option<Vec<SnapshotFile>>,
    validated_file_keys: Option<super::pk_index::PkDigestSet>,
    append_sequence: Option<i64>,
}

/// Object-store handle, table-level WAL prefix, and canonical backend identity.
pub type PartitionedWalObjectStore = (Arc<dyn object_store::ObjectStore>, ObjectStorePath, String);

impl std::fmt::Debug for PreparedStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStagedAppend")
            .field("table", &self.table.table_name())
            .field("staging_snapshot_id", &self.staging_snapshot_id)
            .field("source_snapshot_id", &self.source_snapshot_id)
            .field("target_snapshot_id", &self.target_snapshot_id)
            .field("target_kind", &self.target_kind)
            .field("row_count", &self.row_count)
            .field("has_write_guard", &self.write_guard.is_some())
            .field("has_ivm_feed", &self.ivm_feed_batches.is_some())
            .field("has_on_conflict", &self.prepared_on_conflict.is_some())
            .field("has_deferred_manifest", &self.deferred_manifest.is_some())
            .field(
                "has_validated_file_keys",
                &self.validated_file_keys.is_some(),
            )
            .field("append_sequence", &self.append_sequence)
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

    /// Attach the IVM insert batches captured at Stage A (write path → receipt),
    /// to be fed to the maintained-aggregate registry under the publish fence in
    /// `apply_under_barrier` / `apply_under_held_barrier`. `None` (the default)
    /// means no incremental feed — the registry is marked stale if IVM is
    /// registered, falling queries back to a base scan.
    pub(crate) fn set_ivm_feed_batches(&mut self, batches: Option<Arc<Vec<RecordBatch>>>) {
        self.ivm_feed_batches = batches;
    }

    pub(crate) fn set_prepared_on_conflict(&mut self, prepared: PreparedOnConflictDeletionPublish) {
        self.prepared_on_conflict = Some(prepared);
    }

    pub(crate) fn set_validated_file_keys(&mut self, keys: super::pk_index::PkDigestSet) {
        self.validated_file_keys = Some(keys);
    }

    /// Publish primary-key digests validated while the staged snapshot was private.
    ///
    /// `on_conflict_sequence` is the publish sequence when this append carries a
    /// prepared on-conflict deletion (whose `prepared_on_conflict` was moved out
    /// before publication). It takes precedence because an on-conflict append has
    /// no `append_sequence` — stamping the fallback `0` would let a later
    /// transaction that read these keys miss the conflict (silent lost update).
    pub fn publish_validated_file_keys(&self, on_conflict_sequence: Option<i64>) {
        if let Some(keys) = &self.validated_file_keys {
            // Stamp the appended keys with this append's commit sequence for the
            // per-key optimistic-concurrency check (a transaction that read these
            // keys before the append sees them advance and conflicts).
            if let Some(sequence) = on_conflict_sequence.or(self.append_sequence) {
                self.table.record_file_pk_keys(keys, sequence);
            } else {
                // Neither sequence is available. Stamping the fallback `0` would
                // fail OPEN for per-key OCC — a transaction that read these keys
                // would see stamp 0 <= its begin token and MISS the conflict
                // (silent lost update). Degrade per-key OCC to the per-table
                // fallback, and do it BEFORE publishing the stamp-0 keys: a reader
                // observes the keys only after acquiring the pk_keyset_cache lock
                // that `record_file_pk_keys` releases, and that lock's
                // release/acquire chains after this `Release` store, so any reader
                // that sees a stamp-0 entry is guaranteed to also see degraded and
                // take the per-table fallback (setting the flag after the publish
                // would leave a window where the untrustworthy stamp is trusted).
                self.table.mark_pk_keyset_occ_degraded();
                self.table.record_file_pk_keys(keys, 0);
            }
        }
    }

    /// Remove and return deferred on-conflict publication state from this receipt.
    pub fn take_prepared_on_conflict(&mut self) -> Option<PreparedOnConflictDeletionPublish> {
        self.prepared_on_conflict.take()
    }

    /// Restore deferred on-conflict state after durable-outcome reconciliation.
    pub fn restore_prepared_on_conflict(
        &mut self,
        prepared: Option<PreparedOnConflictDeletionPublish>,
    ) {
        self.prepared_on_conflict = prepared;
    }

    /// Preserve physical deletion files for top-level WAL recovery while
    /// relinquishing process-local cleanup bookkeeping.
    pub fn retain_files_for_wal_recovery(&mut self) {
        if let Some(prepared) = self.prepared_on_conflict.as_mut() {
            prepared.retain_files_for_wal_recovery();
        }
    }

    /// Resolve abort-cleanup ownership after a cancelled shared commit.
    /// Recovery calls this only after the durable snapshot pointer proves the
    /// transaction committed, then verifies every generated delete-file path is
    /// present in the committed catalog payload before disarming cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if committed deletion metadata cannot be loaded or does
    /// not exactly contain every prepared deletion-vector path.
    pub async fn reconcile_committed_on_conflict_cleanup(&mut self) -> Result<()> {
        let Some(prepared) = self.prepared_on_conflict.as_mut() else {
            return Ok(());
        };
        let committed_paths = self
            .table
            .metadata_catalog()
            .get_table_delete_files(self.table.table_id())
            .await?
            .into_iter()
            .map(|file| file.path)
            .collect::<std::collections::HashSet<_>>();
        if !prepared.mark_catalog_committed_if_paths_match(&committed_paths) {
            return Err(Error::IncompleteWrite {
                table: self.table.table_name().to_string(),
                message: format!(
                    "Catalog committed deferred snapshot '{}', but its prepared deletion-vector files do not exactly match the committed metadata; retaining cleanup ownership and requiring manual recovery",
                    self.target_snapshot_id
                ),
            });
        }
        Ok(())
    }

    /// Reconcile this receipt's table from its durable staging WAL and catalog
    /// pointer while the receipt still owns its write guard.
    ///
    /// # Errors
    ///
    /// Returns an error if staged-write recovery cannot converge safely.
    pub async fn recover_committed_snapshot(&self) -> Result<()> {
        self.table.ensure_no_incomplete_write().await
    }

    /// Return the exact manifest prepared for this deferred snapshot.
    #[must_use]
    pub fn deferred_manifest(&self) -> Option<&[SnapshotFile]> {
        self.deferred_manifest.as_deref()
    }

    /// Build and validate the target snapshot's exact durable manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if source or target files cannot be listed, metadata
    /// cannot be loaded, or the target manifest is inconsistent.
    pub async fn prepare_deferred_manifest(&mut self) -> Result<()> {
        let source_snapshot_id =
            self.source_snapshot_id
                .as_ref()
                .ok_or_else(|| Error::Internal {
                    table: self.table.table_name().to_string(),
                    message: "Deferred manifest preparation requires a source snapshot".to_string(),
                })?;
        let source_manifest = self
            .table
            .metadata_catalog()
            .get_snapshot_files(self.table.table_id(), source_snapshot_id)
            .await?;
        let source_names = self
            .table
            .list_snapshot_files_with_sizes(source_snapshot_id)
            .await?
            .into_iter()
            .map(|(file, _)| file)
            .collect::<std::collections::HashSet<_>>();
        let new_sequence = if let Some(prepared) = &self.prepared_on_conflict {
            prepared.snapshot_sequence
        } else if let Some(sequence) = self.append_sequence {
            sequence
        } else {
            return Err(Error::Internal {
                table: self.table.table_name().to_string(),
                message: "Deferred append manifest has no reserved append sequence".to_string(),
            });
        };
        let listed_files = self
            .table
            .list_snapshot_files_with_sizes(&self.target_snapshot_id)
            .await?;
        let target_names = listed_files
            .iter()
            .map(|(file, _)| file.clone())
            .collect::<std::collections::HashSet<_>>();
        let listed_file_count = listed_files.len();
        if target_names.len() != listed_files.len() {
            return Err(Error::Internal {
                table: self.table.table_name().to_string(),
                message: format!(
                    "Deferred snapshot '{}' contains duplicate data-file names",
                    self.target_snapshot_id
                ),
            });
        }
        let mut manifest = source_manifest
            .into_iter()
            .filter(|file| {
                source_names.contains(&file.file_path) && target_names.contains(&file.file_path)
            })
            .map(|mut file| {
                file.snapshot_id.clone_from(&self.target_snapshot_id);
                file
            })
            .collect::<Vec<_>>();
        let manifest_names = manifest
            .iter()
            .map(|file| file.file_path.clone())
            .collect::<std::collections::HashSet<_>>();
        manifest.extend(
            listed_files
                .iter()
                .filter(|(name, _)| source_names.contains(name) && !manifest_names.contains(name))
                .map(|(file_path, file_size_bytes)| SnapshotFile {
                    table_id: self.table.table_id().to_string(),
                    snapshot_id: self.target_snapshot_id.clone(),
                    file_path: file_path.clone(),
                    row_count: 0,
                    file_size_bytes: i64::try_from(*file_size_bytes).unwrap_or(i64::MAX),
                    min_sequence: 0,
                    max_sequence: new_sequence,
                    digest: None,
                }),
        );
        manifest.extend(
            listed_files
                .into_iter()
                .filter(|(name, _)| !source_names.contains(name))
                .map(|(file_path, file_size_bytes)| SnapshotFile {
                    table_id: self.table.table_id().to_string(),
                    snapshot_id: self.target_snapshot_id.clone(),
                    file_path,
                    row_count: 0,
                    file_size_bytes: i64::try_from(file_size_bytes).unwrap_or(i64::MAX),
                    min_sequence: new_sequence,
                    max_sequence: new_sequence,
                    digest: None,
                }),
        );
        manifest.sort_unstable_by(|left, right| left.file_path.cmp(&right.file_path));
        if manifest.len() != listed_file_count {
            return Err(Error::Internal {
                table: self.table.table_name().to_string(),
                message: format!(
                    "Deferred manifest for snapshot '{}' has {} rows for {} physical files",
                    self.target_snapshot_id,
                    manifest.len(),
                    listed_file_count
                ),
            });
        }
        self.deferred_manifest = Some(manifest);
        Ok(())
    }

    /// Publish prepared on-conflict state while the caller holds the listing fence.
    pub fn publish_on_conflict_under_held_fence(
        &self,
        prepared: PreparedOnConflictDeletionPublish,
    ) {
        self.table.publish_prepared_on_conflict_deletions(prepared);
    }

    async fn lock_current_snapshot_for_apply(&self) -> Option<OwnedMutexGuard<()>> {
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            Some(self.table.write_lock_arc().lock_owned().await)
        } else {
            None
        }
    }

    fn try_lock_current_snapshot_for_held_barrier(&self) -> Result<Option<OwnedMutexGuard<()>>> {
        if self.target_kind != StagingWalTargetKind::CurrentSnapshot || self.write_guard.is_some() {
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
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .remove_staging_wal_for(&self.staging_snapshot_id)
                .await?;
            self.table
                .publish_current_snapshot_files_changed_under_held_fence();
        }
        // IVM: feed the maintained-aggregate registry from this staged publish,
        // atomically with the held listing fence (the serializer for concurrent
        // finish() tasks) so applier-enqueue order == epoch order. `None` feed
        // (non-IVM table, or a non-incremental write) marks stale instead.
        self.table
            .feed_staged_ivm_under_fence(self.ivm_feed_batches.as_ref());
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
        if self.target_kind == StagingWalTargetKind::CurrentSnapshot {
            self.table
                .remove_staging_wal_for(&self.staging_snapshot_id)
                .await?;
            self.table
                .publish_current_snapshot_files_changed_under_held_fence();
        }
        // IVM: feed the maintained-aggregate registry from this staged publish,
        // atomically with the held listing fence (the serializer for concurrent
        // finish() tasks) so applier-enqueue order == epoch order. `None` feed
        // (non-IVM table, or a non-incremental write) marks stale instead.
        self.table
            .feed_staged_ivm_under_fence(self.ivm_feed_batches.as_ref());
        Ok(())
    }

    /// Returns the partition's catalog `table_id`. Used by the cross-partition
    /// coordinator to populate the top-level WAL.
    #[must_use]
    pub fn table_id(&self) -> &str {
        self.table.table_id()
    }

    /// The snapshot containing this prepared append's complete post-commit
    /// contents.
    #[must_use]
    pub fn target_snapshot_id(&self) -> &str {
        &self.target_snapshot_id
    }

    /// Build the fallible listing-table state needed to publish this deferred
    /// append. Call before the cross-partition catalog transaction commits.
    ///
    /// # Errors
    ///
    /// Returns an error if the receipt is not a protected-snapshot append or
    /// listing-table publication state cannot be prepared.
    pub fn prepare_deferred_snapshot_publish(&self) -> Result<PreparedAppendSnapshotPublish> {
        if self.target_kind != StagingWalTargetKind::ProtectedSnapshot {
            return Err(Error::Internal {
                table: self.table.table_name().to_string(),
                message: "Deferred snapshot preparation requires a protected-snapshot target"
                    .to_string(),
            });
        }
        self.table
            .prepare_append_snapshot_publish(&self.target_snapshot_id)
    }

    /// Publish a prebuilt deferred-snapshot append after the cross-partition
    /// catalog transaction has atomically advanced every partition pointer.
    /// This is deliberately synchronous: once the durable commit decision has
    /// been recorded, cancellation must not interrupt publication midway.
    pub fn publish_deferred_snapshot_under_held_fence(
        &self,
        prepared: PreparedAppendSnapshotPublish,
    ) {
        self.table
            .publish_append_snapshot_under_held_fence(prepared);
    }

    /// Remove this append's staging WAL after its committed protected-snapshot
    /// has been finalized (replacement files durably in the target snapshot).
    /// Called by the cross-partition coordinator after its multi-partition
    /// commit and by the single-table CDC upsert finalize (`CayenneCdcWrite::finish`),
    /// since `apply_under_held_barrier` removes the WAL only for `CurrentSnapshot`
    /// targets.
    ///
    /// Callers treat failure as best-effort recoverable maintenance: the append
    /// is already durably committed, so a failed WAL removal must NOT turn it
    /// into a client-visible failure — the leftover WAL is rolled forward
    /// idempotently by the next write's `ensure_no_incomplete_write`.
    ///
    /// # Errors
    ///
    /// Returns an error if the local or object-store WAL cannot be removed.
    pub async fn remove_committed_staging_wal(&self) -> Result<()> {
        self.table
            .remove_staging_wal_for(&self.staging_snapshot_id)
            .await
    }

    /// Run best-effort maintenance after the deferred snapshot is visible.
    pub async fn finish_deferred_snapshot_maintenance(&self) {
        self.table
            .finish_deferred_append_snapshot(&self.target_snapshot_id)
            .await;
        if let Some(source_snapshot_id) = &self.source_snapshot_id {
            self.table
                .retire_snapshot_dirs(std::iter::once(source_snapshot_id.as_str()));
        }
    }

    /// Returns this partition's absolute staging-WAL path, used by the
    /// cross-partition coordinator to record what the top-level WAL refers
    /// to.
    #[must_use]
    pub fn staging_wal_path(&self) -> std::path::PathBuf {
        self.table
            .staging_wal_path_for_recovery_for(&self.staging_snapshot_id)
    }

    /// Return the object store and table-level prefix used for a top-level
    /// cross-partition WAL. Local tables return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an error if object-store configuration or the canonical table
    /// prefix cannot be resolved.
    pub fn partitioned_wal_object_store(&self) -> Result<Option<PartitionedWalObjectStore>> {
        self.table.partitioned_wal_object_store()
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
        if self.target_kind == StagingWalTargetKind::ProtectedSnapshot {
            self.table
                .clear_snapshot_dir(&self.target_snapshot_id)
                .await?;
        }
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
    outcome: StagingWalOutcome,
    location: String,
}

/// The result of reading one staging-WAL record on recovery.
#[derive(Debug)]
enum StagingWalOutcome {
    /// A record that read back and (when framed) passed its checksum.
    Parsed(StagingWal),
    /// A checksum-framed record that failed its integrity check (bit-rot or a
    /// torn write). Carries a human-readable reason for logs. Recovery discards
    /// it — its staged files were never durably committed into a snapshot (the
    /// metastore visibility commit, not this marker, is the durable commit
    /// point; see `write_staging_wal_local`), so dropping them converges to the
    /// last committed snapshot rather than replaying corrupted move
    /// instructions.
    Corrupt(String),
}

impl CayenneTableProvider {
    /// Return the object store and table-level prefix used for top-level
    /// cross-partition WALs. Local tables return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an error if object-store configuration or the canonical table
    /// prefix cannot be resolved.
    pub fn partitioned_wal_object_store(&self) -> Result<Option<PartitionedWalObjectStore>> {
        if !self.table_path().starts_with("s3://") {
            return Ok(None);
        }
        let config = self.require_object_store()?;
        let snapshot_id = self.get_current_snapshot_id();
        let partition_prefix = self
            .snapshot_object_store_prefix(&snapshot_id)?
            .ok_or_else(|| Error::Internal {
                table: self.table_name().to_string(),
                message: "Missing object-store snapshot prefix".to_string(),
            })?;
        let suffix = format!("{}/{snapshot_id}", self.table_id());
        let prefix = partition_prefix
            .as_ref()
            .strip_suffix(&suffix)
            .ok_or_else(|| Error::Internal {
                table: self.table_name().to_string(),
                message: format!(
                    "Snapshot prefix '{partition_prefix}' does not end with expected suffix '{suffix}'"
                ),
            })?;
        Ok(Some((
            Arc::clone(&config.store),
            ObjectStorePath::from(prefix),
            config.url.to_string(),
        )))
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

    /// Begin a staged append into a fresh snapshot cloned from the current
    /// snapshot. The new snapshot remains invisible until its catalog pointer is
    /// committed and [`PreparedStagedAppend::publish_deferred_snapshot`] runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the source snapshot cannot be cloned, the append
    /// cannot be staged, or its exact target manifest cannot be prepared.
    pub async fn begin_deferred_snapshot_append(
        &self,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<PreparedStagedAppend> {
        struct DeferredSetupCleanup {
            table: CayenneTableProvider,
            snapshots: Vec<String>,
            armed: bool,
        }
        impl Drop for DeferredSetupCleanup {
            fn drop(&mut self) {
                if !self.armed {
                    return;
                }
                let table = self.table.clone_for_write_operations();
                let snapshots = std::mem::take(&mut self.snapshots);
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move {
                        for snapshot in snapshots {
                            let _ = table.clear_snapshot_dir(&snapshot).await;
                            let _ = table.clear_staging_snapshot_dir(&snapshot).await;
                        }
                    });
                } else {
                    std::thread::spawn(move || {
                        let runtime = match tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                tracing::warn!(
                                    table = table.table_name(),
                                    %error,
                                    "Failed to start cleanup runtime for deferred snapshot setup"
                                );
                                return;
                            }
                        };
                        runtime.block_on(async move {
                            for snapshot in snapshots {
                                let _ = table.clear_snapshot_dir(&snapshot).await;
                                let _ = table.clear_staging_snapshot_dir(&snapshot).await;
                            }
                        });
                    });
                }
            }
        }
        let write_guard = self.write_lock_arc().lock_owned().await;
        self.ensure_no_incomplete_write().await?;

        // The private target currently clones only immutable snapshot data.
        // Pending deletion vectors, inline rows/deletes, and protected snapshot
        // state are separate visibility inputs and cannot be dropped from a
        // cross-partition append. Refuse before cloning or consuming input until
        // those states are included in the coordinated transaction.
        let current_snapshot_id = self.get_current_snapshot_id();
        let (_, target_snapshot_id) = Self::new_staging_snapshot_id_pair();
        let mut setup_cleanup = DeferredSetupCleanup {
            table: self.clone_for_write_operations(),
            snapshots: vec![target_snapshot_id.clone()],
            armed: true,
        };
        self.clone_snapshot_files(&current_snapshot_id, &target_snapshot_id)
            .await?;

        let staging_snapshot_id = Self::new_staging_snapshot_id();
        setup_cleanup.snapshots.push(staging_snapshot_id.clone());
        self.clear_staging_snapshot_dir(&staging_snapshot_id)
            .await?;
        let prepared_insert = match self.prepare_stream_for_insert(data).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.clear_snapshot_dir(&target_snapshot_id).await?;
                return Err(error);
            }
        };
        let may_have_on_conflict_deletions = prepared_insert.may_have_on_conflict_deletions();
        let post_validation = prepared_insert.post_validation();
        let row_count = match self
            .write_stream_to_staging_snapshot(
                prepared_insert.stream,
                &staging_snapshot_id,
                target_partitions,
            )
            .await
        {
            Ok(row_count) => row_count,
            Err(error) => {
                self.clear_staging_snapshot_dir(&staging_snapshot_id)
                    .await?;
                self.clear_snapshot_dir(&target_snapshot_id).await?;
                return Err(error);
            }
        };
        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = post_validation.lock().take().unwrap_or_default();
        let prepared_on_conflict = if may_have_on_conflict_deletions || self.has_pending_deletions()
        {
            match self
                .prepare_on_conflict_deletions_for_staged_snapshot(
                    on_conflict_deletions,
                    target_snapshot_id.clone(),
                    true,
                )
                .await
            {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    let _ = self.clear_staging_snapshot_dir(&staging_snapshot_id).await;
                    let _ = self.clear_snapshot_dir(&target_snapshot_id).await;
                    return Err(error.into());
                }
            }
        } else {
            None
        };
        let append_sequence = if prepared_on_conflict.is_none() {
            Some(self.reserve_sequences_local(1).await?)
        } else {
            None
        };
        let staged = CayenneStagedAppend::from_staged_append_to_snapshot(
            self.clone_for_write_operations(),
            Some(write_guard),
            staging_snapshot_id,
            current_snapshot_id,
            target_snapshot_id,
            StagingWalTargetKind::ProtectedSnapshot,
            row_count,
        );
        let mut prepared = staged.prepare().await?;
        if let Some(on_conflict) = prepared_on_conflict {
            prepared.set_prepared_on_conflict(on_conflict);
        }
        prepared.set_validated_file_keys(validated_keys);
        prepared.append_sequence = append_sequence;
        setup_cleanup.armed = false;
        Ok(prepared)
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
        // With integrity checksums enabled, wrap the JSON payload in a checksum
        // envelope so a corrupt/torn record is detected on recovery instead of
        // parsed as garbage. Off → byte-identical legacy pure-JSON.
        let record_bytes: Vec<u8> = if self.integrity_checksums() {
            super::wal_checksum::frame(content.as_bytes())
        } else {
            content.into_bytes()
        };

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
        file.write_all(&record_bytes).await?;
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
        // See `write_staging_wal_local`: frame with a checksum envelope when
        // integrity checksums are enabled, else write byte-identical legacy JSON.
        let record_bytes: Vec<u8> = if self.integrity_checksums() {
            super::wal_checksum::frame(content.as_bytes())
        } else {
            content.into_bytes()
        };

        let wal_key =
            ObjectStorePath::from(format!("{}{STAGING_WAL_FILENAME}", staging_prefix.as_ref()));
        config
            .store
            .put(&wal_key, record_bytes.into())
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
    /// removal fails, the commit is not reported as complete: callers must retain
    /// the recovery intent and surface the failure.
    pub(crate) async fn remove_staging_wal_for(&self, staging_snapshot_id: &str) -> Result<()> {
        if self.table_path().starts_with("s3://") {
            let config = self.require_object_store()?;
            if let Some(staging_prefix) = self.snapshot_object_store_prefix(staging_snapshot_id)? {
                let wal_key = ObjectStorePath::from(format!(
                    "{}{STAGING_WAL_FILENAME}",
                    staging_prefix.as_ref()
                ));
                match config.store.delete(&wal_key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        if !self.has_inflight_staging_appends() {
                            self.staging_wal_present().store(false, Ordering::Release);
                            self.staging_may_have_files()
                                .store(false, Ordering::Release);
                        }
                    }
                    Err(e) => {
                        return Err(Error::ObjectStore {
                            operation: "remove staging WAL after commit",
                            table: self.table_name().to_string(),
                            source: e,
                        });
                    }
                }
            }
        } else {
            let staging_dir =
                Self::snapshot_dir_path(self.table_path(), self.table_id(), staging_snapshot_id);
            let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
            let removed = match tokio::fs::remove_file(&wal_path).await {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                Err(source) => return Err(Error::IoError { source }),
            };

            if removed {
                if !self.has_inflight_staging_appends() {
                    self.staging_wal_present().store(false, Ordering::Release);
                    self.staging_may_have_files()
                        .store(false, Ordering::Release);
                }
                // No staging-dir fsync after the WAL unlink. The data move's
                // target-snapshot dir fsync (`move_staging_files_local`) already
                // made this commit durable BEFORE this point, so persisting the
                // WAL *unlink* is a recovery-hygiene ordering hint, not a
                // durability barrier — and it bought nothing end-to-end (the
                // next line `remove_dir`s this same directory without a sync
                // anyway). A crash in this window self-heals: recovery's
                // `ensure_no_incomplete_write` audit finds every WAL-listed file
                // already in the target snapshot and re-drives the idempotent
                // (now no-op) move, then removes the stale WAL. Dropping this
                // barrier cuts one ordering-tier `fsync(2)` from EVERY staged
                // commit — a real saving on EBS / provisioned-IOPS, where each
                // barrier is a billed, capped operation. The durable commit
                // boundary (the target-dir fsync) and the recovery substrate
                // (the WAL-content fsync + the post-rename staging-dir fsync)
                // are untouched.
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

    /// Ensure no incomplete write is pending before starting a new write, and
    /// roll any recoverable one forward (or back) so the table is writable.
    ///
    /// Runs on the pre-write gate (under `write_lock`) and at provider open. For
    /// each leftover staging WAL it either self-heals — idempotently completing
    /// the staged file move (roll FORWARD) or discarding an uncommitted staged
    /// append (roll BACK) — or, for a genuinely ambiguous / torn write, refuses
    /// and returns [`Error::IncompleteWrite`] so an operator can intervene.
    ///
    /// ## Why this can never double-publish or roll back a *committed* append
    ///
    /// Finalize normally clears a committed append's WAL itself, so recovery
    /// fires only for interrupted writes (a crash between the durable move and
    /// the WAL removal), the cross-partition coordinator's brief post-commit
    /// window, or a failed best-effort WAL removal. Even when it rolls a
    /// *committed* `ProtectedSnapshot` append forward, a layered invariant keeps
    /// that safe:
    ///
    /// 1. **In-flight registration covers the entire live-finalize window.**
    ///    `CayenneStagedAppend::prepare` registers the append in-flight *before*
    ///    its WAL becomes discoverable, and only `finish()` (which runs *after*
    ///    `apply_*` has completed the move), `rollback()`, or `Drop`
    ///    (cancellation) clears it. The `staging_append_is_inflight` skip below
    ///    therefore excludes any append whose finalize is still live — recovery
    ///    only ever acts on an append whose finalize has already completed, been
    ///    rolled back, or been abandoned.
    /// 2. **`visibility_lock` serializes recovery's move against a concurrent
    ///    finalize's move**, so even at the instant of hand-off there is no torn
    ///    concurrent directory mutation (see the lock note below).
    /// 3. **Roll-forward vs roll-back is decided by the DURABLE snapshot sequence,
    ///    not the provider pointer.** A single-table CDC upsert commits its
    ///    `snapshot_sequence` synchronously in Stage A
    ///    (`commit_on_conflict_deletions_with_tombstone`), which runs *before*
    ///    `finish()`. Since `not-in-flight ⟹ finish() ran (or the receipt was
    ///    dropped)`, a *committed* upsert always has a durable sequence when
    ///    recovery sees its WAL ⟹ it is always rolled FORWARD, never back. An
    ///    append abandoned *before* its sequence commit has no durable sequence
    ///    and is correctly rolled back (it published no rows).
    /// 4. **The roll-forward move is idempotent.**
    ///    `move_staged_files_to_snapshot` renames whatever is in the (uuid-named)
    ///    `_staging/<id>/` dir into the target; once finalize has moved the files
    ///    the staging dir is empty and recovery's move is a no-op — so re-running
    ///    recovery any number of times neither duplicates rows nor loses them.
    ///
    /// # Errors
    ///
    /// Returns [`Error::IncompleteWrite`] when a leftover WAL cannot be safely
    /// self-healed: its target current-snapshot has moved, WAL-listed files are
    /// missing from both staging and the target snapshot, or the WAL removal
    /// after a successful move fails.
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

        let mut recovered_current_snapshot_any = false;
        let mut recovered_protected_snapshots = Vec::new();
        for located_wal in located_wals {
            if self.staging_append_is_inflight(&located_wal.staging_snapshot_id) {
                continue;
            }

            let staging_snapshot_id = located_wal.staging_snapshot_id;
            let wal_location = located_wal.location;
            let table_name = self.table_name().to_string();

            // A checksum-framed record that failed its integrity check is
            // *discarded* rather than parsed as garbage. It cannot be an
            // in-flight append this process staged (that WAL was just written
            // with a valid checksum and is excluded above), so a mismatch means
            // a prior process's torn write or on-disk bit-rot. The staged files
            // were never moved into a snapshot, so removing the staging dir
            // converges to the last committed snapshot. This keeps the table
            // usable (unlike the conservative "refuse all writes" path for
            // genuinely ambiguous cases below), which is only safe *because* the
            // checksum proves the record is untrustworthy.
            let wal = match located_wal.outcome {
                StagingWalOutcome::Parsed(wal) => wal,
                StagingWalOutcome::Corrupt(reason) => {
                    tracing::error!(
                        table = %table_name,
                        location = %wal_location,
                        staging_snapshot_id = %staging_snapshot_id,
                        "Discarding corrupt staging WAL detected by integrity checksum ({reason}); \
                         its staged files were never committed, converging to the last snapshot",
                    );
                    self.clear_staging_snapshot_dir(&staging_snapshot_id)
                        .await?;
                    continue;
                }
            };

            // If this per-partition incomplete write belongs to a cross-partition
            // commit, carry the commit id through every operator-facing recovery
            // error so related partition failures can be correlated.
            let mut extra = String::new();
            let partitioned_table_root =
                std::path::Path::new(self.table_path())
                    .ancestors()
                    .find(|path| {
                        path.join(super::partitioned_wal::PARTITIONED_WAL_DIR)
                            .exists()
                    });
            if let Some(partitioned_table_root) = partitioned_table_root
                && let Ok(all_wals) = PartitionedWal::read_all_in(partitioned_table_root).await
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

            // The provider-local pointer can be stale if cancellation happens
            // while the metastore COMMIT is in flight: both SQLite and Turso
            // may complete that COMMIT after the coordinator future is dropped,
            // before its synchronous publication block runs. Classify protected
            // targets from the durable catalog pointer so recovery never deletes
            // a snapshot that the catalog has committed.
            let provider_snapshot = self.get_current_snapshot_id();
            let durable_snapshot = if wal.target_kind == StagingWalTargetKind::ProtectedSnapshot {
                self.metadata_catalog()
                    .get_table(self.table_name())
                    .await?
                    .current_snapshot_id
            } else {
                provider_snapshot.clone()
            };
            if wal.target_kind == StagingWalTargetKind::CurrentSnapshot
                && provider_snapshot != wal.target_snapshot
            {
                return Err(Error::IncompleteWrite {
                    table: table_name,
                    message: format!(
                        "A previous write was interrupted while moving {} file(s) to '{}' (started at {}), but the current snapshot is now '{}'. Automated recovery refused to avoid moving staged files into the wrong snapshot. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                        wal.staged_files.len(),
                        wal.target_snapshot,
                        wal.created_at,
                        provider_snapshot,
                    ),
                });
            }
            if wal.target_kind == StagingWalTargetKind::ProtectedSnapshot
                && durable_snapshot != wal.target_snapshot
            {
                // The target is NOT the durable current-snapshot pointer. Whether
                // it is committed is decided by its durable snapshot sequence
                // (`cayenne_snapshot_sequence`), NOT the pointer, because the two
                // protected-snapshot producers commit differently:
                //
                //   * A single-table CDC upsert writes its protected target's
                //     sequence SYNCHRONOUSLY in Stage A — before `write_cdc_append_stream`
                //     returns and the CDC source offset advances — and its
                //     writer-free Stage-B publish (`CayenneCdcWrite::finish`) never
                //     moves the current pointer: the target is a merge-on-read
                //     OVERLAY layered on the unchanged current snapshot. Such an
                //     append is COMMITTED. It must be rolled FORWARD, both when a
                //     crash lands between Stage A and Stage B (recovered at reopen)
                //     and when the NEXT write's recovery pass finds a finalized
                //     batch's still-present WAL (Stage B does not remove a
                //     protected-snapshot WAL). Rolling it back deletes durably
                //     committed rows — including rows the upsert did NOT supersede.
                //   * A cross-partition deferred append DEFERS its sequence write
                //     into the coordinator transaction that also flips every
                //     partition's pointer, so a crash before that COMMIT leaves no
                //     durable sequence and the pointer unmoved — genuinely
                //     uncommitted, and correctly rolled back.
                //
                // Durable sequence present => roll FORWARD by falling through to the
                // move + WAL-removal below. The pointer is deliberately NOT advanced:
                // `recovered_protected_snapshots` (which drives
                // `publish_recovered_deferred_snapshot`, and with it `current = target`)
                // stays gated on `durable_snapshot == target`, reserved for the
                // pointer-committed cross-partition case. A CDC-upsert overlay's
                // in-memory visibility is (re)established by the owning `finish()` or,
                // after a crash, by table-open reloading `protected_snapshots` and
                // activating orphan inline tombstones — recovery here only makes the
                // staged files durable in the target dir and clears the WAL.
                let durably_committed = self
                    .metadata_catalog()
                    .get_snapshot_sequence(self.table_id(), &wal.target_snapshot)
                    .await?
                    .is_some();
                if !durably_committed {
                    tracing::warn!(
                        table = table_name.as_str(),
                        target_snapshot = wal.target_snapshot.as_str(),
                        current_snapshot = durable_snapshot.as_str(),
                        "Rolling back an uncommitted deferred snapshot append"
                    );
                    self.clear_staging_snapshot_dir(&staging_snapshot_id)
                        .await?;
                    self.clear_snapshot_dir(&wal.target_snapshot).await?;
                    continue;
                }
                tracing::debug!(
                    table = table_name.as_str(),
                    target_snapshot = wal.target_snapshot.as_str(),
                    current_snapshot = durable_snapshot.as_str(),
                    "Recovering a durably-committed protected-snapshot append forward (merge-on-read overlay; current pointer unchanged)"
                );
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

                let Some(target_prefix) = self
                    .snapshot_object_store_prefix(&wal.target_snapshot)
                    .map_err(|error| Error::IncompleteWrite {
                        table: table_name.clone(),
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}'. Failed to determine the target S3 prefix for the pre-recovery audit ({error}); refusing automated recovery because file reachability is unknown. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot
                        ),
                    })?
                else {
                    return Err(Error::IncompleteWrite {
                        table: table_name.clone(),
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}'. Could not determine the target S3 prefix for the pre-recovery audit; refusing automated recovery because file reachability is unknown. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot
                        ),
                    });
                };

                let mut reachable: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let expected = wal
                    .staged_files
                    .iter()
                    .map(String::as_str)
                    .collect::<std::collections::HashSet<_>>();

                let mut staging_objects = config.store.list(Some(&staging_prefix));
                while let Some(meta) = staging_objects.next().await {
                    let meta = meta.map_err(|error| Error::IncompleteWrite {
                        table: table_name.clone(),
                        message: format!(
                            "A previous write was interrupted while moving {} file(s) to '{}'. Failed to list the S3 staging prefix during the pre-recovery audit ({error}); refusing automated recovery because file reachability is unknown. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                            wal.staged_files.len(),
                            wal.target_snapshot
                        ),
                    })?;
                    if let Some(rel) = meta.location.as_ref().strip_prefix(staging_prefix.as_ref())
                        && expected.contains(rel)
                    {
                        reachable.insert(rel.to_string());
                        if reachable.len() == expected.len() {
                            break;
                        }
                    }
                }

                if reachable.len() != expected.len() {
                    let mut target_objects = config.store.list(Some(&target_prefix));
                    while let Some(meta) = target_objects.next().await {
                        let meta = meta.map_err(|error| Error::IncompleteWrite {
                            table: table_name.clone(),
                            message: format!(
                                "A previous write was interrupted while moving {} file(s) to '{}'. Failed to list the target S3 prefix during the pre-recovery audit ({error}); refusing automated recovery because file reachability is unknown. Manual resolution is required. The WAL file is located at '{wal_location}'.{extra}",
                                wal.staged_files.len(),
                                wal.target_snapshot
                            ),
                        })?;
                        if let Some(rel) =
                            meta.location.as_ref().strip_prefix(target_prefix.as_ref())
                            && expected.contains(rel)
                        {
                            reachable.insert(rel.to_string());
                            if reachable.len() == expected.len() {
                                break;
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
                    // A protected-snapshot target is visible when the durable
                    // catalog pointer already equals it (the committed
                    // cross-partition append case). Refresh the provider's
                    // in-memory pointer/listing just as for a current-snapshot
                    // append; otherwise a long-lived provider would keep serving
                    // its pre-crash snapshot until reopened.
                    if durable_snapshot == wal.target_snapshot {
                        match wal.target_kind {
                            StagingWalTargetKind::CurrentSnapshot => {
                                recovered_current_snapshot_any = true;
                            }
                            StagingWalTargetKind::ProtectedSnapshot => {
                                recovered_protected_snapshots.push(wal.target_snapshot.clone());
                            }
                        }
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

        // A protected target became the durable current snapshot in the shared
        // cross-partition transaction. Rehydrate every catalog-backed
        // visibility input before publishing the pointer/listing under its
        // listing fence; a pointer-only refresh would leave stale deletions and
        // protected-snapshot thresholds after cancellation at COMMIT.
        for snapshot_id in recovered_protected_snapshots {
            self.publish_recovered_deferred_snapshot(&snapshot_id)
                .await?;
        }
        if recovered_current_snapshot_any {
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

    /// Reconcile leftover staged appends after a cross-partition coordinator
    /// crash. The catalog pointer is the durable commit decision: protected
    /// targets equal to the pointer are completed; all others are rolled back.
    ///
    /// # Errors
    ///
    /// Returns an error if a WAL cannot be read, its durable outcome cannot be
    /// classified safely, or committed publication/rollback cannot complete.
    pub async fn recover_incomplete_writes(&self) -> Result<()> {
        let _write_guard = self.write_lock_arc().lock_owned().await;
        self.ensure_no_incomplete_write().await
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
        // Read raw bytes (not a String): a checksum-framed record is binary. The
        // envelope is auto-detected, so both framed and legacy pure-JSON records
        // are handled regardless of the current `integrity_checksums` setting.
        match tokio::fs::read(&wal_path).await {
            Ok(bytes) => match super::wal_checksum::verify(&bytes) {
                Ok(payload) => match serde_json::from_slice::<StagingWal>(payload.bytes()) {
                    Ok(wal) => Ok(Some(LocatedStagingWal {
                        staging_snapshot_id: staging_snapshot_id.to_string(),
                        outcome: StagingWalOutcome::Parsed(wal),
                        location,
                    })),
                    // The bytes are intact (checksum passed, or legacy
                    // unchecksummed) but do not parse as a `StagingWal`. Keep the
                    // conservative refuse-and-flag behavior: the record is not
                    // corrupt, so we must not silently drop a possibly committed
                    // append.
                    Err(e) => Err(Error::IncompleteWrite {
                        table: self.table_name().to_string(),
                        message: format!(
                            "Found unreadable staging WAL at '{location}': {e}. Refusing writes to avoid ignoring a possibly committed staged append. Manual resolution is required."
                        ),
                    }),
                },
                // The checksum envelope failed to verify → corrupt/torn record.
                // Signal it up so recovery discards the staging dir.
                Err(checksum_err) => Ok(Some(LocatedStagingWal {
                    staging_snapshot_id: staging_snapshot_id.to_string(),
                    outcome: StagingWalOutcome::Corrupt(checksum_err.to_string()),
                    location,
                })),
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
            // Auto-detect the checksum envelope (both framed and legacy
            // pure-JSON records are handled regardless of `integrity_checksums`).
            let outcome = match super::wal_checksum::verify(&bytes) {
                Ok(payload) => match serde_json::from_slice::<StagingWal>(payload.bytes()) {
                    Ok(wal) => StagingWalOutcome::Parsed(wal),
                    Err(e) => {
                        // Intact bytes that do not parse as a `StagingWal`:
                        // conservatively refuse (not corruption).
                        return Err(Error::IncompleteWrite {
                            table: self.table_name().to_string(),
                            message: format!(
                                "Found unreadable staging WAL at '{location}': {e}. Refusing writes to avoid ignoring a possibly committed staged append. Manual resolution is required."
                            ),
                        });
                    }
                },
                Err(checksum_err) => StagingWalOutcome::Corrupt(checksum_err.to_string()),
            };
            wals.push(LocatedStagingWal {
                staging_snapshot_id,
                outcome,
                location,
            });
        }

        Ok(wals)
    }
}
