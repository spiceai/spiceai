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
//!
//! # Inlined overwrites
//!
//! A refresh small enough to clear the inline-admission caps skips the Vortex
//! encode entirely and rides in the metastore instead (the same
//! `cayenne_inlined_data` tier the CDC write path uses). This is the common
//! shape for a full refresh of a small dimension table: without it every refresh
//! writes `write_concurrency` tiny Vortex files that no compaction pass will
//! ever merge, because a whole-table replace leaves nothing to consolidate.
//!
//! The inline rows are inserted by the SAME transaction that clears the old
//! inline corpus and flips the snapshot pointer, so the replace is atomic; see
//! [`CayenneCatalog::commit_overwrite_in_txn`]. Everything else in the lifecycle
//! is unchanged — including the (empty) snapshot directory, which is still
//! created so the listing table and old-snapshot GC behave exactly as they do
//! for a zero-row overwrite.
//!
//! Visibility across the commit is the subtle part, because the catalog
//! transaction and the in-memory publish are two atomic units with a scan-sized
//! gap between them:
//! [`CayenneTableProvider::prepublish_inlined_overwrite`] admits the replacement
//! rows BEFORE the transaction runs, and
//! [`CayenneTableProvider::warm_inlined_cache_for_overwrite`] holds the
//! pre-overwrite corpus in memory when the refresh is NOT inlined. Both are
//! documented at length on those methods; between them, a scan in the gap sees
//! either the complete pre-overwrite table or the complete post-overwrite one.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit};

use super::Result;
use super::column_stats::ColumnStatsAccumulator;
use super::mutation_writer::InlineBatchBuffer;
use super::table::{CayenneTableProvider, InlinedOverwritePublish, serialize_batches_to_ipc};
use crate::CayenneCatalog;
use crate::catalog::CatalogResult;
use crate::metadata::InlinedData;
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
    /// `Some` when the overwrite's rows live in the metastore rather than in
    /// Vortex files under `new_snapshot_id` (whose directory is then empty): the
    /// row to insert, atomically with the snapshot flip that clears its
    /// predecessors.
    inlined: Option<InlinedData>,
    /// The context's inline-admission slot, held from the moment the payload is
    /// buffered until it is committed and published — the whole span over which
    /// the buffered batches and the serialized blob are resident.
    _inline_admission: Option<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for PreparedOverwrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedOverwrite")
            .field("table", &self.table.table_name())
            .field("new_snapshot_id", &self.new_snapshot_id)
            .field("row_count", &self.row_count)
            .field("has_write_guard", &self.write_guard.is_some())
            .field("inlined", &self.inlined.is_some())
            .finish_non_exhaustive()
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
    /// Executes the SQL batch from
    /// [`crate::CayenneCatalog::commit_overwrite_in_txn`] — the per-snapshot
    /// delete/insert/sequence tables are cleared, the inlined memtable and
    /// table statistics are dropped (everything keyed on the old snapshot),
    /// and the snapshot pointer is advanced — all against the caller's `txn`
    /// instead of opening a new transaction.
    ///
    /// The atomic inlined-data clear is what differentiates this from
    /// `commit_compaction_in_txn`: scans UNION the listing table with
    /// inlined data, so if the pointer flip committed but a subsequent
    /// (non-transactional) clear failed mid-flight, stale inlined rows
    /// would re-appear in scans of the new snapshot. Bundling them into
    /// the same transaction closes that consistency window.
    ///
    /// The caller owns the transaction lifecycle: this method does not
    /// commit, roll back, or retry. Cross-partition coordinators batch every
    /// participating partition's `apply_in_txn` inside one shared transaction
    /// so the pointer flips happen atomically.
    ///
    /// Single-partition callers can use [`Self::apply_owned_txn`] instead,
    /// which goes through the trait-based [`crate::MetadataCatalog::commit_overwrite`]
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
            .commit_overwrite_in_txn(
                txn,
                self.table_id(),
                &self.new_snapshot_id,
                self.inlined.as_ref(),
            )
            .await
    }

    /// Apply the catalog mutation by opening a dedicated single-partition
    /// transaction.
    ///
    /// Convenience for callers that don't need to batch with other partitions
    /// (e.g. [`super::sink::CayenneDataSink::write_all`] in overwrite mode).
    /// Delegates to [`crate::MetadataCatalog::commit_overwrite`] which opens
    /// its own transaction with retry-on-conflict and atomically clears the
    /// inlined data, inlined deletes, and table statistics along with the
    /// snapshot pointer flip.
    ///
    /// # Errors
    ///
    /// Returns any error surfaced by the catalog's `commit_overwrite`.
    pub async fn apply_owned_txn(&self) -> CatalogResult<()> {
        self.table
            .catalog()
            .commit_overwrite(
                self.table_id(),
                &self.new_snapshot_id,
                self.inlined.as_ref(),
            )
            .await
    }

    /// Publish the new snapshot in memory after the caller's transaction has
    /// committed.
    ///
    /// The catalog-side clears (inlined data, inlined deletes, table stats,
    /// delete files, insert records, snapshot sequences) happen ATOMICALLY
    /// with the snapshot pointer flip inside `apply_in_txn` / `apply_owned_txn`
    /// — see [`crate::CayenneCatalog::commit_overwrite_in_txn`]. This method
    /// only has to sync the in-memory state to match what the catalog now
    /// reflects:
    ///
    /// - Update the in-memory `current_snapshot_id`.
    /// - Clear all deletion caches (the new snapshot has no pending deletions).
    /// - Atomically swap the in-memory `ListingTable` to the new snapshot
    ///   (under [`CayenneTableProvider::listing_fence`] write — §6.4).
    /// - Invalidate the in-memory optimizer cache (the catalog stats row was
    ///   already dropped by `commit_overwrite_in_txn`).
    /// - Persist the new statistics accumulator.
    /// - Trigger background cleanup of old snapshot directories.
    ///
    /// If `finish` itself fails or the process crashes between
    /// `apply_*_txn` and `finish`, the next `CayenneTableProviderBuilder::open`
    /// will reconstruct the same in-memory state from the catalog (which
    /// already reflects the new snapshot), so durability is preserved.
    ///
    /// # Errors
    ///
    /// Returns an error if swapping the listing table fails. Other steps are best-effort.
    pub async fn finish(self) -> Result<u64> {
        // Publish the new snapshot as a single atomic visibility flip under the listing
        // fence (snapshot id + deletion caches + inline cache + listing swap), so a
        // concurrent scan never observes a torn state. Full rationale on
        // `CayenneTableProvider::publish_overwrite_snapshot`.
        //
        // An inlined overwrite's replacement rows move into that same flip: the
        // catalog clear and insert already committed together, so the in-memory
        // inline counters must go from "old corpus" to "these rows" without a
        // window in which the new snapshot is paired with an empty inline view.
        self.table
            .publish_overwrite_snapshot(
                &self.new_snapshot_id,
                self.inlined
                    .as_ref()
                    .map(|inlined| InlinedOverwritePublish {
                        row_count: inlined.record_count,
                        sequence_number: inlined.sequence_number,
                    }),
            )
            .await?;

        // Drain the metastore WAL on the debounced maintenance tick. An inlined
        // overwrite's whole payload is an Arrow IPC BLOB written straight into
        // the metastore, and with the inline auto-checkpoint disabled by default
        // that tick is the only WAL drain — a table refreshed on a schedule would
        // otherwise grow its WAL by one blob per refresh forever. Scheduled here,
        // AFTER the commit: a checkpoint inside the transaction would land an
        // fsync in its WAL-write-locked window.
        self.table.schedule_wal_checkpoint();

        // Manifest snapshot model: the catalog flip is committed, so the OLD
        // snapshot's manifest rows are now dead — prune everything but the new
        // snapshot's rows (authored in `begin_overwrite` with `[S, S]`; an inlined
        // overwrite writes no files and so has no rows of its own, leaving the
        // prune to simply clear the old snapshot's). Deferred
        // to here (post-commit), matching the full-rewrite path: a crash between
        // the flip and this prune leaves only stale rows for a non-live snapshot,
        // which the GC live-set filter already excludes and a scan never reads.
        // Best-effort — a failure cannot resurrect rows or lose the overwrite.
        if let Err(error) = self
            .table
            .prune_snapshot_manifest_to(&self.new_snapshot_id)
            .await
        {
            tracing::warn!(
                table = self.table.table_name(),
                %error,
                new_snapshot_id = self.new_snapshot_id.as_str(),
                "Failed to prune stale snapshot manifest rows after overwrite commit"
            );
        }

        self.table
            .trigger_old_snapshot_cleanup(&self.new_snapshot_id)
            .await;

        // Invalidate the in-memory optimizer cache so a zero-row overwrite
        // leaves the cache empty rather than stale; `persist_table_stats`
        // repopulates it when the accumulator has rows. The catalog row was
        // already cleared atomically with the snapshot pointer flip.
        self.table
            .reset_table_stats_after_overwrite(&self.write_stats_acc)
            .await;

        // Drop the write guard before arming retention below, and after everything
        // above it, so all visibility-related updates happen under exclusive table
        // access while the retention pass — which takes this same lock — can proceed.
        let _ = self.write_guard;

        // Arm retention, the same way an append does (see
        // `AppendMutationWriter::write_prepared_stream`). An overwrite reloads every
        // source row, so a `retention_sql` / `retention_period` predicate has to run
        // again over the new snapshot — otherwise the rows it deletes come straight
        // back on each full refresh and the acceleration keeps data the user asked to
        // be deleted.
        if self.table.has_retention_delete_filters() {
            self.table
                .schedule_post_write_maintenance(None, false, true, 0);
        }

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
        // Hold the per-table write guard for the whole cleanup so another
        // writer can't acquire the lock and start a new commit while the
        // staged snapshot directory is mid-deletion.
        let _write_guard = self.write_guard;

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

/// An overwrite that cleared the inline-admission caps: the metastore row to
/// insert, plus what the caller still needs to build a [`PreparedOverwrite`].
struct BufferedInlineOverwrite {
    data: InlinedData,
    row_count: u64,
    stats: Arc<ColumnStatsAccumulator>,
    admission: OwnedSemaphorePermit,
}

/// Outcome of the overwrite's inline-admission probe.
///
/// An enum rather than `(stream, Option<payload>)` so the inline arm cannot hand
/// back a live-but-already-drained stream that a caller might try to write.
enum OverwriteAdmission {
    /// The whole refresh fits in the metastore. The input stream was consumed to
    /// build this payload; there is nothing left to write.
    Inlined(Box<BufferedInlineOverwrite>),
    /// The refresh goes to Vortex. Carries the original stream reconstituted —
    /// the buffered head replayed ahead of the untouched remainder — so the
    /// caller writes exactly the rows it would have without the probe.
    Fallback(SendableRecordBatchStream),
}

impl CayenneTableProvider {
    /// Buffer the head of an overwrite stream and decide whether the whole thing
    /// can live in the metastore instead of in Vortex files.
    ///
    /// Buffering stops at `inline_max_buffer_bytes`, so the memory cost is
    /// bounded however large the refresh turns out to be, and the buffer is
    /// admitted through the context's single inline slot so N partition children
    /// cannot hold N buffers at once. Inlining is off (`inline_max_rows` /
    /// `inline_max_bytes` = 0) for refresh profiles that should not use the tier,
    /// in which case this is a cheap no-op.
    async fn try_admit_overwrite_inline(
        &self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<OverwriteAdmission> {
        let inline_max_rows = self.context().inline_max_rows();
        let inline_max_bytes = self.context().inline_max_bytes();
        let inline_max_buffer_bytes = self.context().inline_max_buffer_bytes();
        if inline_max_rows == 0 || inline_max_bytes == 0 || !self.inline_overwrite_admissible() {
            return Ok(OverwriteAdmission::Fallback(stream));
        }
        let Some(admission) = self.context().try_acquire_overwrite_inline_admission() else {
            self.track_overwrite_inline_fallback("admission_busy");
            return Ok(OverwriteAdmission::Fallback(stream));
        };

        let mut buffer =
            InlineBatchBuffer::new(stream.schema(), inline_max_rows, inline_max_buffer_bytes);
        while let Some(batch) = StreamExt::next(&mut stream).await {
            buffer.push(batch?);
            if !buffer.should_continue_buffering() {
                break;
            }
        }

        if let Some(reason) = buffer.overflow_reason() {
            self.track_overwrite_inline_fallback(reason);
            return Ok(OverwriteAdmission::Fallback(
                buffer.into_chained_stream(stream),
            ));
        }

        // A zero-row overwrite takes the normal path: it writes no files anyway,
        // and the commit's clear alone is the correct end state (an empty table).
        // An inline entry with no rows would just be a row to read back and drop.
        if buffer.total_rows() == 0 {
            return Ok(OverwriteAdmission::Fallback(
                buffer.into_chained_stream(stream),
            ));
        }

        let ipc_bytes = serialize_batches_to_ipc(buffer.batches())
            .map_err(|e| super::Error::Arrow { source: e })?;
        if ipc_bytes.len() > inline_max_bytes {
            self.track_overwrite_inline_fallback("ipc_bytes_cap");
            return Ok(OverwriteAdmission::Fallback(
                buffer.into_chained_stream(stream),
            ));
        }

        // Past this point the overwrite WILL be inlined. Collect the same column
        // statistics the Vortex write path accumulates, so the published table
        // stats do not depend on which path a refresh took. NDV is included:
        // unlike a CDC delta, this batch IS the whole table, so its distinct
        // counts are exact and there is no later checkpoint to fold them in.
        let stats = Arc::new(ColumnStatsAccumulator::new_with_ndv(
            buffer.schema().as_ref(),
            true,
        ));
        for batch in buffer.batches() {
            stats.update(batch);
        }
        let total_rows = buffer.total_rows();

        // One sequence for the inline row, from the same monotone allocator every
        // other write uses, so the entry sorts above anything the old snapshot
        // held and the inline visibility watermark advances monotonically across
        // overwrites.
        let sequence_number = self.reserve_sequences_local(1).await?;
        let mut data = InlinedData::pending_catalog_insert(
            self.table_id().to_string(),
            None,
            ipc_bytes,
            i64::try_from(total_rows).unwrap_or(i64::MAX),
        );
        // Fixing the identity here (rather than letting the insert mint one per
        // attempt) is what makes a retried `commit_overwrite` idempotent.
        data.inlined_id = uuid::Uuid::now_v7().to_string();
        data.sequence_number = sequence_number;

        Ok(OverwriteAdmission::Inlined(Box::new(
            BufferedInlineOverwrite {
                data,
                row_count: u64::try_from(total_rows).unwrap_or(u64::MAX),
                stats,
                admission,
            },
        )))
    }

    fn track_overwrite_inline_fallback(&self, reason: &'static str) {
        telemetry::cayenne::track_inline_fallback(&[
            telemetry::KeyValue::new("table", self.table_name().to_string()),
            telemetry::KeyValue::new("reason", reason),
        ]);
    }

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
        target_partitions: usize,
    ) -> Result<PreparedOverwrite> {
        let write_guard = self.write_lock_arc().lock_owned().await;

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let is_s3 = self.table_path().starts_with("s3://");

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        // Try to take the whole overwrite into the metastore inline tier. Buffers
        // at most `inline_max_buffer_bytes` before giving up, so a large refresh
        // pays one bounded buffer and then streams to Vortex as before. The
        // snapshot directory stays created-but-empty on the inline path — the
        // same shape a zero-row overwrite already produces.
        let data = match self.try_admit_overwrite_inline(data).await? {
            OverwriteAdmission::Inlined(inlined) => {
                // Admit the replacement rows before the caller's transaction runs,
                // so the gap between that transaction and `finish` cannot serve an
                // empty table. See `prepublish_inlined_overwrite`.
                self.prepublish_inlined_overwrite(InlinedOverwritePublish {
                    row_count: inlined.data.record_count,
                    sequence_number: inlined.data.sequence_number,
                });
                return Ok(PreparedOverwrite {
                    table: self.clone_for_write(),
                    write_guard: Some(write_guard),
                    new_snapshot_id,
                    row_count: inlined.row_count,
                    write_stats_acc: inlined.stats,
                    inlined: Some(inlined.data),
                    _inline_admission: Some(inlined.admission),
                });
            }
            OverwriteAdmission::Fallback(stream) => stream,
        };

        // The commit clears the inline corpus and puts nothing back on this path,
        // so materialize the pre-overwrite view first: it is what a scan landing
        // between the commit and `finish` must still see. See
        // `warm_inlined_cache_for_overwrite`.
        self.warm_inlined_cache_for_overwrite().await;

        let target_size_bytes = self.target_file_size_bytes();
        // Overwrite replaces the entire table, and anything that reached here did
        // not fit the inline caps, so it is large by definition; shard across the
        // full write concurrency (no size cap on the fan-out). Deliberately NOT
        // sized from the bytes the probe buffered: that is only a lower bound, and
        // under-sharding a multi-GB refresh to one writer would serialize the encode.
        let (row_count, _files_written, write_stats_acc) = self
            .write_to_snapshot(
                data,
                target_size_bytes,
                &new_snapshot_id,
                target_partitions,
                None,
                crate::provider::delta_encoding::WritePolicy::MAINTENANCE,
            )
            .await?;

        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::sync_snapshot_dir(&snapshot_dir).await?;
        }

        // Manifest snapshot model: reserve ONE sequence `S` for this overwrite
        // and AUTHOR the new snapshot's manifest with `[S, S]` — every file was
        // written by this single commit, so that range is exact. Reserving `S`
        // also advances the monotonic counter past it, so no later write reuses
        // it. The old snapshot's now-dead manifest rows are pruned in
        // `PreparedOverwrite::finish` (after the catalog flip commits), mirroring
        // the full-rewrite path's publish-before-clear ordering. Best-effort: a
        // failure leaves the scan on directory listing and never loses rows.
        // On reservation failure do NOT fabricate a sequence: authoring a manifest
        // with a bogus `[0, 0]` range would make seq-based decisions (the
        // seq-prefix bake) treat this snapshot as "very old" and risk an unsafe
        // prune. Skip authoring entirely and leave the scan on directory listing.
        match self.reserve_sequences_local(1).await {
            Ok(overwrite_sequence) => {
                if let Err(error) = self
                    .author_uniform_snapshot_manifest(
                        &new_snapshot_id,
                        overwrite_sequence,
                        overwrite_sequence,
                    )
                    .await
                {
                    tracing::warn!(
                        table = self.table_name(),
                        %error,
                        new_snapshot_id = new_snapshot_id.as_str(),
                        "Failed to author overwrite snapshot manifest before commit; \
                         scan falls back to directory listing"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    table = self.table_name(),
                    %error,
                    new_snapshot_id = new_snapshot_id.as_str(),
                    "Failed to reserve a sequence for the overwrite snapshot manifest; \
                     skipping manifest authoring — scan falls back to directory listing"
                );
            }
        }

        Ok(PreparedOverwrite {
            table: self.clone_for_write(),
            write_guard: Some(write_guard),
            new_snapshot_id,
            row_count,
            write_stats_acc,
            inlined: None,
            _inline_admission: None,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Visibility of an overwrite across the gap between its catalog transaction
    //! and its in-memory publish.
    //!
    //! The catalog transaction (clear the inline corpus, insert any replacement
    //! row, flip the durable snapshot pointer) and the in-memory publish (snapshot
    //! id, deletion caches, inline counters, listing table — all under
    //! `listing_fence.write()`) are two atomic units, and a scan holds
    //! `listing_fence.read()`, so it can land between them. These tests drive the
    //! `PreparedOverwrite` lifecycle by hand to put a scan exactly there and assert
    //! it sees a COMPLETE table: either every pre-overwrite row or every
    //! post-overwrite row, never a partial set and above all never zero.
    //!
    //! Zero is the case worth naming. It needs three things at once, all of which
    //! an inlined overwrite of an already-inlined table produces: the pre-overwrite
    //! snapshot directory is empty (its rows lived in the metastore), the catalog's
    //! inline corpus now holds only the replacement row, and the inline read is
    //! taking the metastore rebuild path rather than a warm cache. An empty inline
    //! view unioned with an empty directory is an empty table — reported as a
    //! successful query, which is why this is a correctness test and not a
    //! performance one.

    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DataFusionError;
    use tempfile::TempDir;

    use super::{PreparedOverwrite, SendableRecordBatchStream};
    use crate::metadata::{CreateTableOptions, VortexConfig};
    use crate::provider::context::CayenneContext;
    use crate::provider::table::{CayenneTableProvider, CayenneTableProviderBuilder};
    use crate::{CayenneCatalog, MetadataCatalog};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn id_stream(ids: &[i64]) -> SendableRecordBatchStream {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .expect("batch");
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
        ))
    }

    /// A catalog plus `count` independent Cayenne tables sharing it — the same
    /// shape `cross_partition_overwrite_test.rs` uses to stand in for the
    /// partition children of one dataset. Each table gets its own
    /// `CayenneContext`, so each holds its own inline-admission slot and every
    /// partition takes the inline path; that maximizes the number of tables
    /// sitting in the commit/publish gap at once, which is what these tests are
    /// about.
    async fn setup(count: usize) -> (TempDir, Arc<CayenneCatalog>, Vec<CayenneTableProvider>) {
        let temp_dir = TempDir::new().expect("tempdir");
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let connection_string = format!(
            "sqlite://{}",
            temp_dir.path().join("cayenne.db").to_string_lossy()
        );
        let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog"));
        catalog.init().await.expect("catalog init");

        let ctx = SessionContext::new();
        let mut tables = Vec::with_capacity(count);
        for i in 0..count {
            let provider = CayenneTableProviderBuilder::new(
                Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
                ctx.runtime_env(),
            )
            .create(CreateTableOptions {
                table_name: format!("overwrite_window_{i}"),
                schema: test_schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_dir.to_string_lossy().to_string(),
                partition_column: None,
                // Stock config: the inline-admission caps are non-zero by default,
                // which is what the whole-table-replace refresh profile resolves to.
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("create table");
            tables.push(provider);
        }
        (temp_dir, catalog, tables)
    }

    /// Every id the table currently serves, sorted. Reads through a fresh
    /// `SessionContext` so nothing about the plan is cached between calls.
    async fn scan_ids(provider: &CayenneTableProvider) -> Vec<i64> {
        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(Arc::new(provider.clone_for_write()))
            .expect("read_table")
            .collect()
            .await
            .expect("scan");
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 ids")
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Run a whole overwrite through the single-partition lifecycle.
    async fn overwrite(provider: &CayenneTableProvider, ids: &[i64]) {
        let prepared = provider
            .begin_overwrite(id_stream(ids), 1)
            .await
            .expect("begin_overwrite");
        prepared.apply_owned_txn().await.expect("apply_owned_txn");
        prepared.finish().await.expect("finish");
    }

    async fn inline_row_count(catalog: &CayenneCatalog, provider: &CayenneTableProvider) -> i64 {
        catalog
            .get_inlined_data_count(provider.table_id())
            .await
            .expect("get_inlined_data_count")
    }

    /// The window scan must return one of the two COMPLETE row sets. Anything
    /// else — above all an empty table — is silent data loss.
    fn assert_complete(observed: &[i64], old: &[i64], new: &[i64], context: &str) {
        // Row counts, not the sets themselves: one of these is deliberately
        // larger than the inline-admission cap, and dumping it buries the failure.
        let shape = |ids: &[i64]| match (ids.first(), ids.last()) {
            (Some(first), Some(last)) => format!("{} row(s) {first}..={last}", ids.len()),
            _ => "0 row(s)".to_string(),
        };
        assert!(
            !observed.is_empty(),
            "{context}: a scan between an overwrite's commit and its publish returned ZERO rows; \
             expected either the complete pre-overwrite set ({}) or the complete post-overwrite \
             set ({})",
            shape(old),
            shape(new)
        );
        assert!(
            observed == old || observed == new,
            "{context}: a scan between an overwrite's commit and its publish returned {}, which \
             is neither the complete pre-overwrite set ({}) nor the complete post-overwrite set ({})",
            shape(observed),
            shape(old),
            shape(new)
        );
    }

    /// A small overwrite lands in the metastore instead of writing tiny Vortex
    /// files, and each refresh REPLACES the previous corpus rather than adding to
    /// it — in the catalog and in what a scan returns.
    #[tokio::test]
    async fn small_overwrite_is_inlined_and_replaced() {
        let (_tmp, catalog, tables) = setup(1).await;
        let provider = &tables[0];

        overwrite(provider, &[1, 2, 3, 4, 5]).await;
        assert_eq!(
            inline_row_count(&catalog, provider).await,
            5,
            "a small whole-table replace must inline rather than write Vortex files"
        );
        assert_eq!(scan_ids(provider).await, vec![1, 2, 3, 4, 5]);

        overwrite(provider, &[7, 8]).await;
        assert_eq!(
            inline_row_count(&catalog, provider).await,
            2,
            "each refresh replaces the inline corpus rather than appending to it"
        );
        assert_eq!(
            scan_ids(provider).await,
            vec![7, 8],
            "a survivor of the previous corpus here is silent data corruption"
        );

        // An empty refresh empties the table and leaves nothing inline behind.
        let prepared = provider
            .begin_overwrite(id_stream(&[]), 1)
            .await
            .expect("begin_overwrite");
        prepared.apply_owned_txn().await.expect("apply_owned_txn");
        prepared.finish().await.expect("finish");
        assert_eq!(inline_row_count(&catalog, provider).await, 0);
        assert_eq!(scan_ids(provider).await, Vec::<i64>::new());
    }

    /// Regression for the partition children of a partitioned dataset: they must
    /// never inline, however small the refresh.
    ///
    /// A child carries `partition_column: None` — the partition-column rule reads
    /// the *table's* own metadata, and a partition is not itself partitioned — so
    /// that rule alone leaves every child admissible. But the children share one
    /// `CayenneContext` and therefore its single `try_acquire`-only admission
    /// slot, and they overwrite concurrently under one routing demux, so exactly
    /// one of them would take the slot and inline while its siblings wrote Vortex
    /// files: a whole-table replace split across both tiers, with the inlined
    /// partition decided by whichever task reached admission first. The
    /// coupled-writer flag is what actually bars them.
    ///
    /// The contrast half of this test is the point — both tables run the same
    /// refresh through the same caps on the same catalog, so a failure here is the
    /// flag no longer being consulted, not inlining having been switched off.
    #[tokio::test]
    async fn partition_child_overwrite_is_never_inlined() {
        let (tmp, catalog, tables) = setup(1).await;
        let ordinary = &tables[0];

        let ctx = SessionContext::new();
        let child_context = CayenneContext::new_for_partition_child(
            &VortexConfig::default(),
            ctx.runtime_env(),
            "partitioned_parent",
        );
        assert!(
            child_context.is_coupled_writer(),
            "the partition-child context must be marked coupled — the bar below reads this flag"
        );
        let child = CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .with_context(child_context)
        .create(CreateTableOptions {
            table_name: "partitioned_parent_p0".to_string(),
            schema: test_schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: tmp.path().join("data").to_string_lossy().to_string(),
            // Exactly how the partition creators build a child: a partition is not
            // itself partitioned, so the partition-column rule cannot see this.
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("create partition child table");

        overwrite(&child, &[1, 2, 3, 4, 5]).await;
        assert_eq!(
            inline_row_count(&catalog, &child).await,
            0,
            "a partition child must write Vortex files, not inline: its siblings share one \
             admission slot, so an inlined child leaves the replace split across both tiers with \
             the inlined partition picked by a race"
        );
        assert_eq!(
            scan_ids(&child).await,
            vec![1, 2, 3, 4, 5],
            "taking the Vortex path must still serve the whole replacement set"
        );

        // Same refresh, same caps, same catalog — only the coupled-writer flag differs.
        overwrite(ordinary, &[1, 2, 3, 4, 5]).await;
        assert_eq!(
            inline_row_count(&catalog, ordinary).await,
            5,
            "an unpartitioned table on the same caps must still inline — otherwise this test \
             would pass with inlining disabled outright"
        );
    }

    /// The acceptance test for the commit/publish gap: an inlined overwrite of an
    /// already-inlined table, scanned from inside the gap with a cold inline
    /// cache. Fails (zero rows) without the pre-commit visibility publish in
    /// `begin_overwrite`.
    #[tokio::test]
    async fn scan_between_commit_and_publish_sees_a_complete_table() {
        let (_tmp, catalog, tables) = setup(1).await;
        let provider = &tables[0];

        // Pre-state: inlined, so the snapshot directory this scan will read is
        // EMPTY and the inline corpus is the table's entire contents.
        overwrite(provider, &[1, 2, 3, 4, 5]).await;
        assert_eq!(inline_row_count(&catalog, provider).await, 5);
        assert_eq!(scan_ids(provider).await, vec![1, 2, 3, 4, 5]);

        let prepared = provider
            .begin_overwrite(id_stream(&[7, 8]), 1)
            .await
            .expect("begin_overwrite");
        prepared.apply_owned_txn().await.expect("apply_owned_txn");

        // The gap is only observable when the inline read reaches the metastore.
        // A provider that has served a scan holds a generation-current view and
        // answers from it; a freshly opened one does not. Reproduce the latter —
        // it is what any restart, and any structural cache invalidation, leaves
        // behind — without disturbing the generation, epoch, watermark or counts.
        provider.drop_inlined_cache_for_test();

        let in_window = scan_ids(provider).await;
        assert_complete(&in_window, &[1, 2, 3, 4, 5], &[7, 8], "inlined overwrite");

        prepared.finish().await.expect("finish");
        assert_eq!(scan_ids(provider).await, vec![7, 8]);
    }

    /// The same gap for an overwrite that does NOT inline. Its transaction clears
    /// the inline corpus and puts nothing back, so a rebuild inside the gap sees
    /// an empty corpus; paired with the pre-overwrite snapshot's empty directory
    /// that is again an empty table. Fails without the pre-commit cache warm in
    /// `begin_overwrite`.
    #[tokio::test]
    async fn scan_between_commit_and_publish_of_a_file_backed_overwrite() {
        let (_tmp, catalog, tables) = setup(1).await;
        let provider = &tables[0];

        overwrite(provider, &[1, 2, 3]).await;
        assert_eq!(inline_row_count(&catalog, provider).await, 3);
        assert_eq!(scan_ids(provider).await, vec![1, 2, 3]);

        // Cold inline cache going INTO the overwrite — a freshly opened provider
        // that has not yet served a scan. Unlike the inlined case, this one is
        // dropped before `begin_overwrite` rather than after the commit, because
        // the pre-overwrite rows only exist in the catalog until the commit
        // deletes them: memory is the only place they can come from afterwards,
        // so the overwrite has to capture them on the way in. Losing the cache
        // mid-window is not a hazard this has to survive — that only happens on a
        // restart, which also loses the unpublished overwrite, and the reopened
        // provider reads a consistent catalog.
        provider.drop_inlined_cache_for_test();

        // Past the row-count admission cap, so this one writes Vortex files.
        let big: Vec<i64> = (0..2000).collect();
        let prepared = provider
            .begin_overwrite(id_stream(&big), 1)
            .await
            .expect("begin_overwrite");
        prepared.apply_owned_txn().await.expect("apply_owned_txn");

        let in_window = scan_ids(provider).await;
        assert_complete(&in_window, &[1, 2, 3], &big, "file-backed overwrite");

        prepared.finish().await.expect("finish");
        assert_eq!(scan_ids(provider).await, big);
    }

    #[tokio::test]
    async fn committed_overwrite_invalidates_retired_segments_after_cleanup() {
        let (_tmp, _catalog, tables) = setup(1).await;
        let provider = &tables[0];
        let old: Vec<i64> = (0..2_000).collect();
        overwrite(provider, &old).await;
        assert_eq!(scan_ids(provider).await, old);

        let old_cache_entries = provider
            .context()
            .file_format()
            .segment_cache_entry_count()
            .await
            .expect("the file-backed test enables the segment cache");
        assert!(
            old_cache_entries > 0,
            "the old snapshot scan must populate the cache"
        );
        let old_snapshot = provider.get_current_snapshot_id();

        let rollback_rows: Vec<i64> = (2_000..4_000).collect();
        let rolled_back = provider
            .begin_overwrite(id_stream(&rollback_rows), 1)
            .await
            .expect("begin rolled-back overwrite");
        rolled_back.rollback().await.expect("rollback overwrite");
        assert_eq!(
            provider
                .context()
                .file_format()
                .segment_cache_entry_count()
                .await,
            Some(old_cache_entries),
            "rolling back an unpublished overwrite must not invalidate live inputs"
        );

        let new: Vec<i64> = (4_000..6_000).collect();
        let prepared = provider
            .begin_overwrite(id_stream(&new), 1)
            .await
            .expect("begin committed overwrite");
        prepared.apply_owned_txn().await.expect("commit overwrite");
        assert_eq!(
            provider
                .context()
                .file_format()
                .segment_cache_entry_count()
                .await,
            Some(old_cache_entries),
            "the catalog commit alone must not invalidate a snapshot an in-flight scan can still use"
        );

        prepared.finish().await.expect("publish overwrite");
        provider
            .drain_in_flight_maintenance()
            .await
            .expect("drain post-overwrite maintenance");
        assert_eq!(scan_ids(provider).await, new);
        let entries_before_cleanup = provider
            .context()
            .file_format()
            .segment_cache_entry_count()
            .await
            .expect("segment cache remains enabled");
        assert!(
            entries_before_cleanup > old_cache_entries,
            "scanning the replacement must cache live segments alongside the retired snapshot"
        );

        let current_snapshot = provider.get_current_snapshot_id();
        provider
            .cleanup_old_snapshots_with_protected_now_for_test(
                &current_snapshot,
                HashSet::from([old_snapshot]),
            )
            .await
            .expect("protected cleanup pass");
        assert_eq!(
            provider
                .context()
                .file_format()
                .segment_cache_entry_count()
                .await,
            Some(entries_before_cleanup),
            "protected overwrite inputs must remain cached"
        );
        provider
            .cleanup_old_snapshots_with_protected_now_for_test(&current_snapshot, HashSet::new())
            .await
            .expect("cleanup committed retired snapshots after protection is released");
        let entries_after_cleanup = provider
            .context()
            .file_format()
            .segment_cache_entry_count()
            .await
            .expect("segment cache remains enabled");
        assert!(
            entries_after_cleanup > 0 && entries_after_cleanup < entries_before_cleanup,
            "cleanup must remove retired entries while preserving the replacement snapshot cache"
        );
    }

    /// Repeated inline overwrites must not grow the metastore WAL without bound.
    ///
    /// Every inlined refresh writes an Arrow IPC blob straight into the
    /// metastore, and the inline auto-checkpoint is disabled by default
    /// (`wal_autocheckpoint_pages = 0`), so the debounced maintenance tick is the
    /// only thing that ever drains the WAL. A refresh that queues no maintenance
    /// queues no drain either, and a table refreshed on a schedule then
    /// accumulates one un-drained blob per refresh forever.
    /// Asserted as a PLATEAU rather than an absolute size: a drained WAL keeps
    /// reusing the space it reclaims, so its file settles at the high-water mark
    /// of the traffic rather than at any particular byte count, while an
    /// un-drained one grows with the refresh count. Tripling the refreshes and
    /// requiring the file not to double separates the two cleanly.
    #[tokio::test]
    async fn repeated_inline_overwrites_keep_the_wal_bounded() {
        /// Refreshes before the baseline measurement; 3x that many after it.
        const SETTLE_REFRESHES: usize = 16;
        const ROWS: i64 = 1000;

        let (tmp, catalog, tables) = setup(1).await;
        let provider = &tables[0];
        let wal_path = tmp.path().join("cayenne.db-wal");
        let ids: Vec<i64> = (0..ROWS).collect();

        let refresh = async |count: usize| {
            for _ in 0..count {
                overwrite(provider, &ids).await;
                // Drain synchronously so the measurement is the steady state
                // rather than a race with the 100 ms debounce.
                provider
                    .flush_pending_maintenance()
                    .await
                    .expect("flush_pending_maintenance");
            }
            std::fs::metadata(&wal_path).map_or(0, |m| m.len())
        };

        let settled = refresh(SETTLE_REFRESHES).await;
        let after = refresh(SETTLE_REFRESHES * 3).await;
        assert_eq!(inline_row_count(&catalog, provider).await, ROWS);

        assert!(
            after <= settled.saturating_mul(2),
            "the metastore WAL grew from {settled} to {after} bytes when the refresh count \
             tripled; with the inline auto-checkpoint off, a WAL that tracks the refresh count \
             means the overwrite never scheduled a drain"
        );
    }

    /// The partitioned shape: the coordinator commits every partition in ONE
    /// transaction and only then publishes them one at a time, so the gap spans
    /// all of them and each partition sits in it for a different span. Every
    /// partition must still read as a complete table throughout.
    #[tokio::test]
    async fn cross_partition_overwrite_window_sees_complete_partitions() {
        const PARTITIONS: usize = 3;
        let (_tmp, catalog, tables) = setup(PARTITIONS).await;

        let old_ids: Vec<Vec<i64>> = (0..PARTITIONS)
            .map(|p| {
                let base = i64::try_from(p).expect("partition index fits i64") * 100;
                vec![base + 1, base + 2]
            })
            .collect();
        let new_ids: Vec<Vec<i64>> = (0..PARTITIONS)
            .map(|p| {
                let base = i64::try_from(p).expect("partition index fits i64") * 100;
                vec![base + 7, base + 8, base + 9]
            })
            .collect();

        for (provider, ids) in tables.iter().zip(&old_ids) {
            overwrite(provider, ids).await;
            assert_eq!(inline_row_count(&catalog, provider).await, 2);
        }

        let mut prepared: Vec<PreparedOverwrite> = Vec::with_capacity(PARTITIONS);
        for (provider, ids) in tables.iter().zip(&new_ids) {
            prepared.push(
                provider
                    .begin_overwrite(id_stream(ids), 1)
                    .await
                    .expect("begin_overwrite"),
            );
        }

        // One shared transaction: every partition's pointer flip is atomic with
        // every other's, and none of them has published yet.
        let mut txn = catalog
            .begin_transaction()
            .await
            .expect("begin_transaction");
        for prep in &prepared {
            prep.apply_in_txn(&catalog, &mut *txn)
                .await
                .expect("apply_in_txn");
        }
        txn.commit().await.expect("shared txn commit");

        for provider in &tables {
            provider.drop_inlined_cache_for_test();
        }

        // Publish one partition at a time, re-reading every partition after each
        // step: the partitions behind the cursor are published, the ones ahead are
        // still in the gap, and all of them must read complete.
        for published in 0..=PARTITIONS {
            for (p, provider) in tables.iter().enumerate() {
                let observed = scan_ids(provider).await;
                assert_complete(
                    &observed,
                    &old_ids[p],
                    &new_ids[p],
                    &format!("partition {p} with {published} partition(s) published"),
                );
                if p < published {
                    assert_eq!(
                        observed, new_ids[p],
                        "partition {p} must serve its replacement rows once published"
                    );
                }
            }
            if published < PARTITIONS {
                prepared.remove(0).finish().await.expect("finish");
            }
        }

        for (p, provider) in tables.iter().enumerate() {
            assert_eq!(scan_ids(provider).await, new_ids[p]);
        }
    }
}
