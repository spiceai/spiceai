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

//! Append-side mutation writer for [`CayenneTableProvider`].
//!
//! `AppendMutationWriter` owns the logic that turns a `SendableRecordBatchStream`
//! into either an inline-memtable update (small writes, no blocking config) or a
//! staged Vortex write. Two entry points:
//!
//! - [`AppendMutationWriter::write`] — the synchronous append path used by
//!   `DataFusion`'s `INSERT INTO` and by CDC fallback. Runs prepare →
//!   try-inline-or-stage → optional on-conflict deletion vectors → optional
//!   retention/sort → schedule post-write maintenance (debounced refresh +
//!   stats + compaction).
//! - [`AppendMutationWriter::write_cdc_pipelined`] — the CDC fast path. Stage A
//!   writes Vortex files into the staging dir and returns a [`super::table::CayenneCdcWrite`]
//!   that owns the staging-WAL receipt and the still-held per-table write
//!   guard. The runtime spawns Stage B on a background task so the next CDC
//!   burst can begin while burst N's catalog/listing finalization is in flight.
//!
//! ## Pipelined vs. synchronous routing
//!
//! `write_cdc_pipelined` short-circuits to the synchronous `write_prepared_stream`
//! path when the table has pending PK deletions or is partitioned — those need
//! state held until the visibility flip is durable and can't be deferred to
//! Stage B.
//!
//! On-conflict (upsert) tables DO pipeline: the burst stages into a new
//! protected snapshot and the on-conflict deletions are resolved and published
//! by the backgrounded `finish()`. Batches that replace *inlined* rows pipeline
//! too (Option D): `prepare_on_conflict_deletions_for_staged_snapshot` writes
//! the inline tombstone durably with `published = false`, the read filter skips
//! unpublished tombstones, and `finish()` flips the flag durably under the
//! listing fence before the replacement files become discoverable — so the old
//! inline row stays visible until, and is hidden exactly when, the replacement
//! appears (no transient vanish, and no synchronous-publish fallback).
//!
//! ## Inline-memtable admission
//!
//! `try_inline_or_restream` buffers up to
//! [`crate::metadata::VortexConfig::inline_max_buffer_bytes`] of Arrow data and
//! checks the per-write admission gate (`inline_max_rows`, `inline_max_bytes`).
//! If it fits, the batch is serialized to Arrow IPC and inserted into the
//! metastore's `cayenne_inlined_data` table. Otherwise the buffered batches are
//! restreamed into the regular staged-write path.
//!
//! The cumulative memtable flush thresholds (`inline_flush_max_*` on
//! `VortexConfig`) are evaluated by
//! [`super::table::CayenneTableProvider::checkpoint_inlined_data_if_memtable_pressure_exceeded`]
//! after every inline insert, and trigger a checkpoint to a Vortex file when
//! exceeded.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{SendableRecordBatchStream, execute_stream};
use futures::StreamExt;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::OwnedMutexGuard;

use super::Result;
use super::context::CayenneContext;
use super::staging_wal::{CayenneStagedAppend, PreparedStagedAppend, StagingWalTargetKind};
use super::table::{
    CayenneCdcWrite, CayenneTableProvider, ColumnStatsAccumulator, PostValidationState,
    record_cayenne_write_phase,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InlineMutationPolicy {
    Inline,
    Vortex,
}

impl InlineMutationPolicy {
    #[must_use]
    pub(crate) fn from_blocking_conditions(blocking_conditions: [bool; 4]) -> Self {
        if blocking_conditions.into_iter().any(|condition| condition) {
            Self::Vortex
        } else {
            Self::Inline
        }
    }

    #[must_use]
    pub(crate) fn can_inline(self) -> bool {
        matches!(self, Self::Inline)
    }
}

#[derive(Debug)]
pub(crate) struct InlineBatchBuffer {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    max_rows: usize,
    max_buffer_bytes: usize,
    total_rows: usize,
    total_bytes: usize,
    exceeded: bool,
}

impl InlineBatchBuffer {
    #[must_use]
    pub(crate) fn new(schema: SchemaRef, max_rows: usize, max_buffer_bytes: usize) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            max_rows,
            max_buffer_bytes,
            total_rows: 0,
            total_bytes: 0,
            exceeded: false,
        }
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) {
        self.total_rows = self.total_rows.saturating_add(batch.num_rows());
        self.total_bytes = self
            .total_bytes
            .saturating_add(batch.get_array_memory_size());
        self.batches.push(batch);
        self.exceeded = self.total_rows > self.max_rows || self.total_bytes > self.max_buffer_bytes;
    }

    #[must_use]
    pub(crate) fn should_continue_buffering(&self) -> bool {
        !self.exceeded
    }

    #[must_use]
    pub(crate) fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// In-memory Arrow bytes buffered so far. When the buffer overflows and the
    /// write falls back to a Vortex write, this is a *lower bound* on the
    /// delta's total size (the un-buffered remainder of the stream is added on
    /// top). It is used only to size the write shard count, where under-counting
    /// is the safe direction: it can only keep the write on fewer (never more)
    /// shards than the true size warrants.
    #[must_use]
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[must_use]
    pub(crate) fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub(crate) fn into_chained_stream(
        self,
        remaining_stream: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let buffered_exec =
            MemorySourceConfig::try_new_exec(&[self.batches], Arc::clone(&self.schema), None)?;
        let buffered_stream = execute_stream(buffered_exec, Arc::clone(context))?;
        let chained_stream = Box::pin(StreamExt::chain(buffered_stream, remaining_stream));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema,
            chained_stream,
        )))
    }
}

enum InlineMutationOutcome {
    Inlined {
        rows: u64,
        post_validation: PostValidationState,
    },
    Fallback {
        stream: SendableRecordBatchStream,
        /// Lower bound on the delta's byte size: the bytes buffered before the
        /// inline buffer overflowed. Threaded into the snapshot write as its
        /// size estimate so small (but non-inlinable) deltas stay a single file.
        buffered_bytes: u64,
    },
}

struct PreparedStagedAppendTarget {
    staging_snapshot_id: String,
    target_snapshot_id: String,
    target_kind: StagingWalTargetKind,
    estimated_bytes: Option<u64>,
}

fn take_post_validation(
    post_validation: &Arc<ParkingMutex<Option<PostValidationState>>>,
) -> PostValidationState {
    post_validation.lock().take().unwrap_or_default()
}

fn restore_post_validation(
    post_validation: &Arc<ParkingMutex<Option<PostValidationState>>>,
    state: PostValidationState,
) {
    *post_validation.lock() = Some(state);
}

pub(super) struct AppendMutationWriter<'a> {
    table: &'a CayenneTableProvider,
    context: &'a Arc<CayenneContext>,
    task_context: &'a Arc<TaskContext>,
}

impl<'a> AppendMutationWriter<'a> {
    #[must_use]
    pub(super) fn new(
        table: &'a CayenneTableProvider,
        context: &'a Arc<CayenneContext>,
        task_context: &'a Arc<TaskContext>,
    ) -> Self {
        Self {
            table,
            context,
            task_context,
        }
    }

    pub(super) async fn write_cdc_pipelined(
        &self,
        data: SendableRecordBatchStream,
        write_guard: OwnedMutexGuard<()>,
    ) -> Result<CayenneCdcWrite> {
        self.table.ensure_no_incomplete_write().await?;
        let write_start = Instant::now();

        let pending_pk_deletions = !self.table.pk_deletion_strategy().is_position_based()
            && self.table.has_pending_deletions();

        let prepared = self.table.prepare_stream_for_insert(data).await?;
        let post_validation = prepared.post_validation();
        let may_have_on_conflict_deletions = prepared.may_have_on_conflict_deletions();
        let mut prepared_stream = prepared.stream;

        // Retention used to block the pipelined path because it ran inline
        // under `write_lock`. Now that retention is scheduled via
        // `PostWriteMaintenance`, the pipelined path can run for retention-
        // configured tables — the bg scheduler picks up the retention request
        // after publish (see `CayenneCdcWrite::finish`).
        //
        // On-conflict upserts always *attempt* to stage: the Vortex files are
        // written into a staging snapshot and whether a batch actually replaces
        // inlined rows is only known after validation, so we stage optimistically.
        // A batch that replaces inlined rows ALSO publishes from the background
        // (Option D — the durable per-tombstone activation flag):
        // `prepare_on_conflict_deletions_for_staged_snapshot` writes the inline
        // tombstone with `published = false` at a `delete_sequence` below the
        // staged `snapshot_sequence`; the read filter (`load_inlined_deletion_maps`)
        // skips unpublished tombstones, so an inline-cache rebuild triggered by a
        // concurrent same-table inline INSERT during the staged window cannot hide
        // the old inline row (no transient vanish). `CayenneCdcWrite::finish` flips
        // the flag durably under the listing fence, BEFORE the replacement files
        // are moved into the snapshot, then bumps the inline generation — so live
        // readers see the old row until exactly the moment the replacement appears.
        // The previous unconditional synchronous inline-fallback is removed; the
        // only synchronous resort left is the hard error when staging genuinely
        // cannot complete.
        //
        // A table that already holds pending PK deletions (`pending_pk_deletions`)
        // no longer forces the blocking synchronous path. Such a batch stages into
        // a ProtectedSnapshot whose deletion threshold (`snapshot_sequence`) is
        // reserved at stage time ABOVE the current max delete sequence, so the
        // replacement rows in the new snapshot apply only deletes with
        // `delete_seq > snapshot_sequence` — they are immune to every pre-existing
        // tombstone and can neither resurface nor vanish (see
        // `prepare_on_conflict_deletions_for_staged_snapshot` and
        // `process_stream_into_keyset`). Partitioned tables still take the blocking
        // path: their visibility flip can't be deferred to a backgrounded publish.
        let can_stage_for_pipeline = self.table.metadata().partition_column.is_none();

        if !can_stage_for_pipeline {
            let _write_guard = write_guard;
            let rows = self
                .write_prepared_stream(
                    prepared_stream,
                    post_validation,
                    pending_pk_deletions,
                    may_have_on_conflict_deletions,
                )
                .await?;
            tracing::debug!(
                table = self.table.table_name(),
                rows,
                duration_ms = write_start.elapsed().as_millis(),
                inlined = false,
                "CDC pipelined append completed on synchronous path"
            );
            // End-to-end Cayenne-write wall-clock for the synchronous (non-pipelined)
            // path, labeled by path. For this path publish runs inline, so this IS
            // the full slot-apply→publish-complete latency. The gap between this
            // `total` and the sum of the named sub-phases (apply_on_conflict_deletions
            // + vortex_write + publish) is the currently-unmeasured prepare/validation
            // + lock-wait + fsync cost.
            record_cayenne_write_phase(
                self.table.table_name(),
                "cdc_path_synchronous",
                write_start,
            );
            return Ok(CayenneCdcWrite::completed(
                self.table.clone_for_write_operations(),
                rows,
            ));
        }

        match self
            .try_inline_or_restream(prepared_stream, &post_validation)
            .await?
        {
            InlineMutationOutcome::Inlined {
                rows,
                post_validation,
            } => {
                self.table
                    .record_inlined_pk_keys(&post_validation.validated_keys);
                tracing::debug!(
                    table = self.table.table_name(),
                    rows,
                    inlined = true,
                    "CDC pipelined append completed as inlined write"
                );
                record_cayenne_write_phase(
                    self.table.table_name(),
                    "cdc_path_inlined",
                    write_start,
                );
                Ok(CayenneCdcWrite::completed(
                    self.table.clone_for_write_operations(),
                    rows,
                ))
            }
            InlineMutationOutcome::Fallback {
                stream,
                buffered_bytes,
            } => {
                prepared_stream = stream;
                let estimated_bytes = Some(buffered_bytes);
                // Stage into a ProtectedSnapshot whenever the batch may carry
                // on-conflict deletions OR the table already holds pending PK
                // deletions. The latter case may produce no new delete payload
                // (a non-upsert append into a table that has tombstones), but it
                // still needs a ProtectedSnapshot so the new rows get a sequence
                // (`snapshot_sequence`) reserved above the existing tombstones and
                // are therefore not hidden by them — a plain current-snapshot
                // append could let an existing tombstone mask a freshly appended
                // row at the same PK. `prepare_on_conflict_deletions_for_staged_snapshot`
                // handles the empty-delete case (reserve 1 sequence, publish a
                // bare ProtectedSnapshot).
                let stage_on_conflict = may_have_on_conflict_deletions || pending_pk_deletions;
                let (staging_snapshot_id, target_snapshot_id, target_kind) = if stage_on_conflict {
                    let (staging_snapshot_id, target_snapshot_id) =
                        CayenneTableProvider::new_staging_snapshot_id_pair();
                    (
                        staging_snapshot_id,
                        target_snapshot_id,
                        StagingWalTargetKind::ProtectedSnapshot,
                    )
                } else {
                    (
                        CayenneTableProvider::new_staging_snapshot_id(),
                        self.table.get_current_snapshot_id(),
                        StagingWalTargetKind::CurrentSnapshot,
                    )
                };
                let target_size_bytes = self.context.target_file_size_bytes();
                self.table
                    .clear_staging_snapshot_dir(&staging_snapshot_id)
                    .await?;

                let (write_guard_for_prepare, held_write_guard) = if stage_on_conflict {
                    (None, Some(write_guard))
                } else {
                    (Some(write_guard), None)
                };

                let (rows, writer_ops, stats_acc, prepared_append) = self
                    .write_staged_append_prepared(
                        prepared_stream,
                        target_size_bytes,
                        write_guard_for_prepare,
                        PreparedStagedAppendTarget {
                            staging_snapshot_id,
                            target_snapshot_id: target_snapshot_id.clone(),
                            target_kind,
                            estimated_bytes,
                        },
                    )
                    .await?;

                let PostValidationState {
                    on_conflict_deletions,
                    validated_keys,
                } = take_post_validation(&post_validation);

                // Inline-conflict batches now STAGE inert (Option D), exactly like
                // file-conflict batches: `prepare_on_conflict_deletions_for_staged_snapshot`
                // writes the inline tombstone durably with `published = false` at a
                // `delete_sequence` reserved below the staged `snapshot_sequence`,
                // and the read filter (`load_inlined_deletion_maps`) skips
                // unpublished tombstones, so the old inline row stays visible
                // throughout the staged window even if a concurrent same-table
                // inline INSERT triggers an inline-cache rebuild. The owning
                // snapshot's finalize (`CayenneCdcWrite::finish`) flips the flag
                // durably — before the replacement files become discoverable — so
                // the old row is hidden exactly when the replacement appears (no
                // transient vanish). The previous unconditional synchronous
                // inline-fallback (which blocked publish under the write guard) is
                // gone; the only remaining synchronous resort is the hard error
                // path below when staging genuinely cannot complete.
                let prepared_on_conflict = if stage_on_conflict {
                    match self
                        .table
                        .prepare_on_conflict_deletions_for_staged_snapshot(
                            on_conflict_deletions,
                            target_snapshot_id,
                        )
                        .await
                    {
                        Ok(prepared_on_conflict) => Some(prepared_on_conflict),
                        Err(err) => {
                            if let Err(cleanup_err) = prepared_append.rollback().await {
                                tracing::warn!(
                                    "Failed to rollback staged append after on-conflict metadata error for table {}: {cleanup_err}",
                                    self.table.table_name(),
                                );
                            }
                            return Err(err.into());
                        }
                    }
                } else {
                    None
                };

                if stage_on_conflict {
                    self.table.record_file_pk_keys(&validated_keys);
                }
                drop(held_write_guard);

                tracing::debug!(
                    table = self.table.table_name(),
                    rows,
                    writer_ops,
                    duration_ms = write_start.elapsed().as_millis(),
                    inlined = false,
                    "CDC pipelined append staged; WAL is durable, publish/finalize is pending"
                );
                // Time to durable WAL + return on the staged (pipelined) path.
                // NOTE: publish/finalize is backgrounded here, so unlike
                // `cdc_path_synchronous` this does NOT include publish — it is
                // the staged-prepare latency, not full end-to-end.
                record_cayenne_write_phase(self.table.table_name(), "cdc_path_staged", write_start);

                if let Some(prepared_on_conflict) = prepared_on_conflict {
                    Ok(CayenneCdcWrite::prepared_upsert_append(
                        self.table.clone_for_write_operations(),
                        rows,
                        prepared_append,
                        prepared_on_conflict,
                        stats_acc,
                        validated_keys,
                    ))
                } else {
                    Ok(CayenneCdcWrite::prepared_append(
                        self.table.clone_for_write_operations(),
                        rows,
                        prepared_append,
                        stats_acc,
                        validated_keys,
                    ))
                }
            }
        }
    }

    pub(super) async fn write(&self, data: SendableRecordBatchStream) -> Result<u64> {
        self.table.ensure_no_incomplete_write().await?;

        let pending_pk_deletions = !self.table.pk_deletion_strategy().is_position_based()
            && self.table.has_pending_deletions();

        if pending_pk_deletions {
            tracing::debug!(
                "Table {} has pending PK-based deletions, will write to new snapshot",
                self.table.table_name()
            );
        }

        let prepared = self.table.prepare_stream_for_insert(data).await?;
        let post_validation = prepared.post_validation();
        let may_have_on_conflict_deletions = prepared.may_have_on_conflict_deletions();
        let prepared_stream = prepared.stream;

        self.write_prepared_stream(
            prepared_stream,
            post_validation,
            pending_pk_deletions,
            may_have_on_conflict_deletions,
        )
        .await
    }

    async fn write_prepared_stream(
        &self,
        mut prepared_stream: SendableRecordBatchStream,
        post_validation: Arc<ParkingMutex<Option<PostValidationState>>>,
        pending_pk_deletions: bool,
        may_have_on_conflict_deletions: bool,
    ) -> Result<u64> {
        let has_on_conflict_deletions = may_have_on_conflict_deletions;

        tracing::debug!(
            "write_all_append: pending_deletions={}, on_conflict_deletions_possible={}",
            pending_pk_deletions,
            has_on_conflict_deletions
        );

        let inline_policy = InlineMutationPolicy::from_blocking_conditions([
            false,
            false,
            self.table.metadata().partition_column.is_some(),
            self.table.has_retention_delete_filters(),
        ]);

        // Lower-bound size estimate for the staged write. Populated from the
        // inline buffer when we attempt to inline; left `None` when inlining is
        // skipped (partition/retention tables) so the write keeps the prior
        // full-fan-out behavior.
        let mut estimated_bytes: Option<u64> = None;

        if inline_policy.can_inline() {
            match self
                .try_inline_or_restream(prepared_stream, &post_validation)
                .await?
            {
                InlineMutationOutcome::Inlined {
                    rows,
                    post_validation,
                } => {
                    self.table
                        .record_inlined_pk_keys(&post_validation.validated_keys);
                    return Ok(rows);
                }
                InlineMutationOutcome::Fallback {
                    stream,
                    buffered_bytes,
                } => {
                    prepared_stream = stream;
                    estimated_bytes = Some(buffered_bytes);
                }
            }
        }

        let needs_new_snapshot = pending_pk_deletions || may_have_on_conflict_deletions;

        // `superseded` = existing rows replaced by this upsert (deleted as part
        // of the conflict resolution). The live-row delta is `inserted -
        // superseded`, which keeps the metastore `num_rows` tracking COUNT(*)
        // under CDC upsert instead of summing every insert.
        let (total_rows, write_stats_acc, validated_keys, superseded) = if needs_new_snapshot {
            let new_snapshot_start = Instant::now();
            let (rows, stats_acc, validated_keys, superseded) = self
                .write_new_snapshot_after_validation(prepared_stream, &post_validation)
                .await?;
            tracing::debug!(
                table = self.table.table_name(),
                rows,
                superseded,
                duration_ms = new_snapshot_start.elapsed().as_millis(),
                "New snapshot write and publish completed"
            );
            (rows, stats_acc, validated_keys, superseded)
        } else {
            let target_size_bytes = self.context.target_file_size_bytes();
            let write_start = Instant::now();
            let (rows, writer_ops, stats_acc) = self
                .write_staged_append(prepared_stream, target_size_bytes, estimated_bytes)
                .await?;

            tracing::debug!(
                table = self.table.table_name(),
                rows,
                writer_ops,
                duration_ms = write_start.elapsed().as_millis(),
                "Insert completed"
            );

            let PostValidationState {
                on_conflict_deletions,
                validated_keys,
            } = take_post_validation(&post_validation);

            let superseded = on_conflict_deletions.total_superseded();
            let update = self
                .table
                .apply_on_conflict_deletions(on_conflict_deletions)
                .await?;
            // Publish any deletion-cache update under the consistency lock. This path writes no protected snapshot
            self.table.commit_on_conflict_publish(update, None).await;

            (rows, stats_acc, validated_keys, superseded)
        };

        let retention_requested = self.table.has_retention_delete_filters();

        let live_rows_delta = i64::try_from(total_rows)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(write_stats_acc),
            needs_new_snapshot,
            retention_requested,
            live_rows_delta,
        );

        if retention_requested {
            // Retention runs asynchronously after this write returns; its delete
            // outcome is not yet known. Clearing the cache is the conservative
            // path — any subsequent insert pays one fresh disk-scan to rebuild
            // (vs the existing pre-fix logic, which read the inline delete
            // count and cleared only when retention had actually deleted rows).
            self.table.clear_cached_pk_keyset();
        } else {
            self.table.record_file_pk_keys(&validated_keys);
        }

        Ok(total_rows)
    }

    async fn write_new_snapshot_after_validation(
        &self,
        prepared_stream: SendableRecordBatchStream,
        post_validation: &Arc<ParkingMutex<Option<PostValidationState>>>,
    ) -> Result<(
        u64,
        Arc<ColumnStatsAccumulator>,
        std::collections::HashSet<arrow_row::OwnedRow>,
        usize,
    )> {
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let target_size_bytes = self.context.target_file_size_bytes();
        let write_start = Instant::now();
        let (rows, writer_ops, stats_acc) = self
            .table
            .write_to_snapshot(
                prepared_stream,
                target_size_bytes,
                &new_snapshot_id,
                self.task_context.session_config().target_partitions(),
                // This pending-deletions / on-conflict new-snapshot path does not
                // pre-buffer the delta, so its size is unknown here; keep the
                // prior full-fan-out behavior. (Reached only when a write carries
                // pending PK deletes or on-conflict upserts.)
                None,
                crate::provider::delta_encoding::WriteClass::Delta,
            )
            .await?;
        record_cayenne_write_phase(self.table.table_name(), "vortex_write", write_start);
        tracing::trace!(
            table = self.table.table_name(),
            new_snapshot_id,
            rows,
            writer_ops,
            duration_ms = write_start.elapsed().as_millis(),
            "Write to new snapshot completed"
        );

        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = take_post_validation(post_validation);

        let superseded = on_conflict_deletions.total_superseded();
        // Acquiring the visibility lock + listing fence serializes this table's
        // commits; under concurrent upserts `publish_lock_wait` is the contention
        // signal (commits queueing). Split it out so it is not hidden inside the
        // deletion-apply phase below — apply_on_conflict_deletions now measures
        // only the merge/delete work, not the wait for these locks.
        let lock_start = Instant::now();
        let _visibility = self.table.visibility_lock_arc().lock_owned().await;
        let _fence = self.table.lock_listing_fence_write_owned().await;
        record_cayenne_write_phase(self.table.table_name(), "publish_lock_wait", lock_start);

        let deletion_start = Instant::now();
        let update = self
            .table
            .apply_on_conflict_deletions(on_conflict_deletions)
            .await?;
        record_cayenne_write_phase(
            self.table.table_name(),
            "apply_on_conflict_deletions",
            deletion_start,
        );

        // `publish` is the metastore finalization total; the sub-phases attribute
        // it — `publish_seq` is sequence allocation + the durable sequence record,
        // `publish_cas` is the atomic deletion-cache + protected-snapshot publish.
        let publish_start = Instant::now();
        let seq_start = Instant::now();
        let new_sequence = self
            .table
            .catalog()
            .increment_sequence_number(self.table.table_id())
            .await?;

        // Durably record the new snapshot's sequence before making it visible.
        self.table
            .record_written_snapshot_sequence(&new_snapshot_id, new_sequence)
            .await?;
        record_cayenne_write_phase(self.table.table_name(), "publish_seq", seq_start);
        // Atomically publish the deletion-cache update and the protected snapshot
        // so concurrent scans never observe the new protected snapshot with a stale deletion view (the duplicate-PK window).
        let cas_start = Instant::now();
        self.table
            .commit_on_conflict_publish(update, Some((&new_snapshot_id, new_sequence)))
            .await;
        record_cayenne_write_phase(self.table.table_name(), "publish_cas", cas_start);
        record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);

        Ok((rows, stats_acc, validated_keys, superseded))
    }

    async fn try_inline_or_restream(
        &self,
        mut prepared_stream: SendableRecordBatchStream,
        post_validation: &Arc<ParkingMutex<Option<PostValidationState>>>,
    ) -> Result<InlineMutationOutcome> {
        let schema = prepared_stream.schema();
        let mut buffer = InlineBatchBuffer::new(
            Arc::clone(&schema),
            self.context.inline_max_rows(),
            self.context.inline_max_buffer_bytes(),
        );

        while let Some(batch) = StreamExt::next(&mut prepared_stream).await {
            buffer.push(batch?);
            if !buffer.should_continue_buffering() {
                break;
            }
        }

        if buffer.should_continue_buffering() {
            let state = take_post_validation(post_validation);

            if buffer.total_rows() == 0 {
                return Ok(InlineMutationOutcome::Inlined {
                    rows: 0,
                    post_validation: state,
                });
            }

            if self
                .table
                .try_inline_batches_with_inlined_deletions(
                    buffer.batches(),
                    &state.on_conflict_deletions.deleted_inlined_pk_i64,
                    &state.on_conflict_deletions.deleted_inlined_row_keys,
                    &state.on_conflict_deletions.deleted_pk_i64,
                    &state.on_conflict_deletions.deleted_row_keys,
                )
                .await?
            {
                let stats_acc = ColumnStatsAccumulator::new(&schema);
                for batch in buffer.batches() {
                    stats_acc.update(batch);
                }

                // Net live-row delta: inlined inserts minus rows superseded by
                // this inline upsert (across inlined + file-backed deletes).
                let superseded = state.on_conflict_deletions.total_superseded();
                let live_rows_delta = i64::try_from(buffer.total_rows())
                    .unwrap_or(i64::MAX)
                    .saturating_sub(i64::try_from(superseded).unwrap_or(i64::MAX));
                self.table.schedule_post_write_maintenance(
                    Some(Arc::new(stats_acc)),
                    false,
                    false,
                    live_rows_delta,
                );

                self.table
                    .schedule_inline_checkpoint_if_memtable_pressure_exceeded();

                return Ok(InlineMutationOutcome::Inlined {
                    rows: u64::try_from(buffer.total_rows()).unwrap_or(u64::MAX),
                    post_validation: state,
                });
            }

            restore_post_validation(post_validation, state);
        }

        // Bytes seen while buffering — a lower bound on the delta size (the
        // chained remainder of `prepared_stream` may add more). Used to size the
        // write shard count: small deltas stay a single file. Captured before
        // `into_chained_stream` consumes the buffer.
        let buffered_bytes = buffer.total_bytes() as u64;
        let re_stream = buffer.into_chained_stream(prepared_stream, self.task_context)?;
        Ok(InlineMutationOutcome::Fallback {
            stream: re_stream,
            buffered_bytes,
        })
    }

    async fn write_staged_append(
        &self,
        stream: SendableRecordBatchStream,
        target_size_bytes: usize,
        estimated_bytes: Option<u64>,
    ) -> Result<(u64, usize, Arc<ColumnStatsAccumulator>)> {
        let staging_snapshot_id = CayenneTableProvider::new_staging_snapshot_id();
        self.table
            .clear_staging_snapshot_dir(&staging_snapshot_id)
            .await?;

        // We are about to (or have started to) write Vortex files into the
        // staging directory. Mark it "dirty" so recovery/root cleanup
        // (on this or a future writer, or on recovery after a crash) will
        // actually perform the cleanup instead of taking the fast path.
        self.table
            .staging_may_have_files()
            .store(true, Ordering::Release);

        let write_start = Instant::now();
        let result = match self
            .table
            .write_to_snapshot(
                stream,
                target_size_bytes,
                &staging_snapshot_id,
                self.task_context.session_config().target_partitions(),
                estimated_bytes,
                crate::provider::delta_encoding::WriteClass::Delta,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                if let Err(cleanup_err) = self
                    .table
                    .clear_staging_snapshot_dir(&staging_snapshot_id)
                    .await
                {
                    tracing::warn!(
                        "Failed to clean staging dir after write error for table {}: {cleanup_err}",
                        self.table.table_name(),
                    );
                }
                return Err(e);
            }
        };
        record_cayenne_write_phase(self.table.table_name(), "vortex_write", write_start);

        let staged_append = CayenneStagedAppend::from_staged_append_in(
            self.table.clone_for_write_operations(),
            None,
            staging_snapshot_id,
            result.0,
        );
        let publish_start = Instant::now();
        staged_append.finalize_staged_write().await?;
        record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);

        Ok(result)
    }

    async fn write_staged_append_prepared(
        &self,
        stream: SendableRecordBatchStream,
        target_size_bytes: usize,
        write_guard: Option<OwnedMutexGuard<()>>,
        target: PreparedStagedAppendTarget,
    ) -> Result<(
        u64,
        usize,
        Arc<ColumnStatsAccumulator>,
        PreparedStagedAppend,
    )> {
        self.table
            .staging_may_have_files()
            .store(true, Ordering::Release);

        let write_start = Instant::now();
        let (rows, writer_ops, stats_acc) = match self
            .table
            .write_to_snapshot(
                stream,
                target_size_bytes,
                &target.staging_snapshot_id,
                self.task_context.session_config().target_partitions(),
                target.estimated_bytes,
                crate::provider::delta_encoding::WriteClass::Delta,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                if let Err(cleanup_err) = self
                    .table
                    .clear_staging_snapshot_dir(&target.staging_snapshot_id)
                    .await
                {
                    tracing::warn!(
                        "Failed to clean staging dir after write error for table {}: {cleanup_err}",
                        self.table.table_name(),
                    );
                }
                return Err(e);
            }
        };
        record_cayenne_write_phase(self.table.table_name(), "vortex_write", write_start);

        let staged_append = CayenneStagedAppend::from_staged_append_to_snapshot(
            self.table.clone_for_write_operations(),
            write_guard,
            target.staging_snapshot_id.clone(),
            target.target_snapshot_id,
            target.target_kind,
            rows,
        );
        let prepare_start = Instant::now();
        let prepared_append = match staged_append.prepare().await {
            Ok(prepared_append) => prepared_append,
            Err(e) => {
                if let Err(cleanup_err) = self
                    .table
                    .clear_staging_snapshot_dir(&target.staging_snapshot_id)
                    .await
                {
                    tracing::warn!(
                        "Failed to clean staging dir after WAL prepare error for table {}: {cleanup_err}",
                        self.table.table_name(),
                    );
                }
                return Err(e);
            }
        };
        record_cayenne_write_phase(self.table.table_name(), "stage_wal_prepare", prepare_start);

        Ok((rows, writer_ops, stats_acc, prepared_append))
    }
}

#[cfg(test)]
mod tests {
    use super::super::table::{INLINE_MAX_BUFFER_BYTES, INLINE_MAX_ROWS};
    use super::*;
    use arrow::array::{BinaryArray, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn inline_policy_requires_simple_append_shape() {
        assert!(InlineMutationPolicy::from_blocking_conditions([false; 4]).can_inline());

        for blocking_condition_index in 0..4 {
            let mut blocking_conditions = [false; 4];
            blocking_conditions[blocking_condition_index] = true;
            assert!(
                !InlineMutationPolicy::from_blocking_conditions(blocking_conditions).can_inline()
            );
        }
    }

    #[test]
    fn inline_buffer_allows_boundary_row_count() {
        let inline_max_rows =
            i64::try_from(INLINE_MAX_ROWS).expect("INLINE_MAX_ROWS should fit in i64");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..inline_max_rows))],
        )
        .expect("batch should be valid");

        let mut buffer = InlineBatchBuffer::new(schema, INLINE_MAX_ROWS, INLINE_MAX_BUFFER_BYTES);
        buffer.push(batch);

        assert_eq!(buffer.total_rows(), INLINE_MAX_ROWS);
        assert!(buffer.should_continue_buffering());
    }

    #[test]
    fn inline_buffer_exceeds_after_row_limit() {
        let inline_max_rows =
            i64::try_from(INLINE_MAX_ROWS).expect("INLINE_MAX_ROWS should fit in i64");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..=inline_max_rows))],
        )
        .expect("batch should be valid");

        let mut buffer = InlineBatchBuffer::new(schema, INLINE_MAX_ROWS, INLINE_MAX_BUFFER_BYTES);
        buffer.push(batch);

        assert_eq!(buffer.total_rows(), INLINE_MAX_ROWS + 1);
        assert!(!buffer.should_continue_buffering());
    }

    #[test]
    fn inline_buffer_exceeds_after_byte_limit() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let payload = vec![7_u8; INLINE_MAX_BUFFER_BYTES + 1];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from_vec(vec![payload.as_slice()]))],
        )
        .expect("batch should be valid");

        let mut buffer = InlineBatchBuffer::new(schema, INLINE_MAX_ROWS, INLINE_MAX_BUFFER_BYTES);
        buffer.push(batch);

        assert!(!buffer.should_continue_buffering());
    }
}
