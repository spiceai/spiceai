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
use super::column_stats::ColumnStatsAccumulator;
use super::context::CayenneContext;
use super::mem_tier_budget;
use super::on_conflict::{PostValidationState, PreparedShardedInsertStream};
use super::pk_index::PkDigestSet;
use super::staging_wal::{CayenneStagedAppend, PreparedStagedAppend, StagingWalTargetKind};
use super::table::{CayenneCdcWrite, CayenneTableProvider, record_cayenne_write_phase};

/// Record METRIC 4 (`cayenne_cdc_burst_rows` / `cayenne_cdc_burst_bytes`) for one
/// prepared CDC batch at the staged/inlined write entry, labeled by table. The
/// values come from the inline buffer at the call sites; on the inline-overflow
/// FALLBACK path they are therefore the buffered LOWER BOUND (the unbuffered
/// stream remainder is not counted — see `InlineBatchBuffer::total_bytes`).
/// Forwarding both together keeps the two histograms paired per batch.
fn record_cayenne_cdc_burst(table_name: &str, rows: u64, bytes: u64) {
    let dims = [telemetry::KeyValue::new("table", table_name.to_string())];
    telemetry::cayenne::track_cdc_burst_rows(rows, &dims);
    telemetry::cayenne::track_cdc_burst_bytes(bytes, &dims);
}

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

    /// The cap that tripped the inline-admission fallback, as the METRIC 3
    /// `reason` label. Checks rows first (the order `push` evaluates them), so a
    /// burst that blew both caps reports `rows_cap`. Returns `None` when neither
    /// cap is exceeded (the buffer still fits — the caller inlines or the stream
    /// simply ended).
    #[must_use]
    pub(crate) fn overflow_reason(&self) -> Option<&'static str> {
        if self.total_rows > self.max_rows {
            Some("rows_cap")
        } else if self.total_bytes > self.max_buffer_bytes {
            Some("bytes_cap")
        } else {
            None
        }
    }

    pub(crate) fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// In-memory Arrow bytes buffered so far. When the buffer overflows and the
    /// write falls back to a Vortex write, this is a *lower bound* on the
    /// delta's total size because the un-buffered remainder has not been
    /// measured. The fallback path still passes this lower bound as
    /// `estimated_bytes`, deliberately keeping hot CDC bursts on fewer write
    /// shards than their true size might warrant.
    #[must_use]
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[must_use]
    pub(crate) fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Reconstitute the original stream: re-emit the buffered head ahead of the
    /// unconsumed remainder, so the write path sees every row exactly once and in
    /// order.
    ///
    /// The buffer already owns the batches, so replaying them is a plain
    /// `stream::iter` — no `MemorySourceConfig`/`ExecutionPlan` round trip, and
    /// therefore no `TaskContext` and nothing to fail.
    pub(crate) fn into_chained_stream(
        self,
        remaining_stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let replay = futures::stream::iter(self.batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema,
            StreamExt::chain(replay, remaining_stream),
        ))
    }
}

enum InlineMutationOutcome {
    Inlined {
        rows: u64,
        post_validation: PostValidationState,
    },
    Fallback {
        stream: SendableRecordBatchStream,
        /// Size estimate for the staged write: the bytes buffered before
        /// falling back (exact when the stream ended inside the caps; a lower
        /// bound when it overflowed). The lower bound is used AS the size on
        /// purpose: it floors overflowing CDC bursts to a single encode shard.
        /// Fanning such bursts out to full write-concurrency was measured as a
        /// strict regression (file flood → slower encode, fatter publish,
        /// 2-4x OLAP read-amp; 2026-06-06 run) — see the comment at the
        /// construction site.
        estimated_bytes: Option<u64>,
    },
}

/// Outcome of the in-memory CDC write attempt (`cdc_durability: memory`).
enum MemWriteOutcome {
    /// The batch was appended to the RAM tier (or absorbed by a spill); the
    /// returned `CayenneCdcWrite` carries the mem-tier epoch for slot deferral.
    /// Boxed because `CayenneCdcWrite` is large and the other variant is small.
    Done(Box<CayenneCdcWrite>),
    /// Sustained overload — the global byte budget could not admit the batch
    /// even after a bounded wait for other tables' releases AND spilling the
    /// tier durable. The caller must take the durable path for this batch (its
    /// committer advances the slot per-batch, which is safe because the spill
    /// drained every prior mem batch to durable first). The re-streamed batches
    /// + the held write guard are handed back.
    FallBackToDurable {
        stream: SendableRecordBatchStream,
        write_guard: OwnedMutexGuard<()>,
    },
}

/// Outcome of the N>1 sharded in-memory CDC write attempt (§5 Phase 3).
enum MemShardedOutcome {
    /// Appended to the per-shard RAM tiers; carries the slot-deferral receipt.
    Done(Box<CayenneCdcWrite>),
    /// Sustained overload — take the durable path. The buffered RAW batches +
    /// schema are handed back; the caller re-streams them through the standard
    /// serial prepare so the durable path re-validates them.
    FallBackToDurable {
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        write_guard: OwnedMutexGuard<()>,
    },
}

struct PreparedStagedAppendTarget {
    staging_snapshot_id: String,
    source_snapshot_id: String,
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
        // Empty-table probe: on the very first write of a freshly-created
        // table, install warm empty PK caches so the initial load maintains
        // them and the first upsert never pays the full cold index scan.
        self.table.maybe_install_warm_pk_caches().await;
        let write_start = Instant::now();

        let pending_pk_deletions = !self.table.pk_deletion_strategy().is_position_based()
            && self.table.has_pending_deletions();

        // N>1 in-memory CDC: validate + append per PK-hash shard within one apply
        // (§5 Phase 3). Engaged for EVERY in-memory upsert apply on a non-partitioned,
        // deferral-armed, >1-shard table.
        //
        // CRITICAL — this path must NOT be gated on `!pending_pk_deletions`. A real CDC
        // table almost always carries a tombstone in its durable deletion index (every
        // upsert supersedes, every delete tombstones), so gating on it diverted ~98% of
        // applies to the SERIAL shard-0 path — which stamps `source_position = None`, so
        // the checkpoint's `durable_epoch = MAX(source_position)` never covered them and
        // the source slot froze (the SF-100 N=4 WAL→38 GB stall; ~2239 serial vs ~82
        // sharded applies on order_line). Sharding a pending-deletion upsert is SOUND: the
        // per-shard validation handles on-conflict supersession (it builds per-shard
        // `OnConflictDeletions`), and a new row is immune to any pre-existing tombstone
        // because its `data_sequence` is reserved strictly above every prior
        // `delete_sequence` — the same seq-ordering the serial staged path relies on — so a
        // stale-PRESENT existence-index hit costs at most a harmless redundant tombstone
        // under upsert semantics. CDC DELETE bursts take the separately-sharded
        // `append_delete_intents_sharded`, so EVERY apply at N>1 stays on the one
        // `apply_epoch` slot-ack axis the checkpoint reconciles. Every other case — and
        // ALWAYS at N=1 — falls through to the byte-identical serial path below.
        let mem_tier_shards = self.table.mem_tier_shard_count();
        if mem_tier_shards > 1
            && self.table.is_cdc_memory_mode()
            && self.table.has_slot_advancer()
            && self.table.metadata().partition_column.is_none()
        {
            if let Some(prepared) = self
                .table
                .prepare_stream_for_insert_sharded(data, mem_tier_shards)
                .await?
            {
                match self
                    .write_cdc_in_memory_sharded(prepared, write_guard, write_start)
                    .await?
                {
                    MemShardedOutcome::Done(cdc_write) => return Ok(*cdc_write),
                    MemShardedOutcome::FallBackToDurable {
                        batches,
                        schema,
                        write_guard,
                    } => {
                        // Sustained overload: the sharded path bailed BEFORE any
                        // validation/append (no tier mutation occurred, the spill
                        // already drained + acked prior mem batches). Re-stream the
                        // buffered RAW batches through the standard serial
                        // `prepare_stream_for_insert` so the durable path re-runs
                        // on-conflict validation against the single index — keeping
                        // conflict semantics intact without a second sharded apply.
                        let _write_guard = write_guard;
                        let raw = MemorySourceConfig::try_new_exec(&[batches], schema, None)
                            .and_then(|exec| execute_stream(exec, Arc::clone(self.task_context)))?;
                        let prepared = self.table.prepare_stream_for_insert(raw).await?;
                        let post_validation = prepared.post_validation();
                        let may_have_on_conflict_deletions =
                            prepared.may_have_on_conflict_deletions();
                        let rows = self
                            .write_prepared_stream(
                                prepared.stream,
                                post_validation,
                                pending_pk_deletions,
                                may_have_on_conflict_deletions,
                            )
                            .await?;
                        record_cayenne_write_phase(
                            self.table.table_name(),
                            "cdc_path_inmemory_sharded_fallback",
                            write_start,
                        );
                        return Ok(CayenneCdcWrite::completed(
                            self.table.clone_for_write_operations(),
                            rows,
                        ));
                    }
                }
            }
            // `prepare_stream_for_insert_sharded` returned None (no PK): fall through
            // is impossible — `data` was consumed. PK-less tables never reach
            // `is_cdc_memory_mode` (it requires a key-based merge-on-read shape), so
            // this branch is unreachable in practice. Guard defensively.
            return Err(super::Error::Internal {
                table: self.table.table_name().to_string(),
                message: "sharded in-memory CDC path engaged on a table without a primary key"
                    .to_string(),
            });
        }

        let prepared = self.table.prepare_stream_for_insert(data).await?;
        let post_validation = prepared.post_validation();
        let may_have_on_conflict_deletions = prepared.may_have_on_conflict_deletions();
        let prepared_stream = prepared.stream;

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

        // In-memory write path: append the validated batch to the RAM tier instead
        // of persisting a per-batch durable BLOB. Taken when EITHER the table is a
        // `mode: memory` accelerator (`is_memory_resident_mode` — the mem-tier is
        // its permanent store) OR a key-based, non-partitioned CDC table
        // (`is_cdc_memory_mode`) whose runtime has armed deferral for a replayable
        // source (`has_slot_advancer`). The two differ in how the runtime acks the
        // source slot: `mode: memory` never checkpoints, so the slot is committed
        // immediately (nothing to defer behind); `cdc_durability: memory` defers the
        // ack behind the covering durable checkpoint. Every other table/source keeps
        // the durable path below, byte-identical.
        let (mut prepared_stream, write_guard) = if self.table.is_memory_resident_mode()
            || (self.table.is_cdc_memory_mode() && self.table.has_slot_advancer())
        {
            match self
                .write_cdc_in_memory(prepared_stream, &post_validation, write_guard, write_start)
                .await?
            {
                MemWriteOutcome::Done(cdc_write) => return Ok(*cdc_write),
                // Sustained overload: the global budget is full even after a
                // bounded wait for other tables to release AND a spill drained
                // the tier (firing the slot advancer, so the prior mem batches
                // are durable). This batch takes the durable path below with a
                // NORMAL committer — safe because the slot is not ahead of
                // durable (spill-then-fallback ordering guard).
                MemWriteOutcome::FallBackToDurable {
                    stream,
                    write_guard,
                } => (stream, write_guard),
            }
        } else {
            (prepared_stream, write_guard)
        };

        match self
            .try_inline_or_restream(prepared_stream, &post_validation)
            .await?
        {
            InlineMutationOutcome::Inlined {
                rows,
                post_validation,
            } => {
                let record_seq = self.table.sequence_high_water().await;
                self.table
                    .record_inlined_pk_keys(&post_validation.validated_keys, record_seq);
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
                estimated_bytes,
            } => {
                prepared_stream = stream;
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

                // Hold `write_lock` only through Stage A (staging the WAL and
                // registering the in-flight append), then release it at the
                // `drop(held_write_guard)` below — the same discipline the
                // on-conflict path already uses. Retaining the guard in the
                // prepared receipt would block a second pipelined Stage A on this
                // write's finalize and self-deadlock finish()'s
                // `lock_current_snapshot_for_apply` re-acquire. Compaction-skip
                // (via the in-flight registration) and apply-time consistency
                // (`ensure_current_snapshot_target_unchanged` + the re-acquired
                // lock under the listing fence) do not need the guard held across
                // the staged window. The cross-partition coordinator retains its
                // own guard via a separate entry point (`begin_deferred_snapshot_append`).
                let (write_guard_for_prepare, held_write_guard) = (None, Some(write_guard));

                let (rows, writer_ops, stats_acc, prepared_append) = self
                    .write_staged_append_prepared(
                        prepared_stream,
                        target_size_bytes,
                        write_guard_for_prepare,
                        PreparedStagedAppendTarget {
                            staging_snapshot_id,
                            source_snapshot_id: self.table.get_current_snapshot_id(),
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
                            false,
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
                    let record_seq = self.table.sequence_high_water().await;
                    self.table.record_file_pk_keys(&validated_keys, record_seq);
                    // Unlike every other `record_file_pk_keys` call site, this one
                    // records BEFORE the publish: the staged files are not yet
                    // discoverable and the read filter skips the unpublished
                    // tombstone, so until `CayenneCdcWrite::finish` these keys exist
                    // only in the PK cache. Anything that drops that cache
                    // (`clear_cached_pk_keyset`, a discarded index, an abandoned
                    // validation) does so on the premise that the next rebuild reads
                    // the commit back from the table, which is not true yet — so
                    // also hand the keys to the in-flight registration, where a
                    // rebuild folds them in and the publish retires them.
                    self.table.attach_inflight_staged_pk_keys(
                        prepared_append.staging_snapshot_id(),
                        &validated_keys,
                    );
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

    /// In-memory CDC write path, shared by `cdc_durability: memory` and
    /// `mode: memory`. Drains the validated stream into RAM, computes the
    /// on-conflict tombstones in memory, and appends to the mem tier (returning the
    /// mem-tier epoch) under byte caps (per-table, plus a process-wide one when the
    /// runtime has installed the global budget) that bound the resident tier.
    ///
    /// The modes diverge on cap breach and slot ack. `cdc_durability: memory`
    /// buffers the whole burst, then on breach spills the tier durable (and, under
    /// sustained overload, falls back to the durable path), and the runtime DEFERS
    /// the source slot ack behind the covering durable checkpoint. `mode: memory`
    /// never checkpoints or spills: it enforces the RAM bound incrementally as the
    /// burst is buffered — an oversized burst returns `MemTierLimitExceeded` before
    /// it can allocate toward OOM — and the runtime commits the slot immediately.
    /// PK conflict validation still runs either way (it populated `post_validation`
    /// as the stream was prepared).
    async fn write_cdc_in_memory(
        &self,
        mut prepared_stream: SendableRecordBatchStream,
        post_validation: &Arc<ParkingMutex<Option<PostValidationState>>>,
        write_guard: OwnedMutexGuard<()>,
        write_start: Instant,
    ) -> Result<MemWriteOutcome> {
        // Drain the prepared stream into RAM (CDC batches are small per apply).
        // Draining also RUNS the deferred PK-conflict validation, populating
        // `post_validation` with the on-conflict deletions.
        let schema = prepared_stream.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut incoming_bytes: u64 = 0;
        let mut incoming_rows: u64 = 0;
        let drain_start = Instant::now();
        while let Some(batch) = StreamExt::next(&mut prepared_stream).await {
            let batch = batch?;
            incoming_bytes = incoming_bytes.saturating_add(batch.get_array_memory_size() as u64);
            incoming_rows = incoming_rows.saturating_add(batch.num_rows() as u64);
            // Memory mode never spills, so enforce the per-table RAM bound AS the
            // burst is buffered: an oversized burst fails fast with the structured
            // error instead of allocating the whole burst toward OOM before the
            // post-drain check. `spill_mem_tier_if_cap_breached` errors (does not
            // checkpoint) in memory mode; the cheap `mem_tier_per_table_cap_breached`
            // pre-check keeps it off the hot path until the cap is actually breached.
            // Non-memory CDC keeps buffering here and spills after the drain (below).
            if self.table.is_memory_resident_mode()
                && self.table.mem_tier_per_table_cap_breached(incoming_bytes)
            {
                self.table
                    .spill_mem_tier_if_cap_breached(incoming_bytes)
                    .await?;
            }
            batches.push(batch);
        }
        drop(prepared_stream);
        // Decompose `cdc_path_inmemory`: draining the prepared stream RUNS the
        // deferred PK-conflict validation and decodes the upstream CDC batches,
        // so this is the "produce + validate the batch" slice — separating
        // upstream-bound cost from the fence + append cost that follows.
        record_cayenne_write_phase(
            self.table.table_name(),
            "inmemory_stream_drain",
            drain_start,
        );

        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = take_post_validation(post_validation);
        let superseded =
            u64::try_from(on_conflict_deletions.total_superseded()).unwrap_or(u64::MAX);

        // CAP CHECK + spill/fallback decision (OOM-safety, correctness item #2).
        //
        // 1. Per-table BYTE cap breached → spill (checkpoint) FIRST — double-
        //    checked under the checkpoint lock, see
        //    `spill_mem_tier_if_cap_breached` — then append into the bounded
        //    tier. Byte cap only: the tier's AGE cap is enforced by the 1s
        //    background tick without blocking this writer (the age-sharing
        //    variant made the applier ride out checkpoint outages — the
        //    measured 33-41s apply stalls behind compaction-starved encodes).
        // 2. Global budget can't admit the bytes → wait (bounded) for ANOTHER
        //    table's checkpoint to release budget; on timeout spill self (which
        //    releases the flushed epoch's budget) and retry once; still refused
        //    → fall back to the durable path for this batch. The spill fires
        //    the slot advancer, draining every prior mem batch to durable, so a
        //    durable fallback batch is never ahead of durable.
        //
        // Both admission stalls are timed (`inmemory_spill` /
        // `inmemory_budget_wait`): they were the invisible bulk of the apply
        // path under overload — a synchronous whole-tier checkpoint inside the
        // write path that no phase metric covered.
        if self.table.mem_tier_per_table_cap_breached(incoming_bytes) {
            let spill_start = Instant::now();
            let spill_result = self
                .table
                .spill_mem_tier_if_cap_breached(incoming_bytes)
                .await;
            record_cayenne_write_phase(self.table.table_name(), "inmemory_spill", spill_start);
            spill_result?;
        }

        // Memory mode never spills to the durable path — the per-table RAM bound
        // above (`spill_mem_tier_if_cap_breached`, which errors in memory mode) is
        // the sole limit, so skip the process-global budget reserve/wait/fallback
        // (which could otherwise force the Vortex durable write memory mode forbids).
        if !self.table.is_memory_resident_mode()
            && !mem_tier_budget::try_reserve_bytes(incoming_bytes)
        {
            let wait_start = Instant::now();
            let admitted = self.table.wait_for_budget_or_spill(incoming_bytes).await;
            record_cayenne_write_phase(self.table.table_name(), "inmemory_budget_wait", wait_start);
            if !admitted? {
                // Still over budget after waiting AND spilling (other tables
                // hold it): fall back to the durable path. The spill already
                // drained and acked this table's prior mem batches
                // (spill-then-fallback ordering).
                record_cayenne_write_phase(
                    self.table.table_name(),
                    "cdc_path_inmemory_fallback",
                    write_start,
                );
                let stream = MemorySourceConfig::try_new_exec(&[batches], schema, None)
                    .and_then(|exec| execute_stream(exec, Arc::clone(self.task_context)))?;
                // Restore the post-validation state consumed by `take_post_validation`
                // above. The durable fallback path (`try_inline_or_restream`) re-reads
                // `post_validation`, so without this the on-conflict deletions and
                // validated-key bookkeeping would be lost — silently skipping conflict
                // semantics under sustained memory-mode overload (a correctness risk).
                restore_post_validation(
                    post_validation,
                    PostValidationState {
                        on_conflict_deletions,
                        validated_keys,
                    },
                );
                return Ok(MemWriteOutcome::FallBackToDurable {
                    stream,
                    write_guard,
                });
            }
        }

        // Append to the RAM tier under the listing fence. The reserved bytes stay
        // held (released by the checkpoint that flushes this epoch). On append
        // error, release the reservation so the budget doesn't leak.
        let epoch = match self
            .table
            .append_to_mem_tier(batches, &on_conflict_deletions, incoming_bytes, superseded)
            .await
        {
            Ok(epoch) => epoch,
            Err(e) => {
                // Memory mode skipped the reservation above, so must not release.
                if !self.table.is_memory_resident_mode() {
                    mem_tier_budget::release_bytes(incoming_bytes);
                }
                return Err(e);
            }
        };
        // Record the inlined PK keys so a subsequent same-table upsert sees this
        // batch's rows as present (same bookkeeping as the durable inline path).
        let record_seq = self.table.sequence_high_water().await;
        self.table
            .record_inlined_pk_keys(&validated_keys, record_seq);

        drop(write_guard);
        record_cayenne_write_phase(self.table.table_name(), "cdc_path_inmemory", write_start);
        Ok(MemWriteOutcome::Done(Box::new(
            CayenneCdcWrite::in_memory_staged(
                self.table.clone_for_write_operations(),
                incoming_rows,
                epoch,
            ),
        )))
    }

    /// Sharded (N>1) in-memory CDC write path (§5 Phase 3, step b). Drains the
    /// RAW decoded stream, applies the whole-apply OOM-safety caps/budget exactly
    /// as [`Self::write_cdc_in_memory`], then DECOUPLES decode from validation:
    /// each batch is split by PK shard and the per-batch on-conflict validation
    /// runs PER SHARD ([`CayenneTableProvider::validate_and_append_sharded`]),
    /// with the N shard appends joined concurrently. The combined post-validation
    /// state is published for the durable fallback.
    ///
    /// `prepared.sharded_index` carries the checkout window the index was taken
    /// under, so the early exits here — a stream error, a spill failure, and the
    /// sustained-overload diversion to the durable path, which abandons the index
    /// entirely — close that window on the way out instead of leaving it latched.
    ///
    /// Engaged only at N>1; the N=1 write path never reaches here, so today's
    /// behavior is byte-identical.
    async fn write_cdc_in_memory_sharded(
        &self,
        prepared: PreparedShardedInsertStream,
        write_guard: OwnedMutexGuard<()>,
        write_start: Instant,
    ) -> Result<MemShardedOutcome> {
        let PreparedShardedInsertStream {
            mut stream,
            pk_indices,
            converter,
            sharded_index,
            on_conflict,
        } = prepared;

        // Drain the RAW stream into RAM (no validation wrapper — validation is
        // deferred to the per-shard step below).
        let schema = stream.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut incoming_bytes: u64 = 0;
        let mut incoming_rows: u64 = 0;
        let drain_start = Instant::now();
        while let Some(batch) = StreamExt::next(&mut stream).await {
            let batch = batch?;
            incoming_bytes = incoming_bytes.saturating_add(batch.get_array_memory_size() as u64);
            incoming_rows = incoming_rows.saturating_add(batch.num_rows() as u64);
            batches.push(batch);
        }
        drop(stream);
        record_cayenne_write_phase(
            self.table.table_name(),
            "inmemory_stream_drain",
            drain_start,
        );

        // Whole-apply (whole-tier) OOM-safety: per-table byte cap spill + global
        // budget reservation, identical to the serial path. The byte trigger is
        // whole-tier (sum across shards), never budget/N-per-shard (§3.4 Fix 2).
        if self.table.mem_tier_per_table_cap_breached(incoming_bytes) {
            let spill_start = Instant::now();
            let spill_result = self
                .table
                .spill_mem_tier_if_cap_breached(incoming_bytes)
                .await;
            record_cayenne_write_phase(self.table.table_name(), "inmemory_spill", spill_start);
            spill_result?;
        }

        // Memory mode never spills to the durable path — the per-table RAM bound
        // above (`spill_mem_tier_if_cap_breached`, which errors in memory mode) is
        // the sole limit, so skip the process-global budget reserve/wait/fallback
        // (which could otherwise force the Vortex durable write memory mode forbids).
        if !self.table.is_memory_resident_mode()
            && !mem_tier_budget::try_reserve_bytes(incoming_bytes)
        {
            let wait_start = Instant::now();
            let admitted = self.table.wait_for_budget_or_spill(incoming_bytes).await;
            record_cayenne_write_phase(self.table.table_name(), "inmemory_budget_wait", wait_start);
            if !admitted? {
                // Sustained overload: hand the buffered raw batches to the durable
                // path. Populate the combined post-validation state by running the
                // sharded validate+append? No — on fallback we must NOT append to
                // the tier. Instead run validation only to produce the combined
                // on-conflict deletions for the durable path. The simplest correct
                // route: re-run validation through the standard serial prepare on
                // the durable side (it rebuilds the single index). We therefore
                // hand back the raw batches with an EMPTY post-validation; the
                // durable `write_prepared_stream` re-validates via its own
                // `prepare_stream_for_insert`. To keep that contract, the fallback
                // re-streams the raw batches into a FRESH `prepare_stream_for_insert`
                // at the caller.
                return Ok(MemShardedOutcome::FallBackToDurable {
                    batches,
                    schema,
                    write_guard,
                });
            }
        }

        // Validate + append per shard. On error, release the byte reservation so
        // the global budget doesn't leak (matching the serial path).
        let apply = match self
            .table
            .validate_and_append_sharded(
                batches,
                sharded_index,
                &pk_indices,
                &converter,
                &on_conflict,
                incoming_bytes,
            )
            .await
        {
            Ok(apply) => apply,
            Err(e) => {
                // Memory mode skipped the reservation above, so must not release.
                if !self.table.is_memory_resident_mode() {
                    mem_tier_budget::release_bytes(incoming_bytes);
                }
                return Err(e);
            }
        };

        drop(write_guard);
        tracing::debug!(
            table = self.table.table_name(),
            shards = self.table.mem_tier_shard_count(),
            incoming_rows,
            superseded = apply.superseded,
            validated_keys = apply.validated_keys.len(),
            file_key_deletes = apply.on_conflict_deletions.deleted_pk_i64.len()
                + apply.on_conflict_deletions.deleted_row_keys.len(),
            "Sharded in-memory CDC apply completed"
        );
        record_cayenne_write_phase(
            self.table.table_name(),
            "cdc_path_inmemory_sharded",
            write_start,
        );
        Ok(MemShardedOutcome::Done(Box::new(
            CayenneCdcWrite::in_memory_staged(
                self.table.clone_for_write_operations(),
                incoming_rows,
                apply.epoch,
            ),
        )))
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

        // Size estimate for the staged write. Populated from the inline buffer
        // whenever we attempt to inline first: an exact size when the buffer
        // captured the whole delta, and a buffered LOWER BOUND when it overflowed
        // — the lower bound is kept ON PURPOSE (it floors a hot CDC burst to a
        // single encode shard; fanning overflowing bursts out to full
        // write-concurrency was measured as a strict regression — file flood,
        // slower encode, fatter publish, 2-4x OLAP read-amp). `None` only when
        // inlining is skipped entirely (partition/retention tables), which keep
        // the prior full-fan-out sizing. See `try_inline_or_restream`.
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
                    let record_seq = self.table.sequence_high_water().await;
                    self.table
                        .record_inlined_pk_keys(&post_validation.validated_keys, record_seq);
                    return Ok(rows);
                }
                InlineMutationOutcome::Fallback {
                    stream,
                    estimated_bytes: fallback_estimate,
                } => {
                    prepared_stream = stream;
                    estimated_bytes = fallback_estimate;
                }
            }
        } else {
            // METRIC 3 (inline admission flip): the table's shape bars the inline
            // memtable outright (partition column or retention delete filters), so
            // this write goes straight to a staged Vortex write without ever
            // buffering. `try_inline_or_restream` (which records rows_cap/bytes_cap
            // and the burst shape) is skipped, so attribute the flip here.
            telemetry::cayenne::track_inline_fallback(&[
                telemetry::KeyValue::new("table", self.table.table_name().to_string()),
                telemetry::KeyValue::new("reason", "blocking_config"),
            ]);
        }

        let needs_new_snapshot = pending_pk_deletions || may_have_on_conflict_deletions;

        // Taken before either publish below: both make rows visible well before
        // the `num_rows` delta describing them reaches the maintenance queue, and
        // a reader landing in between would be served the pre-write count as a
        // provably exact one. Released on drop if the write returns early —
        // nothing was published.
        let reserved_live_rows_delta = self.table.reserve_live_rows_delta();

        // `superseded` = existing rows replaced by this upsert (deleted as part
        // of the conflict resolution). The live-row delta is `inserted -
        // superseded`, which keeps the metastore `num_rows` tracking COUNT(*)
        // under CDC upsert instead of summing every insert.
        let (total_rows, write_stats_acc, validated_keys, superseded) = if needs_new_snapshot {
            let new_snapshot_start = Instant::now();
            let (rows, stats_acc, validated_keys, superseded) = self
                .write_new_snapshot_after_validation(
                    prepared_stream,
                    &post_validation,
                    estimated_bytes,
                )
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

        // Both branches above have made this commit's rows visible, so from here
        // the claim survives a cancellation or a failure: a commit that dies
        // after publishing has left rows it will never queue a delta for.
        let published_live_rows_delta = reserved_live_rows_delta.published();

        let retention_requested = self.table.has_retention_delete_filters();

        let live_rows_delta = i64::try_from(total_rows)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(write_stats_acc),
            needs_new_snapshot,
            retention_requested,
            live_rows_delta,
            published_live_rows_delta,
        );

        if retention_requested {
            // Retention runs asynchronously after this write returns; its delete
            // outcome is not yet known. Clearing the cache is the conservative
            // path — any subsequent insert pays one fresh disk-scan to rebuild
            // (vs the existing pre-fix logic, which read the inline delete
            // count and cleared only when retention had actually deleted rows).
            self.table.clear_cached_pk_keyset();
        } else {
            let record_seq = self.table.sequence_high_water().await;
            self.table.record_file_pk_keys(&validated_keys, record_seq);
        }

        Ok(total_rows)
    }

    async fn write_new_snapshot_after_validation(
        &self,
        prepared_stream: SendableRecordBatchStream,
        post_validation: &Arc<ParkingMutex<Option<PostValidationState>>>,
        estimated_bytes: Option<u64>,
    ) -> Result<(u64, Arc<ColumnStatsAccumulator>, PkDigestSet, usize)> {
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
                // Size estimate from the inline-gate buffer when the write
                // attempted to inline first (the common on-conflict upsert shape:
                // small deltas fully buffered by the gate, so the bound is
                // exact). On overflow it is a buffered LOWER BOUND, kept ON
                // PURPOSE so a hot CDC burst stays a single encode shard
                // (fanning overflowing bursts out regressed encode/publish/OLAP
                // read-amp 2-4x). `None` only when inlining was skipped
                // (partition/retention tables) — those keep the prior full
                // fan-out sizing and the full default delta encoding.
                estimated_bytes,
                crate::provider::delta_encoding::WritePolicy::DELTA,
            )
            .await?;
        record_cayenne_write_phase(self.table.table_name(), "vortex_write", write_start);
        // Fold the encode + object-store/disk upload latency into the adaptive
        // tuner's I/O-bound signal (CDC-apply path only; compaction is excluded).
        self.context.record_io_latency(write_start.elapsed());
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

        // A write that carried no rows produced no data files, so the snapshot
        // directory was never materialized on disk (the Vortex sink only
        // creates it when writing a file) and there is nothing to fsync,
        // sequence, or protect. Publish any on-conflict update without a
        // protected-snapshot entry and skip the sequence record — recording a
        // snapshot that has no directory would fail the directory sync with
        // NotFound and leave the catalog referencing a phantom snapshot. This
        // is the steady state of a scheduled append refresh whose source has
        // no new rows.
        if rows == 0 {
            self.table.commit_on_conflict_publish(update, None).await;
            return Ok((rows, stats_acc, validated_keys, superseded));
        }

        // `publish` is the metastore finalization total; the sub-phases attribute
        // it — `publish_seq` is sequence allocation + the durable sequence record,
        // `publish_cas` is the atomic deletion-cache + protected-snapshot publish.
        let publish_start = Instant::now();
        let seq_start = Instant::now();
        // Lever B2: in-memory allocator (shared with the staged path on this
        // same provider), so the sync-publish snapshot sequence stays on the one
        // monotone source instead of acquiring the metastore writer here.
        let new_sequence = self.table.reserve_sequences_local(1).await?;

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
        // Fold the metastore publish-wall latency into the adaptive tuner's
        // publish-bound signal (the single-writer finalization on the CDC-apply path).
        self.context.record_publish_latency(publish_start.elapsed());

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

            // Taken before the inline write, which makes its rows visible as
            // soon as it returns. The resident-inline-row proxy covers this
            // window today, but it is cleared by a checkpoint that does not
            // drain the delta queue, so the count still needs its own claim.
            let reserved_live_rows_delta = self.table.reserve_live_rows_delta();

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
                // The inline rows are visible now, so the claim survives from
                // here. `try_inline_batches_with_inlined_deletions` awaits the
                // maintained-aggregate apply after its own visibility flip, so
                // a cancellation inside it still loses the claim — see #13721.
                let published_live_rows_delta = reserved_live_rows_delta.published();

                // Inline tier0 (metastore BLOB) write — the synchronous CDC hot
                // loop. Always skip NDV here (lazy): these rows contribute their
                // distinct-count for free when the inline memtable later spills to
                // a Vortex file at checkpoint (`write_to_snapshot` folds NDV
                // there). Min/max/null-count stats are maintained regardless.
                let stats_acc = ColumnStatsAccumulator::new_with_ndv(&schema, false);
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
                    published_live_rows_delta,
                );

                self.table
                    .schedule_inline_checkpoint_if_memtable_pressure_exceeded();

                // METRIC 4 (burst shape): rows + Arrow bytes of this inlined CDC
                // batch. Both are exact here — the whole delta fit in the buffer.
                record_cayenne_cdc_burst(
                    self.table.table_name(),
                    buffer.total_rows() as u64,
                    buffer.total_bytes() as u64,
                );

                return Ok(InlineMutationOutcome::Inlined {
                    rows: u64::try_from(buffer.total_rows()).unwrap_or(u64::MAX),
                    post_validation: state,
                });
            }

            restore_post_validation(post_validation, state);
        }

        // Size the staged write with the buffered byte count — DELIBERATELY,
        // even though on overflow it is only a lower bound on the delta size.
        // Treating an overflowed (unsized) burst as unknown and fanning out to
        // full write-concurrency was tried and MEASURED AS A REGRESSION
        // (2026-06-06, SF-100 @10K txn/s): 8 shards x every hot burst produced
        // ~10K vortex files/table in one run, per-file fixed costs made encode
        // 2.5x SLOWER per batch, publish grew with the per-file metastore rows,
        // and scan read-amp regressed every OLAP query 2-4x. Hot CDC bursts
        // must stay a single file (the SOTA shape: parallelize ACROSS flushes,
        // never within small ones); large EXACT-sized writes (inline
        // checkpoints) still fan out via the encode-shard unit in
        // `snapshot_shard_count`. Captured before `into_chained_stream`
        // consumes the buffer.
        let estimated_bytes = Some(buffer.total_bytes() as u64);
        // METRIC 3 (inline admission flip): record the flip when a cap tripped,
        // attributed to which one (`rows_cap` / `bytes_cap`). The other way to
        // reach here is the buffer fitting but `try_inline_batches_with_inlined_deletions`
        // returning false (a rare deletions-couldn't-apply-inline case) — that is
        // not an admission-cap event, so it is deliberately left uncounted rather
        // than mislabeled as a cap.
        if let Some(reason) = buffer.overflow_reason() {
            telemetry::cayenne::track_inline_fallback(&[
                telemetry::KeyValue::new("table", self.table.table_name().to_string()),
                telemetry::KeyValue::new("reason", reason),
            ]);
        }
        // METRIC 4 (burst shape): rows + Arrow bytes of this CDC batch, captured
        // before `into_chained_stream` consumes the buffer. On the fallback path
        // the byte count is a lower bound (the un-buffered stream remainder is not
        // yet counted), but the row count is exact for what was buffered.
        record_cayenne_cdc_burst(
            self.table.table_name(),
            buffer.total_rows() as u64,
            buffer.total_bytes() as u64,
        );
        let re_stream = buffer.into_chained_stream(prepared_stream);
        Ok(InlineMutationOutcome::Fallback {
            stream: re_stream,
            estimated_bytes,
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
                crate::provider::delta_encoding::WritePolicy::DELTA,
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
        // Fold the encode + object-store/disk upload latency into the adaptive
        // tuner's I/O-bound signal (CDC-apply path only; compaction is excluded).
        self.context.record_io_latency(write_start.elapsed());

        let staged_append = CayenneStagedAppend::from_staged_append_in(
            self.table.clone_for_write_operations(),
            None,
            staging_snapshot_id,
            result.0,
        );
        let publish_start = Instant::now();
        staged_append.finalize_staged_write().await?;
        record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);
        // Fold the metastore publish-wall latency into the adaptive tuner's
        // publish-bound signal (the single-writer finalization on the CDC-apply path).
        self.context.record_publish_latency(publish_start.elapsed());

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

        // IVM staged capture (Edit B): when the table has a retraction-capable
        // maintained aggregate, buffer the insert batches and re-stream them into
        // the encoder unchanged; the buffer feeds the registry incrementally at
        // publish (attached to the receipt below). Non-IVM / non-retraction tables
        // (the common case) stream straight through at zero cost. The Stage-A
        // buffer is accepted for the prototype (the only staged IVM table is a
        // small rollup); a large staged IVM table needs deferred two-phase capture.
        let (stream, ivm_captured): (SendableRecordBatchStream, Option<Arc<Vec<RecordBatch>>>) =
            if self.table.should_capture_staged_ivm_feed() {
                let schema = stream.schema();
                let mut buffered: Vec<RecordBatch> = Vec::new();
                let mut src = stream;
                while let Some(batch) = src.next().await {
                    buffered.push(batch.map_err(|e| super::Error::Internal {
                        table: self.table.table_name().to_string(),
                        message: format!("IVM staged capture failed to read a batch: {e}"),
                    })?);
                }
                // Share ONE backing buffer between the re-stream and the publish
                // receipt: `RecordBatch` is cheaply clonable (it shares the
                // underlying Arrow buffers), so the encoder re-stream clones each
                // batch lazily off the shared `Arc<Vec<_>>` rather than duplicating
                // the whole `Vec`.
                let captured = Arc::new(buffered);
                let stream_batches = Arc::clone(&captured);
                let restream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter((0..stream_batches.len()).map(move |i| {
                        Ok::<_, datafusion::error::DataFusionError>(stream_batches[i].clone())
                    })),
                ));
                (restream, Some(captured))
            } else {
                (stream, None)
            };

        let write_start = Instant::now();
        let (rows, writer_ops, stats_acc) = match self
            .table
            .write_to_snapshot(
                stream,
                target_size_bytes,
                &target.staging_snapshot_id,
                self.task_context.session_config().target_partitions(),
                target.estimated_bytes,
                crate::provider::delta_encoding::WritePolicy::DELTA,
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
        // Fold the encode + object-store/disk upload latency into the adaptive
        // tuner's I/O-bound signal (CDC-apply path only; compaction is excluded).
        self.context.record_io_latency(write_start.elapsed());

        let staged_append = CayenneStagedAppend::from_staged_append_to_snapshot(
            self.table.clone_for_write_operations(),
            write_guard,
            target.staging_snapshot_id.clone(),
            target.source_snapshot_id,
            target.target_snapshot_id,
            target.target_kind,
            rows,
        );
        let prepare_start = Instant::now();
        let mut prepared_append = match staged_append.prepare().await {
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

        // Hand the captured insert batches to the receipt so the staged publish
        // (apply_under_barrier / apply_under_held_barrier) feeds the maintained-
        // aggregate registry under the held listing fence.
        prepared_append.set_ivm_feed_batches(ivm_captured);

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
        // METRIC 3: a row-cap overflow attributes to `rows_cap`.
        assert_eq!(buffer.overflow_reason(), Some("rows_cap"));
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
        // METRIC 3: a byte-cap overflow (within the row cap) attributes to
        // `bytes_cap`.
        assert_eq!(buffer.overflow_reason(), Some("bytes_cap"));
    }

    #[test]
    fn inline_buffer_overflow_reason_none_when_within_caps() {
        // METRIC 3: a buffer that fits both caps reports no fallback reason, so the
        // inline-admission counter is not incremented for a write that inlines.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..8))],
        )
        .expect("batch should be valid");

        let mut buffer = InlineBatchBuffer::new(schema, INLINE_MAX_ROWS, INLINE_MAX_BUFFER_BYTES);
        buffer.push(batch);

        assert!(buffer.should_continue_buffering());
        assert_eq!(buffer.overflow_reason(), None);
    }
}
