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
//! path when any of these hold:
//!
//! - the table has pending PK deletions
//! - the burst produced file-level on-conflict deletions
//! - the table has any on-conflict deletions
//! - the table has `sort_columns` configured
//! - the table is partitioned
//!
//! Those paths can't be safely deferred to Stage B because they require holding
//! state (deletion vectors, sort order) until the visibility flip is durable.
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
use super::staging_wal::{CayenneStagedAppend, PreparedStagedAppend};
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
    Fallback(SendableRecordBatchStream),
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
        let can_stage_for_pipeline = !pending_pk_deletions
            && !may_have_on_conflict_deletions
            && self.table.metadata().partition_column.is_none();

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
                Ok(CayenneCdcWrite::completed(
                    self.table.clone_for_write_operations(),
                    rows,
                ))
            }
            InlineMutationOutcome::Fallback(re_stream) => {
                prepared_stream = re_stream;
                let staging_snapshot_id = CayenneTableProvider::new_staging_snapshot_id();
                let target_size_bytes = self.context.target_file_size_bytes();
                self.table
                    .clear_staging_snapshot_dir(&staging_snapshot_id)
                    .await?;
                let (rows, writer_ops, stats_acc, prepared_append) = self
                    .write_staged_append_prepared(
                        prepared_stream,
                        target_size_bytes,
                        Some(write_guard),
                        staging_snapshot_id,
                    )
                    .await?;

                tracing::debug!(
                    "CDC append staged, wrote {} rows to Vortex in {} writer operation(s); WAL is durable",
                    rows,
                    writer_ops
                );

                Ok(CayenneCdcWrite::prepared_append(
                    self.table.clone_for_write_operations(),
                    rows,
                    prepared_append,
                    stats_acc,
                    take_post_validation(&post_validation).validated_keys,
                ))
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
                InlineMutationOutcome::Fallback(re_stream) => {
                    prepared_stream = re_stream;
                }
            }
        }

        let needs_new_snapshot = pending_pk_deletions || may_have_on_conflict_deletions;

        let (total_rows, write_stats_acc, validated_keys) = if needs_new_snapshot {
            self.write_new_snapshot_after_validation(prepared_stream, &post_validation)
                .await?
        } else {
            let target_size_bytes = self.context.target_file_size_bytes();
            let (rows, writer_ops, stats_acc) = self
                .write_staged_append(prepared_stream, target_size_bytes)
                .await?;

            tracing::debug!(
                "Insert completed, wrote {} rows to Vortex in {} writer operation(s)",
                rows,
                writer_ops
            );

            let PostValidationState {
                on_conflict_deletions,
                validated_keys,
            } = take_post_validation(&post_validation);

            self.table
                .apply_on_conflict_deletions(on_conflict_deletions)
                .await?;

            (rows, stats_acc, validated_keys)
        };

        let retention_requested = self.table.has_retention_delete_filters();

        self.table.schedule_post_write_maintenance(
            Some(write_stats_acc),
            needs_new_snapshot,
            retention_requested,
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
            )
            .await?;
        record_cayenne_write_phase(self.table.table_name(), "vortex_write", write_start);

        tracing::debug!(
            "Insert to deferred-validation snapshot {} completed, wrote {} rows to Vortex in {} writer operation(s)",
            new_snapshot_id,
            rows,
            writer_ops
        );

        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = take_post_validation(post_validation);

        let deletion_start = Instant::now();
        self.table
            .apply_on_conflict_deletions(on_conflict_deletions)
            .await?;
        record_cayenne_write_phase(
            self.table.table_name(),
            "apply_on_conflict_deletions",
            deletion_start,
        );

        let publish_start = Instant::now();
        let new_sequence = self
            .table
            .catalog()
            .increment_sequence_number(self.table.table_id())
            .await?;

        self.table
            .publish_written_snapshot_with_sequence(&new_snapshot_id, new_sequence)
            .await?;
        record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);

        Ok((rows, stats_acc, validated_keys))
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

                self.table
                    .schedule_post_write_maintenance(Some(Arc::new(stats_acc)), false, false);

                self.table
                    .schedule_inline_checkpoint_if_memtable_pressure_exceeded();

                return Ok(InlineMutationOutcome::Inlined {
                    rows: u64::try_from(buffer.total_rows()).unwrap_or(u64::MAX),
                    post_validation: state,
                });
            }

            restore_post_validation(post_validation, state);
        }

        let re_stream = buffer.into_chained_stream(prepared_stream, self.task_context)?;
        Ok(InlineMutationOutcome::Fallback(re_stream))
    }

    async fn write_staged_append(
        &self,
        stream: SendableRecordBatchStream,
        target_size_bytes: usize,
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
        staging_snapshot_id: String,
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
                &staging_snapshot_id,
                self.task_context.session_config().target_partitions(),
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
            write_guard,
            staging_snapshot_id.clone(),
            rows,
        );
        let prepare_start = Instant::now();
        let prepared_append = match staged_append.prepare().await {
            Ok(prepared_append) => prepared_append,
            Err(e) => {
                if let Err(cleanup_err) = self
                    .table
                    .clear_staging_snapshot_dir(&staging_snapshot_id)
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
