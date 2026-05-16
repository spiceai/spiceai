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
//! - the table has retention filters
//!
//! Those paths can't be safely deferred to Stage B because they require holding
//! state (deletion vectors, sort order, retention pruning) until the visibility
//! flip is durable.
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

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{SendableRecordBatchStream, execute_stream};
use futures::StreamExt;
use tokio::sync::OwnedMutexGuard;

use super::Result;
use super::context::CayenneContext;
use super::staging_wal::{CayenneStagedAppend, PreparedStagedAppend};
use super::table::{
    CayenneCdcWrite, CayenneTableProvider, ColumnStatsAccumulator, OnConflictDeletions,
    PreparedInsertStream,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InlineMutationPolicy {
    Inline,
    Vortex,
}

impl InlineMutationPolicy {
    #[must_use]
    pub(crate) fn from_blocking_conditions(blocking_conditions: [bool; 5]) -> Self {
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
    Inlined(u64),
    Fallback(SendableRecordBatchStream),
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

        let PreparedInsertStream {
            stream: mut prepared_stream,
            on_conflict_deletions,
            validated_keys,
        } = self.table.prepare_stream_for_insert(data).await?;

        let has_file_on_conflict_deletions = on_conflict_deletions.has_file_deletions();
        let has_on_conflict_deletions = !on_conflict_deletions.is_empty();
        let can_stage_for_pipeline = !pending_pk_deletions
            && !has_file_on_conflict_deletions
            && !has_on_conflict_deletions
            && !self.context.has_sort_columns()
            && self.table.metadata().partition_column.is_none()
            && !self.table.has_retention_filters();

        if !can_stage_for_pipeline {
            let _write_guard = write_guard;
            let rows = self
                .write_prepared_stream(
                    prepared_stream,
                    on_conflict_deletions,
                    pending_pk_deletions,
                    has_file_on_conflict_deletions,
                    validated_keys,
                )
                .await?;
            return Ok(CayenneCdcWrite::completed(
                self.table.clone_for_write_operations(),
                rows,
            ));
        }

        match self
            .try_inline_or_restream(prepared_stream, &[], &[])
            .await?
        {
            InlineMutationOutcome::Inlined(rows) => {
                self.table.record_inlined_pk_keys(&validated_keys);
                Ok(CayenneCdcWrite::completed(
                    self.table.clone_for_write_operations(),
                    rows,
                ))
            }
            InlineMutationOutcome::Fallback(re_stream) => {
                prepared_stream = re_stream;
                let staging_snapshot_id = self.table.new_staging_snapshot_id();
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
                    validated_keys,
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

        let PreparedInsertStream {
            stream: prepared_stream,
            on_conflict_deletions,
            validated_keys,
        } = self.table.prepare_stream_for_insert(data).await?;

        let has_file_on_conflict_deletions = on_conflict_deletions.has_file_deletions();

        self.write_prepared_stream(
            prepared_stream,
            on_conflict_deletions,
            pending_pk_deletions,
            has_file_on_conflict_deletions,
            validated_keys,
        )
        .await
    }

    async fn write_prepared_stream(
        &self,
        mut prepared_stream: SendableRecordBatchStream,
        on_conflict_deletions: OnConflictDeletions,
        pending_pk_deletions: bool,
        has_file_on_conflict_deletions: bool,
        validated_keys: std::collections::HashSet<arrow_row::OwnedRow>,
    ) -> Result<u64> {
        let has_on_conflict_deletions = !on_conflict_deletions.is_empty();

        tracing::debug!(
            "write_all_append: delete_specs={} files, deleted_keys={} keys, pending_deletions={}, on_conflict_deletions={}",
            on_conflict_deletions.file_delete_specs_count(),
            on_conflict_deletions.deleted_key_count(),
            pending_pk_deletions,
            has_on_conflict_deletions
        );

        let needs_new_snapshot = pending_pk_deletions || has_file_on_conflict_deletions;

        let inline_policy = InlineMutationPolicy::from_blocking_conditions([
            pending_pk_deletions,
            has_file_on_conflict_deletions,
            self.context.has_sort_columns(),
            self.table.metadata().partition_column.is_some(),
            self.table.has_retention_filters(),
        ]);

        if inline_policy.can_inline() {
            match self
                .try_inline_or_restream(
                    prepared_stream,
                    &on_conflict_deletions.deleted_inlined_pk_i64,
                    &on_conflict_deletions.deleted_inlined_row_keys,
                )
                .await?
            {
                InlineMutationOutcome::Inlined(rows) => {
                    self.table.record_inlined_pk_keys(&validated_keys);
                    return Ok(rows);
                }
                InlineMutationOutcome::Fallback(re_stream) => {
                    prepared_stream = re_stream;
                    let target_size_bytes = self.context.target_file_size_bytes();
                    let (rows, _writer_ops, stats_acc) = self
                        .write_staged_append(prepared_stream, target_size_bytes)
                        .await?;

                    self.table
                        .apply_on_conflict_deletions(on_conflict_deletions)
                        .await?;

                    let retention_deleted_rows = self.apply_retention_if_configured().await?;
                    let sorted = self.sort_if_configured().await?;
                    self.table.schedule_post_write_maintenance(
                        Some(stats_acc),
                        should_refresh_listing_table_after_post_write(
                            retention_deleted_rows,
                            sorted,
                        ),
                    );

                    if retention_deleted_rows > 0 {
                        self.table.clear_cached_pk_keyset();
                    } else {
                        self.table.record_file_pk_keys(&validated_keys);
                    }

                    return Ok(rows);
                }
            }
        }

        let (total_rows, write_stats_acc) = if needs_new_snapshot {
            self.table
                .apply_on_conflict_deletions(on_conflict_deletions)
                .await?;

            let new_sequence = self
                .table
                .catalog()
                .increment_sequence_number(self.table.table_id())
                .await?;

            self.table
                .insert_to_new_snapshot_with_sequence(
                    prepared_stream,
                    new_sequence,
                    self.task_context.session_config().target_partitions(),
                )
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

            self.table
                .apply_on_conflict_deletions(on_conflict_deletions)
                .await?;

            (rows, stats_acc)
        };

        let retention_deleted_rows = self.apply_retention_if_configured().await?;
        let sorted = self.sort_if_configured().await?;

        self.table.schedule_post_write_maintenance(
            Some(write_stats_acc),
            needs_new_snapshot
                || should_refresh_listing_table_after_post_write(retention_deleted_rows, sorted),
        );

        if retention_deleted_rows > 0 {
            self.table.clear_cached_pk_keyset();
        } else {
            self.table.record_file_pk_keys(&validated_keys);
        }

        Ok(total_rows)
    }

    async fn try_inline_or_restream(
        &self,
        mut prepared_stream: SendableRecordBatchStream,
        deleted_inlined_pk_i64: &[i64],
        deleted_inlined_row_keys: &[Box<[u8]>],
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

        if buffer.should_continue_buffering() && buffer.total_rows() == 0 {
            return Ok(InlineMutationOutcome::Inlined(0));
        }

        if buffer.should_continue_buffering()
            && self
                .table
                .try_inline_batches_with_inlined_deletions(
                    buffer.batches(),
                    deleted_inlined_pk_i64,
                    deleted_inlined_row_keys,
                )
                .await?
        {
            let stats_acc = ColumnStatsAccumulator::new(&schema);
            for batch in buffer.batches() {
                stats_acc.update(batch);
            }

            self.table
                .schedule_post_write_maintenance(Some(Arc::new(stats_acc)), false);

            if let Err(e) = self
                .table
                .checkpoint_inlined_data_if_memtable_pressure_exceeded()
                .await
            {
                tracing::warn!(
                    "Auto-checkpoint of inline memtable failed for {}: {e}",
                    self.table.table_name(),
                );
            }

            return Ok(InlineMutationOutcome::Inlined(
                u64::try_from(buffer.total_rows()).unwrap_or(u64::MAX),
            ));
        }

        let re_stream = buffer.into_chained_stream(prepared_stream, self.task_context)?;
        Ok(InlineMutationOutcome::Fallback(re_stream))
    }

    async fn write_staged_append(
        &self,
        stream: SendableRecordBatchStream,
        target_size_bytes: usize,
    ) -> Result<(u64, usize, Arc<ColumnStatsAccumulator>)> {
        let staging_snapshot_id = self.table.new_staging_snapshot_id();
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

        let staged_append = CayenneStagedAppend::from_staged_append_in(
            self.table.clone_for_write_operations(),
            None,
            staging_snapshot_id,
            result.0,
        );
        staged_append.finalize_staged_write().await?;

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

        let staged_append = CayenneStagedAppend::from_staged_append_in(
            self.table.clone_for_write_operations(),
                write_guard,
            staging_snapshot_id.clone(),
            rows,
        );
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

        Ok((rows, writer_ops, stats_acc, prepared_append))
    }

    async fn apply_retention_if_configured(&self) -> Result<u64> {
        if !self.table.has_retention_filters() {
            return Ok(0);
        }

        let deleted = self.table.apply_retention_filters().await?;
        if deleted > 0 {
            tracing::info!(
                "Retention filters deleted {} row(s) for table {}",
                deleted,
                self.table.table_name()
            );
        } else {
            tracing::debug!(
                "Retention filters found no rows to delete for table {}",
                self.table.table_name()
            );
        }
        Ok(deleted)
    }

    async fn sort_if_configured(&self) -> Result<bool> {
        if !self.context.has_sort_columns() {
            return Ok(false);
        }

        let target_size_bytes = self.context.target_file_size_bytes();
        self.table.sort_and_rewrite_data(target_size_bytes).await?;
        Ok(true)
    }
}

fn should_refresh_listing_table_after_post_write(
    retention_deleted_rows: u64,
    sorted: bool,
) -> bool {
    retention_deleted_rows > 0 || sorted
}

#[cfg(test)]
mod tests {
    use super::super::table::{INLINE_MAX_BUFFER_BYTES, INLINE_MAX_ROWS};
    use super::*;
    use arrow::array::{BinaryArray, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn inline_policy_requires_simple_append_shape() {
        assert!(InlineMutationPolicy::from_blocking_conditions([false; 5]).can_inline());

        for blocking_condition_index in 0..5 {
            let mut blocking_conditions = [false; 5];
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

    #[test]
    fn refresh_listing_table_only_when_post_write_steps_changed_files() {
        assert!(!should_refresh_listing_table_after_post_write(0, false));
        assert!(should_refresh_listing_table_after_post_write(1, false));
        assert!(should_refresh_listing_table_after_post_write(0, true));
        assert!(should_refresh_listing_table_after_post_write(1, true));
    }
}
