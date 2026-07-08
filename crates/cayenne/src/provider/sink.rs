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

//! A [`DataSink`] implementation that writes data to a Cayenne table.

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::sink::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::Result as DFResult;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use futures::StreamExt;

use super::context::CayenneContext;
use super::mutation_writer::AppendMutationWriter;
use super::table::CayenneTableProvider;

/// A [`DataSink`] implementation that writes data to a Cayenne table.
///
/// Supports two write modes via [`InsertOp`]:
/// - **Append**: Adds data to the current (or a new) snapshot, with PK on-conflict
///   handling, retention filters, and optional sorting.
/// - **Overwrite**: Replaces all data by writing to a fresh snapshot and atomically
///   updating the catalog.
///
/// # File Sizing
///
/// File sizing is delegated to the downstream Vortex/DataFusion writer using
/// the configured target file size (`VortexConfig.target_vortex_file_size_mb`,
/// default 256 MB).
///
/// # Performance
///
/// - **Streaming**: Forwards the input stream directly to the writer
/// - **Writer-managed buffering**: Avoids duplicate chunking heuristics in this sink
/// - **Zero-copy**: Reuses `RecordBatch` Arc references, no data copying
///
/// # Concurrency
///
/// A per-table write lock (acquired in [`DataSink::write_all`]) serializes all write
/// operations. Multiple concurrent `insert_into()` calls on the same table will block,
/// ensuring only one write runs at a time.
pub struct CayenneDataSink {
    /// The Cayenne table provider to write to.
    table: CayenneTableProvider,

    /// The insert operation mode (Append, Overwrite).
    overwrite: InsertOp,

    /// Schema of the data being written.
    schema: SchemaRef,

    /// Shared context containing configuration (file size, concurrency, sort columns, etc.)
    /// and cached resources (upload semaphore, Vortex format).
    context: Arc<CayenneContext>,
}

impl CayenneDataSink {
    /// Creates a new `CayenneDataSink`.
    ///
    /// # Arguments
    ///
    /// * `table` - The Cayenne table provider to write to
    /// * `overwrite` - The insert operation mode
    /// * `schema` - Schema of the data being written
    /// * `context` - Shared context with configuration and resources
    #[must_use]
    pub fn new(
        table: CayenneTableProvider,
        overwrite: InsertOp,
        schema: SchemaRef,
        context: Arc<CayenneContext>,
    ) -> Self {
        Self {
            table,
            overwrite,
            schema,
            context,
        }
    }
}

impl fmt::Debug for CayenneDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDataSink")
            .field("table", &self.table.table_name())
            .field("overwrite", &self.overwrite)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayenneDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "CayenneDataSink(table={}, mode={:?})",
                    self.table.table_name(),
                    self.overwrite
                )
            }
        }
    }
}

#[async_trait]
impl DataSink for CayenneDataSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        // Normalize incoming batches to the table schema (e.g. CDC nullability mismatches)
        // causing Vortex assertion failures.
        let target_schema = Arc::clone(&self.schema);
        let normalized = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&target_schema),
            data.map(move |batch_result| {
                batch_result.and_then(|batch| {
                    arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&target_schema))
                        .map_err(Into::into)
                })
            }),
        ));

        // Memory mode (`mode: memory`): all data lives in the RAM mem-tier, so
        // collect the stream and route it there — an atomic replace on
        // overwrite/full refresh, otherwise an append. Nothing is ever encoded to
        // Vortex. The write lock serializes memory-mode writes so an overwrite never
        // interleaves with a concurrent append.
        if self.table.is_memory_resident_mode() {
            let overwrite = self.overwrite == InsertOp::Overwrite;
            let mut data = normalized;
            let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
            let mut incoming_bytes: u64 = 0;
            while let Some(batch) = data.next().await {
                let batch = batch?;
                incoming_bytes =
                    incoming_bytes.saturating_add(batch.get_array_memory_size() as u64);
                batches.push(batch);
            }
            let _write_guard = self.table.write_lock().lock().await;
            return self
                .table
                .write_batches_memory_mode(batches, incoming_bytes, overwrite)
                .await
                .map_err(Into::into);
        }

        if self.overwrite == InsertOp::Overwrite {
            // Overwrite path: `CayenneTableProvider::begin_overwrite` acquires the
            // table write lock internally and the lock is held inside the
            // returned `PreparedOverwrite` until `finish`/`rollback`. Acquiring
            // it again here would deadlock.
            self.write_all_overwrite(normalized, context)
                .await
                .map_err(Into::into)
        } else if let Some(interval) = self.context.stream_publish_interval() {
            // Append path with bounded publish latency: cut the input stream
            // into age/size-bounded segments and run a complete
            // prepare→stage→publish write per segment, so rows on a long-lived
            // insert stream (e.g. ADBC bulk ingest) become queryable within
            // ~`interval` of arrival instead of only when the stream ends.
            self.write_append_segmented(normalized, context, interval)
                .await
                .map_err(Into::into)
        } else {
            // Append path: `write_all_append` uses the existing-staging helpers
            // that assume the caller already holds the write lock.
            let _write_guard = self.table.write_lock().lock().await;
            self.write_all_append(normalized, context)
                .await
                .map_err(Into::into)
        }
    }
}

impl CayenneDataSink {
    /// Append with bounded ingest-to-queryable latency: consume the input in
    /// segments, each capped by age (`interval`, measured from the segment's
    /// first buffered batch) and by in-memory size, and run the full existing
    /// append write (prepare → stage → publish) per segment under the table
    /// write lock.
    ///
    /// Each segment is exactly one pre-existing whole-stream write, so
    /// durability and crash recovery per segment are unchanged. On a
    /// mid-stream error, segments already published stay published — the same
    /// visible state as if the client had sent them as separate requests;
    /// PK on-conflict handling keeps whole-payload retries convergent.
    ///
    /// The size cap bounds buffered memory per active stream (segments are
    /// buffered before writing): the configured target file size, clamped to
    /// [8 MiB, 256 MiB] (256 MiB when size-rolling is disabled).
    async fn write_append_segmented(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
        interval: std::time::Duration,
    ) -> super::Result<u64> {
        const MIN_SEGMENT_BYTES: usize = 8 * 1024 * 1024;
        const MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
        let target = self.context.target_file_size_bytes();
        let byte_cap = if target == 0 {
            MAX_SEGMENT_BYTES
        } else {
            target.clamp(MIN_SEGMENT_BYTES, MAX_SEGMENT_BYTES)
        };

        let mut total_rows: u64 = 0;
        let mut segments: u64 = 0;
        let mut stream_ended = false;
        while !stream_ended {
            let mut segment: Vec<arrow::record_batch::RecordBatch> = Vec::new();
            let mut segment_bytes = 0usize;
            let mut deadline: Option<tokio::time::Instant> = None;
            loop {
                if segment_bytes >= byte_cap {
                    break;
                }
                let next = if let Some(deadline) = deadline {
                    match tokio::time::timeout_at(deadline, data.next()).await {
                        Ok(item) => item,
                        Err(_elapsed) => break,
                    }
                } else {
                    // Empty segment: wait for the first batch without a
                    // deadline so an idle stream never publishes empties and
                    // the age budget starts at first buffered data.
                    data.next().await
                };
                match next {
                    Some(Ok(batch)) => {
                        segment_bytes += batch.get_array_memory_size();
                        segment.push(batch);
                        if deadline.is_none() {
                            deadline = Some(tokio::time::Instant::now() + interval);
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => {
                        stream_ended = true;
                        break;
                    }
                }
            }
            if segment.is_empty() {
                continue;
            }
            segments += 1;
            let segment_stream: SendableRecordBatchStream =
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    futures::stream::iter(segment.into_iter().map(Ok)),
                ));
            let segment_start = std::time::Instant::now();
            let _write_guard = self.table.write_lock().lock().await;
            let lock_wait_ms = segment_start.elapsed().as_millis();
            let segment_rows = self.write_all_append(segment_stream, context).await?;
            total_rows += segment_rows;
            tracing::debug!(
                table = self.table.table_name(),
                segment = segments,
                rows = segment_rows,
                bytes = segment_bytes,
                lock_wait_ms,
                duration_ms = segment_start.elapsed().as_millis(),
                "Streaming append segment published"
            );
        }
        if segments > 1 {
            tracing::debug!(
                table = self.table.table_name(),
                segments,
                rows = total_rows,
                "Streaming append published in segments"
            );
        }
        Ok(total_rows)
    }

    /// Append data from a record batch stream into the Cayenne table.
    ///
    /// Writes data to the current snapshot (via [`CayenneTableProvider::chunk_and_write_parallel`])
    /// or stages a new snapshot when deletion isolation is needed.
    ///
    /// # Write Pipeline
    ///
    /// 1. Checks for pending PK-based deletions
    /// 2. Validates primary key on-conflict constraints via `prepare_stream_for_insert`
    /// 3. Writes data to a new snapshot (if PK-based deletions pending or on-conflict
    ///    deletions exist) or the current snapshot
    /// 4. Applies on-conflict deletion vectors
    /// 5. Applies retention filters
    /// 6. Sorts and rewrites data if configured
    /// 7. Refreshes the listing table
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be inserted.
    async fn write_all_append(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> super::Result<u64> {
        AppendMutationWriter::new(&self.table, &self.context, context)
            .write(data)
            .await
    }

    /// Handles overwrite-mode writes by staging the new snapshot via
    /// [`CayenneTableProvider::begin_overwrite`] and then committing it
    /// through a dedicated single-partition transaction.
    ///
    /// 1. [`begin_overwrite`] writes the input stream to a fresh
    ///    `<table_id>/<new_snapshot>/` directory and acquires the table write
    ///    lock. `target_partitions` is sourced from the session config so the
    ///    underlying parallel-writer fan-out matches the rest of the query.
    /// 2. [`PreparedOverwrite::apply_owned_txn`] flips the catalog pointer
    ///    via the trait-based `commit_compaction` (own transaction, with
    ///    retry-on-conflict), preserving exact pre-issue-#10125 retry
    ///    semantics for non-coordinated writes.
    /// 3. [`PreparedOverwrite::finish`] updates the in-memory snapshot id,
    ///    listing table, deletion caches, inlined-data caches, and triggers
    ///    old-snapshot GC.
    ///
    /// Cross-partition coordinators (issue #10125 step 4b) take the same
    /// `PreparedOverwrite` handle and call
    /// [`PreparedOverwrite::apply_in_txn`] inside one shared transaction
    /// so every participating partition's pointer flip is atomic.
    async fn write_all_overwrite(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> super::Result<u64> {
        let target_partitions = context.session_config().target_partitions();
        let prepared = self.table.begin_overwrite(data, target_partitions).await?;
        prepared
            .apply_owned_txn()
            .await
            .map_err(super::Error::from)?;
        prepared.finish().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::sink::DataSink;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::dml::InsertOp;
    use datafusion_table_providers::util::column_reference::ColumnReference;
    use datafusion_table_providers::util::on_conflict::OnConflict;
    use tokio::sync::Notify;

    use super::CayenneDataSink;
    use crate::CayenneCatalog;
    use crate::MetadataCatalog;
    use crate::metadata::{CreateTableOptions, VortexConfig};
    use crate::provider::context::CayenneContext;
    use crate::provider::table::{CayenneTableProvider, CayenneTableProviderBuilder};

    async fn visible_rows(ctx: &SessionContext, provider: &CayenneTableProvider) -> usize {
        let df = ctx
            .read_table(Arc::new(provider.clone_for_write()))
            .expect("read_table");
        df.count().await.expect("count")
    }

    /// The age-bounded segment cut: rows streamed on a still-open append stream
    /// become queryable within ~`stream_publish_interval_ms`, without waiting
    /// for the stream to end. Guards the events-mode ingest-to-queryable
    /// latency fix (long-lived ADBC bulk-ingest streams).
    #[tokio::test(flavor = "multi_thread")]
    async fn stream_publish_interval_publishes_before_stream_end() {
        let ctx = SessionContext::new();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("path"));
        let data_dir = format!("{}/data", temp_dir.path().to_str().expect("path"));
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
        let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog"))
            as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog init");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let vortex_config = VortexConfig {
            stream_publish_interval_ms: 100,
            ..VortexConfig::default()
        };
        let context = CayenneContext::new(&vortex_config, ctx.runtime_env(), "seg_pub");
        let sink_context = Arc::clone(&context);
        let options = CreateTableOptions {
            table_name: "seg_pub".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_dir,
            partition_column: None,
            vortex_config,
        };
        let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .with_context(context)
            .create(options)
            .await
            .expect("table created");

        let release_second = Arc::new(Notify::new());
        let release_for_stream = Arc::clone(&release_second);
        let stream_schema = Arc::clone(&schema);
        let batches = futures::stream::unfold(0_i64, move |i| {
            let release = Arc::clone(&release_for_stream);
            let schema = Arc::clone(&stream_schema);
            async move {
                match i {
                    0 => {
                        let batch = RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
                        )
                        .expect("batch");
                        Some((Ok(batch), 1))
                    }
                    1 => {
                        // Hold the stream open until the test observes the
                        // first segment's rows.
                        release.notified().await;
                        let batch = RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(Int64Array::from(vec![3_i64]))],
                        )
                        .expect("batch");
                        Some((Ok(batch), 2))
                    }
                    _ => None,
                }
            }
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), batches));

        let sink = CayenneDataSink::new(
            provider.clone_for_write(),
            InsertOp::Append,
            Arc::clone(&schema),
            sink_context,
        );
        let task_ctx = ctx.task_ctx();
        let write = tokio::spawn(async move { sink.write_all(stream, &task_ctx).await });

        // Rows from the first batch must become visible while the stream is
        // still open (the second batch is gated on `release_second`).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if visible_rows(&ctx, &provider).await == 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "first segment was not published while the stream was open"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        release_second.notify_one();
        let written = write.await.expect("join").expect("write_all");
        assert_eq!(written, 3, "all rows accounted across segments");
        assert_eq!(
            visible_rows(&ctx, &provider).await,
            3,
            "all rows visible after stream end"
        );
    }
}
