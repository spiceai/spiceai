// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::Any;
use std::sync::Arc;

use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Result as DFResult;
use datafusion_common::arrow::array::RecordBatch;
use datafusion_common::arrow::array::RecordBatchOptions;
use datafusion_common::exec_datafusion_err;
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSink;
use datafusion_datasource::write::get_writer_schema;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::metrics::Time;
use datafusion_physical_plan::repartition::BatchPartitioner;
use futures::SinkExt;
use futures::StreamExt;
use object_store::Error as ObjectStoreError;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteSummary;
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::session::VortexSession;
use vortex_utils::aliases::hash_set::HashSet;

/// How [`VortexSink`] fans a single input stream into N concurrent file writers.
///
/// `Single` reproduces the historical one-writer-per-statement behavior. The
/// parallel variants spawn one [`ActiveFileWriter`] per shard so Vortex column
/// compression runs concurrently across cores within a single write. The input
/// stream is always a single (coalesced) partition — `DataSinkExec` requires
/// `SinglePartition` — so the fan-out is performed here, in the sink.
#[derive(Debug, Clone)]
pub enum ShardSpec {
    /// One writer; byte-for-byte identical to the pre-sharding behavior.
    Single,
    /// `n` writers, batches distributed round-robin (parallel encode, no clustering).
    RoundRobin(usize),
    /// `n` writers, rows hash-partitioned by `exprs` (parallel encode + key-clustered files).
    Hash {
        exprs: Vec<PhysicalExprRef>,
        partitions: usize,
    },
}

impl ShardSpec {
    /// Number of output shards (concurrent writers) this spec requests.
    #[must_use]
    pub fn partitions(&self) -> usize {
        match self {
            ShardSpec::Single => 1,
            ShardSpec::RoundRobin(n) => *n,
            ShardSpec::Hash { partitions, .. } => *partitions,
        }
    }

    /// Build a `BatchPartitioner` that routes each input batch to one of
    /// `num_shards` shards according to this spec. `Single`/`RoundRobin` route
    /// whole batches; `Hash` splits batches row-wise on the key expressions.
    fn batch_partitioner(&self, num_shards: usize) -> BatchPartitioner {
        let timer = Time::default();
        match self {
            ShardSpec::Hash { exprs, .. } => {
                BatchPartitioner::new_hash_partitioner(exprs.clone(), num_shards, timer)
            }
            ShardSpec::Single | ShardSpec::RoundRobin(_) => {
                BatchPartitioner::new_round_robin_partitioner(num_shards, timer, 0, 1)
            }
        }
    }
}

struct WriteOutputOptions<'a> {
    base_output_path: &'a ListingTableUrl,
    target_file_size: Option<u64>,
    extension: &'a str,
    write_id: &'a str,
    partition_column_names: &'a [String],
    keep_partition_by_columns: bool,
    shard_spec: &'a ShardSpec,
}

#[derive(Clone, Copy)]
struct CompressionEstimate {
    prev_compressed_bytes: u64,
    prev_uncompressed_bytes: u64,
}

impl CompressionEstimate {
    fn identity() -> Self {
        Self {
            prev_compressed_bytes: 1,
            prev_uncompressed_bytes: 1,
        }
    }

    fn from_file_sizes(compressed_bytes: u64, uncompressed_bytes: u64) -> DFResult<Self> {
        if uncompressed_bytes == 0 {
            return Err(exec_datafusion_err!(
                "Cannot derive compression estimate from zero uncompressed bytes"
            ));
        }

        Ok(Self {
            prev_compressed_bytes: compressed_bytes,
            prev_uncompressed_bytes: uncompressed_bytes,
        })
    }

    fn estimate_compressed_size(self, uncompressed_bytes: u64) -> DFResult<u64> {
        if self.prev_uncompressed_bytes == 0 {
            return Err(exec_datafusion_err!(
                "Compression estimate denominator must be non-zero"
            ));
        }

        let estimated = u128::from(uncompressed_bytes)
            .checked_mul(u128::from(self.prev_compressed_bytes))
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "Compressed size estimate overflow for {} * {}",
                    uncompressed_bytes,
                    self.prev_compressed_bytes
                )
            })?
            / u128::from(self.prev_uncompressed_bytes);

        u64::try_from(estimated).map_err(|_| {
            exec_datafusion_err!("Compressed size estimate does not fit in u64: {estimated}")
        })
    }
}

struct ActiveFileWriter {
    path: Path,
    sender: futures::channel::mpsc::Sender<RecordBatch>,
    task: JoinHandle<DFResult<WriteSummary>>,
}

pub struct VortexSink {
    config: FileSinkConfig,
    schema: SchemaRef,
    session: VortexSession,
    target_file_size: Option<u64>,
    shard_spec: ShardSpec,
}

impl VortexSink {
    pub fn new(
        config: FileSinkConfig,
        schema: SchemaRef,
        session: VortexSession,
        target_file_size: Option<u64>,
        shard_spec: ShardSpec,
    ) -> Self {
        Self {
            config,
            schema,
            session,
            target_file_size,
            shard_spec,
        }
    }

    fn base_output_path(&self) -> DFResult<&ListingTableUrl> {
        self.config
            .table_paths
            .first()
            .ok_or_else(|| exec_datafusion_err!("Vortex sink requires at least one table path"))
    }
}

impl std::fmt::Debug for VortexSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexSink").finish()
    }
}

impl DisplayAs for VortexSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "VortexSink")
            }
        }
    }
}

#[async_trait]
impl DataSink for VortexSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;
        let writer_schema = get_writer_schema(&self.config);
        let dtype = DType::from_arrow(writer_schema);
        let write_id = Uuid::now_v7().simple().to_string();
        let base_output_path = self.base_output_path()?;
        let partition_column_names = self
            .config
            .table_partition_cols
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();

        let summaries = write_record_batch_stream_to_files(
            self.session.clone(),
            object_store,
            dtype,
            data,
            &WriteOutputOptions {
                base_output_path,
                target_file_size: self.target_file_size,
                extension: &self.config.file_extension,
                write_id: &write_id,
                partition_column_names: &partition_column_names,
                keep_partition_by_columns: self.config.keep_partition_by_columns,
                shard_spec: &self.shard_spec,
            },
        )
        .await?;

        let mut row_count = 0_u64;
        for (path, summary) in summaries {
            row_count = row_count.checked_add(summary.row_count()).ok_or_else(|| {
                exec_datafusion_err!(
                    "Row count overflow while aggregating sink summaries (current={}, file={})",
                    row_count,
                    summary.row_count()
                )
            })?;
            tracing::debug!(path = %path, "Successfully written file");
        }

        Ok(row_count)
    }
}

/// Write batches from a single input stream to one or more output files,
/// fanning the work across `ShardSpec::partitions()` concurrent shard writers.
///
/// `DataSinkExec` always hands us a single (coalesced) input partition, so the
/// parallelism is created here: the input is demuxed (round-robin or hashed by
/// key) across N shards, each shard runs an independent [`run_shard_writer`]
/// task, and Vortex column compression therefore runs concurrently across
/// cores. Single-file output and Hive-partitioned writes stay on one shard.
///
/// For collection paths, files are emitted using the
/// `{write_id}_{file_index:05}.{extension}` scheme for a single shard, or
/// `{write_id}_p{shard:03}_{file_index:05}.{extension}` when sharded.
async fn write_record_batch_stream_to_files(
    session: VortexSession,
    object_store: Arc<dyn ObjectStore>,
    dtype: DType,
    mut data: SendableRecordBatchStream,
    output_options: &WriteOutputOptions<'_>,
) -> DFResult<Vec<(Path, WriteSummary)>> {
    let target = output_options.target_file_size.map(|t| t.max(1));
    let single_file_output = !output_options.base_output_path.is_collection()
        && output_options.base_output_path.file_extension().is_some();

    // Single-file output (an exact target path) and Hive-partitioned writes
    // stay on one writer to preserve their path / sub-directory contracts.
    let num_shards = if single_file_output || !output_options.partition_column_names.is_empty() {
        1
    } else {
        output_options.shard_spec.partitions().max(1)
    };

    // Owned copies for the spawned shard tasks.
    let write_id = output_options.write_id.to_string();
    let extension = output_options.extension.to_string();
    let base_output_path = output_options.base_output_path.clone();

    // One writer task per shard, each fed by a bounded channel so a slow shard
    // exerts backpressure on the demux loop (peak buffer ~= 2 * N batches).
    let mut senders = Vec::with_capacity(num_shards);
    let mut handles = Vec::with_capacity(num_shards);
    let started_paths = Arc::new(Mutex::new(HashSet::new()));
    for shard_id in 0..num_shards {
        let (tx, rx) = futures::channel::mpsc::channel::<RecordBatch>(1);
        senders.push(tx);
        handles.push(tokio::spawn(run_shard_writer(
            session.clone(),
            Arc::clone(&object_store),
            dtype.clone(),
            rx,
            target,
            write_id.clone(),
            base_output_path.clone(),
            extension.clone(),
            single_file_output,
            shard_id,
            num_shards,
            Arc::clone(&started_paths),
        )));
    }

    let mut partitioner =
        (num_shards > 1).then(|| output_options.shard_spec.batch_partitioner(num_shards));

    // A failed send means a shard writer already dropped its receiver — i.e. it
    // exited early with an error (the happy path holds the receiver open until
    // we drop the sender below). Record that and stop routing; the join loop
    // surfaces that shard's real error, so we never mask the root cause (a
    // disk-full write, a cancelled task on shutdown, etc.) behind a generic
    // "receiver is gone". Genuine upstream/transform failures still propagate
    // via `?` and take precedence.
    let mut shard_closed_early = false;
    let demux_result: DFResult<()> = async {
        while let Some(batch) = data.next().await.transpose()? {
            let batch = if output_options.keep_partition_by_columns
                || output_options.partition_column_names.is_empty()
            {
                batch
            } else {
                remove_partition_columns(&batch, output_options.partition_column_names)?
            };

            match partitioner.as_mut() {
                None => {
                    if senders[0].send(batch).await.is_err() {
                        shard_closed_early = true;
                        break;
                    }
                }
                Some(partitioner) => {
                    // `partition` invokes the closure synchronously, so collect
                    // the (shard, sub-batch) pairs and await the sends afterward.
                    let mut routed: Vec<(usize, RecordBatch)> = Vec::new();
                    partitioner.partition(batch, |idx, sub| {
                        routed.push((idx, sub));
                        Ok(())
                    })?;
                    for (idx, sub) in routed {
                        if senders[idx].send(sub).await.is_err() {
                            shard_closed_early = true;
                            break;
                        }
                    }
                    if shard_closed_early {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    // Close all senders so each shard finalizes its trailing file.
    drop(senders);

    // Join every shard, aggregating results and surfacing the first error.
    // Precedence: a real upstream/transform error from the demux, then any
    // shard writer's own error, then a defensive fallback if a shard closed its
    // receiver early yet somehow reported success.
    let mut results: Vec<(Path, WriteSummary)> = Vec::new();
    let mut first_err = demux_result.err();
    for handle in handles {
        match handle.await {
            Ok(Ok(shard_results)) => results.extend(shard_results),
            Ok(Err(err)) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(join_err) => {
                if first_err.is_none() {
                    first_err = Some(exec_datafusion_err!(
                        "Vortex sink shard writer task failed to join: {join_err}"
                    ));
                }
            }
        }
    }

    if first_err.is_none() && shard_closed_early {
        first_err = Some(exec_datafusion_err!(
            "Vortex sink shard writer closed early before the input stream was fully routed"
        ));
    }

    if let Some(err) = first_err {
        let cleanup_paths = {
            let started_paths = started_paths.lock().await;
            started_paths.iter().cloned().collect()
        };
        cleanup_failed_write(object_store, Vec::new(), cleanup_paths).await;
        return Err(err);
    }

    Ok(results)
}

/// A single shard writer: drains its receiver, rolling output files by
/// estimated compressed size (independent per shard), and returns the files it
/// wrote. On error it cleans up its own active + finished files before
/// returning; the caller aggregates across shards.
#[expect(clippy::too_many_arguments)]
async fn run_shard_writer(
    session: VortexSession,
    object_store: Arc<dyn ObjectStore>,
    dtype: DType,
    mut receiver: futures::channel::mpsc::Receiver<RecordBatch>,
    target: Option<u64>,
    write_id: String,
    base_output_path: ListingTableUrl,
    extension: String,
    single_file_output: bool,
    shard_id: usize,
    num_shards: usize,
    started_paths: Arc<Mutex<HashSet<Path>>>,
) -> DFResult<Vec<(Path, WriteSummary)>> {
    let mut results: Vec<(Path, WriteSummary)> = Vec::new();
    let mut active_writer: Option<ActiveFileWriter> = None;
    let mut uncompressed_bytes_in_file = 0_u64;
    let mut file_index = 0_usize;
    let mut compression_estimate = CompressionEstimate::identity();

    let write_result: DFResult<()> = async {
        while let Some(batch) = receiver.next().await {
            if active_writer.is_none() {
                let file_path = output_file_path(
                    &base_output_path,
                    file_index,
                    &extension,
                    single_file_output,
                    &write_id,
                    shard_id,
                    num_shards,
                );
                started_paths.lock().await.insert(file_path.clone());
                active_writer = Some(start_file_writer(
                    &session,
                    Arc::clone(&object_store),
                    file_path,
                    dtype.clone(),
                ));
            }

            let batch_bytes = batch_uncompressed_bytes(&batch)?;
            send_batch_to_active_writer(&mut active_writer, batch).await?;
            let active_path = active_writer
                .as_ref()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "Missing active file writer while updating sink byte counter"
                    )
                })?
                .path
                .as_ref();
            uncompressed_bytes_in_file = uncompressed_bytes_in_file
                .checked_add(batch_bytes)
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "Uncompressed byte counter overflow for sink output file {active_path}"
                    )
                })?;

            if let Some(target) = target {
                let estimated_compressed =
                    compression_estimate.estimate_compressed_size(uncompressed_bytes_in_file)?;
                if estimated_compressed >= target {
                    let writer = active_writer.take().ok_or_else(|| {
                        exec_datafusion_err!(
                            "Missing active file writer while finalizing rotated output file"
                        )
                    })?;
                    let file_path = writer.path.clone();
                    let summary = finish_file_writer(writer).await?;
                    if uncompressed_bytes_in_file > 0 {
                        compression_estimate = CompressionEstimate::from_file_sizes(
                            summary.size(),
                            uncompressed_bytes_in_file,
                        )?;
                    }

                    results.push((file_path, summary));
                    uncompressed_bytes_in_file = 0;
                    file_index += 1;
                }
            }
        }

        if let Some(writer) = active_writer.take() {
            let file_path = writer.path.clone();
            let summary = finish_file_writer(writer).await?;
            results.push((file_path, summary));
        }

        Ok(())
    }
    .await;

    if let Err(err) = write_result {
        cleanup_failed_write(
            object_store,
            active_writer.into_iter().collect(),
            results.iter().map(|(path, _)| path.clone()).collect(),
        )
        .await;
        return Err(err);
    }

    Ok(results)
}

/// Generate a numbered file path from an existing path for size-based splitting.
///
/// Given `base/file.vortex`, produces `base/file_00000.vortex`.
/// If the path has no recognized extension, appends `_00000.{extension}`.
fn numbered_path(original: &Path, index: usize, extension: &str) -> Path {
    let s = original.to_string();
    let suffix = format!(".{extension}");
    if let Some(stem) = s.strip_suffix(&suffix) {
        Path::from(format!("{stem}_{index:05}{suffix}"))
    } else {
        Path::from(format!("{s}_{index:05}.{extension}"))
    }
}
fn start_file_writer(
    session: &VortexSession,
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    dtype: DType,
) -> ActiveFileWriter {
    // Use a small bounded channel to enforce backpressure and avoid unbounded buffering.
    let (sender, receiver) = futures::channel::mpsc::channel::<RecordBatch>(1);
    let session = session.clone();
    let path_for_task = path.clone();

    let task = tokio::spawn(async move {
        let mut object_writer = ObjectStoreWrite::new(object_store, &path_for_task)
            .await
            .map_err(|e| {
                exec_datafusion_err!(
                    "Failed to create ObjectStoreWrite for '{}': {e}",
                    path_for_task
                )
            })?;

        let stream = receiver.map(|rb| ArrayRef::from_arrow(rb, false));
        let stream_adapter = ArrayStreamAdapter::new(dtype, stream);

        let summary = session
            .write_options()
            .write(&mut object_writer, stream_adapter)
            .await
            .map_err(|e| {
                exec_datafusion_err!("Failed to write Vortex file '{}': {e}", path_for_task)
            })?;

        object_writer.shutdown().await.map_err(|e| {
            exec_datafusion_err!("Failed to shutdown Vortex writer '{}': {e}", path_for_task)
        })?;

        Ok(summary)
    });

    ActiveFileWriter { path, sender, task }
}

async fn cleanup_failed_write(
    object_store: Arc<dyn ObjectStore>,
    active_writers: Vec<ActiveFileWriter>,
    finished_paths: Vec<Path>,
) {
    let mut cleanup_paths = HashSet::new();

    for writer in active_writers {
        cleanup_paths.insert(writer.path.clone());
        writer.task.abort();
        drop(writer.task.await);
    }

    for path in finished_paths {
        cleanup_paths.insert(path);
    }

    for path in cleanup_paths {
        match object_store.delete(&path).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {}
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "Failed to delete sink output during error cleanup");
            }
        }
    }
}

/// Send a batch to the shard's active encoder task. A failed send means that
/// task already dropped its receiver — i.e. it exited, ~always with an error —
/// so finish the writer to surface that real error (e.g. a disk-full write)
/// instead of the generic "receiver is gone" send failure. Takes the writer
/// slot so the dead writer is consumed and its task joined exactly once, never
/// double-joined by later cleanup.
async fn send_batch_to_active_writer(
    active_writer: &mut Option<ActiveFileWriter>,
    batch: RecordBatch,
) -> DFResult<()> {
    let writer = active_writer
        .as_mut()
        .ok_or_else(|| exec_datafusion_err!("Missing active file writer for sink output"))?;
    if writer.sender.send(batch).await.is_ok() {
        return Ok(());
    }
    let writer = active_writer
        .take()
        .ok_or_else(|| exec_datafusion_err!("Missing active file writer after failed send"))?;
    let path = writer.path.clone();
    Err(match finish_file_writer(writer).await {
        Ok(_) => {
            exec_datafusion_err!(
                "Vortex writer task for '{path}' exited before consuming all input"
            )
        }
        Err(real) => real,
    })
}

async fn finish_file_writer(mut writer: ActiveFileWriter) -> DFResult<WriteSummary> {
    writer.sender.close_channel();
    match writer.task.await {
        Ok(result) => result,
        Err(e) => Err(exec_datafusion_err!(
            "Vortex writer task for '{}' failed to join: {e}",
            writer.path
        )),
    }
}

fn batch_uncompressed_bytes(batch: &RecordBatch) -> DFResult<u64> {
    u64::try_from(batch.get_array_memory_size()).map_err(|_| {
        exec_datafusion_err!(
            "RecordBatch memory size does not fit in u64: {}",
            batch.get_array_memory_size()
        )
    })
}

fn remove_partition_columns(
    batch: &RecordBatch,
    partition_column_names: &[String],
) -> DFResult<RecordBatch> {
    let partition_name_set = partition_column_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let projection = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            (!partition_name_set.contains(field.name().as_str())).then_some(index)
        })
        .collect::<Vec<_>>();

    if projection.is_empty() {
        let schema = Schema::empty();
        let options = RecordBatchOptions::default().with_row_count(Some(batch.num_rows()));
        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema),
            vec![],
            &options,
        )?);
    }

    Ok(batch.project(&projection)?)
}

/// Build the output path for a rolling write.
///
/// With a single shard (`num_shards <= 1`) the historical
/// `{write_id}_{file_index:05}` scheme is used; with multiple shards the path
/// gains a `_p{shard_id:03}` segment so concurrent shards never collide while
/// keeping a single `write_id` per statement.
fn output_file_path(
    base_output_path: &ListingTableUrl,
    file_index: usize,
    extension: &str,
    single_file_output: bool,
    write_id: &str,
    shard_id: usize,
    num_shards: usize,
) -> Path {
    if single_file_output {
        if file_index == 0 {
            return base_output_path.prefix().clone();
        }
        return numbered_path(base_output_path.prefix(), file_index, extension);
    }

    let mut base = base_output_path.prefix().to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    if num_shards > 1 {
        Path::from(format!(
            "{base}{write_id}_p{shard_id:03}_{file_index:05}.{extension}"
        ))
    } else {
        Path::from(format!("{base}{write_id}_{file_index:05}.{extension}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use datafusion::arrow::array::Int8Array;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::Values;
    use datafusion_common::ScalarValue;
    use datafusion_common::exec_datafusion_err;
    use datafusion_datasource::ListingTableUrl;
    use datafusion_datasource::file_format::format_as_file_type;
    use futures::TryStreamExt;
    use rstest::rstest;
    use tokio::sync::oneshot;
    use tokio::time::Duration;

    use std::collections::HashMap;
    use std::collections::HashSet;

    use arrow_schema::SchemaRef;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_execution::SendableRecordBatchStream;
    use datafusion_physical_expr::PhysicalExprRef;
    use datafusion_physical_expr::expressions::Column;
    use object_store::path::Path;
    use vortex::VortexSessionDefault;
    use vortex::dtype::DType;
    use vortex::dtype::arrow::FromArrowType;
    use vortex::file::WriteSummary;
    use vortex::session::VortexSession;

    use crate::common_tests::TestSessionContext;
    use crate::persistent::VortexFormatFactory;
    use crate::persistent::VortexTableOptions;
    use crate::persistent::sink::ActiveFileWriter;
    use crate::persistent::sink::ShardSpec;
    use crate::persistent::sink::WriteOutputOptions;
    use crate::persistent::sink::finish_file_writer;
    use crate::persistent::sink::write_record_batch_stream_to_files;

    fn split_path(
        base_path: &object_store::path::Path,
        file_index: usize,
        extension: &str,
    ) -> object_store::path::Path {
        let mut base = base_path.to_string();
        if !base.ends_with('/') {
            base.push('/');
        }
        let filename = format!("part-{file_index:05}.{extension}");
        object_store::path::Path::from(format!("{base}{filename}"))
    }

    #[tokio::test]
    async fn test_insert_into_sql() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                    (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/';",
            )
            .await?;

        ctx.session
            .sql("INSERT INTO my_tbl VALUES ('hello', 1), ('world', 2);")
            .await?
            .collect()
            .await?;

        let batches = ctx
            .session
            .sql("SELECT * from my_tbl")
            .await?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            &[
                "+-------+----+",
                "| c1    | c2 |",
                "+-------+----+",
                "| hello | 1  |",
                "| world | 2  |",
                "+-------+----+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_logical_plan() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                    (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/';",
            )
            .await?;

        let my_tbl = ctx.session.table("my_tbl").await?;

        // It's valuable to have two insert code paths because they actually behave slightly differently
        let values = Values {
            schema: Arc::new(my_tbl.schema().clone()),
            values: vec![vec![
                Expr::Literal(ScalarValue::new_utf8view("hello"), None),
                Expr::Literal(42_i32.into(), None),
            ]],
        };

        let tbl_provider = ctx.session.table_provider("my_tbl").await?;

        let logical_plan = LogicalPlanBuilder::insert_into(
            LogicalPlan::Values(values.clone()),
            "my_tbl",
            Arc::new(DefaultTableSource::new(tbl_provider.clone())),
            datafusion::logical_expr::dml::InsertOp::Append,
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let batches = ctx.session.read_table(tbl_provider)?.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+-------+----+",
                "| c1    | c2 |",
                "+-------+----+",
                "| hello | 42 |",
                "+-------+----+",
            ],
            &batches
        );

        Ok(())
    }

    /// Reproduction by <https://github.com/vortex-data/vortex/issues/4315>.
    /// Uses a 1MB target file size to exercise file splitting behavior.
    #[rstest]
    #[case(1_000, 1)]
    #[case(5_000_000, 6)]
    #[case(10_000_000, 10)]
    #[tokio::test]
    async fn test_write_large_batch(
        #[case] entries: usize,
        #[case] expected_files: usize,
    ) -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        let opts = VortexTableOptions {
            target_file_size_mb: 1,
            ..Default::default()
        };

        let factory = VortexFormatFactory::new().with_options(opts);

        let values: Vec<i8> = (0..entries)
            .map(|i| i8::try_from(i % 127))
            .collect::<Result<_, _>>()?;

        let data = ctx.session.read_batch(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)])),
            vec![Arc::new(Int8Array::from(values))],
        )?)?;

        let logical_plan = LogicalPlanBuilder::copy_to(
            data.logical_plan().clone(),
            "/table/".to_string(),
            format_as_file_type(Arc::new(factory)),
            Default::default(),
            vec![],
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let result = ctx
            .session
            .sql("SELECT COUNT(*) as count FROM '/table/'")
            .await?
            .collect()
            .await?;

        assert_eq!(result.len(), 1);
        let count_batch = &result[0];
        assert_eq!(count_batch.num_rows(), 1);

        let count_value = count_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);

        assert_eq!(
            count_value, entries as i64,
            "Expected {} entries, but found {}",
            entries, count_value
        );

        let file_metas = ctx
            .store
            .list(Some(&"table".into()))
            .try_collect::<Vec<_>>()
            .await?;

        assert!(
            file_metas.len() >= expected_files,
            "Expected at least {expected_files} files for {entries} values, got {}",
            file_metas.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_large_batch_default_target_is_128mb() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        let entries = 1_000_000;
        let values: Vec<i8> = (0..entries)
            .map(|i| i8::try_from(i % 127))
            .collect::<Result<_, _>>()?;

        let data = ctx.session.read_batch(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)])),
            vec![Arc::new(Int8Array::from(values))],
        )?)?;

        let logical_plan = LogicalPlanBuilder::copy_to(
            data.logical_plan().clone(),
            "/table/".to_string(),
            format_as_file_type(Arc::new(VortexFormatFactory::new())),
            Default::default(),
            vec![],
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let file_metas = ctx
            .store
            .list(Some(&"/table".into()))
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(file_metas.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_partitioned() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        let _unused = ctx
            .session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/' \
                PARTITIONED BY (c1);",
            )
            .await?;

        ctx.session
            .sql("INSERT INTO my_tbl (c1, c2) VALUES ('world', 24), ('world', 25), ('hello', 42);")
            .await?
            .collect()
            .await?;

        let table = ctx.session.table("my_tbl").await?;
        assert_eq!(table.count().await?, 3);

        let location = object_store::path::Path::parse("table/")?;
        let file_metas = ctx
            .store
            .list(Some(&location))
            .try_collect::<Vec<_>>()
            .await?;

        for meta in file_metas.into_iter() {
            let location = meta.location;
            assert!(
                location.prefix_matches(&"c1=hello".into())
                    || location.prefix_matches(&"c1=world".into())
            );
        }

        Ok(())
    }

    #[test]
    fn test_split_path_basic() {
        let path = object_store::path::Path::from("data/output");
        assert_eq!(
            split_path(&path, 0, "vortex").to_string(),
            "data/output/part-00000.vortex"
        );
        assert_eq!(
            split_path(&path, 12, "vortex").to_string(),
            "data/output/part-00012.vortex"
        );
    }

    #[test]
    fn test_split_path_preserves_trailing_slash() {
        let path = object_store::path::Path::from("nested/path/");
        assert_eq!(
            split_path(&path, 3, "vx").to_string(),
            "nested/path/part-00003.vx"
        );
    }

    #[test]
    fn test_numbered_path() {
        use super::numbered_path;

        let path = object_store::path::Path::from("table/c1=alpha/abc123.vortex");
        assert_eq!(
            numbered_path(&path, 0, "vortex").to_string(),
            "table/c1=alpha/abc123_00000.vortex"
        );
        assert_eq!(
            numbered_path(&path, 5, "vortex").to_string(),
            "table/c1=alpha/abc123_00005.vortex"
        );
    }

    #[test]
    fn test_numbered_path_no_extension() {
        use super::numbered_path;

        let path = object_store::path::Path::from("table/output");
        assert_eq!(
            numbered_path(&path, 0, "vortex").to_string(),
            "table/output_00000.vortex"
        );
    }

    #[test]
    fn test_output_file_path_single_file_and_collection() {
        use super::output_file_path;

        let single = ListingTableUrl::parse("file:///tmp/output.vortex")
            .expect("single-file listing table URL should parse");
        assert_eq!(
            output_file_path(&single, 0, "vortex", true, "wid", 0, 1).to_string(),
            "tmp/output.vortex"
        );
        assert_eq!(
            output_file_path(&single, 2, "vortex", true, "wid", 0, 1).to_string(),
            "tmp/output_00002.vortex"
        );

        let collection = ListingTableUrl::parse("file:///tmp/table/")
            .expect("collection listing table URL should parse");
        // Single shard: historical naming (no shard segment).
        assert_eq!(
            output_file_path(&collection, 3, "vortex", false, "wid", 0, 1).to_string(),
            "tmp/table/wid_00003.vortex"
        );
        // Multiple shards: a `_p{shard}` segment disambiguates concurrent shards.
        assert_eq!(
            output_file_path(&collection, 3, "vortex", false, "wid", 1, 4).to_string(),
            "tmp/table/wid_p001_00003.vortex"
        );
    }

    #[tokio::test]
    async fn test_finish_file_writer_waits_for_task_completion() -> anyhow::Result<()> {
        let (sender, receiver) = futures::channel::mpsc::channel::<RecordBatch>(1);
        drop(receiver);

        let (gate_tx, gate_rx) = oneshot::channel::<()>();

        let writer = ActiveFileWriter {
            path: object_store::path::Path::from("table/pending.vortex"),
            sender,
            task: tokio::spawn(async move {
                let _ = gate_rx.await;
                Err(exec_datafusion_err!(
                    "synthetic writer failure after completion gate"
                ))
            }),
        };

        let mut finish_fut = Box::pin(finish_file_writer(writer));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut finish_fut)
                .await
                .is_err(),
            "finish_file_writer returned before writer task completed"
        );

        gate_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("failed to release writer completion gate in test"))?;

        let err = match finish_fut.await {
            Ok(_) => {
                return Err(anyhow::anyhow!(
                    "finish_file_writer unexpectedly succeeded after gate release"
                ));
            }
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("synthetic writer failure"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[test]
    fn test_remove_partition_columns() -> anyhow::Result<()> {
        use datafusion::arrow::array::ArrayRef;
        use datafusion::arrow::array::StringArray;

        use super::remove_partition_columns;

        let part_col = Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef;
        let val_col = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("part", DataType::Utf8, false),
                Field::new("val", DataType::Int64, false),
            ])),
            vec![Arc::clone(&part_col), Arc::clone(&val_col)],
        )?;

        let out = remove_partition_columns(&batch, &["part".to_string()])?;
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.schema().field(0).name(), "val");
        assert_eq!(out.num_rows(), 2);
        assert!(Arc::ptr_eq(out.column(0), &val_col));

        Ok(())
    }

    #[test]
    fn test_remove_partition_columns_all_columns_partitioned() -> anyhow::Result<()> {
        use datafusion::arrow::array::StringArray;

        use super::remove_partition_columns;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("part", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["x", "y", "z"]))],
        )?;

        let out = remove_partition_columns(&batch, &["part".to_string()])?;
        assert_eq!(out.num_columns(), 0);
        assert_eq!(out.num_rows(), 3);

        Ok(())
    }

    /// Generate `count` pseudo-random i64 values using a simple LCG.
    /// These values resist compression (unlike sequential or modular data),
    /// giving more realistic compressed file sizes.
    fn pseudo_random_i64s(count: usize, seed: i64) -> Vec<i64> {
        let mut v = seed;
        (0..count)
            .map(|_| {
                v = v
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                v
            })
            .collect()
    }

    /// Tests file splitting through the full DataFusion pipeline.
    ///
    /// Writes ~62MB of pseudo-random Int64 data (near 1:1 compression ratio)
    /// via COPY TO with a 16MB target file size. Verifies that exactly 4 files
    /// are produced and each file's compressed size is approximately 16MB.
    ///
    /// This exercises the complete COPY TO write path through DataFusion and
    /// VortexSink, unlike a direct `write_stream_to_files` call.
    #[tokio::test]
    async fn test_file_splitting_62mb_into_4_files() -> anyhow::Result<()> {
        use datafusion::datasource::MemTable;
        use datafusion_datasource::file_format::format_as_file_type;

        let ctx = TestSessionContext::default();

        let target_mb = 16_usize;
        let opts = VortexTableOptions {
            target_file_size_mb: target_mb,
            ..Default::default()
        };
        let factory = VortexFormatFactory::new().with_options(opts);

        let batch_rows = 8192_usize;
        let total_elements = 62 * 1024 * 1024 / 8; // ~8,126,464 i64 values ≈ 62MB Arrow memory
        let num_batches = total_elements / batch_rows;
        let expected_total_rows = (num_batches * batch_rows) as i64;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut batches = Vec::new();
        for i in 0..num_batches {
            let values = pseudo_random_i64s(batch_rows, (i * batch_rows) as i64);
            batches.push(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?);
        }

        let table = MemTable::try_new(schema.clone(), vec![batches])?;
        ctx.session.register_table("source", Arc::new(table))?;

        let source = ctx.session.table("source").await?;
        let logical_plan = LogicalPlanBuilder::copy_to(
            source.logical_plan().clone(),
            "/table/".to_string(),
            format_as_file_type(Arc::new(factory)),
            Default::default(),
            vec![],
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let file_metas = ctx
            .store
            .list(Some(&"/table".into()))
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            file_metas.len(),
            4,
            "Expected 4 files for ~62MB data with {target_mb}MB target, got {} (sizes: {:?})",
            file_metas.len(),
            file_metas.iter().map(|m| m.size).collect::<Vec<_>>()
        );

        let target_bytes = u64::try_from(target_mb * 1024 * 1024)?;
        for meta in &file_metas {
            assert!(
                meta.size > target_bytes / 2,
                "File {} is {}B, expected at least {}B (target/2)",
                meta.location,
                meta.size,
                target_bytes / 2
            );
        }

        // Verify total row count.
        let result = ctx
            .session
            .sql("SELECT COUNT(*) as cnt FROM '/table/'")
            .await?
            .collect()
            .await?;

        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);

        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    /// Tests file splitting with compressible data through the full pipeline.
    ///
    /// Uses low-entropy Int64 values (repeating 0..255) which compress ~8:1 in
    /// Vortex. With the current code that compares Arrow memory size against
    /// `target_file_size`, files are split far too early, producing many tiny
    /// compressed files instead of files that are close to the target.
    ///
    /// For ~32MB of Arrow data (~4MB compressed at 8:1) with a 1MB target:
    ///   - **Correct**: 4 files of ~1MB compressed each
    ///   - **Bug**: 32 files of ~0.125MB compressed each
    #[tokio::test]
    async fn test_file_splitting_compressible_data() -> anyhow::Result<()> {
        use datafusion::datasource::MemTable;
        use datafusion_datasource::file_format::format_as_file_type;

        let ctx = TestSessionContext::default();

        let target_mb = 1_usize;
        let opts = VortexTableOptions {
            target_file_size_mb: target_mb,
            ..Default::default()
        };
        let factory = VortexFormatFactory::new().with_options(opts);

        // Generate low-entropy Int64 values: repeating 0..255.
        // Arrow memory: 4M × 8 bytes = 32MB.
        // Vortex compressed: each value only needs ~1 byte → ~4MB total.
        let total_elements = 4_000_000_usize;
        let batch_rows = 8192_usize;
        let num_batches = total_elements / batch_rows;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut batches = Vec::new();
        for i in 0..num_batches {
            let values: Vec<i64> = (0..batch_rows)
                .map(|j| ((i * batch_rows + j) % 256) as i64)
                .collect();
            batches.push(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?);
        }

        let table = MemTable::try_new(schema.clone(), vec![batches])?;
        ctx.session.register_table("source", Arc::new(table))?;

        let source = ctx.session.table("source").await?;
        let logical_plan = LogicalPlanBuilder::copy_to(
            source.logical_plan().clone(),
            "/table/".to_string(),
            format_as_file_type(Arc::new(factory)),
            Default::default(),
            vec![],
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let file_metas = ctx
            .store
            .list(Some(&"/table".into()))
            .try_collect::<Vec<_>>()
            .await?;

        // With compressible data, there should be few files (not > 10).
        // The buggy code produces many tiny files because it splits on Arrow
        // memory (32MB / 1MB = 32 files) instead of compressed size (~4MB / 1MB = 4 files).
        let total_compressed: u64 = file_metas.iter().map(|m| m.size).sum();
        let target_bytes = u64::try_from(target_mb * 1024 * 1024)?;

        // We should have at most ~(total_compressed / target) + 1 files, not
        // ~(arrow_memory / target) files.
        let max_expected = usize::try_from(total_compressed / target_bytes + 2)?;
        assert!(
            file_metas.len() <= max_expected,
            "Too many files: got {} but total compressed is {}B with {}B target \
             (expected at most {max_expected}). Files are being split on Arrow memory \
             instead of compressed size. Sizes: {:?}",
            file_metas.len(),
            total_compressed,
            target_bytes,
            file_metas.iter().map(|m| m.size).collect::<Vec<_>>()
        );

        // Every file except the first should be reasonably sized. The first
        // file may be smaller because the compression ratio is unknown until
        // the first write completes.
        for meta in file_metas.iter().skip(1) {
            assert!(
                meta.size > target_bytes / 4,
                "File {} is {}B, far below target {}B — splitting on Arrow memory, not compressed size",
                meta.location,
                meta.size,
                target_bytes
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_write_large_batch_target_file_size_disabled() -> anyhow::Result<()> {
        use datafusion::datasource::MemTable;
        use datafusion_datasource::file_format::format_as_file_type;

        let ctx = TestSessionContext::default();

        let opts = VortexTableOptions {
            // Disable sink-side rolling/splitting.
            target_file_size_mb: 0,
            ..Default::default()
        };
        let factory = VortexFormatFactory::new().with_options(opts);

        let rows_per_partition = 300_000_usize;
        let num_partitions = 8_usize;
        let expected_total_rows = (rows_per_partition * num_partitions) as i64;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut partitions: Vec<Vec<RecordBatch>> = Vec::new();
        for p in 0..num_partitions {
            let values = pseudo_random_i64s(rows_per_partition, (p * rows_per_partition) as i64);
            partitions.push(vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?]);
        }

        let table = MemTable::try_new(schema, partitions)?;
        ctx.session.register_table("source", Arc::new(table))?;

        let source = ctx.session.table("source").await?;
        let logical_plan = LogicalPlanBuilder::copy_to(
            source.logical_plan().clone(),
            "/table/".to_string(),
            format_as_file_type(Arc::new(factory)),
            Default::default(),
            vec![],
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let file_metas = ctx
            .store
            .list(Some(&"/table".into()))
            .try_collect::<Vec<_>>()
            .await?;

        let unique_write_ids: vortex_utils::aliases::hash_set::HashSet<_> = file_metas
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|name| name.split_once('_'))
                    .map(|(prefix, _)| prefix.to_string())
            })
            .collect();

        assert_eq!(
            unique_write_ids.len(),
            1,
            "Expected one write_id with target size disabled; got {:?} from files: {:?}",
            unique_write_ids,
            file_metas
                .iter()
                .map(|m| format!("{}: {}B", m.location, m.size))
                .collect::<Vec<_>>()
        );

        assert_eq!(
            file_metas.len(),
            1,
            "Expected exactly one output file when target size is disabled, got {}",
            file_metas.len()
        );

        let result = ctx
            .session
            .sql("SELECT COUNT(*) as cnt FROM '/table/'")
            .await?
            .collect()
            .await?;

        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);

        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    #[tokio::test]
    async fn test_target_file_size_uses_single_sink_input_partition() -> anyhow::Result<()> {
        use datafusion::datasource::MemTable;
        use datafusion_datasource::file_format::format_as_file_type;

        let ctx = TestSessionContext::default();

        let opts = VortexTableOptions {
            // Enable sink-side sizing, but make the threshold large enough
            // that all input data should fit in a single file.
            target_file_size_mb: 512,
            ..Default::default()
        };
        let factory = VortexFormatFactory::new().with_options(opts);

        let rows_per_partition = 300_000_usize;
        let num_partitions = 8_usize;
        let expected_total_rows = (rows_per_partition * num_partitions) as i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        // Build a MemTable with multiple physical input partitions to mimic
        // DataFusion's parallel writer inputs.
        let mut partitions: Vec<Vec<RecordBatch>> = Vec::new();
        for p in 0..num_partitions {
            let values = pseudo_random_i64s(rows_per_partition, (p * rows_per_partition) as i64);
            partitions.push(vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?]);
        }

        let table = MemTable::try_new(schema, partitions)?;
        ctx.session.register_table("source", Arc::new(table))?;

        let source = ctx.session.table("source").await?;
        let logical_plan = LogicalPlanBuilder::copy_to(
            source.logical_plan().clone(),
            "/table/".to_string(),
            format_as_file_type(Arc::new(factory)),
            Default::default(),
            vec![],
        )?
        .build()?;

        ctx.session
            .execute_logical_plan(logical_plan)
            .await?
            .collect()
            .await?;

        let file_metas = ctx
            .store
            .list(Some(&"/table".into()))
            .try_collect::<Vec<_>>()
            .await?;

        let unique_write_ids: vortex_utils::aliases::hash_set::HashSet<_> = file_metas
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|name| name.split_once('_'))
                    .map(|(prefix, _)| prefix.to_string())
            })
            .collect();

        assert_eq!(
            unique_write_ids.len(),
            1,
            "Expected one write_id (single sink stream), got {:?} from files: {:?}",
            unique_write_ids,
            file_metas
                .iter()
                .map(|m| format!("{}: {}B", m.location, m.size))
                .collect::<Vec<_>>()
        );

        assert!(
            file_metas.len() < num_partitions,
            "Expected fewer output files than input partitions after coalescing; got {} files for {num_partitions} input partitions",
            file_metas.len()
        );

        let result = ctx
            .session
            .sql("SELECT COUNT(*) AS cnt FROM '/table/'")
            .await?
            .collect()
            .await?;

        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);
        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_sql_target_size_multi_partition_source_single_write_id()
    -> anyhow::Result<()> {
        use datafusion::datasource::MemTable;

        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (a BIGINT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/' \
                OPTIONS(target_file_size_mb '64');",
            )
            .await?;

        let rows_per_partition = 300_000_usize;
        let num_partitions = 8_usize;
        let expected_total_rows = (rows_per_partition * num_partitions) as i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut partitions: Vec<Vec<RecordBatch>> = Vec::new();
        for p in 0..num_partitions {
            let values = pseudo_random_i64s(rows_per_partition, (p * rows_per_partition) as i64);
            partitions.push(vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values))],
            )?]);
        }

        let source = MemTable::try_new(schema, partitions)?;
        ctx.session.register_table("source", Arc::new(source))?;

        ctx.session
            .sql("INSERT INTO my_tbl SELECT a FROM source")
            .await?
            .collect()
            .await?;

        let all_files = ctx.store.list(None).try_collect::<Vec<_>>().await?;

        let unique_write_ids: vortex_utils::aliases::hash_set::HashSet<_> = all_files
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|name| name.split_once('_'))
                    .map(|(prefix, _)| prefix.to_string())
            })
            .collect();

        assert_eq!(
            unique_write_ids.len(),
            1,
            "Expected one write_id, got {:?} from files: {:?}",
            unique_write_ids,
            all_files
                .iter()
                .map(|m| format!("{}: {}B", m.location, m.size))
                .collect::<Vec<_>>()
        );
        assert!(
            all_files.len() < num_partitions,
            "Expected fewer files than input partitions; got {} files for {num_partitions} input partitions",
            all_files.len()
        );

        let result = ctx
            .session
            .sql("SELECT COUNT(*) AS cnt FROM my_tbl")
            .await?
            .collect()
            .await?;
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);
        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_sql_streaming_source_single_write_id() -> anyhow::Result<()> {
        use arrow_schema::SchemaRef;
        use datafusion::catalog::streaming::StreamingTable;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::streaming::PartitionStream;
        use futures::stream;

        #[derive(Debug)]
        struct StaticPartitionStream {
            schema: SchemaRef,
            batch: RecordBatch,
        }

        impl PartitionStream for StaticPartitionStream {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }

            fn execute(
                &self,
                _ctx: Arc<datafusion::execution::TaskContext>,
            ) -> datafusion::physical_plan::SendableRecordBatchStream {
                let schema = Arc::clone(&self.schema);
                let batch = self.batch.clone();
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(vec![Ok(batch)]),
                ))
            }
        }

        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (a BIGINT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/' \
                OPTIONS(target_file_size_mb '64');",
            )
            .await?;

        let rows_per_partition = 300_000_usize;
        let num_partitions = 8_usize;
        let expected_total_rows = (rows_per_partition * num_partitions) as i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut partitions: Vec<Arc<dyn PartitionStream>> = Vec::new();
        for p in 0..num_partitions {
            let values = pseudo_random_i64s(rows_per_partition, (p * rows_per_partition) as i64);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])?;

            partitions.push(Arc::new(StaticPartitionStream {
                schema: schema.clone(),
                batch,
            }));
        }

        let source = StreamingTable::try_new(schema, partitions)?;
        ctx.session
            .register_table("source_stream", Arc::new(source))?;

        ctx.session
            .sql("INSERT INTO my_tbl SELECT a FROM source_stream")
            .await?
            .collect()
            .await?;

        let all_files = ctx.store.list(None).try_collect::<Vec<_>>().await?;

        let unique_write_ids: vortex_utils::aliases::hash_set::HashSet<_> = all_files
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|name| name.split_once('_'))
                    .map(|(prefix, _)| prefix.to_string())
            })
            .collect();

        assert_eq!(
            unique_write_ids.len(),
            1,
            "Expected one write_id for streaming source insert, got {:?} from files: {:?}",
            unique_write_ids,
            all_files
                .iter()
                .map(|m| format!("{}: {}B", m.location, m.size))
                .collect::<Vec<_>>()
        );

        let result = ctx
            .session
            .sql("SELECT COUNT(*) AS cnt FROM my_tbl")
            .await?
            .collect()
            .await?;
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);
        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    #[tokio::test]
    async fn test_listing_table_direct_insert_into_streaming_exec_single_write_id()
    -> anyhow::Result<()> {
        use arrow_schema::SchemaRef;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion::physical_plan::collect;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::streaming::PartitionStream;
        use datafusion::physical_plan::streaming::StreamingTableExec;
        use datafusion_expr::dml::InsertOp;
        use futures::stream;

        #[derive(Debug)]
        struct StaticPartitionStream {
            schema: SchemaRef,
            batch: RecordBatch,
        }

        impl PartitionStream for StaticPartitionStream {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }

            fn execute(
                &self,
                _ctx: Arc<datafusion::execution::TaskContext>,
            ) -> datafusion::physical_plan::SendableRecordBatchStream {
                let schema = Arc::clone(&self.schema);
                let batch = self.batch.clone();
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(vec![Ok(batch)]),
                ))
            }
        }

        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (a BIGINT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/' \
                OPTIONS(target_file_size_mb '64');",
            )
            .await?;

        let table_provider = ctx.session.table_provider("my_tbl").await?;

        let rows_per_partition = 300_000_usize;
        let num_partitions = 8_usize;
        let expected_total_rows = (rows_per_partition * num_partitions) as i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut partitions: Vec<Arc<dyn PartitionStream>> = Vec::new();
        for p in 0..num_partitions {
            let values = pseudo_random_i64s(rows_per_partition, (p * rows_per_partition) as i64);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])?;

            partitions.push(Arc::new(StaticPartitionStream {
                schema: schema.clone(),
                batch,
            }));
        }

        let input = Arc::new(StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            Vec::new(),
            false,
            None,
        )?) as Arc<dyn ExecutionPlan>;

        let plan = table_provider
            .insert_into(&ctx.session.state(), input, InsertOp::Append)
            .await?;
        let _count_batches = collect(plan, ctx.session.task_ctx()).await?;

        let all_files = ctx.store.list(None).try_collect::<Vec<_>>().await?;

        let unique_write_ids: vortex_utils::aliases::hash_set::HashSet<_> = all_files
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|name| name.split_once('_'))
                    .map(|(prefix, _)| prefix.to_string())
            })
            .collect();

        assert_eq!(
            unique_write_ids.len(),
            1,
            "Expected one write_id for direct insert_into streaming exec, got {:?} from files: {:?}",
            unique_write_ids,
            all_files
                .iter()
                .map(|m| format!("{}: {}B", m.location, m.size))
                .collect::<Vec<_>>()
        );

        let result = ctx
            .session
            .sql("SELECT COUNT(*) AS cnt FROM my_tbl")
            .await?
            .collect()
            .await?;
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);
        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    #[tokio::test]
    async fn test_listing_table_direct_insert_into_unbounded_streaming_exec_single_write_id()
    -> anyhow::Result<()> {
        use arrow_schema::SchemaRef;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion::physical_plan::collect;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::streaming::PartitionStream;
        use datafusion::physical_plan::streaming::StreamingTableExec;
        use datafusion_expr::dml::InsertOp;
        use futures::stream;

        #[derive(Debug)]
        struct StaticPartitionStream {
            schema: SchemaRef,
            batch: RecordBatch,
        }

        impl PartitionStream for StaticPartitionStream {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }

            fn execute(
                &self,
                _ctx: Arc<datafusion::execution::TaskContext>,
            ) -> datafusion::physical_plan::SendableRecordBatchStream {
                let schema = Arc::clone(&self.schema);
                let batch = self.batch.clone();
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(vec![Ok(batch)]),
                ))
            }
        }

        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (a BIGINT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/' \
                OPTIONS(target_file_size_mb '64');",
            )
            .await?;

        let table_provider = ctx.session.table_provider("my_tbl").await?;

        let rows_per_partition = 100_000_usize;
        let num_partitions = 8_usize;
        let expected_total_rows = (rows_per_partition * num_partitions) as i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut partitions: Vec<Arc<dyn PartitionStream>> = Vec::new();
        for p in 0..num_partitions {
            let values = pseudo_random_i64s(rows_per_partition, (p * rows_per_partition) as i64);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])?;

            partitions.push(Arc::new(StaticPartitionStream {
                schema: schema.clone(),
                batch,
            }));
        }

        let input = Arc::new(StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            Vec::new(),
            true,
            None,
        )?) as Arc<dyn ExecutionPlan>;

        let plan = table_provider
            .insert_into(&ctx.session.state(), input, InsertOp::Append)
            .await?;
        let _count_batches = collect(plan, ctx.session.task_ctx()).await?;

        let all_files = ctx.store.list(None).try_collect::<Vec<_>>().await?;

        let unique_write_ids: vortex_utils::aliases::hash_set::HashSet<_> = all_files
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|name| name.split_once('_'))
                    .map(|(prefix, _)| prefix.to_string())
            })
            .collect();

        assert_eq!(
            unique_write_ids.len(),
            1,
            "Expected one write_id for unbounded streaming insert, got {:?} from files: {:?}",
            unique_write_ids,
            all_files
                .iter()
                .map(|m| format!("{}: {}B", m.location, m.size))
                .collect::<Vec<_>>()
        );

        let result = ctx
            .session
            .sql("SELECT COUNT(*) AS cnt FROM my_tbl")
            .await?
            .collect()
            .await?;
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column should be an Int64Array")
            .value(0);
        assert_eq!(count, expected_total_rows, "Total row count mismatch");

        Ok(())
    }

    // ---- Intra-write sharding (parallel encode) ---------------------------
    //
    // Drive `write_record_batch_stream_to_files` directly with an explicit
    // `ShardSpec` — the same fan-out the Cayenne accelerator selects via
    // `VortexFormat::with_write_shard`. Covers the file-count / naming contract,
    // round-trip correctness, PK-hash clustering, per-shard rolling, and
    // cleanup-on-error.

    const SHARD_WRITE_ID: &str = "testwriteid";

    fn batches_to_stream(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> SendableRecordBatchStream {
        let stream = futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<RecordBatch, datafusion_common::DataFusionError>),
        );
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    async fn run_sharded_write(
        store: Arc<dyn object_store::ObjectStore>,
        schema: SchemaRef,
        data: SendableRecordBatchStream,
        target_file_size: Option<u64>,
        shard_spec: ShardSpec,
    ) -> datafusion_common::Result<Vec<(Path, WriteSummary)>> {
        let dtype = DType::from_arrow(Arc::clone(&schema));
        let base = ListingTableUrl::parse("file:///table/")
            .expect("file:///table/ should parse as a listing url");
        write_record_batch_stream_to_files(
            VortexSession::default(),
            store,
            dtype,
            data,
            &WriteOutputOptions {
                base_output_path: &base,
                target_file_size,
                extension: "vortex",
                write_id: SHARD_WRITE_ID,
                partition_column_names: &[],
                keep_partition_by_columns: false,
                shard_spec: &shard_spec,
            },
        )
        .await
    }

    /// An [`object_store::ObjectStore`] that delegates to `inner` but fails
    /// every write whose path contains `fail_marker`. Used to make a single
    /// shard's writer error mid-write so we can assert the demux surfaces that
    /// writer's real error rather than the downstream "receiver is gone" send
    /// failure.
    #[derive(Debug)]
    struct FailWritesContaining {
        inner: Arc<dyn object_store::ObjectStore>,
        fail_marker: String,
    }

    impl std::fmt::Display for FailWritesContaining {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailWritesContaining({})", self.fail_marker)
        }
    }

    impl FailWritesContaining {
        fn injected(&self, location: &Path) -> Option<object_store::Error> {
            location
                .as_ref()
                .contains(self.fail_marker.as_str())
                .then(|| object_store::Error::Generic {
                    store: "FailWritesContaining",
                    source: "injected shard write failure".into(),
                })
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailWritesContaining {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            match self.injected(location) {
                Some(e) => Err(e),
                None => self.inner.put_opts(location, payload, opts).await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            match self.injected(location) {
                Some(e) => Err(e),
                None => self.inner.put_multipart_opts(location, opts).await,
            }
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// The `_p{shard:03}` segment of a sharded output path, if present.
    fn shard_segment(path: &Path) -> Option<String> {
        let marker = format!("{SHARD_WRITE_ID}_p");
        path.as_ref()
            .split(marker.as_str())
            .nth(1)
            .and_then(|rest| rest.get(..3).map(str::to_string))
    }

    fn one_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn one_col_batch(schema: &SchemaRef, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))])
            .expect("single-column i64 batch")
    }

    #[tokio::test]
    async fn test_round_robin_sharding_writes_one_file_per_shard() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = one_col_schema();
        // 8 batches, round-robin across 4 shards → 2 batches each → 4 files
        // (rolling disabled).
        let batches: Vec<RecordBatch> = (0i64..8)
            .map(|i| one_col_batch(&schema, vec![i; 16]))
            .collect();

        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), batches),
            None,
            ShardSpec::RoundRobin(4),
        )
        .await?;

        assert_eq!(results.len(), 4, "expected one file per shard");

        // One write_id, four distinct shard segments.
        let segments: HashSet<String> = results
            .iter()
            .filter_map(|(p, _)| shard_segment(p))
            .collect();
        assert_eq!(
            segments.len(),
            4,
            "expected 4 distinct shard segments, got files {:?}",
            results
                .iter()
                .map(|(p, _)| p.to_string())
                .collect::<Vec<_>>()
        );

        let total_rows: u64 = results.iter().map(|(_, s)| s.row_count()).sum();
        assert_eq!(total_rows, 8 * 16);

        Ok(())
    }

    #[tokio::test]
    async fn test_round_robin_sharding_round_trips_all_rows() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = one_col_schema();
        // Values 0..50 across 5 batches; round-robin into 3 shards.
        let batches: Vec<RecordBatch> = (0i64..5)
            .map(|b| one_col_batch(&schema, (b * 10..b * 10 + 10).collect()))
            .collect();

        run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), batches),
            None,
            ShardSpec::RoundRobin(3),
        )
        .await?;

        let got = ctx
            .session
            .sql("SELECT a FROM '/table/' ORDER BY a")
            .await?
            .collect()
            .await?;
        let mut got_vals = Vec::new();
        for batch in &got {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("a column should be Int64Array");
            for i in 0..col.len() {
                got_vals.push(col.value(i));
            }
        }
        assert_eq!(got_vals, (0..50).collect::<Vec<i64>>());

        Ok(())
    }

    #[tokio::test]
    async fn test_hash_sharding_clusters_keys_into_disjoint_files() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        // 100 distinct keys spread across 4 batches.
        let batches: Vec<RecordBatch> = (0i64..4)
            .map(|b| {
                let keys: Vec<i64> = (b * 25..b * 25 + 25).collect();
                let vals: Vec<i64> = keys.iter().map(|k| k * 2).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(Int64Array::from(vals)),
                    ],
                )
                .expect("key/val batch")
            })
            .collect();

        let exprs: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("key", 0))];
        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), batches),
            None,
            ShardSpec::Hash {
                exprs,
                partitions: 4,
            },
        )
        .await?;

        // Every key must land in exactly one file (deterministic hash routing),
        // and all 100 keys must be present across the file set.
        let mut key_to_file: HashMap<i64, String> = HashMap::new();
        for (path, _) in &results {
            let url = format!("/{path}");
            let rows = ctx
                .session
                .sql(&format!("SELECT DISTINCT key FROM '{url}'"))
                .await?
                .collect()
                .await?;
            for batch in &rows {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("key column should be Int64Array");
                for i in 0..col.len() {
                    let key = col.value(i);
                    if let Some(prev) = key_to_file.insert(key, url.clone()) {
                        assert_eq!(
                            prev, url,
                            "key {key} appeared in two shard files: {prev} and {url}"
                        );
                    }
                }
            }
        }
        assert_eq!(
            key_to_file.len(),
            100,
            "all keys should be present exactly once"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_per_shard_rolling_rolls_each_shard_independently() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = one_col_schema();
        // ~8 MiB of incompressible data, round-robin across 2 shards, 1 MiB
        // target → each shard must roll into multiple files.
        let batch_rows = 8192usize;
        let num_batches = 128usize;
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|i| {
                one_col_batch(
                    &schema,
                    pseudo_random_i64s(batch_rows, (i * batch_rows) as i64),
                )
            })
            .collect();

        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), batches),
            Some(1024 * 1024),
            ShardSpec::RoundRobin(2),
        )
        .await?;

        let mut files_per_shard: HashMap<String, usize> = HashMap::new();
        for (path, _) in &results {
            if let Some(seg) = shard_segment(path) {
                *files_per_shard.entry(seg).or_default() += 1;
            }
        }
        assert_eq!(
            files_per_shard.len(),
            2,
            "both shards should have written files"
        );
        for (seg, count) in &files_per_shard {
            assert!(
                *count >= 2,
                "shard {seg} rolled only {count} file(s), expected >= 2"
            );
        }

        let total_rows: u64 = results.iter().map(|(_, s)| s.row_count()).sum();
        assert_eq!(total_rows, (num_batches * batch_rows) as u64);

        Ok(())
    }

    #[tokio::test]
    async fn test_single_shard_keeps_unsuffixed_file_names() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = one_col_schema();
        let batches: Vec<RecordBatch> = (0i64..3)
            .map(|i| one_col_batch(&schema, vec![i; 16]))
            .collect();

        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), batches),
            None,
            ShardSpec::Single,
        )
        .await?;

        assert_eq!(results.len(), 1, "Single shard writes exactly one file");
        let name = results[0].0.to_string();
        assert!(
            name.ends_with(&format!("{SHARD_WRITE_ID}_00000.vortex")),
            "unexpected single-shard file name: {name}"
        );
        assert!(
            shard_segment(&results[0].0).is_none(),
            "Single shard must not be _p-suffixed"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sharded_write_cleans_up_all_files_on_stream_error() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = one_col_schema();
        // Four good batches (one per shard) then an injected error.
        let mut items: Vec<datafusion_common::Result<RecordBatch>> = (0i64..4)
            .map(|i| Ok(one_col_batch(&schema, vec![i; 16])))
            .collect();
        items.push(Err(exec_datafusion_err!("injected mid-stream failure")));
        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(items),
        ));

        let result = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            data,
            None,
            ShardSpec::RoundRobin(4),
        )
        .await;
        assert!(result.is_err(), "stream error should fail the write");

        let remaining = ctx
            .store
            .list(Some(&"table".into()))
            .try_collect::<Vec<_>>()
            .await?;
        assert!(
            remaining.is_empty(),
            "failed write must clean up every shard file, found {}",
            remaining.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_shard_writer_surfaces_real_error_not_receiver_gone() -> anyhow::Result<()>
    {
        let schema = one_col_schema();
        // Writes to shard 2's file (`..._p002_...`) fail; every other shard
        // succeeds. The failing shard drops its receiver, which would make the
        // demux's next send report "receiver is gone" — assert we surface the
        // writer's real error instead of masking it behind the send failure.
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(FailWritesContaining {
            inner: Arc::new(object_store::memory::InMemory::new()),
            fail_marker: "_p002_".to_string(),
        });
        // 12 batches round-robin across 4 shards → shard 2 opens its file early
        // and fails, while the demux keeps routing further batches to it.
        let batches: Vec<RecordBatch> = (0i64..12)
            .map(|i| one_col_batch(&schema, vec![i; 16]))
            .collect();

        // `WriteSummary` is not `Debug`, so match rather than `expect_err`.
        let err = match run_sharded_write(
            Arc::clone(&store),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), batches),
            None,
            ShardSpec::RoundRobin(4),
        )
        .await
        {
            Ok(_) => panic!("a failing shard writer must fail the whole write"),
            Err(e) => e,
        };

        let msg = err.to_string();
        assert!(
            msg.contains("injected shard write failure"),
            "expected the shard writer's real error, got: {msg}"
        );
        assert!(
            !msg.contains("receiver is gone"),
            "must not mask the root cause behind the demux send failure, got: {msg}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hash_sharding_handles_null_keys() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        // Nullable key column with interleaved NULLs — NULL keys must hash
        // deterministically, not panic or drop rows.
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let keys: Vec<Option<i64>> = (0i64..40)
            .map(|i| if i % 3 == 0 { None } else { Some(i) })
            .collect();
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(keys))])
                .expect("nullable key batch");

        let exprs: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("key", 0))];
        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), vec![batch]),
            None,
            ShardSpec::Hash {
                exprs,
                partitions: 4,
            },
        )
        .await?;

        let mut total = 0usize;
        for (path, _) in &results {
            let url = format!("/{path}");
            total += ctx
                .session
                .sql(&format!("SELECT key FROM '{url}'"))
                .await?
                .count()
                .await?;
        }
        assert_eq!(total, 40, "every row including NULL keys must round-trip");
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_sharding_composite_key_round_trips() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        // Composite (multi-column) key, as real PKs often are, e.g. (w_id, d_id).
        let schema = Arc::new(Schema::new(vec![
            Field::new("w", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
        ]));
        let ws: Vec<i64> = (0i64..36).map(|i| i / 6).collect();
        let ds: Vec<i64> = (0i64..36).map(|i| i % 6).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ws)),
                Arc::new(Int64Array::from(ds)),
            ],
        )
        .expect("composite key batch");

        let exprs: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new("w", 0)), Arc::new(Column::new("d", 1))];
        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), vec![batch]),
            None,
            ShardSpec::Hash {
                exprs,
                partitions: 4,
            },
        )
        .await?;

        let mut total = 0usize;
        for (path, _) in &results {
            let url = format!("/{path}");
            total += ctx
                .session
                .sql(&format!("SELECT w FROM '{url}'"))
                .await?
                .count()
                .await?;
        }
        assert_eq!(total, 36, "composite-key hash must round-trip every row");
        Ok(())
    }

    #[tokio::test]
    async fn test_sharded_write_with_empty_input_writes_no_files() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();
        let schema = one_col_schema();
        // An empty stream must open no files on any shard (no empty artifacts).
        let results = run_sharded_write(
            ctx.store.clone(),
            Arc::clone(&schema),
            batches_to_stream(Arc::clone(&schema), vec![]),
            None,
            ShardSpec::RoundRobin(4),
        )
        .await?;

        assert!(
            results.is_empty(),
            "empty input must produce no files, got {}",
            results.len()
        );
        let listed = ctx
            .store
            .list(Some(&"table".into()))
            .try_collect::<Vec<_>>()
            .await?;
        assert!(
            listed.is_empty(),
            "no objects should be written for empty input, found {}",
            listed.len()
        );
        Ok(())
    }
}
