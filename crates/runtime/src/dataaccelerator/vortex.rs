/*
Copyright 2025 The Spice.ai OSS Authors

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

use arrow::array::{
    ArrayBuilder, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DurationNanosecondArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray, LargeBinaryArray,
    LargeStringArray, ListBuilder, NullArray, StringArray, StructArray, Time32SecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, IntervalDayTime};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use futures::StreamExt;
use runtime_table_partition::expression::PartitionedBy;
use snafu::prelude::*;
use std::any::Any;
use std::convert::TryFrom;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use vortex::ArrayRef;
use vortex::arrow::FromArrowArray;
use vortex::file::VortexWriteOptions;
use vortex_datafusion::VortexFormat;

use super::{AccelerationSource, DataAccelerator};
use crate::component::dataset::acceleration::Engine;
use crate::dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed};
use crate::make_spice_data_directory;
use crate::parameters::ParameterSpec;
use crate::spice_data_base_path;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration initialization failed: {source}"))]
    AccelerationInitializationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid Vortex acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display("Vortex feature not enabled in build"))]
    FeatureNotEnabled,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Check if a data type is supported by Vortex
fn is_vortex_supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        // Vortex requires Microsecond timestamps but we accept all timestamp types and convert them.
        DataType::Timestamp(_, _)
            // Float16 will be converted to Float32.
            | DataType::Float16
            // Most other basic types are supported as-is.
            | DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Filter schema to only include Vortex-supported fields
/// Converts non-Microsecond timestamps to Microsecond and Float16 to Float32
fn filter_schema_for_vortex(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let filtered_fields: Vec<_> = schema
        .fields()
        .iter()
        .filter_map(|field| {
            if !is_vortex_supported_type(field.data_type()) {
                return None;
            }

            // Convert Float16 to Float32
            if matches!(field.data_type(), DataType::Float16) {
                tracing::warn!(
                    "Converting Float16 field '{}' to Float32 for Vortex compatibility",
                    field.name()
                );
                return Some(Arc::new(arrow::datatypes::Field::new(
                    field.name(),
                    DataType::Float32,
                    field.is_nullable(),
                )));
            }

            // Convert non-Microsecond timestamps to Microsecond
            if let DataType::Timestamp(unit, tz) = field.data_type()
                && !matches!(unit, arrow::datatypes::TimeUnit::Microsecond)
            {
                tracing::warn!(
                    "Converting timestamp field '{}' from {:?} to Microsecond precision for Vortex compatibility",
                    field.name(),
                    unit
                );
                return Some(Arc::new(arrow::datatypes::Field::new(
                    field.name(),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz.clone()),
                    field.is_nullable(),
                )));
            }

            Some(Arc::clone(field))
        })
        .collect();

    arrow::datatypes::Schema::new(filtered_fields)
}

/// Custom data sink for streaming Vortex writes with optimized async I/O
///
/// This implementation provides significant performance improvements over the default
/// `ListingTable` insert mechanism:
///
/// 1. **Async I/O**: Uses `tokio::fs::OpenOptions` for non-blocking file operations
///    instead of synchronous I/O that can block the runtime.
///
/// 2. **Context-Aware Parallelism**: Uses `DataFusion`'s `target_partitions` configuration
///    to determine optimal parallel write concurrency, respecting user settings and system
///    capabilities (capped at 4 to avoid "too many open files" errors).
///
/// 3. **Buffering Strategy**: Accumulates batches up to a configurable target file size
///    (default 512MB) before writing to disk, reducing the total number of files and
///    file handle pressure.
///
/// 4. **Streaming Processing**: Processes record batches as they arrive from the stream
///    without buffering all data in memory, reducing memory pressure during large imports.
///
/// 5. **Backpressure Management**: Limits concurrent writes based on `DataFusion` config
///    to avoid overwhelming the I/O subsystem while maintaining high throughput.
#[derive(Debug, Clone)]
struct VortexDataSink {
    dir_path: String,
    schema: SchemaRef,
    target_file_size_bytes: usize,
}

impl VortexDataSink {
    fn new(dir_path: String, schema: SchemaRef, target_file_size_bytes: usize) -> Self {
        Self {
            dir_path,
            schema,
            target_file_size_bytes,
        }
    }
}

impl DisplayAs for VortexDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, "VortexDataSink(dir={})", self.dir_path)
    }
}

#[async_trait]
impl DataSink for VortexDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[allow(clippy::too_many_lines)]
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let mut total_rows = 0u64;
        let mut file_counter = 0usize;

        // Get configuration from DataFusion context
        let session_config = context.session_config();

        // Use target_partitions to determine parallel write concurrency
        // This respects the user's configured parallelism settings
        let target_partitions = session_config.target_partitions();
        let max_parallel_writes = target_partitions.clamp(1, 4);

        // Get batch size from config if available, otherwise use default
        let batch_size = session_config.batch_size();

        tracing::trace!(
            "Vortex: using {} parallel writes, batch_size={}, target_partitions={}, target_file_size={}MB",
            max_parallel_writes,
            batch_size,
            target_partitions,
            self.target_file_size_bytes / (1024 * 1024)
        );

        // Buffer to accumulate batches before writing
        let target_file_size_bytes = self.target_file_size_bytes;
        let mut buffered_batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
        let mut buffered_size_bytes: usize = 0;

        // Helper function to write buffered batches to a file
        let write_buffered_batches = |batches: Vec<arrow::record_batch::RecordBatch>,
                                      dir_path: String,
                                      counter: usize|
         -> tokio::task::JoinHandle<
            datafusion::error::Result<u64>,
        > {
            tokio::spawn(async move {
                if batches.is_empty() {
                    return Ok(0);
                }

                let total_batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

                // Generate unique filename
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?
                    .as_millis();
                let filename = format!("data_{timestamp}_{counter}_rows_{total_batch_rows}.vortex");

                let file_path = PathBuf::from(&dir_path).join(&filename);

                // Open file for writing with async I/O
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&file_path)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                // Concatenate all batches into a single batch if schema matches
                let combined_batch = if batches.len() == 1 {
                    batches.into_iter().next().ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "Missing record batch for Vortex write".to_string(),
                        )
                    })?
                } else {
                    arrow::compute::concat_batches(&batches[0].schema(), &batches).map_err(|e| {
                        datafusion::error::DataFusionError::External(Box::new(
                            std::io::Error::other(format!("Failed to concatenate batches: {e}")),
                        ))
                    })?
                };

                // Convert Arrow RecordBatch to Vortex Array
                let vortex_array = ArrayRef::from_arrow(&combined_batch, false);

                // Write using async I/O with VortexWriteOptions
                VortexWriteOptions::default()
                    .write(&mut file, vortex_array.to_array_stream())
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::External(Box::new(
                            std::io::Error::other(format!("Failed to write Vortex file: {e}")),
                        ))
                    })?;

                tracing::debug!(
                    "Vortex: wrote file {} with {} rows to {}",
                    filename,
                    total_batch_rows,
                    dir_path
                );

                Ok(total_batch_rows)
            })
        };

        let mut write_tasks = Vec::new();

        // Process batches and buffer them until reaching target size
        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            if num_rows == 0 {
                continue;
            }

            // Calculate approximate size of this batch
            let batch_size = batch.get_array_memory_size();
            buffered_batches.push(batch);
            buffered_size_bytes += batch_size;
            total_rows += num_rows as u64;

            // Write when buffer reaches target size
            if buffered_size_bytes >= target_file_size_bytes {
                let batches_to_write = std::mem::take(&mut buffered_batches);
                buffered_size_bytes = 0;

                let write_task =
                    write_buffered_batches(batches_to_write, self.dir_path.clone(), file_counter);
                write_tasks.push(write_task);
                file_counter += 1;

                // Wait for oldest task if we've reached parallelism limit
                if write_tasks.len() >= max_parallel_writes
                    && let Some(task) = write_tasks.first_mut()
                {
                    match task.await {
                        Ok(Ok(rows)) => {
                            tracing::trace!("Vortex: completed write of {} rows", rows);
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(e) => {
                            return Err(datafusion::error::DataFusionError::External(Box::new(e)));
                        }
                    }
                    write_tasks.remove(0);
                }
            }

            // Log progress periodically
            if total_rows % 100_000 == 0 {
                tracing::trace!(
                    "Vortex: processed {} rows ({} files written, {} MB buffered)",
                    total_rows,
                    file_counter,
                    buffered_size_bytes / (1024 * 1024)
                );
            }
        }

        // Write any remaining buffered batches
        if !buffered_batches.is_empty() {
            let write_task =
                write_buffered_batches(buffered_batches, self.dir_path.clone(), file_counter);
            write_tasks.push(write_task);
            file_counter += 1;
        }

        // Wait for all remaining write tasks to complete
        for task in write_tasks {
            match task.await {
                Ok(Ok(rows)) => {
                    tracing::trace!("Vortex: completed write of {} rows", rows);
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
            }
        }

        tracing::info!(
            "Vortex: completed writing {} files, {} total rows to {}",
            file_counter,
            total_rows,
            self.dir_path
        );

        Ok(total_rows)
    }
}

/// Wrapper around `ListingTable` that uses custom `VortexDataSink` for efficient streaming writes.
/// This is required for better performance with async I/O and to handle append operations properly.
#[derive(Debug)]
struct VortexTableProvider {
    inner: Arc<ListingTable>,
    dir_path: String,
    target_file_size_bytes: usize,
}

#[async_trait]
impl TableProvider for VortexTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Log warnings for unsupported operations
        match overwrite {
            InsertOp::Append => {}
            InsertOp::Overwrite => {
                tracing::warn!(
                    "Vortex does not support overwrite operations yet, using append instead"
                );
            }
            InsertOp::Replace => {
                tracing::warn!(
                    "Vortex does not support replace operations yet, using append instead"
                );
            }
        }

        // Use custom VortexDataSink for efficient async streaming writes
        let sink = Arc::new(VortexDataSink::new(
            self.dir_path.clone(),
            self.schema(),
            self.target_file_size_bytes,
        ));

        Ok(Arc::new(DataSinkExec::new(
            input, sink, None, // No count schema
        )))
    }
}

pub struct VortexAccelerator {
    _marker: std::marker::PhantomData<()>,
}

impl Default for VortexAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

impl VortexAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the `Vortex` data directory path that would be used for a file-based `Vortex` accelerator from this dataset.
    /// Vortex uses a directory-based approach to support append operations.
    pub fn vortex_data_dir(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = source.acceleration() {
            let acceleration_params = acceleration.params.clone();

            // Get the sanitized dataset name
            let dataset_name = source.name().to_string().replace(['.', '/'], "_");

            // Use file_path if provided as base, otherwise use default: spice_data_base_path() + dataset_name
            let dir_path = if let Some(custom_path) = acceleration_params.get("file_path") {
                custom_path.clone()
            } else {
                format!("{}/{}", spice_data_base_path(), dataset_name)
            };

            // Ensure the path ends with a trailing slash for directory operations
            if dir_path.ends_with('/') {
                Ok(dir_path)
            } else {
                Ok(format!("{dir_path}/"))
            }
        } else {
            Err(Error::AccelerationNotEnabled {
                dataset: Arc::from(source.name().to_string()),
            })
        }
    }

    fn resolve_storage_config(&self, source: &dyn AccelerationSource) -> Result<(String, usize)> {
        let path = self
            .file_path(source)
            .map_err(|err| Error::AccelerationCreationFailed {
                source: Box::new(err),
            })?;

        let target_file_size_mb = source
            .acceleration()
            .and_then(|accel| accel.params.get("target_file_size_mb"))
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(512);

        let target_size_bytes = target_file_size_mb * 1024 * 1024;

        tracing::trace!(
            "Vortex: configured target file size: {}MB ({}bytes)",
            target_file_size_mb,
            target_size_bytes
        );

        Ok((path, target_size_bytes))
    }

    fn filtered_arrow_schema(cmd: &CreateExternalTable) -> (SchemaRef, usize) {
        let full_schema: arrow::datatypes::Schema = cmd.schema.as_ref().clone().into();
        let filtered_schema = filter_schema_for_vortex(&full_schema);
        let filtered_count = full_schema.fields().len() - filtered_schema.fields().len();

        (Arc::new(filtered_schema), filtered_count)
    }

    fn ensure_directory(dir_path: &str) -> Result<PathBuf> {
        let path_buf = PathBuf::from(dir_path);
        if !path_buf.exists() {
            std::fs::create_dir_all(&path_buf).map_err(|err| {
                Error::AccelerationCreationFailed {
                    source: Box::new(err),
                }
            })?;
        }

        Ok(path_buf)
    }

    async fn recreate_dummy_file(path: &Path, arrow_schema: &SchemaRef) -> Result<()> {
        let dummy_file_path = path.join("init.vortex");

        if dummy_file_path.exists() {
            tokio::fs::remove_file(&dummy_file_path)
                .await
                .map_err(|err| Error::AccelerationCreationFailed {
                    source: Box::new(err),
                })?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dummy_file_path)
            .await
            .map_err(|err| Error::AccelerationCreationFailed {
                source: Box::new(err),
            })?;

        let dummy_batch = Self::create_dummy_batch(arrow_schema)?;

        let vortex_array = ArrayRef::from_arrow(&dummy_batch, false);

        VortexWriteOptions::default()
            .write(&mut file, vortex_array.to_array_stream())
            .await
            .map_err(|err| Error::AccelerationCreationFailed {
                source: Box::new(std::io::Error::other(format!(
                    "Failed to write Vortex file: {err}"
                ))),
            })?;

        Ok(())
    }

    fn create_dummy_batch(arrow_schema: &SchemaRef) -> Result<arrow::record_batch::RecordBatch> {
        let columns = Self::dummy_columns(arrow_schema);

        arrow::record_batch::RecordBatch::try_new(Arc::clone(arrow_schema), columns).map_err(
            |err| Error::AccelerationCreationFailed {
                source: Box::new(err),
            },
        )
    }

    fn dummy_columns(arrow_schema: &SchemaRef) -> Vec<Arc<dyn arrow::array::Array>> {
        arrow_schema
            .fields()
            .iter()
            .map(|field| Self::dummy_array_for_type(field.data_type()))
            .collect()
    }

    fn dummy_array_for_type(data_type: &DataType) -> Arc<dyn arrow::array::Array> {
        match data_type {
            DataType::Boolean => Arc::new(BooleanArray::from(vec![false])),
            DataType::Int8 => Arc::new(Int8Array::from(vec![0i8])),
            DataType::Int16 => Arc::new(Int16Array::from(vec![0i16])),
            DataType::Int32 => Arc::new(Int32Array::from(vec![0i32])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![0i64])),
            DataType::UInt8 => Arc::new(UInt8Array::from(vec![0u8])),
            DataType::UInt16 => Arc::new(UInt16Array::from(vec![0u16])),
            DataType::UInt32 => Arc::new(UInt32Array::from(vec![0u32])),
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![0u64])),
            DataType::Float32 => Arc::new(Float32Array::from(vec![0.0f32])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![0.0f64])),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz) => {
                Arc::new(TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(tz.clone()))
            }
            DataType::Date32 => Arc::new(Date32Array::from(vec![0i32])),
            DataType::Date64 => Arc::new(Date64Array::from(vec![0i64])),
            DataType::Time32(_) => Arc::new(Time32SecondArray::from(vec![0i32])),
            DataType::Time64(_) => Arc::new(Time64NanosecondArray::from(vec![0i64])),
            DataType::Duration(_) => Arc::new(DurationNanosecondArray::from(vec![0i64])),
            DataType::Interval(_) => {
                Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(0, 0)]))
            }
            DataType::Binary => Arc::new(BinaryArray::from_vec(vec![b""])),
            DataType::FixedSizeBinary(size) => match usize::try_from(*size) {
                Ok(size_usize) => {
                    match FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![
                        0u8;
                        size_usize
                    ])) {
                        Ok(array) => Arc::new(array),
                        Err(err) => {
                            tracing::warn!(
                                ?err,
                                "Failed to create FixedSizeBinary dummy value. Falling back to null."
                            );
                            Arc::new(NullArray::new(1))
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "Invalid size for FixedSizeBinary dummy value. Falling back to null."
                    );
                    Arc::new(NullArray::new(1))
                }
            },
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from_vec(vec![b""])),
            DataType::Utf8 => Arc::new(StringArray::from(vec![""])),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![""])),
            DataType::Decimal128(precision, scale) => match Decimal128Array::from(vec![0i128])
                .with_precision_and_scale(*precision, *scale)
            {
                Ok(array) => Arc::new(array),
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "Failed to configure Decimal128 dummy value. Falling back to null."
                    );
                    Arc::new(NullArray::new(1))
                }
            },
            DataType::Decimal256(precision, scale) => {
                match Decimal256Array::from(vec![arrow::datatypes::i256::from_i128(0)])
                    .with_precision_and_scale(*precision, *scale)
                {
                    Ok(array) => Arc::new(array),
                    Err(err) => {
                        tracing::warn!(
                            ?err,
                            "Failed to configure Decimal256 dummy value. Falling back to null."
                        );
                        Arc::new(NullArray::new(1))
                    }
                }
            }
            DataType::List(field) => {
                let value_builder = match field.data_type() {
                    DataType::Int32 => Box::new(Int32Array::builder(0)) as Box<dyn ArrayBuilder>,
                    _ => Box::new(Int32Array::builder(0)) as Box<dyn ArrayBuilder>,
                };
                let mut builder = ListBuilder::new(value_builder);
                builder.append(true);
                Arc::new(builder.finish())
            }
            DataType::Struct(fields) => {
                let field_arrays: Vec<Arc<dyn arrow::array::Array>> = fields
                    .iter()
                    .map(|_| Arc::new(Int32Array::from(vec![0i32])) as Arc<dyn arrow::array::Array>)
                    .collect();
                Arc::new(StructArray::new(fields.clone(), field_arrays, None))
            }
            _ => Arc::new(NullArray::new(1)),
        }
    }

    async fn create_listing_table(dir_path: &str) -> Result<ListingTable> {
        let ctx = SessionContext::new();
        let format = Arc::new(VortexFormat::default());

        let dir_url_str = if dir_path.ends_with('/') {
            dir_path.to_string()
        } else {
            format!("{dir_path}/")
        };

        let table_url = ListingTableUrl::parse(&dir_url_str).map_err(|err| {
            Error::AccelerationCreationFailed {
                source: Box::new(err),
            }
        })?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(
                ListingOptions::new(format).with_session_config_options(ctx.state().config()),
            )
            .infer_schema(&ctx.state())
            .await
            .map_err(|err| Error::AccelerationCreationFailed {
                source: Box::new(err),
            })?;

        ListingTable::try_new(config).map_err(|err| Error::AccelerationCreationFailed {
            source: Box::new(err),
        })
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file_path"),
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::runtime("target_file_size_mb")
        .description("Target size in MB for each Vortex file before flushing (default: 512MB)"),
];

#[async_trait]
impl DataAccelerator for VortexAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "vortex"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["vortex"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.vortex_data_dir(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Vortex,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode Vortex is always initialized
        }

        // otherwise, we're initialized if the directory exists
        if let Ok(dir_path) = self.file_path(source) {
            PathBuf::from(dir_path).exists()
        } else {
            false
        }
    }

    /// Initializes a `Vortex` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// Creates the data directory if it doesn't exist
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::warn!(
            "⚠️  Vortex data accelerator is in ALPHA stage and should NOT be used in production. \
             Data format and API may change without notice."
        );

        if !source.is_file_accelerated() {
            return Ok(());
        }

        let dir_path = self.file_path(source)?;

        // Ensure the spice data base directory exists
        make_spice_data_directory()
            .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;

        // Create the vortex data directory if it doesn't exist
        let path_buf = PathBuf::from(&dir_path);
        if !path_buf.exists() {
            std::fs::create_dir_all(&path_buf)
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
        }

        if let Some(acceleration) = source.acceleration() {
            download_snapshot_if_needed(acceleration, source, path_buf).await;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    /// Vortex only supports file mode and creates an empty file with the given schema.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_empty(),
            super::InvalidConfigurationSnafu {
                msg: "Vortex data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        let Some(src) = source else {
            return Err(Box::new(Error::InvalidConfiguration {
                detail: Arc::from("Source required for Vortex accelerator"),
            }));
        };

        let (dir_path, target_file_size_bytes) = self
            .resolve_storage_config(src)
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;

        let (arrow_schema, filtered_count) = Self::filtered_arrow_schema(&cmd);

        if filtered_count > 0 {
            tracing::warn!(
                "Filtered out {} unsupported field(s) for Vortex acceleration. Supported types are limited.",
                filtered_count
            );
        }

        let path_buf = Self::ensure_directory(&dir_path)
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;

        Self::recreate_dummy_file(path_buf.as_path(), &arrow_schema)
            .await
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;

        let listing_table = Self::create_listing_table(&dir_path)
            .await
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;

        // Wrap in VortexTableProvider with custom data sink for efficient streaming writes
        let wrapped_table = VortexTableProvider {
            inner: Arc::new(listing_table),
            dir_path: dir_path.clone(),
            target_file_size_bytes,
        };

        Ok(Arc::new(wrapped_table))
    }

    fn prefix(&self) -> &'static str {
        "vortex"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::{Acceleration, Mode};
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_vortex_file_path_generation() {
        let app = AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "vortex_data_accelerator_test".to_string(),
            "vortex_data_accelerator_test",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Vortex,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = VortexAccelerator::new();
        let data_dir = accelerator.vortex_data_dir(&dataset);

        assert!(data_dir.is_ok());
        let dir_path = data_dir.unwrap();
        assert!(dir_path.contains("vortex_data_accelerator_test"));
        assert!(dir_path.ends_with("/"));
    }

    #[tokio::test]
    async fn test_vortex_memory_mode() {
        let app = AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset =
            DatasetBuilder::try_new("vortex_memory_test".to_string(), "vortex_memory_test")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Vortex,
            mode: Mode::Memory,
            ..Default::default()
        });

        let accelerator = VortexAccelerator::new();

        // Memory mode should always be initialized
        assert!(accelerator.is_initialized(&dataset));

        // Init should be a no-op for memory mode
        assert!(accelerator.init(&dataset).await.is_ok());
    }
}
