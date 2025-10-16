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

#![cfg(feature = "vortex")]

use arrow::array::*;
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
use runtime_table_partition::expression::PartitionBy;
use snafu::prelude::*;
use std::any::Any;
use std::fmt;
use std::path::PathBuf;
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
    match data_type {
        // Vortex has issues with certain timestamp precisions
        // Only Microsecond timestamps seem to work reliably
        DataType::Timestamp(unit, _) => matches!(unit, arrow::datatypes::TimeUnit::Microsecond),
        // Float16 is uncommon and not well supported
        DataType::Float16 => false,
        // Most other basic types are supported
        DataType::Null
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
        | DataType::Decimal256(_, _) => true,
        // Conservative approach: only allow explicitly supported types
        _ => false,
    }
}

/// Filter schema to only include Vortex-supported fields
fn filter_schema_for_vortex(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let filtered_fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| is_vortex_supported_type(field.data_type()))
        .cloned()
        .collect();

    arrow::datatypes::Schema::new(filtered_fields)
}

/// Custom data sink for streaming Vortex writes with optimized async I/O
///
/// This implementation provides significant performance improvements over the default
/// ListingTable insert mechanism:
///
/// 1. **Async I/O**: Uses `tokio::fs::OpenOptions` for non-blocking file operations
///    instead of synchronous I/O that can block the runtime.
///
/// 2. **Parallel Writes**: Spawns up to MAX_PARALLEL_WRITES concurrent write tasks,
///    allowing multiple batches to be written simultaneously. This maximizes throughput
///    by utilizing available I/O bandwidth and CPU cores.
///
/// 3. **Streaming Processing**: Processes record batches as they arrive from the stream
///    without buffering all data in memory, reducing memory pressure during large imports.
///
/// 4. **Backpressure Management**: Limits concurrent writes to avoid overwhelming the I/O
///    subsystem while still maintaining high throughput.
#[derive(Debug, Clone)]
struct VortexDataSink {
    dir_path: String,
    schema: SchemaRef,
}

impl VortexDataSink {
    fn new(dir_path: String, schema: SchemaRef) -> Self {
        Self { dir_path, schema }
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

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let mut total_rows = 0u64;
        let mut file_counter = 0usize;

        // Buffer to accumulate batches before writing
        // Target 256MB per file to reduce number of files and avoid "too many open files" error
        const TARGET_FILE_SIZE_BYTES: usize = 256 * 1024 * 1024; // 256MB
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
                    .unwrap()
                    .as_millis();
                let filename = format!(
                    "data_{}_{}_rows_{}.vortex",
                    timestamp, counter, total_batch_rows
                );

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
                    batches.into_iter().next().unwrap()
                } else {
                    arrow::compute::concat_batches(&batches[0].schema(), &batches).map_err(|e| {
                        datafusion::error::DataFusionError::External(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to concatenate batches: {}", e),
                        )))
                    })?
                };

                // Convert Arrow RecordBatch to Vortex Array
                let vortex_array = ArrayRef::from_arrow(&combined_batch, false);

                // Write using async I/O with VortexWriteOptions
                VortexWriteOptions::default()
                    .write(&mut file, vortex_array.to_array_stream())
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::External(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to write Vortex file: {}", e),
                        )))
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
        const MAX_PARALLEL_WRITES: usize = 2; // Reduce parallelism to limit open files

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
            if buffered_size_bytes >= TARGET_FILE_SIZE_BYTES {
                let batches_to_write = std::mem::take(&mut buffered_batches);
                buffered_size_bytes = 0;

                let write_task =
                    write_buffered_batches(batches_to_write, self.dir_path.clone(), file_counter);
                write_tasks.push(write_task);
                file_counter += 1;

                // Wait for oldest task if we've reached parallelism limit
                if write_tasks.len() >= MAX_PARALLEL_WRITES {
                    if let Some(task) = write_tasks.first_mut() {
                        match task.await {
                            Ok(Ok(rows)) => {
                                tracing::trace!("Vortex: completed write of {} rows", rows);
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(datafusion::error::DataFusionError::External(
                                    Box::new(e),
                                ));
                            }
                        }
                        write_tasks.remove(0);
                    }
                }
            }

            // Log progress periodically
            if total_rows % 100_000 == 0 {
                tracing::debug!(
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

/// Wrapper around ListingTable that uses custom VortexDataSink for efficient streaming writes.
/// This is required for better performance with async I/O and to handle append operations properly.
#[derive(Debug)]
struct VortexTableProvider {
    inner: Arc<ListingTable>,
    dir_path: String,
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
        };

        // Use custom VortexDataSink for efficient async streaming writes
        let sink = Arc::new(VortexDataSink::new(self.dir_path.clone(), self.schema()));

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
            let dataset_name = source
                .name()
                .to_string()
                .replace('.', "_")
                .replace('/', "_");

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
                Ok(format!("{}/", dir_path))
            }
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file_path"),
    ParameterSpec::runtime("file_watcher"),
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
        _partition_by: Option<PartitionBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        // Vortex only supports file mode with directory-based storage
        let dir_path = if let Some(src) = source {
            self.file_path(src)?
        } else {
            return Err(Error::InvalidConfiguration {
                detail: Arc::from("Source required for Vortex accelerator"),
            }
            .into());
        };

        // Convert DFSchema to Arrow Schema and filter for Vortex-supported types
        let full_schema: arrow::datatypes::Schema = cmd.schema.as_ref().clone().into();
        let filtered_schema = filter_schema_for_vortex(&full_schema);

        // Log warning if fields were filtered out
        let filtered_count = full_schema.fields().len() - filtered_schema.fields().len();
        if filtered_count > 0 {
            tracing::warn!(
                "Filtered out {} unsupported field(s) for Vortex acceleration. Supported types are limited.",
                filtered_count
            );
        }

        let arrow_schema: SchemaRef = Arc::new(filtered_schema);

        let path_buf = PathBuf::from(&dir_path);

        // Ensure the directory exists
        if !path_buf.exists() {
            std::fs::create_dir_all(&path_buf)
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
        }

        // Create an initial dummy file inside the directory to initialize the table
        // This is required because Vortex needs at least EOF_SIZE (8) bytes and ListingTable needs files to scan
        let dummy_file_path = path_buf.join("init.vortex");

        // Always recreate the dummy file to ensure fresh state
        if dummy_file_path.exists() {
            tokio::fs::remove_file(&dummy_file_path)
                .await
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
        }

        {
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&dummy_file_path)
                .await
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;

            // Create arrays with 1 row of dummy data for each field
            let columns: Vec<Arc<dyn arrow::array::Array>> = arrow_schema
                .fields()
                .iter()
                .map(|field| -> Arc<dyn arrow::array::Array> {
                    match field.data_type() {
                        DataType::Null => Arc::new(NullArray::new(1)),
                        DataType::Boolean => Arc::new(BooleanArray::from(vec![false])),
                        DataType::Int8 => Arc::new(Int8Array::from(vec![0i8])),
                        DataType::Int16 => Arc::new(Int16Array::from(vec![0i16])),
                        DataType::Int32 => Arc::new(Int32Array::from(vec![0i32])),
                        DataType::Int64 => Arc::new(Int64Array::from(vec![0i64])),
                        DataType::UInt8 => Arc::new(UInt8Array::from(vec![0u8])),
                        DataType::UInt16 => Arc::new(UInt16Array::from(vec![0u16])),
                        DataType::UInt32 => Arc::new(UInt32Array::from(vec![0u32])),
                        DataType::UInt64 => Arc::new(UInt64Array::from(vec![0u64])),
                        DataType::Float16 => {
                            // Float16 is uncommon, use null for simplicity
                            Arc::new(NullArray::new(1))
                        }
                        DataType::Float32 => Arc::new(Float32Array::from(vec![0.0f32])),
                        DataType::Float64 => Arc::new(Float64Array::from(vec![0.0f64])),
                        DataType::Timestamp(unit, tz) => {
                            // Vortex only supports Microsecond timestamps reliably
                            match unit {
                                arrow::datatypes::TimeUnit::Microsecond => Arc::new(
                                    TimestampMicrosecondArray::from(vec![0i64])
                                        .with_timezone_opt(tz.clone()),
                                ),
                                // This shouldn't happen since we filter the schema
                                _ => Arc::new(NullArray::new(1)),
                            }
                        }
                        DataType::Date32 => Arc::new(Date32Array::from(vec![0i32])),
                        DataType::Date64 => Arc::new(Date64Array::from(vec![0i64])),
                        DataType::Time32(_) => Arc::new(Time32SecondArray::from(vec![0i32])),
                        DataType::Time64(_) => Arc::new(Time64NanosecondArray::from(vec![0i64])),
                        DataType::Duration(_) => {
                            Arc::new(DurationNanosecondArray::from(vec![0i64]))
                        }
                        DataType::Interval(_) => {
                            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(0, 0)]))
                        }
                        DataType::Binary => Arc::new(BinaryArray::from_vec(vec![b""])),
                        DataType::FixedSizeBinary(size) => {
                            Arc::new(FixedSizeBinaryArray::from(vec![
                                vec![0u8; *size as usize].as_slice(),
                            ]))
                        }
                        DataType::LargeBinary => Arc::new(LargeBinaryArray::from_vec(vec![b""])),
                        DataType::Utf8 => Arc::new(StringArray::from(vec![""])),
                        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![""])),
                        DataType::Decimal128(precision, scale) => Arc::new(
                            Decimal128Array::from(vec![0i128])
                                .with_precision_and_scale(*precision, *scale)
                                .unwrap(),
                        ),
                        DataType::Decimal256(precision, scale) => Arc::new(
                            Decimal256Array::from(vec![arrow::datatypes::i256::from_i128(0)])
                                .with_precision_and_scale(*precision, *scale)
                                .unwrap(),
                        ),
                        DataType::List(field) => {
                            let value_builder = match field.data_type() {
                                DataType::Int32 => {
                                    Box::new(Int32Array::builder(0)) as Box<dyn ArrayBuilder>
                                }
                                _ => Box::new(Int32Array::builder(0)) as Box<dyn ArrayBuilder>,
                            };
                            let mut builder = ListBuilder::new(value_builder);
                            builder.append(true);
                            Arc::new(builder.finish())
                        }
                        DataType::Struct(fields) => {
                            let field_arrays: Vec<Arc<dyn arrow::array::Array>> = fields
                                .iter()
                                .map(|_| {
                                    Arc::new(Int32Array::from(vec![0i32]))
                                        as Arc<dyn arrow::array::Array>
                                })
                                .collect();
                            Arc::new(StructArray::new(fields.clone(), field_arrays, None))
                        }
                        _ => {
                            // For unsupported types, use a null array
                            Arc::new(NullArray::new(1))
                        }
                    }
                })
                .collect();

            // Create the record batch with 1 row
            let dummy_batch = RecordBatch::try_new(arrow_schema.clone(), columns).map_err(|e| {
                Error::AccelerationCreationFailed {
                    source: Box::new(e),
                }
            })?;

            // Convert Arrow RecordBatch to Vortex Array
            let vortex_array = ArrayRef::from_arrow(&dummy_batch, false);

            // Write the dummy batch using VortexWriteOptions
            VortexWriteOptions::default()
                .write(&mut file, vortex_array.to_array_stream())
                .await
                .map_err(|e| Error::AccelerationCreationFailed {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to write Vortex file: {}", e),
                    )),
                })?;
        }

        let ctx = SessionContext::new();
        let format = Arc::new(VortexFormat::default());

        // Use the directory path with trailing slash for ListingTable
        let dir_url_str = if dir_path.ends_with('/') {
            dir_path.clone()
        } else {
            format!("{}/", dir_path)
        };

        let table_url = ListingTableUrl::parse(&dir_url_str).map_err(|e| {
            Error::AccelerationCreationFailed {
                source: Box::new(e),
            }
        })?;

        // Infer schema from the created file
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(
                ListingOptions::new(format).with_session_config_options(ctx.state().config()),
            )
            .infer_schema(&ctx.state())
            .await
            .map_err(|e| Error::AccelerationCreationFailed {
                source: Box::new(e),
            })?;

        let listing_table =
            ListingTable::try_new(config).map_err(|e| Error::AccelerationCreationFailed {
                source: Box::new(e),
            })?;

        // Wrap in VortexTableProvider with custom data sink for efficient streaming writes
        let wrapped_table = VortexTableProvider {
            inner: Arc::new(listing_table),
            dir_path: dir_path.clone(),
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
