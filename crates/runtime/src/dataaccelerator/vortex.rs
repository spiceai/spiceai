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
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use runtime_table_partition::expression::PartitionBy;
use snafu::prelude::*;
use std::any::Any;
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

/// Wrapper around ListingTable that forces InsertOp::Append for all insert operations.
/// This is required because Vortex doesn't support overwrites yet.
#[derive(Debug)]
struct VortexTableProvider {
    inner: Arc<ListingTable>,
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
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Force all insert operations to use Append, since Vortex doesn't support overwrites yet
        let insert_op = match overwrite {
            InsertOp::Append => InsertOp::Append,
            InsertOp::Overwrite => {
                tracing::warn!(
                    "Vortex does not support overwrite operations yet, using append instead"
                );
                InsertOp::Append
            }
            InsertOp::Replace => {
                tracing::warn!(
                    "Vortex does not support replace operations yet, using append instead"
                );
                InsertOp::Append
            }
        };
        self.inner.insert_into(state, input, insert_op).await
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

    /// Returns the `Vortex` data file path that would be used for a file-based `Vortex` accelerator from this dataset
    pub fn vortex_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
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

            // Use file_path if provided, otherwise use default: spice_data_base_path() + dataset_name.vortex
            let file_path = if let Some(custom_path) = acceleration_params.get("file_path") {
                custom_path.clone()
            } else {
                format!("{}/{}.vortex", spice_data_base_path(), dataset_name)
            };

            Ok(file_path)
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
        self.vortex_file_path(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Vortex,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode Vortex is always initialized
        }

        // otherwise, we're initialized if the file exists
        if let Ok(file_path) = self.file_path(source) {
            PathBuf::from(file_path).exists()
        } else {
            false
        }
    }

    /// Initializes a `Vortex` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// Creates the parent directory if it doesn't exist
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Ok(());
        }

        let file_path = self.file_path(source)?;

        // Ensure the spice data base directory exists
        make_spice_data_directory()
            .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;

        // Create the parent directory if it doesn't exist
        let path_buf = PathBuf::from(&file_path);
        if let Some(parent) = path_buf.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            }
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
        // Vortex only supports file mode
        let file_path = if let Some(src) = source {
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

        let path_buf = PathBuf::from(&file_path);

        // Ensure the parent directory exists
        if let Some(parent) = path_buf.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            }
        }

        // Always delete the existing file if it exists, since we only support append
        // and need to start fresh with a new dummy file
        if path_buf.exists() {
            tokio::fs::remove_file(&path_buf)
                .await
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
        }

        // Create a Vortex file with 1 row of dummy data
        // This is required because Vortex needs at least EOF_SIZE (8) bytes
        {
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&path_buf)
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
        let table_url = ListingTableUrl::parse(path_buf.to_str().ok_or_else(|| {
            Error::InvalidConfiguration {
                detail: Arc::from("Path is not valid UTF-8"),
            }
        })?)
        .map_err(|e| Error::AccelerationCreationFailed {
            source: Box::new(e),
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

        // Wrap in VortexTableProvider to force InsertOp::Append
        let wrapped_table = VortexTableProvider {
            inner: Arc::new(listing_table),
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
        let file_path = accelerator.vortex_file_path(&dataset);

        assert!(file_path.is_ok());
        let path = file_path.unwrap();
        assert!(path.contains("vortex_data_accelerator_test"));
        assert!(path.ends_with(".vortex"));
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
