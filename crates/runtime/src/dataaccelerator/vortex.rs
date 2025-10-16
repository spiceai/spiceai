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

use arrow::datatypes::DataType;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::CreateExternalTable;
use runtime_table_partition::expression::PartitionedBy;
use snafu::prelude::*;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
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

    fn create_listing_table(dir_path: &str, schema: Arc<Schema>) -> Result<ListingTable> {
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
            .with_schema(schema);

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

        // Vortex requires a source for file mode with directory-based storage
        let source = source.ok_or_else(|| {
            Box::new(Error::InvalidConfiguration {
                detail: Arc::from("Source required for Vortex accelerator"),
            }) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let (dir_path, _target_file_size_bytes) = self.resolve_storage_config(source).boxed()?;

        let (arrow_schema, filtered_count) = Self::filtered_arrow_schema(&cmd);

        if filtered_count > 0 {
            tracing::warn!(
                "Filtered out {filtered_count} unsupported field(s) for Vortex acceleration. Supported types are limited."
            );
        }

        let _ = Self::ensure_directory(&dir_path).boxed()?;

        let listing_table =
            Self::create_listing_table(&dir_path, Arc::clone(&arrow_schema)).boxed()?;

        Ok(Arc::new(listing_table))
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

        let dir_path = match data_dir {
            Ok(path) => path,
            Err(err) => panic!("Expected Vortex data directory to resolve, but got {err}"),
        };
        assert!(dir_path.contains("vortex_data_accelerator_test"));
        assert!(dir_path.ends_with('/'));
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
