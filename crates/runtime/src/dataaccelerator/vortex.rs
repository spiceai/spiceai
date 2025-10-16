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

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::{
    datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable, physical_plan::ExecutionPlan,
};
use runtime_table_partition::expression::PartitionBy;
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, path::PathBuf, sync::Arc};

use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use vortex_datafusion::VortexFormat;

use crate::{
    component::dataset::acceleration::Engine,
    dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed},
    make_spice_data_directory,
    parameters::ParameterSpec,
    spice_data_base_path,
};

use super::{AccelerationSource, DataAccelerator};

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

    /// Returns the `Vortex` data directory path that would be used for a file-based `Vortex` accelerator from this dataset
    pub fn vortex_data_path(&self, source: &dyn AccelerationSource) -> Result<String> {
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

            // Use vortex_data_path if provided, otherwise use default: spice_data_base_path() + dataset_name
            let data_path =
                if let Some(vortex_data_path) = acceleration_params.get("vortex_data_path") {
                    vortex_data_path.clone()
                } else {
                    format!("{}/{}", spice_data_base_path(), dataset_name)
                };

            // Ensure the path ends with a separator for directory operations
            if data_path.ends_with('/') {
                Ok(data_path)
            } else {
                Ok(format!("{}/", data_path))
            }
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("data_path"),
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
        self.vortex_data_path(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Vortex,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode Vortex is always initialized
        }

        // otherwise, we're initialized if the data directory exists
        if let Ok(data_path) = self.file_path(source) {
            PathBuf::from(data_path).exists()
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

        let data_path = self.file_path(source)?;

        // Ensure the spice data base directory exists
        make_spice_data_directory()
            .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;

        // Create the vortex data directory if it doesn't exist
        let path_buf = PathBuf::from(&data_path);
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
    /// Vortex only supports file mode and requires a data directory.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        _partition_by: Option<PartitionBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(feature = "vortex")]
        {
            // Vortex only supports file mode with a data directory
            let data_path = if let Some(src) = source {
                self.file_path(src)?
            } else {
                return Err(Error::InvalidConfiguration {
                    detail: Arc::from("Source required for Vortex accelerator"),
                }
                .into());
            };

            // Ensure the data directory exists
            let path_buf = PathBuf::from(&data_path);
            if !path_buf.exists() {
                std::fs::create_dir_all(&path_buf)
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            }

            let ctx = SessionContext::new();
            let format = Arc::new(VortexFormat::default());
            let table_url = ListingTableUrl::parse(&data_path).map_err(|e| {
                Error::AccelerationCreationFailed {
                    source: Box::new(e),
                }
            })?;

            // Use the schema from the command instead of trying to infer from a potentially non-existent file
            // Convert DFSchema to Arrow Schema
            let arrow_schema: Arc<arrow::datatypes::Schema> =
                Arc::new(cmd.schema.as_ref().clone().into());
            let config = ListingTableConfig::new(table_url)
                .with_listing_options(
                    ListingOptions::new(format).with_session_config_options(ctx.state().config()),
                )
                .with_schema(arrow_schema);

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

        #[cfg(not(feature = "vortex"))]
        {
            Err(Error::FeatureNotEnabled.into())
        }
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
    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::builder::DatasetBuilder;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_vortex_data_path_generation() {
        let app = crate::app::AppBuilder::new("test").build();
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
        let data_path = accelerator.vortex_data_path(&dataset);

        assert!(data_path.is_ok());
        let path = data_path.unwrap();
        assert!(path.contains("vortex_data_accelerator_test"));
        assert!(path.ends_with('/'));
    }
}
