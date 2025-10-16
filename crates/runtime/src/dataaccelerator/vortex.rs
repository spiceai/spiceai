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
use datafusion::{
    datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use runtime_table_partition::expression::PartitionBy;
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, path::PathBuf, sync::Arc};

use crate::{
    component::dataset::acceleration::{Engine, Mode},
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

    #[snafu(display(
        "The \"vortex_file\" acceleration parameter has an invalid extension. Expected one of \"{valid_extensions}\" but got \"{extension}\"."
    ))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display("The \"vortex_file\" acceleration parameter value is a directory."))]
    InvalidFileIsDirectory,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid Vortex acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display("Vortex feature not enabled in build"))]
    FeatureNotEnabled,
}

type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Returns the `Vortex` file path that would be used for a file-based `Vortex` accelerator from this dataset
    pub fn vortex_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = source.acceleration() {
            let acceleration_params = acceleration.params.clone();

            // Check for vortex_file parameter first
            if let Some(vortex_file) = acceleration_params.get("vortex_file") {
                return Ok(vortex_file.clone());
            }

            // Otherwise, use default path pattern
            let data_directory = acceleration_params
                .get("vortex_data_dir")
                .map(|s| s.as_str())
                .unwrap_or(&spice_data_base_path());

            let dataset_name = source
                .name()
                .to_string()
                .replace('.', "_")
                .replace('/', "_");

            Ok(format!("{data_directory}/{dataset_name}_vortex.vortex"))
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file"),
    ParameterSpec::component("data_dir"),
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
        self.has_existing_file(source)
    }

    /// Initializes a `Vortex` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Ok(());
        }

        let path = self.file_path(source)?;

        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("vortex_file") {
                make_spice_data_directory()
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            } else if !self.is_valid_file(source) {
                if std::path::Path::new(&path).is_dir() {
                    return Err(Error::InvalidFileIsDirectory.into());
                }

                let extension = std::path::Path::new(&path)
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("");

                return Err(Error::InvalidFileExtension {
                    valid_extensions: self.valid_file_extensions().join(","),
                    extension: extension.to_string(),
                }
                .into());
            }

            download_snapshot_if_needed(acceleration, source, PathBuf::from(path)).await;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        _partition_by: Option<PartitionBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(feature = "vortex")]
        {
            use datafusion::datasource::listing::{
                ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
            };
            use vortex_datafusion::persistent::format::VortexFormat;

            let mode = cmd
                .options
                .get("mode")
                .map(|s| s.as_str())
                .unwrap_or("memory");

            match mode {
                "file" => {
                    // File mode: Use ListingTable with VortexFormat
                    let file_path = if let Some(src) = source {
                        self.file_path(src)?
                    } else {
                        return Err(Error::InvalidConfiguration {
                            detail: Arc::from("File path required for file mode"),
                        }
                        .into());
                    };

                    let ctx = SessionContext::new();
                    let format = Arc::new(VortexFormat::default());
                    let table_url = ListingTableUrl::parse(&file_path).map_err(|e| {
                        Error::AccelerationCreationFailed {
                            source: Box::new(e),
                        }
                    })?;

                    let config = ListingTableConfig::new(table_url)
                        .with_listing_options(
                            ListingOptions::new(format)
                                .with_session_config_options(ctx.state().config()),
                        )
                        .infer_schema(&ctx.state())
                        .await
                        .map_err(|e| Error::AccelerationCreationFailed {
                            source: Box::new(e),
                        })?;

                    let listing_table = ListingTable::try_new(config).map_err(|e| {
                        Error::AccelerationCreationFailed {
                            source: Box::new(e),
                        }
                    })?;

                    Ok(Arc::new(listing_table))
                }
                "memory" => {
                    // Memory mode: Create an empty VortexMemTable
                    // Note: This creates an empty table. Data will need to be inserted.
                    use arrow::datatypes::Schema as ArrowSchema;
                    use vortex::IntoArrayData;
                    use vortex::array::StructArray;
                    use vortex::validity::Validity;
                    use vortex_datafusion::memory::{VortexMemTable, VortexMemTableOptions};

                    // Convert DataFusion schema to Arrow schema
                    let arrow_schema: ArrowSchema = cmd.schema.as_ref().clone().into();

                    // Create an empty Vortex StructArray with the schema
                    let field_names: Vec<_> = arrow_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone().into())
                        .collect();

                    let empty_arrays: Vec<_> = arrow_schema
                        .fields()
                        .iter()
                        .map(|_| {
                            vortex::array::PrimitiveArray::from(Vec::<i32>::new()).into_array()
                        })
                        .collect();

                    let struct_array = StructArray::try_new(
                        field_names.into(),
                        empty_arrays,
                        0,
                        Validity::NonNullable,
                    )
                    .map_err(|e| Error::AccelerationCreationFailed {
                        source: Box::new(e),
                    })?;

                    let vortex_table = VortexMemTable::new(
                        struct_array.into_array_data(),
                        VortexMemTableOptions::default(),
                    );

                    Ok(Arc::new(vortex_table))
                }
                _ => Err(Error::InvalidConfiguration {
                    detail: Arc::from(format!("Invalid mode: {mode}")),
                }
                .into()),
            }
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
    async fn test_vortex_file_path_generation() {
        let app = crate::app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "vortex_file_accelerator_test".to_string(),
            "vortex_file_accelerator_test",
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
        assert!(path.contains("vortex_file_accelerator_test"));
        assert!(path.ends_with(".vortex"));
    }

    #[tokio::test]
    async fn test_vortex_memory_mode() {
        let app = crate::app::AppBuilder::new("test").build();
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
