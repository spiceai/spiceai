/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{any::Any, path::Path, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider, logical_expr::CreateExternalTable, prelude::Expr,
    scalar::ScalarValue,
};
use datafusion_table_providers::duckdb::{DuckDBSettingsRegistry, DuckDBTableProviderFactory};
use duckdb::AccessMode;
use runtime_table_partition::{
    Partition,
    creator::{
        self, PartitionCreator,
        filename::{decode_scalar_value, encode_scalar_value},
    },
    provider::PartitionTableProvider,
};
use snafu::prelude::*;
use tokio::sync::Mutex;

use super::{
    AccelerationSource, DataAccelerator, Error as DataAcceleratorError,
    duckdb::{DuckDBAccelerator, create_table_provider, settings::OrderByNonIntegerLiteral},
};
use crate::{
    component::dataset::acceleration::Mode, datafusion::dialect::new_duckdb_dialect,
    parameters::ParameterSpec, spice_data_base_path,
};

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

    #[snafu(display(r#"The "duckdb_file" acceleration parameter has an invalid extension. Expected one of "{valid_extensions}" but got "{extension}"."#))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display(r#"The "duckdb_file" acceleration parameter is a directory."#))]
    InvalidFileIsDirectory,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid DuckDB acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display("Partitioned DuckDB acceleration only supported for file mode."))]
    FileModeOnly,

    #[snafu(display("Partitioned DuckDB acceleration only supports a single table"))]
    SingleTable,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct PartitionedDuckDBAccelerator {
    base_accelerator: DuckDBAccelerator,
    table_provider: Mutex<Option<Arc<PartitionTableProvider>>>,
}

impl PartitionedDuckDBAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            base_accelerator: DuckDBAccelerator::new(),
            table_provider: Mutex::new(None),
        }
    }
}

impl Default for PartitionedDuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataAccelerator for PartitionedDuckDBAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "partitioned_duckdb"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        self.base_accelerator.valid_file_extensions()
    }

    fn file_path(&self, _source: &dyn AccelerationSource) -> Result<String, DataAcceleratorError> {
        // We have (possibly) many file paths because we make a file for each partition
        Ok(String::new())
    }

    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(acceleration_settings) = source.acceleration() {
            ensure!(
                matches!(acceleration_settings.mode, Mode::File),
                FileModeOnlySnafu
            );
        }
        Ok(())
    }

    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<Expr>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(source) = source {
            if let Some(temp_directory) = &source.app().runtime.temp_directory.clone() {
                cmd.options
                    .insert("temp_directory".to_string(), temp_directory.to_string());
            }
        }

        let mut table_provider_guard = self.table_provider.lock().await;
        ensure!(table_provider_guard.is_none(), SingleTableSnafu);

        let schema = Arc::new(cmd.schema.as_arrow().clone());
        let creator = Arc::new(DuckDBPartitionCreator::new(cmd));
        let table_provider =
            Arc::new(PartitionTableProvider::new(creator, partition_by, schema).await?);

        *table_provider_guard = Some(Arc::clone(&table_provider));

        Ok(table_provider)
    }

    fn prefix(&self) -> &'static str {
        self.base_accelerator.prefix()
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        self.base_accelerator.parameters()
    }
}

#[derive(Debug)]
struct DuckDBPartitionCreator {
    cmd: CreateExternalTable,
    duckdb_factory: DuckDBTableProviderFactory,
}

impl DuckDBPartitionCreator {
    fn new(cmd: CreateExternalTable) -> Self {
        Self {
            cmd,
            duckdb_factory: DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
                .with_dialect(new_duckdb_dialect())
                .with_settings_registry(
                    DuckDBSettingsRegistry::new().with_setting(Box::new(OrderByNonIntegerLiteral)),
                ),
        }
    }
}

#[async_trait]
impl PartitionCreator for DuckDBPartitionCreator {
    async fn create_partition(
        &self,
        partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        let mut cmd = self.cmd.clone();
        let partition_value_str = encode_scalar_value(&partition_value).unwrap();

        let data_path = spice_data_base_path();

        cmd.location = format!("{data_path}/{partition_value_str}.duckdb");

        tracing::debug!("creating partition at {}", &cmd.location);

        let table_provider = create_table_provider(&self.duckdb_factory, &cmd)
            .await
            .unwrap();

        let partition = Partition {
            partition_value,
            table_provider,
        };

        Ok(partition)
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        let data_path = spice_data_base_path();
        let data_path = Path::new(&data_path);

        let mut dir_entries = match tokio::fs::read_dir(data_path).await {
            Ok(entries) => entries,
            Err(e) => {
                panic!("{e}");
            }
        };

        let mut partitions = Vec::new();

        let valid_extensions = vec!["duckdb"]; // From base_accelerator.valid_file_extensions()

        while let Some(entry) = dir_entries.next_entry().await.unwrap() {
            let path = entry.path();
            if path.is_file() {
                let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
                if !valid_extensions.contains(&extension) {
                    continue;
                }

                let file_name = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| panic!(""))?;

                let partition_value = match decode_scalar_value(file_name) {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::debug!("{e}");
                        continue;
                    }
                };

                let mut cmd = self.cmd.clone();
                cmd.options
                    .insert("location".to_string(), path.to_string_lossy().into_owned());

                let table_provider = create_table_provider(&self.duckdb_factory, &self.cmd)
                    .await
                    .unwrap();

                partitions.push(Partition {
                    partition_value,
                    table_provider,
                });
            }
        }

        tracing::info!(
            "Inferred {} existing partitions from '{}'",
            partitions.len(),
            data_path.display()
        );
        Ok(partitions)
    }
}
