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
use datafusion_table_providers::{
    duckdb::{DuckDBSettingsRegistry, DuckDBTableProviderFactory},
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPoolBuilder,
};
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
use tokio::{fs::create_dir, sync::Mutex};

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

    #[snafu(display(
        "The 'duckdb_file' acceleration parameter has an invalid extension. Expected one of '{valid_extensions}' but got '{extension}'."
    ))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display(r"The 'duckdb_file' acceleration parameter is a directory."))]
    InvalidFileIsDirectory,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid DuckDB acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display("Partitioned DuckDB acceleration only supported for file mode."))]
    FileModeOnly,

    #[snafu(display("Partitioned DuckDB acceleration only supports a single table"))]
    SingleTable,

    #[snafu(display("Unable to read directory: {source}"))]
    UnableToReadDirectory { source: std::io::Error },

    #[snafu(display("Unable to get file stem"))]
    UnableToGetFileStem,

    #[snafu(display("Unable to create partition: {source}"))]
    UnableToCreatePartition {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
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
pub(crate) struct DuckDBPartitionCreator {
    cmd: CreateExternalTable,
    duckdb_factory: DuckDBTableProviderFactory,
}

impl DuckDBPartitionCreator {
    pub(crate) fn new(cmd: CreateExternalTable) -> Self {
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
    ) -> Result<Arc<dyn TableProvider>, creator::Error> {
        let mut cmd = self.cmd.clone();
        let partition_value_str = encode_scalar_value(&partition_value)
            .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

        let table = cmd.name.table();
        let data_path = spice_data_base_path();

        let duckdb_file = format!("{data_path}/{table}/{partition_value_str}.db");
        let pool_builder = DuckDbConnectionPoolBuilder::file(&duckdb_file)
            .with_max_size(Some(10))
            .with_min_idle(Some(10));
        self.duckdb_factory
            .get_or_init_instance_with_builder(pool_builder)
            .await
            .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

        cmd.options.insert("open".to_string(), duckdb_file);

        tracing::debug!("creating partition at {}", &cmd.location);

        let table_provider = create_table_provider(&self.duckdb_factory, &cmd)
            .await
            .map_err(|e| creator::Error::CreatePartition { source: e })?;

        Ok(table_provider)
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        let data_path = spice_data_base_path();
        let table = self.cmd.name.table();
        let data_path = Path::new(&data_path).join(table);

        if !data_path.is_dir() {
            create_dir(data_path)
                .await
                .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;
            return Ok(vec![]);
        }

        let mut dir_entries = tokio::fs::read_dir(&data_path)
            .await
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        let mut partitions = Vec::new();

        let valid_extensions = ["db"];

        while let Some(entry) = dir_entries
            .next_entry()
            .await
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?
        {
            let path = entry.path();
            if path.is_file() {
                let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
                if !valid_extensions.contains(&extension) {
                    continue;
                }

                let Some(file_name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                    continue;
                };

                let partition_value = match decode_scalar_value(file_name) {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::trace!("Unable to decode ScalarValue: {e}");
                        continue;
                    }
                };

                let table_provider = create_table_provider(&self.duckdb_factory, &self.cmd)
                    .await
                    .map_err(|e| creator::Error::InferringPartitions { source: e })?;

                partitions.push(Partition {
                    partition_value,
                    table_provider,
                });
            }
        }

        tracing::info!(
            "inferred {} existing partitions from '{}'",
            partitions.len(),
            data_path.display()
        );
        Ok(partitions)
    }
}
