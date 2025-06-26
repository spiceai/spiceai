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

use crate::{
    App, Runtime,
    component::dataset::acceleration::{Engine, Mode},
    datafusion::dialect::new_duckdb_dialect,
    parameters::ParameterSpec,
};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider, logical_expr::CreateExternalTable, prelude::Expr,
    scalar::ScalarValue,
};
use datafusion_table_providers::duckdb::{DuckDBSettingsRegistry, DuckDBTableProviderFactory};
use duckdb::AccessMode;
use runtime_table_partition::{
    Partition,
    creator::{self, PartitionCreator},
    provider::PartitionTableProvider,
};
use snafu::prelude::*;
use std::{any::Any, cmp::max, sync::Arc};
use tokio::sync::Mutex;

use super::{
    AccelerationSource, DataAccelerator, Error as DataAcceleratorError,
    duckdb::{DuckDBAccelerator, settings::OrderByNonIntegerLiteral},
};

const DEFAULT_MIN_IDLE_CONNECTIONS: u32 = 10;

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

    fn get_num_accelerating_datasets(
        &self,
        path: Option<&str>,
        app: &Arc<App>,
        rt: Arc<Runtime>,
    ) -> u32 {
        let mut instance_usage: u32 = 1;

        let datasets = rt.get_valid_datasets(app, crate::LogErrors(false));
        for ds in datasets {
            if let Some(acceleration) = &ds.acceleration {
                if acceleration.engine != Engine::DuckDB {
                    continue;
                }

                // If the path is Some, we're counting the number of file instances
                if let Some(this_file_path) = path {
                    if acceleration.mode == Mode::File {
                        if let Ok(file_path) = self.file_path(ds.as_ref()) {
                            if this_file_path == file_path {
                                instance_usage += 1;
                            }
                        }
                    }
                } else {
                    // If the path is None, we're just counting the number of memory instances
                    if acceleration.mode == Mode::Memory {
                        instance_usage += 1;
                    }
                }
            }
        }

        instance_usage
    }

    fn get_max_size(num_accelerating_datasets: u32) -> u32 {
        max(DEFAULT_MIN_IDLE_CONNECTIONS, num_accelerating_datasets)
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

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        _source: Option<&dyn AccelerationSource>,
        partition_by: Vec<Expr>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
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
        _partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        todo!()
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        todo!()
    }
}
