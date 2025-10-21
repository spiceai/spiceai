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

use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::DFSchema,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{CreateExternalTable, TableProviderFilterPushDown},
    prelude::Expr,
    scalar::ScalarValue,
    sql::unparser::expr_to_sql,
};
use datafusion_table_providers::{
    duckdb::{DuckDBSettingsRegistry, DuckDBTableProviderFactory},
    sql::db_connection_pool::duckdbpool::{DuckDbConnectionPool, DuckDbConnectionPoolBuilder},
};
use duckdb::AccessMode;
use runtime_table_partition::{
    Partition,
    creator::{
        self, PartitionCreator,
        filename::{discover_hive_partitions, to_hive_partition_dir},
    },
    expression::PartitionedBy,
    provider::PartitionTableProvider,
};
use snafu::{OptionExt, prelude::*};
use tokio::{fs::create_dir_all, sync::Mutex};

use super::{
    AccelerationSource, DataAccelerator,
    duckdb::{DuckDBAccelerator, create_table_provider, settings::OrderByNonIntegerLiteral},
};
use crate::{
    component::dataset::acceleration::Mode, dataaccelerator::FilePathError,
    datafusion::dialect::new_duckdb_dialect, parameters::ParameterSpec, spice_data_base_path,
};

pub mod tables_mode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuckDBPartitionMode {
    Tables,
    Files,
}

impl DuckDBPartitionMode {
    pub fn parse_str(s: &str) -> Self {
        match s {
            "tables" => DuckDBPartitionMode::Tables,
            "files" => DuckDBPartitionMode::Files,
            other => {
                tracing::warn!(
                    "Unknown `partition_mode` '{}', defaulting to 'files' mode.",
                    other
                );
                DuckDBPartitionMode::Files
            }
        }
    }
}

#[must_use]
pub fn get_duckdb_partition_mode(params: &Option<spicepod::param::Params>) -> DuckDBPartitionMode {
    params
        .as_ref()
        .and_then(|p| p.as_string_map().get("partition_mode").cloned())
        .map_or(DuckDBPartitionMode::Files, |v| {
            DuckDBPartitionMode::parse_str(&v)
        })
}

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

    #[snafu(display("Unable to read directory: {source}"))]
    UnableToReadDirectory { source: std::io::Error },

    #[snafu(display("Unable to create checkpointing pool: {source}"))]
    FailedToCreateCheckpointingPool {
        source: datafusion_table_providers::duckdb::Error,
    },

    #[snafu(display("Unable to create DuckDB connection pool: {source}"))]
    FailedToCreateConnectionPool {
        source: datafusion_table_providers::duckdb::Error,
    },

    #[snafu(display("Unable to get file stem"))]
    UnableToGetFileStem,

    #[snafu(display("Partitioned DuckDB expected an AccelerationSource"))]
    ExpectedAccelerationSource,

    #[snafu(display(
        "A single partition by expression is required for Partitioned DuckDB acceleration"
    ))]
    PartitionByRequired,

    #[snafu(display("Unable to create partition: {source}"))]
    UnableToCreatePartition {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) struct PartitionedDuckDBAccelerator {
    base_accelerator: DuckDBAccelerator,
    table_provider: Mutex<Option<Arc<PartitionTableProvider>>>,
    is_initialized: AtomicBool,
    duckdb_factory: DuckDBTableProviderFactory,
}

impl PartitionedDuckDBAccelerator {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            base_accelerator: DuckDBAccelerator::new(),
            table_provider: Mutex::new(None),
            is_initialized: AtomicBool::new(false),
            duckdb_factory: create_factory(),
        }
    }

    /// Returns an existing `DuckDB` connection pool for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<DuckDbConnectionPool>> {
        let duckdb_path = partition_dir(source)
            .join("checkpoint.db")
            .display()
            .to_string();

        get_pool(&self.duckdb_factory, &duckdb_path)
            .await
            .context(FailedToCreateCheckpointingPoolSnafu)
    }
}

fn parameter_validation(source: &dyn AccelerationSource) {
    if let Some(acceleration) = source.acceleration() {
        if acceleration.params.contains_key("duckdb_file") {
            tracing::warn!(
                "'duckdb_file' was specified and will be ignored because it is not applicable for partitioned DuckDB acceleration."
            );
        }

        if !acceleration.params.contains_key("duckdb_data_dir") {
            tracing::debug!(
                "'duckdb_data_dir' was not specified. Defaulting to {} directory.",
                spice_data_base_path()
            );
        }
    }
}

fn partition_dir(source: &dyn AccelerationSource) -> PathBuf {
    let fallback = spice_data_base_path();
    let base_dir = source
        .acceleration()
        .and_then(|a| a.params.get("duckdb_data_dir"))
        .filter(|dir| {
            let is_dir = Path::new(dir).is_dir();
            if !is_dir && std::fs::create_dir_all(dir).is_err() {
                tracing::warn!("'duckdb_data_dir' ({dir}) is not a directory and could not be created. Using default directory {fallback} instead.");
            }
            is_dir
        })
        .unwrap_or(&fallback);

    PathBuf::from(base_dir).join(source.name().to_string())
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

    fn is_initialized(&self, _source: &dyn AccelerationSource) -> bool {
        self.is_initialized.load(Ordering::Acquire)
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        DuckDBPartitionCreator::valid_file_extensions()
    }

    fn file_path(&self, _source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        // There is no one file path but one for each partition
        // This function is only internally used (within this trait) in the
        // DuckDBAccelerator, for example, but is never used in this
        // implementation.
        Err(FilePathError::FileModeUnsupported {})
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
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        self.is_initialized.store(false, Ordering::Release);

        let partition_by_first = partition_by
            .first()
            .context(PartitionByRequiredSnafu)?
            .clone();

        let source = source.context(ExpectedAccelerationSourceSnafu)?;

        parameter_validation(source);

        let mut table_provider_guard = self.table_provider.lock().await;

        let schema = Arc::new(cmd.schema.as_arrow().clone());
        let creator = Arc::new(DuckDBPartitionCreator::new(
            partition_dir(source),
            cmd,
            partition_by_first,
            Arc::clone(&schema),
        ));
        let table_provider =
            Arc::new(PartitionTableProvider::new(creator, partition_by, schema).await?);

        *table_provider_guard = Some(Arc::clone(&table_provider));
        self.is_initialized.store(true, Ordering::Release);

        Ok(table_provider as Arc<dyn TableProvider>)
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
    partition_dir: PathBuf,
    partition_by: PartitionedBy,
    schema: SchemaRef,
}

impl DuckDBPartitionCreator {
    pub(crate) fn new(
        partition_dir: PathBuf,
        cmd: CreateExternalTable,
        partition_by: PartitionedBy,
        schema: SchemaRef,
    ) -> Self {
        let duckdb_factory = create_factory();

        Self {
            cmd,
            duckdb_factory,
            partition_dir,
            partition_by,
            schema,
        }
    }

    fn valid_file_extensions() -> Vec<&'static str> {
        vec!["db", "ddb", "duckdb"]
    }

    fn add_open(
        &self,
        cmd: &mut CreateExternalTable,
        partition_value: &ScalarValue,
    ) -> Result<String, creator::Error> {
        let hive_path =
            to_hive_partition_dir(&[(self.partition_by.clone(), partition_value.clone())])
                .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;
        let duckdb_path = self.partition_dir.join(&hive_path);
        if !duckdb_path.is_dir() {
            std::fs::create_dir_all(&duckdb_path)
                .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;
        }
        let duckdb_path = duckdb_path.join("data.db");
        let duckdb_path = duckdb_path.display().to_string();
        cmd.options.insert("open".to_string(), duckdb_path.clone());

        Ok(duckdb_path)
    }
}

#[async_trait]
impl PartitionCreator for DuckDBPartitionCreator {
    async fn create_partition(
        &self,
        partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        let mut cmd = self.cmd.clone();
        let duckdb_path = self
            .add_open(&mut cmd, &partition_value)
            .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

        tracing::debug!("creating partition at {duckdb_path}");

        let table_provider = create_table_provider(&self.duckdb_factory, &cmd)
            .await
            .map_err(|e| creator::Error::CreatePartition { source: e })?;

        let partition = Partition {
            partition_value,
            table_provider,
        };

        Ok(partition)
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        if !self.partition_dir.is_dir() {
            create_dir_all(&self.partition_dir)
                .await
                .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;
            return Ok(vec![]);
        }

        let schema = DFSchema::try_from(Arc::clone(&self.schema))
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;
        let hive_partitions = discover_hive_partitions(
            &schema,
            &self.partition_dir,
            std::slice::from_ref(&self.partition_by),
        )
        .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        let mut partitions = Vec::with_capacity(hive_partitions.len());
        for (mut keys, path) in hive_partitions {
            if keys.len() != 1 {
                continue;
            }

            let Some(partition_value) = keys.pop() else {
                continue;
            };

            let mut cmd = self.cmd.clone();
            self.add_open(&mut cmd, &partition_value)
                .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

            let duckdb_path = path.display().to_string();
            get_pool(&self.duckdb_factory, &duckdb_path)
                .await
                .map_err(|e| creator::Error::CreatePartition { source: e.into() })?;

            let table_provider = create_table_provider(&self.duckdb_factory, &cmd)
                .await
                .map_err(|e| creator::Error::InferringPartitions { source: e })?;

            partitions.push(Partition {
                partition_value,
                table_provider,
            });
        }

        tracing::debug!(
            "inferred {} existing partitions from '{}'",
            partitions.len(),
            self.partition_dir.display().to_string(),
        );
        Ok(partitions)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|expr| {
                if expr_to_sql(expr).is_ok() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

fn create_factory() -> DuckDBTableProviderFactory {
    DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
        .with_dialect(new_duckdb_dialect())
        .with_settings_registry(
            DuckDBSettingsRegistry::new().with_setting(Box::new(OrderByNonIntegerLiteral)),
        )
}

async fn get_pool(
    duckdb_factory: &DuckDBTableProviderFactory,
    duckdb_path: &str,
) -> Result<Arc<DuckDbConnectionPool>, datafusion_table_providers::duckdb::Error> {
    let pool_builder = DuckDbConnectionPoolBuilder::file(duckdb_path)
        .with_max_size(Some(10))
        .with_min_idle(Some(10));
    Ok(Arc::new(
        duckdb_factory
            .get_or_init_instance_with_builder(pool_builder)
            .await?,
    ))
}
