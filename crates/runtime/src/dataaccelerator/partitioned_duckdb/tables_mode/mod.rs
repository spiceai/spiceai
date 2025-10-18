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

mod insert;
mod partition_buffer;
mod sink;

pub use insert::DuckDBPartitionedInsertStrategy;

use std::{any::Any, ffi::OsStr, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
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
    duckdb::{
        DuckDB, DuckDBSettingsRegistry, DuckDBTableFactory, DuckDBTableProviderFactory,
        TableDefinition, write::DuckDBTableWriter,
    },
    sql::db_connection_pool::duckdbpool::{DuckDbConnectionPool, DuckDbConnectionPoolBuilder},
};
use duckdb::AccessMode;
use runtime_table_partition::{
    Partition,
    creator::{self, PartitionCreator, filename::parse_partition_value},
    expression::PartitionedBy,
    provider::PartitionTableProvider,
};
use snafu::{OptionExt, prelude::*};

use crate::{
    component::dataset::acceleration::{Engine, Mode},
    dataaccelerator::{
        AccelerationSource, DataAccelerator, FilePathError,
        duckdb::{
            DuckDBAccelerator, create_table_provider, duckdb_file_path,
            settings::OrderByNonIntegerLiteral,
        },
        partitioned_duckdb::{
            ExpectedAccelerationSourceSnafu, FailedToCreateConnectionPoolSnafu, FileModeOnlySnafu,
        },
    },
    datafusion::dialect::new_duckdb_dialect,
    make_spice_data_directory,
    parameters::ParameterSpec,
};

type Result<T, E = super::Error> = std::result::Result<T, E>;

pub(crate) struct TablesModePartitionedDuckDBAccelerator {
    base_accelerator: DuckDBAccelerator,
    duckdb_factory: DuckDBTableProviderFactory,
}

impl TablesModePartitionedDuckDBAccelerator {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            base_accelerator: DuckDBAccelerator::new(),
            duckdb_factory: create_factory(),
        }
    }

    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<DuckDbConnectionPool>> {
        let duckdb_path = self
            .file_path(source)
            .map_err(|e| super::Error::AccelerationInitializationFailed { source: e.into() })?;

        get_pool(&self.duckdb_factory, &duckdb_path)
            .await
            .context(FailedToCreateConnectionPoolSnafu)
    }
}

impl Default for TablesModePartitionedDuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataAccelerator for TablesModePartitionedDuckDBAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "partitioned_duckdb(tables_mode)"
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        self.has_existing_file(source)
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        DuckDBPartitionCreator::valid_file_extensions()
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        duckdb_file_path(&self.duckdb_factory, source).map_err(|e| FilePathError::External {
            engine: Engine::DuckDB,
            source: e.into(),
        })
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

        let path = self.file_path(source)?;

        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("duckdb_file") {
                make_spice_data_directory().map_err(|err| {
                    super::Error::AccelerationInitializationFailed { source: err.into() }
                })?;
            } else if !self.is_valid_file(source) {
                if std::path::Path::new(&path).is_dir() {
                    return Err(super::Error::InvalidFileIsDirectory.into());
                }

                let extension = std::path::Path::new(&path)
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("");

                return Err(super::Error::InvalidFileExtension {
                    valid_extensions: self.valid_file_extensions().join(","),
                    extension: extension.to_string(),
                }
                .into());
            }
            self.get_shared_pool(source).await?;
        }
        Ok(())
    }

    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let partition_by_first = partition_by
            .first()
            .context(super::PartitionByRequiredSnafu)?
            .clone();

        let source = source.context(ExpectedAccelerationSourceSnafu)?;

        super::parameter_validation(source);

        if !cmd.options.contains_key("open") {
            let duckdb_file = duckdb_file_path(&self.duckdb_factory, source)?;
            cmd.options.insert("open".to_string(), duckdb_file);
        }

        let schema = Arc::new(cmd.schema.as_arrow().clone());
        let creator = Arc::new(
            DuckDBPartitionCreator::new(
                self.get_shared_pool(source).await?,
                cmd.clone(),
                partition_by_first,
                Arc::clone(&schema),
            )
            .await?,
        );

        // Create custom DuckDB insertion strategy
        let insert_strategy = Arc::new(DuckDBPartitionedInsertStrategy::new(
            self.get_shared_pool(source).await?,
            creator.table_definition(),
        ));

        let table_provider = Arc::new(
            PartitionTableProvider::new_with_insert_strategy(
                creator,
                partition_by,
                schema,
                insert_strategy,
            )
            .await?,
        );

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
struct DuckDBPartitionCreator {
    pool: Arc<DuckDbConnectionPool>,
    cmd: CreateExternalTable,
    table_definition: Arc<TableDefinition>,
    partition_by: PartitionedBy,
    schema: SchemaRef,
}

impl DuckDBPartitionCreator {
    async fn new(
        pool: Arc<DuckDbConnectionPool>,
        cmd: CreateExternalTable,
        partition_by: PartitionedBy,
        schema: SchemaRef,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let duckdb_factory = create_factory();

        let table_provider = create_table_provider(&duckdb_factory, &cmd)
            .await
            .map_err(|e| format!("Failed to create table provider: {e}"))?;

        let poly_table = table_provider
            .as_any()
            .downcast_ref::<PolyTableProvider>()
            .ok_or("Expected PolyTableProvider but got different table provider type")?;

        let writer = poly_table.writer();

        let writer = writer
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .ok_or("Expected DuckDBTableWriter but got different writer type")?;

        Ok(Self {
            pool,
            cmd,
            table_definition: writer.table_definition(),
            partition_by,
            schema,
        })
    }

    pub(crate) fn table_definition(&self) -> Arc<TableDefinition> {
        Arc::clone(&self.table_definition)
    }

    fn list_partitioned_tables(&self) -> Result<Vec<String>, creator::Error> {
        let pool = Arc::clone(&self.pool);
        let mut conn = pool
            .connect_sync()
            .map_err(|e| creator::Error::InferringPartitions { source: e })?;

        let conn = DuckDB::duckdb_conn(&mut conn)
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        // collect all views and table names following format '/<table-name>', for example expr0=17/my_table
        let mut stmt = conn
            .conn
            .prepare(&format!(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%/{}'",
                self.cmd.name
            ))
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        Ok(table_names)
    }

    fn valid_file_extensions() -> Vec<&'static str> {
        vec!["db", "ddb", "duckdb"]
    }
}

#[async_trait]
impl PartitionCreator for DuckDBPartitionCreator {
    async fn create_partition(
        &self,
        _partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        Err(creator::Error::CreatePartition {
            source: "Table-based partitions must not be manually created".into(),
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        let partitioned_tables = self.list_partitioned_tables()?;

        let table_name = self.cmd.name.clone();
        let schema = DFSchema::try_from(Arc::clone(&self.schema))
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        let duckdb_table_factory =
            DuckDBTableFactory::new(Arc::clone(&self.pool)).with_dialect(new_duckdb_dialect());

        let mut partitions = Vec::with_capacity(partitioned_tables.len());
        for table in partitioned_tables {
            let Some(partition_expr) = table.strip_suffix(&format!("/{table_name}")) else {
                tracing::warn!(
                    "Excluded partitioned table '{table}' as it does not match expected partitioning pattern"
                );
                continue;
            };

            let Some((_, value_str)) = partition_expr.split_once('=') else {
                tracing::warn!(
                    "Excluded partitioned table '{table}' as it does not match expected partitioning pattern"
                );
                continue;
            };

            let partition_value = parse_partition_value(&schema, &self.partition_by, value_str)
                .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

            let table_provider = duckdb_table_factory
                .table_provider(table.into())
                .await
                .map_err(|e| creator::Error::InferringPartitions { source: e })?;

            partitions.push(Partition {
                partition_value,
                table_provider,
            });
        }

        tracing::debug!(
            "inferred {} existing partitions for '{table_name}'",
            partitions.len()
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
