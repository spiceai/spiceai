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
    util::{constraints::UpsertOptions, on_conflict::OnConflict},
};
use duckdb::AccessMode;
use runtime_table_partition::{
    Partition,
    creator::{self, PartitionCreator, filename::parse_partition_value},
    expression::PartitionedBy,
    provider::PartitionTableProvider,
};
use snafu::{OptionExt, prelude::*};

use crate::dataaccelerator::{BootstrapStatus, upsert_dedup::UpsertDedupTableProvider};
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
    datafusion::{dialect::new_duckdb_dialect, udf::deny_spice_specific_functions},
    make_spice_data_directory,
    parameters::ParameterSpec,
    register_data_accelerator,
};

type Result<T, E = super::Error> = std::result::Result<T, E>;

/// Accelerator for managing `DuckDB` table-based partitioning within a single database file.
/// This struct coordinates partitioned data storage and access using `DuckDB` tables,
/// enabling partition management and query execution in a unified database.
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

        let pool_size = source
            .acceleration()
            .and_then(|accel| accel.params.get("connection_pool_size"))
            .and_then(|size_str| size_str.parse::<u32>().ok());

        get_pool(&self.duckdb_factory, &duckdb_path, pool_size)
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
        "partitioned_duckdb[tables]"
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        self.has_existing_file(source)
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        DuckDBPartitionCreator::valid_file_extensions()
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        duckdb_file_path(&self.duckdb_factory, source, &source.name().to_string()).map_err(|e| {
            FilePathError::External {
                engine: Engine::DuckDB,
                source: e.into(),
            }
        })
    }

    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
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
        Ok(BootstrapStatus::none())
    }

    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let partition_by_last = partition_by
            .last()
            .context(super::PartitionByRequiredSnafu)?
            .clone();

        let source = source.context(ExpectedAccelerationSourceSnafu)?;

        if !cmd.options.contains_key("open") {
            let duckdb_file = self.file_path(source)?;
            cmd.options.insert("open".to_string(), duckdb_file);
        }

        let schema = Arc::new(cmd.schema.as_arrow().clone());
        let creator = Arc::new(
            DuckDBPartitionCreator::new(
                self.get_shared_pool(source).await?,
                cmd.clone(),
                partition_by_last,
                Arc::clone(&schema),
                &self.duckdb_factory,
            )
            .await?,
        );

        // Create custom DuckDB insertion strategy
        let insert_strategy = Arc::new(DuckDBPartitionedInsertStrategy::new(
            self.get_shared_pool(source).await?,
            creator.table_definition(),
            creator.on_conflict().cloned(),
            creator.upsert_options().clone(),
            source,
        ));

        let table_provider = Arc::new(
            PartitionTableProvider::new(creator, partition_by, schema)
                .await?
                .with_insert_strategy(insert_strategy),
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

/// Responsible for discovering and managing table-based partitions in `DuckDB` by
/// encapsulating the logic for creating and handling partitions based on table
/// definitions and partitioning expressions, interacting with the `DuckDB` connection
/// pool and external table creation commands to ensure partitions are correctly
/// discovered, initialized, and managed according to the schema and partitioning
/// strategy specified.
#[derive(Debug)]
struct DuckDBPartitionCreator {
    pool: Arc<DuckDbConnectionPool>,
    cmd: CreateExternalTable,
    table_definition: Arc<TableDefinition>,
    on_conflict: Option<OnConflict>,
    upsert_options: UpsertOptions,
    partition_by: PartitionedBy,
    schema: SchemaRef,
}

impl DuckDBPartitionCreator {
    async fn new(
        pool: Arc<DuckDbConnectionPool>,
        cmd: CreateExternalTable,
        partition_by: PartitionedBy,
        schema: SchemaRef,
        duckdb_factory: &DuckDBTableProviderFactory,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // We use the DuckDB factory to create a table provider in order to extract
        // target table definition and on_conflict settings that will be used directly for each partition.
        let table_provider = create_table_provider(duckdb_factory, &cmd, None)
            .await
            .map_err(|e| format!("Failed to create table provider: {e}"))?;

        let poly_table = table_provider
            .as_any()
            .downcast_ref::<PolyTableProvider>()
            .ok_or("Expected PolyTableProvider but got different table provider type")?;

        let writer = poly_table.writer();
        let writer = extract_duckdb_writer(&writer)?;

        // Extract UpsertOptions from cmd options
        let upsert_options = Self::extract_upsert_options(&cmd);

        Ok(Self {
            pool,
            cmd,
            table_definition: writer.table_definition(),
            on_conflict: writer.on_conflict().cloned(),
            upsert_options,
            partition_by,
            schema,
        })
    }

    /// Extracts `UpsertOptions` from the command options.
    fn extract_upsert_options(cmd: &CreateExternalTable) -> UpsertOptions {
        let remove_duplicates = cmd
            .options
            .get("upsert_remove_duplicates")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let last_write_wins = cmd
            .options
            .get("upsert_last_write_wins")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        UpsertOptions {
            remove_duplicates,
            last_write_wins,
        }
    }

    pub(crate) fn table_definition(&self) -> Arc<TableDefinition> {
        Arc::clone(&self.table_definition)
    }

    pub fn on_conflict(&self) -> Option<&OnConflict> {
        self.on_conflict.as_ref()
    }

    pub fn upsert_options(&self) -> &UpsertOptions {
        &self.upsert_options
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
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_name LIKE ?")
            .map_err(|e| creator::Error::InferringPartitions { source: e.into() })?;

        let pattern = format!("%/{}", self.cmd.name);

        let table_names: Vec<String> = stmt
            .query_map([&pattern], |row| row.get::<_, String>(0))
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

        let duckdb_table_factory = DuckDBTableFactory::new(Arc::clone(&self.pool))
            .with_dialect(new_duckdb_dialect())
            .with_schema(Arc::clone(&self.schema))
            .with_indexes(self.table_definition.indexes().to_vec());

        let mut partitions = Vec::with_capacity(partitioned_tables.len());
        for table in partitioned_tables {
            let Some(partition_expr) = table.strip_suffix(&format!("/{table_name}")) else {
                tracing::warn!(
                    "Excluded partitioned table '{table}' as it does not match expected partitioning pattern"
                );
                continue;
            };

            // Extract the partition value by removing the partition name prefix
            // The partition_expr is in format "{partition_by.name}={value}"
            // For example: "bucket(3, passenger_count)=2"
            // We need to extract the value after the last '=' that follows the partition name
            let partition_prefix = format!("{}=", self.partition_by.name);
            let Some(value_str) = partition_expr.strip_prefix(&partition_prefix) else {
                tracing::warn!(
                    "Excluded partitioned table '{table}' as partition expression '{partition_expr}' does not match expected prefix '{partition_prefix}'"
                );
                continue;
            };

            let partition_value = parse_partition_value(&schema, &self.partition_by, value_str)
                .map_err(|e| {
                    tracing::error!(
                        "Failed to parse partition value from table '{table}': partition_expr='{partition_expr}', value_str='{value_str}', partition_by.name='{}', error: {e}",
                        self.partition_by.name
                    );
                    creator::Error::InferringPartitions { source: e.into() }
                })?;

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

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        if self.cmd.constraints.is_empty() {
            None
        } else {
            Some(&self.cmd.constraints)
        }
    }
}

fn create_factory() -> DuckDBTableProviderFactory {
    DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
        .with_dialect(new_duckdb_dialect())
        .with_settings_registry(
            DuckDBSettingsRegistry::new().with_setting(Box::new(OrderByNonIntegerLiteral)),
        )
        .with_function_support(deny_spice_specific_functions())
}

async fn get_pool(
    duckdb_factory: &DuckDBTableProviderFactory,
    duckdb_path: &str,
    connection_pool_size: Option<u32>,
) -> Result<Arc<DuckDbConnectionPool>, datafusion_table_providers::duckdb::Error> {
    let pool_builder = DuckDbConnectionPoolBuilder::file(duckdb_path)
        .with_max_size(Some(connection_pool_size.unwrap_or(10)))
        .with_min_idle(Some(
            crate::dataaccelerator::duckdb::DEFAULT_MIN_IDLE_CONNECTIONS,
        ));
    Ok(Arc::new(
        duckdb_factory
            .get_or_init_instance_with_builder(pool_builder)
            .await?,
    ))
}

/// Extracts the `DuckDBTableWriter` from a table provider, handling the case where
/// it may be wrapped in an `UpsertDedupTableProvider` when upsert options are enabled.
fn extract_duckdb_writer(
    writer: &Arc<dyn TableProvider>,
) -> std::result::Result<&DuckDBTableWriter, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(w) = writer.as_any().downcast_ref::<DuckDBTableWriter>() {
        Ok(w)
    } else if let Some(upsert_provider) = writer.as_any().downcast_ref::<UpsertDedupTableProvider>()
    {
        upsert_provider
            .inner()
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .ok_or_else(|| "UpsertDedupTableProvider inner is not DuckDBTableWriter".into())
    } else {
        Err(
            "Expected DuckDBTableWriter or UpsertDedupTableProvider but got different writer type"
                .into(),
        )
    }
}

register_data_accelerator!(
    Engine::TableModePartitionedDuckDB,
    TablesModePartitionedDuckDBAccelerator
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Runtime;
    use crate::component::dataset::acceleration::{Acceleration, Engine, Mode};
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataaccelerator::DataAccelerator;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{CreateExternalTable, col, dml::InsertOp},
        physical_plan::collect,
    };
    use datafusion_table_providers::util::test::MockExec;
    use runtime_table_partition::expression::PartitionedBy;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_tables_mode_partitioned_duckdb_accelerator() {
        // Ensure no previous database version exists
        let test_db_path = "./test_table.db";
        if std::path::Path::new(test_db_path).exists() {
            std::fs::remove_file(test_db_path).expect("Failed to remove existing test database");
        }

        // Create app and runtime
        let app = app::AppBuilder::new("test_partitioned_duckdb").build();
        let rt = Runtime::builder().build().await;

        // Create dataset with partitioned DuckDB acceleration
        let mut dataset = DatasetBuilder::try_new("test_source".to_string(), "test_table")
            .expect("Failed to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::TableModePartitionedDuckDB,
            mode: Mode::File,
            enabled: true,
            params: {
                let mut params = HashMap::new();
                params.insert("duckdb_file".to_string(), test_db_path.to_string());
                params
            },
            ..Default::default()
        });

        // Create schema matching the sink test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let partitioned_by = vec![PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        }];

        let accelerator = TablesModePartitionedDuckDBAccelerator::new();

        let table = accelerator
            .create_external_table(external_table, Some(&dataset), partitioned_by)
            .await
            .expect("accelerated table created");

        let test_data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                Arc::new(StringArray::from(vec![
                    Some("us-east-1"),
                    Some("us-west-1"),
                    Some("us-east-1"),
                    Some("us-west-1"),
                ])),
            ],
        )
        .expect("test data should be created");

        // Create DataFusion context and insert data using MockExec
        let ctx = SessionContext::new();
        let exec = Arc::new(MockExec::new(vec![Ok(test_data)], Arc::clone(&schema)));

        let insertion = table
            .insert_into(&ctx.state(), exec, InsertOp::Overwrite)
            .await
            .expect("insertion plan created");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insertion successful");

        // Verify insertion result
        assert!(!result.is_empty());

        ctx.register_table("test_table", Arc::clone(&table))
            .expect("table registration successful");

        // Test 1: Show all data
        // The order of partitions in SELECT * is not deterministic so we don't snapshot explain
        run_query_and_snapshot(
            &ctx,
            "SELECT * FROM test_table order by id",
            "select_all",
            false,
        )
        .await
        .expect("select all query successful");

        // Test 2: Filter by existing region = 'us-east-1'
        run_query_and_snapshot(
            &ctx,
            "SELECT * FROM test_table WHERE region = 'us-east-1'",
            "filter_us_east",
            true,
        )
        .await
        .expect("east region query successful");

        // Test 3: Filter by non-existent region
        run_query_and_snapshot(
            &ctx,
            "SELECT * FROM test_table WHERE region = 'eu-central-1'",
            "filter_nonexistent",
            true,
        )
        .await
        .expect("non-existent region query successful");

        // cleanup
        std::fs::remove_file(test_db_path).expect("file should be removed");
    }

    async fn run_query_and_snapshot(
        ctx: &SessionContext,
        query_string: impl AsRef<str>,
        test_name: &str,
        snapshot_explain: bool,
    ) -> anyhow::Result<()> {
        let query_string = query_string.as_ref();

        if snapshot_explain {
            // Execute EXPLAIN query and snapshot the result
            let explain_result = ctx.sql(&format!("EXPLAIN {query_string}")).await?;
            let explain_batches = explain_result.collect().await?;
            let explain_pretty = arrow::util::pretty::pretty_format_batches(&explain_batches)?;
            insta::assert_snapshot!(format!("{test_name}_explain"), explain_pretty);
        }

        // Execute actual query and snapshot the result
        let query_result = ctx.sql(query_string).await?;
        let result_batches = query_result.collect().await?;
        let result_pretty = arrow::util::pretty::pretty_format_batches(&result_batches)?;
        insta::assert_snapshot!(format!("{test_name}_result"), result_pretty);

        Ok(())
    }
}
