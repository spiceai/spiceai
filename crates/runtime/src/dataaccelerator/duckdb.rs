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

use super::{AccelerationSource, BootstrapStatus, DataAccelerator};
use crate::{
    App, Runtime,
    component::{
        dataset::{
            Dataset,
            acceleration::{Acceleration, Engine, Mode},
        },
        view::View,
    },
    dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed},
    datafusion::{dialect::new_duckdb_dialect, udf::deny_spice_specific_functions},
    make_spice_data_directory,
    parameters::ParameterSpec,
    register_data_accelerator, spice_data_base_path,
};
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::error::DataFusionError;
use datafusion::{
    catalog::TableProviderFactory,
    datasource::TableProvider,
    execution::context::SessionContext,
    logical_expr::CreateExternalTable,
    sql::sqlparser::ast::{
        Delete, FromTable, Ident, ObjectName, ObjectNamePart, Statement as SQLStatement,
        TableFactor,
    },
};
use datafusion_table_providers::{
    duckdb::{
        DuckDBSettingsRegistry, DuckDBTableProviderFactory,
        write::{DuckDBTableWriter, WriteCompletionHandler},
    },
    sql::db_connection_pool::duckdbpool::{DuckDbConnectionPool, DuckDbConnectionPoolBuilder},
};
use duckdb::AccessMode;
use itertools::Itertools;
use runtime_acceleration::snapshot::AccelerationEngine;
use runtime_table_partition::expression::PartitionedBy;
use settings::OrderByNonIntegerLiteral;
use snafu::prelude::*;
use std::collections::HashMap;
use std::{
    any::Any,
    cmp::max,
    collections::HashSet,
    ffi::OsStr,
    path::PathBuf,
    sync::{Arc, Once},
};

pub(crate) mod settings;

pub(crate) const DEFAULT_MIN_IDLE_CONNECTIONS: u32 = 10;
pub(crate) const SPICE_ACCELERATOR_METADATA_KEY: &str = "spice.accelerator";
pub(crate) const SPICE_OPT_DUCKDB_AGG_PUSHDOWN_KEY: &str =
    "spice.optimizer.duckdb_aggregate_pushdown";

use super::upsert_dedup;

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
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBAccelerator {
    duckdb_factory: DuckDBTableProviderFactory,
}

impl DuckDBAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // DuckDB accelerator uses params.duckdb_file for file connection
            duckdb_factory: DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
                .with_dialect(new_duckdb_dialect())
                .with_settings_registry(
                    DuckDBSettingsRegistry::new()
                        .with_setting(Box::new(OrderByNonIntegerLiteral))
                        .with_setting(Box::new(settings::IndexScanPercentage))
                        .with_setting(Box::new(settings::IndexScanMaxCount))
                        .with_setting(Box::new(settings::TimeZone)),
                )
                .with_function_support(deny_spice_specific_functions()),
        }
    }

    /// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` accelerator from this dataset
    pub fn duckdb_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        duckdb_file_path(&self.duckdb_factory, source, "accelerated_duckdb")
    }

    /// Returns an existing `DuckDB` connection pool for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<DuckDbConnectionPool> {
        let duckdb_file = self.duckdb_file_path(source);

        let acceleration = source.acceleration().context(AccelerationNotEnabledSnafu {
            dataset: source.name().to_string(),
        })?;

        let pool = match (duckdb_file, acceleration.mode) {
            (Ok(duckdb_file), Mode::File | Mode::FileCreate) => {
                let num_accelerating_datasets = self.get_num_accelerating_datasets(
                    Some(duckdb_file.as_str()),
                    &source.app(),
                    source.runtime(),
                );
                let max_size = Self::get_pool_max_size(num_accelerating_datasets, acceleration);
                let pool_builder = DuckDbConnectionPoolBuilder::file(&duckdb_file)
                    .with_max_size(Some(max_size))
                    .with_min_idle(Some(DEFAULT_MIN_IDLE_CONNECTIONS))
                    .with_connection_setup_query("PRAGMA enable_checkpoint_on_shutdown");
                self.duckdb_factory
                    .get_or_init_instance_with_builder(pool_builder)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
            }
            (_, Mode::Memory) => {
                let num_accelerating_datasets =
                    self.get_num_accelerating_datasets(None, &source.app(), source.runtime());
                let max_size = Self::get_pool_max_size(num_accelerating_datasets, acceleration);
                let pool_builder = DuckDbConnectionPoolBuilder::memory()
                    .with_max_size(Some(max_size))
                    .with_min_idle(Some(DEFAULT_MIN_IDLE_CONNECTIONS))
                    .with_connection_setup_query("PRAGMA enable_checkpoint_on_shutdown");
                self.duckdb_factory
                    .get_or_init_instance_with_builder(pool_builder)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
            }
            (Err(e), Mode::File | Mode::FileCreate) => {
                return Err(Error::InvalidConfiguration {
                    detail: Arc::from(e.to_string()),
                });
            }
        };

        Ok(pool)
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
                    if matches!(acceleration.mode, Mode::File | Mode::FileCreate)
                        && let Ok(file_path) = self.file_path(ds.as_ref())
                        && this_file_path == file_path
                    {
                        instance_usage += 1;
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

    fn get_pool_max_size(num_accelerating_datasets: u32, acceleration: &Acceleration) -> u32 {
        let pool_size_param = acceleration
            .params
            .get("connection_pool_size")
            .and_then(|size_str| size_str.parse::<u32>().ok());

        pool_size_param
            .unwrap_or_else(|| max(DEFAULT_MIN_IDLE_CONNECTIONS, num_accelerating_datasets))
    }
}

/// Returns the `DuckDB` file path that would be used for a file-based `DuckDB` acceleration for this acceleration source
///
/// # Parameters
///
/// * `duckdb_factory` - The `DuckDB` table provider factory used to generate the file path
/// * `source` - The acceleration source (dataset or view) containing acceleration configuration
/// * `default_db_name` - Default database file name to use if the `duckdb_file` parameter is not specified
pub fn duckdb_file_path(
    duckdb_factory: &DuckDBTableProviderFactory,
    source: &dyn AccelerationSource,
    default_db_name: &str,
) -> Result<String> {
    if !source.is_file_accelerated() {
        Err(Error::InvalidConfiguration {
            detail: Arc::from("Dataset is not file accelerated"),
        })
    } else if let Some(acceleration) = source.acceleration().as_ref() {
        let mut params = acceleration.params.clone();
        let mut using_duckdb_data_dir = true;
        let data_directory = params.remove("duckdb_data_dir").unwrap_or_else(|| {
            using_duckdb_data_dir = false;
            spice_data_base_path()
        });
        params.insert("data_directory".to_string(), data_directory);

        if let Some(duckdb_file) = params.remove("duckdb_file") {
            if using_duckdb_data_dir {
                static WARN_ONCE: Once = Once::new();
                WARN_ONCE.call_once(|| {
                    tracing::warn!(
                        "'duckdb_data_dir' and 'duckdb_file' were both specified but 'duckdb_file' ({duckdb_file}) will be used."
                    );
                });
            }
            params.insert("duckdb_open".to_string(), duckdb_file);
        }

        duckdb_factory
            .duckdb_file_path(default_db_name, &mut params)
            .map_err(|err| Error::InvalidConfiguration {
                detail: Arc::from(err.to_string()),
            })
    } else {
        unreachable!("Expected dataset to have acceleration parameters, but none were found")
    }
}

impl Default for DuckDBAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::component("file"),
    ParameterSpec::component("data_dir"),
    ParameterSpec::component("memory_limit"),
    ParameterSpec::component("preserve_insertion_order"),
    ParameterSpec::component("index_scan_percentage"),
    ParameterSpec::component("index_scan_max_count"),
    ParameterSpec::runtime("partition_mode"),
    ParameterSpec::component("partitioned_write_flush_threshold"),
    ParameterSpec::runtime("connection_pool_size").description(
        "The maximum number of client connections created in the duckdb connection pool.",
    ),
    ParameterSpec::runtime("on_refresh_recompute_statistics"),
    ParameterSpec::runtime("partitioned_write_buffer"),
    ParameterSpec::runtime("optimizer_duckdb_aggregate_pushdown"),
];

#[async_trait]
impl DataAccelerator for DuckDBAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["db", "ddb", "duckdb"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.duckdb_file_path(source)
            .map_err(|e| FilePathError::External {
                engine: Engine::DuckDB,
                source: e.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode DuckDB is always initialized
        }

        // otherwise, we're initialized if the file exists
        self.has_existing_file(source)
    }

    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Ok(BootstrapStatus::none());
        }

        let path = self.file_path(source)?;

        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("duckdb_file") {
                make_spice_data_directory().map_err(|err| {
                    Error::AccelerationInitializationFailed { source: err.into() }
                })?;
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

            // If mode is FileCreate, delete the existing file to start fresh
            if acceleration.mode == Mode::FileCreate {
                let file_path = std::path::Path::new(&path);
                if file_path.exists() {
                    tracing::warn!(
                        "DuckDB acceleration mode is 'file_create', removing existing file: {}",
                        path
                    );
                    std::fs::remove_file(file_path).map_err(|err| {
                        Error::AccelerationInitializationFailed { source: err.into() }
                    })?;
                }
            }

            let bootstrap_status = download_snapshot_if_needed(
                acceleration,
                source,
                runtime_acceleration::snapshot::SnapshotAdapter::file(PathBuf::from(path)),
                AccelerationEngine::DuckDB,
            )
            .await;

            self.get_shared_pool(source).await?;

            return Ok(bootstrap_status);
        }

        Ok(BootstrapStatus::none())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        _partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(duckdb_file) = cmd.options.remove("file") {
            cmd.options.insert("open".to_string(), duckdb_file);
        }

        if let Some(recompute_statistics_on_write) =
            cmd.options.remove("on_refresh_recompute_statistics")
        {
            // Translate Spice parameter to DuckDB write setting
            cmd.options.insert(
                "recompute_statistics_on_write".to_string(),
                recompute_statistics_on_write,
            );
        }

        // Modify the `cmd` by adding options to attach other databases
        if let Some(source) = source {
            if let Some(temp_directory) = source
                .app()
                .runtime
                .query
                .clone()
                .unwrap_or_default()
                .temp_directory
            {
                cmd.options
                    .insert("temp_directory".to_string(), temp_directory);
            }

            if source.is_file_accelerated() {
                // If the user didn't specify a DuckDB file and this is a file-mode DuckDB,
                // then use the shared DuckDB file `accelerated_duckdb.db`
                if !cmd.options.contains_key("open") {
                    let duckdb_file = self.duckdb_file_path(source)?;
                    cmd.options.insert("open".to_string(), duckdb_file);
                }

                let datasets: Vec<Arc<Dataset>> = Arc::clone(&source.runtime())
                    .get_initialized_datasets(&source.app(), crate::LogErrors(false))
                    .await;

                let views: Vec<Arc<View>> = Arc::clone(&source.runtime())
                    .get_initialized_views(&source.app(), crate::LogErrors(false))
                    .await;

                let self_path = self.file_path(source)?;
                let attach_databases = datasets
                    .into_iter()
                    .map(|ds| ds as Arc<dyn AccelerationSource>)
                    .chain(
                        views
                            .into_iter()
                            .map(|view| view as Arc<dyn AccelerationSource>),
                    )
                    .filter_map(|other_source| {
                        if other_source.acceleration().is_some_and(|a| {
                            a.engine == Engine::DuckDB
                                && matches!(a.mode, Mode::File | Mode::FileCreate)
                        }) {
                            if other_source.name() == source.name() {
                                None
                            } else {
                                let other_path = self.file_path(other_source.as_ref());
                                other_path.ok().filter(|p| p != &self_path)
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<HashSet<_>>(); // collect unique paths using HashSet

                if !attach_databases.is_empty() {
                    cmd.options.insert(
                        "attach_databases".to_string(),
                        attach_databases.iter().join(";"),
                    );
                }
            }
        }

        let write_completion_handler = source.and_then(|src| {
            let retention_sql = src
                .acceleration()
                .and_then(|acc| acc.retention_sql.as_deref())
                .map(str::trim)
                .filter(|sql| !sql.is_empty())?
                .to_string();

            let dataset_name = src.name().to_string();
            let schema = Arc::new(cmd.schema.as_arrow().clone());

            match crate::datafusion::retention_sql::parse_retention_sql(
                src.name(),
                &retention_sql,
                schema,
            ) {
                Ok(parsed_sql) => Some(make_retention_write_handler(
                    dataset_name,
                    parsed_sql.delete_statement,
                )),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse retention_sql for dataset {}: {}. Retention SQL will not be applied.",
                        dataset_name, e
                    );
                    None
                }
            }
        });

        Ok(create_table_provider(&self.duckdb_factory, &cmd, write_completion_handler).await?)
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

pub(crate) async fn create_table_provider(
    duckdb_factory: &DuckDBTableProviderFactory,
    cmd: &CreateExternalTable,
    on_data_written: Option<WriteCompletionHandler>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let ctx = SessionContext::new();

    let table_provider = duckdb_factory
        .create(&ctx.state(), cmd)
        .await
        .context(UnableToCreateTableSnafu)
        .boxed()?;

    let Some(duckdb_writer) = table_provider.as_any().downcast_ref::<DuckDBTableWriter>() else {
        unreachable!("DuckDBTableWriter should be returned from DuckDBTableProviderFactory")
    };

    let read_provider = Arc::clone(&duckdb_writer.read_provider);
    let duckdb_writer: Arc<DuckDBTableWriter> = match on_data_written {
        Some(handler) => Arc::new(duckdb_writer.clone().with_on_data_written_handler(handler)),
        None => Arc::new(duckdb_writer.clone()),
    };

    // Wrap with upsert deduplication if needed
    let (write_provider, delete_provider) =
        upsert_dedup::wrap_with_upsert_dedup_if_needed(duckdb_writer, &cmd.options);

    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        SPICE_ACCELERATOR_METADATA_KEY.to_string(),
        "duckdb".to_string(),
    );

    let agg_pushdown_optimization = cmd
        .options
        .get("optimizer_duckdb_aggregate_pushdown")
        .map_or("disabled", |v| v.as_str())
        .to_lowercase();

    schema_metadata.insert(
        SPICE_OPT_DUCKDB_AGG_PUSHDOWN_KEY.to_string(),
        agg_pushdown_optimization,
    );

    let table_provider = Arc::new(PolyTableProvider::new_with_schema_metadata(
        write_provider,
        delete_provider,
        read_provider,
        schema_metadata,
    ));

    Ok(table_provider)
}

/// Reconstruct the DELETE statement with the internal `DuckDB` table name.
fn reconstruct_retention_sql_with_table_name(
    delete: &Delete,
    internal_table_name: &str,
) -> Result<String, String> {
    // Clone the delete statement and modify the table name
    let mut modified_delete = delete.clone();

    // Replace the table name with the internal table name
    // DuckDB internal table names should be used as-is without schema qualification
    let FromTable::WithFromKeyword(from_tables) = &mut modified_delete.from else {
        return Err("DELETE statement must use FROM keyword".to_string());
    };

    // Replace the first table's name, keeping all other properties
    let Some(table_relation) = from_tables.first_mut() else {
        return Err("No table specified in DELETE statement".to_string());
    };

    if let TableFactor::Table { name, .. } = &mut table_relation.relation {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            internal_table_name,
        ))]);
    } else {
        return Err("DELETE statement must reference a simple table".to_string());
    }

    // Simply convert the AST to string using Display trait
    let statement = SQLStatement::Delete(modified_delete);
    Ok(statement.to_string())
}

fn make_retention_write_handler(
    dataset_name: String,
    parsed_delete: Delete,
) -> WriteCompletionHandler {
    Arc::new(move |tx, table_manager, _schema, inserted_rows| {
        let internal_table_name = table_manager.table_name().to_string();

        tracing::debug!(
            dataset = %dataset_name,
            table = %internal_table_name,
            inserted_rows,
            "Applying retention SQL before commit"
        );

        // Reconstruct the SQL with the internal table name
        let reconstructed_sql =
            match reconstruct_retention_sql_with_table_name(&parsed_delete, &internal_table_name) {
                Ok(sql) => sql,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to reconstruct retention SQL for dataset {dataset_name}: {e}"
                    )));
                }
            };

        tracing::debug!(
            dataset = %dataset_name,
            table = %internal_table_name,
            sql = %reconstructed_sql,
            "Reconstructed retention SQL with internal table name"
        );

        match tx.execute(reconstructed_sql.as_str(), []) {
            Ok(affected_rows) => {
                tracing::debug!(
                    dataset = %dataset_name,
                    table = %internal_table_name,
                    affected_rows,
                    "Retention SQL applied before commit"
                );
                Ok(())
            }
            Err(err) => Err(DataFusionError::Execution(format!(
                "Failed to apply retention SQL for dataset {dataset_name} (table {internal_table_name}): {err}"
            ))),
        }
    })
}

register_data_accelerator!(Engine::DuckDB, DuckDBAccelerator);

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::component::dataset::builder::DatasetBuilder;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    };
    use data_components::delete::get_deletion_provider;
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{CreateExternalTable, cast, col, dml::InsertOp, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;

    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::acceleration::{Engine, Mode};
    use crate::dataaccelerator::{DataAccelerator, duckdb::DuckDBAccelerator};

    #[tokio::test]
    async fn retention_sql_applies_before_commit() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("retention_table"),
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

        let duckdb_accelerator = DuckDBAccelerator::new();
        let retention_sql = "DELETE FROM retention_table WHERE value < 5";
        let parsed_delete = crate::datafusion::retention_sql::parse_retention_sql(
            &TableReference::bare("retention_table"),
            retention_sql,
            Arc::clone(&schema),
        )
        .expect("should parse retention SQL")
        .delete_statement;
        let handler =
            super::make_retention_write_handler("retention_dataset".to_string(), parsed_delete);

        let table = super::create_table_provider(
            &duckdb_accelerator.duckdb_factory,
            &external_table,
            Some(handler),
        )
        .await
        .expect("table should be created");

        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 3, 5, 7]))],
        )
        .expect("record batch");

        let exec = Arc::new(MockExec::new(vec![Ok(input)], schema));

        let write_ctx = SessionContext::new();
        let insert_plan = table
            .insert_into(
                &write_ctx.state(),
                Arc::<MockExec>::clone(&exec),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");

        collect(insert_plan, write_ctx.task_ctx())
            .await
            .expect("insert succeeds");

        let read_ctx = SessionContext::new();
        let scan_plan = table
            .scan(&read_ctx.state(), None, &[], None)
            .await
            .expect("scan plan");

        let batches = collect(scan_plan, read_ctx.task_ctx())
            .await
            .expect("scan succeeds");

        let mut values = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column");
            values.extend((0..column.len()).map(|idx| column.value(idx)));
        }

        assert_eq!(values, vec![5, 7]);
    }

    #[tokio::test]
    async fn retention_sql_fails_with_internal_tables() {
        use datafusion_table_providers::duckdb::DuckDB;
        use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
        use tempfile::TempDir;

        // This test reproduces the bug where retention SQL fails with internal tables.
        //
        // When DuckDB uses internal tables (for indexes/constraints via preserve_insertion_order),
        // the write completion handler receives the internal table name (like __data_table_123)
        // from table_manager.table_name(), but the retention SQL references the logical table name.
        //
        // DuckDB's error: "Can only delete from base table!" occurs because DELETE statements
        // must target the base/view table name, not the internal table directly.

        let temp_dir = TempDir::new().expect("create temp dir");
        let db_path = temp_dir.path().join("test_retention.db");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let mut options = HashMap::new();
        // Use file mode to enable full DuckDB features
        options.insert(
            "open".to_string(),
            db_path.to_str().expect("path").to_string(),
        );
        // Enable preserve_insertion_order which triggers internal table creation
        options.insert("preserve_insertion_order".to_string(), "true".to_string());

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("taxi_trips"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let duckdb_accelerator = DuckDBAccelerator::new();

        // The retention SQL references the logical table name "taxi_trips"
        let retention_sql = "DELETE FROM taxi_trips WHERE value < 5";
        let parsed_delete = crate::datafusion::retention_sql::parse_retention_sql(
            &TableReference::bare("taxi_trips"),
            retention_sql,
            Arc::clone(&schema),
        )
        .expect("should parse retention SQL")
        .delete_statement;
        let handler = super::make_retention_write_handler("taxi_trips".to_string(), parsed_delete);

        let table = super::create_table_provider(
            &duckdb_accelerator.duckdb_factory,
            &external_table,
            Some(handler),
        )
        .await
        .expect("table should be created");

        // Insert initial data
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![1, 3, 5, 7])),
            ],
        )
        .expect("record batch");

        let exec = Arc::new(MockExec::new(vec![Ok(input.clone())], Arc::clone(&schema)));

        let write_ctx = SessionContext::new();
        let insert_plan = table
            .insert_into(
                &write_ctx.state(),
                Arc::<MockExec>::clone(&exec),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");

        // First insert should succeed
        collect(insert_plan, write_ctx.task_ctx())
            .await
            .expect("first insert should succeed");

        // Verify internal tables were created by checking DuckDB directly
        let pool = Arc::new(
            DuckDbConnectionPool::new_file(
                db_path.to_str().expect("path"),
                &duckdb::AccessMode::ReadWrite,
            )
            .expect("create pool"),
        );

        let mut conn = pool.connect_sync().expect("connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("get duckdb conn");

        // Check for internal tables (they follow the pattern __data_*)
        let internal_tables: Vec<String> = duckdb_conn
            .get_underlying_conn_mut()
            .prepare(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '__data_%'",
            )
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");

        if internal_tables.is_empty() {
            eprintln!(
                "WARNING: No internal tables found. Test may not be validating the bug correctly."
            );
            eprintln!(
                "This could mean preserve_insertion_order didn't trigger internal table creation."
            );
            return;
        }

        eprintln!("Found internal tables: {internal_tables:?}");

        // Now try to insert more data - this should trigger the retention SQL
        // and fail because it tries to DELETE from the internal table name
        let exec2 = Arc::new(MockExec::new(vec![Ok(input)], schema));

        let insert_plan2 = table
            .insert_into(&write_ctx.state(), exec2, InsertOp::Append)
            .await
            .expect("insert plan");

        let result = collect(insert_plan2, write_ctx.task_ctx()).await;

        // This should fail with "Can only delete from base table!"
        assert!(
            result.is_err(),
            "Expected an error due to retention SQL targeting internal table, but insert succeeded"
        );

        let error_msg = result.expect_err("Expected error").to_string();
        assert!(
            error_msg.contains("Can only delete from base table")
                || error_msg.contains("Binder Error")
                || error_msg.contains("Failed to apply retention SQL"),
            "Expected error about deleting from base table, got: {error_msg}"
        );

        eprintln!("✓ Test correctly reproduced the error: {error_msg}");
    }

    #[tokio::test]
    #[expect(clippy::unreadable_literal)]
    async fn test_round_trip_duckdb() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            ),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
            arrow::datatypes::Field::new(
                "time_with_zone",
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Second,
                    Some("Etc/UTC".to_string().into()),
                ),
                false,
            ),
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
        let duckdb_accelerator = DuckDBAccelerator::new();
        let ctx = SessionContext::new();
        let table = duckdb_accelerator
            .create_external_table(external_table, None, vec![])
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr2 = TimestampSecondArray::from(vec![0, 1354360271, 1354360272]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let arr4 = arrow::compute::cast(
            &arr2,
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Second,
                Some("Etc/UTC".to_string().into()),
            ),
        )
        .expect("casting works");
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arr1),
                Arc::new(arr2),
                Arc::new(arr3),
                Arc::new(arr4),
            ],
        )
        .expect("data should be created");

        let exec = Arc::new(MockExec::new(vec![Ok(data)], schema));

        let insertion = table
            .insert_into(
                &ctx.state(),
                Arc::<MockExec>::clone(&exec),
                InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletion provider");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = delete_table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
        let plan = delete_table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![1]);
        assert_eq!(actual, &expected);

        let insertion = table
            .insert_into(
                &ctx.state(),
                Arc::<MockExec>::clone(&exec),
                InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletion provider");

        let filter = col("time").lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = delete_table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);

        let insertion = table
            .insert_into(&ctx.state(), exec, InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let delete_table = get_deletion_provider(Arc::clone(&table))
            .expect("table should be returned as deletion provider");

        let filter = col("time_with_zone").lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = delete_table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);
    }

    #[tokio::test]
    async fn test_duckdb_file_initialization() {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "duckdb_file_accelerator_init".to_string(),
            "duckdb_file_accelerator_init",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::DuckDB,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = DuckDBAccelerator::new();
        assert!(!accelerator.is_initialized(&dataset));

        accelerator
            .init(&dataset)
            .await
            .expect("initialization should be successful");

        assert!(accelerator.is_initialized(&dataset));

        let path = accelerator.file_path(&dataset).expect("path should exist");
        assert!(std::path::Path::new(&path).exists());

        // cleanup
        std::fs::remove_file(&path).expect("file should be removed");
    }

    #[tokio::test]
    async fn test_retention_sql_with_duckdb_accelerator() {
        use tempfile::TempDir;

        // Create a temporary directory for the DuckDB file
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_retention.db");

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        // Prepare the external table command with file path
        let mut external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("retention_test_dataset"),
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
        external_table.options.insert(
            "open".to_string(),
            db_path.to_str().expect("path").to_string(),
        );

        // Parse retention SQL and create handler
        let retention_sql = "DELETE FROM retention_test_dataset WHERE value < 5";
        let parsed_delete = crate::datafusion::retention_sql::parse_retention_sql(
            &TableReference::bare("retention_test_dataset"),
            retention_sql,
            Arc::clone(&schema),
        )
        .expect("should parse retention SQL")
        .delete_statement;
        let handler = super::make_retention_write_handler(
            "retention_test_dataset".to_string(),
            parsed_delete,
        );

        // Create the accelerator and table
        let accelerator = DuckDBAccelerator::new();
        let table = super::create_table_provider(
            &accelerator.duckdb_factory,
            &external_table,
            Some(handler),
        )
        .await
        .expect("table should be created");

        // Insert initial data with values both above and below the retention threshold
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Int64Array::from(vec![2, 3, 4, 6, 7, 8])), // values: 2, 3, 4 should be deleted (< 5)
            ],
        )
        .expect("record batch");

        let exec = Arc::new(MockExec::new(vec![Ok(input.clone())], Arc::clone(&schema)));

        let write_ctx = SessionContext::new();
        let insert_plan = table
            .insert_into(
                &write_ctx.state(),
                Arc::<MockExec>::clone(&exec),
                InsertOp::Append,
            )
            .await
            .expect("insert plan");

        // Execute the insert - this should trigger the retention SQL
        collect(insert_plan, write_ctx.task_ctx())
            .await
            .expect("insert should succeed");

        // Query the table to verify retention SQL was applied
        let read_ctx = SessionContext::new();
        let scan = table
            .scan(&read_ctx.state(), None, &[], None)
            .await
            .expect("scan should succeed");

        let results = collect(scan, read_ctx.task_ctx())
            .await
            .expect("collect should succeed");

        // Verify that only rows with value >= 5 remain
        let mut total_rows = 0;
        let mut values = Vec::new();
        for batch in &results {
            total_rows += batch.num_rows();
            let value_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column should be Int64Array");

            // Collect all values
            for i in 0..value_array.len() {
                values.push(value_array.value(i));
            }
        }

        // All values should be >= 5
        for value in &values {
            assert!(
                *value >= 5,
                "Found value {value} which should have been deleted by retention SQL"
            );
        }

        // We should have 3 rows remaining (values 6, 7, 8)
        assert_eq!(
            total_rows, 3,
            "Expected 3 rows after retention (values >= 5), found {total_rows}. Values: {values:?}"
        );

        // cleanup
        drop(table);
        drop(temp_dir);
    }

    #[tokio::test]
    async fn test_reconstruct_retention_sql() {
        let sql = "DELETE FROM taxi_trips WHERE status = 'expired'";
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let parsed = crate::datafusion::retention_sql::parse_retention_sql(
            &TableReference::bare("taxi_trips"),
            sql,
            schema,
        )
        .expect("should parse");

        let internal_name = "__data_taxi_trips_1234567890";
        let result = super::reconstruct_retention_sql_with_table_name(
            &parsed.delete_statement,
            internal_name,
        );

        assert!(result.is_ok(), "Should reconstruct SQL successfully");
        let reconstructed = result.expect("reconstructed");

        // Verify the internal table name is used
        assert!(
            reconstructed.contains(internal_name),
            "Should contain internal table name"
        );

        // Verify the WHERE clause is preserved
        assert!(
            reconstructed.contains("status = 'expired'")
                || reconstructed.contains("status = \"expired\""),
            "Should preserve WHERE clause"
        );

        // Verify it's still a DELETE statement
        assert!(
            reconstructed.to_lowercase().starts_with("delete from"),
            "Should start with DELETE FROM"
        );
    }

    #[tokio::test]
    async fn test_reconstruct_retention_sql_complex_where() {
        let sql = "DELETE FROM orders WHERE created_at < NOW() - INTERVAL '30 days' AND status IN ('cancelled', 'expired')";
        let schema = Arc::new(Schema::new(vec![
            Field::new("created_at", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        let parsed = crate::datafusion::retention_sql::parse_retention_sql(
            &TableReference::bare("orders"),
            sql,
            schema,
        )
        .expect("should parse");

        let internal_name = "__data_orders_9876543210";
        let result = super::reconstruct_retention_sql_with_table_name(
            &parsed.delete_statement,
            internal_name,
        );

        assert!(
            result.is_ok(),
            "Should reconstruct complex SQL successfully"
        );
        let reconstructed = result.expect("reconstructed");

        // Verify the internal table name is used
        assert!(
            reconstructed.contains(internal_name),
            "Should contain internal table name"
        );

        // Basic sanity check - make sure it's still a valid DELETE statement structure
        assert!(
            reconstructed.to_lowercase().starts_with("delete from"),
            "Should start with DELETE FROM"
        );
    }
}
