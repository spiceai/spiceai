/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::accelerated_table::{refresh::Refresh, AcceleratedTable, Retention};
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::{Dataset, Mode};
use crate::dataaccelerator::{self, create_accelerator_table};
use crate::dataconnector::{DataConnector, DataConnectorError};
use crate::dataupdate::{
    DataUpdate, StreamingDataUpdate, StreamingDataUpdateExecutionPlan, UpdateType,
};
use crate::object_store_registry::default_runtime_env;
use crate::secrets::Secret;
use crate::{embeddings, get_dependent_table_names};

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow_tools::schema::verify_schema;
use cache::QueryResultsCacheProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::physical_plan::collect;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{sqlparser, TableReference};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use query::{Protocol, QueryBuilder};
use snafu::prelude::*;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::time::{sleep, Instant};

pub mod query;

pub mod filter_converter;
pub mod initial_load;
pub mod refresh_sql;
pub mod schema;
pub mod udf;

use self::schema::SpiceSchemaProvider;

pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_RUNTIME_SCHEMA: &str = "runtime";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";
pub const SPICE_METADATA_SCHEMA: &str = "metadata";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table already exists"))]
    TableAlreadyExists {},

    #[snafu(display("Unable to create dataset acceleration: {source}"))]
    UnableToCreateDataAccelerator { source: dataaccelerator::Error },

    #[snafu(display("Unable to create view: {reason}"))]
    UnableToCreateView { reason: String },

    #[snafu(display("Unable to delete table: {reason}"))]
    UnableToDeleteTable { reason: String },

    #[snafu(display("Unable to parse SQL: {source}"))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("{source}"))]
    RefreshSql { source: refresh_sql::Error },

    #[snafu(display("Unable to get table: {source}"))]
    UnableToGetTable { source: DataFusionError },

    #[snafu(display("Unable to list tables: {source}"))]
    UnableToGetTables { source: DataFusionError },

    #[snafu(display("Unable to resolve table provider: {source}"))]
    UnableToResolveTableProvider { source: DataConnectorError },

    #[snafu(display("Table {table_name} was marked as read_write, but the underlying provider only supports reads."))]
    WriteProviderNotImplemented { table_name: String },

    #[snafu(display("Table {table_name} is expected to provide metadata, but the underlying provider does not support this."))]
    MetadataProviderNotImplemented { table_name: String },

    #[snafu(display("Unable to register table: {source}"))]
    UnableToRegisterTable { source: crate::dataconnector::Error },

    #[snafu(display("Unable to register table in DataFusion: {source}"))]
    UnableToRegisterTableToDataFusion { source: DataFusionError },

    #[snafu(display("Unable to register {schema} table in DataFusion: {source}"))]
    UnableToRegisterTableToDataFusionSchema {
        schema: String,
        source: DataFusionError,
    },

    #[snafu(display("Expected acceleration settings for {name}, found None"))]
    ExpectedAccelerationSettings { name: String },

    #[snafu(display("Unable to get object store configuration: {source}"))]
    InvalidObjectStore {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("The table {table_name} is not writable"))]
    TableNotWritable { table_name: String },

    #[snafu(display("Unable to plan the table insert for {table_name}: {source}"))]
    UnableToPlanTableInsert {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Unable to execute the table insert for {table_name}: {source}"))]
    UnableToExecuteTableInsert {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Unable to trigger refresh for {table_name}: {source}"))]
    UnableToTriggerRefresh {
        table_name: String,
        source: crate::accelerated_table::Error,
    },

    #[snafu(display("Table {table_name} is not accelerated"))]
    NotAcceleratedTable { table_name: String },

    #[snafu(display("Schema mismatch: {source}"))]
    SchemaMismatch { source: arrow_tools::schema::Error },

    #[snafu(display("The catalog {catalog} is not registered."))]
    CatalogMissing { catalog: String },

    #[snafu(display("The schema {schema} is not registered."))]
    SchemaMissing { schema: String },

    #[snafu(display("Unable to get {schema} schema: {source}"))]
    UnableToGetSchema {
        schema: String,
        source: DataFusionError,
    },

    #[snafu(display("Table {schema}.{table} not registered"))]
    TableMissing { schema: String, table: String },

    #[snafu(display("Catalog already exists: {catalog}"))]
    CatalogAlreadyExists { catalog: String },

    #[snafu(display("Unable to get object store configuration: {source}"))]
    UnableToGetSchemaTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get the lock of data writers"))]
    UnableToLockDataWriters {},

    #[snafu(display("The schema returned by the data connector for 'refresh_mode: changes' does not contain a data field"))]
    ChangeSchemaWithoutDataField { source: ArrowError },

    #[snafu(display("Unable to create streaming data update: {source}"))]
    UnableToCreateStreamingUpdate {
        source: datafusion::error::DataFusionError,
    },
}

pub enum Table {
    Accelerated {
        source: Arc<dyn DataConnector>,
        federated_read_table: Arc<dyn TableProvider>,
        acceleration_secret: Option<Secret>,
        accelerated_table: Option<AcceleratedTable>,
    },
    Federated {
        data_connector: Arc<dyn DataConnector>,
        federated_read_table: Arc<dyn TableProvider>,
    },
    View(String),
}

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    data_writers: RwLock<HashSet<TableReference>>,
    cache_provider: RwLock<Option<Arc<QueryResultsCacheProvider>>>,

    /// Has the initial load of the data been completed? It is the responsibility of the caller to call `mark_initial_load_complete` when the initial load is complete.
    initial_load_complete: Mutex<bool>,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_cache_provider(None)
    }

    /// Create a new `DataFusion` instance.
    ///
    /// # Panics
    ///
    /// Panics if the default schema cannot be registered.
    #[must_use]
    pub fn new_with_cache_provider(cache_provider: Option<Arc<QueryResultsCacheProvider>>) -> Self {
        let mut df_config = SessionConfig::new()
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(false)
            .set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            );

        df_config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
        df_config.options_mut().catalog.default_catalog = SPICE_DEFAULT_CATALOG.to_string();
        df_config.options_mut().catalog.default_schema = SPICE_DEFAULT_SCHEMA.to_string();

        let state = SessionState::new_with_config_rt(df_config, default_runtime_env())
            .add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()));

        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(embeddings::array_distance::ArrayDistance::new().into());
        ctx.register_udf(crate::datafusion::udf::Greatest::new().into());
        ctx.register_udf(crate::datafusion::udf::Least::new().into());
        let catalog = MemoryCatalogProvider::new();
        let default_schema = SpiceSchemaProvider::new();
        let runtime_schema = SpiceSchemaProvider::new();
        let metadata_schema = SpiceSchemaProvider::new();

        match catalog.register_schema(SPICE_DEFAULT_SCHEMA, Arc::new(default_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register default schema: {e}");
            }
        }

        match catalog.register_schema(SPICE_RUNTIME_SCHEMA, Arc::new(runtime_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice runtime schema: {e}");
            }
        }

        match catalog.register_schema(SPICE_METADATA_SCHEMA, Arc::new(metadata_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice runtime schema: {e}");
            }
        }

        ctx.register_catalog(SPICE_DEFAULT_CATALOG, Arc::new(catalog));

        DataFusion {
            ctx: Arc::new(ctx),
            data_writers: RwLock::new(HashSet::new()),
            cache_provider: RwLock::new(cache_provider),
            initial_load_complete: Mutex::new(false),
        }
    }

    #[must_use]
    fn runtime_schema(&self) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(catalog) = self.ctx.catalog(SPICE_DEFAULT_CATALOG) {
            return catalog.schema(SPICE_RUNTIME_SCHEMA);
        }

        None
    }

    #[must_use]
    fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(catalog) = self.ctx.catalog(SPICE_DEFAULT_CATALOG) {
            return catalog.schema(schema_name);
        }

        None
    }

    pub fn set_cache_provider(&self, cache_provider: QueryResultsCacheProvider) {
        if let Ok(mut a) = self.cache_provider.write() {
            *a = Some(Arc::new(cache_provider));
        };
    }

    pub async fn has_table(&self, table_reference: &TableReference) -> bool {
        let table_name = table_reference.table();

        if let Some(schema_name) = table_reference.schema() {
            if let Some(schema) = self.schema(schema_name) {
                return match schema.table(table_name).await {
                    Ok(table) => table.is_some(),
                    Err(_) => false,
                };
            }
        }

        self.ctx.table(table_name).await.is_ok()
    }

    pub async fn get_table(
        &self,
        table_reference: TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        self.ctx.table_provider(table_reference).await.ok()
    }

    pub fn register_runtime_table(
        &self,
        table_name: TableReference,
        table: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        if let Some(runtime_schema) = self.runtime_schema() {
            runtime_schema
                .register_table(table_name.table().to_string(), table)
                .context(UnableToRegisterTableToDataFusionSchemaSnafu { schema: "runtime" })?;

            self.data_writers
                .write()
                .map_err(|_| Error::UnableToLockDataWriters {})?
                .insert(table_name);
        }

        Ok(())
    }

    pub fn register_catalog(&self, name: &str, catalog: Arc<dyn CatalogProvider>) -> Result<()> {
        if self.ctx.catalog(name).is_some() {
            CatalogAlreadyExistsSnafu {
                catalog: name.to_string(),
            }
            .fail()?;
        }

        self.ctx.register_catalog(name, catalog);

        Ok(())
    }

    pub async fn register_table(&self, dataset: impl Borrow<Dataset>, table: Table) -> Result<()> {
        let dataset = dataset.borrow();

        schema::ensure_schema_exists(&self.ctx, SPICE_DEFAULT_CATALOG, &dataset.name)?;

        match table {
            Table::Accelerated {
                source,
                federated_read_table,
                acceleration_secret,
                accelerated_table,
            } => {
                if let Some(accelerated_table) = accelerated_table {
                    tracing::debug!(
                        "Registering dataset {dataset:?} with preloaded accelerated table"
                    );

                    self.ctx
                        .register_table(dataset.name.clone(), Arc::new(accelerated_table))
                        .context(UnableToRegisterTableToDataFusionSnafu)?;

                    return Ok(());
                }
                self.register_accelerated_table(
                    dataset,
                    source,
                    federated_read_table,
                    acceleration_secret,
                )
                .await?;
            }
            Table::Federated {
                data_connector,
                federated_read_table,
            } => {
                self.register_federated_table(dataset, data_connector, federated_read_table)
                    .await?;
            }
            Table::View(sql) => self.register_view(dataset.name.clone(), sql)?,
        }

        if matches!(dataset.mode(), Mode::ReadWrite) {
            self.data_writers
                .write()
                .map_err(|_| Error::UnableToLockDataWriters {})?
                .insert(dataset.name.clone());
        }

        Ok(())
    }

    #[must_use]
    pub fn is_writable(&self, table_reference: &TableReference) -> bool {
        if let Ok(writers) = self.data_writers.read() {
            writers.iter().any(|s| s == table_reference)
        } else {
            false
        }
    }

    async fn get_table_provider(
        &self,
        table_reference: &TableReference,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_reference.table();

        if let Some(schema_name) = table_reference.schema() {
            if let Some(schema) = self.schema(schema_name) {
                let table_provider = schema
                    .table(table_name)
                    .await
                    .context(UnableToGetTableSnafu)?
                    .ok_or_else(|| {
                        TableMissingSnafu {
                            schema: schema_name.to_string(),
                            table: table_name.to_string(),
                        }
                        .build()
                    })
                    .boxed()
                    .context(UnableToGetSchemaTableSnafu)?;

                return Ok(table_provider);
            }
        }

        let table_provider = self
            .ctx
            .table_provider(TableReference::bare(table_name.to_string()))
            .await
            .context(UnableToGetTableSnafu)?;

        Ok(table_provider)
    }

    pub async fn write_data(
        &self,
        table_reference: TableReference,
        data_update: DataUpdate,
    ) -> Result<()> {
        if !self.is_writable(&table_reference) {
            TableNotWritableSnafu {
                table_name: table_reference.to_string(),
            }
            .fail()?;
        }

        let table_provider = self.get_table_provider(&table_reference).await?;

        verify_schema(
            table_provider.schema().fields(),
            data_update.schema.fields(),
        )
        .context(SchemaMismatchSnafu)?;

        let overwrite = data_update.update_type == UpdateType::Overwrite;

        let streaming_update = StreamingDataUpdate::try_from(data_update)
            .context(UnableToCreateStreamingUpdateSnafu)?;

        let insert_plan = table_provider
            .insert_into(
                &self.ctx.state(),
                Arc::new(StreamingDataUpdateExecutionPlan::new(streaming_update.data)),
                overwrite,
            )
            .await
            .context(UnableToPlanTableInsertSnafu {
                table_name: table_reference.to_string(),
            })?;

        let _ = collect(insert_plan, self.ctx.task_ctx()).await.context(
            UnableToExecuteTableInsertSnafu {
                table_name: table_reference.to_string(),
            },
        )?;

        Ok(())
    }

    pub async fn get_arrow_schema(&self, dataset: &str) -> Result<Schema> {
        let data_frame = self
            .ctx
            .table(dataset)
            .await
            .context(UnableToGetTableSnafu)?;
        Ok(Schema::from(data_frame.schema()))
    }

    #[must_use]
    pub fn table_exists(&self, dataset_name: TableReference) -> bool {
        self.ctx.table_exist(dataset_name).unwrap_or(false)
    }

    pub fn remove_table(&self, dataset_name: &TableReference) -> Result<()> {
        if !self.ctx.table_exist(dataset_name.clone()).unwrap_or(false) {
            return Ok(());
        }

        if let Err(e) = self.ctx.deregister_table(dataset_name.clone()) {
            return UnableToDeleteTableSnafu {
                reason: e.to_string(),
            }
            .fail();
        }

        if self.is_writable(dataset_name) {
            self.data_writers
                .write()
                .map_err(|_| Error::UnableToLockDataWriters {})?
                .remove(dataset_name);
        }

        Ok(())
    }

    pub async fn create_accelerated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        federated_read_table: Arc<dyn TableProvider>,
        acceleration_secret: Option<Secret>,
    ) -> Result<(AcceleratedTable, oneshot::Receiver<()>)> {
        tracing::debug!("Creating accelerated table {dataset:?}");
        let source_table_provider = match dataset.mode() {
            Mode::Read => federated_read_table,
            Mode::ReadWrite => source
                .read_write_provider(dataset)
                .await
                .ok_or_else(|| {
                    WriteProviderNotImplementedSnafu {
                        table_name: dataset.name.to_string(),
                    }
                    .build()
                })?
                .context(UnableToResolveTableProviderSnafu)?,
        };

        let source_schema = source_table_provider.schema();

        let acceleration_settings =
            dataset
                .acceleration
                .clone()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: dataset.name.to_string(),
                })?;

        let accelerated_table_provider = create_accelerator_table(
            dataset.name.clone(),
            source_schema,
            source_table_provider.constraints(),
            &acceleration_settings,
            acceleration_secret,
        )
        .await
        .context(UnableToCreateDataAcceleratorSnafu)?;

        let refresh_sql = dataset.refresh_sql();
        if let Some(refresh_sql) = &refresh_sql {
            refresh_sql::validate_refresh_sql(dataset.name.clone(), refresh_sql.as_str())
                .context(RefreshSqlSnafu)?;
        }

        let refresh_mode = source.resolve_refresh_mode(acceleration_settings.refresh_mode);
        let refresh = Refresh::new(
            dataset.time_column.clone(),
            dataset.time_format,
            dataset.refresh_check_interval(),
            refresh_sql.clone(),
            refresh_mode,
            dataset.refresh_data_window(),
            acceleration_settings.refresh_append_overlap,
        )
        .with_retry(
            dataset.refresh_retry_enabled(),
            dataset.refresh_retry_max_attempts(),
        );

        let mut accelerated_table_builder = AcceleratedTable::builder(
            dataset.name.clone(),
            Arc::clone(&source_table_provider),
            accelerated_table_provider,
            refresh,
        );
        accelerated_table_builder.retention(Retention::new(
            dataset.time_column.clone(),
            dataset.time_format,
            dataset.retention_period(),
            dataset.retention_check_interval(),
            acceleration_settings.retention_check_enabled,
        ));

        accelerated_table_builder.zero_results_action(acceleration_settings.on_zero_results);

        accelerated_table_builder.cache_provider(self.cache_provider());

        if refresh_mode == RefreshMode::Changes {
            let source = Box::leak(Box::new(source));
            let changes_stream = source.changes_stream(source_table_provider);
            if let Some(changes_stream) = changes_stream {
                accelerated_table_builder.changes_stream(changes_stream);
            }
        }

        Ok(accelerated_table_builder.build().await)
    }

    pub fn cache_provider(&self) -> Option<Arc<QueryResultsCacheProvider>> {
        let Ok(provider) = self.cache_provider.read() else {
            return None;
        };

        provider.clone()
    }

    async fn register_accelerated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        federated_read_table: Arc<dyn TableProvider>,
        acceleration_secret: Option<Secret>,
    ) -> Result<()> {
        let (accelerated_table, _) = self
            .create_accelerated_table(
                dataset,
                Arc::clone(&source),
                federated_read_table,
                acceleration_secret,
            )
            .await?;

        self.ctx
            .register_table(dataset.name.clone(), Arc::new(accelerated_table))
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        self.register_metadata_table(dataset, Arc::clone(&source))
            .await?;

        Ok(())
    }

    pub async fn refresh_table(&self, dataset_name: &str) -> Result<()> {
        let table = self
            .ctx
            .table_provider(TableReference::from(dataset_name.to_string()))
            .await
            .context(UnableToGetTableSnafu)?;

        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            accelerated_table
                .trigger_refresh()
                .await
                .context(UnableToTriggerRefreshSnafu {
                    table_name: dataset_name.to_string(),
                })?;
        } else {
            NotAcceleratedTableSnafu {
                table_name: dataset_name.to_string(),
            }
            .fail()?;
        }

        Ok(())
    }

    pub async fn update_refresh_sql(
        &self,
        dataset_name: TableReference,
        refresh_sql: Option<String>,
    ) -> Result<()> {
        if let Some(sql) = &refresh_sql {
            refresh_sql::validate_refresh_sql(dataset_name.clone(), sql)
                .context(RefreshSqlSnafu)?;
        }

        let table = self
            .ctx
            .table_provider(dataset_name.clone())
            .await
            .context(UnableToGetTableSnafu)?;

        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            accelerated_table
                .update_refresh_sql(refresh_sql)
                .await
                .context(UnableToTriggerRefreshSnafu {
                    table_name: dataset_name.to_string(),
                })?;
        }
        Ok(())
    }

    /// Federated tables are attached directly as tables visible in the public `DataFusion` context.
    async fn register_federated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        federated_read_table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        tracing::debug!("Registering federated table {dataset:?}");
        let table_exists = self.ctx.table_exist(dataset.name.clone()).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let source_table_provider = match dataset.mode() {
            Mode::Read => federated_read_table,
            Mode::ReadWrite => source
                .read_write_provider(dataset)
                .await
                .ok_or_else(|| {
                    WriteProviderNotImplementedSnafu {
                        table_name: dataset.name.to_string(),
                    }
                    .build()
                })?
                .context(UnableToResolveTableProviderSnafu)?,
        };

        self.register_metadata_table(dataset, Arc::clone(&source))
            .await?;

        self.ctx
            .register_table(dataset.name.clone(), source_table_provider)
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        Ok(())
    }

    /// Register a metadata table to the `DataFusion` context if supported by the underlying data connector.
    /// For a dataset `name`, the metadata table will be under `metadata.$name`
    async fn register_metadata_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
    ) -> Result<()> {
        if let Some(table) = source
            .metadata_provider(dataset)
            .await
            .transpose()
            .context(UnableToResolveTableProviderSnafu)?
        {
            self.ctx
                .register_table(
                    TableReference::partial(SPICE_METADATA_SCHEMA, dataset.name.to_string()),
                    table,
                )
                .context(UnableToRegisterTableToDataFusionSnafu)?;
        };
        Ok(())
    }

    pub(crate) fn register_view(&self, table: TableReference, view: String) -> Result<()> {
        let table_exists = self.ctx.table_exist(table.clone()).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let statements = DFParser::parse_sql_with_dialect(view.as_str(), &PostgreSqlDialect {})
            .context(UnableToParseSqlSnafu)?;
        if statements.len() != 1 {
            return UnableToCreateViewSnafu {
                reason: format!(
                    "Expected 1 statement to create view from, received {}",
                    statements.len()
                )
                .to_string(),
            }
            .fail();
        }

        let ctx = Arc::clone(&self.ctx);
        spawn(async move {
            // Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
            // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
            // the tables are created. To handle this, wait until all tables are created.

            let deadline = Instant::now() + Duration::from_secs(60);
            let mut unresolved_dependent_table: Option<TableReference> = None;
            let dependent_table_names = get_dependent_table_names(&statements[0]);
            for dependent_table_name in dependent_table_names {
                let mut attempts = 0;

                if unresolved_dependent_table.is_some() {
                    break;
                }

                loop {
                    if !ctx
                        .table_exist(dependent_table_name.clone())
                        .unwrap_or(false)
                    {
                        if Instant::now() >= deadline {
                            unresolved_dependent_table = Some(dependent_table_name.clone());
                            break;
                        }

                        if attempts % 10 == 0 {
                            tracing::warn!("Dependent table {dependent_table_name} for view {table} does not exist, retrying...");
                        }
                        attempts += 1;
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    break;
                }
            }

            if let Some(missing_table) = unresolved_dependent_table {
                tracing::error!("Failed to create view {table}. Dependent table {missing_table} does not exist.");
                return;
            }

            let plan = match ctx.state().statement_to_plan(statements[0].clone()).await {
                Ok(plan) => plan,
                Err(e) => {
                    tracing::error!("Failed to create view: {e}");
                    return;
                }
            };

            let view_table = match ViewTable::try_new(plan, Some(view)) {
                Ok(view_table) => view_table,
                Err(e) => {
                    tracing::error!("Failed to create view: {e}");
                    return;
                }
            };
            if let Err(e) = ctx.register_table(table.clone(), Arc::new(view_table)) {
                tracing::error!("Failed to create view: {e}");
            };

            tracing::info!("Created view {table}");
        });

        Ok(())
    }

    pub fn get_public_table_names(&self) -> Result<Vec<String>> {
        Ok(self
            .ctx
            .catalog(SPICE_DEFAULT_CATALOG)
            .context(CatalogMissingSnafu {
                catalog: SPICE_DEFAULT_CATALOG.to_string(),
            })?
            .schema(SPICE_DEFAULT_SCHEMA)
            .context(SchemaMissingSnafu {
                schema: SPICE_DEFAULT_SCHEMA.to_string(),
            })?
            .table_names())
    }

    pub fn query_builder(self: &Arc<Self>, sql: String, protocol: Protocol) -> QueryBuilder {
        QueryBuilder::new(sql, Arc::clone(self), protocol)
    }
}

impl Default for DataFusion {
    fn default() -> Self {
        Self::new()
    }
}
