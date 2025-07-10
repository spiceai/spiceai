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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::accelerated_table::refresh::{self, RefreshOverrides};
use crate::accelerated_table::{self, AcceleratedTableBuilderError};
use crate::accelerated_table::{AcceleratedTable, Retention, refresh::Refresh};
use crate::catalogconnector::deferred::DeferredCatalogProvider;
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::{Dataset, Mode, ReadyState};
use crate::component::view::View;
use crate::dataaccelerator::AcceleratorEngineRegistry;
use crate::dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint;
use crate::dataaccelerator::{
    self,
    behaviors::{Behavior, Behaviors},
};
use crate::dataconnector::deferred::DeferredConnector;
use crate::dataconnector::localpod::LOCALPOD_DATACONNECTOR;
use crate::dataconnector::sink::SinkConnector;
use crate::dataconnector::{DataConnector, DataConnectorError};
use crate::datafusion::schema::SpiceSchemaProvider;
use crate::dataupdate::{
    DataUpdate, StreamingDataUpdate, StreamingDataUpdateExecutionPlan, UpdateType,
};
use crate::federated_table::FederatedTable;
use crate::search::full_text::udtf::TEXT_SEARCH_UDTF_NAME;
use crate::secrets::Secrets;
use crate::tracing_util::view_registered_trace;
use crate::view::create_view_table;
use crate::{status, view};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow_tools::schema::verify_schema;
use builder::DataFusionBuilder;
use cache::result::search::CachedSearchResult;
use cache::{CacheProvider, Caching, QueryResultsCacheProvider, key::RawCacheKey};
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{ResolvedTableReference, TableReference, sqlparser};
use datafusion_federation::FederatedTableProviderAdaptor;
use error::find_datafusion_root;
use itertools::Itertools;
use query::QueryBuilder;
use schema::ensure_schema_exists;
use snafu::prelude::*;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::{RwLock as TokioRwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

pub mod query;

pub mod builder;
pub mod dialect;
pub mod error;
pub mod extension;
pub mod filter_converter;
pub mod param_utils;
pub mod refresh_sql;
pub mod request_context_extension;
pub mod schema;
pub mod udf;

pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_RUNTIME_SCHEMA: &str = "runtime";
pub const SPICE_EVAL_SCHEMA: &str = "eval";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";
pub const SPICE_METADATA_SCHEMA: &str = "metadata";
pub const SPICE_SCP_SCHEMA: &str = "scp";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("When processing the acceleration registration: {source}"))]
    AccelerationRegistration {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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

    #[snafu(display(
        "Table {table_name} was marked as read_write, but the underlying provider only supports reads."
    ))]
    WriteProviderNotImplemented { table_name: String },

    #[snafu(display(
        "Table {table_name} is expected to provide metadata, but the underlying provider does not support this."
    ))]
    MetadataProviderNotImplemented { table_name: String },

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

    #[snafu(display("Failed to refresh the dataset {dataset_name}.\n{source}"))]
    UnableToTriggerRefresh {
        dataset_name: String,
        source: crate::accelerated_table::Error,
    },

    #[snafu(display(
        "Changing the schema of an accelerated table via the Refresh SQL is not allowed.\nRetry the request, changing the SELECT statement from 'SELECT {selected_columns}' to 'SELECT {refresh_columns}'"
    ))]
    RefreshSqlSchemaChangeDisallowed {
        dataset_name: Arc<str>,
        selected_columns: Arc<str>,
        refresh_columns: Arc<str>,
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

    #[snafu(display("Unable to get object store configuration: {source}"))]
    UnableToGetSchemaTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get the lock of data writers"))]
    UnableToLockDataWriters {},

    #[snafu(display(
        "The schema returned by the data connector for 'refresh_mode: changes' does not contain a data field"
    ))]
    ChangeSchemaWithoutDataField { source: ArrowError },

    #[snafu(display("Unable to create streaming data update: {source}"))]
    UnableToCreateStreamingUpdate {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("{source}"))]
    InvalidTimeColumnTimeFormat { source: refresh::Error },

    #[snafu(display(
        "Acceleration mode `append` requires `time_column` parameter for source {from}.\nConfigure `time_column` parameter and try again.\nFor details, visit: https://spiceai.org/docs/reference/spicepod/datasets#time_column"
    ))]
    AppendRequiresTimeColumn { from: String },

    #[snafu(display("Unable to retrieve underlying table provider from federation"))]
    UnableToRetrieveTableFromFederation { table_name: String },

    #[snafu(display(
        "Failed to create an accelerated table for the dataset {dataset_name}.\n{source}"
    ))]
    UnableToBuildAcceleratedTable {
        dataset_name: String,
        source: AcceleratedTableBuilderError,
    },

    #[snafu(display(
        "Failed to create an accelerated table for {component_name}.\nError setting the underlying table provider: {source}"
    ))]
    UnableToSetUnderlyingTableProvider {
        component_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Failed register a '{index_type}' index for the table '{dataset_name}'"))]
    UnableToRegisterTableIndex {
        dataset_name: String,
        index_type: String,
    },

    #[snafu(display("Failed get the '{index_type}' index for the table '{dataset_name}'"))]
    UnableToGetTableIndex {
        dataset_name: String,
        index_type: String,
    },
}

pub enum Table {
    Accelerated {
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        accelerated_table: Option<AcceleratedTable>,
        secrets: Arc<TokioRwLock<Secrets>>,
    },
    Federated {
        data_connector: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
    },
}

struct PendingSinkRegistration {
    dataset: Arc<Dataset>,
    secrets: Arc<TokioRwLock<Secrets>>,
}

struct DeferredTableRegistration {
    dataset: Arc<Dataset>,
    connector: Arc<dyn DataConnector>,
}

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    runtime_status: Arc<status::RuntimeStatus>,
    data_writers: RwLock<HashSet<TableReference>>,
    accelerated_tables: TokioRwLock<HashSet<TableReference>>,
    caching: Arc<Caching>,
    pending_sink_tables: TokioRwLock<Vec<PendingSinkRegistration>>,
    deferred_tables: TokioRwLock<HashMap<String, DeferredTableRegistration>>,
    deferred_catalogs: TokioRwLock<HashMap<String, Arc<DeferredCatalogProvider>>>,

    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    // Controls the parallelism of accelerated table refreshes
    acceleration_refresh_semaphore: Option<Arc<Semaphore>>,
    pub(crate) task_history_enabled: bool,
}

impl std::fmt::Debug for DataFusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusion")
            .field("runtime_status", &self.runtime_status)
            .field("data_writers", &self.data_writers)
            .field("accelerated_tables", &self.accelerated_tables)
            .field("caching", &self.caching)
            .finish_non_exhaustive()
    }
}

impl DataFusion {
    #[must_use]
    pub fn builder(
        status: Arc<status::RuntimeStatus>,
        accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    ) -> DataFusionBuilder {
        DataFusionBuilder::new(status, accelerator_engine_registry)
    }

    #[must_use]
    pub fn runtime_status(&self) -> Arc<status::RuntimeStatus> {
        Arc::clone(&self.runtime_status)
    }

    #[must_use]
    fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(catalog) = self.ctx.catalog(SPICE_DEFAULT_CATALOG) {
            return catalog.schema(schema_name);
        }

        None
    }

    pub fn accelerator_engine_registry(&self) -> Arc<AcceleratorEngineRegistry> {
        Arc::clone(&self.accelerator_engine_registry)
    }

    pub async fn get_table(
        &self,
        table_reference: &TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        let catalog_provider = self.resolve_catalog_provider(table_reference)?;

        let schema_provider = Self::resolve_schema_provider(&catalog_provider, table_reference)?;

        schema_provider
            .table(table_reference.table())
            .await
            .ok()
            .flatten()
    }

    /// Returns the `TableProvider` for the given `TableReference` synchronously.
    ///
    /// This method may return `None` if the table is registered from a catalog provider that doesn't support synchronous table access.
    /// All tables registered in the default catalog (i.e. `spice`) are available synchronously.
    /// Catalog implementations that use `SpiceSchemaProvider` objects are also available synchronously.
    pub fn get_table_sync(
        &self,
        table_reference: &TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        let catalog_provider = self.resolve_catalog_provider(table_reference)?;

        let schema_provider = Self::resolve_schema_provider(&catalog_provider, table_reference)?;

        let spice_schema_provider = schema_provider
            .as_any()
            .downcast_ref::<SpiceSchemaProvider>()?;

        spice_schema_provider.table_sync(table_reference.table())
    }

    /// Register a table with its [`SchemaProvider`] if it exists and marks it as writable.
    ///
    /// This method is generally used for tables that are created by the Spice runtime.
    pub fn register_table_as_writable_and_with_schema(
        &self,
        table_name: TableReference,
        table: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        if let Some(schema) = table_name.schema() {
            if let Some(eval_schema) = self.schema(schema) {
                eval_schema
                    .register_table(table_name.table().to_string(), table)
                    .map_err(find_datafusion_root)
                    .context(UnableToRegisterTableToDataFusionSchemaSnafu {
                        schema: SPICE_EVAL_SCHEMA,
                    })?;
            }
        }

        self.data_writers
            .write()
            .map_err(|_| Error::UnableToLockDataWriters {})?
            .insert(table_name);

        Ok(())
    }

    pub async fn register_catalog(
        &self,
        name: &str,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Result<()> {
        if let Some(deferred_catalog) = catalog.as_any().downcast_ref::<DeferredCatalogProvider>() {
            self.deferred_catalogs
                .write()
                .await
                .insert(name.to_string(), Arc::new(deferred_catalog.clone()));
        } else {
            self.ctx.register_catalog(name, catalog);
        }

        Ok(())
    }

    // Returns a Notify if the table supports notifying the runtime when the table is ready.
    pub async fn register_table(
        &self,
        dataset: Arc<Dataset>,
        table: Table,
    ) -> Result<Option<Arc<Notify>>> {
        schema::ensure_schema_exists(&self.ctx, SPICE_DEFAULT_CATALOG, &dataset.name)?;

        let dataset_mode = dataset.mode();
        let dataset_table_ref = dataset.name.clone();

        let is_ready = match table {
            Table::Accelerated {
                source,
                federated_read_table,
                accelerated_table,
                secrets,
            } => {
                if let Some(accelerated_table) = accelerated_table {
                    tracing::debug!(
                        "Registering dataset {dataset:?} with preloaded accelerated table"
                    );
                    let notifier = accelerated_table.refresher().on_complete_notification();
                    self.ctx
                        .register_table(
                            dataset_table_ref.clone(),
                            Arc::new(accelerated_table).table_provider(),
                        )
                        .map_err(find_datafusion_root)
                        .context(UnableToRegisterTableToDataFusionSnafu)?;
                    notifier
                } else if source.as_any().downcast_ref::<SinkConnector>().is_some() {
                    // Sink connectors don't know their schema until the first data is received. Park this registration until the schema is known via the first write.
                    self.runtime_status
                        .update_dataset(&dataset_table_ref, status::ComponentStatus::Ready);
                    self.pending_sink_tables
                        .write()
                        .await
                        .push(PendingSinkRegistration {
                            dataset: Arc::clone(&dataset),
                            secrets: Arc::clone(&secrets),
                        });
                    None
                } else {
                    self.register_accelerated_table(dataset, source, federated_read_table, secrets)
                        .await?
                }
            }
            Table::Federated {
                data_connector,
                federated_read_table,
            } => {
                if let Some(deferred_connector) =
                    data_connector.as_any().downcast_ref::<DeferredConnector>()
                {
                    self.runtime_status
                        .update_dataset(&dataset_table_ref, status::ComponentStatus::Ready);

                    self.deferred_tables.write().await.insert(
                        dataset.name.to_string(),
                        DeferredTableRegistration {
                            dataset: Arc::clone(&dataset),
                            connector: deferred_connector.source(),
                        },
                    );
                } else {
                    self.register_federated_table(&dataset, data_connector, federated_read_table)
                        .await?;
                }

                None
            }
        };

        if matches!(dataset_mode, Mode::ReadWrite) {
            self.data_writers
                .write()
                .map_err(|_| Error::UnableToLockDataWriters {})?
                .insert(dataset_table_ref.clone());
        }

        Ok(is_ready)
    }

    #[must_use]
    pub fn is_writable(&self, table_reference: &TableReference) -> bool {
        if let Ok(writers) = self.data_writers.read() {
            writers.iter().any(|s| s.resolved_eq(table_reference))
        } else {
            false
        }
    }

    #[must_use]
    pub async fn is_accelerated(&self, table_reference: &TableReference) -> bool {
        self.accelerated_tables
            .read()
            .await
            .contains(table_reference)
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
                    .map_err(find_datafusion_root)
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
            .map_err(find_datafusion_root)
            .context(UnableToGetTableSnafu)?;

        Ok(table_provider)
    }

    pub async fn load_deferred_dataset(&self, table_reference: TableReference) -> Result<()> {
        let deferred_tables = self.deferred_tables.read().await;
        if let Some(deferred_registration) = deferred_tables.get(&table_reference.to_string()) {
            let read_provider = deferred_registration
                .connector
                .read_provider(&deferred_registration.dataset)
                .await
                .context(UnableToResolveTableProviderSnafu)?;

            let federated_table = FederatedTable::new_unchecked(read_provider);
            self.register_federated_table(
                &deferred_registration.dataset,
                Arc::clone(&deferred_registration.connector),
                federated_table,
            )
            .await?;

            drop(deferred_tables);

            let mut deferred_tables = self.deferred_tables.write().await;
            deferred_tables.remove(&table_reference.to_string());
        }

        Ok(())
    }

    pub async fn load_deferred_catalog(&self, name: &str) -> Result<()> {
        let deferred_catalogs = self.deferred_catalogs.read().await;
        if let Some(catalog) = deferred_catalogs.get(name) {
            if let Ok(provider) = catalog.get_catalog_provider().await {
                self.ctx.register_catalog(name, Arc::clone(&provider));
            }

            drop(deferred_catalogs);

            let mut deferred_catalogs = self.deferred_catalogs.write().await;
            deferred_catalogs.remove(name);
        }

        Ok(())
    }

    async fn ensure_sink_dataset(
        &self,
        table_reference: TableReference,
        schema: SchemaRef,
    ) -> Result<()> {
        let pending_sink_registrations = self.pending_sink_tables.read().await;

        let mut pending_registration = None;
        for pending_sink_registration in pending_sink_registrations.iter() {
            if pending_sink_registration.dataset.name == table_reference {
                pending_registration = Some(pending_sink_registration);
                break;
            }
        }

        let Some(pending_registration) = pending_registration else {
            return Ok(());
        };

        let sink_connector = Arc::new(SinkConnector::new(schema)) as Arc<dyn DataConnector>;
        let read_provider = sink_connector
            .read_provider(&pending_registration.dataset)
            .await
            .context(UnableToResolveTableProviderSnafu)?;
        let federated_table = FederatedTable::new_unchecked(read_provider);

        tracing::info!(
            "Loading data for dataset {}",
            pending_registration.dataset.name
        );
        self.register_accelerated_table(
            Arc::clone(&pending_registration.dataset),
            sink_connector,
            federated_table,
            Arc::clone(&pending_registration.secrets),
        )
        .await?;

        drop(pending_sink_registrations);

        let mut pending_sink_registrations = self.pending_sink_tables.write().await;
        let mut pending_registration_idx = Some(0);
        for (pending_sink_registration_idx, pending_sink_registration) in
            pending_sink_registrations.iter().enumerate()
        {
            if pending_sink_registration.dataset.name == table_reference {
                pending_registration_idx = Some(pending_sink_registration_idx);
                break;
            }
        }
        if let Some(pending_registration_idx) = pending_registration_idx {
            pending_sink_registrations.remove(pending_registration_idx);
        }

        Ok(())
    }

    pub async fn write_data(
        &self,
        table_reference: &TableReference,
        data_update: DataUpdate,
    ) -> Result<()> {
        if !self.is_writable(table_reference) {
            TableNotWritableSnafu {
                table_name: table_reference.to_string(),
            }
            .fail()?;
        }

        self.ensure_sink_dataset(table_reference.clone(), Arc::clone(&data_update.schema))
            .await?;

        let table_provider = self.get_table_provider(table_reference).await?;

        verify_schema(
            table_provider.schema().fields(),
            data_update.schema.fields(),
        )
        .context(SchemaMismatchSnafu)?;

        let overwrite = match data_update.update_type {
            UpdateType::Overwrite => InsertOp::Overwrite,
            UpdateType::Append => InsertOp::Append,
            UpdateType::Changes => InsertOp::Replace,
        };

        let streaming_update = StreamingDataUpdate::try_from(data_update)
            .map_err(find_datafusion_root)
            .context(UnableToCreateStreamingUpdateSnafu)?;

        let insert_plan = table_provider
            .insert_into(
                &self.ctx.state(),
                Arc::new(StreamingDataUpdateExecutionPlan::new(streaming_update.data)),
                overwrite,
            )
            .await
            .map_err(find_datafusion_root)
            .context(UnableToPlanTableInsertSnafu {
                table_name: table_reference.to_string(),
            })?;

        let _ = collect(insert_plan, self.ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(UnableToExecuteTableInsertSnafu {
                table_name: table_reference.to_string(),
            })?;

        self.runtime_status
            .update_dataset(table_reference, status::ComponentStatus::Ready);

        Ok(())
    }

    pub async fn write_streaming_data(
        &self,
        table_reference: &TableReference,
        streaming_update: StreamingDataUpdate,
    ) -> Result<()> {
        if !self.is_writable(table_reference) {
            TableNotWritableSnafu {
                table_name: table_reference.to_string(),
            }
            .fail()?;
        }

        let update_schema = streaming_update.data.schema();

        self.ensure_sink_dataset(table_reference.clone(), Arc::clone(&update_schema))
            .await?;

        let table_provider = self.get_table_provider(table_reference).await?;

        verify_schema(table_provider.schema().fields(), update_schema.fields())
            .context(SchemaMismatchSnafu)?;

        let overwrite = match streaming_update.update_type {
            UpdateType::Overwrite => InsertOp::Overwrite,
            UpdateType::Append => InsertOp::Append,
            UpdateType::Changes => InsertOp::Replace,
        };

        let insert_plan = table_provider
            .insert_into(
                &self.ctx.state(),
                Arc::new(StreamingDataUpdateExecutionPlan::new(streaming_update.data)),
                overwrite,
            )
            .await
            .map_err(find_datafusion_root)
            .context(UnableToPlanTableInsertSnafu {
                table_name: table_reference.to_string(),
            })?;

        let _ = collect(insert_plan, self.ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(UnableToExecuteTableInsertSnafu {
                table_name: table_reference.to_string(),
            })?;

        Ok(())
    }

    pub async fn get_arrow_schema(&self, dataset: impl Into<TableReference>) -> Result<Schema> {
        let data_frame = self
            .ctx
            .table(dataset)
            .await
            .map_err(find_datafusion_root)
            .context(UnableToGetTableSnafu)?;
        Ok(Schema::from(data_frame.schema()))
    }

    #[must_use]
    pub fn table_exists(&self, dataset_name: TableReference) -> bool {
        self.ctx.table_exist(dataset_name).unwrap_or(false)
    }

    #[must_use]
    pub fn catalog_exists(&self, catalog: &str) -> bool {
        self.ctx.catalog(catalog).is_some()
    }

    pub async fn remove_view(&self, view_name: &TableReference) -> Result<()> {
        if !self.ctx.table_exist(view_name.clone()).unwrap_or(false) {
            return Ok(());
        }

        if let Err(e) = self.ctx.deregister_table(view_name.clone()) {
            return UnableToDeleteTableSnafu {
                reason: e.to_string(),
            }
            .fail();
        }

        if self.is_accelerated(view_name).await {
            self.accelerated_tables.write().await.remove(view_name);
        }

        Ok(())
    }

    pub async fn remove_table(&self, dataset_name: &TableReference) -> Result<()> {
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

        if self.is_accelerated(dataset_name).await {
            self.accelerated_tables.write().await.remove(dataset_name);
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub async fn create_accelerated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        secrets: Arc<TokioRwLock<Secrets>>,
    ) -> Result<AcceleratedTable> {
        tracing::debug!("Creating accelerated table {dataset:?}");
        let source_table_provider = match dataset.mode() {
            Mode::Read => Arc::new(federated_read_table),
            Mode::ReadWrite => {
                let read_write_provider = source
                    .read_write_provider(dataset)
                    .await
                    .ok_or_else(|| {
                        WriteProviderNotImplementedSnafu {
                            table_name: dataset.name.to_string(),
                        }
                        .build()
                    })?
                    .context(UnableToResolveTableProviderSnafu)?;
                Arc::new(FederatedTable::new_unchecked(read_write_provider))
            }
        };

        let source_schema = source_table_provider.schema();

        let refresh_sql = dataset.refresh_sql();
        let refresh_schema = if let Some(refresh_sql) = &refresh_sql {
            refresh_sql::validate_refresh_sql(
                dataset.name.clone(),
                refresh_sql.as_str(),
                source_schema,
            )
            .context(RefreshSqlSnafu)?
        } else {
            source_schema
        };

        let acceleration_settings =
            dataset
                .acceleration
                .clone()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: dataset.name.to_string(),
                })?;

        let constraints = match &*source_table_provider {
            FederatedTable::Immediate(table_provider) => table_provider.constraints(),
            FederatedTable::Deferred(_) => None,
        };

        let (accelerated_table_provider, accelerated_table_behaviors) = self
            .accelerator_engine_registry
            .create_accelerator_table(
                dataset.name.clone(),
                Arc::clone(&refresh_schema),
                constraints,
                &acceleration_settings,
                secrets,
                Some(dataset),
                Arc::clone(&self.ctx),
            )
            .await
            .context(UnableToCreateDataAcceleratorSnafu)?;

        handle_accelerated_table_behavior(
            accelerated_table_behaviors,
            &source_table_provider,
            &dataset.name.to_string(),
        )?;

        // If we already have an existing dataset checkpoint table that has been checkpointed,
        // it means there is data from a previous acceleration and we don't need
        // to wait for the first refresh to complete to mark it ready.
        let mut initial_load_complete = false;
        if let Ok(checkpoint) = DatasetCheckpoint::try_new(dataset).await {
            if checkpoint.exists().await {
                self.runtime_status
                    .update_dataset(&dataset.name, status::ComponentStatus::Ready);
                initial_load_complete = true;
            }
        }

        let refresh_mode = source.resolve_refresh_mode(acceleration_settings.refresh_mode);

        let mut refresh = Refresh::new(refresh_mode).with_retry(
            dataset.refresh_retry_enabled(),
            dataset.refresh_retry_max_attempts(),
        );
        if let Some(sql) = &refresh_sql {
            refresh = refresh.sql(sql.clone());
        }
        if let Some(format) = dataset.time_format {
            refresh = refresh.time_format(format);
        }
        if let Some(time_col) = &dataset.time_column {
            refresh = refresh.time_column(time_col.clone());
        }
        if let Some(time_partition_column) = &dataset.time_partition_column {
            refresh = refresh.time_partition_column(time_partition_column.clone());
        }
        if let Some(time_partition_format) = dataset.time_partition_format {
            refresh = refresh.time_partition_format(time_partition_format);
        }
        if let Some(check_interval) = dataset.refresh_check_interval() {
            refresh = refresh.check_interval(check_interval);
        }
        if let Some(max_jitter) = dataset.refresh_max_jitter() {
            refresh = refresh.max_jitter(max_jitter);
        }
        if let Some(append_overlap) = acceleration_settings.refresh_append_overlap {
            refresh = refresh.append_overlap(append_overlap);
        }

        // we must not fetch data older than the explicitly set refresh data window or retention period
        let refresh_data_window = dataset.refresh_data_window().or(dataset.retention_period());

        if let Some(refresh_data_window) = refresh_data_window {
            refresh = refresh.period(refresh_data_window);
        }
        refresh
            .validate_time_format(dataset.name.to_string(), &refresh_schema)
            .context(InvalidTimeColumnTimeFormatSnafu)?;

        let mut accelerated_table_builder = AcceleratedTable::builder(
            Arc::clone(&self.runtime_status),
            dataset.name.clone(),
            Arc::clone(&source_table_provider),
            dataset.source().to_string(),
            accelerated_table_provider,
            refresh,
        );

        accelerated_table_builder.retention(Retention::new(
            dataset.time_column.clone(),
            dataset.time_format,
            dataset.time_partition_column.clone(),
            dataset.time_partition_format,
            dataset.retention_period(),
            dataset.retention_check_interval(),
            acceleration_settings.retention_check_enabled,
        ));

        accelerated_table_builder.zero_results_action(acceleration_settings.on_zero_results);

        accelerated_table_builder.refresh_on_startup(acceleration_settings.refresh_on_startup);

        accelerated_table_builder.ready_state(dataset.ready_state);

        accelerated_table_builder.caching(Some(Arc::clone(&self.caching)));

        accelerated_table_builder.checkpointer_opt(DatasetCheckpoint::try_new(dataset).await.ok());

        accelerated_table_builder.initial_load_complete(initial_load_complete);

        if acceleration_settings.disable_federation {
            accelerated_table_builder.disable_federation();
        }

        if let Some(semaphore) = &self.acceleration_refresh_semaphore {
            accelerated_table_builder.refresh_semaphore(Arc::clone(semaphore));
        }

        if refresh_mode == RefreshMode::Changes {
            let changes_stream = source.changes_stream(Arc::clone(&source_table_provider));

            if let Some(changes_stream) = changes_stream {
                accelerated_table_builder.changes_stream(changes_stream);
            }
        }

        if refresh_mode == RefreshMode::Append && dataset.time_column.is_none() {
            let append_stream = source.append_stream(source_table_provider);
            if let Some(append_stream) = append_stream {
                accelerated_table_builder.append_stream(append_stream);
            } else {
                return Err(Error::AppendRequiresTimeColumn {
                    from: dataset.from.clone(),
                });
            }
        }

        // If this is a localpod accelerated table, attempt to synchronize refreshes with the parent table
        if dataset.source() == LOCALPOD_DATACONNECTOR {
            self.attempt_to_synchronize_accelerated_table(&mut accelerated_table_builder, dataset)
                .await;
        }

        accelerated_table_builder
            .build()
            .await
            .context(UnableToBuildAcceleratedTableSnafu {
                dataset_name: dataset.name.to_string(),
            })
    }

    /// Attempt to synchronize refreshes with the parent table for localpod accelerated tables.
    ///
    /// This will not work if:
    /// - The parent table is not an accelerated table.
    /// - The parent or child acceleration is not configured as `RefreshMode::Full`.
    ///
    /// It is safe to fallback to the existing acceleration behavior, but the refreshes won't be synchronized.
    pub async fn attempt_to_synchronize_accelerated_table(
        &self,
        accelerated_table_builder: &mut accelerated_table::Builder,
        dataset: &Dataset,
    ) {
        let parent_table_reference = TableReference::parse_str(dataset.path());
        let Ok(parent_table) = self.get_table_provider(&parent_table_reference).await else {
            tracing::debug!(
                "Could not synchronize refreshes with parent table {parent_table_reference}. Parent table not found."
            );
            return;
        };
        let Some(parent_table_federation_adaptor) = parent_table
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>(
        ) else {
            tracing::debug!(
                "Could not synchronize refreshes with parent table {parent_table_reference}. Parent table is not a federated table."
            );
            return;
        };
        let Some(parent_table) = parent_table_federation_adaptor.table_provider.clone() else {
            tracing::debug!(
                "Could not synchronize refreshes with parent table {parent_table_reference}. Parent federated table doesn't contain a table provider."
            );
            return;
        };
        let Some(parent_table) = parent_table.as_any().downcast_ref::<AcceleratedTable>() else {
            tracing::debug!(
                "Could not synchronize refreshes with parent table {parent_table_reference}. Parent table is not an accelerated table."
            );
            return;
        };
        if let Err(e) = accelerated_table_builder
            .synchronize_with(parent_table)
            .await
        {
            tracing::debug!(
                "Could not synchronize refreshes with parent table {parent_table_reference}. Error: {e}"
            );
            return;
        }

        tracing::info!(
            "Localpod dataset {} synchronizing refreshes with parent table {parent_table_reference}",
            dataset.name
        );
    }

    pub fn results_cache_provider(&self) -> Option<Arc<QueryResultsCacheProvider>> {
        self.caching.results.clone()
    }

    pub fn plans_cache_provider(
        &self,
    ) -> Option<Arc<dyn CacheProvider<LogicalPlan> + Send + Sync>> {
        self.caching.plans.clone()
    }

    pub fn search_cache_provider(
        &self,
    ) -> Option<Arc<dyn CacheProvider<CachedSearchResult> + Send + Sync>> {
        self.caching.search.clone()
    }

    async fn register_accelerated_table(
        &self,
        dataset: Arc<Dataset>,
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        secrets: Arc<TokioRwLock<Secrets>>,
    ) -> Result<Option<Arc<Notify>>> {
        let mut accelerated_table = self
            .create_accelerated_table(&dataset, Arc::clone(&source), federated_read_table, secrets)
            .await?;
        let notifier = accelerated_table.refresher().on_complete_notification();

        source
            .on_accelerated_table_registration(&dataset, &mut accelerated_table)
            .await
            .context(AccelerationRegistrationSnafu)?;

        self.ctx
            .register_table(
                dataset.name.clone(),
                Arc::new(accelerated_table).table_provider(),
            )
            .map_err(find_datafusion_root)
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        self.register_metadata_table(&dataset, Arc::clone(&source))
            .await?;

        self.accelerated_tables
            .write()
            .await
            .insert(dataset.name.clone());

        Ok(notifier)
    }

    pub async fn refresh_table(
        &self,
        dataset_name: &TableReference,
        overrides: Option<RefreshOverrides>,
    ) -> Result<Option<Arc<Notify>>> {
        let table = self
            .get_accelerated_table_provider(dataset_name.to_string().as_str())
            .await?;
        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            let notifier = accelerated_table.refresher().on_complete_notification();
            accelerated_table.trigger_refresh(overrides).await.context(
                UnableToTriggerRefreshSnafu {
                    dataset_name: dataset_name.to_string(),
                },
            )?;

            return Ok(notifier);
        }
        NotAcceleratedTableSnafu {
            table_name: dataset_name.to_string(),
        }
        .fail()?
    }

    pub async fn update_refresh_sql(
        &self,
        dataset_name: TableReference,
        refresh_sql: Option<String>,
    ) -> Result<()> {
        let table = self
            .get_accelerated_table_provider(&dataset_name.to_string())
            .await?;

        let refresh_schema = table.schema();

        if let Some(sql) = &refresh_sql {
            let selected_schema = refresh_sql::validate_refresh_sql(
                dataset_name.clone(),
                sql,
                Arc::clone(&refresh_schema),
            )
            .context(RefreshSqlSnafu)?;
            if selected_schema != refresh_schema {
                return RefreshSqlSchemaChangeDisallowedSnafu {
                    dataset_name: Arc::from(dataset_name.to_string()),
                    selected_columns: Arc::from(
                        selected_schema.fields().iter().map(|f| f.name()).join(", "),
                    ),
                    refresh_columns: Arc::from(
                        refresh_schema.fields().iter().map(|f| f.name()).join(", "),
                    ),
                }
                .fail();
            }
        }

        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            accelerated_table
                .update_refresh_sql(refresh_sql)
                .await
                .context(UnableToTriggerRefreshSnafu {
                    dataset_name: dataset_name.to_string(),
                })?;
        }

        Ok(())
    }

    pub async fn get_accelerated_table_provider(
        &self,
        dataset_name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let mut table = self
            .ctx
            .table_provider(dataset_name)
            .await
            .map_err(find_datafusion_root)
            .context(UnableToGetTableSnafu)?;
        if let Some(adaptor) = table
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
        {
            if let Some(nested_table) = adaptor.table_provider.clone() {
                table = nested_table;
            } else {
                return UnableToRetrieveTableFromFederationSnafu {
                    table_name: dataset_name.to_string(),
                }
                .fail();
            }
        }
        Ok(table)
    }

    /// Federated tables are attached directly as tables visible in the public `DataFusion` context.
    async fn register_federated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
    ) -> Result<()> {
        tracing::debug!("Registering federated table {dataset:?}");
        let table_exists = self.ctx.table_exist(dataset.name.clone()).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let federated_table_provider = federated_read_table.table_provider().await;

        let source_table_provider = match dataset.mode() {
            Mode::Read => federated_table_provider,
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
            .map_err(find_datafusion_root)
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
                .map_err(find_datafusion_root)
                .context(UnableToRegisterTableToDataFusionSnafu)?;
        }
        Ok(())
    }

    pub(crate) fn register_view(
        self: &Arc<Self>,
        view: Arc<View>,
        secrets: Arc<TokioRwLock<Secrets>>,
    ) -> Result<JoinHandle<Option<Arc<Notify>>>> {
        tracing::info!("Initializing view {}", &view.name);

        let table_exists = self.ctx.table_exist(view.name.clone()).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }
        ensure_schema_exists(&self.ctx, SPICE_DEFAULT_CATALOG, &view.name)?;

        let statements = DFParser::parse_sql_with_dialect(&view.sql, &PostgreSqlDialect {})
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
        let df_ref = Arc::clone(self);
        let dependent_table_names = view::get_dependent_table_names(&statements[0]);
        let status = self.runtime_status();

        let register_task: JoinHandle<Option<Arc<Notify>>> = spawn(async move {
            // Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
            // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
            // the tables are created. To handle this, wait until all tables are created.

            let deadline = Instant::now() + Duration::from_secs(60);
            let mut unresolved_dependent_table: Option<TableReference> = None;

            let table = &view.name;

            for dependent_table_name in &dependent_table_names {
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
                            tracing::warn!(
                                "Dependent table {dependent_table_name} for view {table} does not exist, retrying..."
                            );
                        }
                        attempts += 1;
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    break;
                }
            }

            if let Some(missing_table) = unresolved_dependent_table {
                tracing::error!(
                    "Failed to create view {table}. Dependent table {missing_table} does not exist."
                );
                status.update_view(table, status::ComponentStatus::Error);
                return None;
            }

            let view_table = match create_view_table(&ctx, &statements[0], view.sql.as_ref()).await
            {
                Ok(view_table) => view_table,
                Err(e) => {
                    tracing::error!("Failed to create view: {e}");
                    status.update_view(table, status::ComponentStatus::Error);
                    return None;
                }
            };

            if let Some(acceleration) = &view.acceleration {
                if acceleration.enabled {
                    match df_ref
                        .create_accelerated_view(&view, view_table, &dependent_table_names, secrets)
                        .await
                    {
                        Ok(is_ready) => {
                            return is_ready;
                        }
                        Err(e) => {
                            tracing::error!("Failed to create view: {e}");
                            status.update_view(table, status::ComponentStatus::Error);
                            return None;
                        }
                    }
                }
            }

            // non-accelerated view
            if let Err(e) = ctx.register_table(table.clone(), Arc::new(view_table)) {
                tracing::error!("Failed to create view: {e}");
                status.update_view(table, status::ComponentStatus::Error);
                return None;
            }
            tracing::info!("{}", view_registered_trace(table, None));
            status.update_view(table, status::ComponentStatus::Ready);

            None
        });

        Ok(register_task)
    }

    pub async fn create_accelerated_view(
        self: &Arc<Self>,
        view: &View,
        view_table: ViewTable,
        dependent_tables: &[TableReference],
        secrets: Arc<TokioRwLock<Secrets>>,
    ) -> Result<Option<Arc<Notify>>> {
        let table = &view.name;

        tracing::debug!(
            "Creating accelerated view {table} with dependent tables {dependent_tables:?}"
        );

        let acceleration =
            view.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: table.to_string(),
                })?;

        let runtime_status = self.runtime_status();

        // If accelerated view depends on other tables, wait until they are ready; this is required to complete
        // initial data load and avoid errors indicating that the load can't be completed because tables are still loading or connecting
        wait_until_dependent_tables_are_ready(table, dependent_tables, &runtime_status).await;

        let schema = view_table.schema();
        let federated_table =
            FederatedTable::new_unchecked(Arc::new(view_table) as Arc<dyn TableProvider>);

        let (accelerated_table_provider, accelerated_table_behaviors) = self
            .accelerator_engine_registry()
            .create_accelerator_table(
                table.clone(),
                schema,
                None,
                acceleration,
                secrets,
                Some(view),
                Arc::clone(&self.ctx),
            )
            .await
            .map_err(|e| Error::UnableToCreateView {
                reason: format!("Failed to create view acceleration: {e}"),
            })?;

        handle_accelerated_table_behavior(
            accelerated_table_behaviors,
            &federated_table,
            &view.name.to_string(),
        )?;

        // Detect if data for view was already loaded so we don't need to wait for the first refresh to complete to mark it as ready.
        let mut initial_load_complete = false;
        if let Ok(checkpoint) = DatasetCheckpoint::try_new(view).await {
            if checkpoint.exists().await {
                initial_load_complete = true;
            }
        }

        let mut refresh = Refresh::new(RefreshMode::Full).with_retry(
            view.refresh_retry_enabled(),
            view.refresh_retry_max_attempts(),
        );
        if let Some(refresh_check_interval) = acceleration.refresh_check_interval {
            refresh = refresh.check_interval(refresh_check_interval);
        }

        if let Some(max_jitter) = view.refresh_max_jitter() {
            refresh = refresh.max_jitter(max_jitter);
        }

        let mut builder = AcceleratedTable::builder(
            Arc::clone(&runtime_status),
            table.clone(),
            federated_table.into(),
            "view".to_string(),
            accelerated_table_provider,
            refresh,
        );
        builder.initial_load_complete(initial_load_complete);
        builder.caching(Some(Arc::clone(&self.caching)));
        builder.checkpointer_opt(DatasetCheckpoint::try_new(view).await.ok());
        builder.refresh_on_startup(acceleration.refresh_on_startup);
        builder.ready_state(view.ready_state);
        if acceleration.disable_federation {
            builder.disable_federation();
        }

        if let Some(semaphore) = &self.acceleration_refresh_semaphore {
            builder.refresh_semaphore(Arc::clone(semaphore));
        }

        let accelerated_table =
            builder
                .build()
                .await
                .context(UnableToBuildAcceleratedTableSnafu {
                    dataset_name: table.to_string(),
                })?;

        let is_ready = accelerated_table.refresher().on_complete_notification();

        self.ctx
            .register_table(table.clone(), Arc::new(accelerated_table).table_provider())
            .map_err(|e| Error::UnableToCreateView {
                reason: format!("Failed to registed view: {e}"),
            })?;

        tracing::info!("{}", view_registered_trace(table, Some(acceleration)));

        self.accelerated_tables
            .write()
            .await
            .insert(view.name.clone());

        // if initial load completed, mark view as ready; otherwise, ready status will be updated by acceleration
        if initial_load_complete || view.ready_state == ReadyState::OnRegistration {
            self.runtime_status
                .update_view(&view.name, status::ComponentStatus::Ready);
        }

        Ok(is_ready)
    }

    /// Returns all table names in user defined schemas (i.e. not system or runtime schemas).
    ///
    /// Specifically filters out:
    ///  - `spice.runtime`
    ///  - `spice.metadata`
    ///  - `spice.eval`
    pub fn get_user_table_names(&self) -> Vec<TableReference> {
        self.ctx
            .catalog_names()
            .iter()
            .flat_map(|ctlg| {
                let schemas = self
                    .ctx
                    .catalog(ctlg)
                    .map(|c| c.schema_names())
                    .unwrap_or_default();

                self.ctx
                    .catalog(ctlg)
                    .map(|c| {
                        schemas
                            .iter()
                            .filter(|schema| {
                                !(ctlg == SPICE_DEFAULT_CATALOG && *schema == SPICE_RUNTIME_SCHEMA
                                    || *schema == SPICE_METADATA_SCHEMA
                                    || *schema == SPICE_SCP_SCHEMA
                                    || *schema == SPICE_EVAL_SCHEMA)
                            })
                            .flat_map(|schema| {
                                c.schema(schema)
                                    .map(|s| s.table_names())
                                    .unwrap_or_default()
                                    .iter()
                                    .map(|t| {
                                        TableReference::full(
                                            Arc::from(ctlg.clone()),
                                            Arc::from(schema.clone()),
                                            Arc::from(t.clone()),
                                        )
                                    })
                                    .collect::<Vec<TableReference>>()
                            })
                            .collect::<Vec<TableReference>>()
                    })
                    .unwrap_or_default()
            })
            .collect_vec()
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

    pub fn query_builder<'a>(self: &Arc<Self>, sql: &'a str) -> QueryBuilder<'a> {
        QueryBuilder::new(sql, Arc::clone(self))
    }

    /// Performs `DataFusion` cleanup during shutdown.
    /// Currently performs cleanup of accelerated tables only.
    pub async fn shutdown(&self) {
        // Don't block self.accelerated_tables as it needs to be modified during table removal
        // and will be cleaned up authomatically by removing accelerated tables.
        tracing::debug!("Datafusion shutdown started");

        let accelerated_tables = self.accelerated_tables.read().await.clone();

        for table in &accelerated_tables {
            if let Err(err) = self.remove_table(table).await {
                tracing::error!("Failed to clean up '{table}' during shutdown: {err}");
            }
        }

        self.ctx.deregister_udtf(TEXT_SEARCH_UDTF_NAME);
    }

    /// Create or get a logical plan from the query
    async fn get_or_create_logical_plan(
        &self,
        session: &SessionState,
        key: &RawCacheKey,
        sql: &str,
    ) -> Result<LogicalPlan, DataFusionError> {
        let Some(plans_cache) = self.plans_cache_provider() else {
            return session.create_logical_plan(sql).await;
        };

        if let Some(plan) = plans_cache.get_raw_key(&key.as_u64()).await {
            tracing::trace!("using cached plan for {sql}");
            return Ok(plan);
        }

        let plan = session.create_logical_plan(sql).await?;

        tracing::trace!("caching plan for {sql}");
        plans_cache.put_raw_key(&key.as_u64(), plan.clone()).await;

        Ok(plan)
    }

    pub(crate) fn clear_cached_plans(&self) {
        tracing::trace!("clearing cached logical plans");
        if let Some(cache_provider) = self.plans_cache_provider() {
            cache_provider.invalidate_all();
        }
    }

    fn resolve_catalog_provider(
        &self,
        table_reference: &TableReference,
    ) -> Option<Arc<dyn CatalogProvider>> {
        match table_reference {
            TableReference::Bare { .. } | TableReference::Partial { .. } => {
                self.ctx.catalog(SPICE_DEFAULT_CATALOG)
            }
            TableReference::Full { catalog, .. } => self.ctx.catalog(catalog),
        }
    }

    fn resolve_schema_provider(
        catalog_provider: &Arc<dyn CatalogProvider>,
        table_reference: &TableReference,
    ) -> Option<Arc<dyn SchemaProvider>> {
        match table_reference {
            TableReference::Bare { .. } => catalog_provider.schema(SPICE_DEFAULT_SCHEMA),
            TableReference::Partial { schema, .. } | TableReference::Full { schema, .. } => {
                catalog_provider.schema(schema)
            }
        }
    }
}

#[must_use]
pub fn is_spice_internal_dataset(dataset: &TableReference) -> bool {
    match (dataset.catalog(), dataset.schema()) {
        (Some(catalog), Some(schema)) => is_spice_internal_schema(catalog, schema),
        (None, Some(schema)) => is_spice_internal_schema(SPICE_DEFAULT_CATALOG, schema),
        _ => false,
    }
}

// Normalizes a table reference to a full table reference with catalog, schema, and table name
// so it can be used for comparison.
fn resolve_table_reference(table: TableReference) -> ResolvedTableReference {
    table.resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
}

pub fn handle_accelerated_table_behavior(
    accelerated_table_behaviors: Behaviors,
    federated_table: &FederatedTable,
    component_name: &str,
) -> Result<()> {
    for behavior in accelerated_table_behaviors {
        match behavior {
            Behavior::WantsUnderlyingTableProvider(wants_underlying_table_provider) => {
                if let Some(underlying_provider) = federated_table.try_table_provider_sync() {
                    wants_underlying_table_provider
                        .set(underlying_provider)
                        .map_err(find_datafusion_root)
                        .context(UnableToSetUnderlyingTableProviderSnafu {
                            component_name: component_name.to_string(),
                        })?;
                }
            }
        }
    }

    Ok(())
}

#[must_use]
pub fn is_spice_internal_schema(catalog: &str, schema: &str) -> bool {
    catalog == SPICE_DEFAULT_CATALOG
        && (schema == SPICE_RUNTIME_SCHEMA
            || schema == SPICE_METADATA_SCHEMA
            || schema == SPICE_SCP_SCHEMA
            || schema == SPICE_EVAL_SCHEMA)
}

impl Drop for DataFusion {
    fn drop(&mut self) {
        tracing::debug!("DataFusion resources cleanup");
    }
}

async fn wait_until_dependent_tables_are_ready(
    table: &TableReference,
    dependent_tables: &[TableReference],
    runtime_status: &Arc<status::RuntimeStatus>,
) {
    // Exponential retry with max duration of 10 seconds between retries
    let retry_strategy = FibonacciBackoffBuilder::new()
        .max_retries(None)
        .max_duration(Some(Duration::from_secs(10)))
        .build();
    let dependent_tables = dependent_tables
        .iter()
        .cloned()
        .map(resolve_table_reference)
        .collect::<Vec<_>>();

    let _ = retry(retry_strategy, || async {
        let statuses = runtime_status
            .get_dataset_statuses()
            .into_iter()
            .map(|(key, value)| (resolve_table_reference(key), value))
            .collect::<std::collections::HashMap<_, _>>();

        if let Some(not_ready_table) = dependent_tables.iter().find(|dependent_table| {
            statuses.get(dependent_table) != Some(&status::ComponentStatus::Ready)
        }) {
            tracing::debug!(
                "Dependent table {not_ready_table} is not ready for {table}. Retrying..."
            );

            return Err(RetryError::transient(()));
        }
        Ok(())
    })
    .await;
}

#[cfg(test)]
mod tests {
    use cache::{SimpleCache, key::CacheKey};

    use crate::builder::RuntimeBuilder;

    use super::*;

    #[tokio::test]
    async fn test_get_or_create_logical_plan() {
        static SQL: &str = "SELECT 1";
        let raw_cache_key =
            CacheKey::Query(SQL, None).as_raw_key(Box::new(std::hash::DefaultHasher::new()));

        let runtime = RuntimeBuilder::new().build().await;

        let plan_cache_provider = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::RandomState::default(),
        ));
        let df = Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
            )
            .with_caching(Arc::new(
                Caching::new().with_plans_cache(plan_cache_provider),
            ))
            .build(),
        );

        let session = df.ctx.state();

        df.get_or_create_logical_plan(&session, &raw_cache_key, SQL)
            .await
            .expect("logical plan");

        let Some(cache_provider) = df.plans_cache_provider() else {
            unreachable!("Cache provider should be available");
        };

        cache_provider.checkpoint().await; // Ensure entry gets logged
        assert_eq!(cache_provider.item_count(), 1);
        drop(cache_provider);

        // Reusing the same query should no longer at to the cache
        df.get_or_create_logical_plan(&session, &raw_cache_key, SQL)
            .await
            .expect("logical plan");

        let Some(cache_provider) = df.plans_cache_provider() else {
            unreachable!("Cache provider should be available");
        };
        cache_provider.checkpoint().await; // Ensure entry gets logged
        assert_eq!(cache_provider.item_count(), 1);
    }
}
