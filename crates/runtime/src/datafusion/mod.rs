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
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use crate::accelerated_table::refresh::{self, RefreshOverrides};
use crate::accelerated_table::{
    self, AcceleratedTableBuilderError, SnapshotCreateTrigger, SnapshotCreationConfig,
};
use crate::accelerated_table::{AcceleratedTable, Retention, refresh::Refresh};
use crate::catalogconnector::deferred::DeferredCatalogProvider;
use crate::component::access::AccessMode;
use crate::component::dataset::acceleration::{Acceleration, Engine, RefreshMode};
use crate::component::dataset::{Dataset, ReadyState};
use crate::component::view::View;
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint;
use crate::dataaccelerator::{self, BootstrapStatus};
use crate::dataaccelerator::{AcceleratorEngineRegistry, get_acceleration_layout};
use crate::dataconnector::deferred::DeferredConnector;
use crate::dataconnector::localpod::LOCALPOD_DATACONNECTOR;
use crate::dataconnector::sink::SinkConnector;
use crate::dataconnector::{DataConnector, DataConnectorError};
use crate::datafusion::query::Query;
use crate::dataupdate::{
    DataUpdate, StreamingDataUpdate, StreamingDataUpdateExecutionPlan, UpdateType,
};
use crate::federated_table::FederatedTable;
use crate::search::full_text::udtf::TEXT_SEARCH_UDTF_NAME;
use crate::secrets::Secrets;
use crate::tracing_util::view_registered_trace;
use crate::view::prepare_view;
use crate::{status, view};

use {
    crate::cluster::ResolvedClusterConfig,
    ballista_executor::executor::Executor,
    ballista_scheduler::scheduler_server::SchedulerServer,
    datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode},
};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow_tools::schema::verify_schema;
use builder::DataFusionBuilder;
use cache::TabledCacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use cache::result::search::CachedSearchResult;
use cache::{CacheProvider, Caching, QueryResultsCacheProvider, key::RawCacheKey};
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{ResolvedTableReference, TableReference};
use datafusion_federation::FederatedTableProviderAdaptor;
use error::find_datafusion_root;
use itertools::Itertools;
use query::QueryBuilder;
#[cfg(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "postgres",
    not(windows)
))]
use runtime_acceleration::snapshot::AccelerationEngine;
use runtime_acceleration::snapshot::AccelerationLayout;
#[cfg(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "postgres",
    not(windows)
))]
use runtime_acceleration::snapshot::SnapshotManager;
use runtime_async::ManagedTokioRuntime;
use runtime_datafusion::schema_provider::SpiceSchemaProvider;
use schema::ensure_schema_exists;
use snafu::prelude::*;
use spicepod::acceleration::SnapshotsTrigger;
use spicepod::metric::Metrics;
use tokio::runtime::Handle;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::{RwLock as TokioRwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

pub mod query;

pub mod app_context_extension;
pub mod builder;
pub mod dialect;
pub mod error;
pub mod filter_converter;
pub mod flight_session_extension;
pub mod job_executor_context_extension;
pub mod managed_runtime;
pub mod param_utils;
pub mod refresh_sql;
pub mod request_context_extension;
pub mod retention_sql;
pub mod schema;
pub mod secrets_context_extension;
pub mod sort_columns;
pub(crate) mod sql_validator;
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
    UnableToParseSql { source: DataFusionError },

    #[snafu(display("{source}"))]
    RefreshSql { source: refresh_sql::Error },

    #[snafu(display("{source}"))]
    RetentionSql { source: retention_sql::Error },

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

    #[snafu(display("Failed to refresh the dataset {dataset_name}. {source}"))]
    UnableToTriggerRefresh {
        dataset_name: String,
        source: crate::accelerated_table::Error,
    },

    #[snafu(display(
        "Changing the schema of an accelerated table via the Refresh SQL is not allowed. Retry the request, changing the SELECT statement from 'SELECT {selected_columns}' to 'SELECT {refresh_columns}'"
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

    #[snafu(display("Unable to acquire lock for writable catalogs"))]
    UnableToLockWritableCatalogs {},

    #[snafu(display("Unable to acquire lock for cluster scheduler state"))]
    UnableToLockWritableSchedulerHandle {},

    #[snafu(display("Unable to acquire lock for cluster scheduler state"))]
    UnableToLockWritableExecutorHandle {},

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
        "Acceleration mode `append` requires `time_column` parameter for source {from}. Configure `time_column` parameter and try again. For details, visit: https://spiceai.org/docs/reference/spicepod/datasets#time_column"
    ))]
    AppendRequiresTimeColumn { from: String },

    #[snafu(display(
        "Failed to create an accelerated table for dataset {dataset_name} ({connector}): `refresh_mode: caching` is only supported with the HTTP/HTTPS or localpod data connectors. See https://spiceai.org/docs/features/data-acceleration/refresh-modes/caching"
    ))]
    InvalidCachingRefreshMode {
        dataset_name: String,
        connector: String,
    },

    #[snafu(display(
        "Conflicting stale-while-revalidate settings for dataset {dataset_name}. When using `refresh_mode: caching`, set either acceleration `caching_stale_while_revalidate_ttl` or results cache `stale_while_revalidate_ttl`, but not both."
    ))]
    ConflictingStaleWhileRevalidateConfig { dataset_name: String },

    #[snafu(display("Unable to retrieve underlying table provider from federation"))]
    UnableToRetrieveTableFromFederation { table_name: String },

    #[snafu(display(
        "Failed to create an accelerated table for the dataset {dataset_name}. {source}"
    ))]
    UnableToBuildAcceleratedTable {
        dataset_name: String,
        source: AcceleratedTableBuilderError,
    },

    #[snafu(display(
        "Failed to create an accelerated table for {component_name}. Error setting the underlying table provider: {source}"
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

    #[snafu(display("Invalid snapshots_trigger_threshold value: expected time interval"))]
    InvalidSnapshotCreationInterval { source: fundu::ParseError },

    #[snafu(display("Invalid snapshots_trigger_threshold value: expected integer"))]
    InvalidSnapshotCreationBatches { source: std::num::ParseIntError },

    #[snafu(display("snapshots_trigger_threshold value should be positive integer"))]
    SnapshotCreationBatchesShouldBePositive,

    #[snafu(display(
        "'stream_batches' is not supported for batch-backed datasets. Use 'refresh_complete' or 'time_interval' instead"
    ))]
    UnsupportedStreamBatchesForBatchRefresh,

    #[snafu(display(
        "'refresh_complete' is not supported for stream-backed datasets. Use 'time_interval' or 'stream_batches' instead"
    ))]
    UnsupportedRefreshCompleteForStream,

    #[snafu(display("Caching refresh mode only supports 'time_interval' for snapshots_trigger"))]
    UnsupportedSnapshotTriggerForCaching,

    #[snafu(display(
        "Invalid snapshot configuration: Only DuckDB, Turso and SQlite support snapshots"
    ))]
    UnsupportedAccelerationEngineForSnapshots,
}

const DEFAULT_SNAPSHOT_CREATION_INTERVAL: Duration = Duration::from_mins(10);
const DEFAULT_SNAPSHOT_CREATION_BATCHES: i64 = 100;

pub enum Table {
    Accelerated {
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        accelerated_table: Option<Arc<AcceleratedTable>>,
        secrets: Arc<TokioRwLock<Secrets>>,
        bootstrap_status: BootstrapStatus,
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
    writable_catalogs: RwLock<HashSet<String>>,
    accelerated_tables: TokioRwLock<HashSet<TableReference>>,
    caching: Arc<Caching>,
    pending_sink_tables: TokioRwLock<Vec<PendingSinkRegistration>>,
    deferred_tables: TokioRwLock<HashMap<String, DeferredTableRegistration>>,
    deferred_catalogs: TokioRwLock<HashMap<String, Arc<DeferredCatalogProvider>>>,

    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    // Controls the parallelism of accelerated table refreshes
    acceleration_refresh_semaphore: Option<Arc<Semaphore>>,
    pub(crate) task_history_enabled: bool,
    // Dedicated runtime for CPU-bound DataFusion queries
    cpu_runtime: OnceLock<ManagedTokioRuntime>,
    // Dedicated runtime for CPU-bound DataFusion acceleration for dataset acceleration refresh tasks
    refresh_runtime: OnceLock<ManagedTokioRuntime>,
    io_runtime: Handle,
    metrics: Option<Metrics>,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,

    pub temp_directory: Option<String>,
    pub cluster_config: Arc<ResolvedClusterConfig>,
    pub scheduler_server: RwLock<Option<Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>>>,
    pub executor: RwLock<Option<Arc<Executor>>>,
}

impl std::fmt::Debug for DataFusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusion")
            .field("runtime_status", &self.runtime_status)
            .field("data_writers", &self.data_writers)
            .field("writable_catalogs", &self.writable_catalogs)
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
        io_runtime: Handle,
    ) -> DataFusionBuilder {
        DataFusionBuilder::new(status, accelerator_engine_registry, io_runtime)
    }

    #[must_use]
    pub fn runtime_status(&self) -> Arc<status::RuntimeStatus> {
        Arc::clone(&self.runtime_status)
    }

    #[must_use]
    pub fn caching(&self) -> Arc<Caching> {
        Arc::clone(&self.caching)
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
        if let Some(schema) = table_name.schema()
            && let Some(eval_schema) = self.schema(schema)
        {
            eval_schema
                .register_table(table_name.table().to_string(), table)
                .map_err(find_datafusion_root)
                .context(UnableToRegisterTableToDataFusionSchemaSnafu {
                    schema: SPICE_EVAL_SCHEMA,
                })?;
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
        access: &AccessMode,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Result<()> {
        if let Some(deferred_catalog) = catalog.as_any().downcast_ref::<DeferredCatalogProvider>() {
            self.deferred_catalogs
                .write()
                .await
                .insert(name.to_string(), Arc::new(deferred_catalog.clone()));
        } else {
            self.ctx.register_catalog(name, catalog);

            if matches!(access, AccessMode::ReadWrite) {
                self.mark_catalog_writable(name)?;
            }
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

        let dataset_access_mode = dataset.access();
        let dataset_table_ref = dataset.name.clone();

        let is_ready = match table {
            Table::Accelerated {
                source,
                federated_read_table,
                accelerated_table,
                secrets,
                bootstrap_status,
            } => {
                if let Some(accelerated_table) = accelerated_table {
                    tracing::debug!(
                        "Registering dataset {dataset:?} with preloaded accelerated table"
                    );
                    let notifier = accelerated_table.refresher().on_complete_notification();
                    self.ctx
                        .register_table(
                            dataset_table_ref.clone(),
                            accelerated_table.table_provider(),
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
                    self.register_accelerated_table(
                        dataset,
                        source,
                        federated_read_table,
                        secrets,
                        bootstrap_status,
                    )
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

        if matches!(dataset_access_mode, AccessMode::ReadWrite) {
            self.mark_dataset_writable(&dataset_table_ref)?;
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
    pub fn is_catalog_writable(&self, catalog_name: &str) -> bool {
        if let Ok(writable_catalogs) = self.writable_catalogs.read() {
            writable_catalogs.contains(catalog_name)
        } else {
            false
        }
    }

    pub fn mark_catalog_writable(&self, catalog_name: &str) -> Result<()> {
        tracing::warn!(
            "Access mode 'read_write' is enabled for catalog {catalog_name}. This feature is currently in preview."
        );
        self.writable_catalogs
            .write()
            .map_err(|_| Error::UnableToLockWritableCatalogs {})?
            .insert(catalog_name.to_string());
        Ok(())
    }

    pub fn mark_dataset_writable(&self, dataset_name: &TableReference) -> Result<()> {
        tracing::warn!(
            "Access mode 'read_write' is enabled for dataset {dataset_name}. This feature is currently in preview."
        );
        self.data_writers
            .write()
            .map_err(|_| Error::UnableToLockDataWriters {})?
            .insert(dataset_name.clone());
        Ok(())
    }

    #[must_use]
    pub async fn is_accelerated(&self, table_reference: &TableReference) -> bool {
        self.accelerated_tables
            .read()
            .await
            .contains(table_reference)
    }

    pub fn set_cpu_runtime(&self, handle: ManagedTokioRuntime) {
        if self.cpu_runtime.set(handle).is_err() {
            // Failure to set means this was already set - that shouldn't happen.
            tracing::error!(
                "Failed to set cpu tokio runtime on the Datafusion struct, this is an unexpected internal error"
            );
        }
    }

    #[must_use]
    pub fn cpu_runtime(&self) -> Option<&tokio::runtime::Handle> {
        self.cpu_runtime.get().map(ManagedTokioRuntime::handle)
    }

    /// Set the dedicated refresh runtime for acceleration refresh workers.
    /// This runtime is isolated from the query runtime to prevent refresh workloads from impacting query latency.
    pub fn set_refresh_runtime(&self, handle: ManagedTokioRuntime) {
        if self.refresh_runtime.set(handle).is_err() {
            // Failure to set means this was already set - that shouldn't happen.
            tracing::error!(
                "Failed to set refresh tokio runtime on the Datafusion struct, this is an unexpected internal error"
            );
        }
    }

    /// Returns the dedicated refresh runtime for acceleration refresh workers.
    /// Falls back to `cpu_runtime()` if no dedicated refresh runtime is set.
    #[must_use]
    pub fn refresh_runtime(&self) -> Option<&tokio::runtime::Handle> {
        self.refresh_runtime
            .get()
            .map(ManagedTokioRuntime::handle)
            .or_else(|| self.cpu_runtime())
    }

    async fn get_table_provider(
        &self,
        table_reference: &TableReference,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_reference.table();

        if let Some(schema_name) = table_reference.schema()
            && let Some(schema) = self.schema(schema_name)
        {
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

    pub async fn load_deferred_catalog(&self, name: &str, access: &AccessMode) -> Result<()> {
        let deferred_catalogs = self.deferred_catalogs.read().await;
        if let Some(catalog) = deferred_catalogs.get(name) {
            if let Ok(provider) = catalog.get_catalog_provider().await {
                self.ctx.register_catalog(name, Arc::clone(&provider));
                if matches!(access, AccessMode::ReadWrite) {
                    self.mark_catalog_writable(name)?;
                }
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
            "Dataset {} loading data...",
            pending_registration.dataset.name
        );
        self.register_accelerated_table(
            Arc::clone(&pending_registration.dataset),
            sink_connector,
            federated_table,
            Arc::clone(&pending_registration.secrets),
            BootstrapStatus::none(), // Sink datasets don't bootstrap from snapshots
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
        Ok(data_frame.schema().as_arrow().clone())
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

    pub async fn create_accelerated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        secrets: Arc<TokioRwLock<Secrets>>,
        bootstrap_status: BootstrapStatus,
    ) -> Result<AcceleratedTable> {
        tracing::trace!("Creating accelerated table {dataset:?}");

        // For accelerated tables with on_conflict configured, the source doesn't need
        // to support writes - writes go to the accelerated table only.
        // Only require a read-write source when replication is enabled and no on_conflict
        // is configured (writes need to go back to the source).
        let has_on_conflict = dataset
            .acceleration
            .as_ref()
            .is_some_and(|acc| !acc.on_conflict.is_empty());
        let needs_source_writes = dataset.access() == AccessMode::ReadWrite && !has_on_conflict;

        let source_table_provider = if needs_source_writes {
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
        } else {
            Arc::new(federated_read_table)
        };

        let source_schema = source_table_provider.schema();

        let acceleration_settings =
            dataset
                .acceleration
                .clone()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: dataset.name.to_string(),
                })?;

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

        let refresh_mode = source.resolve_refresh_mode(acceleration_settings.refresh_mode);
        if refresh_mode == RefreshMode::Caching {
            let connector = dataset.source();
            let is_http_connector =
                connector.eq_ignore_ascii_case("http") || connector.eq_ignore_ascii_case("https");
            let is_localpod_connector = connector.eq_ignore_ascii_case(LOCALPOD_DATACONNECTOR);
            ensure!(
                is_http_connector || is_localpod_connector,
                InvalidCachingRefreshModeSnafu {
                    dataset_name: dataset.name.to_string(),
                    connector: connector.to_string(),
                }
            );
        }

        // Determine if we should pass constraints to the accelerator
        // Only pass constraints if not using refresh_sql (schema might have different column ordering)
        //
        // For caching mode with DuckDB/Cayenne: constraints enable upsert behavior
        // For caching mode with Arrow: constraints are required for InsertOp::Replace to work correctly
        let use_constraints = refresh_sql.is_none();

        let constraints = if use_constraints {
            match &*source_table_provider {
                FederatedTable::Immediate(table_provider) => table_provider.constraints(),
                FederatedTable::Deferred(_) => None,
            }
        } else {
            None
        };

        let accelerated_table_provider = self
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

        // If we already have an existing dataset checkpoint table that has been checkpointed,
        // it means there is data from a previous acceleration and we don't need
        // to wait for the first refresh to complete to mark it ready.
        // For caching mode, we always start ready since it fetches data on-demand.
        let mut initial_load_complete = matches!(refresh_mode, RefreshMode::Caching);
        if initial_load_complete {
            // Caching mode datasets are always ready immediately
            self.runtime_status
                .update_dataset(&dataset.name, status::ComponentStatus::Ready);
        } else if let Ok(checkpoint) =
            DatasetCheckpoint::try_new(dataset, OpenOption::OpenExisting).await
            && checkpoint.exists().await
        {
            // For append refreshes that rely on a time column (i.e. file-based appends) that have
            // snapshotting enabled, we delay readiness until the first refresh completes so that
            // the append window is initialized with newly ingested data rather than pre-existing checkpoint files.
            let delay_initial_ready = matches!(refresh_mode, RefreshMode::Append)
                && dataset.time_column.is_some()
                && acceleration_settings.snapshot_behavior.bootstrap_enabled();

            if !delay_initial_ready {
                self.runtime_status
                    .update_dataset(&dataset.name, status::ComponentStatus::Ready);
                initial_load_complete = true;
            }
        }

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
        if let Some(caching_ttl) = acceleration_settings.caching_ttl {
            refresh = refresh.caching_ttl(caching_ttl);
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
            self.io_runtime.clone(),
        );
        accelerated_table_builder.cpu_runtime(self.refresh_runtime().cloned());

        let retention_delete_expr = match dataset.retention_sql() {
            Some(retention_sql) => {
                let parsed = retention_sql::parse_retention_sql(
                    &dataset.name,
                    retention_sql.as_str(),
                    source_table_provider.schema(),
                )
                .context(RetentionSqlSnafu)?;

                Some(parsed.delete_expr)
            }
            None => None,
        };

        let retention = Retention::builder()
            .time_column(dataset.time_column.clone())
            .time_format(dataset.time_format)
            .time_partition_column(dataset.time_partition_column.clone())
            .time_partition_format(dataset.time_partition_format)
            .time_period(dataset.retention_period())
            .check_interval(dataset.retention_check_interval())
            .enabled(acceleration_settings.retention_check_enabled)
            .delete_expr(retention_delete_expr)
            .build();

        accelerated_table_builder.retention(retention);

        accelerated_table_builder
            .zero_results_action(acceleration_settings.on_zero_results.clone());

        accelerated_table_builder.refresh_on_startup(acceleration_settings.refresh_on_startup);

        accelerated_table_builder.ready_state(dataset.ready_state);

        accelerated_table_builder.caching(Some(Arc::clone(&self.caching)));

        // For caching mode, set the TTL (max_age) and stale_while_revalidate from params
        if refresh_mode == RefreshMode::Caching {
            // Check for conflicting stale_while_revalidate configuration
            if acceleration_settings
                .caching_stale_while_revalidate_ttl
                .is_some()
                && let Some(results_cache) = &self.caching.results
            {
                ensure!(
                    results_cache.stale_while_revalidate_ttl().is_none(),
                    ConflictingStaleWhileRevalidateConfigSnafu {
                        dataset_name: dataset.name.to_string(),
                    }
                );
            }

            accelerated_table_builder.caching_ttl(acceleration_settings.caching_ttl);
            accelerated_table_builder.caching_stale_while_revalidate_ttl(
                acceleration_settings.caching_stale_while_revalidate_ttl,
            );
            accelerated_table_builder
                .caching_stale_if_error(acceleration_settings.caching_stale_if_error.is_enabled());
        }

        // Get the acceleration layout (used for snapshots and size metrics)
        let acceleration_layout = get_acceleration_layout(dataset).await.ok();

        if acceleration_settings.snapshot_behavior.create_enabled() {
            if let Some(ref layout) = acceleration_layout {
                if layout.is_enabled() {
                    if let Some(snapshot_config) = build_snapshot_creation_config(
                        dataset,
                        &acceleration_settings,
                        refresh_mode,
                        layout.clone(),
                    )
                    .await?
                    {
                        accelerated_table_builder.snapshot_creation_config(Some(snapshot_config));
                    }
                } else {
                    tracing::warn!(
                        "Dataset {} accelerator does not support snapshots.",
                        dataset.name
                    );
                }
            } else {
                tracing::warn!(
                    "Dataset {} is not file accelerated. Snapshot creation is not supported.",
                    dataset.name
                );
            }
        }

        // Pass the acceleration layout for size metrics
        if let Some(layout) = acceleration_layout {
            accelerated_table_builder.acceleration_layout(layout);
        }

        accelerated_table_builder.checkpointer_opt(
            DatasetCheckpoint::try_new(dataset, OpenOption::CreateIfNotExists)
                .await
                .map(|checkpoint| {
                    checkpoint
                        .with_snapshot_behavior(acceleration_settings.snapshot_behavior)
                        .to_arc()
                })
                .ok(),
        );

        accelerated_table_builder.initial_load_complete(initial_load_complete);

        // Caching mode requires federation to be disabled so that queries go through
        // AcceleratedTable::scan to trigger the cache miss/hit logic
        if acceleration_settings.disable_federation || matches!(refresh_mode, RefreshMode::Caching)
        {
            accelerated_table_builder.disable_federation();
        }

        if let Some(semaphore) = &self.acceleration_refresh_semaphore {
            accelerated_table_builder.refresh_semaphore(Arc::clone(semaphore));
        }

        if let Some(ref resource_monitor) = self.resource_monitor {
            accelerated_table_builder.with_resource_monitor(resource_monitor.clone());
        }

        if let Some(metrics) = &self.metrics {
            accelerated_table_builder.metrics(metrics.clone());
        }

        if refresh_mode == RefreshMode::Changes {
            let changes_stream = source.changes_stream(Arc::clone(&source_table_provider), dataset);

            if let Some(changes_stream) = changes_stream {
                accelerated_table_builder.changes_stream(changes_stream);
            }
        }

        // For append mode without time_column, check if source provides append_stream
        // Skip this check for Cayenne which has its own validation (supports primary_key or time_column)
        if refresh_mode == RefreshMode::Append
            && dataset.time_column.is_none()
            && acceleration_settings.engine != Engine::Cayenne
        {
            if let Some(append_stream) = source.append_stream(source_table_provider) {
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

        // When on_conflict is configured, writes go to the accelerated table only,
        // not to the federated source (which may not support writes).
        if has_on_conflict {
            accelerated_table_builder.write_to_accelerator_only();
        }

        accelerated_table_builder.bootstrap_status(bootstrap_status);

        // Check if this is an S3 Express One Zone acceleration (Cayenne with S3 Express config)
        // This is used for better error messages when S3 Express upload fails
        let is_s3_express_acceleration = acceleration_settings.engine == Engine::Cayenne
            && (acceleration_settings
                .params
                .get("cayenne_file_path")
                .is_some_and(|path| {
                    crate::dataaccelerator::cayenne::CayenneAccelerator::is_s3_express_path(path)
                })
                || acceleration_settings
                    .params
                    .contains_key("cayenne_s3_zone_ids"));
        accelerated_table_builder.s3_express_acceleration(is_s3_express_acceleration);

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
    /// - The parent and child acceleration modes don't match (both must be Full or both must be Caching).
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
    ) -> Option<Arc<dyn TabledCacheProvider<LogicalPlan> + Send + Sync>> {
        self.caching.plans.clone()
    }

    pub fn embeddings_cache_provider(
        &self,
    ) -> Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>> {
        self.caching.embeddings.clone()
    }

    pub fn search_cache_provider(
        &self,
    ) -> Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>> {
        self.caching.search.clone()
    }

    async fn register_accelerated_table(
        &self,
        dataset: Arc<Dataset>,
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        secrets: Arc<TokioRwLock<Secrets>>,
        bootstrap_status: BootstrapStatus,
    ) -> Result<Option<Arc<Notify>>> {
        let mut accelerated_table = self
            .create_accelerated_table(
                &dataset,
                Arc::clone(&source),
                federated_read_table,
                secrets,
                bootstrap_status,
            )
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

        let source_table_provider = match dataset.access() {
            AccessMode::Read => federated_table_provider,
            AccessMode::ReadWrite => source
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
        if self.ctx.table_exist(view.name.clone()).unwrap_or(false) {
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
                ),
            }
            .fail();
        }

        let ctx = Arc::clone(&self.ctx);
        let df_ref = Arc::clone(self);
        let dependent_table_names = view::get_dependent_table_names(&statements[0]);
        let status = self.runtime_status();

        let table = view.name.clone();
        tracing::debug!("Creating view {table} with dependent tables {dependent_table_names:?}");

        let register_task: JoinHandle<Option<Arc<Notify>>> = spawn(async move {
            // Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
            // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
            // the tables are created. To handle this, wait until all tables are created.

            let deadline = Instant::now() + Duration::from_secs(60);
            let mut unresolved_dependent_table: Option<TableReference> = None;

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
                if attempts > 0 {
                    tracing::info!("Dependent table {dependent_table_name} for view {table} found");
                }
            }

            if let Some(missing_table) = unresolved_dependent_table {
                tracing::error!(
                    "Failed to create view {table}. Dependent table {missing_table} does not exist."
                );
                status.update_view(&table, status::ComponentStatus::Error);
                return None;
            }

            // If view depends on other tables, wait until they are ready
            wait_until_dependent_tables_are_ready(&table, &dependent_table_names, &status).await;

            let tbl_provider = match prepare_view(&ctx, &statements[0], &view).await {
                Ok(tbl) => tbl,
                Err(e) => {
                    tracing::error!("Failed to create view {table}: {e}");
                    status.update_view(&table, status::ComponentStatus::Error);
                    return None;
                }
            };
            if let Some(acceleration) = &view.acceleration
                && acceleration.enabled
            {
                match df_ref
                    .create_accelerated_view(&view, tbl_provider, secrets)
                    .await
                {
                    Ok(is_ready) => {
                        return is_ready;
                    }
                    Err(e) => {
                        tracing::error!("Failed to create view {table}: {e}");
                        status.update_view(&table, status::ComponentStatus::Error);
                        return None;
                    }
                }
            }

            // non-accelerated view
            if let Err(e) = ctx.register_table(table.clone(), tbl_provider) {
                tracing::error!("Failed to create view {table}: {e}");
                status.update_view(&table, status::ComponentStatus::Error);
                return None;
            }
            tracing::info!("{}", view_registered_trace(&table, None));
            status.update_view(&table, status::ComponentStatus::Ready);

            None
        });

        Ok(register_task)
    }

    pub async fn create_accelerated_view(
        self: &Arc<Self>,
        view: &View,
        view_table: Arc<dyn TableProvider>,
        secrets: Arc<TokioRwLock<Secrets>>,
    ) -> Result<Option<Arc<Notify>>> {
        let table = &view.name;

        let acceleration =
            view.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: table.to_string(),
                })?;

        let schema = view_table.schema();

        let accelerated_table_provider = self
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

        // Detect if data for view was already loaded so we don't need to wait for the first refresh to complete to mark it as ready.
        let mut initial_load_complete = false;
        if let Ok(checkpoint) = DatasetCheckpoint::try_new(view, OpenOption::OpenExisting).await
            && checkpoint.exists().await
        {
            initial_load_complete = true;
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
            self.runtime_status(),
            table.clone(),
            Arc::new(FederatedTable::new_unchecked(view_table)),
            "view".to_string(),
            accelerated_table_provider,
            refresh,
            self.io_runtime.clone(),
        );
        builder.cpu_runtime(self.refresh_runtime().cloned());
        builder.initial_load_complete(initial_load_complete);
        builder.caching(Some(Arc::clone(&self.caching)));
        builder.checkpointer_opt(
            DatasetCheckpoint::try_new(view, OpenOption::CreateIfNotExists)
                .await
                .map(|checkpoint| {
                    checkpoint
                        .with_snapshot_behavior(acceleration.snapshot_behavior.clone())
                        .to_arc()
                })
                .ok(),
        );
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

    /// Create a [`Query`] based on a constructed [`LogicalPlan`].
    ///
    /// The `plan` should be valid, constructed off the [`DataFusion`]'s [`SessionContext`].
    pub fn query_from_logical_plan(self: &Arc<Self>, plan: &LogicalPlan) -> Query {
        Query::from_logical_plan(self, plan)
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

    pub(crate) async fn clear_cached_plans(&self) {
        tracing::trace!("clearing cached logical plans");
        if let Some(cache_provider) = self.plans_cache_provider() {
            cache_provider.invalidate_all().await;
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

    pub fn bind_scheduler_server(
        &self,
        server: Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>,
    ) -> Result<()> {
        let mut scheduler_server = self
            .scheduler_server
            .try_write()
            .map_err(|_| Error::UnableToLockWritableSchedulerHandle {})?;
        *scheduler_server = Some(server);
        Ok(())
    }

    pub fn bind_executor(&self, executor: Arc<Executor>) -> Result<()> {
        let mut executor_handle = self
            .executor
            .try_write()
            .map_err(|_| Error::UnableToLockWritableExecutorHandle {})?;
        *executor_handle = Some(executor);
        Ok(())
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

pub(crate) fn resolved_equality(a: TableReference, b: TableReference) -> bool {
    resolve_table_reference(a) == resolve_table_reference(b)
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
    tracing::debug!(
        "Waiting for dependent tables {dependent_tables:?} to be ready for {table}",
        table = table
    );

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
        let mut table_statuses = runtime_status.get_dataset_statuses();
        table_statuses.extend(runtime_status.get_view_statuses());
        let statuses = table_statuses
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

async fn build_snapshot_creation_config(
    dataset: &Dataset,
    acceleration_settings: &Acceleration,
    refresh_mode: RefreshMode,
    acceleration_layout: AccelerationLayout,
) -> Result<Option<SnapshotCreationConfig>> {
    let is_streaming_refresh = matches!(refresh_mode, RefreshMode::Changes)
        || (matches!(refresh_mode, RefreshMode::Append) && dataset.time_column.is_none());
    let snapshot_trigger = &acceleration_settings.snapshots_trigger;
    let snapshot_threshold: Option<String> =
        acceleration_settings.snapshots_trigger_threshold.clone();

    let parse_interval = |threshold: &Option<String>| -> Result<Duration> {
        match threshold {
            Some(s) => {
                // Check if string contains a valid time unit
                if !s.chars().any(char::is_alphabetic) {
                    return Err(Error::InvalidSnapshotCreationInterval {
                        source: fundu::ParseError::InvalidInput(
                            "duration must include a unit (e.g., ms, s, m, h)".into(),
                        ),
                    });
                }
                fundu::parse_duration(s).context(InvalidSnapshotCreationIntervalSnafu)
            }
            None => Ok(DEFAULT_SNAPSHOT_CREATION_INTERVAL),
        }
    };

    let parse_batches = |threshold: &Option<String>| -> Result<i64> {
        match threshold {
            Some(s) => {
                let batches = s
                    .parse::<i64>()
                    .context(InvalidSnapshotCreationBatchesSnafu)?;
                if batches <= 0 {
                    SnapshotCreationBatchesShouldBePositiveSnafu.fail()
                } else {
                    Ok(batches)
                }
            }
            None => Ok(DEFAULT_SNAPSHOT_CREATION_BATCHES),
        }
    };

    // Caching mode only supports time_interval - no "refresh complete" or "stream_batches" events.
    let is_caching = matches!(refresh_mode, RefreshMode::Caching);

    let snapshot_creation_trigger = if is_caching {
        match snapshot_trigger {
            None | Some(SnapshotsTrigger::TimeInterval) => {
                let interval = parse_interval(&snapshot_threshold)?;
                SnapshotCreateTrigger::Interval(interval)
            }
            Some(SnapshotsTrigger::RefreshComplete | SnapshotsTrigger::StreamBatches) => {
                return Err(Error::UnsupportedSnapshotTriggerForCaching);
            }
        }
    } else if is_streaming_refresh {
        match snapshot_trigger {
            None | Some(SnapshotsTrigger::TimeInterval) => {
                let interval = parse_interval(&snapshot_threshold)?;
                SnapshotCreateTrigger::Interval(interval)
            }
            Some(SnapshotsTrigger::RefreshComplete) => {
                return Err(Error::UnsupportedRefreshCompleteForStream);
            }
            Some(SnapshotsTrigger::StreamBatches) => {
                let batches = parse_batches(&snapshot_threshold)?;
                SnapshotCreateTrigger::Batches(batches)
            }
        }
    } else {
        match snapshot_trigger {
            None | Some(SnapshotsTrigger::RefreshComplete) => {
                SnapshotCreateTrigger::RefreshComplete
            }
            Some(SnapshotsTrigger::TimeInterval) => {
                let interval = parse_interval(&snapshot_threshold)?;
                SnapshotCreateTrigger::Interval(interval)
            }
            Some(SnapshotsTrigger::StreamBatches) => {
                return Err(Error::UnsupportedStreamBatchesForBatchRefresh);
            }
        }
    };

    #[cfg(any(
        feature = "duckdb",
        feature = "sqlite",
        feature = "postgres",
        not(windows)
    ))]
    let acceleration_engine = match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => AccelerationEngine::DuckDB,
        #[cfg(feature = "duckdb")]
        Engine::TableModePartitionedDuckDB => AccelerationEngine::DuckDB,
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => AccelerationEngine::Sqlite,
        #[cfg(feature = "turso")]
        Engine::Turso => AccelerationEngine::Turso,
        #[cfg(not(windows))]
        Engine::Cayenne => AccelerationEngine::Cayenne,
        _ => {
            // This code is unreachable since build_snapshot_creation_config is
            // only called iff acceleration_file_path returned Some(<file_path>)
            return Err(Error::UnsupportedAccelerationEngineForSnapshots);
        }
    };

    #[cfg(not(any(
        feature = "duckdb",
        feature = "sqlite",
        feature = "postgres",
        not(windows)
    )))]
    {
        let _ = acceleration_layout;
        let _ = snapshot_creation_trigger;
        return Err(Error::UnsupportedAccelerationEngineForSnapshots);
    }

    #[cfg(any(
        feature = "duckdb",
        feature = "sqlite",
        feature = "postgres",
        not(windows)
    ))]
    Ok(SnapshotManager::try_new(
        dataset.name.to_string(),
        acceleration_settings.snapshot_behavior.clone(),
        acceleration_layout,
        acceleration_engine,
    )
    .await
    .map(|sm| {
        let sm = sm.with_snapshots_creation_policy(acceleration_settings.snapshots_creation_policy);
        SnapshotCreationConfig::new(Arc::new(sm), snapshot_creation_trigger)
    }))
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
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ));
        let df = Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                Handle::current(),
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
        assert_eq!(cache_provider.item_count().await, 1);
        drop(cache_provider);

        // Reusing the same query should no longer at to the cache
        df.get_or_create_logical_plan(&session, &raw_cache_key, SQL)
            .await
            .expect("logical plan");

        let Some(cache_provider) = df.plans_cache_provider() else {
            unreachable!("Cache provider should be available");
        };
        cache_provider.checkpoint().await; // Ensure entry gets logged
        assert_eq!(cache_provider.item_count().await, 1);
    }

    #[cfg(all(feature = "duckdb", feature = "snapshots",))]
    mod build_snapshot_creation_config_tests {
        use super::*;
        use crate::component::dataset::Dataset;
        use crate::component::dataset::acceleration::{Acceleration, RefreshMode};
        use runtime_acceleration::snapshot::SnapshotBehavior;
        use spicepod::acceleration::{SnapshotsCompaction, SnapshotsTrigger};
        use spicepod::component::snapshot::Snapshots;
        use std::sync::Arc;
        use tempfile::TempDir;

        async fn create_test_dataset(time_column: Option<String>) -> Dataset {
            let runtime = crate::Runtime::builder().build().await;
            Dataset {
                from: "test".to_string(),
                name: TableReference::bare("test_dataset"),
                access: AccessMode::Read,
                params: HashMap::new(),
                metadata: HashMap::new(),
                columns: vec![],
                has_metadata_table: false,
                replication: None,
                time_column,
                time_format: None,
                time_partition_column: None,
                time_partition_format: None,
                acceleration: None,
                embeddings: vec![],
                app: Arc::new(app::App::default()),
                unsupported_type_action: None,
                ready_state: ReadyState::OnRegistration,
                metrics: Metrics::default(),
                runtime: Arc::new(runtime),
                vectors: None,
                check_availability: crate::component::dataset::CheckAvailability::Disabled,
            }
        }

        fn create_snapshots_behavior(
            location: Option<String>,
            secrets: &Arc<TokioRwLock<Secrets>>,
        ) -> SnapshotBehavior {
            SnapshotBehavior::enabled(
                Arc::new(Snapshots {
                    location,
                    enabled: true,
                    ..Snapshots::default()
                }),
                Arc::downgrade(secrets),
                Handle::current(),
                SnapshotsCompaction::Enabled,
            )
        }

        fn create_acceleration_with_trigger(
            snapshot_location: Option<String>,
            engine: Engine,
            trigger: Option<SnapshotsTrigger>,
            threshold: Option<String>,
            secrets: &Arc<TokioRwLock<Secrets>>,
        ) -> Acceleration {
            Acceleration {
                snapshot_behavior: create_snapshots_behavior(snapshot_location, secrets),
                engine,
                snapshots_trigger: trigger,
                snapshots_trigger_threshold: threshold,
                ..Default::default()
            }
        }

        #[tokio::test]
        async fn test_default() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                None,
                Engine::DuckDB,
                None,
                None,
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Full,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            assert!(result.expect("config should exist").is_none());
        }

        #[tokio::test]
        async fn test_stream_batches_for_append_streaming_mode() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                Some("file:///tmp".to_string()),
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                Some("25".to_string()),
                &dataset.runtime().secrets(),
            );

            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");
            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Append,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            // StreamBatches should work for streaming mode
            assert!(
                result.is_ok(),
                "Expected Ok for streaming with StreamBatches, got: {result:?}",
            );
            let config = result
                .expect("config should exist")
                .expect("config should be Some");
            match config.create_trigger {
                SnapshotCreateTrigger::Batches(count) => {
                    assert_eq!(count, 25, "Expected 25 batches");
                }
                other => panic!("Expected Batches trigger, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_stream_batches_for_changes_streaming_mode() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                Some("file:///tmp".to_string()),
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                Some("25".to_string()),
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Changes,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            // StreamBatches should work for streaming mode
            assert!(
                result.is_ok(),
                "Expected Ok for streaming with StreamBatches, got: {result:?}",
            );
            let config = result
                .expect("config should exist")
                .expect("config should be Some");
            match config.create_trigger {
                SnapshotCreateTrigger::Batches(count) => {
                    assert_eq!(count, 25, "Expected 25 batches");
                }
                other => panic!("Expected Batches trigger, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_stream_batches_unsupported_for_full_refresh_mode() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                Some("file:///tmp".to_string()),
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                None,
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Full,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            // RefreshComplete should fail for streaming mode
            assert!(
                result.is_err(),
                "Expected error: Full + time_column should be streaming"
            );
            assert!(
                matches!(result, Err(Error::UnsupportedStreamBatchesForBatchRefresh)),
                "Expected UnsupportedRefreshCompleteForStream error, got: {result:?}",
            );
        }

        #[tokio::test]
        async fn test_stream_batches_unsupported_for_batch_append_refresh_mode() {
            let dataset = create_test_dataset(Some("created_at".to_string())).await;
            let acceleration = create_acceleration_with_trigger(
                Some("file:///tmp".to_string()),
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                None,
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Append,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            // RefreshComplete should fail for streaming mode
            assert!(
                result.is_err(),
                "Expected error: Full + time_column should be streaming"
            );
            assert!(
                matches!(result, Err(Error::UnsupportedStreamBatchesForBatchRefresh)),
                "Expected UnsupportedRefreshCompleteForStream error, got: {result:?}",
            );
        }

        #[tokio::test]
        async fn test_stream_batches_for_stream_append_refresh_mode() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                Some("file:///tmp".to_string()),
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                None,
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Append,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            let config = result
                .expect("config should exist")
                .expect("config should be Some");
            match config.create_trigger {
                SnapshotCreateTrigger::Batches(count) => {
                    assert_eq!(count, 100, "Expected 25 batches");
                }
                other => panic!("Expected Batches trigger, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn test_negative_batch_count() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                None,
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                Some("-10".to_string()),
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Append,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            assert!(result.is_err(), "Empty string should fail interval parsing");
            assert!(
                matches!(result, Err(Error::SnapshotCreationBatchesShouldBePositive)),
                "Expected SnapshotCreationBatchesShouldBePositive error, got: {result:?}"
            );
        }

        #[tokio::test]
        async fn test_zero_batch_count() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                None,
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                Some("0".to_string()),
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Append,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            assert!(result.is_err(), "Empty string should fail interval parsing");
            assert!(
                matches!(result, Err(Error::SnapshotCreationBatchesShouldBePositive)),
                "Expected SnapshotCreationBatchesShouldBePositive error, got: {result:?}",
            );
        }

        #[tokio::test]
        async fn test_empty_string_threshold_for_interval() {
            let dataset = create_test_dataset(Some("ts".to_string())).await;
            let acceleration = create_acceleration_with_trigger(
                None,
                Engine::DuckDB,
                Some(SnapshotsTrigger::TimeInterval),
                Some(String::new()),
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Full,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            assert!(result.is_err(), "Empty string should fail interval parsing");
            assert!(
                matches!(
                    result,
                    Err(Error::InvalidSnapshotCreationInterval { source: _ })
                ),
                "Expected InvalidSnapshotCreationInterval error, got: {result:?}",
            );
        }

        #[tokio::test]
        async fn test_empty_string_threshold_for_batches() {
            let dataset = create_test_dataset(None).await;
            let acceleration = create_acceleration_with_trigger(
                None,
                Engine::DuckDB,
                Some(SnapshotsTrigger::StreamBatches),
                Some(String::new()),
                &dataset.runtime().secrets(),
            );
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let snapshot_path = temp_dir.path().join("snapshot.db");

            let result = build_snapshot_creation_config(
                &dataset,
                &acceleration,
                RefreshMode::Changes,
                AccelerationLayout::file(snapshot_path),
            )
            .await;

            assert!(result.is_err(), "Empty string should fail batch parsing");
            assert!(
                matches!(
                    result,
                    Err(Error::InvalidSnapshotCreationBatches { source: _ })
                ),
                "Expected InvalidSnapshotCreationBatches error, got: {result:?}",
            );
        }
    }
}
