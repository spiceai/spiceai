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
use crate::accelerated_table::snapshots::SnapshotRefreshState;
use crate::accelerated_table::{
    self, AcceleratedTableBuilderError, SnapshotCreateTrigger, SnapshotCreationConfig,
};
use crate::accelerated_table::{AcceleratedTable, Retention, refresh::Refresh};
use crate::catalogconnector::deferred::DeferredCatalogProvider;
use crate::component::access::AccessMode;
use crate::component::dataset::acceleration::{Acceleration, Engine, Mode, RefreshMode};
use crate::component::dataset::{Dataset, ReadyState};
use crate::component::view::View;
use crate::dataaccelerator::ReloadProviderFactory;
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint;
use crate::dataaccelerator::swappable::SwappableTableProvider;
use crate::dataaccelerator::{self, BootstrapStatus};
use crate::dataaccelerator::{AcceleratorEngineRegistry, get_acceleration_layout};
use crate::dataconnector::deferred::DeferredConnector;
use crate::dataconnector::localpod::LOCALPOD_DATACONNECTOR;
use crate::dataconnector::sink::SinkConnector;
use crate::dataconnector::{DataConnector, DataConnectorError};
use crate::datafusion::query::{Query, registry::QueryCancelRegistry};
use crate::dataupdate::{
    DataUpdate, DataUpdateBroadcaster, StreamingDataUpdate, StreamingDataUpdateExecutionPlan,
    UpdateType,
};
use crate::federated_table::FederatedTable;
use crate::search::full_text::udtf::TEXT_SEARCH_UDTF_NAME;
use crate::secrets::Secrets;
use crate::tracing_util::view_registered_trace;
use crate::view::prepare_view;
use crate::{status, view};

use {
    crate::cluster::{ExecutorControlStreamRegistry, ExecutorRegistry, ResolvedClusterConfig},
    ballista_executor::executor::Executor,
    ballista_scheduler::scheduler_server::SchedulerServer,
    datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode},
};

use crate::cluster::partition::service::PartitionService;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_tools::schema::verify_schema;
use builder::DataFusionBuilder;
use cache::TabledCacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use cache::result::search::CachedSearchResult;
use cache::{CacheProvider, Caching, QueryResultsCacheProvider, key::RawCacheKey};
use data_components::poly::PolyTableProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::common::{Constraint, Constraints, ToDFSchema};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{ResolvedTableReference, TableReference};
use datafusion_expr::Expr;
use datafusion_federation::FederatedTableProviderAdaptor;
use error::{find_datafusion_root, format_datafusion_error};
use futures::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex as ParkingMutex;
use query::QueryBuilder;
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
use runtime_table_partition::provider::PartitionTableProvider;
use schema::ensure_schema_exists;
use snafu::prelude::*;
use spicepod::acceleration::SnapshotsTrigger;
use spicepod::metric::Metrics;
use tokio::runtime::Handle;
use tokio::spawn;
use tokio::sync::{Mutex, Notify};
use tokio::sync::{RwLock as TokioRwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

pub mod query;

pub mod app_context_extension;
pub mod builder;
#[cfg(not(windows))]
pub mod cayenne_ddl;
pub use runtime_datafusion::composed_catalog;
pub use runtime_datafusion::dialect;
pub use runtime_datafusion::error;
pub mod filter_converter;
pub mod flight_session_extension;
pub mod iceberg_ddl;
pub mod job_executor_context_extension;
pub use runtime_datafusion::managed_runtime;
pub use runtime_datafusion::param_utils;
#[cfg(not(windows))]
pub mod planner;
pub mod refresh_sql;
pub mod request_context_extension;
pub mod retention_sql;
pub mod schema;
pub mod secrets_context_extension;
pub mod table;
pub use runtime_datafusion::sort_columns;
pub(crate) mod sql_validator;
pub mod tool_udf;
pub mod udf;
pub mod udtf;

pub use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

pub const SPICE_RUNTIME_SCHEMA: &str = "runtime";
pub const SPICE_EVAL_SCHEMA: &str = "eval";
pub const SPICE_METADATA_SCHEMA: &str = "metadata";
pub const SPICE_SCP_SCHEMA: &str = "scp";

const MAX_STREAMING_BROADCAST_BATCHES: usize = 128;
const MAX_STREAMING_BROADCAST_ROWS: usize = 1_000_000;
const MAX_STREAMING_BROADCAST_BYTES: usize = 128 * 1024 * 1024;

#[derive(Default)]
struct StreamingBroadcastBuffer {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    limit_exceeded: bool,
}

impl StreamingBroadcastBuffer {
    fn push(&mut self, batch: &RecordBatch) -> bool {
        if self.limit_exceeded {
            return false;
        }

        let next_batches = self.batches.len().saturating_add(1);
        let next_rows = self.rows.saturating_add(batch.num_rows());
        let next_bytes = self.bytes.saturating_add(batch.get_array_memory_size());
        if next_batches > MAX_STREAMING_BROADCAST_BATCHES
            || next_rows > MAX_STREAMING_BROADCAST_ROWS
            || next_bytes > MAX_STREAMING_BROADCAST_BYTES
        {
            self.batches.clear();
            self.rows = 0;
            self.bytes = 0;
            self.limit_exceeded = true;
            return true;
        }

        self.rows = next_rows;
        self.bytes = next_bytes;
        self.batches.push(batch.clone());
        false
    }

    fn batches(&self) -> Option<Vec<RecordBatch>> {
        (!self.limit_exceeded).then(|| self.batches.clone())
    }
}

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

    #[snafu(display("Unable to parse SQL: {}", format_datafusion_error(source)))]
    UnableToParseSql { source: DataFusionError },

    #[snafu(display("{source}"))]
    RefreshSql { source: refresh_sql::Error },

    #[snafu(display("{source}"))]
    RetentionSql { source: retention_sql::Error },

    #[snafu(display("Unable to get table: {}", format_datafusion_error(source)))]
    UnableToGetTable { source: DataFusionError },

    #[snafu(display("Unable to list tables: {}", format_datafusion_error(source)))]
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

    #[snafu(display(
        "Unable to register table in DataFusion: {}",
        format_datafusion_error(source)
    ))]
    UnableToRegisterTableToDataFusion { source: DataFusionError },

    #[snafu(display(
        "Unable to register {schema} table in DataFusion: {}",
        format_datafusion_error(source)
    ))]
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

    #[snafu(display(
        "Unable to plan the table insert for {table_name}: {}",
        format_datafusion_error(source)
    ))]
    UnableToPlanTableInsert {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display(
        "Unable to execute the table insert for {table_name}: {}",
        format_datafusion_error(source)
    ))]
    UnableToExecuteTableInsert {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display("No executors available to forward write for table {table_name}"))]
    NoExecutorsAvailable { table_name: String },

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

    #[snafu(display("The catalog {catalog} does not support partition metadata lookups."))]
    CatalogNotPartitionAware { catalog: String },

    #[snafu(display(
        "Failed to read partition metadata for table {catalog}.{schema}.{table}: {source}"
    ))]
    UnableToReadPartitionMetadata {
        catalog: String,
        schema: String,
        table: String,
        source: Box<crate::catalogconnector::Error>,
    },

    #[snafu(display("Unable to get {schema} schema: {}", format_datafusion_error(source)))]
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

    #[snafu(display("Unable to acquire lock for DDL-enabled catalogs"))]
    UnableToLockDdlEnabledCatalogs {},

    #[snafu(display("Unable to acquire lock for cluster scheduler state"))]
    UnableToLockWritableSchedulerHandle {},

    #[snafu(display("Unable to acquire lock for cluster executor state"))]
    UnableToLockWritableExecutorHandle {},

    #[snafu(display("Unable to acquire lock for executor stream registry"))]
    UnableToLockWritableExecutorStreamRegistry {},

    #[snafu(display(
        "The schema returned by the data connector for 'refresh_mode: changes' does not contain a data field"
    ))]
    ChangeSchemaWithoutDataField { source: ArrowError },

    #[snafu(display(
        "Unable to create streaming data update: {}",
        format_datafusion_error(source)
    ))]
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
        "Failed to create an accelerated table for dataset {dataset_name}: the '{engine}' engine is not supported for distributed acceleration. Use 'arrow' (optionally with 'partition_by') or 'cayenne' instead."
    ))]
    UnsupportedDistributedAccelerationEngine {
        dataset_name: String,
        engine: String,
    },

    #[snafu(display(
        "Failed to create an accelerated table for {component_name}. Error setting the underlying table provider: {}",
        format_datafusion_error(source)
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

    #[snafu(display(
        "refresh_mode: snapshot requires snapshot bootstrap to be enabled \
         (set `acceleration.snapshots: enabled` or `bootstrap_only`); \
         `disabled` and `create_only` are not sufficient because the dataset \
         must be able to load from a snapshot."
    ))]
    SnapshotRefreshModeRequiresSnapshots,

    #[snafu(display(
        "refresh_mode: snapshot requires a snapshot-capable file-based engine \
         (DuckDB, SQLite, Cayenne, or Turso); engine '{engine}' is not supported."
    ))]
    SnapshotRefreshModeUnsupportedEngine { engine: String },

    #[snafu(display(
        "refresh_mode: snapshot requires the accelerator to support snapshot reload, but \
         engine '{engine}' does not implement `reload_from_snapshot`."
    ))]
    SnapshotRefreshModeReloadUnsupported { engine: String },

    #[snafu(display("Failed to construct snapshot manager for refresh_mode: snapshot."))]
    SnapshotRefreshModeManagerUnavailable,

    #[snafu(display(
        "refresh_mode: snapshot could not resolve the accelerator file layout: {source}"
    ))]
    SnapshotRefreshModeLayoutUnavailable {
        source: crate::dataaccelerator::FilePathError,
    },

    #[snafu(display("Pre-refresh partition discovery failed for table '{table_name}': {source}"))]
    PreRefreshPartitionDiscoveryFailed {
        table_name: String,
        source: Box<crate::cluster::partition::service::Error>,
    },
}

/// Validates that the acceleration engine is supported in distributed mode.
///
/// Only Arrow, `PartitionedArrow`, and Cayenne engines are supported for distributed acceleration.
/// Returns an error if a distributed role is active and the engine is unsupported.
fn validate_distributed_engine(
    cluster_config: &ResolvedClusterConfig,
    engine: Engine,
    dataset_name: &str,
) -> Result<()> {
    if cluster_config.effective_role().is_some()
        && !matches!(
            engine,
            Engine::Arrow | Engine::PartitionedArrow | Engine::Cayenne
        )
    {
        return UnsupportedDistributedAccelerationEngineSnafu {
            dataset_name: dataset_name.to_string(),
            engine: engine.to_string(),
        }
        .fail();
    }
    Ok(())
}

/// Converts a runtime `Engine` to a snapshot `AccelerationEngine`.
///
/// Returns `None` for engines that don't support file-based snapshots (e.g. Arrow, `PostgreSQL`).
fn engine_to_acceleration_engine(engine: Engine) -> Option<AccelerationEngine> {
    match engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB | Engine::TableModePartitionedDuckDB => Some(AccelerationEngine::DuckDB),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => Some(AccelerationEngine::Sqlite),
        #[cfg(feature = "turso")]
        Engine::Turso => Some(AccelerationEngine::Turso),
        #[cfg(not(windows))]
        Engine::Cayenne => Some(AccelerationEngine::Cayenne),
        _ => None,
    }
}

/// Remap constraint column indices from the source schema to the refresh schema.
///
/// When `refresh_sql` selects a subset or reordered set of columns, the primary key
/// column indices in the source constraints no longer match the refresh schema.
/// This function maps column names from source indices to their positions in the
/// refresh schema. Returns `None` if any primary key column is missing from the
/// refresh schema.
fn remap_constraints_to_refresh_schema(
    source_constraints: &Constraints,
    source_schema: &SchemaRef,
    refresh_schema: &SchemaRef,
) -> Option<Constraints> {
    // Helper to remap column indices from source to refresh schema using bounds-checked lookups.
    let remap_indices = |indices: &[usize]| -> Option<Vec<usize>> {
        indices
            .iter()
            .map(|&idx| {
                let field = source_schema.fields().get(idx)?;
                refresh_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == field.name())
            })
            .collect()
    };

    // If any PrimaryKey constraint cannot be fully remapped, return None entirely
    // to avoid creating a table with Unique-only constraints that downstream code
    // might incorrectly treat as having upsert (on_conflict) capability.
    let has_unmappable_pk = source_constraints
        .iter()
        .any(|c| matches!(c, Constraint::PrimaryKey(indices) if remap_indices(indices).is_none()));
    if has_unmappable_pk {
        return None;
    }

    let remapped: Vec<Constraint> = source_constraints
        .iter()
        .filter_map(|constraint| match constraint {
            Constraint::PrimaryKey(indices) => remap_indices(indices).map(Constraint::PrimaryKey),
            Constraint::Unique(indices) => remap_indices(indices).map(Constraint::Unique),
        })
        .collect();

    if remapped.is_empty() {
        None
    } else {
        Some(Constraints::new_unverified(remapped))
    }
}

const DEFAULT_SNAPSHOT_CREATION_INTERVAL: Duration = Duration::from_mins(10);
const DEFAULT_SNAPSHOT_CREATION_BATCHES: i64 = 100;

/// Default polling interval for `refresh_mode: snapshot` when the user does
/// not specify `refresh_check_interval` explicitly. Picked to be slightly
/// shorter than the default snapshot creation interval so a freshly created
/// snapshot is picked up promptly without aggressive object-store load.
const DEFAULT_SNAPSHOT_REFRESH_CHECK_INTERVAL: Duration = Duration::from_mins(1);

pub enum Table {
    Accelerated {
        source: Arc<dyn DataConnector>,
        federated_read_table: FederatedTable,
        accelerated_table: Option<Arc<AcceleratedTable>>,
        secrets: Arc<TokioRwLock<Secrets>>,
        bootstrap_status: BootstrapStatus,
        /// Initial partition filter expressions to apply before the refresher starts.
        /// These are set on the `Refresh` during table registration to avoid a race
        /// where the first refresh runs before partition filters are applied.
        initial_partition_filters: Vec<datafusion_expr::Expr>,
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
    pub(crate) runtime_status: Arc<status::RuntimeStatus>,
    data_writers: RwLock<HashSet<TableReference>>,
    data_update_broadcaster: DataUpdateBroadcaster,
    writable_catalogs: RwLock<HashSet<String>>,
    /// Catalogs that allow DDL operations (CREATE TABLE, DROP TABLE, etc.)
    ddl_enabled_catalogs: Arc<RwLock<HashSet<String>>>,
    /// Shared store for DDL extensions from `CREATE TABLE` statements.
    ddl_extension_store: datafusion_ddl::SharedDdlExtensionStore,
    /// Shared weak self-reference, populated after `Arc::new(DataFusion)`.
    /// Used by the extension planner to pass `Weak<DataFusion>` to physical plans.
    datafusion_ref: iceberg_ddl::SharedDataFusionRef,
    accelerated_tables: TokioRwLock<HashSet<TableReference>>,
    caching: Arc<Caching>,
    pending_sink_tables: TokioRwLock<Vec<PendingSinkRegistration>>,
    deferred_tables: TokioRwLock<HashMap<String, DeferredTableRegistration>>,
    deferred_catalogs: TokioRwLock<HashMap<String, Arc<DeferredCatalogProvider>>>,

    /// Registry of dataset placeholders awaiting first-reference
    /// initialization. Populated by `register_deferred_dataset` and
    /// drained by `resolve_pending_initializations`.
    pending_initializations: TokioRwLock<
        HashMap<
            TableReference,
            Arc<crate::datafusion::table::dataset_table_provider::DatasetTableProvider>,
        >,
    >,
    /// Mirrors `pending_initializations` size and is read on the
    /// steady-state hot path: when zero, queries pay only a single
    /// `Acquire`-ordered atomic load and skip the lookup entirely.
    pending_initializations_count: std::sync::atomic::AtomicUsize,
    query_cancel_registry: Arc<QueryCancelRegistry>,

    pub(crate) accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    // Controls the parallelism of accelerated table refreshes
    acceleration_refresh_semaphore: Option<Arc<Semaphore>>,
    pub(crate) task_history_enabled: bool,
    // Dedicated runtime for CPU-bound DataFusion queries
    cpu_runtime: OnceLock<ManagedTokioRuntime>,
    // Dedicated runtime for CPU-bound DataFusion acceleration for dataset acceleration refresh tasks
    refresh_runtime: OnceLock<ManagedTokioRuntime>,
    pub(crate) io_runtime: Handle,
    metrics: Option<Metrics>,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,

    pub temp_directory: Option<String>,
    pub cluster_config: Arc<ResolvedClusterConfig>,
    pub scheduler_server: RwLock<Option<Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>>>,
    pub executor: RwLock<Option<Arc<Executor>>>,
    /// Registry of connected executor control streams for `PollNow` broadcasts.
    /// Only used in scheduler mode.
    pub executor_stream_registry: RwLock<Option<ExecutorControlStreamRegistry>>,
    /// Partition service for discovering/assigning partitions (scheduler mode only).
    pub(crate) partition_service: Option<Arc<PartitionService>>,
    #[cfg(not(windows))]
    pub(crate) cayenne_ddl_handler: Option<Arc<dyn datafusion_ddl::CatalogDdlHandler>>,
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
    pub fn data_update_broadcaster(&self) -> DataUpdateBroadcaster {
        self.data_update_broadcaster.clone()
    }

    #[must_use]
    pub(crate) fn normalize_table_reference(
        &self,
        table_reference: TableReference,
    ) -> TableReference {
        // NOTE: this uses synchronous `table_exist` checks on schema providers. These
        // checks are expected to be in-memory lookups in current catalog implementations.
        match table_reference {
            TableReference::Full { .. } => table_reference,
            TableReference::Partial { schema, table } => {
                let matching_catalogs = self
                    .ctx
                    .catalog_names()
                    .into_iter()
                    .filter(|catalog_name| {
                        self.ctx
                            .catalog(catalog_name)
                            .and_then(|catalog| catalog.schema(schema.as_ref()))
                            .is_some_and(|schema_provider| {
                                schema_provider.table_exist(table.as_ref())
                                    || self.is_catalog_writable(catalog_name)
                            })
                    })
                    .collect::<Vec<_>>();

                if matching_catalogs.len() == 1 {
                    return TableReference::full(
                        matching_catalogs[0].clone(),
                        schema.to_string(),
                        table.to_string(),
                    );
                }

                TableReference::partial(schema, table)
            }
            TableReference::Bare { table } => {
                let table_name = table.to_string();
                let matching_tables =
                    self.ctx
                        .catalog_names()
                        .into_iter()
                        .flat_map(|catalog_name| {
                            let table_name_for_catalog = table_name.clone();
                            self.ctx
                                .catalog(&catalog_name)
                                .into_iter()
                                .flat_map(move |catalog| {
                                    let catalog_name = catalog_name.clone();
                                    let table_name = table_name_for_catalog.clone();
                                    catalog.schema_names().into_iter().filter_map(
                                        move |schema_name| {
                                            let table_name = table_name.clone();
                                            catalog
                                                .schema(&schema_name)
                                                .filter(|schema_provider| {
                                                    schema_provider.table_exist(table_name.as_str())
                                                })
                                                .map(|_| {
                                                    (catalog_name.clone(), schema_name, table_name)
                                                })
                                        },
                                    )
                                })
                        })
                        .collect::<Vec<_>>();

                if matching_tables.len() == 1 {
                    let (catalog, schema, table_name) = matching_tables[0].clone();
                    return TableReference::full(catalog, schema, table_name);
                }

                TableReference::bare(table_name)
            }
        }
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

    #[must_use]
    pub fn query_cancel_registry(&self) -> Arc<QueryCancelRegistry> {
        Arc::clone(&self.query_cancel_registry)
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
            let catalog_to_register = if name == SPICE_DEFAULT_CATALOG {
                // When overriding the default catalog, preserve internal schemas
                self.compose_with_internal_schemas(catalog)
            } else {
                catalog
            };

            self.ctx.register_catalog(name, catalog_to_register);

            if access.allows_ddl() {
                self.mark_catalog_ddl_enabled(name)?;
            } else if access.allows_write() {
                self.mark_catalog_writable(name)?;
            }
        }

        Ok(())
    }

    /// When an external catalog replaces the default `spice` catalog, extract the
    /// internal schemas (`runtime`, `metadata`, `eval`, `scp`) from the current
    /// default catalog and wrap the external catalog in a [`ComposedCatalogProvider`]
    /// that preserves those internal schemas.
    fn compose_with_internal_schemas(
        &self,
        external: Arc<dyn CatalogProvider>,
    ) -> Arc<dyn CatalogProvider> {
        use composed_catalog::ComposedCatalogProvider;
        use std::collections::HashMap;

        let internal_schema_names = [
            SPICE_RUNTIME_SCHEMA,
            SPICE_METADATA_SCHEMA,
            SPICE_SCP_SCHEMA,
            #[cfg(feature = "models")]
            SPICE_EVAL_SCHEMA,
        ];

        let mut internal_schemas: HashMap<String, Arc<dyn datafusion::catalog::SchemaProvider>> =
            HashMap::new();

        if let Some(current_catalog) = self.ctx.catalog(SPICE_DEFAULT_CATALOG) {
            for schema_name in &internal_schema_names {
                if let Some(schema) = current_catalog.schema(schema_name) {
                    internal_schemas.insert((*schema_name).to_string(), schema);
                }
            }
        }

        Arc::new(ComposedCatalogProvider::new(external, internal_schemas))
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
                initial_partition_filters,
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
                        initial_partition_filters,
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

        if dataset_access_mode.allows_write() {
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

    /// Check if a table reference belongs to a writable catalog.
    /// Handles both explicit catalog names and bare names (defaults to `SPICE_DEFAULT_CATALOG`).
    #[must_use]
    pub fn is_path_catalog_writable(&self, table_reference: &TableReference) -> bool {
        let catalog = table_reference.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
        self.is_catalog_writable(catalog)
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

    /// Returns true if the catalog allows DDL operations (CREATE TABLE, DROP TABLE, etc.).
    #[must_use]
    pub fn is_catalog_ddl_enabled(&self, catalog_name: &str) -> bool {
        if let Ok(ddl_catalogs) = self.ddl_enabled_catalogs.read() {
            ddl_catalogs.contains(catalog_name)
        } else {
            false
        }
    }

    /// Marks a catalog as DDL-enabled, allowing CREATE TABLE, DROP TABLE, etc. operations.
    pub fn mark_catalog_ddl_enabled(&self, catalog_name: &str) -> Result<()> {
        tracing::warn!(
            "Access mode 'read_write_create' is enabled for catalog {catalog_name}. DDL operations are allowed. This feature is currently in preview."
        );
        self.ddl_enabled_catalogs
            .write()
            .map_err(|_| Error::UnableToLockDdlEnabledCatalogs {})?
            .insert(catalog_name.to_string());
        self.writable_catalogs
            .write()
            .map_err(|_| Error::UnableToLockWritableCatalogs {})?
            .insert(catalog_name.to_string());
        Ok(())
    }

    /// Returns a reference to the shared DDL extension store.
    ///
    /// Used by the query execution path to insert extensions extracted from
    /// `CREATE TABLE` statements (e.g. `WITH (acceleration.*, dataset.*)` or
    /// `PARTITION BY`), which are then consumed by catalog-specific analyzer rules.
    #[must_use]
    pub fn ddl_extension_store(&self) -> &datafusion_ddl::SharedDdlExtensionStore {
        &self.ddl_extension_store
    }

    /// Returns the shared weak self-reference holder.
    ///
    /// The extension planner uses this to obtain a `Weak<DataFusion>` for physical plans.
    #[must_use]
    pub fn datafusion_ref(&self) -> &iceberg_ddl::SharedDataFusionRef {
        &self.datafusion_ref
    }

    /// Populate the shared weak self-reference. Must be called once after
    /// wrapping `DataFusion` in `Arc`.
    pub fn set_self_ref(self: &Arc<Self>) {
        let _ = self.datafusion_ref.set(Arc::downgrade(self));
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

    /// Returns `true` if any DDL-enabled catalog is Cayenne-backed.
    #[cfg(not(windows))]
    fn has_cayenne_catalog(&self) -> bool {
        match self.ddl_enabled_catalogs.read() {
            Ok(cats) => cats.iter().any(|name| {
                self.ctx
                    .catalog(name)
                    .is_some_and(|c| cayenne_ddl::is_cayenne_catalog(c.as_ref()))
            }),
            Err(err) => {
                tracing::error!(
                    "Failed to acquire read lock for ddl_enabled_catalogs; \
                     assuming Cayenne-backed catalogs are present to avoid \
                     silently disabling distributed DELETE planning: {err}"
                );
                true
            }
        }
    }

    /// Returns `true` if the given table reference resolves to a Cayenne-backed catalog.
    #[must_use]
    pub fn is_cayenne_catalog(&self, table_reference: &TableReference) -> bool {
        let catalog_name = table_reference.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);

        #[cfg(not(windows))]
        {
            self.ctx
                .catalog(catalog_name)
                .is_some_and(|catalog| cayenne_ddl::is_cayenne_catalog(catalog.as_ref()))
        }

        #[cfg(windows)]
        {
            let _ = catalog_name;
            false
        }
    }

    /// Returns the partition expression string for a table by querying the catalog provider.
    ///
    /// Delegates to the catalog provider's [`PartitionAwareCatalog`] implementation,
    /// which reads from the catalog's persistent metadata store (e.g. Cayenne's `SQLite`),
    /// and returns a SQL partition expression as a string.
    ///
    /// This function does not parse or validate the returned string into a `DataFusion`
    /// [`Expr`]; callers that require a parsed expression must perform that parsing
    /// themselves against the table's schema.
    ///
    /// When the catalog returns an auto-generated label like `"expr0"` (used for function
    /// partition expressions such as `bucket(3, c_nationkey)`), the original SQL expression
    /// string is resolved from [`TablePartitionMetadata`] stored in the partition manager.
    pub async fn get_table_partition_expr(
        &self,
        table_reference: &TableReference,
    ) -> Result<Option<String>, DataFusionError> {
        let catalog = self.resolve_catalog_provider(table_reference);
        resolve_table_partition_expr(
            catalog.as_deref(),
            self.executor_registry().map(Arc::as_ref),
            table_reference,
        )
        .await
    }

    /// Returns the partition expression string for a table using only a `SessionContext`
    /// and `ExecutorRegistry`, without requiring a full `DataFusion` reference.
    ///
    /// This is used by `DistributedCayenneInsertExec` which is constructed from the
    /// extension planner and only has access to the session context.
    pub async fn get_table_partition_expr_from_ctx(
        ctx: &datafusion::prelude::SessionContext,
        executor_registry: &ExecutorRegistry,
        table_reference: &TableReference,
    ) -> Result<Option<String>, DataFusionError> {
        let catalog_name = table_reference.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
        let catalog = ctx.catalog(catalog_name);
        let partition_expr = resolve_table_partition_expr(
            catalog.as_deref(),
            Some(executor_registry),
            table_reference,
        )
        .await?
        .map(strip_outer_parens);

        Ok(partition_expr)
    }

    /// Parses a SQL expression string into a `DataFusion` `Expr`, using the schema of the given table reference for resolution.
    pub async fn sql_expr(
        &self,
        tbl: &TableReference,
        expr: &str,
    ) -> Result<Expr, DataFusionError> {
        let df_schema = self
            .get_table(tbl)
            .await
            .ok_or(DataFusionError::Plan(format!(
                "Table not found for SQL expression: {tbl}"
            )))?
            .schema()
            .try_into()?;

        self.ctx.parse_sql_expr(expr, &df_schema)
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
        let catalog_name = table_reference.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
        let table_name = table_reference.table();
        let schema_name = table_reference.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);

        let catalog_provider =
            self.resolve_catalog_provider(table_reference)
                .context(CatalogMissingSnafu {
                    catalog: catalog_name.to_string(),
                })?;

        let schema_provider = Self::resolve_schema_provider(&catalog_provider, table_reference)
            .context(SchemaMissingSnafu {
                schema: schema_name.to_string(),
            })?;

        let table_provider = schema_provider
            .table(table_name)
            .await
            .map_err(find_datafusion_root)
            .context(UnableToGetTableSnafu)?
            .context(TableMissingSnafu {
                schema: schema_name.to_string(),
                table: table_name.to_string(),
            })?;

        Ok(table_provider)
    }

    /// Resolver hook used by `create_logical_plan` to ensure any
    /// `DatasetTableProvider` placeholders referenced by `statement`
    /// are initialized (and swapped to their real providers in the
    /// catalog) before logical planning runs federation analysis.
    ///
    /// Hot-path fast exit: a single `Acquire`-ordered atomic load on
    /// `pending_initializations_count` skips the
    /// `resolve_table_references` call when no datasets are pending.
    async fn resolve_pending_initializations_for_statement(
        &self,
        session: &SessionState,
        statement: &Statement,
    ) -> Result<(), DataFusionError> {
        if !self.has_pending_initializations() {
            return Ok(());
        }
        let table_refs = session.resolve_table_references(statement)?;
        self.resolve_pending_initializations(&table_refs).await
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

    /// Register a deferred dataset.
    ///
    /// Inserts a `DatasetTableProvider` placeholder in the catalog with
    /// the supplied schema and tracks the dataset in the pending
    /// initialization registry. The lazy `DatasetInitialization` is
    /// consumed at most once on first reference via the resolver hook
    /// in `create_logical_plan`.
    pub async fn register_deferred_dataset(
        &self,
        dataset: Arc<Dataset>,
        init: crate::init::dataset_initialization::DatasetInitialization,
        schema: arrow_schema::SchemaRef,
    ) -> Result<()> {
        use crate::datafusion::table::dataset_table_provider::DatasetTableProvider;
        schema::ensure_schema_exists(&self.ctx, SPICE_DEFAULT_CATALOG, &dataset.name)?;

        let placeholder = Arc::new(DatasetTableProvider::new(
            dataset.name.clone(),
            schema,
            init,
        ));

        self.ctx
            .register_table(
                dataset.name.clone(),
                Arc::clone(&placeholder) as Arc<dyn TableProvider>,
            )
            .map_err(find_datafusion_root)
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        self.pending_initializations
            .write()
            .await
            .insert(dataset.name.clone(), placeholder);
        self.pending_initializations_count
            .fetch_add(1, std::sync::atomic::Ordering::Release);

        self.runtime_status
            .update_dataset(&dataset.name, status::ComponentStatus::Ready);

        Ok(())
    }

    /// Steady-state hot-path probe: returns `true` iff at least one
    /// dataset placeholder is awaiting initialization.
    ///
    /// Single `Acquire`-ordered atomic load. Pods with no deferred
    /// datasets, and post-warm-up pods, pay nothing here.
    #[must_use]
    pub fn has_pending_initializations(&self) -> bool {
        self.pending_initializations_count
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
    }

    /// Resolve any pending placeholders that match `table_refs`.
    ///
    /// For each match, calls `ensure_ready` on the placeholder; on
    /// success, removes it from the pending registry and decrements
    /// the counter, then swaps the real provider into the catalog via
    /// `replace_table`.
    pub async fn resolve_pending_initializations(
        &self,
        table_refs: &[TableReference],
    ) -> std::result::Result<(), DataFusionError> {
        if !self.has_pending_initializations() {
            return Ok(());
        }

        // Snapshot just the matching placeholders under the read lock,
        // then run `ensure_ready` (which awaits source I/O) without
        // holding it.
        let to_resolve: Vec<_> = {
            let pending = self.pending_initializations.read().await;
            table_refs
                .iter()
                .filter_map(|r| pending.get(r).map(|p| (r.clone(), Arc::clone(p))))
                .collect()
        };

        for (table_ref, placeholder) in to_resolve {
            let ready = placeholder.ensure_ready().await.map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
            })?;

            // Swap the placeholder out of the catalog with the real
            // provider so federation analysis on the eventual logical
            // plan downcasts to the underlying
            // `FederatedTableProviderAdaptor`.
            if let Some(real_provider) = ready.table_provider.clone() {
                self.replace_table(&table_ref, real_provider).map_err(|e| {
                    DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
                })?;
            }

            // Drop from the pending registry. Decrement only if the
            // entry was still present (concurrent resolvers may have
            // already removed it).
            let mut pending = self.pending_initializations.write().await;
            if pending.remove(&table_ref).is_some() {
                self.pending_initializations_count
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
            }
        }

        Ok(())
    }

    /// Replace the table provider registered under `name` with
    /// `provider`. Used by the deferred dataset initialization swap.
    pub fn replace_table(
        &self,
        name: &TableReference,
        provider: Arc<dyn TableProvider>,
    ) -> Result<()> {
        // DataFusion has no atomic replace; deregister + register is
        // the documented pattern.
        let _ = self.ctx.deregister_table(name.clone());
        self.ctx
            .register_table(name.clone(), provider)
            .map_err(find_datafusion_root)
            .context(UnableToRegisterTableToDataFusionSnafu)?;
        Ok(())
    }

    /// Deregister a placeholder so the eager bring-up path
    /// can register the real (accelerated) table provider in its
    /// place. Removes the entry from the pending registry and
    /// decrements the counter so the resolver fast-path stops
    /// matching this dataset.
    pub async fn drop_pending_initialization(&self, name: &TableReference) -> Result<()> {
        let _ = self.ctx.deregister_table(name.clone());
        let mut pending = self.pending_initializations.write().await;
        if pending.remove(name).is_some() {
            self.pending_initializations_count
                .fetch_sub(1, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }

    /// Clear placeholder bookkeeping after a swap-in-place. Unlike
    /// `drop_pending_initialization`, this does **not** call
    /// `deregister_table`. The caller has already replaced the
    /// placeholder with a real provider (e.g. via `register_table`
    /// inside `register_loaded_dataset`), so deregistering would
    /// remove the freshly-registered real provider.
    pub async fn complete_pending_initialization(&self, name: &TableReference) {
        let mut pending = self.pending_initializations.write().await;
        if pending.remove(name).is_some() {
            self.pending_initializations_count
                .fetch_sub(1, std::sync::atomic::Ordering::Release);
        }
    }

    pub async fn load_deferred_catalog(&self, name: &str, access: &AccessMode) -> Result<()> {
        let deferred_catalogs = self.deferred_catalogs.read().await;
        if let Some(catalog) = deferred_catalogs.get(name) {
            if let Ok(provider) = catalog.get_catalog_provider().await {
                self.ctx.register_catalog(name, Arc::clone(&provider));
                if access.allows_ddl() {
                    self.mark_catalog_ddl_enabled(name)?;
                } else if access.allows_write() {
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
            vec![],
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
        if !self.is_writable(table_reference) && !self.is_path_catalog_writable(table_reference) {
            TableNotWritableSnafu {
                table_name: table_reference.to_string(),
            }
            .fail()?;
        }

        self.ensure_sink_dataset(table_reference.clone(), Arc::clone(&data_update.schema))
            .await?;

        let table_provider = self.get_table_provider(table_reference).await?;

        let DataUpdate {
            schema: update_schema,
            data: update_data,
            update_type,
        } = data_update;

        verify_schema(table_provider.schema().fields(), update_schema.fields())
            .context(SchemaMismatchSnafu)?;
        for batch in &update_data {
            verify_schema(update_schema.fields(), batch.schema().fields())
                .context(SchemaMismatchSnafu)?;
        }

        let update_data = Arc::new(update_data);

        let overwrite = match &update_type {
            UpdateType::Overwrite => InsertOp::Overwrite,
            UpdateType::Append => InsertOp::Append,
            UpdateType::Changes => InsertOp::Replace,
        };

        {
            let insert_data = Arc::clone(&update_data);
            let insert_stream: datafusion::execution::SendableRecordBatchStream = Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&update_schema),
                    Box::pin(futures::stream::iter((0..insert_data.len()).map(
                        move |batch_index| {
                            Ok::<_, DataFusionError>(insert_data[batch_index].clone())
                        },
                    ))),
                ),
            );

            let insert_plan = table_provider
                .insert_into(
                    &self.ctx.state(),
                    Arc::new(StreamingDataUpdateExecutionPlan::new(insert_stream)),
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
        }

        // Invalidate cached query state for this table.
        // Both results and logical plans can become stale after a write:
        // - results cache may otherwise replay pre-write answers
        // - plans cache may hold stale `Arc<dyn TableProvider>` references
        //   whose in-memory state (e.g. Cayenne protected snapshots / deletion
        //   caches) no longer reflects the latest write.
        if let Err(e) = self.caching().invalidate_for_table(table_reference.clone()) {
            tracing::warn!(
                "Failed to invalidate caches for table {table_reference} after write: {e}"
            );
        }

        self.runtime_status
            .update_dataset(table_reference, status::ComponentStatus::Ready);

        let broadcast_table_reference = self.normalize_table_reference(table_reference.clone());
        if self
            .data_update_broadcaster
            .has_subscribers(&broadcast_table_reference)
            .await
        {
            let data = Arc::try_unwrap(update_data).unwrap_or_else(|data| data.as_ref().clone());
            self.data_update_broadcaster
                .publish(
                    &broadcast_table_reference,
                    DataUpdate {
                        schema: update_schema,
                        data,
                        update_type,
                    },
                )
                .await;
        }

        Ok(())
    }

    pub async fn write_streaming_data(
        &self,
        table_reference: &TableReference,
        streaming_update: StreamingDataUpdate,
    ) -> Result<()> {
        if !self.is_writable(table_reference) && !self.is_path_catalog_writable(table_reference) {
            TableNotWritableSnafu {
                table_name: table_reference.to_string(),
            }
            .fail()?;
        }

        let StreamingDataUpdate { data, update_type } = streaming_update;
        let update_schema = data.schema();
        let broadcast_table_reference = self.normalize_table_reference(table_reference.clone());

        self.ensure_sink_dataset(table_reference.clone(), Arc::clone(&update_schema))
            .await?;

        let table_provider = self.get_table_provider(table_reference).await?;

        verify_schema(table_provider.schema().fields(), update_schema.fields())
            .context(SchemaMismatchSnafu)?;

        let overwrite = match update_type {
            UpdateType::Overwrite => InsertOp::Overwrite,
            UpdateType::Append => InsertOp::Append,
            UpdateType::Changes => InsertOp::Replace,
        };

        let (broadcast_batches, data): (
            Option<Arc<ParkingMutex<StreamingBroadcastBuffer>>>,
            datafusion::execution::SendableRecordBatchStream,
        ) = if self
            .data_update_broadcaster
            .has_subscribers(&broadcast_table_reference)
            .await
        {
            let broadcast_batches =
                Arc::new(ParkingMutex::new(StreamingBroadcastBuffer::default()));
            let batches = Arc::clone(&broadcast_batches);
            let stream = data.map(move |batch_result| {
                if let Ok(batch) = &batch_result {
                    batches.lock().push(batch);
                }
                batch_result
            });
            let data: datafusion::execution::SendableRecordBatchStream = Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&update_schema),
                    Box::pin(stream),
                ),
            );
            (Some(broadcast_batches), data)
        } else {
            (None, data)
        };

        let insert_plan = table_provider
            .insert_into(
                &self.ctx.state(),
                Arc::new(StreamingDataUpdateExecutionPlan::new(data)),
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

        // Invalidate cached query state for this table.
        // Both results and logical plans can become stale after a write:
        // - results cache may otherwise replay pre-write answers
        // - plans cache may hold stale `Arc<dyn TableProvider>` references
        //   whose in-memory state (e.g. Cayenne protected snapshots / deletion
        //   caches) no longer reflects the latest write.
        if let Err(e) = self.caching().invalidate_for_table(table_reference.clone()) {
            tracing::warn!(
                "Failed to invalidate caches for table {table_reference} after streaming write: {e}"
            );
        }

        self.runtime_status
            .update_dataset(table_reference, status::ComponentStatus::Ready);

        if let Some(broadcast_batches) = broadcast_batches
            && self
                .data_update_broadcaster
                .has_subscribers(&broadcast_table_reference)
                .await
        {
            let broadcast_data = broadcast_batches.lock().batches();
            if let Some(data) = broadcast_data {
                self.data_update_broadcaster
                    .publish(
                        &broadcast_table_reference,
                        DataUpdate {
                            schema: update_schema,
                            data,
                            update_type,
                        },
                    )
                    .await;
            } else {
                let subscribers_closed = self
                    .data_update_broadcaster
                    .close_subscribers(&broadcast_table_reference)
                    .await;
                tracing::warn!(
                    dataset = %broadcast_table_reference,
                    max_batches = MAX_STREAMING_BROADCAST_BATCHES,
                    max_rows = MAX_STREAMING_BROADCAST_ROWS,
                    max_bytes = MAX_STREAMING_BROADCAST_BYTES,
                    subscribers_closed,
                    "Closed DoExchange subscribers because the buffered streaming data update exceeded limits; subscribers must reconnect to receive a fresh snapshot"
                );
            }
        }

        Ok(())
    }

    pub async fn get_arrow_schema(&self, dataset: impl Into<TableReference>) -> Result<Schema> {
        let table_reference = dataset.into();
        let table_provider = self.get_table_provider(&table_reference).await?;
        Ok(table_provider.schema().as_ref().clone())
    }

    #[must_use]
    pub fn table_exists(&self, dataset_name: &TableReference) -> bool {
        let Some(catalog) = self.resolve_catalog_provider(dataset_name) else {
            return false;
        };
        let Some(s) = Self::resolve_schema_provider(&catalog, dataset_name) else {
            return false;
        };
        s.table_exist(dataset_name.table())
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
        initial_partition_filters: Vec<datafusion_expr::Expr>,
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
        // When refresh_mode is `changes` (CDC), on_conflict provides WAL UPDATE upsert routing
        // only — it does not imply accelerator-only writes. Writes should reach the federated
        // source per write_mode. Without CDC, on_conflict means the source may be read-only and
        // writes are directed to the accelerator only.
        let has_changes_refresh = dataset.acceleration.as_ref().is_some_and(|acc| {
            acc.refresh_mode
                .is_some_and(|m| matches!(m, RefreshMode::Changes))
        });
        let needs_source_writes =
            dataset.access().allows_write() && (!has_on_conflict || has_changes_refresh);

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

        let refresh_sql_str = dataset.refresh_sql();
        let (parsed_refresh_sql, refresh_schema) = if let Some(sql_str) = &refresh_sql_str {
            let (parsed, schema) = refresh_sql::parse_refresh_sql(
                dataset.name.clone(),
                sql_str.as_str(),
                Arc::clone(&source_schema),
            )
            .context(RefreshSqlSnafu)?;
            (Some(parsed), schema)
        } else {
            (None, Arc::clone(&source_schema))
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

        self.handle_schema_difference(
            dataset,
            &acceleration_settings,
            &refresh_schema,
            refresh_mode,
        )
        .await?;

        // Get source constraints (primary keys) for upsert behavior.
        //
        // For caching mode with DuckDB/Cayenne: constraints enable upsert behavior
        // For caching mode with Arrow: constraints are required for InsertOp::Replace to work correctly
        let source_constraints = match &*source_table_provider {
            FederatedTable::Immediate(table_provider) => table_provider.constraints(),
            FederatedTable::Deferred(_) => None,
        };

        // When refresh_sql is used, the accelerated table has a different schema than
        // the source. Remap constraint column indices from the source schema to the
        // refresh schema so that upsert/on_conflict still works correctly.
        let constraints = if parsed_refresh_sql.is_some() {
            source_constraints.and_then(|c| {
                remap_constraints_to_refresh_schema(c, &source_schema, &refresh_schema)
            })
        } else {
            source_constraints.cloned()
        };

        // Distributed acceleration is only supported with Arrow, PartitionedArrow, or Cayenne engines.
        validate_distributed_engine(
            &self.cluster_config,
            acceleration_settings.engine,
            &dataset.name.to_string(),
        )?;

        // For caching mode, the underlying accelerator storage is augmented
        // with a hidden `__spice_cache_namespace` column so cached rows can be
        // scoped per-principal. The user-facing schema (and therefore query
        // planning, projection indices, and federation) continues to see only
        // the original columns. This is a breaking change: existing caching
        // accelerator storage from earlier Spice versions does not have the
        // column and must be deleted (e.g. remove the duckdb_file or drop the
        // SQLite/Postgres/Cayenne backing table) before upgrading.
        let storage_schema = if matches!(refresh_mode, RefreshMode::Caching) {
            Arc::new(
                crate::accelerated_table::caching::extend_schema_with_cache_namespace(
                    &dataset.name.to_string(),
                    &refresh_schema,
                )
                .map_err(|source| Error::UnableToCreateDataAccelerator {
                    source: crate::dataaccelerator::Error::InvalidConfiguration {
                        msg: source.to_string(),
                    },
                })?,
            )
        } else {
            Arc::clone(&refresh_schema)
        };

        let accelerated_table_provider = self
            .accelerator_engine_registry
            .create_accelerator_table(
                dataset.name.clone(),
                Arc::clone(&storage_schema),
                constraints.as_ref(),
                &acceleration_settings,
                Arc::clone(&secrets),
                Some(dataset),
                Arc::clone(&self.ctx),
            )
            .await
            .context(UnableToCreateDataAcceleratorSnafu)?;

        // For RefreshMode::Snapshot, wrap the accelerator in a SwappableTableProvider
        // so the underlying provider can be replaced atomically when a newer snapshot
        // is loaded. The snapshot refresh state captures everything `RefreshTask` needs
        // to query the snapshot store and rebuild the provider on reload.
        let (accelerated_table_provider, snapshot_refresh_state) =
            if matches!(refresh_mode, RefreshMode::Snapshot) {
                let snapshot_state = build_snapshot_refresh_state(
                    self,
                    dataset,
                    Arc::clone(&refresh_schema),
                    constraints.clone(),
                    &acceleration_settings,
                    Arc::clone(&secrets),
                    Arc::clone(&accelerated_table_provider),
                    bootstrap_status.loaded_snapshot_id(),
                )
                .await?;
                let swappable: Arc<dyn TableProvider> =
                    Arc::clone(&snapshot_state.swappable_provider) as Arc<dyn TableProvider>;
                (swappable, Some(snapshot_state))
            } else {
                (accelerated_table_provider, None)
            };

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
            // Additionally, for CDC we let connector/stream to decide when dataset is ready.
            let delay_initial_ready = matches!(refresh_mode, RefreshMode::Append)
                && dataset.time_column.is_some()
                && acceleration_settings.snapshot_behavior.bootstrap_enabled()
                || matches!(refresh_mode, RefreshMode::Changes);

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
        if let Some(sql) = parsed_refresh_sql {
            refresh = refresh.refresh_sql(sql);
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
        } else if matches!(refresh_mode, RefreshMode::Snapshot) {
            // Snapshot mode polls the snapshot store for newer snapshots; if the
            // user did not configure a polling interval, fall back to a sensible
            // default so the dataset stays current without requiring manual config.
            tracing::info!(
                dataset = %dataset.name,
                interval_secs = DEFAULT_SNAPSHOT_REFRESH_CHECK_INTERVAL.as_secs(),
                "refresh_mode: snapshot - using default refresh_check_interval"
            );
            refresh = refresh.check_interval(DEFAULT_SNAPSHOT_REFRESH_CHECK_INTERVAL);
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

        // Apply initial partition filters before the refresher starts to avoid a race
        // where the first refresh runs without partition filters.
        if !initial_partition_filters.is_empty() {
            use crate::accelerated_table::refresh::{RefreshSQL, RefreshSQLColumns};
            if let Some(ref mut sql) = refresh.sql {
                sql.set_partition_filters(initial_partition_filters);
            } else {
                let mut sql =
                    RefreshSQL::new(dataset.name.clone(), RefreshSQLColumns::All, vec![], None);
                sql.set_partition_filters(initial_partition_filters);
                refresh = refresh.refresh_sql(sql);
            }
        }

        // Create the accelerator write mutex early so it can be shared between the DataConnector, Refresher and the AcceleratedTable.
        let accelerator_write_mutex: Arc<Mutex<()>> = Arc::new(Mutex::new(()));

        let mut accelerated_table_builder = AcceleratedTable::builder(
            Arc::clone(&self.runtime_status),
            dataset.name.clone(),
            Arc::clone(&source_table_provider),
            dataset.source().to_string(),
            Arc::clone(&accelerated_table_provider),
            refresh,
            self.io_runtime.clone(),
        );
        accelerated_table_builder.cpu_runtime(self.refresh_runtime().cloned());
        accelerated_table_builder.cluster_role(self.cluster_config.effective_role());
        accelerated_table_builder.accelerator_write_mutex(Arc::clone(&accelerator_write_mutex));
        if matches!(refresh_mode, RefreshMode::Caching) {
            // Hide the storage-only namespace column from query planning. Users
            // see the same columns they would have seen pre-isolation.
            accelerated_table_builder.user_facing_schema(Arc::clone(&refresh_schema));
        }

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

        // If the source is deferred (e.g. a Databricks U2M connector that hasn't been triggered
        // yet), the `FederatedTable` holds only a placeholder schema/provider — not a real
        // access-verified source. In that case, force `OnLoad` so the dataset isn't marked ready
        // with a fake schema. Once the deferred connector is triggered, the source will be
        // re-initialized with a real provider.
        let effective_ready_state = if source.as_any().is::<DeferredConnector>() {
            if dataset.ready_state != ReadyState::OnLoad {
                tracing::warn!(
                    "Dataset {dataset_name}: configured ready_state '{configured}' is overridden to '{forced}' because the source connector is deferred (e.g. awaiting interactive auth); the dataset will be marked ready only after the initial load completes.",
                    dataset_name = dataset.name,
                    configured = dataset.ready_state,
                    forced = ReadyState::OnLoad,
                );
            }
            ReadyState::OnLoad
        } else {
            dataset.ready_state
        };
        accelerated_table_builder.ready_state(effective_ready_state);

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

            // Auto-configure cache retention when stale_if_error is disabled.
            // Expired cache entries (past max_age + SWR) are never served and waste storage.
            if !acceleration_settings.caching_stale_if_error.is_enabled() {
                if dataset.retention_period().is_some() {
                    tracing::warn!(
                        dataset = %dataset.name,
                        "User-specified retention_period is overridden by automatic cache retention in caching mode",
                    );
                }

                let max_age = acceleration_settings
                    .caching_ttl
                    .unwrap_or(Duration::from_secs(30));
                let swr = acceleration_settings
                    .caching_stale_while_revalidate_ttl
                    .unwrap_or_default();
                let retention_period = max_age + swr;
                let check_interval = retention_period.max(Duration::from_secs(30));

                let cache_retention = Retention::builder()
                    .time_column(Some(
                        crate::accelerated_table::caching::CACHE_REFRESHED_AT_COLUMN,
                    ))
                    .time_period(Some(retention_period))
                    .check_interval(Some(check_interval))
                    .enabled(true)
                    .build();

                accelerated_table_builder.retention(cache_retention);
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
                    // Resolve any engine-specific snapshot engine override
                    // (e.g. CayenneSnapshotEngine) so the upload pipeline
                    // ships the engine's preferred archive format.
                    let snapshot_engine_override = match self
                        .accelerator_engine_registry
                        .get_accelerator_engine(acceleration_settings.engine)
                        .await
                    {
                        Some(accel) => accel.snapshot_engine_for_source(dataset).await,
                        None => None,
                    };
                    if let Some(snapshot_config) = build_snapshot_creation_config(
                        dataset,
                        &acceleration_settings,
                        refresh_mode,
                        layout.clone(),
                        snapshot_engine_override,
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

        accelerated_table_builder.snapshot_refresh_state(snapshot_refresh_state);

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
            let changes_stream = source.changes_stream(
                Arc::clone(&source_table_provider),
                dataset,
                Arc::clone(&accelerated_table_provider),
                Arc::clone(&accelerator_write_mutex),
                self.refresh_runtime().cloned(),
            );

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

        // on_conflict forces accelerator-only writes when CDC is not in use. With CDC
        // (refresh_mode: changes), on_conflict is for WAL UPDATE upsert routing only and
        // does not override the write destination — writes follow write_mode instead.
        if has_on_conflict && !has_changes_refresh {
            accelerated_table_builder.write_to_accelerator_only();
        } else if dataset.access().allows_write() {
            match acceleration_settings.write_mode {
                spicepod::acceleration::WriteMode::WriteBack => {
                    accelerated_table_builder.write_back();
                }
                spicepod::acceleration::WriteMode::WriteThrough
                    if acceleration_settings.engine == Engine::Cayenne =>
                {
                    // write_through with staged commit/rollback is only supported for Cayenne.
                    // For other engines (e.g. DuckDB + CDC), writes fall through to FederatedOnly:
                    // the write goes directly to the federated source and CDC propagates it back.
                    accelerated_table_builder.write_through();
                }
                spicepod::acceleration::WriteMode::WriteThrough => {
                    // FederatedOnly is the default for non-Cayenne engines.
                }
            }
        }

        accelerated_table_builder.bootstrap_status(bootstrap_status);

        // Check if this is an S3 Express One Zone acceleration (Cayenne with S3 Express config)
        // This is used for better error messages when S3 Express upload fails
        #[cfg(not(windows))]
        let is_s3_express_acceleration = acceleration_settings.engine == Engine::Cayenne
            && (acceleration_settings
                .params
                .get("cayenne_file_path")
                .is_some_and(|path| crate::dataaccelerator::cayenne::s3::is_s3_express_path(path))
                || acceleration_settings
                    .params
                    .contains_key("cayenne_s3_zone_ids"));
        #[cfg(windows)]
        let is_s3_express_acceleration = false;
        accelerated_table_builder.s3_express_acceleration(is_s3_express_acceleration);

        source
            .on_accelerator_setup(dataset, &mut accelerated_table_builder)
            .await
            .context(AccelerationRegistrationSnafu)?;

        accelerated_table_builder
            .build()
            .await
            .context(UnableToBuildAcceleratedTableSnafu {
                dataset_name: dataset.name.to_string(),
            })
    }

    // For file_update mode: compare the checkpoint schema (from the previous run) against
    // the source/refresh schema. If there is any schema difference, snapshot the current
    // file and recreate the acceleration.
    //
    // We read the schema from the checkpoint rather than from accelerated_table_provider
    // because the provider reports the schema it was *created with* (refresh_schema),
    // not the actual schema stored in the acceleration file.
    //
    // Normalize Dictionary types in refresh_schema the same way create_accelerator_table
    // does, so that Dictionary→value type normalization doesn't trigger a false mismatch.
    async fn handle_schema_difference(
        &self,
        dataset: &Dataset,
        acceleration_settings: &Acceleration,
        refresh_schema: &Arc<Schema>,
        refresh_mode: RefreshMode,
    ) -> Result<(), Error> {
        if acceleration_settings.mode == Mode::FileUpdate
            && refresh_mode != RefreshMode::Disabled
            && let Ok(cp) = DatasetCheckpoint::try_new(dataset, OpenOption::OpenExisting).await
            && let Some(existing_schema) = cp.get_schema().await.ok().flatten()
        {
            let needs_dict_normalization = matches!(
                acceleration_settings.engine.to_unpartitioned(),
                Engine::DuckDB | Engine::Sqlite | Engine::Turso
            );
            let normalized_refresh_schema = if needs_dict_normalization
                && arrow_tools::schema::has_dictionary_types(refresh_schema)
            {
                Arc::new(arrow_tools::schema::normalize_dictionary_types(
                    refresh_schema,
                ))
            } else {
                Arc::clone(refresh_schema)
            };

            if let Some(diff) =
                arrow_tools::schema::schema_difference(&existing_schema, &normalized_refresh_schema)
            {
                tracing::warn!(
                    "Dataset {} schema change detected in file_update mode. {diff}. Acceleration file is replaced.",
                    dataset.name
                );

                // Snapshot before recreating (best-effort)
                if let Ok(layout) = get_acceleration_layout(dataset).await
                    && let Some(accel_engine) =
                        engine_to_acceleration_engine(acceleration_settings.engine)
                {
                    dataaccelerator::snapshots::snapshot_before_recreate(
                        acceleration_settings,
                        &dataset.name.to_string(),
                        layout,
                        accel_engine,
                        Arc::clone(&existing_schema),
                        None,
                    )
                    .await;
                }

                // Drop the existing table from the acceleration engine so it can be recreated
                // with the updated schema
                let accelerator = self
                    .accelerator_engine_registry
                    .get_accelerator_engine(acceleration_settings.engine)
                    .await
                    .ok_or_else(|| Error::ExpectedAccelerationSettings {
                        name: dataset.name.to_string(),
                    })?;
                accelerator
                    .drop_table(&dataset.name.to_string(), dataset)
                    .await
                    .map_err(|e| dataaccelerator::Error::AccelerationCreationFailed { source: e })
                    .context(UnableToCreateDataAcceleratorSnafu)?;

                // Clear the checkpoint so the refresh treats this as a fresh table
                let _ = cp.delete().await;
            }
        }

        Ok(())
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
        initial_partition_filters: Vec<datafusion_expr::Expr>,
    ) -> Result<Option<Arc<Notify>>> {
        let mut accelerated_table = self
            .create_accelerated_table(
                &dataset,
                Arc::clone(&source),
                federated_read_table,
                secrets,
                bootstrap_status,
                initial_partition_filters,
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
        self: &Arc<Self>,
        dataset_name: &TableReference,
        overrides: Option<RefreshOverrides>,
    ) -> Result<Option<Arc<Notify>>> {
        // If we're a scheduler with a partition service, forward refresh to executors
        // instead of trying to refresh locally (the scheduler doesn't run refresh workers).
        if matches!(
            self.cluster_config.effective_role(),
            Some(crate::config::ClusterRole::Scheduler)
        ) && let Some(partition_service) = &self.partition_service
        {
            return self
                .forward_refresh_to_executors(partition_service, dataset_name, overrides.as_ref())
                .await;
        }

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

    /// Forwards a dataset refresh command to all connected executors via the control stream.
    /// Returns `Ok(None)` because the scheduler does not have a local notifier for executor refreshes.
    ///
    /// Before forwarding the refresh, runs on-demand partition discovery to ensure new
    /// partitions are assigned to executors. This prevents data loss when `spice refresh`
    /// is triggered before the periodic partition management task discovers new partitions.
    async fn forward_refresh_to_executors(
        self: &Arc<Self>,
        partition_service: &PartitionService,
        dataset_name: &TableReference,
        overrides: Option<&RefreshOverrides>,
    ) -> Result<Option<Arc<Notify>>> {
        // Run on-demand partition discovery before forwarding the refresh command.
        // This ensures that any new partition values in the source data are discovered,
        // assigned to executors, and executors are notified -- before they receive the
        // refresh command. Without this, the periodic partition management task might
        // not have discovered new partitions yet, causing executors to ignore data from
        // unassigned partitions. (fixes #10075)
        partition_service
            .reconcile_table(dataset_name, self.as_ref())
            .await
            .map_err(|source| Error::PreRefreshPartitionDiscoveryFailed {
                table_name: dataset_name.to_string(),
                source: Box::new(source),
            })?;

        let executor_registry = &partition_service.executor_registry;

        let overrides_json = match overrides {
            Some(o) => {
                Some(
                    serde_json::to_string(o).map_err(|_| Error::UnableToTriggerRefresh {
                        dataset_name: dataset_name.to_string(),
                        source: crate::accelerated_table::Error::FailedToTriggerRefresh {
                            source: tokio::sync::mpsc::error::SendError(None),
                        },
                    })?,
                )
            }
            None => None,
        };

        let command = runtime_proto::SchedulerControlMessage {
            message: Some(
                runtime_proto::scheduler_control_message::Message::RefreshDataset(
                    runtime_proto::RefreshDatasetCommand {
                        dataset_name: dataset_name.to_string(),
                        overrides_json,
                    },
                ),
            ),
        };

        let executor_ids = executor_registry.connected_executors().await;
        if executor_ids.is_empty() {
            tracing::warn!(
                "No executors connected to forward refresh for dataset '{dataset_name}'"
            );
            return Ok(None);
        }

        let mut failures = Vec::new();
        for executor_id in &executor_ids {
            if let Err(e) = executor_registry
                .send_command(executor_id, command.clone())
                .await
            {
                tracing::warn!("Failed to send refresh command to executor {executor_id}: {e}");
                failures.push(executor_id.clone());
            }
        }

        if failures.is_empty() {
            tracing::info!(
                "Forwarded refresh for dataset '{dataset_name}' to {} executor(s)",
                executor_ids.len()
            );
        } else {
            tracing::warn!(
                "Refresh for '{dataset_name}' failed for {}/{} executor(s)",
                failures.len(),
                executor_ids.len()
            );
        }

        Ok(None)
    }

    pub async fn update_refresh_sql(
        &self,
        dataset_name: TableReference,
        refresh_sql: String,
    ) -> Result<()> {
        let table = self
            .get_accelerated_table_provider(&dataset_name.to_string())
            .await?;

        let refresh_schema = table.schema();

        let (parsed, selected_schema) = refresh_sql::parse_refresh_sql(
            dataset_name.clone(),
            &refresh_sql,
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

        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            accelerated_table.update_refresh_sql(parsed).await.context(
                UnableToTriggerRefreshSnafu {
                    dataset_name: dataset_name.to_string(),
                },
            )?;
        }

        Ok(())
    }

    /// Update only the partition filters on an accelerated table's refresh.
    pub async fn update_partition_filters(
        &self,
        dataset_name: TableReference,
        filters: Vec<datafusion_expr::Expr>,
    ) -> Result<()> {
        let table = self
            .get_accelerated_table_provider(&dataset_name.to_string())
            .await?;

        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            accelerated_table
                .update_partition_filters(filters)
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

        let source_table_provider: Arc<dyn TableProvider> = match dataset.access() {
            AccessMode::Read => federated_table_provider,
            AccessMode::ReadWrite | AccessMode::ReadWriteCreate => source
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
                    if !df_ref.table_exists(dependent_table_name) {
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
                status.update_view(
                    &table,
                    status::ComponentStatus::error_with_message(format!(
                        "Dependent table {missing_table} does not exist"
                    )),
                );
                return None;
            }

            // If view depends on other tables, wait until they are ready
            wait_until_dependent_tables_are_ready(&table, &dependent_table_names, &status).await;

            let tbl_provider = match prepare_view(&ctx, &statements[0], &view).await {
                Ok(tbl) => tbl,
                Err(e) => {
                    tracing::error!("Failed to create view {table}: {e}");
                    status.update_view(
                        &table,
                        status::ComponentStatus::error_with_message(e.to_string()),
                    );
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
                        status.update_view(
                            &table,
                            status::ComponentStatus::error_with_message(e.to_string()),
                        );
                        return None;
                    }
                }
            }

            // non-accelerated view
            if let Err(e) = ctx.register_table(table.clone(), tbl_provider) {
                tracing::error!("Failed to create view {table}: {e}");
                status.update_view(
                    &table,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );
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

        // Distributed acceleration is only supported with Arrow, PartitionedArrow, or Cayenne engines.
        validate_distributed_engine(
            &self.cluster_config,
            acceleration.engine,
            &table.to_string(),
        )?;

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
        builder.cluster_role(self.cluster_config.effective_role());
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
        builder.zero_results_action(acceleration.on_zero_results.clone());
        if acceleration.disable_federation {
            builder.disable_federation();
        }

        if let Some(semaphore) = &self.acceleration_refresh_semaphore {
            builder.refresh_semaphore(Arc::clone(semaphore));
        }

        // Wrap the DuckDB accelerator with HNSW vector indexes (if applicable).
        // This mirrors the dataset path in `EmbeddingConnector::on_accelerator_setup`.
        #[cfg(feature = "duckdb")]
        {
            crate::embeddings::connector::try_wrap_view_accelerator_with_hnsw(
                view,
                table,
                &mut builder,
            )
            .await
            .map_err(|e| Error::UnableToCreateView {
                reason: format!("Failed to create HNSW vector indexes for view: {e}"),
            })?;
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
        if initial_load_complete
            || matches!(
                view.ready_state,
                ReadyState::OnRegistration | ReadyState::OnSchemaResolved
            )
        {
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
    /// Currently cancels active queries and cleans up accelerated tables.
    pub async fn shutdown(&self) {
        // Don't block self.accelerated_tables as it needs to be modified during table removal
        // and will be cleaned up authomatically by removing accelerated tables.
        tracing::debug!("Datafusion shutdown started");

        let cancelled_queries = self.query_cancel_registry.cancel_all();
        if cancelled_queries > 0 {
            tracing::debug!(
                cancelled_queries,
                "Cancelled active queries during DataFusion shutdown"
            );
        }

        let accelerated_tables = self.accelerated_tables.read().await.clone();

        for table in &accelerated_tables {
            if let Err(err) = self.remove_table(table).await {
                tracing::error!("Failed to clean up '{table}' during shutdown: {err}");
            }
        }

        self.ctx.deregister_udtf(TEXT_SEARCH_UDTF_NAME);
    }

    /// Create or get a logical plan from the query
    pub(crate) async fn get_or_create_logical_plan(
        &self,
        session: &SessionState,
        cache_key_opt: Option<&RawCacheKey>,
        sql: &str,
    ) -> Result<LogicalPlan, DataFusionError> {
        let plans_cache = if let Some(cache_key) = cache_key_opt {
            let plans_cache = self.plans_cache_provider();

            if let Some(cache) = plans_cache.as_ref()
                && let Some(plan) = cache.get_raw_key(&cache_key.as_u64()).await
            {
                tracing::trace!("using cached plan for {sql}");
                return Ok(plan);
            }
            plans_cache
        } else {
            None
        };

        let plan = self.create_logical_plan(session, sql).await?;

        if let Some(cache) = plans_cache
            && let Some(cache_key) = cache_key_opt
        {
            tracing::trace!("caching plan for {sql}");
            cache.put_raw_key(&cache_key.as_u64(), plan.clone()).await;
        }

        Ok(plan)
    }

    /// Route SQL through the planner, which intercepts DDL extensions and
    /// Cayenne DML at the statement level, or falls back to `DataFusion`'s
    /// standard planner.
    #[cfg(not(windows))]
    pub(crate) async fn create_logical_plan(
        &self,
        session: &SessionState,
        sql: &str,
    ) -> Result<LogicalPlan, DataFusionError> {
        let dialect = session.config().options().sql_parser.dialect;
        let statement = session.sql_to_statement(sql, &dialect)?;
        self.resolve_pending_initializations_for_statement(session, &statement)
            .await?;

        let ctx = planner::PlannerContext {
            catalog_mode: if self.has_cayenne_catalog() {
                planner::CatalogMode::Cayenne
            } else {
                planner::CatalogMode::Standard
            },
            cluster_role: self.cluster_config.effective_role(),
            ddl_extension_store: Arc::clone(&self.ddl_extension_store),
            executor_registry: self.executor_registry().cloned(),
            ddl_handler: self.cayenne_ddl_handler.clone(),
            io_runtime: self.io_runtime.clone(),
        };

        planner::create_logical_plan_from_statement(sql, statement, session, &ctx).await
    }

    /// On Windows the `planner` module is not available, so delegate
    /// directly to DataFusion's standard logical planner.
    #[cfg(windows)]
    pub(crate) async fn create_logical_plan(
        &self,
        session: &SessionState,
        sql: &str,
    ) -> Result<LogicalPlan, DataFusionError> {
        let dialect = session.config().options().sql_parser.dialect;
        let statement = session.sql_to_statement(sql, &dialect)?;
        self.resolve_pending_initializations_for_statement(session, &statement)
            .await?;
        session.statement_to_plan(statement).await
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

    pub fn bind_executor_stream_registry(
        &self,
        registry: ExecutorControlStreamRegistry,
    ) -> Result<()> {
        let mut executor_stream_registry = self
            .executor_stream_registry
            .try_write()
            .map_err(|_| Error::UnableToLockWritableExecutorStreamRegistry {})?;
        *executor_stream_registry = Some(registry);
        Ok(())
    }

    /// Returns the executor stream registry if one is bound.
    ///
    /// Returns `None` if no registry is bound or if the read lock cannot be acquired.
    #[must_use]
    pub fn executor_stream_registry(&self) -> Option<ExecutorControlStreamRegistry> {
        self.executor_stream_registry.read().ok()?.clone()
    }

    #[must_use]
    pub fn executor_registry(&self) -> Option<&Arc<ExecutorRegistry>> {
        self.partition_service
            .as_ref()
            .map(|ps| &ps.executor_registry)
    }

    pub fn bind_executor(&self, executor: Arc<Executor>) -> Result<()> {
        let mut executor_handle = self
            .executor
            .try_write()
            .map_err(|_| Error::UnableToLockWritableExecutorHandle {})?;
        *executor_handle = Some(executor);
        Ok(())
    }

    /// Parse a given [`Expr`] from a SQL string.
    ///
    /// The entire `expr` must only contain expressions from the provided [`TableReference`], which itself should be a registered table (i.e. [`DataFusion::register_table`]).
    pub async fn try_parse_expr(
        &self,
        tbl: &TableReference,
        expr: &str,
    ) -> Result<Expr, DataFusionError> {
        let Some(tbl_provider) = self.get_table(tbl).await else {
            return Err(DataFusionError::Plan(format!(
                "Table {tbl} not found when parsing expression"
            )));
        };
        self.ctx
            .parse_sql_expr(expr, &tbl_provider.schema().to_dfschema()?)
    }
}

#[async_trait::async_trait]
impl runtime_cluster::context::PartitionExprResolver for DataFusion {
    async fn try_parse_expr(
        &self,
        tbl: &TableReference,
        expr: &str,
    ) -> Result<Expr, DataFusionError> {
        DataFusion::try_parse_expr(self, tbl, expr).await
    }
}

#[async_trait::async_trait]
impl runtime_cluster::context::PartitionDiscoverer for DataFusion {
    async fn table_partition_values(
        &self,
        table: &TableReference,
        partition_by: &[spicepod::partitioning::PartitionedBy],
    ) -> Result<Vec<runtime_cluster::PartitionValue>, Box<dyn std::error::Error + Send + Sync>>
    {
        crate::cluster::partition::discovery::query_source_partitions(table, partition_by, self)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Strips a single layer of outer parentheses from `s` if, and only if, it both starts
/// with `(` and ends with `)`.  For example `(bucket(10, foo))` → `bucket(10, foo)`.
///
/// Using [`str::trim_start_matches`] / [`str::trim_end_matches`] would greedily strip
/// *all* consecutive matching characters, corrupting expressions like `bucket(10, foo)`.
fn strip_outer_parens(s: String) -> String {
    if s.starts_with('(') && s.ends_with(')') {
        s[1..s.len() - 1].to_string()
    } else {
        s
    }
}

/// Shared implementation for resolving a table's partition expression from its catalog
/// provider and optional [`ExecutorRegistry`] metadata.
///
/// When the catalog returns an auto-generated label like `"expr0"` (used for function
/// partition expressions such as `bucket(3, c_nationkey)`), the original SQL expression
/// string is resolved from [`TablePartitionMetadata`] stored in the partition manager.
async fn resolve_table_partition_expr(
    catalog: Option<&dyn CatalogProvider>,
    executor_registry: Option<&ExecutorRegistry>,
    table_reference: &TableReference,
) -> Result<Option<String>, DataFusionError> {
    let schema_name = table_reference.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);

    let expr_string = if let Some(catalog) = catalog
        && let Some(aware) = cayenne_ddl::as_partition_aware(catalog)
    {
        aware
            .table_partition_expr(schema_name, table_reference.table())
            .await
            .boxed()
            .map_err(DataFusionError::External)?
    } else {
        None
    };

    let provider_expr_string = if let Some(catalog) = catalog
        && let Some(schema) = catalog.schema(schema_name)
    {
        match schema.table(table_reference.table()).await {
            Ok(Some(table_provider)) => partition_expr_from_table_provider(&table_provider),
            Ok(None) => None,
            Err(err) => {
                tracing::debug!(table = %table_reference, error = %err, "Failed to resolve table provider while reading partition expression");
                None
            }
        }
    } else {
        None
    };

    let Some(expr_string) = expr_string.or(provider_expr_string).or_else(|| {
        executor_registry.and_then(|registry| {
            registry
                .federated_partition_store()
                .get_cached_table_metadata(table_reference)
                .and_then(|metadata| metadata.partition_expressions.first().cloned())
        })
    }) else {
        return Ok(None);
    };

    // Resolve auto-generated labels (e.g. "expr0") from partition manager metadata.
    if let Some(Ok(idx)) = expr_string.strip_prefix("expr").map(str::parse::<usize>)
        && let Some(executor_registry) = executor_registry
        && let Some(metadata) = executor_registry
            .federated_partition_store()
            .get_table_metadata(table_reference)
            .await
            .boxed()
            .map_err(DataFusionError::External)?
        && let Some(original) = metadata.partition_expressions.get(idx)
    {
        return Ok(Some(original.clone()));
    }

    Ok(Some(expr_string))
}

fn partition_expr_from_table_provider(table_provider: &Arc<dyn TableProvider>) -> Option<String> {
    if let Some(partitioned) = table_provider
        .as_any()
        .downcast_ref::<PartitionTableProvider>()
    {
        let partition_exprs = partitioned
            .partition_by()
            .iter()
            .map(|partition| partition.expression.to_string())
            .collect_vec();

        return match partition_exprs.as_slice() {
            [] => None,
            [single] => Some(single.clone()),
            _ => unreachable!(
                "Multi-expression partition expressions are not supported yet: https://github.com/spiceai/spiceai/issues/9937"
            ),
        };
    }

    if let Some(poly) = table_provider.as_any().downcast_ref::<PolyTableProvider>() {
        return partition_expr_from_table_provider(&poly.writer());
    }

    if let Some(accelerated) = table_provider.as_any().downcast_ref::<AcceleratedTable>() {
        return partition_expr_from_table_provider(&accelerated.get_accelerator());
    }

    if let Some(adaptor) = table_provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
        && let Some(inner_provider) = adaptor.table_provider.as_ref()
    {
        return partition_expr_from_table_provider(inner_provider);
    }

    None
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
    snapshot_engine_override: Option<
        Arc<dyn runtime_acceleration::snapshot::engine::SnapshotEngine>,
    >,
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
        let sm = if let Some(engine) = snapshot_engine_override {
            sm.with_snapshot_engine(engine)
        } else {
            sm
        };
        SnapshotCreationConfig::new(Arc::new(sm), snapshot_creation_trigger)
    }))
}

/// Build the per-dataset state required to drive `RefreshMode::Snapshot`.
///
/// Validates that the configuration is sound (snapshots enabled, supported
/// engine, supported reload), constructs a [`SnapshotManager`] for the
/// dataset, wraps the freshly-created accelerator provider in a
/// [`SwappableTableProvider`], and captures a [`ReloadProviderFactory`] that
/// re-runs `create_accelerator_table` on each reload.
#[expect(clippy::too_many_arguments)]
async fn build_snapshot_refresh_state(
    df: &DataFusion,
    dataset: &Dataset,
    refresh_schema: SchemaRef,
    constraints: Option<datafusion::common::Constraints>,
    acceleration_settings: &Acceleration,
    secrets: Arc<TokioRwLock<Secrets>>,
    initial_provider: Arc<dyn TableProvider>,
    bootstrap_loaded_id: Option<u64>,
) -> Result<SnapshotRefreshState> {
    // 1. snapshots must be enabled.
    if !acceleration_settings.snapshot_behavior.bootstrap_enabled() {
        return SnapshotRefreshModeRequiresSnapshotsSnafu.fail();
    }

    // 2. engine must be snapshot-capable (file-based with a known layout).
    let acceleration_engine = engine_to_acceleration_engine(acceleration_settings.engine)
        .ok_or_else(|| Error::SnapshotRefreshModeUnsupportedEngine {
            engine: acceleration_settings.engine.to_string(),
        })?;

    // 3. accelerator must support reload_from_snapshot.
    let accelerator = df
        .accelerator_engine_registry
        .get_accelerator_engine(acceleration_settings.engine)
        .await
        .ok_or_else(|| Error::SnapshotRefreshModeUnsupportedEngine {
            engine: acceleration_settings.engine.to_string(),
        })?;
    if !accelerator.supports_snapshot_reload() {
        return SnapshotRefreshModeReloadUnsupportedSnafu {
            engine: acceleration_settings.engine.to_string(),
        }
        .fail();
    }

    // 4. obtain (or warn) a SnapshotManager for this dataset.
    let acceleration_layout = get_acceleration_layout(dataset)
        .await
        .context(SnapshotRefreshModeLayoutUnavailableSnafu)?;
    if !acceleration_layout.is_enabled() {
        return Err(Error::SnapshotRefreshModeManagerUnavailable);
    }

    let manager = SnapshotManager::try_new(
        dataset.name.to_string(),
        acceleration_settings.snapshot_behavior.clone(),
        acceleration_layout,
        acceleration_engine,
    )
    .await
    .ok_or(Error::SnapshotRefreshModeManagerUnavailable)?;
    // Apply any engine-specific snapshot-engine override (e.g. CayenneSnapshotEngine).
    let manager = match accelerator.snapshot_engine_for_source(dataset).await {
        Some(engine) => manager.with_snapshot_engine(engine),
        None => manager,
    };
    // Build a checkpointer factory mirroring the bootstrap path so the
    // refresh-time `download_latest_snapshot` call can succeed (it requires a
    // factory to materialize a checkpoint for restore).
    let source_for_checkpointer: Arc<dyn crate::dataaccelerator::AccelerationSource> =
        Arc::new(dataset.clone());
    let snapshot_behavior_for_checkpointer = acceleration_settings.snapshot_behavior.clone();
    let checkpoint_factory =
        runtime_acceleration::dataset_checkpoint::make_checkpointer_factory(move || {
            let source = Arc::clone(&source_for_checkpointer);
            let snapshot_behavior = snapshot_behavior_for_checkpointer.clone();
            async move {
                use crate::dataaccelerator::spice_sys::OpenOption;
                use crate::dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint;
                use snafu::ResultExt;
                DatasetCheckpoint::try_new(source.as_ref(), OpenOption::OpenExisting)
                    .await
                    .boxed()
                    .map(|checkpoint| {
                        checkpoint
                            .with_snapshot_behavior(snapshot_behavior)
                            .to_arc()
                    })
            }
        });
    let manager = manager
        .with_snapshots_creation_policy(acceleration_settings.snapshots_creation_policy)
        .with_checkpointer_factory(checkpoint_factory);
    let manager = Arc::new(manager);

    // 5. clone everything the reload factory needs into 'static state.
    let registry = Arc::clone(&df.accelerator_engine_registry);
    let dataset_owned = Arc::new(dataset.clone());
    let acceleration_settings_owned = acceleration_settings.clone();
    let ctx_owned = Arc::clone(&df.ctx);
    let secrets_for_factory = Arc::clone(&secrets);
    let table_name = dataset.name.clone();
    let schema_for_factory = Arc::clone(&refresh_schema);
    let constraints_for_factory = constraints;

    let provider_factory: ReloadProviderFactory = Arc::new(move || {
        let registry = Arc::clone(&registry);
        let dataset_owned = Arc::clone(&dataset_owned);
        let acceleration_settings_owned = acceleration_settings_owned.clone();
        let ctx_owned = Arc::clone(&ctx_owned);
        let secrets_for_factory = Arc::clone(&secrets_for_factory);
        let table_name = table_name.clone();
        let schema_for_factory = Arc::clone(&schema_for_factory);
        let constraints_for_factory = constraints_for_factory.clone();
        Box::pin(async move {
            registry
                .create_accelerator_table(
                    table_name,
                    schema_for_factory,
                    constraints_for_factory.as_ref(),
                    &acceleration_settings_owned,
                    secrets_for_factory,
                    Some(dataset_owned.as_ref()),
                    ctx_owned,
                )
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
        })
    });

    let swappable_provider = SwappableTableProvider::new(initial_provider);
    let current_snapshot_id = std::sync::Arc::new(std::sync::Mutex::new(bootstrap_loaded_id));

    Ok(SnapshotRefreshState {
        manager,
        accelerator,
        source: Arc::new(dataset.clone()),
        swappable_provider,
        provider_factory,
        current_snapshot_id,
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use cache::{SimpleCache, key::CacheKey};
    use datafusion::datasource::MemTable;

    use crate::builder::RuntimeBuilder;

    use super::*;

    fn streaming_broadcast_test_batch(value: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![value])) as arrow::array::ArrayRef],
        )
        .expect("test record batch should be valid")
    }

    #[test]
    fn test_streaming_broadcast_buffer_records_within_limit() {
        let mut buffer = StreamingBroadcastBuffer::default();

        assert!(!buffer.push(&streaming_broadcast_test_batch(1)));

        let batches = buffer.batches().expect("buffer should be publishable");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(buffer.bytes, batches[0].get_array_memory_size());
    }

    #[test]
    fn test_streaming_broadcast_buffer_disables_when_batch_limit_exceeded() {
        let mut buffer = StreamingBroadcastBuffer::default();

        for value in 0..MAX_STREAMING_BROADCAST_BATCHES {
            assert!(!buffer.push(&streaming_broadcast_test_batch(
                i32::try_from(value).expect("test value fits in i32")
            )));
        }

        assert!(buffer.push(&streaming_broadcast_test_batch(999)));
        assert!(buffer.batches().is_none());
        assert!(!buffer.push(&streaming_broadcast_test_batch(1000)));
    }

    #[tokio::test]
    async fn test_normalize_table_reference_expands_unique_bare_reference() {
        let runtime = RuntimeBuilder::new().build().await;
        let df = DataFusion::builder(
            status::RuntimeStatus::new(),
            runtime.accelerator_engine_registry(),
            Handle::current(),
        )
        .build();
        let table_reference =
            TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "cdc_table");
        let table = Arc::new(
            MemTable::try_new(streaming_broadcast_test_batch(1).schema(), vec![vec![]])
                .expect("mem table should be created"),
        );

        df.ctx
            .register_table(table_reference.clone(), table)
            .expect("table should be registered");

        assert_eq!(
            df.normalize_table_reference(TableReference::bare("cdc_table")),
            table_reference
        );
    }

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

        df.get_or_create_logical_plan(&session, Some(&raw_cache_key), SQL)
            .await
            .expect("logical plan");

        let Some(cache_provider) = df.plans_cache_provider() else {
            unreachable!("Cache provider should be available");
        };

        cache_provider.checkpoint().await; // Ensure entry gets logged
        assert_eq!(cache_provider.item_count().await, 1);
        drop(cache_provider);

        // Reusing the same query should no longer at to the cache
        df.get_or_create_logical_plan(&session, Some(&raw_cache_key), SQL)
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
                full_text_search: None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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

    mod validate_distributed_engine_tests {
        use super::super::*;
        use crate::config::{ClusterConfig, ClusterRole};

        fn make_cluster_config(role: ClusterRole) -> ResolvedClusterConfig {
            let config = ClusterConfig {
                role: Some(role),
                allow_insecure_connections: true,
                node_advertise_address: Some("127.0.0.1".to_string()),
                ..ClusterConfig::default()
            };
            ResolvedClusterConfig::try_new(config).expect("valid test cluster config")
        }

        fn make_non_distributed_config() -> ResolvedClusterConfig {
            ResolvedClusterConfig::try_new(ClusterConfig::default())
                .expect("valid default cluster config")
        }

        #[test]
        fn arrow_allowed_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            validate_distributed_engine(&config, Engine::Arrow, "ds")
                .expect("arrow engine should be allowed in distributed mode");
        }

        #[test]
        fn partitioned_arrow_allowed_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            validate_distributed_engine(&config, Engine::PartitionedArrow, "ds")
                .expect("partitioned_arrow engine should be allowed in distributed mode");
        }

        #[test]
        fn cayenne_allowed_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            validate_distributed_engine(&config, Engine::Cayenne, "ds")
                .expect("cayenne engine should be allowed in distributed mode");
        }

        #[test]
        fn duckdb_rejected_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            let result = validate_distributed_engine(&config, Engine::DuckDB, "my_dataset");
            assert!(
                matches!(
                    result,
                    Err(Error::UnsupportedDistributedAccelerationEngine { .. })
                ),
                "Expected UnsupportedDistributedAccelerationEngine, got: {result:?}",
            );
        }

        #[test]
        fn sqlite_rejected_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Executor);
            let result = validate_distributed_engine(&config, Engine::Sqlite, "my_dataset");
            assert!(
                matches!(
                    result,
                    Err(Error::UnsupportedDistributedAccelerationEngine { .. })
                ),
                "Expected UnsupportedDistributedAccelerationEngine, got: {result:?}",
            );
        }

        #[test]
        fn postgresql_rejected_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            let result = validate_distributed_engine(&config, Engine::PostgreSQL, "my_dataset");
            assert!(
                matches!(
                    result,
                    Err(Error::UnsupportedDistributedAccelerationEngine { .. })
                ),
                "Expected UnsupportedDistributedAccelerationEngine, got: {result:?}",
            );
        }

        #[test]
        fn turso_rejected_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            let result = validate_distributed_engine(&config, Engine::Turso, "my_dataset");
            assert!(
                matches!(
                    result,
                    Err(Error::UnsupportedDistributedAccelerationEngine { .. })
                ),
                "Expected UnsupportedDistributedAccelerationEngine, got: {result:?}",
            );
        }

        #[test]
        fn partitioned_duckdb_rejected_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            let result =
                validate_distributed_engine(&config, Engine::PartitionedDuckDB, "my_dataset");
            assert!(
                matches!(
                    result,
                    Err(Error::UnsupportedDistributedAccelerationEngine { .. })
                ),
                "Expected UnsupportedDistributedAccelerationEngine, got: {result:?}",
            );
        }

        #[test]
        fn table_mode_partitioned_duckdb_rejected_in_distributed_mode() {
            let config = make_cluster_config(ClusterRole::Scheduler);
            let result = validate_distributed_engine(
                &config,
                Engine::TableModePartitionedDuckDB,
                "my_dataset",
            );
            assert!(
                matches!(
                    result,
                    Err(Error::UnsupportedDistributedAccelerationEngine { .. })
                ),
                "Expected UnsupportedDistributedAccelerationEngine, got: {result:?}",
            );
        }

        #[test]
        fn any_engine_allowed_in_non_distributed_mode() {
            let config = make_non_distributed_config();
            validate_distributed_engine(&config, Engine::DuckDB, "ds")
                .expect("duckdb should be allowed when not in distributed mode");
            validate_distributed_engine(&config, Engine::Sqlite, "ds")
                .expect("sqlite should be allowed when not in distributed mode");
            validate_distributed_engine(&config, Engine::PostgreSQL, "ds")
                .expect("postgresql should be allowed when not in distributed mode");
            validate_distributed_engine(&config, Engine::Turso, "ds")
                .expect("turso should be allowed when not in distributed mode");
            validate_distributed_engine(&config, Engine::PartitionedDuckDB, "ds")
                .expect("partitioned_duckdb should be allowed when not in distributed mode");
            validate_distributed_engine(&config, Engine::TableModePartitionedDuckDB, "ds").expect(
                "table_mode_partitioned_duckdb should be allowed when not in distributed mode",
            );
            validate_distributed_engine(&config, Engine::Arrow, "ds")
                .expect("arrow should be allowed when not in distributed mode");
            validate_distributed_engine(&config, Engine::Cayenne, "ds")
                .expect("cayenne should be allowed when not in distributed mode");
        }
    }

    mod remap_constraints_tests {
        use super::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{Constraint, Constraints};

        fn schema(fields: &[&str]) -> SchemaRef {
            Arc::new(Schema::new(
                fields
                    .iter()
                    .map(|name| Field::new(*name, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            ))
        }

        #[test]
        fn remap_pk_with_reordered_columns() {
            // Source: id(0), created_at(1), email(2)
            // Refresh: email(0), id(1)
            // Source PK: [0] (id) → Refresh PK: [1] (id)
            let source = schema(&["id", "created_at", "email"]);
            let refresh = schema(&["email", "id"]);
            let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result,
                Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                    vec![1]
                )]))
            );
        }

        #[test]
        fn remap_composite_pk() {
            // Source: id(0), org_id(1), name(2), email(3)
            // Refresh: name(0), id(1), org_id(2)
            // Source PK: [0, 1] (id, org_id) → Refresh PK: [1, 2]
            let source = schema(&["id", "org_id", "name", "email"]);
            let refresh = schema(&["name", "id", "org_id"]);
            let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result,
                Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                    vec![1, 2]
                )]))
            );
        }

        #[test]
        fn remap_pk_missing_column_returns_none() {
            // Source: id(0), name(1), email(2)
            // Refresh: name(0), email(1) — PK column "id" is missing
            let source = schema(&["id", "name", "email"]);
            let refresh = schema(&["name", "email"]);
            let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(result, None);
        }

        #[test]
        fn remap_same_schema_preserves_indices() {
            // Source and refresh have the same schema
            let source = schema(&["id", "email"]);
            let refresh = schema(&["id", "email"]);
            let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result,
                Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                    vec![0]
                )]))
            );
        }

        #[test]
        fn remap_unique_constraint() {
            let source = schema(&["id", "email", "name"]);
            let refresh = schema(&["name", "email"]);
            let constraints = Constraints::new_unverified(vec![Constraint::Unique(vec![1])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result,
                Some(Constraints::new_unverified(vec![Constraint::Unique(vec![
                    1
                ])]))
            );
        }

        #[test]
        fn remap_no_constraints_returns_none() {
            let source = schema(&["id", "email"]);
            let refresh = schema(&["id", "email"]);
            let constraints = Constraints::new_unverified(vec![]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(result, None);
        }

        #[test]
        fn remap_debezium_refresh_sql_scenario() {
            // Simulates the exact scenario from issue #9035:
            // Debezium source: users table with all columns
            // refresh_sql: SELECT id, email FROM users
            let source = schema(&[
                "id",
                "created_at",
                "updated_at",
                "name",
                "email",
                "password_hash",
            ]);
            let refresh = schema(&["id", "email"]);
            // Debezium sets PK on "id" column, index 0 in source schema
            let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            // "id" is at index 0 in the refresh schema too
            assert_eq!(
                result,
                Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                    vec![0]
                )]))
            );
        }

        #[test]
        fn remap_mixed_pk_and_unique_with_missing_pk_returns_none() {
            // If PK can't be remapped, return None even if Unique can be remapped.
            // This prevents downstream code from seeing Unique-only constraints
            // and incorrectly deriving on_conflict upsert behavior.
            let source = schema(&["id", "email", "name"]);
            let refresh = schema(&["email", "name"]);
            let constraints = Constraints::new_unverified(vec![
                Constraint::PrimaryKey(vec![0]), // "id" - missing from refresh
                Constraint::Unique(vec![1]),     // "email" - present in refresh
            ]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result, None,
                "Should return None when PK cannot be remapped"
            );
        }

        #[test]
        fn remap_out_of_bounds_index_returns_none() {
            // Constraint with out-of-bounds index should not panic
            let source = schema(&["id", "email"]);
            let refresh = schema(&["id", "email"]);
            let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![99])]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result, None,
                "Out-of-bounds index should return None, not panic"
            );
        }

        #[test]
        fn remap_mixed_pk_and_unique_both_present() {
            // When both PK and Unique can be remapped, both should be preserved
            let source = schema(&["id", "email", "name"]);
            let refresh = schema(&["name", "id", "email"]);
            let constraints = Constraints::new_unverified(vec![
                Constraint::PrimaryKey(vec![0]), // "id" → index 1 in refresh
                Constraint::Unique(vec![1]),     // "email" → index 2 in refresh
            ]);

            let result = remap_constraints_to_refresh_schema(&constraints, &source, &refresh);
            assert_eq!(
                result,
                Some(Constraints::new_unverified(vec![
                    Constraint::PrimaryKey(vec![1]),
                    Constraint::Unique(vec![2]),
                ]))
            );
        }
    }

    mod strip_outer_parens_tests {
        use super::super::strip_outer_parens;

        #[test]
        fn strip_outer_parens_cases() {
            // Primary case: catalog stores "(bucket(10, foo))" and we want "bucket(10, foo)"
            assert_eq!(
                strip_outer_parens("(bucket(10, foo))".to_string()),
                "bucket(10, foo)"
            );

            // Expression that is already bare must not be corrupted
            assert_eq!(
                strip_outer_parens("bucket(10, foo)".to_string()),
                "bucket(10, foo)"
            );

            assert_eq!(strip_outer_parens("foo".to_string()), "foo");
            assert_eq!(strip_outer_parens("(foo".to_string()), "(foo");
            assert_eq!(strip_outer_parens("foo)".to_string()), "foo)");
        }
    }
}
