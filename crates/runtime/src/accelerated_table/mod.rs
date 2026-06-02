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

use crate::config::ClusterRole;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{any::Any, sync::Arc, time::Duration};

use crate::component::dataset::acceleration::{RefreshMode, RefreshOnStartup, ZeroResultsAction};
use crate::component::dataset::{ReadyState, TimeFormat};
use crate::dataaccelerator::{BootstrapStatus, get_primary_keys_from_constraints};
use crate::datafusion::error::{SpiceExternalError, format_datafusion_error};
use crate::datafusion::is_spice_internal_dataset;
use crate::datafusion::udf::deny_spice_specific_functions;
use crate::federated_table::FederatedTable;
use crate::status;
use ::cache::Caching;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
};
use opentelemetry::KeyValue;
use refresh::RefreshOverrides;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_datafusion::execution_plan::fallback_on_zero_results::FallbackAsyncTableProvider;
use runtime_datafusion::execution_plan::{
    TableScanParams, fallback_on_zero_results::FallbackOnZeroResultsScanExec,
    schema_cast::SchemaCastScanExec, wrap_with_filter,
};

use snafu::prelude::*;
use spicepod::metric::Metrics;
use synchronized_table::SynchronizedTable;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, mpsc};
use tokio::task::JoinHandle;

pub mod caching;
pub mod federation;
mod metrics;
pub mod refresh;
pub mod refresh_task;
mod refresh_task_runner;
mod retention;
pub(crate) mod sink;
pub(crate) mod snapshots;
mod synchronized_table;
mod timestamp_metrics_utils;
pub mod write;

pub(crate) use write::WriteMode;

pub use refresh_task_runner::RefreshTaskRunner;
pub use snapshots::SnapshotCreationConfig;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to fetch data from the data connector: {}. Ensure the dataset source path and connector configuration are valid, and try again.",
        format_datafusion_error(source)
    ))]
    UnableToGetDataFromConnector { source: DataFusionError },

    #[snafu(display(
        "Failed to refresh dataset from the data connector: {}. Verify the dataset configuration and data connector status, and try again.",
        format_datafusion_error(source)
    ))]
    FailedToRefreshDataset { source: DataFusionError },

    #[snafu(display(
        "Failed to scan the dataset from the data connector: {}. Ensure the dataset configuration is valid, and try again.",
        format_datafusion_error(source)
    ))]
    UnableToScanTableProvider { source: DataFusionError },

    #[snafu(display(
        "Failed to apply data update to the accelerated dataset: {}. Ensure the dataset schema is compatible, and try again.",
        format_datafusion_error(source)
    ))]
    UnableToCreateMemTableFromUpdate { source: DataFusionError },

    #[snafu(display(
        "Failed to refresh dataset {dataset_name}: refresh worker panicked. {message}"
    ))]
    RefreshWorkerPanicked {
        dataset_name: String,
        message: String,
    },

    #[snafu(display(
        "Failed to trigger dataset refresh: the refresh worker is no longer running. {source}"
    ))]
    FailedToTriggerRefresh {
        source: tokio::sync::mpsc::error::SendError<Option<RefreshOverrides>>,
    },

    #[snafu(display(
        "Manual refresh is not supported for `append` mode. Only `full` refresh mode supports manual refreshes."
    ))]
    ManualRefreshIsNotSupported {},

    #[snafu(display(
        "A refresh must be triggered on the dataset '{parent_dataset}', which will propagate to this table."
    ))]
    RefreshNotSupportedForChildTable { parent_dataset: TableReference },

    #[snafu(display(
        "Failed to find latest timestamp in accelerated table: {}. Is the 'time_column' parameter correct?",
        format_datafusion_error(source)
    ))]
    FailedToQueryLatestTimestamp { source: DataFusionError },

    #[snafu(display("{reason}"))]
    FailedToFindLatestTimestamp { reason: String },

    #[snafu(display("Failed to filter update data for the accelerated dataset: {source}"))]
    FailedToFilterUpdates { source: ArrowError },

    #[snafu(display(
        "Failed to write data into the accelerated dataset: {}",
        format_datafusion_error(source)
    ))]
    FailedToWriteData { source: DataFusionError },

    #[snafu(display(
        "The accelerated table does not support delete operations. Use a different acceleration engine which supports delete operations. For details, visit: https://spiceai.org/docs/components/data-accelerators"
    ))]
    AcceleratedTableDoesntSupportDelete {},

    #[snafu(display(
        "Expected the schema to have field '{field_name}', but it did not. Spice found the schema: {schema} Is the primary key configuration correct?"
    ))]
    PrimaryKeyExpectedSchemaToHaveField {
        field_name: String,
        schema: SchemaRef,
    },

    #[snafu(display(
        "Expected the field in schema '{field_name}' to have type '{expected_data_type}', but it did not. Spice found the schema: {schema} Is the primary key configuration correct?"
    ))]
    PrimaryKeyArrayDataTypeMismatch {
        field_name: String,
        expected_data_type: String,
        schema: SchemaRef,
    },

    #[snafu(display(
        "The type of the primary key '{data_type}' is not yet supported for change deletion. Use a different primary key or change the data type."
    ))]
    PrimaryKeyTypeNotYetSupported { data_type: String },

    #[snafu(display(
        "Primary key column '{field_name}' contains a NULL value in a CDC change record. NULL primary keys cannot be used for delete or upsert operations."
    ))]
    PrimaryKeyNullValue { field_name: String },

    #[snafu(display("Invalid time column format: {source}"))]
    InvalidTimeColumnTimeFormat { source: refresh::Error },

    #[snafu(display("Failed to start dataset refresh: the refresh task was already started."))]
    RefreshTaskAlreadyStarted {},

    #[snafu(display("Failed to construct data for the accelerated dataset: {source}"))]
    FailedToBuildRecordBatch { source: ArrowError },

    #[snafu(display("Failed to process upsert batch for dataset {dataset_name}: {reason}"))]
    InvalidUpsertPrimaryKeys {
        dataset_name: String,
        reason: String,
    },

    #[snafu(display("No primary keys defined for dataset {dataset_name}"))]
    NoPrimaryKeysDefined { dataset_name: String },

    #[snafu(transparent)]
    PkFilterExpr {
        source: data_components::pk_filter_expr::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum AcceleratedTableBuilderError {
    #[snafu(display(
        "A changes stream is required when `refresh_mode` is set to `changes`. For details, visit: https://spiceai.org/docs/features/cdc"
    ))]
    ExpectedChangesStream,

    #[snafu(display(
        "An append stream is required when `refresh_mode` is set to `append` without a `time_column`. For details, visit: https://spiceai.org/docs/components/data-accelerators/data-refresh#append"
    ))]
    AppendStreamRequired,

    #[snafu(display(
        "Append mode requires either `time_column` or `primary_key` to be specified in the dataset configuration. For details, visit: https://spiceai.org/docs/components/data-accelerators/data-refresh#append"
    ))]
    NeitherTimeColumnNorPrimaryKey,

    #[snafu(display(
        "A synchronized accelerated table requires full or caching refresh mode. Set `refresh_mode` to 'full' or 'caching', and try again."
    ))]
    SynchronizedAcceleratedTableRequiresFullOrCachingRefresh,

    #[snafu(display(
        "Refresh mode must be set to `changes` to use a changes stream. For details, visit: https://spiceai.org/docs/features/cdc"
    ))]
    ExpectedChangesModeForChangesStream,

    #[snafu(display(
        "Refresh mode must be set to `append` to use an append stream. For details, visit: https://spiceai.org/docs/components/data-accelerators/data-refresh#append"
    ))]
    ExpectedAppendModeForAppendStream,

    #[snafu(transparent)]
    AcceleratedTableError { source: Error },
}

pub type AcceleratedTableBuilderResult<T> = std::result::Result<T, AcceleratedTableBuilderError>;

// An accelerated table consists of a federated table and a local accelerator.
//
// The accelerator must support inserts.
// AcceleratedTable::new returns an instance of the table and a oneshot receiver that will be triggered when the table is ready, right after the initial data refresh finishes.
pub struct AcceleratedTable {
    dataset_name: TableReference,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
    refresh_trigger: Option<mpsc::Sender<Option<RefreshOverrides>>>,

    // Async background tasks relevant to the accelerated table (i.e should be stopped when the table is dropped).
    pub(crate) handlers: Vec<JoinHandle<()>>,
    zero_results_action: ZeroResultsAction,
    ready_state: ReadyState,
    refresh_params: Arc<RwLock<refresh::Refresh>>,
    refresh_mode: RefreshMode,
    refresher: Arc<refresh::Refresher>,
    disable_federation: bool,
    /// Controls where writes (INSERT INTO) are directed.
    write_mode: WriteMode,
    synchronized_with: Option<SynchronizedTable>,
    /// Child accelerators that should receive cached data when this parent stores new cache entries (caching mode only)
    synchronized_children: Arc<RwLock<Vec<Arc<dyn TableProvider>>>>,
    cache_ttl: Option<Duration>,
    cache_stale_while_revalidate_ttl: Option<Duration>,
    cache_stale_if_error: bool,
    io_runtime: Handle,
    /// Mutex to protect concurrent access to the accelerator during cache/snapshot operations
    accelerator_write_mutex: Arc<Mutex<()>>,
    /// Tracks in-flight revalidation requests to avoid duplicate upstream requests during SWR window
    in_flight_revalidations: caching::InFlightRevalidations,
    /// Timestamp (milliseconds since epoch) of the last `insert_into` operation.
    /// `None` if no insert has occurred yet (and no bootstrap timestamp was provided).
    /// Shared with `RefreshTask`
    last_updated_at: Arc<AtomicI64>,
    /// Sender for batched cache writes. Only used in caching refresh mode.
    batch_write_tx: Option<caching::CacheWriteSender>,
    cluster_role: Option<ClusterRole>,
    /// Schema exposed to user-facing query planning when it differs from the
    /// underlying accelerator's storage schema. Currently set only in caching
    /// mode, where the storage schema is augmented with a hidden
    /// [`caching::CACHE_NAMESPACE_COLUMN`] for per-principal isolation.
    user_facing_schema: Option<SchemaRef>,
}

impl std::fmt::Debug for AcceleratedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceleratedTable")
            .field("dataset_name", &self.dataset_name)
            .field("accelerator", &self.accelerator)
            .field("federated", &self.federated)
            .field("zero_results_action", &self.zero_results_action)
            .field("ready_state", &self.ready_state)
            .field("refresh_params", &self.refresh_params)
            .field("disable_federation", &self.disable_federation)
            .field("write_mode", &self.write_mode)
            .field("synchronized_with", &self.synchronized_with)
            .finish_non_exhaustive()
    }
}

fn validate_refresh_data_window(
    refresh: &refresh::Refresh,
    dataset: &TableReference,
    schema: &SchemaRef,
) {
    if refresh.period.is_some() {
        if let Some(time_column) = &refresh.time_column {
            if schema.column_with_name(time_column).is_none() {
                tracing::warn!(
                    "No matching column {time_column} found in the source table, refresh_data_window will be ignored for dataset {dataset}"
                );
            }
        } else {
            tracing::warn!(
                "No time_column was provided, refresh_data_window will be ignored for {dataset}"
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotCreateTrigger {
    RefreshComplete,
    Interval(Duration),
    Batches(i64),
}

#[expect(clippy::struct_excessive_bools)]
pub struct Builder {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    federated: Arc<FederatedTable>,
    federated_source: String,
    accelerator: Arc<dyn TableProvider>,
    refresh: refresh::Refresh,
    retention: Option<Retention>,
    zero_results_action: ZeroResultsAction,
    refresh_on_startup: RefreshOnStartup,
    ready_state: ReadyState,
    caching: Option<Arc<Caching>>,
    changes_stream: Option<ChangesStream>,
    append_stream: Option<ChangesStream>,
    disable_federation: bool,
    write_to_accelerator_only: bool,
    dual_write: bool,
    write_back: bool,
    refresh_semaphore: Option<Arc<Semaphore>>,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    synchronize_with: Option<SynchronizedTable>,
    initial_load_complete: bool,
    snapshot_creation_config: Option<SnapshotCreationConfig>,
    /// Per-dataset state for `RefreshMode::Snapshot`. Required when the
    /// refresh mode is Snapshot; ignored otherwise.
    snapshot_refresh_state: Option<snapshots::SnapshotRefreshState>,
    metrics: Option<Metrics>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    caching_ttl: Option<Duration>,
    caching_stale_while_revalidate_ttl: Option<Duration>,
    caching_stale_if_error: bool,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
    bootstrap_status: BootstrapStatus,
    /// Whether the acceleration uses S3 Express One Zone storage.
    is_s3_express_acceleration: bool,
    acceleration_layout: Option<runtime_acceleration::snapshot::AccelerationLayout>,
    cluster_role: Option<ClusterRole>,
    user_facing_schema: Option<SchemaRef>,
    accelerator_write_mutex: Arc<Mutex<()>>,
}

impl Builder {
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: String,
        accelerator: Arc<dyn TableProvider>,
        refresh: refresh::Refresh,
        io_runtime: Handle,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            accelerator,
            refresh,
            retention: None,
            zero_results_action: ZeroResultsAction::default(),
            refresh_on_startup: RefreshOnStartup::default(),
            ready_state: ReadyState::default(),
            caching: None,
            changes_stream: None,
            append_stream: None,
            checkpointer: None,
            synchronize_with: None,
            disable_federation: false,
            write_to_accelerator_only: false,
            dual_write: false,
            write_back: false,
            initial_load_complete: false,
            refresh_semaphore: None,
            snapshot_creation_config: None,
            snapshot_refresh_state: None,
            metrics: None,
            cpu_runtime: None,
            io_runtime,
            caching_ttl: None,
            caching_stale_while_revalidate_ttl: None,
            caching_stale_if_error: false,
            resource_monitor: None,
            bootstrap_status: BootstrapStatus::none(),
            acceleration_layout: None,
            is_s3_express_acceleration: false,
            cluster_role: None,
            accelerator_write_mutex: Arc::new(Mutex::new(())), // can be overridden
            user_facing_schema: None,
        }
    }

    /// Override the schema reported by the resulting [`AcceleratedTable`] to
    /// query planners. Used by caching mode to hide the internal namespace
    /// storage column from users.
    pub fn user_facing_schema(&mut self, schema: SchemaRef) -> &mut Self {
        self.user_facing_schema = Some(schema);
        self
    }

    pub fn cluster_role(&mut self, role: Option<ClusterRole>) -> &mut Self {
        self.cluster_role = role;
        self
    }

    pub fn acceleration_layout(
        &mut self,
        layout: runtime_acceleration::snapshot::AccelerationLayout,
    ) -> &mut Self {
        self.acceleration_layout = Some(layout);
        self
    }

    pub fn retention(&mut self, retention: Option<Retention>) -> &mut Self {
        self.retention = retention;
        self
    }

    pub fn zero_results_action(&mut self, zero_results_action: ZeroResultsAction) -> &mut Self {
        self.zero_results_action = zero_results_action;
        self
    }

    pub fn refresh_on_startup(&mut self, refresh_on_startup: RefreshOnStartup) -> &mut Self {
        self.refresh_on_startup = refresh_on_startup;
        self
    }

    pub fn ready_state(&mut self, ready_state: ReadyState) -> &mut Self {
        self.ready_state = ready_state;
        self
    }

    pub fn caching(&mut self, caching: Option<Arc<Caching>>) -> &mut Self {
        self.caching = caching;
        self
    }

    pub fn disable_federation(&mut self) -> &mut Self {
        self.disable_federation = true;
        self
    }

    /// Returns a clone of the accelerator `Arc`.
    #[must_use]
    pub fn get_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.accelerator)
    }

    /// Replace the accelerator provider.
    ///
    /// This must be called **before** [`build`](Self::build) so that the
    /// refresher (created during build) receives the updated provider.
    pub fn set_accelerator(&mut self, accelerator: Arc<dyn TableProvider>) {
        self.accelerator = accelerator;
    }

    /// Set to only write to the accelerator (not replicate to federated source).
    /// This is used when `on_conflict` is configured - writes go only to the accelerator.
    pub fn write_to_accelerator_only(&mut self) -> &mut Self {
        self.write_to_accelerator_only = true;
        self
    }

    /// Enable dual-write mode: writes go simultaneously to both the federated source
    /// and the local Cayenne accelerator using staged append/commit/rollback semantics.
    /// Reserved for the Iceberg federated catalog cache path — not driven by the
    /// user-facing `write_mode: write_through` setting.
    pub fn dual_write(&mut self) -> &mut Self {
        self.dual_write = true;
        self
    }

    /// Enable write-back mode: writes commit to the local accelerator first,
    /// then asynchronously persist to the federated source.
    pub fn write_back(&mut self) -> &mut Self {
        self.write_back = true;
        self
    }

    pub fn refresh_semaphore(&mut self, refresh_semaphore: Arc<Semaphore>) -> &mut Self {
        self.refresh_semaphore = Some(refresh_semaphore);
        self
    }

    pub fn metrics(&mut self, metrics: Metrics) -> &mut Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn cpu_runtime(&mut self, runtime: Option<Handle>) -> &mut Self {
        self.cpu_runtime = runtime;
        self
    }

    pub fn with_resource_monitor(
        &mut self,
        monitor: crate::resource_monitor::ResourceMonitor,
    ) -> &mut Self {
        self.resource_monitor = Some(monitor);
        self
    }

    /// Set the changes stream for the accelerated table
    pub fn changes_stream(&mut self, changes_stream: ChangesStream) -> &mut Self {
        self.changes_stream = Some(changes_stream);
        self
    }

    /// Set the append stream for the accelerated table
    pub fn append_stream(&mut self, append_stream: ChangesStream) -> &mut Self {
        self.append_stream = Some(append_stream);
        self
    }

    /// Set the checkpointer for the accelerated table
    pub fn checkpointer(&mut self, checkpointer: Arc<dyn DatasetCheckpointer>) -> &mut Self {
        self.checkpointer = Some(checkpointer);
        self
    }

    /// Set the checkpointer for the accelerated table
    pub fn checkpointer_opt(
        &mut self,
        checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    ) -> &mut Self {
        self.checkpointer = checkpointer;
        self
    }

    /// Set the existing accelerated table to synchronize with.
    ///
    /// For Full refresh mode: A full table scan of the existing accelerated table is required
    /// to initialize a synchronized accelerated table after the initial load completes.
    ///
    /// For Caching refresh mode: The child accelerator will receive data whenever the parent
    /// stores new cache entries. The parent must also be in caching mode.
    ///
    /// Handling append/changes mode should be possible, but requires more care to ensure
    /// that delta updates are applied correctly after the initial table scan.
    pub async fn synchronize_with(
        &mut self,
        existing_accelerated_table: &AcceleratedTable,
    ) -> AcceleratedTableBuilderResult<&mut Self> {
        let child_mode = self.refresh.mode;
        let parent_mode = existing_accelerated_table.refresh_params.read().await.mode;

        // Both parent and child must use the same refresh mode (Full or Caching)
        let is_valid_sync = matches!(
            (child_mode, parent_mode),
            (RefreshMode::Full, RefreshMode::Full) | (RefreshMode::Caching, RefreshMode::Caching)
        );
        ensure!(
            is_valid_sync,
            SynchronizedAcceleratedTableRequiresFullOrCachingRefreshSnafu
        );

        let synchronized_table = SynchronizedTable::from(
            existing_accelerated_table,
            Arc::clone(&self.accelerator),
            self.dataset_name.clone(),
        );
        self.synchronize_with = Some(synchronized_table);
        Ok(self)
    }

    /// Tell the accelerated table that an initial load has already been completed, via a previous dataset checkpoint.
    ///
    /// This will allow the table to be marked as ready immediately.
    pub fn initial_load_complete(&mut self, initial_load_complete: bool) -> &mut Self {
        self.initial_load_complete = initial_load_complete;
        self
    }

    /// Configure whether snapshots are taken of the accelerated table after refreshes.
    pub fn snapshot_creation_config(
        &mut self,
        snapshot_config: Option<SnapshotCreationConfig>,
    ) -> &mut Self {
        self.snapshot_creation_config = snapshot_config;
        self
    }

    /// Configure per-dataset state for `RefreshMode::Snapshot`. Required when
    /// the refresh mode is Snapshot.
    pub fn snapshot_refresh_state(
        &mut self,
        state: Option<snapshots::SnapshotRefreshState>,
    ) -> &mut Self {
        self.snapshot_refresh_state = state;
        self
    }

    /// Set the TTL for cache mode
    pub fn caching_ttl(&mut self, ttl: Option<Duration>) -> &mut Self {
        self.caching_ttl = ttl;
        self
    }

    /// Set the stale-while-revalidate duration for cache mode
    pub fn caching_stale_while_revalidate_ttl(
        &mut self,
        stale_while_revalidate: Option<Duration>,
    ) -> &mut Self {
        self.caching_stale_while_revalidate_ttl = stale_while_revalidate;
        self
    }

    /// Set whether to serve expired data on upstream error in cache mode
    pub fn caching_stale_if_error(&mut self, enabled: bool) -> &mut Self {
        self.caching_stale_if_error = enabled;
        self
    }

    /// Set whether the dataset was bootstrapped from a snapshot.
    pub fn bootstrap_status(&mut self, bootstrap_status: BootstrapStatus) -> &mut Self {
        self.bootstrap_status = bootstrap_status;
        self
    }

    /// Set whether the acceleration uses S3 Express One Zone storage.
    pub fn s3_express_acceleration(&mut self, is_s3_express: bool) -> &mut Self {
        self.is_s3_express_acceleration = is_s3_express;
        self
    }

    /// Mutex to protect concurrent access to the accelerator during insert/update/delete/cache/snapshot operations
    /// Shared with `DataConnector`, `Refresher` and `CachingAccelerationScanExec`.
    pub fn accelerator_write_mutex(
        &mut self,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> &mut Self {
        self.accelerator_write_mutex = accelerator_write_mutex;
        self
    }

    /// Build the accelerated table
    pub async fn build(self) -> AcceleratedTableBuilderResult<AcceleratedTable> {
        if self.refresh.mode != RefreshMode::Changes && self.changes_stream.is_some() {
            return ExpectedChangesModeForChangesStreamSnafu.fail();
        }

        if self.refresh.mode != RefreshMode::Append && self.append_stream.is_some() {
            return ExpectedAppendModeForAppendStreamSnafu.fail();
        }

        let on_complete_notification = Arc::new(Notify::new());

        let (acceleration_refresh_mode, refresh_trigger) = match self.refresh.mode {
            RefreshMode::Disabled => (refresh::AccelerationRefreshMode::Disabled, None),
            RefreshMode::Append => {
                enum AppendMode {
                    TimeColumnOrPrimaryKey,
                    ChangesStream,
                }
                impl AppendMode {
                    fn try_new(
                        has_time_column: bool,
                        has_primary_key: bool,
                        has_append_stream: bool,
                    ) -> AcceleratedTableBuilderResult<Self> {
                        if has_append_stream {
                            Ok(AppendMode::ChangesStream)
                        } else if has_time_column || has_primary_key {
                            Ok(AppendMode::TimeColumnOrPrimaryKey)
                        } else {
                            NeitherTimeColumnNorPrimaryKeySnafu.fail()
                        }
                    }
                }

                let schema = self.accelerator.schema();
                let primary_keys = self
                    .accelerator
                    .constraints()
                    .map_or_else(Vec::new, |constraints| {
                        get_primary_keys_from_constraints(constraints, &schema)
                    });
                let has_primary_key = !primary_keys.is_empty();
                let has_time_column = self.refresh.time_column.is_some();
                let has_append_stream = self.append_stream.is_some();

                let append_mode =
                    AppendMode::try_new(has_time_column, has_primary_key, has_append_stream)?;

                // Log append mode configuration for debugging
                match (has_primary_key, has_time_column, has_append_stream) {
                    (_, _, true) => {
                        tracing::debug!(
                            dataset = %self.dataset_name,
                            "Append mode: using changes stream"
                        );
                    }
                    (true, true, false) => {
                        tracing::debug!(
                            dataset = %self.dataset_name,
                            ?primary_keys,
                            "Append mode: using time_column for incremental queries and primary_key for deduplication"
                        );
                    }
                    (true, false, false) => {
                        tracing::debug!(
                            dataset = %self.dataset_name,
                            ?primary_keys,
                            "Append mode: using primary_key for deduplication (full fetch each refresh)"
                        );
                    }
                    (false, true, false) => {
                        tracing::debug!(
                            dataset = %self.dataset_name,
                            "Append mode: using time_column for incremental queries"
                        );
                    }
                    (false, false, false) => {
                        // This case is handled by AppendMode::try_new returning an error
                    }
                }

                match append_mode {
                    AppendMode::ChangesStream => {
                        let Some(append_stream) = self.append_stream else {
                            return AppendStreamRequiredSnafu.fail();
                        };
                        (
                            refresh::AccelerationRefreshMode::Changes(append_stream),
                            None,
                        )
                    }
                    AppendMode::TimeColumnOrPrimaryKey => {
                        let (start_refresh, on_start_refresh) =
                            mpsc::channel::<Option<RefreshOverrides>>(1);
                        (
                            refresh::AccelerationRefreshMode::Append(on_start_refresh),
                            Some(start_refresh),
                        )
                    }
                }
            }
            RefreshMode::Full => {
                let (start_refresh, on_start_refresh) =
                    mpsc::channel::<Option<RefreshOverrides>>(1);
                (
                    refresh::AccelerationRefreshMode::Full(on_start_refresh),
                    Some(start_refresh),
                )
            }
            RefreshMode::Changes => {
                let Some(changes_stream) = self.changes_stream else {
                    return ExpectedChangesStreamSnafu.fail();
                };
                (
                    refresh::AccelerationRefreshMode::Changes(changes_stream),
                    None,
                )
            }
            RefreshMode::Caching => {
                // Cache mode supports manual refresh triggers to force refresh of stale data
                let (start_refresh, on_start_refresh) =
                    mpsc::channel::<Option<RefreshOverrides>>(1);
                (
                    refresh::AccelerationRefreshMode::Caching(on_start_refresh),
                    Some(start_refresh),
                )
            }
            RefreshMode::Snapshot => {
                // Snapshot mode is interval-driven and supports manual refresh triggers
                // to force a poll of the snapshot store outside the regular cadence.
                let (start_refresh, on_start_refresh) =
                    mpsc::channel::<Option<RefreshOverrides>>(1);
                (
                    refresh::AccelerationRefreshMode::Snapshot(on_start_refresh),
                    Some(start_refresh),
                )
            }
        };

        validate_refresh_data_window(&self.refresh, &self.dataset_name, &self.federated.schema());
        let refresh_mode = self.refresh.mode;
        let refresh_params = Arc::new(RwLock::new(self.refresh));
        // Create the in-flight revalidations tracker to avoid duplicate upstream requests during SWR window.
        let in_flight_revalidations: caching::InFlightRevalidations =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        // Create last_updated_at atomic to track insert_into timestamps, shared with Refresher for snapshots.
        // Initialize from bootstrap metadata if available.
        let last_updated_at = Arc::new(
            self.bootstrap_status
                .last_updated_at()
                .map_or(AtomicI64::new(0), AtomicI64::new),
        );
        let mut refresher = refresh::Refresher::new(
            Arc::clone(&self.runtime_status),
            self.dataset_name.clone(),
            Arc::clone(&self.federated),
            Some(self.federated_source),
            Arc::clone(&refresh_params),
            Arc::clone(&self.accelerator),
            self.cpu_runtime.clone(),
            self.io_runtime.clone(),
            Arc::clone(&self.accelerator_write_mutex),
        );
        refresher.with_completion_notifier(Arc::clone(&on_complete_notification));
        refresher.with_last_updated_at(Arc::clone(&last_updated_at));
        refresher.caching(&self.caching);
        refresher.checkpointer(self.checkpointer);
        refresher.refresh_on_startup(self.refresh_on_startup);
        refresher.set_initial_load_completed(self.initial_load_complete);
        refresher.disable_federation(self.disable_federation);
        refresher.with_metrics(self.metrics);
        if let Some(synchronize_with) = &self.synchronize_with {
            refresher.synchronize_with(synchronize_with.clone());
        }
        if let Some(semaphore) = self.refresh_semaphore {
            refresher.semaphore(semaphore);
        }

        refresher.with_snapshot_creation_config(self.snapshot_creation_config);
        refresher.with_snapshot_refresh_state(self.snapshot_refresh_state);
        refresher.set_bootstrap_status(self.bootstrap_status);

        if let Some(ref resource_monitor) = self.resource_monitor {
            refresher.with_resource_monitor(resource_monitor.clone());
        }

        refresher.with_s3_express_acceleration(self.is_s3_express_acceleration);

        let (refresh_handle, refresh_trigger) =
            if matches!(self.cluster_role, Some(ClusterRole::Scheduler)) {
                // Accelerated tables aren't loaded locally on the scheduler —
                // executors do. Don't start a refresh task, and leave the
                // dataset status as `Refreshing` (set by the caller before
                // this point). The scheduler flips the dataset to `Ready`
                // only after executors confirm via `PartitionsLoaded` acks
                // that every assigned partition is loaded; see
                // `runtime_cluster::PartitionLoadTracker` and the
                // `PartitionsLoaded` handler in `cluster::service`.
                // Previously this branch flipped the dataset to `Ready`
                // immediately, which made `/v1/ready` claim the cluster was
                // queryable before any data had been loaded on executors.
                //
                // `refresh_trigger` is None because the receiver will be
                // dropped (refresher.start() is not called).
                //
                // Notify completion waiters so the schedule-creation path
                // doesn't block waiting on a refresh that won't run here —
                // dataset readiness is a separate concern handled above.
                on_complete_notification.notify_waiters();
                (None, None)
            } else {
                (
                    refresher.start(acceleration_refresh_mode).await?,
                    refresh_trigger,
                )
            };
        let refresher = Arc::new(refresher);

        let mut handlers = vec![];
        if let Some(refresh_handle) = refresh_handle {
            handlers.push(refresh_handle);
        }

        // In caching mode, `on_zero_results` is effectively a no-op: the
        // caching scan already treats a zero-row accelerator result (whether
        // because the cache is empty or because the user's predicate
        // eliminated every cached row) as a cache miss and fetches the source.
        // That happens regardless of the configured `on_zero_results`, so the
        // default `return_empty` is misleading -- we always fall back to
        // source, not return empty. Warn so users don't reason about caching
        // mode through the lens of `on_zero_results`.
        if refresh_mode == RefreshMode::Caching {
            tracing::warn!(
                "Dataset {dataset}: `on_zero_results` is ignored when `refresh_mode: caching` is set. \
                 Caching mode always queries the source on a cache miss. \
                 Remove `on_zero_results` from the dataset configuration to silence this warning. \
                 For details, visit: https://spiceai.org/docs/components/data-accelerators/data-refresh#refresh-modes",
                dataset = self.dataset_name,
            );
        }

        // For caching mode, create the batched write channel and spawn consumer task.
        let batch_write_tx = if refresh_mode == RefreshMode::Caching {
            let (tx, rx) = caching::create_cache_write_channel();
            let consumer_handle = caching::spawn_batched_cache_write_task(
                rx,
                Arc::clone(&self.accelerator),
                self.dataset_name.to_string(),
                Arc::clone(&self.accelerator_write_mutex),
                Arc::clone(&in_flight_revalidations),
                Arc::clone(&last_updated_at),
            );
            // The consumer task will be automatically stopped (aborted) when AcceleratedTable is dropped
            handlers.push(consumer_handle);
            Some(tx)
        } else {
            None
        };

        if let Some(retention) = self.retention {
            let retention_check_handle = tokio::spawn(AcceleratedTable::start_retention_check(
                self.dataset_name.clone(),
                Arc::clone(&self.accelerator),
                retention,
                self.caching.clone(),
                self.io_runtime.clone(),
                Arc::clone(&self.accelerator_write_mutex),
            ));
            handlers.push(retention_check_handle);
        }

        // Spawn size metrics task for file-based accelerators
        if let Some(ref layout) = self.acceleration_layout
            && layout.is_enabled()
        {
            let size_metrics_handle = tokio::spawn(AcceleratedTable::start_size_metrics_task(
                self.dataset_name.clone(),
                layout.clone(),
            ));
            handlers.push(size_metrics_handle);
        }

        // If the table should be ready immediately, mark it as ready.
        // For `OnSchemaResolved`, the dataset is marked ready once the federated source's schema
        // has been resolved (its `TableProvider` has been successfully resolved, which also implies
        // access to the source has been verified). For an immediate federated table this has already
        // occurred synchronously before the builder ran, so we can mark it ready here. For a deferred
        // federated table we spawn a background task that waits for the deferred provider to resolve
        // before marking the dataset ready.
        match self.ready_state {
            ReadyState::OnRegistration => {
                self.runtime_status
                    .update_dataset(&self.dataset_name, status::ComponentStatus::Ready);
            }
            ReadyState::OnSchemaResolved => match &*self.federated {
                FederatedTable::Immediate(_) => {
                    self.runtime_status
                        .update_dataset(&self.dataset_name, status::ComponentStatus::Ready);
                }
                FederatedTable::Deferred(_) => {
                    let runtime_status = Arc::clone(&self.runtime_status);
                    let dataset_name = self.dataset_name.clone();
                    let federated = Arc::clone(&self.federated);
                    let wait_handle = tokio::spawn(async move {
                        // Wait for the deferred federated table provider to resolve. Only mark
                        // the dataset ready if the deferred provider actually connected (its
                        // schema was resolved and access was verified). If resolution failed
                        // (e.g. shutdown or task panic), `try_wait_table_provider` returns
                        // `Err(FederatedResolutionError::Unavailable, ..)`; leave the status
                        // untouched so the caller surfaces the error through the refresh path
                        // instead of a misleading `Ready`.
                        match federated.try_wait_table_provider().await {
                            Err((
                                crate::federated_table::FederatedResolutionError::Unavailable,
                                _,
                            )) => {
                                tracing::warn!(
                                    "Deferred federated provider for dataset {dataset_name} did not resolve successfully; leaving dataset status unchanged"
                                );
                            }
                            Ok(_) => {
                                // If the refresh path has already marked the dataset as `Error`
                                // (e.g. the initial refresh failed quickly), don't overwrite it
                                // with `Ready` — schema-resolution readiness must not mask refresh
                                // failures that are surfaced via dataset status and metrics.
                                let current_status = runtime_status
                                    .get_component_status(&format!("dataset:{dataset_name}"));
                                if matches!(current_status, Some(status::ComponentStatus::Error(_)))
                                {
                                    tracing::debug!(
                                        "Deferred federated provider for dataset {dataset_name} resolved successfully, but dataset status is already Error; leaving dataset status unchanged"
                                    );
                                } else {
                                    runtime_status.update_dataset(
                                        &dataset_name,
                                        status::ComponentStatus::Ready,
                                    );
                                }
                            }
                        }
                    });
                    handlers.push(wait_handle);
                }
            },
            ReadyState::OnLoad => {}
        }

        // For caching mode with synchronization, register the child with the parent immediately
        // so the parent can propagate cached data to this child.
        if refresh_mode == RefreshMode::Caching
            && let Some(synchronize_with) = &self.synchronize_with
        {
            synchronize_with.register_child_with_parent().await;
            tracing::info!(
                "Registered caching child {} with parent {}",
                self.dataset_name,
                synchronize_with.parent_dataset_name()
            );

            // Initialize child accelerator from parent's existing cached data.
            // This ensures the child has the parent's cache state when the parent
            // has existing data (e.g., from file-mode DuckDB restored from disk,
            // or from a snapshot bootstrap).
            let parent_accelerator = synchronize_with.parent_accelerator();
            match caching::CacheRefreshHelper::initialize_child_from_parent(
                &parent_accelerator,
                &self.accelerator,
                &self.dataset_name.to_string(),
            )
            .await
            {
                Ok(rows) if rows > 0 => {
                    tracing::info!(
                        "Initialized caching child {} with {} rows from parent {}",
                        self.dataset_name,
                        rows,
                        synchronize_with.parent_dataset_name()
                    );
                }
                Ok(_) => {
                    tracing::debug!(
                        "No existing data in parent {} to initialize child {}",
                        synchronize_with.parent_dataset_name(),
                        self.dataset_name
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to initialize caching child {} from parent {}: {}",
                        self.dataset_name,
                        synchronize_with.parent_dataset_name(),
                        e
                    );
                }
            }
        }

        let write_mode = if self.dual_write {
            WriteMode::resolve_dual_write(&self.accelerator, &self.federated)?
        } else if self.write_back {
            WriteMode::WriteBack
        } else if self.write_to_accelerator_only {
            WriteMode::AcceleratorOnly
        } else {
            WriteMode::WriteThrough
        };

        Ok(AcceleratedTable {
            dataset_name: self.dataset_name,
            accelerator: self.accelerator,
            federated: self.federated,
            refresh_trigger,
            handlers,
            zero_results_action: self.zero_results_action,
            ready_state: self.ready_state,
            refresh_params,
            refresh_mode,
            refresher,
            disable_federation: self.disable_federation,
            write_mode,
            synchronized_with: self.synchronize_with,
            synchronized_children: Arc::new(RwLock::new(Vec::new())),
            cache_ttl: self.caching_ttl,
            cache_stale_while_revalidate_ttl: self.caching_stale_while_revalidate_ttl,
            cache_stale_if_error: self.caching_stale_if_error,
            io_runtime: self.io_runtime,
            accelerator_write_mutex: self.accelerator_write_mutex,
            in_flight_revalidations,
            last_updated_at,
            batch_write_tx,
            cluster_role: self.cluster_role,
            user_facing_schema: self.user_facing_schema,
        })
    }
}

impl AcceleratedTable {
    pub fn builder(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: String,
        accelerator: Arc<dyn TableProvider>,
        refresh: refresh::Refresh,
        io_runtime: Handle,
    ) -> Builder {
        Builder::new(
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            accelerator,
            refresh,
            io_runtime,
        )
    }

    /// Periodically emits the `dataset_acceleration_size_bytes` metric for file-based accelerators.
    pub(crate) async fn start_size_metrics_task(
        dataset_name: TableReference,
        layout: runtime_acceleration::snapshot::AccelerationLayout,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            let size = layout.total_size();
            metrics::SIZE_BYTES.record(size, &[KeyValue::new("dataset", dataset_name.to_string())]);
        }
    }

    #[must_use]
    pub fn refresher(&self) -> Arc<refresh::Refresher> {
        Arc::clone(&self.refresher)
    }

    #[must_use]
    pub fn refresh_params(&self) -> Arc<RwLock<refresh::Refresh>> {
        Arc::clone(&self.refresh_params)
    }

    #[must_use]
    pub fn refresh_trigger(&self) -> Option<&mpsc::Sender<Option<RefreshOverrides>>> {
        match &self.synchronized_with {
            Some(_) => None,
            None => self.refresh_trigger.as_ref(),
        }
    }

    pub async fn trigger_refresh(&self, overrides: Option<RefreshOverrides>) -> Result<()> {
        if let Some(refresh_trigger) = self.refresh_trigger() {
            refresh_trigger
                .send(overrides)
                .await
                .context(FailedToTriggerRefreshSnafu)?;
        } else {
            if let Some(synchronized_with) = &self.synchronized_with {
                RefreshNotSupportedForChildTableSnafu {
                    parent_dataset: synchronized_with.parent_dataset_name(),
                }
                .fail()?;
            }
            ManualRefreshIsNotSupportedSnafu.fail()?;
        }

        Ok(())
    }

    #[must_use]
    pub fn get_federated_table(&self) -> Arc<FederatedTable> {
        Arc::clone(&self.federated)
    }

    #[must_use]
    pub fn get_federated_table_ref(&self) -> &Arc<FederatedTable> {
        &self.federated
    }

    #[must_use]
    pub fn is_dual_write(&self) -> bool {
        self.write_mode.is_dual_write()
    }

    #[must_use]
    pub fn get_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.accelerator)
    }

    #[must_use]
    pub(crate) fn get_accelerator_ref(&self) -> &Arc<dyn TableProvider> {
        &self.accelerator
    }

    /// Add a child accelerator that should receive cached data when this parent stores new cache entries.
    /// This is used for localpod caching synchronization.
    pub async fn add_synchronized_child(&self, child_accelerator: Arc<dyn TableProvider>) {
        self.synchronized_children
            .write()
            .await
            .push(child_accelerator);
    }

    /// Get the list of synchronized child accelerators for caching mode.
    #[must_use]
    pub fn synchronized_children(&self) -> Arc<RwLock<Vec<Arc<dyn TableProvider>>>> {
        Arc::clone(&self.synchronized_children)
    }

    pub async fn update_refresh_sql(&self, mut refresh_sql: refresh::RefreshSQL) -> Result<()> {
        let dataset_name = &self.dataset_name;

        let mut refresh = self.refresh_params.write().await;
        // Preserve existing partition_filters when updating user SQL
        let existing_partition_filters = refresh
            .sql
            .as_ref()
            .map(|s| s.partition_filters().to_vec())
            .unwrap_or_default();

        if !existing_partition_filters.is_empty() {
            refresh_sql.set_partition_filters(existing_partition_filters);
        }
        if !is_spice_internal_dataset(&self.dataset_name) {
            tracing::info!(
                "[refresh] Updated refresh SQL for {dataset_name} to {}",
                refresh_sql.display_sql()
            );
        }
        refresh.sql = Some(refresh_sql);

        Ok(())
    }

    /// Update only the partition filters on the refresh SQL, preserving user SQL parts.
    pub async fn update_partition_filters(
        &self,
        filters: Vec<datafusion_expr::Expr>,
    ) -> Result<()> {
        let mut refresh = self.refresh_params.write().await;
        if let Some(ref mut sql) = refresh.sql {
            sql.set_partition_filters(filters);
        } else {
            // No user SQL, but we still need partition filters.
            // Create a minimal RefreshSQL with All columns and only partition filters.
            let mut sql = crate::accelerated_table::refresh::RefreshSQL::new(
                self.dataset_name.clone(),
                crate::accelerated_table::refresh::RefreshSQLColumns::All,
                vec![],
                None,
            );
            sql.set_partition_filters(filters);
            refresh.sql = Some(sql);
        }
        Ok(())
    }

    /// Returns the subset of filters that the accelerator does not fully support
    /// (i.e., `Inexact` or `Unsupported`) and need to be re-applied after scanning.
    fn get_filters_to_reapply(&self, filters: &[Expr]) -> DataFusionResult<Vec<Expr>> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }

        let filter_refs: Vec<&Expr> = filters.iter().collect();
        let pushdown_support = self.accelerator.supports_filters_pushdown(&filter_refs)?;

        let filters_to_reapply: Vec<Expr> = filters
            .iter()
            .zip(pushdown_support.iter())
            .filter_map(|(filter, support)| match support {
                TableProviderFilterPushDown::Exact => None,
                TableProviderFilterPushDown::Inexact | TableProviderFilterPushDown::Unsupported => {
                    Some(filter.clone())
                }
            })
            .collect();

        Ok(filters_to_reapply)
    }

    fn update_last_updated_at(&self) {
        Self::set_timestamp_to_now(&self.last_updated_at);
    }

    /// Sets an `AtomicI64` timestamp to the current time in milliseconds.
    /// Used by both `AcceleratedTable` instance methods and the caching background task.
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn set_timestamp_to_now(last_updated_at: &AtomicI64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        last_updated_at.store(now_ms, Ordering::Release);
    }
}

impl Drop for AcceleratedTable {
    fn drop(&mut self) {
        for handler in self.handlers.drain(..) {
            handler.abort();
        }
    }
}

#[async_trait]
impl TableProvider for AcceleratedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.accelerator.constraints()
    }

    fn schema(&self) -> SchemaRef {
        if let Some(s) = self.user_facing_schema.as_ref() {
            return Arc::clone(s);
        }
        self.accelerator.schema()
    }

    fn table_type(&self) -> TableType {
        self.accelerator.table_type()
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.accelerator.statistics()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // In caching mode, we handle filters ourselves (not pushed to accelerator)
        // Return Inexact to indicate we'll use the filters but they shouldn't be optimized away
        if self.refresh_mode == RefreshMode::Caching {
            return Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()]);
        }

        match self.zero_results_action {
            ZeroResultsAction::ReturnEmpty => {
                let mut results = self.accelerator.supports_filters_pushdown(filters)?;
                let function_support = deny_spice_specific_functions();
                for (i, filter) in filters.iter().enumerate() {
                    if !matches!(results[i], TableProviderFilterPushDown::Unsupported)
                        && !function_support.supports(filter)
                    {
                        results[i] = TableProviderFilterPushDown::Unsupported;
                    }
                }
                Ok(results)
            }
            ZeroResultsAction::UseSource => {
                // In UseSource mode, all filters must still flow into scan() so that
                // FallbackOnZeroResultsScanExec receives the full predicate set and can use
                // its internal filter_plan to evaluate those predicates before making a
                // correct fallback decision. Unsupported-function filters are therefore kept
                // out of accelerator SQL pushdown, but still participate in the fallback check.
                Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
            }
        }
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let is_caching_mode = self.refresh_mode == RefreshMode::Caching;

        if matches!(self.cluster_role, Some(ClusterRole::Scheduler)) {
            // Accelerated tables aren't accelerated on scheduler. Just scan the federated source.
            let federated_provider = self.federated.table_provider().await;
            return federated_provider
                .scan(state, projection, filters, limit)
                .await;
        }

        // If the initial load hasn't completed yet, we need to handle the loading behavior.
        if !self.refresher().initial_load_completed() && !is_caching_mode {
            match self.ready_state {
                ReadyState::OnLoad => {
                    return Err(DataFusionError::External(
                        SpiceExternalError::acceleration_not_ready(self.dataset_name.to_string()),
                    ));
                }
                ReadyState::OnRegistration | ReadyState::OnSchemaResolved => {
                    // Before the initial accelerated load completes, these ready states fall back
                    // to the federated source. Resolving the federated provider is still
                    // asynchronous here and may await the deferred provider becoming available.
                    let federated_provider = self.federated.table_provider().await;
                    metrics::READY_STATE_FALLBACK.add(
                        1,
                        &[KeyValue::new("dataset_name", self.dataset_name.to_string())],
                    );
                    return federated_provider
                        .scan(state, projection, filters, limit)
                        .await;
                }
            }
        }

        // For caching mode, extend the accelerator scan projection to
        // include the storage-only columns the caching pipeline needs:
        // `fetched_at` (freshness check inside
        // `CachingAccelerationScanExec`) and `__spice_cache_namespace`
        // (per-principal isolation `FilterExec` applied below). The added
        // columns are stripped from the user-facing output by
        // `SchemaCastScanExec` on top of the plan, so they never leak
        // into query results.
        //
        // Done unconditionally for caching mode (not gated on having
        // user filters) because the namespace `FilterExec` is always
        // applied and must always be able to resolve the namespace
        // column. Without this, a caching-mode scan with a non-empty
        // user projection but no user filters (e.g. `SELECT content FROM
        // ds`) would push only the user's columns to the accelerator
        // and the FilterExec on top would fail with `No field named
        // __spice_cache_namespace`.
        let extended_projection = if is_caching_mode {
            extend_projection_for_caching(projection, &self.accelerator.schema())
        } else {
            None
        };
        let scan_projection = extended_projection.as_ref().or(projection);
        // For UseSource mode, the scan is handled inside the match arm below (with filter
        // splitting). For all other modes, perform the accelerator scan upfront.
        // For caching mode, scope the accelerator scan to the current
        // request's namespace by appending a `__spice_cache_namespace = $ns_id`
        // predicate. The federated source still receives only the user's
        // original filters via `CachingAccelerationScanExec`, since the
        // namespace column does not exist on the source side. Skipped when
        // the storage schema does not have the column (e.g. unit-test mocks).
        //
        // The originating request context is attached to the session as an
        // extension by `Query::run_internal`. We must read it from there
        // and NOT from `RequestContext::current()`, because DataFusion does
        // not propagate Tokio task-locals across the `TableProvider::scan`
        // await point. The task-local lookup would silently fall back to
        // the global `INTERNAL_REQUEST_CONTEXT` (Protocol::Internal, no
        // principal), collapsing every caller to `CacheNamespace::System`
        // and defeating isolation.
        let namespace_filter: Option<Expr> = if is_caching_mode
            && self
                .accelerator
                .schema()
                .column_with_name(caching::CACHE_NAMESPACE_COLUMN)
                .is_some()
        {
            let ns = state
                .config()
                .get_extension::<runtime_request_context::RequestContext>()
                .map_or(runtime_request_context::CacheNamespace::System, |ctx| {
                    ctx.cache_namespace()
                });
            Some(caching::namespace_filter_expr(ns.storage_id()))
        } else {
            None
        };
        let storage_filters: Vec<Expr> = if let Some(ref nf) = namespace_filter {
            let mut sf = filters.to_vec();
            sf.push(nf.clone());
            sf
        } else {
            filters.to_vec()
        };
        let scan_filters: &[Expr] = if is_caching_mode {
            &storage_filters
        } else {
            filters
        };
        let input = if matches!(
            (is_caching_mode, &self.zero_results_action),
            (false, ZeroResultsAction::UseSource)
        ) {
            None
        } else {
            Some(
                self.accelerator
                    .scan(state, scan_projection, scan_filters, limit)
                    .await?,
            )
        };
        let federated = Arc::clone(&self.federated);
        let fallback_fn: FallbackAsyncTableProvider = Arc::new(move || {
            let federated = Arc::clone(&federated);
            Box::pin(async move { federated.table_provider().await })
        });

        let plan: Arc<dyn ExecutionPlan> = match (is_caching_mode, &self.zero_results_action) {
            (true, _) => {
                // Caching mode: wrap with cache execution plan to handle staleness and background refresh
                let input = input.ok_or_else(|| {
                    DataFusionError::Internal(
                        "accelerator scan input missing in caching mode".to_string(),
                    )
                })?;

                // Check which user filters the accelerator doesn't fully
                // support and need to be re-applied. This ensures correct
                // results when the accelerator returns Inexact or
                // Unsupported for some filters.
                let mut filters_to_reapply = self.get_filters_to_reapply(filters)?;
                // Re-apply the cache-namespace predicate as a hard
                // FilterExec only if the accelerator does NOT report exact
                // pushdown for it.
                //
                // The DataFusion contract for `supports_filters_pushdown`
                // is: `Exact` means the provider guarantees the predicate
                // will be applied (the caller does not have to re-apply);
                // `Inexact` / `Unsupported` mean the caller MUST re-apply
                // or rows that should be filtered may slip through. Cache
                // isolation is a correctness invariant, so we re-apply
                // whenever the accelerator does not give an exact
                // guarantee.
                //
                // We deliberately do NOT wrap on `Exact`: the wrap is not
                // just redundant, it is harmful. `FilterExec` coalesces
                // its output through `BatchCoalescer`, which strictly
                // compares `Field` metadata across consecutive batches.
                // Some accelerator <-> source schema combinations (notably
                // `Map` field naming round-tripped through DuckDB, which
                // canonicalizes `keys`/`values` to `key`/`value`) trigger
                // a false-positive panic in `BatchCoalescer` even though
                // the data itself is well-formed. This bites the localpod
                // chained-accelerator path in particular.
                if let Some(nf) = namespace_filter {
                    let nf_pushdown = self
                        .accelerator
                        .supports_filters_pushdown(&[&nf])?
                        .into_iter()
                        .next()
                        .unwrap_or(TableProviderFilterPushDown::Unsupported);
                    if !matches!(nf_pushdown, TableProviderFilterPushDown::Exact) {
                        filters_to_reapply.push(nf);
                    }
                }
                let input = if filters_to_reapply.is_empty() {
                    input
                } else {
                    wrap_with_filter(input, state, &filters_to_reapply)?
                };

                let federated_provider = self.federated.table_provider().await;
                // SAFETY: batch_write_tx is always Some in caching mode (set in start())
                let batch_write_tx = self.batch_write_tx.clone().ok_or_else(|| {
                    DataFusionError::Internal("batch_write_tx missing in caching mode".to_string())
                })?;
                Arc::new(caching::CachingAccelerationScanExec::new(
                    input,
                    self.cache_ttl,
                    self.cache_stale_while_revalidate_ttl,
                    self.cache_stale_if_error,
                    federated_provider,
                    Arc::clone(&self.accelerator),
                    self.dataset_name.to_string(),
                    self.io_runtime.clone(),
                    filters.to_vec(),
                    projection.cloned(),
                    limit,
                    Arc::clone(&self.accelerator_write_mutex),
                    Arc::clone(&self.in_flight_revalidations),
                    Arc::clone(&self.synchronized_children),
                    batch_write_tx,
                ))
            }
            (false, ZeroResultsAction::ReturnEmpty) => input.ok_or_else(|| {
                DataFusionError::Internal(
                    "accelerator scan input missing in ReturnEmpty mode".to_string(),
                )
            })?,
            (false, ZeroResultsAction::UseSource) => {
                let filter_refs: Vec<&Expr> = filters.iter().collect();
                let pushdown_support = self.accelerator.supports_filters_pushdown(&filter_refs)?;
                let accelerator_filters = filters_for_accelerator_scan(filters, &pushdown_support)?;

                let accelerator_limit = if accelerator_filters.len() == filters.len() {
                    limit
                } else {
                    None
                };
                let input = self
                    .accelerator
                    .scan(
                        state,
                        scan_projection,
                        &accelerator_filters,
                        accelerator_limit,
                    )
                    .await?;
                Arc::new(FallbackOnZeroResultsScanExec::new(
                    self.dataset_name.clone(),
                    input,
                    fallback_fn,
                    TableScanParams::new(state, projection, filters, limit),
                ))
            }
        };

        // Compute the target schema based on user's original projection.
        // SchemaCastScanExec strips extra columns (like fetched_at added for caching)
        // and casts types. The schema should match what the user requested.
        let target_schema = match projection {
            Some(indices) => {
                let full_schema = self.schema();
                let projected_fields: Vec<_> = indices
                    .iter()
                    .filter_map(|&i| full_schema.fields().get(i).cloned())
                    .collect();
                Arc::new(Schema::new_with_metadata(
                    projected_fields,
                    full_schema.metadata().clone(),
                ))
            }
            None => self.schema(),
        };

        Ok(Arc::new(SchemaCastScanExec::new(plan, target_schema)))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // In `refresh_mode: snapshot`, the accelerator is a read-only mirror
        // of the snapshot store. Accepting writes here would either be
        // silently overwritten by the next snapshot reload (data loss) or
        // race with the file replacement performed during refresh. Reject
        // explicitly so callers fail loudly rather than observing surprising
        // behavior.
        if self.refresh_mode == RefreshMode::Snapshot {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "writes to accelerated table {} are not permitted when refresh_mode is 'snapshot'; the accelerator is driven exclusively from the snapshot store",
                self.dataset_name
            )));
        }

        self.update_last_updated_at();

        match &self.write_mode {
            WriteMode::AcceleratorOnly => {
                // When on_conflict is configured, writes go only to the accelerator
                // (the federated source may not support writes, e.g., file connector).
                let accelerated_insert_plan = self
                    .accelerator
                    .insert_into(state, input, overwrite)
                    .await?;
                self.refresher().set_initial_load_completed(true);
                Ok(accelerated_insert_plan)
            }
            WriteMode::WriteThrough => {
                // Writes go to the federated source synchronously. The acceleration
                // refresh mechanism (CDC for refresh_mode: changes, otherwise the
                // periodic refresh cycle) propagates the change to the accelerator.
                let federated_table = self.federated.table_provider().await;
                federated_table.insert_into(state, input, overwrite).await
            }
            WriteMode::WriteBack => {
                write::write_back::validate_insert_op(overwrite)?;
                write::write_back::insert_write_back(
                    state,
                    input,
                    overwrite,
                    Arc::clone(&self.accelerator),
                    Arc::clone(&self.federated),
                    Arc::clone(&self.refresher),
                    self.schema(),
                )
            }
            WriteMode::DualWrite {
                cayenne_target,
                federated_provider,
            } => write::dual_write::insert_dual_write(
                input,
                overwrite,
                cayenne_target.as_ref(),
                Arc::clone(federated_provider),
                &self.refresher,
                self.schema(),
            ),
        }
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if self.refresh_mode == RefreshMode::Snapshot {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "deletes on accelerated table {} are not permitted when refresh_mode is 'snapshot'; the accelerator is driven exclusively from the snapshot store",
                self.dataset_name
            )));
        }

        self.update_last_updated_at();

        match &self.write_mode {
            WriteMode::AcceleratorOnly => self.accelerator.delete_from(state, filters).await,
            WriteMode::WriteThrough => {
                let federated_table = self.federated.table_provider().await;
                federated_table.delete_from(state, filters).await
            }
            WriteMode::WriteBack => {
                write::write_back::delete_write_back(
                    state,
                    filters,
                    Arc::clone(&self.accelerator),
                    Arc::clone(&self.federated),
                )
                .await
            }
            WriteMode::DualWrite {
                cayenne_target,
                federated_provider,
            } => {
                write::dual_write::delete_dual_write(
                    state,
                    filters,
                    cayenne_target.as_ref(),
                    Arc::clone(federated_provider),
                )
                .await
            }
        }
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if self.refresh_mode == RefreshMode::Snapshot {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "updates on accelerated table {} are not permitted when refresh_mode is 'snapshot'; the accelerator is driven exclusively from the snapshot store",
                self.dataset_name
            )));
        }

        self.update_last_updated_at();

        match &self.write_mode {
            WriteMode::AcceleratorOnly => {
                self.accelerator.update(state, assignments, filters).await
            }
            WriteMode::WriteThrough => {
                let federated_table = self.federated.table_provider().await;
                federated_table.update(state, assignments, filters).await
            }
            WriteMode::WriteBack => {
                write::write_back::update_write_back(
                    state,
                    assignments,
                    filters,
                    Arc::clone(&self.accelerator),
                    Arc::clone(&self.federated),
                )
                .await
            }
            WriteMode::DualWrite {
                cayenne_target,
                federated_provider,
            } => {
                write::dual_write::update_dual_write(
                    state,
                    assignments,
                    filters,
                    cayenne_target.as_ref(),
                    Arc::clone(federated_provider),
                )
                .await
            }
        }
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.accelerator.get_table_definition()
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        self.accelerator.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.accelerator.get_column_default(column)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DataFusionResult<datafusion::catalog::ScanResult> {
        let plan = self
            .scan(
                state,
                args.projection().map(<[usize]>::to_vec).as_ref(),
                args.filters().unwrap_or(&[]),
                args.limit(),
            )
            .await?;
        Ok(plan.into())
    }

    async fn truncate(&self, state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if self.refresh_mode == RefreshMode::Snapshot {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "truncate on accelerated table {} is not permitted when refresh_mode is 'snapshot'; the accelerator is driven exclusively from the snapshot store",
                self.dataset_name
            )));
        }

        self.update_last_updated_at();

        match &self.write_mode {
            WriteMode::AcceleratorOnly => self.accelerator.truncate(state).await,
            WriteMode::WriteThrough => {
                let federated_table = self.federated.table_provider().await;
                federated_table.truncate(state).await
            }
            WriteMode::WriteBack | WriteMode::DualWrite { .. } => {
                Err(datafusion::error::DataFusionError::Plan(
                    "TRUNCATE is not supported for write_back or dual_write accelerated tables"
                        .to_string(),
                ))
            }
        }
    }
}

/// Extends projection to include columns required by the caching pipeline
/// for accelerator scans: `fetched_at` (freshness check) and
/// `__spice_cache_namespace` (per-principal isolation filter applied as a
/// hard `FilterExec` on top of the scan).
///
/// Returns `Some(extended_projection)` if any extension was needed, or
/// `None` if both columns are already present (or `projection` is `None`,
/// meaning the caller already wants the full schema).
fn extend_projection_for_caching(
    projection: Option<&Vec<usize>>,
    schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let proj = projection?;
    let mut extended: Option<Vec<usize>> = None;
    for col in [
        caching::CACHE_REFRESHED_AT_COLUMN,
        caching::CACHE_NAMESPACE_COLUMN,
    ] {
        let Ok(idx) = schema.index_of(col) else {
            continue;
        };
        if proj.contains(&idx) {
            continue;
        }
        let target = extended.get_or_insert_with(|| proj.clone());
        if !target.contains(&idx) {
            target.push(idx);
        }
    }
    extended
}

fn filters_for_accelerator_scan(
    filters: &[Expr],
    pushdown_support: &[TableProviderFilterPushDown],
) -> DataFusionResult<Vec<Expr>> {
    if filters.len() != pushdown_support.len() {
        return Err(DataFusionError::Internal(format!(
            "accelerator filter support length mismatch: expected {}, got {}",
            filters.len(),
            pushdown_support.len()
        )));
    }

    let function_support = deny_spice_specific_functions();
    let mut accelerator_filters = Vec::with_capacity(filters.len());

    for (filter, support) in filters.iter().zip(pushdown_support.iter()) {
        let function_supported = function_support.supports(filter);
        let can_run_in_accelerator =
            function_supported && !matches!(support, TableProviderFilterPushDown::Unsupported);
        if can_run_in_accelerator {
            accelerator_filters.push(filter.clone());
        }
    }

    Ok(accelerator_filters)
}

#[derive(Debug)]
pub enum DataRetentionFilter {
    Time {
        period: Duration,
        time_column: String,
        time_format: Option<TimeFormat>,
        time_partition_column: Option<String>,
        time_partition_format: Option<TimeFormat>,
    },
    Expression {
        delete_expr: Box<Expr>,
    },
}

pub struct RetentionBuilder {
    time_column: Option<String>,
    time_format: Option<TimeFormat>,
    time_period: Option<Duration>,
    time_partition_column: Option<String>,
    time_partition_format: Option<TimeFormat>,
    delete_expr: Option<Expr>,
    check_interval: Option<Duration>,
    enabled: bool,
}

impl RetentionBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            time_column: None,
            time_format: None,
            time_partition_column: None,
            time_partition_format: None,
            delete_expr: None,
            time_period: None,
            check_interval: None,
            enabled: true,
        }
    }

    #[must_use]
    pub fn time_column<S: Into<String>>(mut self, time_column: Option<S>) -> Self {
        self.time_column = time_column.map(Into::into);
        self
    }

    #[must_use]
    pub fn time_format(mut self, time_format: Option<TimeFormat>) -> Self {
        self.time_format = time_format;
        self
    }

    #[must_use]
    pub fn time_partition_column<S: Into<String>>(
        mut self,
        time_partition_column: Option<S>,
    ) -> Self {
        self.time_partition_column = time_partition_column.map(Into::into);
        self
    }

    #[must_use]
    pub fn time_partition_format(mut self, time_partition_format: Option<TimeFormat>) -> Self {
        self.time_partition_format = time_partition_format;
        self
    }

    #[must_use]
    pub fn delete_expr(mut self, delete_expr: Option<Expr>) -> Self {
        self.delete_expr = delete_expr;
        self
    }

    #[must_use]
    pub fn time_period(mut self, time_period: Option<Duration>) -> Self {
        self.time_period = time_period;
        self
    }

    #[must_use]
    pub fn check_interval(mut self, check_interval: Option<Duration>) -> Self {
        self.check_interval = check_interval;
        self
    }

    #[must_use]
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    #[must_use]
    pub fn build(self) -> Option<Retention> {
        if !self.enabled {
            return None;
        }

        let check_interval = self.check_interval?;
        let mut filters = Vec::new();

        // Add time-based filter if period and time_column are provided
        if let Some(period) = self.time_period {
            let Some(time_column) = self.time_column else {
                tracing::error!(
                    "[retention] The `time_column` must be specified for time-based retention"
                );
                return None;
            };

            filters.push(DataRetentionFilter::Time {
                period,
                time_column,
                time_format: self.time_format,
                time_partition_column: self.time_partition_column.clone(),
                time_partition_format: self.time_partition_format,
            });
        }

        // Add expression-based filter
        if let Some(delete_expr) = self.delete_expr {
            filters.push(DataRetentionFilter::Expression {
                delete_expr: Box::new(delete_expr),
            });
        }

        if filters.is_empty() {
            tracing::error!(
                "[retention] The `retention_period` or `retention_sql` must be specified for retention"
            );
            return None;
        }

        Some(Retention {
            filters,
            check_interval,
        })
    }
}

impl Default for RetentionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Retention {
    pub(crate) filters: Vec<DataRetentionFilter>,
    pub(crate) check_interval: Duration,
}

impl Retention {
    #[must_use]
    pub fn builder() -> RetentionBuilder {
        RetentionBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::prelude::{col, lit};
    use datafusion_functions_json::udfs::json_get_str_udf;

    fn schema_with_fetched_at() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, true),
            Field::new(
                caching::CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]))
    }

    fn expr_strings(filters: &[Expr]) -> Vec<String> {
        filters.iter().map(ToString::to_string).collect()
    }

    fn json_get_str_filter() -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            json_get_str_udf(),
            vec![col("content"), lit("key")],
        ))
        .eq(lit("needle"))
    }

    #[test]
    fn test_extend_projection_none_returns_none() {
        let schema = schema_with_fetched_at();
        let result = extend_projection_for_caching(None, &schema);
        assert!(result.is_none(), "None projection should return None");
    }

    #[test]
    fn test_extend_projection_already_includes_fetched_at() {
        let schema = schema_with_fetched_at();
        // Projection includes fetched_at (index 3)
        let projection = vec![0, 1, 3];
        let result = extend_projection_for_caching(Some(&projection), &schema);
        assert!(
            result.is_none(),
            "Projection already including fetched_at should return None"
        );
    }

    #[test]
    fn test_extend_projection_adds_fetched_at() {
        let schema = schema_with_fetched_at();
        // Projection does NOT include fetched_at
        let projection = vec![0, 2]; // id, content
        let extended = extend_projection_for_caching(Some(&projection), &schema)
            .expect("Should extend projection");
        assert_eq!(
            extended,
            vec![0, 2, 3],
            "Should add fetched_at index at end"
        );
    }

    #[test]
    fn test_extend_projection_single_column() {
        let schema = schema_with_fetched_at();
        let projection = vec![2]; // just content
        let extended = extend_projection_for_caching(Some(&projection), &schema)
            .expect("Should extend projection");
        assert_eq!(
            extended,
            vec![2, 3],
            "Should add fetched_at to single column"
        );
    }

    #[test]
    fn test_filters_for_accelerator_scan_excludes_local_only_filters() {
        let exact_filter = col("id").eq(lit(42_i64));
        let inexact_filter = col("name").eq(lit("espresso"));
        let unsupported_filter = col("content").eq(lit("local only"));
        let denied_filter = json_get_str_filter();
        let filters = vec![
            exact_filter.clone(),
            inexact_filter.clone(),
            unsupported_filter,
            denied_filter,
        ];
        let pushdown_support = vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Unsupported,
            TableProviderFilterPushDown::Exact,
        ];

        let accelerator_filters = filters_for_accelerator_scan(&filters, &pushdown_support)
            .expect("filter split should succeed");
        let expected_accelerator_filters = expr_strings(&[exact_filter, inexact_filter]);

        assert_eq!(
            expr_strings(&accelerator_filters),
            expected_accelerator_filters
        );
    }

    #[test]
    fn test_filters_for_accelerator_scan_validates_support_length() {
        let err = filters_for_accelerator_scan(&[col("id").eq(lit(42_i64))], &[])
            .expect_err("mismatched filter support should fail");

        assert!(
            matches!(err, DataFusionError::Internal(message) if message.contains("accelerator filter support length mismatch"))
        );
    }
}
