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

use std::path::PathBuf;
use std::{any::Any, sync::Arc, time::Duration};

use crate::component::dataset::acceleration::{RefreshMode, RefreshOnStartup, ZeroResultsAction};
use crate::component::dataset::{ReadyState, TimeFormat};
use crate::dataaccelerator::get_primary_keys_from_constraints;
use crate::datafusion::error::SpiceExternalError;
use crate::datafusion::is_spice_internal_dataset;
use crate::federated_table::FederatedTable;
use crate::status;
use ::cache::Caching;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use datafusion::sql::TableReference;
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
};
use opentelemetry::KeyValue;
use refresh::RefreshOverrides;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::SnapshotBehavior;
use runtime_datafusion::execution_plan::fallback_on_zero_results::FallbackAsyncTableProvider;
use runtime_datafusion::execution_plan::{
    TableScanParams, fallback_on_zero_results::FallbackOnZeroResultsScanExec,
    schema_cast::SchemaCastScanExec, slice::SliceExec, tee::TeeExec, wrap_with_filter,
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
mod sink;
mod synchronized_table;
mod timestamp_metrics_utils;

pub use refresh_task_runner::RefreshTaskRunner;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to get data from the connector. {source} Ensure the dataset configuration is valid, and try again."
    ))]
    UnableToGetDataFromConnector { source: DataFusionError },

    #[snafu(display(
        "Failed to get data from the connector. {source} Ensure the dataset configuration is valid, and try again."
    ))]
    FailedToRefreshDataset { source: DataFusionError },

    #[snafu(display(
        "Failed to get data from the connector. {source} Ensure the dataset configuration is valid, and try again."
    ))]
    UnableToScanTableProvider { source: DataFusionError },

    #[snafu(display(
        "Failed to get data from the connector. {source} Ensure the dataset configuration is valid, and try again."
    ))]
    UnableToCreateMemTableFromUpdate { source: DataFusionError },

    #[snafu(display(
        "Failed to refresh dataset {dataset_name}: refresh worker panicked. {message}"
    ))]
    RefreshWorkerPanicked {
        dataset_name: String,
        message: String,
    },

    #[snafu(display("Failed to refresh the dataset. {source}"))]
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
        "Failed to find latest timestamp in accelerated table: {source}. Is the 'time_column' parameter correct?"
    ))]
    FailedToQueryLatestTimestamp { source: DataFusionError },

    #[snafu(display("{reason}"))]
    FailedToFindLatestTimestamp { reason: String },

    #[snafu(display("Failed to filter update data. {source}"))]
    FailedToFilterUpdates { source: ArrowError },

    #[snafu(display("Failed to write data into accelerated table. {source}"))]
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

    #[snafu(display("{source}"))]
    InvalidTimeColumnTimeFormat { source: refresh::Error },

    #[snafu(display("Failed to start refresh task. The task was already started."))]
    RefreshTaskAlreadyStarted {},

    #[snafu(display("Failed to create RecordBatch: {source}"))]
    FailedToBuildRecordBatch { source: ArrowError },
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
        "A synchronized accelerated table requires full refresh mode. Set `refresh_mode` to 'full', and try again."
    ))]
    SynchronizedAcceleratedTableRequiresFullRefresh,

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
    synchronized_with: Option<SynchronizedTable>,
    cache_ttl: Option<Duration>,
    cache_stale_while_revalidate_ttl: Option<Duration>,
    cache_stale_if_error: bool,
    io_runtime: Handle,
    /// Mutex to protect concurrent cache operations (insert, upsert) to the accelerator
    cache_mutex: Arc<Mutex<()>>,
    /// Tracks in-flight revalidation requests to avoid duplicate upstream requests during SWR window
    in_flight_revalidations: caching::InFlightRevalidations,
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
    refresh_semaphore: Option<Arc<Semaphore>>,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    synchronize_with: Option<SynchronizedTable>,
    initial_load_complete: bool,
    snapshot_behavior: SnapshotBehavior,
    snapshot_local_path: Option<PathBuf>,
    snapshots_trigger_threshold: Option<i64>,
    metrics: Option<Metrics>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    caching_ttl: Option<Duration>,
    caching_stale_while_revalidate_ttl: Option<Duration>,
    caching_stale_if_error: bool,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
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
            initial_load_complete: false,
            refresh_semaphore: None,
            snapshot_behavior: SnapshotBehavior::default(),
            snapshot_local_path: None,
            snapshots_trigger_threshold: None,
            metrics: None,
            cpu_runtime: None,
            io_runtime,
            caching_ttl: None,
            caching_stale_while_revalidate_ttl: None,
            caching_stale_if_error: false,
            resource_monitor: None,
        }
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

    /// Set the existing full refresh mode accelerated table to synchronize with after the initial load completes
    ///
    /// A full table scan of the existing accelerated table is required to initialize a synchronized accelerated table.
    ///
    /// Handling append/changes mode should be possible, but requires more care to ensure
    /// that delta updates are applied correctly after the initial table scan.
    pub async fn synchronize_with(
        &mut self,
        existing_accelerated_table: &AcceleratedTable,
    ) -> AcceleratedTableBuilderResult<&mut Self> {
        ensure!(
            matches!(self.refresh.mode, RefreshMode::Full),
            SynchronizedAcceleratedTableRequiresFullRefreshSnafu
        );
        ensure!(
            matches!(
                existing_accelerated_table.refresh_params.read().await.mode,
                RefreshMode::Full
            ),
            SynchronizedAcceleratedTableRequiresFullRefreshSnafu
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
    pub fn snapshot_behavior(
        &mut self,
        snapshot_behavior: SnapshotBehavior,
        snapshot_path: Option<PathBuf>,
        snapshots_trigger_threshold: Option<i64>,
    ) -> &mut Self {
        self.snapshot_behavior = snapshot_behavior;
        self.snapshot_local_path = snapshot_path;
        self.snapshots_trigger_threshold = snapshots_trigger_threshold;
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

    /// Build the accelerated table
    #[expect(clippy::too_many_lines)]
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
                let has_primary_key = self.accelerator.constraints().is_some_and(|constraints| {
                    !get_primary_keys_from_constraints(constraints, &schema).is_empty()
                });
                let has_time_column = self.refresh.time_column.is_some();
                let has_append_stream = self.append_stream.is_some();

                let append_mode =
                    AppendMode::try_new(has_time_column, has_primary_key, has_append_stream)?;

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
        };

        validate_refresh_data_window(&self.refresh, &self.dataset_name, &self.federated.schema());
        let refresh_mode = self.refresh.mode;
        let refresh_params = Arc::new(RwLock::new(self.refresh));
        // Create the cache mutex early so it can be shared between the Refresher and the AcceleratedTable.
        let cache_mutex: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
        // Create the in-flight revalidations tracker to avoid duplicate upstream requests during SWR window.
        let in_flight_revalidations: caching::InFlightRevalidations =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        let mut refresher = refresh::Refresher::new(
            Arc::clone(&self.runtime_status),
            self.dataset_name.clone(),
            Arc::clone(&self.federated),
            Some(self.federated_source),
            Arc::clone(&refresh_params),
            Arc::clone(&self.accelerator),
            self.cpu_runtime.clone(),
            self.io_runtime.clone(),
            Arc::clone(&cache_mutex),
        );
        refresher.caching(&self.caching);
        refresher.checkpointer(self.checkpointer);
        refresher.refresh_on_startup(self.refresh_on_startup);
        refresher.set_initial_load_completed(self.initial_load_complete);
        refresher.disable_federation(self.disable_federation);
        refresher.with_completion_notifier(Arc::clone(&on_complete_notification));
        refresher.with_metrics(self.metrics);
        if let Some(synchronize_with) = &self.synchronize_with {
            refresher.synchronize_with(synchronize_with.clone());
        }
        if let Some(semaphore) = self.refresh_semaphore {
            refresher.semaphore(semaphore);
        }
        refresher.with_snapshot_behavior(
            self.snapshot_behavior,
            self.snapshot_local_path.clone(),
            self.snapshots_trigger_threshold,
        );

        if let Some(ref resource_monitor) = self.resource_monitor {
            refresher.with_resource_monitor(resource_monitor.clone());
        }

        let refresh_handle = refresher.start(acceleration_refresh_mode).await?;
        let refresher = Arc::new(refresher);

        let mut handlers = vec![];
        if let Some(refresh_handle) = refresh_handle {
            handlers.push(refresh_handle);
        }

        if let Some(retention) = self.retention {
            let retention_check_handle = tokio::spawn(AcceleratedTable::start_retention_check(
                self.dataset_name.clone(),
                Arc::clone(&self.accelerator),
                retention,
                self.caching.clone(),
                self.io_runtime.clone(),
            ));
            handlers.push(retention_check_handle);
        }

        // If the table should be ready immediately, mark it as ready.
        if self.ready_state == ReadyState::OnRegistration {
            self.runtime_status
                .update_dataset(&self.dataset_name, status::ComponentStatus::Ready);
        }

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
            synchronized_with: self.synchronize_with,
            cache_ttl: self.caching_ttl,
            cache_stale_while_revalidate_ttl: self.caching_stale_while_revalidate_ttl,
            cache_stale_if_error: self.caching_stale_if_error,
            io_runtime: self.io_runtime,
            cache_mutex,
            in_flight_revalidations,
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
    pub fn get_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.accelerator)
    }

    pub async fn update_refresh_sql(&self, refresh_sql: Option<String>) -> Result<()> {
        let dataset_name = &self.dataset_name;

        let mut refresh = self.refresh_params.write().await;
        refresh.sql.clone_from(&refresh_sql);

        if !is_spice_internal_dataset(&self.dataset_name) {
            if let Some(sql_str) = &refresh_sql {
                tracing::info!("[refresh] Updated refresh SQL for {dataset_name} to {sql_str}");
            } else {
                tracing::info!("[refresh] Removed refresh SQL for {dataset_name}");
            }
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
        self.accelerator.schema()
    }

    fn table_type(&self) -> TableType {
        self.accelerator.table_type()
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
            ZeroResultsAction::ReturnEmpty => self.accelerator.supports_filters_pushdown(filters),
            ZeroResultsAction::UseSource => {
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
        // Check if we're in caching mode
        let is_caching_mode = self.refresh_params.read().await.mode == RefreshMode::Caching;

        // If the initial load hasn't completed yet, we need to handle the loading behavior.
        if !self.refresher().initial_load_completed() && !is_caching_mode {
            match self.ready_state {
                ReadyState::OnLoad => {
                    return Err(DataFusionError::External(
                        SpiceExternalError::acceleration_not_ready(self.dataset_name.to_string()),
                    ));
                }
                ReadyState::OnRegistration => {
                    // Getting the federated_provider should always return immediately here, because by definition an accelerated table has
                    // completed its initial load if it has a previous checkpoint.
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

        // In caching mode, pass filters to accelerator so it can check for cached data.
        // If accelerator returns 0 rows → cache miss → fetch from source.
        let input = self
            .accelerator
            .scan(state, projection, filters, limit)
            .await?;
        let federated = Arc::clone(&self.federated);
        let fallback_fn: FallbackAsyncTableProvider = Arc::new(move || {
            let federated = Arc::clone(&federated);
            Box::pin(async move { federated.table_provider().await })
        });

        let plan: Arc<dyn ExecutionPlan> = match (is_caching_mode, &self.zero_results_action) {
            (true, _) => {
                // Caching mode: wrap with cache execution plan to handle staleness and background refresh

                // Check which filters the accelerator doesn't fully support and need to be re-applied.
                // This ensures correct results when the accelerator returns Inexact or Unsupported for some filters.
                let filters_to_reapply = self.get_filters_to_reapply(filters)?;
                let input = if filters_to_reapply.is_empty() {
                    input
                } else {
                    wrap_with_filter(input, state, &filters_to_reapply)?
                };

                let federated_provider = self.federated.table_provider().await;
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
                    Arc::clone(&self.cache_mutex),
                    Arc::clone(&self.in_flight_revalidations),
                ))
            }
            (false, ZeroResultsAction::ReturnEmpty) => input,
            (false, ZeroResultsAction::UseSource) => Arc::new(FallbackOnZeroResultsScanExec::new(
                self.dataset_name.clone(),
                input,
                fallback_fn,
                TableScanParams::new(state, projection, filters, limit),
            )),
        };

        Ok(Arc::new(SchemaCastScanExec::new(plan, self.schema())))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Duplicate the input into two streams
        let tee_input: Arc<dyn ExecutionPlan> = Arc::new(TeeExec::new(input, 2));

        // Slice the duplicated stream by partition to get separate streams for the accelerated & federated inserts.
        let accelerated_input = Arc::new(SliceExec::new(Arc::clone(&tee_input), 0));
        let accelerated_insert_plan = self
            .accelerator
            .insert_into(state, accelerated_input, overwrite)
            .await?;

        let federated_input = Arc::new(SliceExec::new(tee_input, 1));
        let federated_table = self.federated.table_provider().await;
        let federated_insert_plan = federated_table
            .insert_into(state, federated_input, overwrite)
            .await?;

        // Return the equivalent of a UNION ALL that inserts both into the acceleration and federated source tables.
        let union_plan = Arc::new(UnionExec::new(vec![
            accelerated_insert_plan,
            federated_insert_plan,
        ]));

        self.refresher().set_initial_load_completed(true);

        Ok(union_plan)
    }
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
