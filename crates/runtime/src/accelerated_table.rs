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

use std::time::SystemTime;
use std::{any::Any, sync::Arc, time::Duration};

use crate::component::dataset::acceleration::{RefreshMode, ZeroResultsAction};
use crate::component::dataset::TimeFormat;
use crate::dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint;
use crate::datafusion::SPICE_RUNTIME_SCHEMA;
use crate::status;
use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use async_trait::async_trait;
use cache::QueryResultsCacheProvider;
use data_components::cdc::ChangesStream;
use data_components::delete::get_deletion_provider;
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::TableReference;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::Expr,
};
use refresh::RefreshOverrides;
use snafu::prelude::*;
use synchronized_table::SynchronizedTable;
use tokio::task::JoinHandle;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::datafusion::filter_converter::TimestampFilterConvert;
use crate::execution_plan::fallback_on_zero_results::FallbackOnZeroResultsScanExec;
use crate::execution_plan::schema_cast::SchemaCastScanExec;
use crate::execution_plan::slice::SliceExec;
use crate::execution_plan::tee::TeeExec;
use crate::execution_plan::TableScanParams;

pub mod federation;
mod metrics;
pub mod refresh;
pub mod refresh_task;
mod refresh_task_runner;
mod sink;
mod synchronized_table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get data from connector: {source}"))]
    UnableToGetDataFromConnector { source: DataFusionError },

    #[snafu(display("Dataset refresh failed with error: {source}"))]
    FailedToRefreshDataset { source: DataFusionError },

    #[snafu(display("Unable to scan table provider: {source}"))]
    UnableToScanTableProvider {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to create MemTable from data update: {source}"))]
    UnableToCreateMemTableFromUpdate {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to trigger table refresh: {source}"))]
    FailedToTriggerRefresh {
        source: tokio::sync::mpsc::error::SendError<Option<RefreshOverrides>>,
    },

    #[snafu(display("Manual refresh is not supported for `append` mode"))]
    ManualRefreshIsNotSupported {},

    #[snafu(display("Failed to find latest timestamp in accelerated table"))]
    FailedToQueryLatestTimestamp {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("{reason}"))]
    FailedToFindLatestTimestamp { reason: String },

    #[snafu(display("Failed to filter update data: {source}"))]
    FailedToFilterUpdates { source: ArrowError },

    #[snafu(display("Failed to write data into accelerated table: {source}"))]
    FailedToWriteData {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("The accelerated table does not support delete operations"))]
    AcceleratedTableDoesntSupportDelete {},

    #[snafu(display("Expected schema to have field '{field_name}' schema={schema}"))]
    ExpectedSchemaToHaveField {
        field_name: String,
        schema: SchemaRef,
    },

    #[snafu(display("Expected field in schema '{field_name}' to have type '{expected_data_type}' schema={schema}"))]
    ArrayDataTypeMismatch {
        field_name: String,
        expected_data_type: String,
        schema: SchemaRef,
    },

    #[snafu(display(
        "The type of the primary key '{data_type}' is not yet supported for change deletion."
    ))]
    PrimaryKeyTypeNotYetSupported { data_type: String },

    #[snafu(display("{source}"))]
    InvalidTimeColumnTimeFormat { source: refresh::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum AcceleratedTableBuilderError {
    #[snafu(display("Expected changes stream for `RefreshMode::Changes`"))]
    ExpectedChangesStream,

    #[snafu(display("Append stream is required for `RefreshMode::Append` without time_column"))]
    AppendStreamRequired,

    #[snafu(display("Synchronized accelerated table requires full refresh mode"))]
    SynchronizedAcceleratedTableRequiresFullRefresh,
}

pub type AcceleratedTableBuilderResult<T> = std::result::Result<T, AcceleratedTableBuilderError>;

// An accelerated table consists of a federated table and a local accelerator.
//
// The accelerator must support inserts.
// AcceleratedTable::new returns an instance of the table and a oneshot receiver that will be triggered when the table is ready, right after the initial data refresh finishes.
pub struct AcceleratedTable {
    dataset_name: TableReference,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<dyn TableProvider>,
    refresh_trigger: Option<mpsc::Sender<Option<RefreshOverrides>>>,

    // Async background tasks relevant to the accelerated table (i.e should be stopped when the table is dropped).
    pub(crate) handlers: Vec<JoinHandle<()>>,
    zero_results_action: ZeroResultsAction,
    refresh_params: Arc<RwLock<refresh::Refresh>>,
    refresher: Arc<refresh::Refresher>,
    disable_query_push_down: bool,
    synchronized_with: Option<SynchronizedTable>,
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
    federated: Arc<dyn TableProvider>,
    accelerator: Arc<dyn TableProvider>,
    refresh: refresh::Refresh,
    retention: Option<Retention>,
    zero_results_action: ZeroResultsAction,
    cache_provider: Option<Arc<QueryResultsCacheProvider>>,
    changes_stream: Option<ChangesStream>,
    append_stream: Option<ChangesStream>,
    disable_query_push_down: bool,
    checkpointer: Option<DatasetCheckpoint>,
    synchronize_with: Option<SynchronizedTable>,
}

impl Builder {
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        refresh: refresh::Refresh,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            accelerator,
            refresh,
            retention: None,
            zero_results_action: ZeroResultsAction::default(),
            cache_provider: None,
            changes_stream: None,
            append_stream: None,
            checkpointer: None,
            synchronize_with: None,
            disable_query_push_down: false,
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

    pub fn cache_provider(
        &mut self,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
    ) -> &mut Self {
        self.cache_provider = cache_provider;
        self
    }

    pub fn disable_query_push_down(&mut self) -> &mut Self {
        self.disable_query_push_down = true;
        self
    }

    /// Set the changes stream for the accelerated table
    ///
    /// # Panics
    ///
    /// Panics if the refresh mode isn't `RefreshMode::Changes`.
    pub fn changes_stream(&mut self, changes_stream: ChangesStream) -> &mut Self {
        assert!(self.refresh.mode == RefreshMode::Changes);
        self.changes_stream = Some(changes_stream);
        self
    }

    /// Set the append stream for the accelerated table
    ///
    /// # Panics
    ///
    /// Panics if the refresh mode isn't `RefreshMode::Append`.
    pub fn append_stream(&mut self, append_stream: ChangesStream) -> &mut Self {
        assert!(self.refresh.mode == RefreshMode::Append);
        self.append_stream = Some(append_stream);
        self
    }

    /// Set the checkpointer for the accelerated table
    pub fn checkpointer(&mut self, checkpointer: DatasetCheckpoint) -> &mut Self {
        self.checkpointer = Some(checkpointer);
        self
    }

    /// Set the checkpointer for the accelerated table
    pub fn checkpointer_opt(&mut self, checkpointer: Option<DatasetCheckpoint>) -> &mut Self {
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

    /// Build the accelerated table
    pub async fn build(
        self,
    ) -> AcceleratedTableBuilderResult<(AcceleratedTable, oneshot::Receiver<()>)> {
        let (ready_sender, is_ready) = oneshot::channel::<()>();

        let (acceleration_refresh_mode, refresh_trigger) = match self.refresh.mode {
            RefreshMode::Disabled => (refresh::AccelerationRefreshMode::Disabled, None),
            RefreshMode::Append => {
                if self.refresh.time_column.is_none() {
                    // Get the append stream
                    let Some(append_stream) = self.append_stream else {
                        return AppendStreamRequiredSnafu.fail();
                    };
                    (
                        refresh::AccelerationRefreshMode::Changes(append_stream),
                        None,
                    )
                } else {
                    let (start_refresh, on_start_refresh) =
                        mpsc::channel::<Option<RefreshOverrides>>(1);
                    (
                        refresh::AccelerationRefreshMode::Append(Some(on_start_refresh)),
                        Some(start_refresh),
                    )
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
        };

        validate_refresh_data_window(&self.refresh, &self.dataset_name, &self.federated.schema());
        let refresh_params = Arc::new(RwLock::new(self.refresh));
        let mut refresher = refresh::Refresher::new(
            Arc::clone(&self.runtime_status),
            self.dataset_name.clone(),
            Arc::clone(&self.federated),
            Arc::clone(&refresh_params),
            Arc::clone(&self.accelerator),
        );
        refresher.cache_provider(self.cache_provider.clone());
        refresher.checkpointer(self.checkpointer);

        if let Some(synchronize_with) = &self.synchronize_with {
            refresher.synchronize_with(synchronize_with.clone());
        }

        let refresh_handle = refresher
            .start(acceleration_refresh_mode, ready_sender)
            .await;
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
                self.cache_provider.clone(),
            ));
            handlers.push(retention_check_handle);
        }
        Ok((
            AcceleratedTable {
                dataset_name: self.dataset_name,
                accelerator: self.accelerator,
                federated: self.federated,
                refresh_trigger,
                handlers,
                zero_results_action: self.zero_results_action,
                refresh_params,
                refresher,
                disable_query_push_down: self.disable_query_push_down,
                synchronized_with: self.synchronize_with,
            },
            is_ready,
        ))
    }
}

impl AcceleratedTable {
    pub fn builder(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        refresh: refresh::Refresh,
    ) -> Builder {
        Builder::new(
            runtime_status,
            dataset_name,
            federated,
            accelerator,
            refresh,
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
            Some(synchronized_table) => synchronized_table.refresh_trigger(),
            None => self.refresh_trigger.as_ref(),
        }
    }

    pub async fn trigger_refresh(&self, overrides: Option<RefreshOverrides>) -> Result<()> {
        match self.refresh_trigger() {
            Some(refresh_trigger) => {
                refresh_trigger
                    .send(overrides)
                    .await
                    .context(FailedToTriggerRefreshSnafu)?;
            }
            None => {
                ManualRefreshIsNotSupportedSnafu.fail()?;
            }
        }

        Ok(())
    }

    #[must_use]
    pub fn get_federated_table(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.federated)
    }

    #[must_use]
    pub fn get_accelerator(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.accelerator)
    }

    pub async fn update_refresh_sql(&self, refresh_sql: Option<String>) -> Result<()> {
        let dataset_name = &self.dataset_name;

        let mut refresh = self.refresh_params.write().await;
        refresh.sql.clone_from(&refresh_sql);

        if self.dataset_name.schema() != Some(SPICE_RUNTIME_SCHEMA) {
            if let Some(sql_str) = &refresh_sql {
                tracing::info!("[refresh] Updated refresh SQL for {dataset_name} to {sql_str}");
            } else {
                tracing::info!("[refresh] Removed refresh SQL for {dataset_name}");
            }
        }

        Ok(())
    }

    #[allow(clippy::cast_possible_wrap)]
    #[allow(clippy::cast_possible_truncation)]
    async fn start_retention_check(
        dataset_name: TableReference,
        accelerator: Arc<dyn TableProvider>,
        retention: Retention,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
    ) {
        let time_column = retention.time_column;
        let retention_period = retention.period;
        let schema = accelerator.schema();
        let field = schema
            .column_with_name(time_column.as_str())
            .map(|(_, f)| f);

        let mut interval_timer = tokio::time::interval(retention.check_interval);

        let Some(timestamp_filter_converter) = TimestampFilterConvert::create(
            field.cloned(),
            Some(time_column.clone()),
            retention.time_format,
        ) else {
            tracing::error!("[retention] Failed to get the expression time format for {time_column}, check schema and time format");
            return;
        };

        loop {
            interval_timer.tick().await;

            if let Some(deleted_table_provider) = get_deletion_provider(Arc::clone(&accelerator)) {
                let ctx = SessionContext::new();

                let start = SystemTime::now() - retention_period;

                let timestamp = refresh::get_timestamp(start);
                let expr = timestamp_filter_converter.convert(timestamp, Operator::Lt);

                let timestamp = if let Some(value) =
                    chrono::DateTime::from_timestamp((timestamp / 1_000_000_000) as i64, 0)
                {
                    value.to_rfc3339()
                } else {
                    tracing::warn!("[retention] Unable to convert timestamp");
                    continue;
                };
                if dataset_name.schema() == Some(SPICE_RUNTIME_SCHEMA) {
                    tracing::debug!(
                        "[retention] Evicting data for {dataset_name} where {time_column} < {}...",
                        timestamp
                    );
                } else {
                    tracing::info!(
                        "[retention] Evicting data for {dataset_name} where {time_column} < {}...",
                        timestamp
                    );
                }

                tracing::debug!("[retention] Expr {expr:?}");

                let plan = deleted_table_provider
                    .delete_from(&ctx.state(), &vec![expr])
                    .await;
                match plan {
                    Ok(plan) => {
                        match collect(plan, ctx.task_ctx()).await {
                            Err(e) => {
                                tracing::error!("[retention] Error running retention check: {e}");
                            }
                            Ok(deleted) => {
                                let num_records = deleted.first().map_or(0, |f| {
                                    f.column(0)
                                        .as_any()
                                        .downcast_ref::<UInt64Array>()
                                        .map_or(0, |v| v.values().first().map_or(0, |f| *f))
                                });

                                if dataset_name.schema() == Some(SPICE_RUNTIME_SCHEMA) {
                                    tracing::debug!("[retention] Evicted {num_records} records for {dataset_name}");
                                } else {
                                    tracing::info!("[retention] Evicted {num_records} records for {dataset_name}");
                                }

                                if num_records > 0 {
                                    if let Some(cache_provider) = &cache_provider {
                                        if let Err(e) = cache_provider
                                            .invalidate_for_table(dataset_name.clone())
                                            .await
                                        {
                                            tracing::error!("Failed to invalidate cached results for dataset {}: {e}", &dataset_name);
                                        }
                                    }
                                }
                            }
                        };
                    }
                    Err(e) => {
                        tracing::error!("[retention] Error running retention check: {e}");
                    }
                }
            } else {
                tracing::error!("[retention] Accelerated table does not support delete");
            }
        }
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
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = self
            .accelerator
            .scan(state, projection, filters, limit)
            .await?;

        let plan: Arc<dyn ExecutionPlan> = match self.zero_results_action {
            ZeroResultsAction::ReturnEmpty => input,
            ZeroResultsAction::UseSource => Arc::new(FallbackOnZeroResultsScanExec::new(
                self.dataset_name.clone(),
                input,
                Arc::clone(&self.federated),
                TableScanParams::new(state, projection, filters, limit),
            )),
        };

        Ok(Arc::new(SchemaCastScanExec::new(plan, self.schema())))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
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
        let federated_insert_plan = self
            .federated
            .insert_into(state, federated_input, overwrite)
            .await?;

        // Return the equivalent of a UNION ALL that inserts both into the acceleration and federated source tables.
        let union_plan = Arc::new(UnionExec::new(vec![
            accelerated_insert_plan,
            federated_insert_plan,
        ]));

        Ok(union_plan)
    }
}

pub struct Retention {
    pub(crate) time_column: String,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) period: Duration,
    pub(crate) check_interval: Duration,
}

impl Retention {
    #[must_use]
    pub fn new(
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        retention_period: Option<Duration>,
        retention_check_interval: Option<Duration>,
        retention_check_enabled: bool,
    ) -> Option<Self> {
        if !retention_check_enabled {
            return None;
        }
        if let (Some(time_column), Some(period), Some(check_interval)) =
            (time_column, retention_period, retention_check_interval)
        {
            Some(Self {
                time_column,
                time_format,
                period,
                check_interval,
            })
        } else {
            None
        }
    }
}
