use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{any::Any, sync::Arc, time::Duration};

use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use data_components::delete::get_deletion_provider;
use datafusion::common::OwnedTableReference;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanProperties};
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::Expr,
};
use futures::{stream::BoxStream, StreamExt};
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{RefreshMode, ZeroResultsAction};
use spicepod::component::dataset::TimeFormat;
use tokio::task::JoinHandle;
use tokio::time::interval;

use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_stream::wrappers::ReceiverStream;

use crate::datafusion::filter_converter::TimestampFilterConvert;
use crate::execution_plan::fallback_on_zero_results::FallbackOnZeroResultsScanExec;
use crate::execution_plan::schema_cast::SchemaCastScanExec;
use crate::execution_plan::slice::SliceExec;
use crate::execution_plan::tee::TeeExec;
use crate::execution_plan::TableScanParams;
use crate::object_store_registry::default_runtime_env;
use crate::{
    dataconnector::{self, get_data},
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    status,
    timing::TimeMeasurement,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get data from connector: {source}"))]
    UnableToGetDataFromConnector { source: dataconnector::Error },

    #[snafu(display("Unable to scan table provider: {source}"))]
    UnableToScanTableProvider {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to trigger table refresh: {source}"))]
    FailedToTriggerRefresh {
        source: tokio::sync::mpsc::error::SendError<()>,
    },

    #[snafu(display("Manual refresh is not supported for `append` mode"))]
    ManualRefreshIsNotSupported {},

    #[snafu(display("Unable to get unix timestamp: {source}"))]
    UnableToGetUnixTimestamp { source: SystemTimeError },
}

pub type Result<T> = std::result::Result<T, Error>;

// An accelerated table consists of a federated table and a local accelerator.
//
// The accelerator must support inserts.
// AcceleratedTable::new returns an instance of the table and a oneshot receiver that will be triggered when the table is ready, right after the initial data refresh finishes.
pub struct AcceleratedTable {
    dataset_name: String,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<dyn TableProvider>,
    refresh_trigger: Option<mpsc::Sender<()>>,
    handlers: Vec<JoinHandle<()>>,
    zero_results_action: ZeroResultsAction,
    refresh_params: Arc<RwLock<Refresh>>,
}

enum AccelerationRefreshMode {
    Full(Receiver<()>),
    Append,
}

fn validate_refresh_data_window(refresh: &Refresh, dataset: &str, schema: &SchemaRef) {
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
    dataset_name: String,
    federated: Arc<dyn TableProvider>,
    accelerator: Arc<dyn TableProvider>,
    refresh: Refresh,
    retention: Option<Retention>,
    zero_results_action: ZeroResultsAction,
}

impl Builder {
    pub fn new(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        refresh: Refresh,
    ) -> Self {
        Self {
            dataset_name,
            federated,
            accelerator,
            refresh,
            retention: None,
            zero_results_action: ZeroResultsAction::default(),
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

    pub async fn build(self) -> (AcceleratedTable, oneshot::Receiver<()>) {
        let mut refresh_trigger = None;
        let mut scheduled_refreshes_handle: Option<JoinHandle<()>> = None;
        let (ready_sender, is_ready) = oneshot::channel::<()>();

        let acceleration_refresh_mode: AccelerationRefreshMode = match self.refresh.mode {
            RefreshMode::Append => AccelerationRefreshMode::Append,
            RefreshMode::Full => {
                let (trigger, receiver) = mpsc::channel::<()>(1);
                refresh_trigger = Some(trigger.clone());
                scheduled_refreshes_handle = AcceleratedTable::schedule_regular_refreshes(
                    self.refresh.check_interval,
                    trigger,
                )
                .await;
                AccelerationRefreshMode::Full(receiver)
            }
        };

        validate_refresh_data_window(&self.refresh, &self.dataset_name, &self.federated.schema());

        let refresh_params = Arc::new(RwLock::new(self.refresh));

        let refresh_handle = tokio::spawn(AcceleratedTable::start_refresh(
            self.dataset_name.clone(),
            Arc::clone(&self.federated),
            Arc::clone(&refresh_params),
            acceleration_refresh_mode,
            Arc::clone(&self.accelerator),
            ready_sender,
        ));

        let mut handlers = vec![];
        handlers.push(refresh_handle);

        if let Some(scheduled_refreshes_handle) = scheduled_refreshes_handle {
            handlers.push(scheduled_refreshes_handle);
        }

        if let Some(retention) = self.retention {
            let retention_check_handle = tokio::spawn(AcceleratedTable::start_retention_check(
                self.dataset_name.clone(),
                Arc::clone(&self.accelerator),
                retention,
            ));
            handlers.push(retention_check_handle);
        }

        (
            AcceleratedTable {
                dataset_name: self.dataset_name,
                accelerator: self.accelerator,
                federated: self.federated,
                refresh_trigger,
                handlers,
                zero_results_action: self.zero_results_action,
                refresh_params,
            },
            is_ready,
        )
    }
}

impl AcceleratedTable {
    pub fn builder(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        refresh: Refresh,
    ) -> Builder {
        Builder::new(dataset_name, federated, accelerator, refresh)
    }

    pub async fn trigger_refresh(&self) -> Result<()> {
        match &self.refresh_trigger {
            Some(refresh_trigger) => {
                refresh_trigger
                    .send(())
                    .await
                    .context(FailedToTriggerRefreshSnafu)?;
            }
            None => {
                ManualRefreshIsNotSupportedSnafu.fail()?;
            }
        }

        Ok(())
    }

    pub async fn update_refresh_sql(&self, refresh_sql: Option<String>) -> Result<()> {
        let dataset_name = &self.dataset_name;

        if refresh_sql.is_some() {
            tracing::info!("[refresh] Updating refresh SQL for {dataset_name}");
        } else {
            tracing::info!("[refresh] Removing refresh SQL for {dataset_name}");
        }
        let mut refresh = self.refresh_params.write().await;
        refresh.sql = refresh_sql;
        Ok(())
    }

    async fn schedule_regular_refreshes(
        refresh_check_interval: Option<Duration>,
        refresh_trigger: mpsc::Sender<()>,
    ) -> Option<JoinHandle<()>> {
        if let Some(refresh_check_interval) = refresh_check_interval {
            let mut interval_timer = interval(refresh_check_interval);
            let trigger = refresh_trigger.clone();
            let handle = tokio::spawn(async move {
                loop {
                    interval_timer.tick().await;
                    // If sending fails, it means the receiver is dropped, and we should stop the task.
                    if trigger.send(()).await.is_err() {
                        break;
                    }
                }
            });

            return Some(handle);
        } else if let Err(err) = refresh_trigger.send(()).await {
            tracing::error!("Failed to trigger refresh: {err}");
        }

        None
    }

    #[allow(clippy::cast_possible_wrap)]
    async fn start_retention_check(
        dataset_name: String,
        accelerator: Arc<dyn TableProvider>,
        retention: Retention,
    ) {
        let time_column = retention.time_column;
        let retention_period = retention.period;
        let schema = accelerator.schema();
        let field = schema
            .column_with_name(time_column.as_str())
            .map(|(_, f)| f);

        let mut interval_timer = tokio::time::interval(retention.check_interval);

        let Some(timestamp_filter_converter) =
            TimestampFilterConvert::create(field, Some(time_column.clone()), retention.time_format)
        else {
            tracing::error!("[retention] Failed to get the expression time format for {time_column}, check schema and time format");
            return;
        };

        loop {
            interval_timer.tick().await;

            if let Some(deleted_table_provider) = get_deletion_provider(Arc::clone(&accelerator)) {
                let ctx = SessionContext::new();

                let start = SystemTime::now() - retention_period;

                let Ok(timestamp) = get_timestamp(start) else {
                    tracing::error!("[retention] Failed to get timestamp");
                    continue;
                };
                let expr = timestamp_filter_converter.convert(timestamp, Operator::Lt);
                tracing::info!(
                    "[retention] Evicting data for {dataset_name} where {time_column} < {}...",
                    if let Some(value) = chrono::DateTime::from_timestamp(timestamp as i64, 0) {
                        value.to_rfc3339()
                    } else {
                        tracing::warn!("[retention] Unable to convert timestamp");
                        continue;
                    }
                );

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
                                let result = deleted.first().map_or(0, |f| {
                                    f.column(0)
                                        .as_any()
                                        .downcast_ref::<UInt64Array>()
                                        .map_or(0, |v| v.values().first().map_or(0, |f| *f))
                                });

                                tracing::info!(
                                    "[retention] Evicted {result} records for {dataset_name}",
                                );
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

    async fn start_refresh(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        refresh_params: Arc<RwLock<Refresh>>,
        acceleration_refresh_mode: AccelerationRefreshMode,
        accelerator: Arc<dyn TableProvider>,
        ready_sender: oneshot::Sender<()>,
    ) {
        let mut stream = Self::stream_updates(
            dataset_name.clone(),
            federated,
            refresh_params,
            acceleration_refresh_mode,
        );

        let ctx = SessionContext::new();

        let mut ready_sender = Some(ready_sender);

        loop {
            let future_result = stream.next().await;

            match future_result {
                Some(data_update) => {
                    let Ok(data_update) = data_update else {
                        continue;
                    };
                    let state = ctx.state();

                    let overwrite = data_update.update_type == UpdateType::Overwrite;
                    match accelerator
                        .insert_into(
                            &state,
                            Arc::new(DataUpdateExecutionPlan::new(data_update)),
                            overwrite,
                        )
                        .await
                    {
                        Ok(plan) => {
                            if let Err(e) = collect(plan, ctx.task_ctx()).await {
                                tracing::error!("Error adding data for {dataset_name}: {e}");
                            } else if let Some(sender) = ready_sender.take() {
                                sender.send(()).ok();
                            };
                        }
                        Err(e) => {
                            tracing::error!("Error adding data for {dataset_name}: {e}");
                        }
                    }
                }
                None => break,
            };
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn stream_updates<'a>(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        refresh_params: Arc<RwLock<Refresh>>,
        acceleration_refresh_mode: AccelerationRefreshMode,
    ) -> BoxStream<'a, Result<DataUpdate>> {
        let mut ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            default_runtime_env(),
        );
        if let Err(e) = ctx.register_table(
            OwnedTableReference::bare(dataset_name.clone()),
            Arc::clone(&federated),
        ) {
            tracing::error!("Unable to register federated table: {e}");
        }

        Box::pin(stream! {
            match acceleration_refresh_mode {
                AccelerationRefreshMode::Append => {
                    let plan = federated.scan(&ctx.state(), None, &[], None).await.context(UnableToScanTableProviderSnafu {})?;

                    if plan.output_partitioning().partition_count() > 1 {
                        tracing::error!("Append mode is not supported for datasets with multiple partitions: {dataset_name}");
                        return;
                    }

                    let schema = federated.schema();

                    let mut stream = plan.execute(0, ctx.task_ctx()).context(UnableToScanTableProviderSnafu {})?;
                    loop {
                        match stream.next().await {
                            Some(Ok(batch)) => {
                                yield Ok(DataUpdate {
                                    schema: Arc::clone(&schema),
                                    data: vec![batch],
                                    update_type: UpdateType::Append,
                                });
                            }
                            Some(Err(e)) => {
                                tracing::error!("Error reading data for dataset {dataset_name}: {e}");
                                yield Err(Error::UnableToScanTableProvider { source: e });
                            }
                            None => break,
                        }
                    }
                }
                AccelerationRefreshMode::Full(receiver) => {

                    let refresh_params_lock = refresh_params.read().await;
                    let refresh_settings = &*refresh_params_lock;

                    let schema = federated.schema();
                    let column = refresh_settings.time_column.as_deref().unwrap_or_default();
                    let field = schema.column_with_name(column).map(|(_, f)| f);
                    let filter_converter = TimestampFilterConvert::create(field, refresh_settings.time_column.clone(), refresh_settings.time_format.clone());
                    drop(refresh_params_lock);

                    let mut refresh_stream = ReceiverStream::new(receiver);

                    while refresh_stream.next().await.is_some() {
                        tracing::info!("[refresh] Loading data for dataset {dataset_name}");
                        status::update_dataset(&dataset_name, status::ComponentStatus::Refreshing);

                        let refresh_params_lock = refresh_params.read().await;
                        let refresh_settings = &*refresh_params_lock;

                        let timer = TimeMeasurement::new("load_dataset_duration_ms", vec![("dataset", dataset_name.clone())]);
                        let filters = match (refresh_settings.period, filter_converter.as_ref()){
                            (Some(period), Some(converter)) => {
                                let start = SystemTime::now() - period;

                                let Ok(timestamp) = get_timestamp(start) else {
                                    tracing::error!("[refresh] Failed to calculate timestamp of refresh period start");
                                    continue;
                                };
                                vec![converter.convert(timestamp, Operator::Gt)]
                            },
                            _ => vec![],
                        };

                        let refresh_sql = refresh_settings.sql.clone();
                        drop(refresh_params_lock);

                        let data = match get_data(&mut ctx, OwnedTableReference::bare(dataset_name.clone()), Arc::clone(&federated), refresh_sql, filters).await {
                            Ok(data) => data,
                            Err(e) => {
                                tracing::error!("[refresh] Failed to load data for dataset {dataset_name}: {e}");
                                yield Err(Error::UnableToGetDataFromConnector { source: e });
                                continue;
                            }
                        };
                        yield Ok(DataUpdate {
                            schema: data.0,
                            data: data.1,
                            update_type: UpdateType::Overwrite,
                        });

                        drop(timer);
                    }
                }
            }
        })
    }
}

fn get_timestamp(time: SystemTime) -> Result<u64> {
    let timestamp = time
        .duration_since(UNIX_EPOCH)
        .context(UnableToGetUnixTimestampSnafu)?;
    Ok(timestamp.as_secs())
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
        state: &SessionState,
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
        state: &SessionState,
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
    pub(crate) fn new(
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

#[derive(Clone, Debug)]
pub struct Refresh {
    pub(crate) time_column: Option<String>,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) check_interval: Option<Duration>,
    pub(crate) sql: Option<String>,
    pub(crate) mode: RefreshMode,
    pub(crate) period: Option<Duration>,
}

impl Refresh {
    #[allow(clippy::needless_pass_by_value)]
    #[must_use]
    pub(crate) fn new(
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        check_interval: Option<Duration>,
        sql: Option<String>,
        mode: RefreshMode,
        period: Option<Duration>,
    ) -> Self {
        Self {
            time_column,
            time_format,
            check_interval,
            sql,
            mode,
            period,
        }
    }
}
