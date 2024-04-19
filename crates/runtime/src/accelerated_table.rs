use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{any::Any, sync::Arc, time::Duration};

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, SchemaRef};
use async_stream::stream;
use async_trait::async_trait;
use data_components::delete::get_deletion_provider;
use datafusion::common::OwnedTableReference;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{cast, col, lit, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, ExecutionPlan, ExecutionPlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::Expr,
};
use futures::{stream::BoxStream, StreamExt};
use object_store::ObjectStore;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{RefreshMode, TimeFormat};
use tokio::task::JoinHandle;
use tokio::time::interval;
use url::Url;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;

use crate::execution_plan::slice::SliceExec;
use crate::execution_plan::tee::TeeExec;
use crate::{
    dataconnector::{self, get_all_data},
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    status,
    timing::TimeMeasurement,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get data from connector: {source}"))]
    UnableToGetDataFromConnector {
        source: dataconnector::Error,
    },

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

    UnableToGetUnixTimestamp {
        source: SystemTimeError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

// An accelerated table consists of a federated table and a local accelerator.
//
// The accelerator must support inserts.
pub(crate) struct AcceleratedTable {
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<dyn TableProvider>,
    refresh_trigger: Option<mpsc::Sender<()>>,
    handlers: Vec<JoinHandle<()>>,
}

enum AccelerationRefreshMode {
    Full(Receiver<()>),
    Append,
}

#[derive(Debug, Clone)]
enum ExprTimeFormat {
    ISO8601,
    UnixTimestamp(ExprUnixTimestamp),
    Timestamp,
}

#[derive(Debug, Clone)]
struct ExprUnixTimestamp {
    scale: u64,
}

#[allow(clippy::too_many_arguments)]
impl AcceleratedTable {
    pub async fn new(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        refresh_mode: RefreshMode,
        refresh_interval: Option<Duration>,
        refresh_sql: Option<String>,
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        retention_check_interval: Option<Duration>,
        retention_period: Option<Duration>,
        retention_enabled: bool,
        object_store: Option<(Url, Arc<dyn ObjectStore + 'static>)>,
    ) -> Self {
        let mut refresh_trigger = None;
        let mut scheduled_refreshes_handle: Option<JoinHandle<()>> = None;

        let acceleration_refresh_mode: AccelerationRefreshMode = match refresh_mode {
            RefreshMode::Append => AccelerationRefreshMode::Append,
            RefreshMode::Full => {
                let (trigger, receiver) = mpsc::channel::<()>(1);
                refresh_trigger = Some(trigger.clone());
                scheduled_refreshes_handle =
                    Self::schedule_regular_refreshes(refresh_interval, trigger).await;
                AccelerationRefreshMode::Full(receiver)
            }
        };

        let refresh_handle = tokio::spawn(Self::start_refresh(
            dataset_name.clone(),
            Arc::clone(&federated),
            acceleration_refresh_mode,
            refresh_sql,
            Arc::clone(&accelerator),
            object_store,
        ));

        let mut handlers = vec![];
        handlers.push(refresh_handle);

        if let Some(scheduled_refreshes_handle) = scheduled_refreshes_handle {
            handlers.push(scheduled_refreshes_handle);
        }

        if retention_enabled {
            if let (Some(time_column), Some(retention_period), Some(retention_check_interval)) =
                (time_column, retention_period, retention_check_interval)
            {
                let retention_check_handle = tokio::spawn(Self::start_retention_check(
                    dataset_name,
                    Arc::clone(&accelerator),
                    retention_check_interval,
                    time_column,
                    time_format,
                    retention_period,
                ));
                handlers.push(retention_check_handle);
            }
        }

        Self {
            accelerator,
            federated,
            refresh_trigger,
            handlers,
        }
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

    async fn schedule_regular_refreshes(
        refresh_interval: Option<Duration>,
        refresh_trigger: mpsc::Sender<()>,
    ) -> Option<JoinHandle<()>> {
        if let Some(refresh_interval) = refresh_interval {
            let mut interval_timer = interval(refresh_interval);
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

    async fn start_retention_check(
        dataset_name: String,
        accelerator: Arc<dyn TableProvider>,
        interval: Duration,
        time_column: String,
        time_format: Option<TimeFormat>,
        retention_period: Duration,
    ) {
        let mut interval_timer = tokio::time::interval(interval);

        let schema = accelerator.schema();

        let field = schema.column_with_name(time_column.as_str());

        let Some(expr_time_format) = get_expr_time_format(field, &time_format) else {
            tracing::error!("[retention] Failed to get the expression time format for {time_column}, check schema and time format");
            return;
        };

        loop {
            interval_timer.tick().await;

            tracing::info!("[retention] Running retention check for {dataset_name}...");

            if let Some(deleted_table_provider) = get_deletion_provider(Arc::clone(&accelerator)) {
                let ctx = SessionContext::new();

                let Some(expr) = get_expr(retention_period, &time_column, expr_time_format.clone())
                else {
                    continue;
                };

                tracing::debug!(
                    "[retention] Evicting data for {dataset_name} {:?} {:?}...",
                    expr,
                    retention_period
                );

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
        acceleration_refresh_mode: AccelerationRefreshMode,
        refresh_sql: Option<String>,
        accelerator: Arc<dyn TableProvider>,
        object_store: Option<(Url, Arc<dyn ObjectStore + 'static>)>,
    ) {
        let mut stream = Self::stream_updates(
            dataset_name.clone(),
            federated,
            acceleration_refresh_mode,
            refresh_sql,
            object_store,
        );

        let ctx = SessionContext::new();
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
        acceleration_refresh_mode: AccelerationRefreshMode,
        refresh_sql: Option<String>,
        object_store: Option<(Url, Arc<dyn ObjectStore + 'static>)>,
    ) -> BoxStream<'a, Result<DataUpdate>> {
        let mut ctx = SessionContext::new();
        if let Some((ref url, ref object_store)) = object_store {
            ctx.runtime_env()
                .register_object_store(url, Arc::clone(object_store));
        }
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
                        tracing::error!("Append is not supported for tables with multiple partitions: {dataset_name}");
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
                                tracing::error!("Error reading data for {dataset_name}: {e}");
                                yield Err(Error::UnableToScanTableProvider { source: e });
                            }
                            None => break,
                        }
                    }
                }
                AccelerationRefreshMode::Full(receiver) => {
                    let mut refresh_stream = ReceiverStream::new(receiver);

                    while refresh_stream.next().await.is_some() {
                        tracing::info!("Refreshing data for {dataset_name}");
                        status::update_dataset(&dataset_name, status::ComponentStatus::Refreshing);
                        let timer = TimeMeasurement::new("load_dataset_duration_ms", vec![("dataset", dataset_name.clone())]);
                        let all_data = match get_all_data(&mut ctx, OwnedTableReference::bare(dataset_name.clone()), Arc::clone(&federated), refresh_sql.clone()).await {
                            Ok(data) => data,
                            Err(e) => {
                                tracing::error!("Error refreshing data for {dataset_name}: {e}");
                                yield Err(Error::UnableToGetDataFromConnector { source: e });
                                continue;
                            }
                        };
                        yield Ok(DataUpdate {
                            schema: all_data.0,
                            data: all_data.1,
                            update_type: UpdateType::Overwrite,
                        });

                        drop(timer);
                    }
                }
            }
        })
    }
}

fn get_expr_time_format(
    field: Option<(usize, &arrow::datatypes::Field)>,
    time_format: &Option<TimeFormat>,
) -> Option<ExprTimeFormat> {
    let expr_time_format = match field {
        Some(field) => match field.1.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64 => {
                let mut scale = 1;
                if let Some(time_format) = time_format.clone() {
                    if time_format == TimeFormat::UnixMillis {
                        scale = 1000;
                    }
                }
                ExprTimeFormat::UnixTimestamp(ExprUnixTimestamp { scale })
            }
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => ExprTimeFormat::Timestamp,
            DataType::Utf8 | DataType::LargeUtf8 => ExprTimeFormat::ISO8601,
            _ => {
                tracing::warn!("date type is not handled yet: {}", field.1.data_type());
                return None;
            }
        },
        None => {
            return None;
        }
    };
    Some(expr_time_format)
}

#[allow(clippy::cast_possible_wrap)]
fn get_expr(
    retention_period: Duration,
    time_column: &str,
    expr_time_format: ExprTimeFormat,
) -> Option<Expr> {
    let start = SystemTime::now();
    let timestamp = (start - retention_period).duration_since(UNIX_EPOCH);
    if timestamp.clone().is_err() {
        tracing::error!("[retention] Failed to get the unix timestamp");
        return None;
    }
    let timestamp = timestamp.map_or(0, |f| f.as_secs());

    match expr_time_format {
        ExprTimeFormat::ISO8601 => Some(
            cast(
                col(time_column),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            )
            .lt(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some((timestamp * 1000) as i64),
                None,
            ))),
        ),
        ExprTimeFormat::UnixTimestamp(format) => {
            Some(col(time_column).lt(lit(timestamp * format.scale)))
        }
        ExprTimeFormat::Timestamp => Some(col(time_column).lt(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some((timestamp * 1000) as i64), None),
        ))),
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
        self.accelerator.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.accelerator
            .scan(state, projection, filters, limit)
            .await
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
