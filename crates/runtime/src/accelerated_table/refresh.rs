use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_stream::stream;
use datafusion::common::OwnedTableReference;
use datafusion::execution::config::SessionConfig;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::{collect, ExecutionPlanProperties};
use datafusion::{
    datasource::TableProvider, execution::context::SessionContext, logical_expr::Expr,
};
use futures::Stream;
use futures::{stream::BoxStream, StreamExt};
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::RefreshMode;

use spicepod::component::dataset::TimeFormat;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;

use crate::datafusion::filter_converter::TimestampFilterConvert;
use crate::object_store_registry::default_runtime_env;
use crate::{
    dataconnector::get_data,
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    status,
    timing::TimeMeasurement,
};

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

pub(crate) async fn start(
    dataset_name: String,
    federated: Arc<dyn TableProvider>,
    refresh: Refresh,
    acceleration_refresh_mode: super::AccelerationRefreshMode,
    accelerator: Arc<dyn TableProvider>,
    ready_sender: oneshot::Sender<()>,
) {
    let mut stream = stream_updates(
        dataset_name.clone(),
        Arc::clone(&accelerator),
        federated,
        refresh,
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
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<dyn TableProvider>,
    refresh: super::Refresh,
    acceleration_refresh_mode: super::AccelerationRefreshMode,
) -> BoxStream<'a, super::Result<DataUpdate>> {
    let ctx = get_refresh_df_context(&dataset_name, &federated, &accelerator);

    match acceleration_refresh_mode {
        super::AccelerationRefreshMode::Append => {
            Box::pin(get_append_stream(dataset_name, federated, ctx))
        }
        super::AccelerationRefreshMode::Full(receiver)
        | super::AccelerationRefreshMode::BatchAppend(receiver) => Box::pin(
            get_full_or_batch_append_update_stream(federated, refresh, receiver, dataset_name, ctx),
        ),
    }
}

fn get_full_or_batch_append_update_stream(
    federated: Arc<dyn TableProvider>,
    refresh: super::Refresh,
    receiver: Receiver<()>,
    dataset_name: String,
    mut ctx: SessionContext,
) -> impl Stream<Item = super::Result<DataUpdate>> {
    let schema = federated.schema();
    let column = refresh.time_column.as_deref().unwrap_or_default();
    let field = schema.column_with_name(column).map(|(_, f)| f).cloned();
    let mut refresh_stream = ReceiverStream::new(receiver);
    let filter_converter = TimestampFilterConvert::create(
        field,
        refresh.time_column.clone(),
        refresh.time_format.clone(),
    );
    stream! {
        while refresh_stream.next().await.is_some() {
            let timer = TimeMeasurement::new(
                "load_dataset_duration_ms",
                vec![("dataset", dataset_name.clone())],
            );
            yield get_full_or_batch_append_update(
                dataset_name.clone(),
                refresh.clone(),
                filter_converter.clone(),
                &mut ctx,
                Arc::clone(&federated),
            )
            .await;
            drop(timer);
        }
    }
}

async fn get_full_or_batch_append_update(
    dataset_name: String,
    refresh: super::Refresh,
    filter_converter: Option<TimestampFilterConvert>,
    ctx: &mut SessionContext,
    federated: Arc<dyn TableProvider>,
) -> super::Result<DataUpdate> {
    tracing::info!("[refresh] Loading data for dataset {dataset_name}");
    status::update_dataset(dataset_name.as_str(), status::ComponentStatus::Refreshing);
    let refresh = refresh.clone();
    let filters =
        if let (Some(period), Some(converter)) = (refresh.period, filter_converter.as_ref()) {
            let start = SystemTime::now() - period;

            let timestamp = get_timestamp(start);
            vec![converter.convert(timestamp, Operator::Gt)]
        } else {
            vec![]
        };

    let update_type = match refresh.mode {
        RefreshMode::Full => UpdateType::Overwrite,
        _ => UpdateType::Append,
    };

    match get_data_update(
        ctx,
        dataset_name.clone(),
        federated,
        refresh.sql,
        filters,
        update_type,
    )
    .await
    {
        Ok(data) => Ok(data),
        Err(e) => {
            tracing::error!("[refresh] Failed to load data for dataset {dataset_name}: {e}");
            Err(e)
        }
    }
}

async fn get_data_update(
    ctx: &mut SessionContext,
    dataset_name: String,
    federated: Arc<dyn TableProvider>,
    sql: Option<String>,
    filters: Vec<Expr>,
    update_type: UpdateType,
) -> super::Result<DataUpdate> {
    match get_data(
        ctx,
        OwnedTableReference::bare(dataset_name.clone()),
        Arc::clone(&federated),
        sql,
        filters,
    )
    .await
    .map(|data| DataUpdate {
        schema: data.0,
        data: data.1,
        update_type,
    }) {
        Ok(data) => Ok(data),
        Err(e) => Err(super::Error::UnableToGetDataFromConnector { source: e }),
    }
}

fn get_append_stream(
    dataset_name: String,
    federated: Arc<dyn TableProvider>,
    ctx: SessionContext,
) -> impl Stream<Item = super::Result<DataUpdate>> {
    stream! {
        let plan = federated
            .scan(&ctx.state(), None, &[], None)
            .await
            .context(super::UnableToScanTableProviderSnafu {})?;

        if plan.output_partitioning().partition_count() > 1 {
            tracing::error!(
                "Append mode is not supported for datasets with multiple partitions: {dataset_name}"
            );
            return;
        }

        let schema = federated.schema();

        let mut stream = plan
            .execute(0, ctx.task_ctx())
            .context(super::UnableToScanTableProviderSnafu {})?;
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
                    yield Err(super::Error::UnableToScanTableProvider { source: e });
                }
                None => break,
            }
        }
    }
}

fn get_refresh_df_context(
    dataset_name: &String,
    federated: &Arc<dyn TableProvider>,
    accelerator: &Arc<dyn TableProvider>,
) -> SessionContext {
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().set_bool(
            "datafusion.execution.listing_table_ignore_subdirectory",
            false,
        ),
        default_runtime_env(),
    );
    if let Err(e) = ctx.register_table(
        OwnedTableReference::bare(dataset_name.clone()),
        Arc::clone(federated),
    ) {
        tracing::error!("Unable to register federated table: {e}");
    }

    let acc_dataset_name = format!("accelerated_{dataset_name}");

    if let Err(e) = ctx.register_table(
        OwnedTableReference::bare(acc_dataset_name),
        Arc::clone(accelerator),
    ) {
        tracing::error!("Unable to register accelerator table: {e}");
    }
    ctx
}

pub(crate) fn get_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .expect("Clock should not go backwards more than EPOCH")
        .as_nanos()
}
