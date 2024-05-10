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

pub(crate) enum AccelerationRefreshMode {
    Full(Receiver<()>),
    BatchAppend(Receiver<()>),
    Append,
}

pub(crate) struct Refresher {
    dataset_name: String,
    federated: Arc<dyn TableProvider>,
    refresh: Refresh,
    accelerator: Arc<dyn TableProvider>,
}

impl Refresher {
    pub(crate) fn new(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        refresh: Refresh,
        accelerator: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            dataset_name,
            federated,
            refresh,
            accelerator,
        }
    }

    pub(crate) async fn start(
        &self,
        acceleration_refresh_mode: AccelerationRefreshMode,
        ready_sender: oneshot::Sender<()>,
    ) {
        let dataset_name = self.dataset_name.clone();
        let mut stream = self.stream_updates(acceleration_refresh_mode);

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
                    match self
                        .accelerator
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

    fn stream_updates(
        &self,
        acceleration_refresh_mode: AccelerationRefreshMode,
    ) -> BoxStream<'_, super::Result<DataUpdate>> {
        match acceleration_refresh_mode {
            AccelerationRefreshMode::Append => Box::pin(self.get_append_stream()),
            AccelerationRefreshMode::Full(receiver) => {
                Box::pin(self.get_full_update_stream(receiver))
            }
            AccelerationRefreshMode::BatchAppend(receiver) => {
                Box::pin(self.get_batch_append_update_stream(receiver))
            }
        }
    }

    fn get_append_stream(&self) -> impl Stream<Item = super::Result<DataUpdate>> {
        let ctx = self.get_refresh_df_context();
        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();

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

    fn get_full_update_stream(
        &self,
        receiver: Receiver<()>,
    ) -> impl Stream<Item = super::Result<DataUpdate>> + '_ {
        let dataset_name = self.dataset_name.clone();

        let mut refresh_stream = ReceiverStream::new(receiver);
        stream! {
            while refresh_stream.next().await.is_some() {
                let timer = TimeMeasurement::new(
                    "load_dataset_duration_ms",
                    vec![("dataset", dataset_name.clone())],
                );
                yield self.get_full_or_batch_append_update(None).await;
                drop(timer);
            }
        }
    }

    fn get_batch_append_update_stream(
        &self,
        receiver: Receiver<()>,
    ) -> impl Stream<Item = super::Result<DataUpdate>> + '_ {
        let dataset_name = self.dataset_name.clone();

        let mut refresh_stream = ReceiverStream::new(receiver);
        stream! {
            while refresh_stream.next().await.is_some() {
                let timer = TimeMeasurement::new(
                    "batch_append_dataset_duration_ms",
                    vec![("dataset", dataset_name.clone())],
                );
                yield self.get_full_or_batch_append_update(None).await;
                drop(timer);
            }
        }
    }

    async fn get_full_or_batch_append_update(
        &self,
        overwrite_timestamp_in_nano: Option<u128>,
    ) -> super::Result<DataUpdate> {
        let dataset_name = self.dataset_name.clone();
        let refresh = self.refresh.clone();
        let filter_converter = self.get_filter_converter();

        tracing::info!("[refresh] Loading data for dataset {dataset_name}");
        status::update_dataset(dataset_name.as_str(), status::ComponentStatus::Refreshing);
        let refresh = refresh.clone();
        let filters =
            if let (Some(period), Some(converter)) = (refresh.period, filter_converter.as_ref()) {
                let timestamp = overwrite_timestamp_in_nano
                    .unwrap_or(get_timestamp(SystemTime::now() - period));

                vec![converter.convert(timestamp, Operator::Gt)]
            } else {
                vec![]
            };

        match self.get_data_update(filters).await {
            Ok(data) => Ok(data),
            Err(e) => {
                tracing::error!("[refresh] Failed to load data for dataset {dataset_name}: {e}");
                Err(e)
            }
        }
    }

    async fn get_data_update(&self, filters: Vec<Expr>) -> super::Result<DataUpdate> {
        let update_type = match self.refresh.clone().mode {
            RefreshMode::Full => UpdateType::Overwrite,
            _ => UpdateType::Append,
        };
        let mut ctx = self.get_refresh_df_context();
        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();
        match get_data(
            &mut ctx,
            OwnedTableReference::bare(dataset_name.clone()),
            Arc::clone(&federated),
            self.refresh.sql.clone(),
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

    fn get_refresh_df_context(&self) -> SessionContext {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            default_runtime_env(),
        );
        let dataset_name = self.dataset_name.clone();
        if let Err(e) = ctx.register_table(
            OwnedTableReference::bare(dataset_name.clone()),
            Arc::clone(&self.federated),
        ) {
            tracing::error!("Unable to register federated table: {e}");
        }

        let acc_dataset_name = format!("accelerated_{dataset_name}");

        if let Err(e) = ctx.register_table(
            OwnedTableReference::bare(acc_dataset_name),
            Arc::clone(&self.accelerator),
        ) {
            tracing::error!("Unable to register accelerator table: {e}");
        }
        ctx
    }

    fn get_filter_converter(&self) -> Option<TimestampFilterConvert> {
        let schema = self.federated.schema();
        let column = self.refresh.time_column.as_deref().unwrap_or_default();
        let field = schema.column_with_name(column).map(|(_, f)| f).cloned();

        TimestampFilterConvert::create(
            field,
            self.refresh.time_column.clone(),
            self.refresh.time_format.clone(),
        )
    }
}

pub(crate) fn get_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
