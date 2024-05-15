use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::TimestampNanosecondArray;
use async_stream::stream;
use datafusion::common::TableReference;
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
use tokio::sync::RwLock;
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
    Append(Option<Receiver<()>>),
}

pub(crate) struct Refresher {
    dataset_name: String,
    federated: Arc<dyn TableProvider>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
}

impl Refresher {
    pub(crate) fn new(
        dataset_name: String,
        federated: Arc<dyn TableProvider>,
        refresh: Arc<RwLock<Refresh>>,
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
        let mut stream = self.stream_updates(acceleration_refresh_mode).await;

        let ctx = SessionContext::new();

        let mut ready_sender = Some(ready_sender);

        loop {
            let future_result = stream.next().await;

            match future_result {
                Some(data_update) => {
                    let Ok(data_update) = data_update else {
                        continue;
                    };

                    if data_update.data.is_empty() {
                        if let Some(sender) = ready_sender.take() {
                            sender.send(()).ok();
                        };
                        status::update_dataset(
                            dataset_name.as_str(),
                            status::ComponentStatus::Ready,
                        );
                        continue;
                    };

                    if let Some(data) = data_update.data.first() {
                        if data.columns().is_empty() {
                            if let Some(sender) = ready_sender.take() {
                                sender.send(()).ok();
                            };
                            status::update_dataset(
                                dataset_name.as_str(),
                                status::ComponentStatus::Ready,
                            );
                            continue;
                        }
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
                            status::update_dataset(
                                dataset_name.as_str(),
                                status::ComponentStatus::Ready,
                            );
                        }
                        Err(e) => {
                            status::update_dataset(
                                dataset_name.as_str(),
                                status::ComponentStatus::Error,
                            );
                            tracing::error!("Error adding data for {dataset_name}: {e}");
                        }
                    }
                }
                None => break,
            };
        }
    }

    async fn stream_updates(
        &self,
        acceleration_refresh_mode: AccelerationRefreshMode,
    ) -> BoxStream<'_, super::Result<DataUpdate>> {
        let time_column = self.refresh.read().await.time_column.clone();

        match acceleration_refresh_mode {
            AccelerationRefreshMode::Append(receiver) => {
                if let (Some(receiver), Some(_)) = (receiver, time_column) {
                    Box::pin(self.get_incremental_append_update_stream(receiver))
                } else {
                    Box::pin(self.get_append_stream())
                }
            }
            AccelerationRefreshMode::Full(receiver) => {
                Box::pin(self.get_full_update_stream(receiver))
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
                yield self.get_full_or_incremental_append_update(None).await;
                drop(timer);
            }
        }
    }

    fn get_incremental_append_update_stream(
        &self,
        receiver: Receiver<()>,
    ) -> impl Stream<Item = super::Result<DataUpdate>> + '_ {
        let dataset_name = self.dataset_name.clone();

        let mut refresh_stream = ReceiverStream::new(receiver);
        stream! {
            while refresh_stream.next().await.is_some() {
                let timer = TimeMeasurement::new(
                    "append_dataset_duration_ms",
                    vec![("dataset", dataset_name.clone())],
                );
                match self.get_latest_timestamp().await {
                    Ok(timestamp) => {
                        yield self.get_full_or_incremental_append_update(timestamp).await;
                    }
                    Err(e) => {
                        tracing::error!("No latest timestamp is found: {e}");
                    }
                }
                drop(timer);
            }
        }
    }

    #[allow(clippy::cast_sign_loss)]
    async fn get_latest_timestamp(&self) -> super::Result<Option<u128>> {
        let ctx = self.get_refresh_df_context();
        let refresh = self.refresh.read().await;

        let column =
            refresh
                .time_column
                .clone()
                .context(super::FailedToFindLatestTimestampSnafu {
                    reason: "Failed to get latest timestamp due to time column not specified",
                })?;
        let df = ctx
            .sql(
                format!(
                    "SELECT CAST({column} AS timestamp) AS a FROM accelerated_{} ORDER BY a DESC LIMIT 1",
                    self.dataset_name.clone()
                )
                .as_str(),
            )
            .await.context(super::FailedToQueryLatestTimestampSnafu)?;
        let result = &df
            .collect()
            .await
            .context(super::FailedToQueryLatestTimestampSnafu)?;

        let Some(result) = result.first() else {
            return Ok(None);
        };

        let array = result.column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .context(super::FailedToFindLatestTimestampSnafu {
                reason: "Failed to get latest timestamp during incremental appending process due to time column is unable to cast to timestamp",
            })?;

        if array.is_empty() {
            return Ok(None);
        }

        let mut value = array.value(0) as u128;

        let schema = &self.accelerator.schema();
        let Ok(accelerated_field) = schema.field_with_name(&column) else {
            return Err(super::Error::FailedToFindLatestTimestamp {
                reason: "Failed to get latest timestamp due to time column not specified"
                    .to_string(),
            });
        };

        if let arrow::datatypes::DataType::Int8
        | arrow::datatypes::DataType::Int16
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::UInt8
        | arrow::datatypes::DataType::UInt16
        | arrow::datatypes::DataType::UInt32
        | arrow::datatypes::DataType::UInt64 = accelerated_field.data_type()
        {
            match refresh.time_format.clone() {
                Some(TimeFormat::UnixMillis) => {
                    value *= 1_000_000;
                }
                Some(TimeFormat::UnixSeconds) => {
                    value *= 1_000_000_000;
                }
                _ => (),
            }
        };

        Ok(Some(value))
    }

    async fn get_full_or_incremental_append_update(
        &self,
        overwrite_timestamp_in_nano: Option<u128>,
    ) -> super::Result<DataUpdate> {
        let dataset_name = self.dataset_name.clone();
        let refresh = self.refresh.read().await;
        let filter_converter = self.get_filter_converter(&refresh);

        tracing::info!("[refresh] Loading data for dataset {dataset_name}");
        status::update_dataset(dataset_name.as_str(), status::ComponentStatus::Refreshing);
        let refresh = refresh.clone();
        let mut filters = vec![];
        if let Some(converter) = filter_converter.as_ref() {
            if let Some(timestamp) = overwrite_timestamp_in_nano {
                filters.push(converter.convert(timestamp, Operator::Gt));
            } else if let Some(period) = refresh.period {
                filters.push(
                    converter.convert(get_timestamp(SystemTime::now() - period), Operator::Gt),
                );
            }
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
        let refresh = self.refresh.read().await;
        let update_type = match refresh.mode {
            RefreshMode::Full => UpdateType::Overwrite,
            RefreshMode::Append => UpdateType::Append,
        };
        let mut ctx = self.get_refresh_df_context();
        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();
        match get_data(
            &mut ctx,
            TableReference::bare(dataset_name.clone()),
            Arc::clone(&federated),
            refresh.sql.clone(),
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
            TableReference::bare(dataset_name.clone()),
            Arc::clone(&self.federated),
        ) {
            tracing::error!("Unable to register federated table: {e}");
        }

        let acc_dataset_name = format!("accelerated_{dataset_name}");

        if let Err(e) = ctx.register_table(
            TableReference::bare(acc_dataset_name),
            Arc::clone(&self.accelerator),
        ) {
            tracing::error!("Unable to register accelerator table: {e}");
        }
        ctx
    }

    fn get_filter_converter(&self, refresh: &Refresh) -> Option<TimestampFilterConvert> {
        let schema = self.federated.schema();
        let column = refresh.time_column.as_deref().unwrap_or_default();
        let field = schema.column_with_name(column).map(|(_, f)| f).cloned();

        TimestampFilterConvert::create(
            field,
            refresh.time_column.clone(),
            refresh.time_format.clone(),
        )
    }
}

pub(crate) fn get_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{RecordBatch, StringArray},
        datatypes::{DataType, Schema},
    };
    use data_components::arrow::write::MemTable;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio::{sync::mpsc, time::timeout};

    use super::*;

    async fn setup_and_test(
        source_data: Vec<&str>,
        existing_data: Vec<&str>,
        expected_size: usize,
    ) {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "time_in_string",
            DataType::Utf8,
            false,
        )]));
        let arr = StringArray::from(source_data);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let federated = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                .expect("mem table should be created"),
        );

        let arr = StringArray::from(existing_data);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let accelerator = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;

        let refresh = Refresh::new(None, None, None, None, RefreshMode::Full, None);

        let refresher = Refresher::new(
            "test".to_string(),
            federated,
            Arc::new(RwLock::new(refresh)),
            Arc::clone(&accelerator),
        );

        let (trigger, receiver) = mpsc::channel::<()>(1);
        let (ready_sender, is_ready) = oneshot::channel::<()>();
        let acceleration_refresh_mode = AccelerationRefreshMode::Full(receiver);
        let refresh_handle = tokio::spawn(async move {
            refresher
                .start(acceleration_refresh_mode, ready_sender)
                .await;
        });

        trigger
            .send(())
            .await
            .expect("trigger sent correctly to refresh");

        timeout(Duration::from_secs(2), async move {
            is_ready.await.expect("data is received");
        })
        .await
        .expect("finish before the timeout");

        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = accelerator
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        assert_eq!(expected_size, result.first().expect("result").num_rows());

        drop(refresh_handle);
    }

    #[tokio::test]
    async fn test_refresh_full() {
        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;
        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            3,
        )
        .await;
        setup_and_test(
            vec![],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn test_refresh_status_change_to_ready_when_there_are_data_updates() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::set_global_recorder(recorder).expect("recorder is set globally");

        status::update_dataset("test", status::ComponentStatus::Refreshing);

        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;

        let hashmap = snapshotter.snapshot().into_vec();
        let (_, _, _, value) = hashmap.first().expect("at least one metric exists");
        assert_eq!(
            value,
            &DebugValue::Gauge((status::ComponentStatus::Ready as i32).into())
        );
    }

    #[tokio::test]
    async fn test_refresh_status_change_to_ready_when_there_are_no_updates() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::set_global_recorder(recorder).expect("recorder is set globally");

        status::update_dataset("test", status::ComponentStatus::Refreshing);

        setup_and_test(vec![], vec![], 0).await;

        let hashmap = snapshotter.snapshot().into_vec();
        let (_, _, _, value) = hashmap.first().expect("at least one metric exists");
        assert_eq!(
            value,
            &DebugValue::Gauge((status::ComponentStatus::Ready as i32).into())
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch() {
        async fn test(
            source_data: Vec<&str>,
            existing_data: Vec<&str>,
            expected_size: usize,
            message: &str,
        ) {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time_in_string",
                DataType::Utf8,
                false,
            )]));
            let arr = StringArray::from(source_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let federated = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );

            let arr = StringArray::from(existing_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(
                Some("time_in_string".to_string()),
                None,
                None,
                None,
                RefreshMode::Append,
                None,
            );

            let refresher = Refresher::new(
                "test".to_string(),
                federated,
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
            );

            let (trigger, receiver) = mpsc::channel::<()>(1);
            let (ready_sender, is_ready) = oneshot::channel::<()>();
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(Some(receiver));
            let refresh_handle = tokio::spawn(async move {
                refresher
                    .start(acceleration_refresh_mode, ready_sender)
                    .await;
            });
            trigger
                .send(())
                .await
                .expect("trigger sent correctly to refresh");

            timeout(Duration::from_secs(2), async move {
                is_ready.await.expect("data is received");
            })
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec!["2012-12-01T11:11:16Z", "2012-12-01T11:11:17Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            6,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec!["2012-12-01T11:11:15Z", "2012-12-01T11:11:15Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should override existing data",
        )
        .await;
    }
}
