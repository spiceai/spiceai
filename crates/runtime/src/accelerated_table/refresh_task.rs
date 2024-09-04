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

use arrow::compute::{filter_record_batch, SortOptions};
use arrow::{
    array::{make_comparator, RecordBatch, StructArray, TimestampNanosecondArray},
    datatypes::DataType,
};
use arrow_schema::SchemaRef;
use async_stream::stream;
use cache::QueryResultsCacheProvider;
use datafusion_table_providers::util::retriable_error::{
    check_and_mark_retriable_error, is_retriable_error,
};
use futures::{stream, Stream, StreamExt};
use opentelemetry::Key;
use snafu::{OptionExt, ResultExt};
use tracing::Instrument;
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{retry, RetryError};

use crate::datafusion::schema::BaseSchema;
use crate::dataupdate::StreamingDataUpdateExecutionPlan;
use crate::{
    component::dataset::acceleration::RefreshMode,
    dataconnector::get_data,
    datafusion::{filter_converter::TimestampFilterConvert, schema, SPICE_RUNTIME_SCHEMA},
    dataupdate::{DataUpdate, StreamingDataUpdate, UpdateType},
    execution_plan::schema_cast::EnsureSchema,
    object_store_registry::default_runtime_env,
    status,
    timing::TimeMeasurement,
};

use super::refresh::{get_timestamp, RefreshOverrides};
use super::{metrics, UnableToCreateMemTableFromUpdateSnafu};

use crate::component::dataset::TimeFormat;
use std::time::UNIX_EPOCH;
use std::{cmp::Ordering, sync::Arc, time::SystemTime};
use tokio::sync::{oneshot, RwLock};

use datafusion::{
    dataframe::DataFrame,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{cast, col, Expr, Operator},
    physical_plan::{stream::RecordBatchStreamAdapter, ExecutionPlanProperties},
    prelude::SessionConfig,
    sql::TableReference,
};
use datafusion::{execution::context::SessionContext, physical_plan::collect};

use super::refresh::Refresh;

mod changes;

#[derive(Debug, Clone, Default)]
struct RefreshStat {
    pub num_rows: usize,
    pub memory_size: usize,
}

pub struct RefreshTask {
    dataset_name: TableReference,
    federated: Arc<dyn TableProvider>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
}

impl RefreshTask {
    #[must_use]
    pub fn new(
        dataset_name: TableReference,
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

    pub async fn start_streaming_append(
        &self,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
        ready_sender: Option<oneshot::Sender<()>>,
    ) -> super::Result<()> {
        self.mark_dataset_status(status::ComponentStatus::Refreshing)
            .await;

        let mut stream = Box::pin(self.get_append_stream());

        let dataset_name = self.dataset_name.clone();

        let mut ready_sender = ready_sender;

        while let Some(update) = stream.next().await {
            match update {
                Ok((start_time, data_update)) => {
                    // write_data_update updates dataset status and logs errors so we don't do this here
                    if self
                        .write_data_update(start_time, data_update)
                        .await
                        .is_ok()
                    {
                        if let Some(ready_sender) = ready_sender.take() {
                            ready_sender.send(()).ok();
                        }

                        if let Some(cache_provider) = &cache_provider {
                            if let Err(e) = cache_provider
                                .invalidate_for_table(dataset_name.clone())
                                .await
                            {
                                tracing::error!(
                                    "Failed to invalidate cached results for dataset {}: {e}",
                                    &dataset_name.to_string()
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error getting update for dataset {dataset_name}: {e}");
                    self.mark_dataset_status(status::ComponentStatus::Error)
                        .await;
                }
            }
        }

        Ok(())
    }

    pub async fn run(&self, overrides: Option<RefreshOverrides>) -> super::Result<()> {
        if let Some(overrides) = overrides {
            println!("Hello there! Here were your overrides: {:#?}", overrides);
        }
        let (refresh_retry_enabled, refresh_retry_max_attempts) = {
            let refresh = self.refresh.read().await;
            (
                refresh.refresh_retry_enabled,
                refresh.refresh_retry_max_attempts,
            )
        };

        let max_retries = if refresh_retry_enabled {
            refresh_retry_max_attempts
        } else {
            Some(0)
        };

        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(max_retries)
            .build();

        let dataset_name = self.dataset_name.clone();

        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "accelerated_refresh", input = %dataset_name);

        retry(retry_strategy, || async {
            self.run_once().await.map_err(|err| {
                let labels = [Key::from_static_str("dataset").string(dataset_name.to_string())];
                metrics::REFRESH_ERRORS.add(1, &labels);
                err
            })
        })
        .instrument(span.clone())
        .await
        .map_err(|e| {
            tracing::error!("Failed to refresh dataset {}: {e}", dataset_name);
            tracing::error!(target: "task_history", parent: &span, "{e}");
            e
        })
    }

    async fn run_once(&self) -> Result<(), RetryError<super::Error>> {
        self.mark_dataset_status(status::ComponentStatus::Refreshing)
            .await;

        let dataset_name = self.dataset_name.clone();

        let mode = self.refresh.read().await.mode;

        let _timer = TimeMeasurement::new(
            match mode {
                RefreshMode::Disabled => {
                    unreachable!("Refresh cannot be called when acceleration is disabled")
                }
                RefreshMode::Full => &metrics::LOAD_DURATION_MS,
                RefreshMode::Append => &metrics::APPEND_DURATION_MS,
                RefreshMode::Changes => unreachable!("changes are handled upstream"),
            },
            vec![Key::from_static_str("dataset").string(dataset_name.to_string())],
        );

        let start_time = SystemTime::now();

        let get_data_update_result = match mode {
            RefreshMode::Disabled => {
                unreachable!("Refresh cannot be called when acceleration is disabled")
            }
            RefreshMode::Full => self.get_full_update().await,
            RefreshMode::Append => self.get_incremental_append_update().await,
            RefreshMode::Changes => unreachable!("changes are handled upstream"),
        };

        let streaming_data_update = match get_data_update_result {
            Ok(data_update) => data_update,
            Err(e) => {
                tracing::warn!("Failed to load data for dataset {dataset_name}: {e}");
                self.mark_dataset_status(status::ComponentStatus::Error)
                    .await;
                return Err(e);
            }
        };

        self.write_streaming_data_update(Some(start_time), streaming_data_update)
            .await
    }

    async fn write_streaming_data_update(
        &self,
        start_time: Option<SystemTime>,
        data_update: StreamingDataUpdate,
    ) -> Result<(), RetryError<super::Error>> {
        let dataset_name = self.dataset_name.clone();

        let overwrite = data_update.update_type == UpdateType::Overwrite;

        let schema = Arc::clone(&data_update.schema);

        let (notify_written_data_stat_available, mut on_written_data_stat_available) =
            oneshot::channel::<RefreshStat>();

        let observed_record_batch_stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::unfold(
                (
                    data_update.data,
                    RefreshStat::default(),
                    dataset_name.to_string(),
                    notify_written_data_stat_available,
                ),
                move |(mut stream, mut stat, ds_name, notify_refresh_stat_available)| async move {
                    if let Some(batch) = stream.next().await {
                        match batch {
                            Ok(batch) => {
                                tracing::trace!(
                                    "[refresh] Received {} rows for dataset: {}",
                                    batch.num_rows(),
                                    ds_name
                                );
                                stat.num_rows += batch.num_rows();
                                stat.memory_size += batch.get_array_memory_size();
                                Some((
                                    Ok(batch),
                                    (stream, stat, ds_name, notify_refresh_stat_available),
                                ))
                            }
                            Err(err) => Some((
                                Err(err),
                                (stream, stat, ds_name, notify_refresh_stat_available),
                            )),
                        }
                    } else {
                        if notify_refresh_stat_available.send(stat).is_err() {
                            tracing::error!("Failed to provide stats on the amount of data written to the dataset: {ds_name}");
                        }
                        None
                    }
                },
            ),
        );

        let ctx = SessionContext::new();

        let insertion_plan = match self
            .accelerator
            .insert_into(
                &ctx.state(),
                Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(
                    observed_record_batch_stream,
                ))),
                overwrite,
            )
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                self.mark_dataset_status(status::ComponentStatus::Error)
                    .await;
                // Should not retry if we are unable to create execution plan to insert data
                return Err(RetryError::permanent(super::Error::FailedToWriteData {
                    source: e,
                }));
            }
        };

        if let Err(e) = collect(insertion_plan, ctx.task_ctx()).await {
            tracing::warn!("Failed to update dataset {dataset_name}: {e}");
            self.mark_dataset_status(status::ComponentStatus::Error)
                .await;
            return Err(retry_from_df_error(e));
        }

        if let (Some(start_time), Ok(refresh_stat)) =
            (start_time, on_written_data_stat_available.try_recv())
        {
            self.trace_dataset_loaded(start_time, refresh_stat.num_rows, refresh_stat.memory_size);
        }

        self.mark_dataset_status(status::ComponentStatus::Ready)
            .await;

        Ok(())
    }

    pub async fn get_full_or_incremental_append_update(
        &self,
        overwrite_timestamp_in_nano: Option<u128>,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let dataset_name = self.dataset_name.clone();
        let refresh = self.refresh.read().await;
        let filter_converter = self.get_filter_converter(&refresh);

        if dataset_name.schema() == Some(SPICE_RUNTIME_SCHEMA) {
            tracing::debug!("Loading data for dataset {dataset_name}");
        } else {
            tracing::info!("Loading data for dataset {dataset_name}");
        }
        status::update_dataset(&dataset_name, status::ComponentStatus::Refreshing);
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

        self.get_data_update(filters).await
    }

    async fn write_data_update(
        &self,
        start_time: Option<SystemTime>,
        data_update: DataUpdate,
    ) -> super::Result<()> {
        if data_update.data.is_empty()
            || data_update
                .data
                .first()
                .map_or(false, |x| x.columns().is_empty())
        {
            if let Some(start_time) = start_time {
                self.trace_dataset_loaded(start_time, 0, 0);
            }

            self.mark_dataset_status(status::ComponentStatus::Ready)
                .await;

            return Ok(());
        };

        let streaming_update = StreamingDataUpdate::try_from(data_update)
            .context(UnableToCreateMemTableFromUpdateSnafu)?;

        self.write_streaming_data_update(start_time, streaming_update)
            .await
            .map_err(inner_err_from_retry)
    }

    async fn get_full_update(&self) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        match self.get_full_or_incremental_append_update(None).await {
            Ok(data) => Ok(data),
            Err(e) => Err(e),
        }
    }

    async fn get_incremental_append_update(
        &self,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        match self
            .timestamp_nanos_for_append_query()
            .await
            .map_err(RetryError::permanent)
        {
            Ok(timestamp) => match self.get_full_or_incremental_append_update(timestamp).await {
                Ok(data) => match self.except_existing_records_from(data).await {
                    Ok(data) => Ok(data),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            },
            Err(e) => {
                tracing::error!("No latest timestamp is found: {e}");
                Err(e)
            }
        }
    }

    fn get_append_stream(
        &self,
    ) -> impl Stream<Item = super::Result<(Option<SystemTime>, DataUpdate)>> {
        let ctx = self.refresh_df_context();
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
                        yield Ok((None, DataUpdate {
                            schema: Arc::clone(&schema),
                            data: vec![batch],
                            update_type: UpdateType::Append,
                        }));
                    }
                    Some(Err(e)) => {
                        tracing::error!("Error reading data for dataset {dataset_name}: {e}");
                        yield Err(super::Error::UnableToScanTableProvider { source: e });
                    }
                    None => {
                        tracing::warn!("Append stream ended for dataset {dataset_name}");
                        break;
                    },
                }
            }
        }
    }

    fn trace_dataset_loaded(&self, start_time: SystemTime, num_rows: usize, memory_size: usize) {
        if let Ok(elapsed) = util::humantime_elapsed(start_time) {
            let dataset_name = &self.dataset_name;
            let num_rows = util::pretty_print_number(num_rows);
            let memory_size = if memory_size > 0 {
                format!(" ({})", util::human_readable_bytes(memory_size))
            } else {
                String::new()
            };

            if self.dataset_name.schema() == Some(SPICE_RUNTIME_SCHEMA) {
                tracing::debug!(
                    "Loaded {num_rows} rows{memory_size} for dataset {dataset_name} in {elapsed}.",
                );
            } else {
                tracing::info!(
                    "Loaded {num_rows} rows{memory_size} for dataset {dataset_name} in {elapsed}."
                );
            }
        }
    }

    async fn get_data_update(
        &self,
        filters: Vec<Expr>,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let refresh = Arc::clone(&self.refresh);

        let mut ctx = self.refresh_df_context();
        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();

        let (sql, update_type) = {
            let refresh = refresh.read().await;
            (
                refresh.sql.clone(),
                match refresh.mode {
                    RefreshMode::Disabled => {
                        unreachable!("Refresh cannot be called when acceleration is disabled")
                    }
                    RefreshMode::Full => UpdateType::Overwrite,
                    RefreshMode::Append => UpdateType::Append,
                    RefreshMode::Changes => unreachable!("changes are handled upstream"),
                },
            )
        };

        let get_data_result = get_data(
            &mut ctx,
            dataset_name.clone(),
            Arc::clone(&federated),
            sql,
            filters.clone(),
        )
        .await
        .map_err(check_and_mark_retriable_error);

        match get_data_result {
            Ok(data) => Ok(StreamingDataUpdate::new(data.0, data.1, update_type)),
            Err(e) => Err(retry_from_df_error(e)),
        }
    }

    fn get_filter_converter(&self, refresh: &Refresh) -> Option<TimestampFilterConvert> {
        let schema = self.federated.schema();
        let column = refresh.time_column.as_deref().unwrap_or_default();
        let field = schema.column_with_name(column).map(|(_, f)| f).cloned();

        TimestampFilterConvert::create(field, refresh.time_column.clone(), refresh.time_format)
    }

    fn refresh_df_context(&self) -> SessionContext {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            default_runtime_env(),
        );

        let ctx_state = ctx.state();
        let default_catalog = &ctx_state.config_options().catalog.default_catalog;
        match schema::ensure_schema_exists(&ctx, default_catalog, &self.dataset_name) {
            Ok(()) => (),
            Err(_) => {
                unreachable!("The default catalog should always exist");
            }
        };

        if let Err(e) = ctx.register_table(self.dataset_name.clone(), Arc::clone(&self.federated)) {
            tracing::error!("Unable to register federated table: {e}");
        }

        let mut acc_dataset_name = String::with_capacity(
            self.dataset_name.table().len() + self.dataset_name.schema().map_or(0, str::len),
        );

        if let Some(schema) = self.dataset_name.schema() {
            acc_dataset_name.push_str(schema);
        }

        acc_dataset_name.push_str("accelerated_");
        acc_dataset_name.push_str(self.dataset_name.table());

        if let Err(e) = ctx.register_table(
            TableReference::parse_str(&acc_dataset_name),
            Arc::new(EnsureSchema::new(Arc::clone(&self.accelerator))),
        ) {
            tracing::error!("Unable to register accelerator table: {e}");
        }
        ctx
    }

    #[allow(clippy::needless_pass_by_value)]
    async fn max_timestamp_df(
        &self,
        ctx: SessionContext,
        column: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let expr = cast(
            col(format!(r#""{column}""#)),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        )
        .alias("a");

        self.accelerator_df(ctx)
            .await?
            .select(vec![expr])?
            .sort(vec![col("a").sort(false, false)])?
            .limit(0, Some(1))
    }

    async fn accelerator_df(&self, ctx: SessionContext) -> Result<DataFrame, DataFusionError> {
        if let Some(sql) = &self.refresh.read().await.sql {
            ctx.sql(sql.as_str()).await
        } else {
            ctx.read_table(Arc::new(EnsureSchema::new(Arc::clone(&self.accelerator))))
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn except_existing_records_from(
        &self,
        mut update: StreamingDataUpdate,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let Some(value) = self.timestamp_nanos_for_append_query().await? else {
            return Ok(update);
        };
        let refresh = self.refresh.read().await;
        let Some(filter_converter) = self.get_filter_converter(&refresh) else {
            return Ok(update);
        };

        let existing_records = self
            .accelerator_df(self.refresh_df_context())
            .await
            .context(super::UnableToScanTableProviderSnafu)?
            .filter(filter_converter.convert(value, Operator::Gt))
            .context(super::UnableToScanTableProviderSnafu)?
            .collect()
            .await
            .context(super::UnableToScanTableProviderSnafu)?;

        let filter_schema = BaseSchema::get_schema(&self.federated);
        let schema = Arc::clone(&update.schema);
        let update_type = update.update_type.clone();

        let filtered_data = Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&update.schema), {
            stream! {
                while let Some(batch) = update.data.next().await {
                    let batch = filter_records(&batch?, &existing_records, &filter_schema);
                    yield batch.map_err(|e| { DataFusionError::External(Box::new(e)) });
                }
            }
        }));

        Ok(StreamingDataUpdate::new(schema, filtered_data, update_type))
    }

    async fn refresh_append_overlap_nanos(&self) -> u128 {
        self.refresh
            .read()
            .await
            .append_overlap
            .map(|f| f.as_nanos())
            .unwrap_or_default()
    }

    #[allow(clippy::cast_sign_loss)]
    async fn timestamp_nanos_for_append_query(&self) -> super::Result<Option<u128>> {
        let ctx = self.refresh_df_context();
        let refresh = self.refresh.read().await;

        refresh
            .validate_time_format(self.dataset_name.to_string(), &self.accelerator.schema())
            .context(super::InvalidTimeColumnTimeFormatSnafu)?;

        let column =
            refresh
                .time_column
                .clone()
                .context(super::FailedToFindLatestTimestampSnafu {
                    reason: "Failed to get latest timestamp due to time column not specified",
                })?;

        let df = self
            .max_timestamp_df(ctx, &column)
            .await
            .context(super::UnableToScanTableProviderSnafu)?;
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
            match refresh.time_format {
                Some(TimeFormat::UnixMillis) => {
                    value *= 1_000_000;
                }
                Some(TimeFormat::UnixSeconds) => {
                    value *= 1_000_000_000;
                }
                Some(TimeFormat::ISO8601 | TimeFormat::Timestamp | TimeFormat::Timestamptz)
                | None => unreachable!("refresh.validate_time_format should've returned error"),
            }
        };

        let refresh_append_value = self.refresh_append_overlap_nanos().await;

        if refresh_append_value > value {
            Ok(Some(0))
        } else {
            Ok(Some(value - refresh_append_value))
        }
    }

    async fn mark_dataset_status(&self, status: status::ComponentStatus) {
        status::update_dataset(&self.dataset_name, status);

        if status == status::ComponentStatus::Error {
            let labels = [Key::from_static_str("dataset").string(self.dataset_name.to_string())];
            metrics::REFRESH_ERRORS.add(1, &labels);
        }

        if status == status::ComponentStatus::Ready {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();

            let mut labels =
                vec![Key::from_static_str("dataset").string(self.dataset_name.to_string())];
            if let Some(sql) = &self.refresh.read().await.sql {
                labels.push(Key::from_static_str("sql").string(sql.to_string()));
            };

            metrics::LAST_REFRESH_TIME.record(now.as_secs_f64(), &labels);
        }
    }
}

fn filter_records(
    update_data: &RecordBatch,
    existing_records: &Vec<RecordBatch>,
    filter_schema: &SchemaRef,
) -> super::Result<RecordBatch> {
    let mut predicates = vec![];
    let mut comparators = vec![];

    let update_struct_array = StructArray::from(
        filter_schema
            .fields()
            .iter()
            .map(|field| {
                let column_idx = update_data
                    .schema()
                    .index_of(field.name())
                    .context(super::FailedToFilterUpdatesSnafu)?;
                Ok((Arc::clone(field), update_data.column(column_idx).to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );

    for existing in existing_records {
        let existing_struct_array = StructArray::from(
            filter_schema
                .fields()
                .iter()
                .map(|field| {
                    let column_idx = existing
                        .schema()
                        .index_of(field.name())
                        .context(super::FailedToFilterUpdatesSnafu)?;
                    Ok((Arc::clone(field), existing.column(column_idx).to_owned()))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );

        comparators.push((
            existing.num_rows(),
            make_comparator(
                &update_struct_array,
                &existing_struct_array,
                SortOptions::default(),
            )
            .context(super::FailedToFilterUpdatesSnafu)?,
        ));
    }

    for i in 0..update_data.num_rows() {
        let mut not_matched = true;
        for (size, comparator) in &comparators {
            if (0..*size).any(|j| comparator(i, j) == Ordering::Equal) {
                not_matched = false;
                break;
            }
        }

        predicates.push(not_matched);
    }

    filter_record_batch(update_data, &predicates.into()).context(super::FailedToFilterUpdatesSnafu)
}

fn retry_from_df_error(error: DataFusionError) -> RetryError<super::Error> {
    if is_retriable_error(&error) {
        return RetryError::transient(super::Error::UnableToGetDataFromConnector { source: error });
    }
    RetryError::permanent(super::Error::FailedToRefreshDataset { source: error })
}

fn inner_err_from_retry(error: RetryError<super::Error>) -> super::Error {
    match error {
        RetryError::Permanent(inner_err) | RetryError::Transient { err: inner_err, .. } => {
            inner_err
        }
    }
}
