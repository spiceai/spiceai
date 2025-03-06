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

use arrow::compute::{filter_record_batch, SortOptions};
use arrow::{
    array::{make_comparator, RecordBatch, StructArray, TimestampNanosecondArray},
    datatypes::DataType,
};
use arrow_schema::SchemaRef;
use async_stream::stream;
use datafusion::logical_expr::dml::InsertOp;
use datafusion_table_providers::util::retriable_error::{
    check_and_mark_retriable_error, is_retriable_error,
};
use futures::{stream, StreamExt};
use opentelemetry::KeyValue;
use snafu::{OptionExt, ResultExt};
use tracing::{Instrument, Span};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{retry, RetryError};

use crate::datafusion::builder::get_df_default_config;
use crate::datafusion::error::{find_datafusion_root, get_spice_df_error, SpiceExternalError};
use crate::datafusion::is_spice_internal_dataset;
use crate::datafusion::schema::BaseSchema;
use crate::federated_table::FederatedTable;
use crate::timing::MultiTimeMeasurement;
use crate::{
    component::dataset::acceleration::RefreshMode,
    dataconnector::get_data,
    datafusion::{filter_converter::TimestampFilterConvert, schema},
    dataupdate::{DataUpdate, StreamingDataUpdate, UpdateType},
    execution_plan::schema_cast::EnsureSchema,
    object_store_registry::default_runtime_env,
    status,
};

use super::refresh::get_timestamp;
use super::sink::AccelerationSink;
use super::synchronized_table::SynchronizedTable;
use super::{metrics, UnableToCreateMemTableFromUpdateSnafu};

use crate::component::dataset::TimeFormat;
use std::time::UNIX_EPOCH;
use std::{cmp::Ordering, sync::Arc, time::SystemTime};
use tokio::sync::{oneshot, RwLock};

use datafusion::execution::context::SessionContext;
use datafusion::{
    dataframe::DataFrame,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{cast, col, Expr, Operator},
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

use super::refresh::Refresh;

mod changes;
mod streaming_append;

#[derive(Debug, Clone, Default)]
struct RefreshStat {
    pub num_rows: usize,
    pub memory_size: usize,
}

pub struct RefreshTask {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    federated: Arc<FederatedTable>,
    federated_source: Option<String>,
    accelerator: Arc<dyn TableProvider>,
    sink: Arc<RwLock<AccelerationSink>>,
}

impl RefreshTask {
    #[must_use]
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        accelerator: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            accelerator: Arc::clone(&accelerator),
            sink: Arc::new(RwLock::new(AccelerationSink::new(accelerator))),
        }
    }

    /// Subscribes a new acceleration table provider to the existing `AccelerationSink` managed by this `RefreshTask`.
    pub async fn add_synchronized_table(&self, synchronized_table: SynchronizedTable) {
        self.sink
            .write()
            .await
            .add_synchronized_table(synchronized_table);
    }

    pub async fn run(&self, refresh: Refresh) -> super::Result<()> {
        let max_retries = if refresh.refresh_retry_enabled {
            refresh.refresh_retry_max_attempts
        } else {
            Some(0)
        };

        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(max_retries)
            .build();

        let mut spans = vec![];
        let mut parent_span = Span::current();
        for dataset_name in self.get_dataset_names().await {
            let span = tracing::span!(target: "task_history", parent: &parent_span, tracing::Level::INFO, "accelerated_refresh", input = %dataset_name);
            spans.push(span.clone());
            parent_span = span;
        }
        let span = spans
            .iter()
            .last()
            .unwrap_or_else(|| unreachable!("There is always at least one span"));
        retry(retry_strategy, || async {
            match self.run_once(&refresh).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    for label_set in self.get_dataset_label_sets(&refresh.mode).await {
                        metrics::REFRESH_ERRORS.add(1, &label_set);
                    }
                    Err(e)
                }
            }
        })
        .instrument(span.clone())
        .await
        .inspect_err(|e| {
            tracing::error!(
                "Failed to refresh dataset {}: {e}",
                include_source_to_dataset_name(
                    &self.dataset_name,
                    self.federated_source.as_deref()
                )
            );
            for span in &spans {
                tracing::error!(target: "task_history", parent: span, "{e}");
            }
        })
    }

    async fn run_once(&self, refresh: &Refresh) -> Result<(), RetryError<super::Error>> {
        self.mark_dataset_status(refresh.sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;

        let _timer = MultiTimeMeasurement::new(
            match refresh.mode {
                RefreshMode::Disabled => {
                    unreachable!("Refresh cannot be called when acceleration is disabled")
                }
                RefreshMode::Full | RefreshMode::Append => &metrics::REFRESH_DURATION_MS,
                RefreshMode::Changes => unreachable!("changes are handled upstream"),
            },
            self.get_dataset_label_sets(&refresh.mode).await,
        );

        let start_time = SystemTime::now();

        let get_data_update_result = match refresh.mode {
            RefreshMode::Disabled => {
                unreachable!("Refresh cannot be called when acceleration is disabled")
            }
            RefreshMode::Full => {
                self.get_full_or_incremental_append_update(refresh, None)
                    .await
            }
            RefreshMode::Append => self.get_incremental_append_update(refresh).await,
            RefreshMode::Changes => unreachable!("changes are handled upstream"),
        };

        let streaming_data_update = match get_data_update_result {
            Ok(data_update) => data_update,
            Err(e) => {
                self.log_refresh_error(inner_err_from_retry_ref(&e), refresh.sql.as_deref())
                    .await;
                return Err(e);
            }
        };

        self.write_streaming_data_update(
            Some(start_time),
            streaming_data_update,
            refresh.sql.as_deref(),
        )
        .await
        .inspect_err(|e| {
            tracing::warn!(
                "Failed to load data for dataset {}: {}",
                include_source_to_dataset_name(
                    &self.dataset_name,
                    self.federated_source.as_deref()
                ),
                inner_err_from_retry_ref(e)
            );
        })
    }

    async fn write_streaming_data_update(
        &self,
        start_time: Option<SystemTime>,
        data_update: StreamingDataUpdate,
        sql: Option<&str>,
    ) -> Result<(), RetryError<super::Error>> {
        let dataset_name = self.dataset_name.clone();

        let overwrite = if data_update.update_type == UpdateType::Overwrite {
            InsertOp::Overwrite
        } else {
            InsertOp::Append
        };

        let schema = Arc::clone(&data_update.data.schema());

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

        let record_batch_stream = Box::pin(observed_record_batch_stream);
        let sink_lock = self.sink.read().await;
        let sink = &*sink_lock;

        if let Err(e) = sink.insert_into(record_batch_stream, overwrite).await {
            self.mark_dataset_status(sql, status::ComponentStatus::Error)
                .await;
            return Err(e);
        }

        if let (Some(start_time), Ok(refresh_stat)) =
            (start_time, on_written_data_stat_available.try_recv())
        {
            self.trace_dataset_loaded(start_time, refresh_stat.num_rows, refresh_stat.memory_size)
                .await;
        }

        self.mark_dataset_status(sql, status::ComponentStatus::Ready)
            .await;

        Ok(())
    }

    pub async fn get_full_or_incremental_append_update(
        &self,
        refresh: &Refresh,
        overwrite_timestamp_in_nano: Option<u128>,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let dataset_name = self.dataset_name.clone();
        let filter_converter = self.get_filter_converter(refresh);

        if is_spice_internal_dataset(&dataset_name) {
            tracing::debug!("Loading data for dataset {dataset_name}");
        } else {
            tracing::info!("Loading data for dataset {dataset_name}");
        }
        self.runtime_status
            .update_dataset(&dataset_name, status::ComponentStatus::Refreshing);
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

        self.get_data_update(filters, &refresh).await
    }

    async fn write_data_update(
        &self,
        sql: Option<String>,
        start_time: Option<SystemTime>,
        data_update: DataUpdate,
    ) -> super::Result<()> {
        if data_update.data.is_empty()
            || data_update
                .data
                .first()
                .is_some_and(|x| x.columns().is_empty())
        {
            if let Some(start_time) = start_time {
                self.trace_dataset_loaded(start_time, 0, 0).await;
            }

            self.mark_dataset_status(sql.as_deref(), status::ComponentStatus::Ready)
                .await;

            return Ok(());
        };

        let streaming_update = StreamingDataUpdate::try_from(data_update)
            .map_err(find_datafusion_root)
            .context(UnableToCreateMemTableFromUpdateSnafu)?;

        self.write_streaming_data_update(start_time, streaming_update, sql.as_deref())
            .await
            .map_err(inner_err_from_retry)
    }

    async fn get_incremental_append_update(
        &self,
        refresh: &Refresh,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        // If we've gotten to this point and we don't have a time column, skip trying to filter by timestamp.
        //
        // Normally we don't allow this configuration, but it's possible to get here with an accelerated dataset
        // configured with `refresh_mode: full` and the user calls the `POST /v1/datasets/{dataset}/acceleration/refresh` API
        // and overrides the `refresh_mode` to `append`.
        if refresh.time_column.is_none() {
            return self
                .get_full_or_incremental_append_update(refresh, None)
                .await;
        }

        match self
            .timestamp_nanos_for_append_query(refresh)
            .await
            .map_err(RetryError::permanent)
        {
            Ok(timestamp) => match self
                .get_full_or_incremental_append_update(refresh, timestamp)
                .await
            {
                Ok(data) => match self.except_existing_records_from(refresh, data).await {
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

    async fn trace_dataset_loaded(
        &self,
        start_time: SystemTime,
        num_rows: usize,
        memory_size: usize,
    ) {
        if let Ok(elapsed) = util::humantime_elapsed(start_time) {
            let dataset_name = &self.dataset_name;
            let num_rows = util::pretty_print_number(num_rows);
            let memory_size = if memory_size > 0 {
                format!(" ({})", util::human_readable_bytes(memory_size))
            } else {
                String::new()
            };

            if is_spice_internal_dataset(&self.dataset_name) {
                tracing::debug!(
                    "Loaded {num_rows} rows{memory_size} for dataset {dataset_name} in {elapsed}.",
                );
            } else {
                tracing::info!(
                    "Loaded {num_rows} rows{memory_size} for dataset {dataset_name} in {elapsed}."
                );
                for synchronized_table in self.sink.read().await.synchronized_tables() {
                    tracing::info!(
                        "Loaded {num_rows} rows{memory_size} for dataset {} in {elapsed}.",
                        synchronized_table.child_dataset_name()
                    );
                }
            }
        }
    }

    async fn get_data_update(
        &self,
        filters: Vec<Expr>,
        refresh: &Refresh,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let federated_provider = self.federated.table_provider().await;

        let mut ctx = self.refresh_df_context(Arc::clone(&federated_provider));
        let dataset_name = self.dataset_name.clone();

        let update_type = match refresh.mode {
            RefreshMode::Disabled => {
                unreachable!("Refresh cannot be called when acceleration is disabled")
            }
            RefreshMode::Full => UpdateType::Overwrite,
            RefreshMode::Append => UpdateType::Append,
            RefreshMode::Changes => unreachable!("changes are handled upstream"),
        };

        let get_data_result = get_data(
            &mut ctx,
            dataset_name.clone(),
            federated_provider,
            refresh.sql.clone(),
            filters.clone(),
        )
        .await
        .map_err(check_and_mark_retriable_error);

        match get_data_result {
            Ok(data) => Ok(StreamingDataUpdate::new(data, update_type)),
            Err(e) => Err(retry_from_df_error(e)),
        }
    }

    fn get_filter_converter(&self, refresh: &Refresh) -> Option<TimestampFilterConvert> {
        let schema = self.federated.schema();
        let column = refresh.time_column.as_deref().unwrap_or_default();
        let field = schema.column_with_name(column).map(|(_, f)| f).cloned();
        let time_partition_column = refresh.time_partition_column.as_deref();
        let partition_field = schema
            .column_with_name(time_partition_column.unwrap_or_default())
            .map(|(_, f)| f)
            .cloned();

        TimestampFilterConvert::create(
            field,
            refresh.time_column.clone(),
            refresh.time_format,
            partition_field,
            refresh.time_partition_column.clone(),
            refresh.time_partition_format,
        )
    }

    fn refresh_df_context(&self, federated_provider: Arc<dyn TableProvider>) -> SessionContext {
        let ctx =
            SessionContext::new_with_config_rt(get_df_default_config(), default_runtime_env());

        let ctx_state = ctx.state();
        let default_catalog = &ctx_state.config_options().catalog.default_catalog;
        match schema::ensure_schema_exists(&ctx, default_catalog, &self.dataset_name) {
            Ok(()) => (),
            Err(_) => {
                unreachable!("The default catalog should always exist");
            }
        };

        if let Err(e) = ctx.register_table(self.dataset_name.clone(), federated_provider) {
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
    fn max_timestamp_df(
        &self,
        ctx: SessionContext,
        column: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let expr = cast(
            col(format!(r#""{column}""#)),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        )
        .alias("a");

        self.accelerator_df(&ctx)?
            .select(vec![expr])?
            .sort(vec![col("a").sort(false, false)])?
            .limit(0, Some(1))
    }

    fn accelerator_df(&self, ctx: &SessionContext) -> Result<DataFrame, DataFusionError> {
        // Records in the accelerator table are already filtered so we don't need to apply refresh SQL
        ctx.read_table(Arc::new(EnsureSchema::new(Arc::clone(&self.accelerator))))
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn except_existing_records_from(
        &self,
        refresh: &Refresh,
        mut update: StreamingDataUpdate,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let Some(value) = self.timestamp_nanos_for_append_query(refresh).await? else {
            return Ok(update);
        };
        let Some(filter_converter) = self.get_filter_converter(refresh) else {
            return Ok(update);
        };

        let federated_provider = self.federated.table_provider().await;

        let existing_records = self
            .accelerator_df(&self.refresh_df_context(Arc::clone(&federated_provider)))
            .map_err(find_datafusion_root)
            .context(super::UnableToScanTableProviderSnafu)?
            .filter(filter_converter.convert(value, Operator::Gt))
            .map_err(find_datafusion_root)
            .context(super::UnableToScanTableProviderSnafu)?
            .collect()
            .await
            .map_err(find_datafusion_root)
            .context(super::UnableToScanTableProviderSnafu)?;

        let filter_schema = BaseSchema::get_schema(&federated_provider);
        let update_type = update.update_type.clone();

        let filtered_data = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&update.data.schema()),
            {
                stream! {
                    while let Some(batch) = update.data.next().await {
                        let batch = filter_records(&batch?, &existing_records, &filter_schema);
                        yield batch.map_err(|e| { DataFusionError::External(Box::new(e)) });
                    }
                }
            },
        ));

        Ok(StreamingDataUpdate::new(filtered_data, update_type))
    }

    #[allow(clippy::cast_sign_loss)]
    async fn timestamp_nanos_for_append_query(
        &self,
        refresh: &Refresh,
    ) -> super::Result<Option<u128>> {
        let federated = self.federated.table_provider().await;
        let ctx = self.refresh_df_context(federated);

        refresh
            .validate_time_format(self.dataset_name.to_string(), &self.accelerator.schema())
            .context(super::InvalidTimeColumnTimeFormatSnafu)?;

        let column = refresh
            .time_column
            .clone()
            .context(super::FailedToFindLatestTimestampSnafu {
            reason:
                "Failed to get the latest timestamp.\nThe `time_column` parameter must be specified.",
        })?;

        let df = self
            .max_timestamp_df(ctx, &column)
            .map_err(find_datafusion_root)
            .context(super::UnableToScanTableProviderSnafu)?;
        let result = &df
            .collect()
            .await
            .map_err(find_datafusion_root)
            .context(super::FailedToQueryLatestTimestampSnafu)?;

        let Some(result) = result.first() else {
            return Ok(None);
        };

        let array = result.column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .context(super::FailedToFindLatestTimestampSnafu {
                reason: "Failed to get the latest timestamp during incremental appending.\nFailed to convert the value of the time column to a timestamp. Verify the column is a timestamp.",
            })?;

        if array.is_empty() {
            return Ok(None);
        }

        let mut value = array.value(0) as u128;

        let schema = &self.accelerator.schema();
        let Ok(accelerated_field) = schema.field_with_name(&column) else {
            return Err(super::Error::FailedToFindLatestTimestamp {
                reason: "Failed to get the latest timestamp.\nThe `time_column` parameter must be specified."
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
                Some(
                    TimeFormat::ISO8601
                    | TimeFormat::Timestamp
                    | TimeFormat::Timestamptz
                    | TimeFormat::Date,
                )
                | None => unreachable!("refresh.validate_time_format should've returned error"),
            }
        };

        let refresh_append_value = refresh
            .append_overlap
            .map(|f| f.as_nanos())
            .unwrap_or_default();

        if refresh_append_value > value {
            Ok(Some(0))
        } else {
            Ok(Some(value - refresh_append_value))
        }
    }

    async fn get_dataset_names(&self) -> Vec<TableReference> {
        let mut dataset_names = vec![self.dataset_name.clone()];
        for synchronized_table in self.sink.read().await.synchronized_tables() {
            dataset_names.push(synchronized_table.child_dataset_name());
        }
        dataset_names
    }

    async fn get_dataset_label_sets(&self, mode: &RefreshMode) -> Vec<Vec<KeyValue>> {
        let dataset_names = self.get_dataset_names().await;
        dataset_names
            .into_iter()
            .map(|name| {
                let mut label_set = vec![KeyValue::new("dataset", name.to_string())];
                match mode {
                    RefreshMode::Full => label_set.push(KeyValue::new("mode", "full".to_string())),
                    RefreshMode::Append => {
                        label_set.push(KeyValue::new("mode", "append".to_string()));
                    }
                    _ => (),
                }
                label_set
            })
            .collect()
    }

    async fn mark_dataset_status(&self, sql: Option<&str>, status: status::ComponentStatus) {
        let dataset_names = self.get_dataset_names().await;

        for dataset_name in dataset_names {
            self.runtime_status.update_dataset(&dataset_name, status);

            if status == status::ComponentStatus::Error {
                let labels = [KeyValue::new("dataset", dataset_name.to_string())];
                metrics::REFRESH_ERRORS.add(1, &labels);
            }

            if status == status::ComponentStatus::Ready {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();

                let mut labels = vec![KeyValue::new("dataset", dataset_name.to_string())];
                if let Some(sql) = sql {
                    labels.push(KeyValue::new("sql", sql.to_string()));
                };

                metrics::LAST_REFRESH_TIME_MS.record(now.as_secs_f64() * 1000.0, &labels);
            }
        }
    }

    async fn log_refresh_error(&self, error: &super::Error, refresh_sql: Option<&str>) {
        if let super::Error::UnableToGetDataFromConnector { source } = error {
            if let Some(SpiceExternalError::AccelerationNotReady { dataset_name }) =
                get_spice_df_error(source)
            {
                tracing::warn!(
                    "Dataset {} is waiting for {dataset_name} to finish loading initial acceleration.",
                    self.dataset_name
                );
                self.mark_dataset_status(refresh_sql, status::ComponentStatus::Initializing)
                    .await;
                return;
            }
        }

        // For all errors that result from calling DataFusion, check if they are due to the task being cancelled and ignore them
        match error {
            super::Error::UnableToGetDataFromConnector { source }
            | super::Error::FailedToRefreshDataset { source }
            | super::Error::UnableToScanTableProvider { source }
            | super::Error::UnableToCreateMemTableFromUpdate { source }
            | super::Error::FailedToQueryLatestTimestamp { source }
            | super::Error::FailedToWriteData { source } => {
                // Match against an Internal error with the message "Non Panic Task error":
                // <https://github.com/apache/datafusion/blob/f6c92fecb23c927bdc6a9feb058f03a2fb61d63f/datafusion/physical-plan/src/stream.rs#L132>
                if let DataFusionError::Internal(msg) = &source {
                    if msg.contains("Non Panic Task error") && msg.contains("was cancelled") {
                        tracing::debug!(
                            "Ignoring DataFusion error due to task cancellation: {source}"
                        );
                        return;
                    }
                }
            }
            _ => (),
        }

        tracing::warn!(
            "Failed to load data for dataset {}: {error}",
            include_source_to_dataset_name(&self.dataset_name, self.federated_source.as_deref()),
        );
        self.mark_dataset_status(refresh_sql, status::ComponentStatus::Error)
            .await;
    }
}

fn include_source_to_dataset_name(dataset_name: &TableReference, source: Option<&str>) -> String {
    match source {
        Some(source) => format!("{dataset_name} ({source})"),
        None => dataset_name.to_string(),
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

pub(crate) fn retry_from_df_error(error: DataFusionError) -> RetryError<super::Error> {
    if is_retriable_error(&error) {
        return RetryError::transient(super::Error::UnableToGetDataFromConnector {
            source: find_datafusion_root(error),
        });
    }
    RetryError::permanent(super::Error::FailedToRefreshDataset {
        source: find_datafusion_root(error),
    })
}

fn inner_err_from_retry(error: RetryError<super::Error>) -> super::Error {
    match error {
        RetryError::Permanent(inner_err) | RetryError::Transient { err: inner_err, .. } => {
            inner_err
        }
    }
}

fn inner_err_from_retry_ref(error: &RetryError<super::Error>) -> &super::Error {
    match error {
        RetryError::Permanent(inner_err) | RetryError::Transient { err: inner_err, .. } => {
            inner_err
        }
    }
}
