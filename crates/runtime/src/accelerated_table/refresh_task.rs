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

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::compute::{filter_record_batch, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::{
    array::{make_comparator, StructArray, TimestampNanosecondArray},
    datatypes::DataType,
};
use async_stream::stream;
use cache::QueryResultsCacheProvider;
use data_components::delete::get_deletion_provider;
use datafusion::logical_expr::lit;
use futures::{Stream, StreamExt};
use snafu::{OptionExt, ResultExt};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{retry, RetryError};

use crate::{
    accelerated_table::Error,
    component::dataset::acceleration::RefreshMode,
    dataconnector::get_data,
    datafusion::{filter_converter::TimestampFilterConvert, schema, SPICE_RUNTIME_SCHEMA},
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    execution_plan::schema_cast::EnsureSchema,
    object_store_registry::default_runtime_env,
    status,
    timing::TimeMeasurement,
};

use super::refresh::get_timestamp;

use crate::component::dataset::TimeFormat;
use std::time::UNIX_EPOCH;
use std::{cmp::Ordering, sync::Arc, time::SystemTime};
use tokio::sync::{oneshot, RwLock};

use datafusion::{
    dataframe::DataFrame,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{cast, col, Expr, Operator},
    physical_plan::ExecutionPlanProperties,
    prelude::SessionConfig,
    sql::TableReference,
};
use datafusion::{execution::context::SessionContext, physical_plan::collect};

use super::refresh::Refresh;
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

    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.federated.schema()
    }

    pub async fn start_changes_stream(
        &self,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
        ready_sender: Option<oneshot::Sender<()>>,
    ) -> super::Result<()> {
        self.mark_dataset_status(status::ComponentStatus::Refreshing)
            .await;

        let mut stream = Box::pin(self.get_changes_stream());

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
                                .invalidate_for_table(&dataset_name.to_string())
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
                                .invalidate_for_table(&dataset_name.to_string())
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

    pub async fn run(&self) -> super::Result<()> {
        self.mark_dataset_status(status::ComponentStatus::Refreshing)
            .await;

        let dataset_name = self.dataset_name.clone();

        let refresh = self.refresh.read().await;

        let timer = TimeMeasurement::new(
            match refresh.mode {
                RefreshMode::Full => "load_dataset_duration_ms",
                RefreshMode::Append => "append_dataset_duration_ms",
                RefreshMode::Changes => "changes_dataset_duration_ms",
            },
            vec![("dataset", dataset_name.to_string())],
        );

        let get_data_update_result = match refresh.mode {
            RefreshMode::Full => self.get_full_update().await,
            RefreshMode::Append => self.get_incremental_append_update().await,
            RefreshMode::Changes => unreachable!("changes are handled in a separate stream"),
        };

        let (start_time, data_update) = match get_data_update_result {
            Ok((start_time, data_update)) => (start_time, data_update),
            Err(e) => {
                tracing::error!("Failed to load data for dataset {dataset_name}: {e}");
                self.mark_dataset_status(status::ComponentStatus::Error)
                    .await;
                return Err(e);
            }
        };

        drop(timer);

        self.write_data_update(start_time, data_update).await
    }

    pub async fn get_full_or_incremental_append_update(
        &self,
        overwrite_timestamp_in_nano: Option<u128>,
    ) -> super::Result<DataUpdate> {
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

    #[allow(clippy::too_many_lines)]
    async fn write_data_update(
        &self,
        start_time: Option<SystemTime>,
        data_update: DataUpdate,
    ) -> super::Result<()> {
        let dataset_name = self.dataset_name.clone();

        if data_update.data.is_empty()
            || data_update
                .data
                .first()
                .map_or(false, |x| x.columns().is_empty())
        {
            if let Some(start_time) = start_time {
                self.trace_dataset_loaded(start_time, 0, None);
            }

            self.mark_dataset_status(status::ComponentStatus::Ready)
                .await;

            return Ok(());
        };

        let ctx = SessionContext::new();

        if data_update.update_type == UpdateType::Changes {
            tracing::info!("Processing changes for {dataset_name}");
            let Some(deletion_provider) = get_deletion_provider(Arc::clone(&self.accelerator))
            else {
                panic!("Failed to get deletion provider");
            };

            for data_batch in &data_update.data {
                let Some(op_col) = data_batch.column(0).as_any().downcast_ref::<StringArray>()
                else {
                    panic!("Failed to get op column");
                };
                let Some(data_col) = data_batch.column(2).as_any().downcast_ref::<StructArray>()
                else {
                    panic!("Failed to get data column");
                };
                for row in 0..data_batch.num_rows() {
                    let op = op_col.value(row);
                    match op {
                        "d" => {
                            let inner_data: RecordBatch = data_col.slice(row, 1).into();
                            let Some(id_col) = inner_data.column_by_name("id") else {
                                panic!("Failed to get id column");
                            };
                            let Some(id_col) = id_col.as_any().downcast_ref::<Int32Array>() else {
                                panic!("Failed to get id column as Int32Array, {inner_data:?}");
                            };
                            let delete_where_expr = col("id").eq(lit(id_col.value(0)));

                            tracing::info!(
                                "Deleting data for {dataset_name} where {delete_where_expr}"
                            );

                            let ctx = SessionContext::new();
                            let session_state = ctx.state();

                            let Ok(delete_plan) = deletion_provider
                                .delete_from(&session_state, &[delete_where_expr])
                                .await
                            else {
                                panic!("Failed to create delete plan");
                            };

                            if let Err(e) = collect(delete_plan, ctx.task_ctx()).await {
                                panic!("Failed to delete data: {e}");
                            };
                        }
                        "c" | "u" | "r" => {
                            let inner_data: RecordBatch = data_col.slice(row, 1).into();
                            let ctx = SessionContext::new();
                            let session_state = ctx.state();

                            tracing::info!("Inserting data row for {dataset_name}");

                            let Ok(insert_plan) = self
                                .accelerator
                                .insert_into(
                                    &session_state,
                                    Arc::new(DataUpdateExecutionPlan::new(DataUpdate {
                                        schema: inner_data.schema(),
                                        data: vec![inner_data],
                                        update_type: UpdateType::Append,
                                    })),
                                    false,
                                )
                                .await
                            else {
                                panic!("Failed to create insert plan");
                            };

                            if let Err(e) = collect(insert_plan, ctx.task_ctx()).await {
                                panic!("Failed to insert data: {e}");
                            };
                        }
                        _ => panic!("Unknown operation"),
                    }
                }
            }

            return Ok(());
        }

        let overwrite = data_update.update_type == UpdateType::Overwrite;
        match self
            .accelerator
            .insert_into(
                &ctx.state(),
                Arc::new(DataUpdateExecutionPlan::new(data_update.clone())),
                overwrite,
            )
            .await
        {
            Ok(plan) => {
                if let Err(e) = collect(plan, ctx.task_ctx()).await {
                    tracing::error!("Error adding data for {dataset_name}: {e}");
                    self.mark_dataset_status(status::ComponentStatus::Error)
                        .await;
                    return Err(Error::FailedToWriteData { source: e });
                }
                if let Some(start_time) = start_time {
                    let num_rows = data_update
                        .clone()
                        .data
                        .into_iter()
                        .map(|x| x.num_rows())
                        .sum::<usize>();

                    let memory_size = data_update
                        .data
                        .into_iter()
                        .map(|x| x.get_array_memory_size())
                        .sum::<usize>();

                    self.trace_dataset_loaded(start_time, num_rows, Some(memory_size));
                }

                self.mark_dataset_status(status::ComponentStatus::Ready)
                    .await;

                Ok(())
            }
            Err(e) => {
                self.mark_dataset_status(status::ComponentStatus::Error)
                    .await;
                tracing::error!("Error adding data for {dataset_name}: {e}");
                Err(Error::FailedToWriteData { source: e })
            }
        }
    }

    async fn get_full_update(&self) -> super::Result<(Option<SystemTime>, DataUpdate)> {
        let start = SystemTime::now();
        match self.get_full_or_incremental_append_update(None).await {
            Ok(data) => Ok((Some(start), data)),
            Err(e) => Err(e),
        }
    }

    async fn get_incremental_append_update(
        &self,
    ) -> super::Result<(Option<SystemTime>, DataUpdate)> {
        match self.timestamp_nanos_for_append_query().await {
            Ok(timestamp) => {
                let start = SystemTime::now();
                match self.get_full_or_incremental_append_update(timestamp).await {
                    Ok(data) => match self.except_existing_records_from(data).await {
                        Ok(data) => Ok((Some(start), data)),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }
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

    fn trace_dataset_loaded(
        &self,
        start_time: SystemTime,
        num_rows: usize,
        memory_size: Option<usize>,
    ) {
        if let Ok(elapse) = util::humantime_elapsed(start_time) {
            let dataset_name = &self.dataset_name;
            let num_rows = util::pretty_print_number(num_rows);
            let memory_size = match memory_size {
                Some(memory_size) => format!(" ({})", util::human_readable_bytes(memory_size)),
                None => String::new(),
            };

            if self.dataset_name.schema() == Some(SPICE_RUNTIME_SCHEMA) {
                tracing::debug!(
                    "Loaded {num_rows} rows{memory_size} for dataset {dataset_name} in {elapse}.",
                );
            } else {
                tracing::info!(
                    "Loaded {num_rows} rows{memory_size} for dataset {dataset_name} in {elapse}."
                );
            }
        }
    }

    async fn get_data_update(&self, filters: Vec<Expr>) -> super::Result<DataUpdate> {
        let refresh = Arc::clone(&self.refresh);

        let ctx = self.refresh_df_context();
        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(0)).build();

        retry(retry_strategy, || async {
            let mut ctx_clone = ctx.clone();

            let (sql, update_type) = {
                let refresh = refresh.read().await;
                (
                    refresh.sql.clone(),
                    match refresh.mode {
                        RefreshMode::Full => UpdateType::Overwrite,
                        RefreshMode::Append => UpdateType::Append,
                        RefreshMode::Changes => {
                            unreachable!("changes are handled in a separate stream")
                        }
                    },
                )
            };

            let get_data_result = get_data(
                &mut ctx_clone,
                dataset_name.clone(),
                Arc::clone(&federated),
                sql,
                filters.clone(),
            )
            .await;

            match get_data_result {
                Ok(data) => Ok(DataUpdate {
                    schema: data.0,
                    data: data.1,
                    update_type,
                }),
                Err(e) => {
                    tracing::warn!(
                        "Failed to get refresh data for dataset {}: {}",
                        dataset_name,
                        e
                    );
                    let labels = [("dataset", dataset_name.to_string())];
                    metrics::counter!("datasets_acceleration_refresh_errors", &labels).increment(1);

                    if should_retry_df_error(&e) {
                        Err(RetryError::Transient {
                            err: e,
                            retry_after: None,
                        })
                    } else {
                        Err(RetryError::Permanent(e))
                    }
                }
            }
        })
        .await
        .context(super::UnableToGetDataFromConnectorSnafu)
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
    async fn except_existing_records_from(&self, update: DataUpdate) -> super::Result<DataUpdate> {
        let Some(value) = self.timestamp_nanos_for_append_query().await? else {
            return Ok(update);
        };
        let refresh = self.refresh.read().await;
        let Some(filter_converter) = self.get_filter_converter(&refresh) else {
            return Ok(update);
        };

        let update_data = update.clone();
        let Some(update_data) = update_data.data.first() else {
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

        let mut predicates = vec![];
        let mut comparators = vec![];

        let update_struct_array = StructArray::from(update_data.clone());
        for existing in existing_records {
            let existing_array = StructArray::from(existing.clone());
            comparators.push((
                existing.num_rows(),
                make_comparator(
                    &update_struct_array,
                    &existing_array,
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

        filter_record_batch(update_data, &predicates.into())
            .map(|data| DataUpdate {
                schema: update.schema,
                data: vec![data],
                update_type: update.update_type,
            })
            .context(super::FailedToFilterUpdatesSnafu)
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
                Some(TimeFormat::UnixSeconds) | None => {
                    value *= 1_000_000_000;
                }
                Some(TimeFormat::ISO8601) => (),
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
            let labels = [("dataset", self.dataset_name.to_string())];
            metrics::counter!("datasets_acceleration_refresh_errors", &labels).increment(1);
        }

        if status == status::ComponentStatus::Ready {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();

            let mut labels = vec![("dataset", self.dataset_name.to_string())];
            if let Some(sql) = &self.refresh.read().await.sql {
                labels.push(("sql", sql.to_string()));
            };

            metrics::gauge!("datasets_acceleration_last_refresh_time", &labels)
                .set(now.as_secs_f64());
        }
    }
}

fn should_retry_df_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::Context(_, err) => should_retry_df_error(err.as_ref()),
        DataFusionError::SQL(..) | DataFusionError::Plan(..) | DataFusionError::SchemaError(..) => {
            false
        }
        _ => true,
    }
}
