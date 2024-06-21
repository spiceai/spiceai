use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::TimeFormat;
use crate::datafusion::filter_converter::TimestampFilterConvert;
use crate::datafusion::{schema, SPICE_RUNTIME_SCHEMA};
use crate::execution_plan::schema_cast::EnsureSchema;
use crate::object_store_registry::default_runtime_env;
use crate::{
    dataconnector::get_data,
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    status,
    timing::TimeMeasurement,
};
use arrow::array::{
    make_comparator, Int64Array, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::compute::{filter_record_batch, SortOptions};
use arrow::datatypes::{DataType, SchemaRef};
use async_stream::stream;
use cache::QueryResultsCacheProvider;
use data_components::delete::get_deletion_provider;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::logical_expr::{cast, col, lit, Expr, Operator};
use datafusion::physical_plan::{collect, ExecutionPlanProperties};
use datafusion::prelude::DataFrame;
use datafusion::{datasource::TableProvider, execution::context::SessionContext};
use futures::Stream;
use futures::{stream::BoxStream, StreamExt};
use snafu::prelude::*;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone, Debug)]
pub struct Refresh {
    pub(crate) time_column: Option<String>,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) check_interval: Option<Duration>,
    pub(crate) sql: Option<String>,
    pub(crate) mode: RefreshMode,
    pub(crate) period: Option<Duration>,
    pub(crate) append_overlap: Option<Duration>,
}

impl Refresh {
    #[allow(clippy::needless_pass_by_value)]
    #[must_use]
    pub fn new(
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        check_interval: Option<Duration>,
        sql: Option<String>,
        mode: RefreshMode,
        period: Option<Duration>,
        append_overlap: Option<Duration>,
    ) -> Self {
        Self {
            time_column,
            time_format,
            check_interval,
            sql,
            mode,
            period,
            append_overlap,
        }
    }
}

impl Default for Refresh {
    fn default() -> Self {
        Self {
            time_column: None,
            time_format: None,
            check_interval: None,
            sql: None,
            mode: RefreshMode::Full,
            period: None,
            append_overlap: None,
        }
    }
}

pub(crate) enum AccelerationRefreshMode {
    Full(Receiver<()>),
    Append(Option<Receiver<()>>),
    Changes,
}

pub struct Refresher {
    dataset_name: TableReference,
    federated: Arc<dyn TableProvider>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
    cache_provider: Option<Arc<QueryResultsCacheProvider>>,
}

impl Refresher {
    pub(crate) fn new(
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
            cache_provider: None,
        }
    }

    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.accelerator.schema()
    }

    pub fn cache_provider(
        &mut self,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
    ) -> &mut Self {
        self.cache_provider = cache_provider;
        self
    }

    #[allow(clippy::too_many_lines)]
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
                Some(result) => {
                    let (start_time, data_update) = match result {
                        Ok((start_time, data_update)) => (start_time, data_update),
                        Err(e) => {
                            tracing::error!("Error getting update for dataset {dataset_name}: {e}");
                            self.mark_dataset_status(status::ComponentStatus::Error);
                            continue;
                        }
                    };

                    if data_update.data.is_empty()
                        || data_update
                            .data
                            .first()
                            .map_or(false, |x| x.columns().is_empty())
                    {
                        if let Some(start_time) = start_time {
                            self.trace_dataset_loaded(start_time, 0, None);
                        }
                        self.notify_refresh_done(&mut ready_sender, status::ComponentStatus::Ready)
                            .await;
                        continue;
                    };

                    if data_update.update_type == UpdateType::Changes {
                        tracing::info!("Processing changes for {dataset_name}");
                        let Some(deletion_provider) =
                            get_deletion_provider(Arc::clone(&self.accelerator))
                        else {
                            panic!("Failed to get deletion provider");
                        };

                        for data_batch in &data_update.data {
                            let Some(op_col) =
                                data_batch.column(0).as_any().downcast_ref::<StringArray>()
                            else {
                                panic!("Failed to get op column");
                            };
                            let Some(data_col) =
                                data_batch.column(3).as_any().downcast_ref::<StructArray>()
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
                                        let Some(id_col) =
                                            id_col.as_any().downcast_ref::<Int64Array>()
                                        else {
                                            panic!("Failed to get id column as Int64Array");
                                        };
                                        let delete_where_expr = col("id").eq(lit(id_col.value(0)));

                                        tracing::info!("Deleting data for {dataset_name} where {delete_where_expr}");

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
                                                Arc::new(DataUpdateExecutionPlan::new(
                                                    DataUpdate {
                                                        schema: inner_data.schema(),
                                                        data: vec![inner_data],
                                                        update_type: UpdateType::Append,
                                                    },
                                                )),
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
                                self.mark_dataset_status(status::ComponentStatus::Error);
                            } else {
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

                                    self.trace_dataset_loaded(
                                        start_time,
                                        num_rows,
                                        Some(memory_size),
                                    );

                                    if let Some(cache_provider) = &self.cache_provider {
                                        if let Err(e) = cache_provider
                                            .invalidate_for_table(&dataset_name.to_string())
                                            .await
                                        {
                                            tracing::error!("Failed to invalidate cached results for dataset {}: {e}", &dataset_name.to_string());
                                        }
                                    }
                                }

                                self.notify_refresh_done(
                                    &mut ready_sender,
                                    status::ComponentStatus::Ready,
                                )
                                .await;
                            };
                        }
                        Err(e) => {
                            self.mark_dataset_status(status::ComponentStatus::Error);
                            tracing::error!("Error adding data for {dataset_name}: {e}");
                        }
                    }
                }
                None => break,
            };
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

    async fn stream_updates(
        &self,
        acceleration_refresh_mode: AccelerationRefreshMode,
    ) -> BoxStream<'_, super::Result<(Option<SystemTime>, DataUpdate)>> {
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
            AccelerationRefreshMode::Changes => Box::pin(self.get_changes_stream()),
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

    fn get_full_update_stream(
        &self,
        receiver: Receiver<()>,
    ) -> impl Stream<Item = super::Result<(Option<SystemTime>, DataUpdate)>> + '_ {
        let dataset_name = self.dataset_name.clone();

        let mut refresh_stream = ReceiverStream::new(receiver);
        stream! {
            while refresh_stream.next().await.is_some() {
                let timer = TimeMeasurement::new(
                    "load_dataset_duration_ms",
                    vec![("dataset", dataset_name.to_string())],
                );
                let start = SystemTime::now();
                match self.get_full_or_incremental_append_update(None).await {
                    Ok(data) => yield Ok((Some(start), data)),
                    Err(e) => yield Err(e),
                };
                drop(timer);
            }
        }
    }

    fn get_incremental_append_update_stream(
        &self,
        receiver: Receiver<()>,
    ) -> impl Stream<Item = super::Result<(Option<SystemTime>, DataUpdate)>> + '_ {
        let dataset_name = self.dataset_name.clone();

        let mut refresh_stream = ReceiverStream::new(receiver);

        stream! {
            while refresh_stream.next().await.is_some() {
                let timer = TimeMeasurement::new(
                    "append_dataset_duration_ms",
                    vec![("dataset", dataset_name.to_string())],
                );
                match self.timestamp_nanos_for_append_query().await {
                    Ok(timestamp) => {
                        let start = SystemTime::now();
                        match self.get_full_or_incremental_append_update(timestamp).await {
                            Ok(data) => match self.except_existing_records_from(data).await {
                                Ok(data) => yield Ok((Some(start), data)),
                                Err(e) => yield Err(e),
                            },
                            Err(e) => yield Err(e),
                        }
                    }
                    Err(e) => {
                        tracing::error!("No latest timestamp is found: {e}");
                    }
                }
                drop(timer);
            }
        }
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

        tracing::debug!(
            "Filters: {:?} used to refresh {}",
            filters,
            self.dataset_name
        );

        match self.get_data_update(filters).await {
            Ok(data) => Ok(data),
            Err(e) => {
                tracing::error!("Failed to load data for dataset {dataset_name}: {e}");
                Err(e)
            }
        }
    }

    async fn get_data_update(&self, filters: Vec<Expr>) -> super::Result<DataUpdate> {
        let refresh = self.refresh.read().await;
        let update_type = match refresh.mode {
            RefreshMode::Full => UpdateType::Overwrite,
            RefreshMode::Append => UpdateType::Append,
            RefreshMode::Changes => unreachable!("Should not trigger this code path for changes"),
        };
        let mut ctx = self.refresh_df_context();
        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();
        match get_data(
            &mut ctx,
            dataset_name.clone(),
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

    fn get_filter_converter(&self, refresh: &Refresh) -> Option<TimestampFilterConvert> {
        let schema = self.federated.schema();
        let column = refresh.time_column.as_deref().unwrap_or_default();
        let field = schema.column_with_name(column).map(|(_, f)| f).cloned();

        TimestampFilterConvert::create(field, refresh.time_column.clone(), refresh.time_format)
    }

    async fn notify_refresh_done(
        &self,
        ready_sender: &mut Option<oneshot::Sender<()>>,
        status: status::ComponentStatus,
    ) {
        if let Some(sender) = ready_sender.take() {
            sender.send(()).ok();
        };

        self.mark_dataset_status(status);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let mut labels = vec![("dataset", self.dataset_name.to_string())];
        if let Some(sql) = &self.refresh.read().await.sql {
            labels.push(("sql", sql.to_string()));
        };

        metrics::gauge!("datasets_acceleration_last_refresh_time", &labels).set(now.as_secs_f64());
    }

    fn mark_dataset_status(&self, status: status::ComponentStatus) {
        status::update_dataset(&self.dataset_name, status);

        if status == status::ComponentStatus::Error {
            let labels = [("dataset", self.dataset_name.to_string())];
            metrics::counter!("datasets_acceleration_refresh_errors", &labels).increment(1);
        }
    }
}

pub(crate) fn get_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use arrow::{
        array::{ArrowNativeTypeOp, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Fields, Schema},
    };
    use data_components::arrow::write::MemTable;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
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

        let refresh = Refresh::new(None, None, None, None, RefreshMode::Full, None, None);

        let refresher = Refresher::new(
            TableReference::bare("test"),
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
    async fn test_refresh_status_change_to_ready() {
        fn wait_until_ready_status(
            snapshotter: &Snapshotter,
            desired: status::ComponentStatus,
        ) -> bool {
            for _i in 1..20 {
                let hashmap = snapshotter.snapshot().into_vec();
                let (_, _, _, value) = hashmap.first().expect("at least one metric exists");
                match value {
                    DebugValue::Gauge(i) => {
                        let value = i.into_inner();

                        if value.is_eq(f64::from(desired as i32)) {
                            return true;
                        }
                    }
                    _ => panic!("not testing this"),
                }

                sleep(Duration::from_micros(100));
            }

            false
        }

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::set_global_recorder(recorder).expect("recorder is set globally");

        status::update_dataset(
            &TableReference::bare("test"),
            status::ComponentStatus::Refreshing,
        );

        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;

        assert!(wait_until_ready_status(
            &snapshotter,
            status::ComponentStatus::Ready
        ));

        status::update_dataset(
            &TableReference::bare("test"),
            status::ComponentStatus::Refreshing,
        );

        setup_and_test(vec![], vec![], 0).await;

        assert!(wait_until_ready_status(
            &snapshotter,
            status::ComponentStatus::Ready
        ));
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch_for_iso8601() {
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
                None,
            );

            let refresher = Refresher::new(
                TableReference::bare("test"),
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
            "should not apply same timestamp data",
        )
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch_for_timestamp() {
        async fn test(
            source_data: Vec<u64>,
            existing_data: Vec<u64>,
            expected_size: usize,
            time_format: Option<TimeFormat>,
            append_overlap: Option<Duration>,
            message: &str,
        ) {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time",
                DataType::UInt64,
                false,
            )]));
            let arr = UInt64Array::from(source_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let federated = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );

            let arr = UInt64Array::from(existing_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(
                Some("time".to_string()),
                time_format,
                None,
                None,
                RefreshMode::Append,
                None,
                append_overlap,
            );

            let refresher = Refresher::new(
                TableReference::bare("test"),
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
            vec![1, 2, 3],
            vec![],
            3,
            Some(TimeFormat::UnixSeconds),
            None,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec![1, 2, 3],
            vec![2, 3, 4, 5],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec![5, 6],
            vec![1, 2, 3, 4],
            6,
            Some(TimeFormat::UnixSeconds),
            None,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec![4, 4],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not apply same timestamp data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10,
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            "should apply late arrival and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(3)),
            "should apply late arrival within the append overlap period and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            None,
            Some(Duration::from_secs(3)),
            "should default to time unix seconds",
        )
        .await;
        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            "should fetch all data as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch_for_timestamp_with_more_complicated_structs() {
        async fn test(
            source_data: Vec<u64>,
            existing_data: Vec<u64>,
            expected_size: usize,
            time_format: Option<TimeFormat>,
            append_overlap: Option<Duration>,
            message: &str,
        ) {
            let original_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time",
                DataType::UInt64,
                false,
            )]));
            let arr = UInt64Array::from(source_data);
            let batch =
                RecordBatch::try_new(Arc::clone(&original_schema), vec![Arc::new(arr.clone())])
                    .expect("data should be created");

            let struct_array = StructArray::from(batch);
            let schema = Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("time", DataType::UInt64, false),
                arrow::datatypes::Field::new(
                    "struct",
                    DataType::Struct(Fields::from(vec![arrow::datatypes::Field::new(
                        "time",
                        DataType::UInt64,
                        false,
                    )])),
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arr), Arc::new(struct_array)],
            )
            .expect("data should be created");

            let federated = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );

            let arr = UInt64Array::from(existing_data);
            let batch =
                RecordBatch::try_new(Arc::clone(&original_schema), vec![Arc::new(arr.clone())])
                    .expect("data should be created");
            let struct_array = StructArray::from(batch);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arr), Arc::new(struct_array)],
            )
            .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(
                Some("time".to_string()),
                time_format,
                None,
                None,
                RefreshMode::Append,
                None,
                append_overlap,
            );

            let refresher = Refresher::new(
                TableReference::bare("test"),
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
            vec![1, 2, 3],
            vec![],
            3,
            Some(TimeFormat::UnixSeconds),
            None,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec![1, 2, 3],
            vec![2, 3, 4, 5],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec![5, 6],
            vec![1, 2, 3, 4],
            6,
            Some(TimeFormat::UnixSeconds),
            None,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec![4, 4],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not apply same timestamp data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10,
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            "should apply late arrival and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(3)),
            "should apply late arrival within the append overlap period and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            None,
            Some(Duration::from_secs(3)),
            "should default to time unix seconds",
        )
        .await;
        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            "should fetch all data as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
    }
}
