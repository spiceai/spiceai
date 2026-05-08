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

use super::metrics;
use super::refresh::Refresh;
use super::refresh::get_timestamp;
use super::sink::AccelerationSink;
use super::synchronized_table::SynchronizedTable;
use crate::accelerated_table::caching::CacheRefreshHelper;
use crate::accelerated_table::timestamp_metrics_utils::with_find_max_timestamp_in_stream;
use crate::component::dataset::TimeFormat;
use crate::datafusion::builder::{AnalyzerRulesBuilder, get_df_default_config};
use crate::datafusion::error::{
    SpiceExternalError, find_datafusion_root, format_datafusion_error, get_spice_df_error,
};
use crate::datafusion::filter_converter::create_timestamp_filter_convert;
use crate::datafusion::is_spice_internal_dataset;
use crate::datafusion::managed_runtime::{self, ManagedRuntimeError};
use crate::datafusion::refresh_sql;
use crate::federated_table::FederatedTable;
use crate::metrics::telemetry::track_bytes_processed;
use crate::{
    component::dataset::acceleration::RefreshMode,
    dataconnector::get_data,
    datafusion::{filter_converter::TimestampFilterConvert, schema},
    dataupdate::{StreamingDataUpdate, UpdateType},
    status,
};
use arrow::compute::{SortOptions, filter_record_batch};
use arrow::{
    array::{Array, RecordBatch, StructArray, TimestampNanosecondArray, make_comparator},
    datatypes::DataType,
};
use arrow_schema::SchemaRef;
use async_stream::stream;
use data_components::poly::PolyTableProvider;
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::datasource::{DefaultTableSource, TableType};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::{
    dataframe::DataFrame,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{Expr, Operator, cast, col},
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};
use datafusion_expr::{LogicalPlanBuilder, UNNAMED_TABLE, ident};
use datafusion_federation::{FederatedPlanner, FederatedTableProviderAdaptor};
use datafusion_optimizer_rules::physical_plan::HttpParamsPushdown;
use datafusion_table_providers::util::retriable_error::{
    check_and_mark_retriable_error, is_retriable_error,
};
use futures::{StreamExt, stream};
use opentelemetry::KeyValue;
use runtime_datafusion::execution_plan::schema_cast::EnsureSchema;
use runtime_datafusion::extension::ExtensionPlanQueryPlanner;
use runtime_datafusion::extension::bytes_processed::BytesProcessedPhysicalOptimizer;
use runtime_datafusion::optimizer_rule::avoid_vector_columns_on_index::AvoidDerivedVectorColumnOnIndexRule;
use runtime_datafusion_index::analyzer::{
    IndexTableScanExtensionPlanner, IndexTableScanOptimizerRule,
};
use runtime_object_store::registry::default_runtime_env;
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::{OptionExt, ResultExt};
use spicepod::metric::Metrics;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::AtomicI64;
use std::time::{Duration, UNIX_EPOCH};
use std::{cmp::Ordering, sync::Arc, time::SystemTime};
use telemetry::timing::MultiTimeMeasurement;
use tokio::{
    runtime::Handle,
    sync::{Mutex, RwLock, Semaphore, oneshot},
    time::Instant,
};
use tracing::{Instrument, Span};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

mod changes;
mod deletion;

const NANOS_TO_MILLIS: u128 = 1_000_000;

// Callback which is called after each batch of streaming data is processed by the `RefreshTask`.
type StreamBatchProcessCallback =
    Arc<Mutex<Box<dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>>;

#[derive(Debug, Clone, Default)]
struct RefreshStat {
    pub num_rows: usize,
    pub memory_size: usize,
}

/// Synchronous traversal: walks a provider chain and collects indexes from every
/// [`IndexedTableProvider`] layer. Kept as a plain fn (not async) so that the
/// `HashSet<*const ()>` used for dedup never appears inside an async fn and cannot
/// make the enclosing future non-`Send`.
fn collect_indexes_from_provider(
    root: Arc<dyn datafusion::catalog::TableProvider>,
) -> Vec<Arc<dyn runtime_datafusion_index::Index + Send + Sync>> {
    use crate::embeddings::table::EmbeddingTable;
    use runtime_datafusion_index::IndexedTableProvider;

    let mut indexes: Vec<Arc<dyn runtime_datafusion_index::Index + Send + Sync>> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut current = Some(root);

    while let Some(provider) = current.take() {
        if let Some(indexed) = provider.as_any().downcast_ref::<IndexedTableProvider>() {
            for index in indexed.get_all_indexes() {
                let ptr = Arc::as_ptr(&index) as *const ();
                if seen.insert(ptr) {
                    indexes.push(index);
                }
            }
        }

        current = if let Some(adaptor) = provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
        {
            adaptor.table_provider.as_ref().map(Arc::clone)
        } else if let Some(embedding_table) = provider.as_any().downcast_ref::<EmbeddingTable>() {
            Some(Arc::clone(embedding_table.get_underlying_ref()))
        } else if let Some(indexed) = provider.as_any().downcast_ref::<IndexedTableProvider>() {
            Some(indexed.get_underlying())
        } else {
            None
        };
    }

    indexes
}

/// Walks the federated provider chain and collects indexes from **every** [`IndexedTableProvider`]
/// layer encountered. Known wrapper types (`FederatedTableProviderAdaptor`, `EmbeddingTable`) are
/// unwrapped so that indexes nested inside them are not silently missed. These indexes receive
/// write lifecycle hooks alongside accelerator refreshes.
async fn indexes_from_federated(
    federated: &Arc<FederatedTable>,
) -> Vec<Arc<dyn runtime_datafusion_index::Index + Send + Sync>> {
    let root = federated.table_provider().await;
    collect_indexes_from_provider(root)
}

pub struct RefreshTaskBuilder {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    federated: Arc<FederatedTable>,
    federated_source: Option<String>,
    accelerator: Arc<dyn TableProvider>,
    disable_federation: bool,
    // Used to control how many parallel refreshes the runtime performs.
    semaphore: Option<Arc<Semaphore>>,
    metrics: Option<Metrics>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
    /// Mutex to protect concurrent access to the accelerator during cache/snapshot operations.
    accelerator_write_mutex: Arc<Mutex<()>>,
    on_stream_batch_process_callback: Option<StreamBatchProcessCallback>,
    last_updated_at: Arc<AtomicI64>,
    /// Whether the acceleration uses S3 Express One Zone storage.
    is_s3_express_acceleration: bool,
    /// State for `refresh_mode: snapshot`. Required when the refresh mode is
    /// [`RefreshMode::Snapshot`]; ignored otherwise.
    snapshot_refresh_state: Option<crate::accelerated_table::snapshots::SnapshotRefreshState>,
}

impl RefreshTaskBuilder {
    #[must_use]
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        accelerator: Arc<dyn TableProvider>,
        io_runtime: Handle,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            accelerator,
            disable_federation: false,
            semaphore: None,
            metrics: None,
            cpu_runtime: None,
            io_runtime,
            resource_monitor: None,
            accelerator_write_mutex,
            on_stream_batch_process_callback: None,
            last_updated_at: Arc::new(AtomicI64::new(0)),
            is_s3_express_acceleration: false,
            snapshot_refresh_state: None,
        }
    }

    /// Sets the `disable_federation` flag
    #[must_use]
    pub fn with_disable_federation(mut self, disable: bool) -> RefreshTaskBuilder {
        self.disable_federation = disable;
        self
    }

    #[must_use]
    pub fn with_semaphore(mut self, semaphore: Arc<Semaphore>) -> RefreshTaskBuilder {
        self.semaphore = Some(semaphore);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Option<Metrics>) -> RefreshTaskBuilder {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn with_cpu_runtime(mut self, runtime: Option<Handle>) -> RefreshTaskBuilder {
        self.cpu_runtime = runtime;
        self
    }

    #[must_use]
    pub fn with_resource_monitor(
        mut self,
        monitor: crate::resource_monitor::ResourceMonitor,
    ) -> RefreshTaskBuilder {
        self.resource_monitor = Some(monitor);
        self
    }

    #[must_use]
    pub fn with_on_stream_batch_process_callback(
        mut self,
        callback: Option<StreamBatchProcessCallback>,
    ) -> RefreshTaskBuilder {
        self.on_stream_batch_process_callback = callback;
        self
    }

    #[must_use]
    pub fn with_last_updated_at(mut self, last_updated_at: Arc<AtomicI64>) -> RefreshTaskBuilder {
        self.last_updated_at = last_updated_at;
        self
    }

    /// Set whether the acceleration uses S3 Express One Zone storage.
    #[must_use]
    pub fn with_s3_express_acceleration(mut self, is_s3_express: bool) -> RefreshTaskBuilder {
        self.is_s3_express_acceleration = is_s3_express;
        self
    }

    /// Provide the snapshot-refresh state required for `RefreshMode::Snapshot`.
    #[must_use]
    pub fn with_snapshot_refresh_state(
        mut self,
        state: Option<crate::accelerated_table::snapshots::SnapshotRefreshState>,
    ) -> RefreshTaskBuilder {
        self.snapshot_refresh_state = state;
        self
    }

    #[must_use]
    pub async fn build(self) -> RefreshTask {
        let semaphore = self
            .semaphore
            .unwrap_or_else(|| Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)));

        // Create the acceleration sink at build time rather than storing it in the builder.
        //
        // Design rationale: While this creates the sink even if the RefreshTask is never used,
        // this approach is necessary because:
        // 1. The sink requires the accelerator Arc, which the builder owns
        // 2. Storing the sink in the builder would require the builder to be mutable or use
        //    interior mutability (e.g., `Option<Arc<RwLock<AccelerationSink>>>`)
        // 3. In practice, RefreshTask is always used immediately after building - it's not a
        //    speculative construction pattern
        // 4. The sink itself is lightweight (just wraps an Arc to the accelerator)
        //
        // Trade-off: This creates a small amount of overhead (one Arc + RwLock allocation) even
        // if the task is never executed, but simplifies the builder API and ownership model.
        // The alternative of lazy initialization would add complexity without meaningful benefit
        // given the typical usage pattern.
        // Extract indexes from the federated provider chain so they receive write
        // lifecycle hooks without needing to be manually plumbed through as sink_indexes.
        let federated_indexes = indexes_from_federated(&self.federated).await;
        let sink = Arc::new(RwLock::new(
            AccelerationSink::new(Arc::clone(&self.accelerator))
                .with_sink_indexes(federated_indexes),
        ));

        RefreshTask {
            runtime_status: self.runtime_status,
            dataset_name: self.dataset_name,
            federated: self.federated,
            federated_source: self.federated_source,
            accelerator: self.accelerator,
            sink,
            disable_federation: self.disable_federation,
            semaphore,
            enabled_metrics: self
                .metrics
                .as_ref()
                .map(spicepod::metric::Metrics::enabled_metrics)
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .cloned()
                .collect(),
            cpu_runtime: self.cpu_runtime,
            io_runtime: self.io_runtime,
            resource_monitor: self.resource_monitor,
            accelerator_write_mutex: self.accelerator_write_mutex,
            on_stream_batch_process_callback: self.on_stream_batch_process_callback,
            last_updated_at: self.last_updated_at,
            is_s3_express_acceleration: self.is_s3_express_acceleration,
            snapshot_refresh_state: self.snapshot_refresh_state,
        }
    }
}

pub struct RefreshTask {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    federated: Arc<FederatedTable>,
    federated_source: Option<String>,
    accelerator: Arc<dyn TableProvider>,
    sink: Arc<RwLock<AccelerationSink>>,
    disable_federation: bool,
    // Used to control how many parallel refreshes the runtime performs.
    semaphore: Arc<Semaphore>,
    enabled_metrics: HashSet<String>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
    /// Mutex to protect concurrent access to the accelerator during cache/snapshot operations.
    accelerator_write_mutex: Arc<Mutex<()>>,
    on_stream_batch_process_callback: Option<StreamBatchProcessCallback>,
    last_updated_at: Arc<AtomicI64>,
    /// Whether the acceleration uses S3 Express One Zone storage.
    is_s3_express_acceleration: bool,
    /// Per-dataset state required for `RefreshMode::Snapshot`. `None` for all
    /// other refresh modes.
    snapshot_refresh_state: Option<crate::accelerated_table::snapshots::SnapshotRefreshState>,
}

impl std::fmt::Debug for RefreshTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshTask")
            .field("runtime_status", &self.runtime_status)
            .field("dataset_name", &self.dataset_name)
            .field("federated", &self.federated)
            .field("federated_source", &self.federated_source)
            .field("accelerator", &self.accelerator)
            .field("sink", &self.sink)
            .field("disable_federation", &self.disable_federation)
            .field("semaphore", &self.semaphore)
            .field("enabled_metrics", &self.enabled_metrics)
            .field("cpu_runtime", &self.cpu_runtime)
            .field("io_runtime", &self.io_runtime)
            .field("resource_monitor", &self.resource_monitor)
            .finish_non_exhaustive()
    }
}

impl RefreshTask {
    #[must_use]
    pub fn builder(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        accelerator: Arc<dyn TableProvider>,
        io_runtime: Handle,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> RefreshTaskBuilder {
        RefreshTaskBuilder::new(
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            accelerator,
            io_runtime,
            accelerator_write_mutex,
        )
    }

    /// Subscribes a new acceleration table provider to the existing `AccelerationSink` managed by this `RefreshTask`.
    pub async fn add_synchronized_table(&self, synchronized_table: SynchronizedTable) {
        self.sink
            .write()
            .await
            .add_synchronized_table(synchronized_table);
    }

    pub async fn run(&self, refresh: Refresh) -> super::Result<()> {
        // Limit parallel refreshes via a semaphore
        let _permit = self.semaphore.acquire().await;

        let max_retries = if refresh.retry_enabled {
            refresh.retry_max_attempts
        } else {
            Some(0)
        };

        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(max_retries)
            .build();

        let mut spans = vec![];
        let mut parent_span = Span::current();
        for dataset_name in self.get_dataset_names().await {
            let span = tracing::span!(target: "task_history", parent: &parent_span, tracing::Level::INFO, "acceleration_refresh", input = %dataset_name);
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
            // During runtime shutdown, refresh tasks are canceled resulting in acceleration error.
            // This is expected and should not be logged as an error.
            if !self.runtime_status.is_shutdown() {
                tracing::error!(
                    "Failed to refresh {} {}: {e}",
                    self.component_type(),
                    include_source_to_table_name(
                        &self.dataset_name,
                        self.federated_source.as_deref()
                    )
                );
                for span in &spans {
                    tracing::error!(target: "task_history", parent: span, "{e}");
                }
            }
        })
    }

    async fn run_once(&self, refresh: &Refresh) -> Result<(), RetryError<super::Error>> {
        self.set_refresh_status(
            refresh.display_sql().as_deref(),
            status::ComponentStatus::Refreshing,
        )
        .await;

        let dataset_metrics_label_sets = self.get_dataset_label_sets(&refresh.mode).await;

        // max_timestamp_before_refresh is needed if at least one of the following metrics is enabled:
        //  * METRIC_MAX_TIMESTAMP_BEFORE_REFRESH_MS
        //  * METRIC_REFRESH_LAG_MS
        // max_timestamp_after_refresh is needed if at least one of the following metrics is enabled:
        //  * METRIC_MAX_TIMESTAMP_AFTER_REFRESH_MS
        //  * METRIC_REFRESH_LAG_MS
        //  * METRIC_INGESTION_LAG_MS
        let (need_max_timestamp_before_refresh, need_max_timestamp_after_refresh) = (
            self.is_metric_enabled(metrics::METRIC_MAX_TIMESTAMP_BEFORE_REFRESH_MS)
                || self.is_metric_enabled(metrics::METRIC_REFRESH_LAG_MS),
            self.is_metric_enabled(metrics::METRIC_MAX_TIMESTAMP_AFTER_REFRESH_MS)
                || self.is_metric_enabled(metrics::METRIC_REFRESH_LAG_MS)
                || self.is_metric_enabled(metrics::METRIC_INGESTION_LAG_MS),
        );

        let max_timestamp_before_refresh_ms = if need_max_timestamp_before_refresh {
            self.get_max_timestamp_before_refresh(refresh).await
        } else {
            None
        };

        // For table providers with refresh skip support, check if the refresh can be skipped to
        // avoid unnecessary data fetching when the underlying data is unchanged.
        if refresh.mode == RefreshMode::Full || refresh.mode == RefreshMode::Append {
            let table_provider = self.federated.table_provider().await;

            match data_components::refresh_skip::should_skip_refresh_for_table_provider(
                table_provider.as_ref(),
            )
            .await
            {
                Ok(Some(true)) => {
                    tracing::debug!(
                        "Skipping refresh for {} - data unchanged",
                        self.dataset_name
                    );

                    for label_set in &dataset_metrics_label_sets {
                        metrics::REFRESH_DATA_FETCHES_SKIPPED.add(1, label_set);
                    }

                    self.set_refresh_status(
                        refresh.display_sql().as_deref(),
                        status::ComponentStatus::Ready,
                    )
                    .await;
                    return Ok(());
                }
                Ok(_) => {
                    // Data may have changed or provider does not support skipping; continue with refresh.
                }
                Err(e) => {
                    tracing::debug!(
                        "Failed to check if refresh should be skipped for {}, proceeding with refresh: {}",
                        self.dataset_name,
                        e
                    );
                }
            }
        }

        // Start timing the actual refresh operation (after early return checks)
        let _timer = MultiTimeMeasurement::new(
            #[expect(clippy::match_same_arms)] // Caching will have different behavior in future
            match refresh.mode {
                RefreshMode::Disabled => {
                    unreachable!("Refresh cannot be called when acceleration is disabled")
                }
                RefreshMode::Full | RefreshMode::Append => &metrics::REFRESH_DURATION_MS,
                RefreshMode::Changes => unreachable!("changes are handled upstream"),
                RefreshMode::Caching => &metrics::REFRESH_DURATION_MS,
                RefreshMode::Snapshot => &metrics::REFRESH_DURATION_MS,
            },
            &dataset_metrics_label_sets,
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
            RefreshMode::Caching => {
                // For caching mode, identify and refresh stale rows based on fetched_at and TTL
                return self.refresh_stale_cached_rows(refresh).await;
            }
            RefreshMode::Snapshot => {
                // For snapshot mode, poll the snapshot store for a newer snapshot
                // and reload the accelerator from it. The federated source is
                // never queried for refreshes in this mode.
                return self.refresh_from_snapshot(refresh).await;
            }
        };

        let streaming_data_update = match get_data_update_result {
            Ok(data_update) => data_update,
            Err(e) => {
                // During runtime shutdown, refresh tasks are canceled resulting in acceleration error.
                // This is expected and should not be logged as an error.
                if self.runtime_status.is_shutdown() {
                    return Ok(());
                }
                self.log_refresh_error(
                    inner_err_from_retry_ref(&e),
                    refresh.display_sql().as_deref(),
                )
                .await;
                return Err(e);
            }
        };

        let (streaming_data_update, max_timestamp_after_refresh_ms) =
            if need_max_timestamp_after_refresh {
                let source_name = format!(
                    "{} {}",
                    self.component_type(),
                    include_source_to_table_name(
                        &self.dataset_name,
                        self.federated_source.as_deref()
                    )
                );
                with_find_max_timestamp_in_stream(
                    streaming_data_update,
                    self.federated.schema(),
                    refresh.time_column.clone(),
                    refresh.time_format,
                    source_name,
                )
                .await
            } else {
                (streaming_data_update, None)
            };

        if let Err(e) = self
            .write_streaming_data_update(
                Some(start_time),
                streaming_data_update,
                refresh.display_sql().as_deref(),
            )
            .await
        {
            // During runtime shutdown, refresh tasks are canceled resulting in acceleration error.
            // This is expected and should not be logged as an error.
            if self.runtime_status.is_shutdown() {
                return Ok(());
            }
            tracing::warn!(
                "Failed to load data for {} {}: {}",
                self.component_type(),
                include_source_to_table_name(&self.dataset_name, self.federated_source.as_deref()),
                inner_err_from_retry_ref(&e)
            );
            return Err(e);
        }

        // Only record metrics if a refresh was successful
        self.handle_metrics(
            &dataset_metrics_label_sets,
            max_timestamp_before_refresh_ms,
            max_timestamp_after_refresh_ms,
        )
        .await;

        Ok(())
    }

    fn is_metric_enabled(&self, metric_name: &str) -> bool {
        self.enabled_metrics.contains(metric_name)
    }

    async fn get_max_timestamp_before_refresh(&self, refresh: &Refresh) -> Option<i64> {
        if refresh.time_column.is_some() {
            match self.timestamp_nanos_for_append_query(refresh).await {
                Ok(Some(time_nanos)) => i64::try_from(time_nanos / NANOS_TO_MILLIS).ok(),
                Ok(None) => None,
                Err(e) => {
                    if !self.runtime_status.is_shutdown() {
                        tracing::warn!(
                            "Failed to fetch max_timestamp_before_refresh for {} {}: {}",
                            self.component_type(),
                            include_source_to_table_name(
                                &self.dataset_name,
                                self.federated_source.as_deref()
                            ),
                            e
                        );
                    }
                    None
                }
            }
        } else {
            None
        }
    }

    async fn handle_metrics(
        &self,
        dataset_metrics_label_sets: &[Vec<KeyValue>],
        max_timestamp_before_refresh_ms: Option<i64>,
        max_timestamp_after_refresh_ms: Option<Arc<Mutex<Option<i64>>>>,
    ) {
        let max_timestamp_after_refresh_ms_value = match &max_timestamp_after_refresh_ms {
            Some(arc_mutex) => {
                let guard = arc_mutex.lock().await;
                *guard
            }
            None => None,
        };

        #[expect(clippy::cast_possible_truncation)]
        let current_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        for label_set in dataset_metrics_label_sets {
            if self.is_metric_enabled(metrics::METRIC_MAX_TIMESTAMP_BEFORE_REFRESH_MS)
                && let Some(val) = max_timestamp_before_refresh_ms
            {
                metrics::MAX_TIMESTAMP_BEFORE_REFRESH_MS.record(val, label_set);
            }

            if self.is_metric_enabled(metrics::METRIC_MAX_TIMESTAMP_AFTER_REFRESH_MS)
                && let Some(val) = max_timestamp_after_refresh_ms_value
            {
                metrics::MAX_TIMESTAMP_AFTER_REFRESH_MS.record(val, label_set);
            }

            if self.is_metric_enabled(metrics::METRIC_REFRESH_LAG_MS)
                && let (Some(before), Some(after)) = (
                    max_timestamp_before_refresh_ms,
                    max_timestamp_after_refresh_ms_value,
                )
            {
                let refresh_lag_ms = after - before;
                metrics::REFRESH_LAG_MS.record(refresh_lag_ms, label_set);
            }

            if self.is_metric_enabled(metrics::METRIC_INGESTION_LAG_MS)
                && let Some(after) = max_timestamp_after_refresh_ms_value
            {
                let ingestion_lag_ms = current_time_ms - after;
                metrics::INGESTION_LAG_MS.record(ingestion_lag_ms, label_set);
            }
        }
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

        let resource_monitor = self.resource_monitor.clone();
        let observed_record_batch_stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::unfold(
                (
                    data_update.data,
                    RefreshStat::default(),
                    dataset_name.to_string(),
                    notify_written_data_stat_available,
                    DataLoadTracing::new(&self.dataset_name),
                    resource_monitor,
                ),
                move |(
                    mut stream,
                    mut stat,
                    ds_name,
                    notify_refresh_stat_available,
                    mut tracing,
                    resource_monitor,
                )| async move {
                    if let Some(batch) = stream.next().await {
                        match batch {
                            Ok(batch) => {
                                tracing.on_new_batch_received(&batch);
                                stat.num_rows += batch.num_rows();
                                stat.memory_size += batch.get_array_memory_size();

                                // Record incremental ingestion counters per batch.
                                let labels = [KeyValue::new("dataset", ds_name.clone())];
                                metrics::REFRESH_ROWS_WRITTEN.add(batch.num_rows() as u64, &labels);
                                metrics::REFRESH_BYTES_WRITTEN
                                    .add(batch.get_array_memory_size() as u64, &labels);

                                // Check memory usage after processing each batch
                                if let Some(ref monitor) = resource_monitor {
                                    monitor.check_memory_usage(&ds_name);
                                }

                                Some((
                                    Ok(batch),
                                    (
                                        stream,
                                        stat,
                                        ds_name,
                                        notify_refresh_stat_available,
                                        tracing,
                                        resource_monitor,
                                    ),
                                ))
                            }
                            Err(err) => Some((
                                Err(err),
                                (
                                    stream,
                                    stat,
                                    ds_name,
                                    notify_refresh_stat_available,
                                    tracing,
                                    resource_monitor,
                                ),
                            )),
                        }
                    } else {
                        if notify_refresh_stat_available.send(stat).is_err() {
                            tracing::error!(
                                "Failed to provide stats on the amount of data written into {ds_name}"
                            );
                        }
                        None
                    }
                },
            ),
        );

        let record_batch_stream = Box::pin(observed_record_batch_stream);
        let sink_lock = self.sink.read().await;
        let sink = &*sink_lock;

        let _lock_guard = self.accelerator_write_mutex.lock().await;
        if let Err(e) = sink.insert_into(record_batch_stream, overwrite).await {
            let error_message = format_datafusion_error(&e);
            self.set_refresh_status(
                sql,
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
            return Err(e);
        }

        let refresh_stat = on_written_data_stat_available.try_recv().ok();

        if let (Some(start_time), Some(stat)) = (start_time, &refresh_stat) {
            self.trace_load_completed(start_time, stat.num_rows, stat.memory_size)
                .await;
        }

        if let Some(stat) = &refresh_stat {
            for dataset_name in self.get_dataset_names().await {
                let labels = [KeyValue::new("dataset", dataset_name.to_string())];
                metrics::REFRESH_PROCESSED_ROWS.add(stat.num_rows as u64, &labels);
                metrics::REFRESH_PROCESSED_BYTES.add(stat.memory_size as u64, &labels);
            }
        }

        self.set_refresh_status(sql, status::ComponentStatus::Ready)
            .await;

        self.maybe_update_last_updated_at(
            &data_update.update_type,
            refresh_stat.map_or(0, |s| s.num_rows),
        );

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
            tracing::debug!("Loading data for {} {dataset_name}", self.component_type());
        } else {
            tracing::info!("Loading data for {} {dataset_name}", self.component_type());
        }

        self.set_refresh_status(
            refresh.display_sql().as_deref(),
            status::ComponentStatus::Refreshing,
        )
        .await;

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
        }

        self.get_data_update(filters, &refresh).await
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
            Ok(timestamp) => {
                tracing::debug!(
                    "Found max timestamp for {} {}: {:?}",
                    self.component_type(),
                    self.dataset_name,
                    timestamp
                );

                match self
                    .get_full_or_incremental_append_update(refresh, timestamp)
                    .await
                {
                    Ok(data) => match self.except_existing_records_from(refresh, data).await {
                        Ok(data) => Ok(data),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }
            Err(e) => {
                if !self.runtime_status.is_shutdown() {
                    tracing::error!("No latest timestamp is found: {e}");
                }
                Err(e)
            }
        }
    }

    async fn refresh_stale_cached_rows(
        &self,
        refresh: &Refresh,
    ) -> Result<(), RetryError<super::Error>> {
        // Get the caching TTL from refresh settings - default to 30 seconds if not specified
        let ttl = refresh.caching_ttl.unwrap_or(Duration::from_secs(30));

        tracing::info!(
            "Starting stale row refresh for dataset {} with TTL {ttl:?}",
            self.dataset_name,
        );

        // Use the CacheRefreshHelper to identify and refresh all stale rows
        let federated_provider = self.federated.table_provider().await;
        let refreshed_count = CacheRefreshHelper::refresh_all_stale_rows(
            federated_provider,
            Arc::clone(&self.accelerator),
            self.dataset_name.to_string().as_str(),
            ttl,
            Arc::clone(&self.accelerator_write_mutex),
        )
        .await
        .map_err(|e| RetryError::permanent(super::Error::FailedToRefreshDataset { source: e }))?;

        tracing::info!(
            "Completed stale row refresh for dataset {} - refreshed {refreshed_count} rows",
            self.dataset_name,
        );

        Ok(())
    }

    /// Drives `RefreshMode::Snapshot`: poll the snapshot store for a snapshot
    /// strictly newer than what is currently loaded; if found, download it
    /// (which writes to the accelerator's primary path) and call into the
    /// accelerator's `reload_from_snapshot` to swap in a fresh `TableProvider`.
    ///
    /// The federated source is never queried by this code path. When no newer
    /// snapshot is available the call is a no-op (Ready, no swap).
    async fn refresh_from_snapshot(
        &self,
        refresh: &Refresh,
    ) -> Result<(), RetryError<super::Error>> {
        let _ = refresh; // refresh sql / window are intentionally unused for snapshot mode

        let Some(state) = self.snapshot_refresh_state.clone() else {
            // This is a configuration bug: the refresh mode is Snapshot but no
            // SnapshotRefreshState was attached. Surface as a permanent error so
            // the dataset is marked unhealthy rather than retried indefinitely.
            tracing::error!(
                dataset = %self.dataset_name,
                "refresh_mode: snapshot is configured but no SnapshotRefreshState is available; \
                 this indicates a runtime configuration bug."
            );
            self.set_refresh_status(
                None,
                status::ComponentStatus::error_with_message("snapshot refresh failure".to_string()),
            )
            .await;
            return Err(RetryError::permanent(
                super::Error::FailedToRefreshDataset {
                    source: datafusion::error::DataFusionError::Internal(
                        "snapshot refresh state missing".to_string(),
                    ),
                },
            ));
        };

        self.set_refresh_status(None, status::ComponentStatus::Refreshing)
            .await;

        let start_time = SystemTime::now();
        let current_local_id = state.current_loaded_id();

        // Take the accelerator write mutex up front so the entire refresh
        // (download + provider rebuild + swap) is serialized with other code
        // paths that take this mutex. `AcceleratedTable::insert_into` rejects
        // writes outright when `refresh_mode: snapshot` is enabled, so this
        // mutex's only remaining job here is to serialize concurrent snapshot
        // refreshes / cache writes against the swap. The atomic rename inside
        // `download_if_newer` independently protects against partial-file
        // reads from in-flight queries that hold their own connection refs to
        // the prior file inode.
        let _write_guard = Arc::clone(&self.accelerator_write_mutex).lock_owned().await;

        // Hand the snapshot manager a schema validator that runs against
        // the snapshot metadata's recorded schema **before** the file is
        // downloaded or renamed. This guarantees a schema-incompatible
        // snapshot can never overwrite the accelerator's primary file.
        let live_schema = state.swappable_provider.schema();
        let live_schema_for_validate = Arc::clone(&live_schema);
        let validator: Box<dyn Fn(&arrow_schema::SchemaRef) -> bool + Send + Sync> =
            Box::new(move |candidate: &arrow_schema::SchemaRef| {
                schemas_compatible(candidate.as_ref(), live_schema_for_validate.as_ref())
            });
        let download_result = state
            .manager
            .download_if_newer(current_local_id, Some(validator.as_ref()))
            .await;

        let info = match download_result {
            Ok(Some(info)) => info,
            Ok(None) => {
                tracing::debug!(
                    dataset = %self.dataset_name,
                    current_snapshot_id = ?current_local_id,
                    "refresh_mode: snapshot - no newer snapshot available; skipping reload"
                );
                let dataset_metrics_label_sets =
                    self.get_dataset_label_sets(&RefreshMode::Snapshot).await;
                for label_set in &dataset_metrics_label_sets {
                    metrics::REFRESH_DATA_FETCHES_SKIPPED.add(1, label_set);
                }
                self.set_refresh_status(None, status::ComponentStatus::Ready)
                    .await;
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(
                    dataset = %self.dataset_name,
                    error = %e,
                    "refresh_mode: snapshot - failed to check/download snapshot"
                );
                self.set_refresh_status(
                    None,
                    status::ComponentStatus::error_with_message(
                        "snapshot refresh failure".to_string(),
                    ),
                )
                .await;
                return Err(RetryError::transient(
                    super::Error::FailedToRefreshDataset {
                        source: datafusion::error::DataFusionError::External(Box::new(e)),
                    },
                ));
            }
        };

        // The snapshot manager already rejected schema-incompatible
        // snapshots before download; this is a defense-in-depth check
        // against the (rare) case where the metadata's recorded schema
        // differed from the schema actually embedded in the downloaded
        // file. The downloaded file may have replaced the primary path
        // here, but `reload_from_snapshot` is gated below — and a
        // schema-mismatch returned here is treated as permanent.
        if !schemas_compatible(info.schema.as_ref(), live_schema.as_ref()) {
            tracing::error!(
                dataset = %self.dataset_name,
                snapshot_id = info.snapshot_id,
                "refresh_mode: snapshot - downloaded snapshot schema does not match \
                 accelerator schema; refusing to swap"
            );
            self.set_refresh_status(
                None,
                status::ComponentStatus::error_with_message("snapshot refresh failure".to_string()),
            )
            .await;
            return Err(RetryError::permanent(
                super::Error::FailedToRefreshDataset {
                    source: datafusion::error::DataFusionError::Internal(
                        "snapshot schema mismatch".to_string(),
                    ),
                },
            ));
        }

        // The accelerator write mutex was taken above, before the download,
        // so the entire reload + swap remains serialized with concurrent
        // accelerator writes.
        let new_provider = match state
            .accelerator
            .reload_from_snapshot(
                state.source.as_ref(),
                state.swappable_provider.current(),
                Arc::clone(&state.provider_factory),
            )
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    dataset = %self.dataset_name,
                    snapshot_id = info.snapshot_id,
                    error = %e,
                    "refresh_mode: snapshot - accelerator failed to reload from snapshot"
                );
                self.set_refresh_status(
                    None,
                    status::ComponentStatus::error_with_message(
                        "snapshot refresh failure".to_string(),
                    ),
                )
                .await;
                return Err(RetryError::transient(
                    super::Error::FailedToRefreshDataset {
                        source: datafusion::error::DataFusionError::Internal(e.to_string()),
                    },
                ));
            }
        };

        if !schemas_compatible(new_provider.schema().as_ref(), live_schema.as_ref()) {
            tracing::error!(
                dataset = %self.dataset_name,
                snapshot_id = info.snapshot_id,
                "refresh_mode: snapshot - reloaded provider schema does not match accelerator \
                 schema; refusing to swap"
            );
            self.set_refresh_status(
                None,
                status::ComponentStatus::error_with_message("snapshot refresh failure".to_string()),
            )
            .await;
            return Err(RetryError::permanent(
                super::Error::FailedToRefreshDataset {
                    source: datafusion::error::DataFusionError::Internal(
                        "reloaded snapshot provider schema mismatch".to_string(),
                    ),
                },
            ));
        }

        if let Err(swap_err) = state.swappable_provider.swap(new_provider) {
            tracing::error!(
                dataset = %self.dataset_name,
                snapshot_id = info.snapshot_id,
                error = %swap_err,
                "refresh_mode: snapshot - swap rejected by SwappableTableProvider"
            );
            self.set_refresh_status(
                None,
                status::ComponentStatus::error_with_message("snapshot refresh failure".to_string()),
            )
            .await;
            return Err(RetryError::permanent(
                super::Error::FailedToRefreshDataset {
                    source: datafusion::error::DataFusionError::Internal(format!(
                        "snapshot swap rejected: {swap_err}"
                    )),
                },
            ));
        }
        state.set_current_loaded_id(info.snapshot_id);
        if let Some(updated_at) = info.last_updated_at {
            self.last_updated_at
                .store(updated_at, std::sync::atomic::Ordering::Release);
        }

        if let Ok(elapsed) = util::humantime_elapsed(start_time) {
            tracing::info!(
                dataset = %self.dataset_name,
                snapshot_id = info.snapshot_id,
                bytes = info.bytes_downloaded,
                "Loaded snapshot in {elapsed}"
            );
        }

        self.set_refresh_status(None, status::ComponentStatus::Ready)
            .await;
        Ok(())
    }

    async fn trace_load_completed(
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

            let component_type = self.component_type();

            if is_spice_internal_dataset(&self.dataset_name) {
                tracing::debug!(
                    "Loaded {num_rows} rows{memory_size} for {component_type} {dataset_name} in {elapsed}.",
                );
            } else {
                tracing::info!(
                    "Loaded {num_rows} rows{memory_size} for {component_type} {dataset_name} in {elapsed}."
                );
                for synchronized_table in self.sink.read().await.synchronized_tables() {
                    tracing::info!(
                        "Loaded {num_rows} rows{memory_size} for {component_type} {} in {elapsed}.",
                        synchronized_table.child_dataset_name()
                    );
                }
            }
        }
    }

    async fn get_data_update(
        &self,
        mut filters: Vec<Expr>,
        refresh: &Refresh,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let federated_provider = self.federated.table_provider().await;

        let dataset_name = self.dataset_name.clone();
        #[expect(clippy::match_same_arms)] // Caching will have different behavior in future
        let update_type = match refresh.mode {
            RefreshMode::Disabled => {
                unreachable!("Refresh cannot be called when acceleration is disabled")
            }
            RefreshMode::Full => UpdateType::Overwrite,
            RefreshMode::Append => UpdateType::Append,
            RefreshMode::Changes => unreachable!("changes are handled upstream"),
            RefreshMode::Caching => UpdateType::Overwrite,
            RefreshMode::Snapshot => {
                unreachable!("snapshot mode is handled by refresh_from_snapshot")
            }
        };

        // If a refresh SQL is explicitly provided for this `RefreshTask` (instead of provided at startup within the
        // spicepod), parse and use it. Transfer partition filters from the base refresh SQL.
        let effective_sql = match refresh.override_sql_raw.as_ref().map(|s| {
            refresh_sql::parse_refresh_sql(dataset_name.clone(), s, federated_provider.schema())
        }) {
            Some(Ok((mut parsed, _schema))) => {
                if let Some(base) = &refresh.sql {
                    parsed.set_partition_filters(base.partition_filters().to_vec());
                }
                Some(parsed)
            }
            Some(Err(e)) => {
                tracing::error!("Failed to parse override refresh_sql for {dataset_name}: {e}");
                return Err(RetryError::permanent(
                    super::Error::FailedToRefreshDataset {
                        source: DataFusionError::Plan(format!("Invalid override refresh_sql: {e}")),
                    },
                ));
            }
            None => refresh.sql.clone(),
        };

        // Extract SQL string and partition filters from RefreshSQL
        let sql_string = effective_sql
            .as_ref()
            .map(super::refresh::RefreshSQL::to_sql);
        if let Some(ref s) = effective_sql {
            filters.extend(s.partition_filters().iter().cloned());
        }

        if let Some(cpu_runtime_handle) = self.cpu_runtime.clone() {
            let dataset_name_for_runtime = dataset_name.clone();
            let filters_for_runtime = filters.clone();
            let update_type_for_runtime = update_type.clone();
            let provider_for_runtime = Arc::clone(&federated_provider);
            let sql_for_runtime = sql_string.clone();
            let request_context = RequestContext::current(AsyncMarker::new().await);
            let span = Span::current();

            // Capture necessary state to create ctx inside the closure
            let dataset_name_for_ctx = self.dataset_name.clone();
            let accelerator_for_ctx = Arc::clone(&self.accelerator);
            let disable_federation = self.disable_federation;
            let io_runtime = self.io_runtime.clone();

            let managed_stream = managed_runtime::run_record_batch_stream_on_runtime(
                cpu_runtime_handle,
                request_context,
                span,
                async move {
                    // Create ctx inside the managed runtime to avoid creating it twice
                    let mut ctx = Self::create_refresh_df_context(
                        Arc::clone(&provider_for_runtime),
                        &dataset_name_for_ctx,
                        &accelerator_for_ctx,
                        disable_federation,
                        io_runtime,
                    )
                    .await;

                    let data = get_data(
                        &mut ctx,
                        dataset_name_for_runtime,
                        provider_for_runtime,
                        sql_for_runtime,
                        filters_for_runtime,
                    )
                    .await
                    .map_err(check_and_mark_retriable_error)?;
                    Ok((update_type_for_runtime, data))
                },
            )
            .await
            .map_err(|err| match err {
                ManagedRuntimeError::Future(df_err) => retry_from_df_error(df_err),
                ManagedRuntimeError::DriverTaskEnded => {
                    retry_from_df_error(DataFusionError::Execution(
                        "Refresh driver task ended unexpectedly".to_string(),
                    ))
                }
            })?;

            let (update_type, stream) = managed_stream.into_parts();
            return Ok(StreamingDataUpdate::new(stream, update_type));
        }

        // Create ctx only in the fallback path (no managed runtime)
        let mut ctx = Self::create_refresh_df_context(
            Arc::clone(&federated_provider),
            &self.dataset_name,
            &self.accelerator,
            self.disable_federation,
            self.io_runtime.clone(),
        )
        .await;

        let get_data_result = get_data(
            &mut ctx,
            dataset_name,
            federated_provider,
            sql_string,
            filters,
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
        Self::build_filter_converter(&schema, refresh)
    }

    fn get_accelerator_filter_converter(
        &self,
        refresh: &Refresh,
    ) -> Option<TimestampFilterConvert> {
        let schema = self.accelerator.schema();
        Self::build_filter_converter(&schema, refresh)
    }

    fn build_filter_converter(
        schema: &SchemaRef,
        refresh: &Refresh,
    ) -> Option<TimestampFilterConvert> {
        let column = refresh.time_column.as_deref().unwrap_or_default();
        let field = schema.column_with_name(column).map(|(_, f)| f).cloned();
        let time_partition_column = refresh.time_partition_column.as_deref();
        let partition_field = schema
            .column_with_name(time_partition_column.unwrap_or_default())
            .map(|(_, f)| f)
            .cloned();

        create_timestamp_filter_convert(
            field,
            refresh.time_column.clone(),
            refresh.time_format,
            partition_field,
            refresh.time_partition_column.clone(),
            refresh.time_partition_format,
        )
    }

    /// Static helper method to create a `DataFusion` context for refresh operations.
    /// This is separated from `refresh_df_context` to allow it to be called from async closures
    /// without requiring `self`, avoiding the need to create the context twice.
    async fn create_refresh_df_context(
        federated_provider: Arc<dyn TableProvider>,
        dataset_name: &TableReference,
        accelerator: &Arc<dyn TableProvider>,
        disable_federation: bool,
        io_runtime: Handle,
    ) -> SessionContext {
        let state_builder = SessionStateBuilder::new()
            .with_config(get_df_default_config())
            .with_runtime_env(default_runtime_env(io_runtime))
            .with_default_features();

        let mut extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(IndexTableScanExtensionPlanner::new())];

        let mut analyzer_rules_builder = AnalyzerRulesBuilder::default();

        // If federation is disabled, disable the federation analyzer rule and don't include the federated planner.
        if disable_federation {
            analyzer_rules_builder = analyzer_rules_builder.include_federation(false);
        } else {
            analyzer_rules_builder = analyzer_rules_builder.include_federation(true);
            extension_planners.push(Arc::new(FederatedPlanner::new()));
        }

        let mut state = state_builder
            .with_query_planner(Arc::new(
                ExtensionPlanQueryPlanner::from_extension_planners(extension_planners),
            ))
            .with_optimizer_rule(Arc::new(IndexTableScanOptimizerRule::new()))
            .with_optimizer_rule(Arc::new(AvoidDerivedVectorColumnOnIndexRule {}))
            .with_physical_optimizer_rule(Arc::new(HttpParamsPushdown))
            .with_physical_optimizer_rule(Arc::new(BytesProcessedPhysicalOptimizer::new(Arc::new(
                Box::new(track_bytes_processed),
            ))))
            .with_analyzer_rules(analyzer_rules_builder.build())
            .build();

        state
            .config_mut()
            .set_extension(RequestContext::current(AsyncMarker::new().await));

        if let Err(e) = datafusion_functions_json::register_all(&mut state) {
            tracing::error!("Unable to register JSON functions: {e}");
        }

        let default_catalog = state.config_options().catalog.default_catalog.clone();
        let ctx = SessionContext::new_with_state(state);

        // Register core scalar UDFs (e.g. bucket())
        crate::datafusion::udf::register_core_scalar_udfs(&ctx);

        match schema::ensure_schema_exists(&ctx, &default_catalog, dataset_name) {
            Ok(()) => (),
            Err(_) => {
                unreachable!("The default catalog should always exist");
            }
        }

        if let Some(catalog_name) = dataset_name.catalog()
            && ctx.catalog(catalog_name).is_none()
        {
            ctx.register_catalog(catalog_name, Arc::new(MemoryCatalogProvider::new()));
        }

        let target_catalog = dataset_name.catalog().unwrap_or(&default_catalog);
        if let Err(e) = schema::ensure_schema_exists(&ctx, target_catalog, dataset_name) {
            tracing::error!(
                "Unable to ensure schema exists for refresh context {}.{}: {e}",
                target_catalog,
                dataset_name.schema().unwrap_or_default()
            );
        }

        if let Err(e) = ctx.register_table(dataset_name.clone(), federated_provider) {
            tracing::error!("Unable to register federated table: {e}");
        }

        let mut acc_dataset_name = String::with_capacity(
            dataset_name.table().len() + dataset_name.schema().map_or(0, str::len),
        );

        if let Some(schema) = dataset_name.schema() {
            acc_dataset_name.push_str(schema);
        }

        acc_dataset_name.push_str("accelerated_");
        acc_dataset_name.push_str(dataset_name.table());

        if let Err(e) = ctx.register_table(
            TableReference::parse_str(&acc_dataset_name),
            Arc::new(EnsureSchema::new(Arc::clone(accelerator))),
        ) {
            tracing::error!("Unable to register accelerator table: {e}");
        }
        ctx
    }

    async fn except_existing_records_from(
        &self,
        refresh: &Refresh,
        mut update: StreamingDataUpdate,
    ) -> Result<StreamingDataUpdate, RetryError<super::Error>> {
        let Some(value) = self.timestamp_nanos_for_append_query(refresh).await? else {
            return Ok(update);
        };
        let Some(filter_converter) = self.get_accelerator_filter_converter(refresh) else {
            return Ok(update);
        };

        let federated_provider = self.federated.table_provider().await;

        let existing_records = accelerator_df(
            &Arc::clone(&self.accelerator),
            &Self::create_refresh_df_context(
                Arc::clone(&federated_provider),
                &self.dataset_name,
                &self.accelerator,
                self.disable_federation,
                self.io_runtime.clone(),
            )
            .await,
        )
        .map_err(find_datafusion_root)
        .context(super::UnableToScanTableProviderSnafu)?
        .filter(filter_converter.convert(value, Operator::Gt))
        .map_err(find_datafusion_root)
        .context(super::UnableToScanTableProviderSnafu)?
        .collect()
        .await
        .map_err(find_datafusion_root)
        .context(super::UnableToScanTableProviderSnafu)?;

        // Use the update stream's schema for dedup comparison, not the full federated
        // provider schema.  When `refresh_sql` selects a column subset, the incoming
        // batches and accelerated table only contain those columns.
        let filter_schema = update.data.schema();
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

    #[expect(clippy::cast_sign_loss)]
    async fn timestamp_nanos_for_append_query(
        &self,
        refresh: &Refresh,
    ) -> super::Result<Option<u128>> {
        let federated = self.federated.table_provider().await;
        let ctx = Self::create_refresh_df_context(
            federated,
            &self.dataset_name,
            &self.accelerator,
            self.disable_federation,
            self.io_runtime.clone(),
        )
        .await;

        refresh
            .validate_time_format(self.dataset_name.to_string(), &self.accelerator.schema())
            .context(super::InvalidTimeColumnTimeFormatSnafu)?;

        let column = refresh
            .time_column
            .clone()
            .context(super::FailedToFindLatestTimestampSnafu {
            reason:
                "Failed to get the latest timestamp. The `time_column` parameter must be specified.",
        })?;

        let df = max_timestamp_df(&Arc::clone(&self.accelerator), ctx, &column)
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

        if result.num_rows() == 0 {
            return Ok(None);
        }

        let col_array = result.column(0);

        let schema = &self.accelerator.schema();
        let Ok(accelerated_field) = schema.field_with_name(&column) else {
            return Err(super::Error::FailedToFindLatestTimestamp {
                reason: "Failed to get the latest timestamp. The `time_column` parameter must be specified."
                    .to_string(),
            });
        };

        let is_integer_time_column = matches!(
            accelerated_field.data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        );

        // Extract the max timestamp value based on the column's data type.
        // - String columns (ISO8601): parse the ISO string back to nanos
        // - Integer columns (UnixSeconds/UnixMillis): read raw integer value
        // - Timestamp columns: read as TimestampNanosecondArray (was CAST'd by max_timestamp_df)
        // Handle all string array types (Utf8, LargeUtf8, Utf8View) for ISO8601 columns.
        let iso_str_value = col_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .and_then(|a| (!a.is_null(0)).then(|| a.value(0).to_string()))
            .or_else(|| {
                col_array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .and_then(|a| (!a.is_null(0)).then(|| a.value(0).to_string()))
            })
            .or_else(|| {
                col_array
                    .as_any()
                    .downcast_ref::<arrow::array::StringViewArray>()
                    .and_then(|a| (!a.is_null(0)).then(|| a.value(0).to_string()))
            });

        let mut value: u128 = if let Some(iso_str) = iso_str_value {
            util::timestamp_filter::parse_iso8601_to_nanos(&iso_str).context(
                super::FailedToFindLatestTimestampSnafu {
                    reason: format!(
                        "Failed to parse ISO8601 timestamp '{iso_str}' from time column"
                    ),
                },
            )?
        } else if is_integer_time_column {
            // Integer time columns are returned as-is (not cast to Timestamp)
            // to avoid DuckDB cast errors. Extract the integer value directly.
            if col_array.is_empty() {
                return Ok(None);
            }
            match accelerated_field.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    let arr = arrow::compute::cast(col_array, &DataType::Int64).map_err(|e| {
                        super::Error::FailedToFindLatestTimestamp {
                            reason: format!("Failed to cast integer time column to Int64: {e}"),
                        }
                    })?;
                    let arr = arr
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| super::Error::FailedToFindLatestTimestamp {
                            reason: "Failed to downcast integer time column to Int64Array."
                                .to_string(),
                        })?;
                    if arr.is_null(0) {
                        return Ok(None);
                    }
                    let int_val = arr.value(0);
                    u128::try_from(int_val).map_err(|_| {
                        super::Error::FailedToFindLatestTimestamp {
                            reason: format!(
                                "Integer time column value {int_val} is negative and cannot be used as a timestamp."
                            ),
                        }
                    })?
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    let arr = arrow::compute::cast(col_array, &DataType::UInt64).map_err(|e| {
                        super::Error::FailedToFindLatestTimestamp {
                            reason: format!(
                                "Failed to cast unsigned integer time column to UInt64: {e}"
                            ),
                        }
                    })?;
                    let arr = arr
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .ok_or_else(|| super::Error::FailedToFindLatestTimestamp {
                            reason:
                                "Failed to downcast unsigned integer time column to UInt64Array."
                                    .to_string(),
                        })?;
                    if arr.is_null(0) {
                        return Ok(None);
                    }
                    u128::from(arr.value(0))
                }
                other => {
                    return Err(super::Error::FailedToFindLatestTimestamp {
                        reason: format!("Unexpected data type {other} for integer time column."),
                    });
                }
            }
        } else {
            let array = col_array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context(super::FailedToFindLatestTimestampSnafu {
                    reason: "Failed to get the latest timestamp during incremental appending. Failed to convert the value of the time column to a timestamp. Verify the column is a timestamp.",
                })?;

            if array.is_empty() || array.is_null(0) {
                return Ok(None);
            }

            array.value(0) as u128
        };

        if is_integer_time_column {
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
        }

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

    async fn set_refresh_status(&self, sql: Option<&str>, status: status::ComponentStatus) {
        let is_error = status.is_error();
        let is_ready = status == status::ComponentStatus::Ready;

        // runtime status update
        self.update_component_status(status).await;

        // telemetry update
        for dataset_name in self.get_dataset_names().await {
            if is_error {
                let labels = [KeyValue::new("dataset", dataset_name.to_string())];
                metrics::REFRESH_ERRORS.add(1, &labels);
            }

            if is_ready {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();

                let mut labels = vec![KeyValue::new("dataset", dataset_name.to_string())];
                if let Some(sql) = sql {
                    labels.push(KeyValue::new("sql", sql.to_string()));
                }

                metrics::LAST_REFRESH_TIME_MS.record(now.as_secs_f64() * 1000.0, &labels);
            }
        }
    }

    fn component_type(&self) -> &'static str {
        if self.is_view_acceleration() {
            "view"
        } else {
            "dataset"
        }
    }

    async fn update_component_status(&self, status: status::ComponentStatus) {
        // main component status update
        if self.is_view_acceleration() {
            self.runtime_status
                .update_view(&self.dataset_name, status.clone());
        } else {
            self.runtime_status
                .update_dataset(&self.dataset_name, status.clone());
        }

        // synchronized tables can be datasets only
        for synchronized_table in self.sink.read().await.synchronized_tables() {
            self.runtime_status
                .update_dataset(&synchronized_table.child_dataset_name(), status.clone());
        }
    }

    fn is_view_acceleration(&self) -> bool {
        match &*self.federated {
            FederatedTable::Immediate(provider) => provider.table_type() == TableType::View,
            FederatedTable::Deferred(_) => false,
        }
    }

    async fn log_refresh_error(&self, error: &super::Error, refresh_sql: Option<&str>) {
        if let super::Error::UnableToGetDataFromConnector { source } = error
            && let Some(SpiceExternalError::AccelerationNotReady { dataset_name }) =
                get_spice_df_error(source)
        {
            tracing::warn!(
                "Dataset {} is waiting for {dataset_name} to finish loading initial acceleration.",
                self.dataset_name
            );
            self.set_refresh_status(refresh_sql, status::ComponentStatus::Initializing)
                .await;
            return;
        }

        // Check for S3 Express One Zone upload speed error and provide user-friendly message.
        // ClientUploadSpeedTooSlow is specific to S3 Express One Zone (directory buckets).
        if self.is_s3_express_acceleration && is_s3_express_upload_speed_error(&error.to_string()) {
            let error_message = format_datafusion_error(error);
            let table_name =
                include_source_to_table_name(&self.dataset_name, self.federated_source.as_deref());
            tracing::warn!(
                error = %error_message,
                "Failed to load data for {} {table_name}: S3 upload speed too slow. This typically occurs when uploading to S3 Express One Zone from outside AWS or over a slow network connection. Consider: (1) Running Spice closer to your S3 bucket (same region/AZ), (2) Reducing dataset size or using incremental refresh, (3) Increasing 'cayenne_target_file_size_mb' to reduce the number of files uploaded.",
                self.component_type(),
            );
            self.set_refresh_status(
                refresh_sql,
                status::ComponentStatus::error_with_message(error_message),
            )
            .await;
            return;
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
                if let DataFusionError::Internal(msg) = &source
                    && msg.contains("Non Panic Task error")
                    && msg.contains("was cancelled")
                {
                    tracing::debug!("Ignoring DataFusion error due to task cancellation: {source}");
                    return;
                }
            }
            _ => (),
        }

        if let Some(message) = schema_evolution_mismatch_refresh_message(
            self.component_type(),
            &include_source_to_table_name(&self.dataset_name, self.federated_source.as_deref()),
            error,
        ) {
            tracing::warn!("{message}");
            self.set_refresh_status(
                refresh_sql,
                status::ComponentStatus::error_with_message(message),
            )
            .await;
            return;
        }

        let error_message = format_datafusion_error(error);
        tracing::warn!(
            "Failed to load data for {} {}: {}",
            self.component_type(),
            include_source_to_table_name(&self.dataset_name, self.federated_source.as_deref()),
            error_message,
        );
        self.set_refresh_status(
            refresh_sql,
            status::ComponentStatus::error_with_message(error_message),
        )
        .await;
    }

    /// Updates `last_updated_at` timestamp based on refresh type and row count.
    ///
    /// - For `Overwrite` and `Changes`: Always updates (data is replaced/modified)
    /// - For `Append`: Only updates if rows were actually written (`num_rows > 0`)
    fn maybe_update_last_updated_at(&self, update_type: &UpdateType, num_rows: usize) {
        let should_update = match update_type {
            UpdateType::Overwrite | UpdateType::Changes => true,
            UpdateType::Append => num_rows > 0,
        };

        if should_update {
            self.update_last_updated_at();
        }
    }

    fn update_last_updated_at(&self) {
        super::AcceleratedTable::set_timestamp_to_now(&self.last_updated_at);
    }
}

/// Returns true when `candidate` is structurally compatible with `expected`
/// for swapping a `TableProvider` under a `SwappableTableProvider`. See
/// [`crate::dataaccelerator::swappable::schemas_compatible`] for the precise
/// rules; this is a thin re-export so callers in this module can keep using
/// the unqualified name.
fn schemas_compatible(candidate: &arrow_schema::Schema, expected: &arrow_schema::Schema) -> bool {
    crate::dataaccelerator::swappable::schemas_compatible(candidate, expected)
}

#[derive(Debug)]
/// Tracks and logs data load progress for a dataset, periodically reporting the number of records received
struct DataLoadTracing {
    dataset: TableReference,
    num_records_received: usize,
    bytes_received: usize,
    start_time: Instant,
    last_updated_time: Instant,
    log_interval: Duration,
}

impl DataLoadTracing {
    fn new(dataset: &TableReference) -> Self {
        let now = Instant::now();
        Self {
            dataset: dataset.clone(),
            num_records_received: 0,
            bytes_received: 0,
            start_time: now,
            last_updated_time: now,
            log_interval: Duration::from_secs(10),
        }
    }

    fn on_new_batch_received(&mut self, batch: &RecordBatch) {
        let num_rows = batch.num_rows();
        let batch_size = batch.get_array_memory_size();

        tracing::trace!("Dataset {} received {num_rows} records", self.dataset,);
        self.num_records_received += num_rows;
        self.bytes_received += batch_size;

        // Log progress every 10 seconds showing cumulative stats
        if self.last_updated_time.elapsed() > self.log_interval {
            let pretty_records = util::pretty_print_number(self.num_records_received);
            let elapsed = self.start_time.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();

            // Calculate throughput
            #[expect(clippy::cast_precision_loss)]
            #[expect(clippy::cast_possible_truncation)]
            #[expect(clippy::cast_sign_loss)]
            let throughput = if elapsed_secs > 0.0 {
                let bytes_per_sec = (self.bytes_received as f64 / elapsed_secs) as usize;
                format!("{}/s", util::human_readable_bytes(bytes_per_sec))
            } else {
                "calculating...".to_string()
            };

            let size = util::human_readable_bytes(self.bytes_received);
            let elapsed_str = format!("{}s", elapsed.as_secs());

            // Note: size and throughput are based on uncompressed in-memory Arrow data size,
            // not actual network transfer. Actual network bytes may be significantly smaller
            // due to compression.
            if is_spice_internal_dataset(&self.dataset) {
                tracing::debug!(
                    "Dataset {} received {pretty_records} records ({size} uncompressed) in {elapsed_str}, {throughput}",
                    self.dataset
                );
            } else {
                tracing::info!(
                    "Dataset {} received {pretty_records} records ({size} uncompressed) in {elapsed_str}, {throughput}",
                    self.dataset
                );
            }

            self.last_updated_time = Instant::now();
        }
    }
}

#[expect(clippy::needless_pass_by_value)]
pub fn max_timestamp_df(
    accelerator: &Arc<dyn TableProvider>,
    ctx: SessionContext,
    column: &str,
) -> Result<DataFrame, DataFusionError> {
    let schema = accelerator.schema();
    let needs_cast = schema.column_with_name(column).is_some_and(|(_, f)| {
        // Only CAST for native date/time/timestamp types that need precision normalization.
        // Integers (UnixSeconds/UnixMillis) and strings (ISO8601) are directly sortable
        // without CAST, which avoids engine-specific cast limitations (e.g. DuckDB can't
        // cast BIGINT→TIMESTAMP, Vortex can't cast UTF8→TIMESTAMP).
        matches!(
            f.data_type(),
            DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        )
    });

    let expr = if needs_cast {
        cast(
            col(format!(r#""{column}""#)),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        )
        .alias("a")
    } else {
        col(format!(r#""{column}""#)).alias("a")
    };

    accelerator_df(accelerator, &ctx)?
        .select(vec![expr])?
        .sort(vec![col("a").sort(false, false)])?
        .limit(0, Some(1))
}

fn accelerator_df(
    accelerator: &Arc<dyn TableProvider>,
    ctx: &SessionContext,
) -> Result<DataFrame, DataFusionError> {
    // The purpose behind this logic is:
    // 1. If possible, extract FederatedTableProviderAdaptor from PolyTableProvider and make it the top-level table provider (needed by datafusion-federation)
    // 2. Make sure EnsureSchema is present (either on top-level or under FederatedTableProviderAdaptor)
    let accelerator: Arc<dyn TableProvider> = accelerator_table_provider(accelerator);

    let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&accelerator)));

    // Get the columns so we can add projection to the plan. This
    // converts the plan to federated where the correct dialect is applied
    let schema = accelerator.schema();
    let columns: Vec<Expr> = schema.fields().iter().map(|f| ident(f.name())).collect();

    // Records in the accelerator table are already filtered so we don't need to apply refresh SQL
    let logical_plan = LogicalPlanBuilder::scan(UNNAMED_TABLE, table_source, None)
        .map_err(find_datafusion_root)?
        .project(columns)?
        .build()
        .map_err(find_datafusion_root)?;

    Ok(DataFrame::new(ctx.state(), logical_plan))
}

pub fn accelerator_table_provider(accelerator: &Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    match accelerator.as_any().downcast_ref::<PolyTableProvider>() {
        Some(poly) => match poly
            .get_federated_table_provider()
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
        {
            Some(FederatedTableProviderAdaptor {
                source,
                table_provider: Some(table_provider),
            }) => Arc::new(FederatedTableProviderAdaptor::new_with_provider(
                Arc::clone(source),
                Arc::new(EnsureSchema::new(Arc::clone(table_provider))),
            )) as Arc<dyn TableProvider>,
            None
            | Some(FederatedTableProviderAdaptor {
                source: _,
                table_provider: None,
            }) => Arc::new(EnsureSchema::new(Arc::new(poly.clone()))),
        },
        None => Arc::new(EnsureSchema::new(Arc::clone(accelerator))),
    }
}

fn include_source_to_table_name(name: &TableReference, source: Option<&str>) -> String {
    match source {
        Some(source) => format!("{name} ({source})"),
        None => name.to_string(),
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

fn inner_err_from_retry_ref(error: &RetryError<super::Error>) -> &super::Error {
    match error {
        RetryError::Permanent(inner_err) | RetryError::Transient { err: inner_err, .. } => {
            inner_err
        }
    }
}

/// Check if an error message indicates an S3 Express One Zone upload speed error.
///
/// S3 Express One Zone (directory buckets) returns `ClientUploadSpeedTooSlow` when
/// the client's upload speed is below the minimum threshold. This typically occurs
/// when uploading from outside AWS or over slow network connections.
///
/// This function is extracted to enable unit testing of the detection logic.
fn is_s3_express_upload_speed_error(error_message: &str) -> bool {
    error_message.contains("ClientUploadSpeedTooSlow")
}

fn is_insert_schema_mismatch_error(error_message: &str) -> bool {
    error_message.contains("Inserting query must have the same schema length as the table")
        || error_message.contains("Inserting query must have same schema length as the table")
}

fn schema_evolution_mismatch_refresh_message(
    component_type: &str,
    table_name: &str,
    error: &super::Error,
) -> Option<String> {
    let (super::Error::FailedToWriteData { source }
    | super::Error::FailedToRefreshDataset { source }) = error
    else {
        return None;
    };

    if !is_insert_schema_mismatch_error(&source.to_string()) {
        return None;
    }

    Some(format!(
        "Failed to load data for {component_type} {table_name}: schema mismatch between the existing accelerated table and current source schema; fully featured schema evolution is on the roadmap, and acceleration does not apply this schema evolution automatically today; delete the existing acceleration data and restart Spice to rebuild it with the updated schema."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataupdate::{StreamingDataUpdate, UpdateType};
    use crate::federated_table::FederatedTable;
    use arrow::array::{
        Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
        TimestampNanosecondArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::TimeUnit;
    use arrow_schema::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[test]
    fn test_data_load_tracing_tracks_bytes_and_rows() {
        let dataset = TableReference::bare("test_dataset");
        let mut tracing = DataLoadTracing::new(&dataset);

        // Create a test batch
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("Failed to create batch");

        let batch_size = batch.get_array_memory_size();
        let num_rows = batch.num_rows();

        // Process the batch
        tracing.on_new_batch_received(&batch);

        // Verify state
        assert_eq!(tracing.num_records_received, num_rows);
        assert_eq!(tracing.bytes_received, batch_size);
        assert!(tracing.bytes_received > 0);
    }

    #[test]
    fn test_is_s3_express_upload_speed_error() {
        // Should detect ClientUploadSpeedTooSlow error
        assert!(is_s3_express_upload_speed_error(
            "Error: ClientUploadSpeedTooSlow: Upload speed is below minimum threshold"
        ));
        // Should detect when error is part of a larger message
        assert!(is_s3_express_upload_speed_error(
            "Failed to upload: S3 returned ClientUploadSpeedTooSlow for bucket mybucket--usw2-az1--x-s3"
        ));

        // Should not match unrelated errors
        assert!(!is_s3_express_upload_speed_error("Connection timeout"));
        assert!(!is_s3_express_upload_speed_error("Access denied"));
        assert!(!is_s3_express_upload_speed_error("NoSuchBucket"));

        // Should not match partial error names
        assert!(!is_s3_express_upload_speed_error("ClientUpload"));
        assert!(!is_s3_express_upload_speed_error("SpeedTooSlow"));
    }

    #[test]
    fn test_is_insert_schema_mismatch_error() {
        assert!(is_insert_schema_mismatch_error(
            "Error during planning: Inserting query must have the same schema length as the table. Expected table schema length: 4, got: 5"
        ));
        assert!(!is_insert_schema_mismatch_error(
            "Error during planning: failed to parse expression"
        ));
    }

    #[test]
    fn test_schema_evolution_mismatch_refresh_message_for_write_error() {
        let error = super::super::Error::FailedToWriteData {
            source: DataFusionError::Execution(
                "Inserting query must have the same schema length as the table. Expected table schema length: 4, got: 5".to_string(),
            ),
        };

        let message = schema_evolution_mismatch_refresh_message("dataset", "nation", &error)
            .expect("should detect schema mismatch");
        assert!(message.contains("schema mismatch"));
        assert!(message.contains("on the roadmap"));
        assert!(message.contains("delete the existing acceleration data"));
    }

    #[test]
    fn test_schema_evolution_mismatch_refresh_message_non_schema_error() {
        let error = super::super::Error::FailedToRefreshDataset {
            source: DataFusionError::Execution("other failure".to_string()),
        };

        assert!(schema_evolution_mismatch_refresh_message("dataset", "nation", &error).is_none());
    }

    /// Tests that `max_timestamp_df` returns the maximum value for integer time columns
    /// (e.g. `unix_seconds` / `unix_millis` time formats) without casting to a Timestamp type.
    #[tokio::test]
    async fn test_max_timestamp_df_integer_time_column() {
        async fn run_test(data_type: DataType, array: Arc<dyn arrow::array::Array>) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new("ts", data_type, false)]));
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![array]).expect("batch created");
            let mem_table =
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table created"))
                    as Arc<dyn TableProvider>;

            let ctx = SessionContext::new();
            let df = max_timestamp_df(&mem_table, ctx.clone(), "ts").expect("df created");
            let results = collect(
                df.create_physical_plan()
                    .await
                    .expect("physical plan created"),
                ctx.task_ctx(),
            )
            .await
            .expect("query succeeded");
            results.into_iter().next().expect("at least one batch")
        }

        // Signed Int64: column is returned as Int64.
        let signed_vals = Int64Array::from(vec![100_i64, 200, 50]);
        let batch = run_test(DataType::Int64, Arc::new(signed_vals)).await;
        let max_val = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array")
            .value(0);
        assert_eq!(max_val, 200, "Int64: expected max value 200");

        // Signed Int32: column is returned as Int32.
        let signed_vals = Int32Array::from(vec![10_i32, 30, 20]);
        let batch = run_test(DataType::Int32, Arc::new(signed_vals)).await;
        let max_val = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("Int32Array")
            .value(0);
        assert_eq!(max_val, 30, "Int32: expected max value 30");

        // Unsigned UInt64: column is returned as UInt64.
        let unsigned_vals = UInt64Array::from(vec![1_000_u64, 5_000, 3_000]);
        let batch = run_test(DataType::UInt64, Arc::new(unsigned_vals)).await;
        let max_val = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64Array")
            .value(0);
        assert_eq!(max_val, 5_000, "UInt64: expected max value 5000");

        // Unsigned UInt32: column is returned as UInt32.
        let unsigned_vals = UInt32Array::from(vec![7_u32, 42, 3]);
        let batch = run_test(DataType::UInt32, Arc::new(unsigned_vals)).await;
        let max_val = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array")
            .value(0);
        assert_eq!(max_val, 42, "UInt32: expected max value 42");
    }

    /// Verifies that `max_timestamp_df` uses sort+limit on raw string (no CAST)
    /// for utf8 columns, which avoids the Vortex/Cayenne cast kernel issue.
    #[tokio::test]
    async fn test_max_timestamp_df_utf8_no_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time_col",
            DataType::Utf8,
            false,
        )]));

        let mem_table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])
            .expect("mem table should be created");
        let accelerator: Arc<dyn TableProvider> = Arc::new(mem_table);

        let ctx = SessionContext::new();
        let df = max_timestamp_df(&accelerator, ctx, "time_col").expect("should build df");

        // Verify the plan uses sort+limit on raw string, not Cast or MAX
        let plan_str = format!("{:?}", df.logical_plan());
        assert!(
            !plan_str.contains("Cast("),
            "utf8 column should NOT use Cast, got: {plan_str}"
        );
        assert!(
            plan_str.contains("Sort") && plan_str.contains("Limit"),
            "utf8 column should use Sort+Limit, got: {plan_str}"
        );
    }

    /// Verifies that `max_timestamp_df` still uses CAST+sort for non-string columns.
    #[tokio::test]
    async fn test_max_timestamp_df_timestamp_uses_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time_col",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                1_000_000_000,
                3_000_000_000,
                2_000_000_000,
            ]))],
        )
        .expect("batch");

        let mem_table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
            .expect("mem table should be created");
        let accelerator: Arc<dyn TableProvider> = Arc::new(mem_table);

        let ctx = SessionContext::new();
        let df = max_timestamp_df(&accelerator, ctx, "time_col").expect("should build df");

        // Verify the plan uses Cast, not MAX
        let plan_str = format!("{:?}", df.logical_plan());
        assert!(
            plan_str.contains("Cast("),
            "timestamp column should use Cast, got: {plan_str}"
        );
    }

    /// Helper: run `max_timestamp_df` on an accelerator and extract the ISO string
    /// from the result, using the same downcast chain as `timestamp_nanos_for_append_query`.
    async fn collect_iso_string_from_max_df(
        accelerator: &Arc<dyn TableProvider>,
        col: &str,
    ) -> Option<String> {
        let ctx = SessionContext::new();
        let df = max_timestamp_df(accelerator, ctx, col).expect("should build df");
        let results = df.collect().await.expect("should collect");
        let result = results.first()?;
        if result.num_rows() == 0 {
            return None;
        }
        let col_array = result.column(0);
        col_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .and_then(|a| (!a.is_null(0)).then(|| a.value(0).to_string()))
            .or_else(|| {
                col_array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .and_then(|a| (!a.is_null(0)).then(|| a.value(0).to_string()))
            })
            .or_else(|| {
                col_array
                    .as_any()
                    .downcast_ref::<arrow::array::StringViewArray>()
                    .and_then(|a| (!a.is_null(0)).then(|| a.value(0).to_string()))
            })
    }

    #[tokio::test]
    async fn test_max_timestamp_iso8601_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                "2024-01-01T00:00:00",
                "2024-06-15T12:30:00",
                "2024-03-10T08:00:00",
            ]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        let val = collect_iso_string_from_max_df(&mem, "t").await;
        assert_eq!(val.as_deref(), Some("2024-06-15T12:30:00"));
    }

    #[tokio::test]
    async fn test_max_timestamp_iso8601_large_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::LargeUtf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeStringArray::from(vec![
                "2024-01-01T00:00:00",
                "2024-06-15T12:30:00",
                "2024-03-10T08:00:00",
            ]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        let val = collect_iso_string_from_max_df(&mem, "t").await;
        assert_eq!(val.as_deref(), Some("2024-06-15T12:30:00"));
    }

    #[tokio::test]
    async fn test_max_timestamp_iso8601_utf8_view() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringViewArray::from(vec![
                "2024-01-01T00:00:00",
                "2024-06-15T12:30:00",
                "2024-03-10T08:00:00",
            ]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        let val = collect_iso_string_from_max_df(&mem, "t").await;
        assert_eq!(val.as_deref(), Some("2024-06-15T12:30:00"));
    }

    /// Verifies that `max_timestamp_df` does NOT use CAST for integer columns (`UnixSeconds`).
    #[tokio::test]
    async fn test_max_timestamp_df_int64_no_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time_col",
            DataType::Int64,
            false,
        )]));

        let mem_table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])
            .expect("mem table should be created");
        let accelerator: Arc<dyn TableProvider> = Arc::new(mem_table);

        let ctx = SessionContext::new();
        let df = max_timestamp_df(&accelerator, ctx, "time_col").expect("should build df");

        let plan_str = format!("{:?}", df.logical_plan());
        assert!(
            !plan_str.contains("Cast("),
            "integer column should NOT use Cast, got: {plan_str}"
        );
        assert!(
            plan_str.contains("Sort") && plan_str.contains("Limit"),
            "integer column should use Sort+Limit, got: {plan_str}"
        );
    }

    /// Helper: run `max_timestamp_df` on a numeric accelerator and extract the max
    /// value using the same `arrow::compute::cast` to `Int64` path as
    /// `timestamp_nanos_for_append_query`.
    async fn collect_numeric_from_max_df(
        accelerator: &Arc<dyn TableProvider>,
        col: &str,
    ) -> Option<i64> {
        let ctx = SessionContext::new();
        let df = max_timestamp_df(accelerator, ctx, col).expect("should build df");
        let results = df.collect().await.expect("should collect");
        let result = results.first()?;
        if result.num_rows() == 0 {
            return None;
        }
        let col_array = result.column(0);
        arrow::compute::cast(col_array.as_ref(), &DataType::Int64)
            .ok()
            .and_then(|arr| {
                let int_array = arr.as_any().downcast_ref::<arrow::array::Int64Array>()?;
                if int_array.is_null(0) {
                    return None;
                }
                Some(int_array.value(0))
            })
    }

    #[tokio::test]
    async fn test_max_timestamp_df_int64_extraction() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![100, 300, 200]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(collect_numeric_from_max_df(&mem, "t").await, Some(300));
    }

    #[tokio::test]
    async fn test_max_timestamp_df_uint64_extraction() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![100u64, 300, 200]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(collect_numeric_from_max_df(&mem, "t").await, Some(300));
    }

    #[tokio::test]
    async fn test_max_timestamp_df_int32_extraction() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![100, 300, 200]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(collect_numeric_from_max_df(&mem, "t").await, Some(300));
    }

    #[tokio::test]
    async fn test_max_timestamp_df_uint32_extraction() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from(vec![100u32, 300, 200]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(collect_numeric_from_max_df(&mem, "t").await, Some(300));
    }

    #[tokio::test]
    async fn test_max_timestamp_df_float64_extraction() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![100.0, 300.0, 200.0]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(collect_numeric_from_max_df(&mem, "t").await, Some(300));
    }

    #[tokio::test]
    async fn test_max_timestamp_df_timestamp_ns_extraction() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                1_000_000_000,
                3_000_000_000,
                2_000_000_000,
            ]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(
            collect_numeric_from_max_df(&mem, "t").await,
            Some(3_000_000_000)
        );
    }

    /// When `refresh_sql` selects a column subset, `filter_records` must use the
    /// update data's schema (not the full source schema) to compare incoming rows
    /// against existing rows.
    #[tokio::test]
    async fn test_except_existing_records_column_subset() {
        // Federated (source) schema is wider: ts + id + extra_col
        let federated_schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("id", DataType::Int32, false),
            Field::new("extra_col", DataType::Int32, false),
        ]));
        let federated_batch = RecordBatch::try_new(
            Arc::clone(&federated_schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_000, 2_000, 3_000])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![100, 200, 300])),
            ],
        )
        .expect("federated batch");
        let federated_table = Arc::new(
            MemTable::try_new(Arc::clone(&federated_schema), vec![vec![federated_batch]])
                .expect("federated MemTable"),
        ) as Arc<dyn TableProvider>;

        // Accelerator schema is the subset: ts + id (no extra_col)
        let accel_schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("id", DataType::Int32, false),
        ]));
        // Pre-populate the accelerator with one existing row (ts=1000, id=1)
        let existing_batch = RecordBatch::try_new(
            Arc::clone(&accel_schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_000])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .expect("existing batch");
        let accelerator = Arc::new(
            MemTable::try_new(Arc::clone(&accel_schema), vec![vec![existing_batch]])
                .expect("accelerator MemTable"),
        ) as Arc<dyn TableProvider>;

        // Build the RefreshTask with the wider federated schema
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&federated_table)));
        let task = RefreshTaskBuilder::new(
            crate::status::RuntimeStatus::new(),
            TableReference::bare("test_subset"),
            federated,
            None,
            Arc::clone(&accelerator),
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .build();

        // The refresh must have a time_column so the dedup path is entered.
        // append_overlap of 1s ensures the overlap window includes the existing
        // row at ts=1000ns (query becomes ts > max_ts - 1s = ts > 0).
        let refresh = Refresh::new(RefreshMode::Append)
            .time_column("ts".to_string())
            .append_overlap(Duration::from_secs(1));

        // Build the update stream with the SUBSET schema (only ts + id, no extra_col)
        let update_batch = RecordBatch::try_new(
            Arc::clone(&accel_schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_000, 2_000])),
                Arc::new(Int32Array::from(vec![1, 4])),
            ],
        )
        .expect("update batch");
        let update_stream: SendableRecordBatchStream = Box::pin(
            MemoryStream::try_new(vec![update_batch], Arc::clone(&accel_schema), None)
                .expect("update stream"),
        );
        let update = StreamingDataUpdate::new(update_stream, UpdateType::Append);

        let result = task
            .except_existing_records_from(&refresh, update)
            .await
            .expect("except_existing_records_from should succeed with column subset");

        let collected = result
            .collect_data()
            .await
            .expect("collecting filtered data should succeed");

        // Row (ts=1000, id=1) matches existing data and should be filtered out.
        // Only row (ts=2000, id=4) should remain.
        assert_eq!(collected.data.len(), 1, "should have one output batch");
        assert_eq!(
            collected.data[0].num_rows(),
            1,
            "should have one row after dedup"
        );
        let id_col = collected.data[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32");
        assert_eq!(id_col.value(0), 4, "remaining row should be id=4");
    }

    #[tokio::test]
    async fn test_max_timestamp_df_timestamp_ns_all_null_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                None, None, None,
            ]))],
        )
        .expect("batch");
        let mem = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;
        assert_eq!(collect_numeric_from_max_df(&mem, "t").await, None);
    }
}
