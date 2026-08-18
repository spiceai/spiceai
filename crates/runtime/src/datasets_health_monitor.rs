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

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::TimestampNanosecondType;
use datafusion::{datasource::TableProvider, error::DataFusionError, sql::TableReference};
use futures::{future::join_all, stream::TryStreamExt};
use iceberg_datafusion::IcebergTableProvider;
use opentelemetry::KeyValue;
use snafu::{ResultExt, Snafu};
use tokio::sync::Mutex;
use tracing_futures::Instrument;

use crate::{
    component::dataset::{CheckAvailability, Dataset},
    datafusion::{
        DataFusion,
        error::{find_datafusion_root, format_datafusion_error},
    },
    search::util::find_concrete_table_provider,
    status::{ComponentStatus, RuntimeStatus},
};
use runtime_metrics as metrics;

/// Lower bound on the monitor's wake-up cadence, so a very small
/// `check_availability_interval` cannot spin the loop.
const MIN_CHECK_TICK: Duration = Duration::from_secs(1);

/// How often the loop wakes when no dataset is being monitored — just enough to
/// notice newly registered datasets. Not user-facing (nothing is probed until a
/// dataset opts in via `check_availability_interval`).
const MONITOR_IDLE_TICK: Duration = Duration::from_mins(1);

/// Upper bound on how far back the `task_history` "recently queried" lookup
/// reaches, regardless of a dataset's configured interval, to keep that query
/// cheap. Datasets with an interval longer than this are still probed directly.
const MAX_RECENT_QUERY_LOOKBACK: Duration = Duration::from_hours(1); // 1 hour

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to read the dataset table for health check: {}",
        format_datafusion_error(source)
    ))]
    UnableToGetTable { source: DataFusionError },

    #[snafu(display("Failed to query dataset health status: {source}"))]
    DataFusionQuery {
        source: crate::datafusion::query::Error,
    },

    #[snafu(display(
        "Failed to get recently accessed datasets. {}",
        format_datafusion_error(source)
    ))]
    UnableToGetRecentlyAccessedDatasets { source: DataFusionError },

    #[snafu(display(
        "Spice received an unexpected data type from a `task_history` query: {data_type} This is likely a bug in Spice, which can be reported here: https://github.com/spiceai/spiceai/issues"
    ))]
    UnexpectedDataType {
        data_type: arrow::datatypes::DataType,
    },
}

#[derive(Clone)]
pub struct DatasetAvailabilityInfo {
    name: String,
    /// Canonical table reference used to key the runtime status registry.
    table_ref: TableReference,
    table_provider: Arc<dyn TableProvider>,
    last_available_time: SystemTime,
    /// How often this dataset's source should be probed.
    interval: Duration,
}

impl Debug for DatasetAvailabilityInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetAvailabilityInfo")
            .field("name", &self.name)
            .field("last_available_time", &self.last_available_time)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

impl DatasetAvailabilityInfo {
    pub fn new(
        table_ref: TableReference,
        table_provider: Arc<dyn TableProvider>,
        interval: Duration,
    ) -> Self {
        Self {
            name: table_ref.to_string(),
            table_ref,
            table_provider,
            last_available_time: SystemTime::now(),
            interval,
        }
    }
}

enum AvailabilityVerificationResult {
    /// The source was reachable; carries the time availability was confirmed
    /// (probe time, or the end time of a recent successful query).
    Available(SystemTime),
    Unavailable(SystemTime, String),
}

pub struct DatasetsHealthMonitor {
    df: Arc<DataFusion>,
    pub monitored_datasets: Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
    is_task_history_enabled: bool,
}

impl DatasetsHealthMonitor {
    #[must_use]
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self {
            df,
            monitored_datasets: Arc::new(Mutex::new(HashMap::new())),
            is_task_history_enabled: false,
        }
    }

    #[must_use]
    pub fn with_task_history_enabled(mut self, is_enabled: bool) -> Self {
        self.is_task_history_enabled = is_enabled;
        self
    }

    pub async fn register_dataset(&self, dataset: &Dataset) -> Result<()> {
        if dataset.is_accelerated() {
            return Ok(());
        }

        if matches!(dataset.check_availability, CheckAvailability::Disabled) {
            tracing::debug!(
                "Skipping dataset {} for availability monitoring (disabled in config)",
                dataset.name
            );
            return Ok(());
        }

        // Availability monitoring is opt-in: a dataset is only monitored when it
        // configures `check_availability_interval`.
        let Some(interval) = dataset.check_availability_interval else {
            tracing::debug!(
                "Skipping dataset {} for availability monitoring (no check_availability_interval configured)",
                dataset.name
            );
            return Ok(());
        };

        let dataset_name = &dataset.name.to_string();

        tracing::debug!(
            "Registering dataset {dataset_name} for periodic availability check every {}s",
            interval.as_secs()
        );

        let table_provider = self.get_table_provider(dataset.name.clone()).await?;

        // Don't enable health check for IcebergTableProvider until this is fixed:
        // https://github.com/spiceai/spiceai/issues/6994
        if find_concrete_table_provider::<IcebergTableProvider>(&table_provider).is_some() {
            tracing::debug!(
                "Availability monitoring skipped for dataset '{dataset_name}': Iceberg format unsupported. Support planned for future release.",
            );
            return Ok(());
        }

        let mut monitored_datasets = self.monitored_datasets.lock().await;
        monitored_datasets.insert(
            dataset_name.clone(),
            Arc::new(DatasetAvailabilityInfo::new(
                dataset.name.clone(),
                table_provider,
                interval,
            )),
        );

        report_dataset_unavailable_time(dataset_name, None);

        Ok(())
    }

    pub async fn deregister_dataset(&self, dataset_name: &String) {
        tracing::debug!("Removing dataset {dataset_name} from periodic availability check");
        let mut monitored_datasets = self.monitored_datasets.lock().await;
        monitored_datasets.remove(dataset_name);
    }

    async fn get_table_provider(
        &self,
        table_ref: TableReference,
    ) -> Result<Arc<dyn TableProvider>> {
        let table = self
            .df
            .ctx
            .table_provider(table_ref)
            .await
            .map_err(find_datafusion_root)
            .context(UnableToGetTableSnafu)?;

        Ok(table)
    }

    /// Returns, per dataset, the most recent time it was the subject of a
    /// successful (`error_code IS NULL`) query within `lookback`.
    ///
    /// A recent successful query is itself proof the source is reachable, so the
    /// monitor can skip a redundant probe (and clear a stale `Error`) for
    /// actively-queried datasets.
    async fn recent_success_times(
        df: Arc<DataFusion>,
        lookback: Duration,
    ) -> Result<HashMap<String, SystemTime>> {
        let lookback_secs = lookback.as_secs().max(1);
        let query = format!(
            "
SELECT labels.datasets AS datasets, end_time
FROM runtime.task_history
WHERE labels.datasets IS NOT NULL
AND NOW() < end_time + INTERVAL '{lookback_secs}' SECOND
AND labels.error_code IS NULL"
        );
        let query_result = df
            .query_builder(&query)
            .build()
            .run()
            .await
            .context(DataFusionQuerySnafu)?;
        let batches = query_result
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(find_datafusion_root)
            .context(UnableToGetRecentlyAccessedDatasetsSnafu)?;

        let mut latest: HashMap<String, SystemTime> = HashMap::new();

        for batch in &batches {
            let datasets_col = batch.column(0);
            let names: Vec<Option<&str>> = match datasets_col.data_type() {
                arrow::datatypes::DataType::Utf8 => {
                    datasets_col.as_string::<i32>().iter().collect()
                }
                arrow::datatypes::DataType::LargeUtf8 => {
                    datasets_col.as_string::<i64>().iter().collect()
                }
                dt => {
                    return Err(Error::UnexpectedDataType {
                        data_type: dt.clone(),
                    });
                }
            };
            let end_times = batch.column(1).as_primitive::<TimestampNanosecondType>();

            for (row, maybe_names) in names.into_iter().enumerate() {
                let Some(row_names) = maybe_names else {
                    continue;
                };
                if end_times.is_null(row) {
                    continue;
                }
                // `end_time` is stored as nanoseconds since the Unix epoch.
                let Ok(nanos) = u64::try_from(end_times.value(row)) else {
                    continue;
                };
                let ts = UNIX_EPOCH + Duration::from_nanos(nanos);
                for name in row_names.split(',') {
                    latest
                        .entry(name.to_string())
                        .and_modify(|existing| {
                            if ts > *existing {
                                *existing = ts;
                            }
                        })
                        .or_insert(ts);
                }
            }
        }

        Ok(latest)
    }

    pub fn start(&self) {
        tracing::debug!("Starting datasets availability monitoring");
        let monitored_datasets = Arc::clone(&self.monitored_datasets);
        let df = Arc::clone(&self.df);
        let runtime_status = df.runtime_status();
        let is_task_history_enabled = self.is_task_history_enabled;
        tokio::spawn(async move {
            loop {
                // Wake at the shortest configured interval so each dataset is
                // probed close to its own cadence; the floor stops a tiny
                // interval from spinning the loop.
                let tick = shortest_interval(&monitored_datasets).await;
                tokio::time::sleep(tick).await;

                let (snapshot, max_interval) = snapshot_datasets(&monitored_datasets).await;
                if snapshot.is_empty() {
                    continue;
                }
                tracing::debug!("Checking datasets availability");

                // A recent successful query proves the source is reachable, so
                // fold it in to avoid redundantly probing actively-queried
                // datasets (and to clear a stale Error for them).
                let recent = if is_task_history_enabled {
                    let lookback = max_interval.min(MAX_RECENT_QUERY_LOOKBACK);
                    match Self::recent_success_times(Arc::clone(&df), lookback).await {
                        Ok(map) => map,
                        Err(e) => {
                            tracing::warn!("{e}");
                            HashMap::new()
                        }
                    }
                } else {
                    HashMap::new()
                };

                let now = SystemTime::now();
                let mut tasks = Vec::new();
                for item in snapshot {
                    // Confirm availability from a recent successful query without a probe.
                    if let Some(query_time) = recent.get(&item.name).copied()
                        && now.duration_since(query_time).unwrap_or(Duration::MAX) < item.interval
                    {
                        update_dataset_availability_info(
                            &monitored_datasets,
                            &runtime_status,
                            &item,
                            AvailabilityVerificationResult::Available(query_time),
                        )
                        .await;
                        continue;
                    }

                    // Not due for a probe yet.
                    if now
                        .duration_since(item.last_available_time)
                        .unwrap_or(Duration::MAX)
                        < item.interval
                    {
                        continue;
                    }

                    let df = Arc::clone(&df);
                    let monitored_datasets = Arc::clone(&monitored_datasets);
                    let runtime_status = Arc::clone(&runtime_status);
                    tasks.push(tokio::spawn(async move {
                        tracing::trace!("Verifying connectivity for dataset {}", &item.name);
                        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "test_connectivity", input = %item.name);
                        let result = match test_connectivity(&item.table_provider, df)
                            .instrument(span.clone())
                            .await
                        {
                            Ok(()) => AvailabilityVerificationResult::Available(SystemTime::now()),
                            Err(err) => {
                                let err_message = match err.find_root() {
                                    DataFusionError::Execution(e) => e.clone(),
                                    _ => err.to_string(),
                                };
                                tracing::error!(target: "task_history", parent: &span, "{err_message}");
                                AvailabilityVerificationResult::Unavailable(
                                    item.last_available_time,
                                    err_message,
                                )
                            }
                        };
                        update_dataset_availability_info(
                            &monitored_datasets,
                            &runtime_status,
                            &item,
                            result,
                        )
                        .await;
                    }));
                }

                join_all(tasks).await;
                tracing::trace!("Finished checking datasets availability");
            }
        });
    }
}

async fn update_dataset_availability_info(
    monitored_datasets: &Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
    runtime_status: &Arc<RuntimeStatus>,
    item: &DatasetAvailabilityInfo,
    test_result: AvailabilityVerificationResult,
) {
    match test_result {
        AvailabilityVerificationResult::Available(available_at) => {
            tracing::trace!(
                "Successfully verified access to federated dataset {}",
                item.name
            );
            {
                let mut lock = monitored_datasets.lock().await;
                if let Some(dataset) = lock.get_mut(&item.name)
                    && available_at > dataset.last_available_time
                {
                    Arc::make_mut(dataset).last_available_time = available_at;
                }
            }
            report_dataset_unavailable_time(&item.name, None);

            // Only restore Ready when we previously flipped the dataset to Error,
            // so a recovery never clobbers a Refreshing/ShuttingDown/Initializing
            // state the rest of the runtime owns.
            if runtime_status
                .get_dataset_status(&item.table_ref)
                .is_some_and(|s| s.is_error())
            {
                runtime_status.update_dataset(&item.table_ref, ComponentStatus::Ready);
                tracing::info!(
                    "Dataset {} source is reachable again; status restored to Ready",
                    item.name
                );
            }
        }
        AvailabilityVerificationResult::Unavailable(last_available_time, err) => {
            tracing::warn!(
                "Failed to verify the dataset {} was available. {err}",
                item.name
            );
            report_dataset_unavailable_time(&item.name, Some(last_available_time));

            // Only transition a healthy (Ready) or already-errored dataset; leave
            // transient lifecycle states (Initializing/Refreshing/ShuttingDown)
            // to the load/refresh paths that own them.
            if runtime_status
                .get_dataset_status(&item.table_ref)
                .is_some_and(|s| matches!(s, ComponentStatus::Ready | ComponentStatus::Error(_)))
            {
                runtime_status.update_dataset(
                    &item.table_ref,
                    ComponentStatus::error_with_message(format!(
                        "Failed to reach the source for dataset {} during availability check: {err}",
                        item.name
                    )),
                );
            }
        }
    }
}

fn report_dataset_unavailable_time(dataset_name: &str, last_available_time: Option<SystemTime>) {
    let labels = vec![KeyValue::new("dataset", dataset_name.to_owned())];

    match last_available_time {
        Some(last_available_time) => metrics::datasets::UNAVAILABLE_TIME_MS.record(
            last_available_time
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
                * 1000.0,
            &labels,
        ),
        None => {
            // use 0 to indicate that the dataset is available; otherwise, the dataset will be shown as unavailable indefinitely
            metrics::datasets::UNAVAILABLE_TIME_MS.record(0.0, &labels);
        }
    }
}

/// Shortest configured probe interval among monitored datasets, clamped to a
/// floor. Falls back to [`MONITOR_IDLE_TICK`] when nothing is monitored, so the
/// loop still wakes periodically to pick up newly registered datasets.
async fn shortest_interval(
    datasets: &Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
) -> Duration {
    let datasets = datasets.lock().await;
    datasets
        .values()
        .map(|d| d.interval)
        .min()
        .unwrap_or(MONITOR_IDLE_TICK)
        .max(MIN_CHECK_TICK)
}

/// Snapshot the monitored datasets plus the longest configured interval (used
/// to bound the `task_history` recent-query lookback).
async fn snapshot_datasets(
    datasets: &Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
) -> (Vec<Arc<DatasetAvailabilityInfo>>, Duration) {
    let datasets = datasets.lock().await;
    let max_interval = datasets
        .values()
        .map(|d| d.interval)
        .max()
        .unwrap_or(MONITOR_IDLE_TICK);
    (datasets.values().map(Arc::clone).collect(), max_interval)
}

/// Attempts a probe makes before reporting a dataset unavailable.
const CONNECTIVITY_ATTEMPTS: u32 = 2;

/// Pause between probe attempts
const CONNECTIVITY_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Probes a dataset, retrying up to [`CONNECTIVITY_ATTEMPTS`] times.
///
/// Retrying is safe regardless of connector: the probe is a `LIMIT 1` read whose result is
/// discarded, so replaying it has no effect beyond the read itself. The retry is deliberately
/// not conditioned on the error kind — a transport reset is not distinguishable across every
/// connector without connector-specific knowledge, and a genuinely unavailable dataset simply
/// fails twice.
async fn test_connectivity(
    table_provider: &Arc<dyn TableProvider>,
    df: Arc<DataFusion>,
) -> std::result::Result<(), DataFusionError> {
    let mut attempt: u32 = 1;
    loop {
        let err = match scan_one_row(table_provider, &df).await {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };

        if attempt >= CONNECTIVITY_ATTEMPTS {
            return Err(err);
        }

        tracing::debug!(
            "Connectivity probe attempt {attempt} of {CONNECTIVITY_ATTEMPTS} failed, retrying: {err}"
        );
        tokio::time::sleep(CONNECTIVITY_RETRY_DELAY).await;
        attempt += 1;
    }
}

async fn scan_one_row(
    table_provider: &Arc<dyn TableProvider>,
    df: &Arc<DataFusion>,
) -> std::result::Result<(), DataFusionError> {
    let plan = table_provider
        .scan(&df.ctx.state(), None, &[], Some(1))
        .await
        .map_err(find_datafusion_root)?;

    let stream = plan
        .execute(0, df.ctx.state().task_ctx())
        .map_err(find_datafusion_root)?;

    stream
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(find_datafusion_root)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::{builder::RuntimeBuilder, status::RuntimeStatus};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        catalog::MemorySchemaProvider, catalog::SchemaProvider, datasource::MemTable,
    };
    use std::sync::Arc;
    use tokio::runtime::Handle;

    #[tokio::test]
    async fn test_register_dataset_with_schema() {
        let app = app::AppBuilder::new("test").build();
        let runtime = RuntimeBuilder::new().build().await;
        let accelerator_engine_registry = runtime.accelerator_engine_registry();
        let df = create_test_datafusion(accelerator_engine_registry);

        let dataset = DatasetBuilder::try_new("spice.ai".to_string(), "foo.dataset_name")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("Failed to build dataset");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let table_provider =
            MemTable::try_new(schema, vec![vec![]]).expect("to create table provider");
        df.ctx
            .register_table(dataset.name.clone(), Arc::new(table_provider))
            .expect("to register table provider");

        let monitor = DatasetsHealthMonitor::new(Arc::clone(&df));

        monitor
            .register_dataset(&dataset)
            .await
            .expect("should register dataset");

        monitor.deregister_dataset(&dataset.name.to_string()).await;
    }

    fn test_availability_info(table_ref: &TableReference) -> DatasetAvailabilityInfo {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("memtable"));
        DatasetAvailabilityInfo::new(table_ref.clone(), table_provider, Duration::from_mins(1))
    }

    /// A failed probe flips a Ready dataset to Error, and a later successful
    /// probe restores it to Ready — this is what surfaces via
    /// `GET /v1/datasets?status=true`.
    #[tokio::test]
    async fn availability_result_drives_dataset_status() {
        let status = RuntimeStatus::new();
        let table_ref = TableReference::bare("orders");
        status.update_dataset(&table_ref, ComponentStatus::Ready);

        let info = test_availability_info(&table_ref);
        let monitored: Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        monitored
            .lock()
            .await
            .insert(info.name.clone(), Arc::new(info.clone()));

        // Source unavailable -> Error (with message).
        update_dataset_availability_info(
            &monitored,
            &status,
            &info,
            AvailabilityVerificationResult::Unavailable(
                SystemTime::now(),
                "connection refused".to_string(),
            ),
        )
        .await;
        let errored = status.get_dataset_status(&table_ref).expect("status");
        assert!(errored.is_error(), "expected Error, got {errored:?}");
        assert!(
            errored
                .error_message()
                .is_some_and(|m| m.contains("connection refused")),
            "error message should carry the source failure"
        );

        // Source reachable again -> Ready.
        update_dataset_availability_info(
            &monitored,
            &status,
            &info,
            AvailabilityVerificationResult::Available(SystemTime::now()),
        )
        .await;
        assert_eq!(
            status.get_dataset_status(&table_ref),
            Some(ComponentStatus::Ready)
        );
    }

    /// The monitor must not clobber a transient lifecycle state (e.g. a dataset
    /// mid-reload) with Error.
    #[tokio::test]
    async fn availability_does_not_clobber_transient_status() {
        let status = RuntimeStatus::new();
        let table_ref = TableReference::bare("orders");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        let info = test_availability_info(&table_ref);
        let monitored: Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        monitored
            .lock()
            .await
            .insert(info.name.clone(), Arc::new(info.clone()));

        update_dataset_availability_info(
            &monitored,
            &status,
            &info,
            AvailabilityVerificationResult::Unavailable(SystemTime::now(), "boom".to_string()),
        )
        .await;

        assert_eq!(
            status.get_dataset_status(&table_ref),
            Some(ComponentStatus::Initializing),
            "a probe failure must not override a non-Ready lifecycle state"
        );
    }

    fn create_test_datafusion(
        accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    ) -> Arc<DataFusion> {
        let df = Arc::new(
            DataFusion::builder(
                RuntimeStatus::new(),
                accelerator_engine_registry,
                Handle::current(),
            )
            .build(),
        );

        let catalog = df.ctx.catalog("spice").expect("default catalog is spice");

        let foo_schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        catalog
            .register_schema("foo", Arc::clone(&foo_schema))
            .expect("to register schema");
        df
    }
}
