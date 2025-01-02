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
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::array::{AsArray, RecordBatch};
use datafusion::{datasource::TableProvider, error::DataFusionError, sql::TableReference};
use futures::{future::join_all, stream::TryStreamExt};
use opentelemetry::KeyValue;
use snafu::{ResultExt, Snafu};
use tokio::sync::Mutex;
use tracing_futures::Instrument;

use crate::{
    component::dataset::Dataset,
    datafusion::{error::find_datafusion_root, DataFusion},
    metrics,
};

const DATASETS_AVAILABILITY_CHECK_INTERVAL_SECONDS: u64 = 60; // every minute
const DATASET_UNAVAILABLE_THRESHOLD_MINUTES: u64 = 10;
const DATASET_UNAVAILABLE_THRESHOLD_SECONDS: u64 = DATASET_UNAVAILABLE_THRESHOLD_MINUTES * 60; // 10 minutes

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read the table.\n{source}"))]
    UnableToGetTable { source: DataFusionError },

    #[snafu(display("{source}"))]
    DataFusionQuery {
        source: crate::datafusion::query::Error,
    },

    #[snafu(display("Failed to get recently access datasets.\n{source}"))]
    UnableToGetRecentlyAccessedDatasets { source: DataFusionError },

    #[snafu(display("Spice received an unexpected data type from a `task_history` query: {data_type}\nThis is likely a bug in Spice, which can be reported here: https://github.com/spiceai/spiceai/issues"))]
    UnexpectedDataType {
        data_type: arrow::datatypes::DataType,
    },
}

#[derive(Clone)]
struct DatasetAvailabilityInfo {
    name: String,
    table_provider: Arc<dyn TableProvider>,
    last_available_time: SystemTime,
}

impl Debug for DatasetAvailabilityInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetAvailabilityInfo")
            .field("name", &self.name)
            .field("last_available_time", &self.last_available_time)
            .finish_non_exhaustive()
    }
}

impl DatasetAvailabilityInfo {
    pub fn new(name: String, table_provider: Arc<dyn TableProvider>) -> Self {
        Self {
            name,
            table_provider,
            last_available_time: SystemTime::now(),
        }
    }
}

enum AvailabilityVerificationResult {
    Available,
    Unavailable(SystemTime, String),
}

pub struct DatasetsHealthMonitor {
    df: Arc<DataFusion>,
    monitored_datasets: Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
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

        let dataset_name = &dataset.name.to_string();

        tracing::debug!("Registering dataset {dataset_name} for periodic availability check");

        let table_provider = self.get_table_provider(dataset.name.clone()).await?;

        let mut monitored_datasets = self.monitored_datasets.lock().await;
        monitored_datasets.insert(
            dataset_name.to_string(),
            Arc::new(DatasetAvailabilityInfo::new(
                dataset_name.to_string(),
                table_provider,
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

    // returns a list of dataset names that had successful queries against them in the last 10 minutes
    pub async fn get_recently_accessed_datasets(
        df: Arc<DataFusion>,
    ) -> Result<Arc<HashSet<String>>> {
        let query = format!(
            "
SELECT labels.datasets AS datasets
FROM runtime.task_history
WHERE labels.datasets IS NOT NULL
AND NOW() < end_time + INTERVAL '{DATASET_UNAVAILABLE_THRESHOLD_MINUTES}' MINUTE
AND labels.error_code IS NULL"
        );
        let query_result = df
            .query_builder(&query)
            .build()
            .run()
            .await
            .context(DataFusionQuerySnafu)?;
        let datasets_with_recent_activity = query_result
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(find_datafusion_root)
            .context(UnableToGetRecentlyAccessedDatasetsSnafu)?;

        let mut datasets_with_recent_activity_set: HashSet<String> = HashSet::new();

        for record_batch in &datasets_with_recent_activity {
            let column = record_batch.column(0);
            let datasets: Vec<&str> = match column.data_type() {
                arrow::datatypes::DataType::Utf8 => {
                    column.as_string::<i32>().into_iter().flatten().collect()
                }
                arrow::datatypes::DataType::LargeUtf8 => {
                    column.as_string::<i64>().into_iter().flatten().collect()
                }
                dt => {
                    return Err(Error::UnexpectedDataType {
                        data_type: dt.clone(),
                    })
                }
            };

            for dataset in datasets {
                for name in dataset.split(',') {
                    datasets_with_recent_activity_set.insert(name.to_string());
                }
            }
        }

        Ok(Arc::new(datasets_with_recent_activity_set))
    }

    pub fn start(&self) {
        tracing::debug!("Starting datasets availability monitoring");
        let monitored_datasets = Arc::clone(&self.monitored_datasets);
        let df = Arc::clone(&self.df);
        let is_task_history_enabled = self.is_task_history_enabled;
        tokio::spawn(async move {
            // no need to check status immediately after start
            tokio::time::sleep(tokio::time::Duration::from_secs(
                DATASET_UNAVAILABLE_THRESHOLD_SECONDS,
            ))
            .await;

            loop {
                tracing::debug!("Checking datasets availability");
                // Only datasets without recent activity/availability
                let datasets_to_check = datasets_for_availability_check(&monitored_datasets).await;

                // check `task_history` first to exlude anything that had a successful query in the last 10 minutes
                let recently_accessed_datasets = if is_task_history_enabled {
                    match Self::get_recently_accessed_datasets(Arc::clone(&df)).await {
                        Ok(datasets) => datasets,
                        Err(e) => {
                            tracing::warn!("{e}");
                            Arc::new(HashSet::new())
                        }
                    }
                } else {
                    Arc::new(HashSet::new())
                };

                tracing::debug!("Datasets excluded from availability check as they were recently successfully accessed: {recently_accessed_datasets:?}");

                // subset them from the datasets to check
                let datasets_to_check: Vec<_> = datasets_to_check
                    .into_iter()
                    .filter(|item| !recently_accessed_datasets.contains(&item.name))
                    .collect();

                tracing::debug!("Datasets to check: {datasets_to_check:?}");

                let tasks: Vec<_> = datasets_to_check
                    .into_iter()
                    .map(|item| {
                        let df = Arc::clone(&df);
                        let monitored_datasets = Arc::clone(&monitored_datasets);
                        tokio::spawn(async move {
                            tracing::trace!("Verifying connectivity for dataset {}", &item.name);

                            let span = tracing::span!(target: "task_history", tracing::Level::INFO, "test_connectivity", input = &item.name);
                            let connectivity_test_result =
                                match test_connectivity(&item.table_provider, df).instrument(span.clone()).await
                                {
                                    Ok(()) => AvailabilityVerificationResult::Available,
                                    Err(err) => {
                                        let err_message = match err.find_root() {
                                            DataFusionError::Execution(e) => e.to_string(),
                                            _ => err.to_string(),
                                        };

                                        tracing::error!(target: "task_history", parent: &span, "{err_message}");
                                        AvailabilityVerificationResult::Unavailable(
                                        item.last_available_time,
                                        err_message,
                                    )},
                                };

                            update_dataset_availability_info(
                                &monitored_datasets,
                                &item.name,
                                connectivity_test_result,
                            )
                            .await;
                        })
                    })
                    .collect();

                join_all(tasks).await;

                tracing::debug!("Finished checking datasets availability");

                tokio::time::sleep(Duration::from_secs(
                    DATASETS_AVAILABILITY_CHECK_INTERVAL_SECONDS,
                ))
                .await;
            }
        });
    }
}

async fn update_dataset_availability_info(
    monitored_datasets: &Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
    dataset_name: &String,
    test_result: AvailabilityVerificationResult,
) {
    match test_result {
        AvailabilityVerificationResult::Available => {
            tracing::debug!("Successfully verified access to federated dataset {dataset_name}");
            let mut monitored_datasets_lock = monitored_datasets.lock().await;
            if let Some(dataset) = monitored_datasets_lock.get_mut(dataset_name) {
                Arc::make_mut(dataset).last_available_time = SystemTime::now();
            }
            report_dataset_unavailable_time(dataset_name, None);
        }
        AvailabilityVerificationResult::Unavailable(last_available_time, err) => {
            tracing::warn!("Failed to verify the dataset {dataset_name} was available.\n{err}\n");
            report_dataset_unavailable_time(dataset_name, Some(last_available_time));
        }
    }
}

fn report_dataset_unavailable_time(dataset_name: &String, last_available_time: Option<SystemTime>) {
    let labels = vec![KeyValue::new("dataset", dataset_name.to_string())];

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

async fn datasets_for_availability_check(
    datasets: &Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
) -> Vec<Arc<DatasetAvailabilityInfo>> {
    let now = SystemTime::now();
    let datasets = datasets.lock().await;
    datasets
        .iter()
        .filter(|(_, item)| {
            now.duration_since(item.last_available_time)
                .unwrap_or_default()
                .as_secs()
                > DATASET_UNAVAILABLE_THRESHOLD_SECONDS
        })
        .map(|(_, item)| Arc::clone(item))
        .collect::<Vec<_>>()
}

async fn test_connectivity(
    table_provider: &Arc<dyn TableProvider>,
    df: Arc<DataFusion>,
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
    use crate::{component::dataset::Dataset, status::RuntimeStatus};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        catalog::SchemaProvider, catalog_common::MemorySchemaProvider, datasource::MemTable,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_register_dataset_with_schema() {
        let df = create_test_datafusion();

        let dataset = Dataset::try_new("spice.ai".to_string(), "foo.dataset_name")
            .expect("to create dataset");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let table_provider = MemTable::try_new(schema, vec![]).expect("to create table provider");
        df.ctx
            .register_table(dataset.name.clone(), Arc::new(table_provider))
            .expect("to register table provider");

        let monitor = DatasetsHealthMonitor::new(Arc::clone(&df));

        assert!(monitor.register_dataset(&dataset).await.is_ok());

        monitor.deregister_dataset(&dataset.name.to_string()).await;
    }

    fn create_test_datafusion() -> Arc<DataFusion> {
        let df = Arc::new(DataFusion::builder(RuntimeStatus::new()).build());

        let catalog = df.ctx.catalog("spice").expect("default catalog is spice");

        let foo_schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        catalog
            .register_schema("foo", Arc::clone(&foo_schema))
            .expect("to register schema");
        df
    }
}
