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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::array::RecordBatch;
use datafusion::{
    datasource::TableProvider, error::DataFusionError, execution::context::SessionContext,
    sql::TableReference,
};
use futures::{future::join_all, stream::TryStreamExt};
use snafu::{ResultExt, Snafu};
use tokio::sync::Mutex;

use crate::component::dataset::Dataset;

const DATASETS_AVAILABILITY_CHECK_INTERVAL_SECONDS: u64 = 60; // every minute
const DATASET_UNAVAILABLE_THRESHOLD_SECONDS: u64 = 10 * 60; // 10 minutes

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get table: {source}"))]
    UnableToGetTable { source: DataFusionError },
}

#[derive(Clone)]
pub struct DatasetAvailabilityInfo {
    name: String,
    table_provider: Arc<dyn TableProvider>,
    last_available_time: SystemTime,
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

pub enum AvailabilityVerificationResult {
    Available,
    Unavailable(SystemTime, String),
}

pub struct DatasetsHealthMonitor {
    df_ctx: Arc<SessionContext>,
    monitored_datasets: Arc<Mutex<HashMap<String, Arc<DatasetAvailabilityInfo>>>>,
}

impl DatasetsHealthMonitor {
    #[must_use]
    pub fn new(df_ctx: Arc<SessionContext>) -> Self {
        Self {
            df_ctx,
            monitored_datasets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_dataset(&self, dataset: &Dataset) -> Result<()> {
        if dataset.is_accelerated() {
            return Ok(());
        }

        let dataset_name = &dataset.name.to_string();

        tracing::debug!("Registering dataset {dataset_name:?} for periodic availability check");

        let table_provider = self.get_table_provider(dataset_name).await?;

        let mut monitored_datasets = self.monitored_datasets.lock().await;
        monitored_datasets.insert(
            dataset_name.to_string(),
            Arc::new(DatasetAvailabilityInfo::new(
                dataset_name.to_string(),
                table_provider,
            )),
        );

        Ok(())
    }

    pub async fn deregister_dataset(&self, dataset_name: &String) {
        tracing::debug!("Removing dataset {dataset_name:?} from periodic availability check");
        let mut monitored_datasets = self.monitored_datasets.lock().await;
        monitored_datasets.remove(dataset_name);
    }

    async fn get_table_provider(&self, dataset_name: &str) -> Result<Arc<dyn TableProvider>> {
        let table = self
            .df_ctx
            .table_provider(TableReference::bare(dataset_name.to_string()))
            .await
            .context(UnableToGetTableSnafu)?;

        Ok(table)
    }

    pub fn start(&self) {
        tracing::debug!("Starting datasets availability monitoring");
        let monitored_datasets = Arc::clone(&self.monitored_datasets);
        let df_ctx = Arc::clone(&self.df_ctx);
        tokio::spawn(async move {
            // no need to check status immediately after start
            tokio::time::sleep(tokio::time::Duration::from_secs(
                DATASET_UNAVAILABLE_THRESHOLD_SECONDS,
            ))
            .await;

            loop {
                // Only datasets without recent activity/availability
                let datasets_to_check = datasets_for_availability_check(&monitored_datasets).await;

                let tasks: Vec<_> = datasets_to_check
                    .into_iter()
                    .map(|item| {
                        let ctx = Arc::clone(&df_ctx);
                        let monitored_datasets = Arc::clone(&monitored_datasets);
                        tokio::spawn(async move {
                            tracing::trace!("Verifying connectivity for dataset {}", &item.name);
                            let connectivity_test_result =
                                match test_connectivity(&item.table_provider, ctx).await {
                                    Ok(()) => AvailabilityVerificationResult::Available,
                                    Err(err) => AvailabilityVerificationResult::Unavailable(
                                        item.last_available_time,
                                        err.to_string(),
                                    ),
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
        }
        AvailabilityVerificationResult::Unavailable(last_available_time, err) => {
            tracing::warn!("Availability verification for dataset {dataset_name} failed: {err}");

            let labels = [("dataset", dataset_name.to_string())];
            metrics::gauge!("datasets_unavailable_time", &labels).set(
                last_available_time
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64(),
            );
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
    ctx: Arc<SessionContext>,
) -> std::result::Result<(), DataFusionError> {
    let plan = table_provider
        .scan(&ctx.state(), None, &[], Some(1))
        .await?;

    let stream = plan.execute(0, ctx.state().task_ctx())?;

    stream.try_collect::<Vec<RecordBatch>>().await?;

    Ok(())
}
