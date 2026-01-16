/*
Copyright 2026 The Spice.ai OSS Authors
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
use crate::accelerated_table::SnapshotCreateTrigger;
use arrow_schema::Schema;
use datafusion::common::TableReference;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::{SnapshotManager, metrics as snapshot_metrics};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct SnapshotCreationConfig {
    pub manager: Arc<SnapshotManager>,
    pub create_trigger: SnapshotCreateTrigger,
}

impl SnapshotCreationConfig {
    #[must_use]
    pub fn new(manager: Arc<SnapshotManager>, create_trigger: SnapshotCreateTrigger) -> Self {
        Self {
            manager,
            create_trigger,
        }
    }
}

pub type SnapshotCallback =
    Arc<Mutex<Box<dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>>;

pub fn spawn_snapshot_interval_task(
    snapshots_create_interval: Option<Duration>,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: TableReference,
    federated_schema: Arc<Schema>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval = snapshots_create_interval?;
    let checkpointer = checkpointer?;
    let snapshot_manager = snapshot_manager?;

    tracing::info!(
        "Snapshots for dataset {dataset_name} will be created every {}s",
        interval.as_secs()
    );

    Some(tokio::spawn(async move {
        let mut initial_delay = interval;
        if let Ok(Some(last_checkpoint_time)) = checkpointer.last_checkpoint_time().await
            && let Ok(elapsed) = SystemTime::now().duration_since(last_checkpoint_time)
        {
            if elapsed < interval {
                initial_delay = interval - elapsed;
            } else {
                initial_delay = Duration::from_secs(0);
            }
        }

        if !initial_delay.is_zero() {
            sleep(initial_delay).await;
        }

        loop {
            create_checkpoint_and_snapshot(
                &checkpointer,
                Some(&snapshot_manager),
                &federated_schema,
                &accelerator_write_mutex,
                &dataset_name,
            )
            .await;

            sleep(interval).await;
        }
    }))
}

pub fn create_periodic_snapshot_callback(
    batches: i64,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: &TableReference,
    federated_schema: Arc<Schema>,
) -> Option<SnapshotCallback> {
    match (checkpointer, snapshot_manager) {
        (Some(checkpointer), Some(snapshot_manager)) => {
            let dataset_name = dataset_name.clone();

            tracing::info!(
                "Snapshots for dataset {dataset_name} will be created every {batches} batch updates"
            );

            // Track number of processed batches since last snapshot
            let batches_processed = Arc::new(RwLock::new(0i64));

            let callback = Arc::new(Mutex::new(Box::new(move || {
                let checkpointer = Arc::clone(&checkpointer);
                let snapshot_manager = Arc::clone(&snapshot_manager);
                let accelerator_write_mutex = Arc::clone(&accelerator_write_mutex);
                let batches_processed = Arc::clone(&batches_processed);
                let federated_schema = Arc::<Schema>::clone(&federated_schema);
                let dataset_name = dataset_name.clone();

                Box::pin(async move {
                    let mut batches_processed_value = batches_processed.write().await;

                    *batches_processed_value += 1;
                    if *batches_processed_value >= batches {
                        *batches_processed_value = 0;

                        create_checkpoint_and_snapshot(
                            &checkpointer,
                            Some(&snapshot_manager),
                            &federated_schema,
                            &accelerator_write_mutex,
                            &dataset_name,
                        )
                        .await;
                    }
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })
                as Box<dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>));

            Some(callback)
        }
        _ => None,
    }
}

pub async fn create_checkpoint_and_snapshot(
    checkpointer: &Arc<dyn DatasetCheckpointer>,
    snapshot_manager: Option<&Arc<SnapshotManager>>,
    federated_schema: &Arc<Schema>,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    dataset_name: &TableReference,
) {
    let lock_guard = Arc::clone(accelerator_write_mutex).lock_owned().await;
    if let Err(e) = checkpointer.checkpoint(federated_schema).await {
        tracing::warn!("Failed to checkpoint dataset {dataset_name}: {e}");
        return;
    }

    if let Some(snapshot_manager) = snapshot_manager {
        if let Err(e) = snapshot_manager
            .create_snapshot(federated_schema, lock_guard)
            .await
        {
            let dataset_label = dataset_name.to_string();
            snapshot_metrics::record_snapshot_failure(&dataset_label);
            tracing::warn!("Failed to create snapshot for dataset {dataset_name}: {e}");
        } else {
            tracing::info!("Successfully created snapshot for dataset: {dataset_name}");
        }
    }
}
