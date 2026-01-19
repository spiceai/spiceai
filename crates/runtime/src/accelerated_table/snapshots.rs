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
use crate::status::RuntimeStatus;
use arrow_schema::Schema;
use datafusion::common::TableReference;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::{SnapshotManager, metrics as snapshot_metrics};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, sleep};

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

/// Spawns a task that periodically creates snapshots at the specified interval.
///
/// If `runtime_status` is provided, the task will wait for the dataset to be ready
/// before starting the snapshot interval loop. This prevents creating snapshots before
/// the dataset has finished its initial load or bootstrap.
///
/// If `bootstrap_status` indicates the dataset was bootstrapped, the first snapshot will be delayed
/// by the full interval after the dataset becomes ready (to avoid creating a snapshot immediately after bootstrap).
#[expect(clippy::too_many_arguments)]
pub fn spawn_snapshot_interval_task(
    snapshots_create_interval: Option<Duration>,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: TableReference,
    federated_schema: Arc<Schema>,
    runtime_status: Arc<RuntimeStatus>,
    bootstrap_status: crate::dataaccelerator::BootstrapStatus,
    last_updated_at: Option<Arc<AtomicI64>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval_duration = snapshots_create_interval?;
    let checkpointer = checkpointer?;
    let snapshot_manager = snapshot_manager?;

    tracing::info!(
        "Snapshots for dataset {dataset_name} will be created every {}s",
        interval_duration.as_secs()
    );

    Some(tokio::spawn(async move {
        // Wait for the dataset to be ready before starting snapshot creation
        tracing::debug!(
            "Snapshot interval task for {dataset_name} waiting for dataset to be ready"
        );
        // Wait for the dataset to become ready
        runtime_status.wait_for_dataset_ready(&dataset_name).await;
        tracing::debug!("Snapshot interval task for {dataset_name} starting after dataset ready");

        // Determine initial delay based on bootstrapping status and checkpoint time
        let initial_delay = if bootstrap_status.is_bootstrapped() {
            // If dataset was bootstrapped, create a snapshot after the full interval after dataset is ready
            let mut delay = interval_duration;
            if let Ok(Some(last_checkpoint_time)) = checkpointer.last_checkpoint_time().await
                && let Ok(elapsed) = SystemTime::now().duration_since(last_checkpoint_time)
            {
                // Calculate delay based on last checkpoint time
                if elapsed < interval_duration {
                    delay = interval_duration - elapsed;
                } else {
                    delay = Duration::from_secs(0);
                }
            }
            delay
        } else {
            // When no bootstrapping, create a snapshot immediately after dataset is ready
            Duration::from_secs(0)
        };

        if !initial_delay.is_zero() {
            sleep(initial_delay).await;
        }

        let mut ticker = interval(interval_duration);

        loop {
            create_checkpoint_and_snapshot(
                &checkpointer,
                Some(&snapshot_manager),
                &federated_schema,
                &accelerator_write_mutex,
                &dataset_name,
                last_updated_at.as_ref(),
            )
            .await;

            // Wait for the next snapshot interval (accounting for time spent during snapshot creation)
            ticker.tick().await;
        }
    }))
}

/// Creates a callback that triggers snapshot creation after a specified number of batch updates.
///
/// If `runtime_status` is provided, batch counting will only start after the dataset
/// is ready. This prevents counting batches during the initial load/bootstrap phase.
#[expect(clippy::too_many_arguments)]
pub fn create_periodic_snapshot_callback(
    batches: i64,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: &TableReference,
    federated_schema: Arc<Schema>,
    runtime_status: Arc<RuntimeStatus>,
    last_updated_at: Option<Arc<AtomicI64>>,
) -> Option<SnapshotCallback> {
    match (checkpointer, snapshot_manager) {
        (Some(checkpointer), Some(snapshot_manager)) => {
            let dataset_name = dataset_name.clone();

            tracing::info!(
                "Snapshots for dataset {dataset_name} will be created every {batches} batch updates"
            );

            // Track number of processed batches since last snapshot
            let batches_processed = Arc::new(RwLock::new(0i64));

            // Track whether the dataset is ready (batch counting should start)
            let dataset_ready = Arc::new(AtomicBool::new(false));

            // Spawn a task to set dataset_ready when notified
            let dataset_ready_clone = Arc::clone(&dataset_ready);
            let dataset_name_clone = dataset_name.clone();
            tokio::spawn(async move {
                runtime_status
                    .wait_for_dataset_ready(&dataset_name_clone)
                    .await;
                dataset_ready_clone.store(true, Ordering::Release);
                tracing::debug!(
                    "Batch-based snapshot counting for {dataset_name_clone} starting after dataset ready"
                );
            });

            let callback = Arc::new(Mutex::new(Box::new(move || {
                let checkpointer = Arc::clone(&checkpointer);
                let snapshot_manager = Arc::clone(&snapshot_manager);
                let accelerator_write_mutex = Arc::clone(&accelerator_write_mutex);
                let batches_processed = Arc::clone(&batches_processed);
                let federated_schema = Arc::<Schema>::clone(&federated_schema);
                let dataset_name = dataset_name.clone();
                let dataset_ready = Arc::clone(&dataset_ready);
                let last_updated_at = last_updated_at.clone();

                Box::pin(async move {
                    // Only count batches after the dataset is ready
                    if !dataset_ready.load(Ordering::Acquire) {
                        return;
                    }

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
                            last_updated_at.as_ref(),
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
    last_updated_at: Option<&Arc<AtomicI64>>,
) {
    let lock_guard = Arc::clone(accelerator_write_mutex).lock_owned().await;
    if let Err(e) = checkpointer.checkpoint(federated_schema).await {
        tracing::warn!("Failed to checkpoint dataset {dataset_name}: {e}");
        return;
    }

    if let Some(snapshot_manager) = snapshot_manager {
        let updated_at = last_updated_at.map(|a| a.load(Ordering::Acquire));

        match snapshot_manager
            .create_snapshot(federated_schema, lock_guard, updated_at)
            .await
        {
            Ok(Some(_)) => {
                tracing::info!("Successfully created snapshot for dataset: {dataset_name}");
            }
            Ok(None) => {
                // Snapshot was skipped (no updates) - metric already recorded
            }
            Err(e) => {
                let dataset_label = dataset_name.to_string();
                snapshot_metrics::record_snapshot_failure(&dataset_label);
                tracing::warn!("Failed to create snapshot for dataset {dataset_name}: {e}");
            }
        }
    }
}
