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
use crate::accelerated_table::refresh::Refresh;
use crate::dataaccelerator::AccelerationSource;
use crate::dataaccelerator::DataAccelerator;
use crate::dataaccelerator::ReloadProviderFactory;
use crate::dataaccelerator::swappable::SwappableTableProvider;
use crate::status::RuntimeStatus;
use arrow_schema::Schema;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::{ForceCreate, SnapshotManager, metrics as snapshot_metrics};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;

/// Per-dataset state required to drive `refresh_mode: snapshot`.
///
/// This bundle is built once during dataset registration and threaded down
/// through `Refresher` -> `RefreshTaskBuilder` -> `RefreshTask`. The refresh
/// task uses it on every tick to:
///   1. Compare the remote `current_snapshot_id` against `current_snapshot_id`.
///   2. Download and reload only when a strictly newer snapshot is available.
///   3. Atomically swap the live `TableProvider` via `swappable_provider`.
#[derive(Clone)]
pub struct SnapshotRefreshState {
    pub manager: Arc<SnapshotManager>,
    pub accelerator: Arc<dyn DataAccelerator>,
    pub source: Arc<dyn AccelerationSource>,
    pub swappable_provider: Arc<SwappableTableProvider>,
    /// Factory that re-runs `create_accelerator_table` for this dataset to
    /// build a fresh provider over the on-disk snapshot file.
    pub provider_factory: ReloadProviderFactory,
    /// The currently-loaded snapshot id, if any. `None` means no snapshot has
    /// been loaded yet for this dataset (e.g. fresh start with no bootstrap).
    /// Wrapped in a sync `Mutex` because updates are infrequent (once per
    /// successful reload) and the inner `Option<u64>` is `Copy` so reads are
    /// trivial. Snapshot ids are not constrained: id `0` is a valid first
    /// snapshot, so `Option<u64>` is the correct representation rather than
    /// using a sentinel value.
    pub current_snapshot_id: Arc<StdMutex<Option<u64>>>,
}

impl std::fmt::Debug for SnapshotRefreshState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id = self.current_snapshot_id.lock().map(|g| *g).unwrap_or(None);
        f.debug_struct("SnapshotRefreshState")
            .field("current_snapshot_id", &id)
            .finish_non_exhaustive()
    }
}

impl SnapshotRefreshState {
    /// Returns the currently-loaded snapshot id, or `None` if no snapshot has
    /// been loaded yet.
    #[must_use]
    pub fn current_loaded_id(&self) -> Option<u64> {
        self.current_snapshot_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied()
    }

    /// Records `snapshot_id` as the most recently loaded snapshot id.
    pub fn set_current_loaded_id(&self, snapshot_id: u64) {
        let mut guard = self
            .current_snapshot_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(snapshot_id);
    }
}

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
/// The task uses the checkpointer's `last_checkpoint_time()` to determine when the next
/// snapshot should be created:
/// - If `snapshots_create_interval` has passed since the last checkpoint, create immediately
/// - Otherwise, schedule the first snapshot at `last_checkpoint_time + snapshots_create_interval`
///
/// If no previous checkpoint exists, a snapshot is created immediately after the runtime is ready.
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
    last_updated_at: Arc<AtomicI64>,
    accelerator: Option<Arc<dyn TableProvider>>,
    refresh: Arc<RwLock<Refresh>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval_duration = snapshots_create_interval?;
    let checkpointer = checkpointer?;
    let snapshot_manager = snapshot_manager?;

    tracing::info!(
        "Snapshots for dataset {dataset_name} will be created every {}s",
        interval_duration.as_secs()
    );

    Some(tokio::spawn(async move {
        // Wait for the runtime to become ready
        runtime_status.wait_for_ready().await;

        // Determine the initial delay based on last checkpoint time
        let initial_delay = if bootstrap_status.is_bootstrapped() {
            match checkpointer.last_checkpoint_time().await {
                Ok(Some(last_checkpoint)) => {
                    let elapsed = last_checkpoint.elapsed().unwrap_or(Duration::ZERO);
                    if elapsed >= interval_duration {
                        Duration::ZERO
                    } else {
                        interval_duration
                            .checked_sub(elapsed)
                            .unwrap_or(Duration::ZERO)
                    }
                }
                Ok(None) | Err(_) => Duration::ZERO,
            }
        } else {
            Duration::ZERO
        };

        if !initial_delay.is_zero() {
            tokio::time::sleep(initial_delay).await;
        }

        let refresh_sql = refresh
            .read()
            .await
            .sql
            .as_ref()
            .map(super::refresh::RefreshSQL::to_sql);
        create_checkpoint_and_snapshot(
            &checkpointer,
            Some(&snapshot_manager),
            &federated_schema,
            &accelerator_write_mutex,
            &dataset_name,
            &last_updated_at,
            // Force creation when interval already elapsed.
            // Even though this may create a snapshot identical to the last one, we do this to avoid
            // losing snapshots due to potential object storage retention policy.
            // Consider use case: periodic
            ForceCreate(initial_delay.is_zero()),
            accelerator.as_ref(),
            refresh_sql.as_deref(),
        )
        .await;

        let mut ticker = interval(interval_duration);
        // Consume the first tick which returns immediately per tokio::time::interval behavior
        ticker.tick().await;

        loop {
            // Wait for the next snapshot interval (accounting for time spent during previous snapshot creation)
            ticker.tick().await;

            let refresh_sql = refresh
                .read()
                .await
                .sql
                .as_ref()
                .map(super::refresh::RefreshSQL::to_sql);
            create_checkpoint_and_snapshot(
                &checkpointer,
                Some(&snapshot_manager),
                &federated_schema,
                &accelerator_write_mutex,
                &dataset_name,
                &last_updated_at,
                ForceCreate(false),
                accelerator.as_ref(),
                refresh_sql.as_deref(),
            )
            .await;
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
    bootstrap_status: crate::dataaccelerator::BootstrapStatus,
    last_updated_at: Arc<AtomicI64>,
    accelerator: Option<Arc<dyn TableProvider>>,
    refresh: Arc<RwLock<Refresh>>,
) -> Option<SnapshotCallback> {
    match (checkpointer, snapshot_manager) {
        (Some(checkpointer), Some(snapshot_manager)) => {
            let dataset_name = dataset_name.clone();

            tracing::info!(
                "Snapshots for dataset {dataset_name} will be created every {batches} batch updates"
            );

            // Track number of processed batches since last snapshot
            let batches_processed = Arc::new(RwLock::new(0i64));

            // Gates when checkpoint counting can start after runtime is ready.
            // Set to true after the initial snapshot task completes (regardless of success).
            let checkpoint_counting_enabled = Arc::new(AtomicBool::new(false));

            // Spawn a task to create initial snapshot once runtime is ready
            let checkpoint_counting_enabled_clone = Arc::clone(&checkpoint_counting_enabled);
            let dataset_name_clone = dataset_name.clone();
            let last_updated_at_clone = Arc::clone(&last_updated_at);
            let checkpointer_clone = Arc::clone(&checkpointer);
            let snapshot_manager_clone = Arc::clone(&snapshot_manager);
            let federated_schema_clone = Arc::clone(&federated_schema);
            let accelerator_write_mutex_clone = Arc::clone(&accelerator_write_mutex);
            let accelerator_clone = accelerator.clone();
            let refresh_clone = Arc::clone(&refresh);
            tokio::spawn(async move {
                runtime_status.wait_for_ready().await;
                if !bootstrap_status.is_bootstrapped() {
                    let refresh_sql = refresh_clone
                        .read()
                        .await
                        .sql
                        .as_ref()
                        .map(super::refresh::RefreshSQL::to_sql);
                    create_checkpoint_and_snapshot(
                        &checkpointer_clone,
                        Some(&snapshot_manager_clone),
                        &federated_schema_clone,
                        &accelerator_write_mutex_clone,
                        &dataset_name_clone,
                        &last_updated_at_clone,
                        ForceCreate(true),
                        accelerator_clone.as_ref(),
                        refresh_sql.as_deref(),
                    )
                    .await;
                }
                checkpoint_counting_enabled_clone.store(true, Ordering::Release);
                tracing::debug!(
                    "Batch-based snapshot counting for {dataset_name_clone} starting after runtime ready"
                );
            });

            let callback = Arc::new(Mutex::new(Box::new(move || {
                let checkpointer = Arc::clone(&checkpointer);
                let snapshot_manager = Arc::clone(&snapshot_manager);
                let accelerator_write_mutex = Arc::clone(&accelerator_write_mutex);
                let batches_processed = Arc::clone(&batches_processed);
                let federated_schema = Arc::<Schema>::clone(&federated_schema);
                let dataset_name = dataset_name.clone();
                let checkpoint_counting_enabled = Arc::clone(&checkpoint_counting_enabled);
                let last_updated_at = Arc::clone(&last_updated_at);
                let accelerator = accelerator.clone();
                let refresh = Arc::clone(&refresh);

                Box::pin(async move {
                    let mut batches_processed_value = batches_processed.write().await;

                    // Only count batches after checkpoint counting is enabled
                    if !checkpoint_counting_enabled.load(Ordering::Acquire) {
                        return;
                    }

                    *batches_processed_value += 1;
                    if *batches_processed_value >= batches {
                        *batches_processed_value = 0;

                        let refresh_sql = refresh
                            .read()
                            .await
                            .sql
                            .as_ref()
                            .map(super::refresh::RefreshSQL::to_sql);
                        create_checkpoint_and_snapshot(
                            &checkpointer,
                            Some(&snapshot_manager),
                            &federated_schema,
                            &accelerator_write_mutex,
                            &dataset_name,
                            &last_updated_at,
                            ForceCreate(false),
                            accelerator.as_ref(),
                            refresh_sql.as_deref(),
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

#[expect(clippy::too_many_arguments)]
pub async fn create_checkpoint_and_snapshot(
    checkpointer: &Arc<dyn DatasetCheckpointer>,
    snapshot_manager: Option<&Arc<SnapshotManager>>,
    federated_schema: &Arc<Schema>,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    dataset_name: &TableReference,
    last_updated_at: &Arc<AtomicI64>,
    force_create: ForceCreate,
    accelerator: Option<&Arc<dyn TableProvider>>,
    refresh_sql: Option<&str>,
) {
    let lock_guard = Arc::clone(accelerator_write_mutex).lock_owned().await;
    if let Err(e) = checkpointer.checkpoint(federated_schema, refresh_sql).await {
        tracing::warn!("Failed to checkpoint dataset {dataset_name}: {e}");
        return;
    }

    if let Some(snapshot_manager) = snapshot_manager {
        let updated_at = match last_updated_at.load(Ordering::Acquire) {
            0 => None,
            i => Some(i),
        };

        // Get the current row count from the accelerator using the `DataFrame` API.
        // This must be done after checkpoint while holding the write lock to ensure atomicity.
        let row_count = if let Some(accelerator) = accelerator {
            get_row_count(accelerator, dataset_name).await
        } else {
            None
        };

        match snapshot_manager
            .create_snapshot(
                federated_schema,
                lock_guard,
                updated_at,
                row_count,
                force_create,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let dataset_label = dataset_name.to_string();
                snapshot_metrics::record_snapshot_failure(&dataset_label);
                tracing::warn!(dataset = %dataset_name, error = %e, "Failed to create snapshot");
            }
        }
    }
}

/// Gets the row count from the accelerator using the `DataFrame` API.
///
/// Returns `None` if the row count cannot be determined (e.g., due to errors).
async fn get_row_count(
    accelerator: &Arc<dyn TableProvider>,
    dataset_name: &TableReference,
) -> Option<u64> {
    let ctx = SessionContext::new();
    let table_name = dataset_name.table();

    if ctx
        .register_table(table_name, Arc::clone(accelerator))
        .is_err()
    {
        tracing::debug!(dataset = %dataset_name, "Failed to register accelerator table for row count query");
        return None;
    }

    match ctx.table(table_name).await {
        Ok(df) => match df.count().await {
            Ok(count) => {
                if let Ok(row_count) = u64::try_from(count) {
                    Some(row_count)
                } else {
                    tracing::debug!(dataset = %dataset_name, "Row count for snapshot exceeds u64::MAX; proceeding without it");
                    None
                }
            }
            Err(e) => {
                tracing::debug!(dataset = %dataset_name, error = %e, "Failed to get row count for snapshot; proceeding without it");
                None
            }
        },
        Err(e) => {
            tracing::debug!(dataset = %dataset_name, error = %e, "Failed to get DataFrame for row count query");
            None
        }
    }
}
