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

use std::sync::Arc;
use std::time::{Duration, Instant};

use snafu::prelude::*;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::CLUSTER_PARTITION_MANAGEMENT_TASK;
use crate::datafusion::DataFusion;
use crate::status::{ComponentStatus, RuntimeStatus};

pub use runtime_cluster::scheduler_task_config::{ConfigError, PartitionManagementConfig};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Partition assignment cycle failed: {source}"))]
    AssignmentCycle {
        source: runtime_cluster::service::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Background task responsible for managing the assignment of accelerated table partitions to executors. Responsible for
/// 1. Discovering new partition values from tables that are accelerated (by querying the underlying source).
/// 2. Adding new partitions to the partition metadata (initially unassigned).
/// 3. Removing partitions that no longer exist in the source and notifying executors to unload them.
/// 4. Assigning unassigned partitions to executors.
pub struct PartitionManagementTask {
    df: Arc<DataFusion>,
    status: Arc<RuntimeStatus>,

    /// How often to run the management cycle
    interval: Duration,

    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
}

impl PartitionManagementTask {
    pub fn new(
        df: Arc<DataFusion>,
        status: Arc<RuntimeStatus>,
        interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            df,
            status,
            interval,
            cancel,
        }
    }

    pub async fn run(self) -> Result<()> {
        tracing::debug!("Starting {CLUSTER_PARTITION_MANAGEMENT_TASK} in background");

        // Seed partition metadata for tables that don't have it yet.
        // This runs once before the periodic loop and is cancellation-aware
        // so it can be interrupted during shutdown (the discovery query against
        // a large source table like S3 can take a long time).
        //
        // The scheduler's `/v1/ready` endpoint will report "not ready" until this completes
        tokio::select! {
            () = self.cancel.cancelled() => {
                tracing::info!("Partition metadata initialization cancelled during shutdown");
                self.status.update_component_status("partition_metadata", ComponentStatus::error_with_message("Cancelled during shutdown"));
                return Ok(());
            }
            result = self.initialize_metadata() => {
                match result {
                    Ok(()) => {
                        self.status.update_component_status("partition_metadata", ComponentStatus::Ready);
                    }
                    Err(err) => {
                        tracing::warn!("Failed to initialize partition metadata: {err}");
                        self.status.update_component_status("partition_metadata", ComponentStatus::error_with_message(format!("Failed to initialize: {err}")));
                    }
                }
            }
        }

        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tracing::debug!("Starting {CLUSTER_PARTITION_MANAGEMENT_TASK} loop");
            tokio::select! {
                () = self.cancel.cancelled() => {
                    tracing::info!("Partition management task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    let cycle_start = Instant::now();

                    match self.run_management_cycle().await {
                        Ok(()) => {
                            self.status.update_component_status("partition_metadata", ComponentStatus::Ready);
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Partition management cycle failed"
                            );
                            self.status.update_component_status(
                                "partition_metadata",
                                ComponentStatus::error_with_message(format!(
                                    "Management cycle failed: {e}"
                                )),
                            );
                        }
                    }

                    let cycle_duration = cycle_start.elapsed();
                    if cycle_duration > self.interval {
                        tracing::warn!(
                            duration_ms = cycle_duration.as_millis(),
                            interval_ms = self.interval.as_millis(),
                            "Partition management cycle took longer than interval"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Underlying logic for a single management cycle.
    /// Delegates to [`super::service::PartitionService::discover_and_assign_all`].
    async fn run_management_cycle(&self) -> Result<()> {
        let Some(service) = &self.df.partition_service else {
            tracing::warn!("Partition service not initialized, skipping management cycle");
            return Ok(());
        };

        service
            .discover_and_assign_all_tables(self.df.as_ref())
            .await
            .map_err(|e| Error::AssignmentCycle { source: e })
    }

    /// Seed partition metadata for all accelerated tables that don't have metadata yet.
    ///
    /// This delegates to [`super::initialize_partition_metadata`] which discovers partition
    /// values from the federated source and writes them as unassigned in the object store.
    async fn initialize_metadata(&self) -> std::result::Result<(), super::Error> {
        let Some(service) = &self.df.partition_service else {
            tracing::debug!("Partition service not initialized, skipping metadata seeding");
            return Ok(());
        };

        let Some(app) = service.app.read().await.clone() else {
            tracing::debug!("App not initialized, skipping partition metadata seeding");
            return Ok(());
        };

        super::initialize_partition_metadata(Arc::clone(&self.df), app, &service.partition_store)
            .await
    }
}

// Partition management logic lives in `super::service::PartitionService`.
