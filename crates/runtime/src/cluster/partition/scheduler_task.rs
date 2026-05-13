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

//! Periodic partition-assignment background task.
//!
//! Core partition-assignment logic lives in
//! [`crate::cluster::partition::service::PartitionService`]; this file only
//! contains the timer-driven driver that invokes the service on an interval
//! and reports status.

use std::sync::Arc;
use std::time::{Duration, Instant};

use snafu::prelude::*;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::CLUSTER_PARTITION_ASSIGNMENT_TASK;
use crate::datafusion::DataFusion;
use crate::status::{ComponentStatus, RuntimeStatus};

pub use runtime_cluster::scheduler_task_config::{ConfigError, PartitionAssignmentConfig};

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
pub struct PartitionAssignmentTask {
    df: Arc<DataFusion>,
    status: Arc<RuntimeStatus>,

    /// How often to run the assignment cycle
    interval: Duration,

    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
}

impl PartitionAssignmentTask {
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
        tracing::debug!("Starting {CLUSTER_PARTITION_ASSIGNMENT_TASK} in background");

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
            tracing::debug!("Starting {CLUSTER_PARTITION_ASSIGNMENT_TASK} loop");
            tokio::select! {
                () = self.cancel.cancelled() => {
                    tracing::info!("Partition assignment task shutting down");
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
                                "Partition assignment cycle failed"
                            );
                            self.status.update_component_status(
                                "partition_metadata",
                                ComponentStatus::error_with_message(format!(
                                    "Assignment cycle failed: {e}"
                                )),
                            );
                        }
                    }

                    let cycle_duration = cycle_start.elapsed();
                    if cycle_duration > self.interval {
                        tracing::warn!(
                            duration_ms = cycle_duration.as_millis(),
                            interval_ms = self.interval.as_millis(),
                            "Partition assignment cycle took longer than interval"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Underlying logic for a single management cycle.
    ///
    /// 1. Check completed discovery jobs and process results.
    /// 2. Submit new discovery jobs for tables without an active one.
    /// 3. Assign unassigned partitions to executors.
    /// Delegates to [`super::service::PartitionService::reconcile_all`].
    async fn run_management_cycle(&self) -> Result<()> {
        let Some(service) = &self.df.partition_service else {
            tracing::warn!("Partition service not initialized, skipping assignment cycle");
            return Ok(());
        };

        // Step 1: Check and process completed discovery jobs.
        service
            .check_and_process_discovery_jobs(self.df.as_ref())
            .await
            .map_err(|e| Error::AssignmentCycle { source: e })?;

        // Step 2: Submit discovery jobs for tables that need them.
        let Some(app) = service.app.read().await.clone() else {
            return Ok(());
        };
        let all_tables = super::startup::accelerated_tables(&app);
        let dynamic_tables: Vec<_> = all_tables
            .into_iter()
            .filter(|(_, partitioning)| {
                super::discovery::try_static_partition_values(partitioning).is_none()
            })
            .collect();
        if !dynamic_tables.is_empty() {
            service
                .submit_discovery_for_pending_tables(&dynamic_tables, self.df.as_ref())
                .await
                .map_err(|e| Error::AssignmentCycle { source: e })?;
        }

        // Step 3: Run the standard reconciliation cycle (assign pending + notify).
        // This handles static tables' discovery + assignment, and assignment of
        // partitions that were seeded by completed discovery jobs above.
        service
            .reconcile_all(self.df.as_ref())
            .await
            .map_err(|e| Error::AssignmentCycle { source: e })
    }

    /// Seed partition metadata for all accelerated tables that don't have metadata yet.
    ///
    /// Delegates to [`super::initialize_partition_metadata`], which loops over
    /// `app.datasets` + `app.views` and calls
    /// [`super::service::PartitionService::seed_table`] per table.
    async fn initialize_metadata(&self) -> std::result::Result<(), super::Error> {
        let Some(service) = &self.df.partition_service else {
            tracing::debug!("Partition service not initialized, skipping metadata seeding");
            return Ok(());
        };

        let Some(app) = service.app.read().await.clone() else {
            tracing::debug!("App not initialized, skipping partition metadata seeding");
            return Ok(());
        };

        super::initialize_partition_metadata(service, &self.df, &app).await
    }
}
