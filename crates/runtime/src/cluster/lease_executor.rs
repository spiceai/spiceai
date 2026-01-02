/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Lease-validating wrapper for Ballista `ExecutorGrpc` service.
//!
//! This module wraps the Ballista executor to add lease validation for task slots.
//! When tasks are launched, the wrapper extracts the `lease_id` from task properties,
//! validates the lease via `LeaseManager`, and tracks slot usage.

use crate::cluster::lease::{LeaseManager, UseSlotResult};
use ballista_core::serde::protobuf::executor_grpc_server::ExecutorGrpc;
use ballista_core::serde::protobuf::{
    CancelTasksParams, CancelTasksResult, KeyValuePair, LaunchMultiTaskParams,
    LaunchMultiTaskResult, LaunchTaskParams, LaunchTaskResult, RemoveJobDataParams,
    RemoveJobDataResult, StopExecutorParams, StopExecutorResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Key used to store lease ID in task properties.
pub const LEASE_ID_PROP_KEY: &str = "spice.cluster.lease_id";

/// Tracks task-to-lease mappings for returning slots on completion.
#[derive(Debug, Default)]
struct TaskLeaseTracker {
    /// Maps task_id to lease_id for slot tracking.
    task_to_lease: HashMap<String, String>,
}

impl TaskLeaseTracker {
    fn track(&mut self, task_id: String, lease_id: String) {
        self.task_to_lease.insert(task_id, lease_id);
    }

    fn remove(&mut self, task_id: &str) -> Option<String> {
        self.task_to_lease.remove(task_id)
    }
}

/// Wrapper around `ExecutorGrpc` that validates leases before task execution.
///
/// This wrapper intercepts `launch_task` and `launch_multi_task` calls to:
/// 1. Extract `lease_id` from task properties
/// 2. Validate the lease via `LeaseManager::try_use_slot()`
/// 3. Reject tasks with invalid/expired leases
/// 4. Track task-to-lease mappings for slot returns
pub struct LeaseValidatingExecutorGrpc<T: ExecutorGrpc> {
    inner: T,
    lease_manager: Arc<LeaseManager>,
    tracker: RwLock<TaskLeaseTracker>,
}

impl<T: ExecutorGrpc> LeaseValidatingExecutorGrpc<T> {
    /// Creates a new lease-validating executor wrapper.
    pub fn new(inner: T, lease_manager: Arc<LeaseManager>) -> Self {
        Self {
            inner,
            lease_manager,
            tracker: RwLock::new(TaskLeaseTracker::default()),
        }
    }

    /// Extracts lease ID from task properties (Vec of KeyValuePair).
    fn extract_lease_id(props: &[KeyValuePair]) -> Option<&str> {
        props
            .iter()
            .find(|kv| kv.key == LEASE_ID_PROP_KEY)
            .and_then(|kv| kv.value.as_deref())
    }

    /// Validates and acquires a slot for a lease.
    async fn validate_lease(&self, lease_id: &str) -> Result<(), Status> {
        match self.lease_manager.try_use_slot(lease_id).await {
            UseSlotResult::Acquired => Ok(()),
            UseSlotResult::Failed { reason } => Err(Status::failed_precondition(format!(
                "Lease validation failed: {reason}"
            ))),
        }
    }

    /// Returns a slot to a lease.
    async fn return_slot(&self, lease_id: &str) {
        self.lease_manager.return_slot(lease_id).await;
    }

    /// Returns slots for completed/failed tasks.
    pub async fn return_slots_for_tasks(&self, task_ids: &[String]) {
        let mut tracker = self.tracker.write().await;
        for task_id in task_ids {
            if let Some(lease_id) = tracker.remove(task_id) {
                self.lease_manager.return_slot(&lease_id).await;
            }
        }
    }
}

#[tonic::async_trait]
impl<T: ExecutorGrpc> ExecutorGrpc for LeaseValidatingExecutorGrpc<T> {
    async fn launch_task(
        &self,
        request: Request<LaunchTaskParams>,
    ) -> Result<Response<LaunchTaskResult>, Status> {
        let params = request.get_ref();
        let mut acquired_slots: Vec<(String, String)> = Vec::new();

        // LaunchTaskParams contains repeated TaskDefinition tasks
        for task in &params.tasks {
            let task_id = format!("{}/{}/{}", task.job_id, task.stage_id, task.partition_id);

            // Check for lease_id in task properties
            if let Some(lease_id) = Self::extract_lease_id(&task.props) {
                // Validate and acquire slot
                if let Err(e) = self.validate_lease(lease_id).await {
                    // Release any slots we've acquired
                    for (_, acquired_lease_id) in &acquired_slots {
                        self.return_slot(acquired_lease_id).await;
                    }
                    return Err(e);
                }

                acquired_slots.push((task_id.clone(), lease_id.to_string()));

                tracing::debug!(
                    task_id = %task_id,
                    lease_id = %lease_id,
                    "Task launched with lease validation"
                );
            } else {
                // No lease_id - allow task (backwards compatibility)
                tracing::trace!(
                    task_id = %task_id,
                    "Task launched without lease (no lease_id in props)"
                );
            }
        }

        // Track all task-to-lease mappings
        if !acquired_slots.is_empty() {
            let mut tracker = self.tracker.write().await;
            for (task_id, lease_id) in &acquired_slots {
                tracker.track(task_id.clone(), lease_id.clone());
            }
        }

        // Forward to inner executor
        match self.inner.launch_task(request).await {
            Ok(response) => Ok(response),
            Err(e) => {
                // Task launch failed - release acquired slots
                for (_, lease_id) in &acquired_slots {
                    self.return_slot(lease_id).await;
                }
                // Also remove from tracker
                {
                    let mut tracker = self.tracker.write().await;
                    for (task_id, _) in &acquired_slots {
                        tracker.remove(task_id);
                    }
                }
                Err(e)
            }
        }
    }

    async fn launch_multi_task(
        &self,
        request: Request<LaunchMultiTaskParams>,
    ) -> Result<Response<LaunchMultiTaskResult>, Status> {
        let params = request.get_ref();
        let mut acquired_slots: Vec<(String, String)> = Vec::new();

        // LaunchMultiTaskParams contains repeated MultiTaskDefinition multi_tasks
        for multi_task in &params.multi_tasks {
            // Check for lease_id in multi_task properties
            if let Some(lease_id) = Self::extract_lease_id(&multi_task.props) {
                // Each task in the multi_task batch shares the same lease
                // Validate and acquire slots for each partition
                for task_id in &multi_task.task_ids {
                    let full_task_id = format!(
                        "{}/{}/{}",
                        multi_task.job_id, multi_task.stage_id, task_id.partition_id
                    );

                    // Validate and acquire slot for each task
                    if let Err(e) = self.validate_lease(lease_id).await {
                        // Release any slots we've acquired
                        for (_, acquired_lease_id) in &acquired_slots {
                            self.return_slot(acquired_lease_id).await;
                        }
                        return Err(e);
                    }

                    acquired_slots.push((full_task_id.clone(), lease_id.to_string()));
                }

                tracing::debug!(
                    job_id = %multi_task.job_id,
                    stage_id = %multi_task.stage_id,
                    lease_id = %lease_id,
                    task_count = multi_task.task_ids.len(),
                    "Multi-task batch launched with lease validation"
                );
            } else {
                // No lease_id - allow tasks (backwards compatibility)
                tracing::trace!(
                    job_id = %multi_task.job_id,
                    stage_id = %multi_task.stage_id,
                    "Multi-task batch launched without lease (no lease_id in props)"
                );
            }
        }

        // Track all task-to-lease mappings
        if !acquired_slots.is_empty() {
            let mut tracker = self.tracker.write().await;
            for (task_id, lease_id) in &acquired_slots {
                tracker.track(task_id.clone(), lease_id.clone());
            }
        }

        // Forward to inner executor
        match self.inner.launch_multi_task(request).await {
            Ok(response) => Ok(response),
            Err(e) => {
                // Task launch failed - release acquired slots
                for (_, lease_id) in &acquired_slots {
                    self.return_slot(lease_id).await;
                }
                // Also remove from tracker
                {
                    let mut tracker = self.tracker.write().await;
                    for (task_id, _) in &acquired_slots {
                        tracker.remove(task_id);
                    }
                }
                Err(e)
            }
        }
    }

    async fn stop_executor(
        &self,
        request: Request<StopExecutorParams>,
    ) -> Result<Response<StopExecutorResult>, Status> {
        self.inner.stop_executor(request).await
    }

    async fn cancel_tasks(
        &self,
        request: Request<CancelTasksParams>,
    ) -> Result<Response<CancelTasksResult>, Status> {
        // Return slots for cancelled tasks
        let task_ids: Vec<String> = request
            .get_ref()
            .task_infos
            .iter()
            .map(|ti| format!("{}/{}/{}", ti.job_id, ti.stage_id, ti.partition_id))
            .collect();

        self.return_slots_for_tasks(&task_ids).await;

        self.inner.cancel_tasks(request).await
    }

    async fn remove_job_data(
        &self,
        request: Request<RemoveJobDataParams>,
    ) -> Result<Response<RemoveJobDataResult>, Status> {
        self.inner.remove_job_data(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_lease_id() {
        let props = vec![KeyValuePair {
            key: LEASE_ID_PROP_KEY.to_string(),
            value: Some("lease-123".to_string()),
        }];

        let lease_id = LeaseValidatingExecutorGrpc::<()>::extract_lease_id(&props);
        assert_eq!(lease_id, Some("lease-123"));
    }

    #[test]
    fn test_extract_lease_id_missing() {
        let props: Vec<KeyValuePair> = vec![];
        let lease_id = LeaseValidatingExecutorGrpc::<()>::extract_lease_id(&props);
        assert_eq!(lease_id, None);
    }

    #[test]
    fn test_extract_lease_id_none_value() {
        let props = vec![KeyValuePair {
            key: LEASE_ID_PROP_KEY.to_string(),
            value: None,
        }];

        let lease_id = LeaseValidatingExecutorGrpc::<()>::extract_lease_id(&props);
        assert_eq!(lease_id, None);
    }
}
