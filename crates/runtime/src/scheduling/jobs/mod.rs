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

//! Scheduled task for cleaning up expired distributed query jobs.

use std::sync::Arc;

use scheduler::Result;
use scheduler::task::ScheduledTask;
use tonic::async_trait;

use crate::jobs::JobStore;

/// A scheduled task that cleans up expired jobs from the job store.
///
/// This task is designed to run periodically (e.g., every 5 minutes) to remove
/// jobs whose results have exceeded their TTL. It logs errors but does not
/// propagate them, allowing the scheduler to continue running subsequent
/// cleanup cycles.
pub struct JobCleanupTask {
    job_store: Arc<JobStore>,
}

impl JobCleanupTask {
    /// Creates a new `JobCleanupTask` with the given job store.
    #[must_use]
    pub fn new(job_store: Arc<JobStore>) -> Self {
        Self { job_store }
    }
}

#[async_trait]
impl ScheduledTask for JobCleanupTask {
    async fn execute(&self) -> Result<()> {
        tracing::debug!("Starting cleanup of expired distributed query jobs");
        match self.job_store.cleanup_expired_jobs().await {
            Ok(deleted_count) => {
                if deleted_count > 0 {
                    tracing::debug!(deleted_count, "Cleaned up expired distributed query jobs");
                } else {
                    tracing::debug!("No expired distributed query jobs to clean up");
                }
            }
            Err(e) => {
                // Log the error but do not propagate - allow the schedule to continue
                // for the next cleanup cycle
                tracing::error!("Failed to clean up expired distributed query jobs. {e}");
            }
        }
        Ok(())
    }
}
