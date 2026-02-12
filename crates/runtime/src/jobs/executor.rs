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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::datafusion::DataFusion;
use crate::datafusion::query::{QueryBuilder, QueryHandle, QueryHandleError};
use crate::http::v1::queries::SubmitQueryRequest;
use crate::jobs::state::JobErrorCode;

use super::Result;
use super::state::{JobState, JobStatus};
use super::store::JobStore;

/// Tracks an active job's cancellation token and query handle (once submitted).
struct ActiveJobInfo {
    cancel_token: CancellationToken,
    /// The Ballista scheduler job ID, set once submitted to the scheduler.
    query_handle: Option<QueryHandle>,
}

/// Manages background execution of async query jobs.
///
/// The `JobExecutor` coordinates asynchronous query execution by:
/// 1. Creating jobs in the `JobStore`
/// 2. Submitting queries via `Query::submit_distributed` to get a `QueryHandle`
/// 3. Polling the `QueryHandle` for completion
/// 4. Writing results to the `JobStore` when complete
pub struct JobExecutor {
    job_store: Arc<JobStore>,
    df: Arc<DataFusion>,
    /// Tracks active jobs by Spice `job_id`
    active_jobs: Arc<RwLock<std::collections::HashMap<String, ActiveJobInfo>>>,
}

impl std::fmt::Debug for JobExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobExecutor")
            .field("job_store", &self.job_store)
            .finish_non_exhaustive()
    }
}

impl JobExecutor {
    /// Creates a new job executor.
    #[must_use]
    pub fn new(job_store: Arc<JobStore>, df: Arc<DataFusion>) -> Self {
        Self {
            job_store,
            df,
            active_jobs: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Submits a new query job for async execution.
    ///
    /// Returns the job state immediately. The query will be executed in the background.
    pub async fn submit(&self, request: SubmitQueryRequest) -> Result<JobState> {
        let state = self.job_store.create_job(request).await?;
        let job_id = state.job_id.clone();

        // Create cancellation token for this job
        let cancel_token = CancellationToken::new();
        {
            let mut active = self.active_jobs.write().await;
            active.insert(
                job_id.clone(),
                ActiveJobInfo {
                    cancel_token: cancel_token.clone(),
                    query_handle: None,
                },
            );
        }

        // Spawn background task to execute the job
        let job_store = Arc::clone(&self.job_store);
        let df = Arc::clone(&self.df);
        let active_jobs = Arc::clone(&self.active_jobs);
        let job_id_clone = job_id.clone();

        tokio::spawn(
            async move {
                let result =
                    Self::execute_job(&job_store, df, &job_id_clone, &active_jobs, cancel_token)
                        .await;

                // Remove from active jobs
                {
                    let mut active = active_jobs.write().await;
                    active.remove(&job_id_clone);
                }

                if let Err(e) = result {
                    tracing::error!(job_id = %job_id_clone, error = %e, "Job execution failed");
                }
            }
            .instrument(tracing::info_span!("job_execution", job_id = %job_id)),
        );

        Ok(state)
    }

    /// Requests cancellation of a running job.
    pub async fn cancel(&self, job_id: &str) -> Result<JobState> {
        // Signal cancellation to the running task
        let active = self.active_jobs.read().await;
        if let Some(info) = active.get(job_id) {
            info.cancel_token.cancel();
        }

        // Update job state
        self.job_store.cancel_job(job_id).await
    }

    /// Gets the current state of a job.
    pub async fn get_status(&self, job_id: &str) -> Result<JobState> {
        self.job_store.get_job(job_id).await
    }

    /// Reads a result chunk for a completed job.
    pub async fn get_chunk(&self, job_id: &str, chunk_index: usize) -> Result<Vec<RecordBatch>> {
        let state = self.job_store.get_job(job_id).await?;

        if state.status != JobStatus::Succeeded {
            return Err(super::error::Error::JobNotComplete {
                job_id: job_id.to_string(),
                status: state.status.to_string(),
            });
        }

        // If the job completed with no rows, there are no chunks to read.
        if let Some(result) = &state.result
            && result.manifest.total_chunk_count == 0
        {
            return Err(super::error::Error::NoRowsReturned {
                job_id: job_id.to_string(),
            });
        }

        self.job_store.read_chunk(job_id, chunk_index).await
    }

    /// Lists all jobs, optionally filtered by status.
    pub async fn list_jobs(&self, status_filter: Option<JobStatus>) -> Result<Vec<JobState>> {
        self.job_store.list_jobs(status_filter).await
    }

    /// Executes a job using `Query::submit_distributed` and writes results to the store.
    async fn execute_job(
        job_store: &JobStore,
        df: Arc<DataFusion>,
        job_id: &str,
        active_jobs: &RwLock<std::collections::HashMap<String, ActiveJobInfo>>,
        cancel: CancellationToken,
    ) -> Result<()> {
        // Get job and mark as running
        let state = job_store.set_job_running(job_id).await?;

        // Check for early cancellation
        if cancel.is_cancelled() {
            job_store.cancel_job(job_id).await?;
            return Ok(());
        }

        // Build and submit the query using Query::submit_distributed
        let mut query_builder = QueryBuilder::new(&state.sql, Arc::clone(&df));

        // Parse parameters if present
        if let Some(p) = state.parameters {
            match crate::datafusion::param_utils::convert_json_to_param_values(p) {
                Ok(params) => {
                    query_builder = query_builder.parameters(Some(params));
                }
                Err(e) => {
                    job_store
                        .fail_job(job_id, JobErrorCode::ParameterBindingFailed, e.to_string())
                        .await?;
                    return Ok(());
                }
            }
        }

        let query = query_builder.build();

        let query_handle = match query.submit_distributed(job_id).await {
            Ok(handle) => handle,
            Err(e) => {
                let error_code = Self::query_error_to_code(&e);
                job_store
                    .fail_job(job_id, error_code, e.to_string())
                    .await?;
                return Ok(());
            }
        };

        tracing::debug!(
            job_id,
            ballista_job_id = %query_handle.ballista_job_id(),
            is_cached = %query_handle.is_cached(),
            "Query submitted for distributed execution"
        );

        // Store the Ballista job ID for cancellation
        let mut active = active_jobs.write().await;
        if let Some(info) = active.get_mut(job_id) {
            info.query_handle = Some(query_handle.clone());
        }

        drop(active);

        let timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>> = state.timeout_seconds.map_or(
            Box::pin(std::future::pending()) as Pin<Box<dyn Future<Output = ()> + Send>>,
            |secs| {
                Box::pin(tokio::time::sleep(Duration::from_secs(secs)))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            },
        );

        tokio::select! {
            () = cancel.cancelled() => {
                tracing::debug!(job_id = %job_id, "Job cancelled before completion");
                if let Err(e) = query_handle.cancel().await {
                    tracing::error!("Failed to cancel the distributed query '{job_id}': {e}");
                }
                job_store.cancel_job(job_id).await?;
                Ok(())
            },
            () = timeout_fut => {
                tracing::debug!(job_id = %job_id, "Job timed out");
                if let Err(e) = query_handle.cancel().await {
                    tracing::error!("Failed to cancel the timed-out query '{job_id}': {e}");
                }
                job_store.fail_job(job_id, JobErrorCode::Timeout, "Job execution timed out".to_string()).await?;
                Ok(())
            }
            result_stream = query_handle.into_stream() => {
                // Wait for completion and get the result stream
                let result_stream = match result_stream {
                    Ok(stream) => stream,
                    Err(e) => {
                        let (error_code, error_msg) = Self::handle_error_to_code_and_msg(&e);
                        job_store.fail_job(job_id, error_code, error_msg).await?;
                        return Ok(());
                    }
                };

                // Write result chunks as batches arrive from the stream
                let job_result = match job_store
                    .write_result_chunks_from_stream(job_id, result_stream)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        job_store
                            .fail_job(job_id, JobErrorCode::FetchingResultsFailed, e.to_string())
                            .await?;
                        return Ok(());
                    }
                };

                // Mark job as succeeded
                job_store.complete_job(job_id, job_result).await?;

                Ok(())
            }
        }
    }

    /// Converts a `Query::Error` to an error code string.
    fn query_error_to_code(e: &crate::datafusion::query::Error) -> JobErrorCode {
        use crate::datafusion::query::Error;
        match e {
            Error::SchedulerUnavailable => JobErrorCode::SchedulerUnavailable,
            Error::SessionCreationFailed { .. } | Error::JobSubmissionFailed { .. } => {
                JobErrorCode::SubmissionFailed
            }
            Error::UnableToExecuteQuery { .. } | Error::TableAccessDisallowed { .. } => {
                JobErrorCode::ExecutionFailed
            }
            Error::BindingParameters { .. } => JobErrorCode::ParameterBindingFailed,
            _ => JobErrorCode::Internal,
        }
    }

    /// Converts a `QueryHandleError` to an error code string and message.
    fn handle_error_to_code_and_msg(e: &QueryHandleError) -> (JobErrorCode, String) {
        match e {
            QueryHandleError::JobCancelled => (JobErrorCode::Cancelled, e.to_string()),
            QueryHandleError::JobFailed { message } => {
                (JobErrorCode::ExecutionFailed, message.clone())
            }
            QueryHandleError::StatusError { message } => (JobErrorCode::Internal, message.clone()),
            QueryHandleError::PartitionLocationError { .. } => {
                (JobErrorCode::Internal, e.to_string())
            }
            QueryHandleError::JobNotFound { .. } => (JobErrorCode::NotFound, e.to_string()),
        }
    }
}
