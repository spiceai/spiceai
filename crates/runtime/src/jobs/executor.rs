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

use arrow::array::RecordBatch;
use datafusion::common::ParamValues;
use futures::TryStreamExt;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::datafusion::DataFusion;
use crate::datafusion::query::QueryBuilder;

use super::Result;
use super::state::{JobState, JobStatus};
use super::store::JobStore;

/// Manages background execution of async query jobs.
pub struct JobExecutor {
    job_store: Arc<JobStore>,
    df: Arc<DataFusion>,
    /// Tracks active job cancellation tokens by `job_id`
    active_jobs: Arc<RwLock<std::collections::HashMap<String, CancellationToken>>>,
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
    pub async fn submit(
        &self,
        sql: String,
        parameters: Option<serde_json::Value>,
    ) -> Result<JobState> {
        let state = self.job_store.create_job(sql, parameters).await?;
        let job_id = state.job_id.clone();

        // Create cancellation token for this job
        let cancel_token = CancellationToken::new();
        {
            let mut active = self.active_jobs.write().await;
            active.insert(job_id.clone(), cancel_token.clone());
        }

        // Spawn background task to execute the job
        let job_store = Arc::clone(&self.job_store);
        let df = Arc::clone(&self.df);
        let active_jobs = Arc::clone(&self.active_jobs);
        let job_id_clone = job_id.clone();

        tokio::spawn(
            async move {
                let result =
                    Self::execute_job(&job_store, df, &job_id_clone, cancel_token.clone()).await;

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
        // Signal cancellation if job is active
        {
            let active = self.active_jobs.read().await;
            if let Some(token) = active.get(job_id) {
                token.cancel();
            }
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

        self.job_store.read_chunk(job_id, chunk_index).await
    }

    /// Lists all jobs, optionally filtered by status.
    pub async fn list_jobs(&self, status_filter: Option<JobStatus>) -> Result<Vec<JobState>> {
        self.job_store.list_jobs(status_filter).await
    }

    async fn execute_job(
        job_store: &JobStore,
        df: Arc<DataFusion>,
        job_id: &str,
        cancel: CancellationToken,
    ) -> Result<()> {
        // Get job and mark as running
        let state = job_store.set_job_running(job_id).await?;

        // Check for early cancellation
        if cancel.is_cancelled() {
            job_store.cancel_job(job_id).await?;
            return Ok(());
        }

        // Parse parameters if present
        let params: Option<ParamValues> = if let Some(p) = state.parameters {
            match crate::datafusion::param_utils::convert_json_to_param_values(p) {
                Ok(params) => Some(params),
                Err(e) => {
                    job_store
                        .fail_job(job_id, "INVALID_PARAMETERS", e.to_string())
                        .await?;
                    return Ok(());
                }
            }
        } else {
            None
        };

        // Execute the query using distributed execution (Ballista)
        let query_result = {
            let mut builder = QueryBuilder::new(&state.sql, Arc::clone(&df));
            if let Some(p) = params {
                builder = builder.parameters(Some(p));
            }

            tokio::select! {
                result = builder.build().run_distributed() => result,
                () = cancel.cancelled() => {
                    job_store.cancel_job(job_id).await?;
                    return Ok(());
                }
            }
        };

        match query_result {
            Ok(result) => {
                // Collect results - check for cancellation periodically
                let mut batches = Vec::new();
                let mut stream = result.data;

                loop {
                    tokio::select! {
                        batch_opt = stream.try_next() => {
                            match batch_opt {
                                Ok(Some(batch)) => batches.push(batch),
                                Ok(None) => break,
                                Err(e) => {
                                    job_store.fail_job(job_id, "QUERY_EXECUTION", e.to_string()).await?;
                                    return Ok(());
                                }
                            }
                        }
                        () = cancel.cancelled() => {
                            job_store.cancel_job(job_id).await?;
                            return Ok(());
                        }
                    }
                }

                // Write result chunks
                let job_result = job_store.write_result_chunks(job_id, batches).await?;

                // Mark job as succeeded
                job_store.complete_job(job_id, job_result).await?;
            }
            Err(e) => {
                let error_message = e.to_string();
                let error_code = categorize_error(&error_message);
                job_store
                    .fail_job(job_id, error_code, error_message)
                    .await?;
            }
        }

        Ok(())
    }
}

/// Categorizes an error message into an error code.
///
/// Returns a generic `QUERY_EXECUTION` error code rather than attempting to
/// infer categories from error message text. String-based error categorization
/// is unreliable because:
/// - Error messages can contain user-controlled content (e.g., SQL with "timeout" in comments)
/// - Error message formats can change between `DataFusion` versions
/// - Misclassification can mislead users about the actual error
///
/// For reliable error categorization, use structured error types from `DataFusion`
/// rather than string matching.
fn categorize_error(_message: &str) -> &'static str {
    "QUERY_EXECUTION"
}
