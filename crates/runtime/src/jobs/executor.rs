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

use runtime_request_context::RequestContext;

use crate::datafusion::DataFusion;
use crate::datafusion::query::{QueryBuilder, QueryHandle, QueryHandleError};
use crate::http::v1::queries::SubmitQueryRequest;
use crate::jobs::state::JobErrorCode;

use super::Result;
use super::error::Error;
use super::state::{JobState, JobStatus};
use super::store::JobStore;

/// How a job comes to be driven by this scheduler.
enum Start {
    /// It was submitted here: start it.
    Submitted,
    /// It was taken over from a scheduler that stopped: this is the state the
    /// takeover wrote.
    TakenOver(Box<JobState>),
}

/// Logs how a job's background task ended. A driver that another scheduler
/// took the job from stops at its next write; that is not a failure.
fn report_job_end(job_id: &str, result: Result<()>, what: &str) {
    match result {
        Ok(()) => {}
        Err(Error::JobTakenOver { .. }) => {
            tracing::debug!(
                job_id,
                "Stopped driving the job: another scheduler took it over"
            );
        }
        Err(e) => tracing::error!(job_id = %job_id, error = %e, "{what}"),
    }
}

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
    pub async fn submit(
        &self,
        request: SubmitQueryRequest,
        read_only: bool,
        owner: String,
    ) -> Result<JobState> {
        let state = self.job_store.create_job(request, read_only, owner).await?;
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

        // Bind the job to the submitting request's context so it runs as the
        // caller's principal — results-cache namespacing and principal-scoped
        // authz/masking key off it.
        RequestContext::spawn_current(
            async move {
                let result = Self::execute_job(
                    &job_store,
                    df,
                    &job_id_clone,
                    &active_jobs,
                    cancel_token,
                    Start::Submitted,
                )
                .await;

                // Remove from active jobs
                {
                    let mut active = active_jobs.write().await;
                    active.remove(&job_id_clone);
                }

                report_job_end(&job_id_clone, result, "Job execution failed");
            }
            .instrument(tracing::info_span!("job_execution", job_id = %job_id)),
        );

        Ok(state)
    }

    /// Re-drives an existing job whose owning scheduler was lost, resuming its
    /// distributed execution on this scheduler. No-op if this scheduler is
    /// already driving the job locally.
    ///
    /// `job` is the state in which the recovery sweep found the job orphaned.
    /// The takeover, or the failure of a principal's job, lands only while the
    /// job is still in that state, so of the schedulers that recover the same
    /// job only one acts on it, and the driver it was taken from is refused at
    /// its next write.
    ///
    /// Resuming re-plans the job's SQL on this scheduler, and planning is where a
    /// principal's table access and masking apply. The job records only its
    /// submitter's opaque storage id, not an identity a plan can be authorized
    /// against, so a job submitted by an authenticated principal is failed with a
    /// request to resubmit rather than re-planned without that principal.
    pub async fn resume(&self, job: &JobState) {
        let job_id = job.job_id.as_str();
        if !job.is_owned_by(super::PUBLIC_JOB_OWNER) {
            tracing::warn!(
                job_id,
                "Async query job '{job_id}' was running on a scheduler that stopped, and it cannot be resumed under the identity that submitted it, so it is marked failed; resubmit the query."
            );
            match self
                .job_store
                .fail_orphaned_job(
                    job,
                    JobErrorCode::SchedulerUnavailable,
                    "The scheduler running this query stopped before it finished, and the query cannot be resumed under the identity that submitted it. Resubmit the query.",
                )
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => tracing::debug!(
                    job_id,
                    "Not recovering the job: another scheduler took it over first, or it finished"
                ),
                Err(e) => tracing::warn!(
                    job_id,
                    "Failed to mark async query job '{job_id}' failed after its scheduler stopped: {e}"
                ),
            }
            return;
        }

        let cancel_token = CancellationToken::new();
        {
            let mut active = self.active_jobs.write().await;
            if active.contains_key(job_id) {
                return;
            }
            active.insert(
                job_id.to_string(),
                ActiveJobInfo {
                    cancel_token: cancel_token.clone(),
                    query_handle: None,
                },
            );
        }

        let taken = match self.job_store.take_over_job(job).await {
            Ok(Some(taken)) => Some(taken),
            Ok(None) => {
                tracing::debug!(
                    job_id,
                    "Not recovering the job: another scheduler took it over first, or it finished"
                );
                None
            }
            Err(e) => {
                report_job_end(job_id, Err(e), "Job recovery failed");
                None
            }
        };
        let Some(taken) = taken else {
            self.active_jobs.write().await.remove(job_id);
            return;
        };

        let job_store = Arc::clone(&self.job_store);
        let df = Arc::clone(&self.df);
        let active_jobs = Arc::clone(&self.active_jobs);
        let job_id_owned = job_id.to_string();

        tokio::spawn(
            async move {
                let result = Self::execute_job(
                    &job_store,
                    df,
                    &job_id_owned,
                    &active_jobs,
                    cancel_token,
                    Start::TakenOver(Box::new(taken)),
                )
                .await;

                {
                    let mut active = active_jobs.write().await;
                    active.remove(&job_id_owned);
                }

                report_job_end(&job_id_owned, result, "Job recovery failed");
            }
            .instrument(tracing::info_span!("job_recovery", job_id = %job_id)),
        );
    }

    /// Requests cancellation of a running job submitted by `caller`.
    pub async fn cancel(&self, job_id: &str, caller: &str) -> Result<JobState> {
        // Resolve ownership before signalling: a caller that did not submit
        // the job must not be able to stop it.
        self.owned_job(job_id, caller).await?;

        // Signal cancellation to the running task
        {
            let active = self.active_jobs.read().await;
            if let Some(info) = active.get(job_id) {
                info.cancel_token.cancel();
            }
        }

        // Update job state
        self.job_store.cancel_job(job_id).await
    }

    /// Gets the current state of a job submitted by `caller`.
    pub async fn get_status(&self, job_id: &str, caller: &str) -> Result<JobState> {
        self.owned_job(job_id, caller).await
    }

    /// Reads a job's state and authorizes `caller` against it.
    ///
    /// Ownership is resolved before expiry so every job `caller` does not own
    /// answers identically, whether it is live, expired, or absent.
    async fn owned_job(&self, job_id: &str, caller: &str) -> Result<JobState> {
        let state = self.job_store.get_job_ignoring_expiry(job_id).await?;
        Self::require_owner(&state, caller)?;
        if state.is_expired() {
            return Err(super::error::Error::JobResultsExpired {
                job_id: job_id.to_string(),
            });
        }
        Ok(state)
    }

    /// Rejects access to a job `caller` did not submit.
    ///
    /// Reports the job as missing rather than forbidden so the API does not
    /// confirm that a job id exists to a principal that cannot read it.
    fn require_owner(state: &JobState, caller: &str) -> Result<()> {
        if state.is_owned_by(caller) {
            return Ok(());
        }
        tracing::debug!(
            job_id = %state.job_id,
            "Refusing access to a job submitted by a different principal"
        );
        Err(super::error::Error::JobNotFound {
            job_id: state.job_id.clone(),
        })
    }

    /// Reads a result chunk for a completed job submitted by `caller`.
    pub async fn get_chunk(
        &self,
        job_id: &str,
        chunk_index: usize,
        caller: &str,
    ) -> Result<Vec<RecordBatch>> {
        let state = self.owned_job(job_id, caller).await?;

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

        // A driver the job was taken over from can leave chunks past the ones
        // the result lists; only listed chunks hold the job's results.
        if let Some(result) = &state.result
            && !result.chunk_indices.contains(&chunk_index)
        {
            return Err(super::error::Error::ChunkNotFound {
                job_id: job_id.to_string(),
                chunk_index,
            });
        }

        self.job_store.read_chunk(job_id, chunk_index).await
    }

    /// Lists the jobs `caller` submitted, optionally filtered by status.
    pub async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        caller: &str,
    ) -> Result<Vec<JobState>> {
        let mut jobs = self.job_store.list_jobs(status_filter).await?;
        jobs.retain(|job| job.is_owned_by(caller));
        Ok(jobs)
    }

    /// The query engine jobs run on.
    pub(crate) fn datafusion(&self) -> &Arc<DataFusion> {
        &self.df
    }

    /// Deletes, with their result chunks, the jobs whose results expired and the
    /// jobs that failed or were cancelled at least the result TTL ago.
    ///
    /// Returns how many were deleted.
    pub async fn cleanup_expired_jobs(&self) -> Result<usize> {
        self.job_store.cleanup_expired_jobs().await
    }

    /// Lists every job regardless of who submitted it.
    ///
    /// For internal schedulers only — the recovery sweep has to see jobs
    /// across all principals to re-drive the ones orphaned by a lost peer.
    /// Never reachable from a client request; API surfaces use
    /// [`Self::list_jobs`], which is scoped to the caller.
    pub async fn list_all_jobs(&self, status_filter: Option<JobStatus>) -> Result<Vec<JobState>> {
        self.job_store.list_jobs(status_filter).await
    }

    /// Executes a job using `Query::submit_distributed` and writes results to the store.
    async fn execute_job(
        job_store: &JobStore,
        df: Arc<DataFusion>,
        job_id: &str,
        active_jobs: &RwLock<std::collections::HashMap<String, ActiveJobInfo>>,
        cancel: CancellationToken,
        start: Start,
    ) -> Result<()> {
        let resume = matches!(start, Start::TakenOver(_));
        let state = match start {
            Start::Submitted => match job_store.set_job_running(job_id).await {
                Ok(state) => state,
                // Cancelled (or otherwise finished) before this task started it.
                Err(Error::JobAlreadyFinished { .. }) => return Ok(()),
                Err(e) => return Err(e),
            },
            Start::TakenOver(state) => *state,
        };
        // Every write this task makes for the job is refused once another
        // scheduler takes the job over.
        let driver = state.driver().ok_or_else(|| Error::JobTakenOver {
            job_id: job_id.to_string(),
        })?;

        // Check for early cancellation
        if cancel.is_cancelled() {
            // Don't call cancel_job here - executor.cancel() already updates the
            // job state via its own cancel_job call.
            return Ok(());
        }

        // Build and submit the query using Query::submit_distributed
        let mut query_builder =
            QueryBuilder::new(&state.sql, Arc::clone(&df)).read_only(state.read_only);

        // Parse parameters if present
        if let Some(p) = state.parameters {
            match crate::datafusion::param_utils::convert_json_to_param_values(p) {
                Ok(params) => {
                    query_builder = query_builder.parameters(Some(params));
                }
                Err(e) => {
                    job_store
                        .fail_job(
                            job_id,
                            &driver,
                            JobErrorCode::ParameterBindingFailed,
                            e.to_string(),
                        )
                        .await?;
                    return Ok(());
                }
            }
        }

        let query = query_builder.build();

        let submit_result = if resume {
            query.resume_distributed(job_id).await
        } else {
            query.submit_distributed(job_id).await
        };
        let query_handle = match submit_result {
            Ok(handle) => handle,
            Err(e) => {
                let error_code = Self::query_error_to_code(&e);
                job_store
                    .fail_job(job_id, &driver, error_code, e.to_string())
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
                // Don't call cancel_job here - executor.cancel() already updates the
                // job state. Doing it here would race with the OCC write and cause
                // ConcurrentModification errors.
                Ok(())
            },
            () = timeout_fut => {
                tracing::debug!(job_id = %job_id, "Job timed out");
                if let Err(e) = query_handle.cancel().await {
                    tracing::error!("Failed to cancel the timed-out query '{job_id}': {e}");
                }
                job_store.fail_job(job_id, &driver, JobErrorCode::Timeout, "Job execution timed out".to_string()).await?;
                Ok(())
            }
            result_stream = query_handle.into_stream() => {
                // Wait for completion and get the result stream
                let result_stream = match result_stream {
                    Ok(stream) => stream,
                    Err(e) => {
                        let (error_code, error_msg) = Self::handle_error_to_code_and_msg(&e);
                        job_store.fail_job(job_id, &driver, error_code, error_msg).await?;
                        return Ok(());
                    }
                };

                // Write result chunks as batches arrive from the stream
                let job_result = match job_store
                    .write_result_chunks_from_stream(job_id, &driver, result_stream)
                    .await
                {
                    Ok(result) => result,
                    Err(e @ Error::JobTakenOver { .. }) => return Err(e),
                    Err(e) => {
                        job_store
                            .fail_job(job_id, &driver, JobErrorCode::FetchingResultsFailed, e.to_string())
                            .await?;
                        return Ok(());
                    }
                };

                // Mark job as succeeded
                job_store.complete_job(job_id, &driver, job_result).await?;

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
            Error::UnableToExecuteQuery { .. }
            | Error::TableAccessDisallowed { .. }
            | Error::AcceleratedTableNotSupportedInDistributedQuery { .. }
            | Error::CayenneCatalogTableNotSupportedInDistributedQuery { .. }
            | Error::TaskHistoryNotSupportedInDistributedQuery { .. } => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::datafusion::builder::DataFusionBuilder;
    use crate::jobs::PUBLIC_JOB_OWNER;
    use object_store::memory::InMemory;
    use runtime_status::RuntimeStatus;
    use tokio::runtime::Handle;

    const OWNER: &str = "apikey:0123456789abcdef";
    const OTHER: &str = "apikey:fedcba9876543210";

    fn executor(job_store: Arc<JobStore>) -> JobExecutor {
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .build(),
        );
        JobExecutor::new(job_store, df)
    }

    /// Creates a job owned by `owner` directly through the store, so the test
    /// exercises the read path without needing a live distributed executor.
    async fn seed_job(job_store: &JobStore, owner: &str) -> String {
        job_store
            .create_job(
                SubmitQueryRequest {
                    sql: "SELECT 1".to_string(),
                    parameters: None,
                    timeout_seconds: None,
                    maximum_size: None,
                },
                true,
                owner.to_string(),
            )
            .await
            .expect("job should be created")
            .job_id
    }

    /// Waits until `job_id` leaves the running state, for at most ten seconds.
    async fn wait_until_finished(job_store: &JobStore, job_id: &str) -> JobState {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let state = job_store.get_job(job_id).await.expect("job exists");
            if state.is_terminal() {
                return state;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "job {job_id} still {} after 10s",
                state.status
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Resuming re-plans the job's SQL, and planning is where a principal's access and
    /// masking apply. The job records only its submitter's opaque id, so a principal's
    /// job is failed with a request to resubmit rather than re-planned without them.
    #[tokio::test]
    async fn resume_fails_a_principals_job_instead_of_running_it_without_the_principal() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let job_id = seed_job(&job_store, OWNER).await;
        let running = job_store
            .set_job_running(&job_id)
            .await
            .expect("the lost scheduler had started the job");

        let executor = executor(Arc::clone(&job_store));
        executor.resume(&running).await;

        assert!(
            executor.active_jobs.read().await.is_empty(),
            "the job must not be re-driven"
        );
        let state = job_store.get_job(&job_id).await.expect("job exists");
        assert_eq!(state.status, JobStatus::Failed);
        let error = state.error.expect("the failure is recorded");
        assert!(
            matches!(error.error_code, JobErrorCode::SchedulerUnavailable),
            "{:?}",
            error.error_code
        );
        assert!(error.message.contains("Resubmit"), "{}", error.message);
    }

    /// A job submitted without a principal carries no identity to lose, so it is still
    /// re-driven. With no distributed scheduler here the re-drive fails, but with the
    /// submission error rather than the identity refusal.
    #[tokio::test]
    async fn resume_still_redrives_a_public_job() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let job_id = seed_job(&job_store, PUBLIC_JOB_OWNER).await;
        let running = job_store
            .set_job_running(&job_id)
            .await
            .expect("the lost scheduler had started the job");

        let executor = executor(Arc::clone(&job_store));
        executor.resume(&running).await;

        let state = wait_until_finished(&job_store, &job_id).await;
        let message = state.error.map(|e| e.message).unwrap_or_default();
        assert!(
            !message.contains("identity that submitted it"),
            "a public job is re-driven, not refused: {message}"
        );
    }

    /// A stream of one batch per entry of `batches`, each in an `id` column.
    fn int_batches(batches: Vec<Vec<i32>>) -> datafusion::execution::SendableRecordBatchStream {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batches: Vec<_> = batches
            .into_iter()
            .map(|values| {
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(values))],
                )
                .expect("a batch"))
            })
            .collect();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        ))
    }

    /// A former driver leaves two chunks; the driver that took the job over
    /// receives the same rows in one batch and completes the job with one
    /// chunk. The second chunk is not a result.
    #[tokio::test]
    async fn get_chunk_refuses_an_index_the_result_does_not_list() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let former = JobStore::new(Arc::clone(&store), "test", "node-a").with_chunk_size(2);
        let successor =
            Arc::new(JobStore::new(Arc::clone(&store), "test", "node-b").with_chunk_size(2));
        let job_id = seed_job(&former, PUBLIC_JOB_OWNER).await;
        let started = former.set_job_running(&job_id).await.expect("to start");
        let left = former
            .write_result_chunks_from_stream(
                &job_id,
                &started.driver().expect("a driver"),
                int_batches(vec![vec![9, 9], vec![9, 9]]),
            )
            .await
            .expect("the former driver writes while it drives");
        assert_eq!(left.chunk_indices, vec![0, 1]);

        let taken = successor
            .take_over_job(&started)
            .await
            .expect("to take over")
            .expect("the job is still driven as observed");
        let driver = taken.driver().expect("a driver");
        let result = successor
            .write_result_chunks_from_stream(&job_id, &driver, int_batches(vec![vec![1, 2, 3, 4]]))
            .await
            .expect("the driver writes results");
        assert_eq!(result.chunk_indices, vec![0]);
        successor
            .complete_job(&job_id, &driver, result)
            .await
            .expect("the driver completes the job");

        let executor = executor(successor);
        executor
            .get_chunk(&job_id, 0, PUBLIC_JOB_OWNER)
            .await
            .expect("the listed chunk");
        let err = executor
            .get_chunk(&job_id, 1, PUBLIC_JOB_OWNER)
            .await
            .expect_err("chunk 1 is not in the result");
        assert!(
            matches!(err, Error::ChunkNotFound { chunk_index: 1, .. }),
            "{err:?}"
        );
    }

    /// Object store that fails the first write marking an async-query job
    /// failed.
    #[derive(Debug)]
    struct FailFirstFailedWrite {
        inner: Arc<dyn object_store::ObjectStore>,
        failed: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for FailFirstFailedWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailFirstFailedWrite")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailFirstFailedWrite {
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let marks_failed = payload
                .iter()
                .any(|chunk| chunk.windows(8).any(|w| w == b"\"FAILED\""));
            if marks_failed && !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "the write did not land".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A principal's orphaned job whose failure write does not land stays
    /// orphaned, so the next recovery sweep fails it.
    #[tokio::test]
    async fn resume_fails_a_principals_job_on_a_later_sweep_after_a_failed_write() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let lost = JobStore::new(Arc::clone(&inner), "test", "node-lost");
        let job_id = seed_job(&lost, OWNER).await;
        let running = lost
            .set_job_running(&job_id)
            .await
            .expect("the lost scheduler had started the job");
        let flaky: Arc<dyn object_store::ObjectStore> = Arc::new(FailFirstFailedWrite {
            inner: Arc::clone(&inner),
            failed: std::sync::atomic::AtomicBool::new(false),
        });
        let recoverer = Arc::new(JobStore::new(flaky, "test", "node-b"));
        let executor = executor(Arc::clone(&recoverer));

        executor.resume(&running).await;
        let orphaned = recoverer.get_job(&job_id).await.expect("job exists");
        assert_eq!(orphaned.status, JobStatus::Running);
        assert_eq!(
            orphaned.scheduler_node.as_deref(),
            Some("node-lost"),
            "the job is still orphaned for the next sweep"
        );

        executor.resume(&orphaned).await;
        let state = recoverer.get_job(&job_id).await.expect("job exists");
        assert_eq!(state.status, JobStatus::Failed);
    }

    /// How a takeover write that landed reports back.
    #[derive(Debug, Clone, Copy)]
    enum LostResponse {
        /// The response never arrived.
        Error,
        /// The write was sent again after it landed, and its condition failed.
        Conflict,
    }

    /// Object store whose first write marking an async-query job running lands
    /// and then reports `lost`.
    #[derive(Debug)]
    struct LoseFirstRunningWrite {
        inner: Arc<dyn object_store::ObjectStore>,
        lost: LostResponse,
        reported: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for LoseFirstRunningWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LoseFirstRunningWrite")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for LoseFirstRunningWrite {
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let marks_running = payload
                .iter()
                .any(|chunk| chunk.windows(9).any(|w| w == b"\"RUNNING\""));
            let written = self.inner.put_opts(location, payload, opts).await?;
            if marks_running
                && !self
                    .reported
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(match self.lost {
                    LostResponse::Error => object_store::Error::Generic {
                        store: "test",
                        source: "the response did not arrive".into(),
                    },
                    LostResponse::Conflict => object_store::Error::Precondition {
                        path: location.to_string(),
                        source: "the object changed".into(),
                    },
                });
            }
            Ok(written)
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A takeover write that lands but reports an error: the recoverer reads
    /// the job back, finds it holds the job, and drives it.
    #[tokio::test]
    async fn resume_drives_a_job_whose_takeover_landed_but_reported_an_error() {
        for lost in [LostResponse::Error, LostResponse::Conflict] {
            let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let lost_scheduler = JobStore::new(Arc::clone(&inner), "test", "node-lost");
            let job_id = seed_job(&lost_scheduler, PUBLIC_JOB_OWNER).await;
            let running = lost_scheduler
                .set_job_running(&job_id)
                .await
                .expect("the lost scheduler had started the job");
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(LoseFirstRunningWrite {
                inner: Arc::clone(&inner),
                lost,
                reported: std::sync::atomic::AtomicBool::new(false),
            });
            let recoverer = Arc::new(JobStore::new(store, "test", "node-b"));
            let executor = executor(Arc::clone(&recoverer));

            executor.resume(&running).await;

            assert!(
                executor.active_jobs.read().await.contains_key(&job_id),
                "{lost:?}: the recoverer drives the job it took over"
            );
            let state = wait_until_finished(&recoverer, &job_id).await;
            assert_eq!(state.scheduler_node.as_deref(), Some("node-b"), "{lost:?}");
            assert_eq!(state.generation, Some(2), "{lost:?}");
        }
    }

    #[tokio::test]
    async fn get_status_refuses_a_job_another_principal_submitted() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = executor(Arc::clone(&job_store));
        let job_id = seed_job(&job_store, OWNER).await;

        executor
            .get_status(&job_id, OWNER)
            .await
            .expect("the submitting principal should read its own job");

        let err = executor
            .get_status(&job_id, OTHER)
            .await
            .expect_err("another principal must not read the job");
        assert!(
            matches!(err, super::super::error::Error::JobNotFound { .. }),
            "a job owned by someone else must report as missing, not as forbidden: {err:?}"
        );
    }

    /// Ownership is resolved before expiry, so a non-owner cannot tell an
    /// expired job from one that never existed. Without this ordering the
    /// expired job answers `JobResultsExpired` (HTTP 410) while a missing id
    /// answers `JobNotFound` (404), which confirms someone else's job id.
    #[tokio::test]
    async fn an_expired_job_reads_as_missing_to_a_non_owner() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = executor(Arc::clone(&job_store));
        let job_id = seed_job(&job_store, OWNER).await;

        let mut state = job_store
            .get_job(&job_id)
            .await
            .expect("the freshly created job should be readable");
        state.expires_at_ms = Some(1);
        job_store
            .update_job(&mut state)
            .await
            .expect("the job should be marked expired");

        let owner_err = executor
            .get_status(&job_id, OWNER)
            .await
            .expect_err("the owner should be told its results expired");
        assert!(
            matches!(
                owner_err,
                super::super::error::Error::JobResultsExpired { .. }
            ),
            "the owner keeps the precise expiry error: {owner_err:?}"
        );

        let other_err = executor
            .get_status(&job_id, OTHER)
            .await
            .expect_err("another principal must not read the job");
        assert!(
            matches!(other_err, super::super::error::Error::JobNotFound { .. }),
            "an expired job must be indistinguishable from a missing one: {other_err:?}"
        );
    }

    #[tokio::test]
    async fn get_chunk_refuses_a_job_another_principal_submitted() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = executor(Arc::clone(&job_store));
        let job_id = seed_job(&job_store, OWNER).await;

        let err = executor
            .get_chunk(&job_id, 0, OTHER)
            .await
            .expect_err("another principal must not read result chunks");
        assert!(
            matches!(err, super::super::error::Error::JobNotFound { .. }),
            "ownership must be resolved before the job's completion state: {err:?}"
        );
    }

    #[tokio::test]
    async fn cancel_refuses_a_job_another_principal_submitted() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = executor(Arc::clone(&job_store));
        let job_id = seed_job(&job_store, OWNER).await;

        let err = executor
            .cancel(&job_id, OTHER)
            .await
            .expect_err("another principal must not cancel the job");
        assert!(
            matches!(err, super::super::error::Error::JobNotFound { .. }),
            "cancellation must be refused before the job is signalled: {err:?}"
        );

        let state = executor
            .get_status(&job_id, OWNER)
            .await
            .expect("the job should still be readable by its owner");
        assert_eq!(
            state.status,
            JobStatus::Pending,
            "a refused cancellation must leave the job running"
        );
    }

    #[tokio::test]
    async fn list_jobs_returns_only_the_callers_jobs() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = executor(Arc::clone(&job_store));
        let mine = seed_job(&job_store, OWNER).await;
        let theirs = seed_job(&job_store, OTHER).await;

        let listed = executor
            .list_jobs(None, OWNER)
            .await
            .expect("listing should succeed");
        let ids: Vec<&str> = listed.iter().map(|j| j.job_id.as_str()).collect();
        assert_eq!(ids, vec![mine.as_str()]);

        let unauthenticated = executor
            .list_jobs(None, PUBLIC_JOB_OWNER)
            .await
            .expect("listing should succeed");
        assert!(
            unauthenticated.is_empty(),
            "the public scope must not see jobs submitted by a principal"
        );

        let all = executor
            .list_all_jobs(None)
            .await
            .expect("internal listing should succeed");
        assert_eq!(
            all.len(),
            2,
            "the internal recovery sweep still sees every job"
        );
        assert!(all.iter().any(|j| j.job_id == theirs));
    }
}
