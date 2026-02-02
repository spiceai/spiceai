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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::job_status;
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::{Stream, StreamExt};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::datafusion::DataFusion;

use super::Result;
use super::state::{JobState, JobStatus};
use super::store::JobStore;

/// Interval between job status polls when waiting for Ballista job completion.
const JOB_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of poll iterations before timing out a job.
/// At 100ms poll interval, this allows ~1 hour of job execution time.
/// Convert polling to notifiers: <https://github.com/spiceai/spiceai/issues/9223>
const MAX_JOB_POLL_ITERATIONS: u64 = 36_000;

/// Default max message size (16MB matches typical default).
const MAX_PARTITION_RETRIEVAL_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Use block transfer mode instead of Arrow Flight for partition retrieval.
/// Block transfer is more efficient for large result sets within a cluster.
const USE_FLIGHT_TRANSFER: bool = false;

/// Tracks an active job's cancellation token and Ballista job ID (once submitted).
struct ActiveJobInfo {
    cancel_token: CancellationToken,
    /// The Ballista scheduler job ID, set once submitted to the scheduler.
    ballista_job_id: Option<String>,
}

/// Manages background execution of async query jobs.
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

    /// Returns a reference to the scheduler server if available.
    fn scheduler_server(
        df: &DataFusion,
    ) -> Option<Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>> {
        df.scheduler_server.try_read().ok()?.clone()
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
        let mut active = self.active_jobs.write().await;
        active.insert(
            job_id.clone(),
            ActiveJobInfo {
                cancel_token: cancel_token.clone(),
                ballista_job_id: None,
            },
        );

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
                let mut active = active_jobs.write().await;
                active.remove(&job_id_clone);

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
        // Signal cancellation and get Ballista job ID if available
        let ballista_job_id = {
            let active = self.active_jobs.read().await;
            if let Some(info) = active.get(job_id) {
                info.cancel_token.cancel();
                info.ballista_job_id.clone()
            } else {
                None
            }
        };

        // Cancel in Ballista if we have a Ballista job ID and scheduler is available
        if let (Some(ballista_id), Some(scheduler)) =
            (ballista_job_id, Self::scheduler_server(&self.df))
            && let Err(e) = scheduler.cancel_job(ballista_id.clone()).await
        {
            tracing::warn!(
                job_id,
                ballista_job_id = %ballista_id,
                error = %e,
                "Failed to cancel job in Ballista scheduler"
            );
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

        // Get the scheduler server - required for job submission
        let Some(scheduler) = Self::scheduler_server(&df) else {
            job_store
                .fail_job(
                    job_id,
                    "SCHEDULER_UNAVAILABLE",
                    "Scheduler server not available",
                )
                .await?;
            return Ok(());
        };

        // Create a session context for this job using the scheduler's session manager.
        // We use SessionConfig::new_with_ballista() to get a config compatible with Ballista.
        let session_config = datafusion::prelude::SessionConfig::new_with_ballista();
        let session_ctx = match scheduler
            .state
            .session_manager
            .create_or_update_session(job_id, &session_config)
            .await
        {
            Ok(ctx) => ctx,
            Err(e) => {
                job_store
                    .fail_job(job_id, "SESSION_ERROR", e.to_string())
                    .await?;
                return Ok(());
            }
        };

        // Parse and create the logical plan
        let logical_plan = match Self::create_logical_plan(&session_ctx, &state.sql).await {
            Ok(plan) => plan,
            Err(e) => {
                job_store
                    .fail_job(job_id, "QUERY_PLANNING", e.to_string())
                    .await?;
                return Ok(());
            }
        };

        // Check for cancellation before submitting
        if cancel.is_cancelled() {
            job_store.cancel_job(job_id).await?;
            return Ok(());
        }

        // Submit the job to the Ballista scheduler
        let ballista_job_id = match scheduler
            .submit_job(job_id, session_ctx, &logical_plan)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                job_store
                    .fail_job(job_id, "JOB_SUBMISSION", e.to_string())
                    .await?;
                return Ok(());
            }
        };

        tracing::debug!(
            job_id,
            ballista_job_id = %ballista_job_id,
            "Job submitted to Ballista scheduler"
        );

        // Store the Ballista job ID for cancellation
        let mut active = active_jobs.write().await;
        if let Some(info) = active.get_mut(job_id) {
            info.ballista_job_id = Some(ballista_job_id.clone());
        }

        // Poll for job completion
        let poll_result = Self::poll_job_status(&scheduler, &ballista_job_id, &cancel).await;

        match poll_result {
            Ok(output_locations) => {
                // Get the schema from the logical plan for the result stream
                let schema: SchemaRef = Arc::new(logical_plan.schema().as_arrow().clone());

                // Create a stream that lazily fetches results from partition locations
                let result_stream = Self::fetch_results_stream(&df, output_locations, schema);

                // Write result chunks as batches arrive from the stream
                let job_result = match job_store
                    .write_result_chunks_from_stream(job_id, result_stream)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        job_store
                            .fail_job(job_id, "RESULT_FETCH", e.to_string())
                            .await?;
                        return Ok(());
                    }
                };

                // Mark job as succeeded
                job_store.complete_job(job_id, job_result).await?;
            }
            Err(JobPollError::Cancelled) => {
                job_store.cancel_job(job_id).await?;
            }
            Err(JobPollError::Failed(msg)) => {
                job_store.fail_job(job_id, "QUERY_EXECUTION", msg).await?;
            }
        }

        Ok(())
    }

    /// Creates a logical plan from a SQL string.
    async fn create_logical_plan(
        ctx: &SessionContext,
        sql: &str,
    ) -> std::result::Result<
        datafusion::logical_expr::LogicalPlan,
        datafusion::error::DataFusionError,
    > {
        ctx.state().create_logical_plan(sql).await
    }

    /// Polls the Ballista scheduler for job completion.
    ///
    /// Returns the output partition locations on success.
    async fn poll_job_status(
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
        ballista_job_id: &str,
        cancel: &CancellationToken,
    ) -> std::result::Result<Vec<PartitionLocation>, JobPollError> {
        let mut missing_retry_count = 0;
        let mut poll_count: u64 = 0;
        loop {
            poll_count += 1;
            if poll_count > MAX_JOB_POLL_ITERATIONS {
                let _ = scheduler.cancel_job(ballista_job_id.to_string()).await;
                return Err(JobPollError::Failed(format!(
                    "Job {ballista_job_id} timed out after {poll_count} poll iterations"
                )));
            }

            // Check for cancellation
            if cancel.is_cancelled() {
                // Try to cancel in Ballista
                let _ = scheduler.cancel_job(ballista_job_id.to_string()).await;
                return Err(JobPollError::Cancelled);
            }

            // Get job status from scheduler's task manager
            let status = scheduler
                .state
                .task_manager
                .get_job_status(ballista_job_id)
                .await
                .map_err(|e| JobPollError::Failed(e.to_string()))?;

            if let Some(job_status) = status {
                match job_status.status {
                    Some(job_status::Status::Successful(success)) => {
                        // Convert protobuf partition locations to core types.
                        // All partition locations must convert successfully to ensure
                        // complete results are returned (data correctness requirement).
                        let mut locations = Vec::with_capacity(success.partition_location.len());
                        for (i, loc) in success.partition_location.into_iter().enumerate() {
                            match loc.try_into() {
                                Ok(partition_loc) => locations.push(partition_loc),
                                Err(e) => {
                                    return Err(JobPollError::Failed(format!(
                                        "Failed to convert partition location {i}: {e}"
                                    )));
                                }
                            }
                        }
                        return Ok(locations);
                    }
                    Some(job_status::Status::Failed(failed)) => {
                        return Err(JobPollError::Failed(failed.error));
                    }
                    Some(job_status::Status::Queued(_) | job_status::Status::Running(_)) | None => {
                        // Still in progress or unknown status, continue polling
                    }
                }
            } else {
                missing_retry_count += 1;
                // If job status is missing for several polls, assume it was cleaned up, failed to submit, or some other issue
                if missing_retry_count >= 5 {
                    return Err(JobPollError::Failed(format!(
                        "Ballista job {ballista_job_id} not found after multiple polls"
                    )));
                }
            }

            // Wait before next poll, checking for cancellation
            tokio::select! {
                () = tokio::time::sleep(JOB_POLL_INTERVAL) => {}
                () = cancel.cancelled() => {
                    let _ = scheduler.cancel_job(ballista_job_id.to_string()).await;
                    return Err(JobPollError::Cancelled);
                }
            }
        }
    }

    /// Fetches results from the output partition locations as a stream.
    ///
    /// Returns a `SendableRecordBatchStream` that lazily fetches batches from
    /// partition locations, avoiding loading all results into memory at once.
    fn fetch_results_stream(
        df: &Arc<DataFusion>,
        locations: Vec<PartitionLocation>,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        let use_tls = df.cluster_config.client_tls_config().is_some();

        // If TLS is configured, create a custom endpoint override function
        let customize_endpoint = if let Some(tls_config) = df.cluster_config.client_tls_config() {
            let tls = tls_config.clone();
            let override_fn: ballista_core::extension::EndpointOverrideFn =
                Arc::new(move |endpoint: tonic::transport::Endpoint| {
                    endpoint
                        .tls_config(tls.clone())
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                });
            Some(Arc::new(
                ballista_core::extension::BallistaConfigGrpcEndpoint::new(override_fn),
            ))
        } else {
            None
        };

        let stream = PartitionResultStream::new(locations, use_tls, customize_endpoint);
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

type NextPartitionResultStream = Pin<
    Box<
        dyn std::future::Future<
                Output = std::result::Result<
                    SendableRecordBatchStream,
                    datafusion::error::DataFusionError,
                >,
            > + Send,
    >,
>;

/// A stream that lazily fetches `RecordBatch`es from multiple partition locations.
///
/// Connects to partition executors one at a time and streams their batches,
/// avoiding loading all results into memory at once.
///
/// Partitions already return a `SendableRecordBatchStream`, so when we connect to a partition we pull its stream and return the items from it until it is exhausted.
///
/// The next partition is then connected to and its stream consumed, until all partitions are processed.
struct PartitionResultStream {
    /// Remaining partition locations to fetch from
    locations: std::collections::VecDeque<PartitionLocation>,
    /// Whether to use TLS for connections
    use_tls: bool,
    /// Optional endpoint customization for TLS
    customize_endpoint: Option<Arc<ballista_core::extension::BallistaConfigGrpcEndpoint>>,
    /// Current record batch stream being consumed (lazily initialized)
    current_record_batch_stream: Option<SendableRecordBatchStream>,
    /// Future for establishing the next partition stream (when transitioning between partitions)
    next_partition_stream: Option<NextPartitionResultStream>,
}

impl PartitionResultStream {
    fn new(
        locations: Vec<PartitionLocation>,
        use_tls: bool,
        customize_endpoint: Option<Arc<ballista_core::extension::BallistaConfigGrpcEndpoint>>,
    ) -> Self {
        Self {
            locations: locations.into(),
            use_tls,
            customize_endpoint,
            current_record_batch_stream: None,
            next_partition_stream: None,
        }
    }

    /// Creates a future that connects to a partition location and returns its stream.
    fn connect_to_partition(
        location: PartitionLocation,
        use_tls: bool,
        customize_endpoint: Option<Arc<ballista_core::extension::BallistaConfigGrpcEndpoint>>,
    ) -> NextPartitionResultStream {
        Box::pin(async move {
            let executor_meta = &location.executor_meta;

            // Create Ballista client to connect to executor
            let mut client = ballista_core::client::BallistaClient::try_new(
                &executor_meta.host,
                executor_meta.port,
                MAX_PARTITION_RETRIEVAL_MESSAGE_SIZE,
                use_tls,
                customize_endpoint,
            )
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::External(Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!(
                        "Failed to create Ballista client for executor {}:{}: {e}",
                        executor_meta.host, executor_meta.port
                    ),
                )))
            })?;

            let stream = client
                .fetch_partition(
                    &executor_meta.id,
                    &location.partition_id,
                    &location.path,
                    &executor_meta.host,
                    executor_meta.port,
                    USE_FLIGHT_TRANSFER,
                )
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::External(Box::new(std::io::Error::other(
                        format!(
                            "Failed to fetch partition {}: {e}",
                            location.partition_id.partition_id
                        ),
                    )))
                })?;

            Ok(stream)
        })
    }
}

impl Stream for PartitionResultStream {
    type Item = std::result::Result<RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // If we have a pending stream, poll it first
            if let Some(ref mut pending) = self.next_partition_stream {
                match pending.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.current_record_batch_stream = Some(stream);
                        self.next_partition_stream = None;
                        // Continue to poll the new stream
                    }
                    Poll::Ready(Err(e)) => {
                        self.next_partition_stream = None;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // If we have a current stream, poll it
            if let Some(ref mut stream) = self.current_record_batch_stream {
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(batch)) => return Poll::Ready(Some(batch)),
                    Poll::Ready(None) => {
                        // Current stream exhausted, move to next partition
                        self.current_record_batch_stream = None;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // No current stream, try to start the next partition
            if let Some(location) = self.locations.pop_front() {
                self.next_partition_stream = Some(Self::connect_to_partition(
                    location,
                    self.use_tls,
                    self.customize_endpoint.clone(),
                ));
                // Loop back to poll the pending connection
            } else {
                // No more partitions, stream is complete
                return Poll::Ready(None);
            }
        }
    }
}

/// Error type for job polling.
enum JobPollError {
    Cancelled,
    Failed(String),
}
