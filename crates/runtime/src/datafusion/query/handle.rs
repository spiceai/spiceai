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

//! Distributed query handle for async query execution via Ballista.
//!
//! This module provides [`QueryHandle`] which represents a submitted distributed query job.
//! It encapsulates the Ballista job ID, scheduler reference, and methods for polling
//! job status and retrieving results.

use std::collections::{HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::datafusion::DataFusion;
use crate::datafusion::query::QueryTracker;
use crate::datafusion::query::error_code::ErrorCode;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::serde::protobuf::job_status;
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_scheduler::scheduler_server::SchedulerServer;
use cache::key::RawCacheKey;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use runtime_request_context::RequestContext;
use snafu::Snafu;
use tokio_util::sync::CancellationToken;

/// Interval between job status polls when waiting for Ballista job completion.
const JOB_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of poll iterations before timing out a job.
/// At 100ms poll interval, this allows ~1 hour of job execution time.
const MAX_JOB_POLL_ITERATIONS: u64 = 36_000;

/// Default max message size (16MB matches typical default).
const MAX_PARTITION_RETRIEVAL_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Use block transfer mode instead of Arrow Flight for partition retrieval.
/// Block transfer is more efficient for large result sets within a cluster.
const USE_FLIGHT_TRANSFER: bool = false;

/// Status of a distributed query job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistributedJobStatus {
    /// Job is waiting to be scheduled.
    Queued,
    /// Job is currently executing.
    Running,
    /// Job completed successfully.
    Succeeded,
    /// Job failed with an error.
    Failed(String),
    /// Job was cancelled.
    Cancelled,
}

/// Error type for distributed query handle operations.
#[derive(Debug, Snafu)]
pub enum QueryHandleError {
    /// Job timed out.
    #[snafu(display("Job {ballista_job_id} timed out after {poll_count} poll iterations"))]
    JobTimeout {
        ballista_job_id: String,
        poll_count: u64,
    },

    /// Job was cancelled.
    #[snafu(display("Job was cancelled"))]
    JobCancelled,

    /// Job execution failed.
    #[snafu(display("Job execution failed: {message}"))]
    JobFailed { message: String },

    /// Failed to get job status from scheduler.
    #[snafu(display("Failed to get job status: {message}"))]
    StatusError { message: String },

    /// Failed to convert partition location.
    #[snafu(display("Failed to convert partition location {index}: {message}"))]
    PartitionLocationError { index: usize, message: String },

    /// Job not found in scheduler after submission.
    #[snafu(display("Job {ballista_job_id} not found after multiple polls"))]
    JobNotFound { ballista_job_id: String },
}

pub type Result<T, E = QueryHandleError> = std::result::Result<T, E>;

/// Internal state of a query handle.
#[derive(Clone)]
enum QueryHandleState {
    /// Query was submitted to Ballista and is being executed.
    Running {
        /// Reference to the Ballista scheduler server.
        scheduler: Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>,
    },
    /// Query results were retrieved from cache.
    Cached {
        /// The cached result stream (wrapped in Mutex for interior mutability).
        cached_stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    },
}

/// A handle to a distributed query job submitted to the Ballista scheduler.
///
/// This struct represents a query that has been submitted for distributed execution.
/// It provides methods for:
/// - Polling the job status
/// - Cancelling the job
/// - Waiting for completion and retrieving results as a stream
/// - Caching results based on the input cache key
///
/// A `QueryHandle` can represent either:
/// - An actively running Ballista job (status can be polled)
/// - A cache hit where results are immediately available
#[derive(Clone)]
pub struct QueryHandle {
    /// The Ballista scheduler job ID (or a synthetic ID for cached results).
    ballista_job_id: String,
    /// Internal state (running or cached).
    state: QueryHandleState,
    /// Result schema from the logical plan.
    schema: SchemaRef,
    /// Input datasets for the query.
    datasets: Option<Arc<HashSet<TableReference>>>,
    /// Reference to `DataFusion` instance.
    df: Arc<DataFusion>,
    /// Cache key for the query results (if caching is enabled).
    cache_key: Option<RawCacheKey>,
    /// Cancellation token for the job.
    cancel_token: CancellationToken,
    /// Optional query tracker for monitoring query execution.
    tracker: Arc<Mutex<Option<QueryTracker>>>,
    /// Request context for tracking and metrics.
    request_context: Arc<RequestContext>,
}

impl std::fmt::Debug for QueryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryHandle")
            .field("ballista_job_id", &self.ballista_job_id)
            .field(
                "is_cached",
                &matches!(self.state, QueryHandleState::Cached { .. }),
            )
            .field(
                "cache_key",
                &self.cache_key.as_ref().map(RawCacheKey::as_u64),
            )
            .finish_non_exhaustive()
    }
}

impl QueryHandle {
    /// Creates a new `QueryHandle` for a submitted distributed query job.
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        ballista_job_id: String,
        scheduler: Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>>,
        schema: SchemaRef,
        datasets: Arc<HashSet<TableReference>>,
        df: Arc<DataFusion>,
        cache_key: Option<RawCacheKey>,
        tracker: Option<QueryTracker>,
        request_context: Arc<RequestContext>,
    ) -> Self {
        Self {
            ballista_job_id,
            state: QueryHandleState::Running { scheduler },
            schema,
            datasets: Some(datasets),
            df,
            cache_key,
            cancel_token: CancellationToken::new(),
            tracker: Arc::new(Mutex::new(tracker)),
            request_context,
        }
    }

    /// Creates a new `QueryHandle` with a cached result stream.
    ///
    /// This is used when the query results are retrieved from the cache
    /// and no Ballista job needs to be executed.
    #[must_use]
    pub(crate) fn new_with_cached_result(
        job_id: String,
        schema: SchemaRef,
        df: Arc<DataFusion>,
        cache_key: Option<RawCacheKey>,
        cached_stream: SendableRecordBatchStream,
        request_context: Arc<RequestContext>,
    ) -> Self {
        Self {
            ballista_job_id: job_id,
            state: QueryHandleState::Cached {
                cached_stream: Arc::new(Mutex::new(Some(cached_stream))),
            },
            datasets: None,
            schema,
            df,
            cache_key,
            cancel_token: CancellationToken::new(),
            tracker: Arc::new(Mutex::new(None)),
            request_context,
        }
    }

    /// Returns the Ballista job ID (or synthetic ID for cached results).
    #[must_use]
    pub fn ballista_job_id(&self) -> &str {
        &self.ballista_job_id
    }

    /// Returns true if this handle represents a cache hit.
    #[must_use]
    pub fn is_cached(&self) -> bool {
        matches!(self.state, QueryHandleState::Cached { .. })
    }

    /// Returns the result schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Returns the cache key if caching is enabled.
    #[must_use]
    pub fn cache_key(&self) -> Option<&RawCacheKey> {
        self.cache_key.as_ref()
    }

    /// Returns the cancellation token for this job.
    #[must_use]
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Polls the current status of the job.
    ///
    /// Returns the job status or an error if the status cannot be retrieved.
    /// For cached results, always returns `DistributedJobStatus::Succeeded`.
    pub async fn poll_status(&self) -> Result<DistributedJobStatus> {
        let QueryHandleState::Running { scheduler } = &self.state else {
            // Cached results are always "succeeded"
            return Ok(DistributedJobStatus::Succeeded);
        };

        let status = scheduler
            .state
            .task_manager
            .get_job_status(&self.ballista_job_id)
            .await
            .map_err(|e| QueryHandleError::StatusError {
                message: e.to_string(),
            })?;

        let Some(job_status) = status else {
            return Ok(DistributedJobStatus::Queued);
        };

        match job_status.status {
            Some(job_status::Status::Successful(_)) => Ok(DistributedJobStatus::Succeeded),
            Some(job_status::Status::Failed(failed)) => {
                Ok(DistributedJobStatus::Failed(failed.error))
            }
            Some(job_status::Status::Queued(_)) | None => Ok(DistributedJobStatus::Queued),
            Some(job_status::Status::Running(_)) => Ok(DistributedJobStatus::Running),
        }
    }

    /// Cancels the job.
    ///
    /// Signals the cancellation token and requests cancellation from the Ballista scheduler.
    /// For cached results, this is a no-op since there's no job to cancel.
    pub async fn cancel(&self) -> Result<()> {
        self.cancel_token.cancel();

        if let QueryHandleState::Running { scheduler } = &self.state {
            scheduler
                .cancel_job(self.ballista_job_id.clone())
                .await
                .map_err(|e| QueryHandleError::StatusError {
                    message: format!("Failed to cancel job: {e}"),
                })?;
        }
        Ok(())
    }

    /// Waits for the job to complete and returns the output partition locations.
    ///
    /// This method polls the scheduler until the job reaches a terminal state.
    /// If the job succeeds, it returns the partition locations where results are stored.
    /// If the job fails or is cancelled, it returns an appropriate error.
    ///
    /// For cached results, this returns an empty vec (results are already available).
    pub async fn wait_for_completion(&self) -> Result<Vec<PartitionLocation>> {
        match &self.state {
            QueryHandleState::Running { scheduler } => {
                self.poll_until_complete(scheduler, &self.cancel_token)
                    .await
            }
            QueryHandleState::Cached { .. } => {
                // Cached results don't need to wait for completion
                Ok(Vec::new())
            }
        }
    }

    /// Polls the scheduler until the job reaches a terminal state.
    async fn poll_until_complete(
        &self,
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
        cancel: &CancellationToken,
    ) -> Result<Vec<PartitionLocation>> {
        let mut missing_retry_count = 0;
        let mut poll_count: u64 = 0;

        loop {
            poll_count += 1;
            if poll_count > MAX_JOB_POLL_ITERATIONS {
                let _ = scheduler.cancel_job(self.ballista_job_id.clone()).await;
                let err = QueryHandleError::JobTimeout {
                    ballista_job_id: self.ballista_job_id.clone(),
                    poll_count,
                };
                self.finish_tracker_with_error(&err);
                return Err(err);
            }

            // Check for cancellation
            if cancel.is_cancelled() {
                let _ = scheduler.cancel_job(self.ballista_job_id.clone()).await;
                let err = QueryHandleError::JobCancelled;
                self.finish_tracker_with_error(&err);
                return Err(err);
            }

            // Get job status from scheduler
            let status = scheduler
                .state
                .task_manager
                .get_job_status(&self.ballista_job_id)
                .await
                .map_err(|e| {
                    let err = QueryHandleError::StatusError {
                        message: e.to_string(),
                    };
                    self.finish_tracker_with_error(&err);
                    err
                })?;

            if let Some(job_status) = status {
                match job_status.status {
                    Some(job_status::Status::Successful(success)) => {
                        // Convert protobuf partition locations to core types.
                        // All partition locations must convert successfully to ensure
                        // complete results are returned (data correctness requirement).
                        let mut locations = Vec::with_capacity(success.partition_location.len());
                        for (i, loc) in success.partition_location.into_iter().enumerate() {
                            let partition_loc: PartitionLocation = loc.try_into().map_err(
                                |e: ballista_core::error::BallistaError| {
                                    let err = QueryHandleError::PartitionLocationError {
                                        index: i,
                                        message: e.to_string(),
                                    };
                                    self.finish_tracker_with_error(&err);
                                    err
                                },
                            )?;
                            locations.push(partition_loc);
                        }

                        // Finish the tracker successfully
                        self.finish_tracker_success();
                        return Ok(locations);
                    }
                    Some(job_status::Status::Failed(failed)) => {
                        let err = QueryHandleError::JobFailed {
                            message: failed.error,
                        };
                        self.finish_tracker_with_error(&err);
                        return Err(err);
                    }
                    Some(job_status::Status::Queued(_) | job_status::Status::Running(_)) | None => {
                        // Still in progress, continue polling
                    }
                }
            } else {
                missing_retry_count += 1;
                if missing_retry_count >= 5 {
                    let err = QueryHandleError::JobNotFound {
                        ballista_job_id: self.ballista_job_id.clone(),
                    };
                    self.finish_tracker_with_error(&err);
                    return Err(err);
                }
            }

            // Wait before next poll, checking for cancellation
            tokio::select! {
                () = tokio::time::sleep(JOB_POLL_INTERVAL) => {}
                () = cancel.cancelled() => {
                    let _ = scheduler.cancel_job(self.ballista_job_id.clone()).await;
                    let err = QueryHandleError::JobCancelled;
                    self.finish_tracker_with_error(&err);
                    return Err(err);
                }
            }
        }
    }

    /// Finishes the query tracker with an error.
    fn finish_tracker_with_error(&self, error: &QueryHandleError) {
        if let Some(tracker) = self.tracker.lock().take() {
            let error_code = match error {
                QueryHandleError::JobTimeout { .. }
                | QueryHandleError::JobCancelled
                | QueryHandleError::JobFailed { .. } => ErrorCode::QueryExecutionError,
                QueryHandleError::StatusError { .. }
                | QueryHandleError::PartitionLocationError { .. }
                | QueryHandleError::JobNotFound { .. } => ErrorCode::InternalError,
            };
            tracker.finish_with_error(&self.request_context, error.to_string(), error_code);
        }
    }

    /// Finishes the query tracker successfully.
    fn finish_tracker_success(&self) {
        if let Some(tracker) = self.tracker.lock().take() {
            tracker.finish(&self.request_context, &Arc::from(""));
        }
    }

    /// Waits for the job to complete and returns a stream of result batches.
    ///
    /// This method waits for the job to complete, then creates a stream that
    /// lazily fetches results from the partition locations.
    ///
    /// If caching is enabled (via `cache_key`), results will also be cached
    /// as they are streamed.
    ///
    /// For cached results, returns the cached stream directly.
    pub async fn into_stream(&self) -> Result<SendableRecordBatchStream> {
        match &self.state {
            QueryHandleState::Cached { cached_stream } => {
                // Return the cached stream directly
                let stream =
                    cached_stream
                        .lock()
                        .take()
                        .ok_or_else(|| QueryHandleError::JobFailed {
                            message: "Cached stream already consumed".to_string(),
                        })?;
                Ok(stream)
            }
            QueryHandleState::Running { scheduler } => {
                // Wait for job completion and fetch results
                let locations = self
                    .poll_until_complete(scheduler, &self.cancel_token)
                    .await?;
                Ok(self.fetch_results_stream(locations))
            }
        }
    }

    /// Creates a stream that lazily fetches results from the partition locations.
    fn fetch_results_stream(&self, locations: Vec<PartitionLocation>) -> SendableRecordBatchStream {
        let use_tls = self.df.cluster_config.client_tls_config().is_some();

        // If TLS is configured, create a custom endpoint override function
        let customize_endpoint =
            if let Some(tls_config) = self.df.cluster_config.client_tls_config() {
                let tls = tls_config.clone();
                let override_fn: ballista_core::extension::EndpointOverrideFn =
                    Arc::new(move |endpoint: tonic::transport::Endpoint| {
                        endpoint
                            .tls_config(tls.clone())
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    });
                Some(Arc::new(BallistaConfigGrpcEndpoint::new(override_fn)))
            } else {
                None
            };

        let stream =
            PartitionResultStream::new(locations, use_tls, customize_endpoint, self.schema());

        // Wrap with cache if cache key is provided
        if let (Some(cache_key), Some(cache_provider)) =
            (self.cache_key, self.df.results_cache_provider())
            && let Some(datasets) = &self.datasets
        {
            cache::to_cached_record_batch_stream(
                cache_provider,
                Box::pin(stream),
                cache_key,
                Arc::clone(datasets),
            )
        } else {
            Box::pin(stream)
        }
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
struct PartitionResultStream {
    /// Remaining partition locations to fetch from
    locations: VecDeque<PartitionLocation>,
    /// Whether to use TLS for connections
    use_tls: bool,
    /// Optional endpoint customization for TLS
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    /// Schema for the result stream
    schema: SchemaRef,
    /// Current record batch stream being consumed (lazily initialized)
    current_record_batch_stream: Option<SendableRecordBatchStream>,
    /// Future for establishing the next partition stream
    next_partition_stream: Option<NextPartitionResultStream>,
}

impl PartitionResultStream {
    fn new(
        locations: Vec<PartitionLocation>,
        use_tls: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            locations: locations.into(),
            use_tls,
            customize_endpoint,
            schema,
            current_record_batch_stream: None,
            next_partition_stream: None,
        }
    }

    /// Creates a future that connects to a partition location and returns its stream.
    fn connect_to_partition(
        location: PartitionLocation,
        use_tls: bool,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
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

impl datafusion::physical_plan::RecordBatchStream for PartitionResultStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
