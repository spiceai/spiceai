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

use crate::datafusion::DataFusion;
use crate::datafusion::query::QueryTracker;
use crate::datafusion::query::error_code::ErrorCode;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::serde::protobuf::job_status;
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_scheduler::scheduler_server::SchedulerServer;
use ballista_scheduler::scheduler_server::job_state_event::JobState as BallistaJobState;
use cache::key::RawCacheKey;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use runtime_request_context::RequestContext;
use snafu::Snafu;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};

/// Default max message size (16MB matches typical default).
const MAX_PARTITION_RETRIEVAL_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Use block transfer mode instead of Arrow Flight for partition retrieval.
/// Block transfer is more efficient for large result sets within a cluster.
const USE_FLIGHT_TRANSFER: bool = false;

/// Number of polls to retry when a Completed event arrives before terminal status persistence.
const COMPLETED_STATUS_MAX_POLLS: usize = 20;

/// Delay between scheduler status polls after receiving a Completed event.
const COMPLETED_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(100);

fn describe_job_status(status: Option<&job_status::Status>) -> &'static str {
    match status {
        Some(job_status::Status::Queued(_)) => "queued",
        Some(job_status::Status::Running(_)) => "running",
        Some(job_status::Status::Successful(_)) => "successful",
        Some(job_status::Status::Failed(_)) => "failed",
        None => "unknown",
    }
}

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
    /// `task_history` span covering the lifetime of this distributed query.
    /// Created at submission and held in a slot shared by every clone so it
    /// can be **taken** at finalization. The span moves into the spawned
    /// finalize future via `.instrument(span)`; once that future ends the
    /// span has no surviving clones and the OTel layer closes it, emitting
    /// the `task_history` row.
    ///
    /// Storing the span behind an `Option` (rather than as a plain field)
    /// is what keeps `execution_duration_ms` honest: if every clone of
    /// `QueryHandle` retained its own clone of the span, the OTel span
    /// would stay open until the *last* handle was dropped — which can be
    /// long after the Ballista job finished — and the duration column
    /// would include arbitrary post-completion handle lifetime. Taking
    /// here means the span closes at finalization (success, failure, or
    /// `Drop`-orphan), not at handle-drop.
    ///
    /// Mutex + `Option::take` also gives us idempotent finalization: only
    /// the first call gets the span; later attempts see `None` and skip.
    task_history_span: Arc<Mutex<Option<Span>>>,
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
        task_history_span: Span,
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
            task_history_span: Arc::new(Mutex::new(Some(task_history_span))),
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
            task_history_span: Arc::new(Mutex::new(None)),
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
    ///
    /// Also finalizes the `task_history` row with `JobCancelled` error so
    /// the recorded `error_message` reflects an explicit user cancel — not
    /// the `Drop` guard's "client disconnected before completion" — when
    /// callers `cancel()` and then drop the handle without ever draining
    /// the result stream. `finish_tracker_with_error` is a no-op if some
    /// other terminal path (e.g., `wait_for_complete` reacting to the
    /// cancel token) raced ahead and already finalized.
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
        self.finish_tracker_with_error(&QueryHandleError::JobCancelled);
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
                self.wait_for_complete(scheduler, &self.cancel_token).await
            }
            QueryHandleState::Cached { .. } => {
                // Cached results don't need to wait for completion
                Ok(Vec::new())
            }
        }
    }

    /// Waits for the job to complete using the broadcast channel for notifications.
    ///
    /// This subscribes to job state events from the scheduler and waits for a terminal
    /// state (completed, failed, or cancelled) for this job. This is more efficient than
    /// polling as it only wakes when state changes occur.
    async fn wait_for_complete(
        &self,
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
        cancel: &CancellationToken,
    ) -> Result<Vec<PartitionLocation>> {
        // Subscribe to job state events from the scheduler's broadcast channel
        let mut receiver = scheduler.subscribe_job_updates();

        // Check if the job is already complete before subscribing
        // This handles the race condition where the job completes before we subscribe
        if let Some(locations) = self.check_job_completed(scheduler).await? {
            return Ok(locations);
        }

        loop {
            tokio::select! {
                // Wait for job state events from the broadcast channel
                event_result = receiver.recv() => {
                    match event_result {
                        Ok(event) => {
                            // Only process events for our job
                            if event.job_id != self.ballista_job_id {
                                continue;
                            }

                            match event.state {
                                BallistaJobState::Completed => {
                                    // Job completed - fetch the partition locations from the scheduler
                                    return self.fetch_completed_job_locations(scheduler).await;
                                }
                                BallistaJobState::Failed(error_message) => {
                                    let err = QueryHandleError::JobFailed {
                                        message: error_message,
                                    };
                                    self.finish_tracker_with_error(&err);
                                    return Err(err);
                                }
                                BallistaJobState::Cancelled => {
                                    let err = QueryHandleError::JobCancelled;
                                    self.finish_tracker_with_error(&err);
                                    return Err(err);
                                }
                                BallistaJobState::Queued | BallistaJobState::Running => {
                                    // Job still in progress, continue waiting for terminal state
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            // Receiver fell behind - some events were dropped.
                            // Check current job status to see if we missed a terminal state.
                            tracing::debug!(
                                job_id = %self.ballista_job_id,
                                skipped_events = skipped,
                                "Job state event receiver lagged behind, checking current job status"
                            );

                            if let Some(locations) = self.check_job_completed(scheduler).await? {
                                return Ok(locations);
                            }
                            // Job still in progress, continue listening for events
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Channel closed - scheduler is shutting down
                            let err = QueryHandleError::StatusError {
                                message: "Job state event channel closed".to_string(),
                            };
                            self.finish_tracker_with_error(&err);
                            return Err(err);
                        }
                    }
                }
                // Check for cancellation
                () = cancel.cancelled() => {
                    let _ = scheduler.cancel_job(self.ballista_job_id.clone()).await;
                    let err = QueryHandleError::JobCancelled;
                    self.finish_tracker_with_error(&err);
                    return Err(err);
                }
            }
        }
    }

    /// Checks if the job is already completed and returns partition locations if so.
    ///
    /// Returns `Ok(Some(locations))` if the job is complete, `Ok(None)` if still in progress,
    /// or an error if the job failed or status could not be retrieved.
    async fn check_job_completed(
        &self,
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    ) -> Result<Option<Vec<PartitionLocation>>> {
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

        let Some(job_status) = status else {
            // Job not found yet (still being registered)
            return Ok(None);
        };

        match job_status.status {
            Some(job_status::Status::Successful(success)) => {
                let locations = self.convert_partition_locations(success.partition_location)?;
                self.finish_tracker_success();
                Ok(Some(locations))
            }
            Some(job_status::Status::Failed(failed)) => {
                let err = QueryHandleError::JobFailed {
                    message: failed.error,
                };
                self.finish_tracker_with_error(&err);
                Err(err)
            }
            Some(job_status::Status::Queued(_) | job_status::Status::Running(_)) | None => {
                // Job still in progress
                Ok(None)
            }
        }
    }

    /// Fetches partition locations for a completed job.
    async fn fetch_completed_job_locations(
        &self,
        scheduler: &SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    ) -> Result<Vec<PartitionLocation>> {
        for attempt in 1..=COMPLETED_STATUS_MAX_POLLS {
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

            let Some(job_status) = status else {
                if attempt < COMPLETED_STATUS_MAX_POLLS {
                    tracing::debug!(
                        job_id = %self.ballista_job_id,
                        attempt,
                        max_attempts = COMPLETED_STATUS_MAX_POLLS,
                        "Job reported as completed before status became visible; retrying status poll"
                    );
                    sleep(COMPLETED_STATUS_POLL_INTERVAL).await;
                    continue;
                }

                let err = QueryHandleError::JobNotFound {
                    ballista_job_id: self.ballista_job_id.clone(),
                };
                self.finish_tracker_with_error(&err);
                return Err(err);
            };

            match job_status.status {
                Some(job_status::Status::Successful(success)) => {
                    let locations = self.convert_partition_locations(success.partition_location)?;
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
                status @ (Some(job_status::Status::Queued(_) | job_status::Status::Running(_))
                | None) => {
                    if attempt < COMPLETED_STATUS_MAX_POLLS {
                        tracing::debug!(
                            job_id = %self.ballista_job_id,
                            attempt,
                            max_attempts = COMPLETED_STATUS_MAX_POLLS,
                            status = describe_job_status(status.as_ref()),
                            "Job reported as completed before terminal status was persisted; retrying status poll"
                        );
                        sleep(COMPLETED_STATUS_POLL_INTERVAL).await;
                        continue;
                    }

                    let err = QueryHandleError::StatusError {
                        message: format!(
                            "Job {} reported as completed but status remained {} after {} polls",
                            self.ballista_job_id,
                            describe_job_status(status.as_ref()),
                            COMPLETED_STATUS_MAX_POLLS
                        ),
                    };
                    self.finish_tracker_with_error(&err);
                    return Err(err);
                }
            }
        }

        unreachable!("status poll loop should always return");
    }

    /// Converts protobuf partition locations to core types.
    ///
    /// All partition locations must convert successfully to ensure complete results
    /// are returned (data correctness requirement).
    fn convert_partition_locations(
        &self,
        proto_locations: Vec<ballista_core::serde::protobuf::PartitionLocation>,
    ) -> Result<Vec<PartitionLocation>> {
        let mut locations = Vec::with_capacity(proto_locations.len());
        for (i, loc) in proto_locations.into_iter().enumerate() {
            let partition_loc: PartitionLocation =
                loc.try_into()
                    .map_err(|e: ballista_core::error::BallistaError| {
                        let err = QueryHandleError::PartitionLocationError {
                            index: i,
                            message: e.to_string(),
                        };
                        self.finish_tracker_with_error(&err);
                        err
                    })?;
            locations.push(partition_loc);
        }
        Ok(locations)
    }

    /// Finishes the query tracker with an error.
    ///
    /// Tracker `finish_with_error` emits `tracing::info!(target: "task_history",
    /// ...)` events that attach to the *current* span. We enter
    /// `task_history_span` first so the events land on the `sql_query` row for
    /// this distributed job rather than whatever ambient span the polling
    /// future happens to be running under.
    fn finish_tracker_with_error(&self, error: &QueryHandleError) {
        if let Some(tracker) = self.tracker.lock().take() {
            let error_code = match error {
                QueryHandleError::JobCancelled | QueryHandleError::JobFailed { .. } => {
                    ErrorCode::QueryExecutionError
                }
                QueryHandleError::StatusError { .. }
                | QueryHandleError::PartitionLocationError { .. }
                | QueryHandleError::JobNotFound { .. } => ErrorCode::InternalError,
            };
            self.spawn_finalize(tracker, Some((error.to_string(), error_code)), false);
        }
    }

    /// Finishes the query tracker successfully.
    fn finish_tracker_success(&self) {
        if let Some(tracker) = self.tracker.lock().take() {
            self.spawn_finalize(tracker, None, false);
        }
    }

    /// Finalize the parent `sql_query` `task_history` row plus per-stage child
    /// rows.
    ///
    /// **Span ownership**: the parent span is *taken* out of the shared
    /// slot here, not cloned. The taken span moves into the spawned
    /// finalize future via `.instrument(span)`; once that future ends and
    /// any extra clones the future held drop, the OTel layer closes the
    /// span. Because no surviving `QueryHandle` clone retains the span
    /// after `spawn_finalize` runs, `execution_duration_ms` reflects the
    /// query's runtime, not arbitrary post-completion handle lifetime.
    /// `Option::take` also makes finalization idempotent.
    ///
    /// **Cancel-on-orphan**: `request_cancel = true` triggers
    /// `scheduler.cancel_job(...)` from inside the spawned task before
    /// recording the row. Set this from the `Drop` guard so a handle that
    /// goes out of scope without the result stream being drained doesn't
    /// leave the scheduler/executors running an unobserved job. Idempotent
    /// w.r.t. the scheduler's own cancel handling.
    ///
    /// **No runtime fallback**: if `Drop` fires outside any tokio context,
    /// finalize synchronously without stage detail — the parent row still
    /// gets correct duration and error_message; stage children are skipped.
    fn spawn_finalize(
        &self,
        tracker: QueryTracker,
        error: Option<(String, ErrorCode)>,
        request_cancel: bool,
    ) {
        // Take the span so the only surviving clones are inside the
        // spawned future. When the future completes, those clones drop
        // and the OTel span closes — capturing the *query* duration, not
        // a handle-lifetime duration.
        let Some(parent_span) = self.task_history_span.lock().take() else {
            // Already finalized by an earlier path; nothing to do.
            return;
        };

        let request_context = Arc::clone(&self.request_context);
        let scheduler = match &self.state {
            QueryHandleState::Running { scheduler } => Some(Arc::clone(scheduler)),
            QueryHandleState::Cached { .. } => None,
        };
        let ballista_job_id = self.ballista_job_id.clone();

        let Some(scheduler) = scheduler else {
            // Cached path — nothing to walk. Synchronous finalize is fine
            // because there's no async fetch to do. The span drops at
            // end-of-scope here, closing the OTel span immediately.
            parent_span.in_scope(|| match error {
                Some((msg, code)) => tracker.finish_with_error(&request_context, msg, code),
                None => tracker.finish(&request_context, &Arc::from("")),
            });
            return;
        };

        let job_id = self.ballista_job_id.clone();
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                // No tokio runtime here (likely Drop on a non-runtime
                // thread). Finalize the parent without stage detail or
                // job cancellation rather than `block_on`'ing into a
                // private API.
                parent_span.in_scope(|| match error {
                    Some((msg, code)) => tracker.finish_with_error(&request_context, msg, code),
                    None => tracker.finish(&request_context, &Arc::from("")),
                });
                return;
            }
        };

        // `.instrument(parent_span)` enters the span on every poll of
        // the spawned future. Production sets the OTel subscriber as
        // the *global* default (`bin/spiced/src/tracing.rs`), so the
        // spawned task — even on a fresh tokio worker thread — picks
        // it up automatically and child `ballista_stage` spans created
        // inside attribute to the correct subscriber. Integration
        // tests that rely on `set_default` (thread-local) must
        // explicitly propagate the dispatcher through the test future
        // (see `crates/runtime/tests/cluster/distributed_task_history.rs`).
        let span_for_record = parent_span.clone();
        let scheduler_for_cancel = Arc::clone(&scheduler);
        let cancel_job_id = job_id.clone();
        handle.spawn(
            async move {
                if request_cancel {
                    // Best-effort: tell the scheduler to stop running this
                    // job. The scheduler treats `cancel_job` as idempotent
                    // so a concurrent path (e.g. `wait_for_complete`
                    // reacting to `cancel_token`) cancelling first is OK.
                    if let Err(e) =
                        scheduler_for_cancel.cancel_job(cancel_job_id.clone()).await
                    {
                        tracing::warn!(
                            target: "task_history",
                            "Failed to cancel Ballista job {cancel_job_id} during orphaned-handle finalize: {e}"
                        );
                    }
                }
                let graph = scheduler
                    .state
                    .task_manager
                    .get_job_execution_graph(&job_id)
                    .await
                    .ok()
                    .flatten();
                if let Some(graph) = graph.as_ref() {
                    crate::datafusion::query::stage_history::record_stage_history(
                        &span_for_record,
                        &ballista_job_id,
                        graph.as_ref(),
                    );
                }
                match error {
                    Some((msg, code)) => {
                        tracker.finish_with_error(&request_context, msg, code)
                    }
                    None => tracker.finish(&request_context, &Arc::from("")),
                }
            }
            .instrument(parent_span),
        );
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
                    .wait_for_complete(scheduler, &self.cancel_token)
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
                let tls = tls_config;
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

/// Finalizes an orphaned tracker when the last `QueryHandle` clone is dropped
/// without the job having reached terminal state through the normal path
/// (e.g., client disconnected before draining the result stream). Without
/// this, the `task_history` row would have empty duration and no error,
/// and the scheduler/executors would keep running an unobserved job.
///
/// The `Arc::strong_count == 1` check ensures only the *last* clone runs
/// finalization — intermediate clones being dropped while other clones still
/// hold the tracker must not finalize it. The `lock().take()` ensures
/// finalization happens at most once even if a race were possible.
///
/// We trigger `cancel_token` synchronously so any in-flight
/// `wait_for_complete` future bails out, *and* pass `request_cancel = true`
/// to `spawn_finalize` so the spawned async task calls
/// `scheduler.cancel_job(...)` directly — that covers the case where no
/// `wait_for_complete` is running (e.g., the caller never started draining
/// the result stream).
impl Drop for QueryHandle {
    fn drop(&mut self) {
        if Arc::strong_count(&self.tracker) != 1 {
            return;
        }
        let Some(tracker) = self.tracker.lock().take() else {
            return;
        };
        self.cancel_token.cancel();
        self.spawn_finalize(
            tracker,
            Some((
                "client disconnected before completion".to_string(),
                ErrorCode::QueryExecutionError,
            )),
            true,
        );
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
