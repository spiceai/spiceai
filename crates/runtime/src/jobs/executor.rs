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

use runtime_request_context::{AsyncMarker, RequestContext};

use crate::datafusion::DataFusion;
use crate::datafusion::query::{QueryBuilder, QueryHandle, QueryHandleError};
use crate::http::v1::queries::SubmitQueryRequest;
use crate::jobs::state::JobErrorCode;

use super::Result;
use super::error::Error;
use super::ledger::{Admission, Limits as JobLimits, MAX_JOB_RESULT_BYTES, Refusal, StoredEntry};
use super::state::{JobResult, JobState, JobStatus, now_ms_or_zero};
use super::store::JobStore;
use runtime_metrics::async_queries::{self as async_query_metrics, ActiveStatus, RejectionReason};

/// How a job comes to be driven by this scheduler.
enum Start {
    /// It was submitted here: start it.
    Submitted,
    /// It was taken over from a scheduler that stopped: this is the state the
    /// takeover wrote.
    TakenOver(Box<JobState>),
}

/// How a job's distributed query and the writing of its results ended, when neither a
/// cancellation nor the timeout stopped them first.
enum JobRun {
    /// Every result chunk was written.
    Finished(JobResult),
    /// The query could not be planned or submitted, it failed, or its results could not be
    /// read back.
    Failed(JobErrorCode, String),
    /// Writing the results to the job store failed.
    WriteFailed(Error),
}

/// When a job submitted at `created_at_ms` with `timeout_seconds` must stop, or `None` when that
/// is too far ahead to represent (in effect, never).
fn job_deadline(created_at_ms: u64, timeout_seconds: u64) -> Option<tokio::time::Instant> {
    let deadline_ms = created_at_ms.saturating_add(timeout_seconds.saturating_mul(1_000));
    let remaining = Duration::from_millis(deadline_ms.saturating_sub(now_ms_or_zero()));
    tokio::time::Instant::now().checked_add(remaining)
}

/// The timeout, in seconds, an async query job runs under: the caller's `timeout_seconds`, or
/// `runtime.query.timeout` (`query_timeout`) when the caller sets none. A `timeout_seconds` longer
/// than `runtime.query.timeout` is refused rather than shortened. A `runtime.query.timeout` with a
/// fraction of a second rounds up to the next second, so a job that sets none never stops before
/// the configured time.
fn job_timeout_seconds(
    requested: Option<u64>,
    query_timeout: Option<Duration>,
) -> Result<Option<u64>> {
    match (requested, query_timeout) {
        (Some(requested), Some(limit)) if Duration::from_secs(requested) > limit => {
            Err(Error::TimeoutAboveQueryTimeout {
                requested,
                limit: format_seconds(limit),
            })
        }
        (Some(requested), _) => Ok(Some(requested)),
        (None, Some(limit)) => Ok(Some(
            limit
                .as_secs()
                .saturating_add(u64::from(limit.subsec_nanos() > 0)),
        )),
        (None, None) => Ok(None),
    }
}

/// `duration` in seconds as a person writes it: `5`, `1.5`, `300`.
fn format_seconds(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    format!("{seconds}")
}

/// The error a refused submission gets, counted by reason.
fn refusal_error(refusal: Refusal) -> Error {
    match refusal {
        Refusal::QueueFull { pending } => {
            async_query_metrics::track_submission_rejected(RejectionReason::QueueFull);
            Error::QueueFull { pending }
        }
        Refusal::ResultBudget {
            stored_bytes,
            oldest_expiry_ms,
        } => {
            async_query_metrics::track_submission_rejected(RejectionReason::ResultBudget);
            Error::ResultBudgetFull {
                stored_bytes,
                oldest_expiry_ms,
            }
        }
    }
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
    /// Starts jobs and keeps each caller's ledger as they start and finish.
    runner: Runner,
}

/// A job accepted by this scheduler that waits for its caller's running slot.
struct PendingJob {
    owner: String,
    /// The submitting request's context, which the job runs under. `None` for a public job
    /// taken over from a scheduler that stopped.
    context: Option<Arc<RequestContext>>,
}

/// What starting a job and keeping its caller's ledger needs, shared with the jobs' tasks.
#[derive(Clone)]
struct Runner {
    job_store: Arc<JobStore>,
    df: Arc<DataFusion>,
    active_jobs: Arc<RwLock<std::collections::HashMap<String, ActiveJobInfo>>>,
    /// Jobs this scheduler accepted that wait for a running slot, by job id. A pending job holds
    /// no task and no query resources.
    pending: Arc<parking_lot::Mutex<std::collections::HashMap<String, PendingJob>>>,
    limits: JobLimits,
}

/// The message a job gets when the scheduler that held it stopped and it cannot be run under
/// the identity that submitted it.
const RESUBMIT_MESSAGE: &str = "The scheduler running this query stopped before it finished, and the query cannot be resumed under the identity that submitted it. Resubmit the query.";

/// How long a job may go without a ledger entry, or a ledger entry without its job, before the
/// recovery sweep treats it as left by a scheduler that stopped between the two writes. It also
/// covers a submission that lands after the sweep listed the jobs.
const UNADMITTED_GRACE_MS: u64 = 60_000;

/// How long this scheduler keeps a caller's ledger in memory after it last changed it.
const LEDGER_IDLE: Duration = Duration::from_mins(10);

impl Runner {
    /// Starts `job_id` in a task of its own, under `context` when it has one, and releases its
    /// running slot when it finishes.
    fn start(&self, job_id: String, owner: String, context: Option<Arc<RequestContext>>) {
        let runner = self.clone();
        let span = tracing::info_span!("job_execution", job_id = %job_id);
        let run = async move {
            let cancel_token = CancellationToken::new();
            runner.active_jobs.write().await.insert(
                job_id.clone(),
                ActiveJobInfo {
                    cancel_token: cancel_token.clone(),
                    query_handle: None,
                },
            );
            async_query_metrics::add_active(ActiveStatus::Running, 1);
            let result = JobExecutor::execute_job(
                &runner.job_store,
                Arc::clone(&runner.df),
                &job_id,
                &runner.active_jobs,
                cancel_token,
                Start::Submitted,
            )
            .await;
            runner.active_jobs.write().await.remove(&job_id);
            async_query_metrics::add_active(ActiveStatus::Running, -1);
            report_job_end(&job_id, result, "Job execution failed");
            runner.finished(&owner, &job_id).await;
        }
        .instrument(span);
        match context {
            // Bind the job to the submitting request's context so it runs as the caller's
            // principal: results-cache namespacing and principal-scoped authz key off it.
            Some(context) => tokio::spawn(context.scope(run)),
            None => tokio::spawn(run),
        };
    }

    /// Releases `job_id`'s running slot once its state is final, keeps its result in the
    /// caller's stored-results budget until it expires, and starts the caller's next queued jobs
    /// this scheduler accepted. A job another scheduler took over keeps its slot.
    async fn finished(&self, owner: &str, job_id: &str) {
        let stored = match self.job_store.get_job_ignoring_expiry(job_id).await {
            Ok(state) if state.is_terminal() => stored_entry(&state),
            Ok(_) => return,
            Err(e) => {
                tracing::debug!(job_id, "Failed to read the finished async query: {e}");
                None
            }
        };
        let node = self.job_store.node_id().to_string();
        let limits = self.limits;
        match self
            .job_store
            .modify_ledger(owner, |ledger| {
                let (changed, promoted) = ledger.finish_and_promote(job_id, stored, &node, limits);
                (promoted, changed)
            })
            .await
        {
            Ok(promoted) => self.start_promoted(owner, promoted).await,
            Err(e) => tracing::warn!(
                job_id,
                "Failed to release the running slot of async query '{job_id}': {e}. The recovery sweep releases it."
            ),
        }
    }

    /// Releases the running slot or queue place of `job_id`, admitted while its job record
    /// failed to be written, and starts the caller's next queued jobs here. A release that does
    /// not land is left to the recovery sweep, which drops an entry without a job once
    /// `UNADMITTED_GRACE_MS` has passed.
    async fn release_unwritten(&self, owner: &str, job_id: &str) {
        let node = self.job_store.node_id().to_string();
        let limits = self.limits;
        match self
            .job_store
            .modify_ledger(owner, |ledger| {
                let (changed, promoted) = ledger.finish_and_promote(job_id, None, &node, limits);
                (promoted, changed)
            })
            .await
        {
            Ok(promoted) => self.start_promoted(owner, promoted).await,
            Err(e) => tracing::warn!(
                job_id,
                "Failed to release the place of async query '{job_id}', whose submission failed: {e}. The recovery sweep releases it."
            ),
        }
    }

    /// Starts the queued jobs of `owner` that this scheduler accepted, while the caller has
    /// running slots free, in submission order.
    async fn promote(&self, owner: &str) {
        let node = self.job_store.node_id().to_string();
        let limits = self.limits;
        let promoted = match self
            .job_store
            .modify_ledger(owner, |ledger| {
                let promoted = ledger.promote(&node, limits);
                let changed = !promoted.is_empty();
                (promoted, changed)
            })
            .await
        {
            Ok(promoted) => promoted,
            Err(e) => {
                tracing::debug!("Failed to start queued async queries: {e}");
                return;
            }
        };
        self.start_promoted(owner, promoted).await;
    }

    /// Starts the queued jobs of `owner` that just got a running slot here.
    async fn start_promoted(&self, owner: &str, promoted: Vec<String>) {
        for job_id in promoted {
            let pending = self.pending.lock().remove(&job_id);
            async_query_metrics::add_active(ActiveStatus::Pending, -i64::from(pending.is_some()));
            match pending {
                Some(job) => self.start(job_id, job.owner, job.context),
                // Accepted here but no longer known to this process: only a public job can run
                // without its submitting request.
                None if owner == super::PUBLIC_JOB_OWNER => {
                    self.start(job_id, owner.to_string(), None);
                }
                None => self.fail_unrunnable(owner, &job_id).await,
            }
        }
    }

    /// Fails a queued job of an authenticated caller that cannot be started under its caller,
    /// and releases its place.
    async fn fail_unrunnable(&self, owner: &str, job_id: &str) {
        tracing::warn!(
            job_id,
            "Async query job '{job_id}' was waiting on a scheduler that stopped, and it cannot be run under the identity that submitted it, so it is marked failed; resubmit the query."
        );
        if let Err(e) = self
            .job_store
            .fail_pending_job(job_id, JobErrorCode::SchedulerUnavailable, RESUBMIT_MESSAGE)
            .await
        {
            tracing::warn!(
                job_id,
                "Failed to mark async query job '{job_id}' failed: {e}"
            );
        }
        if let Err(e) = self
            .job_store
            .modify_ledger(owner, |ledger| ((), ledger.finish(job_id, None)))
            .await
        {
            tracing::debug!(job_id, "Failed to release the async query's place: {e}");
        }
    }
}

/// The stored-results entry of a finished job: its result bytes until they expire, if it
/// succeeded.
fn stored_entry(state: &JobState) -> Option<StoredEntry> {
    let result = state.result.as_ref()?;
    Some(StoredEntry {
        bytes: u64::try_from(result.manifest.total_byte_count).unwrap_or(u64::MAX),
        expires_at_ms: state.expires_at_ms?,
    })
}

/// The result-size limit of a job: the caller's `maximum_size`, or [`MAX_JOB_RESULT_BYTES`]
/// when it sets none. A larger `maximum_size` is refused rather than lowered.
fn job_maximum_size(requested: Option<u64>) -> Result<u64> {
    match requested {
        None => Ok(MAX_JOB_RESULT_BYTES),
        Some(requested) if requested > MAX_JOB_RESULT_BYTES => {
            Err(Error::MaximumSizeAboveLimit { requested })
        }
        Some(requested) => Ok(requested),
    }
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
        Self::with_limits(job_store, df, JobLimits::default())
    }

    /// A job executor with per-caller `limits` other than the product limits.
    #[must_use]
    pub fn with_limits(job_store: Arc<JobStore>, df: Arc<DataFusion>, limits: JobLimits) -> Self {
        async_query_metrics::init();
        let active_jobs = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let runner = Runner {
            job_store: Arc::clone(&job_store),
            df: Arc::clone(&df),
            active_jobs: Arc::clone(&active_jobs),
            pending: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            limits,
        };
        Self {
            job_store,
            df,
            active_jobs,
            runner,
        }
    }

    /// Submits a new query job for async execution.
    ///
    /// Returns the job state immediately. The query will be executed in the background.
    pub async fn submit(
        &self,
        mut request: SubmitQueryRequest,
        read_only: bool,
        owner: String,
    ) -> Result<JobState> {
        // `runtime.query.timeout` bounds every query the runtime's query APIs run, so it bounds
        // an async query too: it is the job's timeout when the caller sets none, and a caller's
        // `timeout_seconds` longer than it is refused before the job is created.
        let context = RequestContext::current(AsyncMarker::new().await);
        request.timeout_seconds =
            job_timeout_seconds(request.timeout_seconds, context.query_timeout())?;
        request.maximum_size = Some(job_maximum_size(request.maximum_size)?);

        // The job is written and admitted to its caller's ledger at the same time, so a
        // submission waits for one write rather than two. Each side is undone when the other
        // fails: a job whose admission did not land is deleted, and an admission whose job was
        // not written is released. A scheduler that stops between the two leaves a job the
        // recovery sweep fails, or a ledger entry it releases once `UNADMITTED_GRACE_MS` has
        // passed.
        let job_id = JobStore::generate_job_id();
        let node = self.job_store.node_id().to_string();
        let limits = self.runner.limits;
        let admitted_at_ms = now_ms_or_zero();
        let (created, admission) = tokio::join!(
            self.job_store
                .create_job_with_id(job_id.clone(), request, read_only, owner.clone()),
            self.job_store.modify_ledger(&owner, |ledger| {
                let admission = ledger.admit(&job_id, &node, limits, admitted_at_ms);
                let changed = admission.is_ok();
                (admission, changed)
            }),
        );
        let (state, admission) = match (created, admission) {
            (Ok(state), Ok(Ok(admission))) => (state, admission),
            (Ok(_), Ok(Err(refusal))) => {
                self.discard(&job_id).await;
                return Err(refusal_error(refusal));
            }
            (Ok(_), Err(e)) => {
                self.discard(&job_id).await;
                return Err(e);
            }
            (Err(e), Ok(Ok(_))) => {
                self.runner.release_unwritten(&owner, &job_id).await;
                return Err(e);
            }
            (Err(_), Ok(Err(refusal))) => return Err(refusal_error(refusal)),
            (Err(e), Err(_)) => return Err(e),
        };

        match admission {
            Admission::Run => self.runner.start(job_id, owner, Some(Arc::clone(&context))),
            Admission::Pending => {
                self.runner.pending.lock().insert(
                    job_id,
                    PendingJob {
                        owner,
                        context: Some(context),
                    },
                );
                async_query_metrics::add_active(ActiveStatus::Pending, 1);
            }
        }

        Ok(state)
    }

    /// Deletes a job that was refused before it could run, best effort.
    async fn discard(&self, job_id: &str) {
        if let Err(e) = self.job_store.delete_job(job_id).await {
            tracing::debug!(job_id, "Failed to delete the refused async query: {e}");
        }
    }

    /// Starts the queued jobs this scheduler accepted whose callers have running slots free.
    /// A slot another scheduler frees is taken here, by the scheduler holding the caller's
    /// request, so each caller's queued jobs start in submission order across the cluster.
    pub async fn promote_pending(&self) {
        let owners: std::collections::BTreeSet<String> = self
            .runner
            .pending
            .lock()
            .values()
            .map(|job| job.owner.clone())
            .collect();
        for owner in owners {
            self.runner.promote(&owner).await;
        }
    }

    /// Reconciles every caller's ledger with `jobs`, every job's state as the recovery sweep
    /// listed it, so a scheduler that stopped between writing a job and its ledger leaves
    /// nothing behind for long:
    /// - a slot or place whose job finished is released, and a succeeded job's result is kept in
    ///   the budget until it expires; one whose job is missing is released once
    ///   `UNADMITTED_GRACE_MS` has passed since it was admitted, since its job may still be
    ///   being written, or have been written after `jobs` was listed;
    /// - a ledger that was lost is rebuilt with the results of the caller's succeeded jobs;
    /// - a queued job accepted by a scheduler that is not live is taken over here when its
    ///   caller is public, and otherwise failed with a request to resubmit, as a running job
    ///   is; the rest keep their order;
    /// - a job that never entered its caller's ledger is failed after the same grace period.
    pub async fn reconcile_ledgers(
        &self,
        jobs: &[JobState],
        live_nodes: &std::collections::HashSet<String>,
    ) {
        let mut owners: std::collections::BTreeSet<String> = jobs
            .iter()
            .map(|job| job.owner_scope().to_string())
            .collect();
        match self.job_store.ledger_owners().await {
            Ok(ledger_owners) => owners.extend(ledger_owners),
            Err(e) => tracing::debug!("Failed to list caller budgets: {e}"),
        }
        let by_id: std::collections::HashMap<&str, &JobState> =
            jobs.iter().map(|job| (job.job_id.as_str(), job)).collect();
        let node = self.job_store.node_id().to_string();
        let now_ms = now_ms_or_zero();

        for owner in owners {
            let mut orphaned = Vec::new();
            let mut tracked = std::collections::HashSet::new();
            let result = self
                .job_store
                .modify_ledger(&owner, |ledger| {
                    orphaned.clear();
                    tracked.clear();
                    let mut changed = ledger.prune_expired(now_ms);
                    let entries: Vec<String> = ledger
                        .running
                        .keys()
                        .cloned()
                        .chain(ledger.pending.iter().map(|entry| entry.job_id.clone()))
                        .collect();
                    for job_id in entries {
                        match by_id.get(job_id.as_str()) {
                            Some(job) if !job.is_terminal() => {
                                tracked.insert(job_id);
                            }
                            Some(job) => changed |= ledger.finish(&job_id, stored_entry(job)),
                            None if ledger.admitted_at_ms(&job_id).is_some_and(|at| {
                                now_ms.saturating_sub(at) <= UNADMITTED_GRACE_MS
                            }) => {}
                            None => changed |= ledger.finish(&job_id, None),
                        }
                    }
                    if ledger.version.is_none() {
                        for job in jobs.iter().filter(|job| job.owner_scope() == owner) {
                            if let Some(stored) = stored_entry(job)
                                && stored.expires_at_ms > now_ms
                            {
                                ledger.keep_stored(stored);
                                changed = true;
                            }
                        }
                    }
                    for entry in &ledger.pending {
                        if !live_nodes.contains(&entry.node) && entry.node != node {
                            orphaned.push(entry.job_id.clone());
                        }
                    }
                    if owner == super::PUBLIC_JOB_OWNER {
                        for job_id in &orphaned {
                            changed |= ledger.take_over(job_id, &node);
                        }
                    }
                    ((), changed)
                })
                .await;
            if let Err(e) = result {
                tracing::debug!("Failed to reconcile a caller's async query budget: {e}");
                continue;
            }

            for job_id in orphaned {
                if owner == super::PUBLIC_JOB_OWNER {
                    tracing::info!(
                        job_id = %job_id,
                        "Taking over a queued async query from a scheduler that stopped"
                    );
                    if self
                        .runner
                        .pending
                        .lock()
                        .insert(
                            job_id,
                            PendingJob {
                                owner: owner.clone(),
                                context: None,
                            },
                        )
                        .is_none()
                    {
                        async_query_metrics::add_active(ActiveStatus::Pending, 1);
                    }
                } else {
                    self.runner.fail_unrunnable(&owner, &job_id).await;
                }
            }

            // A job that was created but never entered its caller's ledger: its scheduler
            // stopped in between.
            for job in jobs.iter().filter(|job| {
                job.owner_scope() == owner
                    && job.status == JobStatus::Pending
                    && !tracked.contains(&job.job_id)
                    && now_ms.saturating_sub(job.created_at_ms) > UNADMITTED_GRACE_MS
            }) {
                if let Err(e) = self
                    .job_store
                    .fail_pending_job(
                        &job.job_id,
                        JobErrorCode::SchedulerUnavailable,
                        RESUBMIT_MESSAGE,
                    )
                    .await
                {
                    tracing::debug!(job_id = %job.job_id, "Failed to fail an abandoned async query: {e}");
                }
            }
            self.runner.promote(&owner).await;
        }

        // Drop queued jobs this scheduler holds that are no longer in their caller's queue
        // (cancelled, or failed by another scheduler).
        let held: Vec<(String, String)> = self
            .runner
            .pending
            .lock()
            .iter()
            .map(|(job_id, job)| (job_id.clone(), job.owner.clone()))
            .collect();
        for (job_id, owner) in held {
            let queued = by_id
                .get(job_id.as_str())
                .is_some_and(|job| job.status == JobStatus::Pending);
            if !queued && self.runner.pending.lock().remove(&job_id).is_some() {
                async_query_metrics::add_active(ActiveStatus::Pending, -1);
                tracing::debug!(job_id = %job_id, owner = %owner, "Dropped a queued async query that is no longer waiting");
            }
        }
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
                .fail_orphaned_job(job, JobErrorCode::SchedulerUnavailable, RESUBMIT_MESSAGE)
                .await
            {
                Ok(Some(_)) => {
                    let owner = job.owner_scope().to_string();
                    if let Err(e) = self
                        .job_store
                        .modify_ledger(&owner, |ledger| ((), ledger.finish(job_id, None)))
                        .await
                    {
                        tracing::debug!(job_id, "Failed to release the async query's slot: {e}");
                    }
                    self.runner.promote(&owner).await;
                }
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

        // The job keeps its caller's running slot, now held by this scheduler.
        let owner = taken.owner_scope().to_string();
        let node = self.job_store.node_id().to_string();
        if let Err(e) = self
            .job_store
            .modify_ledger(&owner, |ledger| ((), ledger.take_over(job_id, &node)))
            .await
        {
            tracing::debug!(job_id, "Failed to move the async query's slot here: {e}");
        }

        let job_store = Arc::clone(&self.job_store);
        let df = Arc::clone(&self.df);
        let active_jobs = Arc::clone(&self.active_jobs);
        let job_id_owned = job_id.to_string();
        let runner = self.runner.clone();

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
                runner.finished(&owner, &job_id_owned).await;
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
        let state = self.job_store.cancel_job(job_id).await?;

        // A queued job holds no task to release its place, so it is released here. A running
        // job releases its slot when its task ends.
        if self.runner.pending.lock().remove(job_id).is_some() {
            async_query_metrics::add_active(ActiveStatus::Pending, -1);
        }
        let owner = state.owner_scope().to_string();
        let queued = self
            .job_store
            .read_ledger(&owner)
            .await
            .is_ok_and(|ledger| ledger.pending.iter().any(|entry| entry.job_id == job_id));
        if queued
            && let Err(e) = self
                .job_store
                .modify_ledger(&owner, |ledger| ((), ledger.finish(job_id, None)))
                .await
        {
            tracing::debug!(
                job_id,
                "Failed to release the cancelled async query's place: {e}"
            );
        }
        Ok(state)
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
    /// jobs that failed or were cancelled at least the result TTL ago, and drops the expired
    /// results from each caller's stored-results budget.
    ///
    /// Returns how many jobs were deleted.
    pub async fn cleanup_expired_jobs(&self) -> Result<usize> {
        self.job_store.forget_idle_ledgers(LEDGER_IDLE);
        let deleted = self.job_store.cleanup_expired_jobs().await?;
        let now_ms = now_ms_or_zero();
        match self.job_store.ledger_owners().await {
            Ok(owners) => {
                for owner in owners {
                    if let Err(e) = self
                        .job_store
                        .modify_ledger(&owner, |ledger| ((), ledger.prune_expired(now_ms)))
                        .await
                    {
                        tracing::debug!(
                            "Failed to drop expired results from a caller's budget: {e}"
                        );
                    }
                }
            }
            Err(e) => tracing::debug!("Failed to list caller budgets during cleanup: {e}"),
        }
        Ok(deleted)
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

        // The job stops at its deadline, `timeout_seconds` after it was submitted, wherever it is:
        // planning and submitting the distributed query, waiting for it, or reading its results
        // back and writing them to the job store. The deadline is fixed by the submission time,
        // so a scheduler that takes the job over keeps it rather than starting the clock again.
        let deadline = state
            .timeout_seconds
            .and_then(|secs| job_deadline(state.created_at_ms, secs));
        let timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>> = match deadline {
            Some(deadline) => Box::pin(tokio::time::sleep_until(deadline)),
            None => Box::pin(std::future::pending()),
        };

        let run = async {
            let mut query_builder =
                QueryBuilder::new(&state.sql, Arc::clone(&df)).read_only(state.read_only);
            if let Some(p) = state.parameters.clone() {
                match crate::datafusion::param_utils::convert_json_to_param_values(p) {
                    Ok(params) => query_builder = query_builder.parameters(Some(params)),
                    Err(e) => {
                        return JobRun::Failed(JobErrorCode::ParameterBindingFailed, e.to_string());
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
                Err(e) => return JobRun::Failed(Self::query_error_to_code(&e), e.to_string()),
            };

            tracing::debug!(
                job_id,
                ballista_job_id = %query_handle.ballista_job_id(),
                is_cached = %query_handle.is_cached(),
                "Query submitted for distributed execution"
            );

            // Store the Ballista job ID for cancellation
            if let Some(info) = active_jobs.write().await.get_mut(job_id) {
                info.query_handle = Some(query_handle.clone());
            }

            let result_stream = match query_handle.into_stream().await {
                Ok(stream) => stream,
                Err(e) => {
                    let (error_code, error_msg) = Self::handle_error_to_code_and_msg(&e);
                    return JobRun::Failed(error_code, error_msg);
                }
            };
            match job_store
                .write_result_chunks_from_stream(job_id, &driver, result_stream)
                .await
            {
                Ok(result) => JobRun::Finished(result),
                Err(e) => JobRun::WriteFailed(e),
            }
        };

        tokio::select! {
            () = cancel.cancelled() => {
                tracing::debug!(job_id = %job_id, "Job cancelled before completion");
                if let Some(query_handle) = Self::submitted_query(active_jobs, job_id).await
                    && let Err(e) = query_handle.cancel().await
                {
                    tracing::error!("Failed to cancel the distributed query '{job_id}': {e}");
                }
                // Don't call cancel_job here - executor.cancel() already updates the
                // job state. Doing it here would race with the OCC write and cause
                // ConcurrentModification errors.
                Ok(())
            },
            () = timeout_fut => {
                tracing::debug!(job_id = %job_id, "Job timed out");
                if let Some(query_handle) = Self::submitted_query(active_jobs, job_id).await
                    && let Err(e) = query_handle.cancel().await
                {
                    tracing::error!("Failed to cancel the timed-out query '{job_id}': {e}");
                }
                job_store.fail_job(job_id, &driver, JobErrorCode::Timeout, "Job execution timed out".to_string()).await?;
                Ok(())
            }
            outcome = run => match outcome {
                JobRun::Finished(job_result) => {
                    job_store.complete_job(job_id, &driver, job_result).await?;
                    Ok(())
                }
                JobRun::Failed(error_code, error_msg) => {
                    job_store.fail_job(job_id, &driver, error_code, error_msg).await?;
                    Ok(())
                }
                JobRun::WriteFailed(e @ Error::JobTakenOver { .. }) => Err(e),
                JobRun::WriteFailed(e @ Error::MaximumJobSizeExceeded { .. }) => {
                    tracing::warn!(job_id = %job_id, "{e}");
                    job_store
                        .fail_job(job_id, &driver, JobErrorCode::FetchingResultsFailed, e.to_string())
                        .await?;
                    job_store.delete_job_chunks(job_id).await;
                    Ok(())
                }
                JobRun::WriteFailed(e) => {
                    job_store
                        .fail_job(job_id, &driver, JobErrorCode::FetchingResultsFailed, e.to_string())
                        .await?;
                    Ok(())
                }
            }
        }
    }

    /// The distributed query of job `job_id`, once it was submitted.
    async fn submitted_query(
        active_jobs: &RwLock<std::collections::HashMap<String, ActiveJobInfo>>,
        job_id: &str,
    ) -> Option<QueryHandle> {
        active_jobs
            .read()
            .await
            .get(job_id)
            .and_then(|info| info.query_handle.clone())
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

    /// `runtime.query.timeout` is the job timeout when the caller sets none, and a longer
    /// `timeout_seconds` is refused, never shortened; a fraction of a second rounds up so a job
    /// that sets none never stops early.
    #[test]
    fn job_timeout_is_bounded_by_the_query_timeout() {
        let limit = Some(Duration::from_secs(30));
        assert_eq!(job_timeout_seconds(None, None).ok(), Some(None));
        assert_eq!(job_timeout_seconds(Some(600), None).ok(), Some(Some(600)));
        assert_eq!(job_timeout_seconds(None, limit).ok(), Some(Some(30)));
        assert_eq!(job_timeout_seconds(Some(30), limit).ok(), Some(Some(30)));
        assert_eq!(job_timeout_seconds(Some(10), limit).ok(), Some(Some(10)));
        assert_eq!(job_timeout_seconds(Some(0), limit).ok(), Some(Some(0)));
        assert_eq!(
            job_timeout_seconds(None, Some(Duration::from_millis(1_500))).ok(),
            Some(Some(2))
        );
        assert_eq!(
            job_timeout_seconds(Some(1), Some(Duration::from_millis(1_500))).ok(),
            Some(Some(1))
        );
        let Err(e) = job_timeout_seconds(Some(31), limit) else {
            panic!("a timeout_seconds above runtime.query.timeout must be refused");
        };
        assert_eq!(
            e.to_string(),
            "`timeout_seconds` is 31, above 30 seconds, this runtime's `runtime.query.timeout` and the longest an async query may run. Omit it to use 30 seconds, or set a smaller value. See https://spiceai.org/docs/api/http/queries"
        );
        let Err(e) = job_timeout_seconds(Some(2), Some(Duration::from_millis(1_500))) else {
            panic!("a timeout_seconds above a fractional runtime.query.timeout must be refused");
        };
        assert!(e.to_string().contains("above 1.5 seconds"), "{e}");
    }

    /// A job's deadline counts from its submission, so a job taken over by another scheduler
    /// after its timeout passed stops at once instead of starting the clock again.
    #[tokio::test]
    async fn job_deadline_counts_from_submission() {
        let now_ms = now_ms_or_zero();
        let submitted_long_ago = job_deadline(now_ms - 60_000, 30).expect("a deadline");
        assert!(submitted_long_ago <= tokio::time::Instant::now());
        let submitted_now = job_deadline(now_ms, 30).expect("a deadline");
        let remaining = submitted_now - tokio::time::Instant::now();
        assert!(
            remaining > Duration::from_secs(25) && remaining <= Duration::from_secs(30),
            "{remaining:?}"
        );
        // A timeout too long to represent is no deadline, or one too far ahead to matter.
        let year = Duration::from_hours(365 * 24);
        assert!(
            job_deadline(now_ms, u64::MAX)
                .is_none_or(|deadline| deadline > tokio::time::Instant::now() + year)
        );
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

    /// An object store that refuses every write of a job record, and serves everything else
    /// from memory: a job write that fails while its ledger admission lands.
    #[derive(Debug)]
    struct FailingJobWrites {
        inner: InMemory,
    }

    impl std::fmt::Display for FailingJobWrites {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingJobWrites")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailingJobWrites {
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
            if location.as_ref().contains("/jobs/") {
                return Err(object_store::Error::Generic {
                    store: "FailingJobWrites",
                    source: "injected job write failure".into(),
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

    fn limited_executor(job_store: Arc<JobStore>, limits: JobLimits) -> JobExecutor {
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .build(),
        );
        JobExecutor::with_limits(job_store, df, limits)
    }

    fn select_one() -> SubmitQueryRequest {
        SubmitQueryRequest {
            sql: "SELECT 1".to_string(),
            parameters: None,
            timeout_seconds: None,
            maximum_size: None,
        }
    }

    /// A submission whose job record fails to be written while its admission lands releases
    /// the slot or place it was admitted to, so the failed submission holds nothing.
    #[tokio::test]
    async fn a_job_write_that_fails_releases_its_admission() {
        let job_store = Arc::new(JobStore::new(
            Arc::new(FailingJobWrites {
                inner: InMemory::new(),
            }),
            "test",
            "node-1",
        ));
        let executor = limited_executor(
            Arc::clone(&job_store),
            JobLimits {
                running: 1,
                pending: 1,
                stored_bytes: u64::MAX,
            },
        );
        // One query of another scheduler holds the only running slot, so the next admission is
        // a queue place; the one after it would be refused.
        job_store
            .modify_ledger(OWNER, |ledger| {
                let admission = ledger.admit("elsewhere", "node-2", executor.runner.limits, 0);
                ((), admission.is_ok())
            })
            .await
            .expect("ledger written");

        let error = executor
            .submit(select_one(), true, OWNER.to_string())
            .await
            .expect_err("the job write fails");
        assert!(matches!(error, Error::ObjectStoreWrite { .. }), "{error:?}");
        let ledger = job_store.read_ledger(OWNER).await.expect("ledger");
        assert_eq!(ledger.running.keys().collect::<Vec<_>>(), vec!["elsewhere"]);
        assert_eq!(ledger.pending.len(), 0);
        assert!(executor.runner.pending.lock().is_empty());

        // The released place is free again: a second failing submission is admitted to it, not
        // refused for a full queue, and releases it too.
        let error = executor
            .submit(select_one(), true, OWNER.to_string())
            .await
            .expect_err("the job write fails again");
        assert!(matches!(error, Error::ObjectStoreWrite { .. }), "{error:?}");
        let ledger = job_store.read_ledger(OWNER).await.expect("ledger");
        assert_eq!(ledger.running.len(), 1);
        assert_eq!(ledger.pending.len(), 0);
    }

    /// A submission refused by its caller's ledger after its job record was written deletes
    /// the job, so the refusal leaves no job and no ledger entry behind.
    #[tokio::test]
    async fn a_refused_admission_deletes_its_written_job() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = limited_executor(
            Arc::clone(&job_store),
            JobLimits {
                running: 0,
                pending: 0,
                stored_bytes: u64::MAX,
            },
        );
        let error = executor
            .submit(select_one(), true, OWNER.to_string())
            .await
            .expect_err("no place is free");
        assert!(
            matches!(error, Error::QueueFull { pending: 0 }),
            "{error:?}"
        );
        assert_eq!(
            job_store.list_jobs(None).await.expect("jobs listed").len(),
            0
        );
        let ledger = job_store.read_ledger(OWNER).await.expect("ledger");
        assert_eq!((ledger.running.len(), ledger.pending.len()), (0, 0));
    }

    /// A scheduler that stopped after admitting a query but before writing its job leaves a
    /// ledger entry with no job. The sweep keeps it while its job may still be being written,
    /// and releases it once the grace period has passed.
    #[tokio::test]
    async fn the_sweep_releases_an_entry_without_a_job_only_after_the_grace_period() {
        let job_store = Arc::new(JobStore::new(Arc::new(InMemory::new()), "test", "node-1"));
        let executor = limited_executor(Arc::clone(&job_store), JobLimits::default());
        let now_ms = now_ms_or_zero();
        let limits = executor.runner.limits;
        job_store
            .modify_ledger(OWNER, |ledger| {
                let recent = ledger.admit("recent", "node-gone", limits, now_ms - 50_000);
                let stale = ledger.admit(
                    "stale",
                    "node-gone",
                    limits,
                    now_ms - UNADMITTED_GRACE_MS - 1_000,
                );
                ((), recent.is_ok() && stale.is_ok())
            })
            .await
            .expect("ledger written");
        let live = std::collections::HashSet::from(["node-1".to_string()]);

        executor.reconcile_ledgers(&[], &live).await;

        let ledger = job_store.read_ledger(OWNER).await.expect("ledger");
        assert_eq!(ledger.running.keys().collect::<Vec<_>>(), vec!["recent"]);
        assert_eq!(ledger.pending.len(), 0);
    }
}
