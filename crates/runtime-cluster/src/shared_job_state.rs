/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! An object-store backed [`JobState`] implementation.
//!
//! Job execution graphs are persisted to shared object storage so that any
//! scheduler in the cluster can read a job's status and, if the owning
//! scheduler is lost, take ownership and resume driving it to completion.
//!
//! Ownership follows [`object_store_occ::lease`]: the owner is an id the
//! owning scheduler takes when it starts, so no two scheduler runs share it,
//! and `epoch` is the generation it owns the job under. Every ownership
//! transfer takes the next generation, and every save checks both the owner and
//! the generation the saver acquired the job under, so a scheduler that is
//! presumed dead but later resurfaces, or a save begun under an earlier
//! generation, is refused.
//!
//! Every write is conditional. Metadata writes are bound to the version they
//! were computed from. A graph write lands only over the graph its owner last
//! wrote or loaded; when another scheduler wrote the graph since, the write is
//! retried only while the writer still owns the job at the same epoch.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, UpdateVersion};
use object_store_occ::{
    Attempt, Change, ConflictRetry, Expected, InsertResult, ModifyError, RetryOnConflictError,
    UpdateResult, conditional_put, retry_on_conflict,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TrySendError;
use uuid::Uuid;

use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{JobStatus, job_status::Status};
use ballista_core::{ConfigProducer, JobStatusSubscriber};
use ballista_scheduler::cluster::event::ClusterEventSender;
use ballista_scheduler::cluster::{JobState, JobStateEvent, JobStateEventStream};
use ballista_scheduler::scheduler_server::SessionBuilder;
use ballista_scheduler::state::execution_graph::{
    ExecutionGraphBox, execution_graph_from_bytes, execution_graph_to_bytes,
};
use ballista_scheduler::state::session_manager::create_datafusion_context;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Persisted, shared metadata for a single job. The execution graph itself is
/// stored separately as an opaque blob; this document is the small,
/// compare-and-set governed record of status and ownership.
#[derive(Clone, Serialize, Deserialize)]
struct JobMetadata {
    job_id: String,
    job_name: String,
    session_id: String,
    owner_instance_id: Option<Uuid>,
    epoch: u64,
    queued_at: u64,
    updated_at: u64,
    /// Encoded [`JobStatus`] protobuf — the source of truth for `get_job_status`.
    status: Vec<u8>,
}

impl JobMetadata {
    fn job_status(&self) -> Result<JobStatus> {
        JobStatus::decode(self.status.as_slice()).map_err(|e| {
            BallistaError::Internal(format!("failed to decode persisted job status: {e}"))
        })
    }

    fn is_terminal(&self) -> bool {
        match self.job_status() {
            Ok(status) => matches!(
                status.status,
                Some(Status::Successful(_) | Status::Failed(_))
            ),
            // A corrupt/undecodable status must not be treated as resumable: report
            // it as terminal so schedulers refuse to take over and drive a job whose
            // recovery state we cannot trust.
            Err(e) => {
                tracing::warn!(
                    "job {} has an undecodable persisted status ({e}); treating as terminal to prevent takeover",
                    self.job_id
                );
                true
            }
        }
    }
}

#[derive(Clone)]
struct LocalJob {
    status: JobStatus,
    subscriber: Option<JobStatusSubscriber>,
    /// The generation (`epoch`) this scheduler owns the job under.
    generation: u64,
}

impl LocalJob {
    fn notify(&self, status: JobStatus) {
        if let Some(subscriber) = &self.subscriber
            && matches!(subscriber.try_send(status), Err(TrySendError::Full(_)))
        {
            tracing::error!(
                "job notification subscriber for {} is blocked; status update dropped",
                self.status.job_id
            );
        }
    }
}

type MetaStore = object_store_occ::ObjectState<JobMetadata>;

pub struct SharedJobState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    owner_instance_id: Uuid,
    /// Advertise address, used only as the human-readable owner in events.
    scheduler: String,
    store: Arc<dyn ObjectStore>,
    graph_prefix: String,
    meta: MetaStore,
    codec: BallistaCodec<T, U>,
    session_builder: SessionBuilder,
    config_producer: ConfigProducer,
    event_sender: ClusterEventSender<JobStateEvent>,
    /// Pre-planning jobs awaiting their execution graph. Local and transient.
    queued_jobs: DashMap<String, (String, u64)>,
    /// Locally driven jobs, retained for fast status reads and subscriber delivery.
    local_jobs: DashMap<String, LocalJob>,
    /// The version of each locally driven job's graph that this scheduler last
    /// wrote or loaded, and the generation it held the job under: the
    /// condition of its next graph write under that generation.
    graph_versions: DashMap<String, (u64, UpdateVersion)>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SharedJobState<T, U> {
    #[must_use]
    pub fn new(
        scheduler: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        base_prefix: impl Into<String>,
        codec: BallistaCodec<T, U>,
        session_builder: SessionBuilder,
        config_producer: ConfigProducer,
    ) -> Self {
        let base = base_prefix.into();
        let base = base.trim_end_matches('/').to_string();
        let meta = object_store_occ::ObjectState::new(Arc::clone(&store))
            .with_prefix(format!("{base}/scheduler/meta/"));
        Self {
            owner_instance_id: Uuid::now_v7(),
            scheduler: scheduler.into(),
            store,
            graph_prefix: format!("{base}/scheduler/graph"),
            meta,
            codec,
            session_builder,
            config_producer,
            event_sender: ClusterEventSender::new(100),
            queued_jobs: DashMap::new(),
            local_jobs: DashMap::new(),
            graph_versions: DashMap::new(),
        }
    }

    fn graph_path(&self, job_id: &str) -> Path {
        Path::from(format!("{}/{job_id}", self.graph_prefix))
    }

    /// Writes `bytes` as the job's graph if the graph is in the `expected`
    /// state. Returns the version written, or `None` when another writer
    /// changed the graph first.
    async fn write_graph(
        &self,
        job_id: &str,
        bytes: &[u8],
        expected: &Expected,
    ) -> Result<Option<UpdateVersion>> {
        match conditional_put(
            self.store.as_ref(),
            &self.graph_path(job_id),
            bytes.to_vec().into(),
            expected,
        )
        .await
        {
            Ok(result) => Ok(Some(UpdateVersion::from(result))),
            Err(err) if err.is_conflict() => Ok(None),
            Err(e) => Err(BallistaError::Internal(format!(
                "failed to persist job graph: {e}"
            ))),
        }
    }

    /// The version of the job's graph as stored now, as a write condition.
    async fn stored_graph_version(&self, job_id: &str) -> Result<Expected> {
        match self.store.head(&self.graph_path(job_id)).await {
            Ok(meta) => Expected::at_version(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            })
            .ok_or_else(|| {
                BallistaError::Internal(format!(
                    "the object store reported no version for the graph of job {job_id}, so it cannot be written conditionally"
                ))
            }),
            Err(object_store::Error::NotFound { .. }) => Ok(Expected::Absent),
            Err(e) => Err(BallistaError::Internal(format!(
                "failed to read job graph: {e}"
            ))),
        }
    }

    /// The job's graph as stored now, as a write condition, or `None` when the
    /// job has a new owner.
    ///
    /// The graph is read before the metadata: a new owner records its claim
    /// before it saves a graph, so a graph it saved is never seen without its
    /// claim.
    async fn owned_graph_version(&self, job_id: &str, epoch: u64) -> Result<Option<Expected>> {
        let stored = self.stored_graph_version(job_id).await?;
        let owned = self
            .meta
            .get(job_id)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
            .is_some_and(|meta| {
                meta.owner_instance_id == Some(self.owner_instance_id) && meta.epoch == epoch
            });
        Ok(owned.then_some(stored))
    }

    /// Persists the graph of a job this scheduler owns at `epoch`.
    ///
    /// The write is conditional on the graph this scheduler last wrote or
    /// loaded under `epoch`. Without one, or after another writer changed the graph, it is
    /// conditional on the graph as stored now and made only while this
    /// scheduler still owns the job at `epoch`. Returns `false` when the job
    /// has a new owner, which keeps its own graph.
    async fn put_graph(&self, job_id: &str, graph: &ExecutionGraphBox, epoch: u64) -> Result<bool> {
        let bytes = execution_graph_to_bytes(graph.as_ref(), &self.codec)?;
        let written = retry_on_conflict(&ConflictRetry::default(), || async {
            let recorded = self
                .graph_versions
                .get(job_id)
                .filter(|recorded| recorded.0 == epoch)
                .map(|recorded| recorded.1.clone());
            let expected = match recorded.and_then(Expected::at_version) {
                Some(expected) => expected,
                None => match self.owned_graph_version(job_id, epoch).await? {
                    Some(expected) => expected,
                    None => return Ok(Attempt::Done(false)),
                },
            };
            if let Some(version) = self.write_graph(job_id, &bytes, &expected).await? {
                self.graph_versions
                    .insert(job_id.to_string(), (epoch, version));
                return Ok(Attempt::Done(true));
            }
            self.graph_versions
                .remove_if(job_id, |_, recorded| recorded.0 == epoch);
            Ok(Attempt::Conflict)
        })
        .await;
        match written {
            Ok(written) => Ok(written),
            Err(RetryOnConflictError::ConflictsExhausted { attempts }) => {
                Err(BallistaError::Internal(format!(
                    "failed to persist the graph of job {job_id}: another writer changed it before each of {attempts} attempts"
                )))
            }
            Err(RetryOnConflictError::Failed(e)) => Err(e),
        }
    }

    /// Reads the job's graph and the version it was stored at.
    async fn load_graph(
        &self,
        job_id: &str,
        meta: &JobMetadata,
    ) -> Result<(ExecutionGraphBox, UpdateVersion)> {
        let result = self
            .store
            .get(&self.graph_path(job_id))
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job graph: {e}")))?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job graph: {e}")))?;
        let ctx = self.session_context(&meta.session_id)?;
        Ok((
            execution_graph_from_bytes(&bytes, &self.codec, ctx.as_ref())?,
            version,
        ))
    }

    /// Deletes the metadata of a submission whose graph could not be written,
    /// unless another scheduler has claimed the job since.
    async fn roll_back_submission(&self, job_id: &str) {
        match self.meta.get(job_id).await {
            Ok(Some(meta))
                if meta.owner_instance_id == Some(self.owner_instance_id) && meta.epoch == 0 =>
            {
                if let Err(cleanup) = self.meta.delete(job_id).await {
                    tracing::warn!(
                        "failed to roll back job meta for {job_id} after graph write failed: {cleanup}"
                    );
                }
            }
            Ok(_) => {}
            Err(cleanup) => {
                tracing::warn!(
                    "failed to roll back job meta for {job_id} after graph write failed: {cleanup}"
                );
            }
        }
    }

    /// Stops driving a job locally.
    fn forget_local(&self, job_id: &str) -> Option<LocalJob> {
        self.graph_versions.remove(job_id);
        self.local_jobs.remove(job_id).map(|(_, local)| local)
    }

    /// Stops driving a job locally if this scheduler still holds it under
    /// `generation`; a later generation it acquired since keeps running.
    fn forget_generation(&self, job_id: &str, generation: u64) {
        if self
            .local_jobs
            .remove_if(job_id, |_, local| local.generation == generation)
            .is_some()
        {
            self.graph_versions.remove(job_id);
        }
    }

    fn session_context(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let config = (self.config_producer)();
        let ctx = create_datafusion_context(&config, Arc::clone(&self.session_builder))?;
        self.event_sender.send(&JobStateEvent::SessionAccessed {
            session_id: session_id.to_string(),
        });
        Ok(ctx)
    }

    fn metadata(
        job_id: &str,
        status: &JobStatus,
        owner: Option<Uuid>,
        epoch: u64,
        queued_at: u64,
    ) -> JobMetadata {
        JobMetadata {
            job_id: job_id.to_string(),
            job_name: status.job_name.clone(),
            session_id: String::new(),
            owner_instance_id: owner,
            epoch,
            queued_at,
            updated_at: now_ms(),
            status: status.encode_to_vec(),
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> JobState for SharedJobState<T, U> {
    fn accept_job(&self, job_id: &JobId, job_name: &str, queued_at: u64) -> Result<()> {
        self.queued_jobs
            .insert(job_id.to_string(), (job_name.to_string(), queued_at));
        Ok(())
    }

    fn pending_job_number(&self) -> usize {
        self.queued_jobs.len()
    }

    async fn submit_job(
        &self,
        job_id: JobId,
        graph: &ExecutionGraphBox,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        let Some((_, (_, queued_at))) = self.queued_jobs.remove(job_id.as_str()) else {
            return Err(BallistaError::Internal(format!(
                "failed to submit job {job_id}, not found in queued jobs"
            )));
        };

        let status = graph.status().clone();

        // Claim ownership via OCC *before* writing the graph blob. Writing the
        // graph first would overwrite an existing owner's graph for the same
        // job_id on a metadata conflict, corrupting their recovery state.
        let mut meta = Self::metadata(
            job_id.as_str(),
            &status,
            Some(self.owner_instance_id),
            0,
            queued_at,
        );
        meta.session_id = graph.session_id().to_string();
        let inserted = self
            .meta
            .insert(job_id.as_str(), &meta)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to persist job meta: {e}")))?;
        if inserted == InsertResult::AlreadyExists {
            return Err(BallistaError::Internal(format!(
                "failed to submit job {job_id}, meta already owned"
            )));
        }
        // With the claim in place, persist the graph, over any graph a removed
        // job with this id left behind. If that fails, best-effort roll back the
        // metadata so other schedulers never observe meta pointing at a missing
        // graph.
        match self.put_graph(job_id.as_str(), graph, 0).await {
            Ok(true) => {}
            Ok(false) => {
                // The metadata is the claim of the scheduler that took the job
                // over, so it stays.
                return Err(BallistaError::Internal(format!(
                    "failed to submit job {job_id}, another scheduler took it over"
                )));
            }
            Err(e) => {
                self.roll_back_submission(job_id.as_str()).await;
                return Err(e);
            }
        }

        self.local_jobs.insert(
            job_id.to_string(),
            LocalJob {
                status,
                subscriber,
                generation: 0,
            },
        );
        self.event_sender.send(&JobStateEvent::JobAcquired {
            job_id,
            owner: self.scheduler.clone(),
        });
        Ok(())
    }

    async fn get_job_status(&self, job_id: &JobId) -> Result<Option<JobStatus>> {
        if let Some((job_name, queued_at)) = self.queued_jobs.get(job_id.as_str()).as_deref() {
            return Ok(Some(JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.clone(),
                status: Some(Status::Queued(ballista_core::serde::protobuf::QueuedJob {
                    queued_at: *queued_at,
                })),
            }));
        }
        if let Some(local) = self.local_jobs.get(job_id.as_str()) {
            return Ok(Some(local.status.clone()));
        }
        match self
            .meta
            .get(job_id.as_str())
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
        {
            Some(meta) => Ok(Some(meta.job_status()?)),
            None => Ok(None),
        }
    }

    async fn get_execution_graph(&self, job_id: &JobId) -> Result<Option<ExecutionGraphBox>> {
        let Some(meta) = self
            .meta
            .get(job_id.as_str())
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
        else {
            return Ok(None);
        };
        Ok(Some(self.load_graph(job_id.as_str(), &meta).await?.0))
    }

    async fn save_job(&self, job_id: &JobId, graph: &ExecutionGraphBox) -> Result<()> {
        let status = graph.status().clone();
        let Some(generation) = self
            .local_jobs
            .get(job_id.as_str())
            .map(|local| local.generation)
        else {
            tracing::debug!("job {job_id} is not driven by this scheduler; not saving it");
            return Ok(());
        };

        // Compare-and-set the ownership metadata before persisting the graph. A
        // scheduler that has lost ownership, or that acquired the job again since
        // this save began, must not touch shared state under the generation it
        // no longer holds: it drops that local entry so reads fall through to
        // the current owner's status.
        let owned = self
            .meta
            .modify(
                job_id.as_str(),
                &ConflictRetry::default(),
                |current| -> Result<Change<JobMetadata, std::result::Result<u64, u64>>> {
                    let Some(current) = current else {
                        return Err(BallistaError::Internal(format!(
                            "job {job_id} metadata no longer exists; cannot persist state"
                        )));
                    };
                    if current.owner_instance_id != Some(self.owner_instance_id)
                        || current.epoch != generation
                    {
                        return Ok(Change::Keep(Err(current.epoch)));
                    }
                    let mut meta = Self::metadata(
                        job_id.as_str(),
                        &status,
                        Some(self.owner_instance_id),
                        current.epoch,
                        current.queued_at,
                    );
                    meta.session_id = current.session_id;
                    Ok(Change::Write(meta, Ok(current.epoch)))
                },
            )
            .await;
        let epoch = match owned {
            Ok(Ok(epoch)) => epoch,
            Ok(Err(owner_epoch)) => {
                tracing::warn!(
                    "job {job_id} owned by another scheduler (epoch {owner_epoch}); yielding"
                );
                self.forget_generation(job_id.as_str(), generation);
                return Ok(());
            }
            Err(ModifyError::Rejected(e)) => {
                self.forget_generation(job_id.as_str(), generation);
                return Err(e);
            }
            Err(e) => {
                return Err(BallistaError::Internal(format!(
                    "failed to persist job meta: {e}"
                )));
            }
        };
        if !self.put_graph(job_id.as_str(), graph, epoch).await? {
            tracing::warn!(
                "job {job_id} was taken over by another scheduler while its graph was being saved; yielding"
            );
            self.forget_generation(job_id.as_str(), generation);
            return Ok(());
        }

        let terminal = matches!(
            status.status,
            Some(Status::Successful(_) | Status::Failed(_))
        );
        if terminal {
            if let Some(local) = self.forget_local(job_id.as_str()) {
                local.notify(status.clone());
            }
        } else if let Some(mut local) = self.local_jobs.get_mut(job_id.as_str()) {
            local.status = status.clone();
            local.notify(status.clone());
        }

        self.event_sender.send(&JobStateEvent::JobUpdated {
            job_id: job_id.clone(),
            status,
        });
        Ok(())
    }

    async fn try_acquire_job(&self, job_id: &JobId) -> Result<Option<ExecutionGraphBox>> {
        let Some((meta, version)) = self
            .meta
            .get_with_version(job_id.as_str())
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
        else {
            // Recovery has already claimed the async query. Reporting absence
            // makes that query wait for a scheduler job that was never saved.
            return Err(BallistaError::Internal(format!(
                "Failed to resume query '{id}': its scheduler stopped before the query was saved. Resubmit the query.",
                id = job_id.as_str()
            )));
        };
        if meta.is_terminal() {
            return Ok(None);
        }
        if meta.owner_instance_id == Some(self.owner_instance_id) {
            return Ok(None);
        }

        let mut claimed = meta.clone();
        claimed.owner_instance_id = Some(self.owner_instance_id);
        claimed.epoch = object_store_occ::lease::next_generation(meta.epoch);
        claimed.updated_at = now_ms();
        // The claim lands only over the metadata it was decided on. Another
        // scheduler won the claim (Conflict), or the job's metadata was removed
        // under us (NotFound) — either way we don't own it.
        if !matches!(
            self.meta
                .update_with_version(job_id.as_str(), &claimed, version)
                .await
                .map_err(|e| BallistaError::Internal(format!("failed to acquire job: {e}")))?,
            UpdateResult::Ok
        ) {
            return Ok(None);
        }

        let (graph, graph_version) = self.load_graph(job_id.as_str(), &claimed).await?;
        self.graph_versions
            .insert(job_id.to_string(), (claimed.epoch, graph_version));
        self.local_jobs.insert(
            job_id.to_string(),
            LocalJob {
                status: graph.status().clone(),
                subscriber: None,
                generation: claimed.epoch,
            },
        );
        self.event_sender.send(&JobStateEvent::JobAcquired {
            job_id: job_id.clone(),
            owner: self.scheduler.clone(),
        });
        Ok(Some(graph))
    }

    async fn job_state_events(&self) -> Result<JobStateEventStream> {
        Ok(Box::pin(self.event_sender.subscribe()))
    }

    async fn remove_job(&self, job_id: &JobId) -> Result<()> {
        self.forget_local(job_id.as_str());
        self.queued_jobs.remove(job_id.as_str());
        // Delete metadata first, and only delete the graph if that succeeds.
        // `ObjectState::delete` treats NotFound as success, so an error here is a
        // real object-store failure: keep the graph so meta still points at a
        // valid blob rather than dangling to a missing graph. (A graph-delete
        // failure afterwards is then only a storage leak, not a correctness bug.)
        if let Err(e) = self.meta.delete(job_id.as_str()).await {
            tracing::warn!("failed to delete job meta for {job_id}; keeping graph: {e}");
            return Ok(());
        }
        if let Err(e) = self.store.delete(&self.graph_path(job_id.as_str())).await
            && !matches!(e, object_store::Error::NotFound { .. })
        {
            tracing::warn!("failed to delete job graph for {job_id}: {e}");
        }
        Ok(())
    }

    async fn get_jobs(&self) -> Result<HashSet<JobId>> {
        let keys = self
            .meta
            .list_keys()
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to list jobs: {e}")))?;
        Ok(keys.into_iter().map(JobId::from).collect())
    }

    async fn get_all_jobs(&self) -> Result<HashSet<JobId>> {
        let mut all_jobs: HashSet<JobId> = self
            .queued_jobs
            .iter()
            .map(|pair| JobId::from(pair.key().as_str()))
            .collect();
        all_jobs.extend(
            self.local_jobs
                .iter()
                .map(|pair| JobId::from(pair.key().as_str())),
        );
        let keys = self
            .meta
            .list_keys()
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to list jobs: {e}")))?;
        all_jobs.extend(keys.into_iter().map(JobId::from));
        Ok(all_jobs)
    }

    async fn fail_unscheduled_job(&self, job_id: &JobId, reason: String) -> Result<()> {
        let Some((_, (job_name, queued_at))) = self.queued_jobs.remove(job_id.as_str()) else {
            return Err(BallistaError::Internal(format!(
                "could not fail unscheduled job {job_id}, not found in queued jobs"
            )));
        };
        let status = JobStatus {
            job_id: job_id.to_string(),
            job_name,
            status: Some(Status::Failed(ballista_core::serde::protobuf::FailedJob {
                error: reason,
                queued_at,
                started_at: 0,
                ended_at: now_ms(),
            })),
        };
        let mut meta = Self::metadata(
            job_id.as_str(),
            &status,
            Some(self.owner_instance_id),
            0,
            queued_at,
        );
        meta.session_id = String::new();
        match self.meta.insert(job_id.as_str(), &meta).await {
            Ok(InsertResult::Ok) => {}
            Ok(InsertResult::AlreadyExists) => {
                tracing::warn!(
                    "job {job_id} already has persisted state; not overwriting it with the failed status of the unscheduled job"
                );
            }
            Err(e) => {
                tracing::warn!("failed to persist failed status for unscheduled job {job_id}: {e}");
            }
        }
        Ok(())
    }

    async fn create_or_update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        self.event_sender.send(&JobStateEvent::SessionAccessed {
            session_id: session_id.to_string(),
        });
        Ok(create_datafusion_context(
            config,
            Arc::clone(&self.session_builder),
        )?)
    }

    async fn remove_session(&self, session_id: &str) -> Result<()> {
        self.event_sender.send(&JobStateEvent::SessionRemoved {
            session_id: session_id.to_string(),
        });
        Ok(())
    }

    fn produce_config(&self) -> SessionConfig {
        (self.config_producer)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::serde::protobuf::{FailedJob, QueuedJob, RunningJob, SuccessfulJob};
    use object_store::memory::InMemory;

    type TestState = SharedJobState<
        datafusion_proto::protobuf::LogicalPlanNode,
        datafusion_proto::protobuf::PhysicalPlanNode,
    >;

    /// A `SharedJobState` backed by an in-memory object store. The codec and
    /// session/config builders are never exercised by these tests (which stay on
    /// the metadata/ownership paths and never `load_graph`), so trivial stubs are
    /// sufficient.
    fn test_state() -> TestState {
        test_state_on(Arc::new(InMemory::new()))
    }

    fn test_state_on(store: Arc<dyn ObjectStore>) -> TestState {
        let config_producer: ConfigProducer = Arc::new(SessionConfig::new);
        let session_builder: SessionBuilder = Arc::new(|_cfg| Ok(SessionContext::new().state()));
        SharedJobState::new(
            "scheduler-test",
            store,
            "",
            BallistaCodec::default(),
            session_builder,
            config_producer,
        )
    }

    fn job_status(job_id: &str, status: Status) -> JobStatus {
        JobStatus {
            job_id: job_id.to_string(),
            job_name: "test".to_string(),
            status: Some(status),
        }
    }

    fn meta_with(job_id: &str, status: Status, owner: Uuid, epoch: u64) -> JobMetadata {
        TestState::metadata(job_id, &job_status(job_id, status), Some(owner), epoch, 0)
    }

    #[test]
    fn is_terminal_classifies_status() {
        let owner = Uuid::new_v4();
        assert!(
            meta_with("j", Status::Successful(SuccessfulJob::default()), owner, 0).is_terminal()
        );
        assert!(meta_with("j", Status::Failed(FailedJob::default()), owner, 0).is_terminal());
        assert!(!meta_with("j", Status::Running(RunningJob::default()), owner, 0).is_terminal());
        assert!(!meta_with("j", Status::Queued(QueuedJob::default()), owner, 0).is_terminal());
    }

    #[test]
    fn is_terminal_treats_corrupt_status_as_terminal() {
        let mut meta = meta_with(
            "j",
            Status::Running(RunningJob::default()),
            Uuid::new_v4(),
            0,
        );
        // Not a decodable `JobStatus` protobuf.
        meta.status = vec![0xff, 0xff, 0xff, 0xff];
        assert!(meta.job_status().is_err(), "status should be undecodable");
        // An undecodable persisted status must be treated as terminal so a job with
        // corrupt metadata is never taken over and resumed.
        assert!(meta.is_terminal());
    }

    #[tokio::test]
    async fn try_acquire_refuses_unknown_self_owned_and_corrupt() {
        let state = test_state();

        // Unknown job: recovery must fail rather than report absence.
        let missing = state.try_acquire_job(&JobId::new("missing")).await;
        assert!(
            missing.is_err(),
            "a job with no saved scheduler state must fail acquisition: {missing:?}"
        );

        // A job this scheduler already owns is not re-acquired.
        let self_owned = meta_with(
            "self",
            Status::Running(RunningJob::default()),
            state.owner_instance_id,
            3,
        );
        state
            .meta
            .insert("self", &self_owned)
            .await
            .expect("persist self-owned meta");
        assert!(
            state
                .try_acquire_job(&JobId::new("self"))
                .await
                .expect("acquire")
                .is_none(),
            "a self-owned job should not be re-acquired"
        );

        // A foreign-owned job whose status is corrupt is treated as terminal, so
        // takeover is refused (rather than driving a job we can't decode).
        let mut corrupt = meta_with(
            "corrupt",
            Status::Running(RunningJob::default()),
            Uuid::new_v4(),
            1,
        );
        corrupt.status = vec![0xff, 0xff, 0xff, 0xff];
        state
            .meta
            .insert("corrupt", &corrupt)
            .await
            .expect("persist corrupt meta");
        assert!(
            state
                .try_acquire_job(&JobId::new("corrupt"))
                .await
                .expect("acquire")
                .is_none(),
            "a job with corrupt status should not be taken over"
        );
    }

    #[tokio::test]
    async fn remove_job_deletes_meta_and_graph() {
        let state = test_state();
        let meta = meta_with(
            "j1",
            Status::Running(RunningJob::default()),
            state.owner_instance_id,
            0,
        );
        state.meta.insert("j1", &meta).await.expect("persist meta");
        // The graph blob is deleted by path and never decoded, so dummy bytes are fine.
        state
            .store
            .put(&state.graph_path("j1"), b"graph-blob".to_vec().into())
            .await
            .expect("persist graph blob");

        assert!(
            state
                .get_job_status(&JobId::new("j1"))
                .await
                .expect("status")
                .is_some(),
            "job should be visible before removal"
        );

        state.remove_job(&JobId::new("j1")).await.expect("remove");

        assert!(
            state
                .get_job_status(&JobId::new("j1"))
                .await
                .expect("status")
                .is_none(),
            "metadata should be gone after removal"
        );
        assert!(
            state.store.get(&state.graph_path("j1")).await.is_err(),
            "graph blob should be gone after removal"
        );
    }

    /// The graph a scheduler persists for a job whose map stage has succeeded: one
    /// multi-partition task that covered input partitions `0`, `1` and `2` of the
    /// stage's four and consumed two vcores — deliberately not one per partition, so
    /// the decoder's fallback of one vcore per partition cannot produce it.
    fn graph_with_a_multi_partition_task(
        codec: &BallistaCodec<
            datafusion_proto::protobuf::LogicalPlanNode,
            datafusion_proto::protobuf::PhysicalPlanNode,
        >,
    ) -> Vec<u8> {
        use ballista_core::serde::protobuf::{
            ExecutionGraph, ExecutionGraphStage, ShuffleWritePartition, SuccessfulStage,
            SuccessfulTask, TaskInfo, execution_graph_stage::StageType, task_info,
        };
        use datafusion::arrow::datatypes::Schema;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion::physical_plan::empty::EmptyExec;

        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let mut plan_bytes = Vec::new();
        datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
            plan,
            codec.physical_extension_codec(),
        )
        .and_then(|node| node.try_encode(&mut plan_bytes))
        .expect("encode the stage plan");

        let task = TaskInfo {
            task_id: 0,
            partition_id: 0,
            status: Some(task_info::Status::Successful(SuccessfulTask {
                executor_id: "executor-1".to_string(),
                partitions: vec![ShuffleWritePartition {
                    partition_id: 0,
                    path: "/job/1/0".to_string(),
                    num_batches: 1,
                    num_rows: 1,
                    num_bytes: 1,
                    ..Default::default()
                }],
                ..Default::default()
            })),
            global_input_partition_ids: vec![0, 1, 2],
            vcores_consumed: 2,
            ..Default::default()
        };
        ExecutionGraph {
            job_id: "j".to_string(),
            session_id: "session".to_string(),
            status: Some(job_status("j", Status::Running(RunningJob::default()))),
            stages: vec![ExecutionGraphStage {
                stage_type: Some(StageType::SuccessfulStage(SuccessfulStage {
                    stage_id: 1,
                    partitions: 4,
                    plan: plan_bytes,
                    task_infos: vec![task],
                    ..Default::default()
                })),
            }],
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// The task info of stage 1's only task in an encoded graph.
    fn persisted_task(bytes: &[u8]) -> ballista_core::serde::protobuf::TaskInfo {
        use ballista_core::serde::protobuf::ExecutionGraph;
        use ballista_core::serde::protobuf::execution_graph_stage::StageType;

        let graph = ExecutionGraph::decode(bytes).expect("decode the persisted graph");
        let Some(StageType::SuccessfulStage(stage)) =
            graph.stages.into_iter().find_map(|stage| stage.stage_type)
        else {
            panic!("the persisted graph lost its successful stage");
        };
        let [task] = <[_; 1]>::try_from(stage.task_infos).unwrap_or_else(|infos: Vec<_>| {
            panic!("expected one persisted task, found {}", infos.len())
        });
        task
    }

    /// Regression test for `TaskInfo` proto fields 11 and 12 —
    /// `global_input_partition_ids` and `vcores_consumed` — which the
    /// `spiceai/datafusion-ballista` fork adds so its execution-graph serialization
    /// can persist them. Upstream's tasks cover several input partitions, but
    /// upstream persists no task info, so its proto has neither field; a merge that
    /// takes upstream's proto still builds, because the decoder falls back to
    /// `[partition_id]` and one vcore per partition.
    ///
    /// Without them a scheduler that takes over a job keeps only the first partition
    /// of each recovered multi-partition task, so after an executor loss the others
    /// are never rescheduled, and it refunds the wrong number of vcores, so the
    /// executor's budget drifts. The graph goes through the same `load_graph` and
    /// `put_graph` a takeover and a save use: decoded, the task has to cover all
    /// three partitions and two vcores, and saved again, the bytes have to carry
    /// both fields.
    #[tokio::test]
    async fn a_persisted_multi_partition_task_keeps_its_partitions_and_vcores() {
        use ballista_scheduler::state::execution_stage::ExecutionStage;

        let state = test_state();
        let meta = meta_with(
            "j",
            Status::Running(RunningJob::default()),
            state.owner_instance_id,
            0,
        );
        state
            .store
            .put(
                &state.graph_path("j"),
                graph_with_a_multi_partition_task(&state.codec).into(),
            )
            .await
            .expect("persist the graph");

        let (graph, version) = state.load_graph("j", &meta).await.expect("load the graph");
        state.graph_versions.insert("j".to_string(), (0, version));
        let Some(ExecutionStage::Successful(map_stage)) = graph.stages().get(&1) else {
            panic!("the loaded graph lost its successful stage");
        };
        let [task] = map_stage.task_infos.as_slice() else {
            panic!(
                "expected one loaded task, found {}",
                map_stage.task_infos.len()
            );
        };
        assert_eq!(
            task.global_input_partition_ids,
            vec![0, 1, 2],
            "a recovered multi-partition task has to keep every partition it covered, or \
             the ones after the first are never rescheduled after an executor loss"
        );
        assert_eq!(
            task.vcores_consumed, 2,
            "a recovered task has to keep the vcores it consumed, or the executor's budget \
             is refunded the wrong amount"
        );

        assert!(
            state
                .put_graph("j", &graph, 0)
                .await
                .expect("save the graph again"),
            "nothing else wrote the graph"
        );
        let saved = state
            .store
            .get(&state.graph_path("j"))
            .await
            .expect("read the saved graph")
            .bytes()
            .await
            .expect("read the saved graph's bytes");
        let task = persisted_task(&saved);
        assert_eq!(
            task.global_input_partition_ids,
            vec![0, 1, 2],
            "a saved graph has to carry `global_input_partition_ids`"
        );
        assert_eq!(
            task.vcores_consumed, 2,
            "a saved graph has to carry `vcores_consumed`"
        );
    }

    type Race = tokio::sync::Mutex<Option<futures::future::BoxFuture<'static, ()>>>;

    /// Runs `before_write` once, right before the next graph write reaches the
    /// store, `before_head` once, right before the next read of the graph's
    /// version, `before_meta_read` once, right before the next metadata read,
    /// and `after_meta_write` once, right after the next metadata write lands.
    struct GraphRace {
        inner: Arc<dyn ObjectStore>,
        before_write: Race,
        before_head: Race,
        before_meta_read: Race,
        after_meta_write: Race,
    }

    impl GraphRace {
        fn on(inner: &Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::clone(inner),
                before_write: tokio::sync::Mutex::new(None),
                before_head: tokio::sync::Mutex::new(None),
                before_meta_read: tokio::sync::Mutex::new(None),
                after_meta_write: tokio::sync::Mutex::new(None),
            })
        }

        async fn run(race: &Race, location: &Path) {
            if location.as_ref().contains("scheduler/graph/") {
                let race = race.lock().await.take();
                if let Some(race) = race {
                    race.await;
                }
            }
        }
    }

    impl std::fmt::Display for GraphRace {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GraphRace")
        }
    }

    impl std::fmt::Debug for GraphRace {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GraphRace")
        }
    }

    #[async_trait]
    impl ObjectStore for GraphRace {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Self::run(&self.before_write, location).await;
            let written = self.inner.put_opts(location, payload, opts).await;
            if location.as_ref().contains("scheduler/meta/") {
                let race = self.after_meta_write.lock().await.take();
                if let Some(race) = race {
                    race.await;
                }
            }
            written
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.head {
                Self::run(&self.before_head, location).await;
            } else if location.as_ref().contains("scheduler/meta/") {
                let race = self.before_meta_read.lock().await.take();
                if let Some(race) = race {
                    race.await;
                }
            }
            self.inner.get_opts(location, options).await
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A running job's graph, told apart from others by its session id.
    fn running_graph(state: &TestState, session_id: &str) -> ExecutionGraphBox {
        use ballista_core::serde::protobuf::ExecutionGraph;

        let mut graph =
            ExecutionGraph::decode(graph_with_a_multi_partition_task(&state.codec).as_slice())
                .expect("decode the graph");
        graph.session_id = session_id.to_string();
        let ctx = state.session_context(session_id).expect("session context");
        execution_graph_from_bytes(&graph.encode_to_vec(), &state.codec, ctx.as_ref())
            .expect("build the graph")
    }

    /// The session id of the graph stored for `job_id`.
    async fn stored_graph_session(
        store: &Arc<dyn ObjectStore>,
        state: &TestState,
        job_id: &str,
    ) -> String {
        use ballista_core::serde::protobuf::ExecutionGraph;

        let bytes = store
            .get(&state.graph_path(job_id))
            .await
            .expect("read the stored graph")
            .bytes()
            .await
            .expect("read the stored graph's bytes");
        ExecutionGraph::decode(bytes.as_ref())
            .expect("decode the stored graph")
            .session_id
    }

    /// A scheduler that loses a job while its graph write is in flight does not
    /// replace the graph the scheduler that took the job over has saved since.
    #[tokio::test]
    async fn a_superseded_owner_does_not_replace_the_new_owners_graph() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let racing = GraphRace::on(&inner);
        let old_owner = test_state_on(Arc::clone(&racing) as Arc<dyn ObjectStore>);
        let new_owner = Arc::new(test_state_on(Arc::clone(&inner)));
        let job = JobId::new("j");

        old_owner.accept_job(&job, "test", 0).expect("accept");
        old_owner
            .submit_job(job.clone(), &running_graph(&old_owner, "old-first"), None)
            .await
            .expect("submit");

        *racing.before_write.lock().await = Some(take_over_and_save(&new_owner, &job));

        old_owner
            .save_job(&job, &running_graph(&old_owner, "old-second"))
            .await
            .expect("a superseded owner yields");

        assert!(
            racing.before_write.lock().await.is_none(),
            "the takeover must have run"
        );
        assert_eq!(
            stored_graph_session(&inner, &old_owner, "j").await,
            "new",
            "the new owner's graph must be kept"
        );
        assert!(
            old_owner.local_jobs.get("j").is_none(),
            "the superseded owner stops driving the job"
        );
    }

    /// A scheduler whose graph write meets a graph another writer changed, and
    /// that loses the job before it retries, does not replace the graph the new
    /// owner saved in between.
    #[tokio::test]
    async fn a_superseded_owner_does_not_retry_over_the_new_owners_graph() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let racing = GraphRace::on(&inner);
        let old_owner = test_state_on(Arc::clone(&racing) as Arc<dyn ObjectStore>);
        let new_owner = Arc::new(test_state_on(Arc::clone(&inner)));
        let job = JobId::new("j");

        old_owner.accept_job(&job, "test", 0).expect("accept");
        old_owner
            .submit_job(job.clone(), &running_graph(&old_owner, "old-first"), None)
            .await
            .expect("submit");
        write_stray_graph(&inner, &old_owner, "j", "stray").await;

        *racing.before_head.lock().await = Some(take_over_and_save(&new_owner, &job));

        old_owner
            .save_job(&job, &running_graph(&old_owner, "old-second"))
            .await
            .expect("a superseded owner yields");

        assert!(
            racing.before_head.lock().await.is_none(),
            "the takeover must have run"
        );
        assert_eq!(
            stored_graph_session(&inner, &old_owner, "j").await,
            "new",
            "the new owner's graph must be kept"
        );
        assert!(
            old_owner.local_jobs.get("j").is_none(),
            "the superseded owner stops driving the job"
        );
    }

    /// `new_owner` takes `job` over and saves a graph with session id `new`.
    fn take_over_and_save(
        new_owner: &Arc<TestState>,
        job: &JobId,
    ) -> futures::future::BoxFuture<'static, ()> {
        let (new_owner, job) = (Arc::clone(new_owner), job.clone());
        Box::pin(async move {
            new_owner
                .try_acquire_job(&job)
                .await
                .expect("acquire")
                .expect("the new owner takes the job over");
            let graph = running_graph(&new_owner, "new");
            new_owner
                .save_job(&job, &graph)
                .await
                .expect("the new owner saves");
        })
    }

    /// Writes a graph for `job_id` straight to the store, outside any
    /// scheduler's bookkeeping.
    async fn write_stray_graph(
        store: &Arc<dyn ObjectStore>,
        state: &TestState,
        job_id: &str,
        session_id: &str,
    ) {
        let bytes =
            execution_graph_to_bytes(running_graph(state, session_id).as_ref(), &state.codec)
                .expect("encode");
        store
            .put(&state.graph_path(job_id), bytes.into())
            .await
            .expect("write the stray graph");
    }

    /// A removal whose graph delete failed leaves the graph behind.
    #[tokio::test]
    async fn a_submission_writes_over_a_graph_a_removed_job_left_behind() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = test_state_on(Arc::clone(&store));
        let job = JobId::new("j");
        write_stray_graph(&store, &state, "j", "orphan").await;

        state.accept_job(&job, "test", 0).expect("accept");
        state
            .submit_job(job.clone(), &running_graph(&state, "fresh"), None)
            .await
            .expect("submit");

        assert_eq!(stored_graph_session(&store, &state, "j").await, "fresh");
        assert!(state.local_jobs.get("j").is_some());
    }

    /// A scheduler that takes a job over before its submitter writes the graph
    /// keeps the job: the submitter neither replaces the graph nor removes the
    /// new owner's metadata.
    #[tokio::test]
    async fn a_submission_keeps_the_claim_of_a_scheduler_that_took_the_job_over() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let racing = GraphRace::on(&inner);
        let submitter = test_state_on(Arc::clone(&racing) as Arc<dyn ObjectStore>);
        let new_owner = Arc::new(test_state_on(Arc::clone(&inner)));
        let job = JobId::new("j");
        write_stray_graph(&inner, &submitter, "j", "orphan").await;

        *racing.before_write.lock().await = Some(take_over_and_save(&new_owner, &job));

        submitter.accept_job(&job, "test", 0).expect("accept");
        let outcome = submitter
            .submit_job(job.clone(), &running_graph(&submitter, "submitted"), None)
            .await;

        assert!(outcome.is_err(), "the submitter does not own the job");
        let meta = new_owner
            .meta
            .get("j")
            .await
            .expect("read meta")
            .expect("the new owner's metadata must remain");
        assert_eq!(meta.owner_instance_id, Some(new_owner.owner_instance_id));
        assert_eq!(stored_graph_session(&inner, &submitter, "j").await, "new");
        assert!(submitter.local_jobs.get("j").is_none());
    }

    /// The new owner's first graph write lands even when the scheduler it took the
    /// job from wrote the graph after the takeover.
    #[tokio::test]
    async fn the_new_owner_writes_over_a_graph_the_old_owner_wrote_late() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old_owner = Arc::new(test_state_on(Arc::clone(&inner)));
        let racing = GraphRace::on(&inner);
        let new_owner = test_state_on(Arc::clone(&racing) as Arc<dyn ObjectStore>);
        let job = JobId::new("j");

        old_owner.accept_job(&job, "test", 0).expect("accept");
        old_owner
            .submit_job(job.clone(), &running_graph(&old_owner, "old-first"), None)
            .await
            .expect("submit");
        new_owner
            .try_acquire_job(&job)
            .await
            .expect("acquire")
            .expect("the new owner takes the job over");

        // The old owner's graph write lands between the takeover and the new
        // owner's first save.
        let (late, store) = (Arc::clone(&old_owner), Arc::clone(&inner));
        *racing.before_write.lock().await = Some(Box::pin(async move {
            write_stray_graph(&store, &late, "j", "old-late").await;
        }));

        new_owner
            .save_job(&job, &running_graph(&new_owner, "new"))
            .await
            .expect("the new owner saves");

        assert_eq!(stored_graph_session(&inner, &new_owner, "j").await, "new");
        assert!(
            new_owner.local_jobs.get("j").is_some(),
            "the new owner keeps driving the job"
        );
    }

    /// A save begun under one generation that reaches the metadata after
    /// another scheduler took the job over and this scheduler took it back is
    /// refused: the generation it was made under is no longer held, even
    /// though the owner is this scheduler again.
    #[tokio::test]
    async fn a_save_from_an_earlier_generation_is_refused_after_the_job_is_taken_back() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let racing = GraphRace::on(&inner);
        let owner = Arc::new(test_state_on(Arc::clone(&racing) as Arc<dyn ObjectStore>));
        let other = Arc::new(test_state_on(Arc::clone(&inner)));
        let job = JobId::new("j");

        owner.accept_job(&job, "test", 0).expect("accept");
        owner
            .submit_job(job.clone(), &running_graph(&owner, "first"), None)
            .await
            .expect("submit");

        let (taker, retaker, job_id) = (Arc::clone(&other), Arc::clone(&owner), job.clone());
        *racing.before_meta_read.lock().await = Some(Box::pin(async move {
            taker
                .try_acquire_job(&job_id)
                .await
                .expect("acquire")
                .expect("another scheduler takes the job over");
            retaker
                .try_acquire_job(&job_id)
                .await
                .expect("acquire")
                .expect("the first owner takes the job back");
            retaker
                .save_job(&job_id, &running_graph(&retaker, "current"))
                .await
                .expect("the current generation saves");
        }));

        owner
            .save_job(&job, &running_graph(&owner, "stale"))
            .await
            .expect("a stale save yields");

        assert!(
            racing.before_meta_read.lock().await.is_none(),
            "the takeovers must have run"
        );
        let meta = owner
            .meta
            .get("j")
            .await
            .expect("read meta")
            .expect("the job's metadata");
        assert_eq!(meta.owner_instance_id, Some(owner.owner_instance_id));
        assert_eq!(meta.epoch, 2);
        assert_eq!(
            stored_graph_session(&inner, &owner, "j").await,
            "current",
            "the stale save must not replace the current generation's graph"
        );
        assert_eq!(
            owner.local_jobs.get("j").map(|local| local.generation),
            Some(2),
            "the current generation keeps driving the job"
        );
    }

    /// A save whose metadata write lands before another scheduler takes the
    /// job over and this scheduler takes it back does not write its graph on
    /// the version the later generation recorded.
    #[tokio::test]
    async fn a_save_paused_after_its_metadata_write_does_not_replace_a_later_generations_graph() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let racing = GraphRace::on(&inner);
        let owner = Arc::new(test_state_on(Arc::clone(&racing) as Arc<dyn ObjectStore>));
        let other = Arc::new(test_state_on(Arc::clone(&inner)));
        let job = JobId::new("j");

        owner.accept_job(&job, "test", 0).expect("accept");
        owner
            .submit_job(job.clone(), &running_graph(&owner, "first"), None)
            .await
            .expect("submit");

        let (taker, retaker, job_id) = (Arc::clone(&other), Arc::clone(&owner), job.clone());
        *racing.after_meta_write.lock().await = Some(Box::pin(async move {
            taker
                .try_acquire_job(&job_id)
                .await
                .expect("acquire")
                .expect("another scheduler takes the job over");
            retaker
                .try_acquire_job(&job_id)
                .await
                .expect("acquire")
                .expect("the first owner takes the job back");
            retaker
                .save_job(&job_id, &running_graph(&retaker, "current"))
                .await
                .expect("the current generation saves");
        }));

        owner
            .save_job(&job, &running_graph(&owner, "stale"))
            .await
            .expect("a stale save yields");

        assert!(
            racing.after_meta_write.lock().await.is_none(),
            "the takeovers must have run"
        );
        assert_eq!(
            stored_graph_session(&inner, &owner, "j").await,
            "current",
            "the stale save must not replace the current generation's graph"
        );
        assert_eq!(
            owner.local_jobs.get("j").map(|local| local.generation),
            Some(2),
            "the current generation keeps driving the job"
        );
    }
}
