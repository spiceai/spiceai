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
//! Ownership is keyed by the owning scheduler's per-process `instance_id`. A
//! monotonic `epoch` is bumped on every ownership transfer and acts as a
//! fencing token: a scheduler that is presumed dead but later resurfaces will
//! observe a higher epoch and decline to continue.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
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
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SharedJobState<T, U> {
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
            owner_instance_id: Uuid::new_v4(),
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
        }
    }

    fn graph_path(&self, job_id: &str) -> Path {
        Path::from(format!("{}/{job_id}", self.graph_prefix))
    }

    async fn put_graph(&self, job_id: &str, graph: &ExecutionGraphBox) -> Result<()> {
        let bytes = execution_graph_to_bytes(graph.as_ref(), &self.codec)?;
        self.store
            .put(&self.graph_path(job_id), bytes.into())
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to persist job graph: {e}")))?;
        Ok(())
    }

    async fn load_graph(&self, job_id: &str, meta: &JobMetadata) -> Result<ExecutionGraphBox> {
        let result = self
            .store
            .get(&self.graph_path(job_id))
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job graph: {e}")))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job graph: {e}")))?;
        let ctx = self.session_context(&meta.session_id)?;
        execution_graph_from_bytes(&bytes, &self.codec, ctx.as_ref())
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
        if let object_store_occ::WriteResult::Conflict { .. } = self
            .meta
            .insert_or_update(job_id.as_str(), &meta)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to persist job meta: {e}")))?
        {
            return Err(BallistaError::Internal(format!(
                "failed to submit job {job_id}, meta already owned"
            )));
        }
        // With the claim in place, persist the graph. If that fails, best-effort
        // roll back the metadata so other schedulers never observe meta pointing
        // at a missing graph.
        if let Err(e) = self.put_graph(job_id.as_str(), graph).await {
            if let Err(cleanup) = self.meta.delete(job_id.as_str()).await {
                tracing::warn!(
                    "failed to roll back job meta for {job_id} after graph write failed: {cleanup}"
                );
            }
            return Err(e);
        }

        self.local_jobs
            .insert(job_id.to_string(), LocalJob { status, subscriber });
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
        Ok(Some(self.load_graph(job_id.as_str(), &meta).await?))
    }

    async fn save_job(&self, job_id: &JobId, graph: &ExecutionGraphBox) -> Result<()> {
        let status = graph.status().clone();

        let current = self
            .meta
            .get(job_id.as_str())
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?;
        // A scheduler that has lost ownership must not touch shared state. Drop
        // the local entry so reads fall through to the new owner's status.
        if let Some(m) = &current
            && m.owner_instance_id != Some(self.owner_instance_id)
        {
            tracing::warn!(
                "job {job_id} owned by another scheduler (epoch {}); yielding",
                m.epoch
            );
            self.local_jobs.remove(job_id.as_str());
            return Ok(());
        }
        let (epoch, queued_at, session_id) = current
            .as_ref()
            .map(|m| (m.epoch, m.queued_at, m.session_id.clone()))
            .unwrap_or((0, 0, graph.session_id().to_string()));

        let mut meta = Self::metadata(
            job_id.as_str(),
            &status,
            Some(self.owner_instance_id),
            epoch,
            queued_at,
        );
        meta.session_id = session_id;
        // Compare-and-set the ownership metadata before persisting the graph, so a
        // scheduler racing a takeover cannot clobber the shared graph blob.
        match self
            .meta
            .update(job_id.as_str(), &meta)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to persist job meta: {e}")))?
        {
            object_store_occ::UpdateResult::Ok => {}
            object_store_occ::UpdateResult::Conflict { current } => {
                tracing::warn!(
                    "job {job_id} ownership changed under us (epoch {} -> {}); yielding",
                    epoch,
                    current.epoch
                );
                self.local_jobs.remove(job_id.as_str());
                return Ok(());
            }
            object_store_occ::UpdateResult::NotFound => {
                self.local_jobs.remove(job_id.as_str());
                return Err(BallistaError::Internal(format!(
                    "job {job_id} metadata no longer exists; cannot persist state"
                )));
            }
        }
        self.put_graph(job_id.as_str(), graph).await?;

        let terminal = matches!(
            status.status,
            Some(Status::Successful(_) | Status::Failed(_))
        );
        if terminal {
            if let Some((_, local)) = self.local_jobs.remove(job_id.as_str()) {
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
        let Some(meta) = self
            .meta
            .get(job_id.as_str())
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
        else {
            return Ok(None);
        };
        if meta.is_terminal() {
            return Ok(None);
        }
        if meta.owner_instance_id == Some(self.owner_instance_id) {
            return Ok(None);
        }

        let mut claimed = meta.clone();
        claimed.owner_instance_id = Some(self.owner_instance_id);
        claimed.epoch = meta.epoch + 1;
        claimed.updated_at = now_ms();
        // Another scheduler won the claim (Conflict), or the job's metadata was
        // removed under us (NotFound) — either way we don't own it.
        if !matches!(
            self.meta
                .update(job_id.as_str(), &claimed)
                .await
                .map_err(|e| BallistaError::Internal(format!("failed to acquire job: {e}")))?,
            object_store_occ::UpdateResult::Ok
        ) {
            return Ok(None);
        }

        let graph = self.load_graph(job_id.as_str(), &claimed).await?;
        self.local_jobs.insert(
            job_id.to_string(),
            LocalJob {
                status: graph.status().clone(),
                subscriber: None,
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
        self.local_jobs.remove(job_id.as_str());
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
        if let Err(e) = self.meta.insert_or_update(job_id.as_str(), &meta).await {
            tracing::warn!("failed to persist failed status for unscheduled job {job_id}: {e}");
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
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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

        // Unknown job: nothing to acquire.
        assert!(
            state
                .try_acquire_job(&JobId::new("missing"))
                .await
                .expect("acquire")
                .is_none(),
            "unknown job should not be acquirable"
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
            .insert_or_update("self", &self_owned)
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
            .insert_or_update("corrupt", &corrupt)
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
        state
            .meta
            .insert_or_update("j1", &meta)
            .await
            .expect("persist meta");
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
}
