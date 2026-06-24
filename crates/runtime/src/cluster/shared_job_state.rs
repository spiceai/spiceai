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
        matches!(
            self.job_status().ok().and_then(|s| s.status),
            Some(Status::Successful(_) | Status::Failed(_))
        )
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
    fn accept_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
        self.queued_jobs
            .insert(job_id.to_string(), (job_name.to_string(), queued_at));
        Ok(())
    }

    fn pending_job_number(&self) -> usize {
        self.queued_jobs.len()
    }

    async fn submit_job(
        &self,
        job_id: String,
        graph: &ExecutionGraphBox,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        let Some((_, (_, queued_at))) = self.queued_jobs.remove(&job_id) else {
            return Err(BallistaError::Internal(format!(
                "failed to submit job {job_id}, not found in queued jobs"
            )));
        };

        let status = graph.status().clone();
        self.put_graph(&job_id, graph).await?;

        let mut meta = Self::metadata(&job_id, &status, Some(self.owner_instance_id), 0, queued_at);
        meta.session_id = graph.session_id().to_string();
        if let object_store_occ::WriteResult::Conflict { .. } = self
            .meta
            .insert_or_update(&job_id, &meta)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to persist job meta: {e}")))?
        {
            return Err(BallistaError::Internal(format!(
                "failed to submit job {job_id}, meta already owned"
            )));
        }

        self.local_jobs
            .insert(job_id.clone(), LocalJob { status, subscriber });
        self.event_sender.send(&JobStateEvent::JobAcquired {
            job_id,
            owner: self.scheduler.clone(),
        });
        Ok(())
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some((job_name, queued_at)) = self.queued_jobs.get(job_id).as_deref() {
            return Ok(Some(JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.clone(),
                status: Some(Status::Queued(ballista_core::serde::protobuf::QueuedJob {
                    queued_at: *queued_at,
                })),
            }));
        }
        if let Some(local) = self.local_jobs.get(job_id) {
            return Ok(Some(local.status.clone()));
        }
        match self
            .meta
            .get(job_id)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
        {
            Some(meta) => Ok(Some(meta.job_status()?)),
            None => Ok(None),
        }
    }

    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraphBox>> {
        let Some(meta) = self
            .meta
            .get(job_id)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?
        else {
            return Ok(None);
        };
        Ok(Some(self.load_graph(job_id, &meta).await?))
    }

    async fn save_job(&self, job_id: &str, graph: &ExecutionGraphBox) -> Result<()> {
        let status = graph.status().clone();
        self.put_graph(job_id, graph).await?;

        let current = self
            .meta
            .get(job_id)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to read job meta: {e}")))?;
        let (epoch, queued_at, session_id) = current
            .as_ref()
            .map(|m| (m.epoch, m.queued_at, m.session_id.clone()))
            .unwrap_or((0, 0, graph.session_id().to_string()));

        let mut meta = Self::metadata(
            job_id,
            &status,
            Some(self.owner_instance_id),
            epoch,
            queued_at,
        );
        meta.session_id = session_id;
        if let object_store_occ::UpdateResult::Conflict { current } = self
            .meta
            .update(job_id, &meta)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to persist job meta: {e}")))?
        {
            // Another scheduler has taken ownership; stop asserting our view.
            tracing::warn!(
                "job {job_id} ownership changed under us (epoch {} -> {}); yielding",
                epoch,
                current.epoch
            );
            return Ok(());
        }

        let terminal = matches!(
            status.status,
            Some(Status::Successful(_) | Status::Failed(_))
        );
        if terminal {
            if let Some((_, local)) = self.local_jobs.remove(job_id) {
                local.notify(status.clone());
            }
        } else if let Some(mut local) = self.local_jobs.get_mut(job_id) {
            local.status = status.clone();
            local.notify(status.clone());
        }

        self.event_sender.send(&JobStateEvent::JobUpdated {
            job_id: job_id.to_string(),
            status,
        });
        Ok(())
    }

    async fn try_acquire_job(&self, job_id: &str) -> Result<Option<ExecutionGraphBox>> {
        let Some(meta) = self
            .meta
            .get(job_id)
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
        if let object_store_occ::UpdateResult::Conflict { .. } = self
            .meta
            .update(job_id, &claimed)
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to acquire job: {e}")))?
        {
            return Ok(None);
        }

        let graph = self.load_graph(job_id, &claimed).await?;
        self.local_jobs.insert(
            job_id.to_string(),
            LocalJob {
                status: graph.status().clone(),
                subscriber: None,
            },
        );
        self.event_sender.send(&JobStateEvent::JobAcquired {
            job_id: job_id.to_string(),
            owner: self.scheduler.clone(),
        });
        Ok(Some(graph))
    }

    async fn job_state_events(&self) -> Result<JobStateEventStream> {
        Ok(Box::pin(self.event_sender.subscribe()))
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        self.local_jobs.remove(job_id);
        self.queued_jobs.remove(job_id);
        let _ = self.store.delete(&self.graph_path(job_id)).await;
        if let Err(e) = self.meta.delete(job_id).await {
            tracing::warn!("failed to delete job meta for {job_id}: {e}");
        }
        Ok(())
    }

    async fn get_jobs(&self) -> Result<HashSet<String>> {
        let keys = self
            .meta
            .list_keys()
            .await
            .map_err(|e| BallistaError::Internal(format!("failed to list jobs: {e}")))?;
        Ok(keys.into_iter().collect())
    }

    async fn fail_unscheduled_job(&self, job_id: &str, reason: String) -> Result<()> {
        let Some((_, (job_name, queued_at))) = self.queued_jobs.remove(job_id) else {
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
        let mut meta = Self::metadata(job_id, &status, Some(self.owner_instance_id), 0, queued_at);
        meta.session_id = String::new();
        let _ = self.meta.insert_or_update(job_id, &meta).await;
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
