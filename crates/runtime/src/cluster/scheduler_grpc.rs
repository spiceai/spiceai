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

//! The Ballista scheduler service as the cluster port serves it.
//!
//! Executors call the executor half of the service — they poll for work and
//! report on it — and each call acts as the executor it names, so it is served
//! only to the node that registered that executor ([`authorize_executor_claim`]).
//!
//! The client half (submitting and managing jobs, sessions and catalogs) is
//! what the scheduler's own query path calls, in process. No cluster node calls
//! it over the network, so the cluster port refuses it: through it, any node
//! could otherwise run queries with the scheduler's access.

use std::sync::Arc;

use ballista_core::serde::protobuf::{
    CancelJobParams, CancelJobResult, CleanJobDataParams, CleanJobDataResult,
    CreateUpdateSessionParams, CreateUpdateSessionResult, ExecuteQueryParams, ExecuteQueryResult,
    ExecutorRegistration, ExecutorStoppedParams, ExecutorStoppedResult, GetCatalogParams,
    GetCatalogResult, GetJobMetricsParams, GetJobMetricsResult, GetJobStatusParams,
    GetJobStatusResult, GetRemoteFunctionsParams, GetRemoteFunctionsResult, HeartBeatParams,
    HeartBeatResult, PollWorkParams, PollWorkResult, RegisterExecutorParams,
    RegisterExecutorResult, RemoveSessionParams, RemoveSessionResult, UpdateTaskStatusParams,
    UpdateTaskStatusResult, scheduler_grpc_server::SchedulerGrpc,
};
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use runtime_cluster::{ExecutorRegistry, peer::PeerCertificate};
use tonic::{Request, Response, Status};

use super::peer::{Advertised, authorize_executor_claim};

type Scheduler = SchedulerServer<LogicalPlanNode, PhysicalPlanNode>;

/// The Ballista scheduler service on the cluster port: the executor half
/// served to the executor each call names, the client half refused.
pub(crate) struct ClusterSchedulerGrpc {
    scheduler: Arc<Scheduler>,
    executor_registry: Arc<ExecutorRegistry>,
}

impl ClusterSchedulerGrpc {
    pub(crate) fn new(scheduler: Arc<Scheduler>, executor_registry: Arc<ExecutorRegistry>) -> Self {
        Self {
            scheduler,
            executor_registry,
        }
    }

    /// Refuses `request` unless it comes from the node that registered
    /// `executor_id` and, with `metadata`, from a node whose certificate names
    /// the host the executor advertises for its shuffle and result data.
    fn authorize<T>(
        &self,
        request: &Request<T>,
        executor_id: &str,
        metadata: Option<&ExecutorRegistration>,
        action: &str,
    ) -> Result<(), Status> {
        let certificate = PeerCertificate::from_request(request);
        // The `host:port` other nodes fetch this executor's shuffle and result
        // data at; an IPv6 literal is bracketed so the port stays separable.
        let advertised = metadata.and_then(|metadata| {
            metadata.host.as_deref().map(|host| {
                if host.contains(':') && !host.starts_with('[') {
                    format!("[{host}]:{}", metadata.port)
                } else {
                    format!("{host}:{}", metadata.port)
                }
            })
        });
        authorize_executor_claim(
            &self.executor_registry,
            certificate.as_ref(),
            executor_id,
            advertised.as_deref().map(Advertised::Address),
            action,
        )
    }
}

/// The refusal of a client-half method on the cluster port.
fn served_in_process_only(method: &str) -> Status {
    Status::permission_denied(format!(
        "The scheduler does not serve {method} on its cluster port: it is called only by the scheduler's own query path"
    ))
}

#[tonic::async_trait]
impl SchedulerGrpc for ClusterSchedulerGrpc {
    async fn poll_work(
        &self,
        request: Request<PollWorkParams>,
    ) -> Result<Response<PollWorkResult>, Status> {
        if let Some(metadata) = request.get_ref().metadata.as_ref() {
            self.authorize(&request, &metadata.id, Some(metadata), "poll for work")?;
        }
        self.scheduler.poll_work(request).await
    }

    async fn register_executor(
        &self,
        request: Request<RegisterExecutorParams>,
    ) -> Result<Response<RegisterExecutorResult>, Status> {
        if let Some(metadata) = request.get_ref().metadata.as_ref() {
            self.authorize(&request, &metadata.id, Some(metadata), "register")?;
        }
        self.scheduler.register_executor(request).await
    }

    async fn heart_beat_from_executor(
        &self,
        request: Request<HeartBeatParams>,
    ) -> Result<Response<HeartBeatResult>, Status> {
        let params = request.get_ref();
        self.authorize(
            &request,
            &params.executor_id,
            params.metadata.as_ref(),
            "accept a heartbeat",
        )?;
        self.scheduler.heart_beat_from_executor(request).await
    }

    async fn update_task_status(
        &self,
        request: Request<UpdateTaskStatusParams>,
    ) -> Result<Response<UpdateTaskStatusResult>, Status> {
        let executor_id = request.get_ref().executor_id.clone();
        self.authorize(&request, &executor_id, None, "update task status")?;
        self.scheduler.update_task_status(request).await
    }

    async fn executor_stopped(
        &self,
        request: Request<ExecutorStoppedParams>,
    ) -> Result<Response<ExecutorStoppedResult>, Status> {
        let executor_id = request.get_ref().executor_id.clone();
        self.authorize(&request, &executor_id, None, "stop")?;
        self.scheduler.executor_stopped(request).await
    }

    async fn create_update_session(
        &self,
        _request: Request<CreateUpdateSessionParams>,
    ) -> Result<Response<CreateUpdateSessionResult>, Status> {
        Err(served_in_process_only("CreateUpdateSession"))
    }

    async fn remove_session(
        &self,
        _request: Request<RemoveSessionParams>,
    ) -> Result<Response<RemoveSessionResult>, Status> {
        Err(served_in_process_only("RemoveSession"))
    }

    type ExecuteQueryPushStream = <Scheduler as SchedulerGrpc>::ExecuteQueryPushStream;

    async fn execute_query_push(
        &self,
        _request: Request<ExecuteQueryParams>,
    ) -> Result<Response<Self::ExecuteQueryPushStream>, Status> {
        Err(served_in_process_only("ExecuteQueryPush"))
    }

    async fn execute_query(
        &self,
        _request: Request<ExecuteQueryParams>,
    ) -> Result<Response<ExecuteQueryResult>, Status> {
        Err(served_in_process_only("ExecuteQuery"))
    }

    async fn get_job_status(
        &self,
        _request: Request<GetJobStatusParams>,
    ) -> Result<Response<GetJobStatusResult>, Status> {
        Err(served_in_process_only("GetJobStatus"))
    }

    async fn get_job_metrics(
        &self,
        _request: Request<GetJobMetricsParams>,
    ) -> Result<Response<GetJobMetricsResult>, Status> {
        Err(served_in_process_only("GetJobMetrics"))
    }

    async fn cancel_job(
        &self,
        _request: Request<CancelJobParams>,
    ) -> Result<Response<CancelJobResult>, Status> {
        Err(served_in_process_only("CancelJob"))
    }

    async fn clean_job_data(
        &self,
        _request: Request<CleanJobDataParams>,
    ) -> Result<Response<CleanJobDataResult>, Status> {
        Err(served_in_process_only("CleanJobData"))
    }

    async fn get_catalog(
        &self,
        _request: Request<GetCatalogParams>,
    ) -> Result<Response<GetCatalogResult>, Status> {
        Err(served_in_process_only("GetCatalog"))
    }

    async fn get_remote_functions(
        &self,
        _request: Request<GetRemoteFunctionsParams>,
    ) -> Result<Response<GetRemoteFunctionsResult>, Status> {
        Err(served_in_process_only("GetRemoteFunctions"))
    }
}
