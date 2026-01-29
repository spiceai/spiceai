# Distributed Job State for Ballista

## Overview

This document describes the architecture for distributed job state management in Ballista, enabling:

1. **Shared job state** between multiple schedulers (currently isolated)
2. **Load balancing** of job scheduling across schedulers (currently one scheduler receives all)
3. **Job failover** when a scheduler dies (another scheduler picks up and resumes)

## Current Architecture

### Ballista Components

- **`ClusterState` trait** (`ballista/scheduler/src/cluster/mod.rs`): Manages executor registration, heartbeats, and task slot availability
- **`JobState` trait** (`ballista/scheduler/src/cluster/mod.rs`): Manages job lifecycle, execution graphs, and sessions
- **`InMemoryClusterState`/`InMemoryJobState`** (`ballista/scheduler/src/cluster/memory.rs`): Current implementations with no persistence or sharing
- **`TaskManager.active_job_cache`** (`ballista/scheduler/src/state/task_manager.rs`): Holds running `ExecutionGraph`s per scheduler

### Spice Integration

- **Scheduler Registry** (`crates/runtime/src/cluster/scheduler_registry.rs`): Uses S3 with conditional writes (`PutMode::Update(UpdateVersion)`) for scheduler heartbeats and membership discovery
- **Jobs API** (`crates/runtime/src/jobs/store.rs`): Uses same S3 object store for async SQL query results
- **Configuration** (`crates/spicepod/src/component/runtime.rs`): `runtime.scheduler.state_location` specifies S3 URI for shared state

### Current S3 Layout

```
s3://{bucket}/{base_prefix}/
├── metadata/cluster.json          # Cluster metadata
├── schedulers/                    # Scheduler registry  
│   └── {scheduler_id}.json        # Scheduler heartbeats (TTL-based)
└── jobs/                          # Async SQL Jobs API
    ├── {job_id}.json              # Job state
    └── {job_id}/
        └── chunk_N.arrow          # Result chunks
```

## Proposed Architecture

### Design Principles

1. **Single source of truth**: One file per job, atomic state transitions via conditional writes
2. **Claim-before-plan**: Schedulers claim jobs with a lightweight CAS operation before doing expensive query planning
3. **No local queueing**: Job submissions write directly to S3; any scheduler can claim
4. **Lease-based ownership**: Schedulers hold time-limited leases on jobs they're executing
5. **Orphan recovery**: Schedulers scan for jobs with expired leases and take them over
6. **Executor-initiated connections only**: Schedulers never open connections to executors; all scheduler→executor communication uses the bidirectional control stream that executors initiate

### Network Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                      Scheduler Network                          │
│  ┌───────────┐    ┌───────────┐    ┌───────────┐               │
│  │Scheduler 1│    │Scheduler 2│    │Scheduler 3│               │
│  └─────▲─────┘    └─────▲─────┘    └─────▲─────┘               │
│        │                │                │                      │
│        │    Control Streams (executor-initiated)                │
│        │                │                │                      │
└────────┼────────────────┼────────────────┼──────────────────────┘
         │                │                │
         │                │                │
┌────────┼────────────────┼────────────────┼──────────────────────┐
│        │                │                │                      │
│  ┌─────┴─────┐    ┌─────┴─────┐    ┌─────┴─────┐               │
│  │ Executor 1│    │ Executor 2│    │ Executor 3│               │
│  └───────────┘    └───────────┘    └───────────┘               │
│                      Executor Network                           │
│                  (can be private subnet)                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │   S3 / Object     │
                    │      Store        │
                    └───────────────────┘
```

The executor-initiated control stream pattern allows:
- Executors to be deployed in private networks without inbound access
- NAT traversal without complex firewall rules
- Dynamic executor scaling without scheduler reconfiguration

### Extended S3 Layout

```
s3://{bucket}/{base_prefix}/
├── metadata/cluster.json
├── schedulers/{scheduler_id}.json
├── jobs/                          # (existing async SQL jobs)
└── ballista/                      # NEW: Ballista distributed execution state
    └── jobs/
        └── {job_id}/
            ├── state.json         # Job metadata, ownership, status
            └── graph.bin          # Serialized ExecutionGraph (protobuf)
```

### Job State Schema

```rust
#[derive(Serialize, Deserialize)]
struct JobState {
    schema_version: u32,  // For future migrations
    
    // Identity
    job_id: String,
    job_name: String,
    session_id: String,
    
    // Status: Pending → Running → Completed/Failed
    status: JobStatus,
    
    // Ownership (None when Pending, Some when claimed)
    owner_scheduler_id: Option<String>,
    lease_expires_at_ms: Option<u64>,
    
    // Logical plan - stored until planning completes
    logical_plan: Vec<u8>,
    
    // Timestamps
    queued_at_ms: u64,
    claimed_at_ms: Option<u64>,
    planned_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
    
    // Populated after planning
    total_stages: Option<u32>,
    
    // Terminal state info
    output_locations: Option<Vec<PartitionLocation>>,
    error_message: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq)]
enum JobStatus {
    Pending,    // Waiting to be claimed
    Running,    // Claimed, being executed
    Completed,  // Successfully finished
    Failed,     // Failed (planning or execution)
}
```

### Configuration

Lease duration and other parameters:

```rust
struct S3JobStateConfig {
    /// How long a scheduler holds a job before lease expires (default: 30s)
    lease_duration: Duration,
    
    /// How often to renew leases on owned jobs (default: 10s)
    lease_renewal_interval: Duration,
    
    /// How often to scan for orphaned jobs (default: 15s)
    orphan_scan_interval: Duration,
    
    /// How often to poll for pending jobs to claim (default: 100ms)
    claim_poll_interval: Duration,
    
    /// Maximum concurrent jobs per scheduler (default: unlimited)
    max_concurrent_jobs: Option<usize>,
}
```

## Job Lifecycle

### 1. Job Submission

When a client submits a job to any scheduler:

```rust
impl S3JobState {
    /// Submit a new job. Writes directly to S3 - does NOT queue locally.
    /// Any scheduler (including a different one) can then claim and plan it.
    async fn accept_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        logical_plan: &[u8],
        queued_at: u64,
    ) -> Result<()> {
        let state = JobState {
            schema_version: 1,
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            session_id: session_id.to_string(),
            queued_at_ms: queued_at,
            status: JobStatus::Pending,
            owner_scheduler_id: None,
            lease_expires_at_ms: None,
            logical_plan: logical_plan.to_vec(),
            claimed_at_ms: None,
            planned_at_ms: None,
            total_stages: None,
            completed_at_ms: None,
            output_locations: None,
            error_message: None,
        };
        
        let path = self.job_state_path(job_id);
        let payload = serde_json::to_vec(&state)?;
        
        // Use Create mode to detect duplicate job IDs
        self.store
            .put_opts(&path, payload.into(), PutOptions::from(PutMode::Create))
            .await
            .map_err(|e| match e {
                ObjectStoreError::AlreadyExists { .. } => {
                    Error::JobAlreadyExists { job_id: job_id.to_string() }
                }
                other => other.into(),
            })?;
        
        tracing::info!(job_id, job_name, "Job submitted to shared state");
        Ok(())
    }
}
```

**Key point**: The receiving scheduler does NOT try to claim the job. It simply writes to S3 and returns. This enables natural load balancing.

### 2. Job Claiming

Schedulers poll for pending jobs and attempt to claim them via conditional update:

```rust
impl S3JobState {
    /// List jobs available for claiming (Pending, or Running with expired lease)
    async fn list_claimable_jobs(&self) -> Result<Vec<String>> {
        let prefix = self.jobs_prefix();
        let now = now_ms();
        let mut claimable = Vec::new();
        
        let mut stream = self.store.list(Some(&prefix));
        while let Some(entry) = stream.next().await {
            let meta = entry?;
            if !meta.location.as_ref().ends_with("/state.json") {
                continue;
            }
            
            // Read and check if claimable
            let result = self.store.get(&meta.location).await?;
            let state: JobState = serde_json::from_slice(&result.bytes().await?)?;
            
            let can_claim = match state.status {
                JobStatus::Pending => true,
                JobStatus::Running => {
                    // Expired lease = orphaned job
                    state.lease_expires_at_ms.map_or(true, |exp| exp < now)
                }
                JobStatus::Completed | JobStatus::Failed => false,
            };
            
            if can_claim {
                claimable.push(state.job_id);
            }
        }
        
        Ok(claimable)
    }
    
    /// Attempt to claim a job via conditional update. Lightweight - no planning.
    /// Returns the logical plan bytes if claim succeeds.
    async fn try_claim_job(&self, job_id: &str) -> Result<Option<ClaimedJob>> {
        let path = self.job_state_path(job_id);
        
        // Read current state
        let result = match self.store.get(&path).await {
            Ok(r) => r,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Check if claimable
        let now = now_ms();
        let can_claim = match state.status {
            JobStatus::Pending => true,
            JobStatus::Running => {
                state.lease_expires_at_ms.map_or(true, |exp| exp < now)
            }
            JobStatus::Completed | JobStatus::Failed => false,
        };
        
        if !can_claim {
            return Ok(None);
        }
        
        // Extract logical plan before modifying state
        let logical_plan = state.logical_plan.clone();
        let was_orphan = state.status == JobStatus::Running;
        
        // Update state to claim ownership
        state.status = JobStatus::Running;
        state.owner_scheduler_id = Some(self.scheduler_id.clone());
        state.lease_expires_at_ms = Some(now + self.lease_duration_ms);
        state.claimed_at_ms = Some(now);
        
        let payload = serde_json::to_vec(&state)?;
        
        // Conditional update - fails if someone else modified it first
        match self.store
            .put_opts(&path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await
        {
            Ok(_) => {
                if was_orphan {
                    tracing::info!(job_id, "Acquired orphaned job");
                } else {
                    tracing::info!(job_id, "Successfully claimed pending job");
                }
                Ok(Some(ClaimedJob {
                    job_id: state.job_id,
                    job_name: state.job_name,
                    session_id: state.session_id,
                    logical_plan,
                    queued_at_ms: state.queued_at_ms,
                    was_orphan,
                }))
            }
            Err(ObjectStoreError::Precondition { .. }) => {
                // Someone else claimed it - that's fine
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}

struct ClaimedJob {
    job_id: String,
    job_name: String,
    session_id: String,
    logical_plan: Vec<u8>,
    queued_at_ms: u64,
    was_orphan: bool,  // True if this was an orphan recovery
}
```

### 3. Query Planning

After successfully claiming a job, the scheduler performs expensive query planning:

```rust
impl S3JobState {
    /// After claiming, do the expensive planning and persist the ExecutionGraph
    async fn complete_planning(
        &self,
        job_id: &str,
        graph: &ExecutionGraph,
    ) -> Result<()> {
        // 1. Serialize and write the execution graph
        let graph_proto: protobuf::ExecutionGraph = graph.try_into()?;
        let graph_bytes = graph_proto.encode_to_vec();
        let graph_path = self.job_graph_path(job_id);
        
        self.store.put(&graph_path, graph_bytes.into()).await?;
        
        // 2. Update state to indicate planning is complete
        let state_path = self.job_state_path(job_id);
        let result = self.store.get(&state_path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Verify we still own this job
        if state.owner_scheduler_id.as_deref() != Some(&self.scheduler_id) {
            return Err(Error::LostJobOwnership { job_id: job_id.to_string() });
        }
        
        let now = now_ms();
        state.planned_at_ms = Some(now);
        state.total_stages = Some(graph.stage_count() as u32);
        state.lease_expires_at_ms = Some(now + self.lease_duration_ms);
        // Clear logical plan to save space (we have the graph now)
        state.logical_plan.clear();
        
        let payload = serde_json::to_vec(&state)?;
        self.store
            .put_opts(&state_path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await?;
        
        tracing::info!(job_id, stages = state.total_stages, "Job planning complete");
        Ok(())
    }
    
    /// If planning fails, mark the job as failed
    async fn fail_planning(&self, job_id: &str, error: &str) -> Result<()> {
        let state_path = self.job_state_path(job_id);
        let result = self.store.get(&state_path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Verify ownership
        if state.owner_scheduler_id.as_deref() != Some(&self.scheduler_id) {
            return Err(Error::LostJobOwnership { job_id: job_id.to_string() });
        }
        
        state.status = JobStatus::Failed;
        state.error_message = Some(error.to_string());
        state.completed_at_ms = Some(now_ms());
        
        let payload = serde_json::to_vec(&state)?;
        self.store
            .put_opts(&state_path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await?;
        
        tracing::info!(job_id, error, "Job planning failed");
        Ok(())
    }
}
```

### 4. Execution Graph Updates

During job execution, persist graph updates after task completions:

```rust
impl S3JobState {
    /// Save updated execution graph state. Called after task completions.
    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        let state_path = self.job_state_path(job_id);
        let graph_path = self.job_graph_path(job_id);
        
        // Read current state to verify ownership and get version
        let result = self.store.get(&state_path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Verify we still own this job
        if state.owner_scheduler_id.as_deref() != Some(&self.scheduler_id) {
            return Err(Error::LostJobOwnership { job_id: job_id.to_string() });
        }
        
        // Serialize and write the execution graph
        let graph_proto: protobuf::ExecutionGraph = graph.try_into()?;
        let graph_bytes = graph_proto.encode_to_vec();
        self.store.put(&graph_path, graph_bytes.into()).await?;
        
        // Renew lease
        state.lease_expires_at_ms = Some(now_ms() + self.lease_duration_ms);
        
        let payload = serde_json::to_vec(&state)?;
        self.store
            .put_opts(&state_path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await?;
        
        Ok(())
    }
}
```

### 5. Lease Renewal

Schedulers periodically renew leases on jobs they own:

```rust
impl S3JobState {
    /// Renew lease on a job we own. Returns false if we lost ownership.
    async fn renew_lease(&self, job_id: &str) -> Result<bool> {
        let path = self.job_state_path(job_id);
        
        let result = self.store.get(&path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Only renew if we still own it
        if state.owner_scheduler_id.as_deref() != Some(&self.scheduler_id) {
            return Ok(false);
        }
        
        state.lease_expires_at_ms = Some(now_ms() + self.lease_duration_ms);
        
        let payload = serde_json::to_vec(&state)?;
        
        match self.store
            .put_opts(&path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await
        {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::Precondition { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
```

### 6. Job Completion

```rust
impl S3JobState {
    /// Mark a job as successfully completed
    async fn complete_job(
        &self,
        job_id: &str,
        output_locations: Vec<PartitionLocation>,
    ) -> Result<()> {
        let state_path = self.job_state_path(job_id);
        let result = self.store.get(&state_path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Verify ownership
        if state.owner_scheduler_id.as_deref() != Some(&self.scheduler_id) {
            return Err(Error::LostJobOwnership { job_id: job_id.to_string() });
        }
        
        state.status = JobStatus::Completed;
        state.completed_at_ms = Some(now_ms());
        state.output_locations = Some(output_locations);
        
        let payload = serde_json::to_vec(&state)?;
        self.store
            .put_opts(&state_path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await?;
        
        tracing::info!(job_id, "Job completed successfully");
        Ok(())
    }
    
    /// Mark a job as failed
    async fn fail_job(&self, job_id: &str, error: &str) -> Result<()> {
        let state_path = self.job_state_path(job_id);
        let result = self.store.get(&state_path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut state: JobState = serde_json::from_slice(&result.bytes().await?)?;
        
        // Verify ownership
        if state.owner_scheduler_id.as_deref() != Some(&self.scheduler_id) {
            return Err(Error::LostJobOwnership { job_id: job_id.to_string() });
        }
        
        state.status = JobStatus::Failed;
        state.error_message = Some(error.to_string());
        state.completed_at_ms = Some(now_ms());
        
        let payload = serde_json::to_vec(&state)?;
        self.store
            .put_opts(&state_path, payload.into(), PutOptions::from(PutMode::Update(version)))
            .await?;
        
        tracing::info!(job_id, error, "Job failed");
        Ok(())
    }
}
```

## Scheduler Work Loop

Each scheduler runs a background loop that handles claiming, planning, lease renewal, and orphan recovery:

```rust
impl SchedulerWorkLoop {
    async fn run(&self) {
        let mut claim_interval = tokio::time::interval(self.config.claim_poll_interval);
        let mut lease_interval = tokio::time::interval(self.config.lease_renewal_interval);
        let mut orphan_interval = tokio::time::interval(self.config.orphan_scan_interval);
        
        loop {
            tokio::select! {
                _ = claim_interval.tick() => {
                    self.try_claim_pending_jobs().await;
                }
                
                _ = lease_interval.tick() => {
                    self.renew_owned_job_leases().await;
                }
                
                _ = orphan_interval.tick() => {
                    // Orphan recovery is handled by try_claim_job checking expired leases
                    // This just triggers a scan
                    self.try_claim_pending_jobs().await;
                }
                
                // Handle task completions from executors
                Some(event) = self.executor_events.recv() => {
                    self.handle_executor_event(event).await;
                }
                
                _ = self.shutdown.cancelled() => {
                    break;
                }
            }
        }
    }
    
    async fn try_claim_pending_jobs(&self) {
        // Check capacity
        let current_jobs = self.active_jobs.len();
        let max_jobs = self.config.max_concurrent_jobs.unwrap_or(usize::MAX);
        if current_jobs >= max_jobs {
            return;
        }
        
        // List claimable jobs
        let claimable = match self.job_state.list_claimable_jobs().await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list claimable jobs");
                return;
            }
        };
        
        for job_id in claimable {
            if self.active_jobs.len() >= max_jobs {
                break;
            }
            
            // Skip jobs we already have locally
            if self.active_jobs.contains_key(&job_id) {
                continue;
            }
            
            // Try to claim
            match self.job_state.try_claim_job(&job_id).await {
                Ok(Some(claimed)) => {
                    if claimed.was_orphan {
                        self.resume_orphaned_job(claimed).await;
                    } else {
                        self.plan_and_schedule(claimed).await;
                    }
                }
                Ok(None) => {
                    // Someone else claimed it
                }
                Err(e) => {
                    tracing::warn!(job_id, error = %e, "Failed to claim job");
                }
            }
        }
    }
    
    async fn plan_and_schedule(&self, claimed: ClaimedJob) {
        let job_id = claimed.job_id.clone();
        
        // Deserialize logical plan
        let logical_plan = match self.deserialize_plan(&claimed.logical_plan).await {
            Ok(plan) => plan,
            Err(e) => {
                tracing::error!(job_id, error = %e, "Failed to deserialize logical plan");
                let _ = self.job_state.fail_planning(&job_id, &e.to_string()).await;
                return;
            }
        };
        
        // Create execution graph (expensive!)
        let graph = match ExecutionGraph::try_new(
            &self.session_ctx,
            &job_id,
            &claimed.job_name,
            logical_plan,
            claimed.queued_at_ms,
        ).await {
            Ok(g) => g,
            Err(e) => {
                tracing::error!(job_id, error = %e, "Failed to create execution graph");
                let _ = self.job_state.fail_planning(&job_id, &e.to_string()).await;
                return;
            }
        };
        
        // Persist the planned graph
        if let Err(e) = self.job_state.complete_planning(&job_id, &graph).await {
            tracing::error!(job_id, error = %e, "Failed to persist execution graph");
            return;
        }
        
        // Add to local active cache for task scheduling
        self.active_jobs.insert(job_id.clone(), JobInfoCache::new(graph));
        
        tracing::info!(job_id, "Job claimed, planned, and ready for execution");
    }
    
    async fn resume_orphaned_job(&self, claimed: ClaimedJob) {
        let job_id = claimed.job_id.clone();
        
        // Load existing execution graph from S3
        let graph = match self.job_state.load_execution_graph(&job_id).await {
            Ok(Some(g)) => g,
            Ok(None) => {
                // No graph means it was orphaned before planning completed
                // Treat as a new job to plan
                self.plan_and_schedule(claimed).await;
                return;
            }
            Err(e) => {
                tracing::error!(job_id, error = %e, "Failed to load execution graph for orphaned job");
                return;
            }
        };
        
        // Query executors for current task status
        // This recovers the actual state of in-flight tasks
        if let Err(e) = self.reconcile_task_status(&job_id, &graph).await {
            tracing::warn!(job_id, error = %e, "Failed to reconcile task status, proceeding with persisted state");
        }
        
        // Add to local active cache and resume scheduling
        self.active_jobs.insert(job_id.clone(), JobInfoCache::new(graph));
        
        tracing::info!(job_id, "Resumed orphaned job");
    }
    
    async fn renew_owned_job_leases(&self) {
        for entry in self.active_jobs.iter() {
            let job_id = entry.key();
            match self.job_state.renew_lease(job_id).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(job_id, "Lost ownership of job during lease renewal");
                    // Remove from local cache - another scheduler has it
                    self.active_jobs.remove(job_id);
                }
                Err(e) => {
                    tracing::warn!(job_id, error = %e, "Failed to renew job lease");
                }
            }
        }
    }
}
```

## Orphan Recovery: Task Status Reconciliation

When a scheduler takes over an orphaned job, it needs to determine the actual state of tasks that were in-flight. This requires querying executors.

### Design Principle: Executor-Initiated Connections Only

**Important**: Schedulers never initiate connections to executors. This allows executors to be deployed in separate networks (e.g., private subnets) from schedulers. All communication from scheduler to executor uses the existing **bidirectional control stream** that executors initiate when they connect.

### Existing Control Stream Infrastructure

Spice already has a control stream mechanism (`crates/runtime/src/cluster/`):

1. **Executor side** (`control_stream_client.rs`): `ControlStreamManager` maintains bidirectional gRPC streams to all known schedulers
2. **Scheduler side** (`executor_registry.rs`): `ExecutorRegistry` tracks connected executor streams and can send messages to them
3. **Proto definition** (`runtime-proto/proto/spice.proto`): `ClusterService.ControlStream` RPC

Current message types:
- **Scheduler → Executor**: `MetricsRequest`, `PollNowCommand`
- **Executor → Scheduler**: `ExecutorHeartbeat`, `MetricsResponse`

### New Control Stream Messages

Extend the existing control stream protocol in `spice.proto`:

```protobuf
// Add to SchedulerControlMessage oneof
message SchedulerControlMessage {
    oneof message {
        MetricsRequest request_metrics = 1;
        PollNowCommand poll_now = 2;
        TaskStatusRequest request_task_status = 3;  // NEW
    }
}

// Add to ExecutorControlMessage oneof  
message ExecutorControlMessage {
    string executor_id = 1;
    oneof message {
        ExecutorHeartbeat heartbeat = 2;
        MetricsResponse metrics = 3;
        TaskStatusResponse task_status = 4;  // NEW
    }
}

// NEW: Request task status for specific jobs
message TaskStatusRequest {
    string request_id = 1;
    repeated string job_ids = 2;  // Jobs to report status for (empty = all)
}

// NEW: Response with current task statuses
message TaskStatusResponse {
    string request_id = 1;
    repeated TaskStatusReport task_reports = 2;
}

// NEW: Status of a single task
message TaskStatusReport {
    string job_id = 1;
    uint32 stage_id = 2;
    uint32 partition_id = 3;
    uint32 task_id = 4;
    uint32 task_attempt_num = 5;
    TaskState state = 6;
    // Only set if state is COMPLETED
    optional bytes shuffle_write_partitions = 7;  // Serialized ShuffleWritePartition[]
    // Only set if state is FAILED
    optional string error_message = 8;
}

enum TaskState {
    TASK_STATE_UNKNOWN = 0;
    TASK_STATE_RUNNING = 1;
    TASK_STATE_COMPLETED = 2;
    TASK_STATE_FAILED = 3;
}
```

### Executor-Side Changes

Extend `handle_scheduler_message()` in `control_stream_client.rs`:

```rust
async fn handle_scheduler_message(
    scheduler_address: &str,
    executor_id: &str,
    message: SchedulerMessage,
    outbound_tx: &mpsc::Sender<ExecutorControlMessage>,
    metrics_reader: Option<&MetricsReader>,
    poll_now_notify: &Notify,
    task_tracker: Option<&TaskTracker>,  // NEW: tracks running/completed tasks
) {
    match message {
        SchedulerMessage::RequestMetrics(request) => {
            // ... existing metrics handling ...
        }
        SchedulerMessage::PollNow(cmd) => {
            // ... existing poll_now handling ...
        }
        SchedulerMessage::RequestTaskStatus(request) => {
            tracing::debug!(
                "Received task status request from {scheduler_address}: request_id={}",
                request.request_id
            );
            
            let task_reports = if let Some(tracker) = task_tracker {
                tracker.get_task_status(&request.job_ids).await
            } else {
                Vec::new()
            };
            
            let response = ExecutorControlMessage {
                executor_id: executor_id.to_string(),
                message: Some(ExecutorMessage::TaskStatus(TaskStatusResponse {
                    request_id: request.request_id,
                    task_reports,
                })),
            };
            
            if let Err(e) = outbound_tx.send(response).await {
                tracing::warn!("Failed to send task status response to {scheduler_address}: {e}");
            }
        }
    }
}
```

### Executor Task Tracker

Add a new component to track task execution state:

```rust
/// Tracks task execution state for reporting to schedulers.
/// 
/// This allows executors to report the status of tasks they're running
/// or have recently completed, enabling scheduler failover recovery.
pub struct TaskTracker {
    /// Currently running tasks: (job_id, stage_id, partition_id) -> TaskInfo
    running: DashMap<(String, u32, u32), RunningTaskInfo>,
    /// Recently completed tasks (kept for a configurable duration)
    completed: DashMap<(String, u32, u32), CompletedTaskInfo>,
    /// How long to retain completed task info (default: 5 minutes)
    retention_duration: Duration,
}

struct RunningTaskInfo {
    task_id: u32,
    task_attempt_num: u32,
    started_at: Instant,
}

struct CompletedTaskInfo {
    task_id: u32,
    task_attempt_num: u32,
    completed_at: Instant,
    result: TaskResult,
}

enum TaskResult {
    Success { shuffle_partitions: Vec<ShuffleWritePartition> },
    Failed { error: String },
}

impl TaskTracker {
    /// Called when a task starts executing
    pub fn task_started(&self, job_id: &str, stage_id: u32, partition_id: u32, task_id: u32, attempt: u32) {
        self.running.insert(
            (job_id.to_string(), stage_id, partition_id),
            RunningTaskInfo {
                task_id,
                task_attempt_num: attempt,
                started_at: Instant::now(),
            },
        );
    }
    
    /// Called when a task completes (success or failure)
    pub fn task_completed(
        &self,
        job_id: &str,
        stage_id: u32,
        partition_id: u32,
        task_id: u32,
        attempt: u32,
        result: TaskResult,
    ) {
        self.running.remove(&(job_id.to_string(), stage_id, partition_id));
        self.completed.insert(
            (job_id.to_string(), stage_id, partition_id),
            CompletedTaskInfo {
                task_id,
                task_attempt_num: attempt,
                completed_at: Instant::now(),
                result,
            },
        );
    }
    
    /// Get task status for the requested jobs
    pub async fn get_task_status(&self, job_ids: &[String]) -> Vec<TaskStatusReport> {
        let mut reports = Vec::new();
        let filter_jobs = !job_ids.is_empty();
        
        // Report running tasks
        for entry in self.running.iter() {
            let (job_id, stage_id, partition_id) = entry.key();
            if filter_jobs && !job_ids.contains(job_id) {
                continue;
            }
            let info = entry.value();
            reports.push(TaskStatusReport {
                job_id: job_id.clone(),
                stage_id: *stage_id,
                partition_id: *partition_id,
                task_id: info.task_id,
                task_attempt_num: info.task_attempt_num,
                state: TaskState::Running,
                shuffle_write_partitions: None,
                error_message: None,
            });
        }
        
        // Report recently completed tasks
        let cutoff = Instant::now() - self.retention_duration;
        for entry in self.completed.iter() {
            let (job_id, stage_id, partition_id) = entry.key();
            if filter_jobs && !job_ids.contains(job_id) {
                continue;
            }
            let info = entry.value();
            if info.completed_at < cutoff {
                continue;  // Too old, skip
            }
            
            let (state, partitions, error) = match &info.result {
                TaskResult::Success { shuffle_partitions } => {
                    (TaskState::Completed, Some(serialize_partitions(shuffle_partitions)), None)
                }
                TaskResult::Failed { error } => {
                    (TaskState::Failed, None, Some(error.clone()))
                }
            };
            
            reports.push(TaskStatusReport {
                job_id: job_id.clone(),
                stage_id: *stage_id,
                partition_id: *partition_id,
                task_id: info.task_id,
                task_attempt_num: info.task_attempt_num,
                state,
                shuffle_write_partitions: partitions,
                error_message: error,
            });
        }
        
        reports
    }
    
    /// Periodically clean up old completed task entries
    pub fn cleanup_old_entries(&self) {
        let cutoff = Instant::now() - self.retention_duration;
        self.completed.retain(|_, info| info.completed_at >= cutoff);
    }
}
```

### Scheduler-Side Reconciliation

Extend `ExecutorRegistry` to support task status requests:

```rust
impl ExecutorRegistry {
    /// Request task status from all connected executors for specific jobs.
    /// Returns aggregated task status reports from all executors.
    pub async fn request_task_status(
        &self,
        job_ids: &[String],
        timeout: Duration,
    ) -> Result<Vec<TaskStatusReport>> {
        let connections = self.connections.read().await;
        
        if connections.is_empty() {
            return Ok(Vec::new());
        }
        
        // Send requests to all executors in parallel
        let mut handles = Vec::with_capacity(connections.len());
        for (executor_id, connection) in connections.iter() {
            let executor_id = executor_id.clone();
            let request_tx = connection.request_tx.clone();
            let pending_requests = connection.pending_task_status_requests();
            let job_ids = job_ids.to_vec();
            
            handles.push(tokio::spawn(async move {
                let request_id = Uuid::new_v4().to_string();
                let (response_tx, response_rx) = oneshot::channel();
                
                // Register pending request
                {
                    let mut pending = pending_requests.write().await;
                    pending.insert(request_id.clone(), response_tx);
                }
                
                // Send request
                let message = SchedulerControlMessage {
                    message: Some(SchedulerMessage::RequestTaskStatus(TaskStatusRequest {
                        request_id: request_id.clone(),
                        job_ids,
                    })),
                };
                
                if request_tx.send(message).await.is_err() {
                    let mut pending = pending_requests.write().await;
                    pending.remove(&request_id);
                    return (executor_id, Err("channel closed".to_string()));
                }
                
                // Wait for response with timeout
                match tokio::time::timeout(timeout, response_rx).await {
                    Ok(Ok(response)) => (executor_id, Ok(response.task_reports)),
                    Ok(Err(_)) => (executor_id, Err("response channel closed".to_string())),
                    Err(_) => {
                        let mut pending = pending_requests.write().await;
                        pending.remove(&request_id);
                        (executor_id, Err("timeout".to_string()))
                    }
                }
            }));
        }
        
        drop(connections);
        
        // Collect results
        let mut all_reports = Vec::new();
        for handle in handles {
            match handle.await {
                Ok((executor_id, Ok(reports))) => {
                    tracing::debug!(
                        executor_id,
                        count = reports.len(),
                        "Received task status reports"
                    );
                    all_reports.extend(reports);
                }
                Ok((executor_id, Err(e))) => {
                    tracing::warn!(executor_id, error = %e, "Failed to get task status");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Task status request task panicked");
                }
            }
        }
        
        Ok(all_reports)
    }
}
```

### Reconciliation in Scheduler Work Loop

```rust
impl SchedulerWorkLoop {
    /// Query executors to reconcile task status for an orphaned job
    async fn reconcile_task_status(
        &self,
        job_id: &str,
        graph: &mut ExecutionGraph,
    ) -> Result<()> {
        // Request task status from all connected executors via control stream
        let reports = self.executor_registry
            .request_task_status(&[job_id.to_string()], Duration::from_secs(5))
            .await?;
        
        tracing::info!(
            job_id,
            report_count = reports.len(),
            "Received task status reports for orphan recovery"
        );
        
        // Update execution graph with reported statuses
        for report in reports {
            match report.state {
                TaskState::Running => {
                    // Task is still running on an executor
                    // Mark it as running in our graph (it will complete and report back)
                    graph.mark_task_running(
                        report.stage_id as usize,
                        report.partition_id as usize,
                        &report.executor_id,
                    )?;
                }
                TaskState::Completed => {
                    // Task completed successfully
                    if let Some(partitions_bytes) = report.shuffle_write_partitions {
                        let partitions = deserialize_partitions(&partitions_bytes)?;
                        graph.mark_task_completed(
                            report.stage_id as usize,
                            report.partition_id as usize,
                            partitions,
                        )?;
                    }
                }
                TaskState::Failed => {
                    // Task failed - will be rescheduled
                    graph.mark_task_failed(
                        report.stage_id as usize,
                        report.partition_id as usize,
                        report.error_message.as_deref().unwrap_or("unknown error"),
                    )?;
                }
                TaskState::Unknown => {
                    // Ignore unknown state
                }
            }
        }
        
        // Any tasks that were marked as Running in the persisted graph
        // but no executor reported them are considered lost (executor crashed)
        // They will be rescheduled
        graph.mark_unreported_running_tasks_as_pending()?;
        
        Ok(())
    }
}
```

## Ballista JobState Trait Implementation

```rust
/// S3-backed implementation of ballista_scheduler::cluster::JobState
pub struct S3JobState {
    store: Arc<dyn ObjectStore>,
    base_prefix: String,
    scheduler_id: String,
    lease_duration_ms: u64,
    session_builder: SessionBuilder,
    config_producer: SessionConfigProducer,
}

impl S3JobState {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        base_prefix: String,
        scheduler_id: String,
        lease_duration: Duration,
        session_builder: SessionBuilder,
        config_producer: SessionConfigProducer,
    ) -> Self {
        Self {
            store,
            base_prefix,
            scheduler_id,
            lease_duration_ms: lease_duration.as_millis() as u64,
            session_builder,
            config_producer,
        }
    }
    
    fn job_state_path(&self, job_id: &str) -> Path {
        Path::from(format!("{}/ballista/jobs/{}/state.json", self.base_prefix, job_id))
    }
    
    fn job_graph_path(&self, job_id: &str) -> Path {
        Path::from(format!("{}/ballista/jobs/{}/graph.bin", self.base_prefix, job_id))
    }
    
    fn jobs_prefix(&self) -> Path {
        Path::from(format!("{}/ballista/jobs/", self.base_prefix))
    }
}

#[async_trait]
impl JobState for S3JobState {
    async fn accept_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        logical_plan: &[u8],
        queued_at: u64,
    ) -> Result<()> {
        // Implementation from section 1 above
    }
    
    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        let path = self.job_state_path(job_id);
        match self.store.get(&path).await {
            Ok(result) => {
                let state: JobState = serde_json::from_slice(&result.bytes().await?)?;
                Ok(Some(state.status))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    
    async fn get_job(&self, job_id: &str) -> Result<Option<(JobState, Option<ExecutionGraph>)>> {
        let state_path = self.job_state_path(job_id);
        let state: JobState = match self.store.get(&state_path).await {
            Ok(result) => serde_json::from_slice(&result.bytes().await?)?,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        
        // Only load graph if job is running and has been planned
        let graph = if state.status == JobStatus::Running && state.planned_at_ms.is_some() {
            self.load_execution_graph(job_id).await?
        } else {
            None
        };
        
        Ok(Some((state, graph)))
    }
    
    async fn load_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>> {
        let graph_path = self.job_graph_path(job_id);
        match self.store.get(&graph_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let proto = protobuf::ExecutionGraph::decode(&*bytes)?;
                let graph = ExecutionGraph::try_from(proto)?;
                Ok(Some(graph))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    
    // ... other trait methods delegate to the implementations above
}
```

## Integration with Spice Runtime

### Modify cluster/mod.rs

```rust
async fn create_scheduler_server(rt: &Arc<Runtime>) -> crate::Result<(...)> {
    // ... existing setup ...
    
    // Determine job state backend based on configuration
    let job_state: Arc<dyn JobState> = if let Some(scheduler_config) = &app.runtime.scheduler {
        // Cluster mode: use S3-backed shared state
        let state_url = Url::parse(&scheduler_config.state_location)?;
        let (store, base_prefix) = build_object_store(rt, &state_url, scheduler_config).await?;
        
        Arc::new(S3JobState::new(
            store,
            base_prefix,
            scheduler_id.clone(),
            Duration::from_secs(30),  // Default lease duration
            session_builder,
            config_producer,
        ))
    } else {
        // Standalone mode: use in-memory state
        Arc::new(InMemoryJobState::new(
            scheduler_id.clone(),
            session_builder,
            config_producer,
        ))
    };
    
    // Create cluster with the appropriate job state backend
    let cluster = BallistaCluster::new(cluster_state, job_state);
    
    // ... rest of setup ...
}
```

## File Changes Summary

### Ballista (datafusion-ballista)

| File | Changes |
|------|---------|
| `ballista/scheduler/src/cluster/mod.rs` | Modify `JobState` trait to support async accept, claim, save |
| `ballista/scheduler/src/cluster/s3.rs` | NEW: `S3JobState` implementation |
| `ballista/scheduler/src/cluster/memory.rs` | Update `InMemoryJobState` to match new trait |
| `ballista/scheduler/src/state/task_manager.rs` | Integrate with `JobState` for persistence |
| `ballista/scheduler/src/scheduler_server/mod.rs` | Add work loop for claiming, lease renewal |

### Spice (spiceai)

| File | Changes |
|------|---------|
| `crates/runtime-proto/proto/spice.proto` | Add `TaskStatusRequest`, `TaskStatusResponse`, `TaskStatusReport` messages to control stream |
| `crates/runtime/src/cluster/mod.rs` | Wire up `S3JobState` when scheduler configured |
| `crates/runtime/src/cluster/control_stream_client.rs` | Handle `RequestTaskStatus` messages, integrate with `TaskTracker` |
| `crates/runtime/src/cluster/executor_registry.rs` | Add `request_task_status()` method for querying executors via control stream |
| `crates/runtime/src/cluster/task_tracker.rs` | NEW: `TaskTracker` for tracking running/completed tasks on executor |
| `crates/runtime/src/cluster/job_state.rs` | NEW: `S3JobState` implementation |
| `crates/runtime/src/cluster/service.rs` | Handle `TaskStatusResponse` messages from executors |

## Testing Strategy

### Unit Tests

1. **Job state transitions**: Pending → Running → Completed/Failed
2. **Conditional update conflicts**: Simulate concurrent claims
3. **Lease expiration**: Verify orphan detection
4. **Serialization roundtrip**: JobState and ExecutionGraph

### Integration Tests

1. **Multi-scheduler claim race**: Start 3 schedulers, submit 10 jobs, verify each job claimed exactly once
2. **Scheduler failover**: Kill scheduler mid-job, verify another takes over
3. **Task reconciliation**: Kill scheduler, restart, verify task status recovered from executors
4. **Load balancing**: Submit all jobs to one scheduler, verify distribution across cluster

### Chaos Tests

1. **Network partition**: Isolate scheduler from S3, verify lease expires and job taken over
2. **S3 latency injection**: Verify system handles slow S3 operations
3. **Executor failures**: Verify task rescheduling after executor crash

## Performance Considerations

### S3 Operation Costs

| Operation | Frequency | Mitigation |
|-----------|-----------|------------|
| List jobs | Every claim poll (100ms) | Use prefix filtering, pagination |
| Read state | Per claim attempt | Cache locally after claiming |
| Write state | Per task completion | Batch updates, write every N tasks or M seconds |
| Write graph | Per task completion | Same as above |

### Recommended Settings

```yaml
runtime:
  scheduler:
    state_location: s3://bucket/prefix
    params:
      lease_duration: 30s
      lease_renewal_interval: 10s
      orphan_scan_interval: 15s
      claim_poll_interval: 100ms
      save_interval: 5s  # Batch graph saves
      save_batch_size: 10  # Save after N task completions
```

## Future Enhancements

1. **Job priorities**: Add priority field, claim higher priority jobs first
2. **Scheduler affinity**: Prefer claiming jobs submitted to this scheduler
3. **Job queuing limits**: Per-session or per-user job limits
4. **Metrics**: Track claim latency, orphan recovery rate, lease renewal failures
5. **S3 notifications**: Use S3 event notifications instead of polling for new jobs
