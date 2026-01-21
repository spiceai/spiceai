# Cluster Mode Metrics Implementation Plan

## Overview

This document outlines the implementation plan for adding comprehensive OpenTelemetry-based metrics to Spice's cluster mode. Currently, cluster mode has minimal metrics instrumentation (only `runtime_flight_server_started`), while other components have rich observability.

## Goals

1. Provide visibility into cluster health and performance
2. Enable operators to monitor distributed query execution
3. Track task scheduling, execution, and shuffle operations
4. Align with existing Spice metrics patterns (OpenTelemetry + Prometheus exporter)

## Repositories Involved

1. **Spice codebase**: `/Users/phillip/code/spiceai/one`
2. **Ballista fork**: `/Users/phillip/code/apache/datafusion-ballista`
   - Development: Patch `Cargo.toml` to point to local path
   - Production: Push to `spiceai/datafusion-ballista` fork and update revision

---

## Metric Definitions (31 Total)

### Naming Conventions

- `node_*` prefix: Shared metrics (both scheduler and executor record these)
- `scheduler_*` prefix: Scheduler-specific metrics
- `executor_*` prefix: Executor-specific metrics
- `node_id` label: Uses `node_advertise_address`
- No `job_id`/`query_id` labels (high cardinality concern)
- Histogram buckets: Same as existing `DURATION_MS_HISTOGRAM_BUCKETS`

### 1. Node Status (3 metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `node_status` | Gauge | `node_id`, `role` | Node health status (1=healthy, 0=unhealthy) |
| `scheduler_active_executors_count` | Gauge | `node_id` | Number of active executors registered with scheduler |
| `scheduler_count` | Gauge | `node_id` | Number of schedulers in the cluster |

### 2. Task Shared (7 metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `node_tasks_total` | Counter | `node_id`, `role`, `status` | Total tasks processed (status: completed/failed/cancelled) |
| `node_tasks_active` | Gauge | `node_id`, `role` | Currently executing tasks |
| `node_task_duration_ms` | Histogram | `node_id`, `role` | Task execution duration |
| `node_task_failures` | Counter | `node_id`, `role`, `error_type` | Task failures by error type |
| `node_task_retries` | Counter | `node_id`, `role` | Task retry attempts |
| `scheduler_task_queue_depth` | Gauge | `node_id` | Tasks waiting to be scheduled |
| `scheduler_task_scheduling_latency_ms` | Histogram | `node_id` | Time from task ready to scheduled |

### 3. Stage (5 metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `scheduler_stages_total` | Counter | `node_id`, `status` | Total stages processed (status: completed/failed) |
| `scheduler_stage_duration_ms` | Histogram | `node_id` | Stage execution duration (wall-clock) |
| `scheduler_stage_failures` | Counter | `node_id`, `error_type` | Stage failures by error type |
| `scheduler_stage_retries` | Counter | `node_id` | Stage retry attempts |
| `scheduler_tasks_per_stage` | Histogram | `node_id` | Number of tasks per stage |

### 4. Executor (4 metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `executor_tasks_active` | Gauge | `node_id` | Currently executing tasks on executor |
| `executor_tasks_total` | Counter | `node_id`, `status` | Total tasks processed by executor |
| `executor_task_failures` | Counter | `node_id`, `error_type` | Task failures on executor |
| `executor_memory_available_bytes` | Gauge | `node_id` | Available memory on executor |

### 5. Shuffle (6 metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `node_shuffle_write_bytes` | Counter | `node_id` | Bytes written during shuffle |
| `node_shuffle_write_rows` | Counter | `node_id` | Rows written during shuffle |
| `node_shuffle_write_duration_ms` | Histogram | `node_id` | Shuffle write duration |
| `node_shuffle_read_bytes` | Counter | `node_id` | Bytes read during shuffle |
| `node_shuffle_read_rows` | Counter | `node_id` | Rows read during shuffle |
| `node_shuffle_read_duration_ms` | Histogram | `node_id` | Shuffle read duration |

### 6. Scheduler Operations (3 metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `scheduler_job_queue_depth` | Gauge | `node_id` | Jobs waiting in queue |
| `scheduler_planning_duration_ms` | Histogram | `node_id` | Query planning duration |
| `scheduler_executor_assignments` | Counter | `node_id`, `executor_id` | Task assignments per executor |

---

## Implementation Phases

### Phase 1: Ballista Fork - Trait Extensions

Extend the metrics collector traits to support all the metrics we need.

#### File: `ballista/executor/src/metrics/mod.rs`

```rust
// Add new methods to ExecutorMetricsCollector trait
pub trait ExecutorMetricsCollector: Send + Sync {
    /// Record that a task has started execution
    fn record_task_started(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
    );

    /// Record metrics for stage after it is executed (existing method, add duration_ms)
    fn record_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        plan: Arc<dyn QueryStageExecutor>,
        duration_ms: u64,
    );

    /// Record that a task has failed
    fn record_task_failed(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        error_type: &str,
    );

    /// Record shuffle write metrics
    fn record_shuffle_write(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );

    /// Record shuffle read metrics
    fn record_shuffle_read(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );
}
```

#### File: `ballista/scheduler/src/metrics/mod.rs`

```rust
// Add new methods to SchedulerMetricsCollector trait
pub trait SchedulerMetricsCollector: Send + Sync {
    // Existing methods...
    fn record_submitted(&self, job_id: &str, queued_at: u64, submitted_at: u64);
    fn record_completed(&self, job_id: &str, queued_at: u64, completed_at: u64);
    fn record_failed(&self, job_id: &str, queued_at: u64, failed_at: u64);
    fn record_cancelled(&self, job_id: &str);
    fn set_pending_tasks_queue_size(&self, value: u64);
    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>>;

    // New methods for stage lifecycle
    fn record_stage_started(&self, job_id: &str, stage_id: usize, task_count: usize);
    fn record_stage_completed(&self, job_id: &str, stage_id: usize, duration_ms: u64);
    fn record_stage_failed(&self, job_id: &str, stage_id: usize, error_type: &str);
    fn record_stage_retry(&self, job_id: &str, stage_id: usize);

    // New methods for task scheduling
    fn record_task_scheduled(&self, job_id: &str, stage_id: usize, executor_id: &str, latency_ms: u64);
    fn record_task_completed(&self, job_id: &str, stage_id: usize, executor_id: &str);
    fn record_task_failed(&self, job_id: &str, stage_id: usize, executor_id: &str, error_type: &str);

    // New methods for executor management
    fn set_active_executor_count(&self, count: usize);
    fn record_executor_registered(&self, executor_id: &str);
    fn record_executor_deregistered(&self, executor_id: &str);

    // New method for planning
    fn record_planning_duration(&self, job_id: &str, duration_ms: u64);
}
```

### Phase 2: Ballista Fork - Instrumentation

Instrument Ballista internals to call the new trait methods.

#### File: `ballista/executor/src/executor.rs`

Modify `execute_query_stage` to track task start/end times and call new metrics methods:

```rust
pub async fn execute_query_stage(
    &self,
    task_id: usize,
    partition: PartitionId,
    query_stage_exec: Arc<dyn QueryStageExecutor>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
    let start_time = std::time::Instant::now();
    
    // Record task started
    self.metrics_collector.record_task_started(
        &partition.job_id,
        partition.stage_id,
        partition.partition_id,
    );

    let (task, abort_handle) = futures::future::abortable(
        query_stage_exec.execute_query_stage(partition.partition_id, task_ctx),
    );

    self.abort_handles
        .insert((task_id, partition.clone()), abort_handle);

    let result = task.await;
    let duration_ms = start_time.elapsed().as_millis() as u64;

    self.abort_handles.remove(&(task_id, partition.clone()));

    match result {
        Ok(Ok(partitions)) => {
            self.metrics_collector.record_stage(
                &partition.job_id,
                partition.stage_id,
                partition.partition_id,
                query_stage_exec,
                duration_ms,
            );
            Ok(partitions)
        }
        Ok(Err(e)) => {
            self.metrics_collector.record_task_failed(
                &partition.job_id,
                partition.stage_id,
                partition.partition_id,
                &categorize_error(&e),
            );
            Err(e)
        }
        Err(_aborted) => {
            // Task was cancelled
            Err(BallistaError::Cancelled)
        }
    }
}
```

#### File: `ballista/scheduler/src/state/execution_graph.rs`

Add stage lifecycle instrumentation in the stage transition methods:
- `update_task_status` - when tasks complete
- Stage state transitions (UnResolved → Resolved → Running → Successful/Failed)

#### File: `ballista/scheduler/src/state/executor_manager.rs`

Instrument executor registration/deregistration:
- `register_executor` - call `record_executor_registered`
- `remove_executor` - call `record_executor_deregistered`

#### File: `ballista/core/src/execution_plans/shuffle_reader.rs`

Add `ShuffleReadMetrics` to track shuffle read operations:

```rust
/// Metrics for shuffle read operations
#[derive(Debug, Default)]
pub struct ShuffleReadMetrics {
    /// Time spent reading shuffle data
    pub read_time: metrics::Time,
    /// Bytes read from shuffle
    pub bytes_read: metrics::Count,
    /// Rows read from shuffle
    pub rows_read: metrics::Count,
}
```

### Phase 3: Spice - Metric Definitions

Create the metric definitions in the Spice codebase.

#### File: `crates/runtime/src/metrics/cluster/mod.rs` (NEW)

```rust
// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

use crate::metrics::DURATION_MS_HISTOGRAM_BUCKETS;

// Node Status metrics
pub static NODE_STATUS: Lazy<Gauge<i64>> = Lazy::new(|| {
    meter().i64_gauge("node_status")
        .with_description("Node health status (1=healthy, 0=unhealthy)")
        .build()
});

pub static SCHEDULER_ACTIVE_EXECUTORS_COUNT: Lazy<Gauge<u64>> = Lazy::new(|| {
    meter().u64_gauge("scheduler_active_executors_count")
        .with_description("Number of active executors registered with scheduler")
        .build()
});

pub static SCHEDULER_COUNT: Lazy<Gauge<u64>> = Lazy::new(|| {
    meter().u64_gauge("scheduler_count")
        .with_description("Number of schedulers in the cluster")
        .build()
});

// Task metrics
pub static NODE_TASKS_TOTAL: Lazy<Counter<u64>> = Lazy::new(|| {
    meter().u64_counter("node_tasks_total")
        .with_description("Total tasks processed")
        .build()
});

pub static NODE_TASKS_ACTIVE: Lazy<Gauge<i64>> = Lazy::new(|| {
    meter().i64_gauge("node_tasks_active")
        .with_description("Currently executing tasks")
        .build()
});

pub static NODE_TASK_DURATION_MS: Lazy<Histogram<u64>> = Lazy::new(|| {
    meter().u64_histogram("node_task_duration_ms")
        .with_description("Task execution duration in milliseconds")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

// ... (continue for all 31 metrics)

fn meter() -> Meter {
    opentelemetry::global::meter("cluster")
}
```

#### File: `crates/runtime/src/metrics/mod.rs`

Add the new module:

```rust
pub mod cluster;
```

### Phase 4: Spice - Metrics Collectors

Implement the OpenTelemetry-based metrics collectors.

#### File: `crates/runtime/src/cluster/metrics_collector.rs` (NEW)

```rust
// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use ballista_executor::metrics::ExecutorMetricsCollector;
use ballista_scheduler::metrics::SchedulerMetricsCollector;
use ballista_core::error::Result;

use crate::metrics::cluster;

/// OpenTelemetry-based executor metrics collector for Spice cluster mode.
pub struct OtelExecutorMetricsCollector {
    node_id: String,
}

impl OtelExecutorMetricsCollector {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

impl ExecutorMetricsCollector for OtelExecutorMetricsCollector {
    fn record_task_started(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
    ) {
        let attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
            opentelemetry::KeyValue::new("role", "executor"),
        ];
        cluster::NODE_TASKS_ACTIVE.add(1, &attrs);
    }

    fn record_stage(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        _plan: Arc<dyn ballista_executor::execution_engine::QueryStageExecutor>,
        duration_ms: u64,
    ) {
        let attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
            opentelemetry::KeyValue::new("role", "executor"),
        ];
        cluster::NODE_TASKS_ACTIVE.add(-1, &attrs);
        cluster::NODE_TASK_DURATION_MS.record(duration_ms, &attrs);
        
        let completed_attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
            opentelemetry::KeyValue::new("role", "executor"),
            opentelemetry::KeyValue::new("status", "completed"),
        ];
        cluster::NODE_TASKS_TOTAL.add(1, &completed_attrs);
    }

    fn record_task_failed(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        error_type: &str,
    ) {
        let attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
            opentelemetry::KeyValue::new("role", "executor"),
        ];
        cluster::NODE_TASKS_ACTIVE.add(-1, &attrs);
        
        let failed_attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
            opentelemetry::KeyValue::new("role", "executor"),
            opentelemetry::KeyValue::new("status", "failed"),
        ];
        cluster::NODE_TASKS_TOTAL.add(1, &failed_attrs);
        
        let error_attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
            opentelemetry::KeyValue::new("role", "executor"),
            opentelemetry::KeyValue::new("error_type", error_type.to_string()),
        ];
        cluster::NODE_TASK_FAILURES.add(1, &error_attrs);
    }

    fn record_shuffle_write(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        let attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
        ];
        cluster::NODE_SHUFFLE_WRITE_BYTES.add(bytes, &attrs);
        cluster::NODE_SHUFFLE_WRITE_ROWS.add(rows, &attrs);
        cluster::NODE_SHUFFLE_WRITE_DURATION_MS.record(duration_ms, &attrs);
    }

    fn record_shuffle_read(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        let attrs = [
            opentelemetry::KeyValue::new("node_id", self.node_id.clone()),
        ];
        cluster::NODE_SHUFFLE_READ_BYTES.add(bytes, &attrs);
        cluster::NODE_SHUFFLE_READ_ROWS.add(rows, &attrs);
        cluster::NODE_SHUFFLE_READ_DURATION_MS.record(duration_ms, &attrs);
    }
}

/// OpenTelemetry-based scheduler metrics collector for Spice cluster mode.
pub struct OtelSchedulerMetricsCollector {
    node_id: String,
}

impl OtelSchedulerMetricsCollector {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

impl SchedulerMetricsCollector for OtelSchedulerMetricsCollector {
    // Implement all trait methods...
    // (full implementation in actual code)
    
    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        // Return None - Spice uses its own Prometheus endpoint
        Ok(None)
    }
}
```

### Phase 5: Spice - Wire Up Collectors

Connect the new collectors to the cluster initialization code.

#### File: `crates/runtime/src/cluster/mod.rs`

Replace `LoggingMetricsCollector` with `OtelExecutorMetricsCollector` at line ~849:

```rust
// Before
let metrics_collector = Arc::new(LoggingMetricsCollector::default());

// After
let metrics_collector = Arc::new(OtelExecutorMetricsCollector::new(
    node_advertise_address.clone(),
));
```

For scheduler, update `SchedulerConfig` to use `OtelSchedulerMetricsCollector`:

```rust
let scheduler_metrics_collector = Arc::new(OtelSchedulerMetricsCollector::new(
    node_advertise_address.clone(),
));

// Pass to SchedulerConfig (verify exact field name during implementation)
```

### Phase 6: Spice - Status Tracking

#### File: `crates/runtime/src/status.rs`

Add metric recording to `update_cluster()`:

```rust
pub fn update_cluster(&mut self, cluster: ClusterState) {
    // Record metrics
    cluster::SCHEDULER_COUNT.record(
        cluster.scheduler_count as u64,
        &[opentelemetry::KeyValue::new("node_id", self.node_id.clone())],
    );
    
    cluster::SCHEDULER_ACTIVE_EXECUTORS_COUNT.record(
        cluster.executor_count as u64,
        &[opentelemetry::KeyValue::new("node_id", self.node_id.clone())],
    );
    
    // Existing logic...
    self.cluster = Some(cluster);
}
```

#### File: `crates/runtime/src/cluster/scheduler_registry.rs`

Update `scheduler_count` metric when scheduler peers change:

```rust
// In the scheduler registry update logic
cluster::SCHEDULER_COUNT.record(
    self.scheduler_peers.len() as u64,
    &[opentelemetry::KeyValue::new("node_id", self.node_id.clone())],
);
```

### Phase 7: Testing

1. **Unit tests**: Test metric collector implementations
2. **Integration tests**: Verify metrics are emitted during distributed queries
3. **Manual verification**: Check Prometheus endpoint for new metrics

---

## Files to Create/Modify Summary

### Spice Codebase (`/Users/phillip/code/spiceai/one`)

| File | Action | Description |
|------|--------|-------------|
| `crates/runtime/src/metrics/cluster/mod.rs` | CREATE | All 31 metric definitions |
| `crates/runtime/src/metrics/mod.rs` | MODIFY | Add `pub mod cluster;` |
| `crates/runtime/src/cluster/metrics_collector.rs` | CREATE | `OtelExecutorMetricsCollector` and `OtelSchedulerMetricsCollector` |
| `crates/runtime/src/cluster/mod.rs` | MODIFY | Wire up collectors, replace `LoggingMetricsCollector` |
| `crates/runtime/src/cluster/scheduler_registry.rs` | MODIFY | Update `scheduler_count` metric |
| `crates/runtime/src/status.rs` | MODIFY | Add metric recording to `update_cluster()` |
| `Cargo.toml` | MODIFY | Add patch to point Ballista deps to local path (during dev) |

### Ballista Fork (`/Users/phillip/code/apache/datafusion-ballista`)

| File | Action | Description |
|------|--------|-------------|
| `ballista/executor/src/metrics/mod.rs` | MODIFY | Extend `ExecutorMetricsCollector` trait |
| `ballista/executor/src/executor.rs` | MODIFY | Call new trait methods, track task timing |
| `ballista/scheduler/src/metrics/mod.rs` | MODIFY | Extend `SchedulerMetricsCollector` trait |
| `ballista/scheduler/src/state/execution_graph.rs` | MODIFY | Instrument stage transitions |
| `ballista/scheduler/src/state/executor_manager.rs` | MODIFY | Instrument executor registration |
| `ballista/core/src/execution_plans/shuffle_reader.rs` | MODIFY | Add `ShuffleReadMetrics` |

---

## Open Questions

1. **Stage duration calculation**: Should we use wall-clock time from first task start to last task completion, or sum of individual task durations?
   - **Recommendation**: Wall-clock time (more useful for observability)

2. **Shuffle read metrics propagation**: How do shuffle read metrics get back to the executor metrics collector?
   - **Investigation needed**: Check how `ShuffleReaderExec` execution results propagate back

3. **Exact `SchedulerConfig` field name**: Need to verify the field name for passing the metrics collector during implementation

---

## Development Workflow

1. **Patch Cargo.toml** to point Ballista dependencies to local path:
   ```toml
   [patch.crates-io]
   ballista-core = { path = "/Users/phillip/code/apache/datafusion-ballista/ballista/core" }
   ballista-executor = { path = "/Users/phillip/code/apache/datafusion-ballista/ballista/executor" }
   ballista-scheduler = { path = "/Users/phillip/code/apache/datafusion-ballista/ballista/scheduler" }
   ```

2. **Implement Ballista changes** (Phases 1-2)

3. **Implement Spice changes** (Phases 3-6)

4. **Test locally**

5. **Push Ballista changes** to `spiceai/datafusion-ballista` fork

6. **Update Spice Cargo.toml** to point to new Ballista fork revision

7. **Remove patch** and verify build

---

## Success Criteria

- [ ] All 31 metrics are defined and emitting data
- [ ] Prometheus endpoint shows new cluster metrics
- [ ] No performance regression in distributed query execution
- [ ] Metrics align with existing Spice patterns (naming, labels, types)
- [ ] Documentation updated with new metrics

---

## Manual Test Plan for Full Cluster Verification

This section provides a step-by-step guide to manually verify all cluster metrics are working correctly in a multi-node cluster setup.

### Prerequisites

1. **Build spiced with cluster support**:
   ```bash
   make install-dev
   ```

2. **Prepare test data** (e.g., a parquet file or database connector)

3. **Create a test spicepod** (`spicepod.yaml`):
   ```yaml
   version: v1beta1
   kind: Spicepod
   name: cluster_metrics_test
   
   datasets:
     - from: s3://spiceai-demo-datasets/taxi_trips/2024/
       name: taxi_trips
       params:
         file_format: parquet
       acceleration:
         enabled: true
         engine: arrow
   ```

### Test Setup: 3-Node Cluster

Start a 3-node cluster (1 scheduler + 2 executors):

**Terminal 1 - Scheduler Node**:
```bash
spiced --cluster-mode scheduler \
  --http-bind-address 0.0.0.0:8090 \
  --flight-bind-address 0.0.0.0:50051 \
  --cluster-advertise-address 127.0.0.1:50051 \
  --metrics-bind-address 0.0.0.0:9090
```

**Terminal 2 - Executor Node 1**:
```bash
spiced --cluster-mode executor \
  --http-bind-address 0.0.0.0:8091 \
  --flight-bind-address 0.0.0.0:50052 \
  --cluster-advertise-address 127.0.0.1:50052 \
  --cluster-scheduler-address 127.0.0.1:50051 \
  --metrics-bind-address 0.0.0.0:9091
```

**Terminal 3 - Executor Node 2**:
```bash
spiced --cluster-mode executor \
  --http-bind-address 0.0.0.0:8092 \
  --flight-bind-address 0.0.0.0:50053 \
  --cluster-advertise-address 127.0.0.1:50053 \
  --cluster-scheduler-address 127.0.0.1:50051 \
  --metrics-bind-address 0.0.0.0:9092
```

### Test Cases

#### Test 1: Node Status Metrics

**Objective**: Verify `node_status`, `scheduler_active_executors_count`, and `scheduler_count` metrics.

**Steps**:
1. Wait for all nodes to start and connect
2. Query the scheduler's metrics endpoint:
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "^(node_status|scheduler_active_executors_count|scheduler_count)"
   ```

**Expected Results**:
```
# Node status should be 1 (healthy)
node_status{node_id="127.0.0.1:50051",role="scheduler"} 1

# Should show 2 active executors
scheduler_active_executors_count{node_id="127.0.0.1:50051"} 2

# Should show 1 scheduler (or more in HA setup)
scheduler_count{node_id="127.0.0.1:50051"} 1
```

**Verification Checklist**:
- [ ] `node_status` = 1 for scheduler
- [ ] `node_status` = 1 for both executors (check their endpoints)
- [ ] `scheduler_active_executors_count` = 2
- [ ] `scheduler_count` >= 1

---

#### Test 2: Task Execution Metrics (Simple Query)

**Objective**: Verify task-related metrics are recorded during query execution.

**Steps**:
1. Execute a simple query:
   ```bash
   spice sql --host 127.0.0.1 --port 50051 \
     "SELECT COUNT(*) FROM taxi_trips"
   ```

2. Check scheduler metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "^(node_tasks|scheduler_task)"
   ```

3. Check executor metrics:
   ```bash
   curl -s http://localhost:9091/metrics | grep -E "^(node_tasks|executor_tasks)"
   curl -s http://localhost:9092/metrics | grep -E "^(node_tasks|executor_tasks)"
   ```

**Expected Results**:
```
# Scheduler should track scheduled tasks
node_tasks_total{node_id="127.0.0.1:50051",role="scheduler",status="completed"} >= 1
scheduler_task_scheduling_latency_ms_bucket{...} (histogram buckets)

# At least one executor should have executed tasks
node_tasks_total{node_id="127.0.0.1:50052",role="executor",status="completed"} >= 0
node_task_duration_ms_bucket{...} (histogram buckets)
executor_tasks_total{node_id="127.0.0.1:50052",status="completed"} >= 0
```

**Verification Checklist**:
- [ ] `node_tasks_total` with `status="completed"` incremented on scheduler
- [ ] `node_tasks_total` with `status="completed"` incremented on executor(s)
- [ ] `node_task_duration_ms` histogram has observations
- [ ] `scheduler_task_scheduling_latency_ms` histogram has observations
- [ ] `executor_tasks_total` incremented
- [ ] `executor_tasks_active` = 0 after query completes

---

#### Test 3: Stage Metrics (Multi-Stage Query)

**Objective**: Verify stage lifecycle metrics with a query that produces multiple stages.

**Steps**:
1. Execute a query with aggregation and joins (forces multiple stages):
   ```bash
   spice sql --host 127.0.0.1 --port 50051 \
     "SELECT 
        payment_type, 
        COUNT(*) as trip_count, 
        AVG(total_amount) as avg_amount
      FROM taxi_trips 
      GROUP BY payment_type 
      ORDER BY trip_count DESC 
      LIMIT 10"
   ```

2. Check scheduler stage metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "^scheduler_stage"
   ```

**Expected Results**:
```
scheduler_stages_total{node_id="127.0.0.1:50051",status="completed"} >= 1
scheduler_stage_duration_ms_bucket{node_id="127.0.0.1:50051",...} (histogram)
scheduler_tasks_per_stage_bucket{node_id="127.0.0.1:50051",...} (histogram)
```

**Verification Checklist**:
- [ ] `scheduler_stages_total` with `status="completed"` incremented
- [ ] `scheduler_stage_duration_ms` histogram has observations
- [ ] `scheduler_tasks_per_stage` histogram shows task distribution
- [ ] No `scheduler_stage_failures` increments (unless expected)

---

#### Test 4: Shuffle Metrics

**Objective**: Verify shuffle read/write metrics during data exchange between stages.

**Steps**:
1. Execute a query that requires shuffle (e.g., repartitioning or aggregation):
   ```bash
   spice sql --host 127.0.0.1 --port 50051 \
     "SELECT 
        vendor_id,
        COUNT(*) as trips,
        SUM(total_amount) as revenue
      FROM taxi_trips
      GROUP BY vendor_id"
   ```

2. Check shuffle metrics on executors:
   ```bash
   curl -s http://localhost:9091/metrics | grep -E "^node_shuffle"
   curl -s http://localhost:9092/metrics | grep -E "^node_shuffle"
   ```

**Expected Results**:
```
# Shuffle write metrics (written by producer stages)
node_shuffle_write_bytes{node_id="127.0.0.1:50052",role="executor"} > 0
node_shuffle_write_rows{node_id="127.0.0.1:50052",role="executor"} > 0
node_shuffle_write_duration_ms_bucket{...} (histogram)

# Shuffle read metrics (read by consumer stages)
node_shuffle_read_bytes{node_id="127.0.0.1:50052",role="executor"} > 0
node_shuffle_read_rows{node_id="127.0.0.1:50052",role="executor"} > 0
node_shuffle_read_duration_ms_bucket{...} (histogram)
```

**Verification Checklist**:
- [ ] `node_shuffle_write_bytes` > 0 on at least one executor
- [ ] `node_shuffle_write_rows` > 0
- [ ] `node_shuffle_write_duration_ms` histogram has observations
- [ ] `node_shuffle_read_bytes` > 0 on at least one executor
- [ ] `node_shuffle_read_rows` > 0
- [ ] `node_shuffle_read_duration_ms` histogram has observations

---

#### Test 5: Queue Depth Metrics

**Objective**: Verify task and job queue depth metrics under load.

**Steps**:
1. Run multiple concurrent queries to create backlog:
   ```bash
   for i in {1..10}; do
     spice sql --host 127.0.0.1 --port 50051 \
       "SELECT COUNT(*) FROM taxi_trips WHERE trip_distance > $i" &
   done
   wait
   ```

2. During execution, check queue metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "^scheduler_(task_queue|job_queue)"
   ```

**Expected Results**:
```
# Queue depth should be > 0 while queries are pending
scheduler_task_queue_depth{node_id="127.0.0.1:50051"} >= 0
scheduler_job_queue_depth{node_id="127.0.0.1:50051"} >= 0
```

**Verification Checklist**:
- [ ] `scheduler_task_queue_depth` observable during high load
- [ ] `scheduler_job_queue_depth` observable during high load
- [ ] Both return to 0 after all queries complete

---

#### Test 6: Planning Duration Metrics

**Objective**: Verify query planning duration is tracked.

**Steps**:
1. Execute a complex query:
   ```bash
   spice sql --host 127.0.0.1 --port 50051 \
     "SELECT * FROM taxi_trips t1 
      JOIN taxi_trips t2 ON t1.vendor_id = t2.vendor_id 
      WHERE t1.trip_distance > 10 
      LIMIT 100"
   ```

2. Check planning metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep "scheduler_planning_duration"
   ```

**Expected Results**:
```
scheduler_planning_duration_ms_bucket{node_id="127.0.0.1:50051",...} (histogram)
scheduler_planning_duration_ms_sum{node_id="127.0.0.1:50051"} > 0
scheduler_planning_duration_ms_count{node_id="127.0.0.1:50051"} >= 1
```

**Verification Checklist**:
- [ ] `scheduler_planning_duration_ms` histogram has observations
- [ ] Planning duration values are reasonable (< 10s for most queries)

---

#### Test 7: Executor Assignment Metrics

**Objective**: Verify task-to-executor assignment tracking.

**Steps**:
1. Run several queries and check assignment metrics:
   ```bash
   for i in {1..5}; do
     spice sql --host 127.0.0.1 --port 50051 \
       "SELECT COUNT(*) FROM taxi_trips"
   done
   ```

2. Check assignment metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep "scheduler_executor_assignments"
   ```

**Expected Results**:
```
scheduler_executor_assignments{node_id="127.0.0.1:50051"} >= 5
```

**Verification Checklist**:
- [ ] `scheduler_executor_assignments` increments with each task assignment
- [ ] Assignments are distributed across executors (check individual executor metrics)

---

#### Test 8: Executor Memory Metrics

**Objective**: Verify executor memory availability tracking.

**Steps**:
1. Check memory metrics on each executor:
   ```bash
   curl -s http://localhost:9091/metrics | grep "executor_memory_available"
   curl -s http://localhost:9092/metrics | grep "executor_memory_available"
   ```

**Expected Results**:
```
executor_memory_available_bytes{node_id="127.0.0.1:50052"} > 0
executor_memory_available_bytes{node_id="127.0.0.1:50053"} > 0
```

**Verification Checklist**:
- [ ] `executor_memory_available_bytes` > 0 on all executors
- [ ] Memory decreases during query execution (if observable)
- [ ] Memory recovers after query completion

---

#### Test 9: Error Handling Metrics

**Objective**: Verify failure and retry metrics work correctly.

**Steps**:
1. Execute a query that will fail (e.g., invalid SQL or missing table):
   ```bash
   spice sql --host 127.0.0.1 --port 50051 \
     "SELECT * FROM nonexistent_table" 2>/dev/null || true
   ```

2. Check failure metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "(failures|retries)"
   ```

**Expected Results**:
```
# Depending on where the error occurs:
node_task_failures{node_id="...",role="scheduler",error_type="..."} >= 0
scheduler_stage_failures{node_id="...",error_type="..."} >= 0
```

**Verification Checklist**:
- [ ] `node_task_failures` increments for task-level failures
- [ ] `scheduler_stage_failures` increments for stage-level failures
- [ ] `error_type` label provides useful categorization
- [ ] Retry metrics increment when retries occur

---

#### Test 10: Executor Disconnect/Reconnect

**Objective**: Verify metrics update when executors join/leave the cluster.

**Steps**:
1. Note current `scheduler_active_executors_count`:
   ```bash
   curl -s http://localhost:9090/metrics | grep "scheduler_active_executors_count"
   ```

2. Stop Executor 2 (Ctrl+C in Terminal 3)

3. Check metrics again:
   ```bash
   curl -s http://localhost:9090/metrics | grep "scheduler_active_executors_count"
   ```

4. Restart Executor 2

5. Check metrics one more time:
   ```bash
   curl -s http://localhost:9090/metrics | grep "scheduler_active_executors_count"
   ```

**Expected Results**:
```
# Before stopping executor
scheduler_active_executors_count{node_id="127.0.0.1:50051"} 2

# After stopping executor
scheduler_active_executors_count{node_id="127.0.0.1:50051"} 1

# After restarting executor
scheduler_active_executors_count{node_id="127.0.0.1:50051"} 2
```

**Verification Checklist**:
- [ ] `scheduler_active_executors_count` decreases when executor stops
- [ ] `scheduler_active_executors_count` increases when executor reconnects
- [ ] Node status reflects unhealthy state appropriately

---

### Complete Metrics Verification Script

Run this script to verify all metrics are present and have the correct types:

```bash
#!/bin/bash
# verify_cluster_metrics.sh

SCHEDULER_URL="http://localhost:9090/metrics"
EXECUTOR1_URL="http://localhost:9091/metrics"
EXECUTOR2_URL="http://localhost:9092/metrics"

echo "=== Checking Scheduler Metrics ==="
curl -s $SCHEDULER_URL | grep -E "^(node_status|scheduler_|node_tasks|node_task_)" | head -50

echo ""
echo "=== Checking Executor 1 Metrics ==="
curl -s $EXECUTOR1_URL | grep -E "^(node_status|executor_|node_tasks|node_task_|node_shuffle)" | head -50

echo ""
echo "=== Checking Executor 2 Metrics ==="
curl -s $EXECUTOR2_URL | grep -E "^(node_status|executor_|node_tasks|node_task_|node_shuffle)" | head -50

echo ""
echo "=== Expected Metrics Summary ==="
echo "Node Status (3 metrics):"
echo "  - node_status"
echo "  - scheduler_active_executors_count"
echo "  - scheduler_count"

echo ""
echo "Task Metrics (7 metrics):"
echo "  - node_tasks_total"
echo "  - node_tasks_active"
echo "  - node_task_duration_ms"
echo "  - node_task_failures"
echo "  - node_task_retries"
echo "  - scheduler_task_queue_depth"
echo "  - scheduler_task_scheduling_latency_ms"

echo ""
echo "Stage Metrics (5 metrics):"
echo "  - scheduler_stages_total"
echo "  - scheduler_stage_duration_ms"
echo "  - scheduler_stage_failures"
echo "  - scheduler_stage_retries"
echo "  - scheduler_tasks_per_stage"

echo ""
echo "Executor Metrics (4 metrics):"
echo "  - executor_tasks_active"
echo "  - executor_tasks_total"
echo "  - executor_task_failures"
echo "  - executor_memory_available_bytes"

echo ""
echo "Shuffle Metrics (6 metrics):"
echo "  - node_shuffle_write_bytes"
echo "  - node_shuffle_write_rows"
echo "  - node_shuffle_write_duration_ms"
echo "  - node_shuffle_read_bytes"
echo "  - node_shuffle_read_rows"
echo "  - node_shuffle_read_duration_ms"

echo ""
echo "Scheduler Operations (3 metrics):"
echo "  - scheduler_job_queue_depth"
echo "  - scheduler_planning_duration_ms"
echo "  - scheduler_executor_assignments"
```

### Troubleshooting

**Metrics not appearing**:
1. Ensure cluster mode is enabled (`--cluster-mode`)
2. Check that metrics endpoint is bound (`--metrics-bind-address`)
3. Verify the metrics collector is wired up in code

**Metrics have unexpected values**:
1. Check `node_id` labels match expected addresses
2. Verify role labels (`scheduler` vs `executor`)
3. Check histogram bucket boundaries match expectations

**Shuffle metrics missing**:
1. Ensure query produces shuffle (GROUP BY, JOIN, etc.)
2. Check multiple executors are running (shuffle requires data exchange)
3. Verify shuffle read extraction is working in Ballista
