# Shuffle Affinity Metrics Implementation Plan

## Overview

This document outlines the implementation plan for adding shuffle locality/affinity metrics to Spice's cluster mode. These metrics enable operators to understand and tune shuffle performance by measuring:

1. **Local vs Remote shuffle reads**: How often executors read shuffle data from local disk vs fetching from remote executors
2. **Shuffle affinity**: Whether the scheduler assigns tasks to executors that already have the required shuffle data locally
3. **Network overhead**: The latency cost of remote shuffle reads

## Problem Statement

When executing distributed queries, Ballista breaks execution into stages separated by shuffle boundaries. Shuffle data written by one stage must be read by subsequent stages. Two performance concerns arise:

1. **Shuffle volume**: Too many stages/shuffles creates overhead from writing/reading shuffle data to/from disk
2. **Shuffle locality**: When an executor needs shuffle data from another executor (remote read), it incurs network latency

Currently, we have aggregate shuffle metrics (`node_shuffle_read_bytes`, `node_shuffle_read_duration_ms`) but cannot distinguish:
- Local reads (shuffle file exists on the same executor)
- Remote reads (must fetch from another executor over the network)
- Whether the scheduler is making locality-aware task assignments

## Goals

1. Measure shuffle locality rate (local vs remote reads)
2. Track scheduler task assignment affinity (does it prefer executors with local shuffle data?)
3. Quantify network overhead from remote shuffle reads
4. Enable tuning of shuffle partitioning and task placement strategies

## Proposed Metrics

### Executor-Side Metrics (7 new metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `executor_shuffle_read_local_bytes` | Counter | `node_id` | Bytes read from local shuffle files |
| `executor_shuffle_read_local_rows` | Counter | `node_id` | Rows read from local shuffle files |
| `executor_shuffle_read_local_count` | Counter | `node_id` | Number of local shuffle partition reads |
| `executor_shuffle_read_local_duration_ms` | Histogram | `node_id` | Duration of local shuffle reads |
| `executor_shuffle_read_remote_bytes` | Counter | `node_id` | Bytes read from remote executors |
| `executor_shuffle_read_remote_rows` | Counter | `node_id` | Rows read from remote executors |
| `executor_shuffle_read_remote_count` | Counter | `node_id` | Number of remote shuffle partition reads |

Note: Remote duration is already captured by existing `node_shuffle_read_duration_ms` when we split local/remote. We'll keep the existing metric for backwards compatibility and add the split metrics.

### Scheduler-Side Metrics (2 new metrics)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `scheduler_task_shuffle_affinity_hit` | Counter | `node_id` | Tasks assigned to executor with local shuffle data |
| `scheduler_task_shuffle_affinity_miss` | Counter | `node_id` | Tasks assigned to executor without local shuffle data |

### Derived Metrics (calculated from above)

| Metric | Formula | Description |
|--------|---------|-------------|
| Shuffle Locality Rate | `local_count / (local_count + remote_count)` | % of shuffle reads from local disk |
| Shuffle Affinity Rate | `hit / (hit + miss)` | % of tasks with shuffle affinity |
| Remote Read Overhead | `remote_duration - local_duration` | Network latency penalty |
| Network Shuffle Volume | `remote_bytes / (local_bytes + remote_bytes)` | % of shuffle data transferred over network |

---

## Implementation Details

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Scheduler                                    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Task Assignment Logic                                        │   │
│  │                                                              │   │
│  │  For each task to schedule:                                  │   │
│  │    1. Get input shuffle partition locations                  │   │
│  │    2. For selected executor, check if any input partition    │   │
│  │       has executor_meta.id == selected_executor.id           │   │
│  │    3. Record affinity_hit or affinity_miss                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  scheduler_task_shuffle_affinity_hit++                              │
│  scheduler_task_shuffle_affinity_miss++                             │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ Task Assignment
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Executor                                     │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ ShuffleReaderExec::execute()                                 │   │
│  │                                                              │   │
│  │  partition_locations.iter() {                                │   │
│  │    if check_is_local_location(location) {                    │   │
│  │      // Local read - file exists on this executor            │   │
│  │      fetch_partition_local()                                 │   │
│  │      record_shuffle_read_local(bytes, rows, duration)        │   │
│  │    } else {                                                  │   │
│  │      // Remote read - fetch from another executor            │   │
│  │      fetch_partition_remote()                                │   │
│  │      record_shuffle_read_remote(bytes, rows, duration)       │   │
│  │    }                                                         │   │
│  │  }                                                           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  node_shuffle_read_local_bytes++                                    │
│  node_shuffle_read_remote_bytes++                                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Ballista Fork - Executor Metrics Trait Extension

### File: `ballista/executor/src/metrics/mod.rs`

Add new methods to `ExecutorMetricsCollector` trait:

```rust
pub trait ExecutorMetricsCollector: Send + Sync {
    // ... existing methods ...

    /// Record local shuffle read metrics (data read from local disk).
    ///
    /// Called when shuffle data is read from a local file (the partition
    /// was written by this same executor in a previous stage).
    fn record_shuffle_read_local(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );

    /// Record remote shuffle read metrics (data fetched from another executor).
    ///
    /// Called when shuffle data must be fetched over the network from
    /// another executor that produced the partition.
    fn record_shuffle_read_remote(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        source_executor_id: &str,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    );
}
```

Update `LoggingMetricsCollector` with default implementations:

```rust
impl ExecutorMetricsCollector for LoggingMetricsCollector {
    // ... existing implementations ...

    fn record_shuffle_read_local(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Local shuffle read: \
             {bytes} bytes, {rows} rows in {duration_ms}ms ==="
        );
    }

    fn record_shuffle_read_remote(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        source_executor_id: &str,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        info!(
            "=== [{job_id}/{stage_id}/{partition}] Remote shuffle read from {source_executor_id}: \
             {bytes} bytes, {rows} rows in {duration_ms}ms ==="
        );
    }
}
```

---

## Phase 2: Ballista Fork - Scheduler Metrics Trait Extension

### File: `ballista/scheduler/src/metrics/mod.rs`

Add new methods to `SchedulerMetricsCollector` trait:

```rust
pub trait SchedulerMetricsCollector: Send + Sync {
    // ... existing methods ...

    /// Record that a task was assigned to an executor that has local shuffle data.
    ///
    /// Called when the scheduler assigns a task to an executor and at least one
    /// of the task's input shuffle partitions is already present on that executor.
    fn record_task_shuffle_affinity_hit(&self, job_id: &str, stage_id: usize, executor_id: &str);

    /// Record that a task was assigned to an executor without local shuffle data.
    ///
    /// Called when the scheduler assigns a task to an executor and none of the
    /// task's input shuffle partitions are present on that executor.
    fn record_task_shuffle_affinity_miss(&self, job_id: &str, stage_id: usize, executor_id: &str);
}
```

Update `NoopMetricsCollector`:

```rust
impl SchedulerMetricsCollector for NoopMetricsCollector {
    // ... existing implementations ...

    fn record_task_shuffle_affinity_hit(&self, _job_id: &str, _stage_id: usize, _executor_id: &str) {}
    fn record_task_shuffle_affinity_miss(&self, _job_id: &str, _stage_id: usize, _executor_id: &str) {}
}
```

---

## Phase 3: Ballista Fork - Shuffle Reader Instrumentation

### File: `ballista/core/src/execution_plans/shuffle_reader.rs`

The key insight is that `send_fetch_partitions()` already splits local and remote reads via `local_remote_read_split()`. We need to:

1. Track metrics separately for local vs remote reads
2. Pass the metrics collector through to record the results

**Option A: Callback-based approach** (recommended)

Add a callback mechanism to report shuffle read metrics:

```rust
/// Callback for reporting shuffle read metrics
pub trait ShuffleReadMetricsCallback: Send + Sync {
    fn on_local_read(&self, partition: &PartitionLocation, bytes: u64, rows: u64, duration_ms: u64);
    fn on_remote_read(&self, partition: &PartitionLocation, bytes: u64, rows: u64, duration_ms: u64);
}

/// No-op implementation for when metrics are not needed
pub struct NoopShuffleReadMetricsCallback;
impl ShuffleReadMetricsCallback for NoopShuffleReadMetricsCallback {
    fn on_local_read(&self, _: &PartitionLocation, _: u64, _: u64, _: u64) {}
    fn on_remote_read(&self, _: &PartitionLocation, _: u64, _: u64, _: u64) {}
}
```

Modify `send_fetch_partitions()` to accept and use the callback:

```rust
fn send_fetch_partitions(
    partition_locations: Vec<PartitionLocation>,
    max_request_num: usize,
    max_message_size: usize,
    force_remote_read: bool,
    flight_transport: bool,
    customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    use_tls: bool,
    metrics_callback: Arc<dyn ShuffleReadMetricsCallback>,  // NEW
) -> AbortableReceiverStream {
    // ... existing setup ...

    let (local_locations, remote_locations) =
        local_remote_read_split(partition_locations, force_remote_read);

    // Local reads - track metrics
    let metrics_callback_local = metrics_callback.clone();
    spawned_tasks.push(SpawnedTask::spawn(async move {
        for p in local_locations {
            let start = std::time::Instant::now();
            let r = PartitionReaderEnum::Local
                .fetch_partition(&p, /* ... */)
                .await;
            
            if r.is_ok() {
                // TODO: Get actual bytes/rows from the stream
                // For now, use partition_stats if available
                let bytes = p.partition_stats.num_bytes.unwrap_or(0);
                let rows = p.partition_stats.num_rows.unwrap_or(0);
                let duration_ms = start.elapsed().as_millis() as u64;
                metrics_callback_local.on_local_read(&p, bytes, rows, duration_ms);
            }
            
            if let Err(e) = response_sender_c.send(r).await {
                error!("Fail to send response: {e}");
            }
        }
    }));

    // Remote reads - track metrics
    for p in remote_locations.into_iter() {
        let metrics_callback_remote = metrics_callback.clone();
        spawned_tasks.push(SpawnedTask::spawn(async move {
            let start = std::time::Instant::now();
            let permit = semaphore.acquire_owned().await.unwrap();
            let r = PartitionReaderEnum::FlightRemote
                .fetch_partition(&p, /* ... */)
                .await;
            
            if r.is_ok() {
                let bytes = p.partition_stats.num_bytes.unwrap_or(0);
                let rows = p.partition_stats.num_rows.unwrap_or(0);
                let duration_ms = start.elapsed().as_millis() as u64;
                metrics_callback_remote.on_remote_read(&p, bytes, rows, duration_ms);
            }
            
            if let Err(e) = response_sender.send(r).await {
                error!("Fail to send response: {e}");
            }
            drop(permit);
        }));
    }

    AbortableReceiverStream::create(response_receiver, spawned_tasks)
}
```

**Challenge**: The `ShuffleReaderExec::execute()` method doesn't have access to the executor's metrics collector. We need to pass it through the `TaskContext` or via session configuration.

**Solution**: Add an extension trait for `SessionConfig` to store the metrics callback:

```rust
// In ballista-core/src/config.rs or similar
pub trait SessionConfigShuffleMetricsExt {
    fn with_shuffle_metrics_callback(
        self,
        callback: Arc<dyn ShuffleReadMetricsCallback>,
    ) -> Self;
    
    fn shuffle_metrics_callback(&self) -> Option<Arc<dyn ShuffleReadMetricsCallback>>;
}
```

Then in the executor, when creating the `TaskContext`, attach the metrics callback:

```rust
// In executor.rs, when setting up TaskContext for a task
let metrics_callback = Arc::new(ExecutorShuffleMetricsCallback::new(
    self.metrics_collector.clone(),
    partition.job_id.clone(),
    partition.stage_id,
));
let session_config = session_config.with_shuffle_metrics_callback(metrics_callback);
```

---

## Phase 4: Ballista Fork - Scheduler Affinity Tracking

### File: `ballista/scheduler/src/state/task_manager.rs` or `execution_graph.rs`

The scheduler assigns tasks to executors. When assigning a task that reads from shuffle (has `UnresolvedShuffleExec` inputs that have been resolved to `ShuffleReaderExec`), we can check if any of the input partition locations match the assigned executor.

Find the task assignment code path and add affinity checking:

```rust
// Pseudocode - exact location TBD during implementation
fn assign_task_to_executor(
    &self,
    task: &TaskDescription,
    executor_id: &str,
    metrics_collector: &Arc<dyn SchedulerMetricsCollector>,
) {
    // Get the input shuffle partition locations for this task
    let input_partitions = self.get_task_input_partitions(task);
    
    // Check if any input partition is on the assigned executor
    let has_local_shuffle = input_partitions.iter().any(|p| {
        p.executor_meta.id == executor_id
    });
    
    if has_local_shuffle {
        metrics_collector.record_task_shuffle_affinity_hit(
            &task.partition_id.job_id,
            task.partition_id.stage_id,
            executor_id,
        );
    } else if !input_partitions.is_empty() {
        // Only count as miss if there are input shuffles
        metrics_collector.record_task_shuffle_affinity_miss(
            &task.partition_id.job_id,
            task.partition_id.stage_id,
            executor_id,
        );
    }
    // Tasks without shuffle inputs don't count toward affinity metrics
    
    // ... continue with actual task assignment ...
}
```

---

## Phase 5: Spice - Metric Definitions

### File: `crates/runtime/src/metrics/cluster/mod.rs`

Add new metric definitions:

```rust
// =============================================================================
// Shuffle Locality Metrics
// =============================================================================

/// Bytes read from local shuffle files.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_LOCAL_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_local_bytes")
        .with_description("Bytes read from local shuffle files (no network transfer).")
        .with_unit("By")
        .build()
});

/// Rows read from local shuffle files.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_LOCAL_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_local_rows")
        .with_description("Rows read from local shuffle files.")
        .with_unit("rows")
        .build()
});

/// Number of local shuffle partition reads.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_LOCAL_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_local_count")
        .with_description("Number of shuffle partitions read from local disk.")
        .with_unit("partitions")
        .build()
});

/// Duration of local shuffle read operations.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_LOCAL_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("node_shuffle_read_local_duration_ms")
        .with_description("Duration of local shuffle read operations in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Bytes read from remote executors.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_REMOTE_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_remote_bytes")
        .with_description("Bytes read from remote executors (network transfer).")
        .with_unit("By")
        .build()
});

/// Rows read from remote executors.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_REMOTE_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_remote_rows")
        .with_description("Rows read from remote executors.")
        .with_unit("rows")
        .build()
});

/// Number of remote shuffle partition reads.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_REMOTE_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_remote_count")
        .with_description("Number of shuffle partitions read from remote executors.")
        .with_unit("partitions")
        .build()
});

// =============================================================================
// Scheduler Affinity Metrics
// =============================================================================

/// Tasks assigned to executor with local shuffle data.
/// Labels: node_id
pub(crate) static SCHEDULER_TASK_SHUFFLE_AFFINITY_HIT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_task_shuffle_affinity_hit")
        .with_description("Tasks assigned to an executor that has local shuffle data.")
        .with_unit("tasks")
        .build()
});

/// Tasks assigned to executor without local shuffle data.
/// Labels: node_id
pub(crate) static SCHEDULER_TASK_SHUFFLE_AFFINITY_MISS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_task_shuffle_affinity_miss")
        .with_description("Tasks assigned to an executor without local shuffle data.")
        .with_unit("tasks")
        .build()
});
```

Add helper functions:

```rust
/// Record local shuffle read metrics.
pub fn record_shuffle_read_local(node_id: &str, role: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_SHUFFLE_READ_LOCAL_BYTES.add(bytes, &labels);
    NODE_SHUFFLE_READ_LOCAL_ROWS.add(rows, &labels);
    NODE_SHUFFLE_READ_LOCAL_COUNT.add(1, &labels);
    NODE_SHUFFLE_READ_LOCAL_DURATION_MS.record(duration_ms, &labels);
}

/// Record remote shuffle read metrics.
pub fn record_shuffle_read_remote(node_id: &str, role: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_SHUFFLE_READ_REMOTE_BYTES.add(bytes, &labels);
    NODE_SHUFFLE_READ_REMOTE_ROWS.add(rows, &labels);
    NODE_SHUFFLE_READ_REMOTE_COUNT.add(1, &labels);
    // Note: duration for remote reads includes network time
}

/// Record shuffle affinity hit.
pub fn record_shuffle_affinity_hit(node_id: &str) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_TASK_SHUFFLE_AFFINITY_HIT.add(1, &labels);
}

/// Record shuffle affinity miss.
pub fn record_shuffle_affinity_miss(node_id: &str) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_TASK_SHUFFLE_AFFINITY_MISS.add(1, &labels);
}
```

---

## Phase 6: Spice - Metrics Collector Implementation

### File: `crates/runtime/src/cluster/metrics_collector.rs`

Update `OtelExecutorMetricsCollector`:

```rust
impl ExecutorMetricsCollector for OtelExecutorMetricsCollector {
    // ... existing implementations ...

    fn record_shuffle_read_local(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        #[expect(clippy::cast_precision_loss)]
        let duration_ms_f64 = duration_ms as f64;
        cluster::record_shuffle_read_local(&self.node_id, "executor", bytes, rows, duration_ms_f64);
    }

    fn record_shuffle_read_remote(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        _source_executor_id: &str,
        bytes: u64,
        rows: u64,
        duration_ms: u64,
    ) {
        #[expect(clippy::cast_precision_loss)]
        let duration_ms_f64 = duration_ms as f64;
        cluster::record_shuffle_read_remote(&self.node_id, "executor", bytes, rows, duration_ms_f64);
    }
}
```

Update `OtelSchedulerMetricsCollector`:

```rust
impl SchedulerMetricsCollector for OtelSchedulerMetricsCollector {
    // ... existing implementations ...

    fn record_task_shuffle_affinity_hit(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _executor_id: &str,
    ) {
        cluster::record_shuffle_affinity_hit(&self.node_id);
    }

    fn record_task_shuffle_affinity_miss(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _executor_id: &str,
    ) {
        cluster::record_shuffle_affinity_miss(&self.node_id);
    }
}
```

---

## Files to Create/Modify Summary

### Ballista Fork (`/Users/phillip/code/apache/datafusion-ballista`)

| File | Action | Description |
|------|--------|-------------|
| `ballista/executor/src/metrics/mod.rs` | MODIFY | Add `record_shuffle_read_local` and `record_shuffle_read_remote` to trait |
| `ballista/core/src/execution_plans/shuffle_reader.rs` | MODIFY | Add metrics callback, instrument local/remote reads |
| `ballista/core/src/config.rs` | MODIFY | Add session config extension for metrics callback |
| `ballista/executor/src/executor.rs` | MODIFY | Wire up shuffle metrics callback to TaskContext |
| `ballista/scheduler/src/metrics/mod.rs` | MODIFY | Add `record_task_shuffle_affinity_hit/miss` to trait |
| `ballista/scheduler/src/state/task_manager.rs` | MODIFY | Add affinity checking during task assignment |

### Spice Codebase (`/Users/phillip/code/spiceai/one`)

| File | Action | Description |
|------|--------|-------------|
| `crates/runtime/src/metrics/cluster/mod.rs` | MODIFY | Add 9 new metric definitions and helper functions |
| `crates/runtime/src/cluster/metrics_collector.rs` | MODIFY | Implement new trait methods |

---

## Testing

### Unit Tests

1. Test metric collector implementations record correct values
2. Test local/remote split logic in shuffle reader
3. Test affinity detection logic in scheduler

### Integration Tests

1. Run multi-stage query and verify:
   - `node_shuffle_read_local_count` + `node_shuffle_read_remote_count` > 0
   - `scheduler_task_shuffle_affinity_hit` + `scheduler_task_shuffle_affinity_miss` > 0

2. Run query on single executor cluster:
   - All reads should be local (`node_shuffle_read_remote_count` = 0)
   - All affinity should be hits (`scheduler_task_shuffle_affinity_miss` = 0)

### Manual Verification

```bash
# After running a distributed query with shuffles
curl -s http://localhost:9090/metrics | grep -E "shuffle_read_(local|remote)|shuffle_affinity"
```

Expected output:
```
node_shuffle_read_local_bytes{node_id="...",role="executor"} 12345
node_shuffle_read_local_rows{node_id="...",role="executor"} 1000
node_shuffle_read_local_count{node_id="...",role="executor"} 4
node_shuffle_read_remote_bytes{node_id="...",role="executor"} 5678
node_shuffle_read_remote_rows{node_id="...",role="executor"} 500
node_shuffle_read_remote_count{node_id="...",role="executor"} 2
scheduler_task_shuffle_affinity_hit{node_id="..."} 8
scheduler_task_shuffle_affinity_miss{node_id="..."} 4
```

---

## Implementation Status

### Completed

**Phase 1: Ballista Executor Metrics Trait Extension** ✅
- Added `record_shuffle_read_local()` and `record_shuffle_read_remote()` to `ExecutorMetricsCollector` trait
- Updated `LoggingMetricsCollector` with logging implementations
- File: `ballista/executor/src/metrics/mod.rs`

**Phase 2: Ballista Scheduler Metrics Trait Extension** ✅
- Added `record_task_shuffle_affinity_hit()` and `record_task_shuffle_affinity_miss()` to `SchedulerMetricsCollector` trait
- Updated `NoopMetricsCollector` with no-op implementations
- File: `ballista/scheduler/src/metrics/mod.rs`

**Phase 3: Ballista Shuffle Reader Instrumentation** ✅
- Created `ShuffleReadMetricsCallback` trait in `ballista/core/src/extension.rs`
- Added `ShuffleReadMetricsCallbackExtension` for session config storage
- Added `SessionConfigExt` methods: `with_ballista_shuffle_read_metrics_callback()` and `ballista_shuffle_read_metrics_callback()`
- Instrumented `send_fetch_partitions()` in shuffle reader to call callback with timing/size data for both local and remote reads
- File: `ballista/core/src/execution_plans/shuffle_reader.rs`

**Phase 5: Spice Metric Definitions** ✅
- Added 7 new executor-side shuffle locality metrics (local/remote bytes, rows, count, duration)
- Added helper functions: `record_shuffle_read_local()`, `record_shuffle_read_remote()`
- File: `crates/runtime/src/metrics/cluster/mod.rs`

**Phase 6: Spice Metrics Collector Implementation** ✅
- Implemented `record_shuffle_read_local()` and `record_shuffle_read_remote()` in `OtelExecutorMetricsCollector`
- Added stub implementations for scheduler affinity methods (see note below)
- Created `OtelShuffleReadMetricsCallback` struct implementing `ShuffleReadMetricsCallback` trait
- Wired up `OtelShuffleReadMetricsCallback` in executor `ConfigProducer` via `with_ballista_shuffle_read_metrics_callback()`
- File: `crates/runtime/src/cluster/metrics_collector.rs`, `crates/runtime/src/cluster/mod.rs`

### Deferred

**Phase 4: Scheduler Affinity Tracking** ⏸️
- The scheduler-side affinity hit/miss tracking was deferred because Ballista's scheduler does not currently implement locality-aware task scheduling
- The trait methods exist but are not called by the scheduler
- This can be implemented in a future enhancement when locality-aware scheduling is added

### Not Yet Completed

**Scheduler Affinity Metrics** (2 metrics)
- `scheduler_task_shuffle_affinity_hit` - metric defined but not emitted (scheduler doesn't track this)
- `scheduler_task_shuffle_affinity_miss` - metric defined but not emitted (scheduler doesn't track this)

### Remaining Work

1. **Commit Ballista changes** - Push to spiceai/datafusion-ballista fork and update Spice's Cargo.toml to point to the new git revision
2. **Testing** - Run cluster mode queries with shuffles and verify metrics are emitted
3. **Documentation** - Update user-facing docs with new metrics

---

## Success Criteria

- [x] All 7 executor-side shuffle locality metrics defined
- [x] Local vs remote shuffle reads correctly distinguished in shuffle reader
- [x] Metrics callback wired up in executor session config
- [ ] Scheduler affinity tracking implemented (deferred - scheduler doesn't do locality-aware scheduling)
- [ ] Prometheus endpoint shows new metrics (needs testing)
- [ ] Can calculate shuffle locality rate from metrics (needs testing)
- [ ] No performance regression in distributed query execution (needs testing)

---

## Future Enhancements

1. **Locality-aware scheduling**: Use affinity metrics to implement a scheduler that prefers executors with local shuffle data
2. **Shuffle data caching**: Cache recently-read remote shuffle data to avoid repeated network transfers
3. **Adaptive partitioning**: Adjust shuffle partition count based on observed shuffle volume metrics
