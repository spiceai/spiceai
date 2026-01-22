# Ballista Metrics Instrumentation Plan

## Overview

This document tracks the implementation of comprehensive metrics instrumentation in Ballista's executor and scheduler components for Spice's cluster mode.

## Status Summary

**All core metrics are now instrumented!** The implementation is complete in Ballista commit `683e793e31913e845ab464d48504b6a7be6ce89d`.

---

## Implementation Status

### Executor Metrics (`ExecutorMetricsCollector`)

| Method | Status | Location |
|--------|--------|----------|
| `record_task_started` | ✅ Complete | `executor.rs:329` |
| `record_stage` | ✅ Complete | `executor.rs:384` |
| `record_task_failed` | ✅ Complete | `executor.rs:394` |
| `record_shuffle_write` | ✅ Complete | `executor.rs:353` |
| `record_shuffle_read` | ✅ Complete | Generic callback for trait compatibility |
| `record_shuffle_read_local` | ✅ Complete | Via `ShuffleReadMetricsCallback` in shuffle reader |
| `record_shuffle_read_remote` | ✅ Complete | Via `ShuffleReadMetricsCallback` in shuffle reader |
| `record_memory_available` | ✅ Complete | Heartbeat loop in executor server |

### Scheduler Metrics (`SchedulerMetricsCollector`)

| Method | Status | Location |
|--------|--------|----------|
| `record_submitted` | ✅ Complete | `query_stage_scheduler.rs` - job submission |
| `record_completed` | ✅ Complete | `query_stage_scheduler.rs` - job completion event |
| `record_failed` | ✅ Complete | `query_stage_scheduler.rs` - job failure event |
| `record_cancelled` | ✅ Complete | `query_stage_scheduler.rs:223` |
| `set_pending_tasks_queue_size` | ✅ Complete | `state/mod.rs` - after task state changes |
| `set_pending_jobs_queue_size` | ✅ Complete | `state/mod.rs` - after job state changes |
| `record_stage_started` | ✅ Complete | `state/mod.rs` - `processing_stages_update()` |
| `record_stage_completed` | ✅ Complete | `state/mod.rs` - `processing_stages_update()` |
| `record_stage_failed` | ✅ Complete | `state/mod.rs` - `processing_stages_update()` |
| `record_stage_retry` | ✅ Complete | `state/mod.rs` - `processing_stages_update()` |
| `record_task_scheduled` | ✅ Complete | `state/mod.rs` - `update_task_status()` |
| `record_task_completed` | ✅ Complete | `state/mod.rs` - `update_task_status()` |
| `record_task_failed` | ✅ Complete | `state/mod.rs` - `update_task_status()` |
| `record_task_retry` | ✅ Complete | `state/mod.rs` - `update_task_status()` |
| `record_task_shuffle_affinity_hit` | ✅ Complete | `state/mod.rs` - `revive_offers()`, `grpc.rs` - `poll_work()` |
| `record_task_shuffle_affinity_miss` | ✅ Complete | `state/mod.rs` - `revive_offers()`, `grpc.rs` - `poll_work()` |
| `set_active_executor_count` | ✅ Complete | `executor_manager.rs` - registration/deregistration |
| `record_executor_registered` | ✅ Complete | `executor_manager.rs` |
| `record_executor_deregistered` | ✅ Complete | `executor_manager.rs` |
| `record_planning_duration` | ✅ Complete | `query_stage_scheduler.rs` - after planning |

---

## Key Implementation Details

### Shuffle Locality Metrics (Executor-side)

The executor tracks whether shuffle reads are local (from disk) or remote (fetched from another executor) via callbacks:

1. **`ShuffleReadMetricsCallback`** trait in `ballista-core/src/extension.rs`:
   - `record_local_read()` - called when reading from local disk
   - `record_remote_read()` - called when fetching from another executor

2. **`ResultFetchMetricsCallback`** trait for scheduler result collection:
   - `record_result_fetch()` - called when scheduler fetches final results

These callbacks are set via `SessionConfig` extensions and invoked by `ShuffleReaderExec` during execution.

### Shuffle Affinity Metrics (Scheduler-side)

The scheduler tracks whether tasks are assigned to executors that have local shuffle data:

1. **`ShuffleAffinityInfo`** struct tracks per-task affinity:
   ```rust
   pub struct ShuffleAffinityInfo {
       pub job_id: String,
       pub stage_id: usize,
       pub executor_id: String,
       pub has_local_data: bool,  // true = hit, false = miss
   }
   ```

2. **`BindingResult`** returned by task binding functions:
   ```rust
   pub struct BindingResult {
       pub bound_tasks: Vec<BoundTask>,
       pub shuffle_affinity: Vec<ShuffleAffinityInfo>,
   }
   ```

3. **Detection logic** in `get_executors_with_local_shuffle_data()`:
   - Checks `running_stage.inputs` to find all executor IDs with partition data
   - When binding a task, if assigned executor is in this set → affinity hit
   - Leaf stages (no shuffle inputs) don't record affinity metrics

### Stage and Task Lifecycle Metrics

Stage and task metrics are recorded in two main flows:

1. **`update_task_status()`** - processes task completion/failure:
   - Returns `TaskStatusUpdateResult` with counts for scheduler to record metrics
   - Tracks task completions, failures, and retries

2. **`processing_stages_update()`** - processes stage state changes:
   - Returns `StageMetricsInfo` with stage lifecycle events
   - Tracks stage starts, completions, failures, and retries

---

## Spice Integration

### Files Modified

| File | Purpose |
|------|---------|
| `crates/runtime/src/cluster/metrics_collector.rs` | `OtelExecutorMetricsCollector`, `OtelSchedulerMetricsCollector`, `OtelShuffleReadMetricsCallback`, `OtelResultFetchMetricsCallback` |
| `crates/runtime/src/metrics/cluster/mod.rs` | OpenTelemetry metric definitions and helper functions |
| `crates/runtime/src/metrics_server/cluster.rs` | Cluster-wide metrics collection for `/metrics?scope=cluster` |
| `crates/runtime/src/cluster/mod.rs` | Wiring callbacks to session config |

### Bug Fixes Applied

1. **Duplicate `node_id` label fix** - `metrics_server/cluster.rs`:
   - `add_labels_to_metric_data_points()` now checks if labels exist before adding
   - Prevents duplicate labels like `node_id="x",node_id="x"` in Prometheus output

---

## Metrics Exposed

### Node Metrics (shared)
- `node_tasks_total{node_id, role, status}` - Total tasks processed
- `node_tasks_active{node_id, role}` - Currently executing tasks
- `node_task_failures{node_id, role, error_type}` - Task failures
- `node_task_retries{node_id, role}` - Task retries
- `node_status{node_id, role}` - Node health status

### Executor Metrics
- `executor_task_duration_ms{node_id}` - Task execution time histogram
- `executor_tasks_active{node_id}` - Active tasks on executor
- `executor_tasks_total{node_id, status}` - Total tasks by status
- `executor_task_failures{node_id, error_type}` - Failures by type
- `executor_memory_available_bytes{node_id}` - Available memory
- `executor_shuffle_write_bytes{node_id}` - Shuffle write volume
- `executor_shuffle_write_rows{node_id}` - Shuffle write row count
- `executor_shuffle_write_duration_ms{node_id}` - Shuffle write time
- `executor_shuffle_read_local_bytes{node_id}` - Local shuffle read volume
- `executor_shuffle_read_local_rows{node_id}` - Local shuffle read rows
- `executor_shuffle_read_local_count{node_id}` - Local read operations
- `executor_shuffle_read_local_duration_ms{node_id}` - Local read time
- `executor_shuffle_read_remote_bytes{node_id}` - Remote shuffle read volume
- `executor_shuffle_read_remote_rows{node_id}` - Remote shuffle read rows
- `executor_shuffle_read_remote_count{node_id}` - Remote read operations
- `executor_shuffle_read_remote_duration_ms{node_id}` - Remote read time

### Scheduler Metrics
- `scheduler_count{node_id}` - Number of schedulers
- `scheduler_active_executors_count{node_id}` - Registered executors
- `scheduler_task_queue_depth{node_id}` - Pending tasks
- `scheduler_job_queue_depth{node_id}` - Pending jobs
- `scheduler_task_scheduling_latency_ms{node_id}` - Scheduling delay
- `scheduler_executor_assignments{node_id}` - Task assignments
- `scheduler_planning_duration_ms{node_id}` - Query planning time
- `scheduler_stages_total{node_id, status}` - Total stages by status
- `scheduler_stage_duration_ms{node_id}` - Stage execution time
- `scheduler_stage_failures{node_id, error_type}` - Stage failures
- `scheduler_stage_retries{node_id}` - Stage retries
- `scheduler_tasks_per_stage{node_id}` - Tasks per stage histogram
- `scheduler_result_fetch_bytes{node_id}` - Result fetch volume
- `scheduler_result_fetch_rows{node_id}` - Result fetch rows
- `scheduler_result_fetch_count{node_id}` - Result fetch operations
- `scheduler_result_fetch_duration_ms{node_id}` - Result fetch time

---

## Ballista Commits

1. `379cf36e` - Add shuffle read metrics extraction and `QueryStageExecutor::plan()` method
2. `d7b0bcf0` - Add shuffle locality metrics to `ExecutorMetricsCollector`, `SchedulerMetricsCollector`, and `ShuffleReaderExec`
3. `0a4174a6` - Add metrics collector to `SchedulerState` and instrument executor and planning metrics
4. `98f7964c` - Add stage and task lifecycle metrics instrumentation to `update_task_status` flow
5. `683e793e` - Add shuffle affinity metrics to scheduler task binding

---

## Testing

### Ballista Tests
```bash
cargo test -p ballista-scheduler test_bind_task  # Task binding tests
cargo test -p ballista-scheduler                  # All scheduler tests
```

### Spice Tests
```bash
cargo test -p runtime metrics_server::cluster::tests  # Cluster metrics tests
```
