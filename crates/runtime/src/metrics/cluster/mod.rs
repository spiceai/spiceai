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

//! OpenTelemetry metrics for Spice cluster mode (Ballista-based distributed query execution).
//!
//! Metrics are organized by prefix:
//! - `node_*`: Shared metrics recorded by both scheduler and executor nodes
//! - `scheduler_*`: Scheduler-specific metrics
//! - `executor_*`: Executor-specific metrics

use std::sync::LazyLock;

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
use opentelemetry::{global, KeyValue};
use telemetry::DURATION_MS_HISTOGRAM_BUCKETS;

pub(crate) static CLUSTER_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("cluster"));

// =============================================================================
// Node Status Metrics (shared)
// =============================================================================

/// Node status gauge: 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining
/// Labels: node_id, role (scheduler|executor)
pub(crate) static NODE_STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("node_status")
        .with_description(
            "Status of the cluster node. 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining.",
        )
        .build()
});

/// Number of active executors registered with the scheduler.
/// Labels: node_id
pub(crate) static SCHEDULER_ACTIVE_EXECUTORS_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_active_executors_count")
        .with_description("Number of active executors registered with the scheduler.")
        .build()
});

/// Number of scheduler instances (for HA configurations).
/// Labels: node_id
pub(crate) static SCHEDULER_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_count")
        .with_description("Number of scheduler instances in the cluster.")
        .build()
});

// =============================================================================
// Task Metrics (shared between scheduler and executor)
// =============================================================================

/// Total number of tasks processed.
/// Labels: node_id, role, status (completed|failed|cancelled)
pub(crate) static NODE_TASKS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_tasks_total")
        .with_description("Total number of tasks processed by the node.")
        .with_unit("tasks")
        .build()
});

/// Number of tasks currently being executed.
/// Labels: node_id, role
pub(crate) static NODE_TASKS_ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    CLUSTER_METER
        .i64_up_down_counter("node_tasks_active")
        .with_description("Number of tasks currently being executed on the node.")
        .with_unit("tasks")
        .build()
});

/// Task execution duration in milliseconds.
/// Labels: node_id, role
pub(crate) static NODE_TASK_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("node_task_duration_ms")
        .with_description("Task execution duration in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total number of task failures.
/// Labels: node_id, role, error_type
pub(crate) static NODE_TASK_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_task_failures")
        .with_description("Total number of task failures.")
        .with_unit("tasks")
        .build()
});

/// Total number of task retries.
/// Labels: node_id, role
pub(crate) static NODE_TASK_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_task_retries")
        .with_description("Total number of task retries.")
        .with_unit("tasks")
        .build()
});

/// Number of tasks waiting to be scheduled.
/// Labels: node_id
pub(crate) static SCHEDULER_TASK_QUEUE_DEPTH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_task_queue_depth")
        .with_description("Number of tasks waiting to be scheduled.")
        .with_unit("tasks")
        .build()
});

/// Time spent scheduling a task in milliseconds.
/// Labels: node_id
pub(crate) static SCHEDULER_TASK_SCHEDULING_LATENCY_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| {
        CLUSTER_METER
            .f64_histogram("scheduler_task_scheduling_latency_ms")
            .with_description("Time spent scheduling a task in milliseconds.")
            .with_unit("ms")
            .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
            .build()
    });

// =============================================================================
// Stage Metrics (scheduler)
// =============================================================================

/// Total number of stages processed.
/// Labels: node_id, status (completed|failed|cancelled)
pub(crate) static SCHEDULER_STAGES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_stages_total")
        .with_description("Total number of stages processed by the scheduler.")
        .with_unit("stages")
        .build()
});

/// Stage execution duration in milliseconds.
/// Labels: node_id
pub(crate) static SCHEDULER_STAGE_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("scheduler_stage_duration_ms")
        .with_description("Stage execution duration in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total number of stage failures.
/// Labels: node_id, error_type
pub(crate) static SCHEDULER_STAGE_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_stage_failures")
        .with_description("Total number of stage failures.")
        .with_unit("stages")
        .build()
});

/// Total number of stage retries.
/// Labels: node_id
pub(crate) static SCHEDULER_STAGE_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_stage_retries")
        .with_description("Total number of stage retries.")
        .with_unit("stages")
        .build()
});

/// Number of tasks per stage.
/// Labels: node_id
pub(crate) static SCHEDULER_TASKS_PER_STAGE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_histogram("scheduler_tasks_per_stage")
        .with_description("Number of tasks per stage.")
        .with_unit("tasks")
        .with_boundaries(vec![
            1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0,
        ])
        .build()
});

// =============================================================================
// Executor Metrics
// =============================================================================

/// Number of tasks currently active on the executor.
/// Labels: node_id
pub(crate) static EXECUTOR_TASKS_ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    CLUSTER_METER
        .i64_up_down_counter("executor_tasks_active")
        .with_description("Number of tasks currently active on the executor.")
        .with_unit("tasks")
        .build()
});

/// Total tasks executed by the executor.
/// Labels: node_id, status (completed|failed)
pub(crate) static EXECUTOR_TASKS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_tasks_total")
        .with_description("Total number of tasks executed by the executor.")
        .with_unit("tasks")
        .build()
});

/// Total task failures on the executor.
/// Labels: node_id, error_type
pub(crate) static EXECUTOR_TASK_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_task_failures")
        .with_description("Total number of task failures on the executor.")
        .with_unit("tasks")
        .build()
});

/// Available memory on the executor in bytes.
/// Labels: node_id
pub(crate) static EXECUTOR_MEMORY_AVAILABLE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("executor_memory_available_bytes")
        .with_description("Available memory on the executor in bytes.")
        .with_unit("By")
        .build()
});

// =============================================================================
// Shuffle Metrics (shared)
// =============================================================================

/// Total bytes written during shuffle operations.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_WRITE_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_write_bytes")
        .with_description("Total bytes written during shuffle operations.")
        .with_unit("By")
        .build()
});

/// Total rows written during shuffle operations.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_WRITE_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_write_rows")
        .with_description("Total rows written during shuffle operations.")
        .with_unit("rows")
        .build()
});

/// Duration of shuffle write operations in milliseconds.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_WRITE_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("node_shuffle_write_duration_ms")
        .with_description("Duration of shuffle write operations in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total bytes read during shuffle operations.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_bytes")
        .with_description("Total bytes read during shuffle operations.")
        .with_unit("By")
        .build()
});

/// Total rows read during shuffle operations.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_shuffle_read_rows")
        .with_description("Total rows read during shuffle operations.")
        .with_unit("rows")
        .build()
});

/// Duration of shuffle read operations in milliseconds.
/// Labels: node_id, role
pub(crate) static NODE_SHUFFLE_READ_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("node_shuffle_read_duration_ms")
        .with_description("Duration of shuffle read operations in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

// =============================================================================
// Scheduler Operations Metrics
// =============================================================================

/// Number of jobs waiting in the scheduler queue.
/// Labels: node_id
pub(crate) static SCHEDULER_JOB_QUEUE_DEPTH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_job_queue_depth")
        .with_description("Number of jobs waiting in the scheduler queue.")
        .with_unit("jobs")
        .build()
});

/// Time spent planning a query in milliseconds.
/// Labels: node_id
pub(crate) static SCHEDULER_PLANNING_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("scheduler_planning_duration_ms")
        .with_description("Time spent planning a query in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total number of task-to-executor assignments.
/// Labels: node_id
pub(crate) static SCHEDULER_EXECUTOR_ASSIGNMENTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_executor_assignments")
        .with_description("Total number of task-to-executor assignments made by the scheduler.")
        .with_unit("assignments")
        .build()
});

// =============================================================================
// Helper Functions for Recording Metrics
// =============================================================================

/// Record that a task has started.
pub fn record_task_started(node_id: &str, role: &str) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_TASKS_ACTIVE.add(1, &labels);
}

/// Record that a task has completed successfully.
pub fn record_task_completed(node_id: &str, role: &str, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_TASKS_ACTIVE.add(-1, &labels);
    NODE_TASK_DURATION_MS.record(duration_ms, &labels);

    let status_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
        KeyValue::new("status", "completed"),
    ];
    NODE_TASKS_TOTAL.add(1, &status_labels);
}

/// Record that a task has failed.
pub fn record_task_failed(node_id: &str, role: &str, error_type: &str) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_TASKS_ACTIVE.add(-1, &labels);

    let status_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
        KeyValue::new("status", "failed"),
    ];
    NODE_TASKS_TOTAL.add(1, &status_labels);

    let failure_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
        KeyValue::new("error_type", error_type.to_string()),
    ];
    NODE_TASK_FAILURES.add(1, &failure_labels);
}

/// Record shuffle write metrics.
pub fn record_shuffle_write(node_id: &str, role: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_SHUFFLE_WRITE_BYTES.add(bytes, &labels);
    NODE_SHUFFLE_WRITE_ROWS.add(rows, &labels);
    NODE_SHUFFLE_WRITE_DURATION_MS.record(duration_ms, &labels);
}

/// Record shuffle read metrics.
pub fn record_shuffle_read(node_id: &str, role: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_SHUFFLE_READ_BYTES.add(bytes, &labels);
    NODE_SHUFFLE_READ_ROWS.add(rows, &labels);
    NODE_SHUFFLE_READ_DURATION_MS.record(duration_ms, &labels);
}

/// Record stage completion on the scheduler.
pub fn record_stage_completed(node_id: &str, duration_ms: f64, task_count: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_STAGE_DURATION_MS.record(duration_ms, &labels);
    SCHEDULER_TASKS_PER_STAGE.record(task_count, &labels);

    let status_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("status", "completed"),
    ];
    SCHEDULER_STAGES_TOTAL.add(1, &status_labels);
}

/// Record stage failure on the scheduler.
pub fn record_stage_failed(node_id: &str, error_type: &str) {
    let status_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("status", "failed"),
    ];
    SCHEDULER_STAGES_TOTAL.add(1, &status_labels);

    let failure_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("error_type", error_type.to_string()),
    ];
    SCHEDULER_STAGE_FAILURES.add(1, &failure_labels);
}

/// Record stage retry on the scheduler.
pub fn record_stage_retry(node_id: &str) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_STAGE_RETRIES.add(1, &labels);
}

/// Record planning duration on the scheduler.
pub fn record_planning_duration(node_id: &str, duration_ms: f64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_PLANNING_DURATION_MS.record(duration_ms, &labels);
}

/// Update the active executor count on the scheduler.
pub fn set_active_executor_count(node_id: &str, count: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_ACTIVE_EXECUTORS_COUNT.record(count, &labels);
}

/// Record an executor assignment.
pub fn record_executor_assignment(node_id: &str) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_EXECUTOR_ASSIGNMENTS.add(1, &labels);
}

/// Update the node status.
pub fn set_node_status(node_id: &str, role: &str, status: u64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_STATUS.record(status, &labels);
}

/// Update task queue depth on the scheduler.
pub fn set_task_queue_depth(node_id: &str, depth: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_TASK_QUEUE_DEPTH.record(depth, &labels);
}

/// Update job queue depth on the scheduler.
pub fn set_job_queue_depth(node_id: &str, depth: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_JOB_QUEUE_DEPTH.record(depth, &labels);
}

/// Update executor memory available.
pub fn set_executor_memory_available(node_id: &str, bytes: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_MEMORY_AVAILABLE_BYTES.record(bytes, &labels);
}

/// Update the scheduler count (number of schedulers in the cluster).
pub fn set_scheduler_count(node_id: &str, count: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_COUNT.record(count, &labels);
}

/// Record a task retry.
pub fn record_task_retry(node_id: &str, role: &str) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_TASK_RETRIES.add(1, &labels);
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Task Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_task_started() {
        // Should not panic
        record_task_started("node-1", "executor");
        record_task_started("node-2", "scheduler");
    }

    #[test]
    fn test_record_task_completed() {
        // Should not panic
        record_task_completed("node-1", "executor", 100.5);
        record_task_completed("node-2", "scheduler", 0.0);
        record_task_completed("node-3", "executor", 10000.0);
    }

    #[test]
    fn test_record_task_failed() {
        // Should not panic
        record_task_failed("node-1", "executor", "timeout");
        record_task_failed("node-1", "scheduler", "out_of_memory");
        record_task_failed("node-2", "executor", "network_error");
    }

    #[test]
    fn test_record_task_retry() {
        // Should not panic
        record_task_retry("node-1", "executor");
        record_task_retry("node-2", "scheduler");
    }

    // =========================================================================
    // Shuffle Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_shuffle_write() {
        // Should not panic
        record_shuffle_write("node-1", "executor", 1024, 100, 50.0);
        record_shuffle_write("node-2", "executor", 0, 0, 0.0);
        record_shuffle_write("node-3", "executor", u64::MAX, u64::MAX, f64::MAX);
    }

    #[test]
    fn test_record_shuffle_read() {
        // Should not panic
        record_shuffle_read("node-1", "executor", 2048, 200, 75.0);
        record_shuffle_read("node-2", "executor", 0, 0, 0.0);
    }

    // =========================================================================
    // Stage Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_stage_completed() {
        // Should not panic
        record_stage_completed("node-1", 1000.0, 4);
        record_stage_completed("node-1", 0.0, 0);
    }

    #[test]
    fn test_record_stage_failed() {
        // Should not panic
        record_stage_failed("node-1", "resource_exhausted");
        record_stage_failed("node-2", "timeout");
    }

    #[test]
    fn test_record_stage_retry() {
        // Should not panic
        record_stage_retry("node-1");
        record_stage_retry("node-2");
    }

    // =========================================================================
    // Scheduler Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_planning_duration() {
        // Should not panic
        record_planning_duration("node-1", 250.0);
        record_planning_duration("node-1", 0.0);
    }

    #[test]
    fn test_set_active_executor_count() {
        // Should not panic
        set_active_executor_count("node-1", 5);
        set_active_executor_count("node-1", 0);
    }

    #[test]
    fn test_record_executor_assignment() {
        // Should not panic
        record_executor_assignment("node-1");
    }

    #[test]
    fn test_set_node_status() {
        // Should not panic - test all status values
        set_node_status("node-1", "scheduler", 0); // Unknown
        set_node_status("node-1", "scheduler", 1); // Healthy
        set_node_status("node-1", "executor", 2); // Unhealthy
        set_node_status("node-2", "executor", 3); // Draining
    }

    #[test]
    fn test_set_task_queue_depth() {
        // Should not panic
        set_task_queue_depth("node-1", 10);
        set_task_queue_depth("node-1", 0);
    }

    #[test]
    fn test_set_job_queue_depth() {
        // Should not panic
        set_job_queue_depth("node-1", 5);
        set_job_queue_depth("node-1", 0);
    }

    #[test]
    fn test_set_executor_memory_available() {
        // Should not panic
        set_executor_memory_available("node-1", 1024 * 1024 * 1024); // 1 GB
        set_executor_memory_available("node-1", 0);
    }

    #[test]
    fn test_set_scheduler_count() {
        // Should not panic
        set_scheduler_count("node-1", 3);
        set_scheduler_count("node-1", 1);
    }
}
