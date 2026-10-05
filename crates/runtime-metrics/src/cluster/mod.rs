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
use opentelemetry::{KeyValue, global};
use telemetry::DURATION_MS_HISTOGRAM_BUCKETS;

pub static CLUSTER_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("cluster"));

// =============================================================================
// Node Status Metrics (shared)
// =============================================================================

/// Node status gauge: 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining
/// Labels: `node_id`, role (scheduler|executor)
pub static NODE_STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("node_status")
        .with_description(
            "Status of the cluster node. 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining.",
        )
        .build()
});

/// Number of active executors registered with the scheduler.
/// Labels: `node_id`
pub static SCHEDULER_ACTIVE_EXECUTORS_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_active_executors_count")
        .with_description("Number of active executors registered with the scheduler.")
        .build()
});

/// Number of scheduler instances (for HA configurations).
/// Labels: `node_id`
pub static SCHEDULER_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_count")
        .with_description("Number of scheduler instances in the cluster.")
        .build()
});

// =============================================================================
// Task Metrics (shared between scheduler and executor)
// =============================================================================

/// Total number of tasks processed.
/// Labels: `node_id`, role, status (completed|failed|cancelled)
pub static NODE_TASKS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_tasks_total")
        .with_description("Total number of tasks processed by the node.")
        .with_unit("tasks")
        .build()
});

/// Number of tasks currently being executed.
/// Labels: `node_id`, role
pub static NODE_TASKS_ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    CLUSTER_METER
        .i64_up_down_counter("node_tasks_active")
        .with_description("Number of tasks currently being executed on the node.")
        .with_unit("tasks")
        .build()
});

/// Task execution duration in milliseconds (executor only).
/// Labels: `node_id`
pub static EXECUTOR_TASK_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("executor_task_duration_ms")
        .with_description("Task execution duration in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total number of task failures.
/// Labels: `node_id`, role, `error_type`
pub static NODE_TASK_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_task_failures")
        .with_description("Total number of task failures.")
        .with_unit("tasks")
        .build()
});

/// Total number of task retries.
/// Labels: `node_id`, role
pub static NODE_TASK_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_task_retries")
        .with_description("Total number of task retries.")
        .with_unit("tasks")
        .build()
});

/// Number of tasks waiting to be scheduled.
/// Labels: `node_id`
pub static SCHEDULER_TASK_QUEUE_DEPTH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_task_queue_depth")
        .with_description("Number of tasks waiting to be scheduled.")
        .with_unit("tasks")
        .build()
});

/// Time spent scheduling a task in milliseconds.
/// Labels: `node_id`
pub static SCHEDULER_TASK_SCHEDULING_LATENCY_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
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
/// Labels: `node_id`, status (completed|failed|cancelled)
pub static SCHEDULER_STAGES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_stages_total")
        .with_description("Total number of stages processed by the scheduler.")
        .with_unit("stages")
        .build()
});

/// Stage execution duration in milliseconds.
/// Labels: `node_id`
pub static SCHEDULER_STAGE_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("scheduler_stage_duration_ms")
        .with_description("Stage execution duration in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total number of stage failures.
/// Labels: `node_id`, `error_type`
pub static SCHEDULER_STAGE_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_stage_failures")
        .with_description("Total number of stage failures.")
        .with_unit("stages")
        .build()
});

/// Total number of stage retries.
/// Labels: `node_id`
pub static SCHEDULER_STAGE_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_stage_retries")
        .with_description("Total number of stage retries.")
        .with_unit("stages")
        .build()
});

/// Number of tasks per stage.
/// Labels: `node_id`
pub static SCHEDULER_TASKS_PER_STAGE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
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
/// Labels: `node_id`
pub static EXECUTOR_TASKS_ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    CLUSTER_METER
        .i64_up_down_counter("executor_tasks_active")
        .with_description("Number of tasks currently active on the executor.")
        .with_unit("tasks")
        .build()
});

/// Total tasks executed by the executor.
/// Labels: `node_id`, status (completed|failed)
pub static EXECUTOR_TASKS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_tasks_total")
        .with_description("Total number of tasks executed by the executor.")
        .with_unit("tasks")
        .build()
});

/// Total task failures on the executor.
/// Labels: `node_id`, `error_type`
pub static EXECUTOR_TASK_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_task_failures")
        .with_description("Total number of task failures on the executor.")
        .with_unit("tasks")
        .build()
});

/// Available memory on the executor in bytes.
/// Labels: `node_id`
pub static EXECUTOR_MEMORY_AVAILABLE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("executor_memory_available_bytes")
        .with_description("Available memory on the executor in bytes.")
        .with_unit("By")
        .build()
});

/// Maximum concurrent task slots on the executor.
/// Labels: `node_id`
pub static EXECUTOR_TASK_SLOTS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("executor_task_slots")
        .with_description("Maximum concurrent task slots on the executor.")
        .with_unit("tasks")
        .build()
});

// =============================================================================
// Shuffle Metrics (shared)
// =============================================================================

/// Total bytes written during shuffle operations by executors.
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_WRITE_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_write_bytes")
        .with_description("Total bytes written during shuffle operations.")
        .with_unit("By")
        .build()
});

/// Total rows written during shuffle operations by executors.
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_WRITE_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_write_rows")
        .with_description("Total rows written during shuffle operations.")
        .with_unit("rows")
        .build()
});

/// Duration of shuffle write operations in milliseconds.
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_WRITE_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("executor_shuffle_write_duration_ms")
        .with_description("Duration of shuffle write operations in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

// =============================================================================
// Shuffle Locality Metrics (executor-side)
// =============================================================================
// These metrics track whether shuffle reads were served locally (from disk)
// or remotely (via network from another executor). High local read ratios
// indicate good data locality and efficient shuffle placement.

/// Total bytes read from local shuffle files (same executor that wrote them).
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_LOCAL_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_read_local_bytes")
        .with_description("Total bytes read from local shuffle files (same executor).")
        .with_unit("By")
        .build()
});

/// Total rows read from local shuffle files (same executor that wrote them).
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_LOCAL_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_read_local_rows")
        .with_description("Total rows read from local shuffle files (same executor).")
        .with_unit("rows")
        .build()
});

/// Count of local shuffle read operations.
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_LOCAL_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_read_local_count")
        .with_description("Count of local shuffle read operations.")
        .with_unit("operations")
        .build()
});

/// Duration of local shuffle read operations in milliseconds.
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_LOCAL_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| {
        CLUSTER_METER
            .f64_histogram("executor_shuffle_read_local_duration_ms")
            .with_description("Duration of local shuffle read operations in milliseconds.")
            .with_unit("ms")
            .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
            .build()
    });

/// Total bytes read from remote shuffle files (fetched from another executor).
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_REMOTE_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_read_remote_bytes")
        .with_description("Total bytes fetched from remote shuffle files (other executors).")
        .with_unit("By")
        .build()
});

/// Total rows read from remote shuffle files (fetched from another executor).
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_REMOTE_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_read_remote_rows")
        .with_description("Total rows fetched from remote shuffle files (other executors).")
        .with_unit("rows")
        .build()
});

/// Count of remote shuffle read operations.
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_REMOTE_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_shuffle_read_remote_count")
        .with_description("Count of remote shuffle read operations.")
        .with_unit("operations")
        .build()
});

/// Duration histogram for remote shuffle read operations (network fetch time).
/// Labels: `node_id`
pub static EXECUTOR_SHUFFLE_READ_REMOTE_DURATION_MS: LazyLock<Histogram<f64>> =
    LazyLock::new(|| {
        CLUSTER_METER
            .f64_histogram("executor_shuffle_read_remote_duration_ms")
            .with_description("Duration of remote shuffle read operations in milliseconds.")
            .with_unit("ms")
            .build()
    });

// =============================================================================
// Scheduler Result Fetch Metrics
// =============================================================================
// These metrics track the scheduler (acting as client) fetching final query
// results from executors after distributed query execution completes.

/// Total bytes fetched by the scheduler when collecting final query results.
/// Labels: `node_id`
pub static SCHEDULER_RESULT_FETCH_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_result_fetch_bytes")
        .with_description("Total bytes fetched when collecting final query results from executors.")
        .with_unit("By")
        .build()
});

/// Total rows fetched by the scheduler when collecting final query results.
/// Labels: `node_id`
pub static SCHEDULER_RESULT_FETCH_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_result_fetch_rows")
        .with_description("Total rows fetched when collecting final query results from executors.")
        .with_unit("rows")
        .build()
});

/// Count of result fetch operations by the scheduler.
/// Labels: `node_id`
pub static SCHEDULER_RESULT_FETCH_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_result_fetch_count")
        .with_description("Count of result fetch operations from executors.")
        .with_unit("operations")
        .build()
});

/// Duration of result fetch operations in milliseconds.
/// Labels: `node_id`
pub static SCHEDULER_RESULT_FETCH_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("scheduler_result_fetch_duration_ms")
        .with_description("Duration of result fetch operations in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

// =============================================================================
// Scheduler Operations Metrics
// =============================================================================

/// Number of jobs waiting in the scheduler queue.
/// Labels: `node_id`
pub static SCHEDULER_JOB_QUEUE_DEPTH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_job_queue_depth")
        .with_description("Number of jobs waiting in the scheduler queue.")
        .with_unit("jobs")
        .build()
});

/// Time spent planning a query in milliseconds.
/// Labels: `node_id`
pub static SCHEDULER_PLANNING_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("scheduler_planning_duration_ms")
        .with_description("Time spent planning a query in milliseconds.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Total number of task-to-executor assignments.
/// Labels: `node_id`
pub static SCHEDULER_EXECUTOR_ASSIGNMENTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
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

/// Record that a task has completed successfully (executor only, with duration).
pub fn record_task_completed(node_id: &str, role: &str, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
    ];
    NODE_TASKS_ACTIVE.add(-1, &labels);

    // Duration is only tracked for executors
    let duration_labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_TASK_DURATION_MS.record(duration_ms, &duration_labels);

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

/// Record shuffle write metrics (executor only).
pub fn record_shuffle_write(node_id: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_SHUFFLE_WRITE_BYTES.add(bytes, &labels);
    EXECUTOR_SHUFFLE_WRITE_ROWS.add(rows, &labels);
    EXECUTOR_SHUFFLE_WRITE_DURATION_MS.record(duration_ms, &labels);
}

/// Record local shuffle read metrics (partition read from local disk).
pub fn record_shuffle_read_local(node_id: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_SHUFFLE_READ_LOCAL_BYTES.add(bytes, &labels);
    EXECUTOR_SHUFFLE_READ_LOCAL_ROWS.add(rows, &labels);
    EXECUTOR_SHUFFLE_READ_LOCAL_COUNT.add(1, &labels);
    EXECUTOR_SHUFFLE_READ_LOCAL_DURATION_MS.record(duration_ms, &labels);
}

/// Record remote shuffle read metrics (partition fetched from another executor).
pub fn record_shuffle_read_remote(node_id: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_SHUFFLE_READ_REMOTE_BYTES.add(bytes, &labels);
    EXECUTOR_SHUFFLE_READ_REMOTE_ROWS.add(rows, &labels);
    EXECUTOR_SHUFFLE_READ_REMOTE_COUNT.add(1, &labels);
    EXECUTOR_SHUFFLE_READ_REMOTE_DURATION_MS.record(duration_ms, &labels);
}

/// Record result fetch metrics (scheduler collecting final results from executors).
pub fn record_result_fetch(node_id: &str, bytes: u64, rows: u64, duration_ms: f64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_RESULT_FETCH_BYTES.add(bytes, &labels);
    SCHEDULER_RESULT_FETCH_ROWS.add(rows, &labels);
    SCHEDULER_RESULT_FETCH_COUNT.add(1, &labels);
    SCHEDULER_RESULT_FETCH_DURATION_MS.record(duration_ms, &labels);
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

/// Set the executor's task slot capacity.
///
/// Called once during executor startup to record the maximum number of
/// concurrent tasks this executor can handle.
pub fn set_executor_task_slots(node_id: &str, slots: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_TASK_SLOTS.record(slots, &labels);
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
    use std::sync::{Arc, OnceLock, Weak};
    use std::time::Duration;

    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use opentelemetry_sdk::metrics::{
        InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
    };

    use super::*;

    /// A [`ManualReader`] the global provider and the tests can share: the
    /// provider takes ownership of its readers, and the tests collect from it.
    #[derive(Clone, Debug)]
    struct SharedManualReader(Arc<ManualReader>);

    impl MetricReader for SharedManualReader {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }

        fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
            self.0.collect(metrics)
        }

        fn force_flush(&self) -> OTelSdkResult {
            self.0.force_flush()
        }

        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.0.shutdown_with_timeout(timeout)
        }

        fn temporality(&self, kind: InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }

    /// The reader every test collects from, behind a real SDK provider
    /// installed as the global one.
    ///
    /// The cluster instruments are statics built from `global::meter`, so they
    /// record into whichever provider is global when they are first used, for
    /// the rest of the process. Every test calls this before it records, so the
    /// first use always finds this provider rather than the no-op default.
    fn install_metrics_reader() -> &'static SharedManualReader {
        static READER: OnceLock<SharedManualReader> = OnceLock::new();
        READER.get_or_init(|| {
            let reader = SharedManualReader(Arc::new(ManualReader::builder().build()));
            global::set_meter_provider(
                SdkMeterProvider::builder()
                    .with_reader(reader.clone())
                    .build(),
            );
            reader
        })
    }

    /// Every collected series of the `cluster` meter labelled with `node_id`,
    /// one line each, sorted: `name kind [unit] {labels} = value`, where a
    /// histogram's value is its sample count and sum.
    ///
    /// Each test records under its own `node_id`, so tests running in parallel
    /// against the shared provider read only their own series, and a series
    /// recorded by mistake under the right node shows up as an extra line.
    fn series_of(node_id: &str) -> Vec<String> {
        let mut collected = ResourceMetrics::default();
        install_metrics_reader()
            .collect(&mut collected)
            .expect("collect the cluster metrics");

        let node_label = format!("node_id={node_id}");
        let mut lines = Vec::new();
        for metric in collected
            .scope_metrics()
            .filter(|scope| scope.scope().name() == "cluster")
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        {
            let mut push = |kind: &str, attributes: Vec<&KeyValue>, value: String| {
                let mut labels: Vec<String> = attributes
                    .iter()
                    .map(|kv| format!("{}={}", kv.key.as_str(), kv.value.as_str()))
                    .collect();
                if !labels.contains(&node_label) {
                    return;
                }
                labels.sort();
                lines.push(format!(
                    "{} {kind} [{}] {{{}}} = {value}",
                    metric.name(),
                    metric.unit(),
                    labels.join(",")
                ));
            };
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                    for point in gauge.data_points() {
                        push(
                            "gauge<u64>",
                            point.attributes().collect(),
                            point.value().to_string(),
                        );
                    }
                }
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    let kind = if sum.is_monotonic() {
                        "counter<u64>"
                    } else {
                        "updown<u64>"
                    };
                    for point in sum.data_points() {
                        push(
                            kind,
                            point.attributes().collect(),
                            point.value().to_string(),
                        );
                    }
                }
                AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                    let kind = if sum.is_monotonic() {
                        "counter<i64>"
                    } else {
                        "updown<i64>"
                    };
                    for point in sum.data_points() {
                        push(
                            kind,
                            point.attributes().collect(),
                            point.value().to_string(),
                        );
                    }
                }
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        push(
                            "histogram<u64>",
                            point.attributes().collect(),
                            format!("count {}, sum {}", point.count(), point.sum()),
                        );
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        push(
                            "histogram<f64>",
                            point.attributes().collect(),
                            format!("count {}, sum {:?}", point.count(), point.sum()),
                        );
                    }
                }
                other => panic!(
                    "cluster metric {} has an unexpected shape: {other:?}",
                    metric.name()
                ),
            }
        }
        lines.sort();
        lines
    }

    // =========================================================================
    // Task Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_task_started() {
        install_metrics_reader();
        let node = "task-started-node";
        record_task_started(node, "executor");
        record_task_started(node, "executor");
        record_task_started(node, "scheduler");
        assert_eq!(
            series_of(node),
            vec![
                "node_tasks_active updown<i64> [tasks] {node_id=task-started-node,role=executor} = 2",
                "node_tasks_active updown<i64> [tasks] {node_id=task-started-node,role=scheduler} = 1",
            ]
        );

        // A started task leaves the active count when it completes or fails.
        record_task_completed(node, "executor", 1.0);
        record_task_failed(node, "executor", "timeout");
        record_task_failed(node, "scheduler", "timeout");
        let active: Vec<String> = series_of(node)
            .into_iter()
            .filter(|line| line.starts_with("node_tasks_active "))
            .collect();
        assert_eq!(
            active,
            vec![
                "node_tasks_active updown<i64> [tasks] {node_id=task-started-node,role=executor} = 0",
                "node_tasks_active updown<i64> [tasks] {node_id=task-started-node,role=scheduler} = 0",
            ]
        );
    }

    #[test]
    fn test_record_task_completed() {
        install_metrics_reader();
        let node = "task-completed-node";
        record_task_started(node, "executor");
        record_task_completed(node, "executor", 100.5);
        assert_eq!(
            series_of(node),
            vec![
                "executor_task_duration_ms histogram<f64> [ms] {node_id=task-completed-node} = count 1, sum 100.5",
                "node_tasks_active updown<i64> [tasks] {node_id=task-completed-node,role=executor} = 0",
                "node_tasks_total counter<u64> [tasks] {node_id=task-completed-node,role=executor,status=completed} = 1",
            ]
        );
    }

    #[test]
    fn test_record_task_failed() {
        install_metrics_reader();
        let node = "task-failed-node";
        record_task_started(node, "executor");
        record_task_failed(node, "executor", "timeout");
        assert_eq!(
            series_of(node),
            vec![
                "node_task_failures counter<u64> [tasks] {error_type=timeout,node_id=task-failed-node,role=executor} = 1",
                "node_tasks_active updown<i64> [tasks] {node_id=task-failed-node,role=executor} = 0",
                "node_tasks_total counter<u64> [tasks] {node_id=task-failed-node,role=executor,status=failed} = 1",
            ]
        );
    }

    // =========================================================================
    // Shuffle Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_shuffle_write() {
        install_metrics_reader();
        record_shuffle_write("shuffle-write-node", 1024, 100, 50.0);
        assert_eq!(
            series_of("shuffle-write-node"),
            vec![
                "executor_shuffle_write_bytes counter<u64> [By] {node_id=shuffle-write-node} = 1024",
                "executor_shuffle_write_duration_ms histogram<f64> [ms] {node_id=shuffle-write-node} = count 1, sum 50.0",
                "executor_shuffle_write_rows counter<u64> [rows] {node_id=shuffle-write-node} = 100",
            ]
        );

        // The largest values pass through unclamped.
        record_shuffle_write("shuffle-write-max-node", u64::MAX, u64::MAX, f64::MAX);
        assert_eq!(
            series_of("shuffle-write-max-node"),
            vec![
                "executor_shuffle_write_bytes counter<u64> [By] {node_id=shuffle-write-max-node} = 18446744073709551615",
                "executor_shuffle_write_duration_ms histogram<f64> [ms] {node_id=shuffle-write-max-node} = count 1, sum 1.7976931348623157e308",
                "executor_shuffle_write_rows counter<u64> [rows] {node_id=shuffle-write-max-node} = 18446744073709551615",
            ]
        );
    }

    #[test]
    fn test_record_shuffle_read_local() {
        install_metrics_reader();
        let node = "shuffle-read-local-node";
        record_shuffle_read_local(node, 1024, 100, 10.0);
        record_shuffle_read_local(node, 1024, 100, 10.0);
        // Only the local series move: nothing lands in the remote ones.
        assert_eq!(
            series_of(node),
            vec![
                "executor_shuffle_read_local_bytes counter<u64> [By] {node_id=shuffle-read-local-node} = 2048",
                "executor_shuffle_read_local_count counter<u64> [operations] {node_id=shuffle-read-local-node} = 2",
                "executor_shuffle_read_local_duration_ms histogram<f64> [ms] {node_id=shuffle-read-local-node} = count 2, sum 20.0",
                "executor_shuffle_read_local_rows counter<u64> [rows] {node_id=shuffle-read-local-node} = 200",
            ]
        );
    }

    #[test]
    fn test_record_shuffle_read_remote() {
        install_metrics_reader();
        let node = "shuffle-read-remote-node";
        record_shuffle_read_remote(node, 2048, 200, 50.0);
        // Only the remote series move: the local ones stay untouched.
        assert_eq!(
            series_of(node),
            vec![
                "executor_shuffle_read_remote_bytes counter<u64> [By] {node_id=shuffle-read-remote-node} = 2048",
                "executor_shuffle_read_remote_count counter<u64> [operations] {node_id=shuffle-read-remote-node} = 1",
                "executor_shuffle_read_remote_duration_ms histogram<f64> [ms] {node_id=shuffle-read-remote-node} = count 1, sum 50.0",
                "executor_shuffle_read_remote_rows counter<u64> [rows] {node_id=shuffle-read-remote-node} = 200",
            ]
        );
    }

    // =========================================================================
    // Stage Metrics Helper Function Tests
    // =========================================================================

    #[test]
    fn test_record_stage_completed() {
        install_metrics_reader();
        let node = "stage-completed-node";
        record_stage_completed(node, 1000.0, 4);
        // Exactly one tasks-per-stage sample, equal to the stage's task count.
        assert_eq!(
            series_of(node),
            vec![
                "scheduler_stage_duration_ms histogram<f64> [ms] {node_id=stage-completed-node} = count 1, sum 1000.0",
                "scheduler_stages_total counter<u64> [stages] {node_id=stage-completed-node,status=completed} = 1",
                "scheduler_tasks_per_stage histogram<u64> [tasks] {node_id=stage-completed-node} = count 1, sum 4",
            ]
        );
    }

    #[test]
    fn test_record_stage_failed() {
        install_metrics_reader();
        let node = "stage-failed-node";
        record_stage_failed(node, "resource_exhausted");
        assert_eq!(
            series_of(node),
            vec![
                "scheduler_stage_failures counter<u64> [stages] {error_type=resource_exhausted,node_id=stage-failed-node} = 1",
                "scheduler_stages_total counter<u64> [stages] {node_id=stage-failed-node,status=failed} = 1",
            ]
        );
    }

    // =========================================================================
    // The Published Metric Set
    // =========================================================================

    /// Every cluster metric a helper records, as a dashboard reads it: name,
    /// instrument kind, unit, labels and value after one call to each helper.
    /// Metric names and labels are user-facing, so a rename, a dropped label or
    /// a helper writing into the wrong instrument shows up here.
    #[test]
    fn every_cluster_metric_is_published_under_its_name_unit_and_labels() {
        install_metrics_reader();
        let node = "metric-set-node";
        set_node_status(node, "scheduler", 1);
        set_active_executor_count(node, 5);
        set_scheduler_count(node, 3);
        record_task_started(node, "executor");
        record_task_started(node, "executor");
        record_task_completed(node, "executor", 100.5);
        record_task_failed(node, "executor", "timeout");
        record_task_retry(node, "executor");
        record_shuffle_write(node, 1024, 100, 50.0);
        record_shuffle_read_local(node, 512, 50, 10.0);
        record_shuffle_read_remote(node, 2048, 200, 20.0);
        record_result_fetch(node, 4096, 400, 30.0);
        record_stage_completed(node, 1000.0, 4);
        record_stage_failed(node, "resource_exhausted");
        record_stage_retry(node);
        record_planning_duration(node, 250.0);
        record_executor_assignment(node);
        set_task_queue_depth(node, 10);
        set_job_queue_depth(node, 2);
        set_executor_memory_available(node, 1_073_741_824);
        set_executor_task_slots(node, 8);

        assert_eq!(
            series_of(node),
            vec![
                "executor_memory_available_bytes gauge<u64> [By] {node_id=metric-set-node} = 1073741824",
                "executor_shuffle_read_local_bytes counter<u64> [By] {node_id=metric-set-node} = 512",
                "executor_shuffle_read_local_count counter<u64> [operations] {node_id=metric-set-node} = 1",
                "executor_shuffle_read_local_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 10.0",
                "executor_shuffle_read_local_rows counter<u64> [rows] {node_id=metric-set-node} = 50",
                "executor_shuffle_read_remote_bytes counter<u64> [By] {node_id=metric-set-node} = 2048",
                "executor_shuffle_read_remote_count counter<u64> [operations] {node_id=metric-set-node} = 1",
                "executor_shuffle_read_remote_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 20.0",
                "executor_shuffle_read_remote_rows counter<u64> [rows] {node_id=metric-set-node} = 200",
                "executor_shuffle_write_bytes counter<u64> [By] {node_id=metric-set-node} = 1024",
                "executor_shuffle_write_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 50.0",
                "executor_shuffle_write_rows counter<u64> [rows] {node_id=metric-set-node} = 100",
                "executor_task_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 100.5",
                "executor_task_slots gauge<u64> [tasks] {node_id=metric-set-node} = 8",
                "node_status gauge<u64> [] {node_id=metric-set-node,role=scheduler} = 1",
                "node_task_failures counter<u64> [tasks] {error_type=timeout,node_id=metric-set-node,role=executor} = 1",
                "node_task_retries counter<u64> [tasks] {node_id=metric-set-node,role=executor} = 1",
                "node_tasks_active updown<i64> [tasks] {node_id=metric-set-node,role=executor} = 0",
                "node_tasks_total counter<u64> [tasks] {node_id=metric-set-node,role=executor,status=completed} = 1",
                "node_tasks_total counter<u64> [tasks] {node_id=metric-set-node,role=executor,status=failed} = 1",
                "scheduler_active_executors_count gauge<u64> [] {node_id=metric-set-node} = 5",
                "scheduler_count gauge<u64> [] {node_id=metric-set-node} = 3",
                "scheduler_executor_assignments counter<u64> [assignments] {node_id=metric-set-node} = 1",
                "scheduler_job_queue_depth gauge<u64> [jobs] {node_id=metric-set-node} = 2",
                "scheduler_planning_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 250.0",
                "scheduler_result_fetch_bytes counter<u64> [By] {node_id=metric-set-node} = 4096",
                "scheduler_result_fetch_count counter<u64> [operations] {node_id=metric-set-node} = 1",
                "scheduler_result_fetch_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 30.0",
                "scheduler_result_fetch_rows counter<u64> [rows] {node_id=metric-set-node} = 400",
                "scheduler_stage_duration_ms histogram<f64> [ms] {node_id=metric-set-node} = count 1, sum 1000.0",
                "scheduler_stage_failures counter<u64> [stages] {error_type=resource_exhausted,node_id=metric-set-node} = 1",
                "scheduler_stage_retries counter<u64> [stages] {node_id=metric-set-node} = 1",
                "scheduler_stages_total counter<u64> [stages] {node_id=metric-set-node,status=completed} = 1",
                "scheduler_stages_total counter<u64> [stages] {node_id=metric-set-node,status=failed} = 1",
                "scheduler_task_queue_depth gauge<u64> [tasks] {node_id=metric-set-node} = 10",
                "scheduler_tasks_per_stage histogram<u64> [tasks] {node_id=metric-set-node} = count 1, sum 4",
            ]
        );
    }
}
