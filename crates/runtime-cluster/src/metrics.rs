/*
Copyright 2026 The Spice.ai OSS Authors

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

//! OpenTelemetry metrics for `SpiceDQ`: distributed query, acceleration
//! partitions, and scheduler↔executor coordination.
//!
//! These metrics are registered under the `cluster` meter so they appear
//! together with the metrics declared in `runtime::metrics::cluster`.
//!
//! Metric name prefixes:
//! - `query_*`: per-query planning metrics
//! - `scheduler_partition*`: scheduler-side partition lifecycle metrics
//! - `executor_assigned_*`: executor-side partition metrics
//! - `*_active_connections` / `*_connection_retries`: coordination metrics

use std::sync::LazyLock;

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};
use telemetry::DURATION_MS_HISTOGRAM_BUCKETS;

static CLUSTER_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("cluster"));

// =============================================================================
// Distributed Query Metrics
// =============================================================================

/// Executors selected per query during partition-aware planning.
/// Labels: `node_id`
static QUERY_EXECUTOR_COUNT: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_histogram("query_executor_count")
        .with_description("Number of executors selected per query during partition-aware planning.")
        .with_unit("executors")
        .with_boundaries(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 10.0, 16.0, 32.0, 64.0, 128.0, 256.0,
        ])
        .build()
});

/// Queries that failed during partition-aware planning before execution.
/// Labels: `node_id`, `error_type` (`missing_partitions` | `no_executors`)
static QUERY_PLANNING_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("query_planning_failures")
        .with_description(
            "Queries that failed during partition-aware planning before execution. \
             Indicates missing partitions or unavailable executors.",
        )
        .with_unit("queries")
        .build()
});

/// Planning-failure error type. Stable label values for `query_planning_failures.error_type`.
#[derive(Debug, Clone, Copy)]
pub enum PlanningFailure {
    /// One or more required partitions are not assigned to any alive executor.
    MissingPartitions,
    /// No executors are connected with a usable `FlightSQL` client.
    NoExecutors,
}

impl PlanningFailure {
    fn as_str(self) -> &'static str {
        match self {
            Self::MissingPartitions => "missing_partitions",
            Self::NoExecutors => "no_executors",
        }
    }
}

/// Record the number of executors selected for a successfully planned query.
pub fn record_query_executor_count(node_id: &str, executors: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    QUERY_EXECUTOR_COUNT.record(executors, &labels);
}

/// Record a query-planning failure with the given error type.
pub fn record_query_planning_failure(node_id: &str, failure: PlanningFailure) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("error_type", failure.as_str()),
    ];
    QUERY_PLANNING_FAILURES.add(1, &labels);
}

// =============================================================================
// Acceleration Partition Metrics — Scheduler
// =============================================================================

/// Number of partitions known to the scheduler, split by assignment status.
/// Labels: `node_id`, `dataset`, `status` (`assigned` | `unassigned`)
static SCHEDULER_PARTITIONS_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_partitions_count")
        .with_description("Number of partitions known to the scheduler, broken down by status.")
        .with_unit("partitions")
        .build()
});

/// Partition assignment operations executed by the scheduler.
/// Labels: `node_id`, `executor`, `status` (`committed` | `failed`)
static SCHEDULER_PARTITION_ASSIGNMENTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_partition_assignments")
        .with_description("Partition assignment operations executed by the scheduler.")
        .with_unit("assignments")
        .build()
});

/// Duration of partition discovery against the upstream source.
/// Labels: `node_id`, `dataset`
static SCHEDULER_PARTITION_DISCOVERY_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    CLUSTER_METER
        .f64_histogram("scheduler_partition_discovery_duration_ms")
        .with_description("Duration of partition discovery against the upstream source.")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

/// Partition status update operations (add / remove / reassign).
/// Labels: `node_id`, `status`
static SCHEDULER_PARTITION_STATE_OPERATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_partition_state_operations")
        .with_description("Partition status update operations on the scheduler.")
        .with_unit("operations")
        .build()
});

/// Partitioned writes forwarded from the scheduler to executors.
/// Labels: `node_id`, `executor`, `status` (`completed` | `failed`)
static SCHEDULER_PARTITIONED_WRITE_FORWARDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_partitioned_write_forwards")
        .with_description("Partitioned writes forwarded by the scheduler to executors.")
        .with_unit("operations")
        .build()
});

/// Status label values for `scheduler_partition_assignments`.
#[derive(Debug, Clone, Copy)]
pub enum AssignmentStatus {
    Committed,
    Failed,
}

impl AssignmentStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }
}

/// Status label values for `scheduler_partitioned_write_forwards`.
#[derive(Debug, Clone, Copy)]
pub enum WriteForwardStatus {
    Completed,
    Failed,
}

impl WriteForwardStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Status label values for `scheduler_partition_state_operations`.
#[derive(Debug, Clone, Copy)]
pub enum PartitionStateOperation {
    /// A new partition was discovered and added to the store.
    Added,
    /// A partition was removed from the store after disappearing from the source.
    Removed,
    /// A partition's assignment was reassigned to a different executor.
    Reassigned,
}

impl PartitionStateOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::Reassigned => "reassigned",
        }
    }
}

/// Set the partition count for a dataset, split by `assigned`/`unassigned`.
pub fn set_scheduler_partitions_count(
    node_id: &str,
    dataset: &str,
    assigned: u64,
    unassigned: u64,
) {
    let assigned_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("dataset", dataset.to_string()),
        KeyValue::new("status", "assigned"),
    ];
    SCHEDULER_PARTITIONS_COUNT.record(assigned, &assigned_labels);

    let unassigned_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("dataset", dataset.to_string()),
        KeyValue::new("status", "unassigned"),
    ];
    SCHEDULER_PARTITIONS_COUNT.record(unassigned, &unassigned_labels);
}

/// Record a partition assignment attempt to a specific executor.
pub fn record_partition_assignment(node_id: &str, executor: &str, status: AssignmentStatus) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("executor", executor.to_string()),
        KeyValue::new("status", status.as_str()),
    ];
    SCHEDULER_PARTITION_ASSIGNMENTS.add(1, &labels);
}

/// Record partition discovery duration against the source for a dataset.
pub fn record_partition_discovery_duration(node_id: &str, dataset: &str, duration_ms: f64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("dataset", dataset.to_string()),
    ];
    SCHEDULER_PARTITION_DISCOVERY_DURATION_MS.record(duration_ms, &labels);
}

/// Record a partition state operation (add / remove / reassign).
pub fn record_partition_state_operation(node_id: &str, op: PartitionStateOperation, count: u64) {
    if count == 0 {
        return;
    }
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("status", op.as_str()),
    ];
    SCHEDULER_PARTITION_STATE_OPERATIONS.add(count, &labels);
}

/// Record a partitioned-write forward to an executor.
pub fn record_partitioned_write_forward(node_id: &str, executor: &str, status: WriteForwardStatus) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("executor", executor.to_string()),
        KeyValue::new("status", status.as_str()),
    ];
    SCHEDULER_PARTITIONED_WRITE_FORWARDS.add(1, &labels);
}

// =============================================================================
// Acceleration Partition Metrics — Executor
// =============================================================================

/// Number of partitions currently assigned to this executor.
/// Labels: `node_id`, `dataset`
static EXECUTOR_ASSIGNED_PARTITIONS_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("executor_assigned_partitions_count")
        .with_description("Number of partitions currently assigned to this executor.")
        .with_unit("partitions")
        .build()
});

/// Set the executor's assigned-partition count for a dataset.
pub fn set_executor_assigned_partitions_count(node_id: &str, dataset: &str, count: u64) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("dataset", dataset.to_string()),
    ];
    EXECUTOR_ASSIGNED_PARTITIONS_COUNT.record(count, &labels);
}

// =============================================================================
// Coordination Metrics — Scheduler ↔ Executor connections
// =============================================================================

/// Active control-stream connections from scheduler to each executor.
/// Labels: `node_id`, `executor`
static SCHEDULER_EXECUTOR_ACTIVE_CONNECTIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_executor_active_connections")
        .with_description("Active control-stream connections from the scheduler to each executor.")
        .with_unit("connections")
        .build()
});

/// Connection retries (reconnections) initiated by the scheduler to executors.
/// Labels: `node_id`, `executor`
static SCHEDULER_EXECUTOR_CONNECTION_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("scheduler_executor_connection_retries")
        .with_description("Reconnections observed by the scheduler for an executor.")
        .with_unit("reconnections")
        .build()
});

/// Active control-stream connections from this executor to each scheduler.
/// Labels: `node_id`, `scheduler`
static EXECUTOR_SCHEDULER_ACTIVE_CONNECTIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("executor_scheduler_active_connections")
        .with_description("Active control-stream connections from the executor to each scheduler.")
        .with_unit("connections")
        .build()
});

/// Connection retries (reconnections) from this executor to each scheduler.
/// Labels: `node_id`, `scheduler`
static EXECUTOR_SCHEDULER_CONNECTION_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_scheduler_connection_retries")
        .with_description("Reconnections from the executor to a scheduler.")
        .with_unit("reconnections")
        .build()
});

/// Set the scheduler→executor active-connection gauge for one executor (0 or 1).
pub fn set_scheduler_executor_active_connection(node_id: &str, executor: &str, active: bool) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("executor", executor.to_string()),
    ];
    SCHEDULER_EXECUTOR_ACTIVE_CONNECTIONS.record(u64::from(active), &labels);
}

/// Increment the scheduler→executor reconnection counter.
pub fn record_scheduler_executor_connection_retry(node_id: &str, executor: &str) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("executor", executor.to_string()),
    ];
    SCHEDULER_EXECUTOR_CONNECTION_RETRIES.add(1, &labels);
}

/// Set the executor→scheduler active-connection gauge for one scheduler (0 or 1).
pub fn set_executor_scheduler_active_connection(node_id: &str, scheduler: &str, active: bool) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("scheduler", scheduler.to_string()),
    ];
    EXECUTOR_SCHEDULER_ACTIVE_CONNECTIONS.record(u64::from(active), &labels);
}

/// Increment the executor→scheduler reconnection counter.
pub fn record_executor_scheduler_connection_retry(node_id: &str, scheduler: &str) {
    let labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("scheduler", scheduler.to_string()),
    ];
    EXECUTOR_SCHEDULER_CONNECTION_RETRIES.add(1, &labels);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_do_not_panic() {
        record_query_executor_count("sched-1:5000", 3);
        record_query_planning_failure("sched-1:5000", PlanningFailure::MissingPartitions);
        record_query_planning_failure("sched-1:5000", PlanningFailure::NoExecutors);

        set_scheduler_partitions_count("sched-1:5000", "eth.recent_blocks", 10, 2);
        record_partition_assignment("sched-1:5000", "exec-1:6000", AssignmentStatus::Committed);
        record_partition_assignment("sched-1:5000", "exec-1:6000", AssignmentStatus::Failed);
        record_partition_discovery_duration("sched-1:5000", "eth.recent_blocks", 42.0);
        record_partition_state_operation("sched-1:5000", PartitionStateOperation::Added, 4);
        record_partition_state_operation("sched-1:5000", PartitionStateOperation::Removed, 1);
        record_partition_state_operation("sched-1:5000", PartitionStateOperation::Reassigned, 0);
        record_partitioned_write_forward(
            "sched-1:5000",
            "exec-1:6000",
            WriteForwardStatus::Completed,
        );
        record_partitioned_write_forward("sched-1:5000", "exec-1:6000", WriteForwardStatus::Failed);

        set_executor_assigned_partitions_count("exec-1:6000", "eth.recent_blocks", 7);

        set_scheduler_executor_active_connection("sched-1:5000", "exec-1:6000", true);
        set_scheduler_executor_active_connection("sched-1:5000", "exec-1:6000", false);
        record_scheduler_executor_connection_retry("sched-1:5000", "exec-1:6000");

        set_executor_scheduler_active_connection("exec-1:6000", "sched-1:5000", true);
        record_executor_scheduler_connection_retry("exec-1:6000", "sched-1:5000");
    }
}
