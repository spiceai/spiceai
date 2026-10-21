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
    use std::collections::BTreeMap;

    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader as _;
    use telemetry::metrics_reader::MetricsReader;

    use super::*;

    /// Set on the child process `run_in_own_process` spawns.
    const OWN_PROCESS_ENV: &str = "SPICE_RUNTIME_CLUSTER_TEST_OWN_PROCESS";

    /// Re-runs the test `name` (in this module) alone in a fresh process of this
    /// test binary and asserts it passed there. Returns `true` only inside that
    /// child, where the caller runs the test body; returns `false` in the parent
    /// once the child has passed.
    ///
    /// The instruments here are `LazyLock`s bound to whichever meter provider is
    /// global when `CLUSTER_METER` is first touched, and under `cargo test` a
    /// sibling test that plans a query or assigns a partition can touch it first,
    /// binding every instrument to the no-op provider. A test that reads the
    /// instruments back needs a process of its own.
    fn run_in_own_process(name: &str) -> bool {
        if std::env::var_os(OWN_PROCESS_ENV).is_some() {
            return true;
        }

        // libtest names tests by module path without the crate name.
        let module = module_path!()
            .split_once("::")
            .map_or(module_path!(), |(_, module)| module);
        let test = format!("{module}::{name}");
        let output =
            std::process::Command::new(std::env::current_exe().expect("to locate the test binary"))
                .args([test.as_str(), "--exact"])
                .env(OWN_PROCESS_ENV, "1")
                .output()
                .expect("to run the test binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed"),
            "{test} failed in its own process ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status
        );
        false
    }

    /// One exported series: a counter or gauge reads as its value, a histogram
    /// as its sample count and sum.
    #[derive(Debug, PartialEq)]
    enum Series {
        Value(u64),
        U64Histogram { count: u64, sum: u64 },
        F64Histogram { count: u64, sum: f64 },
    }

    /// A metric name with its label set, sorted so it compares independent of
    /// the order the labels were recorded in.
    type SeriesKey = (String, Vec<(String, String)>);

    fn series_key(name: &str, labels: &[(&str, &str)]) -> SeriesKey {
        let mut labels: Vec<(String, String)> = labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        labels.sort();
        (name.to_string(), labels)
    }

    fn point_key<'a, I>(name: &str, attributes: I) -> SeriesKey
    where
        I: IntoIterator<Item = &'a KeyValue>,
    {
        let mut labels: Vec<(String, String)> = attributes
            .into_iter()
            .map(|attribute| {
                (
                    attribute.key.as_str().to_string(),
                    attribute.value.as_str().to_string(),
                )
            })
            .collect();
        labels.sort();
        (name.to_string(), labels)
    }

    /// Every series `reader` exports, by metric name and label set.
    fn exported_series(reader: &MetricsReader) -> BTreeMap<SeriesKey, Series> {
        let mut resource_metrics = ResourceMetrics::default();
        reader
            .collect(&mut resource_metrics)
            .expect("collect the cluster metrics");

        let mut series = BTreeMap::new();
        for metric in resource_metrics
            .scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        {
            let name = metric.name();
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    for point in sum.data_points() {
                        series.insert(
                            point_key(name, point.attributes()),
                            Series::Value(point.value()),
                        );
                    }
                }
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                    for point in gauge.data_points() {
                        series.insert(
                            point_key(name, point.attributes()),
                            Series::Value(point.value()),
                        );
                    }
                }
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        series.insert(
                            point_key(name, point.attributes()),
                            Series::U64Histogram {
                                count: point.count(),
                                sum: point.sum(),
                            },
                        );
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        series.insert(
                            point_key(name, point.attributes()),
                            Series::F64Histogram {
                                count: point.count(),
                                sum: point.sum(),
                            },
                        );
                    }
                }
                other => panic!("unexpected aggregation exported for {name}: {other:?}"),
            }
        }
        series
    }

    /// Every helper exports its documented metric name, label set and value
    /// through a real SDK meter provider. A partition-state operation with a
    /// zero count exports no series at all, and the last value set on a gauge
    /// is the one it reports.
    #[test]
    fn helpers_do_not_panic() {
        if !run_in_own_process("helpers_do_not_panic") {
            return;
        }
        let reader = MetricsReader::new();
        global::set_meter_provider(
            SdkMeterProvider::builder()
                .with_reader(reader.clone())
                .build(),
        );

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

        let scheduler = ("node_id", "sched-1:5000");
        let executor_node = ("node_id", "exec-1:6000");
        let dataset = ("dataset", "eth.recent_blocks");
        let executor = ("executor", "exec-1:6000");
        let expected = BTreeMap::from([
            (
                series_key("query_executor_count", &[scheduler]),
                Series::U64Histogram { count: 1, sum: 3 },
            ),
            (
                series_key(
                    "query_planning_failures",
                    &[scheduler, ("error_type", "missing_partitions")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "query_planning_failures",
                    &[scheduler, ("error_type", "no_executors")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "scheduler_partitions_count",
                    &[scheduler, dataset, ("status", "assigned")],
                ),
                Series::Value(10),
            ),
            (
                series_key(
                    "scheduler_partitions_count",
                    &[scheduler, dataset, ("status", "unassigned")],
                ),
                Series::Value(2),
            ),
            (
                series_key(
                    "scheduler_partition_assignments",
                    &[scheduler, executor, ("status", "committed")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "scheduler_partition_assignments",
                    &[scheduler, executor, ("status", "failed")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "scheduler_partition_discovery_duration_ms",
                    &[scheduler, dataset],
                ),
                Series::F64Histogram {
                    count: 1,
                    sum: 42.0,
                },
            ),
            (
                series_key(
                    "scheduler_partition_state_operations",
                    &[scheduler, ("status", "added")],
                ),
                Series::Value(4),
            ),
            (
                series_key(
                    "scheduler_partition_state_operations",
                    &[scheduler, ("status", "removed")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "scheduler_partitioned_write_forwards",
                    &[scheduler, executor, ("status", "completed")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "scheduler_partitioned_write_forwards",
                    &[scheduler, executor, ("status", "failed")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "executor_assigned_partitions_count",
                    &[executor_node, dataset],
                ),
                Series::Value(7),
            ),
            (
                series_key(
                    "scheduler_executor_active_connections",
                    &[scheduler, executor],
                ),
                Series::Value(0),
            ),
            (
                series_key(
                    "scheduler_executor_connection_retries",
                    &[scheduler, executor],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "executor_scheduler_active_connections",
                    &[executor_node, ("scheduler", "sched-1:5000")],
                ),
                Series::Value(1),
            ),
            (
                series_key(
                    "executor_scheduler_connection_retries",
                    &[executor_node, ("scheduler", "sched-1:5000")],
                ),
                Series::Value(1),
            ),
        ]);
        assert_eq!(exported_series(&reader), expected);
    }
}
