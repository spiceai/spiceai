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

use std::sync::LazyLock;

use opentelemetry::metrics::{Counter, Gauge, Meter};
use opentelemetry::{KeyValue, global};

pub(crate) static CLUSTER_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("cluster"));

/// Node status gauge: 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining.
/// Labels: `node_id`, role (scheduler|executor)
pub(crate) static NODE_STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("node_status")
        .with_description(
            "Status of the cluster node. 0=Unknown, 1=Healthy, 2=Unhealthy, 3=Draining.",
        )
        .build()
});

/// Number of active executors registered with the scheduler.
/// Labels: `node_id`
pub(crate) static SCHEDULER_ACTIVE_EXECUTORS_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_active_executors_count")
        .with_description("Number of active executors registered with the scheduler.")
        .build()
});

/// Number of scheduler instances (for HA configurations).
/// Labels: `node_id`
pub(crate) static SCHEDULER_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_count")
        .with_description("Number of scheduler instances in the cluster.")
        .build()
});

/// Total number of tasks processed.
/// Labels: `node_id`, role, status (completed|failed|cancelled)
pub(crate) static NODE_TASKS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("node_tasks_total")
        .with_description("Total number of tasks processed by the node.")
        .with_unit("tasks")
        .build()
});

/// Number of tasks waiting to be scheduled.
/// Labels: `node_id`
pub(crate) static SCHEDULER_TASK_QUEUE_DEPTH: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("scheduler_task_queue_depth")
        .with_description("Number of tasks waiting to be scheduled.")
        .with_unit("tasks")
        .build()
});

/// Total tasks executed by the executor.
/// Labels: `node_id`, status (completed|failed)
pub(crate) static EXECUTOR_TASKS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_counter("executor_tasks_total")
        .with_description("Total number of tasks executed by the executor.")
        .with_unit("tasks")
        .build()
});

/// Maximum concurrent task slots on the executor.
/// Labels: `node_id`
pub(crate) static EXECUTOR_TASK_SLOTS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CLUSTER_METER
        .u64_gauge("executor_task_slots")
        .with_description("Maximum concurrent task slots on the executor.")
        .with_unit("tasks")
        .build()
});

/// Record that a task has completed successfully.
pub fn record_task_completed(node_id: &str, role: &str) {
    let status_labels = [
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("role", role.to_string()),
        KeyValue::new("status", "completed"),
    ];
    NODE_TASKS_TOTAL.add(1, &status_labels);
}

/// Update the active executor count on the scheduler.
pub fn set_active_executor_count(node_id: &str, count: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_ACTIVE_EXECUTORS_COUNT.record(count, &labels);
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

/// Set the executor's task slot capacity.
pub fn set_executor_task_slots(node_id: &str, slots: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    EXECUTOR_TASK_SLOTS.record(slots, &labels);
}

/// Update the scheduler count (number of schedulers in the cluster).
pub fn set_scheduler_count(node_id: &str, count: u64) {
    let labels = [KeyValue::new("node_id", node_id.to_string())];
    SCHEDULER_COUNT.record(count, &labels);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_task_completed() {
        record_task_completed("node-1", "executor");
        record_task_completed("node-2", "scheduler");
    }

    #[test]
    fn test_set_active_executor_count() {
        set_active_executor_count("node-1", 5);
        set_active_executor_count("node-1", 0);
    }

    #[test]
    fn test_set_node_status() {
        set_node_status("node-1", "scheduler", 0);
        set_node_status("node-1", "scheduler", 1);
        set_node_status("node-1", "executor", 2);
        set_node_status("node-2", "executor", 3);
    }

    #[test]
    fn test_set_task_queue_depth() {
        set_task_queue_depth("node-1", 10);
        set_task_queue_depth("node-1", 0);
    }

    #[test]
    fn test_set_executor_task_slots() {
        set_executor_task_slots("node-1", 8);
        set_executor_task_slots("node-2", 16);
        set_executor_task_slots("node-3", 1);
    }

    #[test]
    fn test_set_scheduler_count() {
        set_scheduler_count("node-1", 3);
        set_scheduler_count("node-1", 1);
    }
}
