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

//! OpenTelemetry-based metrics collectors for Ballista executor and scheduler.
//!
//! These collectors implement the Ballista metrics traits and forward metrics
//! to OpenTelemetry, which integrates with Spice's existing metrics infrastructure.

use std::sync::Arc;

use ballista_core::error::Result;
use ballista_executor::execution_engine::QueryStageExecutor;
use ballista_executor::metrics::ExecutorMetricsCollector;
use ballista_scheduler::metrics::SchedulerMetricsCollector;
use opentelemetry::KeyValue;

use crate::metrics::cluster;

/// OpenTelemetry-based metrics collector for Ballista executors.
///
/// This collector implements `ExecutorMetricsCollector` and forwards all metrics
/// to OpenTelemetry, integrating with Spice's metrics infrastructure.
pub struct OtelExecutorMetricsCollector {
    /// The node ID used as a label in all metrics.
    node_id: String,
}

impl OtelExecutorMetricsCollector {
    /// Creates a new `OtelExecutorMetricsCollector` with the given node ID.
    #[must_use]
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

impl ExecutorMetricsCollector for OtelExecutorMetricsCollector {
    fn record_stage(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        _plan: Arc<dyn QueryStageExecutor>,
    ) {
        cluster::record_task_completed(&self.node_id, "executor");

        let status_labels = [
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("status", "completed"),
        ];
        cluster::EXECUTOR_TASKS_TOTAL.add(1, &status_labels);
    }
}

/// OpenTelemetry-based metrics collector for Ballista scheduler.
///
/// This collector implements `SchedulerMetricsCollector` and forwards all metrics
/// to OpenTelemetry, integrating with Spice's metrics infrastructure.
pub struct OtelSchedulerMetricsCollector {
    /// The node ID used as a label in all metrics.
    node_id: String,
}

impl OtelSchedulerMetricsCollector {
    /// Creates a new `OtelSchedulerMetricsCollector` with the given node ID.
    #[must_use]
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

impl SchedulerMetricsCollector for OtelSchedulerMetricsCollector {
    // =========================================================================
    // Job lifecycle events
    // =========================================================================

    fn record_submitted(&self, _job_id: &str, _queued_at: u64, _submitted_at: u64) {
        // Job metrics are tracked at a higher level; we focus on stage/task metrics here.
        // This could be extended to track job queue latency if needed.
    }

    fn record_completed(&self, _job_id: &str, _queued_at: u64, _completed_at: u64) {
        // Job completion is tracked at a higher level.
    }

    fn record_failed(&self, _job_id: &str, _queued_at: u64, _failed_at: u64) {
        // Job failure is tracked at a higher level.
    }

    fn record_cancelled(&self, _job_id: &str) {
        // Job cancellation is tracked at a higher level.
    }

    fn set_pending_tasks_queue_size(&self, value: u64) {
        cluster::set_task_queue_depth(&self.node_id, value);
    }

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        // OpenTelemetry metrics are exported via the OTel exporter, not this method.
        // Return None to indicate no custom metric format is provided.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_collector_new() {
        let collector = OtelExecutorMetricsCollector::new("test-node-1".to_string());
        assert_eq!(collector.node_id, "test-node-1");
    }

    #[test]
    fn test_scheduler_collector_new() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());
        assert_eq!(collector.node_id, "test-scheduler");
    }

    #[test]
    fn test_scheduler_job_lifecycle() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());
        let now = 1_000_000_u64;

        collector.record_submitted("job-1", now, now + 100);
        collector.record_completed("job-1", now, now + 5000);
        collector.record_failed("job-2", now, now + 1000);
        collector.record_cancelled("job-3");
    }

    #[test]
    fn test_scheduler_queue_sizes() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        collector.set_pending_tasks_queue_size(10);
    }

    #[test]
    fn test_scheduler_gather_metrics_returns_none() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // OTel collector returns None since metrics are exported via OTel exporter
        let result = collector.gather_metrics();
        assert!(result.is_ok());
        assert!(result.expect("gather_metrics should succeed").is_none());
    }
}
