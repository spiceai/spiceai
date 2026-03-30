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
use ballista_core::extension::{ResultFetchMetricsCallback, ShuffleReadMetricsCallback};
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
    fn record_task_started(&self, _job_id: &str, _stage_id: usize, _partition: usize) {
        cluster::record_task_started(&self.node_id, "executor");

        // Also update executor-specific active task count
        let labels = [KeyValue::new("node_id", self.node_id.clone())];
        cluster::EXECUTOR_TASKS_ACTIVE.add(1, &labels);
    }

    fn record_stage(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        _plan: Arc<dyn QueryStageExecutor>,
        duration_ms: u64,
    ) {
        #[expect(clippy::cast_precision_loss)]
        let duration_ms_f64 = duration_ms as f64;

        cluster::record_task_completed(&self.node_id, "executor", duration_ms_f64);

        // Update executor-specific metrics
        let labels = [KeyValue::new("node_id", self.node_id.clone())];
        cluster::EXECUTOR_TASKS_ACTIVE.add(-1, &labels);

        let status_labels = [
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("status", "completed"),
        ];
        cluster::EXECUTOR_TASKS_TOTAL.add(1, &status_labels);
    }

    fn record_task_failed(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        error_type: &str,
    ) {
        cluster::record_task_failed(&self.node_id, "executor", error_type);

        // Update executor-specific metrics
        let labels = [KeyValue::new("node_id", self.node_id.clone())];
        cluster::EXECUTOR_TASKS_ACTIVE.add(-1, &labels);

        let status_labels = [
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("status", "failed"),
        ];
        cluster::EXECUTOR_TASKS_TOTAL.add(1, &status_labels);

        let failure_labels = [
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("error_type", error_type.to_string()),
        ];
        cluster::EXECUTOR_TASK_FAILURES.add(1, &failure_labels);
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
        #[expect(clippy::cast_precision_loss)]
        let duration_ms_f64 = duration_ms as f64;
        cluster::record_shuffle_write(&self.node_id, bytes, rows, duration_ms_f64);
    }

    fn record_shuffle_read(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _partition: usize,
        _bytes: u64,
        _rows: u64,
        _duration_ms: u64,
    ) {
        // No-op: We only track locality-aware shuffle reads via record_shuffle_read_local
        // and record_shuffle_read_remote. This generic callback is kept for trait compatibility
        // but the locality-specific callbacks provide more useful metrics.
    }

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
        cluster::record_shuffle_read_local(&self.node_id, bytes, rows, duration_ms_f64);
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
        cluster::record_shuffle_read_remote(&self.node_id, bytes, rows, duration_ms_f64);
    }

    fn record_memory_available(&self, available_bytes: u64) {
        cluster::set_executor_memory_available(&self.node_id, available_bytes);
    }
}

/// OpenTelemetry-based callback for shuffle read locality metrics.
///
/// This callback is passed to the Ballista shuffle reader via session config
/// and is invoked during shuffle operations to record whether reads were
/// local (from disk) or remote (fetched from another executor).
///
/// This enables tracking shuffle locality/affinity metrics to understand
/// data placement efficiency in the cluster.
pub struct OtelShuffleReadMetricsCallback {
    /// The node ID used as a label in all metrics.
    node_id: String,
}

impl OtelShuffleReadMetricsCallback {
    /// Creates a new `OtelShuffleReadMetricsCallback` with the given node ID.
    #[must_use]
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    /// Creates a new callback wrapped in an Arc for use with session config.
    #[must_use]
    pub fn new_arc(node_id: String) -> Arc<dyn ShuffleReadMetricsCallback> {
        Arc::new(Self::new(node_id))
    }
}

impl ShuffleReadMetricsCallback for OtelShuffleReadMetricsCallback {
    fn record_local_read(
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
        cluster::record_shuffle_read_local(&self.node_id, bytes, rows, duration_ms_f64);
    }

    fn record_remote_read(
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
        cluster::record_shuffle_read_remote(&self.node_id, bytes, rows, duration_ms_f64);
    }
}

/// OpenTelemetry-based callback for result fetch metrics.
///
/// This callback is passed to the Ballista `DistributedQueryExec` via session config
/// and is invoked when the scheduler (acting as client) fetches final query results
/// from executors.
pub struct OtelResultFetchMetricsCallback {
    /// The node ID used as a label in all metrics.
    node_id: String,
}

impl OtelResultFetchMetricsCallback {
    /// Creates a new `OtelResultFetchMetricsCallback` with the given node ID.
    #[must_use]
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    /// Creates a new callback wrapped in an Arc for use with session config.
    #[must_use]
    pub fn new_arc(node_id: String) -> Arc<dyn ResultFetchMetricsCallback> {
        Arc::new(Self::new(node_id))
    }
}

impl ResultFetchMetricsCallback for OtelResultFetchMetricsCallback {
    fn record_result_fetch(
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
        cluster::record_result_fetch(&self.node_id, bytes, rows, duration_ms_f64);
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

    fn set_pending_jobs_queue_size(&self, value: u64) {
        cluster::set_job_queue_depth(&self.node_id, value);
    }

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        // OpenTelemetry metrics are exported via the OTel exporter, not this method.
        // Return None to indicate no custom metric format is provided.
        Ok(None)
    }

    // =========================================================================
    // Stage lifecycle events
    // =========================================================================

    fn record_stage_started(&self, _job_id: &str, _stage_id: usize, task_count: usize) {
        // Record the number of tasks per stage when it starts
        let labels = [KeyValue::new("node_id", self.node_id.clone())];
        cluster::SCHEDULER_TASKS_PER_STAGE.record(task_count as u64, &labels);
    }

    fn record_stage_completed(&self, _job_id: &str, _stage_id: usize, duration_ms: u64) {
        #[expect(clippy::cast_precision_loss)]
        let duration_ms_f64 = duration_ms as f64;
        // task_count is recorded in record_stage_started, use 0 here as placeholder
        // since we don't have it available at completion time
        cluster::record_stage_completed(&self.node_id, duration_ms_f64, 0);
    }

    fn record_stage_failed(&self, _job_id: &str, _stage_id: usize, error_type: &str) {
        cluster::record_stage_failed(&self.node_id, error_type);
    }

    fn record_stage_retry(&self, _job_id: &str, _stage_id: usize) {
        cluster::record_stage_retry(&self.node_id);
    }

    // =========================================================================
    // Task scheduling events
    // =========================================================================

    fn record_task_scheduled(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _executor_id: &str,
        latency_ms: u64,
    ) {
        let labels = [KeyValue::new("node_id", self.node_id.clone())];

        #[expect(clippy::cast_precision_loss)]
        cluster::SCHEDULER_TASK_SCHEDULING_LATENCY_MS.record(latency_ms as f64, &labels);

        cluster::record_executor_assignment(&self.node_id);

        // Track task as started from scheduler perspective
        cluster::record_task_started(&self.node_id, "scheduler");
    }

    fn record_task_completed(&self, _job_id: &str, _stage_id: usize, _executor_id: &str) {
        // Task completed - decrement active count
        // Duration is tracked on the executor side
        let labels = [
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("role", "scheduler"),
        ];
        cluster::NODE_TASKS_ACTIVE.add(-1, &labels);

        let status_labels = [
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("role", "scheduler"),
            KeyValue::new("status", "completed"),
        ];
        cluster::NODE_TASKS_TOTAL.add(1, &status_labels);
    }

    fn record_task_failed(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _executor_id: &str,
        error_type: &str,
    ) {
        cluster::record_task_failed(&self.node_id, "scheduler", error_type);
    }

    fn record_task_retry(&self, _job_id: &str, _stage_id: usize) {
        cluster::record_task_retry(&self.node_id, "scheduler");
    }

    fn record_task_shuffle_affinity_hit(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _executor_id: &str,
    ) {
        // Shuffle affinity tracking is not yet implemented in the scheduler.
        // This would require scheduler-side changes to detect when a task
        // is assigned to an executor that has local shuffle data.
        // For now, this is a no-op placeholder.
    }

    fn record_task_shuffle_affinity_miss(
        &self,
        _job_id: &str,
        _stage_id: usize,
        _executor_id: &str,
    ) {
        // Shuffle affinity tracking is not yet implemented in the scheduler.
        // This would require scheduler-side changes to detect when a task
        // is assigned to an executor that does NOT have local shuffle data.
        // For now, this is a no-op placeholder.
    }

    // =========================================================================
    // Executor management events
    // =========================================================================

    fn set_active_executor_count(&self, count: usize) {
        cluster::set_active_executor_count(&self.node_id, count as u64);
    }

    fn record_executor_registered(&self, _executor_id: &str) {
        // Could track executor registration events if needed
        // For now, the count is sufficient
    }

    fn record_executor_deregistered(&self, _executor_id: &str) {
        // Could track executor deregistration events if needed
        // For now, the count is sufficient
    }

    // =========================================================================
    // Planning events
    // =========================================================================

    fn record_planning_duration(&self, _job_id: &str, duration_ms: u64) {
        #[expect(clippy::cast_precision_loss)]
        cluster::record_planning_duration(&self.node_id, duration_ms as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // OtelExecutorMetricsCollector Tests
    // =========================================================================

    #[test]
    fn test_executor_collector_new() {
        let collector = OtelExecutorMetricsCollector::new("test-node-1".to_string());
        assert_eq!(collector.node_id, "test-node-1");
    }

    #[test]
    fn test_executor_record_task_started() {
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        // Should not panic
        collector.record_task_started("job-1", 1, 0);
    }

    #[test]
    fn test_executor_record_task_failed() {
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        // Should not panic
        collector.record_task_failed("job-1", 1, 0, "timeout");
    }

    #[test]
    fn test_executor_record_shuffle_write() {
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        // Should not panic
        collector.record_shuffle_write("job-1", 1, 0, 1024, 100, 50);
    }

    #[test]
    fn test_executor_record_shuffle_read() {
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        // Should not panic
        collector.record_shuffle_read("job-1", 1, 0, 2048, 200, 75);
    }

    #[test]
    fn test_executor_record_memory_available() {
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        // Should not panic
        collector.record_memory_available(1024 * 1024 * 1024); // 1 GB
    }

    // =========================================================================
    // OtelSchedulerMetricsCollector Tests
    // =========================================================================

    #[test]
    fn test_scheduler_collector_new() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());
        assert_eq!(collector.node_id, "test-scheduler");
    }

    #[test]
    fn test_scheduler_job_lifecycle() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());
        let now = 1_000_000_u64;

        // Job lifecycle methods should not panic
        collector.record_submitted("job-1", now, now + 100);
        collector.record_completed("job-1", now, now + 5000);
        collector.record_failed("job-2", now, now + 1000);
        collector.record_cancelled("job-3");
    }

    #[test]
    fn test_scheduler_queue_sizes() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // Queue size methods should not panic
        collector.set_pending_tasks_queue_size(10);
        collector.set_pending_jobs_queue_size(5);
    }

    #[test]
    fn test_scheduler_gather_metrics_returns_none() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // OTel collector returns None since metrics are exported via OTel exporter
        let result = collector.gather_metrics();
        assert!(result.is_ok());
        assert!(result.expect("gather_metrics should succeed").is_none());
    }

    #[test]
    fn test_scheduler_stage_lifecycle() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // Stage lifecycle methods should not panic
        collector.record_stage_started("job-1", 1, 4);
        collector.record_stage_completed("job-1", 1, 1000);
        collector.record_stage_failed("job-2", 2, "resource_exhausted");
        collector.record_stage_retry("job-3", 3);
    }

    #[test]
    fn test_scheduler_task_scheduling() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // Task scheduling methods should not panic
        collector.record_task_scheduled("job-1", 1, "executor-1", 50);
        collector.record_task_completed("job-1", 1, "executor-1");
        collector.record_task_failed("job-2", 2, "executor-2", "network_error");
        collector.record_task_retry("job-3", 3);
    }

    #[test]
    fn test_scheduler_executor_management() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // Executor management methods should not panic
        collector.set_active_executor_count(3);
        collector.record_executor_registered("executor-1");
        collector.record_executor_deregistered("executor-1");
    }

    #[test]
    fn test_scheduler_planning_duration() {
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // Planning duration should not panic
        collector.record_planning_duration("job-1", 250);
    }

    // =========================================================================
    // Integration-style Tests
    // =========================================================================

    #[test]
    fn test_full_task_execution_flow_without_stage() {
        // Simulates a complete task execution flow from start to finish
        // Note: record_stage is tested via actual integration tests since it requires
        // a real QueryStageExecutor implementation
        let executor = OtelExecutorMetricsCollector::new("executor-1".to_string());
        let scheduler = OtelSchedulerMetricsCollector::new("scheduler-1".to_string());

        // Scheduler receives job and schedules task
        scheduler.record_stage_started("job-1", 1, 4);
        scheduler.record_task_scheduled("job-1", 1, "executor-1", 10);

        // Executor picks up and runs task
        executor.record_task_started("job-1", 1, 0);
        executor.record_shuffle_read_local("job-1", 1, 0, 512, 50, 10);
        executor.record_shuffle_read_remote("job-1", 1, 0, "executor-2", 512, 50, 20);

        // Task completes (simulated without calling record_stage)
        executor.record_shuffle_write("job-1", 1, 0, 512, 50, 15);

        // Scheduler records completion
        scheduler.record_task_completed("job-1", 1, "executor-1");
        scheduler.record_stage_completed("job-1", 1, 600);
    }

    #[test]
    fn test_task_failure_flow() {
        // Simulates a task failure scenario
        let executor = OtelExecutorMetricsCollector::new("executor-2".to_string());
        let scheduler = OtelSchedulerMetricsCollector::new("scheduler-1".to_string());

        // Scheduler schedules task
        scheduler.record_task_scheduled("job-fail", 1, "executor-2", 5);

        // Executor starts task but it fails
        executor.record_task_started("job-fail", 1, 0);
        executor.record_task_failed("job-fail", 1, 0, "out_of_memory");

        // Scheduler records failure and retries
        scheduler.record_task_failed("job-fail", 1, "executor-2", "out_of_memory");
        scheduler.record_task_retry("job-fail", 1);
        scheduler.record_stage_retry("job-fail", 1);
    }
}
