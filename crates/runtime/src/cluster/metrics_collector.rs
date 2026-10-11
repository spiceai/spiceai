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

use ballista_core::JobId;
use ballista_core::error::Result;
use ballista_core::extension::{ResultFetchMetricsCallback, ShuffleReadMetricsCallback};
use ballista_executor::execution_engine::QueryStageExecutor;
use ballista_executor::metrics::ExecutorMetricsCollector;
use ballista_scheduler::metrics::SchedulerMetricsCollector;
use opentelemetry::KeyValue;

use runtime_metrics::cluster;

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
    fn record_task_started(&self, _job_id: &JobId, _stage_id: usize, _partition: usize) {
        cluster::record_task_started(&self.node_id, "executor");

        // Also update executor-specific active task count
        let labels = [KeyValue::new("node_id", self.node_id.clone())];
        cluster::EXECUTOR_TASKS_ACTIVE.add(1, &labels);
    }

    fn record_stage(
        &self,
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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
        _job_id: &JobId,
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

    fn record_submitted(&self, _job_id: &JobId, _queued_at: u64, _submitted_at: u64) {
        // Job metrics are tracked at a higher level; we focus on stage/task metrics here.
        // This could be extended to track job queue latency if needed.
    }

    fn record_completed(&self, _job_id: &JobId, _queued_at: u64, _completed_at: u64) {
        // Job completion is tracked at a higher level.
    }

    fn record_failed(&self, _job_id: &JobId, _queued_at: u64, _failed_at: u64) {
        // Job failure is tracked at a higher level.
    }

    fn record_cancelled(&self, _job_id: &JobId) {
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

    fn record_stage_started(&self, _job_id: &JobId, _stage_id: usize, task_count: usize) {
        // Record the number of tasks per stage when it starts
        let labels = [KeyValue::new("node_id", self.node_id.clone())];
        cluster::SCHEDULER_TASKS_PER_STAGE.record(task_count as u64, &labels);
    }

    fn record_stage_completed(&self, _job_id: &JobId, _stage_id: usize, duration_ms: u64) {
        #[expect(clippy::cast_precision_loss)]
        let duration_ms_f64 = duration_ms as f64;
        // task_count is recorded in record_stage_started, use 0 here as placeholder
        // since we don't have it available at completion time
        cluster::record_stage_completed(&self.node_id, duration_ms_f64, 0);
    }

    fn record_stage_failed(&self, _job_id: &JobId, _stage_id: usize, error_type: &str) {
        cluster::record_stage_failed(&self.node_id, error_type);
    }

    fn record_stage_retry(&self, _job_id: &JobId, _stage_id: usize) {
        cluster::record_stage_retry(&self.node_id);
    }

    // =========================================================================
    // Task scheduling events
    // =========================================================================

    fn record_task_scheduled(
        &self,
        _job_id: &JobId,
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

    fn record_task_completed(&self, _job_id: &JobId, _stage_id: usize, _executor_id: &str) {
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
        _job_id: &JobId,
        _stage_id: usize,
        _executor_id: &str,
        error_type: &str,
    ) {
        cluster::record_task_failed(&self.node_id, "scheduler", error_type);
    }

    fn record_task_retry(&self, _job_id: &JobId, _stage_id: usize) {
        cluster::record_task_retry(&self.node_id, "scheduler");
    }

    fn record_task_shuffle_affinity_hit(
        &self,
        _job_id: &JobId,
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
        _job_id: &JobId,
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

    fn record_planning_duration(&self, _job_id: &JobId, duration_ms: u64) {
        #[expect(clippy::cast_precision_loss)]
        cluster::record_planning_duration(&self.node_id, duration_ms as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::proto::MetricType;

    /// Set on the child process `run_in_own_process` spawns.
    const OWN_PROCESS_ENV: &str = "SPICE_RUNTIME_TEST_OWN_PROCESS";

    /// Re-runs the test `name` (in this module) alone in a fresh process of this
    /// test binary and asserts it passed there. Returns `true` only inside that
    /// child, where the caller runs the test body; returns `false` in the parent
    /// once the child has passed.
    ///
    /// The cluster instruments are `LazyLock`s bound to whichever meter provider
    /// is global when the cluster meter is first touched, and under `cargo test`
    /// sibling tests share both that binding and the series they write, so a
    /// test that reads the instruments back needs a process of its own.
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

    /// Installs a global meter provider that exports to a Prometheus registry
    /// configured exactly as `/metrics` serves it, and returns that registry.
    fn install_prometheus_meter_provider() -> prometheus::Registry {
        let registry = prometheus::Registry::new();
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_resource(opentelemetry_sdk::Resource::builder().build())
            .with_reader(
                crate::prometheus_reader(registry.clone()).expect("to build the prometheus reader"),
            )
            .build();
        opentelemetry::global::set_meter_provider(provider);
        registry
    }

    /// One exported series: a counter or gauge reads as its value, a histogram
    /// as its sample count and sum.
    #[derive(Debug, PartialEq)]
    enum Series {
        Value(f64),
        Histogram { count: u64, sum: f64 },
    }

    /// The series of metric `name` whose label set is exactly `labels`, or
    /// `None` when the registry exported no such series.
    fn series(
        registry: &prometheus::Registry,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Option<Series> {
        let mut expected: Vec<(String, String)> = labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        expected.sort();
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)?;
        let field_type = family.get_field_type();
        family.get_metric().iter().find_map(|metric| {
            let mut actual: Vec<(String, String)> = metric
                .get_label()
                .iter()
                .map(|label| (label.name().to_string(), label.value().to_string()))
                .collect();
            actual.sort();
            if actual != expected {
                return None;
            }
            Some(match field_type {
                MetricType::COUNTER => Series::Value(metric.get_counter().value()),
                MetricType::GAUGE => Series::Value(metric.get_gauge().value()),
                MetricType::HISTOGRAM => {
                    let histogram = metric.get_histogram();
                    Series::Histogram {
                        count: histogram.get_sample_count(),
                        sum: histogram.get_sample_sum(),
                    }
                }
                other => panic!("unexpected metric type {other:?} for {name}"),
            })
        })
    }

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
        if !run_in_own_process("test_executor_record_task_started") {
            return;
        }
        let registry = install_prometheus_meter_provider();
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        collector.record_task_started(&JobId::new("job-1"), 1, 0);

        assert_eq!(
            series(
                &registry,
                "executor_tasks_active",
                &[("node_id", "test-executor")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "test-executor"), ("role", "executor")]
            ),
            Some(Series::Value(1.0))
        );
    }

    #[test]
    fn test_executor_record_task_failed() {
        if !run_in_own_process("test_executor_record_task_failed") {
            return;
        }
        let registry = install_prometheus_meter_provider();
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        collector.record_task_started(&JobId::new("job-1"), 1, 0);
        collector.record_task_failed(&JobId::new("job-1"), 1, 0, "timeout");

        assert_eq!(
            series(
                &registry,
                "executor_task_failures",
                &[("node_id", "test-executor"), ("error_type", "timeout")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "executor_tasks_total",
                &[("node_id", "test-executor"), ("status", "failed")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_task_failures",
                &[
                    ("node_id", "test-executor"),
                    ("role", "executor"),
                    ("error_type", "timeout")
                ]
            ),
            Some(Series::Value(1.0))
        );
        // The failed task is no longer running.
        assert_eq!(
            series(
                &registry,
                "executor_tasks_active",
                &[("node_id", "test-executor")]
            ),
            Some(Series::Value(0.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "test-executor"), ("role", "executor")]
            ),
            Some(Series::Value(0.0))
        );
    }

    #[test]
    fn test_executor_record_shuffle_write() {
        if !run_in_own_process("test_executor_record_shuffle_write") {
            return;
        }
        let registry = install_prometheus_meter_provider();
        let collector = OtelExecutorMetricsCollector::new("test-executor".to_string());
        collector.record_shuffle_write(&JobId::new("job-1"), 1, 0, 1024, 100, 50);

        let node = [("node_id", "test-executor")];
        assert_eq!(
            series(&registry, "executor_shuffle_write_bytes", &node),
            Some(Series::Value(1024.0))
        );
        assert_eq!(
            series(&registry, "executor_shuffle_write_rows", &node),
            Some(Series::Value(100.0))
        );
        assert_eq!(
            series(&registry, "executor_shuffle_write_duration_ms", &node),
            Some(Series::Histogram {
                count: 1,
                sum: 50.0
            })
        );
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
        collector.record_stage_started(&JobId::new("job-1"), 1, 4);
        collector.record_stage_completed(&JobId::new("job-1"), 1, 1000);
        collector.record_stage_failed(&JobId::new("job-2"), 2, "resource_exhausted");
        collector.record_stage_retry(&JobId::new("job-3"), 3);
    }

    #[test]
    fn test_scheduler_task_scheduling() {
        if !run_in_own_process("test_scheduler_task_scheduling") {
            return;
        }
        let registry = install_prometheus_meter_provider();
        let collector = OtelSchedulerMetricsCollector::new("test-scheduler".to_string());

        // Two tasks are scheduled; one completes, the other fails and is retried.
        collector.record_task_scheduled(&JobId::new("job-1"), 1, "executor-1", 50);
        collector.record_task_scheduled(&JobId::new("job-2"), 2, "executor-2", 30);
        collector.record_task_completed(&JobId::new("job-1"), 1, "executor-1");
        collector.record_task_failed(&JobId::new("job-2"), 2, "executor-2", "network_error");
        collector.record_task_retry(&JobId::new("job-2"), 2);

        let node = [("node_id", "test-scheduler")];
        assert_eq!(
            series(&registry, "scheduler_task_scheduling_latency_ms", &node),
            Some(Series::Histogram {
                count: 2,
                sum: 80.0
            })
        );
        assert_eq!(
            series(&registry, "scheduler_executor_assignments", &node),
            Some(Series::Value(2.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_total",
                &[
                    ("node_id", "test-scheduler"),
                    ("role", "scheduler"),
                    ("status", "completed")
                ]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_total",
                &[
                    ("node_id", "test-scheduler"),
                    ("role", "scheduler"),
                    ("status", "failed")
                ]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_task_failures",
                &[
                    ("node_id", "test-scheduler"),
                    ("role", "scheduler"),
                    ("error_type", "network_error")
                ]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_task_retries",
                &[("node_id", "test-scheduler"), ("role", "scheduler")]
            ),
            Some(Series::Value(1.0))
        );
        // Both tasks have finished, one way or the other.
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "test-scheduler"), ("role", "scheduler")]
            ),
            Some(Series::Value(0.0))
        );
    }

    // =========================================================================
    // Integration-style Tests
    // =========================================================================

    #[test]
    fn test_full_task_execution_flow_without_stage() {
        if !run_in_own_process("test_full_task_execution_flow_without_stage") {
            return;
        }
        let registry = install_prometheus_meter_provider();
        // Simulates a task execution flow from scheduling to the scheduler's
        // completion. Note: the executor's own completion callback,
        // `record_stage`, needs a real `QueryStageExecutor` and is not part of
        // this flow, so the executor still counts the task as running.
        let executor = OtelExecutorMetricsCollector::new("executor-1".to_string());
        let scheduler = OtelSchedulerMetricsCollector::new("scheduler-1".to_string());

        // Scheduler receives job and schedules task
        scheduler.record_stage_started(&JobId::new("job-1"), 1, 4);
        scheduler.record_task_scheduled(&JobId::new("job-1"), 1, "executor-1", 10);

        // Executor picks up and runs task
        executor.record_task_started(&JobId::new("job-1"), 1, 0);
        executor.record_shuffle_read_local(&JobId::new("job-1"), 1, 0, 512, 50, 10);
        executor.record_shuffle_read_remote(&JobId::new("job-1"), 1, 0, "executor-2", 512, 50, 20);

        // Task completes (simulated without calling record_stage)
        executor.record_shuffle_write(&JobId::new("job-1"), 1, 0, 512, 50, 15);

        // Scheduler records completion
        scheduler.record_task_completed(&JobId::new("job-1"), 1, "executor-1");
        scheduler.record_stage_completed(&JobId::new("job-1"), 1, 600);

        let scheduler_node = [("node_id", "scheduler-1")];
        assert_eq!(
            series(
                &registry,
                "scheduler_task_scheduling_latency_ms",
                &scheduler_node
            ),
            Some(Series::Histogram {
                count: 1,
                sum: 10.0
            })
        );
        assert_eq!(
            series(&registry, "scheduler_executor_assignments", &scheduler_node),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "scheduler-1"), ("role", "scheduler")]
            ),
            Some(Series::Value(0.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_total",
                &[
                    ("node_id", "scheduler-1"),
                    ("role", "scheduler"),
                    ("status", "completed")
                ]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "scheduler_stages_total",
                &[("node_id", "scheduler-1"), ("status", "completed")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(&registry, "scheduler_stage_duration_ms", &scheduler_node),
            Some(Series::Histogram {
                count: 1,
                sum: 600.0
            })
        );

        let executor_node = [("node_id", "executor-1")];
        for (name, expected) in [
            ("executor_shuffle_read_local_bytes", 512.0),
            ("executor_shuffle_read_local_rows", 50.0),
            ("executor_shuffle_read_local_count", 1.0),
            ("executor_shuffle_read_remote_bytes", 512.0),
            ("executor_shuffle_read_remote_rows", 50.0),
            ("executor_shuffle_read_remote_count", 1.0),
            ("executor_shuffle_write_bytes", 512.0),
            ("executor_shuffle_write_rows", 50.0),
            // Still running: `record_stage` is not part of this flow.
            ("executor_tasks_active", 1.0),
        ] {
            assert_eq!(
                series(&registry, name, &executor_node),
                Some(Series::Value(expected)),
                "{name}"
            );
        }
        assert_eq!(
            series(
                &registry,
                "executor_shuffle_read_local_duration_ms",
                &executor_node
            ),
            Some(Series::Histogram {
                count: 1,
                sum: 10.0
            })
        );
        assert_eq!(
            series(
                &registry,
                "executor_shuffle_read_remote_duration_ms",
                &executor_node
            ),
            Some(Series::Histogram {
                count: 1,
                sum: 20.0
            })
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "executor-1"), ("role", "executor")]
            ),
            Some(Series::Value(1.0))
        );
    }

    #[test]
    fn test_task_failure_flow() {
        if !run_in_own_process("test_task_failure_flow") {
            return;
        }
        let registry = install_prometheus_meter_provider();
        // Simulates a task failure scenario
        let executor = OtelExecutorMetricsCollector::new("executor-2".to_string());
        let scheduler = OtelSchedulerMetricsCollector::new("scheduler-1".to_string());

        // Scheduler schedules task
        scheduler.record_task_scheduled(&JobId::new("job-fail"), 1, "executor-2", 5);

        // Executor starts task but it fails
        executor.record_task_started(&JobId::new("job-fail"), 1, 0);
        executor.record_task_failed(&JobId::new("job-fail"), 1, 0, "out_of_memory");

        // Scheduler records failure and retries
        scheduler.record_task_failed(&JobId::new("job-fail"), 1, "executor-2", "out_of_memory");
        scheduler.record_task_retry(&JobId::new("job-fail"), 1);
        scheduler.record_stage_retry(&JobId::new("job-fail"), 1);

        assert_eq!(
            series(
                &registry,
                "executor_task_failures",
                &[("node_id", "executor-2"), ("error_type", "out_of_memory")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "executor_tasks_total",
                &[("node_id", "executor-2"), ("status", "failed")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_task_failures",
                &[
                    ("node_id", "scheduler-1"),
                    ("role", "scheduler"),
                    ("error_type", "out_of_memory")
                ]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_task_retries",
                &[("node_id", "scheduler-1"), ("role", "scheduler")]
            ),
            Some(Series::Value(1.0))
        );
        assert_eq!(
            series(
                &registry,
                "scheduler_stage_retries",
                &[("node_id", "scheduler-1")]
            ),
            Some(Series::Value(1.0))
        );
        // The failed task is no longer running on either side.
        assert_eq!(
            series(
                &registry,
                "executor_tasks_active",
                &[("node_id", "executor-2")]
            ),
            Some(Series::Value(0.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "executor-2"), ("role", "executor")]
            ),
            Some(Series::Value(0.0))
        );
        assert_eq!(
            series(
                &registry,
                "node_tasks_active",
                &[("node_id", "scheduler-1"), ("role", "scheduler")]
            ),
            Some(Series::Value(0.0))
        );
    }
}
