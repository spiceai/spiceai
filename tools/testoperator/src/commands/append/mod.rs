/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use super::get_app_and_start_request;
use crate::{args::AppendTestArgs, health::HealthMonitor, wait_test_and_memory};
use std::time::Duration;
use test_framework::{
    TestType,
    anyhow::{self, Context},
    app::App,
    arrow::{self, array::AsArray, util::pretty::print_batches},
    futures::TryStreamExt,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    queries::{QueryOverrides, QuerySet, TableWithRowCount},
    spiced::SpicedInstance,
    spicepod::acceleration::RefreshMode,
    spicetest::{SpiceTest, append::NotStarted},
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

pub(crate) async fn run(args: &AppendTestArgs) -> anyhow::Result<()> {
    let query_set = args.test_args.load_query_set()?;
    let query_overrides = args
        .test_args
        .query_overrides
        .clone()
        .map(QueryOverrides::from);

    let (app, start_request) = get_app_and_start_request(&args.test_args.common).await?;

    let test_metrics = AppendTestMetrics::new(app.name.clone(), query_set.to_string())
        .with_spiced_commit_sha(
            std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string()),
        );

    check_app_is_appendable(&app)?;

    println!("Running append test");

    let append_test = match SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_query_set(query_set.clone(), query_overrides)
            .await?
            .with_parallel_count(1)
            .with_end_duration(Duration::from_secs(args.test_args.common.duration))
            .with_tempdir_path(start_request.get_tempdir_path())
            .with_load_interval(Duration::from_secs(args.load_interval))
            .with_load_steps(args.load_steps)
            .with_conflict_data(args.with_conflict_data)
            .with_retention_test_data(args.with_retention_data),
    )
    .with_progress_bars(!args.test_args.common.disable_progress_bars)
    .start_appending()
    .await
    {
        Ok(test) => test,
        Err(e) => {
            test_metrics.emit(TestStatus::Failed).await?;
            return Err(e);
        }
    };

    let mut spiced_instance = match SpicedInstance::start(start_request).await {
        Ok(instance) => instance,
        Err(e) => {
            test_metrics.emit(TestStatus::Failed).await?;
            return Err(e);
        }
    };
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process()?.watch_memory(&memory_token);

    if let Err(e) = spiced_instance
        .wait_for_ready(Duration::from_secs(args.test_args.common.ready_wait))
        .await
    {
        test_metrics.emit(TestStatus::Failed).await?;
        return Err(e);
    }
    let health_monitor = HealthMonitor::spawn()?;

    let append_test = append_test
        .with_spiced_instance(spiced_instance)
        .start_test()
        .await?;
    let test = wait_test_and_memory!(append_test, memory_token, memory_readings);
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Append)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    let (max_memory, median_memory) = observe_memory(memory_token, memory_readings).await?;

    let test_metrics = test_metrics
        .with_spiced_version(metrics.spiced_version.clone())
        .with_testoperator_commit_sha(metrics.commit_sha.clone())
        .with_branch_name(metrics.branch_name.clone())
        .with_memory(max_memory, median_memory);

    let table_count_result = check_table_counts(
        &spiced_instance,
        &query_set,
        args.test_args.scale_factor.unwrap_or(1.0),
    )
    .await;

    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Test passes only if: (1) table row counts match expected values, (2) all queries succeeded, and (3) health checks passed
    let test_status: TestStatus =
        (table_count_result.is_ok() && test_succeeded && health_report.is_ok()).into();
    test_metrics.emit(test_status).await?;

    spiced_instance.stop()?;
    let health_report = health_report?;

    table_count_result?;
    if let Some(message) = health_report.failure_message() {
        return Err(anyhow::anyhow!(message));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestStatus {
    /// Test completed successfully
    Passed,
    /// Test failed
    Failed,
}

impl From<bool> for TestStatus {
    fn from(passed: bool) -> Self {
        if passed {
            TestStatus::Passed
        } else {
            TestStatus::Failed
        }
    }
}

impl TestStatus {
    /// Convert `TestStatus` to a u64 value for metrics recording (1 = passed, 0 = failed)
    #[must_use]
    pub fn to_u64(self) -> u64 {
        match self {
            TestStatus::Passed => 1,
            TestStatus::Failed => 0,
        }
    }
}

/// Builder for emitting append test metrics.
struct AppendTestMetrics {
    app_name: String,
    spiced_version: Option<String>,
    query_set: String,
    testoperator_commit_sha: Option<String>,
    spiced_commit_sha: Option<String>,
    branch_name: Option<String>,
    max_memory: Option<f64>,
    median_memory: Option<f64>,
}

impl AppendTestMetrics {
    fn new(app_name: impl Into<String>, query_set: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            query_set: query_set.into(),
            spiced_version: None,
            testoperator_commit_sha: None,
            spiced_commit_sha: None,
            branch_name: None,
            max_memory: None,
            median_memory: None,
        }
    }

    fn with_spiced_version(mut self, version: impl Into<String>) -> Self {
        self.spiced_version = Some(version.into());
        self
    }

    fn with_testoperator_commit_sha(mut self, sha: impl Into<String>) -> Self {
        self.testoperator_commit_sha = Some(sha.into());
        self
    }

    fn with_spiced_commit_sha(mut self, sha: impl Into<String>) -> Self {
        self.spiced_commit_sha = Some(sha.into());
        self
    }

    fn with_branch_name(mut self, name: impl Into<String>) -> Self {
        self.branch_name = Some(name.into());
        self
    }

    fn with_memory(mut self, max_memory: f64, median_memory: f64) -> Self {
        self.max_memory = Some(max_memory);
        self.median_memory = Some(median_memory);
        self
    }

    /// Emit metrics and telemetry for the test result.
    async fn emit(self, test_status: TestStatus) -> anyhow::Result<()> {
        let resource = Resource::builder_empty()
            .with_attributes(vec![
                KeyValue::new("service.name", "testoperator"),
                KeyValue::new("type", "append_test"),
                KeyValue::new("name", self.app_name),
                KeyValue::new(
                    "spiced_version",
                    self.spiced_version.unwrap_or_else(|| "unknown".to_string()),
                ),
                KeyValue::new("query_set", self.query_set),
                KeyValue::new(
                    "testoperator_commit_sha",
                    self.testoperator_commit_sha
                        .unwrap_or_else(|| "unknown".to_string()),
                ),
                KeyValue::new(
                    "spiced_commit_sha",
                    self.spiced_commit_sha
                        .unwrap_or_else(|| "unknown".to_string()),
                ),
                KeyValue::new(
                    "branch_name",
                    self.branch_name.unwrap_or_else(|| "unknown".to_string()),
                ),
            ])
            .build();

        // Create telemetry with resource upfront, before recording any metrics
        let telemetry = Telemetry::new_with_resource(&resource, "SPICEAI_BENCHMARK_METRICS_KEY");

        crate::metrics::STATUS.record(test_status.to_u64(), &[]);

        if let Some(max_mem) = self.max_memory {
            crate::metrics::PEAK_MEMORY_USAGE.record(max_mem * 1024.0, &[]);
        }
        if let Some(median_mem) = self.median_memory {
            crate::metrics::MEDIAN_MEMORY_USAGE.record(median_mem * 1024.0, &[]);
        }

        telemetry.emit().await
    }
}

fn check_app_is_appendable(app: &App) -> anyhow::Result<()> {
    for dataset in &app.datasets {
        // check that each dataset has an append-mode accelerator
        if dataset
            .acceleration
            .as_ref()
            .is_none_or(|a| a.refresh_mode != Some(RefreshMode::Append))
        {
            return Err(anyhow::anyhow!(
                "Dataset {} does not have an append-mode accelerator",
                dataset.name
            ));
        }

        // check that each dataset uses a supported append-mode source
        if dataset.from.split(':').next() != Some("file") {
            return Err(anyhow::anyhow!(
                "Dataset {} does not use a supported append-mode source",
                dataset.name
            ));
        }
    }

    Ok(())
}

async fn check_table_counts(
    spiced: &SpicedInstance,
    query_set: &QuerySet,
    scale_factor: f64,
) -> anyhow::Result<()> {
    let spice_client = spiced.spice_client(None, false).await?;

    let mut any_count_mismatch = false;
    for TableWithRowCount {
        name,
        count: expected_count,
    } in query_set.row_counts()
    {
        let expected_count = f64::from(expected_count) * scale_factor;
        let sql = format!("SELECT COUNT(*) FROM {name}");
        let batches = spice_client
            .query(&sql)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        if batches.len() != 1 {
            return Err(anyhow::anyhow!(
                "Expected 1 batch, got {} batches",
                batches.len()
            ));
        }
        let count = batches[0]
            .column(0)
            .as_primitive_opt::<arrow::datatypes::Int64Type>()
            .context("Failed to get count as a Int64Type")?
            .value(0);

        let count = f64::from(u32::try_from(count)?);
        // Allow a 0.01% margin of error
        let upper_bound = expected_count * 1.0001;
        let lower_bound = expected_count * 0.9999;
        if !(count <= upper_bound && count >= lower_bound) {
            println!("Table {name} has {count} rows, expected {expected_count}");
            any_count_mismatch = true;
        }
    }

    if any_count_mismatch {
        return Err(anyhow::anyhow!(
            "Table row counts do not match expected values"
        ));
    }

    Ok(())
}
