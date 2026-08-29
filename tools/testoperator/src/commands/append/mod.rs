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
use crate::{args::AppendTestArgs, health::HealthMonitor};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use test_framework::{
    TestType,
    anyhow::{self, Context},
    app::App,
    arrow::{
        self,
        array::{AsArray, RecordBatch},
        util::pretty::print_batches,
    },
    execution::{FlightExecutor, QueryExecutor},
    futures::TryStreamExt,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    queries::{
        QueryOverrides, QuerySet, TableWithRowCount,
        validation::{self, QueryValidationResult},
    },
    spiced::SpicedInstance,
    spicepod::acceleration::RefreshMode,
    spicetest::{SpiceTest, append::NotStarted},
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

/// How long to wait for the tables to reach their expected row counts. The last
/// load's refresh can still be in flight when the test window ends.
const VERIFICATION_SETTLE_TIMEOUT: Duration = Duration::from_mins(3);

/// How often to re-count the tables while waiting for them to settle.
const VERIFICATION_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How many rows of a failing query's result to print. A TPC-H answer can run to
/// thousands of rows, and the failure reason names the first row that diverged.
const MAX_PRINTED_FAILURE_ROWS: usize = 20;

pub(crate) async fn run(args: &AppendTestArgs) -> anyhow::Result<()> {
    if args.test_args.common.concurrency == 0 {
        return Err(anyhow::anyhow!(
            "Concurrency should be greater than 0 for an append test"
        ));
    }

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
            .with_query_set(
                query_set.clone(),
                query_overrides,
                args.test_args.scale_factor,
            )
            .await?
            .with_parallel_count(args.test_args.common.concurrency)
            .with_end_duration(Duration::from_secs(args.test_args.common.duration))
            .with_tempdir_path(start_request.get_tempdir_path())
            .with_load_interval(Duration::from_secs(args.load_interval))
            .with_load_steps(args.load_steps)
            .with_conflict_data(args.with_conflict_data)
            .with_retention_test_data(args.with_retention_data),
    )
    .with_progress_bars(!args.test_args.common.disable_progress_bars)
    // Append tests start from a small data subset and load incrementally, so queries legitimately return 0 rows
    .with_validate_row_count(false)
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
    let memory_readings = spiced_instance
        .process()
        .map(|process| process.watch_memory(&memory_token));

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
    let test = match append_test.wait().await {
        Ok(test) => test,
        Err(e) => {
            if let Some(handle) = memory_readings {
                let _ = observe_memory(memory_token, handle).await;
            }
            return Err(e);
        }
    };
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Append)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    let memory_usage = match memory_readings {
        Some(handle) => Some(observe_memory(memory_token, handle).await?),
        None => None,
    };

    let mut test_metrics = test_metrics
        .with_spiced_version(metrics.spiced_version.clone())
        .with_testoperator_commit_sha(metrics.commit_sha.clone())
        .with_branch_name(metrics.branch_name.clone());

    if let Some((max_memory, median_memory)) = memory_usage {
        test_metrics = test_metrics.with_memory(max_memory, median_memory);
    }

    let verification_result = verify_appended_data(
        &spiced_instance,
        &query_set,
        query_overrides,
        args.test_args.scale_factor.unwrap_or(1.0),
        args.test_args.validate,
    )
    .await;

    let metrics = match memory_usage {
        Some((max_memory, _)) => metrics.with_memory_usage(max_memory),
        None => metrics,
    };
    let records = metrics.build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Test passes only if the appended data verifies and every query succeeded.
    let test_status: TestStatus = (verification_result.is_ok() && test_succeeded).into();
    test_metrics.emit(test_status).await?;

    spiced_instance.stop()?;
    let health_report = health_report?;

    verification_result?;
    if let Some(message) = health_report.failure_message() {
        // Health check failures are logged as warnings but don't fail the test
        eprintln!("Warning: {message}");
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

/// Verifies the data the append test left behind.
///
/// Row counts prove only that the right *amount* of data arrived. Comparing the
/// query results against the expected answers proves the right *data* arrived:
/// a duplicated append, a retention policy deleting the wrong row, or a
/// corrupted column can all land on the expected row count.
///
/// No expected-answer query selects the appended `*_created_at` column, so this
/// pass does not observe which of two conflicting versions an upsert kept.
async fn verify_appended_data(
    spiced: &SpicedInstance,
    query_set: &QuerySet,
    query_overrides: Option<QueryOverrides>,
    scale_factor: f64,
    validate_results: bool,
) -> anyhow::Result<()> {
    println!("Verifying appended data");

    // The same queries ran throughout the test against partially loaded data, so
    // a cached result would report an earlier load step.
    let spice_client = Arc::new(spiced.spice_client(None, true).await?);

    check_table_counts(&spice_client, query_set, scale_factor).await?;

    if !validate_results {
        println!("Skipping query result verification, pass --validate to enable it");
        return Ok(());
    }

    check_query_results(&spice_client, query_set, query_overrides, scale_factor).await
}

/// Counts every table in the query set, describing those outside a 0.01% margin
/// of the expected count.
async fn table_count_mismatches(
    spice_client: &spiceai::Client,
    query_set: &QuerySet,
    scale_factor: f64,
) -> anyhow::Result<Vec<String>> {
    let mut mismatches = Vec::new();

    for TableWithRowCount {
        name,
        count: expected_count,
    } in query_set.row_counts()
    {
        let expected_count = f64::from(expected_count) * scale_factor;
        let sql = format!("SELECT COUNT(*) FROM {name}");
        let batches = spice_client
            .sql(&sql)
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
            mismatches.push(format!(
                "table {name} has {count} rows, expected {expected_count}"
            ));
        }
    }

    Ok(mismatches)
}

/// Waits, up to [`VERIFICATION_SETTLE_TIMEOUT`], for every table to reach its
/// expected row count, so verification runs against the fully loaded dataset.
async fn check_table_counts(
    spice_client: &spiceai::Client,
    query_set: &QuerySet,
    scale_factor: f64,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + VERIFICATION_SETTLE_TIMEOUT;
    let mismatches = loop {
        let mismatches = table_count_mismatches(spice_client, query_set, scale_factor).await?;
        if mismatches.is_empty() || Instant::now() >= deadline {
            break mismatches;
        }

        tokio::time::sleep(VERIFICATION_POLL_INTERVAL).await;
    };

    if !mismatches.is_empty() {
        return Err(anyhow::anyhow!(
            "Table row counts do not match expected values: {}",
            mismatches.join("; ")
        ));
    }

    Ok(())
}

/// Runs each query once against the appended data and compares the result
/// against its expected answer.
async fn check_query_results(
    spice_client: &Arc<spiceai::Client>,
    query_set: &QuerySet,
    query_overrides: Option<QueryOverrides>,
    scale_factor: f64,
) -> anyhow::Result<()> {
    let queries = query_set
        .get_queries(query_overrides, None, None, Some(scale_factor))
        .await?;
    let executor = FlightExecutor::new(Arc::clone(spice_client));

    println!(
        "Verifying {} query results against the expected answers",
        queries.len()
    );

    let mut skipped = Vec::new();
    let mut failures = Vec::new();

    for query in &queries {
        // Gate before running the query, so a query that can't be validated
        // isn't executed for a result nothing compares.
        if !validation::should_validate_with_static_tpch_answer(query, scale_factor) {
            skipped.push(query.name.as_ref());
            continue;
        }

        let batches = match executor.execute(query, true).await {
            Ok(result) => result.batches.unwrap_or_default(),
            Err(e) => {
                eprintln!("Query '{}' failed to run: {e}", query.name);
                failures.push(format!("{}: failed to run: {e}", query.name));
                continue;
            }
        };

        if let QueryValidationResult::Fail(reason) =
            validation::validate_tpch_query(query, &batches)?
        {
            eprintln!("\nQuery '{}' returned unexpected results", query.name);
            eprintln!("Query SQL: {}", query.sql);
            eprintln!("Validation failure reason: {reason:?}");
            eprintln!("\nActual results:");
            print_result_head(&batches);
            eprintln!();
            failures.push(format!("{}: {reason:?}", query.name));
        }
    }

    let validated = queries.len() - skipped.len() - failures.len();
    println!(
        "Verified {validated}/{total} query results ({skipped_count} skipped, {failed_count} failed)",
        total = queries.len(),
        skipped_count = skipped.len(),
        failed_count = failures.len(),
    );

    if !failures.is_empty() {
        return Err(anyhow::anyhow!(
            "Query results do not match expected values: {}",
            failures.join("; ")
        ));
    }

    if validated == 0 {
        eprintln!(
            "Warning: no {query_set} query has an expected answer at scale factor {scale_factor}, so the appended data was verified by row count only"
        );
    } else if !skipped.is_empty() {
        println!(
            "Skipped {count} queries with no expected answer at scale factor {scale_factor}: {names}",
            count = skipped.len(),
            names = skipped.join(", "),
        );
    }

    Ok(())
}

/// Prints the first [`MAX_PRINTED_FAILURE_ROWS`] rows of a query result, so a
/// large result doesn't bury the rest of the run's output.
fn print_result_head(batches: &[RecordBatch]) {
    let mut head = Vec::new();
    let mut remaining = MAX_PRINTED_FAILURE_ROWS;
    for batch in batches {
        if remaining == 0 {
            break;
        }

        let rows = remaining.min(batch.num_rows());
        head.push(batch.slice(0, rows));
        remaining -= rows;
    }

    match arrow::util::pretty::pretty_format_batches(&head) {
        Ok(pretty) => eprintln!("{pretty}"),
        Err(e) => eprintln!("Failed to format actual results: {e}"),
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows > MAX_PRINTED_FAILURE_ROWS {
        eprintln!("... {} more rows", total_rows - MAX_PRINTED_FAILURE_ROWS);
    }
}
