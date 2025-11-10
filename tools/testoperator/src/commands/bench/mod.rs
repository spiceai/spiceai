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

use super::{RowCounts, get_app_and_start_request};
use crate::{
    args::DatasetTestArgs, health::HealthMonitor, spiced_metrics::MetricsScraper,
    wait_test_and_memory,
};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use test_framework::{
    TestType, anyhow,
    app::App,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicepod::acceleration::Mode,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::{observe_memory, recursively_get_dir_size},
};

fn emit_acceleration_size_if_applicable(app: &App, app_path: &Path) -> anyhow::Result<()> {
    // determine if any dataset has acceleration enabled with a file mode engine
    if !app.datasets.iter().any(|ds| {
        ds.acceleration.as_ref().is_some_and(|accel| {
            accel.mode == Mode::File
                && accel.enabled
                && matches!(
                    accel.engine.as_deref(),
                    Some("sqlite" | "duckdb" | "cayenne")
                )
        })
    }) {
        return Ok(());
    }

    // calculate the total size of all files inside .spice
    let spice_dir = app_path.join(".spice");
    let total_size = recursively_get_dir_size(&spice_dir)?;

    println!("Total acceleration size on disk: {total_size} bytes");

    crate::metrics::ACCELERATION_SIZE_BYTES.record(total_size.try_into().unwrap_or_default(), &[]);

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<RowCounts> {
    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let (app, start_request) = get_app_and_start_request(&args.common).await?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let ready_wait_start = Instant::now();

    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process().watch_memory(&memory_token);

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    let ready_wait_duration = ready_wait_start.elapsed();
    let health_monitor = HealthMonitor::spawn()?;

    // Start metrics scraper if enabled
    let metrics_scraper = if args.common.scrape_spiced_metrics {
        Some(MetricsScraper::spawn()?)
    } else {
        None
    };

    // baseline run
    println!("Running benchmark test");

    let benchmark_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_query_set(queries.clone())
            .with_parallel_count(1)
            .with_end_condition(EndCondition::QuerySetCompleted(5))
            .with_validate(args.validate)
            .with_disable_caching(args.disable_caching)
            .with_scale_factor(args.scale_factor.unwrap_or(1.0))
            .with_http_client(args.http_clients),
    )
    .with_spiced_instance(spiced_instance)
    .with_explain_plan_snapshot()
    .with_results_snapshot(snapshot_predicate)
    .with_progress_bars(!args.common.disable_progress_bars)
    .start()
    .await?;

    let test = wait_test_and_memory!(benchmark_test, memory_token, memory_readings);

    let row_counts = test.validate_returned_row_counts()?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    let (max_memory, median_memory) = observe_memory(memory_token, memory_readings).await?;

    let commit_sha = metrics.commit_sha.clone();
    let spiced_commit_sha = std::env::var("SPICED_COMMIT").unwrap_or("unknown".to_string());
    let spiced_version = metrics.spiced_version.clone();
    let app_name = app.name.clone();
    let benchmark_resource = Resource::new(vec![
        KeyValue::new("service.name", "testoperator"),
        KeyValue::new("type", "benchmark_query"),
        KeyValue::new("name", app_name.clone()),
        KeyValue::new("spiced_version", spiced_version.clone()),
        KeyValue::new("query_set", query_set.to_string()),
        KeyValue::new("testoperator_commit_sha", commit_sha.clone()),
        KeyValue::new("spiced_commit_sha", spiced_commit_sha),
        KeyValue::new("branch_name", metrics.branch_name.clone()),
        KeyValue::new("scale_factor", args.scale_factor.unwrap_or(1.0).to_string()),
    ]);

    let telemetry = Telemetry::new(&benchmark_resource, "SPICEAI_BENCHMARK_METRICS_KEY");

    let mut failures = Vec::new();
    for query in &metrics.metrics {
        let query_name = &query.query_name;
        let row_count = row_counts.get(query_name).unwrap_or(&0);
        let attributes = vec![KeyValue::new("query_name", query_name.to_string())];

        let status: u64 = u64::from(match &query.query_status {
            QueryStatus::Passed => true,
            QueryStatus::Failed(reason) => {
                if let Some(reason) = reason {
                    failures.push(format!("{query_name}: {reason}"));
                } else {
                    failures.push(format!("{query_name}: failed with an undetermined error"));
                }
                false
            }
        });

        crate::metrics::QUERY_STATUS.record(status, &attributes);
        crate::metrics::MEDIAN_DURATION.record(query.median_duration_ms, &attributes);
        crate::metrics::MIN_DURATION.record(query.min_duration_ms, &attributes);
        crate::metrics::MAX_DURATION.record(query.max_duration_ms, &attributes);
        crate::metrics::ITERATIONS.record(query.iterations.try_into()?, &attributes);
        crate::metrics::P90_DURATION.record(query.percentile_90_duration_ms, &attributes);
        crate::metrics::P95_DURATION.record(query.percentile_95_duration_ms, &attributes);
        crate::metrics::P99_DURATION.record(query.percentile_99_duration_ms, &attributes);
        crate::metrics::ROW_COUNT.record((*row_count).try_into()?, &attributes);
    }

    crate::metrics::READY_DURATION.record(ready_wait_duration.as_millis().try_into()?, &[]);
    crate::metrics::TEST_DURATION
        .record((metrics.finished_at - metrics.started_at).try_into()?, &[]);
    crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
    crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);

    emit_acceleration_size_if_applicable(&app, &spiced_instance.get_tempdir_path())?;

    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Stop and process metrics scraper if enabled
    super::process_spiced_metrics(metrics_scraper, args.common.metrics, &[]).await;

    telemetry.emit().await?;

    spiced_instance.stop()?;
    let health_report = health_report?;
    let mut error_messages = Vec::new();

    if !test_succeeded {
        error_messages.push(format!(
            "Benchmark test failed due to failed queries:\n{}",
            failures.join("\n")
        ));
    }

    if let Some(message) = health_report.failure_message() {
        error_messages.push(message);
    }

    if !error_messages.is_empty() {
        return Err(anyhow::anyhow!(error_messages.join("\n")));
    }

    Ok(row_counts)
}

/// List of query results that should not be snapshotted because they don't return deterministic results
const DISABLED_SNAPSHOT_QUERIES: &[&str] = &[
    "tpcds_q77", // The ORDER BY clause specifies columns that have multiple matches, so the order is unspecified between those rows
];

/// Only snapshot the official TPCH and TPCDS queries, not the "simple" extensions as they don't return consistent results
fn snapshot_predicate(query_name: &str) -> bool {
    (query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q"))
        && !DISABLED_SNAPSHOT_QUERIES.contains(&query_name)
}
