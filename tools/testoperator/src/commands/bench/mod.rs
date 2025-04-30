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
use crate::{args::DatasetTestArgs, wait_test_and_memory};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<RowCounts> {
    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let (app, start_request) = get_app_and_start_request(&args.common)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process().watch_memory(&memory_token);

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // baseline run
    println!("Running benchmark test");

    let benchmark_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_query_set(queries.clone())
            .with_parallel_count(1)
            .with_end_condition(EndCondition::QuerySetCompleted(5))
            .with_validate(args.validate),
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
    let spiced_version = metrics.spiced_version.clone();
    let app_name = app.name.clone();
    let benchmark_resource = Resource::new(vec![
        KeyValue::new("service.name", "testoperator"),
        KeyValue::new("benchmark.name", app_name.clone()),
        KeyValue::new("benchmark.spiced_version", spiced_version.clone()),
        KeyValue::new("benchmark.query_set", query_set.to_string()),
        KeyValue::new("benchmark.spiced_commit_sha", commit_sha.clone()),
        KeyValue::new("benchmark.branch_name", metrics.branch_name.clone()),
        KeyValue::new(
            "benchmark.scale_factor",
            args.scale_factor.unwrap_or(1.0).to_string(),
        ),
    ]);

    let telemetry = Telemetry::new(&benchmark_resource, "SPICEAI_BENCHMARK_METRICS_KEY");

    for query in &metrics.metrics {
        let query_name = query.query_name.clone();
        let row_count = row_counts.get(&query_name).unwrap_or(&0);
        let attributes = vec![
            KeyValue::new("query_name", query_name),
            KeyValue::new("spiced_commit_sha", commit_sha.clone()),
            KeyValue::new("spiced_version", spiced_version.clone()),
            KeyValue::new("query_set", query_set.to_string()),
        ];

        let status: u64 = u64::from(match query.query_status {
            QueryStatus::Passed => true,
            QueryStatus::Failed => false,
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

    crate::metrics::TEST_DURATION
        .record((metrics.finished_at - metrics.started_at).try_into()?, &[]);
    crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
    crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);

    telemetry.emit().await?;

    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;
    spiced_instance.stop()?;

    if !test_succeeded {
        return Err(anyhow::anyhow!(
            "Benchmark test failed due to failed queries"
        ));
    }

    Ok(row_counts)
}

/// Only snapshot the official TPCH and TPCDS queries, not the "simple" extensions as they don't return consistent results
fn snapshot_predicate(query_name: &str) -> bool {
    query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q")
}
