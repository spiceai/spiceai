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
    args::DatasetTestArgs,
    commands::{TEST_RESULTS_API_KEY, TEST_RESULTS_DATASET},
    wait_test_and_memory,
};
use opentelemetry::{KeyValue, metrics::MeterProvider};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    flight::put_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics},
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
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
    .with_api_key(if args.common.upload_results_dataset.is_some() {
        Some(TEST_RESULTS_API_KEY.to_string())
    } else {
        None
    })
    .start()
    .await?;

    let test = wait_test_and_memory!(benchmark_test, memory_token, memory_readings);

    let row_counts = test.validate_returned_row_counts()?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    let (max_memory, _) = observe_memory(memory_token, memory_readings).await?;

    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;
    spiced_instance.stop()?;

    // if args.common.upload_results_dataset.is_some() {
    //     println!("Uploading test results...");
    //     let mut flight_client = spiced_instance
    //         .flight_client(Some(TEST_RESULTS_API_KEY.to_string()))
    //         .await?;
    //     put_batches(&mut flight_client, TEST_RESULTS_DATASET, records).await?;
    // }

    let benchmark_resource = Resource::new(vec![
        KeyValue::new("service.name", "testoperator"),
        KeyValue::new("benchmark.name", app.name.as_str()),
        KeyValue::new("benchmark.version", metrics.spiced_version),
        KeyValue::new("benchmark.query_set", args.query_set.as_str()),
        KeyValue::new("benchmark.commit_sha", metrics.commit_sha),
    ]);

    let provider = SdkMeterProvider::builder()
        .with_resource(benchmark_resource)
        .build();
    let meter = provider.meter("benchmarks_telemetry");

    let query_status = meter.u64_gauge("query_status").with_unit("status").build();
    let row_count = meter.u64_gauge("row_count").with_unit("rows").build();
    let median_query_duration = meter
        .i64_gauge("median_query_duration")
        .with_unit("ms")
        .build();
    let min_query_duration = meter
        .i64_gauge("min_query_duration")
        .with_unit("ms")
        .build();
    let max_query_duration = meter
        .i64_gauge("max_query_duration")
        .with_unit("ms")
        .build();
    let iterations = meter.i64_gauge("iterations").with_unit("count").build();
    let p90_duration = meter.i64_gauge("p90_duration").with_unit("ms").build();
    let p95_duration = meter.i64_gauge("p95_duration").with_unit("ms").build();
    let p99_duration = meter.i64_gauge("p99_duration").with_unit("ms").build();

    for query in metrics.metrics {
        let query_name = query.query_name.clone();
        let commit_sha = metrics.commit_sha.clone();
        let spiced_version = metrics.spiced_version.clone();
        let attributes = vec![
            KeyValue::new("query_name", query_name.as_str()),
            KeyValue::new("commit_sha", commit_sha.as_str()),
            KeyValue::new("spiced_version", spiced_version.as_str()),
            KeyValue::new("query_set", args.query_set.as_str()),
        ];

        let query_status = if matches!(query.query_status, test_framework::QueryStatus::Success) {
            1
        } else {
            0
        };

        query_status.record(query_status, &attributes);
        median_query_duration.record(query.median_duration_ms, &attributes);
        min_query_duration.record(query.min_duration_ms, &attributes);
        max_query_duration.record(query.max_duration_ms, &attributes);
        p90_duration.record(query.percentile_90_duration_ms, &attributes);
        p95_duration.record(query.percentile_95_duration_ms, &attributes);
        p99_duration.record(query.percentile_99_duration_ms, &attributes);
        iterations.record(query.iterations, &attributes);
    }

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
