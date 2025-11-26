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

mod mteb_quora;

use super::get_app_and_start_request;
use crate::{args::SearchTestArgs, health::HealthMonitor, wait_test_and_memory};
use std::time::{Duration, SystemTime};
use test_framework::{
    TestType, anyhow, git,
    metrics::{MetricCollector, QueryMetrics},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        search::{NotStarted, SearchRunMetric},
    },
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};
use tokio::time::sleep;

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(args: &SearchTestArgs) -> anyhow::Result<()> {
    let (app, start_request) = get_app_and_start_request(&args.common).await?;

    match args.benchmark_dataset.as_deref() {
        Some("quora_retrieval") => {
            mteb_quora::prepare_dataset(
                &args
                    .common
                    .data_dir
                    .clone()
                    .unwrap_or(start_request.get_tempdir_path()),
            )
            .await?;
        }
        Some(ds) => {
            return Err(anyhow::anyhow!("Unsupported benchmark-dataset: {ds}"));
        }
        None => {
            return Err(anyhow::anyhow!(
                "Benchmark dataset is required, please specify --benchmark-dataset"
            ));
        }
    }

    let started_at = SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process()?.watch_memory(&memory_token);

    println!("Starting benchmark Spicepod...");

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;
    let health_monitor = HealthMonitor::spawn()?;

    let index_finished_at = SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

    // Allow Spicepod traces to be fully printed before running the test
    sleep(Duration::from_millis(200)).await;

    println!("Running search");

    // Only QuoraRetrieval dataset is currently supported; no need to use `benchmark_dataset` function to determine what config to use.
    let config = mteb_quora::init_search_config(&spiced_instance, Some(10)).await?;

    // retrieve query relevance data
    let qrels = mteb_quora::get_query_relevance_data(&spiced_instance).await?;

    let search_started_at = SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

    let vector_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_config(config)
            .with_parallel_count(args.common.concurrency),
    )
    .with_spiced_instance(spiced_instance)
    .start()?;

    let test = wait_test_and_memory!(vector_test, memory_token, memory_readings);
    let finished_at = SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;

    println!("Search requests completed, calculating results...");

    let p95 = test.get_p95_response_time_metric()?;
    let rps = test.get_rps_metric()?;
    let score = test.calculate_search_score_metric(&qrels, |results| {
        mteb_quora::transform_search_results_for_eval(results)
    })?;

    let metrics: QueryMetrics<_, _> = test
        .collect(TestType::Search)?
        .with_run_metric(SearchRunMetric::new(rps, p95, score));

    let mut spiced_instance = test.end()?;
    let (max_memory, median_memory) = observe_memory(memory_token, memory_readings).await?;

    metrics.with_memory_usage(max_memory).show_run(None)?; // no additional test pass logic applies

    let spiced_commit_sha = std::env::var("SPICED_COMMIT").unwrap_or(git::get_commit_sha());

    // Record benchmark results
    let benchmark_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "search"),
            KeyValue::new("name", app.name.clone()),
            KeyValue::new("spiced_version", spiced_instance.version().to_string()),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("testoperator_commit_sha", git::get_commit_sha()),
            KeyValue::new("branch_name", git::get_branch_name()),
            KeyValue::new("config_name", app.name), // use app name as search configuration
            KeyValue::new(
                "benchmark_dataset",
                args.benchmark_dataset.clone().unwrap_or_default(),
            ),
        ])
        .build();

    let telemetry = Telemetry::new(&benchmark_resource, "SPICEAI_BENCHMARK_METRICS_KEY");

    crate::metrics::TEST_DURATION
        .record(u64::try_from((finished_at - started_at).as_millis())?, &[]);
    crate::metrics::VECTOR_INDEX_CREATION_DURATION.record(
        u64::try_from((index_finished_at - started_at).as_millis())?,
        &[],
    );
    crate::metrics::SEARCH_DURATION.record(
        u64::try_from((finished_at - search_started_at).as_millis())?,
        &[],
    );

    crate::metrics::SEARCH_RPS.record(rps, &[]);
    crate::metrics::SEARCH_P95_RESPONSE_TIME.record(p95, &[]);
    crate::metrics::SCORE.record(score, &[]);
    crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
    crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);

    telemetry.emit().await?;

    let health_report = health_monitor.stop().await;
    spiced_instance.stop()?;
    let health_report = health_report?;

    if let Some(message) = health_report.failure_message() {
        return Err(anyhow::anyhow!(message));
    }

    println!("Benchmark completed successfully!");

    Ok(())
}
