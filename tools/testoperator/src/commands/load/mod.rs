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
use crate::{
    args::LoadTestArgs, health::HealthMonitor, spiced_metrics::MetricsScraper, wait_test_and_memory,
};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, StatisticsCollector},
    opentelemetry::KeyValue,
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(args: &LoadTestArgs) -> anyhow::Result<()> {
    if args.test_args.common.concurrency < 2 {
        return Err(anyhow::anyhow!(
            "Concurrency should be greater than 1 for a load test"
        ));
    }

    let query_set = QuerySet::from(args.test_args.query_set.clone());
    let query_overrides = args
        .test_args
        .query_overrides
        .clone()
        .map(QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let (app, start_request) = get_app_and_start_request(&args.test_args.common).await?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.test_args.common.ready_wait))
        .await?;
    let health_monitor = HealthMonitor::spawn()?;

    // Start metrics scraper if enabled
    let metrics_scraper = if args.test_args.common.scrape_spiced_metrics {
        Some(MetricsScraper::spawn()?)
    } else {
        None
    };

    let test_duration = Duration::from_secs(args.test_args.common.duration);
    let test_hours = (test_duration.as_secs() / 60 / 60).max(1);

    // baseline run
    println!("Running baseline throughput test");
    let baseline_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_parallel_count(args.test_args.common.concurrency)
            .with_query_set(queries.clone())
            .with_end_condition(EndCondition::QuerySetCompleted(test_hours.try_into()?))
            .with_disable_caching(args.test_args.disable_caching)
            .with_http_client(args.test_args.http_clients),
    )
    .with_spiced_instance(spiced_instance)
    .with_progress_bars(!args.test_args.common.disable_progress_bars)
    .start()
    .await?;

    let test = baseline_test.wait().await?;
    let baseline_percentiles = test
        .get_query_durations()
        .statistical_set()?
        .percentile(99.0)?;

    let baseline_metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    println!("Baseline metrics:");
    let records = baseline_metrics.build_records()?;
    print_batches(&records)?;
    let spiced_instance = test.end()?;
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process().watch_memory(&memory_token);

    // load test
    println!("Running load test");
    let throughput_test = SpiceTest::<NotStarted>::new(
        app.name.clone(),
        NotStarted::new()
            .with_parallel_count(args.test_args.common.concurrency)
            .with_query_set(queries.clone())
            .with_end_condition(EndCondition::Duration(Duration::from_secs(
                args.test_args.common.duration,
            )))
            .with_disable_caching(args.test_args.disable_caching)
            .with_http_client(args.test_args.http_clients),
    )
    .with_spiced_instance(spiced_instance)
    .with_progress_bars(!args.test_args.common.disable_progress_bars)
    .start()
    .await?;

    let test = wait_test_and_memory!(throughput_test, memory_token, memory_readings);
    let test_durations = test.get_query_durations().statistical_set()?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    let mut spiced_instance = test.end()?;
    let (max_memory, _) = observe_memory(memory_token, memory_readings).await?;

    println!("Baseline metrics:");
    let baseline_records = baseline_metrics.build_records()?;
    print_batches(&baseline_records)?;
    println!("{}", vec!["-"; 30].join(""));
    println!("Load test metrics:");
    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Stop and process metrics scraper if enabled
    let attributes = vec![KeyValue::new("test", "load")];
    super::process_spiced_metrics(metrics_scraper, args.test_args.common.metrics, &attributes)
        .await;

    spiced_instance.stop()?;
    let health_report = health_report?;

    let mut test_passed = true;
    let mut yellow_measurements = 0;
    for query in queries {
        let Some(baseline_percentile) = baseline_percentiles.get(&query.name) else {
            // Query Failed, no percentile statistics recorded
            continue;
        };

        let Some(duration) = test_durations.get(&query.name) else {
            return Err(anyhow::anyhow!(
                "Query {} not found in test durations",
                query.name
            ));
        };

        let percentile_99th = duration.percentile(99.0)?;
        if percentile_99th.as_millis() < 1000 {
            continue; // skip queries that are too fast to be meaningful
        }

        let percentile_ratio =
            ((percentile_99th.as_secs_f64() / baseline_percentile.as_secs_f64()) - 1.0) * 100.0;

        // yellow measurements = 10% to 20% increase
        // red measurements = > 20% increase
        let (yellow, red) = (
            percentile_ratio > 10.0 && percentile_ratio <= 20.0,
            percentile_ratio > 20.0,
        );

        if red {
            println!(
                "FAIL - Query {query} has a 99th percentile that increased {percentile_ratio}% of the baseline 99th percentile",
                query = query.name
            );
            test_passed = false;
        } else if yellow {
            println!(
                "WARN - Query {query} has a 99th percentile that increased {percentile_ratio}% of the baseline 99th percentile",
                query = query.name
            );
            yellow_measurements += 1;
        }
    }

    let mut failure_messages = Vec::new();
    if !args.no_error && yellow_measurements >= 3 {
        failure_messages.push("Load test failed due to too many yellow measurements".to_string());
    }
    if !args.no_error && !test_passed {
        failure_messages.push("Load test failed.".to_string());
    }
    if let Some(message) = health_report.failure_message() {
        failure_messages.push(message);
    }

    if !failure_messages.is_empty() {
        return Err(anyhow::anyhow!(failure_messages.join("\n")));
    }

    println!("Load test completed");
    Ok(())
}
