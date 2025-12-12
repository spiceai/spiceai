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
use crate::{args::LoadTestArgs, health::HealthMonitor, spiced_metrics::MetricsScraper};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    app::AppBuilder,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, StatisticsCollector},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spiced::SpicedInstance,
    spicepod::Spicepod,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    telemetry::streaming::StreamingOtlpExporter,
    utils::observe_memory,
};
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[expect(clippy::too_many_lines)]
pub(crate) async fn run(args: &LoadTestArgs) -> anyhow::Result<()> {
    if args.test_args.common.concurrency < 2 {
        return Err(anyhow::anyhow!(
            "Concurrency should be greater than 1 for a load test"
        ));
    }

    // Check if connecting to an external instance or starting a new one
    let (app, mut spiced_instance) = if args.test_args.common.is_external_instance() {
        println!(
            "Connecting to external spiced instance at: {}",
            args.test_args.common.spiced_path
        );
        let spicepod = Spicepod::load_exact(args.test_args.common.spicepod_path.clone()).await?;
        let app = AppBuilder::new(spicepod.name.clone())
            .with_spicepod(spicepod)
            .build();
        let instance = SpicedInstance::external(&args.test_args.common.spiced_path);
        (app, instance)
    } else {
        let (app, start_request) = get_app_and_start_request(&args.test_args.common).await?;
        let instance = SpicedInstance::start(start_request).await?;
        (app, instance)
    };

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.test_args.common.ready_wait))
        .await?;

    // Create telemetry early before any metrics calls (e.g., HealthMonitor)
    // Resource will be set later with set_resource() before emit()
    let mut telemetry = super::create_telemetry(&args.test_args.common);

    let health_monitor = HealthMonitor::spawn()?;

    // Start metrics scraper if enabled
    let metrics_scraper = if args.test_args.common.scrape_spiced_metrics {
        Some(MetricsScraper::spawn()?)
    } else {
        None
    };

    // warm up run
    println!("Performing warm up");

    let (_query_set, test_builder) = super::build_test_with_validation(
        &args.test_args,
        NotStarted::new()
            .with_parallel_count(args.test_args.common.concurrency)
            .with_end_condition(EndCondition::QuerySetCompleted(1))
            .with_disable_caching(args.test_args.disable_caching)
            .with_http_client(args.test_args.http_clients),
    )?;

    let warm_up = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.test_args.common.disable_progress_bars)
        .start()
        .await?;

    let spiced_instance = warm_up.wait().await?.end()?;

    let test_duration = Duration::from_secs(args.test_args.common.duration);

    // Calculate baseline duration: 10% of target time, min 1min, max 10min
    let baseline_duration_secs = (test_duration.as_secs() / 10).clamp(60, 600);
    let baseline_duration = Duration::from_secs(baseline_duration_secs);

    // baseline run
    println!("Running baseline throughput test for {baseline_duration_secs}s",);

    let (_query_set, test_builder) = super::build_test_with_validation(
        &args.test_args,
        NotStarted::new()
            .with_parallel_count(args.test_args.common.concurrency)
            .with_end_condition(EndCondition::Duration(baseline_duration))
            .with_disable_caching(args.test_args.disable_caching)
            .with_http_client(args.test_args.http_clients),
    )?;

    let baseline_test = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.test_args.common.disable_progress_bars)
        .start()
        .await?;

    let test = baseline_test.wait().await?;
    let baseline_percentiles = test.get_query_durations().percentile(99.0)?;

    let baseline_metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    println!("Baseline metrics:");
    let records = baseline_metrics.build_records()?;
    print_batches(&records)?;
    let spiced_instance = test.end()?;
    let memory_token = CancellationToken::new();
    // Memory monitoring is only available for owned spiced instances (not external)
    let memory_readings = spiced_instance
        .process()
        .ok()
        .map(|p| p.watch_memory(&memory_token));

    // load test
    println!("Running load test");

    let load_end_condition = if args.run_until_stopped {
        EndCondition::Unlimited
    } else {
        EndCondition::Duration(Duration::from_secs(args.test_args.common.duration))
    };

    // Create streaming OTLP exporter if OTLP endpoint is configured
    let streaming_exporter = args
        .test_args
        .common
        .otlp_endpoint
        .as_ref()
        .map(|endpoint| StreamingOtlpExporter::spawn(endpoint.clone()));

    let mut test_builder = NotStarted::new()
        .with_parallel_count(args.test_args.common.concurrency)
        .with_end_condition(load_end_condition)
        .with_disable_caching(args.test_args.disable_caching)
        .with_http_client(args.test_args.http_clients)
        .with_query_duration_threshold(args.test_args.mark_query_failed_if_exceeds);

    // Add streaming metrics sender if exporter is configured
    if let Some(exporter) = &streaming_exporter {
        test_builder = test_builder.with_streaming_metrics(exporter.sender());
    }

    let (query_set, test_builder) =
        super::build_test_with_validation(&args.test_args, test_builder)?;

    // Use the same query overrides that were applied in build_test_with_validation
    let query_overrides = args
        .test_args
        .query_overrides
        .clone()
        .map(test_framework::queries::QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let throughput_test = SpiceTest::<NotStarted>::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.test_args.common.disable_progress_bars)
        .start()
        .await?;
    let shutdown_token = throughput_test.cancellation_token();
    let test_future = throughput_test.wait();
    tokio::pin!(test_future);
    let test = match tokio::select! {
        res = &mut test_future => res,
        _ = signal::ctrl_c() => {
            println!("Interrupt received, stopping load test...");
            shutdown_token.cancel();
            test_future.await
        }
    } {
        Ok(test) => test,
        Err(e) => {
            if let Some(readings) = memory_readings {
                let _ = observe_memory(memory_token, readings).await;
            }
            return Err(e);
        }
    };
    let _test_durations = test.get_query_durations().statistical_set()?;

    // Get all query durations for overall statistics before ending the test
    let all_durations = test.get_query_durations().clone();

    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    let mut spiced_instance = test.end()?;
    let (max_memory, _median_memory) = if let Some(readings) = memory_readings {
        observe_memory(memory_token, readings).await?
    } else {
        println!("Memory monitoring not available for external spiced instances");
        (0.0, 0.0)
    };

    // Set up telemetry for load test metrics
    let commit_sha = metrics.commit_sha.clone();
    let spiced_version = metrics.spiced_version.clone();
    let spicepod = args.test_args.common.spicepod_path.display().to_string();

    telemetry.set_resource(
        Resource::builder()
            .with_attribute(KeyValue::new("test", "load"))
            .with_attribute(KeyValue::new("commit_sha", commit_sha.clone()))
            .with_attribute(KeyValue::new("spiced_version", spiced_version.clone()))
            .with_attribute(KeyValue::new("spicepod", spicepod.clone()))
            .build(),
    );

    let attributes = [
        KeyValue::new("test", "load"),
        KeyValue::new("commit_sha", commit_sha),
        KeyValue::new("spiced_version", spiced_version),
        KeyValue::new("spicepod", spicepod),
    ];

    println!("Baseline metrics:");
    let baseline_records = baseline_metrics.build_records()?;
    print_batches(&baseline_records)?;
    println!("{}", vec!["-"; 30].join(""));
    println!("Load test metrics:");
    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Stop and process metrics scraper if enabled
    super::process_spiced_metrics(metrics_scraper, args.test_args.common.metrics, &attributes)
        .await;

    // Shutdown streaming exporter before emitting final telemetry
    if let Some(exporter) = streaming_exporter {
        exporter.shutdown().await;
    }

    telemetry.emit().await?;

    spiced_instance.stop()?;
    let health_report = health_report?;

    let mut test_passed = true;
    let mut yellow_measurements = 0;
    for query in queries {
        let Some(baseline_percentile) = baseline_percentiles.get(&query.name) else {
            // Query Failed, no percentile statistics recorded
            continue;
        };

        let Some(duration) = all_durations.get(&query.name) else {
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
