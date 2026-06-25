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

use super::{get_dataset_app_and_start_request, load_app};
use crate::{args::DatasetTestArgs, health::HealthMonitor};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    app::App,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, QueryMetrics, ThroughputMetrics},
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<()> {
    if args.common.concurrency < 2 {
        return Err(anyhow::anyhow!(
            "Concurrency should be greater than 1 for a throughput test"
        ));
    }

    let (app, spiced_instance, system_adapter_session) = if args.common.is_system_adapter() {
        let app = load_app(&args.common).await?;
        let (instance, session) = crate::system_adapter::acquire(&args.common).await?;
        (app, instance, Some(session))
    } else {
        let (app, start_request) = get_dataset_app_and_start_request(args).await?;
        let instance = SpicedInstance::start(start_request).await?;
        (app, instance, None)
    };

    // Mirror bench's finally-block pattern so adapter teardown runs even if
    // the inner flow bails on an error.
    let result = run_inner(args, app, spiced_instance).await;

    if let Some(session) = system_adapter_session {
        session.teardown().await;
    }

    result
}

async fn run_inner(
    args: &DatasetTestArgs,
    app: App,
    mut spiced_instance: SpicedInstance,
) -> anyhow::Result<()> {
    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;
    let health_monitor = HealthMonitor::spawn()?;

    // Create the appropriate query executor based on args
    let executor = super::create_query_executor(args, &spiced_instance).await?;

    // baseline run
    println!("Running baseline test");

    let (_query_set, test_builder) = super::build_test_with_validation(
        args,
        &app,
        NotStarted::new()
            .with_parallel_count(1)
            .with_end_condition(EndCondition::QuerySetCompleted(6))
            .with_query_executor(executor.clone()),
    )
    .await?;

    let baseline_test = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.common.disable_progress_bars)
        .start()?;

    let test = baseline_test.wait().await?;
    let spiced_instance = test.end()?;
    let memory_token = CancellationToken::new();
    // Process-memory watching only applies to the local-spawn path. See
    // bench/mod.rs for context.
    let memory_readings = spiced_instance
        .process()
        .map(|process| process.watch_memory(&memory_token));

    // throughput test
    println!("Running throughput test");

    let (_query_set, test_builder) = super::build_test_with_validation(
        args,
        &app,
        NotStarted::new()
            .with_parallel_count(args.common.concurrency)
            .with_end_condition(EndCondition::QuerySetCompleted(2))
            .with_query_executor(executor),
    )
    .await?;

    let throughput_test = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.common.disable_progress_bars)
        .start()?;

    let test = match throughput_test.wait().await {
        Ok(test) => test,
        Err(e) => {
            if let Some(handle) = memory_readings {
                let _ = observe_memory(memory_token, handle).await;
            }
            return Err(e);
        }
    };
    let throughput_metric = test.get_throughput_metric(args.scale_factor.unwrap_or(1.0))?;
    let metrics: QueryMetrics<_, ThroughputMetrics> = test
        .collect(TestType::Throughput)?
        .with_run_metric(ThroughputMetrics::new(throughput_metric));
    let mut spiced_instance = test.end()?;
    // Leave as `None` when the SUT isn't local — recording 0 would skew
    // memory dashboards.
    let memory_usage = match memory_readings {
        Some(handle) => Some(observe_memory(memory_token, handle).await?),
        None => None,
    };

    let records = metrics.build_records()?;
    print_batches(&records)?;
    let metrics = match memory_usage {
        Some((max_memory, _)) => metrics.with_memory_usage(max_memory),
        None => metrics,
    };
    metrics.show_run(None)?; // no additional test pass logic applies
    let health_report = health_monitor.stop().await;
    spiced_instance.stop()?;

    let health_report = health_report?;

    if let Some(message) = health_report.failure_message() {
        eprintln!("Warning: {message}");
    }

    println!(
        "Throughput test completed with throughput: {} Queries per hour * Scale Factor",
        throughput_metric.round()
    );
    Ok(())
}
