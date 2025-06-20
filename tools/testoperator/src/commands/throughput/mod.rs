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
use crate::{args::DatasetTestArgs, wait_test_and_memory};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, QueryMetrics, ThroughputMetrics},
    queries::{QueryOverrides, QuerySet},
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

    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let (app, start_request) = get_app_and_start_request(&args.common).await?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // baseline run
    println!("Running baseline test");
    let baseline_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_parallel_count(1)
            .with_query_set(queries.clone())
            .with_end_condition(EndCondition::QuerySetCompleted(6))
            .with_disable_caching(args.disable_caching),
    )
    .with_spiced_instance(spiced_instance)
    .with_progress_bars(!args.common.disable_progress_bars)
    .start()
    .await?;

    let test = baseline_test.wait().await?;
    let spiced_instance = test.end()?;
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process().watch_memory(&memory_token);

    // throughput test
    println!("Running throughput test");
    let throughput_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_parallel_count(args.common.concurrency)
            .with_query_set(queries.clone())
            .with_end_condition(EndCondition::QuerySetCompleted(2))
            .with_disable_caching(args.disable_caching),
    )
    .with_spiced_instance(spiced_instance)
    .with_progress_bars(!args.common.disable_progress_bars)
    .start()
    .await?;

    let test = wait_test_and_memory!(throughput_test, memory_token, memory_readings);
    let throughput_metric = test.get_throughput_metric(args.scale_factor.unwrap_or(1.0))?;
    let metrics: QueryMetrics<_, ThroughputMetrics> = test
        .collect(TestType::Throughput)?
        .with_run_metric(ThroughputMetrics::new(throughput_metric));
    let mut spiced_instance = test.end()?;
    let (max_memory, _) = observe_memory(memory_token, memory_readings).await?;

    let records = metrics.build_records()?;
    print_batches(&records)?;
    metrics.with_memory_usage(max_memory).show_run(None)?; // no additional test pass logic applies
    spiced_instance.stop()?;

    println!(
        "Throughput test completed with throughput: {} Queries per hour * Scale Factor",
        throughput_metric.round()
    );
    Ok(())
}
