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
use crate::args::DatasetTestArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    metrics::{MetricCollector, QueryMetrics, ThroughputMetrics},
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicetest::{
        datasets::{EndCondition, NotStarted},
        SpiceTest,
    },
    TestType,
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

    let (app, start_request) = get_app_and_start_request(&args.common)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // baseline run
    println!("Running baseline test");
    let baseline_test = SpiceTest::new(
        app.name.clone(),
        spiced_instance,
        NotStarted::new()
            .with_parallel_count(1)
            .with_query_set(queries.clone())
            .with_end_condition(EndCondition::QuerySetCompleted(6)),
    )
    .with_progress_bars(!args.common.disable_progress_bars)
    .start()
    .await?;

    let test = baseline_test.wait().await?;
    let spiced_instance = test.end();

    // throughput test
    println!("Running throughput test");
    let throughput_test = SpiceTest::new(
        app.name.clone(),
        spiced_instance,
        NotStarted::new()
            .with_parallel_count(args.common.concurrency)
            .with_query_set(queries.clone())
            .with_end_condition(EndCondition::QuerySetCompleted(2)),
    )
    .with_progress_bars(!args.common.disable_progress_bars)
    .start()
    .await?;

    let test = throughput_test.wait().await?;
    let throughput_metric = test.get_throughput_metric(args.scale_factor.unwrap_or(1.0))?;
    let metrics: QueryMetrics<_, ThroughputMetrics> = test
        .collect(TestType::Throughput)?
        .with_run_metric(ThroughputMetrics::new(throughput_metric));
    let mut spiced_instance = test.end();
    let memory_usage = spiced_instance.show_memory_usage()?;

    metrics.show_records()?;
    metrics.with_memory_usage(memory_usage).show_run(None)?; // no additional test pass logic applies
    spiced_instance.stop()?;

    println!(
        "Throughput test completed with throughput: {} Queries per hour * Scale Factor",
        throughput_metric.round()
    );
    Ok(())
}
