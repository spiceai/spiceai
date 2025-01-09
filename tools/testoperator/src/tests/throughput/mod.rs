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
use crate::commands::TestArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    metrics::MetricCollector,
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    throughput::{EndCondition, ThroughputTest},
};

pub(crate) fn export(args: &TestArgs) -> anyhow::Result<()> {
    let (_, mut start_request) = get_app_and_start_request(args)?;

    start_request.prepare()?;
    let tempdir_path = start_request.get_tempdir_path();

    println!(
        "Exported spicepod environment to: {}",
        tempdir_path.to_string_lossy()
    );

    Ok(())
}

pub(crate) async fn run(args: &TestArgs) -> anyhow::Result<()> {
    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let (app, start_request) = get_app_and_start_request(args)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(10))
        .await?;

    // baseline run
    println!("Running baseline test");
    let baseline_test = ThroughputTest::new(app.name.clone(), spiced_instance)
        .with_query_set(queries.clone())
        .with_parallel_count(1)
        .with_end_condition(EndCondition::QuerySetCompleted(10))
        .start()
        .await?;

    let test = baseline_test.wait().await?;
    let spiced_instance = test.end();

    // throughput test
    println!("Running throughput test");
    let throughput_test = ThroughputTest::new(app.name.clone(), spiced_instance)
        .with_query_set(queries.clone())
        .with_parallel_count(args.concurrency.unwrap_or(8))
        .with_end_condition(EndCondition::QuerySetCompleted(2))
        .start()
        .await?;

    let test = throughput_test.wait().await?;
    let throughput_metric = test.get_throughput_metric(args.scale_factor.unwrap_or(1.0))?;
    let metrics = test.collect()?;
    let mut spiced_instance = test.end();

    metrics.show()?;

    spiced_instance.stop()?;

    println!(
        "Throughput test completed with throughput: {} Queries per hour * Scale Factor",
        throughput_metric.round()
    );
    Ok(())
}
