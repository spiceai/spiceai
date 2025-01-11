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
    metrics::{MetricCollector, StatisticsCollector},
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicetest::{EndCondition, SpiceTest},
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
        .wait_for_ready(Duration::from_secs(args.ready_wait.unwrap_or(30) as u64))
        .await?;

    // baseline run
    println!("Running baseline throughput test");
    let baseline_test = SpiceTest::new(app.name.clone(), spiced_instance)
        .with_query_set(queries.clone())
        .with_parallel_count(args.concurrency.unwrap_or(8))
        .with_end_condition(EndCondition::QuerySetCompleted(2))
        .start()
        .await?;

    let test = baseline_test.wait().await?;
    let baseline_percentiles = test
        .get_query_durations()
        .statistical_set()?
        .percentile(0.99)?;
    let baseline_metrics = test.collect()?;
    let spiced_instance = test.end();

    // load test
    println!("Running load test");
    let throughput_test = SpiceTest::new(app.name.clone(), spiced_instance)
        .with_query_set(queries.clone())
        .with_parallel_count(args.concurrency.unwrap_or(8))
        .with_end_condition(EndCondition::Duration(Duration::from_secs(
            args.duration.unwrap_or(60).try_into()?,
        )))
        .start()
        .await?;

    let test = throughput_test.wait().await?;
    let test_durations = test.get_query_durations().statistical_set()?;
    let metrics = test.collect()?;
    let mut spiced_instance = test.end();

    println!("Baseline metrics:");
    baseline_metrics.show()?;
    println!("{}", vec!["-"; 30].join(""));
    println!("Load test metrics:");
    metrics.show()?;

    // collect memory usage before stopping the instance
    let memory_usage = spiced_instance.memory_usage()?;
    // drop memory usage to MB as a u32 before converting to GB as a float
    // we don't really care about the fractional memory usage of KB/MB
    let memory_usage_gb = f64::from(u32::try_from(memory_usage / 1024 / 1024)?) / 1024.0;
    println!("Memory usage: {memory_usage_gb:.2} GB");

    spiced_instance.stop()?;

    let mut test_passed = true;
    for (query, _) in queries {
        let Some(duration) = test_durations.get(query) else {
            return Err(anyhow::anyhow!(
                "Query {} not found in test durations",
                query
            ));
        };

        let median_duration = duration.median()?;
        if median_duration.as_millis() < 500 {
            continue; // skip queries that are too fast for percentile comparisons to be meaningful
        }

        let Some(baseline_percentile) = baseline_percentiles.get(query) else {
            return Err(anyhow::anyhow!(
                "Query {} not found in baseline percentiles",
                query
            ));
        };

        let percentile_99th = duration.percentile(0.99)?;
        let percentile_ratio = percentile_99th.as_secs_f64() / baseline_percentile.as_secs_f64();

        if percentile_ratio > 1.1 {
            println!(
                "FAIL - Query {query} has a 99th percentile that is {percentile_ratio}% of the baseline 99th percentile",
            );
            test_passed = false;
        }
    }

    if !test_passed {
        return Err(anyhow::anyhow!("Load test failed."));
    }

    println!("Load test completed");
    Ok(())
}
