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

use crate::commands::TestArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    app::App,
    metrics::{MetricCollector, StatisticsCollector},
    queries::{QueryOverrides, QuerySet},
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
    throughput::{EndCondition, ThroughputTest},
};

fn get_app_and_start_request(args: &TestArgs) -> anyhow::Result<(App, StartRequest)> {
    let spicepod = Spicepod::load_exact(args.spicepod_path.clone())?;
    let app = test_framework::app::AppBuilder::new(spicepod.name.clone())
        .with_spicepod(spicepod)
        .build();

    let start_request = StartRequest::new(args.spiced_path.clone(), from_app(app.clone()))?;
    let start_request = if let Some(data_dir) = &args.data_dir {
        start_request.with_data_dir(data_dir.clone())
    } else {
        start_request
    };

    Ok((app, start_request))
}

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
    let baseline_durations = test.get_query_durations().statistical_set()?;
    let baseline_percentiles = baseline_durations.percentile(0.99)?;
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
    let query_durations = test.get_query_durations().statistical_set()?;
    let throughput_metric = test.get_throughput_metric(args.scale_factor.unwrap_or(1.0))?;
    let metrics = test.collect()?;
    let mut spiced_instance = test.end();

    metrics.show().await?;

    for (query, _) in queries {
        let Some(duration) = query_durations.get(query) else {
            return Err(anyhow::anyhow!("Query {query} not found in durations"));
        };

        let Some(baseline_percentile) = baseline_percentiles.get(query) else {
            return Err(anyhow::anyhow!(
                "Query {query} not found in baseline percentiles"
            ));
        };

        // TODO: move this into throughput test metricscollector and add an extended metrics for it
        let count_slower_than_baseline_percentile = duration
            .iter()
            .filter(|d| d.as_millis() > baseline_percentile.as_millis())
            .count();
        let percent_slower_than_baseline_percentile =
            f64::from(u32::try_from(count_slower_than_baseline_percentile)?)
                / f64::from(u32::try_from(duration.len())?)
                * 100.0;
        if percent_slower_than_baseline_percentile > 1.0 {
            println!(
                "{query} - {percent_slower_than_baseline_percentile}% of the time it was slower than the baseline 99th percentile"
            );
        }
    }

    spiced_instance.stop()?;

    println!("Throughput test completed with throughput: {throughput_metric}");
    Ok(())
}
