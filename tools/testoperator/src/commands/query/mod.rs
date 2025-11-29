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

use super::RowCounts;
use crate::{
    args::{QueryArgs, QuerySetLoader},
    health::HealthMonitor,
};
use std::time::Duration;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus},
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
};

pub(crate) async fn run(args: &QueryArgs) -> anyhow::Result<RowCounts> {
    let mut spiced_instance = SpicedInstance::empty();

    spiced_instance
        .wait_for_ready(Duration::from_secs(10))
        .await?;

    let health_monitor = HealthMonitor::spawn()?;

    // baseline run
    println!("Running benchmark test");

    let query_set = args.load_query_set()?;
    let query_overrides = args
        .query_overrides
        .clone()
        .map(test_framework::queries::QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let mut test = NotStarted::new()
        .with_parallel_count(1)
        .with_end_condition(EndCondition::QuerySetCompleted(5))
        .with_validate(args.validate)
        .with_disable_caching(args.disable_caching)
        .with_scale_factor(args.scale_factor.unwrap_or(1.0))
        .with_http_client(args.http_clients)
        .with_query_set(queries);

    if args.validate
        && let Some(validation_data) =
            query_set.get_validation_data(args.scenario_query_file.as_deref())?
    {
        test = test.with_validation_data(validation_data);
    }

    if let Some(ref_schema) = &args.reference_schema {
        test = test.with_reference_schema(Some(ref_schema.clone()));
    }

    let benchmark_test = SpiceTest::new("local".to_string(), test)
        .with_spiced_instance(spiced_instance)
        .start()
        .await?;

    let test = benchmark_test.wait().await?;
    let row_counts = test.validate_returned_row_counts()?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let test_succeeded = test.succeeded();
    test.end()?;

    let records = metrics.build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await?;
    let mut error_messages = Vec::new();

    let mut failures = Vec::new();
    for query in &metrics.metrics {
        let query_name = &query.query_name;
        if let QueryStatus::Failed(reason) = &query.query_status {
            if let Some(reason) = reason {
                failures.push(format!("{query_name}: {reason}"));
            } else {
                failures.push(format!("{query_name}: failed with an undetermined error"));
            }
        }
    }

    if !test_succeeded {
        error_messages.push(format!(
            "Benchmark test failed due to failed queries:\n{}",
            failures.join("\n")
        ));
    }

    if let Some(message) = health_report.failure_message() {
        error_messages.push(message);
    }

    if !error_messages.is_empty() {
        return Err(anyhow::anyhow!(error_messages.join("\n")));
    }

    Ok(row_counts)
}
