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

use super::{get_app_and_start_request, RowCounts};
use crate::args::DatasetTestArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics},
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicetest::{
        datasets::{EndCondition, NotStarted},
        SpiceTest,
    },
    TestType,
};

pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<RowCounts> {
    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides);

    let (app, start_request) = get_app_and_start_request(&args.common)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // baseline run
    println!("Running benchmark test");

    let benchmark_test = SpiceTest::new(
        app.name.clone(),
        spiced_instance,
        NotStarted::new()
            .with_query_set(queries.clone())
            .with_parallel_count(1)
            .with_end_condition(EndCondition::QuerySetCompleted(5)),
    )
    .with_progress_bars(!args.common.disable_progress_bars)
    .start()
    .await?;

    let test = benchmark_test.wait().await?;
    let row_counts = test.validate_returned_row_counts()?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let mut spiced_instance = test.end();

    metrics.show_records()?;

    spiced_instance.show_memory_usage()?;
    spiced_instance.stop()?;
    Ok(row_counts)
}
