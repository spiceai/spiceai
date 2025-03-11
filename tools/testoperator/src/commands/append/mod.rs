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
    args::DatasetTestArgs,
    commands::{TEST_RESULTS_API_KEY, TEST_RESULTS_DATASET},
};
use std::time::Duration;
use test_framework::{
    anyhow,
    app::App,
    arrow::util::pretty::print_batches,
    flight::put_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics},
    queries::{QueryOverrides, QuerySet},
    spiced::SpicedInstance,
    spicepod::component::dataset::acceleration::RefreshMode,
    spicetest::{append::NotStarted, SpiceTest},
    TestType,
};

pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<()> {
    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);

    let (app, start_request) = get_app_and_start_request(&args.common)?;

    check_app_is_appendable(&app)?;

    println!("Running append test");

    let append_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_query_set(query_set, query_overrides)
            .with_parallel_count(1)
            .with_end_duration(Duration::from_secs(60 * 60))
            .with_tempdir_path(start_request.get_tempdir_path()),
    )
    .with_explain_plan_snapshot()
    .with_results_snapshot(snapshot_predicate)
    .with_progress_bars(false)
    .with_api_key(if args.common.upload_results_dataset.is_some() {
        Some(TEST_RESULTS_API_KEY.to_string())
    } else {
        None
    })
    .start_appending()
    .await?;

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    let append_test = append_test
        .with_spiced_instance(spiced_instance)
        .start_test()
        .await?;
    let test = append_test.wait().await?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let mut spiced_instance = test.end()?;

    let records = metrics.build_records()?;
    print_batches(&records)?;

    if args.common.upload_results_dataset.is_some() {
        println!("Uploading test results...");
        let mut flight_client = spiced_instance
            .flight_client(Some(TEST_RESULTS_API_KEY.to_string()))
            .await?;
        put_batches(&mut flight_client, TEST_RESULTS_DATASET, records).await?;
    }

    spiced_instance.show_memory_usage()?;
    spiced_instance.stop()?;
    Ok(())
}

/// Only snapshot the official TPCH and TPCDS queries, not the "simple" extensions as they don't return consistent results
fn snapshot_predicate(query_name: &str) -> bool {
    query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q")
}

fn check_app_is_appendable(app: &App) -> anyhow::Result<()> {
    for dataset in &app.datasets {
        // check that each dataset has an append-mode accelerator
        if dataset
            .acceleration
            .as_ref()
            .map_or(true, |a| a.refresh_mode != Some(RefreshMode::Append))
        {
            return Err(anyhow::anyhow!(
                "Dataset {} does not have an append-mode accelerator",
                dataset.name
            ));
        }

        // check that each dataset uses a supported append-mode source
        if dataset.from.split(':').next() != Some("file") {
            return Err(anyhow::anyhow!(
                "Dataset {} does not use a supported append-mode source",
                dataset.name
            ));
        }
    }

    Ok(())
}
