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
    TestType,
    anyhow::{self, Context},
    app::App,
    arrow::{self, array::AsArray, util::pretty::print_batches},
    futures::TryStreamExt,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics},
    queries::{QueryOverrides, QuerySet, TableWithRowCount},
    spiced::SpicedInstance,
    spicepod::acceleration::RefreshMode,
    spicetest::{SpiceTest, append::NotStarted},
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<()> {
    let query_set = QuerySet::from(args.query_set.clone());
    let query_overrides = args.query_overrides.clone().map(QueryOverrides::from);

    let (app, start_request) = get_app_and_start_request(&args.common).await?;

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
    .with_progress_bars(false)
    .start_appending()
    .await?;

    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process().watch_memory(&memory_token);

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    let append_test = append_test
        .with_spiced_instance(spiced_instance)
        .start_test()
        .await?;
    let test = wait_test_and_memory!(append_test, memory_token, memory_readings);
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let mut spiced_instance = test.end()?;
    let (max_memory, _) = observe_memory(memory_token, memory_readings).await?;

    check_table_counts(
        &spiced_instance,
        query_set,
        args.scale_factor.unwrap_or(1.0),
    )
    .await?;

    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;

    spiced_instance.stop()?;
    Ok(())
}

fn check_app_is_appendable(app: &App) -> anyhow::Result<()> {
    for dataset in &app.datasets {
        // check that each dataset has an append-mode accelerator
        if dataset
            .acceleration
            .as_ref()
            .is_none_or(|a| a.refresh_mode != Some(RefreshMode::Append))
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

async fn check_table_counts(
    spiced: &SpicedInstance,
    query_set: QuerySet,
    scale_factor: f64,
) -> anyhow::Result<()> {
    let spice_client = spiced.spice_client(None, false).await?;

    let mut any_count_mismatch = false;
    for TableWithRowCount {
        name,
        count: expected_count,
    } in query_set.row_counts()
    {
        let expected_count = f64::from(expected_count) * scale_factor;
        let sql = format!("SELECT COUNT(*) FROM {name}");
        let batches = spice_client
            .query(&sql)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        if batches.len() != 1 {
            return Err(anyhow::anyhow!(
                "Expected 1 batch, got {} batches",
                batches.len()
            ));
        }
        let count = batches[0]
            .column(0)
            .as_primitive_opt::<arrow::datatypes::Int64Type>()
            .context("Failed to get count as a Int64Type")?
            .value(0);

        let count = f64::from(u32::try_from(count)?);
        // Allow a 0.01% margin of error
        let upper_bound = expected_count * 1.0001;
        let lower_bound = expected_count * 0.9999;
        if !(count <= upper_bound && count >= lower_bound) {
            println!("Table {name} has {count} rows, expected {expected_count}");
            any_count_mismatch = true;
        }
    }

    if any_count_mismatch {
        return Err(anyhow::anyhow!(
            "Table row counts do not match expected values"
        ));
    }

    Ok(())
}
