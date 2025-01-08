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

use crate::RunArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    app::App,
    queries::get_tpch_test_queries,
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
    throughput::ThroughputTest,
};

fn get_app_and_start_request(args: &RunArgs) -> anyhow::Result<(App, StartRequest)> {
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

pub(crate) fn export(args: &RunArgs) -> anyhow::Result<()> {
    let (_, mut start_request) = get_app_and_start_request(args)?;

    start_request.prepare()?;
    let tempdir_path = start_request.get_tempdir_path();

    println!(
        "Exported spicepod environment to: {}",
        tempdir_path.to_string_lossy()
    );

    Ok(())
}

pub(crate) async fn run(args: &RunArgs) -> anyhow::Result<()> {
    let (app, start_request) = get_app_and_start_request(args)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(10))
        .await?;

    let test = ThroughputTest::new(app.name.clone(), spiced_instance)
        .with_query_set(get_tpch_test_queries(None))
        .with_parallel_count(10)
        .start()
        .await?;

    let test = test.wait().await?;
    let query_durations = test.get_query_durations().clone();
    let throughput_metric = test.get_throughput_metric(1.0)?;
    let mut spiced_instance = test.end();
    spiced_instance.stop()?;

    for (query, duration) in query_durations {
        println!("Query {query} took {} milliseconds", duration.as_millis());
    }

    println!("Throughput test completed with throughput: {throughput_metric}");
    Ok(())
}
