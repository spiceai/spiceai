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
use crate::{args::CommonArgs, commands::TEST_RESULTS_API_KEY};
use std::time::Duration;
use test_framework::{
    anyhow,
    arrow::util::pretty::print_batches,
    metrics::MetricCollector,
    spiced::SpicedInstance,
    spicetest::{
        vector_search::{NotStarted, SearchConfig, SearchRequest},
        SpiceTest,
    },
    TestType,
};

pub(crate) async fn run(args: &CommonArgs) -> anyhow::Result<()> {
    let (app, start_request) = get_app_and_start_request(args)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.ready_wait))
        .await?;

    // baseline run
    println!("Running benchmark test");

    // TODO: build search config for vector search elsewhere
    let config = SearchConfig::new()
        .add_request(
            SearchRequest::new("file_connector_recipe_no_keywords", "file connector recipe")
                .with_additional_columns(vec!["path"]),
        )
        .add_request(
            SearchRequest::new(
                "file_connector_recipe_separate_keywords",
                "file connector recipe",
            )
            .with_keywords(vec!["file", "connector"])
            .with_additional_columns(vec!["path"]),
        )
        .add_request(
            SearchRequest::new(
                "file_connector_recipe_combined_keyword",
                "file connector recipe",
            )
            .with_keywords(vec!["file connector"])
            .with_additional_columns(vec!["path"]),
        )
        .add_request(
            SearchRequest::new("file_data_connector_no_keywords", "file data connector")
                .with_additional_columns(vec!["path"]),
        )
        .add_request(
            SearchRequest::new(
                "file_data_connector_separate_keywords",
                "file data connector",
            )
            .with_keywords(vec!["file", "connector"])
            .with_additional_columns(vec!["path"]),
        )
        .add_request(
            SearchRequest::new(
                "file_data_connector_combined_keyword",
                "file data connector",
            )
            .with_keywords(vec!["file connector"])
            .with_additional_columns(vec!["path"]),
        );

    let vector_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new().with_config(config).with_parallel_count(1),
    )
    .with_spiced_instance(spiced_instance)
    .with_api_key(if args.upload_results_dataset.is_some() {
        Some(TEST_RESULTS_API_KEY.to_string())
    } else {
        None
    })
    .start()?;

    let test = vector_test.wait().await?;
    let metrics = test.collect(TestType::VectorSearch)?;
    let mut spiced_instance = test.end()?;

    let records = metrics.build_records()?;
    print_batches(&records)?;

    spiced_instance.show_memory_usage()?;
    spiced_instance.stop()?;
    Ok(())
}
