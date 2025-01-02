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

use std::collections::HashMap;
use std::sync::Arc;

use app::AppBuilder;

use arrow::array::RecordBatch;
use runtime::{status, Runtime};
use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results, utils::test_request_context,
};

fn make_github_dataset(owner: &str, repo: &str, query_type: &str, query_mode: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("github:github.com/{owner}/{repo}/{query_type}"),
        format!("{repo}_{query_type}_{query_mode}"),
    );
    let params = HashMap::from([
        ("github_query_mode".to_string(), query_mode.to_string()),
        (
            "github_token".to_string(),
            "${secrets:GITHUB_TOKEN}".to_string(),
        ),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

#[tokio::test]
async fn test_github_issues() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset("spiceai", "spiceai", "issues", "auto"))
                .with_dataset(make_github_dataset(
                    "spiceai", "spiceai", "issues", "search",
                ))
                .build();
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));
            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = rt.load_components() => {}
            }

            let mut now = std::time::Instant::now();

            run_query_and_check_results(
                &mut rt,
                "test_github_issues_auto",
                "SELECT * FROM spiceai_issues_auto LIMIT 10",
                false, // can't snapshot this plan, as the partition size increases with more issues
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        assert!(batch.num_rows() > 0, "num_rows: {}", batch.num_rows());
                    }
                })),
            )
            .await?;

            let auto_elapsed = now.elapsed();
            now = std::time::Instant::now();

            run_query_and_check_results(
                &mut rt,
                "test_github_issues_search",
                "SELECT * FROM spiceai_issues_search LIMIT 10",
                false, // can't snapshot this plan, as the partition size increases with more issues
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        assert!(batch.num_rows() > 0, "num_rows: {}", batch.num_rows());
                    }
                })),
            )
            .await?;

            let search_elapsed = now.elapsed();
            let auto_elapsed_secs = auto_elapsed.as_secs();
            let search_limit_elapsed_secs = search_elapsed.as_secs();

            // LIMIT should stop this query from retrieving every commit, so it shouldn't take that long
            assert!(
                auto_elapsed_secs < 20,
                "auto_elapsed_secs: {auto_elapsed_secs}"
            );
            assert!(
                search_limit_elapsed_secs < 20,
                "search_limit_elapsed_secs: {search_limit_elapsed_secs}"
            );

            now = std::time::Instant::now();

            run_query_and_check_results(
                &mut rt,
                "test_github_issues_search_author",
                "SELECT * FROM spiceai_issues_search WHERE author = 'peasee' LIMIT 100",
                false, // can't snapshot this plan, as the partition size increases with more issues
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        assert!(batch.num_rows() > 0, "num_rows: {}", batch.num_rows());
                    }
                })),
            )
            .await?;

            let search_author_elapsed = now.elapsed();
            let search_author_elapsed_secs = search_author_elapsed.as_secs();

            // search should push down the filter, preventing the query from retrieving every issue
            assert!(
                search_author_elapsed_secs < 10,
                "search_author_elapsed_secs: {search_author_elapsed_secs}"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_commits() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset("spiceai", "spiceai", "commits", "auto"))
                .build();
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));
            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = rt.load_components() => {}
            }

            let now = std::time::Instant::now();

            run_query_and_check_results(
                &mut rt,
                "test_github_commits_auto",
                "SELECT * FROM spiceai_commits_auto LIMIT 10",
                true,
                Some(Box::new(|result_batches| {
                    let mut row_count = 0;
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 10, "num_cols: {}", batch.num_columns());
                        row_count += batch.num_rows();
                    }
                    assert_eq!(row_count, 10, "num_rows: {row_count}");
                })),
            )
            .await?;

            let elapsed = now.elapsed().as_secs();

            // LIMIT should stop this query from retrieving every commit, so it shouldn't take that long
            assert!(elapsed < 15, "elapsed: {elapsed}");

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_stargazers() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    "spiceai",
                    "spiceai",
                    "stargazers",
                    "auto",
                ))
                .build();
            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));
            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = rt.load_components() => {}
            }

            let now = std::time::Instant::now();

            run_query_and_check_results(
                &mut rt,
                "test_github_stargazers_auto",
                "SELECT * FROM spiceai_stargazers_auto LIMIT 10",
                true,
                Some(Box::new(|result_batches| {
                    let mut row_count = 0;
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 9, "num_cols: {}", batch.num_columns());
                        row_count += batch.num_rows();
                    }
                    assert_eq!(row_count, 10, "num_rows: {row_count}");
                })),
            )
            .await?;

            let elapsed = now.elapsed().as_secs();

            // LIMIT should stop this query from retrieving every stargazer, so it shouldn't take that long
            assert!(elapsed < 15, "elapsed: {elapsed}");

            Ok(())
        })
        .await
}
