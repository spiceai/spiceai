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

use datafusion::common::test_util::batches_to_string;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};

use crate::{
    configure_test_datafusion, init_tracing, run_query_and_check_results,
    utils::{runtime_ready_check, test_request_context},
};

enum GithubDatasetType {
    RepoSpecific {
        owner: String,
        repo: String,
        query_type: String,
    },
    OrgSpecific {
        org: String,
        query_type: String,
    },
}

fn make_github_dataset(
    kind: &GithubDatasetType,
    query_mode: &str,
    additional_params: Option<HashMap<String, String>>,
) -> Dataset {
    let mut dataset = match kind {
        GithubDatasetType::RepoSpecific {
            owner,
            repo,
            query_type,
        } => Dataset::new(
            format!("github:github.com/{owner}/{repo}/{query_type}"),
            format!("{repo}_{query_type}_{query_mode}"),
        ),
        GithubDatasetType::OrgSpecific { org, query_type } => Dataset::new(
            format!("github:github.com/{org}/{query_type}"),
            format!("{org}_{query_type}_{query_mode}"),
        ),
    };

    let mut params = HashMap::from([("github_query_mode".to_string(), query_mode.to_string())]);

    match kind {
        GithubDatasetType::OrgSpecific { .. } => {
            params.insert(
                "github_token".to_string(),
                "${secrets:GITHUB_ORG_TOKEN}".to_string(),
            );
        }
        GithubDatasetType::RepoSpecific { .. } => {
            params.insert(
                "github_token".to_string(),
                "${secrets:GITHUB_TOKEN}".to_string(),
            );
        }
    }

    params.extend(additional_params.unwrap_or_default());

    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_github_issues() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "issues".to_string(),
                    },
                    "auto",
                    None,
                ))
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "issues".to_string(),
                    },
                    "search",
                    None,
                ))
                .build();
            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

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
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "commits".to_string(),
                    },
                    "auto",
                    None,
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

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
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "stargazers".to_string(),
                    },
                    "auto",
                    None,
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

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

#[tokio::test]
async fn test_github_org_members() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::OrgSpecific {
                        org: "spiceai".to_string(),
                        query_type: "members".to_string(),
                    },
                    "auto",
                    None,
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_org_members_auto",
                "SELECT * FROM spiceai_members_auto LIMIT 10",
                false,
                Some(Box::new(|result_batches| {
                    let mut row_count = 0;
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 9, "num_cols: {}", batch.num_columns());
                        row_count += batch.num_rows();
                    }
                    assert!(row_count <= 10, "num_rows: {row_count}");
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_pull_requests_projection_limit_pushdown() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    Some(HashMap::from([
                        ("github_include_comments".to_string(), "all".to_string()),
                        ("github_max_comments_fetched".to_string(), "100".to_string()),
                    ])),
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_auto",
                "SELECT additions, review_comments, discussion FROM spiceai_pulls_auto LIMIT 10",
                true,
                Some(Box::new(|result_batches| {
                    let mut row_count = 0;
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                        row_count += batch.num_rows();
                    }
                    assert_eq!(row_count, 10, "num_rows: {row_count}");
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_pull_requests_schema_changes() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    Some(HashMap::from([
                        ("github_include_comments".to_string(), "review".to_string()),
                        ("github_max_comments_fetched".to_string(), "100".to_string()),
                    ])),
                ))
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "apache".to_string(),
                        repo: "datafusion".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    Some(HashMap::from([
                        (
                            "github_include_comments".to_string(),
                            "discussion".to_string(),
                        ),
                        ("github_max_comments_fetched".to_string(), "100".to_string()),
                    ])),
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let dataset_columns_tests = vec![
                ("spiceai_pulls_auto", "review_comments"),
                ("datafusion_pulls_auto", "discussion"),
            ];

            for (dataset_name, column_name) in dataset_columns_tests {
                run_query_and_check_results(
                    &mut rt,
                    "test_github_pull_requests_schema",
                    format!("SELECT {column_name} FROM {dataset_name} LIMIT 10;").as_str(),
                    false,
                    Some(Box::new(|result_batches| {
                        let mut row_count = 0;
                        for batch in result_batches {
                            let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                            assert_eq!(batch.num_columns(), 1, "num_cols: {}", batch.num_columns());
                            row_count += batch.num_rows();
                        }
                        assert_eq!(row_count, 10, "num_rows: {row_count}");
                    })),
                )
                .await?;
            }

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_pull_requests_schema_no_comments() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "cookbook".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    None,
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_no_comments_auto",
                "describe cookbook_pulls_auto;",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    insta::assert_snapshot!(
                        "pull_requests_no_comments_schema",
                        batches_to_string(&result_batches)
                    );
                    let total_rows = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    assert_eq!(total_rows, 20);
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_pull_requests_schema_review_comments() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "cookbook".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    Some(HashMap::from([
                        ("github_include_comments".to_string(), "review".to_string()),
                        ("github_max_comments_fetched".to_string(), "100".to_string()),
                    ])),
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_review_comments_auto",
                "describe cookbook_pulls_auto;",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    insta::assert_snapshot!(
                        "pull_requests_review_comments_schema",
                        batches_to_string(&result_batches)
                    );
                    let total_rows = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    assert_eq!(total_rows, 21);
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_pull_requests_schema_discussion_comments() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "cookbook".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    Some(HashMap::from([
                        (
                            "github_include_comments".to_string(),
                            "discussion".to_string(),
                        ),
                        ("github_max_comments_fetched".to_string(), "100".to_string()),
                    ])),
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_discussion_comments_auto",
                "describe cookbook_pulls_auto;",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    insta::assert_snapshot!(
                        "pull_requests_discussion_comments_schema",
                        batches_to_string(&result_batches)
                    );
                    let total_rows = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    assert_eq!(total_rows, 21);
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_pull_requests_schema_all_comments() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "cookbook".to_string(),
                        query_type: "pulls".to_string(),
                    },
                    "auto",
                    Some(HashMap::from([
                        ("github_include_comments".to_string(), "all".to_string()),
                        ("github_max_comments_fetched".to_string(), "100".to_string()),
                    ])),
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_all_comments_auto",
                "describe cookbook_pulls_auto;",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    insta::assert_snapshot!(
                        "pull_requests_all_comments_schema",
                        batches_to_string(&result_batches)
                    );
                    let total_rows = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    assert_eq!(total_rows, 22);
                })),
            )
            .await?;

            Ok(())
        })
        .await
}
