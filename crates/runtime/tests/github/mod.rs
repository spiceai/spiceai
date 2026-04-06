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

use arrow::array::{Array, RecordBatch, StringArray};

use datafusion::common::test_util::batches_to_string;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};

use crate::{
    configure_test_datafusion, init_tracing, run_query_and_check_results,
    utils::{
        register_test_connectors, runtime_ready_check, test_request_context,
        verify_env_secret_exists,
    },
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

fn uses_public_github_rest_api(kind: &GithubDatasetType) -> bool {
    match kind {
        GithubDatasetType::RepoSpecific { query_type, .. } => {
            query_type == "files"
                || query_type.starts_with("files/")
                || query_type == "workflows"
                || query_type.starts_with("workflows/")
        }
        GithubDatasetType::OrgSpecific { .. } => false,
    }
}

fn github_secret_reference(secret_name: &str) -> String {
    format!("${{secrets:{secret_name}}}")
}

fn github_secret_reference_if_available(secret_name: &str) -> Option<String> {
    std::env::var_os(secret_name)
        .filter(|value| !value.is_empty())
        .map(|_| github_secret_reference(secret_name))
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

    let secret_name = match kind {
        GithubDatasetType::OrgSpecific { .. } => "GITHUB_ORG_TOKEN",
        GithubDatasetType::RepoSpecific { .. } => "GITHUB_TOKEN",
    };

    if uses_public_github_rest_api(kind) {
        if let Some(secret_reference) = github_secret_reference_if_available(secret_name) {
            params.insert("github_token".to_string(), secret_reference);
        }
    } else {
        params.insert(
            "github_token".to_string(),
            github_secret_reference(secret_name),
        );
    }

    params.extend(additional_params.unwrap_or_default());

    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

async fn github_secret_available(secret_name: &str, test_name: &str) -> bool {
    match verify_env_secret_exists(secret_name).await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                "Skipping {test_name}: required GitHub secret {secret_name} is unavailable: {err}"
            );
            false
        }
    }
}

async fn repo_github_secret_available(test_name: &str) -> bool {
    github_secret_available("GITHUB_TOKEN", test_name).await
}

async fn org_github_secret_available(test_name: &str) -> bool {
    github_secret_available("GITHUB_ORG_TOKEN", test_name).await
}

fn collect_string_values(result_batches: &[RecordBatch], column_index: usize) -> Vec<String> {
    result_batches
        .iter()
        .flat_map(|batch| {
            let strings = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            (0..strings.len())
                .filter(|&index| !strings.is_null(index))
                .map(|index| strings.value(index).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn assert_all_string_values(result_batches: &[RecordBatch], column_index: usize, expected: &str) {
    let values = collect_string_values(result_batches, column_index);
    assert!(!values.is_empty(), "expected at least one string value");
    assert!(
        values.iter().all(|value| value == expected),
        "expected all values in column {column_index} to equal {expected:?}, got {values:?}"
    );
}

fn assert_github_file_ref_results(result_batches: &[RecordBatch], expected_ref: &str) {
    for batch in result_batches {
        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
    }

    let row_count = result_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    assert_eq!(row_count, 1, "num_rows: {row_count}");

    assert_all_string_values(result_batches, 0, expected_ref);
    assert_all_string_values(result_batches, 1, "README.md");

    let download_urls = collect_string_values(result_batches, 2);
    assert_eq!(download_urls.len(), 1, "download_urls: {download_urls:?}");
    assert!(
        download_urls[0].ends_with(&format!("/{expected_ref}/README.md")),
        "download_url: {}",
        download_urls[0]
    );
}

#[tokio::test]
async fn test_github_issues() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !repo_github_secret_available("test_github_issues").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_commits").await {
        return Ok(());
    }
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let mut commits_ref_dataset = make_github_dataset(
                &GithubDatasetType::RepoSpecific {
                    owner: "spiceai".to_string(),
                    repo: "spiceai".to_string(),
                    query_type: "commits/trunk".to_string(),
                },
                "auto",
                None,
            );
            commits_ref_dataset.name = "spiceai_commits_trunk_auto".to_string();

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
                .with_dataset(commits_ref_dataset)
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
                // This live GitHub test can time out during dataset initialization before EXPLAIN
                // runs, so plan snapshots remain disabled until the setup is made deterministic.
                false,
                Some(Box::new(|result_batches| {
                    let mut row_count = 0;
                    for batch in result_batches {
                        let batch: RecordBatch = batch; // Rust can't type infer here for some reason
                        assert_eq!(batch.num_columns(), 17, "num_cols: {}", batch.num_columns());
                        row_count += batch.num_rows();
                    }
                    assert_eq!(row_count, 10, "num_rows: {row_count}");
                })),
            )
            .await?;

            run_query_and_check_results(
                &mut rt,
                "test_github_commits_ref_filter",
                "SELECT ref, sha, committer_name, changed_files, associated_pull_request_number, status FROM spiceai_commits_auto WHERE ref = 'trunk' LIMIT 5",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 6, "num_cols: {}", batch.num_columns());
                    }
                    assert_eq!(row_count, 5, "num_rows: {row_count}");
                    assert_all_string_values(&result_batches, 0, "trunk");
                })),
            )
            .await?;

            run_query_and_check_results(
                &mut rt,
                "test_github_commits_ref_path",
                "SELECT ref, sha FROM spiceai_commits_trunk_auto LIMIT 5",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert_eq!(row_count, 5, "num_rows: {row_count}");
                    assert_all_string_values(&result_batches, 0, "trunk");

                    let shas = collect_string_values(&result_batches, 1);
                    assert_eq!(shas.len(), row_count, "shas: {shas:?}");
                    assert!(
                        shas.iter().all(|sha| !sha.is_empty()),
                        "expected non-empty shas, got {shas:?}"
                    );
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
async fn test_github_files_ref_resolution() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let mut files_ref_dataset = make_github_dataset(
                &GithubDatasetType::RepoSpecific {
                    owner: "spiceai".to_string(),
                    repo: "spiceai".to_string(),
                    query_type: "files/trunk".to_string(),
                },
                "auto",
                None,
            );
            files_ref_dataset.name = "spiceai_files_trunk_auto".to_string();

            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "files".to_string(),
                    },
                    "auto",
                    None,
                ))
                .with_dataset(files_ref_dataset)
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
                "test_github_files_ref_filter",
                "SELECT ref, path, download_url FROM spiceai_files_auto WHERE ref = 'trunk' AND path = 'README.md' LIMIT 1",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    assert_github_file_ref_results(&result_batches, "trunk");
                })),
            )
            .await?;

            run_query_and_check_results(
                &mut rt,
                "test_github_files_ref_path",
                "SELECT ref, path, download_url FROM spiceai_files_trunk_auto WHERE path = 'README.md' LIMIT 1",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    assert_github_file_ref_results(&result_batches, "trunk");
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_stargazers() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !repo_github_secret_available("test_github_stargazers").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !org_github_secret_available("test_github_org_members").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_pull_requests_projection_limit_pushdown").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_pull_requests_schema_changes").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_pull_requests_schema_no_comments").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_pull_requests_schema_review_comments").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_pull_requests_schema_discussion_comments").await {
        return Ok(());
    }
    register_test_connectors().await;

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
    if !repo_github_secret_available("test_github_pull_requests_schema_all_comments").await {
        return Ok(());
    }
    register_test_connectors().await;

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

#[tokio::test]
async fn test_github_workflows() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_integration_test")
                .with_dataset(make_github_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "workflows".to_string(),
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
                "test_github_workflows_list",
                "select name, path from spiceai_workflows_auto ORDER BY created_at ASC limit 10;",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let pretty_batches = batches_to_string(&result_batches);
                    insta::assert_snapshot!("workflows_list_data", pretty_batches);

                    let total_rows = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    assert_eq!(total_rows, 10);
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_workflow_runs() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let mut workflow_runs_dataset = make_github_dataset(
                &GithubDatasetType::RepoSpecific {
                    owner: "spiceai".to_string(),
                    repo: "spiceai".to_string(),
                    query_type: "workflows/testoperator_run_bench.yml/runs".to_string(),
                },
                "auto",
                None,
            );

            workflow_runs_dataset.name = "spiceai_workflow_runs_auto".to_string();
            if let Some(params) = workflow_runs_dataset.params.as_mut() {
                let mut params_map = params.as_string_map();
                params_map.insert("github_workflow_logs".to_string(), "enabled".to_string());
                *params = spicepod::param::Params::from_string_map(params_map);
            } else {
                let mut params_map = HashMap::new();
                params_map.insert("github_workflow_logs".to_string(), "enabled".to_string());
                workflow_runs_dataset.params =
                    Some(spicepod::param::Params::from_string_map(params_map));
            }

            let app = AppBuilder::new("github_integration_test")
                .with_dataset(workflow_runs_dataset)
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
                "test_github_workflow_runs",
                "describe spiceai_workflow_runs_auto;",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    insta::assert_snapshot!(
                        "workflow_runs_schema",
                        batches_to_string(&result_batches)
                    );

                    let total_rows = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    assert_eq!(total_rows, 13);
                })),
            )
            .await?;

            Ok(())
        })
        .await
}
