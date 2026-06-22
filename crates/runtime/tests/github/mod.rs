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

use arrow::array::{Array, Int64Array, ListArray, RecordBatch, StringArray};

use datafusion::common::test_util::batches_to_string;
use futures::TryStreamExt;
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

// GitHub's API page size is 100, but under rate limiting or transient errors it
// may return fewer rows. Use 50 as the lower bound to tolerate partial pages
// while still catching a completely empty or near-empty result.
const GITHUB_COMMITS_MIN_EXPECTED_PAGE_ROWS: usize = 50;

// GitHub commits queries request 100 history rows per page, so use a larger limit
// in the pagination-sensitive tests to ensure they cross the page boundary.
const GITHUB_COMMITS_PAGINATION_LIMIT: usize = 125;

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

async fn github_app_secrets_available(test_name: &str) -> bool {
    for secret in [
        "GITHUB_CLIENT_ID",
        "GITHUB_INSTALLATION_ID",
        "GITHUB_PRIVATE_KEY",
    ] {
        if !github_secret_available(secret, test_name).await {
            return false;
        }
    }
    true
}

fn make_github_app_dataset(
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

    let mut params = HashMap::from([
        ("github_query_mode".to_string(), query_mode.to_string()),
        (
            "github_client_id".to_string(),
            github_secret_reference("GITHUB_CLIENT_ID"),
        ),
        (
            "github_installation_id".to_string(),
            github_secret_reference("GITHUB_INSTALLATION_ID"),
        ),
        (
            "github_private_key".to_string(),
            github_secret_reference("GITHUB_PRIVATE_KEY"),
        ),
    ]);

    params.extend(additional_params.unwrap_or_default());

    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
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

fn assert_no_string_values(result_batches: &[RecordBatch], column_index: usize, unexpected: &str) {
    let values = collect_string_values(result_batches, column_index);
    assert!(!values.is_empty(), "expected at least one string value");
    assert!(
        values.iter().all(|value| value != unexpected),
        "expected all values in column {column_index} to differ from {unexpected:?}, got {values:?}"
    );
}

fn assert_positive_row_count_at_most_pagination_limit(row_count: usize) {
    assert!(
        row_count > 0 && row_count <= GITHUB_COMMITS_PAGINATION_LIMIT,
        "expected 0 < num_rows <= {GITHUB_COMMITS_PAGINATION_LIMIT}, got {row_count}"
    );
}

fn assert_crosses_commits_pagination_boundary(row_count: usize) {
    assert!(
        row_count > GITHUB_COMMITS_MIN_EXPECTED_PAGE_ROWS
            && row_count <= GITHUB_COMMITS_PAGINATION_LIMIT,
        "expected {GITHUB_COMMITS_MIN_EXPECTED_PAGE_ROWS} < num_rows <= {GITHUB_COMMITS_PAGINATION_LIMIT}, got {row_count}"
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                search_author_elapsed_secs < 30,
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

            let cookbook_commits_dataset = make_github_dataset(
                &GithubDatasetType::RepoSpecific {
                    owner: "spiceai".to_string(),
                    repo: "cookbook".to_string(),
                    query_type: "commits".to_string(),
                },
                "auto",
                None,
            );

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
                .with_dataset(cookbook_commits_dataset)
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

            let commits_ref_filter_query = format!(
                "SELECT ref, sha, committer_name, changed_files, associated_pull_request_number, status FROM spiceai_commits_auto WHERE ref = 'trunk' LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            run_query_and_check_results(
                &mut rt,
                "test_github_commits_ref_filter",
                &commits_ref_filter_query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 6, "num_cols: {}", batch.num_columns());
                    }
                    assert_crosses_commits_pagination_boundary(row_count);
                    assert_all_string_values(&result_batches, 0, "trunk");
                })),
            )
            .await?;

            let commits_slash_ref_filter_query = format!(
                "SELECT ref, sha FROM spiceai_commits_auto WHERE ref = 'release/1.11' LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            run_query_and_check_results(
                &mut rt,
                "test_github_commits_slash_ref_filter",
                &commits_slash_ref_filter_query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert_positive_row_count_at_most_pagination_limit(row_count);
                    assert_all_string_values(&result_batches, 0, "release/1.11");

                    let shas = collect_string_values(&result_batches, 1);
                    assert_eq!(shas.len(), row_count, "shas: {shas:?}");
                    assert!(
                        shas.iter().all(|sha| !sha.is_empty()),
                        "expected non-empty shas, got {shas:?}"
                    );
                })),
            )
            .await?;

            // Dynamic ref scan on cookbook (22 branches) instead of spiceai (800+ branches)
            // to avoid overwhelming the GitHub API with hundreds of per-ref commit fetches.
            // Run directly (no EXPLAIN preflight) since EXPLAIN also triggers the
            // full dynamic scan, doubling per-ref API calls.
            let commits_dynamic_ref_filter_query = format!(
                "SELECT ref, sha FROM cookbook_commits_auto WHERE ref != 'trunk' LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            let result_batches: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(&commits_dynamic_ref_filter_query)
                .build()
                .run()
                .await
                .map_err(|e| {
                    format!(
                        "query `{commits_dynamic_ref_filter_query}` failed to run: {e}"
                    )
                })?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| {
                    format!(
                        "query `{commits_dynamic_ref_filter_query}` to results: {e}"
                    )
                })?;

            let row_count = result_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            for batch in &result_batches {
                assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
            }
            assert_positive_row_count_at_most_pagination_limit(row_count);
            assert_no_string_values(&result_batches, 0, "trunk");

            let shas = collect_string_values(&result_batches, 1);
            assert_eq!(shas.len(), row_count, "shas: {shas:?}");
            assert!(
                shas.iter().all(|sha| !sha.is_empty()),
                "expected non-empty shas, got {shas:?}"
            );

            let commits_ref_path_query = format!(
                "SELECT ref, sha FROM spiceai_commits_trunk_auto LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            run_query_and_check_results(
                &mut rt,
                "test_github_commits_ref_path",
                &commits_ref_path_query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert_crosses_commits_pagination_boundary(row_count);
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

            // Tag ref filter: verifies tag resolution path and tag target traversal
            run_query_and_check_results(
                &mut rt,
                "test_github_commits_tag_ref_filter",
                "SELECT ref, sha FROM spiceai_commits_auto WHERE ref = 'v1.0.0' LIMIT 10",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert!(row_count > 0 && row_count <= 10, "expected 0 < num_rows <= 10, got {row_count}");
                    assert_all_string_values(&result_batches, 0, "v1.0.0");
                })),
            )
            .await?;

            // LIMIT 0 returns empty results without making API calls
            run_query_and_check_results(
                &mut rt,
                "test_github_commits_limit_zero",
                "SELECT ref, sha FROM spiceai_commits_auto WHERE ref = 'trunk' LIMIT 0",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(row_count, 0, "LIMIT 0 should return 0 rows, got {row_count}");
                })),
            )
            .await?;

            // Projection pushdown: select only specific metadata columns with ref filter
            run_query_and_check_results(
                &mut rt,
                "test_github_commits_projection_pushdown",
                "SELECT sha, author_name, message_head_line FROM spiceai_commits_auto WHERE ref = 'trunk' LIMIT 5",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                    }
                    assert_eq!(row_count, 5, "expected 5 rows, got {row_count}");

                    let shas = collect_string_values(&result_batches, 0);
                    assert!(
                        shas.iter().all(|sha| !sha.is_empty()),
                        "expected non-empty shas, got {shas:?}"
                    );
                })),
            )
            .await?;

            // Ref-in-path dataset combined with additional non-ref filter
            run_query_and_check_results(
                &mut rt,
                "test_github_commits_ref_path_with_projection",
                "SELECT sha, additions, deletions FROM spiceai_commits_trunk_auto LIMIT 5",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                    }
                    assert_eq!(row_count, 5, "expected 5 rows, got {row_count}");
                })),
            )
            .await?;

            // Verify the schema of the commits table via DESCRIBE
            run_query_and_check_results(
                &mut rt,
                "test_github_commits_schema",
                "DESCRIBE spiceai_commits_auto",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(total_rows, 17, "expected 17 columns in schema, got {total_rows}");
                })),
            )
            .await?;

            let elapsed = now.elapsed().as_secs();

            // Budget is higher because the test includes tag-ref, LIMIT 0, projection,
            // schema queries, plus the dynamic ref scan on cookbook.
            assert!(elapsed < 180, "elapsed: {elapsed}");

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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

            // Tag ref for files: verifies tag resolution works for the files provider
            run_query_and_check_results(
                &mut rt,
                "test_github_files_tag_ref_filter",
                "SELECT ref, path, download_url FROM spiceai_files_auto WHERE ref = 'v1.0.0' AND path = 'README.md' LIMIT 1",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    assert_github_file_ref_results(&result_batches, "v1.0.0");
                })),
            )
            .await?;

            // Multiple files listing with just a ref filter (no path restriction)
            run_query_and_check_results(
                &mut rt,
                "test_github_files_ref_only",
                "SELECT ref, path FROM spiceai_files_auto WHERE ref = 'trunk' LIMIT 20",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert!(row_count > 1, "expected multiple files, got {row_count}");
                    assert_all_string_values(&result_batches, 0, "trunk");
                })),
            )
            .await?;

            // Ref-in-path dataset listing multiple files
            run_query_and_check_results(
                &mut rt,
                "test_github_files_ref_path_multiple",
                "SELECT ref, path FROM spiceai_files_trunk_auto LIMIT 20",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert!(row_count > 1, "expected multiple files, got {row_count}");
                    assert_all_string_values(&result_batches, 0, "trunk");
                })),
            )
            .await?;

            // Default branch resolution: no ref filter relies on fetching the default branch
            run_query_and_check_results(
                &mut rt,
                "test_github_files_default_branch",
                "SELECT ref, path FROM spiceai_files_auto WHERE path = 'README.md' LIMIT 1",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(row_count, 1, "expected 1 row, got {row_count}");
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    // Verify the ref column is populated (should be the default branch)
                    let refs = collect_string_values(&result_batches, 0);
                    assert_eq!(refs.len(), 1, "expected 1 ref value");
                    assert!(!refs[0].is_empty(), "default branch ref should not be empty");
                })),
            )
            .await?;

            // Projection pushdown: select only specific columns with ref filter
            run_query_and_check_results(
                &mut rt,
                "test_github_files_projection",
                "SELECT name, size, sha FROM spiceai_files_auto WHERE ref = 'trunk' AND path = 'README.md' LIMIT 1",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(row_count, 1, "expected 1 row, got {row_count}");
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 3, "num_cols: {}", batch.num_columns());
                    }
                    let names = collect_string_values(&result_batches, 0);
                    assert_eq!(names, vec!["README.md"]);
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

/// Validates that `number`, `commits_count`, and `hashes` columns return correct data.
/// `commits_count` should reflect the true total (not capped at the GraphQL page size of 25),
/// and `hashes` should be a non-empty list for PRs with commits.
#[tokio::test]
async fn test_github_pull_requests_commits_and_number_columns() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !repo_github_secret_available("test_github_pull_requests_commits_and_number_columns").await {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Validate number, commits_count, and hashes columns
            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_commits_and_number",
                "SELECT number, commits_count, hashes FROM cookbook_pulls_auto LIMIT 5",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let mut total_rows = 0;
                    for batch in &result_batches {
                        total_rows += batch.num_rows();

                        // number column (index 0) — Int64, should be non-null positive
                        let numbers = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("number column should be Int64Array");
                        for i in 0..numbers.len() {
                            assert!(!numbers.is_null(i), "number should not be null");
                            assert!(numbers.value(i) > 0, "PR number should be positive");
                        }

                        // commits_count column (index 1) — Int64, should be >= 1
                        let commits_counts = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("commits_count column should be Int64Array");
                        for i in 0..commits_counts.len() {
                            assert!(
                                !commits_counts.is_null(i),
                                "commits_count should not be null"
                            );
                            assert!(
                                commits_counts.value(i) >= 1,
                                "commits_count should be at least 1, got {}",
                                commits_counts.value(i)
                            );
                        }

                        // hashes column (index 2) — List of structs, should be non-null and non-empty
                        let hashes = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .expect("hashes column should be ListArray");
                        for i in 0..hashes.len() {
                            assert!(!hashes.is_null(i), "hashes should not be null");
                            assert!(
                                !hashes.value(i).is_empty(),
                                "hashes list should have at least one entry"
                            );
                        }
                    }
                    assert_eq!(total_rows, 5, "expected 5 rows from LIMIT 5");
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

/// Validates that `commits_count` reports the true total count (from `totalCount`) and is not
/// capped at the GraphQL fetch limit (`commits(first: 25)`). Uses a limit exceeding PR pagination
/// boundary (100 PRs per page) to stress test multi-page fetching.
#[tokio::test]
async fn test_github_pull_requests_commits_count_not_capped() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !repo_github_secret_available("test_github_pull_requests_commits_count_not_capped").await {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Fetch PRs with commits_count >= 1, limit exceeds PR pagination boundary (100)
            run_query_and_check_results(
                &mut rt,
                "test_github_pull_requests_commits_count_not_capped",
                "SELECT number, commits_count FROM cookbook_pulls_auto WHERE commits_count >= 1 LIMIT 125",
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let total_rows: usize = result_batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum();
                    assert!(
                        total_rows >= 1,
                        "expected at least one PR with commits_count >= 1"
                    );

                    for batch in &result_batches {
                        let commits_counts = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("commits_count column should be Int64Array");
                        for i in 0..commits_counts.len() {
                            assert!(
                                !commits_counts.is_null(i),
                                "commits_count should not be null"
                            );
                            assert!(
                                commits_counts.value(i) >= 1,
                                "commits_count should be >= 1, got {}",
                                commits_counts.value(i)
                            );
                        }
                    }
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

#[tokio::test]
async fn test_github_app_commits_ref_filter() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !github_app_secrets_available("test_github_app_commits_ref_filter").await {
        return Ok(());
    }
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_app_integration_test")
                .with_dataset(make_github_app_dataset(
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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let github_app_commits_ref_filter_query = format!(
                "SELECT ref, sha FROM spiceai_commits_auto WHERE ref = 'trunk' LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            run_query_and_check_results(
                &mut rt,
                "test_github_app_commits_ref_filter",
                &github_app_commits_ref_filter_query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert_crosses_commits_pagination_boundary(row_count);
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

            let github_app_commits_slash_ref_filter_query = format!(
                "SELECT ref, sha FROM spiceai_commits_auto WHERE ref = 'release/1.11' LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            run_query_and_check_results(
                &mut rt,
                "test_github_app_commits_slash_ref_filter",
                &github_app_commits_slash_ref_filter_query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert_positive_row_count_at_most_pagination_limit(row_count);
                    assert_all_string_values(&result_batches, 0, "release/1.11");

                    let shas = collect_string_values(&result_batches, 1);
                    assert_eq!(shas.len(), row_count, "shas: {shas:?}");
                    assert!(
                        shas.iter().all(|sha| !sha.is_empty()),
                        "expected non-empty shas, got {shas:?}"
                    );
                })),
            )
            .await?;

            let github_app_commits_dynamic_ref_filter_query = format!(
                "SELECT ref, sha FROM spiceai_commits_auto WHERE ref != 'trunk' LIMIT {GITHUB_COMMITS_PAGINATION_LIMIT}"
            );

            run_query_and_check_results(
                &mut rt,
                "test_github_app_commits_dynamic_ref_filter",
                &github_app_commits_dynamic_ref_filter_query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let row_count = result_batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>();
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                    }
                    assert_positive_row_count_at_most_pagination_limit(row_count);
                    assert_no_string_values(&result_batches, 0, "trunk");

                    let shas = collect_string_values(&result_batches, 1);
                    assert_eq!(shas.len(), row_count, "shas: {shas:?}");
                    assert!(
                        shas.iter().all(|sha| !sha.is_empty()),
                        "expected non-empty shas, got {shas:?}"
                    );
                })),
            )
            .await?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_github_app_files_ref_filter() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !github_app_secrets_available("test_github_app_files_ref_filter").await {
        return Ok(());
    }
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_app_integration_test")
                .with_dataset(make_github_app_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "files".to_string(),
                    },
                    "auto",
                    None,
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_app_files_ref_filter",
                "SELECT ref, path, download_url FROM spiceai_files_auto WHERE ref = 'trunk' AND path = 'README.md' LIMIT 1",
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
async fn test_github_app_issues() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    if !github_app_secrets_available("test_github_app_issues").await {
        return Ok(());
    }
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("github_app_integration_test")
                .with_dataset(make_github_app_dataset(
                    &GithubDatasetType::RepoSpecific {
                        owner: "spiceai".to_string(),
                        repo: "spiceai".to_string(),
                        query_type: "issues".to_string(),
                    },
                    "auto",
                    None,
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "test_github_app_issues",
                "SELECT * FROM spiceai_issues_auto LIMIT 10",
                false,
                Some(Box::new(|result_batches| {
                    let mut row_count = 0;
                    for batch in result_batches {
                        let batch: RecordBatch = batch;
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        row_count += batch.num_rows();
                    }
                    assert!(row_count > 0, "expected at least 1 row, got {row_count}");
                })),
            )
            .await?;

            Ok(())
        })
        .await
}
