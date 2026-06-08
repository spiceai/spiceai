/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::{collections::HashMap, path::Path, process::Command, sync::Arc};

use app::AppBuilder;
// `Array` is the trait that exposes `.len()` on concrete Arrow array types
// (e.g. `StringArray::len`). rustc reports it as unused if we rely solely on
// method syntax, so pull it in via an aliased import to keep both the trait
// methods available and the lint happy.
use arrow::array::Array as _;
use runtime::Runtime;
use spicepod::{
    component::dataset::Dataset,
    param::{ParamValue, Params},
};
use tempfile::TempDir;

use crate::{
    configure_test_datafusion, init_tracing, run_query_and_check_results,
    utils::test_request_context,
};

/// Returns `true` when the `git` CLI is available on `PATH`.
fn git_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .map_or(false, |o| o.status.success())
}

/// Initialize a local git repository with a small, deterministic file set so
/// the tests can exercise the connector end-to-end without any network
/// access.
fn init_test_repo(root: &Path) -> anyhow::Result<()> {
    let run = |args: &[&str]| -> anyhow::Result<()> {
        let status = Command::new("git")
            .args(args)
            .current_dir(root)
            .env("GIT_AUTHOR_NAME", "Spice Test")
            .env("GIT_AUTHOR_EMAIL", "test@spice.ai")
            .env("GIT_COMMITTER_NAME", "Spice Test")
            .env("GIT_COMMITTER_EMAIL", "test@spice.ai")
            .status()?;
        if !status.success() {
            anyhow::bail!("git {args:?} failed with status {status}");
        }
        Ok(())
    };

    run(&["init", "--initial-branch=main"])?;
    run(&["config", "commit.gpgsign", "false"])?;
    run(&["config", "user.name", "Spice Test"])?;
    run(&["config", "user.email", "test@spice.ai"])?;

    std::fs::write(root.join("README.md"), "# Sample Repo\n")?;
    std::fs::create_dir_all(root.join("src"))?;
    std::fs::write(root.join("src/main.rs"), "fn main() {}\n")?;
    std::fs::write(
        root.join("src/lib.rs"),
        "pub fn add(a: i32, b: i32) -> i32 { a + b }\n",
    )?;
    std::fs::write(root.join("config.yaml"), "version: 1\n")?;

    run(&["add", "."])?;
    run(&["commit", "-m", "seed repo"])?;
    Ok(())
}

fn make_git_dataset(repo_path: &Path, cache_path: &Path, include: Option<&str>) -> Dataset {
    // Build the `file://` URL via `Url::from_file_path` so Windows paths
    // (which use backslashes and drive letters) are encoded correctly.
    let file_url =
        url::Url::from_file_path(repo_path).expect("repo path must be an absolute filesystem path");
    let mut dataset = Dataset::new(format!("git:{file_url}@main"), "git_test");

    let mut params = HashMap::new();
    if let Some(pattern) = include {
        params.insert(
            "include".to_string(),
            ParamValue::String(pattern.to_string()),
        );
    }
    params.insert(
        "cache_path".to_string(),
        ParamValue::String(cache_path.to_string_lossy().into_owned()),
    );

    dataset.params = Some(Params { data: params });
    dataset
}

#[tokio::test]
async fn git_connector_local_repo_lists_files() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    if !git_available() {
        eprintln!("Skipping git_connector_local_repo_lists_files: git CLI not available");
        return Ok(());
    }

    test_request_context()
        .scope(async move {
            let repo_dir = TempDir::new()?;
            let cache_root = TempDir::new()?;
            let cache_dir_owned = cache_root.path().join("clone");
            init_test_repo(repo_dir.path())?;

            let app = AppBuilder::new("git_connector_local_repo")
                .with_dataset(make_git_dataset(repo_dir.path(), &cache_dir_owned, None))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            run_query_and_check_results(
                &mut rt,
                "git_connector_local_repo_lists_files",
                "SELECT COUNT(*) AS total FROM git_test",
                false,
                Some(Box::new(|batches: Vec<arrow::array::RecordBatch>| {
                    assert_eq!(batches.len(), 1);
                    let batch = &batches[0];
                    let total = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("int64 count")
                        .value(0);
                    assert_eq!(total, 4, "expected 4 files in the seed repo");
                })),
            )
            .await
            .map_err(|e| anyhow::anyhow!("query failed: {e}"))?;

            Ok(())
        })
        .await
}

#[tokio::test]
async fn git_connector_local_repo_include_glob() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    if !git_available() {
        eprintln!("Skipping git_connector_local_repo_include_glob: git CLI not available");
        return Ok(());
    }

    test_request_context()
        .scope(async move {
            let repo_dir = TempDir::new()?;
            let cache_root = TempDir::new()?;
            let cache_dir_owned = cache_root.path().join("clone");
            init_test_repo(repo_dir.path())?;

            let app = AppBuilder::new("git_connector_local_repo_glob")
                .with_dataset(make_git_dataset(
                    repo_dir.path(),
                    &cache_dir_owned,
                    Some("src/**/*.rs"),
                ))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            run_query_and_check_results(
                &mut rt,
                "git_connector_local_repo_include_glob",
                "SELECT path FROM git_test ORDER BY path",
                false,
                Some(Box::new(|batches: Vec<arrow::array::RecordBatch>| {
                    let total: usize = batches
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum();
                    assert_eq!(total, 2, "expected 2 rust files, got {total}");
                    let paths: Vec<String> = batches
                        .iter()
                        .flat_map(|b| {
                            let col = b
                                .column(0)
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .expect("utf8 column");
                            (0..col.len())
                                .map(|i| col.value(i).to_string())
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    assert!(paths.iter().any(|p| p == "src/lib.rs"), "paths: {paths:?}");
                    assert!(paths.iter().any(|p| p == "src/main.rs"), "paths: {paths:?}");
                })),
            )
            .await
            .map_err(|e| anyhow::anyhow!("query failed: {e}"))?;

            Ok(())
        })
        .await
}
