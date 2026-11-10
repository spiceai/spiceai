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

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;

use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::{
    acceleration::get_params,
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

#[expect(clippy::expect_used)]
fn get_dataset() -> Dataset {
    let path = std::env::current_dir()
        .expect("get current directory")
        .join("test_file_watcher.csv");
    Dataset::new(format!("file://{}", path.display()), "names")
}

const NAMES_CSV: &str = include_str!("data/names.csv");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_watcher() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Write the CSV to a file next to the test binary
            std::fs::write("./test_file_watcher.csv", NAMES_CSV).expect("write file");

            let mut dataset = get_dataset();
            let params = get_params(
                &Mode::File,
                Some("./test_file_watcher.db".to_string()),
                "sqlite",
            )
            .map(|params| {
                let mut map = params.as_string_map();
                map.insert("file_watcher".to_string(), "enabled".to_string());
                Params::from_string_map(map)
            });
            dataset.acceleration = Some(Acceleration {
                params,
                enabled: true,
                engine: Some("sqlite".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                refresh_sql: None,
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_file_watcher")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM names ORDER BY id")
                .build()
                .run()
                .await
                .expect("query is successful")
                .data
                .try_collect()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            // Append a new row to the CSV file
            let new_row = "11,Spaceman,29,LEO,100\n";
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_file_watcher.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // Poll until the file watcher detects the change and the refresh completes
            let start = std::time::Instant::now();
            let result: Vec<RecordBatch> = loop {
                let batches: Vec<RecordBatch> = rt
                    .datafusion()
                    .query_builder("SELECT * FROM names ORDER BY id")
                    .build()
                    .run()
                    .await
                    .expect("query is successful")
                    .data
                    .try_collect()
                    .await
                    .expect("collects results");

                let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                if row_count >= 11 {
                    break batches;
                }

                assert!(
                    start.elapsed() <= std::time::Duration::from_secs(30),
                    "Timed out waiting for file watcher refresh (got {row_count} rows, expected 11)"
                );

                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            };

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            drop(rt);

            // Remove the files
            std::fs::remove_file("./test_file_watcher.db").expect("remove file");
            let _ = std::fs::remove_file("./test_file_watcher.db-shm");
            let _ = std::fs::remove_file("./test_file_watcher.db-wal");
            std::fs::remove_file("./test_file_watcher.csv").expect("remove file");

            Ok(())
        })
        .await
}

fn watcher_dataset(csv_path: &Path, db_path: &Path) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", csv_path.display()), "names");
    let params =
        get_params(&Mode::File, Some(db_path.display().to_string()), "sqlite").map(|params| {
            let mut map = params.as_string_map();
            map.insert("file_watcher".to_string(), "enabled".to_string());
            Params::from_string_map(map)
        });
    dataset.acceleration = Some(Acceleration {
        params,
        enabled: true,
        engine: Some("sqlite".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: None,
        ..Acceleration::default()
    });
    dataset
}

async fn start_watcher_runtime(
    csv_path: &Path,
    db_path: &Path,
) -> Result<Arc<Runtime>, anyhow::Error> {
    std::fs::write(csv_path, NAMES_CSV).expect("write file");
    let app = AppBuilder::new("test_file_watcher")
        .with_dataset(watcher_dataset(csv_path, db_path))
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

async fn names_row_count(rt: &Runtime) -> usize {
    let batches: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder("SELECT * FROM names ORDER BY id")
        .build()
        .run()
        .await
        .expect("query is successful")
        .data
        .try_collect()
        .await
        .expect("collects results");
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn wait_for_row_count(rt: &Runtime, expected: usize) -> Result<(), anyhow::Error> {
    let start = std::time::Instant::now();
    loop {
        let row_count = names_row_count(rt).await;
        if row_count == expected {
            return Ok(());
        }
        if start.elapsed() > std::time::Duration::from_secs(30) {
            return Err(anyhow::Error::msg(format!(
                "Timed out waiting for file watcher refresh (got {row_count} rows, expected {expected})"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

fn csv_with_rows(n: usize) -> String {
    use std::fmt::Write as _;

    let mut out = String::from("id,name,age,city,score\n");
    for i in 1..=n {
        let _ = writeln!(out, "{i},name{i},20,City,1");
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_watcher_atomic_replace_then_append() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let csv_path = dir.path().join("names.csv");
            let db_path = dir.path().join("names.db");
            let rt = start_watcher_runtime(&csv_path, &db_path).await?;
            wait_for_row_count(&rt, 10).await?;

            let tmp = dir.path().join("names.csv.tmp");
            std::fs::write(&tmp, csv_with_rows(11)).expect("write tmp");
            std::fs::rename(&tmp, &csv_path).expect("atomic replace");
            wait_for_row_count(&rt, 11).await?;

            std::fs::OpenOptions::new()
                .append(true)
                .open(&csv_path)
                .expect("open file")
                .write_all(b"12,name12,20,City,1\n")
                .expect("append after replace");
            wait_for_row_count(&rt, 12).await?;

            drop(rt);
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_watcher_trailing_debounce_keeps_burst_appends() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let csv_path = dir.path().join("names.csv");
            let db_path = dir.path().join("names.db");
            let rt = start_watcher_runtime(&csv_path, &db_path).await?;
            wait_for_row_count(&rt, 10).await?;

            let mut expected = 10usize;
            for i in 0..5 {
                let first = 11 + i * 2;
                let second = first + 1;
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&csv_path)
                    .expect("open file")
                    .write_all(format!("{first},name{first},20,City,1\n").as_bytes())
                    .expect("append first");
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&csv_path)
                    .expect("open file")
                    .write_all(format!("{second},name{second},20,City,1\n").as_bytes())
                    .expect("append second");
                expected += 2;
                wait_for_row_count(&rt, expected).await?;
            }

            drop(rt);
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_watcher_large_inplace_rewrite() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let csv_path = dir.path().join("names.csv");
            let db_path = dir.path().join("names.db");
            let rt = start_watcher_runtime(&csv_path, &db_path).await?;
            wait_for_row_count(&rt, 10).await?;

            for n in [3usize, 20, 7] {
                std::fs::write(&csv_path, csv_with_rows(n)).expect("rewrite file");
                wait_for_row_count(&rt, n).await?;
            }

            drop(rt);
            Ok(())
        })
        .await
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_file_watcher_symlink_to_another_directory() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let other = dir.path().join("other");
            std::fs::create_dir(&other).expect("create other dir");
            let real = other.join("actual.csv");
            std::fs::write(&real, NAMES_CSV).expect("write target");
            let csv_path = dir.path().join("names.csv");
            std::os::unix::fs::symlink(&real, &csv_path).expect("symlink dataset path");
            let db_path = dir.path().join("names.db");
            // `start_watcher_runtime` writes through the symlink onto the target.
            let rt = start_watcher_runtime(&csv_path, &db_path).await?;
            wait_for_row_count(&rt, 10).await?;

            std::fs::OpenOptions::new()
                .append(true)
                .open(&csv_path)
                .expect("open symlink")
                .write_all(b"11,name11,20,City,1\n")
                .expect("append via symlink");
            wait_for_row_count(&rt, 11).await?;

            std::fs::OpenOptions::new()
                .append(true)
                .open(&real)
                .expect("open target")
                .write_all(b"12,name12,20,City,1\n")
                .expect("append via target");
            wait_for_row_count(&rt, 12).await?;

            // Delete the target, then recreate it: the watch on its directory
            // must survive the window where the symlink dangles.
            std::fs::remove_file(&real).expect("delete target");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            std::fs::write(&real, csv_with_rows(13)).expect("recreate target");
            wait_for_row_count(&rt, 13).await?;

            std::fs::OpenOptions::new()
                .append(true)
                .open(&real)
                .expect("open recreated target")
                .write_all(b"14,name14,20,City,1\n")
                .expect("append to recreated target");
            wait_for_row_count(&rt, 14).await?;

            drop(rt);
            Ok(())
        })
        .await
}
