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

//! `mode: file_create` recreates a dataset's acceleration file whenever that
//! dataset is loaded, so which datasets a spicepod reload loads is what decides
//! whose accelerated data survives the reload.

use anyhow::anyhow;
use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::acceleration::load_runtime_datasets;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, test_request_context, wait_until_true},
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

fn write_csv_source(path: &Path, rows: u64) -> Result<(), anyhow::Error> {
    let mut csv = String::from("id,payload\n");
    for id in 0..rows {
        writeln!(csv, "{id},payload_{id}")?;
    }
    std::fs::write(path, csv)?;
    Ok(())
}

fn file_create_dataset(from: &str, name: &str, db_path: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        params: Some(Params::from_string_map(
            vec![("duckdb_file".to_string(), db_path.to_string())]
                .into_iter()
                .collect(),
        )),
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::FileCreate,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

fn memory_accelerated_dataset(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

fn row_count(batches: &[RecordBatch]) -> Result<i64, anyhow::Error> {
    batches
        .first()
        .filter(|b| b.num_rows() > 0)
        .ok_or_else(|| anyhow!("expected a count row, got {batches:?}"))?
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|c| c.value(0))
        .ok_or_else(|| anyhow!("count column is not a BIGINT"))
}

async fn wait_for_file(path: &Path) -> Result<(), anyhow::Error> {
    if wait_until_true(LOAD_TIMEOUT, || async { path.exists() }).await {
        Ok(())
    } else {
        Err(anyhow!(
            "acceleration file {} was never created",
            path.display()
        ))
    }
}

/// Applying a spicepod that leaves a dataset's definition untouched must leave
/// that dataset's acceleration alone: its file stays on disk, and the data it
/// already holds is still what the dataset serves. The same reload changes one
/// dataset and adds another, so every arm of the selection is pinned at once.
#[tokio::test]
async fn test_file_create_reload_keeps_unchanged_datasets() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let source = dir.path().join("rows.csv");
            write_csv_source(&source, 8)?;
            let from = format!("file://{}", source.display());

            let kept_db = dir.path().join("kept.db");
            let kept_db_param = kept_db.to_string_lossy().to_string();

            let pod = |edit: bool| {
                let mut edited = memory_accelerated_dataset(&from, "edited");
                let mut builder = AppBuilder::new("test_file_create_reload_keeps_unchanged")
                    .with_dataset(file_create_dataset(&from, "kept", &kept_db_param));
                if edit {
                    edited.params = Some(Params::from_string_map(
                        vec![("file_format".to_string(), "csv".to_string())]
                            .into_iter()
                            .collect(),
                    ));
                    builder = builder.with_dataset(Dataset::new(&from, "added"));
                }
                builder.with_dataset(edited).build()
            };

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(pod(false)).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            wait_for_file(&kept_db).await?;
            for dataset in ["kept", "edited"] {
                let count =
                    row_count(&run_query(&rt, &format!("select count(*) from {dataset}")).await?)?;
                if count != 8 {
                    return Err(anyhow!("{dataset} accelerated {count} rows, expected 8"));
                }
            }

            // Rewriting the source tells "the accelerated data was left alone"
            // apart from "the dataset was reloaded": a reload serves 16 rows.
            write_csv_source(&source, 16)?;

            let reloaded: Arc<App> = Arc::new(pod(true));
            if !Arc::clone(&rt).apply_app(Arc::clone(&reloaded)).await {
                return Err(anyhow!("the runtime did not apply the reloaded spicepod"));
            }

            let edited_reloaded = wait_until_true(LOAD_TIMEOUT, || async {
                run_query(&rt, "select count(*) from edited")
                    .await
                    .ok()
                    .and_then(|batches| row_count(&batches).ok())
                    == Some(16)
            })
            .await;
            if !edited_reloaded {
                return Err(anyhow!(
                    "the reload did not reload the dataset it changed: 'edited' never served the 16 rows now in the source"
                ));
            }

            let added_loaded = wait_until_true(LOAD_TIMEOUT, || async {
                run_query(&rt, "select count(*) from added")
                    .await
                    .ok()
                    .and_then(|batches| row_count(&batches).ok())
                    == Some(16)
            })
            .await;
            if !added_loaded {
                return Err(anyhow!(
                    "the reload did not load the dataset it added: 'added' never served the 16 rows in the source"
                ));
            }

            if !kept_db.exists() {
                return Err(anyhow!(
                    "reload deleted the acceleration file for the unchanged dataset 'kept': {}",
                    kept_db.display()
                ));
            }
            let kept_rows = row_count(&run_query(&rt, "select count(*) from kept").await?)?;
            if kept_rows != 8 {
                return Err(anyhow!(
                    "unchanged dataset 'kept' served {kept_rows} rows after the reload, expected the 8 it was accelerated with"
                ));
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// The first-load half of the `file_create` contract, which the reload fix must
/// not weaken: a first load still discards whatever the configured file held.
#[tokio::test]
async fn test_file_create_recreates_existing_file_on_first_load() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope_retry(3, || async {
            let dir = tempfile::tempdir()?;
            let source = dir.path().join("rows.csv");
            write_csv_source(&source, 8)?;
            let from = format!("file://{}", source.display());

            let db_file = dir.path().join("stale.db");
            std::fs::write(&db_file, b"stale acceleration file")?;

            let app = AppBuilder::new("test_file_create_recreates_existing_file")
                .with_dataset(file_create_dataset(
                    &from,
                    "fresh",
                    &db_file.to_string_lossy(),
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            wait_for_file(&db_file).await?;
            let count = row_count(&run_query(&rt, "select count(*) from fresh").await?)?;
            if count != 8 {
                return Err(anyhow!("fresh accelerated {count} rows, expected 8"));
            }

            // A DuckDB database that still carried the placeholder bytes could not
            // have been opened, let alone accelerated.
            let recreated = std::fs::read(&db_file)?;
            if recreated.starts_with(b"stale acceleration file") {
                return Err(anyhow!(
                    "first load reused the existing file instead of recreating it"
                ));
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}
