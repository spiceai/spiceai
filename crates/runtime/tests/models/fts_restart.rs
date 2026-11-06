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

//! A full-text search over a file-accelerated dataset must keep returning its rows after a
//! restart.
//!
//! The default in-memory full-text index starts every process empty, and a checkpointed
//! `mode: file` dataset skips its startup refresh because the acceleration already holds its
//! rows. Regression test for #14618: the index has to be rebuilt from the acceleration, through
//! the wrapped `FullTextConnector` index the accelerated table discovers, before the dataset is
//! registered.
//!
//! Covers both DuckDB (the original #14618 engine) and Cayenne. Cayenne was still empty after a
//! restart on v2.4.1 because #14954 had not been backported; the rebuild path is engine-agnostic
//! once present, and this test keeps Cayenne on the matrix so a future regression cannot hide
//! behind DuckDB-only coverage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use app::AppBuilder;
use arrow::array::{Array, Int64Array};
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use spicepod::semantic::{Column, FullTextSearchConfig};

use crate::utils::{register_test_connectors, run_query, runtime_ready_check};
use crate::{configure_test_datafusion, init_tracing};

const LOAD_TIMEOUT: Duration = Duration::from_mins(2);

fn docs_columns() -> Vec<Column> {
    vec![
        Column::new("text")
            .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
    ]
}

#[cfg(feature = "duckdb")]
fn duckdb_docs_dataset(source: &str, db_path: &str) -> Dataset {
    let mut dataset = Dataset::new(source, "docs");
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "csv".to_string(),
    )])));
    dataset.columns = docs_columns();
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(HashMap::from([(
            "duckdb_file".to_string(),
            db_path.to_string(),
        )]))),
        ..Acceleration::default()
    });
    dataset
}

fn cayenne_docs_dataset(source: &str, data_dir: &str, metadata_dir: &str) -> Dataset {
    let mut dataset = Dataset::new(source, "docs");
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "csv".to_string(),
    )])));
    dataset.columns = docs_columns();
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        // Keep data files and the metastore inside this run's temp directory so the test
        // does not collide with other Cayenne file-mode datasets in the same process.
        params: Some(Params::from_string_map(HashMap::from([
            ("cayenne_file_path".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]))),
        ..Acceleration::default()
    });
    dataset
}

async fn start_with(dataset: Dataset) -> anyhow::Result<Arc<Runtime>> {
    let app = AppBuilder::new("fts_restart").with_dataset(dataset).build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let load_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(LOAD_TIMEOUT) => anyhow::bail!("timed out loading components"),
        () = load_rt.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// The ids `text_search` finds for `term`.
async fn search_ids(rt: &Arc<Runtime>, term: &str) -> anyhow::Result<Vec<i64>> {
    let batches = run_query(
        rt,
        &format!("SELECT id FROM text_search(docs, '{term}', text) ORDER BY id"),
    )
    .await?;
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch.column_by_name("id").context("no `id` column")?;
        let column = arrow::compute::cast(column, &arrow::datatypes::DataType::Int64)?;
        let column = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("`id` did not cast to BIGINT")?;
        ids.extend((0..column.len()).map(|row| column.value(row)));
    }
    Ok(ids)
}

async fn row_count(rt: &Arc<Runtime>) -> anyhow::Result<i64> {
    let batches = run_query(rt, "SELECT count(*) FROM docs").await?;
    batches
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|count| count.value(0))
        .context("no count row")
}

async fn write_fixture(dir: &tempfile::TempDir) -> anyhow::Result<String> {
    let csv = dir.path().join("docs.csv");
    std::fs::write(&csv, "id,text\n1,alpha\n2,beta\n3,gamma\n")?;
    Ok(format!("file://{}", csv.display()))
}

/// Shared restart assertion: first load indexes, restart reusing the acceleration answers.
async fn assert_persisted_rows_survive_restart(
    first: Arc<Runtime>,
    restart: impl std::future::Future<Output = anyhow::Result<Arc<Runtime>>>,
) -> anyhow::Result<()> {
    assert_eq!(
        search_ids(&first, "alpha").await?,
        vec![1],
        "the first load indexes the rows it writes"
    );
    first.shutdown().await;
    drop(first);

    let restarted = restart.await?;
    assert_eq!(
        row_count(&restarted).await?,
        3,
        "the acceleration kept its rows"
    );
    assert_eq!(
        search_ids(&restarted, "alpha").await?,
        vec![1],
        "after a restart the full-text index must answer for the rows the acceleration kept"
    );
    restarted.shutdown().await;
    Ok(())
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn full_text_search_finds_persisted_rows_after_a_restart() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    configure_test_datafusion();
    register_test_connectors().await;

    let dir = tempfile::tempdir()?;
    let source = write_fixture(&dir).await?;
    let db_path = dir.path().join("docs.db").to_string_lossy().to_string();

    let first = start_with(duckdb_docs_dataset(&source, &db_path)).await?;
    assert_persisted_rows_survive_restart(first, start_with(duckdb_docs_dataset(&source, &db_path)))
        .await
}

#[tokio::test]
async fn cayenne_full_text_search_finds_persisted_rows_after_a_restart() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    configure_test_datafusion();
    register_test_connectors().await;

    let dir = tempfile::tempdir()?;
    let source = write_fixture(&dir).await?;
    let data_dir = dir.path().join("cayenne-data");
    let metadata_dir = dir.path().join("cayenne-metadata");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&metadata_dir)?;
    let data_dir = data_dir.to_string_lossy().to_string();
    let metadata_dir = metadata_dir.to_string_lossy().to_string();

    let first = start_with(cayenne_docs_dataset(&source, &data_dir, &metadata_dir)).await?;
    assert_persisted_rows_survive_restart(
        first,
        start_with(cayenne_docs_dataset(&source, &data_dir, &metadata_dir)),
    )
    .await
}
