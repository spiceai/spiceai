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

fn docs_dataset(source: &str, db_path: &str) -> Dataset {
    let mut dataset = Dataset::new(source, "docs");
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "csv".to_string(),
    )])));
    dataset.columns = vec![
        Column::new("text")
            .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
    ];
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

async fn start(source: &str, db_path: &str) -> anyhow::Result<Arc<Runtime>> {
    let app = AppBuilder::new("fts_restart")
        .with_dataset(docs_dataset(source, db_path))
        .build();
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

#[tokio::test]
async fn full_text_search_finds_persisted_rows_after_a_restart() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    configure_test_datafusion();
    register_test_connectors().await;

    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("docs.csv");
    std::fs::write(&csv, "id,text\n1,alpha\n2,beta\n3,gamma\n")?;
    let source = format!("file://{}", csv.display());
    let db_path = dir.path().join("docs.db").to_string_lossy().to_string();

    let first = start(&source, &db_path).await?;
    assert_eq!(
        search_ids(&first, "alpha").await?,
        vec![1],
        "the first load indexes the rows it writes"
    );
    first.shutdown().await;
    drop(first);

    // The acceleration file keeps its rows and checkpoint, so this start skips the refresh.
    let restarted = start(&source, &db_path).await?;
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
