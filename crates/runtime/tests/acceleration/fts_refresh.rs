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

//! After a full acceleration refresh, `text_search` must search the index as it is now,
//! not as it was when that SQL was first planned.
//!
//! Regression test for #15392 / #15424: `FullTextSearchFieldIndex` used to hold a
//! `tantivy::Searcher` taken at planning, so a repeated query (and, on 2.4, a reworded
//! query with the same search term) kept returning deleted rows and missing new ones.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use app::AppBuilder;
use arrow::array::{Array, Int64Array};
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use spicepod::semantic::{Column, FullTextSearchConfig, IndexStore};

use crate::acceleration::refresh_and_wait;
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

const LOAD_TIMEOUT: Duration = Duration::from_mins(2);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(60);

const BEFORE_CSV: &str = "id,text\n1,apple\n2,banana\n3,cherry\n4,durian\n";
const AFTER_CSV: &str = "id,text\n1,apple\n3,walrus\n4,durian\n5,zephyrine\n";

fn docs_columns(index_store: IndexStore, index_directory: Option<String>) -> Vec<Column> {
    let mut cfg = FullTextSearchConfig::enabled().with_row_id("id");
    cfg.index_store = Some(index_store);
    cfg.index_directory = index_directory;
    vec![Column::new("text").with_full_text_search(cfg)]
}

fn cayenne_docs_dataset(
    source: &str,
    data_dir: &str,
    metadata_dir: &str,
    index_store: IndexStore,
    index_directory: Option<String>,
) -> Dataset {
    let mut dataset = Dataset::new(source, "docs");
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "csv".to_string(),
    )])));
    dataset.columns = docs_columns(index_store, index_directory);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        primary_key: Some("id".to_string()),
        params: Some(Params::from_string_map(HashMap::from([
            ("cayenne_file_path".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]))),
        ..Acceleration::default()
    });
    dataset
}

async fn start_with(dataset: Dataset) -> anyhow::Result<Arc<Runtime>> {
    let app = AppBuilder::new("fts_refresh")
        .with_dataset(dataset)
        // Isolate the searcher-at-execution bug from the SQL results cache, which a
        // refresh does not always invalidate.
        .with_sql_cache(SQLResultsCacheConfig {
            enabled: false,
            ..Default::default()
        })
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

async fn search_ids(rt: &Arc<Runtime>, sql: &str) -> anyhow::Result<Vec<i64>> {
    let batches = run_query(rt, sql).await?;
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
    ids.sort_unstable();
    Ok(ids)
}

async fn assert_search_tracks_a_full_refresh(index_store: IndexStore) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("docs.csv");
    std::fs::write(&csv, BEFORE_CSV)?;
    let source = format!("file://{}", csv.display());

    let data_dir = dir.path().join("cayenne-data");
    let metadata_dir = dir.path().join("cayenne-metadata");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&metadata_dir)?;
    let data_dir = data_dir.to_string_lossy().to_string();
    let metadata_dir = metadata_dir.to_string_lossy().to_string();

    let index_directory = if index_store == IndexStore::File {
        let fts_dir = dir.path().join("fts");
        std::fs::create_dir_all(&fts_dir)?;
        Some(fts_dir.to_string_lossy().to_string())
    } else {
        None
    };

    let rt = start_with(cayenne_docs_dataset(
        &source,
        &data_dir,
        &metadata_dir,
        index_store,
        index_directory,
    ))
    .await?;

    let same_sql = "SELECT id FROM text_search(docs, 'banana', text) ORDER BY id";
    let reworded_sql = "SELECT id FROM text_search(docs, 'banana', text) WHERE true ORDER BY id";

    assert_eq!(
        search_ids(&rt, same_sql).await?,
        vec![2],
        "before refresh, banana is id 2"
    );
    assert_eq!(
        search_ids(
            &rt,
            "SELECT id FROM text_search(docs, 'cherry', text) ORDER BY id"
        )
        .await?,
        vec![3],
        "before refresh, cherry is id 3"
    );

    std::fs::write(&csv, AFTER_CSV)?;
    refresh_and_wait(&rt, "docs", REFRESH_TIMEOUT).await?;

    assert_eq!(
        search_ids(&rt, same_sql).await?,
        Vec::<i64>::new(),
        "the same SQL text must not return the deleted banana row after refresh"
    );
    assert_eq!(
        search_ids(&rt, reworded_sql).await?,
        Vec::<i64>::new(),
        "a reworded query with the same search term must not return the deleted banana row after refresh"
    );
    assert_eq!(
        search_ids(
            &rt,
            "SELECT id FROM text_search(docs, 'walrus', text) ORDER BY id"
        )
        .await?,
        vec![3],
        "after refresh, walrus is the updated id 3"
    );
    assert_eq!(
        search_ids(
            &rt,
            "SELECT id FROM text_search(docs, 'zephyrine', text) ORDER BY id"
        )
        .await?,
        vec![5],
        "after refresh, zephyrine is the inserted id 5"
    );

    rt.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cayenne_text_search_tracks_full_refresh_with_memory_index() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    configure_test_datafusion();
    register_test_connectors().await;

    test_request_context()
        .scope(assert_search_tracks_a_full_refresh(IndexStore::Memory))
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn cayenne_text_search_tracks_full_refresh_with_file_index() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    configure_test_datafusion();
    register_test_connectors().await;

    test_request_context()
        .scope(assert_search_tracks_a_full_refresh(IndexStore::File))
        .await
}
