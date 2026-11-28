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

//! `text_search` queries that use `AND NOT` must return the same rows as the equivalent
//! `-` form. Covers the documented `title:(spice OR runtime) AND NOT draft` syntax.

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

fn docs_dataset(source: &str) -> Dataset {
    let mut dataset = Dataset::new(source, "docs");
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.columns = vec![
        Column::new("title")
            .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
        Column::new("comment")
            .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
    ];
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

async fn start_with(dataset: Dataset) -> anyhow::Result<Arc<Runtime>> {
    let app = AppBuilder::new("fts_and_not").with_dataset(dataset).build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let load_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(LOAD_TIMEOUT) => anyhow::bail!("timed out loading components"),
        () = load_rt.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

async fn search_ids(rt: &Arc<Runtime>, column: &str, query: &str) -> anyhow::Result<Vec<i64>> {
    let sql = format!("SELECT id FROM text_search(docs, '{query}', {column}) ORDER BY id");
    let batches = run_query(rt, &sql).await?;
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

fn write_fixture(dir: &tempfile::TempDir) -> anyhow::Result<String> {
    let csv = dir.path().join("docs.csv");
    std::fs::write(
        &csv,
        "id,title,comment\n\
1,spice runtime,dolphins play\n\
2,spice draft,dolphins furiously\n\
3,runtime notes,pearls and dolphins\n\
4,draft only,pearls furiously\n\
5,other,only pearls\n\
6,spice,furiously alone\n",
    )?;
    Ok(format!("file://{}", csv.display()))
}

#[tokio::test]
async fn text_search_and_not_matches_the_minus_form() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    configure_test_datafusion();
    register_test_connectors().await;

    let dir = tempfile::tempdir()?;
    let source = write_fixture(&dir)?;
    let rt = start_with(docs_dataset(&source)).await?;

    let and_not = search_ids(&rt, "comment", "dolphins AND NOT furiously").await?;
    let minus = search_ids(&rt, "comment", "dolphins -furiously").await?;
    assert_eq!(and_not, minus, "AND NOT must match the '-' form");
    assert_eq!(and_not, vec![1, 3]);

    assert_eq!(
        search_ids(&rt, "comment", "dolphins AND (NOT furiously)").await?,
        and_not
    );
    assert_eq!(
        search_ids(
            &rt,
            "comment",
            "comment:(dolphins OR pearls) AND NOT furiously"
        )
        .await?,
        search_ids(&rt, "comment", "comment:(dolphins OR pearls) -furiously").await?,
    );
    assert_eq!(
        search_ids(
            &rt,
            "comment",
            "comment:(dolphins OR pearls) AND NOT furiously"
        )
        .await?,
        vec![1, 3, 5]
    );

    let documented = search_ids(&rt, "title", "title:(spice OR runtime) AND NOT draft").await?;
    assert_eq!(
        documented,
        search_ids(&rt, "title", "title:(spice OR runtime) -draft").await?,
        "the documented title:(a OR b) AND NOT c form must match the '-' form"
    );
    assert_eq!(documented, vec![1, 3, 6]);

    rt.shutdown().await;
    Ok(())
}
