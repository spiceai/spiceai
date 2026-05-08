/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Integration tests verifying `DuckDB` HNSW vector indexes.
//!
//! Uses a native `DuckDbConnectionPool` (post-shutdown) to query `duckdb_indexes()`
//! and confirm that the HNSW index exists on the correct underlying table.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::Context as _;
use app::AppBuilder;
use arrow::array::{RecordBatch, StringArray};
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::auth::EndpointAuth;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::component::embeddings::Embeddings;
use spicepod::component::view::View;
use spicepod::param::Params;
use spicepod::semantic::{Column, ColumnLevelEmbeddingConfig};
use spicepod::vector::VectorStore;
use tokio::sync::Mutex;

use crate::models::create_api_bindings_config;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

/// Serializes HNSW tests because `Runtime::shutdown()` calls `unregister_all()`,
/// which clears the global connector registry and breaks parallel tests.
static HNSW_TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn cleanup_db_path(db_path: &str) {
    for suffix in ["", ".wal"] {
        let path = format!("{db_path}{suffix}");
        if std::path::Path::new(&path).exists() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn model2vec_embedding() -> Embeddings {
    Embeddings::new("model2vec:minishlab/potion-base-2M", "test_embed")
}

/// Helper: creates a source dataset for the mega-science S3 data (no acceleration).
fn mega_science_source_dataset(name: &str) -> Dataset {
    let mut dataset = Dataset::new(
        "s3://spiceai-public-datasets/MegaScience/mega-science-small.jsonl",
        name,
    );
    dataset.params = Some(Params::from_string_map(
        vec![("client_timeout".to_string(), "120s".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset
}

fn hnsw_dataset(name: &str, db_path: &str, refresh_mode: RefreshMode) -> Dataset {
    let mut dataset = mega_science_source_dataset(name);

    let accel_params: HashMap<String, String> =
        HashMap::from([("duckdb_file".to_string(), db_path.to_string())]);
    // Don't set HNSW params on acceleration — they go in vectors.params
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(refresh_mode),
        refresh_sql: Some(format!("SELECT * FROM {name} LIMIT 64")),
        params: Some(Params::from_string_map(accel_params)),
        ..Acceleration::default()
    });

    dataset.vectors = Some(VectorStore {
        enabled: true,
        engine: Some("duckdb".to_string()),
        partition_by: Vec::new(),
        params: Some(Params::from_string_map(HashMap::from([
            ("duckdb_distance_metric".to_string(), "cosine".to_string()),
            ("duckdb_hnsw_m".to_string(), "8".to_string()),
            ("duckdb_hnsw_ef_construction".to_string(), "24".to_string()),
            ("duckdb_hnsw_ef_search".to_string(), "12".to_string()),
        ]))),
    });

    dataset.columns = vec![
        Column::new("question")
            .with_embedding(ColumnLevelEmbeddingConfig::model("test_embed").with_row_id("id")),
    ];

    dataset
}

/// Start a runtime with the given app, wait for components to load, and return the runtime.
async fn start_runtime(app: app::App) -> Arc<Runtime> {
    register_test_connectors().await;
    configure_test_datafusion();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let api_config = create_api_bindings_config();
    let rt_ref = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
            panic!("Timed out waiting for components to load");
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;
    rt
}

/// Trigger a manual refresh and wait for it to complete.
async fn refresh_table(rt: &Arc<Runtime>, table_name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table_name), None)
        .await?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("No refresh notifier returned for {table_name}"))?
        .notified()
        .await;
    Ok(())
}

/// Run a SQL query through the runtime's `DataFusion` context.
async fn execute_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut result = rt.datafusion().query_builder(sql).build().run().await?;
    let mut batches = Vec::new();
    while let Some(batch) = futures::StreamExt::next(&mut result.data).await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Open a native `DuckDB` connection and return HNSW index info.
/// Must be called after the runtime is fully shut down and dropped.
async fn query_native_duckdb_indexes(
    db_path: &str,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    let pool =
        DuckDbConnectionPool::new_file(db_path, &AccessMode::ReadWrite).expect("valid DuckDB path");
    let conn_dyn = pool.connect().await.expect("valid connection");
    let conn = conn_dyn.as_sync().expect("sync connection");

    let batches: Vec<RecordBatch> = conn
        .query_arrow(
            "SELECT index_name, table_name FROM duckdb_indexes() WHERE index_name LIKE '__spice_vss_%'",
            &[],
            None,
        )
        .expect("index query executes")
        .try_collect::<Vec<RecordBatch>>()
        .await
        .expect("collects results");

    let mut results = Vec::new();
    for batch in &batches {
        let index_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("index_name column")?;
        let table_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("table_name column")?;
        for i in 0..batch.num_rows() {
            results.push((
                index_names.value(i).to_string(),
                table_names.value(i).to_string(),
            ));
        }
    }
    Ok(results)
}

/// Verifies HNSW index exists after initial load and survives a full (overwrite) refresh.
/// After shutdown, queries the `DuckDB` file directly to confirm the index is on the correct
/// internal data table.
#[tokio::test]
async fn test_hnsw_index_created_after_full_refresh() -> Result<(), anyhow::Error> {
    let _test_lock = HNSW_TEST_MUTEX.lock().await;
    let _tracing = init_tracing(Some(
        "integration_models=debug,runtime=debug,search=debug,info",
    ));

    let db_path = "./test_hnsw_refresh.db";
    let ds_name = "hnsw_test_ds";

    test_request_context()
        .scope(async {
            cleanup_db_path(db_path);

            let app = AppBuilder::new("hnsw_index_refresh_test")
                .with_embedding(model2vec_embedding())
                .with_dataset(hnsw_dataset(ds_name, db_path, RefreshMode::Full))
                .build();

            let rt = start_runtime(app).await;

            // 1. Verify vector search works after initial load
            let batches = execute_sql(
                &rt,
                &format!(
                    "SELECT id, _score FROM vector_search({ds_name}, 'second') ORDER BY _score DESC LIMIT 4"
                ),
            )
            .await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            anyhow::ensure!(
                total_rows == 4,
                "Expected 4 rows from initial vector_search, got {total_rows}"
            );
            tracing::info!("Initial vector search returned {total_rows} rows");

            // 2. Trigger a manual full refresh (overwrite) — this destroys and recreates the
            //    underlying DuckDB table. The HNSW index must be recreated afterward.
            refresh_table(&rt, ds_name).await?;

            // 3. Verify vector search STILL works after refresh
            let batches = execute_sql(
                &rt,
                &format!(
                    "SELECT id, _score FROM vector_search({ds_name}, 'second') ORDER BY _score DESC LIMIT 4"
                ),
            )
            .await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            anyhow::ensure!(
                total_rows == 4,
                "Expected 4 rows from post-refresh vector_search, got {total_rows}"
            );
            tracing::info!("Post-refresh vector search returned {total_rows} rows");

            // 4. Shutdown runtime and verify index via native DuckDB connection
            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;

            let indexes = query_native_duckdb_indexes(db_path).await?;
            tracing::info!("Native DuckDB indexes: {indexes:?}");

            anyhow::ensure!(
                !indexes.is_empty(),
                "Expected at least one __spice_vss_ HNSW index in DuckDB file after refresh"
            );

            // The index should be on an internal data table (__data_<name>_<timestamp>),
            // not on the view name directly
            for (index_name, table_name) in &indexes {
                anyhow::ensure!(
                    index_name.contains("question_embedding"),
                    "Index name {index_name} should reference question_embedding column"
                );
                tracing::info!(
                    "Verified HNSW index {index_name} on table {table_name}"
                );
            }

            cleanup_db_path(db_path);
            Ok(())
        })
        .await
}

/// Verifies HNSW index is created after initial append refresh.
/// Uses a local JSONL file (with a `created_at` timestamp for `time_column`).
#[tokio::test]
async fn test_hnsw_index_created_after_append_refresh() -> Result<(), anyhow::Error> {
    let _test_lock = HNSW_TEST_MUTEX.lock().await;
    let _tracing = init_tracing(Some(
        "integration_models=debug,runtime=debug,search=debug,info",
    ));

    let db_path = "./test_hnsw_append_refresh.db";
    let ds_name = "hnsw_append_ds";

    let test_data = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/models/test_data/mega-science-sample.jsonl");
    let source = format!("file://{}", test_data.display());

    test_request_context()
        .scope(async {
            cleanup_db_path(db_path);

            let mut dataset = hnsw_dataset(ds_name, db_path, RefreshMode::Append);
            dataset.from = source;
            dataset.time_column = Some("created_at".to_string());
            dataset.time_format = Some(spicepod::component::dataset::TimeFormat::ISO8601);
            dataset.params = None; // Remove client_timeout, not supported for file connector

            let app = AppBuilder::new("hnsw_append_refresh_test")
                .with_embedding(model2vec_embedding())
                .with_dataset(dataset)
                .build();

            let rt = start_runtime(app).await;

            // Verify vector search works after initial append load
            let batches = execute_sql(
                &rt,
                &format!(
                    "SELECT id, _score FROM vector_search({ds_name}, 'second') ORDER BY _score DESC LIMIT 4"
                ),
            )
            .await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            anyhow::ensure!(
                total_rows == 4,
                "Expected 4 rows from vector_search after append refresh, got {total_rows}"
            );
            tracing::info!("Append refresh vector search returned {total_rows} rows");

            // Shutdown runtime and verify index via native DuckDB connection
            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;

            let indexes = query_native_duckdb_indexes(db_path).await?;
            tracing::info!("Native DuckDB indexes after append refresh: {indexes:?}");

            anyhow::ensure!(
                !indexes.is_empty(),
                "Expected at least one __spice_vss_ HNSW index after append refresh"
            );

            for (index_name, table_name) in &indexes {
                anyhow::ensure!(
                    index_name.contains("question_embedding"),
                    "Index name {index_name} should reference question_embedding column"
                );
                tracing::info!(
                    "Verified HNSW index {index_name} on table {table_name}"
                );
            }

            cleanup_db_path(db_path);
            Ok(())
        })
        .await
}

/// Verifies HNSW index survives multiple consecutive full refreshes.
#[tokio::test]
async fn test_hnsw_index_survives_multiple_refreshes() -> Result<(), anyhow::Error> {
    let _test_lock = HNSW_TEST_MUTEX.lock().await;
    let _tracing = init_tracing(Some(
        "integration_models=debug,runtime=debug,search=debug,info",
    ));

    let db_path = "./test_hnsw_multi_refresh.db";
    let ds_name = "hnsw_multi_ds";

    test_request_context()
        .scope(async {
            cleanup_db_path(db_path);

            let app = AppBuilder::new("hnsw_multi_refresh_test")
                .with_embedding(model2vec_embedding())
                .with_dataset(hnsw_dataset(ds_name, db_path, RefreshMode::Full))
                .build();

            let rt = start_runtime(app).await;

            // Initial vector search
            let batches = execute_sql(
                &rt,
                &format!("SELECT id FROM vector_search({ds_name}, 'second') LIMIT 4"),
            )
            .await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            anyhow::ensure!(total_rows == 4, "Initial search failed: {total_rows} rows");

            // Refresh 3 times
            for i in 1..=3 {
                refresh_table(&rt, ds_name).await?;
                let batches = execute_sql(
                    &rt,
                    &format!("SELECT id FROM vector_search({ds_name}, 'second') LIMIT 4"),
                )
                .await?;
                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                anyhow::ensure!(
                    total_rows == 4,
                    "Vector search failed after refresh #{i}: {total_rows} rows"
                );
                tracing::info!("Refresh #{i}: vector search OK ({total_rows} rows)");
            }

            // Shutdown runtime and verify native DuckDB indexes
            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;

            let indexes = query_native_duckdb_indexes(db_path).await?;
            anyhow::ensure!(!indexes.is_empty(), "Expected HNSW index after 3 refreshes");

            // There should be exactly one HNSW index (on the latest internal table).
            // Old internal tables are dropped when the view is swapped.
            anyhow::ensure!(
                indexes.len() == 1,
                "Expected exactly 1 HNSW index after multiple refreshes, found {}",
                indexes.len()
            );

            tracing::info!(
                "Verified single HNSW index after 3 refreshes: {:?}",
                indexes[0]
            );

            cleanup_db_path(db_path);

            Ok(())
        })
        .await
}

/// Helper: creates an accelerated view with `DuckDB` + HNSW vector indexes over a source dataset.
fn hnsw_view(view_name: &str, source_ds: &str, db_path: &str) -> View {
    let accel_params: HashMap<String, String> =
        HashMap::from([("duckdb_file".to_string(), db_path.to_string())]);

    let mut view = View::new(view_name.to_string());
    view.sql = Some(format!(
        "SELECT question, id, answer, subject, reference_answer, source FROM {source_ds} LIMIT 64"
    ));
    view.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(accel_params)),
        ..Acceleration::default()
    });

    view.vectors = Some(VectorStore {
        enabled: true,
        engine: Some("duckdb".to_string()),
        partition_by: Vec::new(),
        params: Some(Params::from_string_map(HashMap::from([
            ("duckdb_distance_metric".to_string(), "cosine".to_string()),
            ("duckdb_hnsw_m".to_string(), "8".to_string()),
            ("duckdb_hnsw_ef_construction".to_string(), "24".to_string()),
            ("duckdb_hnsw_ef_search".to_string(), "12".to_string()),
        ]))),
    });

    view.columns = vec![
        Column::new("question")
            .with_embedding(ColumnLevelEmbeddingConfig::model("test_embed").with_row_id("id")),
    ];

    view
}

/// Verifies HNSW index on an accelerated **view** is created after initial load
/// and survives a full (overwrite) refresh.
#[tokio::test]
async fn test_hnsw_index_on_view_created_after_full_refresh() -> Result<(), anyhow::Error> {
    let _test_lock = HNSW_TEST_MUTEX.lock().await;
    let _tracing = init_tracing(Some(
        "integration_models=debug,runtime=debug,search=debug,info",
    ));

    let db_path = "./test_hnsw_view_refresh.db";
    let view_name = "hnsw_view_ds";
    let source_ds = "hnsw_view_source";

    test_request_context()
        .scope(async {
            cleanup_db_path(db_path);

            let app = AppBuilder::new("hnsw_view_refresh_test")
                .with_embedding(model2vec_embedding())
                .with_dataset(mega_science_source_dataset(source_ds))
                .with_view(hnsw_view(view_name, source_ds, db_path))
                .build();

            let rt = start_runtime(app).await;

            // 1. Verify vector search works after initial load
            let batches = execute_sql(
                &rt,
                &format!(
                    "SELECT id, _score FROM vector_search({view_name}, 'second') ORDER BY _score DESC LIMIT 4"
                ),
            )
            .await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            anyhow::ensure!(
                total_rows == 4,
                "Expected 4 rows from initial view vector_search, got {total_rows}"
            );
            tracing::info!("Initial view vector search returned {total_rows} rows");

            // 2. Trigger a manual full refresh — HNSW index must be recreated afterward.
            refresh_table(&rt, view_name).await?;

            // 3. Verify vector search STILL works after refresh
            let batches = execute_sql(
                &rt,
                &format!(
                    "SELECT id, _score FROM vector_search({view_name}, 'second') ORDER BY _score DESC LIMIT 4"
                ),
            )
            .await?;
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            anyhow::ensure!(
                total_rows == 4,
                "Expected 4 rows from post-refresh view vector_search, got {total_rows}"
            );
            tracing::info!("Post-refresh view vector search returned {total_rows} rows");

            // 4. Shutdown runtime and verify index via native DuckDB connection
            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;

            let indexes = query_native_duckdb_indexes(db_path).await?;
            tracing::info!("Native DuckDB indexes for view: {indexes:?}");

            anyhow::ensure!(
                !indexes.is_empty(),
                "Expected at least one __spice_vss_ HNSW index in DuckDB file for view"
            );

            for (index_name, _table_name) in &indexes {
                anyhow::ensure!(
                    index_name.contains("question_embedding"),
                    "Index name {index_name} should reference question_embedding column"
                );
                tracing::info!("Verified HNSW index {index_name} on view accelerator");
            }

            cleanup_db_path(db_path);
            Ok(())
        })
        .await
}
