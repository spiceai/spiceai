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

//! Regression test for hot-reloading a view that uses `json_get` on an
//! accelerated HTTP connector dataset.
//!
//! The scenario being tested:
//! 1. An HTTP dataset is accelerated (`DuckDB` or Arrow).
//! 2. A view is created using `json_get` to extract columns from the JSON content.
//! 3. While the runtime is running, the spicepod is updated with a new view SQL
//!    that adds an extra column.
//! 4. The pods watcher detects the change and triggers `apply_view_diff`.
//! 5. After the view is re-initialized, both `DESCRIBE` and `SELECT` must
//!    reflect the new column.

use std::path::PathBuf;
use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Array, RecordBatch};
use axum::{Router, routing::get};
use futures::TryStreamExt;
use runtime::{Runtime, auth::EndpointAuth, config::Config, podswatcher::PodsWatcher};
use tokio::net::TcpListener;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context, wait_until_true},
};

const TEST_JSON: &str = r#"[
    {"id": 1, "name": "alpha", "score": 4.25, "active": true},
    {"id": 2, "name": "beta",  "score": 8.50, "active": false},
    {"id": 3, "name": "gamma", "score": 7.00, "active": true}
]"#;

async fn start_json_server()
-> Result<(tokio::sync::oneshot::Sender<()>, std::net::SocketAddr), String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let app = Router::new().route(
        "/data",
        get(|| async { ([("content-type", "application/json")], TEST_JSON) }),
    );

    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
        tracing::error!("Failed to bind test server: {e}");
        e.to_string()
    })?;
    let addr = tcp_listener.local_addr().map_err(|e| {
        tracing::error!("Failed to read local address: {e}");
        e.to_string()
    })?;

    tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });

    Ok((tx, addr))
}

fn spicepod_yaml_v1(http_addr: &std::net::SocketAddr, engine: &str) -> String {
    let engine_line = if engine.is_empty() {
        String::new()
    } else {
        format!("\n      engine: {engine}")
    };
    format!(
        r#"version: v1
kind: Spicepod
name: http_view_hot_reload

datasets:
  - from: "http://{http_addr}"
    name: http_json_ds
    params:
      file_format: json
      allowed_request_paths: /data
    acceleration:
      enabled: true{engine_line}
      refresh_mode: full
      refresh_sql: "SELECT request_path, content FROM http_json_ds WHERE request_path = '/data'"

views:
  - name: json_view
    sql: |
      SELECT
        CAST(json_get(content, 'id') AS BIGINT) AS id,
        CAST(json_get(content, 'name') AS VARCHAR) AS name
      FROM http_json_ds
      WHERE request_path = '/data'
      ORDER BY id
"#
    )
}

fn spicepod_yaml_v2(http_addr: &std::net::SocketAddr, engine: &str) -> String {
    let engine_line = if engine.is_empty() {
        String::new()
    } else {
        format!("\n      engine: {engine}")
    };
    format!(
        r#"version: v1
kind: Spicepod
name: http_view_hot_reload

datasets:
  - from: "http://{http_addr}"
    name: http_json_ds
    params:
      file_format: json
      allowed_request_paths: /data
    acceleration:
      enabled: true{engine_line}
      refresh_mode: full
      refresh_sql: "SELECT request_path, content FROM http_json_ds WHERE request_path = '/data'"

views:
  - name: json_view
    sql: |
      SELECT
        CAST(json_get(content, 'id') AS BIGINT) AS id,
        CAST(json_get(content, 'name') AS VARCHAR) AS name,
        CAST(json_get(content, 'score') AS DOUBLE) AS score
      FROM http_json_ds
      WHERE request_path = '/data'
      ORDER BY id
"#
    )
}

fn get_test_dir(suffix: &str) -> PathBuf {
    std::env::current_dir()
        .unwrap_or_default()
        .join(format!("http_view_hot_reload_test_{suffix}"))
}

fn write_spicepod(test_dir: &PathBuf, content: &str) {
    std::fs::create_dir_all(test_dir).expect("to create test dir");
    std::fs::write(test_dir.join("spicepod.yaml"), content).expect("to write spicepod.yaml");
}

fn column_names_from_batches(batches: &[RecordBatch], col_name: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name(col_name)
            .unwrap_or_else(|| panic!("expected column '{col_name}'"));
        let str_array = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap_or_else(|| panic!("expected '{col_name}' to be StringArray"));
        for i in 0..str_array.len() {
            values.push(str_array.value(i).to_string());
        }
    }
    values
}

async fn query_batches(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| format!("collect failed: {e}"))
}

/// Core logic for the hot-reload regression test, parameterized by acceleration engine.
///
/// `engine` should be `"duckdb"`, `"arrow"`, `""` (default Arrow), etc.
async fn run_view_hot_reload_test(engine: &str) -> Result<(), String> {
    let test_dir = get_test_dir(if engine.is_empty() { "arrow" } else { engine });
    // Clean up any leftovers from a previous run.
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).ok();
    }

    let (tx, addr) = start_json_server().await?;

    // Write the initial spicepod with view v1 (id, name).
    write_spicepod(&test_dir, &spicepod_yaml_v1(&addr, engine));

    let app = AppBuilder::build_from_path(test_dir.clone())
        .await
        .map_err(|e| format!("Failed to build app: {e}"))?;
    let pods_watcher = PodsWatcher::new(test_dir.clone());

    configure_test_datafusion();
    let rt = Arc::new(
        Runtime::builder()
            .with_app(app)
            .with_pods_watcher(pods_watcher)
            .build()
            .await,
    );

    // Start the server so the pods watcher runs.
    let api_config = Config::new();
    let server_rt = Arc::clone(&rt);
    tokio::spawn(async move {
        let _ = server_rt
            .start_servers(api_config, None, EndpointAuth::no_auth())
            .await;
    });

    let cloned_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err("Timed out waiting for components to load".to_string());
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    // ------------------------------------------------------------------
    // Phase 1: verify the initial view (id, name)
    // ------------------------------------------------------------------
    let describe_v1 = query_batches(&rt, "DESCRIBE json_view").await?;
    let cols_v1 = column_names_from_batches(&describe_v1, "column_name");
    assert_eq!(
        cols_v1,
        vec!["id", "name"],
        "initial DESCRIBE should show [id, name]"
    );

    let select_v1 = query_batches(&rt, "SELECT * FROM json_view").await?;
    let total_rows_v1: usize = select_v1.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows_v1, 3, "initial SELECT should return 3 rows");
    assert_eq!(
        select_v1.first().expect("at least one batch").num_columns(),
        2,
        "initial SELECT should have 2 columns"
    );

    // ------------------------------------------------------------------
    // Phase 2: hot-reload — update the spicepod with view v2 (id, name,
    //          score) and wait for the pods watcher to pick it up.
    // ------------------------------------------------------------------
    write_spicepod(&test_dir, &spicepod_yaml_v2(&addr, engine));

    // Wait until information_schema shows the new `score` column.
    let rt_ref = Arc::clone(&rt);
    let schema_updated = wait_until_true(std::time::Duration::from_secs(30), || {
        let rt_inner = Arc::clone(&rt_ref);
        async move {
            let Ok(batches) = query_batches(
                &rt_inner,
                "SELECT column_name FROM information_schema.columns \
                     WHERE table_name = 'json_view' ORDER BY ordinal_position",
            )
            .await
            else {
                return false;
            };
            let cols = column_names_from_batches(&batches, "column_name");
            cols == vec!["id", "name", "score"]
        }
    })
    .await;
    assert!(
        schema_updated,
        "information_schema should show [id, name, score] after hot-reload"
    );

    // ------------------------------------------------------------------
    // Phase 3: verify DESCRIBE reflects the new column
    // ------------------------------------------------------------------
    let describe_v2 = query_batches(&rt, "DESCRIBE json_view").await?;
    let cols_v2 = column_names_from_batches(&describe_v2, "column_name");
    assert_eq!(
        cols_v2,
        vec!["id", "name", "score"],
        "DESCRIBE after hot-reload should show [id, name, score]"
    );

    // ------------------------------------------------------------------
    // Phase 4: verify SELECT returns the new column with data
    // ------------------------------------------------------------------
    let select_v2 = query_batches(&rt, "SELECT * FROM json_view").await?;
    let total_rows_v2: usize = select_v2.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows_v2, 3,
        "SELECT after hot-reload should return 3 rows"
    );

    let batch = select_v2.first().expect("at least one batch");
    assert_eq!(
        batch.num_columns(),
        3,
        "SELECT after hot-reload should have 3 columns (id, name, score)"
    );

    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "score");

    // ------------------------------------------------------------------
    // Cleanup
    // ------------------------------------------------------------------
    rt.shutdown().await;
    tx.send(())
        .map_err(|()| "Failed to send shutdown signal".to_string())?;
    std::fs::remove_dir_all(&test_dir).ok();

    Ok(())
}

/// Regression test: hot-reloading a view with `json_get` on a DuckDB-accelerated
/// HTTP dataset must update both `DESCRIBE` and `SELECT` results.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_json_view_hot_reload_duckdb() -> Result<(), String> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async { run_view_hot_reload_test("duckdb").await })
        .await
}

/// Same regression test with the default Arrow accelerator to determine
/// whether the bug is DuckDB-specific or affects all acceleration engines.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_json_view_hot_reload_arrow() -> Result<(), String> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async { run_view_hot_reload_test("arrow").await })
        .await
}
