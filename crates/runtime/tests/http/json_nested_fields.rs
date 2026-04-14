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

//! Integration test for HTTP datasets with nested JSON fields extracted via
//! `json_get_str` / `json_get_int` / `json_get_bool` in views, accelerated
//! with `DuckDB`. Validates:
//!
//! 1. Initial load of the view returns correct data.
//! 2. Hot-reload (adding new `json_get_*` columns to the view, including
//!    nested JSON extraction via `json_get_str(json_get_json(...), ...)`) picks
//!    up the schema change without a restart.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use axum::{Router, routing::get};
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::auth::EndpointAuth;
use runtime::config::Config;
use runtime::podswatcher::PodsWatcher;
use tokio::net::TcpListener;

use crate::utils::{
    register_test_connectors, runtime_ready_check, test_request_context, wait_until_true,
};
use crate::{configure_test_datafusion, init_tracing};

/// Simulates an API returning nested JSON audit log entries with mixed field
/// types (strings, booleans, integers, nulls) and nested JSON objects.
const AUDIT_LOGS_JSON: &str = r#"[
  {
    "id": "evt-001",
    "action": "user.login",
    "description": "Successful login from web client",
    "severity": "info",
    "status": "completed",
    "automated": false,
    "retentionDays": 30,
    "timestamp": 1700000000,
    "actor": "admin@example.com",
    "approvedBy": "security@example.com",
    "resourceId": "res-100",
    "policyId": "pol-200",
    "metadata": {"region": "us-east-1", "source": "web-client", "retryCount": 0}
  },
  {
    "id": "evt-002",
    "action": "config.update",
    "description": "Updated rate-limit settings",
    "severity": "warning",
    "status": "pending",
    "automated": true,
    "retentionDays": 7,
    "timestamp": 1710000000,
    "actor": "system",
    "approvedBy": null,
    "resourceId": "res-101",
    "policyId": "pol-201",
    "metadata": {"region": "eu-west-2", "source": "api-gateway", "retryCount": 3}
  },
  {
    "id": "evt-003",
    "action": "data.export",
    "description": null,
    "severity": "info",
    "status": "archived",
    "automated": false,
    "retentionDays": 90,
    "timestamp": 1720000000,
    "actor": "admin@example.com",
    "approvedBy": "admin@example.com",
    "resourceId": "res-102",
    "policyId": null,
    "metadata": null
  }
]"#;

async fn start_audit_log_server() -> Result<(tokio::sync::oneshot::Sender<()>, SocketAddr), String>
{
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let app = Router::new().route(
        "/audit-logs",
        get(|| async { ([("content-type", "application/json")], AUDIT_LOGS_JSON) }),
    );

    let tcp_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let addr = tcp_listener.local_addr().map_err(|e| e.to_string())?;

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

fn get_test_dir() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_default()
        .join("http_json_nested_test")
}

fn write_spicepod(content: &str) -> Result<(), String> {
    let dir = get_test_dir();
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("spicepod.yaml"), content).map_err(|e| e.to_string())?;
    Ok(())
}

/// Generates the initial spicepod YAML with a subset of `json_get_*` columns.
fn spicepod_yaml_initial(base_url: &str) -> String {
    format!(
        r"version: v1
kind: Spicepod
name: http_json_nested_test

datasets:
  - from: {base_url}
    name: audit_logs_raw
    params:
      file_format: json
      allowed_request_paths: /audit-logs
    acceleration:
      enabled: true
      engine: duckdb
      mode: memory
      refresh_mode: full
      refresh_sql: |
        SELECT request_path, content FROM audit_logs_raw WHERE request_path = '/audit-logs'

views:
  - name: audit_logs
    sql: |
      SELECT
        json_get_str(content, 'id') AS id,
        json_get_str(content, 'action') AS action,
        json_get_str(content, 'severity') AS severity,
        json_get_bool(content, 'automated') AS automated,
        json_get_int(content, 'retentionDays') AS retention_days
      FROM audit_logs_raw
      WHERE request_path = '/audit-logs'
      ORDER BY json_get_str(content, 'id')
"
    )
}

/// Generates the updated spicepod YAML with additional `json_get_*` columns
/// added to the view, including nested JSON extraction via
/// `json_get_str(json_get_json(...), ...)`, simulating a hot-reload scenario.
fn spicepod_yaml_updated(base_url: &str) -> String {
    format!(
        r"version: v1
kind: Spicepod
name: http_json_nested_test

datasets:
  - from: {base_url}
    name: audit_logs_raw
    params:
      file_format: json
      allowed_request_paths: /audit-logs
    acceleration:
      enabled: true
      engine: duckdb
      mode: memory
      refresh_mode: full
      refresh_sql: |
        SELECT request_path, content FROM audit_logs_raw WHERE request_path = '/audit-logs'

views:
  - name: audit_logs
    sql: |
      SELECT
        json_get_str(content, 'id') AS id,
        json_get_str(content, 'action') AS action,
        json_get_str(content, 'description') AS description,
        json_get_str(content, 'severity') AS severity,
        json_get_str(content, 'status') AS status,
        json_get_bool(content, 'automated') AS automated,
        json_get_int(content, 'retentionDays') AS retention_days,
        json_get_int(content, 'timestamp') AS timestamp,
        json_get_str(content, 'actor') AS actor,
        json_get_str(content, 'approvedBy') AS approved_by,
        json_get_str(content, 'resourceId') AS resource_id,
        json_get_str(content, 'policyId') AS policy_id,
        json_get_str(json_get(content, 'metadata'), 'region') AS metadata_region,
        json_get_str(json_get(content, 'metadata'), 'source') AS metadata_source,
        json_get_int(json_get(content, 'metadata'), 'retryCount') AS metadata_retry_count
      FROM audit_logs_raw
      WHERE request_path = '/audit-logs'
      ORDER BY json_get_str(content, 'id')
"
    )
}

async fn run_query(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("Query failed: {e}"))?;

    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to collect results: {e}"))
}

/// Validates that an HTTP dataset with `DuckDB` acceleration works with
/// `json_get_str`/`json_get_int`/`json_get_bool` in views, including
/// hot-reload of the view definition with additional nested JSON columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_json_nested_fields_duckdb_hot_reload() -> Result<(), String> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // Start the mock HTTP server
            let (shutdown_tx, addr) = start_audit_log_server().await?;
            let base_url = format!("http://{addr}");

            let spicepod_dir = get_test_dir();

            // Write initial spicepod with a subset of json_get columns
            write_spicepod(spicepod_yaml_initial(&base_url).as_str())?;

            let app = AppBuilder::build_from_path(spicepod_dir.clone())
                .await
                .map_err(|e| format!("Failed to build app: {e}"))?;

            let pods_watcher = PodsWatcher::new(spicepod_dir.clone());

            configure_test_datafusion();
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .with_pods_watcher(pods_watcher)
                    .build()
                    .await,
            );

            // Start runtime servers so the pods watcher can function
            let api_config = Config::new();
            let server_rt = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(server_rt.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            let load_rt = Arc::clone(&rt);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err("Timed out waiting for initial load".to_string());
                }
                () = load_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // ── Phase 1: Verify initial view loads correctly ──
            let initial_query =
                "SELECT id, action, severity, automated, retention_days FROM audit_logs ORDER BY id";
            let batches = run_query(&rt, initial_query).await?;
            let pretty = pretty_format_batches(&batches).map_err(|e| e.to_string())?;
            insta::assert_snapshot!("initial_view_results", pretty);

            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total_rows, 3, "Expected 3 rows in initial view");

            // ── Phase 2: Hot-reload with additional json_get columns ──
            write_spicepod(spicepod_yaml_updated(&base_url).as_str())?;

            // Wait for the new view columns to appear
            let query_rt = Arc::clone(&rt);
            let reload_success = wait_until_true(Duration::from_secs(30), || {
                let rt_ref = Arc::clone(&query_rt);
                async move {
                    let result = rt_ref
                        .datafusion()
                        .query_builder("SELECT resource_id FROM audit_logs LIMIT 1")
                        .build()
                        .run()
                        .await;
                    result.is_ok()
                }
            })
            .await;

            if !reload_success {
                return Err(
                    "Timed out waiting for hot-reload to pick up new view columns".to_string(),
                );
            }

            // Verify the full updated view with all columns, including nested JSON
            let updated_query = "\
                SELECT id, action, description, severity, status, automated, \
                       retention_days, timestamp, actor, approved_by, \
                       resource_id, policy_id, metadata_region, \
                       metadata_source, metadata_retry_count \
                FROM audit_logs ORDER BY id";
            let batches = run_query(&rt, updated_query).await?;
            let pretty = pretty_format_batches(&batches).map_err(|e| e.to_string())?;
            insta::assert_snapshot!("updated_view_results", pretty);

            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total_rows, 3, "Expected 3 rows in updated view");

            // Shut down
            rt.shutdown().await;
            drop(rt);
            let _ = shutdown_tx.send(());

            // Clean up
            if spicepod_dir.exists() {
                std::fs::remove_dir_all(&spicepod_dir).ok();
            }

            Ok(())
        })
        .await
}
