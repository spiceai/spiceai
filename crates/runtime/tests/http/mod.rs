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

#[cfg(feature = "duckdb")]
mod json_nested_fields;
#[cfg(feature = "duckdb")]
mod view_hot_reload;

mod deferred;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use app::{App, AppBuilder};
use arrow::{array::RecordBatch, datatypes::DataType};
use axum::{
    Form, Router,
    extract::State,
    http::{HeaderMap as AxumHeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use cache::result::CacheStatus;
use futures::TryStreamExt;
use reqwest::{Client, header::HeaderMap};
use runtime::{
    Runtime, auth::EndpointAuth, config::Config, datafusion::query::write_to_json_value,
};
use serde_json::{Value, json};
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::{caching::SQLResultsCacheConfig, dataset::Dataset, view::View},
    param::Params as DatasetParams,
};
use tokio::net::TcpListener;

use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results};

type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;

const SHOWS_JSON: &str = r#"[
    {"id": 1, "name": "Breaking Bad", "rating": 9.5},
    {"id": 2, "name": "The Wire", "rating": 9.3},
    {"id": 3, "name": "Better Call Saul", "rating": 8.9}
]"#;

const ITEMS_CSV: &str = "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n3,Doohickey,4.99\n";

const HTTP_JSON_EDGE_CASES: &str = r#"[
  {
    "id": 1,
    "name": "alpha",
    "active": true,
    "score": 4.25,
    "lastModifiedBy": null,
    "sandbox": {
      "enabled": true,
      "limits": {
        "cpu": 2,
        "memory": "4Gi"
      }
    },
    "tags": ["edge", "null-string"],
    "metrics": {
      "views": 10,
      "ratio": 0.5
    }
  },
  {
    "id": 2,
    "name": "beta",
    "active": false,
    "score": null,
    "lastModifiedBy": "alice",
    "sandbox": {
      "enabled": false,
      "limits": null
    },
    "tags": [],
    "metrics": {
      "views": 0,
      "ratio": 1.25
    }
  },
  {
    "id": 3,
    "name": "gamma",
    "active": true,
    "score": 7.0,
    "sandbox": null,
    "tags": ["nested", "array"],
    "metrics": {
      "views": null,
      "ratio": null
    }
  }
]"#;

const HTTP_JSON_EDGE_QUERY: &str = r"
SELECT
    CAST(json_get(content, 'id') AS BIGINT) AS id,
    CAST(json_get(content, 'name') AS VARCHAR) AS name,
    CAST(json_get(content, 'active') AS BOOLEAN) AS active,
    CAST(json_get(content, 'score') AS DOUBLE) AS score,
    CAST(json_get(content, 'lastModifiedBy') AS VARCHAR) AS last_modified_by,
  json_get(content, 'sandbox') AS sandbox,
  json_get(content, 'tags') AS tags,
    CAST(json_get(content, 'metrics', 'views') AS BIGINT) AS views,
    CAST(json_get(content, 'metrics', 'ratio') AS DOUBLE) AS ratio
FROM http_json_edges
WHERE request_path = '/edge'
ORDER BY id
";

fn expected_http_json_edge_rows() -> Value {
    json!([
        {
            "id": 1,
            "name": "alpha",
            "active": true,
            "score": 4.25,
            "last_modified_by": null,
            "sandbox": {
                "enabled": true,
                "limits": {
                    "cpu": 2,
                    "memory": "4Gi"
                }
            },
            "tags": ["edge", "null-string"],
            "views": 10,
            "ratio": 0.5
        },
        {
            "id": 2,
            "name": "beta",
            "active": false,
            "score": null,
            "last_modified_by": "alice",
            "sandbox": {
                "enabled": false,
                "limits": null
            },
            "tags": [],
            "views": 0,
            "ratio": 1.25
        },
        {
            "id": 3,
            "name": "gamma",
            "active": true,
            "score": 7.0,
            "last_modified_by": null,
            "sandbox": null,
            "tags": ["nested", "array"],
            "views": null,
            "ratio": null
        }
    ])
}

async fn start_http_server() -> Result<
    (
        tokio::sync::oneshot::Sender<()>,
        SocketAddr,
        Arc<AtomicUsize>,
    ),
    String,
> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let edge_request_count = Arc::new(AtomicUsize::new(0));
    let edge_request_counter = Arc::clone(&edge_request_count);

    let app = Router::new()
        .route(
            "/api/shows",
            get(|| async { ([("content-type", "application/json")], SHOWS_JSON) }),
        )
        .route(
            "/api/shows/{id}",
            get(
                |axum::extract::Path(id): axum::extract::Path<u32>| async move {
                    let show = match id {
                        1 => r#"{"id": 1, "name": "Breaking Bad", "rating": 9.5}"#,
                        2 => r#"{"id": 2, "name": "The Wire", "rating": 9.3}"#,
                        3 => r#"{"id": 3, "name": "Better Call Saul", "rating": 8.9}"#,
                        _ => return (axum::http::StatusCode::NOT_FOUND, "Not found".to_string()),
                    };
                    (axum::http::StatusCode::OK, show.to_string())
                },
            ),
        )
        .route(
            "/api/edge",
            get(move || {
                let edge_request_counter = Arc::clone(&edge_request_counter);
                async move {
                    edge_request_counter.fetch_add(1, Ordering::SeqCst);
                    ([("content-type", "application/json")], HTTP_JSON_EDGE_CASES)
                }
            }),
        )
        .route(
            "/data/shows.json",
            get(|| async { ([("content-type", "application/json")], SHOWS_JSON) }),
        )
        .route(
            "/api/headers",
            get(|headers: AxumHeaderMap| async move {
                // Echo all x-* custom headers for deterministic testing
                // (filters out standard headers like host, accept, user-agent)
                // Use BTreeMap for stable key ordering across runs
                let mut echoed = std::collections::BTreeMap::new();
                for (name, value) in &headers {
                    if name.as_str().starts_with("x-")
                        && let Ok(val_str) = value.to_str()
                    {
                        echoed.insert(name.to_string(), val_str.to_string());
                    }
                }
                let body =
                    serde_json::to_string(&echoed).expect("BTreeMap should serialize to JSON");
                ([("content-type", "application/json")], body)
            }),
        )
        .route(
            "/data/items.csv",
            get(|| async { ([("content-type", "text/csv")], ITEMS_CSV) }),
        )
        .route(
            "/api/metrics-paginated",
            get(
                |query: axum::extract::Query<HashMap<String, String>>| async move {
                    // Token-based pagination: returns 2 metrics per page, 3 pages total (5 metrics).
                    static METRICS: &[(&str, f64)] = &[
                        ("cpu", 42.0),
                        ("mem", 78.5),
                        ("disk", 55.0),
                        ("net_in", 12.3),
                        ("net_out", 9.7),
                    ];
                    let page: usize = query
                        .get("cursor")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(1);
                    let items_per_page = 2;
                    let start = (page - 1) * items_per_page;
                    let end = std::cmp::min(start + items_per_page, METRICS.len());
                    let items: Vec<Value> = METRICS[start..end]
                        .iter()
                        .map(|(metric, reading)| json!({ "metric": metric, "reading": reading }))
                        .collect();
                    let next_cursor = if end < METRICS.len() {
                        Value::Number(serde_json::Number::from(page + 1))
                    } else {
                        Value::Null
                    };
                    let body = json!({
                        "data": items,
                        "next_cursor": next_cursor,
                    });
                    ([("content-type", "application/json")], body.to_string())
                },
            ),
        );

    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
        tracing::error!("Failed to bind to address: {e}");
        e.to_string()
    })?;
    let addr = tcp_listener.local_addr().map_err(|e| {
        tracing::error!("Failed to get local address: {e}");
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

    Ok((tx, addr, edge_request_count))
}

async fn load_runtime(app: App) -> Result<Runtime, String> {
    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;
    let cloned_rt = Arc::new(rt.clone());

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    Ok(rt)
}

fn make_http_json_edge_dataset(base_url: &str, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(base_url, "http_json_edges");
    dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
        ("file_format".to_string(), "json".to_string()),
        ("allowed_request_paths".to_string(), "/edge".to_string()),
    ])));

    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            refresh_mode: Some(RefreshMode::Full),
            refresh_sql: Some(
                "SELECT request_path, content FROM http_json_edges WHERE request_path = '/edge'"
                    .to_string(),
            ),
            ..Acceleration::default()
        });
    }

    dataset
}

async fn setup_http_json_edge_runtime(
    test_name: &str,
    base_url: &str,
    accelerated: bool,
    sql_results_cache: Option<SQLResultsCacheConfig>,
) -> Result<Arc<Runtime>, String> {
    let mut app_builder =
        AppBuilder::new(test_name).with_dataset(make_http_json_edge_dataset(base_url, accelerated));

    if let Some(sql_results_cache) = sql_results_cache {
        app_builder = app_builder.with_sql_cache(sql_results_cache);
    }

    let rt = load_runtime(app_builder.build()).await?;
    Ok(Arc::new(rt))
}

async fn run_http_json_edge_query(rt: &Runtime) -> Result<(CacheStatus, Vec<RecordBatch>), String> {
    let query_result = rt
        .datafusion()
        .query_builder(HTTP_JSON_EDGE_QUERY)
        .build()
        .run()
        .await
        .map_err(|e| format!("Failed to run HTTP JSON edge query: {e}"))?;

    let cache_status = query_result.cache_status;
    let batches = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to collect HTTP JSON edge query results: {e}"))?;

    Ok((cache_status, batches))
}

fn assert_http_json_edge_batches(batches: &[RecordBatch]) {
    let value = write_to_json_value(batches).expect("edge-case batches should serialize to JSON");
    assert_eq!(value, expected_http_json_edge_rows());

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "expected three edge-case rows");

    let schema = batches
        .first()
        .expect("expected at least one record batch")
        .schema();
    assert!(matches!(schema.field(0).data_type(), DataType::Int64));
    assert!(matches!(
        schema.field(1).data_type(),
        DataType::Utf8 | DataType::LargeUtf8
    ));
    assert!(matches!(schema.field(2).data_type(), DataType::Boolean));
    assert!(matches!(schema.field(3).data_type(), DataType::Float64));
    assert!(matches!(
        schema.field(4).data_type(),
        DataType::Utf8 | DataType::LargeUtf8
    ));
    assert!(matches!(schema.field(5).data_type(), DataType::Union(_, _)));
    assert!(matches!(schema.field(6).data_type(), DataType::Union(_, _)));
    assert!(matches!(schema.field(7).data_type(), DataType::Int64));
    assert!(matches!(schema.field(8).data_type(), DataType::Float64));
}

/// Returns a [`Config`] configured with ephemeral ports for HTTP and Flight.
///
/// Note: there is an inherent TOCTOU race between discovering the free port
/// and `start_servers` actually binding it. In practice this is negligible in
/// test environments; if it ever becomes flaky, switch to passing the bound
/// listeners directly.
fn create_runtime_http_config() -> Config {
    let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let http_listener = std::net::TcpListener::bind(SocketAddr::new(localhost, 0))
        .expect("to bind http test listener");
    let http_port = http_listener
        .local_addr()
        .expect("to read http test listener address")
        .port();
    drop(http_listener);

    let flight_listener = std::net::TcpListener::bind(SocketAddr::new(localhost, 0))
        .expect("to bind flight test listener");
    let flight_port = flight_listener
        .local_addr()
        .expect("to read flight test listener address")
        .port();
    drop(flight_listener);

    Config::new()
        .with_http_bind_address(SocketAddr::new(localhost, http_port))
        .with_flight_bind_address(SocketAddr::new(localhost, flight_port))
}

async fn start_runtime_http_server(rt: Arc<Runtime>) -> Result<String, String> {
    let api_config = create_runtime_http_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);
    let health_url = format!("{http_base_url}/health");
    let server_rt = Arc::clone(&rt);

    tokio::spawn(async move {
        if let Err(e) = server_rt
            .start_servers(api_config, None, EndpointAuth::no_auth())
            .await
        {
            tracing::error!("Test runtime server failed to start: {e}");
        }
    });

    let client = Client::new();
    let is_started = crate::utils::wait_until_true(std::time::Duration::from_secs(10), || {
        let client = client.clone();
        let health_url = health_url.clone();
        async move {
            client
                .get(&health_url)
                .send()
                .await
                .map_or(false, |response| response.status().is_success())
        }
    })
    .await;

    if is_started {
        Ok(http_base_url)
    } else {
        Err("Timed out waiting for runtime HTTP server to start".to_string())
    }
}

async fn post_sql_json(base_url: &str, sql: &str) -> Result<(Option<String>, Value), String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::ACCEPT,
        "application/json".parse().expect("accept header"),
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        "text/plain".parse().expect("content-type header"),
    );

    let response = Client::new()
        .post(format!("{base_url}/v1/sql"))
        .headers(headers)
        .body(sql.to_string())
        .send()
        .await
        .map_err(|e| format!("Failed to call v1/sql endpoint: {e}"))?;

    let status = response.status();
    let cache_header = response
        .headers()
        .get("X-Cache")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);

    let body = response
        .text()
        .await
        .map_err(|e| format!("Failed to read v1/sql response body: {e}"))?;

    if !status.is_success() {
        return Err(format!("HTTP error from v1/sql: {status} - {body}"));
    }

    let value = serde_json::from_str(&body)
        .map_err(|e| format!("Failed to parse v1/sql response JSON: {e}"))?;

    Ok((cache_header, value))
}

/// Test that a dynamic JSON API endpoint (with `allowed_request_paths` and
/// `request_query_filters`) routes through `HttpTableProvider`, not
/// `HttpListingConnector`. This was a regression introduced by adding "json"
/// to the structured formats list.
#[tokio::test]
async fn test_http_json_api_dynamic() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            let mut dataset = Dataset::new(format!("http://{addr}/api"), "shows");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                (
                    "allowed_request_paths".to_string(),
                    "/shows,/shows/*".to_string(),
                ),
                ("request_query_filters".to_string(), "enabled".to_string()),
            ])));

            let app = AppBuilder::new("http_dynamic_test")
                .with_dataset(dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            let queries: QueryTests = vec![(
                "SELECT request_path, content FROM shows WHERE request_path = '/shows'",
                "http_dynamic_json_api",
                Some(Box::new(|result_batches| {
                    let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert!(
                        total_rows > 0,
                        "expected at least one row, got {total_rows}"
                    );
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    snapshot_suffix,
                    query,
                    false,
                    validate_result,
                )
                .await?;
            }

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// Test that dynamic `request_headers` filters are correctly applied to HTTP requests.
///
/// Verifies:
/// - Dynamic headers from `request_headers IN (...)` are sent on the HTTP request
/// - Static headers from `http_headers` param are preserved
/// - Dynamic headers override static headers with the same name
/// - `request_headers` virtual column is populated in query results
#[tokio::test]
async fn test_http_dynamic_request_headers() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            let mut dataset = Dataset::new(format!("http://{addr}/api"), "header_test");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/headers".to_string()),
                (
                    "http_headers".to_string(),
                    "x-static-header: static-value; x-org-id: default-org".to_string(),
                ),
                ("request_header_filters".to_string(), "enabled".to_string()),
                (
                    "request_header_allowlist".to_string(),
                    "x-org-id, x-custom".to_string(),
                ),
                ("max_request_partitions".to_string(), "100".to_string()),
            ])));

            let app = AppBuilder::new("http_dynamic_headers_test")
                .with_dataset(dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            let query = r#"
                SELECT request_headers, content
                FROM header_test
                WHERE request_path = '/headers'
                  AND request_headers IN (
                    '{"x-org-id":"test-1"}',
                    '{"x-org-id":"test-2","x-custom":"val"}'
                  )
                ORDER BY request_headers
            "#;

            run_query_and_check_results(
                &mut rt,
                "http_dynamic_request_headers",
                query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let pretty = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("failed to format batches");
                    insta::assert_snapshot!("http_dynamic_request_headers_results", pretty);
                })),
            )
            .await?;

            // Test with 100 header values to verify parallel partition execution
            let in_values: Vec<String> = (1..=100)
                .map(|i| format!(r#"'{{"x-org-id":"org-{i:03}"}}'"#))
                .collect();
            let query_100 = format!(
                r"SELECT count(*) as cnt
                FROM header_test
                WHERE request_path = '/headers'
                  AND request_headers IN ({})
                ",
                in_values.join(", ")
            );

            run_query_and_check_results(
                &mut rt,
                "http_dynamic_request_headers_100",
                &query_100,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let rows = write_to_json_value(&result_batches)
                        .expect("batches should serialize to JSON");
                    assert_eq!(
                        rows[0]["cnt"], 100,
                        "expected 100 rows from 100 header partitions"
                    );
                })),
            )
            .await?;

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// Test that `IN (SELECT ...)` subqueries against a real registered table
/// trigger the `HttpParamsPushdown` optimizer rule (deferred params path).
///
///   1. A CSV file (`orgs`) with org IDs
///   2. An HTTP dataset (`data_api`) with header filters
///   3. A query that builds JSON headers from the CSV rows and uses
///      `IN (SELECT ...)` to drive dynamic HTTP requests
///
/// `DataFusion` plans the subquery as a `HashJoinExec` (semi-join) over
/// `HttpExec`, which the optimizer rewrites into `HttpWithDeferredParamsExec`.
#[tokio::test]
// Ignored until deferred HTTP params preserve dynamic headers from subqueries; see #10861.
#[ignore = "https://github.com/spiceai/spiceai/issues/10861"]
async fn test_http_dynamic_request_headers_from_subquery() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            // 1. Register both datasets: the S3 CSV lookup table and the HTTP API.
            let orgs_dataset = Dataset::new("s3://spiceai-public-datasets/orgs.csv", "orgs");

            let mut http_dataset = Dataset::new(format!("http://{addr}/api"), "data_api");
            http_dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/headers".to_string()),
                (
                    "http_headers".to_string(),
                    "x-static-header: static-value".to_string(),
                ),
                ("request_header_filters".to_string(), "enabled".to_string()),
                (
                    "request_header_allowlist".to_string(),
                    "x-org-id".to_string(),
                ),
                ("max_request_partitions".to_string(), "100".to_string()),
            ])));

            let app = AppBuilder::new("http_dynamic_headers_subquery_test")
                .with_dataset(orgs_dataset)
                .with_dataset(http_dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            // 2. Build header JSON from CSV rows, use IN (SELECT ...) to drive dynamic HTTP requests
            let query = r#"
                WITH org_headers AS (
                    SELECT '{"x-org-id":"' || org_id || '"}' AS hdr
                    FROM orgs
                )
                SELECT request_headers, content
                FROM data_api
                WHERE request_path = '/headers'
                  AND request_headers IN (SELECT hdr FROM org_headers)
                ORDER BY request_headers
            "#;

            run_query_and_check_results(
                &mut rt,
                "http_dynamic_request_headers_from_subquery",
                query,
                true,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    let pretty = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("failed to format batches");
                    insta::assert_snapshot!(
                        "http_dynamic_request_headers_from_subquery_results",
                        pretty
                    );
                })),
            )
            .await?;

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// Test that an **accelerated view** whose SQL uses `IN (SELECT ...)`
/// against a real registered table triggers the `HttpParamsPushdown`
/// optimizer rule during the refresh/acceleration path.
///
/// ```yaml
/// views:
///   - name: org_headers_view
///     sql: |
///       WITH org_headers AS (
///         SELECT '{"x-org-id":"' || org_id || '"}' AS hdr FROM orgs
///       )
///       SELECT request_headers, content FROM data_api
///       WHERE request_path = '/headers'
///         AND request_headers IN (SELECT hdr FROM org_headers)
///     acceleration:
///       enabled: true
///       refresh_mode: full
/// ```
#[tokio::test]
async fn test_http_dynamic_request_headers_accelerated_view() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            // 1. Register datasets: S3 CSV lookup table and HTTP API.
            let orgs_dataset = Dataset::new("s3://spiceai-public-datasets/orgs.csv", "orgs");

            let mut http_dataset = Dataset::new(format!("http://{addr}/api"), "data_api");
            http_dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/headers".to_string()),
                (
                    "http_headers".to_string(),
                    "x-static-header: static-value".to_string(),
                ),
                ("request_header_filters".to_string(), "enabled".to_string()),
                (
                    "request_header_allowlist".to_string(),
                    "x-org-id".to_string(),
                ),
                ("max_request_partitions".to_string(), "100".to_string()),
            ])));

            // 2. Create an accelerated view with IN (SELECT ...) subquery SQL.
            let mut view = View::new("org_headers_view".to_string());
            view.sql = Some(
                r#"
                WITH org_headers AS (
                    SELECT '{"x-org-id":"' || org_id || '"}' AS hdr
                    FROM orgs
                )
                SELECT request_headers, content
                FROM data_api
                WHERE request_path = '/headers'
                  AND request_headers IN (SELECT hdr FROM org_headers)
                "#
                .to_string(),
            );
            view.acceleration = Some(Acceleration {
                enabled: true,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("http_dynamic_headers_accel_view_test")
                .with_dataset(orgs_dataset)
                .with_dataset(http_dataset)
                .with_view(view)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let cloned_rt = Arc::clone(&rt);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // 4. Query the accelerated view — data was materialized during refresh.
            let query =
                "SELECT request_headers, content FROM org_headers_view ORDER BY request_headers";

            let result_batches: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder(query)
                .build()
                .run()
                .await
                .map_err(|e| format!("query failed: {e}"))?
                .data
                .try_collect()
                .await
                .map_err(|e| format!("collecting results failed: {e}"))?;

            let pretty = arrow::util::pretty::pretty_format_batches(&result_batches)
                .map_err(|e| format!("format failed: {e}"))?;

            insta::with_settings!({
                description => "Accelerated view with IN (SELECT ...) subquery over HTTP dataset",
                omit_expression => true,
            }, {
                insta::assert_snapshot!(
                    "http_dynamic_request_headers_accelerated_view_results",
                    pretty,
                );
            });

            rt.shutdown().await;

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// Test that a static JSON file endpoint (without dynamic API params) correctly
/// routes through `HttpListingConnector`.
#[tokio::test]
async fn test_http_json_static_file() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            let mut dataset =
                Dataset::new(format!("http://{addr}/data/shows.json"), "shows_static");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "json".to_string(),
            )])));

            let app = AppBuilder::new("http_static_json_test")
                .with_dataset(dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            let queries: QueryTests = vec![(
                "SELECT * FROM shows_static",
                "http_static_json_file",
                Some(Box::new(|result_batches| {
                    let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(total_rows, 3, "expected 3 rows, got {total_rows}");
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    snapshot_suffix,
                    query,
                    false,
                    validate_result,
                )
                .await?;
            }

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// Test that a CSV file served over HTTP correctly routes through
/// `HttpListingConnector` (structured format, always).
#[tokio::test]
async fn test_http_csv_static_file() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            let mut dataset = Dataset::new(format!("http://{addr}/data/items.csv"), "items");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));

            let app = AppBuilder::new("http_csv_test")
                .with_dataset(dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            let queries: QueryTests = vec![(
                "SELECT * FROM items",
                "http_csv_static_file",
                Some(Box::new(|result_batches| {
                    let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(total_rows, 3, "expected 3 rows, got {total_rows}");
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    snapshot_suffix,
                    query,
                    false,
                    validate_result,
                )
                .await?;
            }

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_json_edge_cases_federated() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, edge_request_count) = start_http_server().await?;
            let rt = setup_http_json_edge_runtime(
                "http_json_edge_cases_federated",
                &format!("http://{addr}/api"),
                false,
                None,
            )
            .await?;

            let (cache_status, batches) = run_http_json_edge_query(rt.as_ref()).await?;
            assert_eq!(cache_status, CacheStatus::CacheMiss);
            assert_http_json_edge_batches(&batches);
            assert_eq!(edge_request_count.load(Ordering::SeqCst), 1);

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_json_edge_cases_accelerated() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, edge_request_count) = start_http_server().await?;
            let rt = setup_http_json_edge_runtime(
                "http_json_edge_cases_accelerated",
                &format!("http://{addr}/api"),
                true,
                None,
            )
            .await?;

            assert_eq!(
                edge_request_count.load(Ordering::SeqCst),
                1,
                "accelerated HTTP dataset should fetch edge payload once during refresh"
            );

            let (cache_status, batches) = run_http_json_edge_query(rt.as_ref()).await?;
            assert_eq!(cache_status, CacheStatus::CacheMiss);
            assert_http_json_edge_batches(&batches);
            assert_eq!(
                edge_request_count.load(Ordering::SeqCst),
                1,
                "accelerated queries should read refreshed data without re-fetching the HTTP source"
            );

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_json_edge_cases_results_cache() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, edge_request_count) = start_http_server().await?;
            let rt = setup_http_json_edge_runtime(
                "http_json_edge_cases_results_cache",
                &format!("http://{addr}/api"),
                false,
                Some(SQLResultsCacheConfig {
                    item_ttl: Some("60s".to_string()),
                    ..Default::default()
                }),
            )
            .await?;

            let (first_cache_status, first_batches) = run_http_json_edge_query(rt.as_ref()).await?;
            assert_eq!(first_cache_status, CacheStatus::CacheMiss);
            assert_http_json_edge_batches(&first_batches);
            assert_eq!(edge_request_count.load(Ordering::SeqCst), 1);

            let (second_cache_status, second_batches) =
                run_http_json_edge_query(rt.as_ref()).await?;
            assert_eq!(second_cache_status, CacheStatus::CacheHit);
            assert_http_json_edge_batches(&second_batches);
            assert_eq!(
                edge_request_count.load(Ordering::SeqCst),
                1,
                "results-cache hit should avoid a second HTTP fetch"
            );

            let http_base_url = start_runtime_http_server(Arc::clone(&rt)).await?;
            let (first_http_cache_header, first_http_response) =
                post_sql_json(&http_base_url, HTTP_JSON_EDGE_QUERY).await?;
            assert_eq!(first_http_response, expected_http_json_edge_rows());

            let (second_http_cache_header, second_http_response) =
                post_sql_json(&http_base_url, HTTP_JSON_EDGE_QUERY).await?;
            assert_eq!(second_http_response, expected_http_json_edge_rows());
            assert!(
                matches!(
                    first_http_cache_header.as_deref(),
                    Some("Hit from spiceai" | "Miss from spiceai")
                ),
                "unexpected first HTTP cache header: {first_http_cache_header:?}"
            );
            assert_eq!(
                second_http_cache_header.as_deref(),
                Some("Hit from spiceai")
            );

            rt.shutdown().await;

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

const SECURE_DATA_JSON: &str = r#"[
    {"id": 1, "name": "secret-alpha"},
    {"id": 2, "name": "secret-beta"}
]"#;

#[derive(Clone)]
struct OauthServerState {
    /// Access tokens that should be considered valid. Prefixed with
    /// `access-` and a monotonic counter, so each refresh produces a
    /// distinct token.
    issued_access_tokens: Arc<tokio::sync::Mutex<Vec<String>>>,
    /// Refresh tokens considered valid. We seed one entry and append newly
    /// issued refresh tokens on each exchange; previously issued refresh
    /// tokens remain valid for the duration of the test server (we never
    /// invalidate them), which keeps the test stub simple.
    valid_refresh_tokens: Arc<tokio::sync::Mutex<Vec<String>>>,
    token_requests: Arc<AtomicUsize>,
    data_requests: Arc<AtomicUsize>,
    last_auth_header: Arc<tokio::sync::Mutex<Option<String>>>,
    refresh_counter: Arc<AtomicUsize>,
}

impl OauthServerState {
    fn with_seed_refresh(seed: &str) -> Self {
        Self {
            issued_access_tokens: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            valid_refresh_tokens: Arc::new(tokio::sync::Mutex::new(vec![seed.to_string()])),
            token_requests: Arc::new(AtomicUsize::new(0)),
            data_requests: Arc::new(AtomicUsize::new(0)),
            last_auth_header: Arc::new(tokio::sync::Mutex::new(None)),
            refresh_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

async fn oauth_token_handler(
    State(state): State<OauthServerState>,
    Form(params): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    state.token_requests.fetch_add(1, Ordering::SeqCst);

    if params.get("grant_type").map(String::as_str) != Some("refresh_token") {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/json")],
            r#"{"error":"unsupported_grant_type"}"#.to_string(),
        );
    }

    let Some(presented) = params.get("refresh_token") else {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/json")],
            r#"{"error":"invalid_request"}"#.to_string(),
        );
    };

    let valid = {
        let guard = state.valid_refresh_tokens.lock().await;
        guard.iter().any(|t| t == presented)
    };
    if !valid {
        return (
            StatusCode::UNAUTHORIZED,
            [("content-type", "application/json")],
            r#"{"error":"invalid_grant"}"#.to_string(),
        );
    }

    let n = state.refresh_counter.fetch_add(1, Ordering::SeqCst) + 1;
    let access = format!("access-{n}");
    let rotated = format!("refresh-{n}");

    state.issued_access_tokens.lock().await.push(access.clone());
    state
        .valid_refresh_tokens
        .lock()
        .await
        .push(rotated.clone());

    let body = serde_json::json!({
        "access_token": access,
        "refresh_token": rotated,
        "token_type": "Bearer",
        "expires_in": 3600,
    })
    .to_string();

    (StatusCode::OK, [("content-type", "application/json")], body)
}

async fn oauth_secure_handler(
    State(state): State<OauthServerState>,
    headers: AxumHeaderMap,
) -> impl IntoResponse {
    state.data_requests.fetch_add(1, Ordering::SeqCst);

    let auth_value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    *state.last_auth_header.lock().await = auth_value.clone();

    let Some(auth) = auth_value else {
        return (
            StatusCode::UNAUTHORIZED,
            [("content-type", "application/json")],
            r#"{"error":"missing_bearer"}"#.to_string(),
        );
    };

    let Some(token) = auth.strip_prefix("Bearer ") else {
        return (
            StatusCode::UNAUTHORIZED,
            [("content-type", "application/json")],
            r#"{"error":"bad_scheme"}"#.to_string(),
        );
    };

    let known = state
        .issued_access_tokens
        .lock()
        .await
        .iter()
        .any(|t| t == token);

    if !known {
        return (
            StatusCode::UNAUTHORIZED,
            [("content-type", "application/json")],
            r#"{"error":"invalid_token"}"#.to_string(),
        );
    }

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        SECURE_DATA_JSON.to_string(),
    )
}

async fn start_oauth_http_server(
    state: OauthServerState,
) -> Result<(tokio::sync::oneshot::Sender<()>, SocketAddr), String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let app = Router::new()
        .route("/oauth/token", post(oauth_token_handler))
        .route("/api/secure", get(oauth_secure_handler))
        .with_state(state);

    let tcp_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind oauth listener: {e}"))?;
    let addr = tcp_listener
        .local_addr()
        .map_err(|e| format!("oauth local_addr: {e}"))?;

    tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .expect("OAuth test server failed while serving requests");
    });

    Ok((tx, addr))
}

/// End-to-end check for `OAuth2` refresh-token auth on the HTTP connector:
/// the connector should exchange the refresh token at startup, then stamp
/// `Authorization: Bearer <access_token>` onto every data request.
#[tokio::test]
async fn test_http_oauth2_refresh_token_auth() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let state = OauthServerState::with_seed_refresh("seed-refresh");
            let (tx, addr) = start_oauth_http_server(state.clone()).await?;
            tracing::debug!("OAuth test server started at {addr}");

            let mut dataset = Dataset::new(format!("http://{addr}/api"), "secure_data");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/secure".to_string()),
                (
                    "auth_token_url".to_string(),
                    format!("http://{addr}/oauth/token"),
                ),
                (
                    "http_auth_refresh_token".to_string(),
                    "seed-refresh".to_string(),
                ),
                ("http_auth_client_id".to_string(), "test-client".to_string()),
                (
                    "http_auth_client_secret".to_string(),
                    "test-secret".to_string(),
                ),
                ("auth_scopes".to_string(), "read:data".to_string()),
            ])));

            let app = AppBuilder::new("http_oauth_refresh_test")
                .with_dataset(dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            // Initial refresh-token exchange happens during connector init, before
            // the first data query. Confirm it occurred exactly once.
            assert_eq!(
                state.token_requests.load(Ordering::SeqCst),
                1,
                "expected a single OAuth2 token exchange at startup",
            );

            let queries: QueryTests = vec![(
                "SELECT request_path, content FROM secure_data WHERE request_path = '/secure'",
                "http_oauth_refresh_token",
                Some(Box::new(|result_batches| {
                    let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert!(
                        total_rows > 0,
                        "expected at least one row, got {total_rows}"
                    );
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    snapshot_suffix,
                    query,
                    false,
                    validate_result,
                )
                .await?;
            }

            assert!(
                state.data_requests.load(Ordering::SeqCst) >= 1,
                "expected at least one data request to /api/secure"
            );

            let last_auth = state.last_auth_header.lock().await.clone();
            assert_eq!(
                last_auth.as_deref(),
                Some("Bearer access-1"),
                "data request should carry the access token issued by the OAuth server"
            );

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// A missing `auth_token_url` alongside a configured `http_auth_refresh_token`
/// should be rejected at dataset registration instead of silently proceeding
/// without auth.
#[tokio::test]
async fn test_http_oauth2_rejects_partial_configuration() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;

            let mut dataset = Dataset::new(format!("http://{addr}/api"), "partial_auth");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/shows".to_string()),
                (
                    "http_auth_refresh_token".to_string(),
                    "rt-without-url".to_string(),
                ),
            ])));

            let app = AppBuilder::new("http_oauth_partial_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // The dataset should fail to load because http_auth_refresh_token
            // requires auth_token_url. load_components completes even when
            // individual datasets fail, so inspect the query path to confirm
            // the failure surfaced.
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    return Err("Timed out waiting for component load to complete".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let query_result = rt
                .datafusion()
                .query_builder("SELECT 1 FROM partial_auth LIMIT 1")
                .build()
                .run()
                .await;
            assert!(
                query_result.is_err(),
                "partial OAuth2 auth config should prevent the dataset from serving queries"
            );

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}

/// Tests `IN (SELECT ...)` subqueries with **pagination** against a real registered table
///
///   1. A CSV file (`orgs`) with org IDs
///   2. An HTTP dataset (`paginated_api`) with header filters and token-based pagination
///   3. A query that builds JSON headers from the CSV rows and uses
///      `IN (SELECT ...)` to drive dynamic HTTP requests across multiple pages
#[tokio::test]
// Ignored until deferred HTTP params preserve dynamic headers from subqueries; see #10861.
#[ignore = "https://github.com/spiceai/spiceai/issues/10861"]
async fn test_http_dynamic_request_headers_from_subquery_with_pagination() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            // 1. Register both datasets: the S3 CSV lookup table and the paginated HTTP API.
            let orgs_dataset = Dataset::new("s3://spiceai-public-datasets/orgs.csv", "orgs");

            let mut http_dataset = Dataset::new(format!("http://{addr}/api"), "paginated_api");
            http_dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                (
                    "allowed_request_paths".to_string(),
                    "/metrics-paginated".to_string(),
                ),
                ("request_header_filters".to_string(), "enabled".to_string()),
                (
                    "request_header_allowlist".to_string(),
                    "x-org-id".to_string(),
                ),
                ("max_request_partitions".to_string(), "100".to_string()),
                // Token-based pagination config
                ("pagination".to_string(), "enabled".to_string()),
                (
                    "pagination_next_pointer".to_string(),
                    "/next_cursor".to_string(),
                ),
                ("pagination_token_param".to_string(), "cursor".to_string()),
                ("pagination_data_pointer".to_string(), "/data".to_string()),
                ("pagination_max_pages".to_string(), "10".to_string()),
            ])));

            let app = AppBuilder::new("http_dynamic_headers_subquery_paginated_test")
                .with_dataset(orgs_dataset)
                .with_dataset(http_dataset)
                .build();
            let mut rt = load_runtime(app).await?;

            // 2. Build header JSON from CSV rows, use IN (SELECT ...) to drive
            //    dynamic paginated HTTP requests.
            let query = r#"
                WITH org_headers AS (
                    SELECT '{"x-org-id":"' || org_id || '"}' AS hdr
                    FROM orgs
                )
                SELECT
                    request_headers,
                    json_get_str(content, 'metric') AS metric,
                    json_get_float(content, 'reading') AS reading
                FROM paginated_api
                WHERE request_path = '/metrics-paginated'
                  AND request_headers IN (SELECT hdr FROM org_headers)
                ORDER BY request_headers, metric
            "#;

            run_query_and_check_results(
                &mut rt,
                "http_dynamic_request_headers_from_subquery_paginated",
                query,
                false,
                Some(Box::new(|result_batches: Vec<RecordBatch>| {
                    // Each org should get 5 metrics (3 pages: 2+2+1).
                    let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
                    assert!(total_rows > 0, "expected paginated results but got 0 rows");
                    let pretty = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("failed to format batches");
                    insta::assert_snapshot!(
                        "http_dynamic_request_headers_from_subquery_paginated_results",
                        pretty
                    );
                })),
            )
            .await?;

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}
