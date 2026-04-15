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

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use app::{App, AppBuilder};
use arrow::{array::RecordBatch, datatypes::DataType};
use axum::{Router, routing::get};
use cache::result::CacheStatus;
use futures::TryStreamExt;
use reqwest::{Client, header::HeaderMap};
use runtime::{
    Runtime, auth::EndpointAuth, config::Config, datafusion::query::write_to_json_value,
};
use serde_json::{Value, json};
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::{caching::SQLResultsCacheConfig, dataset::Dataset},
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
            "/data/items.csv",
            get(|| async { ([("content-type", "text/csv")], ITEMS_CSV) }),
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
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
                .map(|response| response.status().is_success())
                .unwrap_or(false)
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
