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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use axum::{Router, routing::get};
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tokio::net::TcpListener;

use crate::utils::{register_test_connectors, test_request_context};
use crate::{ValidateFn, configure_test_datafusion, init_tracing, run_query_and_check_results};

const SHOWS_JSON: &str = r#"[
    {"id": 1, "name": "Breaking Bad", "rating": 9.5},
    {"id": 2, "name": "The Wire", "rating": 9.3},
    {"id": 3, "name": "Better Call Saul", "rating": 8.9}
]"#;

const ITEMS_CSV: &str = "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n3,Doohickey,4.99\n";

async fn start_http_server() -> Result<(tokio::sync::oneshot::Sender<()>, SocketAddr), String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

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

    Ok((tx, addr))
}

/// Test that a dynamic JSON API endpoint (with `allowed_request_paths` and
/// `request_query_filters`) routes through `HttpTableProvider`, not
/// `HttpListingConnector`. This was a regression introduced by adding "json"
/// to the structured formats list.
#[tokio::test]
async fn test_http_json_api_dynamic() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr) = start_http_server().await?;
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

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

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
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr) = start_http_server().await?;
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

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

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
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr) = start_http_server().await?;
            tracing::debug!("HTTP test server started at {addr}");

            let mut dataset = Dataset::new(format!("http://{addr}/data/items.csv"), "items");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));

            let app = AppBuilder::new("http_csv_test")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

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
