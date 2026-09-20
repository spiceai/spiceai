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
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{RecordBatch, StringArray};
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::routing::get;
use axum::{Json, Router};
use futures::{FutureExt, TryStreamExt};
use runtime::datafusion::query::QueryBuilder;
use serde_json::{Value, json};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

const REQUIRED_HEADER: &str = "executor-provider";

#[derive(Clone, Debug, PartialEq, Eq)]
struct ObservedRequest {
    page: usize,
    limit: usize,
    configured_header: Option<String>,
}

#[derive(Clone)]
struct FixtureState {
    requests: Arc<Mutex<Vec<ObservedRequest>>>,
}

struct HttpFixture {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<ObservedRequest>>>,
    handle: JoinHandle<()>,
}

impl HttpFixture {
    async fn start() -> Result<Self, anyhow::Error> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = FixtureState {
            requests: Arc::clone(&requests),
        };
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/users", get(users_page))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok(Self {
            addr,
            requests,
            handle,
        })
    }

    async fn observations(&self) -> Vec<ObservedRequest> {
        self.requests.lock().await.clone()
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn users_page(
    State(state): State<FixtureState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Json<Vec<Value>> {
    let page = query
        .get("page")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    let configured_header = headers
        .get("x-cluster-config")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    state.requests.lock().await.push(ObservedRequest {
        page,
        limit,
        configured_header,
    });

    let rows = match page {
        0 => vec![
            json!({"id": 1, "label": "first"}),
            json!({"id": 2, "label": "second"}),
        ],
        1 => vec![
            json!({"id": 3, "label": "needle"}),
            json!({"id": 4, "label": "fourth"}),
        ],
        _ => Vec::new(),
    };
    Json(rows)
}

async fn distributed_query(
    harness: &ClusterHarness,
    sql: &str,
    job_name: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let handle = QueryBuilder::new(sql, harness.scheduler.datafusion())
        .build()
        .submit_distributed(job_name)
        .await
        .map_err(|error| anyhow::anyhow!("submit distributed query: {error}"))?;
    handle
        .into_stream()
        .await
        .map_err(|error| anyhow::anyhow!("open distributed query stream: {error}"))?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|error| anyhow::anyhow!("collect distributed query results: {error}"))
}

fn string_column(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("result should contain {name}"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("{name} should be Utf8"))
                .iter()
                .map(|value| value.expect("test rows should be non-null").to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn run_with_harness<F>(harness: ClusterHarness, f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnOnce(
        &'a ClusterHarness,
    )
        -> Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'a>>,
{
    let result = AssertUnwindSafe(f(&harness)).catch_unwind().await;
    harness.shutdown().await;
    match result {
        Ok(inner) => inner,
        Err(panic_payload) => std::panic::resume_unwind(panic_payload),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn distributed_http_scan_preserves_provider_and_pagination() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let fixture = HttpFixture::start().await?;

            let mut dataset = Dataset::new(format!("http://{}/users", fixture.addr), "users");
            dataset.description = Some("forces metadata provider wrapping".to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                (
                    "http_headers".to_string(),
                    format!("x-cluster-config: {REQUIRED_HEADER}"),
                ),
                ("health_probe".to_string(), "/health".to_string()),
                ("client_timeout".to_string(), "5".to_string()),
                ("max_retries".to_string(), "0".to_string()),
                ("pagination".to_string(), "enabled".to_string()),
                ("pagination_link_header".to_string(), "disabled".to_string()),
                (
                    "pagination_query_params".to_string(),
                    "page={page}&limit={limit}".to_string(),
                ),
                ("pagination_page_size".to_string(), "2".to_string()),
                ("pagination_max_pages".to_string(), "4".to_string()),
                ("response_cache_max_size_bytes".to_string(), "0".to_string()),
            ])));

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("distributed_http")
                        .with_dataset(dataset)
                        .build(),
                )
                .executors(1)
                .start()
                .await?;
            run_with_harness(harness, |harness| {
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(30)).await?;

                    let reported = distributed_query(
                        harness,
                        "SELECT request_query, content FROM users LIMIT 1",
                        "distributed_http_reported_query",
                    )
                    .await?;
                    assert_eq!(
                        string_column(&reported, "request_query"),
                        ["page=0&limit=2"]
                    );
                    let reported_content = string_column(&reported, "content");
                    assert_eq!(reported_content.len(), 1);
                    assert!(reported_content[0].contains("\"id\":1"));

                    let after_reported = fixture.observations().await;
                    assert_eq!(
                        after_reported,
                        [ObservedRequest {
                            page: 0,
                            limit: 2,
                            configured_header: Some(REQUIRED_HEADER.to_string()),
                        }]
                    );

                    let filtered = distributed_query(
                        harness,
                        "SELECT content FROM users WHERE content LIKE '%needle%' LIMIT 1",
                        "distributed_http_residual_filter",
                    )
                    .await?;
                    let filtered_content = string_column(&filtered, "content");
                    assert_eq!(filtered_content.len(), 1);
                    assert!(filtered_content[0].contains("needle"));

                    let all_observations = fixture.observations().await;
                    let filtered_observations = &all_observations[after_reported.len()..];
                    assert_eq!(
                        filtered_observations,
                        [
                            ObservedRequest {
                                page: 0,
                                limit: 2,
                                configured_header: Some(REQUIRED_HEADER.to_string()),
                            },
                            ObservedRequest {
                                page: 1,
                                limit: 2,
                                configured_header: Some(REQUIRED_HEADER.to_string()),
                            },
                        ]
                    );

                    Ok(())
                })
            })
            .await?;
            Ok(())
        })
        .await
}
