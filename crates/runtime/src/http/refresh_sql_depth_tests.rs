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

//! A refresh SQL override nested deeper than the query engine plans, sent
//! through the runtime's own routes to an accelerated dataset.

use std::{sync::Arc, time::Duration};

use arrow::array::{Array, Int64Array};
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use datafusion::arrow::compute::concat;
use http_body_util::BodyExt;
use runtime_query_engine::sql_depth_guard::{MAX_SQL_DATA_TYPE_DEPTH, MAX_SQL_EXPR_DEPTH};
use runtime_search::search_engine::{SearchEngine, parse_explicit_primary_keys};
use serde_json::{Value, json};
use spicepod::component::runtime::CorsConfig;
use spicepod::{acceleration::Acceleration, component::dataset::Dataset};
use tower::ServiceExt;

use crate::{Runtime, config::Config};

const DATASET: &str = "depth_events";
const REFRESH_SQL: &str = "SELECT id FROM depth_events WHERE id > 1";

async fn routes(rt: &Arc<Runtime>) -> Router {
    let search = Arc::new(SearchEngine::new(
        Arc::clone(&rt.df) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
        parse_explicit_primary_keys(Arc::clone(&rt.app))
            .await
            .into(),
        crate::search::util::RuntimeTableProviderExplorer,
    ));
    super::routes::routes(
        rt,
        Arc::new(Config::new()),
        search,
        None,
        &CorsConfig::default(),
        false,
        #[cfg(feature = "mcp")]
        None,
    )
}

async fn send(routes: &Router, method: Method, path: &str, body: &Value) -> (StatusCode, String) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = routes.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// The ids in the dataset's acceleration, in order; an error until its first
/// load completes.
async fn accelerated_ids(rt: &Runtime) -> datafusion::error::Result<Vec<i64>> {
    let batches = rt
        .df
        .ctx
        .sql(&format!("SELECT id FROM {DATASET} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let columns: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
    if columns.is_empty() {
        return Ok(Vec::new());
    }
    let ids = concat(&columns)?;
    Ok(ids
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64")
        .values()
        .to_vec())
}

async fn wait_for_ids(rt: &Runtime, expected: &[i64]) {
    let mut last = String::new();
    for _ in 0..600 {
        match accelerated_ids(rt).await {
            Ok(ids) if ids == expected => return,
            Ok(ids) => last = format!("{ids:?}"),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the acceleration holds {last}, expected {expected:?}");
}

/// Regression test: an override whose `WHERE` chain exceeds the planner's
/// depth limit was accepted with `201 Created`, and the refresh it queued
/// overflowed the refresh worker's stack and aborted the process; an override
/// with a deeply nested data type overflowed the stack it was parsed or dropped
/// on. `POST` refresh and `PATCH` acceleration now refuse each with an error,
/// and the dataset keeps refreshing with its own refresh SQL.
#[tokio::test]
async fn an_over_deep_refresh_sql_override_is_refused() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let csv = dir.path().join("events.csv");
    tokio::fs::write(&csv, "id\n1\n2\n3\n")
        .await
        .expect("source file is written");

    let mut dataset = Dataset::new(format!("file:{}", csv.display()), DATASET);
    dataset.acceleration = Some(Acceleration {
        refresh_sql: Some(REFRESH_SQL.to_string()),
        ..Acceleration::default()
    });
    let app = app::AppBuilder::new("refresh_sql_depth")
        .with_dataset(dataset)
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
        .await
        .expect("components load");
    wait_for_ids(&rt, &[2, 3]).await;

    let routes = routes(&rt).await;
    let deep_overrides = [
        (
            format!(
                "SELECT id FROM {DATASET} WHERE {}",
                vec!["id > 0"; 1200].join(" AND ")
            ),
            format!(
                "The provided Refresh SQL is too complex to run. Error during planning: Query \
                 rejected: an expression is nested more than {MAX_SQL_EXPR_DEPTH} levels deep"
            ),
        ),
        (
            format!(
                "SELECT id FROM {DATASET} WHERE CAST(NULL AS {}INT{}) IS NULL",
                "ARRAY<".repeat(10_000),
                ">".repeat(10_000)
            ),
            format!(
                "The provided Refresh SQL is too complex to run. Error during planning: Query \
                 rejected: a data type is nested more than {MAX_SQL_DATA_TYPE_DEPTH} levels deep"
            ),
        ),
        (
            format!(
                "SELECT id FROM {DATASET} WHERE CAST(NULL AS INT{}) IS NULL",
                "[]".repeat(10_000)
            ),
            "The provided Refresh SQL could not be parsed. SQL error: RecursionLimitExceeded"
                .to_string(),
        ),
    ];
    for (refresh_sql, rejection) in &deep_overrides {
        let deep = json!({ "refresh_sql": refresh_sql });
        // `PATCH` reports every invalid refresh SQL as `500 Internal Server Error`.
        for (method, path, expected_status) in [
            (
                Method::POST,
                format!("/v1/datasets/{DATASET}/acceleration/refresh"),
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::PATCH,
                format!("/v1/datasets/{DATASET}/acceleration"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ] {
            let (status, body) = send(&routes, method.clone(), &path, &deep).await;
            let message = serde_json::from_str::<Value>(&body).expect("JSON body")["message"]
                .as_str()
                .map(ToString::to_string)
                .unwrap_or_default();
            assert_eq!(status, expected_status, "{method} {path}: {body}");
            assert!(
                message.contains(rejection.as_str()),
                "{method} {path}: {message}"
            );
        }
    }

    tokio::fs::write(&csv, "id\n1\n2\n3\n4\n")
        .await
        .expect("source file is rewritten");
    let (status, body) = send(
        &routes,
        Method::POST,
        &format!("/v1/datasets/{DATASET}/acceleration/refresh"),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    wait_for_ids(&rt, &[2, 3, 4]).await;
}
