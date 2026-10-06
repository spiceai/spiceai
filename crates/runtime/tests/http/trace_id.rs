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

//! The trace id an HTTP caller reads back for a query.
//!
//! MCP is served over the same HTTP server, and the id is returned by the
//! layer that wraps every route, so these cover it too.

use std::sync::Arc;

use app::AppBuilder;
use reqwest::Client;
use runtime::Runtime;
use runtime_request_context::{SPICE_TRACE_ID_HEADER as TRACE_ID_HEADER, normalize_trace_id};

use super::start_runtime_http_server;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

const PINNED: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

async fn start() -> Result<(Arc<Runtime>, String), String> {
    register_test_connectors().await;
    configure_test_datafusion();

    let rt = Arc::new(
        Runtime::builder()
            .with_app(AppBuilder::new("trace_id_test").build())
            .build()
            .await,
    );
    Arc::clone(&rt).load_components().await;
    runtime_ready_check(&rt).await;

    let base_url = start_runtime_http_server(Arc::clone(&rt)).await?;
    Ok((rt, base_url))
}

/// Posts `sql` to `/v1/sql`, returning the response status and the trace id it
/// carried, if any.
async fn post_sql(base_url: &str, sql: &str, pin: Option<&str>) -> (reqwest::StatusCode, String) {
    let mut request = Client::new()
        .post(format!("{base_url}/v1/sql"))
        .body(sql.to_string());
    if let Some(pin) = pin {
        request = request.header(TRACE_ID_HEADER, pin);
    }

    let response = request
        .send()
        .await
        .expect("the request reaches the runtime");
    let status = response.status();
    let trace_id = response
        .headers()
        .get(TRACE_ID_HEADER)
        .unwrap_or_else(|| panic!("the response must carry `{TRACE_ID_HEADER}`"))
        .to_str()
        .expect("a trace id is ASCII")
        .to_string();

    (status, trace_id)
}

/// A caller that pinned nothing still gets an id back, and a *failing* query
/// gets one too — the id is written by the layer wrapping every route, below
/// the point a handler's error becomes a response, so the query most worth
/// correlating is not the one that loses its id. Each query gets its own.
#[tokio::test]
async fn http_returns_the_trace_id_for_a_query_and_for_a_failure() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (rt, base_url) = start().await?;

            let (status, succeeded) = post_sql(&base_url, "SELECT 1 AS n", None).await;
            assert!(status.is_success(), "SELECT 1 should succeed, got {status}");
            assert!(
                normalize_trace_id(&succeeded).is_some(),
                "not a usable trace id: `{succeeded}`"
            );

            let (status, failed) = post_sql(&base_url, "SELECT * FROM no_such_table", None).await;
            assert!(
                !status.is_success(),
                "a query naming no table should fail, got {status}"
            );
            assert!(
                normalize_trace_id(&failed).is_some(),
                "a failed query must still name an id, got `{failed}`"
            );

            assert_ne!(succeeded, failed, "each query gets its own id");

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// A caller that pins an id is told the id it sent, not one of the runtime's.
#[tokio::test]
async fn http_returns_a_pinned_trace_id_unchanged() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (rt, base_url) = start().await?;

            let (status, trace_id) = post_sql(&base_url, "SELECT 1 AS n", Some(PINNED)).await;
            assert!(status.is_success(), "SELECT 1 should succeed, got {status}");
            assert_eq!(trace_id, PINNED);

            rt.shutdown().await;
            Ok(())
        })
        .await
}
