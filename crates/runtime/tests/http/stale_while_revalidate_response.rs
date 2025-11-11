/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use runtime::{Runtime, config};
use runtime_testing::{get_test_datafusion, test_request_context};
use spicepod::component::{
    caching::{Caching, SQLResultsCacheConfig},
    runtime as spicepod_runtime,
};

#[tokio::test]
async fn test_http_response_includes_cache_control_header() {
    let _ = runtime_testing::init_tracing(None);

    // Create app with stale-while-revalidate configuration
    let app = Arc::new(
        app::AppBuilder::new("test_app")
            .with_runtime(spicepod_runtime::Runtime {
                caching: Caching {
                    sql_results: Some(SQLResultsCacheConfig {
                        enabled: true,
                        item_ttl: Some("60s".to_string()),
                        stale_while_revalidate_ttl: Some("30s".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            })
            .build(),
    );

    let df = get_test_datafusion(Some(app.clone()), None).await;
    let _request_context = test_request_context(df.clone(), Some(app.clone()));

    // Run a query through HTTP endpoint
    let sql = "SELECT 1 as num";
    let response = runtime::http::v1::sql_to_http_response(
        df.clone(),
        sql,
        None,
        runtime::http::v1::ResponseMimeType::Json,
    )
    .await;

    // Extract headers
    let (status, headers, _body) = response.into_parts();

    // Verify status is OK
    assert_eq!(status, axum::http::StatusCode::OK);

    // Verify Cache-Control header is present with correct value
    let cache_control = headers.get("cache-control");
    assert!(
        cache_control.is_some(),
        "Cache-Control header should be present in response"
    );

    let cache_control_value = cache_control
        .expect("Cache-Control header expected")
        .to_str()
        .expect("Cache-Control should be valid ASCII");

    // Should contain both max-age and stale-while-revalidate
    assert!(
        cache_control_value.contains("max-age=60"),
        "Cache-Control should contain max-age=60, got: {}",
        cache_control_value
    );
    assert!(
        cache_control_value.contains("stale-while-revalidate=30"),
        "Cache-Control should contain stale-while-revalidate=30, got: {}",
        cache_control_value
    );
}

#[tokio::test]
async fn test_http_response_without_stale_while_revalidate_config() {
    let _ = runtime_testing::init_tracing(None);

    // Create app WITHOUT stale-while-revalidate configuration
    let app = Arc::new(
        app::AppBuilder::new("test_app")
            .with_runtime(spicepod_runtime::Runtime {
                caching: Caching {
                    sql_results: Some(SQLResultsCacheConfig {
                        enabled: true,
                        item_ttl: Some("60s".to_string()),
                        stale_while_revalidate_ttl: None, // No stale-while-revalidate
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            })
            .build(),
    );

    let df = get_test_datafusion(Some(app.clone()), None).await;
    let _request_context = test_request_context(df.clone(), Some(app.clone()));

    // Run a query through HTTP endpoint
    let sql = "SELECT 1 as num";
    let response = runtime::http::v1::sql_to_http_response(
        df.clone(),
        sql,
        None,
        runtime::http::v1::ResponseMimeType::Json,
    )
    .await;

    // Extract headers
    let (_status, headers, _body) = response.into_parts();

    // Verify Cache-Control header is NOT present when stale-while-revalidate is not configured
    let cache_control = headers.get("cache-control");
    assert!(
        cache_control.is_none(),
        "Cache-Control header should NOT be present when stale-while-revalidate is not configured"
    );
}

#[tokio::test]
async fn test_stale_while_revalidate_lifecycle() {
    use tokio::time::{Duration, sleep};

    let _ = runtime_testing::init_tracing(None);

    // Create app with short TTL for testing
    let app = Arc::new(
        app::AppBuilder::new("test_app")
            .with_runtime(spicepod_runtime::Runtime {
                caching: Caching {
                    sql_results: Some(SQLResultsCacheConfig {
                        enabled: true,
                        item_ttl: Some("2s".to_string()), // Short TTL for testing
                        stale_while_revalidate_ttl: Some("5s".to_string()), // Allow 5s of stale
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            })
            .build(),
    );

    let df = get_test_datafusion(Some(app.clone()), None).await;
    let _request_context = test_request_context(df.clone(), Some(app.clone()));

    let sql = "SELECT 1 as num";

    // First request - should be MISS (cache is empty)
    let response = runtime::http::v1::sql_to_http_response(
        df.clone(),
        sql,
        None,
        runtime::http::v1::ResponseMimeType::Json,
    )
    .await;
    let (_status, headers, _body) = response.into_parts();

    let results_cache_status = headers
        .get("results-cache-status")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("UNKNOWN");
    assert_eq!(
        results_cache_status, "MISS",
        "First request should be a cache MISS, got: {}",
        results_cache_status
    );

    // Second request - should be HIT (cached and fresh)
    let response = runtime::http::v1::sql_to_http_response(
        df.clone(),
        sql,
        None,
        runtime::http::v1::ResponseMimeType::Json,
    )
    .await;
    let (_status, headers, _body) = response.into_parts();

    let results_cache_status = headers
        .get("results-cache-status")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("UNKNOWN");
    assert_eq!(
        results_cache_status, "HIT",
        "Second request should be a cache HIT, got: {}",
        results_cache_status
    );

    // Wait for TTL to expire (2s + a bit more for safety)
    sleep(Duration::from_millis(2500)).await;

    // Third request - should be STALE (beyond TTL but within stale-while-revalidate window)
    let response = runtime::http::v1::sql_to_http_response(
        df.clone(),
        sql,
        None,
        runtime::http::v1::ResponseMimeType::Json,
    )
    .await;
    let (_status, headers, _body) = response.into_parts();

    let results_cache_status = headers
        .get("results-cache-status")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("UNKNOWN");
    assert_eq!(
        results_cache_status, "STALE",
        "Third request (after TTL expiry) should be STALE, got: {}",
        results_cache_status
    );

    // Give background revalidation a moment to complete
    sleep(Duration::from_millis(500)).await;

    // Fourth request - should be HIT again (freshly revalidated)
    let response = runtime::http::v1::sql_to_http_response(
        df.clone(),
        sql,
        None,
        runtime::http::v1::ResponseMimeType::Json,
    )
    .await;
    let (_status, headers, _body) = response.into_parts();

    let results_cache_status = headers
        .get("results-cache-status")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("UNKNOWN");
    assert_eq!(
        results_cache_status, "HIT",
        "Fourth request (after revalidation) should be HIT, got: {}",
        results_cache_status
    );

    // Verify Cache-Control header is still present
    let cache_control = headers.get("cache-control");
    assert!(
        cache_control.is_some(),
        "Cache-Control header should be present"
    );
}
