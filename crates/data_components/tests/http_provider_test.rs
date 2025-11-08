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

use datafusion::prelude::*;
use data_components::http::provider::HttpTableProvider;
use reqwest::Client;
use std::sync::Arc;

/// Integration test that fetches real data from httpbin.org
#[tokio::test]
async fn test_http_provider_with_real_endpoint() {
    let base_url = url::Url::parse("https://httpbin.org").expect("valid URL");
    let provider = HttpTableProvider::new(
        base_url,
        Client::new(),
        "json".to_string(),
        false,
    );

    // Create a DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query with specific path and no query string
    let df = ctx
        .sql("SELECT path, query, content FROM httpbin WHERE path = '/json'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have exactly one row");

    // Verify the content is JSON (httpbin.org/json returns a JSON object)
    let content_col = batch.column(2);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);
    
    assert!(content.contains("slideshow"), "Content should contain JSON data from httpbin.org/json");
}

/// Test with query parameters
#[tokio::test]
async fn test_http_provider_with_query_params() {
    let base_url = url::Url::parse("https://httpbin.org").expect("valid URL");
    let provider = HttpTableProvider::new(
        base_url,
        Client::new(),
        "json".to_string(),
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query with path and query parameters
    let df = ctx
        .sql("SELECT path, query, content FROM httpbin WHERE path = '/get' AND query = 'test=value'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have exactly one row");

    // Verify the query parameter was sent
    let content_col = batch.column(2);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);
    
    // httpbin.org/get returns the query args in the response
    assert!(content.contains("test"), "Content should contain the query parameter");
    assert!(content.contains("value"), "Content should contain the query parameter value");
}

/// Test scanning without filters (should use base URL)
#[tokio::test]
async fn test_http_provider_without_filters() {
    let base_url = url::Url::parse("https://httpbin.org/json").expect("valid URL");
    let provider = HttpTableProvider::new(
        base_url,
        Client::new(),
        "json".to_string(),
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query without WHERE clause should fetch base URL
    let df = ctx
        .sql("SELECT content FROM httpbin")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have exactly one row");
}

/// Test with base URL that has a path component
#[tokio::test]
async fn test_http_provider_with_base_path() {
    let base_url = url::Url::parse("https://httpbin.org/anything/base").expect("valid URL");
    let provider = HttpTableProvider::new(
        base_url,
        Client::new(),
        "json".to_string(),
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // The filter path should be appended to the base path
    let df = ctx
        .sql("SELECT path, content FROM httpbin WHERE path = '/extra'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    
    let content_col = batch.column(1);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);
    
    // httpbin.org/anything returns the URL in the response
    // Should be: https://httpbin.org/anything/base/extra
    assert!(content.contains("/anything/base/extra"), "Should have appended path to base URL");
}
