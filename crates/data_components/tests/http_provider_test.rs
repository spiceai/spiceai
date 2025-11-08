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

use data_components::http::provider::HttpTableProvider;
use datafusion::prelude::*;
use reqwest::Client;
use std::sync::Arc;

/// Integration test that fetches real data from httpbin.org
#[tokio::test]
async fn test_http_provider_with_real_endpoint() {
    let base_url = url::Url::parse("https://httpbin.org").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    // Create a DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query with specific path and no query string
    let df = ctx
        .sql("SELECT _path, _query, content FROM httpbin WHERE _path = '/json'")
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

    assert!(
        content.contains("slideshow"),
        "Content should contain JSON data from httpbin.org/json"
    );
}

/// Test with query parameters
#[tokio::test]
async fn test_http_provider_with_query_params() {
    let base_url = url::Url::parse("https://httpbin.org").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query with path and query parameters
    let df = ctx
        .sql(
            "SELECT _path, _query, content FROM httpbin WHERE _path = '/get' AND _query = 'test=value'",
        )
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
    assert!(
        content.contains("test"),
        "Content should contain the query parameter"
    );
    assert!(
        content.contains("value"),
        "Content should contain the query parameter value"
    );
}

/// Test scanning without filters (should use base URL)
#[tokio::test]
async fn test_http_provider_without_filters() {
    let base_url = url::Url::parse("https://httpbin.org/get").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

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
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // The filter path should be appended to the base path
    let df = ctx
        .sql("SELECT _path, content FROM httpbin WHERE _path = '/extra'")
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
    assert!(
        content.contains("/anything/base/extra"),
        "Should have appended path to base URL"
    );
}

/// Integration test with TVMaze API - Single JSON object
/// Tests endpoint: https://api.tvmaze.com/shows/169
/// Expected: Returns a single JSON object (Breaking Bad show details)
#[tokio::test]
async fn test_tvmaze_single_object() {
    let base_url = url::Url::parse("https://api.tvmaze.com").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query for a specific show (Breaking Bad, ID 169)
    let df = ctx
        .sql("SELECT _path, _query, content FROM tvmaze WHERE _path = '/shows/169'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "Single JSON object should return exactly one row"
    );

    // Verify the content is a JSON object with show details
    let content_col = batch.column(2);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);

    assert!(
        content.contains("\"id\":169"),
        "Content should contain show ID 169"
    );
    assert!(
        content.contains("Breaking Bad"),
        "Content should contain show name 'Breaking Bad'"
    );
    assert!(
        content.contains("\"type\":\"Scripted\""),
        "Content should be a structured JSON object"
    );
}

/// Integration test with TVMaze API - Multiple JSON objects (array)
/// Tests endpoint: https://api.tvmaze.com/search/people?q=michael
/// Expected: Returns a JSON array with multiple search results, each as a separate row
#[tokio::test]
async fn test_tvmaze_multi_object() {
    let base_url = url::Url::parse("https://api.tvmaze.com").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query for people search results
    let df = ctx
        .sql(
            "SELECT _path, _query, content FROM tvmaze WHERE _path = '/search/people' AND _query = 'q=michael'",
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];

    // JSON array should be expanded into multiple rows
    assert!(
        batch.num_rows() > 1,
        "JSON array should expand to multiple rows, got {}",
        batch.num_rows()
    );

    // Verify the content structure - each row should be a search result object
    let content_col = batch.column(2);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");

    // Check first result
    let first_result = content_array.value(0);
    assert!(
        first_result.contains("\"score\""),
        "Each result should have a score field"
    );
    assert!(
        first_result.contains("\"person\""),
        "Each result should have a person object"
    );

    // Verify all rows have the same _path and _query
    let path_col = batch.column(0);
    let path_array = path_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("_path should be StringArray");

    let query_col = batch.column(1);
    let query_array = query_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("_query should be StringArray");

    for i in 0..batch.num_rows() {
        assert_eq!(
            path_array.value(i),
            "/search/people",
            "All rows should have the same _path"
        );
        assert_eq!(
            query_array.value(i),
            "q=michael",
            "All rows should have the same _query"
        );
    }
}

/// Integration test with TVMaze API - Combined OR filter
/// Tests multiple endpoints in a single query using OR
/// Expected: Returns rows from both endpoints combined
#[tokio::test]
async fn test_tvmaze_combined_or_filter() {
    let base_url = url::Url::parse("https://api.tvmaze.com").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query combining single object and array endpoints
    // Note: We only filter on _path for the single object, not on _query
    let df = ctx
        .sql(
            "SELECT _path, _query, content FROM tvmaze 
             WHERE _path = '/shows/169' 
                OR (_path = '/search/people' AND _query = 'q=michael')",
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");

    // Collect paths to verify we got both endpoints
    let mut has_show = false;
    let mut has_search = false;

    for batch in &results {
        let path_col = batch.column(0);
        let path_array = path_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("_path should be StringArray");

        for i in 0..batch.num_rows() {
            match path_array.value(i) {
                "/shows/169" => has_show = true,
                "/search/people" => has_search = true,
                _ => panic!("Unexpected path value: {}", path_array.value(i)),
            }
        }
    }

    assert!(has_show, "Should have results from /shows/169 endpoint");
    assert!(
        has_search,
        "Should have results from /search/people endpoint"
    );
}

/// Integration test with TVMaze API - IN list filter
/// Tests using IN clause for multiple paths
/// Expected: Returns rows from multiple different endpoints
#[tokio::test]
async fn test_tvmaze_in_list_paths() {
    let base_url = url::Url::parse("https://api.tvmaze.com").expect("valid URL");
    let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query multiple show IDs using IN clause
    let df = ctx
        .sql(
            "SELECT _path, content FROM tvmaze 
             WHERE _path IN ('/shows/169', '/shows/1', '/shows/82')",
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");

    // Collect unique paths and verify we got all 3 paths
    let mut unique_paths = std::collections::HashSet::new();
    let mut show_ids = Vec::new();

    for batch in &results {
        let path_col = batch.column(0);
        let path_array = path_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("_path should be StringArray");

        let content_col = batch.column(1);
        let content_array = content_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be StringArray");

        for i in 0..batch.num_rows() {
            let path = path_array.value(i);
            unique_paths.insert(path.to_string());

            let content = content_array.value(i);
            // Extract ID from JSON (basic check)
            if content.contains("\"id\":169") {
                show_ids.push(169);
            } else if content.contains("\"id\":1") {
                show_ids.push(1);
            } else if content.contains("\"id\":82") {
                show_ids.push(82);
            }
        }
    }

    assert_eq!(
        unique_paths.len(),
        3,
        "Should have queried exactly 3 unique paths"
    );
    assert!(
        unique_paths.contains("/shows/169"),
        "Should include path /shows/169"
    );
    assert!(
        unique_paths.contains("/shows/1"),
        "Should include path /shows/1"
    );
    assert!(
        unique_paths.contains("/shows/82"),
        "Should include path /shows/82"
    );

    // Verify we got data for all 3 shows (even if some return arrays)
    assert!(show_ids.contains(&169), "Should include show 169");
    assert!(show_ids.contains(&1), "Should include show 1");
    assert!(show_ids.contains(&82), "Should include show 82");
}
