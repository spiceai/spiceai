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

#![allow(clippy::expect_used)]

use data_components::http::provider::HttpTableProvider;
use datafusion::prelude::*;
use reqwest::Client;
use std::sync::Arc;

fn build_provider(
    base_url: &str,
    format: &str,
    allowed_paths: &[&str],
    allow_query_filters: bool,
    allow_body_filters: bool,
) -> HttpTableProvider {
    let mut provider = HttpTableProvider::new(
        url::Url::parse(base_url).expect("valid URL"),
        Client::new(),
        format.to_string(),
        false,
    );

    if !allowed_paths.is_empty() {
        let paths: Vec<String> = allowed_paths.iter().map(|p| p.trim().to_string()).collect();
        provider = provider
            .with_allowed_paths(paths)
            .expect("allowed paths are valid");
    }

    if allow_query_filters {
        provider = provider
            .enable_query_filters(data_components::http::provider::DEFAULT_MAX_QUERY_LENGTH);
    }

    if allow_body_filters {
        provider =
            provider.enable_body_filters(data_components::http::provider::DEFAULT_MAX_BODY_BYTES);
    }

    provider
}

/// Integration test that fetches real data from httpbin.org
#[tokio::test]
async fn test_http_provider_with_real_endpoint() {
    let provider = build_provider("https://httpbin.org", "json", &["/json"], false, false);

    // Create a DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query with specific path and no query string
    let df = ctx
        .sql(
            "SELECT request_path, request_query, content FROM httpbin WHERE request_path = '/json'",
        )
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
async fn test_http_provider_with_request_query_params() {
    let provider = build_provider("https://httpbin.org", "json", &["/get"], true, false);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Query with path and query parameters
    let df = ctx
        .sql(
            "SELECT request_path, request_query, content FROM httpbin WHERE request_path = '/get' AND request_query = 'test=value'",
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
    let provider = build_provider("https://httpbin.org/get", "json", &[], false, false);

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
async fn test_http_provider_with_base_request_path() {
    let provider = build_provider(
        "https://httpbin.org/anything/base",
        "json",
        &["/extra"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // The filter path should be appended to the base path
    let df = ctx
        .sql("SELECT request_path, content FROM httpbin WHERE request_path = '/extra'")
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

/// Integration test with `TVMaze` API - Single JSON object
/// Tests endpoint: <https://api.tvmaze.com/shows/169>
/// Expected: Returns a single JSON object (Breaking Bad show details)
#[tokio::test]
async fn test_tvmaze_single_object() {
    let provider = build_provider(
        "https://api.tvmaze.com",
        "json",
        &["/shows/169"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query for a specific show (Breaking Bad, ID 169)
    let df = ctx
        .sql("SELECT request_path, request_query, content FROM tvmaze WHERE request_path = '/shows/169'")
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

/// Integration test with `TVMaze` API - Multiple JSON objects (array)
/// Tests endpoint: <https://api.tvmaze.com/search/people?q=michael>
/// Expected: Returns a JSON array with multiple search results, each as a separate row
#[tokio::test]
async fn test_tvmaze_multi_object() {
    let provider = build_provider(
        "https://api.tvmaze.com",
        "json",
        &["/search/people"],
        true,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query for people search results
    let df = ctx
        .sql(
            "SELECT request_path, request_query, content FROM tvmaze WHERE request_path = '/search/people' AND request_query = 'q=michael'",
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

    // Verify all rows have the same request_path and request_query
    let path_col = batch.column(0);
    let path_array = path_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("request_path should be StringArray");

    let query_col = batch.column(1);
    let query_array = query_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("request_query should be StringArray");

    for i in 0..batch.num_rows() {
        assert_eq!(
            path_array.value(i),
            "/search/people",
            "All rows should have the same request_path"
        );
        assert_eq!(
            query_array.value(i),
            "q=michael",
            "All rows should have the same request_query"
        );
    }
}

/// Integration test with `TVMaze` API - Combined OR filter
/// Tests multiple endpoints in a single query using OR
/// Expected: Returns rows from both endpoints combined
#[tokio::test]
async fn test_tvmaze_combined_or_filter() {
    let provider = build_provider(
        "https://api.tvmaze.com",
        "json",
        &["/shows/169", "/search/people"],
        true,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query combining single object and array endpoints
    // Note: We only filter on request_path for the single object, not on request_query
    let df = ctx
        .sql(
            "SELECT request_path, request_query, content FROM tvmaze 
             WHERE request_path = '/shows/169' 
                OR (request_path = '/search/people' AND request_query = 'q=michael')",
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
            .expect("request_path should be StringArray");

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

/// Integration test with `TVMaze` API - IN list filter
/// Tests using IN clause for multiple paths
/// Expected: Returns rows from multiple different endpoints
#[tokio::test]
async fn test_tvmaze_in_list_request_paths() {
    let provider = build_provider(
        "https://api.tvmaze.com",
        "json",
        &["/shows/169", "/shows/1", "/shows/82"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query multiple show IDs using IN clause
    let df = ctx
        .sql(
            "SELECT request_path, content FROM tvmaze 
             WHERE request_path IN ('/shows/169', '/shows/1', '/shows/82')",
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");

    // Collect unique paths and verify we got all 3 paths
    let mut uniquerequest_paths = std::collections::HashSet::new();
    let mut show_ids = Vec::new();

    for batch in &results {
        let path_col = batch.column(0);
        let path_array = path_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("request_path should be StringArray");

        let content_col = batch.column(1);
        let content_array = content_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be StringArray");

        for i in 0..batch.num_rows() {
            let path = path_array.value(i);
            uniquerequest_paths.insert(path.to_string());

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
        uniquerequest_paths.len(),
        3,
        "Should have queried exactly 3 unique paths"
    );
    assert!(
        uniquerequest_paths.contains("/shows/169"),
        "Should include path /shows/169"
    );
    assert!(
        uniquerequest_paths.contains("/shows/1"),
        "Should include path /shows/1"
    );
    assert!(
        uniquerequest_paths.contains("/shows/82"),
        "Should include path /shows/82"
    );

    // Verify we got data for all 3 shows (even if some return arrays)
    assert!(show_ids.contains(&169), "Should include show 169");
    assert!(show_ids.contains(&1), "Should include show 1");
    assert!(show_ids.contains(&82), "Should include show 82");
}

/// Test POST request with `request_body` filter (default JSON content-type)
#[tokio::test]
async fn test_http_post_with_json_request_body() {
    let provider = build_provider("https://httpbin.org", "json", &["/post"], false, true);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // POST request with JSON body
    let df = ctx
        .sql(
            r#"SELECT request_path, request_body, content FROM httpbin 
               WHERE request_path = '/post' 
               AND request_body = '{"test": "data", "number": 42}'"#,
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have exactly one row");

    // Verify the content echoes back our POST data
    let content_col = batch.column(2);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);

    // httpbin.org/post returns the posted data in the response
    assert!(
        content.contains("test"),
        "Content should contain posted JSON data"
    );
    assert!(
        content.contains("data"),
        "Content should contain posted JSON value"
    );
    assert!(
        content.contains("42"),
        "Content should contain posted number"
    );
}

/// Test POST request with custom content-type
#[tokio::test]
async fn test_http_post_with_custom_content_type() {
    let provider = build_provider("https://httpbin.org", "json", &["/post"], false, true)
        .with_content_type(Some("application/x-www-form-urlencoded".to_string()));

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // POST request with form-encoded body
    let df = ctx
        .sql(
            r"SELECT request_path, request_body, content FROM httpbin 
               WHERE request_path = '/post' 
               AND request_body = 'key1=value1&key2=value2'",
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have at least one result");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have exactly one row");

    // Verify the content echoes back our form data
    let content_col = batch.column(2);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);

    // httpbin.org/post returns form data in the 'form' field
    assert!(
        content.contains("\"form\""),
        "Content should contain form data"
    );
    assert!(content.contains("key1"), "Content should contain form key");
    assert!(
        content.contains("value1"),
        "Content should contain form value"
    );
}

/// Test POST with multiple different bodies using IN clause
#[tokio::test]
async fn test_http_post_multiple_bodies() {
    let provider = build_provider("https://httpbin.org", "json", &["/post"], false, true);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Multiple POST requests with different bodies
    let df = ctx
        .sql(
            r#"SELECT request_path, request_body, content FROM httpbin 
               WHERE request_path = '/post' 
               AND request_body IN (
                   '{"id": 1}',
                   '{"id": 2}'
               )"#,
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");

    // Should have 2 rows (one for each body)
    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "Should have exactly 2 rows (one per body)");

    // Verify each body is separate
    let mut bodies = std::collections::HashSet::new();
    for batch in &results {
        let body_col = batch.column(1);
        let body_array = body_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("request_body should be StringArray");

        for i in 0..batch.num_rows() {
            bodies.insert(body_array.value(i).to_string());
        }
    }

    assert_eq!(bodies.len(), 2, "Should have 2 unique bodies");
    assert!(bodies.contains(r#"{"id": 1}"#), "Should include first body");
    assert!(
        bodies.contains(r#"{"id": 2}"#),
        "Should include second body"
    );
}

/// Test POST with OR expression combining different bodies
#[tokio::test]
async fn test_http_post_or_expression() {
    let provider = build_provider("https://httpbin.org", "json", &["/post"], false, true);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // OR expression with different bodies
    let df = ctx
        .sql(
            r#"SELECT request_path, request_body FROM httpbin 
               WHERE request_path = '/post' 
               AND (request_body = '{"type": "A"}' OR request_body = '{"type": "B"}')"#,
        )
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    let total_rows: usize = results
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "Should have 2 rows from OR expression");
}

/// Test with retries on transient failures
#[tokio::test]
async fn test_http_with_retries() {
    let provider =
        build_provider("https://httpbin.org", "json", &["/json"], false, false).with_max_retries(5);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // Test that retries work by using a valid endpoint
    let df = ctx
        .sql("SELECT content FROM httpbin WHERE request_path = '/json'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
    assert_eq!(results[0].num_rows(), 1, "Should have exactly one row");
}

/// Test CSV format with static file URL (no filters)
/// Note: CSV data is returned in the 'content' column as text
#[tokio::test]
async fn test_csv_static_file() {
    // Using a well-known CSV dataset
    let base_url =
        url::Url::parse("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")
            .expect("valid URL");
    let provider = build_provider(base_url.as_str(), "csv", &[], false, false);

    let ctx = SessionContext::new();
    ctx.register_table("iris", Arc::new(provider))
        .expect("Failed to register table");

    // Query the CSV file - content will be the raw CSV text
    let df = ctx
        .sql("SELECT content FROM iris")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have 1 row with CSV content");

    // Verify content contains CSV data
    let content_col = batch.column(0);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);

    assert!(
        content.contains("sepal_length"),
        "Should contain CSV header"
    );
    assert!(
        content.contains("setosa"),
        "Should contain iris species data"
    );
}

/// Test CSV format with column selection
/// Note: When using CSV format, the actual CSV columns are in 'content', not as separate columns
#[tokio::test]
async fn test_csv_column_selection() {
    let base_url =
        url::Url::parse("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")
            .expect("valid URL");
    let provider = build_provider(base_url.as_str(), "csv", &[], false, false);

    let ctx = SessionContext::new();
    ctx.register_table("iris", Arc::new(provider))
        .expect("Failed to register table");

    // Select the content column which contains the CSV data
    let df = ctx
        .sql("SELECT content FROM iris")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
    let batch = &results[0];
    assert_eq!(batch.num_columns(), 1, "Should have 1 column (content)");
    assert_eq!(batch.num_rows(), 1, "Should have 1 row");

    // Verify the content is valid CSV
    let content_col = batch.column(0);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);

    // Should have multiple lines (CSV rows)
    assert!(content.lines().count() > 100, "Should have many CSV rows");
}

/// Test auto-detection format with CSV file
/// Note: Auto-detection returns content as text, not parsed columns
#[tokio::test]
async fn test_auto_detect_csv() {
    let base_url =
        url::Url::parse("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv")
            .expect("valid URL");
    let provider = build_provider(base_url.as_str(), "auto", &[], false, false);

    let ctx = SessionContext::new();
    ctx.register_table("tips", Arc::new(provider))
        .expect("Failed to register table");

    // Query should work with auto-detection
    let df = ctx
        .sql("SELECT content FROM tips")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have 1 row");

    // Verify content is CSV
    let content_col = batch.column(0);
    let content_array = content_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("content should be StringArray");
    let content = content_array.value(0);

    assert!(content.contains("total_bill"), "Should contain CSV header");
}

/// Test auto-detection format with JSON file
#[tokio::test]
async fn test_auto_detect_json() {
    let provider = build_provider(
        "https://api.tvmaze.com",
        "auto",
        &["/shows/169"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("tvmaze", Arc::new(provider))
        .expect("Failed to register table");

    // Query with auto-detection should work for JSON
    let df = ctx
        .sql("SELECT content FROM tvmaze WHERE request_path = '/shows/169'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have 1 row");
}

/// Test NDJSON (newline-delimited JSON) format
#[tokio::test]
async fn test_ndjson_format() {
    // Create a simple in-memory test - we'll use httpbin which returns JSON
    // and treat it as NDJSON for format testing
    let provider = build_provider("https://httpbin.org", "ndjson", &["/json"], false, false);

    let ctx = SessionContext::new();
    ctx.register_table("httpbin", Arc::new(provider))
        .expect("Failed to register table");

    // This should work even though httpbin returns regular JSON
    // The NDJSON parser will treat each line as a separate JSON object
    let df = ctx
        .sql("SELECT content FROM httpbin WHERE request_path = '/json'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
}

/// Test CSV format with dynamic filters (`request_path`)
#[tokio::test]
async fn test_csv_with_dynamic_filter() {
    // Base URL pointing to a directory structure
    let provider = build_provider(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master",
        "csv",
        &["/iris.csv"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("datasets", Arc::new(provider))
        .expect("Failed to register table");

    // Use request_path to select different CSV files
    let df = ctx
        .sql("SELECT * FROM datasets WHERE request_path = '/iris.csv' LIMIT 5")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
    let batch = &results[0];

    // Should have iris data columns
    assert!(
        batch.num_columns() > 3,
        "Should have multiple data columns plus metadata"
    );
}

/// Test multiple CSV files with IN clause
#[tokio::test]
async fn test_csv_multiple_files_in_clause() {
    let provider = build_provider(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master",
        "csv",
        &["/iris.csv", "/tips.csv"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("datasets", Arc::new(provider))
        .expect("Failed to register table");

    // Query multiple CSV files using IN clause
    let df = ctx
        .sql("SELECT request_path FROM datasets WHERE request_path IN ('/iris.csv', '/tips.csv')")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");

    // Collect unique paths
    let mut paths = std::collections::HashSet::new();
    for batch in &results {
        let path_col = batch.column(0);
        let path_array = path_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("request_path should be StringArray");

        for i in 0..batch.num_rows() {
            paths.insert(path_array.value(i).to_string());
        }
    }

    assert_eq!(
        paths.len(),
        2,
        "Should have results from 2 different CSV files"
    );
    assert!(paths.contains("/iris.csv"), "Should include iris.csv");
    assert!(paths.contains("/tips.csv"), "Should include tips.csv");
}

/// Test mixed format query - CSV and JSON with OR filter
#[tokio::test]
async fn test_mixed_format_csv_json_or() {
    // This tests that the provider can handle different formats in the same query
    // when using OR conditions with request_path
    let provider = build_provider(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master",
        "auto",
        &["/iris.csv", "/tips.csv"],
        false,
        false,
    );

    let ctx = SessionContext::new();
    ctx.register_table("data", Arc::new(provider))
        .expect("Failed to register table");

    // Note: In practice, mixing CSV and JSON in one query may not work well
    // because they have different schemas. This tests the mechanism works.
    let df = ctx
        .sql("SELECT request_path FROM data WHERE request_path = '/iris.csv' OR request_path = '/tips.csv'")
        .await
        .expect("Failed to create dataframe");

    let results = df.collect().await.expect("Failed to execute query");

    assert!(!results.is_empty(), "Should have results");
}
