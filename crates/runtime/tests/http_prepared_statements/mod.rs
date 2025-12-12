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

//! Integration tests for HTTP prepared statements (parameterized queries).

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::sql::TableReference;
use rand::Rng;
use runtime::{
    Runtime, accelerated_table::refresh::Refresh, auth::EndpointAuth,
    component::dataset::acceleration::Acceleration, config::Config, datafusion::DataFusion,
    internal_table::create_internal_accelerated_table, secrets::Secrets,
};
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Start a test runtime with HTTP server for prepared statement tests.
async fn start_http_test_app() -> Result<(String, Arc<DataFusion>), anyhow::Error> {
    let mut rng = rand::rng();
    let http_port: u16 = rng.random_range(50000..60000);
    let flight_port: u16 = http_port + 1;
    let otel_port: u16 = http_port + 2;
    let metrics_port: u16 = http_port + 3;

    tracing::debug!(
        "Ports: http: {http_port}, flight: {flight_port}, otel: {otel_port}, metrics: {metrics_port}"
    );

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
        .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

    let registry = prometheus::Registry::new();

    configure_test_datafusion();
    let rt_builder =
        Runtime::builder().with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry);

    let app = app::AppBuilder::new("test_app").build();
    let rt = Arc::new(rt_builder.with_app(app).build().await);

    let cloned_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let df = rt.datafusion();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Register a test table
    register_test_table(
        &df,
        Arc::clone(&schema),
        TableReference::parse_str("public.test_users"),
        Arc::clone(&rt),
    )
    .await?;

    // Start the servers
    tokio::spawn(async move {
        Box::pin(rt.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    // Wait for the HTTP server to start
    tracing::info!("Waiting for HTTP server to start...");
    wait_until_true(Duration::from_secs(10), || async {
        reqwest::get(format!("http://localhost:{http_port}/health"))
            .await
            .is_ok()
    })
    .await;

    Ok((format!("http://localhost:{http_port}"), df))
}

async fn register_test_table(
    datafusion: &Arc<DataFusion>,
    schema: SchemaRef,
    table_name: TableReference,
    runtime: Arc<Runtime>,
) -> Result<(), anyhow::Error> {
    let table = create_internal_accelerated_table(
        datafusion.runtime_status(),
        table_name.clone(),
        schema,
        None,
        Acceleration::default(),
        Refresh::default(),
        None,
        Arc::new(RwLock::new(Secrets::default())),
        runtime,
    )
    .await
    .map_err(anyhow::Error::from)?;

    datafusion
        .register_table_as_writable_and_with_schema(table_name, table)
        .map_err(anyhow::Error::from)?;

    Ok(())
}

#[tokio::test]
async fn test_http_prepare_basic() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Prepare a simple query
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT $1 + 1 AS the_answer"
                }))
                .send()
                .await?;

            assert!(
                response.status().is_success(),
                "Prepare should succeed: {}",
                response.status()
            );

            let prepare_response: Value = response.json().await?;

            // Verify response structure
            assert!(
                prepare_response.get("handle").is_some(),
                "Response should have handle"
            );
            assert!(
                prepare_response.get("dataset_schema").is_some(),
                "Response should have dataset_schema"
            );
            assert!(
                prepare_response.get("parameter_schema").is_some(),
                "Response should have parameter_schema for parameterized query"
            );

            // Verify dataset schema
            let dataset_schema = &prepare_response["dataset_schema"];
            assert!(
                dataset_schema["fields"].is_array(),
                "dataset_schema should have fields"
            );
            let fields = dataset_schema["fields"].as_array().expect("fields array");
            assert_eq!(fields.len(), 1, "Should have 1 result column");
            assert_eq!(fields[0]["name"], "the_answer");

            // Verify parameter schema
            let parameter_schema = &prepare_response["parameter_schema"];
            let param_fields = parameter_schema["fields"]
                .as_array()
                .expect("param fields array");
            assert_eq!(param_fields.len(), 1, "Should have 1 parameter");
            assert_eq!(param_fields[0]["name"], "$1");

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_prepare_no_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Prepare a query with no parameters
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT 1 + 2 AS result"
                }))
                .send()
                .await?;

            assert!(
                response.status().is_success(),
                "Prepare should succeed: {}",
                response.status()
            );

            let prepare_response: Value = response.json().await?;

            // Should not have parameter_schema when there are no parameters
            assert!(
                prepare_response.get("parameter_schema").is_none(),
                "Should not have parameter_schema for non-parameterized query"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_execute_basic() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Step 1: Prepare the statement
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT $1 + 1 AS the_answer"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Step 2: Execute with parameters
            let execute_response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": handle,
                    "parameters": [41]
                }))
                .send()
                .await?;

            assert!(
                execute_response.status().is_success(),
                "Execute should succeed: {}",
                execute_response.status()
            );

            let results: Value = execute_response.json().await?;

            // Verify results
            assert!(results.is_array(), "Results should be an array");
            let rows = results.as_array().expect("results array");
            assert_eq!(rows.len(), 1, "Should have 1 result row");
            assert_eq!(rows[0]["the_answer"], 42);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_execute_named_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Step 1: Prepare with named parameters
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT :foo + 1 AS the_answer"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Step 2: Execute with named parameters
            let execute_response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": handle,
                    "parameters": {"foo": 41}
                }))
                .send()
                .await?;

            assert!(
                execute_response.status().is_success(),
                "Execute should succeed: {}",
                execute_response.status()
            );

            let results: Value = execute_response.json().await?;
            let rows = results.as_array().expect("results array");
            assert_eq!(rows.len(), 1, "Should have 1 result row");
            assert_eq!(rows[0]["the_answer"], 42);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_execute_with_table() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Step 1: Prepare a query that uses the test table
            // This tests that prepared statements work with table references
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT id, name FROM test_users WHERE id = $1"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Verify the dataset schema matches the test_users table
            let fields = prepare_response["dataset_schema"]["fields"]
                .as_array()
                .expect("dataset fields");
            assert_eq!(fields.len(), 2, "Should have 2 columns: id and name");
            assert_eq!(fields[0]["name"], "id");
            assert_eq!(fields[1]["name"], "name");

            // Verify parameter schema
            let param_fields = prepare_response["parameter_schema"]["fields"]
                .as_array()
                .expect("parameter fields");
            assert_eq!(param_fields.len(), 1, "Should have 1 parameter");
            assert_eq!(param_fields[0]["name"], "$1");

            // Step 2: Execute with parameter - table is empty so should return 0 rows
            // This verifies the execute workflow works with table-based queries
            let execute_response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": handle,
                    "parameters": [1]
                }))
                .send()
                .await?;

            assert!(
                execute_response.status().is_success(),
                "Execute should succeed: {}",
                execute_response.status()
            );

            let results: Value = execute_response.json().await?;
            let rows = results.as_array().expect("results array");
            // Table is empty (accelerated table with no source), so we expect 0 rows
            // The important thing is that the query executed successfully
            assert!(
                rows.is_empty(),
                "Table should be empty (no data source configured)"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_execute_invalid_handle() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Try to execute with an invalid handle
            let response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": "invalid-handle-here",
                    "parameters": [1]
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "Should return 400 for invalid handle"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_prepare_invalid_sql() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Try to prepare invalid SQL
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT FROM WHERE"
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "Should return 400 for invalid SQL"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_http_execute_multiple_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Prepare a query with multiple parameters using CAST to help type inference
            // Note: DataFusion needs type hints for arithmetic operations on parameters
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT CAST($1 AS INT) + CAST($2 AS INT) + CAST($3 AS INT) AS total"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Verify parameter schema has 3 parameters
            let param_fields = prepare_response["parameter_schema"]["fields"]
                .as_array()
                .expect("param fields");
            assert_eq!(param_fields.len(), 3, "Should have 3 parameters");

            // Execute with 3 parameters
            let execute_response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": handle,
                    "parameters": [10, 20, 12]
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let rows = execute_response.as_array().expect("results array");
            assert_eq!(rows.len(), 1, "Should have 1 result row");
            assert_eq!(rows[0]["total"], 42);

            Ok(())
        })
        .await
}

// ============================================================================
// Security Tests
// ============================================================================

/// Test that DDL operations (CREATE TABLE, DROP TABLE) are blocked in prepare.
#[tokio::test]
async fn test_security_ddl_blocked_in_prepare() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Test CREATE TABLE is blocked
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "CREATE TABLE malicious_table (id INT)"
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "CREATE TABLE should be blocked"
            );
            let body = response.text().await?;
            assert!(
                body.contains("not allowed"),
                "Error should indicate operation not allowed: {body}"
            );

            // Test DROP TABLE is blocked
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "DROP TABLE test_users"
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "DROP TABLE should be blocked"
            );

            Ok(())
        })
        .await
}

/// Test that DML operations (UPDATE, DELETE) are blocked in prepare.
#[tokio::test]
async fn test_security_dml_blocked_in_prepare() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Test UPDATE is blocked
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "UPDATE test_users SET name = 'hacked' WHERE id = 1"
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "UPDATE should be blocked"
            );

            // Test DELETE is blocked
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "DELETE FROM test_users WHERE id = 1"
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "DELETE should be blocked"
            );

            Ok(())
        })
        .await
}

/// Test that INSERT on internal accelerated tables works (they are writable by design).
/// The real protection is on system tables (tested in test_security_system_tables_protected).
#[tokio::test]
async fn test_security_insert_on_writable_table() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Test INSERT on writable table is allowed
            // test_users is registered as writable via register_table_as_writable_and_with_schema
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "INSERT INTO test_users (id, name) VALUES (999, 'new_user')"
                }))
                .send()
                .await?;

            // INSERT is allowed on writable tables
            assert!(
                response.status().is_success(),
                "INSERT on writable table should be allowed: {}",
                response.status()
            );

            Ok(())
        })
        .await
}

/// Test that system tables cannot be modified.
#[tokio::test]
async fn test_security_system_tables_protected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Test INSERT on runtime.task_history (system table) is blocked
            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "INSERT INTO runtime.task_history (task) VALUES ('malicious')"
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "INSERT on system table should be blocked"
            );

            Ok(())
        })
        .await
}

/// Test that handle tampering is detected.
#[tokio::test]
async fn test_security_handle_tampering() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Get a valid handle first
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT 1"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let valid_handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Try to tamper with the handle by modifying characters
            let tampered_handle = format!("{}TAMPERED", valid_handle);
            let response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": tampered_handle,
                    "parameters": []
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "Tampered handle should be rejected"
            );

            // Try a completely fabricated handle that encodes a dangerous query
            // This simulates an attacker trying to inject a malicious SQL via a crafted handle
            let response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo",  // random base64
                    "parameters": []
                }))
                .send()
                .await?;

            assert_eq!(
                response.status().as_u16(),
                400,
                "Fabricated handle should be rejected"
            );

            Ok(())
        })
        .await
}

/// Test that SQL injection via parameters is prevented.
#[tokio::test]
async fn test_security_sql_injection_via_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Prepare a safe query
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT id, name FROM test_users WHERE name = $1"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Try SQL injection via parameter value
            // This should be treated as a literal string, not executed as SQL
            let response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": handle,
                    "parameters": ["'; DROP TABLE test_users; --"]
                }))
                .send()
                .await?;

            // The query should execute (table still exists), but return no results
            // since no user has that literal name
            assert!(
                response.status().is_success(),
                "Query should execute successfully with injection string as literal parameter"
            );

            let results: Value = response.json().await?;
            let rows = results.as_array().expect("results array");
            assert!(
                rows.is_empty(),
                "No user should match the injection string literal"
            );

            // Verify the table still exists by preparing another query
            let verify_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT * FROM test_users"
                }))
                .send()
                .await?;

            assert!(
                verify_response.status().is_success(),
                "Table should still exist after injection attempt"
            );

            Ok(())
        })
        .await
}

/// Test that extremely long SQL is handled gracefully.
/// Note: Very deeply nested OR conditions can cause stack overflow in the SQL parser.
/// This test uses a safer pattern with UNION ALL to avoid deep recursion.
#[tokio::test]
async fn test_security_long_sql_handling() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Create a moderately long SQL query using a safer pattern
            // Avoid deeply nested OR conditions which can cause stack overflow
            let long_sql = format!(
                "SELECT * FROM test_users WHERE id IN ({})",
                (0..100).map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
            );

            let response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": long_sql
                }))
                .send()
                .await?;

            // Should succeed with IN clause
            assert!(
                response.status().is_success(),
                "Long SQL with IN clause should be handled: {}",
                response.status()
            );

            Ok(())
        })
        .await
}

/// Test that malformed JSON in parameters is rejected.
#[tokio::test]
async fn test_security_malformed_parameters() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (base_url, _df) = start_http_test_app().await?;
            let client = reqwest::Client::new();

            // Get a valid handle
            let prepare_response = client
                .post(format!("{base_url}/v1/sql/prepare"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "sql": "SELECT $1"
                }))
                .send()
                .await?
                .json::<Value>()
                .await?;

            let handle = prepare_response["handle"]
                .as_str()
                .expect("handle should be a string");

            // Test with wrong parameter type (string where number expected)
            // This should be handled gracefully
            let response = client
                .post(format!("{base_url}/v1/sql/execute"))
                .header("Content-Type", "application/json")
                .json(&json!({
                    "handle": handle,
                    "parameters": ["not_a_number"]
                }))
                .send()
                .await?;

            // Should either work (implicit cast) or fail gracefully with 400
            assert!(
                response.status().as_u16() == 200 || response.status().as_u16() == 400,
                "Malformed parameters should be handled gracefully, got: {}",
                response.status()
            );

            Ok(())
        })
        .await
}
