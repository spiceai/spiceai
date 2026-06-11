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

//! Integration tests for `FlightSQL` `CommandStatementUpdate` (`DoPut`) handling of
//! DML statements (DELETE, UPDATE).
//!
//! These tests use a Cayenne catalog so DELETE and UPDATE are fully supported
//! end-to-end through the runtime's `Query::run()` DML interception path.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use rand::RngExt as _;
use runtime::Runtime;
use runtime::auth::EndpointAuth;
use runtime::config::Config;
use runtime_auth::FlightBasicAuth;
use runtime_auth::api_key::ApiKeyAuth;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::runtime::ApiKey;
use spicepod::param::Params;
use tokio::time::sleep;
use tonic::transport::Channel;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Creates a Cayenne catalog pointed at temp dirs with `read_write_create` access.
fn make_cayenne_catalog(catalog_name: &str, data_dir: &str, metadata_dir: &str) -> Catalog {
    let mut catalog = Catalog::new("cayenne".to_string(), catalog_name.to_string())
        .with_access(AccessMode::ReadWriteCreate);
    catalog.params = Some(Params::from_string_map(
        vec![
            ("cayenne_data_dir".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>(),
    ));
    catalog
}

/// Run SQL against the runtime directly (for setup operations).
async fn run_sql(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query '{sql}' failed: {e}"))?;

    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("collecting results for '{sql}' failed: {e}"))
}

/// Start a test runtime with a Cayenne catalog and Flight/HTTP servers.
/// Returns a connected Flight channel and the runtime for direct setup queries.
async fn start_cayenne_flight_app(
    catalog: Catalog,
) -> Result<(Channel, Arc<Runtime>), anyhow::Error> {
    let mut rng = rand::rng();
    let http_port: u16 = rng.random_range(50000..60000);
    let flight_port: u16 = http_port + 1;
    let metrics_port: u16 = http_port + 2;

    register_test_connectors().await;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
        .with_caching_disabled();

    let registry = prometheus::Registry::new();

    configure_test_datafusion();
    let rt_builder =
        Runtime::builder().with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry);

    let app = app::AppBuilder::new("flightsql_statement_update_test")
        .with_catalog(catalog)
        .build();

    let rt = Arc::new(
        rt_builder
            .with_app(app)
            .with_runtime_config(api_config.clone())
            .build()
            .await,
    );

    let cloned_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = cloned_rt.load_components() => {}
    };

    runtime_ready_check(&rt).await;

    // Auth: API key with read-write access
    let auth_provider = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("test-key:rw")]))
        as Arc<dyn FlightBasicAuth + Send + Sync>;
    let auth = EndpointAuth::default().with_flight_basic_auth(auth_provider);

    // Start servers
    let rt_for_server = Arc::clone(&rt);
    tokio::spawn(async move {
        let _ = Box::pin(rt_for_server.start_servers(api_config, None, auth)).await;
    });

    // Wait for HTTP readiness
    wait_until_true(Duration::from_secs(10), || async {
        reqwest::get(format!("http://localhost:{http_port}/health"))
            .await
            .is_ok()
    })
    .await;

    // Connect Flight channel
    let start_time = std::time::Instant::now();
    let channel = loop {
        if start_time.elapsed() > Duration::from_secs(30) {
            return Err(anyhow::anyhow!("Flight server not ready within 30 seconds"));
        }
        match Channel::from_shared(format!("http://localhost:{flight_port}"))
            .map_err(anyhow::Error::from)?
            .connect()
            .await
        {
            Ok(channel) => break channel,
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    };

    Ok((channel, rt))
}

/// Create an authenticated `FlightSqlServiceClient` via handshake.
async fn create_flightsql_client(
    channel: Channel,
    api_key: &str,
) -> Result<FlightSqlServiceClient<Channel>, anyhow::Error> {
    let mut client = FlightSqlServiceClient::new(channel);
    client.handshake("", api_key).await?;
    Ok(client)
}

/// Execute a SELECT via `FlightSQL` (prepared statement → `DoGet`) and collect results.
async fn flightsql_query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut stmt = client.prepare(sql.to_string(), None).await?;
    let flight_info = stmt.execute().await?;
    let mut batches = Vec::new();
    for endpoint in flight_info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            let stream = client.do_get(ticket).await?;
            let endpoint_batches: Vec<RecordBatch> = stream.try_collect().await?;
            batches.extend(endpoint_batches);
        }
    }
    Ok(batches)
}

/// Full lifecycle: setup table via runtime, then test INSERT, DELETE, UPDATE
/// through `FlightSQL` `execute_update` (`CommandStatementUpdate` / `DoPut` path).
#[tokio::test]
#[ignore = "Requires non-distributed Cayenne catalog support: https://github.com/spiceai/spiceai/issues/9942"]
async fn test_flightsql_execute_update_delete_and_update() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            let metadata_dir = temp_dir.path().join("metadata");

            let catalog = make_cayenne_catalog(
                "test_cat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let (channel, rt) = start_cayenne_flight_app(catalog).await?;

            // -----------------------------------------------------------------
            // Setup: CREATE SCHEMA + TABLE + INSERT via runtime (not FlightSQL)
            // to keep the test focused on testing DML via execute_update.
            // -----------------------------------------------------------------
            run_sql(&rt, "CREATE SCHEMA test_cat.myschema")
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

            run_sql(
                &rt,
                "CREATE TABLE test_cat.myschema.items (
                    id BIGINT NOT NULL,
                    name VARCHAR NOT NULL,
                    price BIGINT NOT NULL
                )",
            )
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

            run_sql(
                &rt,
                "INSERT INTO test_cat.myschema.items VALUES
                    (1, 'apple',  100),
                    (2, 'banana', 200),
                    (3, 'cherry', 300)",
            )
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

            let mut client = create_flightsql_client(channel, "test-key").await?;

            // Verify initial data via FlightSQL SELECT (DoGet path)
            let batches = flightsql_query(
                &mut client,
                "SELECT id, name, price FROM test_cat.myschema.items ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+--------+-------+",
                    "| id | name   | price |",
                    "+----+--------+-------+",
                    "| 1  | apple  | 100   |",
                    "| 2  | banana | 200   |",
                    "| 3  | cherry | 300   |",
                    "+----+--------+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // DELETE via execute_update (the bug path: CommandStatementUpdate)
            // Before the fix, this returned "Unsupported plan: Dml".
            // -----------------------------------------------------------------
            let affected = client
                .execute_update(
                    "DELETE FROM test_cat.myschema.items WHERE id = 2".to_string(),
                    None,
                )
                .await?;
            assert_eq!(affected, 1, "DELETE should report 1 affected row");

            // Verify row is gone
            let batches = flightsql_query(
                &mut client,
                "SELECT id, name, price FROM test_cat.myschema.items ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+--------+-------+",
                    "| id | name   | price |",
                    "+----+--------+-------+",
                    "| 1  | apple  | 100   |",
                    "| 3  | cherry | 300   |",
                    "+----+--------+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // UPDATE via execute_update (the bug path: CommandStatementUpdate)
            // Before the fix, this returned "Unsupported plan: Dml".
            // -----------------------------------------------------------------
            let affected = client
                .execute_update(
                    "UPDATE test_cat.myschema.items SET price = 999 WHERE id = 1".to_string(),
                    None,
                )
                .await?;
            assert_eq!(affected, 1, "UPDATE should report 1 affected row");

            // Verify the update took effect
            let batches = flightsql_query(
                &mut client,
                "SELECT id, name, price FROM test_cat.myschema.items ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+--------+-------+",
                    "| id | name   | price |",
                    "+----+--------+-------+",
                    "| 1  | apple  | 999   |",
                    "| 3  | cherry | 300   |",
                    "+----+--------+-------+",
                ],
                &batches
            );

            // -----------------------------------------------------------------
            // DELETE multiple rows
            // -----------------------------------------------------------------
            let affected = client
                .execute_update(
                    "DELETE FROM test_cat.myschema.items WHERE price < 500".to_string(),
                    None,
                )
                .await?;
            assert_eq!(affected, 1, "DELETE should report 1 affected row (cherry)");

            let batches = flightsql_query(
                &mut client,
                "SELECT id, name, price FROM test_cat.myschema.items ORDER BY id",
            )
            .await?;
            assert_batches_eq!(
                &[
                    "+----+-------+-------+",
                    "| id | name  | price |",
                    "+----+-------+-------+",
                    "| 1  | apple | 999   |",
                    "+----+-------+-------+",
                ],
                &batches
            );

            Ok(())
        })
        .await
}

/// Verify that `execute_update` requires write-level authentication.
#[tokio::test]
#[ignore = "Requires non-distributed Cayenne catalog support: https://github.com/spiceai/spiceai/issues/9942"]
async fn test_flightsql_execute_update_requires_auth() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            let metadata_dir = temp_dir.path().join("metadata");

            let catalog = make_cayenne_catalog(
                "test_cat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );

            let (channel, _rt) = start_cayenne_flight_app(catalog).await?;

            // Client with NO auth — no handshake performed
            let mut no_auth_client = FlightSqlServiceClient::new(channel.clone());
            let result = no_auth_client
                .execute_update("DELETE FROM test_cat.foo WHERE id = 1".to_string(), None)
                .await;
            let err = result
                .expect_err("execute_update without auth should fail")
                .to_string();
            // The tonic Status is wrapped in ArrowError::IpcError(format!("{status:?}")),
            // so we check for the gRPC status code name in the debug representation.
            assert!(
                err.contains("Unauthenticated"),
                "Expected Unauthenticated gRPC status, got: {err}"
            );

            Ok(())
        })
        .await
}
