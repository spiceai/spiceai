/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use futures::TryStreamExt;
use rand::Rng;
use runtime::{Runtime, datafusion::query::QueryBuilder};
use runtime::{auth::EndpointAuth, config::Config};
use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
use spicepod::component::{management::Management, runtime::ApiKey};

use crate::{
    init_tracing,
    utils::{init_tracing_with_task_history, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// This test verifies that when management is enabled,
/// events (such as query executions) are exported from the source app
/// to the remote endpoint configured by the '`data_endpoint`' parameter.
#[tokio::test]
async fn management_data_export() -> Result<(), anyhow::Error> {
    let trace_levels =
        Some("runtime::flight::do_put=debug,runtime::management=trace,integration=debug,info");

    let _tracing = init_tracing(trace_levels);

    test_request_context()
        .scope(async {

            let (data_endpoint_rt, data_endpoint) = create_data_export_endpoint().await?;

            let management = Management {
                enabled: true,
                api_key: "auth_key_1".to_string(), // Must match the one used in the data export endpoint
                params: vec![("data_endpoint".to_string(), data_endpoint)]
                    .into_iter()
                    .collect(),
            };

            let app = AppBuilder::new("management_test_app")
                .with_management(management)
                .build();

            let rt =Arc::new(Runtime::builder()
                .with_app(app)
                .build()
                .await);

            Arc::clone(&rt).load_components().await;

            // Verify sink table exist after components are loaded.
            if !rt.datafusion().table_exists("scp.task_history".into()) {
                return Err(anyhow::anyhow!("There is no 'scp.task_history' table created"));
            }

            // Helper usage is required to activate events write into task history table
            let (_tracing, trace_provider) = init_tracing_with_task_history(trace_levels, &rt);

            // Simulate few events that will be exported (both successful and failed)
            let _ = execute_query(&rt, "SELECT 12345 as test_event").await?;
            let _ = execute_query(&rt, "SELECT invalid_query as test_event").await;

            // Ensure local events are flushed
            let _ = trace_provider.force_flush();
            // Add delay to ensure flushed events are propogated (data export is done every 5 seconds)
            tracing::info!("Waiting 7s for events to be exported...");
            tokio::time::sleep(Duration::from_secs(7)).await;

            // Query server app to ensure data is exported
            let exported_events = execute_query(&data_endpoint_rt, "SELECT input, captured_output, error_message FROM runtime.task_history where input like '%test_event%' order by input ").await?;
            insta::assert_snapshot!("periodic_export", pretty_format_batches(&exported_events)?);

            // Verify final export during shutdown
            let _ = execute_query(&rt, "SELECT 6789 as test_event").await;
            let _ = trace_provider.force_flush();
            rt.shutdown().await;
            let exported_events = execute_query(&data_endpoint_rt, "SELECT input, captured_output, error_message FROM runtime.task_history where input like '%test_event%' order by input ").await?;
            insta::assert_snapshot!("shutdown_export", pretty_format_batches(&exported_events)?);

            Ok(())
        })
        .await
}

async fn create_data_export_endpoint() -> Result<(Arc<Runtime>, String), anyhow::Error> {
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

    let app = AppBuilder::new("management_sink_app").build();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    // Load components to initialize 'spice.runtime.task_history'
    Arc::clone(&rt).load_components().await;

    // Start the servers
    let api_key_auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("auth_key_1:rw")]))
        as Arc<dyn FlightBasicAuth + Send + Sync>;

    let cloned_rt = Arc::clone(&rt);

    tokio::spawn(async move {
        Box::pin(Arc::clone(&cloned_rt).start_servers(
            api_config,
            None,
            EndpointAuth::default().with_flight_basic_auth(api_key_auth),
        ))
        .await
    });

    tracing::info!("Waiting for servers to start...");
    wait_until_true(Duration::from_secs(10), || async {
        reqwest::get(format!("http://localhost:{http_port}/health"))
            .await
            .is_ok()
    })
    .await;

    Ok((Arc::clone(&rt), format!("http://localhost:{flight_port}")))
}

async fn execute_query(rt: &Runtime, query: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let query = QueryBuilder::new(query, rt.datafusion()).build();

    let query_result = query
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute query: {e}"))?;

    query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute query: {e}"))
}
