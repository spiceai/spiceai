/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIE OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use rand::Rng;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};
use tokio::time::sleep;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check_with_timeout, test_request_context},
};

pub fn get_s3_dataset(s3_uri: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(s3_uri, name);
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Test that the `shutdown_timeout` parameter is correctly applied:
/// 1. The runtime shutdown waits for 5 seconds for a long-running HTTP operation to complete.
/// 2. The runtime shutdown is forced after 5 seconds.
#[tokio::test]
async fn runtime_shutdown_timeout_force() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let metrics_port: u16 = http_port + 2;

            tracing::debug!(
                "Ports: http: {http_port}, flight: {flight_port}, metrics: {metrics_port}"
            );

            let api_config = runtime::config::Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = AppBuilder::new("lineitem")
                .with_dataset(get_s3_dataset(
                    "s3://spiceai-public-datasets/tpch_sf100/lineitem.parquet",
                    "lineitem",
                ))
                .with_shutdown_timeout("5s")
                .build();

            configure_test_datafusion();
            let rt =  Arc::new(Runtime::builder()
                .with_app(app)
                .build()
                .await);
            let start_servers_rt = Arc::clone(&rt);
            let load_components_rt = Arc::clone(&rt);

             // Start the servers
            tokio::spawn(async move {
                Box::pin(start_servers_rt.start_servers(api_config, None, runtime::auth::EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = load_components_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(rt.as_ref(), Duration::from_secs(30)).await;

            // Simulate a long running HTTP query
            let addr = format!("127.0.0.1:{http_port}");
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                client.post(format!("http://{addr}/v1/sql"))
                    .body("SELECT AVG(l_quantity) FROM lineitem;")
                    .send()
                    .await
            });

            // Ensures that the HTTP query is started before the shutdown
            sleep(std::time::Duration::from_secs(1)).await;

            let start_time = std::time::Instant::now();

            tokio::select! {
                // Operation is expected to be completed within 5 seconds, add extra buffer to ensure test robustness
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for runtime termination"));
                }
               () = rt.shutdown() => {}
            };
            let elapsed_time = start_time.elapsed();
            tracing::debug!("Runtime shutdown completed in {elapsed_time:?}");

            assert!(
                elapsed_time >= std::time::Duration::from_secs(5),
                "Runtime termination completed in {elapsed_time:?}, but expected to wait at least 5 seconds"
            );

            Ok(())
        })
        .await
}

/// Test graceful shutdown of the runtime:
/// 1. The runtime shutdown waits for a long-running HTTP operation to complete.
/// 2. Once the operation is completed within 5 seconds, the runtime termination is completed immediately.
#[tokio::test]
async fn runtime_shutdown_timeout_grace() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let metrics_port: u16 = http_port + 2;

            tracing::debug!(
                "Ports: http: {http_port}, flight: {flight_port}, metrics: {metrics_port}"
            );

            let api_config = runtime::config::Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = AppBuilder::new("lineitem")
                .with_dataset(get_s3_dataset(
                    "s3://spiceai-public-datasets/tpch_sf100/lineitem.parquet",
                    "lineitem",
                ))
                .with_shutdown_timeout("20s")
                .build();

            configure_test_datafusion();
            let rt =  Arc::new(Runtime::builder()
                .with_app(app)
                .build()
                .await);
            let start_servers_rt = Arc::clone(&rt);
            let load_components_rt = Arc::clone(&rt);

             // Start the servers
             tokio::spawn(async move {
                Box::pin(start_servers_rt.start_servers(api_config, None, runtime::auth::EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = load_components_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(rt.as_ref(), Duration::from_secs(30)).await;

            let start_time = std::time::Instant::now();

            // Simulate a long running HTTP query, that is finished (cancelled) after 5 seconds, 
            // ensure that the runtime is terminated as soon as the query is completed
            let addr = format!("127.0.0.1:{http_port}");
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.post(format!("http://{addr}/v1/sql"))
                        .body("SELECT AVG(l_quantity) FROM lineitem;")
                        .send()
                ).await
            });

            // Ensures that the HTTP query is started before the shutdown
            sleep(std::time::Duration::from_secs(1)).await;

            tokio::select! {
                // Operation is expected to be completed within 5 seconds, add extra buffer to ensure test robustness
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for runtime termination"));
                }
               () = rt.shutdown() => {}
            };
            let elapsed_time = start_time.elapsed();

            tracing::debug!("Runtime shutdown completed in {elapsed_time:?}");

            assert!(
                elapsed_time >= std::time::Duration::from_secs(5),
                "Runtime termination completed in {elapsed_time:?}, but expected to wait 5 seconds for operation completion"
            );

            Ok(())
        })
        .await
}
