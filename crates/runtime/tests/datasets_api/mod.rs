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

//! Tests for the `/v1/datasets` HTTP API endpoint.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use rand::Rng;
use runtime::{Runtime, auth::EndpointAuth, config::Config, status::ComponentStatus};
use serde::Deserialize;
use spicepod::component::dataset::Dataset;

use crate::{
    init_tracing,
    utils::{test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn get_s3_parquet_dataset(name: &str) -> Dataset {
    Dataset::new(
        "s3://spiceai-public-datasets/dictionary_example/dictionary_example.parquet",
        name,
    )
}

#[derive(Debug, Deserialize)]
struct DatasetResponse {
    from: String,
    name: String,
    replication_enabled: bool,
    acceleration_enabled: bool,
    status: Option<String>,
}

/// Tests that the `/v1/datasets?status=true` endpoint returns the correct status
/// from `RuntimeStatus` for each dataset.
#[tokio::test]
async fn test_datasets_api_returns_correct_status() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_datasets_api_returns_correct_status");
            let _span_guard = span.enter();

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;

            tracing::debug!("Datasets API Ports: http: {http_port}, flight: {flight_port}");

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = app::AppBuilder::new("test_datasets_api")
                .with_dataset(get_s3_parquet_dataset("test_dataset"))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            // Start the servers
            tokio::spawn(async move {
                Box::pin(cloned_rt.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            // Wait for components to load
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            let http_client = reqwest::Client::builder().build()?;

            tracing::info!("Waiting for servers to start...");
            wait_until_true(Duration::from_secs(10), || async {
                http_client
                    .get(format!("http://127.0.0.1:{http_port}/health"))
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            // Verify the dataset is Ready in RuntimeStatus
            let status = rt.status();
            let dataset_statuses = status.get_dataset_statuses();
            let dataset_ref = datafusion::sql::TableReference::bare("test_dataset");
            let runtime_status = dataset_statuses
                .get(&dataset_ref)
                .expect("test_dataset should have a status");
            assert_eq!(
                *runtime_status,
                ComponentStatus::Ready,
                "Dataset should be Ready in RuntimeStatus"
            );

            // Call the /v1/datasets?status=true API
            let http_url = format!("http://127.0.0.1:{http_port}/v1/datasets?status=true");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");

            assert!(
                response.status().is_success(),
                "API should return success status"
            );

            let datasets: Vec<DatasetResponse> = response.json().await?;

            // Find our test dataset
            let test_dataset = datasets
                .iter()
                .find(|d| d.name == "test_dataset")
                .expect("test_dataset should be in the response");

            // Verify the status from the API matches RuntimeStatus
            assert_eq!(
                test_dataset.status,
                Some("Ready".to_string()),
                "API status should match RuntimeStatus (Ready)"
            );

            // Additional checks
            assert!(!test_dataset.acceleration_enabled);
            assert!(!test_dataset.replication_enabled);
            assert!(test_dataset.from.contains("s3://"));

            rt.shutdown().await;

            Ok(())
        })
        .await
}

/// Tests that the `/v1/datasets` endpoint (without status=true) does not include status field.
#[tokio::test]
async fn test_datasets_api_without_status_param() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_datasets_api_without_status_param");
            let _span_guard = span.enter();

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = app::AppBuilder::new("test_datasets_api_no_status")
                .with_dataset(get_s3_parquet_dataset("test_dataset_no_status"))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::spawn(async move {
                Box::pin(cloned_rt.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            let http_client = reqwest::Client::builder().build()?;

            wait_until_true(Duration::from_secs(10), || async {
                http_client
                    .get(format!("http://127.0.0.1:{http_port}/health"))
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            // Call the /v1/datasets API without status=true
            let http_url = format!("http://127.0.0.1:{http_port}/v1/datasets");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");

            assert!(response.status().is_success());

            let datasets: Vec<DatasetResponse> = response.json().await?;

            let test_dataset = datasets
                .iter()
                .find(|d| d.name == "test_dataset_no_status")
                .expect("test_dataset_no_status should be in the response");

            // Status should be None when status=true is not specified
            assert_eq!(
                test_dataset.status, None,
                "Status should be None when status param is not provided"
            );

            rt.shutdown().await;

            Ok(())
        })
        .await
}
