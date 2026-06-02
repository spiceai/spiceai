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
    any::Any,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::{MemTable, TableProvider},
};
use rand::RngExt;
use runtime::{
    Runtime,
    auth::EndpointAuth,
    component::dataset::Dataset as RuntimeDataset,
    config::Config,
    dataconnector::{
        self, ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError,
        DataConnectorFactory, DataConnectorResult, NewDataConnectorResult,
    },
    status::ComponentStatus,
};
use runtime_api_types::v1::{ComponentError, ComponentErrorCategory, ComponentErrorType};
use runtime_parameters::ParameterSpec;
use serde::Deserialize;
use serde_json::Value;
use spicepod::component::dataset::Dataset as SpicepodDataset;

use crate::{
    init_tracing,
    utils::{test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

const PERMISSION_STATUS_CONNECTOR: &str = "permissionstatus";

fn get_s3_parquet_dataset(name: &str) -> SpicepodDataset {
    SpicepodDataset::new(
        "s3://spiceai-public-datasets/dictionary_example/dictionary_example.parquet",
        name,
    )
}

fn get_permission_status_dataset(name: &str) -> SpicepodDataset {
    SpicepodDataset::new(format!("{PERMISSION_STATUS_CONNECTOR}:{name}"), name)
}

#[derive(Debug, Deserialize)]
struct DatasetResponse {
    from: String,
    name: String,
    replication_enabled: bool,
    acceleration_enabled: bool,
    status: Option<String>,
    error: Option<ComponentError>,
    error_message: Option<String>,
}

#[derive(Debug)]
struct PermissionStatusConnector;

#[async_trait]
impl DataConnector for PermissionStatusConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &RuntimeDataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        if dataset.name.table() == "permission_denied" {
            return Err(DataConnectorError::InsufficientPermissions {
                dataconnector: PERMISSION_STATUS_CONNECTOR.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Grant SELECT or ALL PRIVILEGES on the table",
                )),
            });
        }

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table =
            MemTable::try_new(schema, vec![Vec::new()]).expect("test memtable should build");

        Ok(Arc::new(table))
    }
}

struct PermissionStatusConnectorFactory;

impl PermissionStatusConnectorFactory {
    fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for PermissionStatusConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(PermissionStatusConnector) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        PERMISSION_STATUS_CONNECTOR
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

async fn register_permission_status_provider() {
    dataconnector::register_connector_factory(
        PERMISSION_STATUS_CONNECTOR,
        PermissionStatusConnectorFactory::new_arc(),
    )
    .await;
}

async fn assert_server_ready(http_client: &reqwest::Client, http_port: u16) {
    assert!(
        wait_until_true(Duration::from_secs(10), || async {
            http_client
                .get(format!("http://127.0.0.1:{http_port}/health"))
                .send()
                .await
                .is_ok()
        })
        .await,
        "Timed out waiting for server health endpoint to become ready on port {http_port}"
    );
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
            assert_server_ready(&http_client, http_port).await;

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

            assert_eq!(
                test_dataset.status,
                Some("Ready".to_string()),
                "API status should initially reflect RuntimeStatus (Ready)"
            );

            status.update_dataset(
                &dataset_ref,
                ComponentStatus::error_with_message("UnableToConnectInvalidUsernameOrPassword"),
            );

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

            let test_dataset = datasets
                .iter()
                .find(|d| d.name == "test_dataset")
                .expect("test_dataset should be in the response");

            // Verify the status from the API matches RuntimeStatus
            assert_eq!(
                test_dataset.status,
                Some("Error".to_string()),
                "API status should match RuntimeStatus (Error)"
            );
            assert_eq!(
                test_dataset.error,
                Some(ComponentError {
                    category: runtime_api_types::v1::ComponentErrorCategory::Dataset,
                    error_type: runtime_api_types::v1::ComponentErrorType::Auth,
                    code: "dataset.auth".to_string(),
                }),
                "API error should provide an error type/code when status=true"
            );
            assert_eq!(
                test_dataset.error_message,
                Some("UnableToConnectInvalidUsernameOrPassword".to_string()),
                "API error_message should be included when status=true and status is Error"
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

#[tokio::test]
async fn test_datasets_api_permission_edge_cases_update_statuses() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    register_permission_status_provider().await;

    test_request_context()
        .scope(async {
            let span =
                tracing::info_span!("test_datasets_api_permission_edge_cases_update_statuses");
            let _span_guard = span.enter();

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = app::AppBuilder::new("test_datasets_api_permission_cases")
                .with_dataset(get_permission_status_dataset("permission_allowed"))
                .with_dataset(get_permission_status_dataset("permission_denied"))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::spawn(async move {
                Box::pin(cloned_rt.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            // Permission-denied datasets are permanent errors so
            // load_components completes without retrying.
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            let http_client = reqwest::Client::builder().build()?;

            assert_server_ready(&http_client, http_port).await;

            let http_url = format!("http://127.0.0.1:{http_port}/v1/datasets?status=true");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");

            assert!(response.status().is_success());

            let datasets: Vec<DatasetResponse> = response.json().await?;

            let permission_allowed = datasets
                .iter()
                .find(|dataset| dataset.name == "permission_allowed")
                .expect("permission_allowed should be in the response");
            assert_eq!(permission_allowed.status, Some("Ready".to_string()));
            assert_eq!(permission_allowed.error, None);
            assert_eq!(permission_allowed.error_message, None);

            let permission_denied = datasets
                .iter()
                .find(|dataset| dataset.name == "permission_denied")
                .expect("permission_denied should be in the response");
            assert_eq!(permission_denied.status, Some("Error".to_string()));
            assert_eq!(
                permission_denied.error,
                Some(ComponentError {
                    category: ComponentErrorCategory::Dataset,
                    error_type: ComponentErrorType::Permission,
                    code: "dataset.permission".to_string(),
                })
            );
            assert!(
                permission_denied
                    .error_message
                    .as_deref()
                    .is_some_and(|message| {
                        message.contains("Insufficient permissions to access")
                            && message.contains("permission_denied")
                    }),
                "API should surface the permission failure message"
            );

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

            assert_server_ready(&http_client, http_port).await;

            // Call the /v1/datasets API without status=true
            let http_url = format!("http://127.0.0.1:{http_port}/v1/datasets");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");

            assert!(response.status().is_success());

            let datasets_json: Vec<Value> = response.json().await?;

            let test_dataset_json = datasets_json
                .iter()
                .find(|d| d.get("name").and_then(Value::as_str) == Some("test_dataset_no_status"))
                .expect("test_dataset_no_status should be in the response");

            assert!(
                test_dataset_json.get("status").is_none(),
                "status should be omitted when status=true is not provided"
            );
            assert!(
                test_dataset_json.get("error").is_none(),
                "error should be omitted when status=true is not provided"
            );
            assert!(
                test_dataset_json.get("error_message").is_none(),
                "error_message should be omitted when status=true is not provided"
            );

            let datasets: Vec<DatasetResponse> =
                serde_json::from_value(Value::Array(datasets_json))?;

            let test_dataset = datasets
                .iter()
                .find(|d| d.name == "test_dataset_no_status")
                .expect("test_dataset_no_status should be in the response");

            // Status should be None when status=true is not specified
            assert_eq!(
                test_dataset.status, None,
                "Status should be None when status param is not provided"
            );
            assert_eq!(
                test_dataset.error, None,
                "Error should be None when status param is not provided"
            );
            assert_eq!(
                test_dataset.error_message, None,
                "Error message should be None when status param is not provided"
            );

            rt.shutdown().await;

            Ok(())
        })
        .await
}
