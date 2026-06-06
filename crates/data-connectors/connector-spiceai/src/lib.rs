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

use std::any::Any;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::DecodedPayload;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_federation::FederatedTableProviderAdaptor;
use flight_client::Credentials;
use flight_client::FlightClient;
use flight_client::tls::{ClientIdentity, ClientTlsOptions};
use futures::{Stream, StreamExt};
use ns_lookup::verify_endpoint_connection;
use snafu::prelude::*;
use spice_cloud_client::endpoints::{
    flight_endpoint as spice_cloud_flight_endpoint,
    flight_endpoint_region as spice_cloud_endpoint_region,
    is_legacy_flight_endpoint as is_legacy_spice_cloud_endpoint,
    is_spice_cloud_flight_endpoint as is_spice_cloud_endpoint, is_valid_region,
};

use data_components::cdc::{
    self, ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
};
use data_components::flight::FlightTable;
use data_components::{Read, ReadWrite};
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
};
use runtime::federated_table::FederatedTable;
use runtime::parameters::ParameterSpec;
use runtime::register_data_connector;
pub use spiceai_connector_types::{SpiceAI, SpiceAIDatasetPath};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/spiceai#configuration"
    ))]
    MissingRequiredParameter { parameter: String },

    #[snafu(display(r#"Failed to connect to SpiceAI endpoint "{endpoint}". {source} Ensure the endpoint is valid and reachable"#))]
    UnableToVerifyEndpointConnection {
        source: ns_lookup::Error,
        endpoint: String,
    },

    #[snafu(display("Failed to create flight client. {source}"))]
    UnableToCreateFlightClient {
        source: spiceai_connector_types::Error,
    },

    #[snafu(display("Failed to get append stream schema. {source}"))]
    UnableToGetAppendSchema { source: flight_client::Error },

    #[snafu(display(
        "Failed to apply parameter '{parameter}': {source}. Ensure the value is valid and retry. For details, visit: https://spiceai.org/docs/components/data-connectors/spiceai#parameters"
    ))]
    InvalidParameterValue {
        parameter: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Invalid Spice Cloud region: {region}. Specify a valid region, for example 'spiceai_region: us-east-1'. To list available regions, run: 'spice cloud regions'"
    ))]
    InvalidRegion { region: String },

    #[snafu(display(
        "Spice Cloud endpoint region mismatch: endpoint {endpoint} does not match region {region}. Use the endpoint for the configured region or remove the endpoint parameter."
    ))]
    CloudEndpointRegionMismatch { endpoint: String, region: String },

    #[snafu(display(
        "Unsupported SpiceAI endpoint scheme in endpoint {endpoint}: grpc:// is not supported. Use http:// for plaintext Flight or https:// or grpc+tls:// for TLS."
    ))]
    UnsupportedEndpointScheme { endpoint: String },

    #[snafu(display(
        "mTLS client identity is half-configured: '{set_field}' is set but '{missing_field}' is missing. Set both fields to present a client certificate to the upstream Spice runtime, or set neither."
    ))]
    IncompleteClientIdentity {
        set_field: String,
        missing_field: String,
    },

    #[snafu(display(
        "mTLS client identity is ambiguous: both file-based ('tls_client_certificate_file') and inline ('tls_client_certificate') params are set. Use one or the other, not both."
    ))]
    AmbiguousClientIdentity,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Resolves the client identity from the four possible params:
/// `tls_client_certificate_file` / `tls_client_key_file` (file-based) and
/// `tls_client_certificate` / `tls_client_key` (inline PEM).
///
/// Returns `Ok(None)` when no client identity is configured, `Ok(Some(...))`
/// when a complete identity is found, or `Err` when the params are
/// half-configured or ambiguous (both file and inline set).
fn resolve_client_identity_params(
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    cert_inline: Option<Vec<u8>>,
    key_inline: Option<Vec<u8>>,
) -> std::result::Result<Option<ClientIdentity>, Box<dyn std::error::Error + Send + Sync>> {
    let has_file_cert = cert_file.is_some();
    let has_file_key = key_file.is_some();
    let has_inline_cert = cert_inline.is_some();
    let has_inline_key = key_inline.is_some();

    // Reject ambiguous: both file and inline cert set.
    if (has_file_cert || has_file_key) && (has_inline_cert || has_inline_key) {
        return Err(Box::new(Error::AmbiguousClientIdentity));
    }

    // File-based identity.
    if has_file_cert || has_file_key {
        return match (cert_file, key_file) {
            (Some(cert_path), Some(key_path)) => Ok(Some(ClientIdentity::FromFiles {
                cert_path,
                key_path,
            })),
            (Some(_), None) => Err(Box::new(Error::IncompleteClientIdentity {
                set_field: "tls_client_certificate_file".to_string(),
                missing_field: "tls_client_key_file".to_string(),
            })),
            (None, Some(_)) => Err(Box::new(Error::IncompleteClientIdentity {
                set_field: "tls_client_key_file".to_string(),
                missing_field: "tls_client_certificate_file".to_string(),
            })),
            (None, None) => unreachable!(),
        };
    }

    // Inline identity.
    if has_inline_cert || has_inline_key {
        return match (cert_inline, key_inline) {
            (Some(cert_pem), Some(key_pem)) => {
                Ok(Some(ClientIdentity::FromPem { cert_pem, key_pem }))
            }
            (Some(_), None) => Err(Box::new(Error::IncompleteClientIdentity {
                set_field: "tls_client_certificate".to_string(),
                missing_field: "tls_client_key".to_string(),
            })),
            (None, Some(_)) => Err(Box::new(Error::IncompleteClientIdentity {
                set_field: "tls_client_key".to_string(),
                missing_field: "tls_client_certificate".to_string(),
            })),
            (None, None) => unreachable!(),
        };
    }

    Ok(None)
}

// SpiceCloudPlatformDialect is re-exported from the shared types crate.
pub use spiceai_connector_types::SpiceCloudDialect as SpiceCloudPlatformDialect;

#[derive(Default, Copy, Clone)]
pub struct SpiceAIFactory {}

impl SpiceAIFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("api_key").secret(),
    ParameterSpec::component("token").secret(),
    ParameterSpec::component("region"),
    ParameterSpec::component("endpoint"),
    ParameterSpec::component("flight_endpoint"),
    ParameterSpec::component("tls_ca_certificate_file")
        .description("Path to a CA certificate file (PEM format) to use for TLS verification instead of system certificates."),
    ParameterSpec::component("tls_client_certificate_file")
        .description("Path to a PEM client certificate chain to present during the TLS handshake when the upstream Spice runtime requires mutual TLS. Must be set together with 'tls_client_key_file'. Mutually exclusive with 'tls_client_certificate'."),
    ParameterSpec::component("tls_client_key_file")
        .description("Path to the PEM private key matching 'tls_client_certificate_file'. Must be set together with 'tls_client_certificate_file'. Mutually exclusive with 'tls_client_key'."),
    ParameterSpec::component("tls_client_certificate").secret()
        .description("Inline PEM client certificate chain (or ${ secrets:... } reference) to present during the TLS handshake for mutual TLS. Must be set together with 'tls_client_key'. Mutually exclusive with 'tls_client_certificate_file'."),
    ParameterSpec::component("tls_client_key").secret()
        .description("Inline PEM private key (or ${ secrets:... } reference) matching 'tls_client_certificate'. Must be set together with 'tls_client_certificate'. Mutually exclusive with 'tls_client_key_file'."),
];

fn get_explicit_endpoint(params: &ConnectorParams) -> Option<&str> {
    params
        .parameters
        .get("endpoint")
        .expose()
        .ok()
        .or_else(|| params.parameters.get("flight_endpoint").expose().ok())
}

fn get_from_endpoint(params: &ConnectorParams) -> Option<&str> {
    let ConnectorComponent::Dataset(dataset) = &params.component else {
        return None;
    };

    let path = dataset.path();
    is_flight_endpoint_path(path).then_some(path)
}

fn is_flight_endpoint_path(path: &str) -> bool {
    path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("grpc://")
        || path.starts_with("grpc+tls://")
}

fn ensure_supported_endpoint_scheme(endpoint: &str) -> Result<()> {
    ensure!(
        !endpoint.starts_with("grpc://"),
        UnsupportedEndpointSchemeSnafu {
            endpoint: endpoint.to_string()
        }
    );

    Ok(())
}

fn get_region(params: &ConnectorParams) -> Option<&str> {
    params.parameters.get("region").expose().ok()
}

fn require_valid_region(region: Option<&str>) -> Result<&str> {
    let region = region.ok_or_else(|| {
        MissingRequiredParameterSnafu {
            parameter: "region".to_string(),
        }
        .build()
    })?;
    ensure!(
        !region.is_empty(),
        MissingRequiredParameterSnafu {
            parameter: "region".to_string()
        }
    );
    ensure!(
        is_valid_region(region),
        InvalidRegionSnafu {
            region: region.to_string()
        }
    );

    Ok(region)
}

fn get_endpoint(params: &ConnectorParams) -> Result<Arc<str>> {
    let region = get_region(params);

    let Some(endpoint) = get_explicit_endpoint(params).or_else(|| get_from_endpoint(params)) else {
        let region = require_valid_region(region)?;
        return Ok(spice_cloud_flight_endpoint(region).into());
    };

    ensure_supported_endpoint_scheme(endpoint)?;

    if is_legacy_spice_cloud_endpoint(endpoint) {
        let region = require_valid_region(region)?;
        return Ok(spice_cloud_flight_endpoint(region).into());
    }

    if let Some(endpoint_region) = spice_cloud_endpoint_region(endpoint) {
        let region = require_valid_region(region)?;
        ensure!(
            endpoint_region == region,
            CloudEndpointRegionMismatchSnafu {
                endpoint: endpoint.to_string(),
                region: region.to_string()
            }
        );
    }

    Ok(endpoint.into())
}

fn get_optional_api_key(params: &ConnectorParams) -> Option<&secrecy::SecretString> {
    if let Some(api_key) = params.parameters.get("api_key").ok() {
        return Some(api_key);
    }

    if let Some(token) = params.parameters.get("token").ok() {
        return Some(token);
    }

    None
}

fn get_credentials(params: &ConnectorParams, endpoint: &str) -> Result<Credentials> {
    if let Some(api_key) = get_optional_api_key(params) {
        return Ok(Credentials::new("", api_key.clone()));
    }

    if is_spice_cloud_endpoint(endpoint) {
        return MissingRequiredParameterSnafu {
            parameter: "api_key or token".to_string(),
        }
        .fail();
    }

    Ok(Credentials::anonymous())
}

impl DataConnectorFactory for SpiceAIFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let url = get_endpoint(&params)?;
            tracing::trace!("Connecting to SpiceAI with flight url: {url}");

            verify_endpoint_connection(&url).await.with_context(|_| {
                UnableToVerifyEndpointConnectionSnafu {
                    endpoint: url.to_string(),
                }
            })?;

            let credentials = get_credentials(&params, &url)?;
            let tls_options = build_tls_options(&params)?;
            let max_message_size = get_max_message_size(&params)?;

            let spiceai =
                SpiceAI::from_raw(url.to_string(), credentials, tls_options, max_message_size)
                    .await
                    .context(UnableToCreateFlightClientSnafu)?;

            Ok(Arc::new(SpiceAIConnector(spiceai)) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "spiceai"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// Configures flight client's message size based on app parameters
/// Extract TLS options from connector params.
fn build_tls_options(params: &ConnectorParams) -> Result<ClientTlsOptions> {
    let ca_certificate_path: Option<PathBuf> = params
        .parameters
        .get("tls_ca_certificate_file")
        .expose()
        .ok()
        .map(PathBuf::from);
    let client_certificate_path: Option<PathBuf> = params
        .parameters
        .get("tls_client_certificate_file")
        .expose()
        .ok()
        .map(PathBuf::from);
    let client_key_path: Option<PathBuf> = params
        .parameters
        .get("tls_client_key_file")
        .expose()
        .ok()
        .map(PathBuf::from);
    let client_certificate_inline: Option<Vec<u8>> = params
        .parameters
        .get("tls_client_certificate")
        .expose()
        .ok()
        .map(|s| s.as_bytes().to_vec());
    let client_key_inline: Option<Vec<u8>> = params
        .parameters
        .get("tls_client_key")
        .expose()
        .ok()
        .map(|s| s.as_bytes().to_vec());
    let client_identity = resolve_client_identity_params(
        client_certificate_path,
        client_key_path,
        client_certificate_inline,
        client_key_inline,
    )
    .map_err(|source| Error::InvalidParameterValue {
        parameter: "tls_client_certificate/tls_client_key".to_string(),
        source,
    })?;
    Ok(ClientTlsOptions {
        ca_certificate_path,
        client_identity,
    })
}

/// Extract max message size from connector params.
fn get_max_message_size(params: &ConnectorParams) -> Result<Option<usize>> {
    if let Some(app) = params.app.as_ref()
        && let Some(flight) = app.runtime.flight.as_ref()
    {
        return flight
            .max_message_size_bytes()
            .map_err(|err| Error::InvalidParameterValue {
                parameter: "max_message_size".to_string(),
                source: err,
            });
    }
    Ok(None)
}

/// Newtype wrapper around [`SpiceAI`] so that we can implement
/// [`DataConnector`] (defined in `runtime`) for a type defined in this crate,
/// satisfying Rust's orphan rules.
#[derive(Debug)]
pub struct SpiceAIConnector(pub SpiceAI);

impl std::ops::Deref for SpiceAIConnector {
    type Target = SpiceAI;
    fn deref(&self) -> &SpiceAI {
        &self.0
    }
}

#[async_trait]
impl DataConnector for SpiceAIConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>> {
        let dataset_path =
            SpiceAI::spice_dataset_path(&dataset.name, dataset.path()).map_err(|e| {
                DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?;

        let (flight_factory, table_reference) = self.flight_factory(dataset_path);

        match Read::table_provider(&flight_factory, table_reference).await {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(data_components::flight::Error::UnableToGetSchema {
                    source: _,
                    table,
                }) = e.downcast_ref::<data_components::flight::Error>()
                {
                    tracing::debug!("{e}");
                    return Err(DataConnectorError::UnableToGetSchema {
                        dataconnector: "spice.ai".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        table_name: table.clone(),
                    });
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                });
            }
        }
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>>> {
        let dataset_path = match SpiceAI::spice_dataset_path(&dataset.name, dataset.path()) {
            Ok(dataset_path) => dataset_path,
            Err(e) => {
                return Some(Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }));
            }
        };
        let (flight_factory, table_reference) = self.flight_factory(dataset_path);

        let read_write_result = ReadWrite::table_provider(&flight_factory, table_reference)
            .await
            .map_err(|source| DataConnectorError::UnableToGetReadWriteProvider {
                dataconnector: "spice.ai".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            });

        Some(read_write_result)
    }

    fn supports_append_stream(&self) -> bool {
        true
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        _dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<tokio::sync::Mutex<()>>,
        _cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<ChangesStream> {
        self.append_stream(federated_table)
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(federated_table_provider_adaptor) = table_provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>() else {
                return;
            };
            let Some(federated_adaptor) = federated_table_provider_adaptor.table_provider.as_ref() else {
                return;
            };
            let Some(flight_table) = federated_adaptor
            .as_any()
            .downcast_ref::<FlightTable>() else {
                return;
            };

            let mut stream = Box::pin(subscribe_to_append_stream(
                flight_table.get_flight_client(),
                flight_table.get_table_reference(),
            ));

            while let Some(item) = stream.next().await {
                yield item;
            }
        }))
    }
}

register_data_connector!(
    register_legacy_spiceai_connector,
    LEGACY_SPICEAI_CONNECTOR_REGISTRATION,
    "spiceai",
    SpiceAIFactory
);

pub fn subscribe_to_append_stream(
    mut client: FlightClient,
    table_reference: String,
) -> impl Stream<Item = Result<ChangeEnvelope, cdc::StreamError>> {
    stream! {
        match client.subscribe(&table_reference).await {
            Ok(mut stream) => {
                while let Some(decoded_data) = stream.next().await {
                    match decoded_data {
                        Ok(decoded_data) => match decoded_data.payload {
                            DecodedPayload::None | DecodedPayload::Schema(_) => {},
                            DecodedPayload::RecordBatch(batch) => {
                                match ChangeBatch::try_new(batch).map(|rb| {
                                    ChangeEnvelope::new(Box::new(SpiceAIChangeCommiter {}), rb, true)
                                }) {
                                    Ok(change_batch) => yield Ok(change_batch),
                                    Err(e) => {
                                        yield Err(cdc::StreamError::SerdeJsonError(e.to_string()))
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            yield Err(cdc::StreamError::Flight(e.to_string()));
                        }
                    }
                }
            }
            Err(e) => {
                yield Err(cdc::StreamError::Flight(e.to_string()));
            }
        }
    }
}

pub struct SpiceAIChangeCommiter {}

#[async_trait]
impl CommitChange for SpiceAIChangeCommiter {
    async fn commit(&self) -> Result<(), CommitError> {
        // Noop
        Ok(())
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "spiceai";

/// Returns a new instance of the Spice.ai connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    SpiceAIFactory::new_arc()
}
