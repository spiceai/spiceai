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
use std::borrow::Borrow;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::DecodedPayload;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle, PostgreSqlDialect};
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
use tonic::metadata::{Ascii, MetadataMap, MetadataValue, errors::InvalidMetadataValue};

use super::client_identity::{
    self as mtls_client_identity, ClientIdentityConfig, ClientIdentityConfigError,
    TLS_CLIENT_CERTIFICATE, TLS_CLIENT_CERTIFICATE_FILE, TLS_CLIENT_KEY, TLS_CLIENT_KEY_FILE,
};
use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};
use crate::{component::dataset::Dataset, federated_table::FederatedTable};
use data_components::cdc::{
    self, ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
};
use data_components::flight::{FlightFactory, FlightTable};
use data_components::{Read, ReadWrite};

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
    UnableToCreateFlightClient { source: flight_client::Error },

    #[snafu(display("Failed to get append stream schema. {source}"))]
    UnableToGetAppendSchema { source: flight_client::Error },

    #[snafu(display(
        "Could not parse <org> or <app> as ASCII: {value} Ensure the org and app are valid ASCII strings and retry."
    ))]
    InvalidMetadataValue {
        value: Arc<str>,
        source: InvalidMetadataValue,
    },

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
        "mTLS client identity is ambiguous: file-based params ('spiceai_tls_client_certificate_file', 'spiceai_tls_client_key_file') cannot be mixed with inline params ('spiceai_tls_client_certificate', 'spiceai_tls_client_key'). Use either the file-based pair or the inline pair, not both."
    ))]
    AmbiguousClientIdentity,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Resolves the client identity from the four possible params:
/// `spiceai_tls_client_certificate_file` / `spiceai_tls_client_key_file` (file-based) and
/// `spiceai_tls_client_certificate` / `spiceai_tls_client_key` (inline PEM).
///
/// Returns `Ok(None)` when no client identity is configured, `Ok(Some(...))`
/// when a complete identity is found, or `Err` when the params are
/// half-configured or ambiguous (both file and inline set).
fn resolve_client_identity_params(
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    cert_inline: Option<Vec<u8>>,
    key_inline: Option<Vec<u8>>,
) -> Result<Option<ClientIdentity>> {
    mtls_client_identity::resolve_client_identity_config(
        cert_file,
        key_file,
        cert_inline,
        key_inline,
    )
    .map_err(|error| match error {
        ClientIdentityConfigError::Incomplete {
            set_field,
            missing_field,
        } => Error::IncompleteClientIdentity {
            set_field: format!("spiceai_{set_field}"),
            missing_field: format!("spiceai_{missing_field}"),
        },
        ClientIdentityConfigError::Ambiguous => Error::AmbiguousClientIdentity,
    })
    .map(|config| {
        config.map(|config| match config {
            ClientIdentityConfig::FromFiles {
                cert_path,
                key_path,
            } => ClientIdentity::FromFiles {
                cert_path,
                key_path,
            },
            ClientIdentityConfig::FromPem { cert_pem, key_pem } => {
                ClientIdentity::FromPem { cert_pem, key_pem }
            }
        })
    })
}

#[derive(Clone, Debug)]
pub struct SpiceAI {
    flight_factory: FlightFactory,
}

impl SpiceAI {
    pub fn flight_factory(
        &self,
        dataset_path: SpiceAIDatasetPath,
    ) -> (FlightFactory, TableReference) {
        let (flight_factory, table_reference) = match dataset_path {
            SpiceAIDatasetPath::OrgAppPath { org, app, path } => {
                let mut map = MetadataMap::new();

                let spiceai_context = format!(
                    "org={},app={}",
                    org.to_str().unwrap_or_default(),
                    app.to_str().unwrap_or_default()
                );

                map.insert(HEADER_ORG, org);
                map.insert(HEADER_APP, app);
                (
                    self.flight_factory
                        .clone()
                        .with_metadata(map)
                        .with_extra_compute_context(spiceai_context.as_str()),
                    path,
                )
            }
            SpiceAIDatasetPath::Path(path) => (self.flight_factory.clone(), path),
        };

        (flight_factory, table_reference)
    }
}

pub struct SpiceCloudPlatformDialect {}

impl Dialect for SpiceCloudPlatformDialect {
    fn use_timestamp_for_date64(&self) -> bool {
        true
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        PostgreSqlDialect {}.identifier_quote_style(identifier)
    }
}
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
    ParameterSpec::component(TLS_CLIENT_CERTIFICATE_FILE)
        .description("Path to a PEM client certificate chain to present during the TLS handshake when the upstream Spice runtime requires mutual TLS. Must be set together with 'tls_client_key_file'. Mutually exclusive with 'tls_client_certificate' and 'tls_client_key'."),
    ParameterSpec::component(TLS_CLIENT_KEY_FILE)
        .description("Path to the PEM private key matching 'tls_client_certificate_file'. Must be set together with 'tls_client_certificate_file'. Mutually exclusive with 'tls_client_certificate' and 'tls_client_key'."),
    ParameterSpec::component(TLS_CLIENT_CERTIFICATE).secret()
        .description("Inline PEM client certificate chain (or ${ secrets:... } reference) to present during the TLS handshake for mutual TLS. Must be set together with 'tls_client_key'. Mutually exclusive with 'tls_client_certificate_file' and 'tls_client_key_file'."),
    ParameterSpec::component(TLS_CLIENT_KEY).secret()
        .description("Inline PEM private key (or ${ secrets:... } reference) matching 'tls_client_certificate'. Must be set together with 'tls_client_certificate'. Mutually exclusive with 'tls_client_certificate_file' and 'tls_client_key_file'."),
];

const HEADER_ORG: &str = "spiceai-org";
const HEADER_APP: &str = "spiceai-app";

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
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let url = get_endpoint(&params)?;
            tracing::trace!("Connecting to SpiceAI with flight url: {url}");

            verify_endpoint_connection(&url).await.with_context(|_| {
                UnableToVerifyEndpointConnectionSnafu {
                    endpoint: url.to_string(),
                }
            })?;

            let credentials = get_credentials(&params, &url)?;
            let ca_certificate_path: Option<PathBuf> = params
                .parameters
                .get("tls_ca_certificate_file")
                .expose()
                .ok()
                .map(PathBuf::from);
            let client_certificate_path: Option<PathBuf> = params
                .parameters
                .get(TLS_CLIENT_CERTIFICATE_FILE)
                .expose()
                .ok()
                .map(PathBuf::from);
            let client_key_path: Option<PathBuf> = params
                .parameters
                .get(TLS_CLIENT_KEY_FILE)
                .expose()
                .ok()
                .map(PathBuf::from);
            let client_certificate_inline: Option<Vec<u8>> = params
                .parameters
                .get(TLS_CLIENT_CERTIFICATE)
                .expose()
                .ok()
                .map(|s| s.as_bytes().to_vec());
            let client_key_inline: Option<Vec<u8>> = params
                .parameters
                .get(TLS_CLIENT_KEY)
                .expose()
                .ok()
                .map(|s| s.as_bytes().to_vec());

            let client_identity = resolve_client_identity_params(
                client_certificate_path,
                client_key_path,
                client_certificate_inline,
                client_key_inline,
            )?;

            let tls_options = ClientTlsOptions {
                ca_certificate_path,
                client_identity,
            };

            let mut flight_client =
                FlightClient::try_new_with_tls_options(url, credentials, None, &tls_options)
                    .await
                    .context(UnableToCreateFlightClientSnafu)?;

            flight_client = configure_max_message_size(flight_client, &params)?;

            let flight_factory = FlightFactory::new(
                "spice.ai",
                flight_client,
                Arc::new(SpiceCloudPlatformDialect {}),
            );
            let spiceai = SpiceAI { flight_factory };
            Ok(Arc::new(spiceai) as Arc<dyn DataConnector>)
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
fn configure_max_message_size(
    mut flight_client: FlightClient,
    params: &ConnectorParams,
) -> Result<FlightClient> {
    if let Some(app) = params.app.as_ref()
        && let Some(flight) = app.runtime.flight.as_ref()
        && let Some(max_message_size) =
            flight
                .max_message_size_bytes()
                .map_err(|err| Error::InvalidParameterValue {
                    parameter: "max_message_size".to_string(),
                    source: err,
                })?
    {
        flight_client = flight_client.with_max_message_size(max_message_size, max_message_size);
    }
    Ok(flight_client)
}

#[async_trait]
impl DataConnector for SpiceAI {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let dataset_path = match SpiceAI::spice_dataset_path(dataset) {
            Ok(dataset_path) => dataset_path,
            Err(e) => {
                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                });
            }
        };

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
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let dataset_path = match SpiceAI::spice_dataset_path(dataset) {
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
            .context(super::UnableToGetReadWriteProviderSnafu {
                dataconnector: "spice.ai",
                connector_component: ConnectorComponent::from(dataset),
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
            .downcast_ref::<FederatedTableProviderAdaptor>() else {
                return;
            };
            let Some(federated_adaptor) = federated_table_provider_adaptor.table_provider.as_ref() else {
                return;
            };
            let Some(flight_table) = federated_adaptor
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

register_data_connector!("spice.ai", SpiceAIFactory);
register_data_connector!(
    register_legacy_spiceai_connector,
    LEGACY_SPICEAI_CONNECTOR_REGISTRATION,
    "spiceai",
    SpiceAIFactory
);

#[derive(Debug, PartialEq, Eq)]
pub enum SpiceAIDatasetPath {
    OrgAppPath {
        org: MetadataValue<Ascii>,
        app: MetadataValue<Ascii>,
        path: TableReference,
    },
    Path(TableReference),
}

impl SpiceAI {
    /// Parses a dataset path from a Spice AI dataset definition.
    ///
    /// Spice AI datasets have the following format for `dataset.path()`:
    /// `<org>/<app>/datasets/<dataset_name>`.
    fn spice_dataset_path<T: Borrow<Dataset>>(dataset: T) -> Result<SpiceAIDatasetPath> {
        let dataset = dataset.borrow();
        let path = dataset.path();
        if is_flight_endpoint_path(path) {
            return Ok(SpiceAIDatasetPath::Path(dataset.name.clone()));
        }

        let path_parts: Vec<&str> = path.split('/').collect();

        match path_parts.as_slice() {
            [org, app, "datasets", dataset_name] => {
                let org: MetadataValue<Ascii> =
                    MetadataValue::try_from(*org).context(InvalidMetadataValueSnafu {
                        value: Arc::from(*org),
                    })?;
                let app: MetadataValue<Ascii> =
                    MetadataValue::try_from(*app).context(InvalidMetadataValueSnafu {
                        value: Arc::from(*app),
                    })?;
                Ok(SpiceAIDatasetPath::OrgAppPath {
                    org,
                    app,
                    path: TableReference::parse_str(dataset_name),
                })
            }
            _ => Ok(SpiceAIDatasetPath::Path(TableReference::parse_str(path))),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::parameters::Parameters;
    use runtime_secrets::Secrets;
    use secrecy::ExposeSecret;
    use secrecy::SecretString;
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    async fn make_params(params: Vec<(String, SecretString)>) -> ConnectorParams {
        make_params_for_from("spice.ai/test.table", params).await
    }

    async fn make_params_for_from(
        dataset_from: impl Into<String>,
        params: Vec<(String, SecretString)>,
    ) -> ConnectorParams {
        let app = app::AppBuilder::new("test").build();
        let runtime = crate::Runtime::builder().build().await;

        let dataset = DatasetBuilder::try_new(dataset_from.into(), "bar")
            .expect("failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("failed to build dataset");

        let parameters = Parameters::try_new(
            "test",
            params,
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");

        ConnectorParams {
            parameters,
            unsupported_type_action: None,
            component: ConnectorComponent::Dataset(Arc::new(dataset)),
            app: None,
            runtime: None,
            io_runtime: Handle::current(),
        }
    }

    #[tokio::test]
    async fn test_spice_dataset_path() {
        let tests = vec![
            (
                "spice.ai:lukekim/demo/datasets/my_data".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("lukekim").expect("failed to parse org"),
                    app: MetadataValue::try_from("demo").expect("failed to parse app"),
                    path: TableReference::parse_str("my_data"),
                },
            ),
            (
                "spice.ai://lukekim/demo/datasets/my_data".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("lukekim").expect("failed to parse org"),
                    app: MetadataValue::try_from("demo").expect("failed to parse app"),
                    path: TableReference::parse_str("my_data"),
                },
            ),
            (
                "spice.ai/lukekim/demo/datasets/my_data".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("lukekim").expect("failed to parse org"),
                    app: MetadataValue::try_from("demo").expect("failed to parse app"),
                    path: TableReference::parse_str("my_data"),
                },
            ),
            (
                "spiceai:http://localhost:50051".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("bar")),
            ),
            (
                "spice.ai:http://localhost:50051".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("bar")),
            ),
            (
                "spice.ai:https://remote.example.com:50051".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("bar")),
            ),
            (
                "spice.ai/eth.recent_blocks".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("eth.recent_blocks")),
            ),
            (
                "spice.ai/eth.transactions".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("eth.transactions")),
            ),
            (
                "spice.ai/public.users".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("public.users")),
            ),
            (
                "spice.ai/org1/app1/datasets/table_with_underscore".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org1").expect("failed to parse org"),
                    app: MetadataValue::try_from("app1").expect("failed to parse app"),
                    path: TableReference::parse_str("table_with_underscore"),
                },
            ),
            (
                "spice.ai/org-name/app-name/datasets/table-name".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org-name").expect("failed to parse org"),
                    app: MetadataValue::try_from("app-name").expect("failed to parse app"),
                    path: TableReference::parse_str("table-name"),
                },
            ),
            (
                "spice.ai/complex.table.name".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("complex.table.name")),
            ),
            (
                "spice.ai/org.name/app.id/datasets/table.name".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org.name").expect("failed to parse org"),
                    app: MetadataValue::try_from("app.id").expect("failed to parse app"),
                    path: TableReference::parse_str("table.name"),
                },
            ),
            (
                "spice.ai/my.org/my.app/datasets/data.table.name".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("my.org").expect("failed to parse org"),
                    app: MetadataValue::try_from("my.app").expect("failed to parse app"),
                    path: TableReference::parse_str("data.table.name"),
                },
            ),
            (
                "spice.ai/schema.name.table".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("schema.name.table")),
            ),
            (
                "spice.ai/org.with.dots/app.with.dots/datasets/table.with.dots".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org.with.dots").expect("failed to parse org"),
                    app: MetadataValue::try_from("app.with.dots").expect("failed to parse app"),
                    path: TableReference::parse_str("table.with.dots"),
                },
            ),
            (
                "spice.ai/a.b.c/x.y.z/datasets/t1.t2.t3".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("a.b.c").expect("failed to parse org"),
                    app: MetadataValue::try_from("x.y.z").expect("failed to parse app"),
                    path: TableReference::parse_str("t1.t2.t3"),
                },
            ),
        ];

        for (input, expected) in tests {
            let app = app::AppBuilder::new("test").build();
            let rt = crate::Runtime::builder().build().await;

            let dataset = DatasetBuilder::try_new(input.clone(), "bar")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");

            let dataset_path = SpiceAI::spice_dataset_path(&dataset).expect("a valid dataset path");
            assert_eq!(dataset_path, expected, "Failed for input: {input}");
        }
    }

    #[tokio::test]
    async fn test_spiceai_from_variants_resolve_connector_params() {
        crate::dataconnector::register_all().await;

        for input in [
            "spiceai:http://localhost:50051",
            "spice.ai:http://localhost:50051",
            "spice.ai:spiceai/quickstart/datasets/taxi_trips",
            "spice.ai/spiceai/quickstart/datasets/taxi_trips",
        ] {
            let app = app::AppBuilder::new("test").build();
            let runtime = crate::Runtime::builder().build().await;
            let dataset = DatasetBuilder::try_new(input.to_string(), "taxi_trips")
                .expect("failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(runtime))
                .build()
                .expect("failed to build dataset");

            crate::dataconnector::parameters::ConnectorParamsBuilder::new(
                dataset.source().into(),
                ConnectorComponent::Dataset(Arc::new(dataset)),
            )
            .build(Arc::new(RwLock::new(Secrets::new())), Handle::current())
            .await
            .expect("spice.ai connector variant should resolve");
        }
    }

    #[tokio::test]
    async fn test_get_credentials_prefers_api_key() {
        let params = make_params(vec![
            ("spiceai_api_key".to_string(), "api-key".to_string().into()),
            (
                "spiceai_token".to_string(),
                "legacy-token".to_string().into(),
            ),
        ])
        .await;

        let endpoint = spice_cloud_flight_endpoint("us-east-1");
        let credentials =
            get_credentials(&params, &endpoint).expect("credentials should resolve from api_key");
        let Credentials::UsernamePassword { username, password } = credentials else {
            panic!("expected username/password credentials");
        };
        assert_eq!(username.as_ref(), "");
        assert_eq!(password.expose_secret(), "api-key");
    }

    #[tokio::test]
    async fn test_get_credentials_uses_legacy_token() {
        let params = make_params(vec![(
            "spiceai_token".to_string(),
            "legacy-token".to_string().into(),
        )])
        .await;

        let endpoint = spice_cloud_flight_endpoint("us-east-1");
        let credentials = get_credentials(&params, &endpoint)
            .expect("credentials should resolve from token fallback");
        let Credentials::UsernamePassword { username, password } = credentials else {
            panic!("expected username/password credentials");
        };
        assert_eq!(username.as_ref(), "");
        assert_eq!(password.expose_secret(), "legacy-token");
    }

    #[tokio::test]
    async fn test_get_credentials_requires_api_key_for_spice_cloud_endpoint() {
        let params = make_params(vec![]).await;

        let endpoint = spice_cloud_flight_endpoint("us-east-1");
        let error = get_credentials(&params, &endpoint)
            .expect_err("missing cloud credentials should return an error");
        assert!(matches!(
            error,
            Error::MissingRequiredParameter { parameter }
            if parameter == "api_key or token"
        ));
    }

    #[tokio::test]
    async fn test_get_credentials_allows_anonymous_custom_endpoint() {
        let params = make_params(vec![]).await;

        let credentials = get_credentials(&params, "http://localhost:50051")
            .expect("custom endpoints should allow anonymous credentials");
        assert!(matches!(credentials, Credentials::Anonymous));
    }

    #[tokio::test]
    async fn test_get_endpoint_builds_cloud_endpoint_from_region() {
        let params = make_params(vec![(
            "spiceai_region".to_string(),
            "us-east-1".to_string().into(),
        )])
        .await;

        assert_eq!(
            get_endpoint(&params)
                .expect("region should build cloud endpoint")
                .as_ref(),
            "https://us-east-1-prod-aws-flight.spiceai.io"
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_uses_from_endpoint_for_self_hosted_variants() {
        for (input, expected) in [
            ("spiceai:http://localhost:50051", "http://localhost:50051"),
            ("spice.ai:http://localhost:50051", "http://localhost:50051"),
            (
                "spice.ai:https://remote.example.com:50051",
                "https://remote.example.com:50051",
            ),
            (
                "spice.ai:grpc+tls://remote.example.com:50051",
                "grpc+tls://remote.example.com:50051",
            ),
        ] {
            let params = make_params_for_from(input, vec![]).await;

            assert_eq!(
                get_endpoint(&params)
                    .expect("from endpoint should not require region")
                    .as_ref(),
                expected,
                "failed for input: {input}"
            );
        }
    }

    #[tokio::test]
    async fn test_get_endpoint_does_not_validate_region_for_self_hosted_endpoint() {
        let params = make_params(vec![
            (
                "spiceai_endpoint".to_string(),
                "http://localhost:50051".to_string().into(),
            ),
            (
                "spiceai_region".to_string(),
                "self-hosted".to_string().into(),
            ),
        ])
        .await;

        assert_eq!(
            get_endpoint(&params)
                .expect("custom endpoint should not validate unrelated region")
                .as_ref(),
            "http://localhost:50051"
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_rejects_grpc_from_endpoint() {
        let params = make_params_for_from("spice.ai:grpc://localhost:50051", vec![]).await;

        let error = get_endpoint(&params).expect_err("grpc endpoint should be rejected");
        assert!(matches!(
            error,
            Error::UnsupportedEndpointScheme { endpoint }
            if endpoint == "grpc://localhost:50051"
        ));
    }

    #[tokio::test]
    async fn test_get_endpoint_rejects_grpc_endpoint_parameter() {
        let params = make_params(vec![(
            "spiceai_endpoint".to_string(),
            "grpc://localhost:50051".to_string().into(),
        )])
        .await;

        let error = get_endpoint(&params).expect_err("grpc endpoint should be rejected");
        assert!(matches!(
            error,
            Error::UnsupportedEndpointScheme { endpoint }
            if endpoint == "grpc://localhost:50051"
        ));
    }

    #[tokio::test]
    async fn test_get_endpoint_requires_region_without_explicit_endpoint() {
        let params = make_params(vec![]).await;

        let error = get_endpoint(&params).expect_err("missing cloud region should error");
        assert!(matches!(
            error,
            Error::MissingRequiredParameter { parameter }
            if parameter == "region"
        ));
    }

    #[tokio::test]
    async fn test_get_endpoint_rewrites_legacy_cloud_endpoint_with_region() {
        let params = make_params(vec![
            (
                "spiceai_endpoint".to_string(),
                "https://flight.spiceai.io".to_string().into(),
            ),
            ("spiceai_region".to_string(), "us-west-2".to_string().into()),
        ])
        .await;

        assert_eq!(
            get_endpoint(&params)
                .expect("legacy endpoint should be replaced by regional cloud endpoint")
                .as_ref(),
            "https://us-west-2-prod-aws-flight.spiceai.io"
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_requires_region_for_cloud_endpoint() {
        let params = make_params(vec![(
            "spiceai_endpoint".to_string(),
            "https://us-east-1-prod-aws-flight.spiceai.io"
                .to_string()
                .into(),
        )])
        .await;

        let error = get_endpoint(&params).expect_err("cloud endpoint should require region");
        assert!(matches!(
            error,
            Error::MissingRequiredParameter { parameter }
            if parameter == "region"
        ));
    }

    #[tokio::test]
    async fn test_get_endpoint_rejects_cloud_endpoint_region_mismatch() {
        let params = make_params(vec![
            (
                "spiceai_endpoint".to_string(),
                "https://us-east-1-prod-aws-flight.spiceai.io"
                    .to_string()
                    .into(),
            ),
            ("spiceai_region".to_string(), "us-west-2".to_string().into()),
        ])
        .await;

        let error = get_endpoint(&params).expect_err("mismatched cloud endpoint should error");
        assert!(matches!(
            error,
            Error::CloudEndpointRegionMismatch { endpoint, region }
            if endpoint == "https://us-east-1-prod-aws-flight.spiceai.io" && region == "us-west-2"
        ));
    }

    #[tokio::test]
    async fn test_get_endpoint_prefers_endpoint_parameter() {
        let params = make_params(vec![
            (
                "spiceai_endpoint".to_string(),
                "http://new:50051".to_string().into(),
            ),
            (
                "spiceai_flight_endpoint".to_string(),
                "http://legacy:50051".to_string().into(),
            ),
        ])
        .await;

        assert_eq!(
            get_endpoint(&params)
                .expect("custom endpoint should not require region")
                .as_ref(),
            "http://new:50051"
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_uses_legacy_flight_endpoint_parameter() {
        let params = make_params(vec![(
            "spiceai_flight_endpoint".to_string(),
            "http://legacy:50051".to_string().into(),
        )])
        .await;

        assert_eq!(
            get_endpoint(&params)
                .expect("custom endpoint should not require region")
                .as_ref(),
            "http://legacy:50051"
        );
    }
}
