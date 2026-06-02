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

use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use data_components::Read;
use data_components::flightsql::FlightSQLFactory as DataComponentFlightSQLFactory;
use datafusion::datasource::TableProvider;
use flight_client::cookie::{CookieService, CookieStore};
use flight_client::tls::{ClientIdentity, ClientTlsOptions, new_tls_flight_channel_with_options};
use flight_client::{MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE};
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::parameters::ParameterSpec;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/flightsql#params"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display("Failed to connect to the Flight server. A TLS error occurred. {source}"))]
    UnableToConstructTlsChannel { source: flight_client::tls::Error },

    #[snafu(display("Failed to connect to the Flight server. {source}"))]
    UnableToPerformHandshake { source: FlightError },

    #[snafu(display(
        "Failed to apply parameter '{parameter}': {source}. Ensure the value is valid and retry. For details, visit: https://spiceai.org/docs/components/data-connectors/flightsql#params"
    ))]
    InvalidParameterValue {
        parameter: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "mTLS client identity is half-configured: '{set_field}' is set but '{missing_field}' is missing. Set both fields to present a client certificate to the upstream Flight server, or set neither."
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

/// Resolve client identity from file-based and inline params.
/// Returns `Ok(None)` if none configured, `Err` if half-configured or ambiguous.
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

    if (has_file_cert || has_file_key) && (has_inline_cert || has_inline_key) {
        return Err(Box::new(Error::AmbiguousClientIdentity));
    }

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

#[derive(Debug, Clone)]
pub struct FlightSQL {
    pub flightsql_factory: DataComponentFlightSQLFactory,
}

#[derive(Default, Debug, Copy, Clone)]
pub struct FlightSQLFactory {}

impl FlightSQLFactory {
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
   ParameterSpec::component("username").secret(),
   ParameterSpec::component("password").secret(),
   ParameterSpec::component("endpoint"),
   ParameterSpec::component("tls_ca_certificate_file")
       .description("Path to a CA certificate file (PEM format) to use for TLS verification instead of system certificates."),
    ParameterSpec::component("tls_client_certificate_file")
        .description("Path to a PEM client certificate chain to present during the TLS handshake when the upstream Flight server requires mutual TLS. Must be set together with 'tls_client_key_file'. Mutually exclusive with 'tls_client_certificate'."),
    ParameterSpec::component("tls_client_key_file")
        .description("Path to the PEM private key matching 'tls_client_certificate_file'. Must be set together with 'tls_client_certificate_file'. Mutually exclusive with 'tls_client_key'."),
    ParameterSpec::component("tls_client_certificate").secret()
        .description("Inline PEM client certificate chain (or ${ secrets:... } reference) for mutual TLS. Must be set together with 'tls_client_key'. Mutually exclusive with 'tls_client_certificate_file'."),
    ParameterSpec::component("tls_client_key").secret()
        .description("Inline PEM private key (or ${ secrets:... } reference) matching 'tls_client_certificate'. Must be set together with 'tls_client_certificate'. Mutually exclusive with 'tls_client_key_file'."),
];

impl DataConnectorFactory for FlightSQLFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let endpoint: String = params
                .parameters
                .get("endpoint")
                .expose()
                .ok_or_else(|p| Error::MissingParameter {
                    parameter: p.to_string(),
                })?
                .to_string();

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
            )?;

            let tls_options = ClientTlsOptions {
                ca_certificate_path: ca_certificate_path.clone(),
                client_identity,
            };

            let cookie_store = Arc::new(CookieStore::new());
            let flight_channel = new_tls_flight_channel_with_options(&endpoint, &tls_options)
                .await
                .context(UnableToConstructTlsChannelSnafu)?;
            let flight_channel = CookieService::new(flight_channel, Arc::clone(&cookie_store));

            let max_message_size =
                match params
                    .app
                    .as_ref()
                    .and_then(|app| app.runtime.flight.as_ref())
                {
                    Some(flight) => flight.max_message_size_bytes().map_err(|err| {
                        Error::InvalidParameterValue {
                            parameter: "max_message_size".to_string(),
                            source: err,
                        }
                    })?,
                    None => None,
                };

            let flight_client = FlightServiceClient::new(flight_channel)
                .max_encoding_message_size(max_message_size.unwrap_or(MAX_ENCODING_MESSAGE_SIZE))
                .max_decoding_message_size(max_message_size.unwrap_or(MAX_DECODING_MESSAGE_SIZE));

            let mut client = FlightSqlServiceClient::new_from_inner(flight_client);
            let username = params.parameters.get("username").expose().ok();
            let password = params.parameters.get("password").expose().ok();
            if let (Some(username), Some(password)) = (username, password) {
                client
                    .handshake(username, password)
                    .await
                    .context(UnableToPerformHandshakeSnafu)?;
            }
            let flightsql_factory =
                DataComponentFlightSQLFactory::new(client, endpoint, cookie_store);
            Ok(Arc::new(FlightSQL { flightsql_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "flightsql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for FlightSQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        match Read::table_provider(&self.flightsql_factory, dataset.path().into()).await {
            Ok(provider) => Ok(provider),
            Err(e) => Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "flightsql".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            }),
        }
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "flightsql";

/// Returns a new instance of the `FlightSQL` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    FlightSQLFactory::new_arc()
}
