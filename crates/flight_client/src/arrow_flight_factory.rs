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

use crate::tls::Error;
use crate::{ArrowFlightSnafu, UnableToConnectToServerSnafu};
use snafu::ResultExt;
use std::str::FromStr;
use tonic::transport::{ClientTlsConfig, Endpoint};

/// Makes an `arrow_flight::FlightClient` with optional authorization header
///
/// # Errors
///
/// Returns an error if:
/// - The endpoint string cannot be parsed as a valid URI
/// - The TLS configuration cannot be applied
/// - The connection to the server fails
/// - The authorization header cannot be added
pub async fn make_arrow_flight_client(
    endpoint: &str,
    api_key: Option<String>,
    tls_config: Option<ClientTlsConfig>,
) -> crate::Result<arrow_flight::FlightClient> {
    let mut ep = Endpoint::from_str(endpoint)
        .map_err(|e| Error::UnableToConnectToEndpoint { source: e })
        .context(UnableToConnectToServerSnafu)?;

    if let Some(tls_config) = tls_config {
        ep = ep
            .tls_config(tls_config)
            .map_err(|e| Error::UnableToConnectToEndpoint { source: e })
            .context(UnableToConnectToServerSnafu)?;
    }

    let flight_channel = ep
        .connect()
        .await
        .map_err(|e| Error::UnableToConnectToEndpoint { source: e })
        .context(UnableToConnectToServerSnafu)?;

    let mut client = arrow_flight::FlightClient::new(flight_channel);

    if let Some(api_key) = api_key {
        client
            .add_header("authorization", format!("Bearer {api_key}").as_str())
            .context(ArrowFlightSnafu)?;
    }

    Ok(client)
}
