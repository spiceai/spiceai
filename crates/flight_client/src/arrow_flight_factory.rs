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
