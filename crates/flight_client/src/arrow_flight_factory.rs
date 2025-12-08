use crate::{ArrowFlightSnafu, UnableToConnectToServerSnafu, tls};
use snafu::ResultExt;

/// Makes an `arrow_flight::FlightClient` with optional authorization header
pub async fn make_arrow_flight_client(
    endpoint: &str,
    api_key: Option<String>,
) -> crate::Result<arrow_flight::FlightClient> {
    let flight_channel = tls::new_tls_flight_channel(endpoint)
        .await
        .context(UnableToConnectToServerSnafu)?;

    let mut client = arrow_flight::FlightClient::new(flight_channel);

    if let Some(api_key) = api_key {
        client
            .add_header("authorization", format!("Bearer {api_key}").as_str())
            .context(ArrowFlightSnafu)?;
    }

    Ok(client)
}
