use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_flight::HandshakeRequest;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use snafu::prelude::*;
use tonic::transport::Channel;
use tonic::IntoRequest;

mod tls;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to connect to server"))]
    UnableToConnectToServer { source: tls::Error },

    #[snafu(display("Invalid metadata value"))]
    InvalidMetadata {
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    #[snafu(display("Unable to perform handshake"))]
    UnableToPerformHandshake { source: tonic::Status },

    #[snafu(display("Unable to convert metadata to string"))]
    UnableToConvertMetadataToString {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display("Unable to query"))]
    UnableToQuery { source: tonic::Status },

    #[snafu(display("Unauthorized"))]
    Unauthorized {},

    #[snafu(display("No endpoints found"))]
    NoEndpointsFound,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct FlightClient {
    token: Option<String>,
    flight_client: FlightServiceClient<Channel>,
    username: String,
    password: String,
}

impl FlightClient {
    /// Creates a new instance of `FlightClient`.
    ///
    /// # Arguments
    ///
    /// * `username` - The username to use.
    /// * `password` - The password to use, if using an API key with Spice then provide it as `password` with an empty username.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to create the `FlightClient`.
    pub async fn new(url: &str, username: &str, password: &str) -> Result<Self> {
        let flight_channel = tls::new_tls_flight_channel(url)
            .await
            .context(UnableToConnectToServerSnafu)?;

        Ok(FlightClient {
            flight_client: FlightServiceClient::new(flight_channel)
                .max_encoding_message_size(100 * 1024 * 1024)
                .max_decoding_message_size(100 * 1024 * 1024),
            token: None,
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Queries the flight service with the specified query.
    ///
    /// # Arguments
    ///
    /// * `query` - The query string.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn query(&mut self, query: &str) -> Result<FlightRecordBatchStream> {
        self.authenticate_basic_token().await?;

        let descriptor = FlightDescriptor::new_cmd(query.to_string());
        let mut req = descriptor.into_request();

        let auth_header_value = match self.token.clone() {
            Some(token) => format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?,
            None => {
                return UnauthorizedSnafu.fail();
            }
        };
        req.metadata_mut()
            .insert("authorization", auth_header_value);

        if let Some(token) = &self.token {
            let val = format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?;
            req.metadata_mut().insert("authorization", val);
        }

        let info = self
            .flight_client
            .clone()
            .get_flight_info(req)
            .await
            .context(UnableToQuerySnafu)?
            .into_inner();

        let ep = info.endpoint[0].clone();
        if let Some(ticket) = ep.ticket {
            let mut req = ticket.into_request();
            let auth_header_value = match self.token.clone() {
                Some(token) => format!("Bearer {token}")
                    .parse()
                    .context(InvalidMetadataSnafu)?,
                None => {
                    return UnauthorizedSnafu.fail();
                }
            };
            req.metadata_mut()
                .insert("authorization", auth_header_value);
            let (md, response_stream, _ext) = self
                .flight_client
                .clone()
                .do_get(req)
                .await
                .context(UnableToQuerySnafu)?
                .into_parts();

            return Ok(FlightRecordBatchStream::new_from_flight_data(
                response_stream.map_err(FlightError::Tonic),
            )
            .with_headers(md));
        }

        NoEndpointsFoundSnafu.fail()
    }

    async fn authenticate_basic_token(&mut self) -> Result<()> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Bytes::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{}:{}", self.username, self.password));
        let val = format!("Basic {val}")
            .parse()
            .context(InvalidMetadataSnafu)?;
        req.metadata_mut().insert("authorization", val);
        let resp = self
            .flight_client
            .clone()
            .handshake(req)
            .await
            .context(UnableToPerformHandshakeSnafu)?;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .context(UnableToConvertMetadataToStringSnafu)?;
            self.token = Some(auth["Bearer ".len()..].to_string());
        }
        Ok(())
    }
}
