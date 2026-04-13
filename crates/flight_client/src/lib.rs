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

use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;
use std::task::Poll;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::HandshakeRequest;
use arrow_flight::decode::FlightDataDecoder;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use futures::{TryStreamExt, ready, stream};
use secrecy::ExposeSecret;
use secrecy::SecretString;
use snafu::prelude::*;
use std::error::Error as StdError;
use tonic::IntoRequest;
use tonic::IntoStreamingRequest;
use tonic::transport::Channel;

pub mod arrow_flight_factory;
pub mod cookie;
pub mod tls;

pub const MAX_ENCODING_MESSAGE_SIZE: usize = 100 * 1024 * 1024;
pub const MAX_DECODING_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

#[derive(Debug)]
pub struct TonicStatusError(Box<tonic::Status>);

impl From<tonic::Status> for TonicStatusError {
    fn from(status: tonic::Status) -> Self {
        TonicStatusError(Box::new(status))
    }
}

impl std::fmt::Display for TonicStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = TonicStatusCode::from(self.0.code());
        let message = TonicStatusMessage::from(self.0.message());
        let source = self.0.source();

        match (source, message.clone()) {
            (Some(source), TonicStatusMessage::TransportError) => write!(f, "{message} {source}"),
            (None, TonicStatusMessage::TransportError) => write!(f, "{message}"),
            (None, TonicStatusMessage::Unmatched(message)) => write!(f, "{code}. {message}"),
            (Some(source), TonicStatusMessage::Unmatched(message)) => {
                write!(f, "{code}. {message} {source}")
            }
        }
    }
}

impl std::error::Error for TonicStatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

#[derive(Debug)]
pub struct TonicStatusCode(tonic::Code);

impl From<tonic::Code> for TonicStatusCode {
    fn from(code: tonic::Code) -> Self {
        TonicStatusCode(code)
    }
}

impl std::fmt::Display for TonicStatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            tonic::Code::Unknown => write!(f, "An unknown error occurred"),
            tonic::Code::Internal => write!(f, "An internal error occurred"),
            _ => write!(f, "{}", self.0),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TonicStatusMessage {
    TransportError,
    Unmatched(String),
}

impl From<&str> for TonicStatusMessage {
    fn from(message: &str) -> Self {
        match message {
            "transport error" => TonicStatusMessage::TransportError,
            _ => TonicStatusMessage::Unmatched(message.to_string()),
        }
    }
}

impl std::fmt::Display for TonicStatusMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TonicStatusMessage::TransportError => write!(
                f,
                "A network error occurred. Check the network connection/server configuration, and try again."
            ),
            TonicStatusMessage::Unmatched(message) => write!(f, "{message}"),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to server: TLS error. {source} Ensure the flight endpoint is valid and reachable."
    ))]
    UnableToConnectToServer { source: tls::Error },

    #[snafu(display("Authentication failed. {source} Ensure the credentials are valid."))]
    InvalidMetadata {
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    #[snafu(display("Failed to connect to Flight server: Handshake failed. {source}"))]
    UnableToPerformHandshake { source: TonicStatusError },

    #[snafu(display(
        "An unexpected error occurred. Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToConvertMetadataToString {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display(
        "Failed to get schema. {source} Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToConvertSchema { source: arrow::error::ArrowError },

    #[snafu(display("Query execution failed. {source}"))]
    UnableToQuery {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to publish data to flight endpoint. {source}"))]
    UnableToPublish { source: TonicStatusError },

    #[snafu(display("Unauthorized. Verify the credentials are configured correctly."))]
    Unauthorized {},

    #[snafu(display("Permission denied. Ensure the credentials have the required permissions."))]
    PermissionDenied {},

    #[snafu(display(
        "No endpoints found. Ensure the endpoint is configured and the server is running."
    ))]
    NoEndpointsFound,

    #[snafu(display("Connection is reset by the server. Please retry the request. {source}"))]
    ConnectionReset { source: TonicStatusError },

    #[snafu(display("Flight connection error: {source}"))]
    ArrowFlightError {
        source: arrow_flight::error::FlightError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub enum Credentials {
    // Basic authentication used to exchange for a Bearer token
    UsernamePassword {
        username: Arc<str>,
        password: Arc<SecretString>,
    },
    // Anonymous access
    Anonymous,
    // An existing bearer token
    Bearer {
        token: Arc<SecretString>,
        prefix: bool, // whether this token requires the 'Bearer ' prefix, or if it is set to the 'authorization' header verbatim
    },
}

struct Token {
    value: Arc<SecretString>,
    bearer: bool,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.bearer {
            write!(f, "Bearer {}", self.value.expose_secret())
        } else {
            write!(f, "{}", self.value.expose_secret())
        }
    }
}

impl Token {
    #[must_use]
    fn new(value: &str, bearer: bool) -> Self {
        Token {
            value: Arc::new(SecretString::new(value.into())),
            bearer,
        }
    }
}

impl Credentials {
    #[must_use]
    pub fn new(username: &str, password: SecretString) -> Self {
        Credentials::UsernamePassword {
            username: username.into(),
            password: password.into(),
        }
    }

    #[must_use]
    pub fn anonymous() -> Self {
        Credentials::Anonymous
    }
}

/// Apache Arrow Flight client for interacting with Apache Arrow Flight services.
///
/// This client is cheap to clone. Most fields are wrapped in `Arc`, and the `FlightServiceClient` is
/// also designed to be cheap to clone.
#[derive(Debug, Clone)]
pub struct FlightClient {
    client: FlightServiceClient<Channel>,
    credentials: Credentials,
    url: Arc<str>,
    metadata: Option<tonic::metadata::MetadataMap>,
}

impl FlightClient {
    /// Creates a new instance of `FlightClient`.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to connect to.
    /// * `credentials` - The credentials to use for authentication.
    /// * `metadata` - Optional metadata to include with requests.
    /// * `ca_certificate_path` - Optional path to a CA certificate file (PEM format)
    ///   for TLS verification. If not provided, system certificates will be used.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to create the `FlightClient`.
    pub async fn try_new(
        url: Arc<str>,
        credentials: Credentials,
        metadata: Option<tonic::metadata::MetadataMap>,
        ca_certificate_path: Option<&std::path::Path>,
    ) -> Result<Self> {
        let flight_channel = tls::new_tls_flight_channel(&url, ca_certificate_path)
            .await
            .context(UnableToConnectToServerSnafu)?;

        Ok(FlightClient {
            client: FlightServiceClient::new(flight_channel)
                .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
                .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
            credentials,
            url,
            metadata,
        })
    }

    /// Overrides the metadata for the flight client.
    #[must_use]
    pub fn with_metadata(mut self, metadata: tonic::metadata::MetadataMap) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Overrides default maximum message size for encoding and decoding.
    #[must_use]
    pub fn with_max_message_size(
        mut self,
        max_encoding_message_size: usize,
        max_decoding_message_size: usize,
    ) -> Self {
        self.client = self
            .client
            .max_encoding_message_size(max_encoding_message_size)
            .max_decoding_message_size(max_decoding_message_size);
        self
    }

    /// Queries the flight service for the schema of the path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path representing the table reference.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_schema(&self, path: Vec<String>) -> Result<Schema> {
        let token = self.authenticate_basic_token().await?;

        let descriptor = FlightDescriptor::new_path(path);
        let mut req = tonic::Request::new(descriptor);

        let auth_header_value = match &token {
            Some(token) => token.to_string().parse().context(InvalidMetadataSnafu)?,
            None => {
                return UnauthorizedSnafu.fail();
            }
        };
        req.metadata_mut()
            .insert("authorization", auth_header_value);
        if let Some(metadata) = &self.metadata {
            for key_and_value in metadata.iter() {
                if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = key_and_value {
                    req.metadata_mut().insert(key, value.clone());
                }
            }
        }

        let schema_result = self
            .client
            .clone()
            .get_schema(req)
            .await
            .map_err(map_tonic_error_to_message)?
            .into_inner();

        Schema::try_from(&schema_result).context(UnableToConvertSchemaSnafu)
    }

    /// Queries the flight service for the schema of the query.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to inspect the schema for.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema inference fails.
    pub async fn get_query_schema(&self, sql: Cow<'_, str>) -> Result<Schema> {
        let token = self.authenticate_basic_token().await?;

        let descriptor = FlightDescriptor::new_cmd(sql.into_owned());
        let mut req = descriptor.into_request();

        let auth_header_value = match &token {
            Some(token) => token.to_string().parse().context(InvalidMetadataSnafu)?,
            None => {
                return UnauthorizedSnafu.fail();
            }
        };
        req.metadata_mut()
            .insert("authorization", auth_header_value);
        if let Some(metadata) = &self.metadata {
            for key_and_value in metadata.iter() {
                if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = key_and_value {
                    req.metadata_mut().insert(key, value.clone());
                }
            }
        }

        let schema_result = self
            .client
            .clone()
            .get_schema(req)
            .await
            .map_err(map_tonic_error_to_message)?
            .into_inner();

        Schema::try_from(&schema_result).context(UnableToConvertSchemaSnafu)
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
    pub async fn query(&self, query: &str) -> Result<FlightRecordBatchStream> {
        let token = self.authenticate_basic_token().await?;

        let descriptor = FlightDescriptor::new_cmd(query.to_string());
        let mut req = descriptor.into_request();

        let auth_header_value = match &token {
            Some(token) => token.to_string().parse().context(InvalidMetadataSnafu)?,
            None => {
                return UnauthorizedSnafu.fail();
            }
        };
        req.metadata_mut()
            .insert("authorization", auth_header_value);
        if let Some(metadata) = &self.metadata {
            for key_and_value in metadata.iter() {
                if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = key_and_value {
                    req.metadata_mut().insert(key, value.clone());
                }
            }
        }

        let info = self
            .client
            .clone()
            .get_flight_info(req)
            .await
            .map_err(map_tonic_error_to_message)?
            .into_inner();

        let ep = info.endpoint[0].clone();
        if let Some(ticket) = ep.ticket {
            let mut req = ticket.into_request();
            let auth_header_value = match token {
                Some(token) => token.to_string().parse().context(InvalidMetadataSnafu)?,
                None => {
                    return UnauthorizedSnafu.fail();
                }
            };
            req.metadata_mut()
                .insert("authorization", auth_header_value);
            if let Some(metadata) = &self.metadata {
                for key_and_value in metadata.iter() {
                    if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = key_and_value {
                        req.metadata_mut().insert(key, value.clone());
                    }
                }
            }

            let (md, response_stream, _ext) = self
                .client
                .clone()
                .do_get(req)
                .await
                .map_err(map_tonic_error_to_message)?
                .into_parts();

            return Ok(FlightRecordBatchStream::new_from_flight_data(
                response_stream.map_err(|status| FlightError::Tonic(Box::new(status))),
            )
            .with_headers(md));
        }

        NoEndpointsFoundSnafu.fail()
    }

    /// Subscribes to a datastream via the `DoExchange` Flight method.
    ///
    /// # Arguments
    ///
    /// * `dataset_path` - The dataset to subscribe to.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset is not available for subscription.
    pub async fn subscribe(&mut self, dataset_path: &str) -> Result<FlightDataDecoder> {
        let token = self.authenticate_basic_token().await?;

        let flight_descriptor = FlightDescriptor::new_path(vec![dataset_path.to_string()]);
        let subscription_request =
            stream::iter(vec![FlightData::new().with_descriptor(flight_descriptor)].into_iter());

        let mut req = subscription_request.into_streaming_request();
        let auth_header_value = match token {
            Some(token) => token.to_string().parse().context(InvalidMetadataSnafu)?,
            None => {
                return UnauthorizedSnafu.fail();
            }
        };
        req.metadata_mut()
            .insert("authorization", auth_header_value);
        if let Some(metadata) = &self.metadata {
            for key_and_value in metadata.iter() {
                if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = key_and_value {
                    req.metadata_mut().insert(key, value.clone());
                }
            }
        }

        let (_md, response_stream, _ext) = self
            .client
            .clone()
            .do_exchange(req)
            .await
            .map_err(map_tonic_error_to_message)?
            .into_parts();

        Ok(FlightDataDecoder::new(
            response_stream.map_err(|status| FlightError::Tonic(Box::new(status))),
        ))
    }

    /// Publishes data to a dataset via the `DoPut` Flight method.
    ///
    /// # Arguments
    ///
    /// * `dataset_path` - The dataset to publish to.
    /// * `data` - The data to publish.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be published to the flight source via `DoPut`.
    pub async fn publish(&mut self, dataset_path: &str, data: Vec<RecordBatch>) -> Result<()> {
        let data_stream = futures::stream::iter(data.into_iter().map(Ok));
        self.publish_streaming(dataset_path, data_stream).await
    }

    /// Publishes a stream of data to a dataset via the `DoPut` Flight method.
    ///
    /// # Arguments
    ///
    /// * `dataset_path` - The dataset to publish to.
    /// * `data_stream` - A stream of [`RecordBatch`] items to publish.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be published to the flight source via `DoPut`.
    pub async fn publish_streaming<S>(&mut self, dataset_path: &str, data_stream: S) -> Result<()>
    where
        S: Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
    {
        let token = self.authenticate_basic_token().await?;

        let flight_descriptor = FlightDescriptor::new_path(vec![dataset_path.to_string()]);

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(data_stream.map(|res| res.map_err(FlightError::from)));

        let mut request = Box::pin(flight_data_stream);
        let request_stream = futures::stream::poll_fn(move |cx| {
            Poll::Ready(match ready!(request.poll_next_unpin(cx)) {
                Some(Ok(data)) => Some(data),
                Some(Err(_)) | None => None,
            })
        });

        let mut publish_request = request_stream.into_streaming_request();
        if let Some(token) = token {
            let auth_header_value = token.to_string().parse().context(InvalidMetadataSnafu)?;

            publish_request
                .metadata_mut()
                .insert("authorization", auth_header_value);
        }

        let resp = match self.client.clone().do_put(publish_request).await {
            Ok(resp) => resp,
            Err(e) => match e.code() {
                tonic::Code::PermissionDenied => PermissionDeniedSnafu.fail(),
                _ => return Err(TonicStatusError::from(e)).context(UnableToPublishSnafu),
            }?,
        };

        // Wait for the server to acknowledge the data
        match resp.into_inner().try_collect::<Vec<_>>().await {
            Ok(_) => Ok(()),
            Err(e) => Err(TonicStatusError::from(e)).context(UnableToPublishSnafu),
        }
    }

    async fn authenticate_basic_token(&self) -> Result<Option<Token>> {
        let (username, password) = match &self.credentials {
            Credentials::UsernamePassword { username, password } => {
                (username.as_ref(), password.expose_secret())
            }
            Credentials::Anonymous => return Ok(None),
            Credentials::Bearer {
                token,
                prefix: bearer,
            } => {
                return Ok(Some(Token::new(token.expose_secret(), *bearer)));
            }
        };

        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Bytes::default(),
        };

        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}",));

        let val = format!("Basic {val}")
            .parse()
            .context(InvalidMetadataSnafu)?;
        req.metadata_mut().insert("authorization", val);
        let mut resp = self.client.clone().handshake(req).await.map_err(|e| {
            if is_connection_reset_error(&e) {
                Error::ConnectionReset {
                    source: TonicStatusError::from(e),
                }
            } else {
                Error::UnableToPerformHandshake {
                    source: TonicStatusError::from(e),
                }
            }
        })?;

        let mut token: Option<Token> = None;

        // Consume the response stream before reading the metadata
        let stream = resp.get_mut();
        while let Some(data) = stream.next().await {
            match data {
                Ok(_) => {}
                Err(e) => {
                    if is_connection_reset_error(&e) {
                        return Err(Error::ConnectionReset {
                            source: TonicStatusError::from(e),
                        });
                    }
                    return Err(Error::UnableToPerformHandshake {
                        source: TonicStatusError::from(e),
                    });
                }
            }
        }

        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .context(UnableToConvertMetadataToStringSnafu)?;
            token = Some(Token::new(&auth["Bearer ".len()..], true));
        }

        Ok(token)
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn username(&self) -> Option<&str> {
        let Credentials::UsernamePassword { username, .. } = &self.credentials else {
            return None;
        };
        Some(username)
    }

    pub fn client(&self) -> &FlightServiceClient<Channel> {
        &self.client
    }
}

fn map_tonic_error_to_message(e: tonic::Status) -> Error {
    if is_connection_reset_error(&e) {
        return Error::ConnectionReset {
            source: TonicStatusError::from(e),
        };
    }
    Error::UnableToQuery {
        source: e.message().into(),
    }
}

#[must_use]
pub fn is_connection_reset_error(error: &tonic::Status) -> bool {
    match error.code() {
        tonic::Code::Internal | tonic::Code::Cancelled | tonic::Code::Unknown => {
            let error_message = error.message().to_lowercase();
            if error_message.contains("operation was canceled")
                || error_message.contains("http2 error")
                || error_message.contains("grpc-status header missing")
                || error_message.contains("received message with invalid compression flag")
                || error_message.contains("error reading a body from connection")
                || error_message.contains("transport error")
            {
                return true;
            }
            false
        }
        _ => false,
    }
}
