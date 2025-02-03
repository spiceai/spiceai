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

use std::borrow::Cow;
use std::sync::Arc;
use std::task::Poll;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightDataDecoder;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::HandshakeRequest;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use futures::{ready, stream, TryStreamExt};
use snafu::prelude::*;
use std::error::Error as StdError;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::IntoStreamingRequest;

pub mod tls;

pub const MAX_ENCODING_MESSAGE_SIZE: usize = 100 * 1024 * 1024;
pub const MAX_DECODING_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

#[derive(Debug)]
pub struct TonicStatusError(tonic::Status);

impl From<tonic::Status> for TonicStatusError {
    fn from(status: tonic::Status) -> Self {
        TonicStatusError(status)
    }
}

impl std::fmt::Display for TonicStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = TonicStatusCode::from(self.0.code());
        let message = TonicStatusMessage::from(self.0.message());
        let source = self.0.source();

        match (source, message.clone()) {
            (Some(source), TonicStatusMessage::TransportError) => write!(f, "{message}\n{source}"),
            (None, TonicStatusMessage::TransportError) => write!(f, "{message}"),
            (None, TonicStatusMessage::Unmatched(message)) => write!(f, "{code}.\n{message}"),
            (Some(source), TonicStatusMessage::Unmatched(message)) => {
                write!(f, "{code}.\n{message}\n{source}")
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
            TonicStatusMessage::TransportError => write!(f, "A network error occurred. Check the network connection/server configuration, and try again."),
            TonicStatusMessage::Unmatched(message) => write!(f, "{message}")
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to server: TLS error.\n{source}\nEnsure the flight endpoint is valid and reachable."
    ))]
    UnableToConnectToServer { source: tls::Error },

    #[snafu(display("Authentication failed.\n{source}\nEnsure the credentials are valid."))]
    InvalidMetadata {
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    #[snafu(display("Failed to connect to Flight server: Handshake failed.\n{source}"))]
    UnableToPerformHandshake { source: TonicStatusError },

    #[snafu(display(
        "An unexpected error occurred. Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToConvertMetadataToString {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display("Failed to get schema.\n{source}\nReport a bug to request support: https://github.com/spiceai/spiceai/issues"))]
    UnableToConvertSchema { source: arrow::error::ArrowError },

    #[snafu(display("Query execution failed.\n{source}"))]
    UnableToQuery {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to publish data to flight endpoint.\n{source}"))]
    UnableToPublish { source: TonicStatusError },

    #[snafu(display("Unauthorized. Verify the credentials are configured correctly."))]
    Unauthorized {},

    #[snafu(display("Permission denied. Ensure the credentials have the required permissions."))]
    PermissionDenied {},

    #[snafu(display(
        "No endpoints found. Ensure the endpoint is configured and the server is running."
    ))]
    NoEndpointsFound,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub enum Credentials {
    UsernamePassword {
        username: Arc<str>,
        password: Arc<str>,
    },
    Anonymous,
}

impl Credentials {
    #[must_use]
    pub fn new(username: &str, password: &str) -> Self {
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
    flight_client: FlightServiceClient<Channel>,
    credentials: Credentials,
    url: Arc<str>,
    metadata: Option<tonic::metadata::MetadataMap>,
}

impl FlightClient {
    /// Creates a new instance of `FlightClient`.
    ///
    /// # Arguments
    ///
    /// * `username` - The username to use.
    /// * `password` - The password to use.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to create the `FlightClient`.
    pub async fn try_new(
        url: Arc<str>,
        credentials: Credentials,
        metadata: Option<tonic::metadata::MetadataMap>,
    ) -> Result<Self> {
        let flight_channel = tls::new_tls_flight_channel(&url)
            .await
            .context(UnableToConnectToServerSnafu)?;

        Ok(FlightClient {
            flight_client: FlightServiceClient::new(flight_channel)
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
            Some(token) => format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?,
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
            .flight_client
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
            Some(token) => format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?,
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
            .flight_client
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
            Some(token) => format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?,
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
            .flight_client
            .clone()
            .get_flight_info(req)
            .await
            .map_err(map_tonic_error_to_message)?
            .into_inner();

        let ep = info.endpoint[0].clone();
        if let Some(ticket) = ep.ticket {
            let mut req = ticket.into_request();
            let auth_header_value = match token {
                Some(token) => format!("Bearer {token}")
                    .parse()
                    .context(InvalidMetadataSnafu)?,
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
                .flight_client
                .clone()
                .do_get(req)
                .await
                .map_err(map_tonic_error_to_message)?
                .into_parts();

            return Ok(FlightRecordBatchStream::new_from_flight_data(
                response_stream.map_err(FlightError::Tonic),
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
            Some(token) => format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?,
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
            .flight_client
            .clone()
            .do_exchange(req)
            .await
            .map_err(map_tonic_error_to_message)?
            .into_parts();

        Ok(FlightDataDecoder::new(
            response_stream.map(|r| r.map_err(FlightError::Tonic)),
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
        let token = self.authenticate_basic_token().await?;

        let flight_descriptor = FlightDescriptor::new_path(vec![dataset_path.to_string()]);

        let converted_input_stream = futures::stream::iter(data.into_iter().map(Ok));

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(converted_input_stream);

        let mut request = Box::pin(flight_data_stream); // Pin to heap
        let request_stream = futures::stream::poll_fn(move |cx| {
            Poll::Ready(match ready!(request.poll_next_unpin(cx)) {
                Some(Ok(data)) => Some(data),
                Some(Err(_)) | None => None,
            })
        });

        let mut publish_request = request_stream.into_streaming_request();
        if let Some(token) = token {
            let auth_header_value = format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?;

            publish_request
                .metadata_mut()
                .insert("authorization", auth_header_value);
        };

        let resp = match self.flight_client.clone().do_put(publish_request).await {
            Ok(resp) => resp,
            Err(e) => match e.code() {
                tonic::Code::PermissionDenied => PermissionDeniedSnafu.fail(),
                _ => return Err(TonicStatusError::from(e)).context(UnableToPublishSnafu),
            }?,
        };

        let resp = resp.into_inner();

        // Wait for the server to acknowledge the data
        resp.for_each(|_| async {}).await;

        Ok(())
    }

    async fn authenticate_basic_token(&self) -> Result<Option<String>> {
        let Credentials::UsernamePassword { username, password } = &self.credentials else {
            return Ok(None);
        };

        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Bytes::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let val = format!("Basic {val}")
            .parse()
            .context(InvalidMetadataSnafu)?;
        req.metadata_mut().insert("authorization", val);
        let resp = self
            .flight_client
            .clone()
            .handshake(req)
            .await
            .map_err(TonicStatusError::from)
            .context(UnableToPerformHandshakeSnafu)?;
        let mut token: Option<String> = None;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .context(UnableToConvertMetadataToStringSnafu)?;
            token = Some(auth["Bearer ".len()..].to_string());
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
}

#[allow(clippy::needless_pass_by_value)]
fn map_tonic_error_to_message(e: tonic::Status) -> Error {
    Error::UnableToQuery {
        source: e.message().into(),
    }
}
