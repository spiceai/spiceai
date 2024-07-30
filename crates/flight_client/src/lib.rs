/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::task::Poll;

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
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::IntoStreamingRequest;

pub mod tls;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to connect to server: {source}"))]
    UnableToConnectToServer { source: tls::Error },

    #[snafu(display("Invalid metadata value: {source}"))]
    InvalidMetadata {
        source: tonic::metadata::errors::InvalidMetadataValue,
    },

    #[snafu(display("Unable to perform handshake: {source}"))]
    UnableToPerformHandshake { source: tonic::Status },

    #[snafu(display("Unable to convert metadata to string: {source}"))]
    UnableToConvertMetadataToString {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display("Unable to query: {source}"))]
    UnableToQuery {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to publish: {source}"))]
    UnableToPublish { source: tonic::Status },

    #[snafu(display("Unauthorized"))]
    Unauthorized {},

    #[snafu(display("Permission denied"))]
    PermissionDenied {},

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
    url: String,
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
            url: url.to_string(),
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
            .map_err(map_tonic_error_to_user_friendly_error)?
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
                .map_err(map_tonic_error_to_user_friendly_error)?
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
        self.authenticate_basic_token().await?;

        let flight_descriptor = FlightDescriptor::new_path(vec![dataset_path.to_string()]);
        let subscription_request =
            stream::iter(vec![FlightData::new().with_descriptor(flight_descriptor)].into_iter());

        let mut req = subscription_request.into_streaming_request();
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

        let (_md, response_stream, _ext) = self
            .flight_client
            .clone()
            .do_exchange(req)
            .await
            .map_err(map_tonic_error_to_user_friendly_error)?
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
        self.authenticate_basic_token().await?;

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
        let auth_header_value = match self.token.clone() {
            Some(token) => format!("Bearer {token}")
                .parse()
                .context(InvalidMetadataSnafu)?,
            None => {
                return UnauthorizedSnafu.fail();
            }
        };

        publish_request
            .metadata_mut()
            .insert("authorization", auth_header_value);

        let resp = match self.flight_client.clone().do_put(publish_request).await {
            Ok(resp) => resp,
            Err(e) => match e.code() {
                tonic::Code::PermissionDenied => PermissionDeniedSnafu.fail(),
                _ => return Err(e).context(UnableToPublishSnafu),
            }?,
        };

        let resp = resp.into_inner();

        // Wait for the server to acknowledge the data
        resp.for_each(|_| async {}).await;

        Ok(())
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

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn username(&self) -> &str {
        &self.username
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_tonic_error_to_user_friendly_error(e: tonic::Status) -> Error {
    if let Some(message) = e.message().split('\n').next() {
        return Error::UnableToQuery {
            source: message.to_string().into(),
        };
    }
    Error::UnableToQuery {
        source: e.message().into(),
    }
}
