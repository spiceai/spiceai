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

use std::{sync::Arc, time::Duration};

use arrow::array::{Array, RecordBatch, StringArray};
use arrow_flight::decode::DecodedPayload;
use async_trait::async_trait;
use flight_client::{Credentials, FlightClient};
use futures::StreamExt;
use secrecy::SecretString;
use snafu::ResultExt;
use tokio::time::sleep;

use crate::{Runtime, http::v1::run_sql};

use super::{
    Error, UnableToCreateDataConnectorSnafu,
    transport::{ManagementEvent, SqlEventData, Transport, TransportProtocol},
};

const DEFAULT_RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_DELAY_SECS: u64 = 60;
const MANAGEMENT_TABLE: &str = "spice.management.events";

/// Apache Arrow Flight RPC transport implementation for Spice Connect
pub struct FlightTransport {
    runtime: Arc<Runtime>,
    endpoint: String,
    client: FlightClient,
}

impl FlightTransport {
    pub async fn new(
        runtime: Arc<Runtime>,
        endpoint: String,
        api_key: String,
    ) -> Result<Self, Error> {
        // Convert endpoint from grpc:// or grpc+tls:// to https://
        let flight_endpoint = Self::normalize_endpoint(&endpoint);

        let credentials = Credentials::new("", SecretString::new(api_key.into()));

        let client = FlightClient::try_new(Arc::from(flight_endpoint.as_str()), credentials, None)
            .await
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;

        Ok(Self {
            runtime,
            endpoint,
            client,
        })
    }

    fn normalize_endpoint(endpoint: &str) -> String {
        if let Some(stripped) = endpoint.strip_prefix("grpc+tls://") {
            format!("https://{stripped}")
        } else if let Some(stripped) = endpoint.strip_prefix("grpc://") {
            format!("http://{stripped}")
        } else if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            // Assume TLS for raw endpoints with port 443
            if endpoint.contains(":443") {
                format!("https://{endpoint}")
            } else {
                format!("http://{endpoint}")
            }
        } else {
            endpoint.to_string()
        }
    }

    async fn subscribe_and_listen(&self) -> Result<(), Error> {
        tracing::info!(
            "Subscribing to Spice Cloud via Arrow Flight: {}",
            self.endpoint
        );

        // Subscribe to the management events stream
        let mut stream = self
            .client
            .clone()
            .subscribe(MANAGEMENT_TABLE)
            .await
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;

        tracing::info!("Connected to Spice Cloud via Arrow Flight");

        while let Some(decoded_result) = stream.next().await {
            match decoded_result {
                Ok(decoded_data) => match decoded_data.payload {
                    DecodedPayload::None => {}
                    DecodedPayload::Schema(_) => {
                        tracing::debug!("Received schema from Flight stream");
                    }
                    DecodedPayload::RecordBatch(batch) => {
                        if let Err(e) = self.handle_batch(batch).await {
                            tracing::error!("Error handling Flight batch: {e}");
                        }
                    }
                },
                Err(e) => {
                    return Err(Error::UnableToCreateDataConnector {
                        source: format!("Flight stream error: {e}").into(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn handle_batch(&self, batch: RecordBatch) -> Result<(), Error> {
        // Expected schema: event_type (utf8), event_data (utf8)
        let schema = batch.schema();

        let event_type_idx = schema
            .index_of("event_type")
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;
        let event_data_idx = schema
            .index_of("event_data")
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;

        let event_types = batch
            .column(event_type_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::UnableToCreateDataConnector {
                source: "event_type column is not a string array".into(),
            })?;

        let event_data = batch
            .column(event_data_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::UnableToCreateDataConnector {
                source: "event_data column is not a string array".into(),
            })?;

        for row in 0..batch.num_rows() {
            if event_types.is_null(row) {
                continue;
            }

            let event_type = event_types.value(row);
            let data_json = if event_data.is_null(row) {
                "{}"
            } else {
                event_data.value(row)
            };

            // Construct a ManagementEvent from the Flight data
            let event = Self::parse_flight_event(event_type, data_json)?;

            if let Err(e) = self.handle_event(event).await {
                tracing::error!("Error handling event: {e}");
            }
        }

        Ok(())
    }

    fn parse_flight_event(event_type: &str, data_json: &str) -> Result<ManagementEvent, Error> {
        match event_type {
            "sql" => {
                let data: SqlEventData = serde_json::from_str(data_json)
                    .boxed()
                    .context(UnableToCreateDataConnectorSnafu)?;
                Ok(ManagementEvent::Sql { data })
            }
            "ping" => Ok(ManagementEvent::Ping),
            _ => Ok(ManagementEvent::Unknown),
        }
    }

    async fn handle_event(&self, event: ManagementEvent) -> Result<(), Error> {
        match event {
            ManagementEvent::Sql { data } => {
                self.handle_sql_event(data).await?;
            }
            ManagementEvent::Ping => {
                tracing::trace!("Received ping event");
            }
            ManagementEvent::Unknown => {
                tracing::debug!("Received unknown event type");
            }
        }

        Ok(())
    }

    async fn handle_sql_event(&self, data: SqlEventData) -> Result<(), Error> {
        let query = data.query;
        let request_id = data.request_id;

        tracing::info!(
            "Executing SQL query from Spice Connect: {} (request_id: {:?})",
            query,
            request_id
        );

        let df = self.runtime.datafusion();

        // Execute the SQL query
        match run_sql(df, &query, None).await {
            Ok((batches, cache_status)) => {
                let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                tracing::info!(
                    "SQL query executed successfully: {} rows (request_id: {:?}, cache: {:?})",
                    row_count,
                    request_id,
                    cache_status
                );
            }
            Err(e) => {
                tracing::error!(
                    "Error executing SQL query (request_id: {:?}): {e}",
                    request_id
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Transport for FlightTransport {
    async fn start(&self) -> Result<(), Error> {
        let mut reconnect_delay = DEFAULT_RECONNECT_DELAY_SECS;

        loop {
            match self.subscribe_and_listen().await {
                Ok(()) => {
                    tracing::info!("Arrow Flight connection closed normally");
                    reconnect_delay = DEFAULT_RECONNECT_DELAY_SECS;
                }
                Err(e) => {
                    tracing::warn!(
                        "Arrow Flight connection error: {e}. Reconnecting in {reconnect_delay}s"
                    );
                    sleep(Duration::from_secs(reconnect_delay)).await;

                    // Exponential backoff with max delay
                    reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY_SECS);
                }
            }
        }
    }

    fn protocol(&self) -> TransportProtocol {
        TransportProtocol::ArrowFlight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_endpoint() {
        assert_eq!(
            FlightTransport::normalize_endpoint("grpc+tls://flight.spiceai.io:443"),
            "https://flight.spiceai.io:443"
        );

        assert_eq!(
            FlightTransport::normalize_endpoint("grpc://localhost:50051"),
            "http://localhost:50051"
        );

        assert_eq!(
            FlightTransport::normalize_endpoint("flight.spiceai.io:443"),
            "https://flight.spiceai.io:443"
        );

        assert_eq!(
            FlightTransport::normalize_endpoint("localhost:50051"),
            "http://localhost:50051"
        );

        assert_eq!(
            FlightTransport::normalize_endpoint("https://flight.spiceai.io:443"),
            "https://flight.spiceai.io:443"
        );
    }
}
