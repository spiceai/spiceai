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

use async_trait::async_trait;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest_eventsource::{Event, RequestBuilderExt};
use snafu::ResultExt;
use tokio::time::sleep;

use crate::{Runtime, http::v1::run_sql};

use super::{
    Error, UnableToCreateDataConnectorSnafu,
    transport::{ManagementEvent, SqlEventData, Transport, TransportProtocol},
};

const DEFAULT_RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_DELAY_SECS: u64 = 60;

/// HTTP Server-Sent Events (SSE) transport implementation for Spice Connect
pub struct HttpSseTransport {
    runtime: Arc<Runtime>,
    endpoint: String,
    api_key: String,
}

impl HttpSseTransport {
    pub fn new(runtime: Arc<Runtime>, endpoint: String, api_key: String) -> Self {
        Self {
            runtime,
            endpoint,
            api_key,
        }
    }

    async fn connect_and_listen(&self) -> Result<(), Error> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.api_key))
                .boxed()
                .context(UnableToCreateDataConnectorSnafu)?,
        );

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;

        let request = client.get(&self.endpoint).headers(headers);

        let mut event_source = request
            .eventsource()
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;

        tracing::info!("Connected to Spice Cloud via HTTP SSE: {}", self.endpoint);

        while let Some(event) = event_source.next().await {
            match event {
                Ok(Event::Open) => {
                    tracing::debug!("HTTP SSE connection opened");
                }
                Ok(Event::Message(message)) => {
                    if let Err(e) = self.handle_message(message.data).await {
                        tracing::error!("Error handling SSE message: {e}");
                    }
                }
                Err(e) => {
                    event_source.close();
                    return Err(Error::UnableToCreateDataConnector {
                        source: format!("SSE error: {e}").into(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&self, data: String) -> Result<(), Error> {
        tracing::debug!("Received SSE message: {data}");

        let event: ManagementEvent = serde_json::from_str(&data)
            .boxed()
            .context(UnableToCreateDataConnectorSnafu)?;

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
                let row_count: usize = batches
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum();
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
impl Transport for HttpSseTransport {
    async fn start(&self) -> Result<(), Error> {
        let mut reconnect_delay = DEFAULT_RECONNECT_DELAY_SECS;

        loop {
            match self.connect_and_listen().await {
                Ok(()) => {
                    tracing::info!("HTTP SSE connection closed normally");
                    reconnect_delay = DEFAULT_RECONNECT_DELAY_SECS;
                }
                Err(e) => {
                    tracing::warn!(
                        "HTTP SSE connection error: {e}. Reconnecting in {reconnect_delay}s"
                    );
                    sleep(Duration::from_secs(reconnect_delay)).await;

                    // Exponential backoff with max delay
                    reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY_SECS);
                }
            }
        }
    }

    fn protocol(&self) -> TransportProtocol {
        TransportProtocol::HttpSse
    }
}
