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

use std::future::Future;
use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use flight_client::FlightClient;
use futures::StreamExt;

#[derive(Debug, Clone)]
pub struct Flight {
    pub client: FlightClient,
}

impl Flight {
    #[must_use]
    pub(crate) fn new(flight_client: FlightClient) -> Self
    where
        Self: Sized,
    {
        Flight {
            client: flight_client,
        }
    }

    pub(crate) fn get_all_data(
        &self,
        dataset_path: &str,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>> {
        let mut client = self.client.clone();
        let dataset_path = dataset_path.to_owned();
        Box::pin(async move {
            let flight_record_batch_stream_result = client
                .query(format!("SELECT * FROM {dataset_path}").as_str())
                .await;

            let mut flight_record_batch_stream = match flight_record_batch_stream_result {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::error!("Failed to query with flight client: {error}",);
                    return vec![];
                }
            };

            let mut result_data = vec![];
            while let Some(batch) = flight_record_batch_stream.next().await {
                match batch {
                    Ok(batch) => {
                        result_data.push(batch);
                    }
                    Err(error) => {
                        render_flight_error(error, "Failed to read batch from flight client");
                        return result_data;
                    }
                };
            }

            result_data
        })
    }
}

// Render a user-friendly message for when a FlightError occurs.
pub fn render_flight_error(error: FlightError, ctx: &str) -> String {
    tracing::trace!("{ctx}: {error}");
    match error {
        FlightError::Arrow(arrow_error) => {
            let error_msg = arrow_error.to_string();
            // `ArrowError`s are formatted as `<error type>: <internal details (optional)>`.
            let public_message = if let Some(index) = error_msg.find(':') {
                let (user_msg, _internal_desc) = error_msg.split_at(index + 1);
                user_msg
            } else {
                &error_msg
            };
            format!("{ctx}: Arrow error occurred: {public_message}")
        }
        FlightError::NotYetImplemented(_message) => {
            format!("{ctx}: Not yet implemented")
        }
        FlightError::Tonic(status) => {
            format!(
                "{ctx}: Tonic error occurred with status code: {}",
                status.code(),
            )
        }
        FlightError::ProtocolError(_message) => {
            format!("{ctx}: Protocol error occurred")
        }
        FlightError::DecodeError(_message) => {
            format!("{ctx}: Failed to decode flight data")
        }
        FlightError::ExternalError(_error) => {
            format!("{ctx}: External error occurred")
        }
    }
}
