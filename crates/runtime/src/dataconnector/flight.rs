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
    match error {
        FlightError::Arrow(arrow_error) => format!("{ctx}: Arrow error occurred: {arrow_error}",),
        FlightError::NotYetImplemented(message) => {
            format!("{ctx}: Not yet implemented: {message}",)
        }
        FlightError::Tonic(status) => {
            format!(
                "{ctx}: Tonic error occurred. code: {}, message: {}. Full: {}",
                status.code(),
                status.message(),
                status
            )
        }
        FlightError::ProtocolError(message) => {
            format!("{ctx}: Protocol error occurred: {message}",)
        }
        FlightError::DecodeError(message) => {
            format!("{ctx}: Decode error occurred: {message}")
        }
        FlightError::ExternalError(error) => {
            format!("{ctx}: External error occurred: {error}")
        }
    }
}
