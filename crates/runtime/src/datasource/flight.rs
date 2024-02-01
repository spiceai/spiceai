use std::future::Future;
use std::pin::Pin;

use crate::auth::AuthProvider;
use arrow::record_batch::RecordBatch;
use flight_client::FlightClient;
use futures::StreamExt;

pub struct Flight {
    pub client: FlightClient,
}

impl Flight {
    #[must_use]
    pub(crate) fn new(
        auth_provider: AuthProvider,
        endpoint: String,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let flight_client = FlightClient::new(
                endpoint.as_str(),
                auth_provider.get_param("username").unwrap_or_default(),
                auth_provider.get_param("password").unwrap_or_default(),
            )
            .await
            .map_err(|e| super::Error::UnableToCreateDataSource { source: e.into() })?;
            Ok(Flight {
                client: flight_client,
            })
        })
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
                    tracing::error!("Failed to query with flight client: {:?}", error);
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
                        tracing::error!("Failed to read batch from flight client: {:?}", error);
                        return result_data;
                    }
                };
            }

            result_data
        })
    }
}
