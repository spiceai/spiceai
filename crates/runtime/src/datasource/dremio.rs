use super::DataSource;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::AuthProvider;
use arrow::record_batch::RecordBatch;
use flight_client::FlightClient;
use futures::StreamExt;
use tokio::sync::Mutex;

pub struct Dremio {
    pub dremio_client: Arc<Mutex<FlightClient>>,
    pub sleep_duration: Duration,
}

impl DataSource for Dremio {
    fn new(
        auth_provider: Box<dyn AuthProvider>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let dremio_flight_client = FlightClient::new(
                "http://dremio-4mimamg7rdeve.eastus.cloudapp.azure.com:32010",
                auth_provider.get_username(),
                auth_provider.get_password(),
            )
            .await
            .map_err(|e| super::Error::UnableToCreateDataSource { source: e.into() })?;
            Ok(Dremio {
                dremio_client: Arc::new(Mutex::new(dremio_flight_client)),
                sleep_duration: Duration::from_secs(10),
            })
        })
    }

    fn get_all_data_refresh_interval(&self, _dataset: &str) -> Option<Duration> {
        Some(self.sleep_duration)
    }

    fn get_all_data(
        &self,
        dataset: &str,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>> {
        let client = self.dremio_client.clone();
        let dataset = dataset.to_string();
        Box::pin(async move {
            let flight_record_batch_stream_result = client
                .lock()
                .await
                .query(format!("SELECT * FROM {dataset}").as_str())
                .await;

            let mut flight_record_batch_stream = match flight_record_batch_stream_result {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::error!("Failed to query with dremio client: {:?}", error);
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
                        tracing::error!("Failed to read batch from dremio client: {:?}", error);
                        return result_data;
                    }
                };
            }

            result_data
        })
    }
}
