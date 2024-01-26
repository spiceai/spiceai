#![allow(clippy::unwrap_used)]

use super::DataSource;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::AuthProvider;
use arrow::record_batch::RecordBatch;
use futures::executor::block_on;
use futures::StreamExt;
use spice_rs::Client;
use tokio::sync::Mutex;

pub struct SpiceAI {
    pub spice_client: Arc<Mutex<Client>>,
    pub sleep_duration: Duration,
}

impl DataSource for SpiceAI {
    fn new(auth_provider: Box<dyn AuthProvider>) -> Self
    where
        Self: Sized,
    {
        SpiceAI {
            spice_client: Arc::new(Mutex::new(
                block_on(Client::new(&auth_provider.get_token())).unwrap(),
            )),
            sleep_duration: Duration::from_secs(10),
        }
    }

    fn get_all_data_refresh_interval(&self, _dataset: &str) -> Option<Duration> {
        Some(self.sleep_duration)
    }

    fn get_all_data(
        &self,
        dataset: &str,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>> {
        let client = self.spice_client.clone();
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
                    tracing::error!("Failed to query with spice client: {:?}", error);
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
                        tracing::error!("Failed to read batch from spice client: {:?}", error);
                        return result_data;
                    }
                };
            }

            result_data
        })
    }
}
