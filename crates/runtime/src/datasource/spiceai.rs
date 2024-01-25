#![allow(clippy::unwrap_used)]

use super::DataSource;

use std::time::Duration;

use crate::auth::Auth;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use spice_rs::Client;

pub struct SpiceAI {
    pub spice_client: Client,
    pub sleep_duration: Duration,
}

impl DataSource for SpiceAI {
    fn get_all_data_refresh_interval(&self, _dataset: &str) -> Option<Duration> {
        Some(self.sleep_duration)
    }

    fn get_all_data(&mut self, dataset: &str) -> Vec<RecordBatch> {
        let handle = tokio::runtime::Handle::current();
        let flight_record_batch_stream_result = handle.block_on(
            self.spice_client
                .query(format!("SELECT * FROM {dataset}").as_str()),
        );
        let mut flight_record_batch_stream = match flight_record_batch_stream_result {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!("Failed to query with spice client: {:?}", error);
                return vec![];
            }
        };

        let mut result_data = vec![];
        while let Some(batch) = handle.block_on(flight_record_batch_stream.next()) {
            match batch {
                Ok(batch) => {
                    result_data.push(batch);
                }
                Err(error) => {
                    tracing::error!("Failed to read batch from spice client: {:?}", error);
                    continue;
                }
            };
        }

        result_data
    }

    fn new<T: Auth>(auth: T) -> Self
    where
        Self: Sized,
    {
        let handle = tokio::runtime::Handle::current();
        SpiceAI {
            spice_client: handle.block_on(Client::new(&auth.get_token())).unwrap(),
            sleep_duration: Duration::from_secs(1),
        }
    }
}
