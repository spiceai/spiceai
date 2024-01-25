#![allow(clippy::unwrap_used)]

use super::DataSource;

use std::time::Duration;

use crate::auth::Auth;
use arrow::record_batch::RecordBatch;
use futures::executor::block_on;
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
        tracing::debug!("Getting all data for dataset: {}", dataset);
        let flight_record_batch_stream_result = block_on(
            self.spice_client
                .query(format!("SELECT * FROM {dataset}").as_str()),
        );
        tracing::debug!(
            "Got flight record batch stream result: {:?}",
            flight_record_batch_stream_result
        );
        let mut flight_record_batch_stream = match flight_record_batch_stream_result {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!("Failed to query with spice client: {:?}", error);
                return vec![];
            }
        };

        tracing::debug!("Reading flight record batch stream");
        let mut result_data = vec![];
        while let Some(batch) = block_on(flight_record_batch_stream.next()) {
            match batch {
                Ok(batch) => {
                    tracing::debug!("Got batch: {:?}", batch);
                    result_data.push(batch);
                }
                Err(error) => {
                    tracing::error!("Failed to read batch from spice client: {:?}", error);
                    continue;
                }
            };
        }

        tracing::debug!("Returning result data: {:?}", result_data);
        result_data
    }

    fn new<T: Auth>(auth: T) -> Self
    where
        Self: Sized,
    {
        SpiceAI {
            spice_client: block_on(Client::new(&auth.get_token())).unwrap(),
            sleep_duration: Duration::from_secs(1),
        }
    }
}
