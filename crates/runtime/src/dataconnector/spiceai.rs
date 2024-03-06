use arrow_flight::decode::DecodedPayload;
use async_stream::stream;
use async_trait::async_trait;
use flight_client::FlightClient;
use futures::StreamExt;
use futures_core::stream::BoxStream;
use snafu::prelude::*;
use std::borrow::Borrow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use flight_datafusion::FlightTable;

use crate::datapublisher::{AddDataResult, DataPublisher};
use crate::dataupdate::{DataUpdate, UpdateType};
use crate::info_spaced;
use crate::secrets::Secret;
use crate::tracers::SpacedTracer;

use super::{flight::Flight, DataConnector};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse SpiceAI dataset path: {dataset_path}"))]
    UnableToParseDatasetPath { dataset_path: String },

    #[snafu(display("Unable to publish data to SpiceAI: {source}"))]
    UnableToPublishData { source: flight_client::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SpiceAI {
    flight: Flight,
    spaced_trace: Arc<SpacedTracer>,
}

#[async_trait]
impl DataConnector for SpiceAI {
    fn new(
        secret: Secret,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        let default_flight_url = if cfg!(feature = "dev") {
            "https://dev-flight.spiceai.io".to_string()
        } else {
            "https://flight.spiceai.io".to_string()
        };
        Box::pin(async move {
            let url: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .unwrap_or(default_flight_url);
            tracing::trace!("Connecting to SpiceAI with flight url: {url}");
            let flight_client =
                FlightClient::new(url.as_str(), "", secret.get("key").unwrap_or_default())
                    .await
                    .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;
            let flight = Flight::new(flight_client);
            Ok(Self {
                flight,
                spaced_trace: Arc::new(SpacedTracer::new(Duration::from_secs(15))),
            })
        })
    }

    fn supports_data_streaming(&self, _dataset: &Dataset) -> bool {
        true
    }

    /// Returns a stream of `DataUpdates` for the given dataset.
    fn stream_data_updates<'a>(&self, dataset: &Dataset) -> BoxStream<'a, DataUpdate> {
        let flight = &self.flight.client;
        let mut flight = flight.clone();
        let spice_dataset_path = Self::spice_dataset_path(dataset);
        Box::pin(stream! {
          let mut stream = match flight.subscribe(&spice_dataset_path).await {
            Ok(stream) => stream,
            Err(error) => {
              tracing::error!("Unable to subscribe to {spice_dataset_path}: {:?}", error);
              return;
            }
          };
          loop {
            match stream.next().await {
              Some(Ok(decoded_data)) => match decoded_data.payload {
                  DecodedPayload::RecordBatch(batch) => {
                      yield DataUpdate {
                        data: vec![batch],
                        update_type: UpdateType::Append,
                      };
                  }
                  _ => {
                      continue;
                  }
                }
              Some(Err(error)) => {
                tracing::error!("Error in subscription to {spice_dataset_path}: {:?}", error);
                continue;
              },
              None => {
                tracing::error!("Flight stream ended for {spice_dataset_path}");
                break;
              }
            };
          }
        })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let spice_dataset_path = Self::spice_dataset_path(dataset);
        self.flight.get_all_data(&spice_dataset_path)
    }

    fn get_data_publisher(&self) -> Option<Box<dyn DataPublisher>> {
        Some(Box::new(self.clone()))
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        let dataset_path = Self::spice_dataset_path(dataset);

        let provider = FlightTable::new(self.flight.client.clone(), dataset_path).await;

        match provider {
            Ok(provider) => Ok(Arc::new(provider)),
            Err(error) => Err(super::Error::UnableToGetTableProvider {
                source: error.into(),
            }),
        }
    }
}

impl DataPublisher for SpiceAI {
    /// Adds data ingested locally back to the source.
    fn add_data(&self, dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let dataset_path = Self::spice_dataset_path(dataset);
        let mut client = self.flight.client.clone();
        let spaced_trace = Arc::clone(&self.spaced_trace);
        Box::pin(async move {
            tracing::debug!("Adding data to {dataset_path}");

            client
                .publish(&dataset_path, data_update.data)
                .await
                .context(UnableToPublishDataSnafu)?;

            info_spaced!(spaced_trace, "Data published to {}", &dataset_path);

            Ok(())
        })
    }

    fn name(&self) -> &str {
        "SpiceAI"
    }
}

impl SpiceAI {
    /// Parses a dataset path from a Spice AI dataset definition.
    ///
    /// Spice AI datasets have several possible formats for `dataset.path()`:
    /// 1. `<org>/<app>/datasets/<dataset_name>` or `spice.ai/<org>/<app>/datasets/<dataset_name>`.
    /// 2. `<org>/<app>` or `spice.ai/<org>/<app>`.
    /// 3. `some.blessed.dataset` or `spice.ai/some.blessed.dataset`.
    ///
    /// The second format is a shorthand for the first format, where the dataset name
    /// is the same as the local table name specified in `name`.
    ///
    /// The third format is a path to a "blessed" Spice AI dataset (i.e. a dataset that is
    /// defined and provided by Spice). If the dataset path does not match the first two formats,
    /// then it is assumed to be a path to a blessed dataset.
    ///
    /// This function returns the full dataset path for the given dataset as you would query for it in Spice.
    /// i.e. `<org>.<app>.<dataset_name>`
    #[allow(clippy::match_same_arms)]
    fn spice_dataset_path<T: Borrow<Dataset>>(dataset: T) -> String {
        let dataset = dataset.borrow();
        let path = dataset.path();
        let path = path.trim_start_matches("spice.ai/");
        let path_parts: Vec<&str> = path.split('/').collect();

        match path_parts.as_slice() {
            [org, app] => format!("{org}.{app}.{dataset_name}", dataset_name = dataset.name),
            [org, app, "datasets", dataset_name] => format!("{org}.{app}.{dataset_name}"),
            [org, app, dataset_name] => format!("{org}.{app}.{dataset_name}"),
            _ => path.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spice_dataset_path() {
        let tests = vec![
            (
                "spiceai:spice.ai/lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spiceai:spice.ai/lukekim/demo/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spiceai:lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spiceai:lukekim/demo/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spice.ai/lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spice.ai/lukekim/demo/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            ("lukekim/demo/my_data".to_string(), "lukekim.demo.my_data"),
            ("eth.recent_blocks".to_string(), "eth.recent_blocks"),
        ];

        for (input, expected) in tests {
            let dataset = Dataset::new(input.clone(), "bar".to_string());
            assert_eq!(SpiceAI::spice_dataset_path(dataset), expected, "{input}");
        }
    }
}
