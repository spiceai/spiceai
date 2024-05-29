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

use super::DataConnector;
use super::DataConnectorFactory;
use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::flight::FlightFactory;
use data_components::{Read, ReadWrite};
use datafusion::datasource::TableProvider;
use flight_client::FlightClient;
use ns_lookup::verify_endpoint_connection;
use secrets::Secret;
use snafu::prelude::*;
use std::any::Any;
use std::borrow::Borrow;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse SpiceAI dataset path: {dataset_path}"))]
    UnableToParseDatasetPath { dataset_path: String },

    #[snafu(display("Unable to publish data to SpiceAI: {source}"))]
    UnableToPublishData { source: flight_client::Error },

    #[snafu(display("Missing required secrets"))]
    MissingRequiredSecrets,

    #[snafu(display(r#"Unable to connect to endpoint "{endpoint}": {source}"#))]
    UnableToVerifyEndpointConnection {
        source: ns_lookup::Error,
        endpoint: String,
    },

    #[snafu(display("Unable to create flight client: {source}"))]
    UnableToCreateFlightClient { source: flight_client::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SpiceAI {
    flight_factory: FlightFactory,
}

impl DataConnectorFactory for SpiceAI {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let default_flight_url = if cfg!(feature = "dev") {
            "https://dev-flight.spiceai.io".to_string()
        } else {
            "https://flight.spiceai.io".to_string()
        };
        Box::pin(async move {
            let secret = secret.context(MissingRequiredSecretsSnafu)?;

            let url: String = params
                .get("endpoint")
                .cloned()
                .unwrap_or(default_flight_url);
            tracing::trace!("Connecting to SpiceAI with flight url: {url}");

            verify_endpoint_connection(&url).await.with_context(|_| {
                UnableToVerifyEndpointConnectionSnafu {
                    endpoint: url.to_string(),
                }
            })?;

            let api_key = secret.get("key").unwrap_or_default();
            let flight_client = FlightClient::new(url.as_str(), "", api_key)
                .await
                .context(UnableToCreateFlightClientSnafu)?;
            let flight_factory = FlightFactory::new("spiceai", flight_client);
            let spiceai = Self { flight_factory };
            Ok(Arc::new(spiceai) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for SpiceAI {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.flight_factory,
            SpiceAI::spice_dataset_path(dataset).into(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "spiceai",
        })?)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let read_write_result = ReadWrite::table_provider(
            &self.flight_factory,
            SpiceAI::spice_dataset_path(dataset).into(),
        )
        .await
        .context(super::UnableToGetReadWriteProviderSnafu {
            dataconnector: "spiceai",
        });

        Some(read_write_result)
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
            let dataset = Dataset::try_new(input.clone(), "bar").expect("a valid dataset");
            assert_eq!(SpiceAI::spice_dataset_path(dataset), expected, "{input}");
        }
    }
}
