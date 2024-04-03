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

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use flight_client::FlightClient;
use flight_datafusion::FlightTable;
use ns_lookup::verify_endpoint_connection;
use spicepod::component::dataset::Dataset;

use secrets::Secret;

use super::DataConnectorFactory;
use super::{flight::Flight, DataConnector};

pub struct Dremio {
    flight: Flight,
}

impl DataConnectorFactory for Dremio {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let secret = secret.ok_or_else(|| super::Error::UnableToCreateDataConnector {
                source: "Missing required secrets".into(),
            })?;

            let endpoint: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataConnector {
                    source: "Missing required parameter: endpoint".into(),
                })?;

            verify_endpoint_connection(&endpoint)
                .await
                .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;

            let flight_client = FlightClient::new(
                endpoint.as_str(),
                secret.get("username").unwrap_or_default(),
                secret.get("password").unwrap_or_default(),
            )
            .await
            .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;
            let flight = Flight::new(flight_client);
            Ok(Box::new(Self { flight }) as Box<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for Dremio {
    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let dremio_path = dataset.path();

        self.flight.get_all_data(&dremio_path)
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        let dremio_path = dataset.path();

        let provider = FlightTable::new(self.flight.client.clone(), dremio_path).await;

        match provider {
            Ok(provider) => Ok(Arc::new(provider)),
            Err(error) => Err(super::Error::UnableToGetTableProvider {
                source: error.into(),
            }),
        }
    }
}
