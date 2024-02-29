use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use flight_client::FlightClient;
use flight_datafusion::FlightTable;
use spicepod::component::dataset::Dataset;

use crate::secretstore::AuthProvider;

use super::{flight::Flight, DataConnector};

pub struct Dremio {
    flight: Flight,
}

#[async_trait]
impl DataConnector for Dremio {
    fn new(
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let endpoint: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataConnector {
                    source: "Missing required parameter: endpoint".into(),
                })?;
            let flight_client = FlightClient::new(
                endpoint.as_str(),
                auth_provider.get_param("username").unwrap_or_default(),
                auth_provider.get_param("password").unwrap_or_default(),
            )
            .await
            .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;
            let flight = Flight::new(flight_client);
            Ok(Self { flight })
        })
    }

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
