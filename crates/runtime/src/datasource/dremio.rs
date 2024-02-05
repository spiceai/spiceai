use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use flight_client::FlightClient;
use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::{flight::Flight, DataSource};

pub struct Dremio {
    flight: Flight,
}

impl DataSource for Dremio {
    fn new(
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let endpoint: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataSource {
                    source: "Missing required parameter: endpoint".into(),
                })?;
            let flight_client = FlightClient::new(
                endpoint.as_str(),
                auth_provider.get_param("username").unwrap_or_default(),
                auth_provider.get_param("password").unwrap_or_default(),
            )
            .await
            .map_err(|e| super::Error::UnableToCreateDataSource { source: e.into() })?;
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
}
