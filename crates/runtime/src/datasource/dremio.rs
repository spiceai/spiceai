use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::{flight::Flight, DataSource};

pub struct Dremio {
    flight: Flight,
}

impl DataSource for Dremio {
    fn new(
        auth_provider: Box<dyn AuthProvider>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let url: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("url").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataSource {
                    source: "Missing required parameter: url".into(),
                })?;
            let flight = Flight::new(auth_provider, url);
            Ok(Self {
                flight: flight.await?,
            })
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
