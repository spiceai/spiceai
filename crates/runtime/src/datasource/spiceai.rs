use snafu::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::{flight::Flight, DataSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse SpiceAI dataset path: {}", dataset_path))]
    UnableToParseDatasetPath { dataset_path: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SpiceAI {
    flight: Flight,
}

impl DataSource for SpiceAI {
    fn new(
        auth_provider: Box<dyn AuthProvider>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let url: String = params
                .as_ref()
                .as_ref()
                .and_then(|params| params.get("url").cloned())
                .unwrap_or_else(|| "https://flight.spiceai.io".to_string());
            let flight = Flight::new(auth_provider, url);
            let flight = flight.await?;
            Ok(Self { flight })
        })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let spice_dataset_path = match Self::spice_dataset_path(dataset) {
            Ok(path) => path,
            Err(error) => {
                tracing::error!("Unable to parse SpiceAI dataset path: {:?}", error);
                return Box::pin(async move { vec![] });
            }
        };

        self.flight.get_all_data(&spice_dataset_path)
    }
}

impl SpiceAI {
    /// Parses a dataset path from a Spice AI dataset definition.
    ///
    /// Spice AI datasets have two possible formats for `dataset.path()`:
    /// 1. `<org>/<app>/datasets/<dataset_name>`.
    /// 2. `<org>/<app>`.
    ///
    /// The second format is a shorthand for the first format, where the dataset name
    /// is the same as the local table name specified in `name`.
    ///
    /// This function returns the full dataset path for the given dataset as you would query for it in Spice.
    /// i.e. `<org>.<app>.<dataset_name>`
    fn spice_dataset_path(dataset: &Dataset) -> Result<String> {
        let path = dataset.path();
        let path_parts: Vec<&str> = path.split('/').collect();

        match path_parts.as_slice() {
            [org, app] => Ok(format!(
                "{org}.{app}.{dataset_name}",
                dataset_name = dataset.name
            )),
            [org, app, "datasets", dataset_name] => Ok(format!("{org}.{app}.{dataset_name}")),
            _ => Err(Error::UnableToParseDatasetPath {
                dataset_path: path.to_string(),
            }),
        }
    }
}
