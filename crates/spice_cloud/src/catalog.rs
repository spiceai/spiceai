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

use datafusion::catalog::{schema::SchemaProvider, CatalogProvider};
use runtime::dataconnector::DataConnector;
use serde::Deserialize;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::SpiceExtension;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to retrieve available datasets from Spice AI: {source}"))]
    UnableToRetrieveDatasets { source: super::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SpiceAICatalogProvider {
    datasets: Vec<DatasetSchemaResponse>,
}

#[derive(Debug, Deserialize)]
struct DatasetSchemaResponse {
    name: String,
}

impl SpiceAICatalogProvider {
    /// Creates a new instance of the [`SpiceAICatalogProvider`].
    ///
    /// # Errors
    ///
    /// Returns an error if the list of available datasets cannot be retrieved from the Spice AI platform.
    pub async fn try_new(
        extension: &SpiceExtension,
        data_connector: Arc<dyn DataConnector>,
    ) -> Result<Self> {
        let datasets: Vec<DatasetSchemaResponse> = extension
            .get_json("/v1/schemas")
            .await
            .context(UnableToRetrieveDatasetsSnafu)?;

        Ok(Self { datasets })
    }
}

impl CatalogProvider for SpiceAICatalogProvider {
    /// Returns the catalog provider as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String> {
        todo!();
    }

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        todo!();
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_creation() {
//         let spice_extension = SpiceExtension::default();
//         let provider = SpiceAICatalogProvider::try_new(&spice_extension)
//             .await
//             .expect("to create provider");

//         dbg!(&provider.datasets);
//     }
// }
