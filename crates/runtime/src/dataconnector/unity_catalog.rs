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
use crate::component::catalog::Catalog;
use crate::component::dataset::Dataset;
use crate::Runtime;
use async_trait::async_trait;
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::provider::UnityCatalogProvider;
use data_components::unity_catalog::UCTable;
use data_components::unity_catalog::UnityCatalog as UnityCatalogClient;
use data_components::Read;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::SecretString;
use snafu::prelude::*;
use std::any::Any;
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
pub struct UnityCatalog {
    params: HashMap<String, SecretString>,
}

#[derive(Default, Copy, Clone)]
pub struct UnityCatalogFactory {}

impl UnityCatalogFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for UnityCatalogFactory {
    fn create(
        &self,
        params: HashMap<String, SecretString>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(UnityCatalog { params }) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "unity_catalog"
    }

    fn autoload_secrets(&self) -> &'static [&'static str] {
        &["token"]
    }
}

#[async_trait]
impl DataConnector for UnityCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Err(super::DataConnectorError::UnableToGetReadProvider {
            dataconnector: "unity_catalog".to_string(),
            source: "Unity Catalog only support catalogs, not individual datasets.".into(),
        })
    }

    async fn catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> Option<super::DataConnectorResult<Arc<dyn CatalogProvider>>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Some(Err(
                super::DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "unity_catalog".into(),
                    message: "Catalog ID is required for Unity Catalog".into(),
                },
            ));
        };

        // The catalog_id for the unity_catalog provider is the full URL to the catalog like:
        // https://<host>/api/2.1/unity-catalog/catalogs/<catalog_id>
        let (endpoint, catalog_id) = match UnityCatalogClient::parse_catalog_url(&catalog_id)
            .map_err(|e| super::DataConnectorError::InvalidConfiguration {
                dataconnector: "unity_catalog".to_string(),
                message: e.to_string(),
                source: Box::new(e),
            }) {
            Ok((endpoint, catalog_id)) => (endpoint, catalog_id),
            Err(e) => return Some(Err(e)),
        };

        let client = Arc::new(UnityCatalogClient::new(
            endpoint,
            self.params.get("token").cloned(),
        ));

        // Copy the catalog params into the dataset params, and allow user to override
        let mut dataset_params: HashMap<String, SecretString> =
            runtime.get_params_with_secrets(&catalog.params).await;

        let secret_dataset_params = runtime
            .get_params_with_secrets(&catalog.dataset_params)
            .await;

        for (key, value) in secret_dataset_params {
            dataset_params.insert(key, value);
        }

        let delta_table_creator = Arc::new(DeltaTableFactory::new(dataset_params)) as Arc<dyn Read>;

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            catalog_id,
            delta_table_creator,
            table_reference_creator,
            catalog.include.clone(),
        )
        .await
        {
            Ok(provider) => provider,
            Err(e) => {
                return Some(Err(super::DataConnectorError::UnableToGetCatalogProvider {
                    dataconnector: "unity_catalog".to_string(),
                    source: Box::new(e),
                }))
            }
        };

        Some(Ok(Arc::new(catalog_provider) as Arc<dyn CatalogProvider>))
    }
}

fn table_reference_creator(uc_table: UCTable) -> Option<TableReference> {
    let storage_location = uc_table.storage_location?;
    Some(TableReference::bare(format!("{storage_location}/")))
}
