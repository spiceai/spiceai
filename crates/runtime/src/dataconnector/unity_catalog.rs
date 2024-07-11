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
use crate::secrets::Secret;
use crate::secrets::SecretMap;
use crate::Runtime;
use async_trait::async_trait;
use data_components::unity_catalog::provider::UnityCatalogProvider;
use data_components::unity_catalog::UnityCatalog as UnityCatalogClient;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
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
    params: SecretMap,
}

impl DataConnectorFactory for UnityCatalog {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let mut params: SecretMap = params.as_ref().into();

        if let Some(secret) = secret {
            for (key, value) in secret.iter() {
                params.insert(key.to_string(), value.clone());
            }
        }
        Box::pin(async move {
            // let (endpoint, catalog_id) = UnityCatalogClient::parse_catalog_url(url);
            let unity_catalog = Self { params };
            Ok(Arc::new(unity_catalog) as Arc<dyn DataConnector>)
        })
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
        _runtime: &Runtime,
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

        let table_creator = Arc::new(|table_reference: TableReference| {
            todo!();
        });

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            catalog_id,
            table_creator,
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
