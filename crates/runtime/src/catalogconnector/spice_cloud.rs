/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use super::{CatalogConnector, ConnectorComponent, ParameterSpec, Parameters};
use crate::{
    component::{catalog::Catalog, dataset::Dataset},
    dataconnector::{
        spiceai::{SpiceAI, SpiceAIDatasetPath, SpiceAIFactory},
        ConnectorParams, ConnectorParamsBuilder, DataConnector, DataConnectorFactory,
    },
    parameters::ExposedParamLookup,
    Runtime,
};
use async_trait::async_trait;
use data_components::{
    iceberg::catalog::RestCatalog, spice_cloud::provider::SpiceCloudPlatformCatalogProvider, Read,
    RefreshableCatalogProvider,
};
use iceberg::NamespaceIdent;
use iceberg_catalog_rest::RestCatalogConfig;
use snafu::prelude::*;
use std::{any::Any, collections::HashMap, sync::Arc};
use tonic::metadata::MetadataValue;

#[derive(Debug, Snafu)]
pub enum Error {
    InvalidPath,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SpiceCloudPlatformCatalog {
    params: Parameters,
}

impl SpiceCloudPlatformCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let (org, app, catalog_name) = Self::parse_and_validate_catalog_id(catalog)?;
        let catalog_client = self.create_rest_catalog_client();
        let read_provider = self
            .create_read_provider(runtime, catalog, &org, &app, &catalog_name)
            .await?;

        let Ok(namespace_ident) = NamespaceIdent::from_vec(vec![org, app, catalog_name]) else {
            unreachable!("This only panics if the vec is empty");
        };

        let catalog_provider = SpiceCloudPlatformCatalogProvider::try_new(
            Arc::new(catalog_client),
            namespace_ident,
            read_provider,
            catalog.include.clone(),
        )
        .await
        .map_err(|e| super::Error::UnableToGetCatalogProvider {
            connector: "iceberg".into(),
            connector_component: ConnectorComponent::from(catalog),
            source: Box::new(e),
        })?;

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }

    fn parse_and_validate_catalog_id(catalog: &Catalog) -> super::Result<(String, String, String)> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "spice.ai".into(),
                    message: "A Catalog Path is required for Spice.ai in the format of: <org>/<app>[/<catalog>] where <catalog> is optional.\nFor details, visit: https://spiceai.org/docs/components/catalogs/spiceai#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                },
            );
        };

        match parse_catalog_slug(catalog_id.as_str()) {
            Ok(result) => Ok(result),
            Err(e) => {
                Err(super::Error::InvalidConfiguration {
                    connector: "spice.ai".into(),
                    message: "A Catalog Path is required for Spice.ai in the format of: <org>/<app>[/<catalog>] where <catalog> is optional.\nFor details, visit: https://spiceai.org/docs/components/catalogs/spiceai#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })
            }
        }
    }

    fn create_rest_catalog_client(&self) -> RestCatalog {
        let endpoint = self
            .params
            .get("http_endpoint")
            .expose()
            .unwrap_or_else(|_| "https://data.spiceai.io");

        let mut props = HashMap::new();
        if let ExposedParamLookup::Present(api_key) = self.params.get("api_key").expose() {
            props.insert("token".to_string(), api_key.to_string());
        };

        let catalog_config = RestCatalogConfig::builder()
            .uri(endpoint.to_string())
            .props(props)
            .build();

        RestCatalog::new(catalog_config)
    }

    async fn create_read_provider(
        &self,
        runtime: &Runtime,
        catalog: &Catalog,
        org: &str,
        app: &str,
        catalog_name: &str,
    ) -> super::Result<Arc<dyn Read>> {
        let connector_factory = self
            .create_data_connector(runtime, catalog, self.create_template_dataset())
            .await?;

        let Some(data_connector) = connector_factory.as_any().downcast_ref::<SpiceAI>() else {
            unreachable!("Spice.ai is the only valid DataConnector");
        };

        let org_metadata = Self::create_metadata_value(org, catalog)?;
        let app_metadata = Self::create_metadata_value(app, catalog)?;

        let (flight_factory, _) = data_connector.flight_factory(SpiceAIDatasetPath::OrgAppPath {
            org: org_metadata,
            app: app_metadata,
            path: catalog_name.into(),
        });

        Ok(Arc::new(flight_factory))
    }

    fn create_template_dataset(&self) -> Dataset {
        let Ok(template_dataset) = Dataset::try_new("spice.ai".into(), "template") else {
            unreachable!("'template' is a valid dataset name");
        };

        let mut params = HashMap::new();
        if let ExposedParamLookup::Present(flight_endpoint) =
            self.params.get("flight_endpoint").expose()
        {
            params.insert("spiceai_endpoint".to_string(), flight_endpoint.to_string());
        }

        if let ExposedParamLookup::Present(api_key) = self.params.get("api_key").expose() {
            params.insert("spiceai_api_key".to_string(), api_key.to_string());
        }

        template_dataset.with_params(params)
    }

    async fn create_data_connector(
        &self,
        runtime: &Runtime,
        catalog: &Catalog,
        template_dataset: Dataset,
    ) -> super::Result<Arc<dyn DataConnector>> {
        SpiceAIFactory::new()
            .create(
                ConnectorParamsBuilder::new(
                    "spice.ai".into(),
                    ConnectorComponent::Dataset(Arc::new(template_dataset)),
                )
                .build(runtime.secrets())
                .await
                .map_err(|e| super::Error::InvalidConfiguration {
                    connector: "spice.ai".into(),
                    connector_component: ConnectorComponent::from(catalog),
                    message: e.to_string(),
                    source: e,
                })?,
            )
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: "spice.ai".into(),
                connector_component: ConnectorComponent::from(catalog),
                source: e,
            })
    }

    fn create_metadata_value(
        value: &str,
        catalog: &Catalog,
    ) -> super::Result<MetadataValue<tonic::metadata::Ascii>> {
        MetadataValue::try_from(value).map_err(|e| super::Error::InvalidConfiguration {
            connector: "spice.ai".into(),
            connector_component: ConnectorComponent::from(catalog),
            message: e.to_string(),
            source: Box::new(e),
        })
    }
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("api_key").secret(),
    ParameterSpec::connector("flight_endpoint"),
    ParameterSpec::connector("http_endpoint"),
];

#[async_trait]
impl CatalogConnector for SpiceCloudPlatformCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        self.refreshable_catalog_provider(runtime, catalog).await
    }
}

fn parse_catalog_slug(catalog_slug: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = catalog_slug.split('/').collect();

    match parts.len() {
        2 | 3 => {
            let org = parts[0].to_string();
            let app = parts[1].to_string();
            let catalog = parts.get(2).map_or("spice", |&c| c).to_string();

            Ok((org, app, catalog))
        }
        _ => Err(Error::InvalidPath),
    }
}
