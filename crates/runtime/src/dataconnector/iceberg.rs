/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! The Iceberg Data Connector is a thin layer over the Iceberg Catalog Connector.
//! It takes the same parameters as the Catalog Connector.

use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use iceberg::TableIdent;
use iceberg_catalog_rest::RestCatalog;
use iceberg_datafusion::IcebergTableProvider;
use secrecy::ExposeSecret;

use super::DataConnectorFactory;
use crate::{
    catalogconnector::iceberg::{
        get_rest_catalog_config, map_param_name_to_iceberg_prop, parse_table_url,
        verify_s3_endpoint,
    },
    component::dataset::Dataset,
    dataconnector::{
        ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError as Error,
    },
    parameters::{ParameterSpec, Parameters},
};

#[derive(Default, Debug, Copy, Clone)]
pub struct IcebergDataConnectorFactory {}

impl IcebergDataConnectorFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for IcebergDataConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let iceberg = IcebergDataConnector {
                params: params.parameters,
            };
            Ok(Arc::new(iceberg) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "iceberg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        crate::catalogconnector::iceberg::PARAMETERS
    }
}

#[derive(Clone, Debug)]
pub struct IcebergDataConnector {
    params: Parameters,
}

impl IcebergDataConnector {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn DataConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }
}

#[async_trait]
impl DataConnector for IcebergDataConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let source = dataset.path();

        let (base_uri, mut props, namespace, table_name) = match parse_table_url(source) {
            Ok(result) => result,
            Err(e) => {
                return Err(Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: format!(
                        "A Dataset Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>/tables/<table_name>.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/iceberg#from\n{e}"
                    ),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                });
            }
        };

        for (key, value) in &self.params {
            if let Some(prop_vec) = map_param_name_to_iceberg_prop(key.as_str()) {
                for prop in prop_vec {
                    props.insert(prop.clone(), value.expose_secret().to_string());
                }
            }
        }

        if let Some(endpoint) = props.get("s3.endpoint") {
            verify_s3_endpoint(endpoint)
                .await
                .map_err(|e| Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?;
        }

        let catalog_config = get_rest_catalog_config(base_uri, props);

        let catalog_client = RestCatalog::new(catalog_config);
        let catalog_client = Arc::new(catalog_client);

        // Load the specific table
        let namespace_ident = namespace.name().clone();
        let table_identifier = TableIdent::new(namespace_ident, table_name);

        // Create a DataFusion TableProvider from the Iceberg table
        let table_provider = IcebergTableProvider::try_new(catalog_client, table_identifier)
            .await
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        Ok(Arc::new(table_provider))
    }
}
