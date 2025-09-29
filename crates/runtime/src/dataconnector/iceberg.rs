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

use std::{any::Any, collections::HashMap, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use aws_sdk_credential_bridge::S3CredentialProvider;
use data_components::iceberg::catalog::hadoop::{HadoopCatalogBuilder, MetadataMode};
use datafusion::catalog::TableProvider;
use iceberg::{TableIdent, io::CustomAwsCredentialLoader};
use iceberg_datafusion::IcebergTableProvider;
use secrecy::ExposeSecret;

use super::DataConnectorFactory;
use crate::{
    catalogconnector::iceberg::{
        ICEBERG_PARAM_LEN, get_rest_catalog, map_param_name_to_iceberg_prop,
        parse_hadoop_table_url, parse_table_url, verify_s3_endpoint,
    },
    component::dataset::Dataset,
    dataconnector::{
        ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError as Error,
        parameters::aws::load_config,
    },
    model::params::concat_arrays,
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

const HADOOP_PARAM_LEN: usize = 1;
pub(crate) const HADOOP_PARAMETERS: [ParameterSpec; HADOOP_PARAM_LEN] = [
    // Hadoop options
    ParameterSpec::runtime("metadata_path")
        .description("The path including scheme to the metadata file for the Hadoop table. Must specify a path to a `.json` file. For example, `s3a://my-bucket/warehouse/namespace/table/metadata/v1.metadata.json`")
];

pub(crate) const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    HADOOP_PARAM_LEN,
    ICEBERG_PARAM_LEN,
    { HADOOP_PARAM_LEN + ICEBERG_PARAM_LEN },
>(
    HADOOP_PARAMETERS,
    crate::catalogconnector::iceberg::PARAMETERS,
);

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
        PARAMETERS
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

    #[allow(clippy::too_many_lines)]
    async fn create_iceberg_table_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let source = dataset.path();

        let mut props = HashMap::new();
        for (key, value) in &self.params {
            if let Some(prop_vec) = map_param_name_to_iceberg_prop(key.as_str()) {
                for prop in prop_vec {
                    props.insert(prop.clone(), value.expose_secret().to_string());
                }
            }
        }

        let custom_credential_loader = if let Some(endpoint) = props.get("s3.endpoint") {
            verify_s3_endpoint(endpoint)
                .await
                .map_err(|e| Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?;

            let aws_sdk_config = load_config(
                "IcebergDataConnector",
                "s3_region",
                "s3_access_key_id",
                "s3_secret_access_key",
                "s3_session_token",
                &self.params,
            )
            .await
            .map_err(|e| Error::InvalidConfiguration {
                dataconnector: "iceberg".into(),
                message: e.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

            Some(
                S3CredentialProvider::from_config(&aws_sdk_config)
                    .map_err(|e| Error::InvalidConfiguration {
                        dataconnector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    })?
                    .into_custom_loader(),
            )
        } else {
            None
        };

        if source.starts_with("file://")
            || source.starts_with("s3://")
            || source.starts_with("s3a://")
        {
            let metadata_mode = self
                .params
                .get("metadata_path")
                .ok()
                .map(|path| MetadataMode::Exact(path.expose_secret().to_string()))
                .unwrap_or_default();

            return IcebergDataConnector::load_hadoop_catalog(
                props,
                custom_credential_loader,
                dataset,
                source,
                metadata_mode,
            )
            .await;
        }

        if self.params.get("metadata_path").ok().is_some() {
            tracing::warn!(
                "The `metadata_path` parameter is valid only for Hadoop Catalogs. The parameter will be ignored for REST Catalogs."
            );
        }

        let (base_uri, new_props, namespace, table_name) = match parse_table_url(source) {
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

        props.extend(new_props);

        let mut catalog_client = get_rest_catalog(base_uri, props).await.map_err(|e| {
            Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            }
        })?;
        if let Some(custom_loader) = custom_credential_loader {
            catalog_client = catalog_client.with_file_io_extension(custom_loader);
        }

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

    async fn load_hadoop_catalog(
        props: HashMap<String, String>,
        custom_credential_loader: Option<CustomAwsCredentialLoader>,
        dataset: &Dataset,
        source: &str,
        metadata_mode: MetadataMode,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let (base_uri, namespace, table_name) = parse_hadoop_table_url(source, None).map_err(|e| {
                Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: format!(
                        "A Dataset Path is required for Iceberg in the format of: file:///tmp/hadoop_warehouse/<namespace>/<table_name> or s3://<bucket>/<namespace>/<table_name>.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/iceberg#from\n{e}"
                    ),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?;

        // Load the specific table
        let table_identifier = TableIdent::new(namespace.name().clone(), table_name);

        let mut catalog_builder = HadoopCatalogBuilder::default()
            .with_warehouse_root(base_uri)
            .with_metadata_mode(metadata_mode)
            .with_properties(props);

        if let Some(custom_loader) = custom_credential_loader {
            catalog_builder = catalog_builder.with_file_io_extension(custom_loader);
        }

        let catalog_client =
            catalog_builder
                .build()
                .await
                .map_err(|e| Error::UnableToGetReadProvider {
                    dataconnector: "iceberg".into(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?;

        // Create a DataFusion TableProvider from the Iceberg table
        let table_provider =
            IcebergTableProvider::try_new(Arc::new(catalog_client), table_identifier)
                .await
                .map_err(|e| Error::UnableToGetReadProvider {
                    dataconnector: "iceberg".into(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?;

        Ok(Arc::new(table_provider))
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
        self.create_iceberg_table_provider(dataset).await
    }

    #[cfg(feature = "iceberg-write")]
    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        // Iceberg supports read and write operations through the same TableProvider interface.
        Some(self.create_iceberg_table_provider(dataset).await)
    }
}
