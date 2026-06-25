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
use iceberg::{Catalog, TableIdent, io::StorageFactory};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use secrecy::ExposeSecret;
use util::concat_arrays;

use super::DataConnectorFactory;
use super::iceberg_cluster::IcebergClusterTableProvider;
use crate::{
    catalogconnector::iceberg::{
        ICEBERG_PARAM_LEN, get_rest_catalog, map_param_name_to_iceberg_prop,
        parse_hadoop_table_url, parse_table_url, verify_s3_endpoint,
    },
    component::dataset::Dataset,
    dataconnector::{
        ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError as Error,
        parameters::aws::initiate_config_with_credentials,
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

/// Holds the components needed for read, read-write, and distributed Iceberg
/// providers: the base provider plus the catalog and identity used to reload the
/// table on remote executors during distributed (Ballista) execution.
struct IcebergTableParts {
    provider: Arc<dyn TableProvider>,
    catalog: Arc<dyn Catalog>,
    table_identifier: TableIdent,
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

    /// Creates the Iceberg table provider along with the catalog and table
    /// identity, used to build read, read-write, and distributed providers.
    async fn create_iceberg_table_parts(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let source = dataset.path();

        let mut props = HashMap::new();
        for (key, value) in &self.params {
            if let Some(prop_vec) = map_param_name_to_iceberg_prop(key.as_str()) {
                for prop in prop_vec {
                    props.insert(prop.clone(), value.expose_secret().to_string());
                }
            }
        }

        let storage_factory: Option<Arc<dyn StorageFactory>> =
            if let Some(endpoint) = props.get("s3.endpoint") {
                verify_s3_endpoint(endpoint)
                    .await
                    .map_err(|e| Error::InvalidConfiguration {
                        dataconnector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    })?;

                let aws_sdk_config = initiate_config_with_credentials(
                    "IcebergDataConnector",
                    "s3_region",
                    "s3_access_key_id",
                    "s3_secret_access_key",
                    "s3_session_token",
                    &self.params,
                    self.params.get("s3_iam_role_source").expose().ok(),
                )
                .await
                .map_err(|e| Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?
                .load()
                .await;

                let custom_loader = S3CredentialProvider::from_config(&aws_sdk_config)
                    .map_err(|e| Error::InvalidConfiguration {
                        dataconnector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    })?
                    .into_custom_loader();

                Some(Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: Some(custom_loader),
                }) as Arc<dyn StorageFactory>)
            } else {
                None
            };

        if source.starts_with("file://")
            || source.starts_with("s3://")
            || source.starts_with("s3a://")
            || source.starts_with("gs://")
            || source.starts_with("gcs://")
        {
            let metadata_mode = self
                .params
                .get("metadata_path")
                .ok()
                .map(|path| MetadataMode::Exact(path.expose_secret().to_string()))
                .unwrap_or_default();

            return IcebergDataConnector::load_hadoop_catalog(
                props,
                storage_factory,
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

        let catalog_client = get_rest_catalog(base_uri, props, storage_factory.clone())
            .await
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        let catalog_client: Arc<dyn Catalog> = Arc::new(catalog_client);

        // Load the specific table
        let namespace_ident = namespace.name().clone();
        let table_identifier = TableIdent::new(namespace_ident, table_name.clone());

        let table_provider = IcebergTableProvider::try_new(
            Arc::clone(&catalog_client),
            table_identifier.namespace().clone(),
            table_identifier.name().to_string(),
        )
        .await
        .map_err(|e| Error::UnableToGetReadProvider {
            dataconnector: "iceberg".into(),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;

        Ok(IcebergTableParts {
            provider: Arc::new(table_provider),
            catalog: catalog_client,
            table_identifier,
        })
    }

    async fn load_hadoop_catalog(
        props: HashMap<String, String>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        dataset: &Dataset,
        source: &str,
        metadata_mode: MetadataMode,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let (base_uri, namespace, table_name) = parse_hadoop_table_url(source, None).map_err(|e| {
                Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: format!("A Dataset Path is required for Iceberg in the format of: file:///tmp/hadoop_warehouse/<namespace>/<table_name>, s3://<bucket>/<namespace>/<table_name>, gs://<bucket>/<namespace>/<table_name>, or gcs://<bucket>/<namespace>/<table_name>. For details, visit: https://spiceai.org/docs/components/data-connectors/iceberg#from. {e}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?;

        // Load the specific table
        let table_identifier = TableIdent::new(namespace.name().clone(), table_name);

        // Determine storage factory from scheme if not explicitly provided
        let factory = storage_factory.unwrap_or_else(|| {
            if source.starts_with("gs://") || source.starts_with("gcs://") {
                Arc::new(OpenDalStorageFactory::Gcs)
            } else if source.starts_with("s3://") || source.starts_with("s3a://") {
                Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                })
            } else {
                Arc::new(iceberg::io::LocalFsStorageFactory) as Arc<dyn StorageFactory>
            }
        });

        // Build an opendal operator for directory listing (scoped to the warehouse root, not the table URL)
        let operator = crate::catalogconnector::iceberg::build_opendal_operator(&base_uri, &props)
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        let catalog_builder = HadoopCatalogBuilder::default()
            .with_warehouse_root(base_uri)
            .with_metadata_mode(metadata_mode)
            .with_storage_factory(factory)
            .with_operator(operator)
            .with_properties(props);

        let catalog_client: Arc<dyn Catalog> =
            Arc::new(catalog_builder.build().await.map_err(|e| {
                Error::UnableToGetReadProvider {
                    dataconnector: "iceberg".into(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?);

        let table_provider = IcebergTableProvider::try_new(
            Arc::clone(&catalog_client),
            table_identifier.namespace().clone(),
            table_identifier.name().to_string(),
        )
        .await
        .map_err(|e| Error::UnableToGetReadProvider {
            dataconnector: "iceberg".into(),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;

        Ok(IcebergTableParts {
            provider: Arc::new(table_provider),
            catalog: catalog_client,
            table_identifier,
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
        let parts = self.create_iceberg_table_parts(dataset).await?;

        // Wrap so the scan can be serialized for distributed (Ballista) execution.
        // In a single-node session this is a transparent pass-through.
        Ok(Arc::new(IcebergClusterTableProvider::new(
            dataset.name.clone(),
            parts.provider,
        )))
    }

    #[cfg(feature = "iceberg-write")]
    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        // Create the table parts which include catalog + identity for delete support
        let parts = match self.create_iceberg_table_parts(dataset).await {
            Ok(parts) => parts,
            Err(e) => return Some(Err(e)),
        };

        // Wrap in IcebergDeletionProvider for DELETE FROM support.
        let deletion_provider = data_components::iceberg::delete::IcebergDeletionProvider::new(
            parts.catalog,
            parts.table_identifier.namespace().clone(),
            parts.table_identifier.name().to_string(),
            parts.provider,
        );

        // Then wrap so the scan can be serialized for distributed execution.
        Some(Ok(Arc::new(IcebergClusterTableProvider::new(
            dataset.name.clone(),
            Arc::new(deletion_provider),
        ))))
    }
}

register_data_connector!("iceberg", IcebergDataConnectorFactory);
