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

use crate::component::catalog::Catalog;
use crate::component::dataset::Dataset;
use crate::Runtime;
use async_trait::async_trait;
use data_components::databricks_delta::DatabricksDelta;
use data_components::databricks_spark::DatabricksSparkConnect;
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::provider::UnityCatalogProvider;
use data_components::unity_catalog::{CatalogId, Endpoint, UCTable, UnityCatalog};
use data_components::Read;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{
    ConnectorComponent, DataConnector, DataConnectorFactory, DataConnectorParams, ParameterSpec,
    Parameters,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display("databricks_use_ssl value {value} is invalid, please use true or false"))]
    InvalidUsessl { value: String },

    #[snafu(display("Endpoint {endpoint} is invalid: {source}"))]
    InvalidEndpoint {
        endpoint: String,
        source: ns_lookup::Error,
    },

    #[snafu(display("{source}"))]
    UnableToConstructDatabricksSpark {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Databricks {
    read_provider: Arc<dyn Read>,
    params: Parameters,
}

impl Databricks {
    pub async fn new(params: Parameters) -> Result<Self> {
        let mode = params.get("mode").expose().ok().unwrap_or_default();
        let endpoint = params
            .get("endpoint")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;
        let token = params
            .get("token")
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        if mode == "delta_lake" {
            let databricks_delta = DatabricksDelta::new(
                Endpoint(endpoint.to_string()),
                token.clone(),
                params.to_secret_map(),
            );
            Ok(Self {
                read_provider: Arc::new(databricks_delta.clone()),
                params,
            })
        } else {
            let mut databricks_use_ssl = true;
            if let Some(databricks_use_ssl_value) = params.get("use_ssl").expose().ok() {
                databricks_use_ssl = match databricks_use_ssl_value {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return InvalidUsesslSnafu {
                            value: databricks_use_ssl_value,
                        }
                        .fail()
                    }
                };
            }
            let cluster_id = params
                .get("cluster_id")
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;
            let databricks_spark = DatabricksSparkConnect::new(
                endpoint.to_string(),
                cluster_id.expose_secret().to_string(),
                token.expose_secret().to_string(),
                databricks_use_ssl,
            )
            .await
            .context(UnableToConstructDatabricksSparkSnafu)?;
            Ok(Self {
                read_provider: Arc::new(databricks_spark.clone()),
                params,
            })
        }
    }

    async fn catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::DataConnectorResult<Arc<dyn CatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "databricks".into(),
                message: "A Catalog Name is required for the Databricks Unity Catalog.\nFor further information, visit: https://docs.spiceai.org/components/catalogs/databricks#from".into(),
                connector_component: ConnectorComponent::from(catalog)
            });
        };

        let endpoint = self.params.get("endpoint").expose().ok_or_else(|p| {
            super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "databricks".into(),
                message: format!("A required parameter was missing: {}.\nFor further information, visit: https://docs.spiceai.org/components/catalogs/databricks#params", p.0),
                connector_component: ConnectorComponent::from(catalog)
            }
        })?;
        let token = self.params.get("token").ok_or_else(|p| {
            super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "databricks".into(),
                message: format!("A required parameter was missing: {}.\nFor further information, visit: https://docs.spiceai.org/components/catalogs/databricks#params", p.0),
                connector_component: ConnectorComponent::from(catalog)
            }
        })?;

        let unity_catalog = UnityCatalog::new(Endpoint(endpoint.to_string()), Some(token.clone()));
        let client = Arc::new(unity_catalog);

        // Copy the catalog params into the dataset params, and allow user to override
        let mut dataset_params: HashMap<String, SecretString> =
            runtime.get_params_with_secrets(&catalog.params).await;

        let secret_dataset_params = runtime
            .get_params_with_secrets(&catalog.dataset_params)
            .await;

        for (key, value) in secret_dataset_params {
            dataset_params.insert(key, value);
        }

        let factory = DatabricksFactory::new();

        let params = Parameters::try_new(
            "connector databricks",
            dataset_params.into_iter().collect(),
            factory.prefix(),
            runtime.secrets(),
            factory.parameters(),
        )
        .await
        .context(super::InternalWithSourceSnafu {
            dataconnector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
        })?;

        let mode = self.params.get("mode").expose().ok();
        let (table_creator, table_reference_creator) = if let Some("delta_lake") = mode {
            (
                Arc::new(DeltaTableFactory::new(params.to_secret_map())) as Arc<dyn Read>,
                table_reference_creator_delta_lake as fn(UCTable) -> Option<TableReference>,
            )
        } else {
            let dataset_databricks = match Databricks::new(params).await.map_err(|source| {
                super::DataConnectorError::UnableToGetCatalogProvider {
                    dataconnector: "databricks".to_string(),
                    source: source.into(),
                    connector_component: ConnectorComponent::from(catalog),
                }
            }) {
                Ok(dataset_databricks) => dataset_databricks,
                Err(e) => return Err(e),
            };

            (
                dataset_databricks.read_provider,
                table_reference_creator_spark as fn(UCTable) -> Option<TableReference>,
            )
        };

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            CatalogId(catalog_id),
            table_creator,
            table_reference_creator,
            catalog.include.clone(),
        )
        .await
        {
            Ok(provider) => provider,
            Err(e) => {
                return Err(super::DataConnectorError::UnableToGetCatalogProvider {
                    dataconnector: "databricks".to_string(),
                    source: Box::new(e),
                    connector_component: ConnectorComponent::from(catalog),
                })
            }
        };

        Ok(Arc::new(catalog_provider) as Arc<dyn CatalogProvider>)
    }
}

#[derive(Default, Clone, Copy)]
pub struct DatabricksFactory {}

impl DatabricksFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("endpoint")
        .required()
        .secret()
        .description("The endpoint of the Databricks instance."),
    ParameterSpec::connector("token")
        .required()
        .secret()
        .description("The personal access token used to authenticate against the DataBricks API."),
    ParameterSpec::runtime("mode")
        .description("The execution mode for querying against Databricks.")
        .default("spark_connect"),
    ParameterSpec::runtime("client_timeout")
        .description("The timeout setting for object store client."),
    ParameterSpec::connector("cluster_id").description("The ID of the compute cluster in Databricks to use for the query. Only valid when mode is spark_connect."),
    ParameterSpec::connector("use_ssl").description("Use a TLS connection to connect to the Databricks Spark Connect endpoint.").default("true"),

    // S3 storage options
    ParameterSpec::connector("aws_region")
        .description("The AWS region to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_endpoint")
        .description("The AWS endpoint to use for S3 storage.")
        .secret(),

    // Azure storage options
    ParameterSpec::connector("azure_storage_account_name")
        .description("The storage account to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_account_key")
        .description("The storage account key to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_id")
        .description("The service principal client id for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_secret")
        .description("The service principal client secret for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_sas_key")
        .description("The shared access signature key for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_endpoint")
        .description("The endpoint for the Azure Blob storage account.")
        .secret(),

    // GCS storage options
    ParameterSpec::connector("google_service_account")
        .description("Filesystem path to the Google service account JSON key file.")
        .secret(),
];

impl DataConnectorFactory for DatabricksFactory {
    fn create(
        &self,
        params: DataConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let databricks = Databricks::new(params.parameters).await?;
            Ok(Arc::new(databricks) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "databricks"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_reference = TableReference::from(dataset.path());
        Ok(self
            .read_provider
            .table_provider(table_reference, dataset.schema())
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "databricks",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }

    async fn catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> Option<super::DataConnectorResult<Arc<dyn CatalogProvider>>> {
        Some(Databricks::catalog_provider(self, runtime, catalog).await)
    }
}

#[allow(clippy::unnecessary_wraps)]
fn table_reference_creator_spark(uc_table: UCTable) -> Option<TableReference> {
    let table_reference = TableReference::Full {
        catalog: uc_table.catalog_name.into(),
        schema: uc_table.schema_name.into(),
        table: uc_table.name.into(),
    };
    Some(table_reference)
}

fn table_reference_creator_delta_lake(uc_table: UCTable) -> Option<TableReference> {
    let storage_location = uc_table.storage_location?;
    Some(TableReference::bare(format!("{storage_location}/")))
}
