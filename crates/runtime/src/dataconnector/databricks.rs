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
use data_components::unity_catalog::{CatalogId, UCTable, UnityCatalog};
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

use super::{DataConnector, DataConnectorFactory, ParameterSpec, Parameters};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: databricks_endpoint"))]
    MissingEndpoint,

    #[snafu(display("Missing required parameter: databricks_cluster_id"))]
    MissingDatabricksClusterId,

    #[snafu(display("Missing required token. {message}"))]
    MissingDatabricksToken { message: String },

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
    params: HashMap<String, SecretString>,
}

impl Databricks {
    pub async fn new(params: Parameters) -> Result<Self> {
        let mode = params.get("mode").expose().ok().unwrap_or_default();

        if mode == "delta_lake" {
            let databricks_delta = DatabricksDelta::new(params.clone());
            Ok(Self {
                read_provider: Arc::new(databricks_delta.clone()),
                params,
            })
        } else {
            let Some(endpoint) = params.get("endpoint") else {
                return MissingEndpointSnafu.fail();
            };
            let user = params.get("user").map(std::borrow::ToOwned::to_owned);
            let mut databricks_use_ssl = true;
            if let Some(databricks_use_ssl_value) = params.get("spark_use_ssl") {
                let databricks_use_ssl_value = databricks_use_ssl_value.expose_secret();
                databricks_use_ssl = match databricks_use_ssl_value.as_str() {
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
            let Some(cluster_id) = params.get("cluster_id") else {
                return MissingDatabricksClusterIdSnafu.fail();
            };
            let Some(token) = params.get("token") else {
                return MissingDatabricksTokenSnafu {
                    message: "DATABRICKS TOKEN not set".to_string(),
                }
                .fail();
            };
            let databricks_spark = DatabricksSparkConnect::new(
                endpoint.expose_secret().to_string(),
                user.map(|u| u.expose_secret().to_string()),
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
    ParameterSpec::connector("token")
        .secret()
        .description("The personal access token used to authenticate against the DataBricks API."),
    ParameterSpec::runtime("mode")
        .description("The execution mode for querying against Databricks.")
        .default("spark_connect"),
    ParameterSpec::connector("cluster_id").description("The ID of the compute cluster in Databricks to use for the query. Only valid when mode is spark_connect."),
    ParameterSpec::connector("use_ssl").description("Use a TLS connection to connect to the Databricks Spark Connect endpoint.").default("true"),
];

impl DataConnectorFactory for DatabricksFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let databricks = Databricks::new(params).await?;
            Ok(Arc::new(databricks) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "databricks"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
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
            })?)
    }

    async fn catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> Option<super::DataConnectorResult<Arc<dyn CatalogProvider>>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Some(Err(
                super::DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "databricks".into(),
                    message: "Catalog ID is required for Databricks Unity Catalog".into(),
                },
            ));
        };

        let unity_catalog = match UnityCatalog::from_params(&self.params).boxed() {
            Ok(unity_catalog) => unity_catalog,
            Err(source) => {
                return Some(Err(super::DataConnectorError::UnableToGetCatalogProvider {
                    dataconnector: "databricks".to_string(),
                    source,
                }))
            }
        };
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

        let mode = self.params.get("mode").map(|v| v.expose_secret().as_str());
        let (table_creator, table_reference_creator) = if let Some("delta_lake") = mode {
            (
                Arc::new(DeltaTableFactory::new(dataset_params)) as Arc<dyn Read>,
                table_reference_creator_delta_lake as fn(UCTable) -> Option<TableReference>,
            )
        } else {
            let dataset_databricks = match Databricks::new(dataset_params).await.map_err(|source| {
                super::DataConnectorError::UnableToGetCatalogProvider {
                    dataconnector: "databricks".to_string(),
                    source: source.into(),
                }
            }) {
                Ok(dataset_databricks) => dataset_databricks,
                Err(e) => return Some(Err(e)),
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
                return Some(Err(super::DataConnectorError::UnableToGetCatalogProvider {
                    dataconnector: "databricks".to_string(),
                    source: Box::new(e),
                }))
            }
        };

        Some(Ok(Arc::new(catalog_provider) as Arc<dyn CatalogProvider>))
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
