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
use crate::secrets::{Secret, SecretMap};
use crate::Runtime;
use async_trait::async_trait;
use data_components::databricks_delta::DatabricksDelta;
use data_components::databricks_spark::DatabricksSparkConnect;
use data_components::Read;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: endpoint"))]
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
}

impl Databricks {
    pub async fn new(secret: Option<Secret>, params: Arc<HashMap<String, String>>) -> Result<Self> {
        let mode = params.get("mode").cloned().unwrap_or_default();

        if mode.as_str() == "delta_lake" {
            let mut params: SecretMap = params.as_ref().into();

            if let Some(secret) = secret {
                for (key, value) in secret.iter() {
                    params.insert(key.to_string(), value.clone());
                }
            }

            let databricks_delta = DatabricksDelta::new(Arc::new(params.into_map()));
            Ok(Self {
                read_provider: Arc::new(databricks_delta.clone()),
            })
        } else {
            let Some(endpoint) = params.get("endpoint") else {
                return MissingEndpointSnafu.fail();
            };
            let user = params.get("user").map(std::borrow::ToOwned::to_owned);
            let mut databricks_use_ssl = true;
            if let Some(databricks_use_ssl_value) = params.get("databricks_use_ssl") {
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
            let ((Some(cluster_id), _) | (_, Some(cluster_id))) = (
                params.get("databricks_cluster_id"),
                params.get("databricks-cluster-id"),
            ) else {
                return MissingDatabricksClusterIdSnafu.fail();
            };
            let Some(secrets) = secret.as_ref() else {
                return MissingDatabricksTokenSnafu {
                    message: "Secrets not available".to_string(),
                }
                .fail();
            };
            let Some(token) = secrets.get("token") else {
                return MissingDatabricksTokenSnafu {
                    message: "DATABRICKS TOKEN not set".to_string(),
                }
                .fail();
            };
            let databricks_spark = DatabricksSparkConnect::new(
                endpoint.to_string(),
                user,
                cluster_id.to_string(),
                token.to_string(),
                databricks_use_ssl,
            )
            .await
            .context(UnableToConstructDatabricksSparkSnafu)?;
            Ok(Self {
                read_provider: Arc::new(databricks_spark.clone()),
            })
        }
    }
}

impl DataConnectorFactory for Databricks {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let databricks = Databricks::new(secret, Arc::clone(&params)).await?;
            Ok(Arc::new(databricks) as Arc<dyn DataConnector>)
        })
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
        _runtime: &Runtime,
        _catalog: &Catalog,
    ) -> Option<super::DataConnectorResult<Arc<dyn CatalogProvider>>> {
        todo!();
    }
}
