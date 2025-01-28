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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::databricks_delta::DatabricksDelta;
use data_components::databricks_spark::DatabricksSparkConnect;
use data_components::unity_catalog::Endpoint;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec,
    Parameters,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"))]
    MissingParameter { parameter: String },

    #[snafu(display("Invalid `databricks_use_ssl` value: '{value}'. Use 'true' or 'false'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"))]
    InvalidUsessl { value: String },

    #[snafu(display("Failed to connect to Databricks Spark.\n{source}\nVerify the connector configuration, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"))]
    UnableToConstructDatabricksSpark {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid `mode` value: '{value}'. Use 'delta_lake' or 'spark_connect'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"))]
    InvalidMode { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Databricks {
    read_provider: Arc<dyn Read>,
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

        match mode {
            "delta_lake" => {
                let databricks_delta = DatabricksDelta::new(
                    Endpoint(endpoint.to_string()),
                    token.clone(),
                    params.to_secret_map(),
                );
                Ok(Self {
                    read_provider: Arc::new(databricks_delta.clone()),
                })
            }
            "spark_connect" => {
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
                })
            }
            _ => Err(Error::InvalidMode {
                value: mode.to_string(),
            }),
        }
    }

    pub(crate) fn read_provider(&self) -> Arc<dyn Read> {
        Arc::clone(&self.read_provider)
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
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
}
