/*
Copyright 2026 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_glue::Client;
use datafusion::catalog::TableProvider;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::sync::LazyLock;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use data_components::glue::InputFormat;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::glue::{create_iceberg_provider, create_s3_provider};
use runtime::dataconnector::{
    DataConnector, DataConnectorFactory,
    parameters::{
        ConnectorParams,
        aws::{self, initiate_config_with_credentials},
    },
};
use runtime::parameters::{ParameterSpec, Parameters};

static PREFIX: &str = "glue";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Cannot retrieve table '{table}' from Glue database '{database}'. Verify that the database and table exist and are accessible. For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    GetTable { database: String, table: String },
    #[snafu(display(
        "Cannot load AWS configuration for Glue data connector. Verify your AWS credentials and region settings. For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/data-connectors/glue {source}"
    ))]
    AWSConfig { source: aws::Error },
    #[snafu(display(
        "No schema specified in path '{path}'. Ensure the dataset path includes a valid schema."
    ))]
    MissingSchema { path: String },
    #[snafu(display(
        "No AWS region specified. Add 'glue_region' to your configuration. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingRegion,
    #[snafu(display(
        "Cannot retrieve AWS credentials. Ensure credentials are configured correctly. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingCredentials,
    #[snafu(display(
        "Invalid AWS credentials provided. Verify your credentials and try again. For help, visit: https://docs.spiceai.org/components/data-connectors/glue {source}"
    ))]
    InvalidCredentials { source: CredentialsError },
    #[snafu(display(
        "Cannot retrieve metadata location for table '{table}'. Ensure the table is correctly configured in AWS Glue. For help, visit: https://docs.spiceai.org/components/data-connectors/glue {message}"
    ))]
    MissingMetadataLocation { table: String, message: String },
    #[snafu(display(
        "No storage descriptor found for table '{table}'. Ensure the table is correctly configured in AWS Glue. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingStorageDescriptor { table: String },
    #[snafu(display(
        "No storage location specified for table '{table}'. Ensure the table has a valid S3 location in AWS Glue. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingStorageLocation { table: String },
}

#[derive(Clone, Debug)]
pub struct GlueDataConnector {
    params: Parameters,
    tokio_io_runtime: tokio::runtime::Handle,
}

impl GlueDataConnector {
    #[must_use]
    pub fn new(params: Parameters, tokio_io_runtime: tokio::runtime::Handle) -> Self {
        Self {
            params,
            tokio_io_runtime,
        }
    }

    async fn create_table_provider(
        &self,
        dataset: &Dataset,
    ) -> runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.parse_path(false, None).map_err(|e| {
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: format!("Cannot parse path for dataset '{}': {e}", dataset.name),
                source: e.into(),
            }
        })?;
        let database = path.schema().ok_or_else(|| {
            let e = Error::MissingSchema {
                path: path.to_string(),
            };
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;
        let table = path.table();

        let config = self.config().await.map_err(|e| {
            let e = Error::AWSConfig { source: e };
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

        let client = Client::new(&config);

        let mut glue_table_builder = client.get_table().database_name(database).name(table);

        if let Some(catalog_id) = self.params.get("catalog_id").ok() {
            glue_table_builder = glue_table_builder.catalog_id(catalog_id.expose_secret());
        }

        let get_table_output = glue_table_builder.send().await.map_err(|_| {
            let e = Error::GetTable {
                database: database.to_string(),
                table: table.to_string(),
            };
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

        let table = get_table_output.table.ok_or_else(|| {
            let e = Error::GetTable {
                database: database.to_string(),
                table: table.to_string(),
            };
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

        match InputFormat::try_from(&table).map_err(|e| {
            runtime::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })? {
            input_format @ (InputFormat::Parquet | InputFormat::Csv) => {
                create_s3_provider(
                    input_format,
                    dataset.clone(),
                    self.params.clone(),
                    &table,
                    self.tokio_io_runtime.clone(),
                )
                .await
            }
            InputFormat::Iceberg => {
                create_iceberg_provider(dataset, &config, database.to_string(), &table).await
            }
        }
    }
}

impl GlueDataConnector {
    async fn config(&self) -> Result<SdkConfig, aws::Error> {
        let iam_role_source = self.params.get("iam_role_source").expose().ok();
        let config = initiate_config_with_credentials(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &self.params,
            iam_role_source,
        )
        .await?
        .load()
        .await;

        Ok(config)
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct GlueDataConnectorFactory {}

impl GlueDataConnectorFactory {
    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

pub(crate) static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[ParameterSpec::component("catalog_id").secret()]);
    all_parameters.extend_from_slice(runtime::dataconnector::s3::PARAMETERS.as_ref());
    all_parameters
});

impl DataConnectorFactory for GlueDataConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let glue = GlueDataConnector::new(params.parameters, params.io_runtime);
            Ok(Arc::new(glue) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS.as_ref()
    }
}

#[async_trait]
impl DataConnector for GlueDataConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>> {
        self.create_table_provider(dataset).await
    }

    #[cfg(feature = "iceberg-write")]
    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<runtime::dataconnector::DataConnectorResult<Arc<dyn TableProvider>>> {
        // Iceberg supports read and write operations through the same TableProvider interface.
        Some(self.create_table_provider(dataset).await)
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "glue";

/// Returns a new instance of the Glue connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    GlueDataConnectorFactory::new_arc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_s3_trailing_slash() {
        assert_eq!(
            ensure_s3_trailing_slash("s3://spiceai-public-datasets/tpch/customer"),
            "s3://spiceai-public-datasets/tpch/customer/"
        );
        assert_eq!(
            ensure_s3_trailing_slash("s3://spiceai-public-datasets/tpch/customer/"),
            "s3://spiceai-public-datasets/tpch/customer/"
        );
        assert_eq!(
            ensure_s3_trailing_slash("s3://spiceai-public-datasets/tpch/customer/customer.csv"),
            "s3://spiceai-public-datasets/tpch/customer/customer.csv"
        );
        assert_eq!(ensure_s3_trailing_slash(""), "");
        assert_eq!(ensure_s3_trailing_slash("/local/path"), "/local/path");
    }
}
