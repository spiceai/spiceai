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

use std::{any::Any, collections::HashMap, path::Path, pin::Pin, sync::Arc};

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_glue::{Client, types::Table};
use aws_sdk_sts::config::ProvideCredentials;
use datafusion::catalog::TableProvider;
use iceberg::{
    NamespaceIdent, TableIdent,
    io::{S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN},
};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, GlueCatalog,
    GlueCatalogConfig,
};
use iceberg_datafusion::IcebergTableProvider;
use snafu::prelude::*;

use crate::{
    component::dataset::Dataset,
    parameters::{ParameterSpec, Parameters},
};

use super::{
    DataConnector, DataConnectorFactory,
    parameters::{
        ConnectorParams,
        aws::{self, load_config},
    },
    s3::S3,
};

static PREFIX: &str = "glue";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Cannot retrieve table '{table}' from Glue database '{database}'.\nVerify that the database and table exist and are accessible.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    GetTable { database: String, table: String },
    #[snafu(display(
        "Cannot load AWS configuration for Glue data connector.\nVerify your AWS credentials and region settings.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/data-connectors/glue\n{source}"
    ))]
    AWSConfig { source: aws::Error },
    #[snafu(display(
        "No schema specified in path '{path}'.\nEnsure the dataset path includes a valid schema."
    ))]
    MissingSchema { path: String },
    #[snafu(display(
        "No AWS region specified.\nAdd 'glue_region' to your configuration.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingRegion,
    #[snafu(display(
        "Cannot retrieve AWS credentials.\nEnsure credentials are configured correctly.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingCredentials,
    #[snafu(display(
        "Invalid AWS credentials provided.\nVerify your credentials and try again.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue\n{source}"
    ))]
    InvalidCredentials { source: CredentialsError },
    #[snafu(display(
        "Cannot retrieve metadata location for table '{table}'.\nEnsure the table is correctly configured in AWS Glue.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue\n{message}"
    ))]
    MissingMetadataLocation { table: String, message: String },
    #[snafu(display(
        "Cannot retrieve input format for table '{table}'.\nEnsure the table is correctly configured in AWS Glue.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingInputFormat { table: String },
    #[snafu(display(
        "The input format {input_format} for table '{table}' is not supported.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    InvalidInputFormat { input_format: String, table: String },
    #[snafu(display(
        "No storage descriptor found for table '{table}'.\nEnsure the table is correctly configured in AWS Glue.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingStorageDescriptor { table: String },
    #[snafu(display(
        "No storage location specified for table '{table}'.\nEnsure the table has a valid S3 location in AWS Glue.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingStorageLocation { table: String },
}

#[derive(Clone, Debug)]
pub struct GlueDataConnector {
    params: Parameters,
    catalog_id: Option<String>,
}

impl GlueDataConnector {
    #[must_use]
    pub fn new(params: Parameters) -> Self {
        Self {
            params,
            catalog_id: None,
        }
    }

    #[must_use]
    pub fn new_with_catalog_id(params: Parameters, catalog_id: Option<String>) -> Self {
        Self { params, catalog_id }
    }
}

impl GlueDataConnector {
    async fn config(&self) -> Result<SdkConfig, aws::Error> {
        let config = load_config(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &self.params,
        )
        .await?;

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

impl DataConnectorFactory for GlueDataConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let glue = GlueDataConnector::new(params.parameters);
            Ok(Arc::new(glue) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        crate::dataconnector::s3::PARAMETERS.as_ref()
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
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.parse_path(false, None).map_err(|e| {
            super::DataConnectorError::InvalidConfiguration {
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
            super::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;
        let table = path.table();

        let config = self.config().await.map_err(|e| {
            let e = Error::AWSConfig { source: e };
            super::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

        let client = Client::new(&config);

        let mut glue_table_builder = client.get_table().database_name(database).name(table);

        if let Some(catalog_id) = &self.catalog_id {
            glue_table_builder = glue_table_builder.catalog_id(catalog_id);
        }

        let get_table_output = glue_table_builder.send().await.map_err(|_| {
            let e = Error::GetTable {
                database: database.to_string(),
                table: table.to_string(),
            };
            super::DataConnectorError::InvalidConfiguration {
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
            super::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

        match InputFormat::try_from(&table).map_err(|e| {
            super::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })? {
            input_format @ (InputFormat::Parquet | InputFormat::Csv) => {
                create_s3_provider(input_format, dataset.clone(), self.params.clone(), &table).await
            }
            InputFormat::Iceberg => {
                create_iceberg_provider(dataset, &config, database.to_string(), &table).await
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum InputFormat {
    // Avro,
    Csv,
    // Json,
    // Xml,
    Parquet,
    // Orc,
    Iceberg,
}

impl InputFormat {
    /// Return the file format of the [`InputFormat`]. For
    /// [`InputFormat::Iceberg`], it's not a file format but we return a value
    /// rather than have to use an `Option` return type for convenience.
    fn file_format(self) -> &'static str {
        match self {
            InputFormat::Csv => "csv",
            InputFormat::Parquet => "parquet",
            InputFormat::Iceberg => "iceberg",
        }
    }
}

impl TryFrom<&Table> for InputFormat {
    type Error = Error;
    fn try_from(table: &Table) -> Result<Self, Self::Error> {
        if table
            .parameters
            .as_ref()
            .and_then(|params| params.get("table_type"))
            .is_some_and(|value| value.to_lowercase() == "iceberg")
        {
            return Ok(Self::Iceberg);
        }

        let Some(storage_descriptor) = table.storage_descriptor() else {
            return Err(Error::MissingStorageDescriptor {
                table: table.name().to_string(),
            });
        };

        let Some(input_format) = storage_descriptor.input_format() else {
            return Err(Error::MissingInputFormat {
                table: table.name().to_string(),
            });
        };

        Ok(match input_format {
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => Self::Parquet,
            "org.apache.hadoop.mapred.TextInputFormat" => Self::Csv,
            input_format => {
                return Err(Error::InvalidInputFormat {
                    input_format: input_format.to_string(),
                    table: table.name().to_string(),
                });
            }
        })
    }
}

async fn create_iceberg_provider(
    dataset: &Dataset,
    config: &SdkConfig,
    database: String,
    table: &Table,
) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
    let region = config.region().ok_or_else(|| {
        let e = Error::MissingRegion;
        super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: e.to_string(),
            source: Box::new(e),
        }
    })?;

    let credentials = config
        .credentials_provider()
        .ok_or_else(|| {
            let e = Error::MissingCredentials;
            super::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?
        .provide_credentials()
        .await
        .map_err(|e| {
            let e = Error::InvalidCredentials { source: e };
            super::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

    let metadata_location = get_metadata_location(table).map_err(|e| {
        super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: e.to_string(),
            source: Box::new(e),
        }
    })?;

    let mut props = HashMap::from([
        (
            AWS_ACCESS_KEY_ID.to_string(),
            credentials.access_key_id().to_string(),
        ),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            credentials.secret_access_key().to_string(),
        ),
        (AWS_REGION_NAME.to_string(), region.to_string()),
        (
            S3_ACCESS_KEY_ID.to_string(),
            credentials.access_key_id().to_string(),
        ),
        (
            S3_SECRET_ACCESS_KEY.to_string(),
            credentials.secret_access_key().to_string(),
        ),
        (S3_REGION.to_string(), region.to_string()),
    ]);

    if let Some(session_token) = credentials.session_token() {
        props.insert(AWS_SESSION_TOKEN.to_string(), session_token.to_string());
        props.insert(S3_SESSION_TOKEN.to_string(), session_token.to_string());
    }

    let config = GlueCatalogConfig::builder()
        .warehouse(metadata_location)
        .catalog_id_opt(table.catalog_id.clone())
        .props(props)
        .build();

    let catalog = GlueCatalog::new(config).await.map_err(|e| {
        super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: format!("Cannot initialize Glue catalog for dataset '{} (glue)'.\nVerify your AWS Glue configuration and credentials.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue", dataset.name),
            source: e.into(),
        }
    })?;

    let identifier = TableIdent::new(NamespaceIdent::new(database), table.name().to_string());

    let table_provider = IcebergTableProvider::try_new(Arc::new(catalog), identifier)
        .await
        .map_err(|e| super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: format!("Cannot load Iceberg table '{}' for dataset '{} (glue)'.\nEnsure the table is correctly configured in AWS Glue.\nFor help, visit: https://docs.spiceai.org/components/data-connectors/glue", table.name(), dataset.name),
            source: e.into(),
        })?;

    Ok(Arc::new(table_provider))
}

async fn create_s3_provider(
    input_format: InputFormat,
    mut dataset: Dataset,
    mut params: Parameters,
    table: &Table,
) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
    let Some(storage_descriptor) = table.storage_descriptor() else {
        let e = Error::MissingStorageDescriptor {
            table: table.name().to_string(),
        };
        return Err(super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: (&dataset).into(),
            message: e.to_string(),
            source: Box::new(e),
        });
    };

    let Some(from) = storage_descriptor.location().map(String::from) else {
        let e = Error::MissingStorageLocation {
            table: table.name().to_string(),
        };
        return Err(super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: (&dataset).into(),
            message: e.to_string(),
            source: Box::new(e),
        });
    };

    let from = ensure_s3_trailing_slash(&from);

    match input_format {
        InputFormat::Csv => {
            // If the table specifies a delimiter, pass it down to the data connector
            // as a parameter
            if let Some(delimiter) = table
                .parameters()
                .and_then(|params| params.get("delimiter"))
            {
                params.insert("csv_delimiter".to_string(), delimiter.as_str().into());
            }
        }
        InputFormat::Parquet => {
            dataset
                .params
                .insert("hive_partitioning_enabled".to_string(), "true".to_string());
        }
        InputFormat::Iceberg => {}
    }

    // Add required file_format parameter for S3
    params.insert("file_format".into(), input_format.file_format().into());
    let s3 = S3 {
        params,
        runtime: Some(Arc::unwrap_or_clone(dataset.runtime())),
    };

    dataset.from = from;

    s3.read_provider(&dataset).await
}

fn ensure_s3_trailing_slash(s3_location: &str) -> String {
    static PREFIX: &str = "s3://";

    if !s3_location.starts_with(PREFIX) {
        return s3_location.to_string();
    }

    let path_part = &s3_location[PREFIX.len()..];

    if path_part.ends_with('/') {
        return s3_location.to_string();
    }

    let path = Path::new(path_part);
    if path.extension().is_some() {
        return s3_location.to_string();
    }

    format!("{s3_location}/")
}

fn get_metadata_location(table: &Table) -> Result<String, Error> {
    const METADATA_LOCATION: &str = "metadata_location";
    match &table.parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.to_string()),
            None => Err(Error::MissingMetadataLocation {
                table: table.name().to_string(),
                message: format!("No property '{METADATA_LOCATION}' found"),
            }),
        },
        None => Err(Error::MissingMetadataLocation {
            table: table.name().to_string(),
            message: "No parameters found".to_string(),
        }),
    }
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
