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
enum Error {
    #[snafu(display(
        "Could not retrieve table '{table}' from database '{database}'. Verify that both the database and table exist and are accessible."
    ))]
    GetTable { database: String, table: String },
    #[snafu(display("Unable to load the AWS configuration.\n{source}"))]
    AWSConfig { source: aws::Error },
}

#[derive(Clone, Debug)]
pub struct GlueDataConnector {
    params: Parameters,
}

impl GlueDataConnector {
    #[must_use]
    pub fn new(params: Parameters) -> Self {
        Self { params }
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
                message: "dataset parse_path failed".to_string(),
                source: e.into(),
            }
        })?;
        let database =
            path.schema()
                .ok_or_else(|| super::DataConnectorError::UnableToGetSchemaInternal {
                    dataconnector: PREFIX.to_string(),
                    connector_component: dataset.into(),
                    source: format!("schema unavailable for path `{path}`").into(),
                })?;
        let table = path.table();

        let config = self.config().await.map_err(|e| {
            super::DataConnectorError::UnableToConnectInternal {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: Box::new(Error::AWSConfig { source: e }),
            }
        })?;

        let client = Client::new(&config);

        let get_table_output = client
            .get_table()
            .database_name(database)
            .name(table)
            .send()
            .await
            .map_err(|_| super::DataConnectorError::UnableToConnectInternal {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: Box::new(Error::GetTable {
                    database: database.to_string(),
                    table: table.to_string(),
                }),
            })?;

        let table =
            get_table_output
                .table
                .ok_or_else(|| super::DataConnectorError::InvalidTableName {
                    dataconnector: PREFIX.to_string(),
                    connector_component: dataset.into(),
                    table_name: table.to_string(),
                })?;

        match InputFormat::try_from(&table).map_err(|message| {
            super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message,
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
    type Error = String;
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
            return Err(format!(
                "table `{}` has no storage descriptor",
                table.name()
            ));
        };

        let Some(input_format) = storage_descriptor.input_format() else {
            return Err(format!("table `{}` has not input format", table.name(),));
        };

        Ok(match input_format {
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => Self::Parquet,
            "org.apache.hadoop.mapred.TextInputFormat" => Self::Csv,
            input_format => return Err(format!("input format `{input_format}` is not supported")),
        })
    }
}

async fn create_iceberg_provider(
    dataset: &Dataset,
    config: &SdkConfig,
    database: String,
    table: &Table,
) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
    let region =
        config
            .region()
            .ok_or_else(|| super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: "No AWS region specified. Add `glue_region` to the spicepod.".to_string(),
            })?;

    let credentials = config
        .credentials_provider()
        .ok_or_else(|| super::DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: "problem getting credentials".to_string(),
        })?
        .provide_credentials()
        .await
        .map_err(|e| super::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: "credentials provided incorrectly".to_string(),
            source: e.into(),
        })?;

    let metadata_location =
        get_metadata_location(table.parameters.as_ref()).map_err(|message| {
            super::DataConnectorError::InternalWithSource {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: message.into(),
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
        .props(props)
        .build();

    let catalog = GlueCatalog::new(config).await.map_err(|e| {
        super::DataConnectorError::UnableToGetCatalogProvider {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            source: e.into(),
        }
    })?;

    let identifier = TableIdent::new(NamespaceIdent::new(database), table.name().to_string());

    let table_provider = IcebergTableProvider::try_new(Arc::new(catalog), identifier)
        .await
        .map_err(|e| super::DataConnectorError::InternalWithSource {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
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
        return Err(super::DataConnectorError::InternalWithSource {
            dataconnector: PREFIX.to_string(),
            connector_component: (&dataset).into(),
            source: format!("table `{}` has no storage descriptor", table.name()).into(),
        });
    };

    let Some(from) = storage_descriptor.location().map(String::from) else {
        return Err(super::DataConnectorError::InternalWithSource {
            dataconnector: PREFIX.to_string(),
            connector_component: (&dataset).into(),
            source: format!(
                "table `{}` storage descriptor has no location",
                table.name()
            )
            .into(),
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
    let s3 = S3 { params };

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

    // Add the trailing slash
    format!("{s3_location}/")
}

fn get_metadata_location(parameters: Option<&HashMap<String, String>>) -> Result<String, String> {
    const METADATA_LOCATION: &str = "metadata_location";
    match parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.to_string()),
            None => Err(format!("no property `{METADATA_LOCATION}` found")),
        },
        None => Err("no parameters found".to_string()),
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

    #[test]
    fn get_metadata_location_success() {
        let mut params = HashMap::new();
        params.insert(
            "metadata_location".to_string(),
            "s3://bucket/path".to_string(),
        );
        let result = get_metadata_location(Some(&params)).expect("metadata");
        assert_eq!(result, "s3://bucket/path");
    }

    #[test]
    fn get_metadata_location_missing_location() {
        let params = HashMap::new();
        assert!(get_metadata_location(Some(&params)).is_err());
    }

    #[tokio::test]
    async fn get_metadata_location_missing() {
        let params: Option<&HashMap<String, String>> = None;
        assert!(get_metadata_location(params).is_err());
    }
}
