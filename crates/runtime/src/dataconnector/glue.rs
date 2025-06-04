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

use std::{any::Any, collections::HashMap, pin::Pin, sync::Arc};

use async_trait::async_trait;
use aws_sdk_glue::{Client, types::Table};
use datafusion::catalog::TableProvider;
use iceberg::{NamespaceIdent, TableIdent, io::S3_REGION};
use iceberg_catalog_glue::{AWS_REGION_NAME, GlueCatalog, GlueCatalogConfig};
use iceberg_datafusion::IcebergTableProvider;

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
    async fn client(&self) -> Result<Client, aws::Error> {
        let config = load_config(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &self.params,
        )
        .await?;

        Ok(Client::new(&config))
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

        let client = self.client().await.map_err(|e| {
            super::DataConnectorError::UnableToConnectInternal {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: e.into(),
            }
        })?;

        let get_table_output = client
            .get_table()
            .database_name(database)
            .name(table)
            .send()
            .await
            .map_err(|e| super::DataConnectorError::UnableToConnectInternal {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: e.into(),
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
            InputFormat::Parquet => {
                create_parquet_provider(dataset.clone(), self.params.clone(), &table).await
            }
            InputFormat::Iceberg => {
                let region = self.params.get("region").expose().ok().ok_or_else(|| {
                    super::DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: PREFIX.to_string(),
                        connector_component: dataset.into(),
                        message: "region not found".to_string(),
                    }
                })?;
                create_iceberg_provider(dataset, region.to_string(), database.to_string(), &table)
                    .await
            }
        }
    }
}

enum InputFormat {
    // Avro,
    // Csv,
    // Json,
    // Xml,
    Parquet,
    // Orc,
    Iceberg,
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
            input_format => return Err(format!("input format `{input_format} is not supported`")),
        })
    }
}

async fn create_iceberg_provider(
    dataset: &Dataset,
    region: String,
    database: String,
    table: &Table,
) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
    let metadata_location =
        get_metadata_location(table.parameters.as_ref()).map_err(|message| {
            super::DataConnectorError::InternalWithSource {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: message.into(),
            }
        })?;

    let props = HashMap::from([
        (AWS_REGION_NAME.to_string(), region.clone()),
        (S3_REGION.to_string(), region),
    ]);

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

async fn create_parquet_provider(
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

    let Some(mut from) = storage_descriptor.location().map(String::from) else {
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

    if !from.ends_with('/') {
        from.push('/');
    }

    // Add required file_format parameter for S3
    params.insert("file_format".into(), "parquet".into());
    let s3 = S3 { params };

    // Modify the dataset for S3 parquet
    dataset.from = from;
    dataset
        .params
        .insert("hive_partitioning_enabled".to_string(), "true".to_string());

    s3.read_provider(&dataset).await
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
