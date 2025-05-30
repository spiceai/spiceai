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

use super::{CatalogConnector, ParameterSpec};
use crate::{
    Runtime,
    component::catalog::Catalog,
    dataconnector::{
        ConnectorComponent,
        parameters::{
            self, ConnectorParams,
            aws::{AuthValidator, RegionValidator, Validator},
        },
        s3,
    },
};
use async_trait::async_trait;
use snafu::prelude::*;
use std::sync::{Arc, LazyLock};
use std::{any::Any, collections::HashMap};

mod provider;
mod state;

use provider::GlueCatalogProvider;

pub static PREFIX: &str = "glue";

pub static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    vec![
        ParameterSpec::component("region")
            .description("The AWS region for Glue operations")
            .secret(),
        ParameterSpec::component("key")
            .description("The AWS access key ID for Glue authentication")
            .secret(),
        ParameterSpec::component("secret")
            .description("The AWS secret access key for Glue authentication")
            .secret(),
        ParameterSpec::component("session_token")
            .description("The AWS session token for Glue authentication")
            .secret(),
    ]
    .into_iter()
    .chain(s3::PARAMETERS.iter().cloned())
    .collect()
});

static VALIDATORS: LazyLock<Vec<Box<dyn Validator + Send + Sync + 'static>>> =
    LazyLock::new(|| vec![Box::new(RegionValidator), Box::new(AuthValidator)]);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get Glue databases: {source}"))]
    GetDatabases {
        source: aws_sdk_glue::error::SdkError<
            aws_sdk_glue::operation::get_databases::GetDatabasesError,
        >,
    },

    #[snafu(display("Failed to get Glue tables: {source}"))]
    GetTables {
        source: aws_sdk_glue::error::SdkError<aws_sdk_glue::operation::get_tables::GetTablesError>,
    },

    #[snafu(display("Failed to build FileIO: {source}"))]
    BuildFileIO { source: iceberg::Error },

    #[snafu(display("Failed to create file input for metadata location '{location}': {source}",))]
    CreateFileInput {
        source: iceberg::Error,
        location: String,
    },

    #[snafu(display("Failed to read metadata from '{location}': {source}"))]
    ReadMetadata {
        source: iceberg::Error,
        location: String,
    },

    #[snafu(display("Failed to deserialize metadata: {source}"))]
    DeserializeMetadata { source: serde_json::Error },

    #[snafu(display("Failed to build Iceberg table: {source}"))]
    BuildIcebergTable { source: iceberg::Error },

    #[snafu(display("Failed to create Iceberg table provider: {source}"))]
    CreateIcebergTableProvider { source: iceberg::Error },

    #[snafu(display("No 'metadata_location' set on table '{table}'"))]
    MissingMetadataLocation { table: String },

    #[snafu(display("No 'parameters' set on table"))]
    MissingParameters,

    #[snafu(display("Parameter validation failed: {source}",))]
    ParameterValidation {
        #[snafu(source)]
        source: parameters::aws::Error,
    },

    #[snafu(display("Configuration loading failed: {message}"))]
    ConfigurationLoadingFailed { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A catalog connector for AWS Glue, providing access to database and table metadata.
#[derive(Clone)]
pub struct GlueCatalog {
    params: ConnectorParams,
}

impl GlueCatalog {
    #[must_use]
    pub fn new(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn data_components::RefreshableCatalogProvider>> {
        Ok(Arc::new(
            GlueCatalogProvider::new(self.params.clone(), catalog, runtime)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        ))
    }
}

type DatabaseName = String;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TableType {
    HiveParquet,
    Iceberg,
    Unsupported,
}

impl TableType {
    fn from(table: &aws_sdk_glue::types::Table) -> TableType {
        if table
            .parameters
            .as_ref()
            .and_then(|params| params.get("table_type"))
            .is_some_and(|value| value.to_lowercase() == "iceberg")
        {
            return Self::Iceberg;
        }

        if table
            .storage_descriptor
            .as_ref()
            .and_then(|sd| sd.input_format.as_ref())
            .is_some_and(|input_format| {
                input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            })
        {
            return Self::HiveParquet;
        }

        Self::Unsupported
    }
}

fn database_might_match(database: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|pattern| {
        pattern == database
            || pattern.starts_with(&format!("{database}."))
            || pattern.starts_with("*.")
            || pattern == "*.*"
    })
}

fn is_included(include: Option<&globset::GlobSet>, database: &str, table: &str) -> bool {
    let database_with_table = format!("{database}.{table}");
    if let Some(include) = include {
        if !include.is_match(&database_with_table) {
            tracing::debug!("skipping table {database_with_table}");
            return false;
        }
    }
    true
}

fn get_metadata_location(
    parameters: Option<&HashMap<String, String>>,
    table: &str,
) -> Result<String> {
    const METADATA_LOCATION: &str = "metadata_location";
    match parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.to_string()),
            None => Err(Error::MissingMetadataLocation {
                table: table.to_string(),
            }),
        },
        None => Err(Error::MissingParameters),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_get_metadata_location_missing() {
        let params: Option<&HashMap<String, String>> = None;
        let result = get_metadata_location(params, "test_table");
        assert!(matches!(result, Err(Error::MissingParameters)));
    }
}
