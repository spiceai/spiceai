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

use super::{CatalogConnector, ParameterSpec, Parameters};
use crate::{
    Runtime,
    component::catalog::Catalog,
    dataconnector::{ConnectorComponent, ConnectorParams},
};
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_glue::{
    Client,
    error::SdkError,
    operation::{get_databases::GetDatabasesError, get_tables::GetTablesError},
    types::Table,
};
use aws_sdk_sts::config::{Credentials, ProvideCredentials};
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
    error::DataFusionError,
};
use globset::GlobSet;
use iceberg::{
    NamespaceIdent, TableIdent,
    io::{FileIOBuilder, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY},
    spec::TableMetadata,
    table::Table as IcebergTable,
};
use iceberg_datafusion::IcebergTableProvider;
use snafu::prelude::*;
use std::fmt;
use std::{any::Any, collections::HashMap, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get Glue databases: {source}"))]
    GetDatabases { source: SdkError<GetDatabasesError> },

    #[snafu(display("Failed to get Glue tables: {source}"))]
    GetTables { source: SdkError<GetTablesError> },

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A catalog connector for AWS Glue, providing access to database and table metadata.
#[derive(Clone)]
pub struct GlueCatalog {
    params: Parameters,
}

type DatabaseName = String;

/// A catalog provider for AWS Glue, managing databases and tables.
pub struct GlueCatalogProvider {
    inner: Arc<GlueCatalogState>,
}

impl fmt::Debug for GlueCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueCatalogProvider")
            .finish_non_exhaustive()
    }
}

struct GlueCatalogState {
    databases: HashMap<DatabaseName, Vec<Table>>,
    config: SdkConfig,
}

/// A schema provider for a specific Glue database, providing table metadata.
pub struct GlueSchemaProvider {
    database: String,
    inner: Arc<GlueCatalogState>,
}

impl fmt::Debug for GlueSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueSchemaProvider")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner
            .databases
            .get(&self.database)
            .map(|tables| tables.iter().map(|t| t.name.clone()).collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.databases.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(table) = self
            .inner
            .databases
            .get(&self.database)
            .and_then(|tables| tables.iter().find(|t| t.name() == name))
        else {
            tracing::error!(
                "Glue table name `{name}` not found in database `{}`",
                self.database
            );
            return Ok(None);
        };

        match TableType::from(table) {
            TableType::HiveParquet => {
                tracing::warn!("Hive Parquet files not supported yet");
                Ok(None)
            }
            TableType::Iceberg => {
                let mut props = Vec::new();
                if let Some(provider) = self.inner.config.credentials_provider() {
                    let creds = provider
                        .provide_credentials()
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    props.push((S3_ACCESS_KEY_ID, creds.access_key_id().to_string()));
                    props.push((S3_SECRET_ACCESS_KEY, creds.secret_access_key().to_string()));
                }

                let file_io = FileIOBuilder::new("s3")
                    .with_props(props)
                    .build()
                    .map_err(|e| {
                        DataFusionError::External(Box::new(Error::BuildFileIO { source: e }))
                    })?;

                let metadata_location = get_metadata_location(table.parameters.as_ref(), name)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let input_file = file_io.new_input(&metadata_location).map_err(|e| {
                    DataFusionError::External(Box::new(Error::CreateFileInput {
                        source: e,
                        location: metadata_location.clone(),
                    }))
                })?;

                let metadata_content = input_file.read().await.map_err(|e| {
                    DataFusionError::External(Box::new(Error::ReadMetadata {
                        source: e,
                        location: metadata_location.clone(),
                    }))
                })?;

                let metadata =
                    serde_json::from_slice::<TableMetadata>(&metadata_content).map_err(|e| {
                        DataFusionError::External(Box::new(Error::DeserializeMetadata {
                            source: e,
                        }))
                    })?;

                let identifier =
                    TableIdent::new(NamespaceIdent::new(self.database.clone()), name.to_string());

                let table = IcebergTable::builder()
                    .file_io(file_io)
                    .metadata(metadata)
                    .identifier(identifier)
                    .build()
                    .map_err(|e| {
                        DataFusionError::External(Box::new(Error::BuildIcebergTable { source: e }))
                    })?;

                let table_provider = IcebergTableProvider::try_new_from_table(table)
                    .await
                    .map_err(|e| {
                        DataFusionError::External(Box::new(Error::CreateIcebergTableProvider {
                            source: e,
                        }))
                    })?;

                Ok(Some(Arc::new(table_provider)))
            }
            TableType::Unsupported => Ok(None),
        }
    }
}

// copy from iceberg-catalog-glue internals
// https://github.com/apache/iceberg-rust/blob/main/crates/catalog/glue/src/utils.rs#L256
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

impl GlueCatalogProvider {
    pub async fn new(params: &Parameters, catalog: &Catalog) -> Result<Self> {
        let config = load_config(params).await;
        let glue = Client::new(&config);

        let get_databases_output = glue
            .get_databases()
            .send()
            .await
            .context(GetDatabasesSnafu)?;

        let mut databases = HashMap::new();
        for db in get_databases_output.database_list {
            if !database_might_match(&db.name, &catalog.orig_include) {
                tracing::debug!("skipping database {}", &db.name);
                continue;
            }

            let get_tables_output = glue
                .get_tables()
                .database_name(&db.name)
                .send()
                .await
                .context(GetTablesSnafu)?;

            let table_names = get_tables_output
                .table_list
                .unwrap_or_default()
                .into_iter()
                .filter(|t| {
                    is_supported(t) && is_included(catalog.include.as_ref(), &db.name, t.name())
                })
                .collect::<Vec<_>>();

            if !table_names.is_empty() {
                databases.insert(db.name, table_names);
            }
        }

        let inner = Arc::new(GlueCatalogState { databases, config });

        Ok(Self { inner })
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

fn is_supported(table: &Table) -> bool {
    if table
        .parameters
        .as_ref()
        .and_then(|params| params.get("table_type"))
        .is_some_and(|value| value.to_lowercase() == "iceberg")
    {
        return true; // Iceberg is supported
    }

    if table
        .storage_descriptor
        .as_ref()
        .and_then(|sd| sd.input_format.as_ref())
        .is_some_and(|input_format| {
            input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        })
    {
        return true; // Hive-style parquet is supported
    }

    false
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TableType {
    HiveParquet,
    Iceberg,
    Unsupported,
}

impl TableType {
    fn from(table: &Table) -> TableType {
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

fn is_included(include: Option<&GlobSet>, database: &str, table: &str) -> bool {
    let database_with_table = format!("{database}.{table}");
    if let Some(include) = include {
        if !include.is_match(&database_with_table) {
            tracing::debug!("skipping table {database_with_table}");
            return false;
        }
    }
    true
}

impl GlueCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("aws_region")
        .description("The AWS region to use for Glue.")
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID to use for Glue.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key to use for Glue.")
        .secret(),
    ParameterSpec::component("aws_session_token")
        .description("The AWS session token to use for Glue.")
        .secret(),
];

#[async_trait]
impl CatalogConnector for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        Ok(Arc::new(
            GlueCatalogProvider::new(&self.params, catalog)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: "glue".to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        ))
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.databases.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        if self.inner.databases.contains_key(name) {
            let schema_provider = GlueSchemaProvider {
                database: name.to_string(),
                inner: Arc::clone(&self.inner),
            };
            Some(Arc::new(schema_provider))
        } else {
            None
        }
    }
}

#[async_trait]
impl RefreshableCatalogProvider for GlueCatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: #6012
        Ok(())
    }
}

async fn load_config(params: &Parameters) -> SdkConfig {
    // Get and own all parameter values upfront
    let region = params
        .get("aws_region")
        .expose()
        .ok()
        .unwrap_or_else(|| {
            let region = "us-east-1";
            tracing::warn!("no AWS region specified, defaulting to {region}");
            region
        })
        .to_string();

    let access_key_id = params
        .get("aws_access_key_id")
        .expose()
        .ok()
        .map(ToString::to_string);

    let secret_access_key = params
        .get("aws_secret_access_key")
        .expose()
        .ok()
        .map(ToString::to_string);

    let session_token = params
        .get("aws_session_token")
        .expose()
        .ok()
        .map(ToString::to_string);

    match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let credentials = Credentials::new(
                access_key_id,
                secret_access_key,
                session_token,
                None,
                "GlueCatalogProvider",
            );

            aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region))
                .credentials_provider(credentials)
                .load()
                .await
        }
        _ => {
            // This will automatically load AWS credentials from the environment, via IAM roles if configured.
            aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region))
                .load()
                .await
        }
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
