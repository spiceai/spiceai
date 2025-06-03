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

use super::state::GlueCatalogState;
use super::{DatabaseName, Error, Result, TableType};
use crate::dataconnector::DataConnectorFactory as _;
use crate::dataconnector::parameters::aws::load_config;
use crate::{
    Runtime,
    component::{catalog::Catalog, dataset::builder::DatasetBuilder},
    dataconnector::{parameters::ConnectorParams, s3::S3Factory},
};
use async_trait::async_trait;
use aws_sdk_glue::{Client, types::Table};
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
    error::DataFusionError,
};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_datafusion::IcebergTableProvider;
use snafu::prelude::*;
use std::sync::Arc;
use std::{any::Any, collections::HashMap, fmt};

/// A catalog provider for AWS Glue, managing databases and tables.
pub struct GlueCatalogProvider {
    pub(super) inner: Arc<GlueCatalogState>,
}

impl fmt::Debug for GlueCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueCatalogProvider")
            .finish_non_exhaustive()
    }
}

/// A schema provider for a specific Glue database, providing table metadata.
#[derive(Debug)]
pub struct GlueSchemaProvider {
    database: DatabaseName,
    state: Arc<GlueCatalogState>,
}

impl GlueCatalogProvider {
    pub async fn new(
        mut parameters: ConnectorParams,
        catalog: &Catalog,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        for validator in super::VALIDATORS.iter() {
            validator
                .validate(&mut parameters)
                .await
                .context(super::ParameterValidationSnafu)?;
        }

        // `file_format` is required early for ListingConnector which the S3
        // connector uses. We can change the file format when we create
        // TableProviders if we need to.
        parameters
            .parameters
            .insert("file_format".to_string(), "parquet".into());

        let config = load_config(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &parameters.parameters,
        )
        .await
        .map_err(|message| super::Error::ConfigurationLoadingFailed { message })?;
        let glue = Client::new(&config);

        let get_databases_output = glue
            .get_databases()
            .send()
            .await
            .context(super::GetDatabasesSnafu)?;

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
                .map_err(|source| super::Error::GetTables {
                    database: db.name.to_string(),
                    source,
                })?;

            let table_names = get_tables_output
                .table_list
                .unwrap_or_default()
                .into_iter()
                .filter(|t| {
                    !matches!(TableType::from(t), TableType::Unsupported)
                        && is_included(catalog.include.as_ref(), &db.name, t.name())
                })
                .collect::<Vec<_>>();

            databases.insert(db.name, table_names);
        }

        let inner = Arc::new(GlueCatalogState::new(databases, parameters, runtime));

        Ok(Self { inner })
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
                state: Arc::clone(&self.inner),
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

impl GlueSchemaProvider {
    async fn create_iceberg_provider(
        &self,
        name: &str,
        table: &Table,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let metadata_location = get_metadata_location(table.parameters.as_ref(), name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let identifier =
            TableIdent::new(NamespaceIdent::new(self.database.clone()), name.to_string());

        let config = GlueCatalogConfig::builder()
            .warehouse(metadata_location)
            .build();
        let catalog = GlueCatalog::new(config)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        let table_provider = IcebergTableProvider::try_new(Arc::new(catalog), identifier)
            .await
            .map_err(|e| {
                DataFusionError::External(Box::new(super::Error::CreateIcebergTableProvider {
                    source: e,
                }))
            })?;

        Ok(Some(Arc::new(table_provider)))
    }

    async fn create_hive_parquet_provider(
        &self,
        name: &str,
        table: &Table,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(storage_descriptor) = table.storage_descriptor() else {
            return Err(DataFusionError::External(
                format!("table `{name}` does not have a storage descriptor").into(),
            ));
        };

        let Some(mut from) = storage_descriptor.location().map(String::from) else {
            return Err(DataFusionError::External(
                format!("table `{name}` does not have a location").into(),
            ));
        };

        if !from.ends_with('/') {
            from.push('/');
        }

        let mut params = self.state.parameters.clone();
        params
            .parameters
            .insert("endpoint".into(), from.clone().into());
        params.parameters.prefix = "s3";

        let s3 = S3Factory::new()
            .create(self.state.parameters.clone())
            .await
            .map_err(DataFusionError::External)?;

        let app = self
            .state
            .parameters
            .app
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| DataFusionError::External("Missing application".into()))?;

        let mut dataset = DatasetBuilder::try_new(from, name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_runtime(Arc::clone(&self.state.runtime))
            .with_app(app)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        dataset
            .params
            .insert("hive_partitioning_enabled".to_string(), "true".to_string());

        let provider = s3
            .read_provider(&dataset)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Some(provider))
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.state
            .databases
            .get(&self.database)
            .map(|tables| tables.iter().map(|t| t.name.clone()).collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.state.databases.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(table) = self
            .state
            .databases
            .get(&self.database)
            .and_then(|tables| tables.iter().find(|t| t.name() == name))
        else {
            tracing::error!(
                "Glue table `{name}` not found in database `{}`",
                self.database
            );
            return Ok(None);
        };

        match TableType::from(table) {
            TableType::HiveParquet => self.create_hive_parquet_provider(name, table).await,
            TableType::Iceberg => self.create_iceberg_provider(name, table).await,
            TableType::Unsupported => Ok(None),
        }
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
    use globset::{Glob, GlobSetBuilder};
    use std::collections::HashMap;

    #[test]
    fn database_might_match_exact_match() {
        let patterns = vec!["mydb".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_prefix_match() {
        let patterns = vec!["mydb.table1".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_wildcard_prefix() {
        let patterns = vec!["*.table1".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_wildcard_all() {
        let patterns = vec!["*.*".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_no_match() {
        let patterns = vec!["otherdb".to_string(), "otherdb.table1".to_string()];
        assert!(!database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_empty_patterns() {
        let patterns: Vec<String> = vec![];
        assert!(!database_might_match("mydb", &patterns));
    }

    #[test]
    fn is_included_no_globset() {
        assert!(is_included(None, "mydb", "table1"));
    }

    #[test]
    fn is_included_matching_glob() {
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new("mydb.table1").expect("builder add"));
        let globset = builder.build().expect("builder build");
        assert!(is_included(Some(&globset), "mydb", "table1"));
    }

    #[test]
    fn is_included_non_matching_glob() {
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new("otherdb.table1").expect("builder add"));
        let globset = builder.build().expect("builder build");
        assert!(!is_included(Some(&globset), "mydb", "table1"));
    }

    #[test]
    fn is_included_wildcard_glob() {
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new("*.table1").expect("builder add"));
        let globset = builder.build().expect("builder build");
        assert!(is_included(Some(&globset), "mydb", "table1"));
    }

    #[test]
    fn get_metadata_location_success() {
        let mut params = HashMap::new();
        params.insert(
            "metadata_location".to_string(),
            "s3://bucket/path".to_string(),
        );
        let result = get_metadata_location(Some(&params), "table").expect("metadata");
        assert_eq!(result, "s3://bucket/path");
    }

    #[test]
    fn get_metadata_location_missing_location() {
        let params = HashMap::new();
        let result = get_metadata_location(Some(&params), "table");
        assert!(matches!(result, Err(Error::MissingMetadataLocation { .. })));
        if let Err(Error::MissingMetadataLocation { table }) = result {
            assert_eq!(table, "table");
        }
    }

    #[tokio::test]
    async fn get_metadata_location_missing() {
        let params: Option<&HashMap<String, String>> = None;
        let result = get_metadata_location(params, "table");
        assert!(matches!(result, Err(Error::MissingParameters)));
    }
}
