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
use crate::{
    Runtime,
    component::{catalog::Catalog, dataset::builder::DatasetBuilder},
    dataconnector::{parameters::ConnectorParams, s3::S3Factory},
};
use async_trait::async_trait;
use aws_sdk_glue::types::Table;
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
    error::DataFusionError,
};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_datafusion::IcebergTableProvider;
use snafu::ResultExt;
use std::sync::Arc;
use std::{any::Any, collections::HashMap, fmt};

/// A catalog provider for AWS Glue, managing databases and tables.
pub struct GlueCatalogProvider {
    pub(super) state: Arc<GlueCatalogState>,
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
        parameters: ConnectorParams,
        catalog: &Catalog,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let state = GlueCatalogState::new(
            catalog.include.clone(),
            catalog.orig_include.clone(),
            parameters,
            runtime,
        )
        .await?;

        // Load the catalog for the first time
        state.refresh().await.context(super::RefreshFailedSnafu)?;

        Ok(Self {
            state: Arc::new(state),
        })
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let databases = match self.state.databases.read() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        databases.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        let databases = match self.state.databases.read() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        if databases.contains_key(name) {
            let schema_provider = GlueSchemaProvider {
                database: name.to_string(),
                state: Arc::clone(&self.state),
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
        self.state.refresh().await
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
        let databases = match self.state.databases.read() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        databases
            .get(&self.database)
            .map(|tables| tables.iter().map(|t| t.name.clone()).collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        let databases = match self.state.databases.read() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        databases.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let table = {
            let databases = match self.state.databases.read() {
                Ok(dbs) => dbs,
                Err(poisoned) => poisoned.into_inner(),
            };
            databases
                .get(&self.database)
                .and_then(|tables| tables.iter().find(|t| t.name() == name))
                .cloned()
        };

        let Some(table) = table else {
            tracing::error!(
                "Glue table `{name}` not found in database `{}`",
                self.database
            );
            return Ok(None);
        };

        match TableType::from(&table) {
            TableType::HiveParquet => self.create_hive_parquet_provider(name, &table).await,
            TableType::Iceberg => self.create_iceberg_provider(name, &table).await,
            TableType::Unsupported => Ok(None),
        }
    }
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
