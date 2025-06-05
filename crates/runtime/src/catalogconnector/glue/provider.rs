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
use super::{DatabaseName, Result};
use crate::dataconnector::DataConnector;
use crate::dataconnector::glue::GlueDataConnector;
use crate::{
    Runtime,
    component::{catalog::Catalog, dataset::builder::DatasetBuilder},
    dataconnector::parameters::ConnectorParams,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
    error::DataFusionError,
};
use snafu::ResultExt as _;
use std::sync::Arc;
use std::{any::Any, fmt};

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
        let connector = GlueDataConnector::new(self.state.parameters.parameters.clone());
        let from = format!("{}.{name}", self.database);
        let runtime = Arc::clone(&self.state.runtime);
        let app = runtime
            .app()
            .read()
            .await
            .clone()
            .ok_or_else(|| DataFusionError::External("no app".into()))?;
        let dataset = DatasetBuilder::try_new(from, name)
            .map_err(|e| DataFusionError::External(e.into()))?
            .with_app(app)
            .with_runtime(runtime)
            .build()
            .map_err(|e| DataFusionError::External(e.into()))?;
        connector
            .read_provider(&dataset)
            .await
            .map(Option::Some)
            .map_err(|e| DataFusionError::External(e.into()))
    }
}
