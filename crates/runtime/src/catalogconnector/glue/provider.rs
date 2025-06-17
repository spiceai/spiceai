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

use super::{DatabaseName, Result};
use crate::catalogconnector::glue::ConfigurationLoadingFailedSnafu;
use crate::dataconnector::DataConnector;
use crate::dataconnector::glue::{GlueDataConnector, InputFormat};
use crate::dataconnector::parameters::aws::load_config;
use crate::{
    Runtime,
    component::{catalog::Catalog, dataset::builder::DatasetBuilder},
    dataconnector::parameters::ConnectorParams,
};
use app::App;
use async_trait::async_trait;
use aws_sdk_glue::Client;
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
};
use globset::GlobSet;
use snafu::ResultExt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{any::Any, fmt};

/// A catalog provider for AWS Glue, managing databases and tables.
pub struct GlueCatalogProvider {
    client: Client,
    include: Option<GlobSet>,
    orig_include: Vec<String>,
    runtime: Arc<Runtime>,
    app: Arc<App>,
    parameters: ConnectorParams,
    databases: RwLock<HashMap<DatabaseName, Arc<dyn SchemaProvider>>>,
}

impl fmt::Debug for GlueCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueCatalogProvider")
            .finish_non_exhaustive()
    }
}

type TableName = String;

/// A schema provider for a specific Glue database, providing table metadata.
#[derive(Debug)]
pub struct GlueSchemaProvider {
    tables: RwLock<HashMap<TableName, Arc<dyn TableProvider>>>,
}

impl GlueCatalogProvider {
    pub async fn new(
        mut parameters: ConnectorParams,
        catalog: &Catalog,
        runtime: Arc<Runtime>,
        app: Arc<App>,
    ) -> Result<Self> {
        Self::validate_parameters(&mut parameters).await?;

        let config = load_config(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &parameters.parameters,
        )
        .await
        .context(ConfigurationLoadingFailedSnafu)?;

        let client = Client::new(&config);

        let databases = RwLock::new(HashMap::new());

        Ok(Self {
            client,
            include: catalog.include.clone(),
            orig_include: catalog.orig_include.clone(),
            runtime,
            app,
            databases,
            parameters,
        })
    }

    async fn create_schema_provider(&self, database: String) -> Result<Arc<dyn SchemaProvider>> {
        let mut paginator = self
            .client
            .get_tables()
            .database_name(&database)
            .into_paginator()
            .send();

        let mut tables = HashMap::new();

        while let Some(maybe_get_tables_output) = paginator.next().await {
            let get_tables_output =
                maybe_get_tables_output.map_err(|source| super::Error::GetTables {
                    database: database.clone(),
                    source,
                })?;
            let some_tables = get_tables_output
                .table_list
                .unwrap_or_default()
                .into_iter()
                .filter(|t| {
                    InputFormat::try_from(t).is_ok()
                        && is_included(self.include.as_ref(), &database, t.name())
                })
                .collect::<Vec<_>>();

            for table in some_tables {
                let connector = GlueDataConnector::new(self.parameters.parameters.clone());
                let from = format!("{database}.{}", table.name());
                let runtime = Arc::clone(&self.runtime);
                let dataset = DatasetBuilder::try_new(from, table.name())
                    .map_err(|e| super::Error::CreatingDataset {
                        dataset: table.name().to_string(),
                        source: e.into(),
                    })?
                    .with_app(Arc::clone(&self.app))
                    .with_runtime(runtime)
                    .build()
                    .map_err(|e| super::Error::CreatingDataset {
                        dataset: table.name().to_string(),
                        source: e.into(),
                    })?;
                let table_provider = connector.read_provider(&dataset).await.map_err(|e| {
                    super::Error::CreatingDataset {
                        dataset: table.name().to_string(),
                        source: e.into(),
                    }
                })?;
                tables.insert(table.name, table_provider);
            }
        }

        let tables = RwLock::new(tables);
        let schema_provider = GlueSchemaProvider { tables };

        Ok(Arc::new(schema_provider))
    }

    async fn validate_parameters(parameters: &mut ConnectorParams) -> Result<()> {
        for validator in super::VALIDATORS.iter() {
            validator
                .validate(parameters)
                .await
                .context(super::ParameterValidationSnafu)?;
        }

        Ok(())
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Schema, here, refers to Glue databases
        let databases = match self.databases.read() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        databases.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        let databases = match self.databases.read() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        databases.get(name).cloned()
    }
}

#[async_trait]
impl RefreshableCatalogProvider for GlueCatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut paginator = self.client.get_databases().into_paginator().send();

        let mut databases = HashMap::new();

        while let Some(maybe_get_databases_output) = paginator.next().await {
            let get_databases_output =
                maybe_get_databases_output.context(super::GetDatabasesSnafu)?;
            for db in get_databases_output.database_list {
                if !database_might_match(&db.name, &self.orig_include) {
                    tracing::debug!("skipping database {}", &db.name);
                    continue;
                }

                let schema_provider = self.create_schema_provider(db.name().to_string()).await?;

                databases.insert(db.name, schema_provider);
            }
        }
        let mut dbs = match self.databases.write() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        *dbs = databases;

        Ok(())
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = match self.tables.read() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = match self.tables.read() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        tables.get(name).is_some()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let tables = match self.tables.read() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        Ok(tables.get(name).cloned())
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

#[cfg(test)]
mod tests {
    use super::*;
    use globset::{Glob, GlobSetBuilder};

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
}
