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

use super::DatabaseName;
use crate::dataconnector::glue::{GlueDataConnector, InputFormat};
use crate::dataconnector::parameters::aws::initiate_config_with_credentials;
use crate::dataconnector::{DataConnector, parameters};
use crate::{
    Runtime,
    component::{catalog::Catalog, dataset::builder::DatasetBuilder},
    dataconnector::parameters::ConnectorParams,
};
use app::App;
use async_trait::async_trait;
use aws_sdk_glue::Client;
use aws_sdk_glue::error::SdkError;
use aws_sdk_glue::operation::get_databases::GetDatabasesError;
use aws_sdk_glue::operation::get_tables::GetTablesError;
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
};
use globset::GlobSet;
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use util::glob::schema_may_contain_selected_table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Cannot connect to AWS Glue to retrieve databases. Verify your AWS credentials and region are configured correctly. For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue  {source}"
    ))]
    GetDatabases { source: SdkError<GetDatabasesError> },

    #[snafu(display(
        "Cannot retrieve tables from Glue database '{database}'. Verify the database exists and you have permissions to access it. {source}"
    ))]
    GetTables {
        database: String,
        source: SdkError<GetTablesError>,
    },

    #[snafu(display(
        "Cannot create dataset for table `{dataset}`. Verify the table configuration and format are supported. For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue {source}"
    ))]
    CreatingDataset {
        dataset: String,
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display(
        "Cannot load AWS configuration for Glue catalog. Verify your AWS credentials and region settings. For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue {source}"
    ))]
    ConfigurationLoadingFailed {
        #[snafu(source)]
        source: parameters::aws::Error,
    },

    #[snafu(display(
        "Invalid AWS configuration for Glue catalog. Verify your region, credentials, and other AWS parameters are correct. For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue {source}",
    ))]
    ParameterValidation {
        #[snafu(source)]
        source: parameters::aws::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A catalog provider for AWS Glue, managing databases and tables.
pub struct GlueCatalogProvider {
    client: Client,
    include: Option<GlobSet>,
    orig_include: Vec<String>,
    runtime: Arc<Runtime>,
    app: Arc<App>,
    parameters: ConnectorParams,
    catalog_id: Option<String>,
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

        let config = initiate_config_with_credentials(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &parameters.parameters,
            parameters.parameters.get("iam_role_source").expose().ok(),
        )
        .await
        .context(ConfigurationLoadingFailedSnafu)?
        .load()
        .await;

        let client = Client::new(&config);

        let databases = RwLock::new(HashMap::new());

        Ok(Self {
            client,
            include: catalog.include.clone(),
            orig_include: catalog.orig_include.clone(),
            runtime,
            app,
            databases,
            catalog_id: catalog.catalog_id.clone(),
            parameters,
        })
    }

    async fn create_schema_provider(&self, database: String) -> Result<Arc<dyn SchemaProvider>> {
        let mut tables_builder = self.client.get_tables().database_name(&database);

        if let Some(catalog_id) = &self.catalog_id {
            tables_builder = tables_builder.catalog_id(catalog_id);
        }

        let mut paginator = tables_builder.into_paginator().send();

        let mut tables = HashMap::new();

        while let Some(maybe_get_tables_output) = paginator.next().await {
            let get_tables_output = maybe_get_tables_output.context(GetTablesSnafu {
                database: database.clone(),
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
                let mut parameters = self.parameters.parameters.clone();
                if let Some(catalog_id) = &self.catalog_id {
                    parameters.insert("catalog_id".to_string(), catalog_id.clone().into());
                }

                let connector = GlueDataConnector::new(
                    parameters,
                    self.parameters.app(),
                    self.parameters.io_runtime.clone(),
                );
                let from = format!("{database}.{}", table.name());
                let runtime = Arc::clone(&self.runtime);
                let dataset = DatasetBuilder::try_new(from, table.name())
                    .boxed()
                    .context(CreatingDatasetSnafu {
                        dataset: table.name().to_string(),
                    })?
                    .with_app(Arc::clone(&self.app))
                    .with_runtime(runtime)
                    .build()
                    .boxed()
                    .context(CreatingDatasetSnafu {
                        dataset: table.name().to_string(),
                    })?;
                let table_provider = connector.read_provider(&dataset).await.boxed().context(
                    CreatingDatasetSnafu {
                        dataset: table.name().to_string(),
                    },
                )?;
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
                .context(ParameterValidationSnafu)?;
        }

        Ok(())
    }
}

impl CatalogProvider for GlueCatalogProvider {
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
        let mut databases_builder = self.client.get_databases();

        if let Some(catalog_id) = &self.catalog_id {
            databases_builder = databases_builder.catalog_id(catalog_id);
        }

        let mut paginator = databases_builder.into_paginator().send();

        let mut databases = HashMap::new();

        while let Some(maybe_get_databases_output) = paginator.next().await {
            let get_databases_output = maybe_get_databases_output.context(GetDatabasesSnafu)?;
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

/// Whether a Glue database is worth interrogating for tables, decided from the
/// raw `include` patterns before any `GetTables` call.
///
/// This must be a *necessary* condition for a table of `database` to be
/// selected: answering `false` skips the database entirely, so a wrong `false`
/// silently drops tables the compiled `GlobSet` would have matched. The shared
/// literal-prefix analysis in [`util::glob`] provides that guarantee for every
/// glob shape; each table is still filtered by [`is_included`] against the
/// compiled set.
fn database_might_match(database: &str, patterns: &[String]) -> bool {
    schema_may_contain_selected_table(database, patterns)
}

fn is_included(include: Option<&globset::GlobSet>, database: &str, table: &str) -> bool {
    let database_with_table = format!("{database}.{table}");
    if let Some(include) = include
        && !include.is_match(&database_with_table)
    {
        tracing::debug!("skipping table {database_with_table}");
        return false;
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
        assert!(database_might_match("mydb", &patterns));
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

    /// Pattern shapes that `database_might_match` used to reject outright, each
    /// naming a database whose tables `is_included` accepts. Regression test for
    /// #12630.
    #[test]
    fn database_might_match_keeps_partial_wildcard_and_class_shapes() {
        for (pattern, database) in [
            ("sales_*.orders", "sales_east"),
            ("sales_*.*", "sales_east"),
            ("*", "public"),
            ("{public,sales}.*", "public"),
            ("[ps]ublic.*", "public"),
            ("?ublic.orders", "public"),
        ] {
            let patterns = vec![pattern.to_string()];
            assert!(
                database_might_match(database, &patterns),
                "pattern {pattern} must not skip database {database}"
            );
        }
    }

    /// A database no pattern can name is still skipped -- the fix must not turn
    /// the pre-filter into an unconditional `true`.
    #[test]
    fn database_might_match_still_skips_an_unreachable_database() {
        let patterns = vec![
            "public.*".to_string(),
            "sales_*.orders".to_string(),
            "otherdb".to_string(),
        ];
        assert!(!database_might_match("warehouse", &patterns));
    }

    /// The invariant that ties the two filters together: the raw patterns decide
    /// which databases are interrogated (`orig_include`) and the compiled set
    /// decides which tables survive (`include`). Both come from the same
    /// configured list, so a table `is_included` accepts must live in a database
    /// `database_might_match` kept -- otherwise it is silently absent from the
    /// catalog.
    #[test]
    fn a_database_holding_an_included_table_is_never_skipped() {
        let patterns = [
            "public.orders",
            "public.*",
            "*.orders",
            "*",
            "*.*",
            "sales_*.orders",
            "sales_*.*",
            "{public,sales}.*",
            "[ps]ublic.*",
            "?ublic.orders",
            "public.ord*",
            "otherdb.*",
        ];
        let databases = ["public", "sales", "sales_east", "salesx", "otherdb", "p"];
        let tables = ["orders", "line_item", "o"];

        for pattern in patterns {
            let mut builder = GlobSetBuilder::new();
            builder.add(Glob::new(pattern).expect("builder add"));
            let globset = builder.build().expect("builder build");
            let orig_include = vec![pattern.to_string()];

            for database in databases {
                let kept = database_might_match(database, &orig_include);
                for table in tables {
                    assert!(
                        !is_included(Some(&globset), database, table) || kept,
                        "pattern {pattern} includes table {database}.{table} but skipped database {database}"
                    );
                }
            }
        }
    }
}
