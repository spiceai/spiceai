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
    component::{
        catalog::{Catalog, table_selector},
        dataset::builder::DatasetBuilder,
    },
    dataconnector::parameters::ConnectorParams,
};
use app::App;
use async_trait::async_trait;
use aws_sdk_glue::Client;
use aws_sdk_glue::error::SdkError;
use aws_sdk_glue::operation::get_databases::GetDatabasesError;
use aws_sdk_glue::operation::get_tables::GetTablesError;
use data_components::RefreshableCatalogProvider;
use data_components::catalog_filter::TableSelector;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
};
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

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
    selector: TableSelector,
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
            selector: table_selector(catalog),
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
                        && is_selected(&self.selector, &database, t.name())
                })
                .collect::<Vec<_>>();

            for table in some_tables {
                let mut parameters = self.parameters.parameters.clone();
                if let Some(catalog_id) = &self.catalog_id {
                    parameters.insert("catalog_id".to_string(), catalog_id.clone().into());
                }

                let connector =
                    GlueDataConnector::new(parameters, self.parameters.io_runtime.clone());
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
                // A database no `include` pattern can reach cannot contribute a
                // table, so skip its `GetTables` pagination entirely. The prune
                // is a necessary condition only -- a wrong `false` would drop
                // tables silently -- and every database it keeps is still
                // filtered table by table through [`is_selected`].
                if !self.selector.may_select_within(&db.name) {
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

/// Whether the catalog registers `{database}.{table}`.
///
/// A free function taking the selector, rather than a method, so the pairing
/// with [`TableSelector::may_select_within`] can be asserted without an AWS
/// client.
fn is_selected(selector: &TableSelector, database: &str, table: &str) -> bool {
    let database_with_table = format!("{database}.{table}");
    if let Some(reason) = selector.rejection_reason(&database_with_table) {
        tracing::debug!("skipping table {database_with_table} ({reason})");
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use globset::{Glob, GlobSet, GlobSetBuilder};
    use runtime_component::catalog::CatalogSpec;

    fn globset(patterns: &[&str]) -> Option<GlobSet> {
        // `compile_globset` yields `None` for an empty list, so an unconfigured
        // half must reach the selector as `None` rather than as a set matching
        // nothing.
        if patterns.is_empty() {
            return None;
        }
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(Glob::new(pattern).expect("test pattern is a valid glob"));
        }
        Some(builder.build().expect("test patterns build into a GlobSet"))
    }

    /// A selector shaped exactly as [`table_selector`] builds one from a
    /// catalog's configuration.
    fn selector(include: &[&str], exclude: &[&str]) -> TableSelector {
        let owned = |patterns: &[&str]| -> Vec<String> {
            patterns.iter().map(|p| (*p).to_string()).collect()
        };
        TableSelector::new(globset(include), globset(exclude))
            .with_include_patterns(&owned(include))
            .with_exclude_patterns(&owned(exclude))
    }

    /// The configuration a Glue catalog is given, as far as the two predicates
    /// this file applies are concerned.
    fn catalog_spec(include: &[&str], exclude: &[&str]) -> CatalogSpec {
        CatalogSpec {
            provider: "glue".to_string(),
            catalog_id: None,
            from: "glue".to_string(),
            name: "glue".to_string(),
            access: crate::component::access::AccessMode::default(),
            orig_include: include.iter().map(|p| (*p).to_string()).collect(),
            include: globset(include),
            orig_exclude: exclude.iter().map(|p| (*p).to_string()).collect(),
            exclude: globset(exclude),
            params: HashMap::default(),
            dataset_params: HashMap::default(),
            acceleration: None,
        }
    }

    #[test]
    fn database_prune_keeps_an_exactly_named_database() {
        assert!(selector(&["mydb"], &[]).may_select_within("mydb"));
        assert!(selector(&["mydb.table1"], &[]).may_select_within("mydb"));
    }

    #[test]
    fn database_prune_keeps_a_wildcard_database_component() {
        assert!(selector(&["*.table1"], &[]).may_select_within("mydb"));
        assert!(selector(&["*.*"], &[]).may_select_within("mydb"));
    }

    #[test]
    fn database_prune_drops_a_database_no_pattern_can_reach() {
        assert!(!selector(&["otherdb", "otherdb.table1"], &[]).may_select_within("mydb"));
    }

    #[test]
    fn database_prune_is_disabled_without_include_patterns() {
        assert!(selector(&[], &[]).may_select_within("mydb"));
    }

    /// Pattern shapes the database prune used to reject outright, each naming a
    /// database whose tables the per-table filter accepts. Regression test for
    /// #12630.
    #[test]
    fn database_prune_keeps_partial_wildcard_and_class_shapes() {
        for (pattern, database) in [
            ("sales_*.orders", "sales_east"),
            ("sales_*.*", "sales_east"),
            ("*", "public"),
            ("{public,sales}.*", "public"),
            ("[ps]ublic.*", "public"),
            ("?ublic.orders", "public"),
        ] {
            assert!(
                selector(&[pattern], &[]).may_select_within(database),
                "pattern {pattern} must not skip database {database}"
            );
        }
    }

    /// A database no pattern can name is still skipped -- the prune must not
    /// degrade into an unconditional `true`.
    #[test]
    fn database_prune_still_skips_an_unreachable_database() {
        let selector = selector(&["public.*", "sales_*.orders", "otherdb"], &[]);
        assert!(!selector.may_select_within("warehouse"));
    }

    #[test]
    fn is_selected_without_patterns_keeps_every_table() {
        assert!(is_selected(&selector(&[], &[]), "mydb", "table1"));
    }

    #[test]
    fn is_selected_honors_include() {
        assert!(is_selected(
            &selector(&["mydb.table1"], &[]),
            "mydb",
            "table1"
        ));
        assert!(!is_selected(
            &selector(&["otherdb.table1"], &[]),
            "mydb",
            "table1"
        ));
        assert!(is_selected(&selector(&["*.table1"], &[]), "mydb", "table1"));
    }

    /// The bug this change fixes (#12634): the connector compiled `exclude` and
    /// then never consulted it, so a table the user asked to keep out was
    /// registered anyway.
    #[test]
    fn is_selected_withholds_an_excluded_table() {
        let selector = selector(&["public.*"], &["public.audit_log"]);
        assert!(is_selected(&selector, "public", "orders"));
        assert!(
            !is_selected(&selector, "public", "audit_log"),
            "a table matched by `exclude` must not be registered"
        );
    }

    /// `exclude` is a veto: a table matched by *both* halves is withheld.
    #[test]
    fn exclude_wins_over_include() {
        let selector = selector(&["public.audit_log"], &["public.audit_log"]);
        assert!(!is_selected(&selector, "public", "audit_log"));
    }

    /// An `exclude` with no `include` still withholds. This is the shape that
    /// reads most obviously as "keep this table out" and was ignored entirely.
    #[test]
    fn exclude_applies_without_an_include() {
        let selector = selector(&[], &["public.audit_log"]);
        assert!(is_selected(&selector, "public", "orders"));
        assert!(!is_selected(&selector, "public", "audit_log"));
        assert!(is_selected(&selector, "private", "audit_log"));
    }

    /// `exclude` only ever removes: a non-matching pattern leaves the table
    /// present, and an excluded name outside `include` does not become included.
    #[test]
    fn exclude_does_not_widen_or_narrow_beyond_its_match() {
        let selector = selector(&["public.*"], &["private.audit_log"]);
        assert!(is_selected(&selector, "public", "audit_log"));
        assert!(!is_selected(&selector, "reporting", "orders"));
    }

    /// A wildcard `exclude` removes every table in a database, while the
    /// database itself is still interrogated -- the prune reads `include` only,
    /// and proving an `exclude` covers *every* table a database can hold is a
    /// far stronger claim than it can make.
    #[test]
    fn a_wildcard_exclude_withholds_the_whole_database() {
        let selector = selector(&[], &["private.*"]);
        assert!(!is_selected(&selector, "private", "orders"));
        assert!(!is_selected(&selector, "private", "secrets"));
        assert!(is_selected(&selector, "public", "orders"));
        assert!(selector.may_select_within("private"));
    }

    /// The whole configuration chain the connector reads: a catalog's compiled
    /// `include`/`exclude` reach both of this file's predicates through
    /// [`table_selector`], which is what `GlueCatalogProvider::new` builds its
    /// selector with.
    #[test]
    fn a_catalogs_exclude_reaches_both_glue_predicates() {
        let selector = table_selector(&catalog_spec(&["public.*"], &["public.audit_log"]));

        assert!(is_selected(&selector, "public", "orders"));
        assert!(!is_selected(&selector, "public", "audit_log"));
        assert!(selector.may_select_within("public"));
        assert!(!selector.may_select_within("private"));
    }

    /// The invariant tying the two filters together: the raw patterns decide
    /// which databases are interrogated and the compiled sets decide which
    /// tables survive. A table `is_selected` accepts must live in a database the
    /// prune kept -- otherwise it is silently absent from the catalog.
    ///
    /// Exercised with `exclude` sets too, because `exclude` narrows
    /// `is_selected` without narrowing the prune: that direction is safe, and
    /// the reverse would not be.
    #[test]
    fn a_database_holding_a_selected_table_is_never_skipped() {
        let includes = [
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
        let excludes: &[&[&str]] = &[
            &[],
            &["public.orders"],
            &["public.*"],
            &["*.orders"],
            &["sales_east.*"],
        ];
        let databases = ["public", "sales", "sales_east", "salesx", "otherdb", "p"];
        let tables = ["orders", "line_item", "o"];

        for include in includes {
            for exclude in excludes {
                let selector = selector(&[include], exclude);
                for database in databases {
                    let kept = selector.may_select_within(database);
                    for table in tables {
                        assert!(
                            !is_selected(&selector, database, table) || kept,
                            "include {include} with exclude {exclude:?} selects \
                             {database}.{table} but skipped database {database}"
                        );
                    }
                }
            }
        }
    }
}
