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
use aws_sdk_glue::types::Table;
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
        let mut unreadable: Vec<String> = Vec::new();

        while let Some(maybe_get_tables_output) = paginator.next().await {
            let get_tables_output = maybe_get_tables_output.context(GetTablesSnafu {
                database: database.clone(),
            })?;
            let some_tables = get_tables_output
                .table_list
                .unwrap_or_default()
                .into_iter()
                .filter(|t| {
                    // Selection first: a table the catalog's `exclude:` withholds
                    // is not one to report as unreadable.
                    is_selected(&self.selector, &database, t.name())
                        && is_readable(&database, t, &mut unreadable)
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

        if let Some(summary) = unreadable_tables_summary(&database, &unreadable) {
            tracing::warn!("{summary}");
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
                // table, so skip its `GetTables` pagination entirely; every
                // database that survives is still filtered table by table.
                //
                // Skipping the database also drops it from `schema_names`, so
                // pruning narrows this catalog's namespace — unlike the
                // `PostgreSQL` connector, which registers a pruned schema empty
                // and keeps the namespace fixed.
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

/// Whether the catalog registers `{database}.{table}`, logging which half of
/// the configuration withheld it when it does not.
///
/// [`TableSelector::selects_table`] answers the same question without the
/// diagnostic; the log is the reason this wrapper exists, and it matches what
/// the `adbc` and `PostgreSQL` connectors do at their own call sites. Taking
/// the already-joined name also keeps it to one `format!` per table, which
/// `selects_table` would double.
fn is_selected(selector: &TableSelector, database: &str, table: &str) -> bool {
    let database_with_table = format!("{database}.{table}");
    if let Some(reason) = selector.rejection_reason(&database_with_table) {
        tracing::debug!("skipping table {database_with_table} ({reason})");
        return false;
    }
    true
}

/// Whether Spice can read `table`'s storage format, recording its name in
/// `unreadable` when it cannot.
///
/// [`InputFormat::try_from`] already names the table and the offending format in
/// a structured error, and `is_ok()` discarded it: a table Spice cannot read was
/// simply absent from the catalog, with no string anywhere in the log for an
/// operator to search for. The reason goes to `debug!` per table, matching
/// [`is_selected`]; [`unreadable_tables_summary`] is what an operator sees by
/// default.
fn is_readable(database: &str, table: &Table, unreadable: &mut Vec<String>) -> bool {
    match InputFormat::try_from(table) {
        Ok(_) => true,
        Err(err) => {
            tracing::debug!("skipping table {database}.{} ({err})", table.name());
            unreadable.push(table.name().to_string());
            false
        }
    }
}

/// The one line an operator sees for the tables a Glue database holds that Spice
/// cannot read, or `None` when it can read all of them.
///
/// One line per database rather than one per table because
/// `RefreshableCatalogProvider::refresh` rebuilds every schema provider on each
/// cycle: a per-table warning would repeat for the life of the process, and a
/// database of ORC tables would bury everything else. The names are sampled for
/// the same reason, and the count names how many were left out rather than
/// truncating silently.
fn unreadable_tables_summary(database: &str, unreadable: &[String]) -> Option<String> {
    /// How many table names the summary spells out before falling back to a count.
    const SAMPLE: usize = 5;

    if unreadable.is_empty() {
        return None;
    }

    let total = unreadable.len();
    let named: Vec<&str> = unreadable.iter().take(SAMPLE).map(String::as_str).collect();
    let elided = total - named.len();
    let elided_note = if elided > 0 {
        format!(", and {elided} more")
    } else {
        String::new()
    };
    let named = named.join(", ");

    Some(format!(
        "Skipping {total} table(s) in Glue database '{database}' that Spice cannot read: {named}{elided_note}. \
        Supported formats are Parquet, CSV and Iceberg; run with debug logging to see why each \
        table was skipped. For help, visit: https://docs.spiceai.org/components/catalogs/glue"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::catalog::CatalogBuilder;
    use spicepod::component::catalog as spicepod_catalog;

    /// A selector built the way the runtime builds one: from a spicepod
    /// catalog's `include:`/`exclude:`, through `CatalogBuilder`'s real
    /// `compile_globset`. Hand-populating a `CatalogSpec` instead would skip the
    /// compile step the user's patterns actually travel.
    fn selector(include: &[&str], exclude: &[&str]) -> TableSelector {
        let catalog = spicepod_catalog::Catalog {
            from: "glue".to_string(),
            name: "glue".to_string(),
            description: None,
            metadata: HashMap::default(),
            access: spicepod::component::access::AccessMode::default(),
            include: include.iter().map(|p| (*p).to_string()).collect(),
            exclude: exclude.iter().map(|p| (*p).to_string()).collect(),
            params: None,
            dataset_params: None,
            depends_on: Vec::default(),
            metrics: None,
            acceleration: None,
        };

        table_selector(
            &CatalogBuilder::try_from(catalog)
                .expect("a catalog with valid glob patterns should build")
                .into_spec(),
        )
    }

    /// The bug this change fixes (#12634): the connector compiled `exclude` and
    /// then consulted only `include`, so a table the user asked to keep out was
    /// registered anyway. Asserted against both predicates the connector
    /// applies, over the four shapes `exclude` is written in.
    #[test]
    fn a_catalogs_exclude_reaches_the_tables_glue_registers() {
        // Excluded out of an included set.
        let s = selector(&["public.*"], &["public.audit_log"]);
        assert!(is_selected(&s, "public", "orders"));
        assert!(
            !is_selected(&s, "public", "audit_log"),
            "a table matched by `exclude` must not be registered"
        );

        // `exclude` wins over an `include` naming the same table.
        let s = selector(&["public.audit_log"], &["public.audit_log"]);
        assert!(!is_selected(&s, "public", "audit_log"));

        // `exclude` with no `include` -- the shape that reads most obviously as
        // "keep this table out", and was ignored entirely.
        let s = selector(&[], &["private.*"]);
        assert!(is_selected(&s, "public", "orders"));
        assert!(!is_selected(&s, "private", "orders"));
        assert!(!is_selected(&s, "private", "secrets"));
        // The database is still interrogated: the prune reads `include` only,
        // and proving an `exclude` covers every table a database can hold is a
        // far stronger claim than it can make.
        assert!(s.may_select_within("private"));

        // A non-matching `exclude` leaves the table present, and does not widen
        // `include` to admit the name it excludes.
        let s = selector(&["public.*"], &["private.audit_log"]);
        assert!(is_selected(&s, "public", "audit_log"));
        assert!(!is_selected(&s, "reporting", "orders"));
        assert!(s.may_select_within("public"));
        assert!(!s.may_select_within("private"));
    }

    /// Pattern shapes the database prune must not reject, each naming a
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

    /// The invariant tying this file's two predicates together: the prune
    /// decides which databases are interrogated and the per-table filter decides
    /// which tables survive. A table `is_selected` accepts must live in a
    /// database the prune kept -- otherwise it is silently absent from the
    /// catalog, and nothing reports it.
    ///
    /// `exclude` is deliberately not a dimension here. It cannot turn
    /// `is_selected` from `false` to `true` and it is not read by the prune at
    /// all, so every exclude set is implied by the empty one -- adding them
    /// would multiply the assertion count by shapes that cannot fail.
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
        let databases = ["public", "sales", "sales_east", "salesx", "otherdb", "p"];
        let tables = ["orders", "line_item", "o"];

        for include in includes {
            let selector = selector(&[include], &[]);
            for database in databases {
                let kept = selector.may_select_within(database);
                for table in tables {
                    assert!(
                        !is_selected(&selector, database, table) || kept,
                        "include {include} selects {database}.{table} \
                         but skipped database {database}"
                    );
                }
            }
        }
    }

    /// A Glue table as `GetTables` returns it: `input_format` is what the
    /// connector reads to decide whether it can read the table at all.
    fn glue_table(name: &str, input_format: Option<&str>) -> Table {
        let mut table = Table::builder().name(name);
        if let Some(input_format) = input_format {
            table = table.storage_descriptor(
                aws_sdk_glue::types::StorageDescriptor::builder()
                    .input_format(input_format)
                    .build(),
            );
        }
        table.build().expect("a Glue table with a name")
    }

    /// An Iceberg table is identified by a table parameter rather than by a
    /// storage descriptor, so it has neither of the fields the other formats use.
    fn iceberg_table(name: &str) -> Table {
        Table::builder()
            .name(name)
            .parameters("table_type", "ICEBERG")
            .build()
            .expect("a Glue table with a name")
    }

    const PARQUET: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    const TEXT: &str = "org.apache.hadoop.mapred.TextInputFormat";
    const ORC: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

    /// Regression test for #13102: `InputFormat::try_from(t).is_ok()` discarded a
    /// structured error that already named the table and the format, so a table
    /// Spice cannot read was absent from the catalog with nothing logged at all.
    /// Every rejecting shape must now be recorded by name.
    #[test]
    fn a_table_glue_cannot_read_is_recorded_rather_than_dropped_silently() {
        let mut unreadable = Vec::new();

        assert!(is_readable(
            "public",
            &glue_table("orders", Some(PARQUET)),
            &mut unreadable
        ));
        assert!(is_readable(
            "public",
            &glue_table("events", Some(TEXT)),
            &mut unreadable
        ));
        assert!(is_readable(
            "public",
            &iceberg_table("ledger"),
            &mut unreadable
        ));
        assert!(
            unreadable.is_empty(),
            "a readable table must not be reported: {unreadable:?}"
        );

        // Unsupported format, no input format, and no storage descriptor -- the
        // three ways `InputFormat::try_from` refuses a table.
        assert!(!is_readable(
            "public",
            &glue_table("archive", Some(ORC)),
            &mut unreadable
        ));
        assert!(!is_readable(
            "public",
            &glue_table("legacy", None),
            &mut unreadable
        ));

        assert_eq!(
            unreadable,
            vec!["archive".to_string(), "legacy".to_string()],
            "every table the connector cannot read must be named"
        );
    }

    /// The summary is what an operator sees by default, so it has to say how many
    /// tables it did not name rather than truncating the list silently.
    #[test]
    fn the_unreadable_summary_names_a_sample_and_counts_the_rest() {
        assert!(
            unreadable_tables_summary("public", &[]).is_none(),
            "a database Spice can read entirely must log nothing"
        );

        let one = unreadable_tables_summary("public", &["archive".to_string()])
            .expect("one unreadable table must be reported");
        assert!(
            one.contains("Skipping 1 table(s) in Glue database 'public'"),
            "{one}"
        );
        assert!(one.contains("archive"), "{one}");
        assert!(!one.contains("more"), "nothing was elided: {one}");

        let many: Vec<String> = (0..8).map(|i| format!("t{i}")).collect();
        let many = unreadable_tables_summary("warehouse", &many)
            .expect("eight unreadable tables must be reported");
        assert!(many.contains("Skipping 8 table(s)"), "{many}");
        assert!(many.contains("t0, t1, t2, t3, t4"), "{many}");
        assert!(
            many.contains("and 3 more"),
            "the summary must count the names it left out: {many}"
        );
        assert!(!many.contains("t5"), "the sample must stop at five: {many}");
    }
}
