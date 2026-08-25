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
use crate::dataconnector::glue::{GlueDataConnector, InputFormat, SUPPORTED_INPUT_FORMATS};
use crate::dataconnector::parameters::RuntimeConnectorContext;
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
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// How long the standing "cannot read these tables" warning is suppressed for
/// after it is emitted. Sized well above the 60s catalog refresh so a condition
/// that persists is reported periodically rather than every cycle.
const UNREADABLE_WARNING_INTERVAL: Duration = Duration::from_mins(30);

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
    catalog_name: String,
    catalog_id: Option<String>,
    databases: RwLock<HashMap<DatabaseName, Arc<dyn SchemaProvider>>>,
    unreadable_warnings: UnreadableWarnings,
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
            catalog_name: catalog.name.clone(),
            catalog_id: catalog.catalog_id.clone(),
            parameters,
            unreadable_warnings: UnreadableWarnings::default(),
        })
    }

    /// Builds the schema provider for one database, and reports the summary of
    /// the tables it holds that Spice cannot read. The summary is returned
    /// rather than logged here so that `refresh` can decide it against the rest
    /// of the catalog, once the whole snapshot is known to have succeeded.
    async fn create_schema_provider(
        &self,
        database: String,
    ) -> Result<(Arc<dyn SchemaProvider>, Option<UnreadableSummary>)> {
        let mut tables_builder = self.client.get_tables().database_name(&database);

        if let Some(catalog_id) = &self.catalog_id {
            tables_builder = tables_builder.catalog_id(catalog_id);
        }

        let mut paginator = tables_builder.into_paginator().send();

        let mut tables = HashMap::new();
        let mut unreadable = UnreadableTables::default();

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

                let connector = GlueDataConnector::new(
                    parameters,
                    Some(Arc::clone(&self.app)),
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
                let context = RuntimeConnectorContext::for_dataset(&dataset);
                let table_provider = connector
                    .read_provider(&context, &dataset)
                    .await
                    .boxed()
                    .context(CreatingDatasetSnafu {
                        dataset: table.name().to_string(),
                    })?;
                tables.insert(table.name, table_provider);
            }
        }

        let summary = unreadable.summary(&self.catalog_name, &database);

        let tables = RwLock::new(tables);
        let schema_provider = GlueSchemaProvider { tables };

        Ok((Arc::new(schema_provider), summary))
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
        let mut unreadable = HashMap::new();

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

                let (schema_provider, summary) =
                    self.create_schema_provider(db.name().to_string()).await?;

                if let Some(summary) = summary {
                    unreadable.insert(db.name.clone(), summary);
                }
                databases.insert(db.name, schema_provider);
            }
        }
        {
            let mut dbs = match self.databases.write() {
                Ok(dbs) => dbs,
                Err(poisoned) => poisoned.into_inner(),
            };

            *dbs = databases;
        }

        // Only now that the whole snapshot succeeded is `unreadable` the
        // complete picture, so this is where the warning state may move.
        for summary in self.unreadable_warnings.reconcile(&unreadable) {
            tracing::warn!("{summary}");
        }

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

/// Whether Spice can read `table`'s storage format, recording it in `unreadable`
/// when it cannot.
///
/// [`InputFormat::try_from`] names the table, and for an unsupported format the
/// format too, in a structured error. The reason goes to `debug!` per table,
/// matching [`is_selected`] one line up; [`UnreadableTables::summary`] is what an
/// operator sees by default.
fn is_readable(database: &str, table: &Table, unreadable: &mut UnreadableTables) -> bool {
    match InputFormat::try_from(table) {
        Ok(_) => true,
        Err(err) => {
            // Both the table name and the error's text carry Glue-controlled
            // strings, so escape them: an embedded newline would split one log
            // record into two.
            tracing::debug!(
                database,
                table = %table.name().escape_debug(),
                "Skipping Glue table Spice cannot read: {}",
                err.to_string().escape_debug()
            );
            unreadable.record(table.name());
            false
        }
    }
}

/// When each database's standing "cannot read these tables" warning was last
/// emitted, and what it said.
///
/// Keyed on the database rather than on the message, so the map holds at most
/// one entry per database the catalog listed on its last successful refresh —
/// every one of which is already resident in [`GlueCatalogProvider::databases`].
/// Keying on the message instead (as a [`util::tracers::SpacedTracer`] does)
/// would retain one entry for every distinct unreadable-table set the process
/// ever saw, which grows without bound in a catalog whose tables churn. Storing
/// the set's fingerprint alongside the instant is what keeps a *changed* set
/// reporting immediately rather than waiting out the interval.
#[derive(Default)]
struct UnreadableWarnings {
    last: Mutex<HashMap<DatabaseName, (u64, Instant)>>,
}

impl UnreadableWarnings {
    /// Moves the warning state to `summaries` — one refresh's complete set of
    /// per-database unreadable-table summaries — and returns the summaries that
    /// are due to be logged.
    fn reconcile(&self, summaries: &HashMap<DatabaseName, UnreadableSummary>) -> Vec<String> {
        self.reconcile_at(summaries, Instant::now())
    }

    /// [`Self::reconcile`] with the clock supplied, so the interval is testable
    /// without sleeping through it.
    ///
    /// Call this only for a refresh that completed: a refresh that gave up
    /// part-way has no complete `summaries`, and applying a partial one would
    /// both discard the state of databases it never reached and leave entries
    /// for databases the catalog does not end up holding.
    fn reconcile_at(
        &self,
        summaries: &HashMap<DatabaseName, UnreadableSummary>,
        now: Instant,
    ) -> Vec<String> {
        let mut last = match self.last.lock() {
            Ok(last) => last,
            Err(poisoned) => poisoned.into_inner(),
        };

        // A database this refresh did not report a summary for either has no
        // unreadable table any more or is no longer in the catalog. Keeping its
        // entry would grow the map as databases come and go, and would suppress
        // the warning if the very same tables became unreadable again inside the
        // interval — the recurrence is news, however recently it was last said.
        last.retain(|database, _| summaries.contains_key(database));

        let mut due = Vec::new();
        for (database, summary) in summaries {
            match last.get_mut(database) {
                Some((last_fingerprint, last_logged))
                    if *last_fingerprint == summary.fingerprint
                        && now.duration_since(*last_logged) < UNREADABLE_WARNING_INTERVAL => {}
                Some(entry) => {
                    *entry = (summary.fingerprint, now);
                    due.push(summary.message.clone());
                }
                None => {
                    last.insert(database.clone(), (summary.fingerprint, now));
                    due.push(summary.message.clone());
                }
            }
        }
        due
    }

    /// How many databases this holds state for. The bound the message-keyed
    /// alternative did not have.
    #[cfg(test)]
    fn tracked_databases(&self) -> usize {
        match self.last.lock() {
            Ok(last) => last.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }
}

/// The tables in one Glue database that Spice cannot read, accumulated as the
/// listing is filtered.
///
/// Kept private to this connector because the reason a table is withheld is
/// connector-specific — `postgres_accelerated`'s `AccelerationSummary` is the
/// sibling doing the same job for its own reasons. Worth extracting into shared
/// catalog infrastructure if a third appears.
#[derive(Default)]
struct UnreadableTables {
    total: usize,
    sample: Vec<String>,
    /// Identifies the *set*, which neither `total` nor `sample` does: the summary
    /// spells out only [`Self::SAMPLE`] names and a count, so two different sets
    /// of the same size that share their first sampled names render the same
    /// line. Accumulated per name rather than stored per name, so a database of
    /// ten thousand unreadable tables costs the same eight bytes as one.
    fingerprint: u64,
}

/// One database's unreadable-table warning: the line an operator sees, and the
/// fingerprint of the complete set that produced it.
///
/// The two travel together because [`UnreadableWarnings`] must decide "is this
/// the same set as last time?" while logging the line — and the line alone
/// cannot answer that, which is the whole reason `fingerprint` exists.
struct UnreadableSummary {
    fingerprint: u64,
    message: String,
}

impl UnreadableTables {
    /// How many table names the summary spells out before falling back to a count.
    const SAMPLE: usize = 5;

    /// Counts `table`, keeping its name only while the sample has room: a
    /// database that is entirely ORC would otherwise retain every name to print
    /// five of them.
    ///
    /// Names come from Glue, so they are escaped — a name holding a newline would
    /// otherwise split the summary across log lines. They are quoted as well as
    /// escaped in [`Self::summary`], so that a name containing the delimiter
    /// cannot read as two names or as the elision that follows them.
    fn record(&mut self, table: &str) {
        self.total += 1;

        // Order-independent, because Glue's listing order is not the operator's
        // concern: the same tables listed differently is not news, and treating
        // it as news would re-log a standing condition on an arbitrary refresh.
        // `wrapping_add` rather than XOR so that a name appearing twice cannot
        // cancel itself back out of the fingerprint.
        let mut hasher = DefaultHasher::new();
        table.hash(&mut hasher);
        self.fingerprint = self.fingerprint.wrapping_add(hasher.finish());

        if self.sample.len() < Self::SAMPLE {
            self.sample.push(table.escape_debug().to_string());
        }
    }

    /// The one line an operator sees for the tables this database holds that
    /// Spice cannot read, or `None` when it can read all of them.
    ///
    /// One line per database rather than one per table, so a database of ORC
    /// tables does not bury everything else in the log. `RefreshableCatalogProvider::refresh`
    /// rebuilds every schema provider on a 60s cycle, so this line is spaced by
    /// [`UnreadableWarnings`] rather than emitted on each one: a standing condition
    /// re-reported every minute for the life of the process is noise, while a set
    /// that *changed* — or that went away and came back — is reported at once.
    fn summary(&self, catalog: &str, database: &str) -> Option<UnreadableSummary> {
        if self.total == 0 {
            return None;
        }

        let total = self.total;
        let named = self
            .sample
            .iter()
            .map(|table| format!("'{table}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let elided = match total - self.sample.len() {
            0 => String::new(),
            n => format!(", and {n} more"),
        };

        // Escaped for the same reason the table names are, and it has to be done
        // here rather than left to the caller: these two sit on the same log line
        // as the names, so a newline in either splits the one WARN an operator
        // gets into two records, the second of which reads as its own event.
        let catalog = catalog.escape_debug();
        let database = database.escape_debug();

        let message = format!(
            "Catalog '{catalog}': skipping {total} Glue table(s) in database '{database}' that Spice cannot read: {named}{elided}. \
            They are not registered, so queries against them will not resolve. \
            Spice reads Glue tables stored as {SUPPORTED_INPUT_FORMATS}; run with debug logging for the reason each table was skipped, \
            or name them in the catalog's `exclude` patterns to suppress this warning. \
            For help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue"
        );

        Some(UnreadableSummary {
            fingerprint: self.fingerprint,
            message,
        })
    }
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

    /// A Glue table as `GetTables` returns it, carrying a storage descriptor
    /// whose `input_format` is what the connector reads to decide whether it can
    /// read the table at all. `None` builds the descriptor without one.
    fn glue_table(name: &str, input_format: Option<&str>) -> Table {
        let mut descriptor = aws_sdk_glue::types::StorageDescriptor::builder();
        if let Some(input_format) = input_format {
            descriptor = descriptor.input_format(input_format);
        }
        Table::builder()
            .name(name)
            .storage_descriptor(descriptor.build())
            .build()
            .expect("a Glue table with a name")
    }

    /// A table with no storage descriptor at all — the third way
    /// `InputFormat::try_from` refuses one, and a different error from a
    /// descriptor that merely lacks an `input_format`.
    fn table_without_storage_descriptor(name: &str) -> Table {
        Table::builder()
            .name(name)
            .build()
            .expect("a Glue table with a name")
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

    /// The summary `database`'s unreadable `tables` produce, built through the
    /// production [`UnreadableTables`] so the fingerprint under test is the one
    /// the connector computes rather than one the test invented.
    fn summary_of(database: &str, tables: &[&str]) -> UnreadableSummary {
        let mut unreadable = UnreadableTables::default();
        for table in tables {
            unreadable.record(table);
        }
        unreadable
            .summary("glue", database)
            .expect("a non-empty unreadable set has a summary")
    }

    /// The line `database`'s unreadable `tables` log, so a test can name what it
    /// expects to come due without re-spelling the wording. These tests are
    /// about *which* refreshes report, not about how the line reads — that is
    /// asserted by `a_table_glue_cannot_read_is_recorded_rather_than_dropped_silently`.
    fn message_of(database: &str, tables: &[&str]) -> String {
        summary_of(database, tables).message
    }

    /// One refresh's per-database summaries, for [`UnreadableWarnings`].
    fn refresh_of(summaries: &[(&str, &[&str])]) -> HashMap<DatabaseName, UnreadableSummary> {
        summaries
            .iter()
            .map(|(database, tables)| ((*database).to_string(), summary_of(database, tables)))
            .collect()
    }

    /// Sorted, because the summaries come back in the iteration order of a
    /// [`HashMap`].
    fn reconcile(
        warnings: &UnreadableWarnings,
        summaries: &[(&str, &[&str])],
        now: Instant,
    ) -> Vec<String> {
        let mut due = warnings.reconcile_at(&refresh_of(summaries), now);
        due.sort();
        due
    }

    /// Three unreadable tables in `sales`, and the sets the other tests move to.
    const SALES3: &[&str] = &["a", "b", "c"];
    const SALES4: &[&str] = &["a", "b", "c", "d"];
    const SALES5: &[&str] = &["a", "b", "c", "d", "e"];
    const SALES6: &[&str] = &["a", "b", "c", "d", "e", "f"];
    const ORDERS3: &[&str] = &["p", "q", "r"];

    /// A changed unreadable set must be reported at once, an unchanged one must
    /// wait out the interval, and neither may cost a map entry — the state is
    /// keyed on the database, so a database that churns through many distinct
    /// summaries holds exactly one.
    #[test]
    fn a_changed_unreadable_set_reports_at_once_without_growing_the_state() {
        let warnings = UnreadableWarnings::default();
        let start = Instant::now();

        assert_eq!(
            reconcile(&warnings, &[("sales", SALES3)], start),
            [message_of("sales", SALES3)],
            "the first summary for a database must be reported"
        );
        assert!(
            reconcile(
                &warnings,
                &[("sales", SALES3)],
                start + Duration::from_mins(1)
            )
            .is_empty(),
            "the same set inside the interval must stay suppressed"
        );

        // A different set is news, whatever the interval says.
        for (elapsed, tables) in [(61, SALES4), (62, SALES5), (63, SALES6)] {
            assert_eq!(
                reconcile(
                    &warnings,
                    &[("sales", tables)],
                    start + Duration::from_secs(elapsed)
                ),
                [message_of("sales", tables)],
                "a changed set must be reported immediately: {tables:?}"
            );
        }
        assert_eq!(
            warnings.tracked_databases(),
            1,
            "four distinct sets for one database must cost one entry, not four"
        );

        assert_eq!(
            reconcile(
                &warnings,
                &[("sales", SALES6), ("orders", ORDERS3)],
                start + Duration::from_secs(64)
            ),
            [message_of("orders", ORDERS3)],
            "a summary another database has never reported must be reported, and \
             `sales` is unchanged inside its interval"
        );
        assert_eq!(
            warnings.tracked_databases(),
            2,
            "state is per database, so a second database adds exactly one entry"
        );

        // A database the catalog no longer lists must not keep its entry.
        assert!(
            reconcile(
                &warnings,
                &[("sales", SALES6)],
                start + Duration::from_secs(65)
            )
            .is_empty(),
            "dropping `orders` must not disturb what `sales` reports"
        );
        assert_eq!(
            warnings.tracked_databases(),
            1,
            "a database dropped from the catalog must not keep its warning state"
        );
        assert_eq!(
            reconcile(
                &warnings,
                &[("sales", SALES6), ("orders", ORDERS3)],
                start + Duration::from_secs(66)
            ),
            [message_of("orders", ORDERS3)],
            "a database re-listed after being dropped starts over, so it reports again"
        );
    }

    /// A database whose tables all become readable again clears its state, so a
    /// recurrence of the very same set is reported rather than suppressed as a
    /// repeat. The recurrence is news: the tables went away and came back.
    #[test]
    fn an_unreadable_set_that_recovers_and_recurs_is_reported_again() {
        let warnings = UnreadableWarnings::default();
        let start = Instant::now();

        assert_eq!(
            reconcile(&warnings, &[("sales", SALES3)], start),
            [message_of("sales", SALES3)]
        );

        // Every table readable again: no summary for `sales` at all.
        assert!(reconcile(&warnings, &[], start + Duration::from_mins(1)).is_empty());
        assert_eq!(
            warnings.tracked_databases(),
            0,
            "a database with nothing unreadable must not keep its warning state"
        );

        assert_eq!(
            reconcile(
                &warnings,
                &[("sales", SALES3)],
                start + Duration::from_mins(2)
            ),
            [message_of("sales", SALES3)],
            "the same set becoming unreadable again is a new event, not a repeat of the old one"
        );
    }

    /// The summary spells out only [`UnreadableTables::SAMPLE`] names and a
    /// count, so two different sets of the same size that agree on their first
    /// sampled names render an identical line. Deciding "same as last time?" on
    /// that line would withhold a genuinely changed set for the whole interval,
    /// leaving the operator acting on a list of tables that is no longer the one
    /// Spice cannot read.
    #[test]
    fn a_changed_set_that_renders_the_same_line_is_still_reported() {
        let before: &[&str] = &["a", "b", "c", "d", "e", "f"];
        let after: &[&str] = &["a", "b", "c", "d", "e", "g"];

        assert_eq!(
            message_of("sales", before),
            message_of("sales", after),
            "these two sets must render the same line, or this test proves nothing about \
             telling them apart"
        );

        let warnings = UnreadableWarnings::default();
        let start = Instant::now();

        assert_eq!(
            reconcile(&warnings, &[("sales", before)], start),
            [message_of("sales", before)]
        );
        assert_eq!(
            reconcile(
                &warnings,
                &[("sales", after)],
                start + Duration::from_mins(1)
            ),
            [message_of("sales", after)],
            "a set that changed beyond the sampled names must be reported, not suppressed \
             as a repeat of the line it happens to share"
        );
    }

    /// The mirror of the case above: Glue returns a database's tables in its own
    /// order, and the same tables listed differently are not news. Reporting
    /// them would re-log a standing condition on whichever refresh the order
    /// happened to change, which is the noise the interval exists to prevent.
    #[test]
    fn the_same_set_listed_in_a_different_order_is_not_reported() {
        let listed: &[&str] = &["a", "b", "c", "d", "e", "f"];
        let relisted: &[&str] = &["f", "e", "d", "c", "b", "a"];

        let warnings = UnreadableWarnings::default();
        let start = Instant::now();

        assert_eq!(
            reconcile(&warnings, &[("sales", listed)], start),
            [message_of("sales", listed)]
        );
        assert!(
            reconcile(
                &warnings,
                &[("sales", relisted)],
                start + Duration::from_mins(1)
            )
            .is_empty(),
            "the same tables in a different listing order are the same set, so the standing \
             warning must stay suppressed"
        );
    }

    /// The interval is what stops a standing condition being re-reported every
    /// 60s refresh, so it has to actually expire.
    #[test]
    fn an_unchanged_unreadable_set_reports_again_once_the_interval_expires() {
        let warnings = UnreadableWarnings::default();
        let start = Instant::now();

        assert_eq!(
            reconcile(&warnings, &[("sales", SALES3)], start),
            [message_of("sales", SALES3)]
        );
        assert!(
            reconcile(
                &warnings,
                &[("sales", SALES3)],
                start
                    + UNREADABLE_WARNING_INTERVAL
                        .checked_sub(Duration::from_secs(1))
                        .expect("the warning interval is longer than one second")
            )
            .is_empty(),
            "one second short of the interval must stay suppressed"
        );
        assert_eq!(
            reconcile(
                &warnings,
                &[("sales", SALES3)],
                start + UNREADABLE_WARNING_INTERVAL
            ),
            [message_of("sales", SALES3)],
            "the interval must expire, or a standing condition is reported once and never again"
        );
    }

    /// Regression test for #13102: every shape `InputFormat::try_from` refuses
    /// must be recorded by name, so a table Spice cannot read is never absent
    /// from the catalog with nothing logged about it.
    #[test]
    fn a_table_glue_cannot_read_is_recorded_rather_than_dropped_silently() {
        let mut unreadable = UnreadableTables::default();

        for readable in [
            glue_table("orders", Some(PARQUET)),
            glue_table("events", Some(TEXT)),
            iceberg_table("ledger"),
        ] {
            assert!(is_readable("public", &readable, &mut unreadable));
        }
        assert_eq!(
            unreadable.total, 0,
            "a readable table must not be reported: {:?}",
            unreadable.sample
        );

        // The three ways `InputFormat::try_from` refuses a table: an unsupported
        // format, a storage descriptor carrying no format, and no descriptor.
        for refused in [
            glue_table("archive", Some(ORC)),
            glue_table("headerless", None),
            table_without_storage_descriptor("legacy"),
        ] {
            assert!(!is_readable("public", &refused, &mut unreadable));
        }

        assert_eq!(unreadable.total, 3);
        assert_eq!(
            unreadable.sample,
            vec![
                "archive".to_string(),
                "headerless".to_string(),
                "legacy".to_string()
            ],
            "every table the connector cannot read must be named"
        );
    }

    /// The summary is what an operator sees by default, so it has to say how many
    /// tables it did not name rather than truncating the list silently.
    #[test]
    fn the_unreadable_summary_names_a_sample_and_counts_the_rest() {
        assert!(
            UnreadableTables::default()
                .summary("glue", "public")
                .is_none(),
            "a database Spice can read entirely must log nothing"
        );

        let mut one = UnreadableTables::default();
        one.record("archive");
        let one = one
            .summary("glue", "public")
            .expect("one unreadable table must be reported")
            .message;
        assert!(
            one.contains("Catalog 'glue': skipping 1 Glue table(s) in database 'public'"),
            "{one}"
        );
        assert!(one.contains("'archive'"), "{one}");
        assert!(one.contains(SUPPORTED_INPUT_FORMATS), "{one}");
        assert!(
            one.contains("queries against them will not resolve"),
            "this is the only line an operator gets for a table that silently is not there, \
             so it has to say what they will observe: {one}"
        );
        assert!(
            one.contains("https://docs.spiceai.org/components/catalogs/glue"),
            "the message must carry the docs link that makes it actionable: {one}"
        );
        assert!(!one.contains("more"), "nothing was elided: {one}");

        let mut many = UnreadableTables::default();
        for i in 0..8 {
            many.record(&format!("t{i}"));
        }
        assert_eq!(
            many.sample.len(),
            UnreadableTables::SAMPLE,
            "the accumulator must stop retaining names past the sample"
        );
        let many = many
            .summary("glue", "warehouse")
            .expect("eight unreadable tables must be reported")
            .message;
        assert!(many.contains("skipping 8 Glue table(s)"), "{many}");
        assert!(many.contains("'t0', 't1', 't2', 't3', 't4'"), "{many}");
        assert!(
            many.contains("and 3 more"),
            "the summary must count the names it left out: {many}"
        );
        assert!(
            !many.contains("'t5'"),
            "the sample must stop at five: {many}"
        );
    }

    /// Table names come from Glue, and a log line has to stay one line.
    ///
    /// Every identifier the line interpolates is checked, not only the table
    /// names: the catalog name comes from the user's spicepod and the database
    /// name from AWS, and all three land on the same line, so a newline in any
    /// one of them splits the single WARN an operator gets for a missing table
    /// into two records — the second of which reads as an event of its own.
    #[test]
    fn no_identifier_can_break_the_summary_across_lines() {
        let forged = "orders\nWARN forged log line";
        for (catalog, database, table) in [
            ("glue", "public", forged),
            ("glue", forged, "orders"),
            (forged, "public", "orders"),
        ] {
            let mut unreadable = UnreadableTables::default();
            unreadable.record(table);

            let summary = unreadable
                .summary(catalog, database)
                .expect("one unreadable table must be reported")
                .message;

            assert!(
                !summary.contains('\n'),
                "an embedded newline must not reach the log: {summary}"
            );
            assert!(
                summary.contains("\\nWARN"),
                "the newline must be shown escaped rather than dropped: {summary}"
            );
        }
    }

    /// A table name may contain the delimiter that separates the sampled names,
    /// or the wording of the elision that follows them. Quoting each name is
    /// what stops one table reading as two, or as a count of tables the summary
    /// never left out — escaping alone does not, since neither a comma nor a
    /// word is a control character.
    #[test]
    fn a_table_name_cannot_impersonate_the_list_or_the_elision() {
        let mut unreadable = UnreadableTables::default();
        unreadable.record("orders, and 7 more");
        unreadable.record("events");

        let summary = unreadable
            .summary("glue", "public")
            .expect("two unreadable tables must be reported")
            .message;

        assert!(
            summary.contains("'orders, and 7 more', 'events'"),
            "each name must be quoted as one identifier: {summary}"
        );
        assert!(
            summary.contains("skipping 2 Glue table(s)"),
            "the count is what says how many there are, and it must not be \
             contradicted by a name that reads like an elision: {summary}"
        );
    }
}
