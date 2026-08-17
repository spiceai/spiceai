/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `PostgreSQL` catalog provider implementation.
//!
//! Discovers schemas and tables in a `PostgreSQL` database using
//! `information_schema` queries and provides them as `DataFusion` catalog/schema providers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::catalog_filter::TableSelector;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::utils::quote_identifier;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use snafu::prelude::*;

use crate::{
    DESCRIPTION_METADATA_KEY, FOREIGN_KEYS_METADATA_KEY, FieldMetadata, Read,
    RefreshableCatalogProvider, SOURCE_TYPE_METADATA_KEY, metadata_enriched_table_provider,
};

/// Every variant is worded to read as the `Cause:` clause of the message that
/// reports it. Each is raised inside a schema or table refresh whose own message
/// states the problem -- naming the catalog, the schema and the step that failed
/// -- and the impact on what the user can query, so a variant that named any of
/// those would say it twice. What they owe the user is the specific failure and
/// its fix.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to PostgreSQL: {source}. Check the `pg_host`, `pg_port`, `pg_user`, `pg_pass` and `pg_sslmode` parameters, and that the database is reachable from Spice. Docs: {POSTGRES_CONNECTOR_DOCS}"
    ))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // Reports Spice's own discovery queries against `information_schema`, never
    // anything the user wrote, so the fix is a grant -- not "check your SQL". The
    // step being performed is named by the message reporting this, and the
    // relation the server objected to by `source`; the query text itself is
    // debug detail and stays out.
    #[snafu(display(
        "A PostgreSQL query failed: {source}. Check that the connected role can read `information_schema` and `pg_catalog`. Docs: {POSTGRES_CONNECTOR_DOCS}"
    ))]
    QueryFailed { source: tokio_postgres::Error },

    /// The bulk column-type lookup failing is reported by the fallback warning,
    /// which names the catalog, the schema and the step, so this adds only the
    /// fix -- hence a display that opens with its own source.
    #[snafu(display(
        "{source}. Check that the connected role can read `pg_catalog`, and that every column type is supported or `unsupported_type_action` permits it. Docs: {POSTGRES_CONNECTOR_DOCS}"
    ))]
    SchemaResolutionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "An unexpected error occurred. Report this bug: https://github.com/spiceai/spiceai/issues"
    ))]
    UnexpectedConnectionType {},

    /// Listing the catalog's schemas is the one discovery step with no
    /// per-schema warning to report it -- it fails the whole refresh -- so
    /// naming the step is this variant's job. The catalog is named by the error
    /// that wraps this one.
    #[snafu(display("Failed to list the catalog's schemas. Cause: {source}"))]
    SchemaListingFailed {
        source: connector_postgres_common::Error,
    },

    /// Wraps errors from the shared `connector-postgres-common` queries
    /// (`list_tables`, re-exported below) so this crate's own callers can still
    /// propagate them with `?`.
    #[snafu(display("{source}"), context(false))]
    Common {
        source: connector_postgres_common::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Linked by the messages about include/exclude patterns and catalog
/// registration, which have no data-connector equivalent.
const POSTGRES_CATALOG_DOCS: &str = "https://spiceai.org/docs/components/catalogs/postgres";

/// Linked by everything a dataset using the `PostgreSQL` data connector would hit
/// identically -- connection parameters, role grants, `unsupported_type_action`.
/// Those are documented with the connector, so the connector's page is the one
/// that answers them for a catalog user too.
const POSTGRES_CONNECTOR_DOCS: &str =
    "https://spiceai.org/docs/components/data-connectors/postgres";

pub use connector_postgres_common::{
    ReplicaIdentityOutcome, ReplicationSlotStatus, SkipReason, ViewRelation,
    check_cdc_prerequisites, classify_replica_identity, ensure_replication_slot_capacity,
    list_schemas, list_tables, list_views, primary_key_columns, replica_identity,
    replication_slot_status, wal_sender_timeout_ms,
};

/// A single foreign key constraint discovered from `information_schema`.
#[derive(Debug, Clone, serde::Serialize)]
struct ForeignKeyConstraint {
    columns: Vec<String>,
    foreign_table: String,
    foreign_columns: Vec<String>,
}

/// FK constraints grouped by source table name within a schema.
type ForeignKeyMap = HashMap<String, Vec<ForeignKeyConstraint>>;

#[derive(Debug, Clone, Default)]
struct TableComments {
    table_comment: Option<String>,
    column_comments: HashMap<String, String>,
    column_source_types: HashMap<String, String>,
}

pub type PostgresTableMetadataRow = (Option<String>, String, Option<String>, String);

/// Comment metadata grouped by source table name within a schema.
type CommentMap = HashMap<String, TableComments>;

/// A catalog provider for `PostgreSQL` that discovers schemas and tables
/// by querying `information_schema`.
pub struct PostgresCatalogProvider {
    catalog_name: String,
    pool: Arc<PostgresConnectionPool>,
    table_creator: Arc<dyn Read>,
    schemas: RwLock<HashMap<String, Arc<PostgresSchemaProvider>>>,
    selector: TableSelector,
    /// Tables registered by the previous refresh, or `None` before the first
    /// one completes. Only [`empty_catalog_warning`] reads it: this provider is
    /// polled every refresh interval, so an empty catalog is reported when it
    /// *becomes* empty rather than once a minute for the life of the process.
    last_table_count: RwLock<Option<usize>>,
}

impl std::fmt::Debug for PostgresCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl PostgresCatalogProvider {
    #[must_use]
    pub fn new(
        catalog_name: String,
        pool: Arc<PostgresConnectionPool>,
        table_creator: Arc<dyn Read>,
        selector: TableSelector,
    ) -> Self {
        Self {
            catalog_name,
            pool,
            table_creator,
            schemas: RwLock::new(HashMap::new()),
            selector,
            last_table_count: RwLock::new(None),
        }
    }

    async fn refresh_schemas(&self) -> Result<()> {
        let schema_names = self.list_schemas().await?;

        let mut schemas = HashMap::new();
        for schema_name in &schema_names {
            // A schema no `include` pattern can reach cannot contribute a table,
            // so skip its `list_foreign_keys` + `list_comments` + `list_tables`
            // round trips. It is still registered, empty, because that is exactly
            // what interrogating it would have produced -- pruning changes the
            // queries issued, never the catalog's namespace.
            if !self.selector.may_select_within(schema_name) {
                tracing::debug!(
                    "Schema '{schema_name}' of PostgreSQL catalog '{}' cannot match any include pattern; skipping its table discovery",
                    self.catalog_name
                );
                schemas.insert(
                    schema_name.clone(),
                    Arc::new(PostgresSchemaProvider::new(
                        self.catalog_name.clone(),
                        Arc::clone(&self.pool),
                        schema_name.clone(),
                        Arc::clone(&self.table_creator),
                        self.selector.clone(),
                    )),
                );
                continue;
            }

            let foreign_keys = match self.list_foreign_keys(schema_name).await {
                Ok(fks) => fks,
                Err(e) => {
                    tracing::warn!(
                        "{}",
                        schema_metadata_warning(
                            &self.catalog_name,
                            schema_name,
                            SchemaMetadata::ForeignKeys,
                            &e.to_string(),
                        )
                    );
                    HashMap::new()
                }
            };
            let comments = match self.list_comments(schema_name).await {
                Ok(comments) => comments,
                Err(e) => {
                    tracing::warn!(
                        "{}",
                        schema_metadata_warning(
                            &self.catalog_name,
                            schema_name,
                            SchemaMetadata::Descriptions,
                            &e.to_string(),
                        )
                    );
                    HashMap::new()
                }
            };

            let schema_provider = PostgresSchemaProvider::new(
                self.catalog_name.clone(),
                Arc::clone(&self.pool),
                schema_name.clone(),
                Arc::clone(&self.table_creator),
                self.selector.clone(),
            );
            // A single schema's table discovery failing (e.g. a transient
            // connection reset or lock timeout) must not abort the whole catalog
            // load; skip just this schema and keep the others (#11724).
            //
            // This provider is polled repeatedly by `RefreshingCatalogProvider`,
            // not just at initial load, so a transient failure on a schema that
            // previously refreshed fine must not remove it from the catalog
            // either. Look up (not clone) the last-known-good entry for just this
            // one schema — only reading `self.schemas` on the failure path avoids
            // cloning the whole map on every refresh cycle when nothing fails.
            let refresh_result = schema_provider
                .refresh_tables(&foreign_keys, &comments)
                .await;
            let previous = if refresh_result.is_ok() {
                None
            } else {
                let guard = match self.schemas.read() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.get(schema_name).cloned()
            };

            let outcome = schema_refresh_outcome(refresh_result.is_ok(), previous.is_some());
            if let Err(e) = &refresh_result
                && let Some(warning) = schema_discovery_warning(
                    &self.catalog_name,
                    schema_name,
                    outcome,
                    &e.to_string(),
                )
            {
                tracing::warn!("{warning}");
            }

            match outcome {
                SchemaRefreshOutcome::InsertNew => {
                    schemas.insert(schema_name.clone(), Arc::new(schema_provider));
                }
                SchemaRefreshOutcome::KeepPrevious => {
                    // Only the (Err, Some) branch reaches here, so both are present.
                    if let Some(previous) = previous {
                        schemas.insert(schema_name.clone(), previous);
                    }
                }
                SchemaRefreshOutcome::Skip => {}
            }
        }

        let table_count: usize = schemas.values().map(|schema| schema.table_count()).sum();
        let selected_count: usize = schemas.values().map(|schema| schema.selected_count()).sum();
        let schemas_registered = schemas.len();

        {
            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = schemas;
        }

        let previous_count = {
            let mut guard = match self.last_table_count.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            guard.replace(table_count)
        };

        if let Some(warning) = empty_catalog_warning(
            &self.catalog_name,
            previous_count,
            table_count,
            selected_count,
            schemas_registered,
            &self.selector,
        ) {
            tracing::warn!("{warning}");
        }

        Ok(())
    }

    /// Query all foreign key constraints for tables in the given schema.
    ///
    /// Returns a map from source table name to its FK constraints.
    async fn list_foreign_keys(&self, schema_name: &str) -> Result<ForeignKeyMap> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        // Use referential_constraints to link FK -> referenced PK/unique constraint,
        // then join key_column_usage on both sides matched by position_in_unique_constraint.
        // This correctly handles composite FKs where column order differs from the referenced
        // unique constraint's column order.
        let rows = conn
            .conn
            .query(
                "SELECT \
                     kcu1.table_name, \
                     kcu1.column_name, \
                     kcu2.table_schema AS referenced_schema, \
                     kcu2.table_name AS referenced_table, \
                     kcu2.column_name AS referenced_column, \
                     rc.constraint_name \
                 FROM information_schema.referential_constraints rc \
                 JOIN information_schema.key_column_usage kcu1 \
                     ON kcu1.constraint_name = rc.constraint_name \
                     AND kcu1.constraint_schema = rc.constraint_schema \
                 JOIN information_schema.key_column_usage kcu2 \
                     ON kcu2.constraint_name = rc.unique_constraint_name \
                     AND kcu2.constraint_schema = rc.unique_constraint_schema \
                     AND kcu2.ordinal_position = kcu1.position_in_unique_constraint \
                 WHERE rc.constraint_schema = $1 \
                 ORDER BY kcu1.table_name, rc.constraint_name, kcu1.ordinal_position",
                &[&schema_name],
            )
            .await
            .context(QueryFailedSnafu)?;

        // Group rows by (table_name, constraint_name) to build composite FK constraints.
        let mut constraints_by_table: HashMap<String, HashMap<String, ForeignKeyConstraint>> =
            HashMap::new();

        for row in &rows {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let referenced_schema: String = row.get(2);
            let referenced_table: String = row.get(3);
            let referenced_column: String = row.get(4);
            let constraint_name: String = row.get(5);

            let table_constraints = constraints_by_table.entry(table_name).or_default();

            let foreign_table =
                foreign_key_target(&self.catalog_name, &referenced_schema, &referenced_table);
            let fk =
                table_constraints
                    .entry(constraint_name)
                    .or_insert_with(|| ForeignKeyConstraint {
                        columns: Vec::new(),
                        foreign_table,
                        foreign_columns: Vec::new(),
                    });

            fk.columns.push(column_name);
            fk.foreign_columns.push(referenced_column);
        }

        // Flatten: HashMap<table, HashMap<constraint, FK>> -> HashMap<table, Vec<FK>>
        let fk_map = constraints_by_table
            .into_iter()
            .map(|(table, constraints)| (table, constraints.into_values().collect()))
            .collect();

        Ok(fk_map)
    }

    /// Query table and column comments for tables in the given schema.
    async fn list_comments(&self, schema_name: &str) -> Result<CommentMap> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        let rows = conn
            .conn
            .query(
                "SELECT \
                     c.relname AS table_name, \
                     obj_description(c.oid, 'pg_class') AS table_comment, \
                     a.attname AS column_name, \
                     col_description(c.oid, a.attnum) AS column_comment, \
                     format_type(a.atttypid, a.atttypmod) AS column_source_type \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 LEFT JOIN pg_catalog.pg_attribute a \
                     ON a.attrelid = c.oid \
                     AND a.attnum > 0 \
                     AND NOT a.attisdropped \
                 WHERE n.nspname = $1 \
                 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
                 ORDER BY c.relname, a.attnum",
                &[&schema_name],
            )
            .await
            .context(QueryFailedSnafu)?;

        let mut comments_by_table = HashMap::new();
        for row in &rows {
            let table_name: String = row.get(0);
            let table_comment: Option<String> = row.get(1);
            let column_name: Option<String> = row.get(2);
            let column_comment: Option<String> = row.get(3);
            let column_source_type: Option<String> = row.get(4);

            let comments: &mut TableComments = comments_by_table.entry(table_name).or_default();
            if comments.table_comment.is_none()
                && let Some(comment) = table_comment.filter(|comment| !comment.is_empty())
            {
                comments.table_comment = Some(comment);
            }
            if let Some(column_name) = column_name {
                if let Some(comment) = column_comment.filter(|comment| !comment.is_empty()) {
                    comments
                        .column_comments
                        .insert(column_name.clone(), comment);
                }
                if let Some(source_type) =
                    column_source_type.filter(|source_type| !source_type.is_empty())
                {
                    comments
                        .column_source_types
                        .insert(column_name, source_type);
                }
            }
        }

        Ok(comments_by_table)
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        list_schemas(&self.pool)
            .await
            .context(SchemaListingFailedSnafu)
    }
}

impl CatalogProvider for PostgresCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let guard = match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let guard = match self.schemas.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

#[async_trait]
impl RefreshableCatalogProvider for PostgresCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for `PostgreSQL` that discovers tables within a schema.
pub struct PostgresSchemaProvider {
    /// Named by every message this schema logs: a schema name alone does not
    /// tell an operator running several catalogs which one is degraded.
    catalog_name: String,
    pool: Arc<PostgresConnectionPool>,
    schema_name: String,
    table_creator: Arc<dyn Read>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    selector: TableSelector,
    /// Relations the last refresh selected, which is not the same as the number
    /// it registered: a selected table whose provider cannot be built is logged
    /// and skipped. Keeping both apart is what lets an empty catalog say
    /// whether the patterns matched nothing or the matches failed to load.
    selected_count: RwLock<usize>,
}

impl std::fmt::Debug for PostgresSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

impl PostgresSchemaProvider {
    #[must_use]
    pub fn new(
        catalog_name: String,
        pool: Arc<PostgresConnectionPool>,
        schema_name: String,
        table_creator: Arc<dyn Read>,
        selector: TableSelector,
    ) -> Self {
        Self {
            catalog_name,
            pool,
            schema_name,
            table_creator,
            tables: RwLock::new(HashMap::new()),
            selector,
            selected_count: RwLock::new(0),
        }
    }

    /// How many relations this schema's last refresh selected.
    fn selected_count(&self) -> usize {
        let guard = match self.selected_count.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        *guard
    }

    /// How many tables this schema registered. Counts without cloning every
    /// name, which `SchemaProvider::table_names` would.
    fn table_count(&self) -> usize {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.len()
    }

    async fn refresh_tables(
        &self,
        foreign_keys: &ForeignKeyMap,
        comments: &CommentMap,
    ) -> Result<()> {
        let table_names = self.list_tables().await?;

        let selected = select_relations(&self.selector, &self.schema_name, &table_names);
        {
            let mut guard = match self.selected_count.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = selected.len();
        }

        // Advisory: an entry lets a table skip its own schema query, and its
        // absence means that table resolves individually. A failure here is
        // therefore not fatal, and relations the catalog query cannot describe
        // are absent for the same reason.
        let schemas = match self.list_column_schemas(&selected).await {
            Ok(schemas) => schemas,
            Err(e) => {
                tracing::warn!(
                    "{}",
                    column_types_fallback_warning(
                        &self.catalog_name,
                        &self.schema_name,
                        &e.to_string(),
                    )
                );
                HashMap::new()
            }
        };

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: &self.catalog_name,
                schema: &self.schema_name,
            },
            table_names,
            &self.table_creator,
            &self.selector,
            foreign_keys,
            comments,
            &schemas,
        )
        .await;

        {
            let mut guard = match self.tables.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = tables;
        }

        Ok(())
    }

    /// Every relation's Arrow schema for this `PostgreSQL` schema, in one query.
    ///
    /// A missing entry means "resolve this one individually", never "no
    /// columns". Relations the catalog query does not describe are absent, as is
    /// every table on Redshift, where the per-table `SHOW COLUMNS` cannot be
    /// batched.
    async fn list_column_schemas(
        &self,
        relations: &[String],
    ) -> Result<HashMap<String, SchemaRef>> {
        // Taken through `DbConnectionPool::connect`, not `connect_direct`: only
        // that path applies the pool's `unsupported_type_action`. A connection
        // without it rejects any column type the mapping does not support, so a
        // schema containing one -- `jsonb`, say -- would fail this lookup even
        // under the catalog's default `string`, and every table in the namespace
        // would fall back to resolving its own schema. The result would still be
        // correct, which is what makes it worth pinning down: the optimization
        // would simply never apply, silently, in the common configuration.
        let conn = DbConnectionPool::connect(&*self.pool)
            .await
            .map_err(|source| Error::ConnectionFailed { source })?;

        let conn = conn
            .as_any()
            .downcast_ref::<PostgresConnection>()
            .context(UnexpectedConnectionTypeSnafu)?;

        conn.get_schemas_in(&self.schema_name, relations)
            .await
            .map_err(|e| Error::SchemaResolutionFailed {
                source: Box::new(e),
            })
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        // Include view-like relations (views, materialized views, foreign
        // tables) here -- the non-accelerated schema provider serves them as
        // ordinary read-only federated tables. The accelerated catalog path
        // passes `include_views: false` so only CDC-able base tables are
        // discovered. See `connector_postgres_common::list_tables`.
        Ok(list_tables(&self.pool, &self.schema_name, true).await?)
    }
}

/// The relations a schema will actually register, which are the only ones worth
/// describing.
///
/// The bulk lookup names its relations rather than taking the whole namespace,
/// so this is what bounds its cost: a schema holding thousands of tables behind
/// an `include` that selects a handful describes the handful. Narrowing here has
/// no effect on the resulting catalog -- the rejected names would be dropped
/// when the providers are built regardless -- which is exactly why it is worth
/// asserting directly.
fn select_relations(
    selector: &TableSelector,
    schema_name: &str,
    table_names: &[String],
) -> Vec<String> {
    table_names
        .iter()
        .filter(|table| selector.selects_table(schema_name, table))
        .cloned()
        .collect()
}

/// Build the fully-qualified name of a foreign-key target table
/// (`catalog.schema.table`) for the `foreign_keys` schema metadata.
///
/// Each component is quoted following `PostgreSQL` `quote_ident` semantics
/// (quoted only when required, doubling any embedded `"`) so the joined name
/// round-trips back to the exact `(catalog, schema, table)` triple via
/// `TableReference::parse_str`, even when a component legally contains a `.`
/// (a quoted identifier such as `"my.schema"`). See #11727.
fn foreign_key_target(catalog: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_identifier(catalog),
        quote_identifier(schema),
        quote_identifier(table),
    )
}

/// The warning for a refresh that registered no tables, or `None` when there is
/// nothing to report.
///
/// A catalog that selects nothing is the one outcome the federated path cannot
/// distinguish from success on its own: it loads, reports ready, and answers
/// every query with "table not found". The accelerated path fails loud here
/// (#11983); the federated path warns instead, because a federated catalog can
/// legitimately be configured before the tables it names exist -- so this must
/// say enough for a user to tell "my patterns are wrong" from "my tables are not
/// there yet".
///
/// Reported on the transition to empty rather than on every refresh: a repeated
/// warning for a steady misconfiguration is how a log stops being read.
///
/// `selected_count` is deliberately separate from `current_count`: a selected
/// table whose provider cannot be built is logged and skipped, so a catalog can
/// register nothing while the patterns matched perfectly well. Blaming the
/// patterns there would contradict the failure logged moments earlier and send
/// the user to fix configuration that is already correct.
fn empty_catalog_warning(
    catalog_name: &str,
    previous_count: Option<usize>,
    current_count: usize,
    selected_count: usize,
    schemas_registered: usize,
    selector: &TableSelector,
) -> Option<String> {
    if current_count > 0 || previous_count == Some(0) {
        return None;
    }

    let patterns = selector.describe();
    let cause = if selected_count > 0 {
        format!(
            "{selected_count} table(s) matched, but none could be registered -- the errors logged above this name the tables that failed and why"
        )
    } else if patterns.is_empty() {
        format!(
            "It selects every table it can see, so either the database has no tables in the {schemas_registered} schema(s) it discovered, or the connected role cannot see them -- check the role's SELECT and USAGE grants"
        )
    } else {
        format!(
            "None of the tables in the {schemas_registered} schema(s) it discovered matched {patterns}. Patterns match the qualified '<schema>.<table>' name, so an unqualified pattern such as 'orders' never matches -- write 'public.orders' (or 'public.*') instead"
        )
    };

    Some(format!(
        "PostgreSQL catalog '{catalog_name}' registered no tables, so queries against it will not resolve any table. {cause}. Docs: {POSTGRES_CATALOG_DOCS}"
    ))
}

/// The metadata a schema query contributes to the tables it describes, named
/// as the user meets it rather than as the query that fetches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaMetadata {
    ForeignKeys,
    Descriptions,
}

/// The one-line report for a schema whose table discovery failed.
///
/// Discovery failing is not fatal (#11724), which is exactly what makes the
/// wording load-bearing: the user's symptom is a schema that is empty or stale,
/// and this line is the only explanation they get.
///
/// Every message here states one problem and then its impact -- "failed to X, so
/// Y" -- rather than reporting the impact and the failure as if they were two
/// separate events, and ends with the cause, which carries the specific fix and
/// a documentation link where it has one.
///
/// Returns `None` for a schema that refreshed successfully, which has nothing
/// to report.
fn schema_discovery_warning(
    catalog_name: &str,
    schema_name: &str,
    outcome: SchemaRefreshOutcome,
    cause: &str,
) -> Option<String> {
    match outcome {
        SchemaRefreshOutcome::InsertNew => None,
        SchemaRefreshOutcome::KeepPrevious => Some(format!(
            "PostgreSQL catalog '{catalog_name}' failed to list the tables of schema '{schema_name}', so it is still serving the tables it discovered earlier, which may now be out of date: a table added, renamed or dropped since then is not reflected. The schema is retried on the next refresh. Cause: {cause}"
        )),
        SchemaRefreshOutcome::Skip => Some(format!(
            "PostgreSQL catalog '{catalog_name}' failed to list the tables of schema '{schema_name}', so no table in that schema is registered and queries against '{catalog_name}.{schema_name}' will not resolve. The rest of the catalog is unaffected, and the schema is retried on the next refresh. Cause: {cause}"
        )),
    }
}

/// The one-line report for a schema whose tables registered without the foreign
/// keys or descriptions a metadata query would have attached.
fn schema_metadata_warning(
    catalog_name: &str,
    schema_name: &str,
    metadata: SchemaMetadata,
    cause: &str,
) -> String {
    match metadata {
        SchemaMetadata::ForeignKeys => format!(
            "PostgreSQL catalog '{catalog_name}' failed to determine the foreign keys of schema '{schema_name}', so anything relying on them -- including SQL generated from natural language -- cannot see how its tables join. Cause: {cause}"
        ),
        SchemaMetadata::Descriptions => format!(
            "PostgreSQL catalog '{catalog_name}' failed to read the table and column descriptions of schema '{schema_name}', so the comments defined in PostgreSQL are unavailable to queries and to the tools that read them. Cause: {cause}"
        ),
    }
}

/// The one-line report for a schema whose column types could not be read in one
/// query. Nothing the user queries changes -- each table is described on its own
/// instead -- so this reports the cost, and says so rather than implying data is
/// missing.
fn column_types_fallback_warning(catalog_name: &str, schema_name: &str, cause: &str) -> String {
    format!(
        "PostgreSQL catalog '{catalog_name}' failed to describe the tables of schema '{schema_name}' in a single query, so each is described separately and this refresh is slower; the tables it registers are unaffected. Cause: {cause}"
    )
}

/// Where a table is being registered, as its messages name it. The catalog and
/// the schema travel together because no message on this path names one without
/// the other: a schema name alone does not tell an operator running several
/// catalogs which one lost a table.
#[derive(Debug, Clone, Copy)]
struct SchemaLocation<'a> {
    catalog: &'a str,
    schema: &'a str,
}

/// The one-line report for a table that was selected but could not be loaded.
/// The cause comes from the table's own loader rather than this module, so this
/// message carries the fix and the documentation link itself.
fn table_skipped_warning(location: SchemaLocation<'_>, table_name: &str, cause: &str) -> String {
    let SchemaLocation { catalog, schema } = location;
    format!(
        "PostgreSQL catalog '{catalog}' failed to load table '{schema}.{table_name}', so it is absent from the catalog and queries against '{catalog}.{schema}.{table_name}' will not resolve. It is retried on the next refresh. Cause: {cause}. Check that the connected role has SELECT on the table, and that its column types are supported or `unsupported_type_action` permits them. Docs: {POSTGRES_CONNECTOR_DOCS}"
    )
}

/// The one-line report for a table registered without its foreign keys because
/// they could not be recorded. Recording them is Spice's own work on data it
/// just read, so a failure here is a bug rather than something the user can fix.
fn table_foreign_keys_warning(
    location: SchemaLocation<'_>,
    table_name: &str,
    cause: &str,
) -> String {
    let SchemaLocation { catalog, schema } = location;
    format!(
        "PostgreSQL catalog '{catalog}' failed to record the foreign keys of table '{schema}.{table_name}', so anything relying on them cannot see how it joins to other tables. Cause: {cause}. Report this bug: https://github.com/spiceai/spiceai/issues"
    )
}

/// What `refresh_schemas` does with a single schema after attempting to refresh
/// its tables (#11724). Factored out as a pure decision so the
/// (refresh succeeded / failed) × (previous entry present / absent) matrix can be
/// unit-tested without live `PostgreSQL` I/O, guarding against a refactor
/// reintroducing the all-or-nothing failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaRefreshOutcome {
    /// Refresh succeeded — install the freshly discovered schema.
    InsertNew,
    /// Refresh failed but a last-known-good entry exists — keep it so a
    /// transient error doesn't flap a previously healthy schema out of the catalog.
    KeepPrevious,
    /// Refresh failed and the schema has never refreshed successfully — drop it
    /// for this cycle.
    Skip,
}
/// Decide the outcome for a schema from whether its table refresh succeeded and
/// whether a previously discovered entry exists. A successful refresh always
/// installs the new schema; a failure keeps the last-known-good entry when one
/// exists and otherwise skips the schema.
fn schema_refresh_outcome(refresh_succeeded: bool, has_previous: bool) -> SchemaRefreshOutcome {
    match (refresh_succeeded, has_previous) {
        (true, _) => SchemaRefreshOutcome::InsertNew,
        (false, true) => SchemaRefreshOutcome::KeepPrevious,
        (false, false) => SchemaRefreshOutcome::Skip,
    }
}

async fn build_table_providers_for_schema(
    location: SchemaLocation<'_>,
    table_names: Vec<String>,
    table_creator: &Arc<dyn Read>,
    selector: &TableSelector,
    foreign_keys: &ForeignKeyMap,
    comments: &CommentMap,
    schemas: &HashMap<String, SchemaRef>,
) -> HashMap<String, Arc<dyn TableProvider>> {
    let SchemaLocation {
        catalog: catalog_name,
        schema: schema_name,
    } = location;
    let mut tables = HashMap::new();

    for table_name in table_names {
        let schema_with_table = format!("{schema_name}.{table_name}");
        if let Some(reason) = selector.rejection_reason(&schema_with_table) {
            tracing::debug!(
                "Table '{schema_with_table}' is not selected ({reason}); it is absent from PostgreSQL catalog '{catalog_name}'"
            );
            continue;
        }

        let table_ref = TableReference::partial(schema_name.to_owned(), table_name.clone());

        // A resolved schema lets this table skip its own schema query; without
        // one, the provider resolves the schema itself.
        let provider = match schemas.get(&table_name) {
            Some(schema) => {
                table_creator
                    .table_provider_with_schema(table_ref, Arc::clone(schema))
                    .await
            }
            None => table_creator.table_provider(table_ref).await,
        };

        match provider {
            Ok(provider) => {
                let mut table_metadata = HashMap::new();
                if let Some(fks) = foreign_keys.get(&table_name) {
                    match serde_json::to_string(fks) {
                        Ok(fk_json) => {
                            table_metadata.insert(FOREIGN_KEYS_METADATA_KEY.to_string(), fk_json);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "{}",
                                table_foreign_keys_warning(location, &table_name, &e.to_string(),)
                            );
                        }
                    }
                }

                let field_metadata =
                    comments
                        .get(&table_name)
                        .map_or_else(HashMap::new, |comments| {
                            let (comment_metadata, field_metadata) =
                                table_comments_metadata(comments);
                            table_metadata.extend(comment_metadata);
                            field_metadata
                        });

                let provider = if table_metadata.is_empty() && field_metadata.is_empty() {
                    provider
                } else {
                    metadata_enriched_table_provider(provider, table_metadata, field_metadata)
                };
                tables.insert(table_name, provider);
            }
            Err(e) => {
                tracing::warn!(
                    "{}",
                    table_skipped_warning(location, &table_name, &e.to_string())
                );
            }
        }
    }

    tables
}

fn table_comments_metadata(comments: &TableComments) -> (HashMap<String, String>, FieldMetadata) {
    let mut table_metadata = HashMap::new();
    if let Some(comment) = &comments.table_comment {
        table_metadata.insert(DESCRIPTION_METADATA_KEY.to_string(), comment.clone());
    }

    let mut field_metadata = FieldMetadata::new();
    for (column, source_type) in &comments.column_source_types {
        field_metadata
            .entry(column.clone())
            .or_default()
            .insert(SOURCE_TYPE_METADATA_KEY.to_string(), source_type.clone());
    }
    for (column, comment) in &comments.column_comments {
        field_metadata
            .entry(column.clone())
            .or_default()
            .insert(DESCRIPTION_METADATA_KEY.to_string(), comment.clone());
    }

    (table_metadata, field_metadata)
}

#[must_use]
pub fn postgres_metadata_from_rows(
    rows: impl IntoIterator<Item = PostgresTableMetadataRow>,
) -> (HashMap<String, String>, FieldMetadata) {
    let mut comments = TableComments::default();
    for (table_comment, column_name, column_comment, column_source_type) in rows {
        if comments.table_comment.is_none()
            && let Some(comment) = table_comment.filter(|comment| !comment.is_empty())
        {
            comments.table_comment = Some(comment);
        }
        if let Some(comment) = column_comment.filter(|comment| !comment.is_empty()) {
            comments
                .column_comments
                .insert(column_name.clone(), comment);
        }
        if !column_source_type.is_empty() {
            comments
                .column_source_types
                .insert(column_name, column_source_type);
        }
    }

    table_comments_metadata(&comments)
}

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        Ok(guard.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommentMap, ForeignKeyConstraint, ForeignKeyMap, POSTGRES_CATALOG_DOCS,
        POSTGRES_CONNECTOR_DOCS, SchemaLocation, SchemaMetadata, SchemaRefreshOutcome,
        TableComments, build_table_providers_for_schema, column_types_fallback_warning,
        empty_catalog_warning, foreign_key_target, schema_discovery_warning,
        schema_metadata_warning, schema_refresh_outcome, select_relations,
        table_foreign_keys_warning, table_skipped_warning,
    };
    use crate::{
        DESCRIPTION_METADATA_KEY, FOREIGN_KEYS_METADATA_KEY, Read, SOURCE_TYPE_METADATA_KEY,
        catalog_filter::TableSelector,
    };
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::error::Result as DataFusionResult;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::Expr;
    use datafusion::sql::TableReference;
    use globset::{Glob, GlobSetBuilder};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct MockTableProvider;

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    "customer_id",
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            ]))
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            unimplemented!("Not needed for tests")
        }
    }

    #[derive(Debug)]
    struct MockRead {
        failing: HashSet<String>,
        requested: Mutex<Vec<String>>,
        /// Tables built from a caller-supplied schema, so a test can tell which
        /// construction path each one took.
        built_from_schema: Mutex<Vec<String>>,
    }

    impl MockRead {
        fn new(failing: HashSet<String>) -> Self {
            Self {
                failing,
                requested: Mutex::new(Vec::new()),
                built_from_schema: Mutex::new(Vec::new()),
            }
        }

        fn supplied_schema_tables(&self) -> Vec<String> {
            self.built_from_schema
                .lock()
                .expect("built_from_schema mutex should not be poisoned")
                .clone()
        }

        fn seen_tables(&self) -> Vec<String> {
            self.requested
                .lock()
                .expect("requested mutex should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl Read for MockRead {
        async fn table_provider(
            &self,
            table_reference: TableReference,
        ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>>
        {
            let (schema, table) = match table_reference {
                TableReference::Partial { schema, table } => {
                    (schema.to_string(), table.to_string())
                }
                _ => return Err("expected partial table reference".into()),
            };

            let full_name = format!("{schema}.{table}");
            self.requested
                .lock()
                .expect("requested mutex should not be poisoned")
                .push(full_name.clone());

            if self.failing.contains(&full_name) {
                return Err("simulated table provider creation failure".into());
            }

            Ok(Arc::new(MockTableProvider))
        }

        async fn table_provider_with_schema(
            &self,
            table_reference: TableReference,
            _schema: SchemaRef,
        ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>>
        {
            self.built_from_schema
                .lock()
                .expect("built_from_schema mutex should not be poisoned")
                .push(table_reference.table().to_string());
            self.table_provider(table_reference).await
        }
    }

    fn make_globset(patterns: &[&str]) -> globset::GlobSet {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(Glob::new(pattern).expect("glob pattern should parse"));
        }
        builder.build().expect("glob set should build")
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    /// The bulk lookup must ask about the tables the schema will register, and
    /// no others.
    ///
    /// Describing a rejected relation is invisible in the catalog -- it is
    /// dropped when the providers are built either way -- so nothing downstream
    /// can catch a selection that widens back to the whole namespace. That is
    /// the cost this change exists to remove, which makes this the assertion
    /// that guards it.
    #[test]
    fn select_relations_names_only_the_tables_the_schema_will_register() {
        let all = names(&["orders", "lineitem", "customer"]);

        let include = TableSelector::new(Some(make_globset(&["public.orders"])), None);
        assert_eq!(
            select_relations(&include, "public", &all),
            names(&["orders"]),
            "an include pattern should leave only the tables it matches"
        );

        let exclude = TableSelector::new(None, Some(make_globset(&["public.lineitem"])));
        assert_eq!(
            select_relations(&exclude, "public", &all),
            names(&["orders", "customer"]),
            "an exclude pattern should drop only the tables it matches"
        );

        assert_eq!(
            select_relations(&TableSelector::select_all(), "public", &all),
            all,
            "an unfiltered catalog should still describe every table"
        );
    }

    /// A selection matching nothing must stay empty rather than falling back to
    /// every table, which is what an "empty means unfiltered" reading would do.
    #[test]
    fn select_relations_is_empty_when_no_table_is_selected() {
        let selector = TableSelector::new(Some(make_globset(&["public.nothing_here"])), None);

        assert!(
            select_relations(&selector, "public", &names(&["orders", "lineitem"])).is_empty(),
            "a selection that matches nothing should describe nothing"
        );
    }

    /// The schema name takes part in the match, so the same table name in a
    /// schema the pattern does not name is not selected.
    #[test]
    fn select_relations_matches_within_the_schema_being_refreshed() {
        let selector = TableSelector::new(Some(make_globset(&["sales.orders"])), None);
        let all = names(&["orders"]);

        assert_eq!(
            select_relations(&selector, "sales", &all),
            names(&["orders"])
        );
        assert!(
            select_relations(&selector, "public", &all).is_empty(),
            "`sales.orders` should not select `public.orders`"
        );
    }

    /// Regression test for #11727: a foreign-key target whose schema or table
    /// name legally contains a `.` (e.g. a schema created as `"my.schema"`)
    /// must round-trip back to the exact `(catalog, schema, table)` triple.
    ///
    /// The pre-fix code joined the parts with a bare `format!("{}.{}.{}")`,
    /// producing the ambiguous `spice.my.schema.customers`. That string has four
    /// identifier parts, which `TableReference::parse_str` cannot resolve to a
    /// 3-part reference — it silently degrades to a bare table name, so a
    /// downstream NL-to-SQL consumer loses (or misresolves) the FK target.
    #[test]
    fn foreign_key_target_with_dotted_identifier_round_trips() {
        let cases = [
            ("spice", "my.schema", "customers"),
            ("spice", "sales", "order.lines"),
            ("odd.catalog", "sales", "customers"),
            // A component containing a literal double-quote must also survive.
            ("spice", "we\"ird", "customers"),
        ];

        for (catalog, schema, table) in cases {
            let target = foreign_key_target(catalog, schema, table);
            let parsed = TableReference::parse_str(&target);

            assert_eq!(
                parsed.catalog(),
                Some(catalog),
                "catalog must round-trip (target = `{target}`)"
            );
            assert_eq!(
                parsed.schema(),
                Some(schema),
                "schema must round-trip (target = `{target}`)"
            );
            assert_eq!(
                parsed.table(),
                table,
                "table must round-trip (target = `{target}`)"
            );
        }
    }

    /// A schema whose discovery fails is skipped or held at its last known
    /// contents, and the log line is the only explanation the user gets for a
    /// catalog that is missing tables or serving stale ones. Both must name the
    /// catalog and the schema, and say which of the two happened.
    #[test]
    fn schema_discovery_warning_says_what_the_user_will_observe() {
        let skipped = schema_discovery_warning(
            "pg",
            "sales",
            SchemaRefreshOutcome::Skip,
            "Failed to connect to PostgreSQL: connection refused",
        )
        .expect("a failed discovery should report");

        assert!(
            skipped.contains("PostgreSQL catalog 'pg'") && skipped.contains("schema 'sales'"),
            "names the catalog and the schema: {skipped}"
        );
        assert!(
            skipped.contains("failed to list the tables of schema 'sales'"),
            "states the problem first: {skipped}"
        );
        assert!(
            skipped.contains("so no table in that schema is registered")
                && skipped.contains("queries against 'pg.sales' will not resolve"),
            "then its impact, as one situation rather than two: {skipped}"
        );
        assert!(
            skipped.contains("Cause: Failed to connect to PostgreSQL: connection refused"),
            "and ends with the cause, labelled: {skipped}"
        );

        let kept = schema_discovery_warning(
            "pg",
            "sales",
            SchemaRefreshOutcome::KeepPrevious,
            "Failed to connect to PostgreSQL: connection refused",
        )
        .expect("a failed discovery should report");
        assert!(
            kept.contains("failed to list the tables of schema 'sales'")
                && kept.contains("out of date"),
            "a schema held at its last known contents is stale, not absent: {kept}"
        );

        for message in [&skipped, &kept] {
            assert!(!message.contains('\n'), "stays on one line: {message:?}");
            assert!(
                !message.contains("table provider") && !message.contains("last-known-good"),
                "uses no internal vocabulary: {message}"
            );
        }

        assert!(
            schema_discovery_warning("pg", "sales", SchemaRefreshOutcome::InsertNew, "").is_none(),
            "a schema that refreshed has nothing to report"
        );
    }

    /// The remaining degrade-and-continue paths leave a table or a whole schema
    /// short of something the user configured, so each has to name the catalog
    /// and what is missing from it.
    #[test]
    fn degraded_refresh_warnings_name_the_catalog_and_the_loss() {
        let fks = schema_metadata_warning("pg", "sales", SchemaMetadata::ForeignKeys, "denied");
        assert!(
            fks.contains(
                "PostgreSQL catalog 'pg' failed to determine the foreign keys of schema 'sales'"
            ) && fks.contains("cannot see how its tables join"),
            "one problem, then its impact: {fks}"
        );

        let comments =
            schema_metadata_warning("pg", "sales", SchemaMetadata::Descriptions, "denied");
        assert!(
            comments.contains("failed to read the table and column descriptions of schema 'sales'"),
            "{comments}"
        );

        let fallback = column_types_fallback_warning("pg", "sales", "denied");
        assert!(
            fallback.contains("slower") && fallback.contains("unaffected"),
            "the fallback costs time, not tables, and must say so: {fallback}"
        );

        let location = SchemaLocation {
            catalog: "pg",
            schema: "sales",
        };
        let skipped = table_skipped_warning(location, "orders", "unsupported type");
        assert!(
            skipped.contains("failed to load table 'sales.orders'")
                && skipped.contains("queries against 'pg.sales.orders' will not resolve"),
            "{skipped}"
        );
        assert!(
            skipped.contains("SELECT on the table") && skipped.contains(POSTGRES_CONNECTOR_DOCS),
            "the loader's error carries no fix, so this message must: {skipped}"
        );

        let table_fks = table_foreign_keys_warning(location, "orders", "invalid json");
        assert!(
            table_fks.contains("github.com/spiceai/spiceai/issues"),
            "recording foreign keys is Spice's own work, so a failure is a bug: {table_fks}"
        );

        for message in [&fks, &comments, &fallback, &skipped, &table_fks] {
            assert!(!message.contains('\n'), "stays on one line: {message:?}");
            assert!(
                message.contains("failed to") && message.contains(", so "),
                "states one problem and then its impact: {message}"
            );
            assert!(
                !message.contains("metadata")
                    && !message.contains("table provider")
                    && !message.contains("serialize"),
                "uses no internal vocabulary: {message}"
            );
        }
    }

    /// The empty-catalog warning has to fire when a catalog *becomes* empty and
    /// stay quiet while it stays that way, or a steady misconfiguration prints
    /// once a refresh interval forever and the log stops being worth reading.
    #[test]
    fn empty_catalog_warning_reports_the_transition_to_empty() {
        let selector = TableSelector::select_all();
        let warn = |previous, current| {
            empty_catalog_warning("pg", previous, current, 0, 1, &selector).is_some()
        };

        assert!(warn(None, 0), "the first refresh finding nothing warns");
        assert!(warn(Some(3), 0), "losing every table warns");
        assert!(
            !warn(Some(0), 0),
            "a catalog that was already empty must not warn again"
        );
        assert!(!warn(None, 3), "a catalog with tables never warns");
        assert!(!warn(Some(0), 3), "recovering from empty does not warn");
    }

    /// The message exists to tell "my patterns are wrong" from "my tables are
    /// not there yet", so it must name the patterns when there are any and say
    /// something useful when there are none.
    #[test]
    fn empty_catalog_warning_names_the_configured_patterns() {
        let filtered = TableSelector::new(Some(make_globset(&["public.orders"])), None)
            .with_include_patterns(&["public.orders".to_string()])
            .with_exclude_patterns(&["public.secret".to_string()]);

        let message = empty_catalog_warning("pg", None, 0, 0, 2, &filtered)
            .expect("an empty filtered catalog should warn");
        assert!(message.contains("'pg'"), "names the catalog: {message}");
        assert!(
            message.contains("include: ['public.orders']")
                && message.contains("exclude: ['public.secret']"),
            "names both halves of the configuration: {message}"
        );
        assert!(
            message.contains("2 schema(s)"),
            "says how much was searched: {message}"
        );
        assert!(
            message.contains(POSTGRES_CATALOG_DOCS),
            "links the docs: {message}"
        );
        assert!(!message.contains('\n'), "stays on one line: {message:?}");

        // An unfiltered catalog cannot blame patterns, so it must point at the
        // other two explanations instead of naming an empty pattern list.
        let unfiltered = empty_catalog_warning("pg", None, 0, 0, 1, &TableSelector::select_all())
            .expect("an empty unfiltered catalog should warn");
        assert!(
            !unfiltered.contains("include:") && !unfiltered.contains("matched"),
            "does not blame patterns that were never configured: {unfiltered}"
        );
        assert!(
            unfiltered.contains("grants"),
            "points at the likely cause instead: {unfiltered}"
        );
    }

    /// A catalog can register nothing while the patterns matched perfectly well:
    /// a selected table whose provider cannot be built is logged and skipped.
    ///
    /// Blaming the patterns there would contradict the failure logged moments
    /// earlier and send the user to fix configuration that is already correct.
    #[test]
    fn empty_catalog_warning_does_not_blame_patterns_for_tables_that_failed_to_build() {
        let filtered = TableSelector::new(Some(make_globset(&["public.orders"])), None)
            .with_include_patterns(&["public.orders".to_string()]);

        let message = empty_catalog_warning("pg", None, 0, 3, 1, &filtered)
            .expect("a catalog that registered nothing should warn");

        assert!(
            message.contains("3 table(s) matched, but none could be registered"),
            "it should report that the matches failed rather than that nothing matched: {message}"
        );
        assert!(
            !message.contains("never matches"),
            "it must not offer the unqualified-pattern advice when the patterns did match: {message}"
        );
        assert!(
            !message.contains("grants"),
            "it must not blame grants either: {message}"
        );
    }

    /// A pattern is user-supplied text and can legally contain a newline, which
    /// would split one warning into what reads as two log records.
    #[test]
    fn empty_catalog_warning_stays_on_one_line_for_a_pattern_containing_a_newline() {
        let hostile = TableSelector::new(None, None)
            .with_include_patterns(&["public.a\nWARN forged line".to_string()]);

        let message = empty_catalog_warning("pg", None, 0, 0, 1, &hostile)
            .expect("an empty filtered catalog should warn");

        assert!(
            !message.contains('\n'),
            "the warning must stay on one line: {message:?}"
        );
        assert!(
            message.contains("\\n"),
            "the newline should survive as an escape so the pattern is still legible: {message}"
        );
    }

    /// Regression coverage for #11724: the per-schema failure decision must keep
    /// a previously healthy schema on a transient error and only drop a schema
    /// that has never refreshed successfully — never abort the whole catalog.
    #[test]
    fn schema_refresh_outcome_covers_full_matrix() {
        // A successful refresh always installs the freshly discovered schema,
        // regardless of whether a previous entry existed.
        assert_eq!(
            schema_refresh_outcome(true, true),
            SchemaRefreshOutcome::InsertNew
        );
        assert_eq!(
            schema_refresh_outcome(true, false),
            SchemaRefreshOutcome::InsertNew
        );
        // A failed refresh keeps the last-known-good entry when one exists...
        assert_eq!(
            schema_refresh_outcome(false, true),
            SchemaRefreshOutcome::KeepPrevious
        );
        // ...and only drops the schema when it has never refreshed successfully.
        assert_eq!(
            schema_refresh_outcome(false, false),
            SchemaRefreshOutcome::Skip
        );
    }

    /// A table whose schema was resolved in bulk must not resolve it again, and
    /// one that was not must still be resolved individually.
    ///
    /// The whole saving depends on that split: sending every table down the
    /// bulk path would mis-describe the ones the catalog query cannot cover,
    /// and sending none down it would restore the per-table round trip the bulk
    /// query was added to remove. Neither is visible in the resulting catalog,
    /// so the paths are recorded and asserted rather than inferred.
    #[tokio::test]
    async fn test_build_table_providers_uses_a_resolved_schema_only_where_one_exists() {
        let table_creator = Arc::new(MockRead::new(HashSet::new()));
        let read: Arc<dyn Read> = Arc::clone(&table_creator) as Arc<dyn Read>;

        // `orders` was covered by the bulk query; `lineitem` was not, standing in
        // for a relation the catalog query cannot describe.
        let mut schemas: HashMap<String, SchemaRef> = HashMap::new();
        schemas.insert(
            "orders".to_string(),
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
        );

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string(), "lineitem".to_string()],
            &read,
            &TableSelector::select_all(),
            &HashMap::new(),
            &HashMap::new(),
            &schemas,
        )
        .await;

        assert_eq!(tables.len(), 2, "both tables should be registered");
        assert_eq!(
            table_creator.supplied_schema_tables(),
            vec!["orders".to_string()],
            "only the table with a resolved schema should skip its own schema query"
        );
        assert_eq!(
            table_creator.seen_tables(),
            vec!["public.orders".to_string(), "public.lineitem".to_string()],
            "both tables should still reach the factory"
        );
    }

    #[tokio::test]
    async fn test_build_table_providers_applies_include_filter_before_factory() {
        let read = Arc::new(MockRead::new(HashSet::new()));
        let selector = TableSelector::new(Some(make_globset(&["public.orders"])), None);
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();
        let no_comments: CommentMap = HashMap::new();

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            &selector,
            &no_fks,
            &no_comments,
            &HashMap::new(),
        )
        .await;

        assert_eq!(tables.len(), 1);
        assert!(tables.contains_key("orders"));
        assert_eq!(read.seen_tables(), vec!["public.orders".to_string()]);
    }

    #[tokio::test]
    async fn test_build_table_providers_applies_exclude_filter_before_factory() {
        let read = Arc::new(MockRead::new(HashSet::new()));
        let selector = TableSelector::new(None, Some(make_globset(&["public.lineitem"])));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();
        let no_comments: CommentMap = HashMap::new();

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            &selector,
            &no_fks,
            &no_comments,
            &HashMap::new(),
        )
        .await;

        assert_eq!(tables.len(), 1);
        assert!(tables.contains_key("orders"));
        assert_eq!(read.seen_tables(), vec!["public.orders".to_string()]);
    }

    #[tokio::test]
    async fn test_build_table_providers_skips_failed_table_provider_creation() {
        let mut failing = HashSet::new();
        failing.insert("public.orders".to_string());
        let read = Arc::new(MockRead::new(failing));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();
        let no_comments: CommentMap = HashMap::new();

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            &TableSelector::select_all(),
            &no_fks,
            &no_comments,
            &HashMap::new(),
        )
        .await;

        assert_eq!(
            tables.keys().cloned().collect::<HashSet<String>>(),
            HashSet::from(["lineitem".to_string()])
        );
        assert_eq!(
            read.seen_tables().into_iter().collect::<HashSet<String>>(),
            HashSet::from(["public.orders".to_string(), "public.lineitem".to_string()])
        );
    }

    #[tokio::test]
    async fn test_build_table_providers_returns_empty_when_all_factory_calls_fail() {
        let failing = HashSet::from(["public.orders".to_string(), "public.lineitem".to_string()]);
        let read = Arc::new(MockRead::new(failing));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();
        let no_comments: CommentMap = HashMap::new();

        let tables: HashMap<String, Arc<dyn TableProvider>> = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            &TableSelector::select_all(),
            &no_fks,
            &no_comments,
            &HashMap::new(),
        )
        .await;

        assert!(tables.is_empty(), "all failing tables should be skipped");
    }

    #[tokio::test]
    async fn test_build_table_providers_injects_foreign_key_metadata() {
        let read = Arc::new(MockRead::new(HashSet::new()));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_comments: CommentMap = HashMap::new();

        let mut fk_map: ForeignKeyMap = HashMap::new();
        fk_map.insert(
            "orders".to_string(),
            vec![ForeignKeyConstraint {
                columns: vec!["customer_id".to_string()],
                foreign_table: "public.customers".to_string(),
                foreign_columns: vec!["id".to_string()],
            }],
        );

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string(), "lineitem".to_string()],
            &table_creator,
            &TableSelector::select_all(),
            &fk_map,
            &no_comments,
            &HashMap::new(),
        )
        .await;

        // orders should have FK metadata
        let orders_provider = tables.get("orders").expect("orders table should exist");
        let orders_metadata = orders_provider.schema().metadata().clone();
        let fk_json = orders_metadata
            .get(FOREIGN_KEYS_METADATA_KEY)
            .expect("orders should have foreign_keys metadata");
        let fks: Vec<serde_json::Value> =
            serde_json::from_str(fk_json).expect("FK metadata should be valid JSON");
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0]["columns"], serde_json::json!(["customer_id"]));
        assert_eq!(fks[0]["foreign_table"], "public.customers");
        assert_eq!(fks[0]["foreign_columns"], serde_json::json!(["id"]));

        // lineitem should have no FK metadata
        let lineitem_provider = tables.get("lineitem").expect("lineitem table should exist");
        let lineitem_schema = lineitem_provider.schema();
        assert!(
            lineitem_schema
                .metadata()
                .get(FOREIGN_KEYS_METADATA_KEY)
                .is_none(),
            "lineitem should not have foreign_keys metadata"
        );
    }

    #[tokio::test]
    async fn test_build_table_providers_injects_composite_foreign_key() {
        let read = Arc::new(MockRead::new(HashSet::new()));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_comments: CommentMap = HashMap::new();

        let mut fk_map: ForeignKeyMap = HashMap::new();
        fk_map.insert(
            "order_lines".to_string(),
            vec![ForeignKeyConstraint {
                columns: vec!["order_id".to_string(), "line_id".to_string()],
                foreign_table: "public.orders".to_string(),
                foreign_columns: vec!["id".to_string(), "line_num".to_string()],
            }],
        );

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["order_lines".to_string()],
            &table_creator,
            &TableSelector::select_all(),
            &fk_map,
            &no_comments,
            &HashMap::new(),
        )
        .await;

        let provider = tables
            .get("order_lines")
            .expect("order_lines table should exist");
        let schema = provider.schema();
        let fk_json = schema
            .metadata()
            .get(FOREIGN_KEYS_METADATA_KEY)
            .expect("should have foreign_keys metadata");
        let fks: Vec<serde_json::Value> =
            serde_json::from_str(fk_json).expect("FK metadata should be valid JSON");
        assert_eq!(fks.len(), 1);
        assert_eq!(
            fks[0]["columns"],
            serde_json::json!(["order_id", "line_id"])
        );
        assert_eq!(fks[0]["foreign_table"], "public.orders");
        assert_eq!(
            fks[0]["foreign_columns"],
            serde_json::json!(["id", "line_num"])
        );
    }

    #[tokio::test]
    async fn test_build_table_providers_injects_comment_metadata() {
        let read = Arc::new(MockRead::new(HashSet::new()));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();

        let mut comments: CommentMap = HashMap::new();
        comments.insert(
            "orders".to_string(),
            TableComments {
                table_comment: Some("order facts".to_string()),
                column_comments: HashMap::from([(
                    "customer_id".to_string(),
                    "customer dimension key".to_string(),
                )]),
                column_source_types: HashMap::from([(
                    "customer_id".to_string(),
                    "bigint".to_string(),
                )]),
            },
        );

        let tables = build_table_providers_for_schema(
            SchemaLocation {
                catalog: "pg",
                schema: "public",
            },
            vec!["orders".to_string()],
            &table_creator,
            &TableSelector::select_all(),
            &no_fks,
            &comments,
            &HashMap::new(),
        )
        .await;

        let provider = tables.get("orders").expect("orders table should exist");
        let schema = provider.schema();
        assert_eq!(
            schema
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("order facts")
        );
        let field = schema
            .field_with_name("customer_id")
            .expect("customer_id field should exist");
        assert_eq!(
            field
                .metadata()
                .get(DESCRIPTION_METADATA_KEY)
                .map(String::as_str),
            Some("customer dimension key")
        );
        assert_eq!(
            field
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("bigint")
        );
    }
}
