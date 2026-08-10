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
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use snafu::prelude::*;

use crate::{
    DESCRIPTION_METADATA_KEY, FOREIGN_KEYS_METADATA_KEY, FieldMetadata, Read,
    RefreshableCatalogProvider, SOURCE_TYPE_METADATA_KEY, metadata_enriched_table_provider,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to get connection from PostgreSQL pool: {source}. Check `pg_host`/`pg_port`/`pg_user`/`pg_pass`/`pg_sslmode` in the dataset params and that the server is reachable. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "PostgreSQL query failed: {source}. Check SQL syntax and that referenced tables exist. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    QueryFailed { source: tokio_postgres::Error },

    #[snafu(display(
        "Failed to resolve table schemas for the PostgreSQL catalog: {source}. Check that the connected role can read `pg_catalog`, and that every column type is supported or `unsupported_type_action` permits it. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    SchemaResolutionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Wraps errors from the shared `connector-postgres-common` queries
    /// (`list_schemas`/`list_tables`, re-exported below) so this crate's own
    /// callers can still propagate them with `?`.
    #[snafu(display("{source}"), context(false))]
    Common {
        source: connector_postgres_common::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
                    schema = %schema_name,
                    "Schema cannot match any include pattern, skipping its metadata queries"
                );
                schemas.insert(
                    schema_name.clone(),
                    Arc::new(PostgresSchemaProvider::new(
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
                        schema = %schema_name,
                        error = %e,
                        "Failed to query foreign keys for schema, continuing without FK metadata"
                    );
                    HashMap::new()
                }
            };
            let comments = match self.list_comments(schema_name).await {
                Ok(comments) => comments,
                Err(e) => {
                    tracing::warn!(
                        schema = %schema_name,
                        error = %e,
                        "Failed to query comments for schema, continuing without comment metadata"
                    );
                    HashMap::new()
                }
            };

            let schema_provider = PostgresSchemaProvider::new(
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

            match schema_refresh_outcome(refresh_result.is_ok(), previous.is_some()) {
                SchemaRefreshOutcome::InsertNew => {
                    schemas.insert(schema_name.clone(), Arc::new(schema_provider));
                }
                SchemaRefreshOutcome::KeepPrevious => {
                    // Only the (Err, Some) branch reaches here, so both are present.
                    if let (Err(e), Some(previous)) = (&refresh_result, previous) {
                        tracing::warn!(
                            schema = %schema_name,
                            error = %e,
                            "Failed to discover tables for schema, keeping last-known-good state"
                        );
                        schemas.insert(schema_name.clone(), previous);
                    }
                }
                SchemaRefreshOutcome::Skip => {
                    if let Err(e) = &refresh_result {
                        tracing::warn!(
                            schema = %schema_name,
                            error = %e,
                            "Failed to discover tables for schema, skipping schema"
                        );
                    }
                }
            }
        }

        {
            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = schemas;
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
        Ok(list_schemas(&self.pool).await?)
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
    pool: Arc<PostgresConnectionPool>,
    schema_name: String,
    table_creator: Arc<dyn Read>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    selector: TableSelector,
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
        pool: Arc<PostgresConnectionPool>,
        schema_name: String,
        table_creator: Arc<dyn Read>,
        selector: TableSelector,
    ) -> Self {
        Self {
            pool,
            schema_name,
            table_creator,
            tables: RwLock::new(HashMap::new()),
            selector,
        }
    }

    async fn refresh_tables(
        &self,
        foreign_keys: &ForeignKeyMap,
        comments: &CommentMap,
    ) -> Result<()> {
        let table_names = self.list_tables().await?;

        // One query for every relation's columns, rather than one per table on
        // the way to building its provider.
        //
        // The map is advisory: an entry lets a table skip its own schema query,
        // and its absence means that table resolves individually. A failure here
        // is therefore not fatal -- an empty map resolves every table
        // individually -- and relations the catalog query cannot describe are
        // absent for the same reason.
        let schemas = match self.list_column_schemas().await {
            Ok(schemas) => schemas,
            Err(e) => {
                tracing::warn!(
                    schema = %self.schema_name,
                    error = %e,
                    "Failed to resolve schemas for this PostgreSQL schema in one query, falling back to per-table resolution"
                );
                HashMap::new()
            }
        };

        let tables = build_table_providers_for_schema(
            &self.schema_name,
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
    async fn list_column_schemas(&self) -> Result<HashMap<String, SchemaRef>> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        conn.get_schemas_in(&self.schema_name)
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
    schema_name: &str,
    table_names: Vec<String>,
    table_creator: &Arc<dyn Read>,
    selector: &TableSelector,
    foreign_keys: &ForeignKeyMap,
    comments: &CommentMap,
    schemas: &HashMap<String, SchemaRef>,
) -> HashMap<String, Arc<dyn TableProvider>> {
    let mut tables = HashMap::new();

    for table_name in table_names {
        let schema_with_table = format!("{schema_name}.{table_name}");
        if let Some(reason) = selector.rejection_reason(&schema_with_table) {
            tracing::debug!("Table {schema_with_table} is not selected ({reason}), skipping");
            continue;
        }

        let table_ref = TableReference::partial(schema_name.to_owned(), table_name.clone());

        // A schema resolved in bulk skips this table's own schema query; without
        // one, the provider resolves it as it always did.
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
                                schema = %schema_name,
                                table = %table_name,
                                error = %e,
                                "Failed to serialize foreign key metadata for table {schema_name}.{table_name}; registering without FK metadata"
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
                    schema = %schema_name,
                    table = %table_name,
                    error = %e,
                    "Failed to create table provider for PostgreSQL table {schema_with_table}, skipping"
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
        CommentMap, ForeignKeyConstraint, ForeignKeyMap, SchemaRefreshOutcome, TableComments,
        build_table_providers_for_schema, foreign_key_target, schema_refresh_outcome,
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
        fail_tables: HashSet<String>,
        seen_tables: Mutex<Vec<String>>,
        /// Tables built from a caller-supplied schema, so a test can tell which
        /// construction path each one took.
        supplied_schema_tables: Mutex<Vec<String>>,
    }

    impl MockRead {
        fn new(fail_tables: HashSet<String>) -> Self {
            Self {
                fail_tables,
                seen_tables: Mutex::new(Vec::new()),
                supplied_schema_tables: Mutex::new(Vec::new()),
            }
        }

        fn supplied_schema_tables(&self) -> Vec<String> {
            self.supplied_schema_tables
                .lock()
                .expect("supplied_schema_tables mutex should not be poisoned")
                .clone()
        }

        fn seen_tables(&self) -> Vec<String> {
            self.seen_tables
                .lock()
                .expect("seen_tables mutex should not be poisoned")
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
            self.seen_tables
                .lock()
                .expect("seen_tables mutex should not be poisoned")
                .push(full_name.clone());

            if self.fail_tables.contains(&full_name) {
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
            self.supplied_schema_tables
                .lock()
                .expect("supplied_schema_tables mutex should not be poisoned")
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
            "public",
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
            "public",
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
            "public",
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
        let mut fail_tables = HashSet::new();
        fail_tables.insert("public.orders".to_string());
        let read = Arc::new(MockRead::new(fail_tables));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();
        let no_comments: CommentMap = HashMap::new();

        let tables = build_table_providers_for_schema(
            "public",
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
        let fail_tables =
            HashSet::from(["public.orders".to_string(), "public.lineitem".to_string()]);
        let read = Arc::new(MockRead::new(fail_tables));
        let table_creator: Arc<dyn Read> = Arc::<MockRead>::clone(&read);
        let no_fks: ForeignKeyMap = HashMap::new();
        let no_comments: CommentMap = HashMap::new();

        let tables: HashMap<String, Arc<dyn TableProvider>> = build_table_providers_for_schema(
            "public",
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
            "public",
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
            "public",
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
            "public",
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
