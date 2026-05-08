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

//! Snowflake catalog provider implementation.
//!
//! Discovers schemas and tables in a Snowflake database using
//! `INFORMATION_SCHEMA` queries and provides them as `DataFusion` catalog/schema providers.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use futures::stream::{self, StreamExt};
use globset::GlobSet;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;

use crate::{Read, ReadWrite, RefreshableCatalogProvider};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute Snowflake query: {source}"))]
    QueryFailed {
        source: snowflake_api::SnowflakeApiError,
    },

    #[snafu(display("Unexpected Snowflake query response: {reason}"))]
    UnexpectedResponse { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
enum SnowflakeTableCreator {
    Read(Arc<dyn Read>),
    ReadWrite(Arc<dyn ReadWrite>),
}

impl SnowflakeTableCreator {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Read(table_creator) => table_creator.table_provider(table_reference).await,
            Self::ReadWrite(table_creator) => table_creator.table_provider(table_reference).await,
        }
    }
}

fn quote_sql_identifier(value: &str) -> Result<String> {
    ensure!(
        !value.contains('\0'),
        UnexpectedResponseSnafu {
            reason: "Snowflake identifier contains NUL byte"
        }
    );

    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn quote_sql_string_literal(value: &str) -> Result<String> {
    ensure!(
        !value.contains('\0'),
        UnexpectedResponseSnafu {
            reason: "Snowflake string literal contains NUL byte"
        }
    );

    Ok(format!("'{}'", value.replace('\'', "''")))
}

/// Maximum number of concurrent Snowflake API requests during catalog discovery.
const CATALOG_DISCOVERY_CONCURRENCY: usize = 10;

/// A catalog provider for Snowflake that discovers schemas and tables
/// by querying Snowflake's `INFORMATION_SCHEMA`.
pub struct SnowflakeCatalogProvider {
    /// Snowflake API client for metadata queries
    api: Arc<SnowflakeApi>,
    /// The Snowflake database name to catalog
    database: String,
    /// Table factory for creating table providers from discovered tables
    table_creator: SnowflakeTableCreator,
    /// Cached schemas (`schema_name` -> `SchemaProvider`)
    schemas: RwLock<HashMap<String, Arc<SnowflakeSchemaProvider>>>,
    /// Optional glob filter for `schema.table` patterns
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for SnowflakeCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeCatalogProvider")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl SnowflakeCatalogProvider {
    #[must_use]
    pub fn new(
        api: Arc<SnowflakeApi>,
        database: String,
        table_creator: Arc<dyn Read>,
        include: Option<GlobSet>,
    ) -> Self {
        Self::new_with_table_creator(
            api,
            database,
            SnowflakeTableCreator::Read(table_creator),
            include,
        )
    }

    #[must_use]
    pub fn new_read_write(
        api: Arc<SnowflakeApi>,
        database: String,
        table_creator: Arc<dyn ReadWrite>,
        include: Option<GlobSet>,
    ) -> Self {
        Self::new_with_table_creator(
            api,
            database,
            SnowflakeTableCreator::ReadWrite(table_creator),
            include,
        )
    }

    fn new_with_table_creator(
        api: Arc<SnowflakeApi>,
        database: String,
        table_creator: SnowflakeTableCreator,
        include: Option<GlobSet>,
    ) -> Self {
        Self {
            api,
            database,
            table_creator,
            schemas: RwLock::new(HashMap::new()),
            include: include.map(Arc::new),
        }
    }

    /// Discovers schemas in the Snowflake database and populates the cache.
    ///
    /// This refresh is intentionally all-or-nothing: a fully rebuilt map is swapped
    /// into the cache only after discovery and table-provider creation succeeds for
    /// every discovered schema. This avoids exposing partially refreshed metadata.
    async fn refresh_schemas(&self) -> Result<()> {
        let refresh_start = Instant::now();
        let schema_names = self.list_schemas().await?;
        tracing::info!(database = %self.database, count = schema_names.len(), "Snowflake: discovered schemas, refreshing tables");

        let results: Vec<(String, Result<Arc<SnowflakeSchemaProvider>>)> =
            stream::iter(schema_names.into_iter().map(|schema_name| {
                let api = Arc::clone(&self.api);
                let database = self.database.clone();
                let table_creator = self.table_creator.clone();
                let include = self.include.clone();
                async move {
                    let provider = SnowflakeSchemaProvider::new(
                        api,
                        database,
                        schema_name.clone(),
                        table_creator,
                        include,
                    );
                    let result = provider.refresh_tables().await;
                    (schema_name, result.map(|()| Arc::new(provider)))
                }
            }))
            .buffer_unordered(CATALOG_DISCOVERY_CONCURRENCY)
            .collect()
            .await;

        let mut schemas = HashMap::new();
        for (schema_name, result) in results {
            let provider = result?;
            schemas.insert(schema_name, provider);
        }

        {
            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = schemas;
        }

        tracing::info!(duration_ms = refresh_start.elapsed().as_millis(), database = %self.database, "Snowflake: catalog refresh complete");

        Ok(())
    }

    /// Lists schema names in the configured database, excluding system schemas.
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let start = Instant::now();
        let quoted_db = quote_sql_identifier(&self.database)?;
        let query = format!(
            "SELECT SCHEMA_NAME FROM {quoted_db}.INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
        );
        tracing::debug!(query = %query, "Snowflake: listing schemas");

        let result = self.api.exec(&query).await.context(QueryFailedSnafu)?;

        match result {
            snowflake_api::QueryResult::Arrow(batches) => {
                let mut names = Vec::new();
                for batch in batches {
                    let col = batch.column_by_name("SCHEMA_NAME").context(
                        UnexpectedResponseSnafu {
                            reason: "Missing SCHEMA_NAME column in Arrow response for schema listing",
                        },
                    )?;

                    let array = col.as_any().downcast_ref::<arrow::array::StringArray>().context(
                        UnexpectedResponseSnafu {
                            reason: "SCHEMA_NAME column has unexpected data type in Arrow response for schema listing; expected Utf8",
                        },
                    )?;

                    for value in array.iter().flatten() {
                        if value != "INFORMATION_SCHEMA" {
                            names.push(value.to_string());
                        }
                    }
                }
                tracing::debug!(
                    duration_ms = start.elapsed().as_millis(),
                    count = names.len(),
                    "Snowflake: listed schemas"
                );
                Ok(names)
            }
            snowflake_api::QueryResult::Json(_) => UnexpectedResponseSnafu {
                reason: "Expected Arrow response for schema listing, got JSON",
            }
            .fail(),
            snowflake_api::QueryResult::Empty => Ok(Vec::new()),
        }
    }
}

impl CatalogProvider for SnowflakeCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
impl RefreshableCatalogProvider for SnowflakeCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for Snowflake that discovers tables within a schema.
pub struct SnowflakeSchemaProvider {
    /// Snowflake API client for metadata queries
    api: Arc<SnowflakeApi>,
    /// The Snowflake database name
    database: String,
    /// The schema name
    schema_name: String,
    /// Table factory for creating table providers
    table_creator: SnowflakeTableCreator,
    /// Cached tables (`table_name` -> `TableProvider`)
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    /// Optional glob filter for `schema.table` patterns
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for SnowflakeSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeSchemaProvider")
            .field("database", &self.database)
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

impl SnowflakeSchemaProvider {
    fn new(
        api: Arc<SnowflakeApi>,
        database: String,
        schema_name: String,
        table_creator: SnowflakeTableCreator,
        include: Option<Arc<GlobSet>>,
    ) -> Self {
        Self {
            api,
            database,
            schema_name,
            table_creator,
            tables: RwLock::new(HashMap::new()),
            include,
        }
    }

    /// Discovers tables in this schema and creates table providers for each.
    ///
    /// Table providers are created concurrently (up to [`CATALOG_DISCOVERY_CONCURRENCY`]
    /// at a time) to reduce overall latency when a schema contains many tables.
    async fn refresh_tables(&self) -> Result<()> {
        let start = Instant::now();
        let table_names = self.list_tables().await?;

        // Filter table names first, then create providers concurrently.
        let filtered_tables: Vec<String> = table_names
            .into_iter()
            .filter(|table_name| {
                let schema_with_table = format!("{}.{}", self.schema_name, table_name);
                if let Some(include) = &self.include
                    && !include.is_match(&schema_with_table)
                {
                    tracing::debug!("Table {schema_with_table} is not included, skipping");
                    return false;
                }
                true
            })
            .collect();

        tracing::debug!(database = %self.database, schema = %self.schema_name, count = filtered_tables.len(), "Snowflake: creating table providers concurrently");

        let mut tables = HashMap::new();
        let mut stream = stream::iter(filtered_tables.into_iter().map(|table_name| {
            let database = self.database.clone();
            let schema_name = self.schema_name.clone();
            let table_creator = self.table_creator.clone();
            async move {
                // Quote each part with double quotes for Snowflake SQL, matching the
                // pattern used by the Snowflake data connector.
                let escaped_database = database.replace('"', "\"\"");
                let escaped_schema = schema_name.replace('"', "\"\"");
                let escaped_table = table_name.replace('"', "\"\"");
                let quoted_path =
                    format!("\"{escaped_database}\".\"{escaped_schema}\".\"{escaped_table}\"");
                let table_ref: TableReference = quoted_path.into();
                let result = table_creator.table_provider(table_ref).await;
                (table_name, result)
            }
        }))
        .buffer_unordered(CATALOG_DISCOVERY_CONCURRENCY);

        while let Some((table_name, result)) = stream.next().await {
            match result {
                Ok(provider) => {
                    tables.insert(table_name, provider);
                }
                Err(e) => {
                    tracing::warn!(
                        database = %self.database,
                        schema = %self.schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for Snowflake table, skipping"
                    );
                }
            }
        }

        let table_count = tables.len();
        {
            let mut guard = match self.tables.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = tables;
        }

        tracing::debug!(duration_ms = start.elapsed().as_millis(), database = %self.database, schema = %self.schema_name, tables = table_count, "Snowflake: schema table refresh complete");

        Ok(())
    }

    /// Lists table names in this schema using `INFORMATION_SCHEMA`.
    async fn list_tables(&self) -> Result<Vec<String>> {
        let start = Instant::now();
        // snowflake-api currently exposes only SQL-string execution APIs (`exec`, `exec_raw`,
        // `exec_streamed`) and does not expose bind-parameter APIs for metadata queries.
        // Build metadata SQL with explicit identifier/literal quoting and validation instead.
        let quoted_db = quote_sql_identifier(&self.database)?;
        let quoted_schema_literal = quote_sql_string_literal(&self.schema_name)?;
        let query = format!(
            "SELECT TABLE_NAME FROM {quoted_db}.INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = {quoted_schema_literal} \
             AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
             ORDER BY TABLE_NAME"
        );
        tracing::debug!(query = %query, "Snowflake: listing tables");

        let result = self.api.exec(&query).await.context(QueryFailedSnafu)?;

        match result {
            snowflake_api::QueryResult::Arrow(batches) => {
                let mut names = Vec::new();
                for batch in batches {
                    let col = batch.column_by_name("TABLE_NAME").context(
                        UnexpectedResponseSnafu {
                            reason: "Missing TABLE_NAME column in Arrow response for table listing",
                        },
                    )?;

                    let array = col.as_any().downcast_ref::<arrow::array::StringArray>().context(
                        UnexpectedResponseSnafu {
                            reason: "TABLE_NAME column has unexpected data type in Arrow response for table listing; expected Utf8",
                        },
                    )?;

                    for value in array.iter().flatten() {
                        names.push(value.to_string());
                    }
                }
                tracing::debug!(duration_ms = start.elapsed().as_millis(), schema = %self.schema_name, count = names.len(), "Snowflake: listed tables");
                Ok(names)
            }
            snowflake_api::QueryResult::Json(_) => UnexpectedResponseSnafu {
                reason: "Expected Arrow response for table listing, got JSON",
            }
            .fail(),
            snowflake_api::QueryResult::Empty => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl SchemaProvider for SnowflakeSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    use super::{SnowflakeTableCreator, quote_sql_identifier, quote_sql_string_literal};
    use crate::{Read, ReadWrite};
    use async_trait::async_trait;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::sql::TableReference;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    struct RecordingTableCreator {
        read_calls: AtomicUsize,
        read_write_calls: AtomicUsize,
    }

    fn empty_table_provider()
    -> std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?))
    }

    #[async_trait]
    impl Read for RecordingTableCreator {
        async fn table_provider(
            &self,
            _table_reference: TableReference,
        ) -> std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>
        {
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            empty_table_provider()
        }
    }

    #[async_trait]
    impl ReadWrite for RecordingTableCreator {
        async fn table_provider(
            &self,
            _table_reference: TableReference,
        ) -> std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>
        {
            self.read_write_calls.fetch_add(1, Ordering::Relaxed);
            empty_table_provider()
        }
    }

    #[test]
    fn test_quote_sql_identifier_no_quotes() {
        assert_eq!(
            quote_sql_identifier("schema_name").expect("identifier should be quoted"),
            "\"schema_name\""
        );
    }

    #[test]
    fn test_quote_sql_identifier_embedded_quote() {
        assert_eq!(
            quote_sql_identifier("schema\"name").expect("identifier should be quoted"),
            "\"schema\"\"name\""
        );
    }

    #[test]
    fn test_quote_sql_string_literal_no_quotes() {
        assert_eq!(
            quote_sql_string_literal("schema_name").expect("string literal should be quoted"),
            "'schema_name'"
        );
    }

    #[test]
    fn test_quote_sql_string_literal_single_quote() {
        assert_eq!(
            quote_sql_string_literal("schema'name").expect("string literal should be quoted"),
            "'schema''name'"
        );
    }

    #[test]
    fn test_quote_sql_string_literal_multiple_single_quotes() {
        assert_eq!(
            quote_sql_string_literal("o'brien'schema").expect("string literal should be quoted"),
            "'o''brien''schema'"
        );
    }

    #[test]
    fn test_quote_helpers_reject_nul_byte() {
        assert!(
            quote_sql_identifier("schema\0name").is_err(),
            "identifier containing NUL should be rejected"
        );
        assert!(
            quote_sql_string_literal("schema\0name").is_err(),
            "string literal containing NUL should be rejected"
        );
    }

    #[tokio::test]
    async fn table_creator_uses_read_provider_by_default() {
        let creator = Arc::new(RecordingTableCreator::default());
        let read_creator: Arc<dyn Read> = creator.clone();
        let table_creator = SnowflakeTableCreator::Read(read_creator);

        table_creator
            .table_provider(TableReference::bare("items"))
            .await
            .expect("table provider should be created");

        assert_eq!(creator.read_calls.load(Ordering::Relaxed), 1);
        assert_eq!(creator.read_write_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn table_creator_uses_read_write_provider_when_configured() {
        let creator = Arc::new(RecordingTableCreator::default());
        let read_write_creator: Arc<dyn ReadWrite> = creator.clone();
        let table_creator = SnowflakeTableCreator::ReadWrite(read_write_creator);

        table_creator
            .table_provider(TableReference::bare("items"))
            .await
            .expect("table provider should be created");

        assert_eq!(creator.read_calls.load(Ordering::Relaxed), 0);
        assert_eq!(creator.read_write_calls.load(Ordering::Relaxed), 1);
    }
}
