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

//! `MySQL` catalog provider implementation.
//!
//! Discovers schemas and tables in a `MySQL` server using
//! `information_schema` queries and provides them as `DataFusion` catalog/schema providers.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use globset::GlobSet;
use mysql_async::prelude::Queryable;
use snafu::prelude::*;

use crate::{
    COMMENT_METADATA_KEY, FieldMetadata, MetadataEnrichedTableProvider, Read,
    RefreshableCatalogProvider, SOURCE_TYPE_METADATA_KEY,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get connection from MySQL pool: {source}"))]
    ConnectionFailed { source: mysql_async::Error },

    #[snafu(display("Failed to execute MySQL query: {source}"))]
    QueryFailed { source: mysql_async::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System schemas to exclude from discovery.
const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "mysql", "performance_schema", "sys"];

#[derive(Debug, Clone, Default)]
struct TableComments {
    table_comment: Option<String>,
    column_comments: HashMap<String, String>,
    column_source_types: HashMap<String, String>,
}

type CommentMap = HashMap<String, TableComments>;

/// A catalog provider for `MySQL` that discovers schemas and tables
/// by querying `information_schema`.
pub struct MySQLCatalogProvider {
    pool: mysql_async::Pool,
    table_creator: Arc<dyn Read>,
    schemas: RwLock<HashMap<String, Arc<MySQLSchemaProvider>>>,
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for MySQLCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySQLCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl MySQLCatalogProvider {
    #[must_use]
    pub fn new(
        pool: mysql_async::Pool,
        table_creator: Arc<dyn Read>,
        include: Option<GlobSet>,
    ) -> Self {
        Self {
            pool,
            table_creator,
            schemas: RwLock::new(HashMap::new()),
            include: include.map(Arc::new),
        }
    }

    async fn refresh_schemas(&self) -> Result<()> {
        let schema_names = self.list_schemas().await?;

        let mut schemas = HashMap::new();
        for schema_name in schema_names {
            let schema_provider = MySQLSchemaProvider::new(
                self.pool.clone(),
                schema_name.clone(),
                Arc::clone(&self.table_creator),
                self.include.clone(),
            );
            schema_provider.refresh_tables().await?;
            schemas.insert(schema_name, Arc::new(schema_provider));
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

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let mut conn = self.pool.get_conn().await.context(ConnectionFailedSnafu)?;

        let rows: Vec<String> = conn
            .query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME")
            .await
            .context(QueryFailedSnafu)?;

        let names: Vec<String> = rows
            .into_iter()
            .filter(|name| !SYSTEM_SCHEMAS.contains(&name.as_str()))
            .collect();

        Ok(names)
    }
}

impl CatalogProvider for MySQLCatalogProvider {
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
impl RefreshableCatalogProvider for MySQLCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for `MySQL` that discovers tables within a schema.
pub struct MySQLSchemaProvider {
    pool: mysql_async::Pool,
    schema_name: String,
    table_creator: Arc<dyn Read>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for MySQLSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySQLSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

impl MySQLSchemaProvider {
    #[must_use]
    pub fn new(
        pool: mysql_async::Pool,
        schema_name: String,
        table_creator: Arc<dyn Read>,
        include: Option<Arc<GlobSet>>,
    ) -> Self {
        Self {
            pool,
            schema_name,
            table_creator,
            tables: RwLock::new(HashMap::new()),
            include,
        }
    }

    async fn refresh_tables(&self) -> Result<()> {
        let table_names = self.list_tables().await?;
        let comments = match self.list_comments().await {
            Ok(comments) => comments,
            Err(error) => {
                tracing::warn!(
                    schema = %self.schema_name,
                    error = %error,
                    "Failed to query MySQL comments for schema, continuing without comment metadata"
                );
                HashMap::new()
            }
        };

        let mut tables = HashMap::new();
        for table_name in table_names {
            let schema_with_table = format!("{}.{}", self.schema_name, table_name);
            if let Some(include) = &self.include
                && !include.is_match(&schema_with_table)
            {
                tracing::debug!("Table {schema_with_table} is not included, skipping");
                continue;
            }

            // Use backtick-quoted identifiers for MySQL.
            let escaped_schema = self.schema_name.replace('`', "``");
            let escaped_table = table_name.replace('`', "``");
            let quoted_path = format!("`{escaped_schema}`.`{escaped_table}`");
            let table_ref: TableReference = quoted_path.clone().into();

            match self.table_creator.table_provider(table_ref).await {
                Ok(provider) => {
                    tables.insert(
                        table_name.clone(),
                        provider_with_comments(provider, comments.get(&table_name)),
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        schema = %self.schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for MySQL table {quoted_path}, skipping"
                    );
                }
            }
        }

        {
            let mut guard = match self.tables.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = tables;
        }

        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let mut conn = self.pool.get_conn().await.context(ConnectionFailedSnafu)?;

        let rows: Vec<String> = conn
            .exec(
                "SELECT TABLE_NAME FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? \
                 AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
                 ORDER BY TABLE_NAME",
                (&self.schema_name,),
            )
            .await
            .context(QueryFailedSnafu)?;

        Ok(rows)
    }

    async fn list_comments(&self) -> Result<CommentMap> {
        let mut conn = self.pool.get_conn().await.context(ConnectionFailedSnafu)?;

        let rows: Vec<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = conn
            .exec(
                "SELECT \
                     t.TABLE_NAME, \
                     NULLIF(t.TABLE_COMMENT, '') AS TABLE_COMMENT, \
                     c.COLUMN_NAME, \
                     NULLIF(c.COLUMN_COMMENT, '') AS COLUMN_COMMENT, \
                     c.COLUMN_TYPE \
                 FROM information_schema.TABLES t \
                 LEFT JOIN information_schema.COLUMNS c \
                     ON c.TABLE_SCHEMA = t.TABLE_SCHEMA \
                     AND c.TABLE_NAME = t.TABLE_NAME \
                 WHERE t.TABLE_SCHEMA = ? \
                 AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
                 ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION",
                (&self.schema_name,),
            )
            .await
            .context(QueryFailedSnafu)?;

        let mut comments_by_table = HashMap::new();
        for (table_name, table_comment, column_name, column_comment, column_source_type) in rows {
            let comments: &mut TableComments = comments_by_table.entry(table_name).or_default();
            if comments.table_comment.is_none()
                && let Some(comment) = table_comment
            {
                comments.table_comment = Some(comment);
            }
            if let Some(column_name) = column_name {
                if let Some(comment) = column_comment {
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
}

fn provider_with_comments(
    provider: Arc<dyn TableProvider>,
    comments: Option<&TableComments>,
) -> Arc<dyn TableProvider> {
    let Some(comments) = comments else {
        return provider;
    };

    let mut table_metadata = HashMap::new();
    if let Some(comment) = &comments.table_comment {
        table_metadata.insert(COMMENT_METADATA_KEY.to_string(), comment.clone());
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
            .insert(COMMENT_METADATA_KEY.to_string(), comment.clone());
    }

    if table_metadata.is_empty() && field_metadata.is_empty() {
        provider
    } else {
        Arc::new(MetadataEnrichedTableProvider::new_with_field_metadata(
            provider,
            table_metadata,
            field_metadata,
        ))
    }
}

#[async_trait]
impl SchemaProvider for MySQLSchemaProvider {
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
