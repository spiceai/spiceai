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

//! PostgreSQL catalog provider implementation.
//!
//! Discovers schemas and tables in a PostgreSQL database using
//! `information_schema` queries and provides them as DataFusion catalog/schema providers.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use globset::GlobSet;
use snafu::prelude::*;

use crate::{Read, RefreshableCatalogProvider};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get connection from PostgreSQL pool: {source}"))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to execute PostgreSQL query: {source}"))]
    QueryFailed { source: tokio_postgres::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System schemas to exclude from discovery.
const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "pg_toast"];

/// A catalog provider for PostgreSQL that discovers schemas and tables
/// by querying `information_schema`.
pub struct PostgresCatalogProvider {
    pool: Arc<PostgresConnectionPool>,
    table_creator: Arc<dyn Read>,
    schemas: RwLock<HashMap<String, Arc<PostgresSchemaProvider>>>,
    include: Option<Arc<GlobSet>>,
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
        pool: Arc<PostgresConnectionPool>,
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
            let schema_provider = PostgresSchemaProvider::new(
                Arc::clone(&self.pool),
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
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        let rows = conn
            .conn
            .query(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
                &[],
            )
            .await
            .context(QueryFailedSnafu)?;

        let names: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                if SYSTEM_SCHEMAS.contains(&name.as_str()) || name.starts_with("pg_temp") {
                    None
                } else {
                    Some(name)
                }
            })
            .collect();

        Ok(names)
    }
}

impl CatalogProvider for PostgresCatalogProvider {
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
impl RefreshableCatalogProvider for PostgresCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for PostgreSQL that discovers tables within a schema.
pub struct PostgresSchemaProvider {
    pool: Arc<PostgresConnectionPool>,
    schema_name: String,
    table_creator: Arc<dyn Read>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    include: Option<Arc<GlobSet>>,
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

        let mut tables = HashMap::new();
        for table_name in table_names {
            let schema_with_table = format!("{}.{}", self.schema_name, table_name);
            if let Some(include) = &self.include
                && !include.is_match(&schema_with_table)
            {
                tracing::debug!("Table {schema_with_table} is not included, skipping");
                continue;
            }

            let table_ref = TableReference::partial(self.schema_name.clone(), table_name.clone());

            match self.table_creator.table_provider(table_ref).await {
                Ok(provider) => {
                    tables.insert(table_name, provider);
                }
                Err(e) => {
                    tracing::warn!(
                        schema = %self.schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for PostgreSQL table {schema_with_table}, skipping"
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
        let conn = self
            .pool
            .connect_direct()
            .await
            .context(ConnectionFailedSnafu)?;

        let rows = conn
            .conn
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 \
                 AND table_type IN ('BASE TABLE', 'VIEW') \
                 ORDER BY table_name",
                &[&self.schema_name],
            )
            .await
            .context(QueryFailedSnafu)?;

        let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(names)
    }
}

#[async_trait]
impl SchemaProvider for PostgresSchemaProvider {
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
