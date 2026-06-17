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

//! MSSQL catalog provider implementation.
//!
//! Discovers schemas and tables in a SQL Server database using
//! `INFORMATION_SCHEMA` queries and provides them as `DataFusion` catalog/schema providers.

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use futures::StreamExt;
use globset::GlobSet;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::SqlServerTableProvider;
use super::connection_manager::SqlServerConnectionPool;
use crate::RefreshableCatalogProvider;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get connection from MSSQL pool: {source}"))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to execute MSSQL query: {source}"))]
    QueryFailed { source: tiberius::error::Error },

    #[snafu(display("Failed to create MSSQL table provider: {source}"))]
    TableProviderFailed { source: super::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System schemas to exclude from discovery.
const SYSTEM_SCHEMAS: &[&str] = &["INFORMATION_SCHEMA", "sys", "guest"];

/// A catalog provider for MSSQL that discovers schemas and tables
/// by querying `INFORMATION_SCHEMA`.
pub struct MssqlCatalogProvider {
    pool: Arc<SqlServerConnectionPool>,
    schemas: RwLock<HashMap<String, Arc<MssqlSchemaProvider>>>,
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for MssqlCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl MssqlCatalogProvider {
    #[must_use]
    pub fn new(pool: Arc<SqlServerConnectionPool>, include: Option<GlobSet>) -> Self {
        Self {
            pool,
            schemas: RwLock::new(HashMap::new()),
            include: include.map(Arc::new),
        }
    }

    async fn refresh_schemas(&self) -> Result<()> {
        let schema_names = self.list_schemas().await?;

        let mut schemas = HashMap::new();
        for schema_name in schema_names {
            let schema_provider = MssqlSchemaProvider::new(
                Arc::clone(&self.pool),
                schema_name.clone(),
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
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ConnectionFailedSnafu)?;

        let mut stream = conn
            .simple_query(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME",
            )
            .await
            .context(QueryFailedSnafu)?
            .into_row_stream();

        let mut names = Vec::new();
        while let Some(row_result) = stream.next().await {
            let row = row_result.context(QueryFailedSnafu)?;
            if let Some(name) = row.get::<&str, _>(0)
                && !SYSTEM_SCHEMAS.contains(&name)
                && !name.starts_with("db_")
            {
                names.push(name.to_string());
            }
        }

        Ok(names)
    }
}

impl CatalogProvider for MssqlCatalogProvider {
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
impl RefreshableCatalogProvider for MssqlCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for MSSQL that discovers tables within a schema.
pub struct MssqlSchemaProvider {
    pool: Arc<SqlServerConnectionPool>,
    schema_name: String,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for MssqlSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

impl MssqlSchemaProvider {
    #[must_use]
    pub fn new(
        pool: Arc<SqlServerConnectionPool>,
        schema_name: String,
        include: Option<Arc<GlobSet>>,
    ) -> Self {
        Self {
            pool,
            schema_name,
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

            match SqlServerTableProvider::new(Arc::clone(&self.pool), &table_ref).await {
                Ok(provider) => {
                    tables.insert(table_name, Arc::new(provider) as Arc<dyn TableProvider>);
                }
                Err(e) => {
                    tracing::warn!(
                        schema = %self.schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for MSSQL table {schema_with_table}, skipping"
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
        let query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = @P1 \
             AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
             ORDER BY TABLE_NAME";

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ConnectionFailedSnafu)?;

        let mut stream = conn
            .query(query, &[&self.schema_name.as_str()])
            .await
            .context(QueryFailedSnafu)?
            .into_row_stream();

        let mut names = Vec::new();
        while let Some(row_result) = stream.next().await {
            let row = row_result.context(QueryFailedSnafu)?;
            if let Some(name) = row.get::<&str, _>(0) {
                names.push(name.to_string());
            }
        }

        Ok(names)
    }
}

#[async_trait]
impl SchemaProvider for MssqlSchemaProvider {
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
