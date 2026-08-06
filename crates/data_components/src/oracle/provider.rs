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

//! Oracle catalog provider implementation.
//!
//! Discovers schemas (owners) and tables in an Oracle database using
//! `ALL_USERS` and `ALL_TABLES`/`ALL_VIEWS` queries, and provides them
//! as `DataFusion` catalog/schema providers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::catalog_filter::TableSelector;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use snafu::prelude::*;

use super::OracleTableProvider;
use super::connection::OracleConnectionPool;
use crate::RefreshableCatalogProvider;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get connection from Oracle pool: {source}"))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to execute Oracle query: {source}"))]
    QueryFailed { source: oracle::Error },

    #[snafu(display("Failed to create Oracle table provider: {source}"))]
    TableProviderFailed { source: super::Error },

    #[snafu(display("Oracle blocking task failed to complete: {source}"))]
    TaskJoin { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System schemas (owners) to exclude from discovery.
const SYSTEM_SCHEMAS: &[&str] = &[
    "SYS",
    "SYSTEM",
    "OUTLN",
    "DBSNMP",
    "ORACLE_OCM",
    "APPQOSSYS",
    "WMSYS",
    "EXFSYS",
    "CTXSYS",
    "ANONYMOUS",
    "XDB",
    "XS$NULL",
    "MDSYS",
    "OLAPSYS",
    "ORDSYS",
    "ORDDATA",
    "ORDPLUGINS",
    "SI_INFORMTN_SCHEMA",
    "DVSYS",
    "LBACSYS",
    "GSMADMIN_INTERNAL",
    "AUDSYS",
    "DBSFWUSER",
    "DIP",
    "GSMCATUSER",
    "GSMUSER",
    "SYSBACKUP",
    "SYSDG",
    "SYSKM",
    "SYSRAC",
    "SYS$UMF",
    "REMOTE_SCHEDULER_AGENT",
    "OJVMSYS",
];

/// A catalog provider for Oracle that discovers schemas (owners) and tables
/// by querying Oracle dictionary views.
pub struct OracleCatalogProvider {
    pool: Arc<OracleConnectionPool>,
    schemas: RwLock<HashMap<String, Arc<OracleSchemaProvider>>>,
    selector: TableSelector,
}

impl std::fmt::Debug for OracleCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl OracleCatalogProvider {
    #[must_use]
    pub fn new(pool: Arc<OracleConnectionPool>, selector: TableSelector) -> Self {
        Self {
            pool,
            schemas: RwLock::new(HashMap::new()),
            selector,
        }
    }

    async fn refresh_schemas(&self) -> Result<()> {
        let schema_names = self.list_schemas().await?;

        let mut schemas = HashMap::new();
        for schema_name in schema_names {
            let schema_provider = OracleSchemaProvider::new(
                Arc::clone(&self.pool),
                schema_name.clone(),
                self.selector.clone(),
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
        // `rust-oracle` is a synchronous OCI driver; the broker round-trip and
        // lazy row fetches block the calling thread. Run them on the blocking
        // pool (with an owned `'static` connection) so catalog refresh doesn't
        // stall the async runtime thread.
        let pooled = self
            .pool
            .get_owned()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ConnectionFailedSnafu)?;

        let names = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let rows = pooled
                .query("SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME", &[])
                .context(QueryFailedSnafu)?;

            let mut names = Vec::new();
            for row_result in rows {
                let row = row_result.context(QueryFailedSnafu)?;
                let name: String = row.get(0).context(QueryFailedSnafu)?;
                if !SYSTEM_SCHEMAS.contains(&name.as_str()) {
                    names.push(name);
                }
            }

            Ok(names)
        })
        .await
        .context(TaskJoinSnafu)??;

        Ok(names)
    }
}

impl CatalogProvider for OracleCatalogProvider {
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
impl RefreshableCatalogProvider for OracleCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for Oracle that discovers tables within a schema (owner).
pub struct OracleSchemaProvider {
    pool: Arc<OracleConnectionPool>,
    schema_name: String,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    selector: TableSelector,
}

impl std::fmt::Debug for OracleSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

impl OracleSchemaProvider {
    #[must_use]
    pub fn new(
        pool: Arc<OracleConnectionPool>,
        schema_name: String,
        selector: TableSelector,
    ) -> Self {
        Self {
            pool,
            schema_name,
            tables: RwLock::new(HashMap::new()),
            selector,
        }
    }

    async fn refresh_tables(&self) -> Result<()> {
        let table_names = self.list_tables_from_db().await?;

        let mut tables = HashMap::new();
        for table_name in table_names {
            let schema_with_table = format!("{}.{}", self.schema_name, table_name);
            if !self.selector.selects_table(&self.schema_name, &table_name) {
                tracing::debug!(
                    "Table {schema_with_table} is not selected by the catalog's include/exclude patterns, skipping"
                );
                continue;
            }

            let table_ref = TableReference::partial(self.schema_name.clone(), table_name.clone());

            match OracleTableProvider::new(Arc::clone(&self.pool), &table_ref).await {
                Ok(provider) => {
                    tables.insert(table_name, Arc::new(provider) as Arc<dyn TableProvider>);
                }
                Err(e) => {
                    tracing::warn!(
                        schema = %self.schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for Oracle table {schema_with_table}, skipping"
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

    async fn list_tables_from_db(&self) -> Result<Vec<String>> {
        // `rust-oracle` is a synchronous OCI driver; the broker round-trip and
        // lazy row fetches block the calling thread. Run them on the blocking
        // pool (with an owned `'static` connection) so catalog refresh doesn't
        // stall the async runtime thread.
        let pooled = self
            .pool
            .get_owned()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ConnectionFailedSnafu)?;

        // Own the bind param before moving into the blocking closure.
        let schema_name = self.schema_name.clone();

        let names = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            // Query both tables and views from the schema
            let query = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1 \
                         UNION \
                         SELECT VIEW_NAME AS TABLE_NAME FROM ALL_VIEWS WHERE OWNER = :1 \
                         ORDER BY TABLE_NAME";

            let params: Vec<&dyn oracle::sql_type::ToSql> = vec![&schema_name];

            let rows = pooled.query(query, &params).context(QueryFailedSnafu)?;

            let mut names = Vec::new();
            for row_result in rows {
                let row = row_result.context(QueryFailedSnafu)?;
                let name: String = row.get(0).context(QueryFailedSnafu)?;
                names.push(name);
            }

            Ok(names)
        })
        .await
        .context(TaskJoinSnafu)??;

        Ok(names)
    }
}

#[async_trait]
impl SchemaProvider for OracleSchemaProvider {
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
