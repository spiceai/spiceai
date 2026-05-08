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

//! `DuckLake` catalog provider implementation.
//!
//! Connects to a `DuckLake` catalog using a dedicated `DuckDB` instance with the `ducklake` extension
//! and provides schema/table discovery by querying the attached `DuckLake` database.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use globset::GlobSet;
use snafu::prelude::*;

use crate::RefreshableCatalogProvider;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to execute DuckDB query: {source}"))]
    QueryFailed { source: duckdb::Error },

    #[snafu(display("Failed to get DuckDB connection: {source}"))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "DDL operations are not allowed on this catalog (access mode does not include create)"
    ))]
    DdlNotAllowed,

    #[snafu(display("Table '{table_name}' already exists in schema '{schema_name}'"))]
    TableAlreadyExists {
        table_name: String,
        schema_name: String,
    },

    #[snafu(display("Cannot drop non-empty schema '{schema_name}' without cascade"))]
    SchemaNotEmpty { schema_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A catalog provider for `DuckLake` that discovers schemas and tables
/// using a dedicated `DuckDB` instance with the `ducklake` extension.
pub struct DuckLakeCatalogProvider {
    /// `DuckDB` connection pool with `ducklake` extension loaded and catalog attached
    pool: Arc<DuckDbConnectionPool>,
    /// `DuckDB` table factory for creating table providers (uses the same pool)
    duckdb_factory: Arc<DuckDBTableFactory>,
    /// The catalog name as attached in `DuckDB`
    catalog_name: String,
    /// Cached schemas (`schema_name` -> `SchemaProvider`)
    schemas: RwLock<HashMap<String, Arc<DuckLakeSchemaProvider>>>,
    /// Whether write operations are allowed
    writable: bool,
    /// Whether DDL operations are allowed
    ddl_enabled: bool,
    /// Optional glob filter for table inclusion (`schema.table` format)
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for DuckLakeCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeCatalogProvider")
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

impl DuckLakeCatalogProvider {
    /// Creates a new `DuckLakeCatalogProvider` with the given `DuckDB` pool.
    ///
    /// The pool should already have the `ducklake` extension loaded and the catalog attached.
    ///
    /// # Arguments
    /// * `pool` - `DuckDB` connection pool with `ducklake` extension loaded
    /// * `catalog_name` - The catalog name as attached in `DuckDB`
    /// * `writable` - Whether write operations (INSERT, UPDATE, DELETE) are allowed
    /// * `ddl_enabled` - Whether DDL operations (CREATE TABLE, DROP TABLE) are allowed
    /// * `include` - Optional glob filter for table inclusion (`schema.table` format)
    #[must_use]
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        catalog_name: String,
        writable: bool,
        ddl_enabled: bool,
        include: Option<GlobSet>,
    ) -> Self {
        // Create a table factory that uses the same pool (with ducklake already attached)
        let duckdb_factory = Arc::new(DuckDBTableFactory::new(Arc::clone(&pool)));
        Self {
            pool,
            duckdb_factory,
            catalog_name,
            schemas: RwLock::new(HashMap::new()),
            writable,
            ddl_enabled,
            include: include.map(Arc::new),
        }
    }

    /// Returns whether write operations are allowed.
    #[must_use]
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Returns whether DDL operations are allowed.
    #[must_use]
    pub fn is_ddl_enabled(&self) -> bool {
        self.ddl_enabled
    }

    /// Refreshes the catalog by querying the attached `DuckLake` database for schemas and tables.
    async fn refresh_schemas(&self) -> Result<()> {
        let pool = Arc::clone(&self.pool);
        let catalog_name = self.catalog_name.clone();

        // Query schemas from the attached `DuckLake` catalog using `information_schema`
        let schema_names = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let conn = pool
                .connect_sync()
                .map_err(|e| Error::ConnectionFailed { source: e })?;

            let duckdb_wrapper = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .ok_or_else(|| Error::ConnectionFailed {
                    source: "Failed to downcast to DuckDbConnection during schema refresh".into(),
                })?;

            // Query the global information_schema, filtering by catalog name.
            // DuckLake catalogs don't expose information_schema under their own catalog prefix.
            let sql = r"SELECT DISTINCT schema_name
                     FROM information_schema.schemata
                   WHERE catalog_name = ?
                   ORDER BY schema_name";

            let mut stmt = duckdb_wrapper.conn.prepare(sql).context(QueryFailedSnafu)?;
            let rows = stmt
                .query_map([&catalog_name], |row| row.get::<_, String>(0))
                .context(QueryFailedSnafu)?;

            let mut names = Vec::new();
            for row_result in rows {
                let name: String = row_result.context(QueryFailedSnafu)?;
                if name != "information_schema" && name != "pg_catalog" {
                    names.push(name);
                }
            }
            Ok(names)
        })
        .await
        .map_err(|e| Error::ConnectionFailed {
            source: Box::new(e),
        })??;

        let mut schemas = HashMap::new();
        for schema_name in schema_names {
            let schema_provider = DuckLakeSchemaProvider::new(
                Arc::clone(&self.pool),
                Arc::clone(&self.duckdb_factory),
                self.catalog_name.clone(),
                schema_name.clone(),
                self.writable,
                self.ddl_enabled,
                self.include.clone(),
            );
            schema_provider.refresh().await?;
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
}

impl CatalogProvider for DuckLakeCatalogProvider {
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

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        if !self.ddl_enabled {
            return Err(datafusion::error::DataFusionError::Execution(
                Error::DdlNotAllowed {}.to_string(),
            ));
        }

        // Execute CREATE SCHEMA in DuckDB
        let pool = Arc::clone(&self.pool);
        let catalog_name = self.catalog_name.clone();
        let schema_name = name.to_string();

        let conn = pool
            .connect_sync()
            .map_err(datafusion::error::DataFusionError::External)?;

        let duckdb_wrapper = conn
            .as_any()
            .downcast_ref::<DuckDbConnection>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Failed to downcast DuckLake connection to DuckDbConnection when registering schema"
                        .to_string(),
                )
            })?;

        let escaped_catalog_name = catalog_name.replace('"', "\"\"");
        let escaped_schema_name = schema_name.replace('"', "\"\"");
        let sql = format!(
            r#"CREATE SCHEMA IF NOT EXISTS "{escaped_catalog_name}"."{escaped_schema_name}""#
        );
        duckdb_wrapper
            .conn
            .execute(&sql, [])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Downcast to our schema type if possible, otherwise create a new one
        let schema_provider = if let Some(ducklake_schema) =
            schema.as_any().downcast_ref::<DuckLakeSchemaProvider>()
        {
            Arc::new(DuckLakeSchemaProvider::new(
                Arc::clone(&ducklake_schema.pool),
                Arc::clone(&ducklake_schema.duckdb_factory),
                ducklake_schema.catalog_name.clone(),
                schema_name,
                ducklake_schema.writable,
                ducklake_schema.ddl_enabled,
                ducklake_schema.include.clone(),
            ))
        } else {
            Arc::new(DuckLakeSchemaProvider::new(
                Arc::clone(&self.pool),
                Arc::clone(&self.duckdb_factory),
                self.catalog_name.clone(),
                schema_name,
                self.writable,
                self.ddl_enabled,
                self.include.clone(),
            ))
        };

        let mut guard = match self.schemas.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };

        let previous = guard
            .insert(name.to_string(), schema_provider)
            .map(|s| s as Arc<dyn SchemaProvider>);
        Ok(previous)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        if !self.ddl_enabled {
            return Err(datafusion::error::DataFusionError::Execution(
                Error::DdlNotAllowed {}.to_string(),
            ));
        }

        // Check if schema exists
        let existing = {
            let guard = match self.schemas.read() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            guard.get(name).cloned()
        };

        let Some(schema) = existing else {
            return Ok(None);
        };

        // Check if schema is empty (if not cascading)
        if !cascade && !schema.table_names().is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                Error::SchemaNotEmpty {
                    schema_name: name.to_string(),
                }
                .to_string(),
            ));
        }

        // Execute DROP SCHEMA in DuckDB
        let pool = Arc::clone(&self.pool);
        let catalog_name = self.catalog_name.clone();
        let schema_name = name.to_string();

        let conn = pool
            .connect_sync()
            .map_err(datafusion::error::DataFusionError::External)?;

        let duckdb_wrapper = conn
            .as_any()
            .downcast_ref::<DuckDbConnection>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Failed to downcast DuckLake connection to DuckDbConnection when deregistering schema"
                        .to_string(),
                )
            })?;

        let escaped_catalog_name = catalog_name.replace('"', "\"\"");
        let escaped_schema_name = schema_name.replace('"', "\"\"");
        let mut sql =
            format!(r#"DROP SCHEMA IF EXISTS "{escaped_catalog_name}"."{escaped_schema_name}""#);
        if cascade {
            sql.push_str(" CASCADE");
        }
        duckdb_wrapper
            .conn
            .execute(&sql, [])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Remove from cache
        let mut guard = match self.schemas.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        Ok(guard.remove(name).map(|s| s as Arc<dyn SchemaProvider>))
    }
}

#[async_trait]
impl RefreshableCatalogProvider for DuckLakeCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

/// A schema provider for `DuckLake` that discovers tables within a schema.
pub struct DuckLakeSchemaProvider {
    /// `DuckDB` connection pool for querying catalog metadata
    pool: Arc<DuckDbConnectionPool>,
    /// `DuckDB` table factory for creating table providers
    duckdb_factory: Arc<DuckDBTableFactory>,
    /// The catalog name as attached in `DuckDB`
    catalog_name: String,
    /// Schema name
    schema_name: String,
    /// Cached tables (`table_name` -> `TableProvider`)
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    /// Whether write operations are allowed
    writable: bool,
    /// Whether DDL operations are allowed
    ddl_enabled: bool,
    /// Optional glob filter for table inclusion (`schema.table` format)
    include: Option<Arc<GlobSet>>,
}

impl std::fmt::Debug for DuckLakeSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeSchemaProvider")
            .field("catalog_name", &self.catalog_name)
            .field("schema_name", &self.schema_name)
            .field("writable", &self.writable)
            .field("ddl_enabled", &self.ddl_enabled)
            .finish_non_exhaustive()
    }
}

impl DuckLakeSchemaProvider {
    /// Creates a new `DuckLakeSchemaProvider` for the given schema.
    ///
    /// # Arguments
    /// * `pool` - `DuckDB` connection pool
    /// * `duckdb_factory` - `DuckDB` table factory for creating table providers
    /// * `catalog_name` - The catalog name as attached in `DuckDB`
    /// * `schema_name` - The schema name
    /// * `writable` - Whether write operations (INSERT, UPDATE, DELETE) are allowed
    /// * `ddl_enabled` - Whether DDL operations (CREATE TABLE, DROP TABLE) are allowed
    /// * `include` - Optional glob filter for table inclusion (`schema.table` format)
    #[must_use]
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        duckdb_factory: Arc<DuckDBTableFactory>,
        catalog_name: String,
        schema_name: String,
        writable: bool,
        ddl_enabled: bool,
        include: Option<Arc<GlobSet>>,
    ) -> Self {
        Self {
            pool,
            duckdb_factory,
            catalog_name,
            schema_name,
            tables: RwLock::new(HashMap::new()),
            writable,
            ddl_enabled,
            include,
        }
    }

    /// Refreshes the schema by querying for tables.
    async fn refresh(&self) -> Result<()> {
        let pool = Arc::clone(&self.pool);
        let catalog_name = self.catalog_name.clone();
        let schema_name = self.schema_name.clone();

        // Query tables from the attached `DuckLake` catalog using `information_schema`
        let table_names = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let conn = pool
                .connect_sync()
                .map_err(|e| Error::ConnectionFailed { source: e })?;

            let duckdb_wrapper = conn
                .as_any()
                .downcast_ref::<DuckDbConnection>()
                .ok_or_else(|| Error::ConnectionFailed {
                    source: "Failed to downcast to DuckDbConnection during table refresh".into(),
                })?;

            // Query the global information_schema, filtering by catalog and schema name.
            // DuckLake catalogs don't expose information_schema under their own catalog prefix.
            let sql = r"SELECT DISTINCT table_name
                     FROM information_schema.tables
                   WHERE table_catalog = ?
                     AND table_schema = ?
                   ORDER BY table_name";

            let mut stmt = duckdb_wrapper.conn.prepare(sql).context(QueryFailedSnafu)?;
            let rows = stmt
                .query_map([&catalog_name, &schema_name], |row| row.get::<_, String>(0))
                .context(QueryFailedSnafu)?;

            let mut names = Vec::new();
            for row_result in rows {
                let name = row_result.context(QueryFailedSnafu)?;
                names.push(name);
            }
            Ok(names)
        })
        .await
        .map_err(|e| Error::ConnectionFailed {
            source: Box::new(e),
        })??;

        let mut tables = HashMap::new();
        for table_name in table_names {
            let schema_with_table = format!("{}.{}", self.schema_name, table_name);
            if let Some(include) = &self.include
                && !include.is_match(&schema_with_table)
            {
                tracing::debug!("Table {schema_with_table} is not included, skipping");
                continue;
            }

            // Create a fully qualified table reference for the DuckLake table
            let table_ref = TableReference::full(
                self.catalog_name.clone(),
                self.schema_name.clone(),
                table_name.clone(),
            );

            // Use the DuckDB table factory (which shares the same pool with ducklake attached)
            match self.duckdb_factory.table_provider(table_ref).await {
                Ok(provider) => {
                    tables.insert(table_name, provider);
                }
                Err(e) => {
                    tracing::warn!(
                        catalog = %self.catalog_name,
                        schema = %self.schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for DuckLake table, skipping"
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
}

#[async_trait]
impl SchemaProvider for DuckLakeSchemaProvider {
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

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !self.ddl_enabled {
            return Err(datafusion::error::DataFusionError::Execution(
                Error::DdlNotAllowed {}.to_string(),
            ));
        }

        // Check if table already exists
        if self.table_exist(&name) {
            return Err(datafusion::error::DataFusionError::Execution(
                Error::TableAlreadyExists {
                    table_name: name,
                    schema_name: self.schema_name.clone(),
                }
                .to_string(),
            ));
        }

        // Note: For DuckLake, actual table creation happens through SQL DDL
        // (CREATE TABLE statement). This method primarily registers the table
        // in the local cache. The caller should have already executed the DDL
        // to create the table in DuckLake.

        let mut guard = match self.tables.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };

        // Insert and return previous (should be None since we checked above)
        Ok(guard.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !self.ddl_enabled {
            return Err(datafusion::error::DataFusionError::Execution(
                Error::DdlNotAllowed {}.to_string(),
            ));
        }

        // Execute DROP TABLE in DuckDB
        let pool = Arc::clone(&self.pool);
        let catalog_name = self.catalog_name.clone();
        let schema_name = self.schema_name.clone();
        let table_name = name.to_string();

        let conn = pool
            .connect_sync()
            .map_err(datafusion::error::DataFusionError::External)?;

        let duckdb_wrapper = conn
            .as_any()
            .downcast_ref::<DuckDbConnection>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Failed to downcast DuckLake connection to DuckDbConnection when deregistering table"
                        .to_string(),
                )
            })?;

        let escaped_catalog_name = catalog_name.replace('"', "\"\"");
        let escaped_schema_name = schema_name.replace('"', "\"\"");
        let escaped_table_name = table_name.replace('"', "\"\"");
        let sql = format!(
            r#"DROP TABLE IF EXISTS "{escaped_catalog_name}"."{escaped_schema_name}"."{escaped_table_name}""#
        );
        duckdb_wrapper
            .conn
            .execute(&sql, [])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Remove from cache
        let mut guard = match self.tables.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        Ok(guard.remove(name))
    }
}
