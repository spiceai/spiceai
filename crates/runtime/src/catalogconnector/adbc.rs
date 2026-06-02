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

//! ADBC catalog connector.
//!
//! Connects to a database via ADBC (Arrow Database Connectivity)
//! and provides schema/table discovery using the ADBC metadata API.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::dataconnector::adbc::{
    build_db_options, build_join_context, dialect_for_driver, enrich_with_bigquery_metadata,
};
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use adbc_core::options::AdbcVersion;
use adbc_core::{Driver as _, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use datafusion_table_providers::adbc::AdbcTableFactory;
use datafusion_table_providers::sql::db_connection_pool::JoinPushDown;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::{
    ADBCPool, AdbcConnectionPoolBuilder,
};
use futures::stream::{self, StreamExt};
use globset::GlobSet;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

pub const PREFIX: &str = "adbc";

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("driver")
        .description("The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres', 'snowflake')")
        .required(),
    ParameterSpec::component("driver_path").description("Optional path to the ADBC driver library"),
    ParameterSpec::component("uri")
        .description("Database URI/connection string for the ADBC driver")
        .required(),
    ParameterSpec::component("username")
        .description("Username for database authentication")
        .secret(),
    ParameterSpec::component("password")
        .description("Password for database authentication")
        .secret(),
    ParameterSpec::component("driver_options").description(
        "Semicolon-delimited driver-specific database options (e.g., 'key1=value1;key2=value2')",
    ),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections in the connection pool.")
        .default("5"),
    ParameterSpec::runtime("connection_pool_min_idle")
        .description("The minimum number of idle connections to keep open in the pool.")
        .default("1"),
];

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: driver"))]
    MissingDriver,

    #[snafu(display("Missing required parameter: uri"))]
    MissingUri,

    #[snafu(display(
        "Invalid value for parameter '{name}': expected a positive integer, got '{value}'"
    ))]
    InvalidPoolParameter { name: String, value: String },

    #[snafu(display("Failed to load ADBC driver '{driver_location}': {source}"))]
    UnableToLoadDriver {
        driver_location: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display(
        "Failed to create ADBC database (driver='{driver_location}', uri='{uri}'): {source}"
    ))]
    UnableToCreateDatabase {
        driver_location: String,
        uri: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display(
        "Failed to create ADBC connection pool (driver='{driver_location}', uri='{uri}'): {source}"
    ))]
    UnableToCreateConnectionPool {
        driver_location: String,
        uri: String,
        source: datafusion_table_providers::sql::db_connection_pool::Error,
    },

    #[snafu(display(
        "In-memory database URIs (for example ':memory:') are not supported for catalog connectors because each pooled connection would create its own isolated in-memory database, leading to incorrect or inconsistent query results; use a file-based or network URI instead"
    ))]
    InMemoryUriNotSupported,

    #[snafu(display("Failed to create ADBC connection pool: {source}"))]
    PoolCreationTaskFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to list schemas: {source}"))]
    UnableToListSchemas {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to list tables in schema '{schema}': {source}"))]
    UnableToListTables {
        schema: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A catalog connector for ADBC, providing access to schemas and tables
/// in any ADBC-compatible database.
#[derive(Clone)]
pub struct AdbcCatalog {
    params: ConnectorParams,
}

impl AdbcCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for AdbcCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let (driver_name, pool) = create_pool(&self.params).await.map_err(|e| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            }
        })?;

        let table_factory = AdbcTableFactory::new(Arc::clone(&pool));

        let provider = Arc::new(AdbcCatalogProvider::new(
            pool,
            table_factory,
            catalog.include.clone(),
            driver_name,
        ));

        provider
            .refresh()
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component,
                source: e,
            })?;

        Ok(provider as Arc<dyn RefreshableCatalogProvider>)
    }
}

/// Maximum number of concurrent ADBC table provider creation tasks during catalog discovery.
const CATALOG_DISCOVERY_CONCURRENCY: usize = 10;

/// Creates an ADBC connection pool from connector parameters.
async fn create_pool(params: &ConnectorParams) -> Result<(String, Arc<ADBCPool<ManagedDatabase>>)> {
    let driver_name = params
        .parameters
        .get("driver")
        .expose()
        .ok()
        .context(MissingDriverSnafu)?;

    let driver_path = params.parameters.get("driver_path").expose().ok();
    let driver_location = driver_path.unwrap_or(driver_name).to_string();

    let uri = params
        .parameters
        .get("uri")
        .expose()
        .ok()
        .context(MissingUriSnafu)?;

    let uri_str = uri.to_string();

    let username = params.parameters.get("username").expose().ok();
    let password = params.parameters.get("password").expose().ok();
    let driver_options = params.parameters.get("driver_options").expose().ok();
    let db_options = build_db_options(&uri_str, username, password, driver_options);

    let join_context = build_join_context(&uri_str, username, None, None);

    let parse_pool_param = |name: &str| -> Result<Option<u32>> {
        match params.parameters.get(name).expose().ok() {
            Some(v) => {
                let parsed = v.parse::<u32>().map_err(|_| Error::InvalidPoolParameter {
                    name: name.to_string(),
                    value: v.to_string(),
                })?;
                if parsed == 0 {
                    return Err(Error::InvalidPoolParameter {
                        name: name.to_string(),
                        value: v.to_string(),
                    });
                }
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    };

    let pool_size = parse_pool_param("connection_pool_size")?;
    let pool_min_idle = parse_pool_param("connection_pool_min_idle")?;
    let driver_name = driver_name.to_string();

    if uri_str == ":memory:" || uri_str.contains("mode=memory") {
        return Err(Error::InMemoryUriNotSupported);
    }

    // Driver loading, database creation, and pool creation are all
    // synchronous FFI/IO operations — offload to a blocking thread.
    tokio::task::spawn_blocking(move || -> Result<(String, Arc<ADBCPool<_>>)> {
        let mut driver = ManagedDriver::load_from_name(
            &driver_location,
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .context(UnableToLoadDriverSnafu {
            driver_location: driver_location.clone(),
        })?;

        let db =
            driver
                .new_database_with_opts(db_options)
                .context(UnableToCreateDatabaseSnafu {
                    driver_location: driver_location.clone(),
                    uri: uri_str.clone(),
                })?;

        let pool = AdbcConnectionPoolBuilder::new(db)
            .with_max_size(pool_size)
            .with_min_idle(pool_min_idle)
            .with_join_push_down(JoinPushDown::AllowedFor(join_context))
            .build()
            .context(UnableToCreateConnectionPoolSnafu {
                driver_location,
                uri: uri_str,
            })?;

        Ok((driver_name, Arc::new(pool)))
    })
    .await
    .map_err(|e| Error::PoolCreationTaskFailed {
        source: Box::new(e),
    })?
}

// -- Catalog Provider --

/// A catalog provider for ADBC that discovers schemas and tables
/// using the ADBC metadata API (`get_objects`).
struct AdbcCatalogProvider {
    pool: Arc<ADBCPool<ManagedDatabase>>,
    table_factory: AdbcTableFactory<ManagedDatabase>,
    schemas: RwLock<HashMap<String, Arc<AdbcSchemaProvider>>>,
    include: Option<Arc<GlobSet>>,
    driver_name: String,
}

impl std::fmt::Debug for AdbcCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcCatalogProvider")
            .finish_non_exhaustive()
    }
}

impl AdbcCatalogProvider {
    fn new(
        pool: Arc<ADBCPool<ManagedDatabase>>,
        table_factory: AdbcTableFactory<ManagedDatabase>,
        include: Option<GlobSet>,
        driver_name: String,
    ) -> Self {
        Self {
            pool,
            table_factory,
            schemas: RwLock::new(HashMap::new()),
            include: include.map(Arc::new),
            driver_name,
        }
    }

    async fn refresh_schemas(&self) -> Result<()> {
        let refresh_start = Instant::now();
        // Phase 1: Discover schemas and tables via ADBC metadata API.
        // These are synchronous FFI calls — offload to a blocking thread
        // to avoid stalling the async runtime.
        let pool = Arc::clone(&self.pool);
        let discovery_start = Instant::now();
        let schema_tables =
            tokio::task::spawn_blocking(move || -> Result<Vec<(String, Vec<String>)>> {
                let conn = pool
                    .connect_sync()
                    .map_err(|e| Error::UnableToListSchemas { source: e })?;

                let sync_conn = conn.as_sync().ok_or_else(|| Error::UnableToListSchemas {
                    source: "ADBC connection does not support synchronous operations".into(),
                })?;

                let schema_names = sync_conn
                    .schemas()
                    .map_err(|e| Error::UnableToListSchemas {
                        source: Box::new(e),
                    })?;

                let mut schema_tables = Vec::with_capacity(schema_names.len());
                for schema_name in schema_names {
                    let table_names =
                        sync_conn
                            .tables(&schema_name)
                            .map_err(|e| Error::UnableToListTables {
                                schema: schema_name.clone(),
                                source: Box::new(e),
                            })?;
                    schema_tables.push((schema_name, table_names));
                }

                Ok(schema_tables)
            })
            .await
            .map_err(|e| Error::UnableToListSchemas {
                source: Box::new(e),
            })??;

        let schema_count = schema_tables.len();
        let table_count: usize = schema_tables.iter().map(|(_, t)| t.len()).sum();
        tracing::info!(
            duration_ms = discovery_start.elapsed().as_millis(),
            schemas = schema_count,
            tables = table_count,
            "ADBC: metadata discovery complete"
        );

        // Phase 2: Create table providers concurrently.
        let mut schemas = HashMap::new();
        for (schema_name, table_names) in schema_tables {
            let tables = self.create_table_providers(&schema_name, table_names).await;
            schemas.insert(
                schema_name,
                Arc::new(AdbcSchemaProvider {
                    tables: RwLock::new(tables),
                }),
            );
        }

        // Phase 3: Swap in the new schemas.
        {
            let mut guard = match self.schemas.write() {
                Ok(guard) => guard,
                Err(e) => e.into_inner(),
            };
            *guard = schemas;
        }

        tracing::info!(
            duration_ms = refresh_start.elapsed().as_millis(),
            "ADBC: catalog refresh complete"
        );

        Ok(())
    }

    async fn create_table_providers(
        &self,
        schema_name: &str,
        table_names: Vec<String>,
    ) -> HashMap<String, Arc<dyn TableProvider>> {
        type ProviderResult =
            std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;

        let start = Instant::now();
        let dialect = dialect_for_driver(&self.driver_name);
        let include = self.include.clone();
        let schema_name_owned = schema_name.to_owned();

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut stream = stream::iter(
            table_names
                .into_iter()
                .filter_map(|table_name| {
                    let schema_with_table = format!("{schema_name_owned}.{table_name}");
                    if let Some(include) = &include
                        && !include.is_match(&schema_with_table)
                    {
                        tracing::debug!("Table {schema_with_table} is not included, skipping");
                        return None;
                    }
                    Some(table_name)
                })
                .map(|table_name| {
                    let table_factory = &self.table_factory;
                    let schema_ref = &schema_name_owned;
                    let dialect = dialect.clone();
                    async move {
                        let table_ref =
                            TableReference::partial(schema_ref.to_owned(), table_name.clone());
                        let result: ProviderResult = match table_factory
                            .table_provider(table_ref.clone(), dialect)
                            .await
                        {
                            Ok(provider) => Ok(enrich_with_bigquery_metadata(
                                &self.driver_name,
                                &self.pool,
                                &table_ref,
                                provider,
                            )
                            .await),
                            Err(error) => Err(error),
                        };
                        (table_name, result)
                    }
                }),
        )
        .buffer_unordered(CATALOG_DISCOVERY_CONCURRENCY);

        while let Some((table_name, result)) = stream.next().await {
            match result {
                Ok(provider) => {
                    tables.insert(table_name, provider);
                }
                Err(e) => {
                    tracing::warn!(
                        schema = %schema_name,
                        table = %table_name,
                        error = %e,
                        "Failed to create table provider for ADBC table, skipping"
                    );
                }
            }
        }

        tracing::debug!(duration_ms = start.elapsed().as_millis(), schema = %schema_name, tables = tables.len(), "ADBC: schema table providers created");
        tables
    }
}

impl CatalogProvider for AdbcCatalogProvider {
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
impl RefreshableCatalogProvider for AdbcCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.refresh_schemas().await?;
        Ok(())
    }
}

// -- Schema Provider --

/// A schema provider for ADBC that holds discovered tables.
struct AdbcSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for AdbcSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcSchemaProvider").finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for AdbcSchemaProvider {
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
    use super::*;

    #[test]
    fn test_parameters() {
        let param_names: Vec<&str> = PARAMETERS.iter().map(|p| p.name).collect();
        assert!(param_names.contains(&"driver"));
        assert!(param_names.contains(&"driver_path"));
        assert!(param_names.contains(&"uri"));
        assert!(param_names.contains(&"username"));
        assert!(param_names.contains(&"password"));
        assert!(param_names.contains(&"driver_options"));
        assert!(param_names.contains(&"connection_pool_size"));
        assert!(param_names.contains(&"connection_pool_min_idle"));
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingDriver;
        assert_eq!(err.to_string(), "Missing required parameter: driver");

        let err = Error::MissingUri;
        assert_eq!(err.to_string(), "Missing required parameter: uri");
    }
}
