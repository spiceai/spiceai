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

//! `DuckLake` catalog connector.
//!
//! Connects to a `DuckLake` catalog using `DuckDB` with the `ducklake` extension
//! and provides schema/table discovery.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{
    Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams,
    parameters::ExposedParamLookup,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::ducklake::provider::DuckLakeCatalogProvider;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use snafu::prelude::*;
use std::any::Any;
use std::sync::Arc;

pub const PREFIX: &str = "ducklake";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/catalogs/ducklake"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display("Failed to create DuckDB connection pool: {source}"))]
    UnableToCreatePool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get DuckDB connection: {source}"))]
    UnableToGetConnection {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to initialize DuckLake: {source}"))]
    UnableToInitializeDuckLake { source: duckdb::Error },

    #[snafu(display("Failed to refresh DuckLake catalog: {source}"))]
    UnableToRefreshCatalog {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description("The DuckLake connection string (e.g., 's3://bucket/path/metadata.ducklake'). If omitted, the catalog id from `from: ducklake:<connection_string>` is used."),
    ParameterSpec::component("name")
        .description("The name to attach the DuckLake catalog as in DuckDB. Defaults to 'ducklake'."),
    ParameterSpec::component("open")
        .description("Optional path to an existing `DuckDB` file. If not provided, an in-memory `DuckDB` is used."),
];

/// A catalog connector for `DuckLake`, providing access to schemas and tables via `DuckDB`.
#[derive(Clone)]
pub struct DuckLakeCatalog {
    params: ConnectorParams,
}

impl DuckLakeCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let connection_string: String =
            match self.params.parameters.get("connection_string").expose() {
                ExposedParamLookup::Present(value) => value.to_string(),
                ExposedParamLookup::Absent(parameter) => {
                    if let Some(catalog_id) = catalog.catalog_id.as_ref() {
                        if !catalog_id.is_empty() {
                            catalog_id.clone()
                        } else {
                            let e = Error::MissingParameter {
                                parameter: parameter.to_string(),
                            };
                            return Err(super::Error::InvalidConfigurationNoSource {
                                connector: PREFIX.to_string(),
                                connector_component,
                                message: e.to_string(),
                            });
                        }
                    } else {
                        let e = Error::MissingParameter {
                            parameter: parameter.to_string(),
                        };
                        return Err(super::Error::InvalidConfigurationNoSource {
                            connector: PREFIX.to_string(),
                            connector_component,
                            message: e.to_string(),
                        });
                    }
                }
            };

        let catalog_name = self
            .params
            .parameters
            .get("name")
            .expose()
            .ok()
            .map_or_else(|| "ducklake".to_string(), ToString::to_string);

        let open_path = self
            .params
            .parameters
            .get("open")
            .expose()
            .ok()
            .map(ToString::to_string);

        // Get the catalog's access mode to determine writable/ddl_enabled flags
        let writable = catalog.access.allows_write();
        let ddl_enabled = catalog.access.allows_ddl();

        // Use the appropriate `DuckDB` access mode based on catalog permissions
        let duckdb_access_mode = if writable {
            AccessMode::ReadWrite
        } else {
            AccessMode::ReadOnly
        };

        let connection_string_for_pool = connection_string;
        let catalog_name_for_pool = catalog_name.clone();
        let connector_component_for_pool = connector_component.clone();

        // Blocking DuckDB setup is isolated from the async runtime thread.
        let pool =
            tokio::task::spawn_blocking(move || -> super::Result<Arc<DuckDbConnectionPool>> {
                let pool = if let Some(path) = open_path.as_deref() {
                    Arc::new(
                        DuckDbConnectionPool::new_file(path, &duckdb_access_mode).map_err(|e| {
                            super::Error::UnableToGetCatalogProvider {
                                connector: PREFIX.to_string(),
                                connector_component: connector_component_for_pool.clone(),
                                source: e,
                            }
                        })?,
                    )
                } else {
                    Arc::new(DuckDbConnectionPool::new_memory().map_err(|e| {
                        super::Error::UnableToGetCatalogProvider {
                            connector: PREFIX.to_string(),
                            connector_component: connector_component_for_pool.clone(),
                            source: e,
                        }
                    })?)
                };

                let conn = Arc::clone(&pool).connect_sync().map_err(|e| {
                    super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: e,
                    }
                })?;

                let duckdb_conn = conn
                    .as_any()
                    .downcast_ref::<duckdb::Connection>()
                    .ok_or_else(|| super::Error::InvalidConfigurationNoSource {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        message: "Failed to get underlying DuckDB connection".to_string(),
                    })?;

                duckdb_conn
                    .execute("INSTALL ducklake", [])
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: Box::new(e),
                    })?;

                duckdb_conn
                    .execute("LOAD ducklake", [])
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool.clone(),
                        source: Box::new(e),
                    })?;

                let attach_sql = format!(
                    "ATTACH 'ducklake:{connection_string_for_pool}' AS \"{catalog_name_for_pool}\""
                );
                duckdb_conn
                    .execute(&attach_sql, [])
                    .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
                    .map_err(|e| super::Error::UnableToGetCatalogProvider {
                        connector: PREFIX.to_string(),
                        connector_component: connector_component_for_pool,
                        source: Box::new(e),
                    })?;

                Ok(pool)
            })
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })??;

        // Create the catalog provider with the pool (which has ducklake extension and catalog attached)
        let catalog_provider = Arc::new(DuckLakeCatalogProvider::new(
            pool,
            catalog_name,
            writable,
            ddl_enabled,
        ));

        // Initial refresh to populate schemas and tables
        catalog_provider
            .refresh()
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component,
                source: e,
            })?;

        Ok(catalog_provider as Arc<dyn RefreshableCatalogProvider>)
    }
}
