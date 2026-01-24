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

//! `DuckLake` data connector.
//!
//! Connects to specific tables in a `DuckLake` catalog using `DuckDB` with the `ducklake` extension.

use crate::{
    component::dataset::Dataset, datafusion::dialect::new_duckdb_dialect, register_data_connector,
};
use async_trait::async_trait;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    AnyErrorResult, ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory, ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: ducklake_connection_string. Specify the DuckLake metadata location."
    ))]
    MissingConnectionString,

    #[snafu(display("Failed to create DuckDB connection pool: {source}"))]
    UnableToCreatePool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to initialize DuckLake extension: {source}"))]
    UnableToInitializeDuckLake { source: duckdb::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckLake {
    duckdb_factory: DuckDBTableFactory,
    catalog_name: String,
}

impl std::fmt::Debug for DuckLake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLake")
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

#[derive(Default, Copy, Clone)]
pub struct DuckLakeFactory {}

impl DuckLakeFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("ducklake_connection_string")
        .description("The DuckLake connection string (e.g., 's3://bucket/path/metadata.ducklake').")
        .required(),
    ParameterSpec::component("ducklake_name").description(
        "The name to attach the DuckLake catalog as in DuckDB. Defaults to 'ducklake'.",
    ),
    ParameterSpec::component("ducklake_open").description(
        "Optional path to an existing DuckDB file. If not provided, an in-memory DuckDB is used.",
    ),
];

fn create_ducklake_factory(
    connection_string: &str,
    catalog_name: &str,
    open_path: Option<&str>,
    params: &ConnectorParams,
) -> AnyErrorResult<(DuckDBTableFactory, String)> {
    // Create the DuckDB connection pool
    let pool = if let Some(path) = open_path {
        Arc::new(
            DuckDbConnectionPool::new_file(path, &AccessMode::ReadWrite)
                .map_err(|source| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "ducklake".to_string(),
                    connector_component: params.component.clone(),
                    source,
                })?
                .with_unsupported_type_action(
                    params
                        .unsupported_type_action
                        .unwrap_or(UnsupportedTypeAction::Error),
                ),
        )
    } else {
        Arc::new(
            DuckDbConnectionPool::new_memory()
                .map_err(|source| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "ducklake".to_string(),
                    connector_component: params.component.clone(),
                    source,
                })?
                .with_unsupported_type_action(
                    params
                        .unsupported_type_action
                        .unwrap_or(UnsupportedTypeAction::Error),
                ),
        )
    };

    // Get a connection to install/load the ducklake extension and attach the catalog
    let conn = Arc::clone(&pool).connect_sync().map_err(|source| {
        DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source,
        }
    })?;

    let duckdb_conn = conn
        .as_any()
        .downcast_ref::<duckdb::Connection>()
        .ok_or_else(|| DataConnectorError::InvalidConfiguration {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            message: "Failed to get underlying DuckDB connection".to_string(),
            source: Box::new(Error::MissingConnectionString),
        })?;

    // Install and load the ducklake extension
    duckdb_conn
        .execute("INSTALL ducklake", [])
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    duckdb_conn
        .execute("LOAD ducklake", [])
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    // Attach the DuckLake catalog
    let attach_sql = format!("ATTACH 'ducklake:{connection_string}' AS \"{catalog_name}\"");
    duckdb_conn
        .execute(&attach_sql, [])
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    let factory = DuckDBTableFactory::new(pool).with_dialect(new_duckdb_dialect());
    Ok((factory, catalog_name.to_string()))
}

impl DataConnectorFactory for DuckLakeFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let connection_string: String = params
                .parameters
                .clone()
                .get("ducklake_connection_string")
                .expose()
                .ok_or_else(|_| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "ducklake".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(Error::MissingConnectionString),
                })?
                .to_string();

            let catalog_name = params
                .parameters
                .get("ducklake_name")
                .expose()
                .ok()
                .map_or_else(|| "ducklake".to_string(), ToString::to_string);

            let open_path = params.parameters.get("ducklake_open").expose().ok();

            let (duckdb_factory, catalog_name) =
                create_ducklake_factory(&connection_string, &catalog_name, open_path, &params)?;

            Ok(Arc::new(DuckLake {
                duckdb_factory,
                catalog_name,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "ducklake"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for DuckLake {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        // The dataset path should be in the format "schema.table" or just "table"
        // We need to prefix it with the catalog name to form a fully qualified reference
        let path = dataset.path();
        let table_ref: TableReference = if path.contains('.') {
            // Already has schema, prefix with catalog
            format!("{}.{}", self.catalog_name, path).into()
        } else {
            // Just table name, use catalog.main.table (DuckLake default schema is "main")
            format!("{}.main.{}", self.catalog_name, path).into()
        };

        Ok(Read::table_provider(&self.duckdb_factory, table_ref)
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "ducklake",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }
}

register_data_connector!("ducklake", DuckLakeFactory);
