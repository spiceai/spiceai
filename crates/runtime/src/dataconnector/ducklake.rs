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

use crate::{component::dataset::Dataset, datafusion::dialect::new_duckdb_dialect};
use async_trait::async_trait;
use data_components::Read;
use data_components::ducklake::writer::DuckDbFederatedTableWriter;
use data_components::ducklake::{DuckLakeS3Params, configure_duckdb_httpfs};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::{
    AnyErrorResult, ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory, ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: connection_string. Specify the DuckLake metadata location."
    ))]
    MissingConnectionString,

    #[snafu(display("Failed to initialize DuckLake extension: {source}"))]
    UnableToInitializeDuckLake { source: duckdb::Error },

    #[snafu(display("Failed to get underlying DuckDB connection"))]
    FailedToGetDuckDbConnection,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckLake {
    duckdb_factory: DuckDBTableFactory,
    pool: Arc<DuckDbConnectionPool>,
    catalog_name: String,
    write_lock: Arc<Mutex<()>>,
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
    ParameterSpec::component("connection_string")
        .description("The DuckLake connection string (e.g., 's3://bucket/path/metadata.ducklake').")
        .required(),
    ParameterSpec::component("name").description(
        "The name to attach the DuckLake catalog as in DuckDB. Defaults to 'ducklake'.",
    ),
    ParameterSpec::component("open").description(
        "Optional path to an existing DuckDB file. If not provided, an in-memory DuckDB is used.",
    ),
    ParameterSpec::component("aws_region")
        .description("The AWS region for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_endpoint")
        .description("Custom S3-compatible endpoint URL (e.g. for MinIO).")
        .secret(),
    ParameterSpec::component("aws_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3."),
];

fn create_ducklake_factory(
    connection_string: &str,
    catalog_name: &str,
    open_path: Option<&str>,
    params: &ConnectorParams,
) -> AnyErrorResult<(DuckDBTableFactory, Arc<DuckDbConnectionPool>, String)> {
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

    let duckdb_wrapper = conn
        .as_any()
        .downcast_ref::<DuckDbConnection>()
        .ok_or_else(|| DataConnectorError::InvalidConfiguration {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            message: "Failed to get underlying DuckDB connection".to_string(),
            source: Box::new(Error::FailedToGetDuckDbConnection),
        })?;

    // Install and load the ducklake extension
    duckdb_wrapper
        .conn
        .execute("INSTALL ducklake", [])
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    duckdb_wrapper
        .conn
        .execute("LOAD ducklake", [])
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    let s3_params = DuckLakeS3Params {
        region: params
            .parameters
            .get("aws_region")
            .expose()
            .ok()
            .map(ToString::to_string),
        access_key_id: params
            .parameters
            .get("aws_access_key_id")
            .expose()
            .ok()
            .map(ToString::to_string),
        secret_access_key: params
            .parameters
            .get("aws_secret_access_key")
            .expose()
            .ok()
            .map(ToString::to_string),
        endpoint: params
            .parameters
            .get("aws_endpoint")
            .expose()
            .ok()
            .map(ToString::to_string),
        allow_http: params
            .parameters
            .get("aws_allow_http")
            .expose()
            .ok()
            .is_some_and(|v| v == "true"),
    };

    configure_duckdb_httpfs(&duckdb_wrapper.conn, &s3_params)
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    // Escape values to avoid breaking the SQL statement or enabling injection
    let escaped_connection_string = connection_string.replace('\'', "''");
    let escaped_catalog_name = catalog_name.replace('"', "\"\"");
    let attach_sql =
        format!("ATTACH 'ducklake:{escaped_connection_string}' AS \"{escaped_catalog_name}\"");
    duckdb_wrapper
        .conn
        .execute(&attach_sql, [])
        .map_err(|e| Error::UnableToInitializeDuckLake { source: e })
        .map_err(|e| DataConnectorError::UnableToConnectInternal {
            dataconnector: "ducklake".to_string(),
            connector_component: params.component.clone(),
            source: Box::new(e),
        })?;

    let factory = DuckDBTableFactory::new(Arc::clone(&pool)).with_dialect(new_duckdb_dialect());
    Ok((factory, pool, catalog_name.to_string()))
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
                .get("connection_string")
                .expose()
                .ok_or_else(|_| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "ducklake".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(Error::MissingConnectionString),
                })?
                .to_string();

            let catalog_name = params
                .parameters
                .get("name")
                .expose()
                .ok()
                .map_or_else(|| "ducklake".to_string(), ToString::to_string);

            let open_path = params
                .parameters
                .get("open")
                .expose()
                .ok()
                .map(ToString::to_string);

            let params_for_factory = params.clone();
            let (duckdb_factory, pool, catalog_name) = tokio::task::spawn_blocking(move || {
                create_ducklake_factory(
                    &connection_string,
                    &catalog_name,
                    open_path.as_deref(),
                    &params_for_factory,
                )
            })
            .await
            .map_err(|source| DataConnectorError::UnableToConnectInternal {
                dataconnector: "ducklake".to_string(),
                connector_component: params.component.clone(),
                source: Box::new(source),
            })??;

            Ok(Arc::new(DuckLake {
                duckdb_factory,
                pool,
                catalog_name,
                write_lock: Arc::new(Mutex::new(())),
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

impl DuckLake {
    /// Builds a fully-qualified `TableReference` for the given dataset path.
    ///
    /// If the path contains a dot (e.g. `schema.table`), it is prefixed with the catalog name.
    /// Otherwise, the default `main` schema is assumed: `catalog.main.table`.
    fn resolve_table_reference(&self, dataset: &Dataset) -> TableReference {
        let path = dataset.path();
        if path.contains('.') {
            format!("{}.{path}", self.catalog_name).into()
        } else {
            format!("{}.main.{path}", self.catalog_name).into()
        }
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
        let table_ref = self.resolve_table_reference(dataset);

        Ok(Read::table_provider(&self.duckdb_factory, table_ref)
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "ducklake",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let table_ref = self.resolve_table_reference(dataset);

        let read_provider = match Read::table_provider(&self.duckdb_factory, table_ref.clone())
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "ducklake",
                connector_component: ConnectorComponent::from(dataset),
            }) {
            Ok(provider) => provider,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok(Arc::new(DuckDbFederatedTableWriter::new(
            read_provider,
            Arc::clone(&self.pool),
            table_ref,
            Arc::clone(&self.write_lock),
        ))))
    }
}

register_data_connector!("ducklake", DuckLakeFactory);
