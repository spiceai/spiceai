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

use async_trait::async_trait;
use data_components::Read;
use data_components::ducklake::writer::DuckDbFederatedTableWriter;
use data_components::ducklake::{
    DuckLakeS3Params, build_ducklake_attach_sql, configure_duckdb_httpfs,
};
use data_connector_api::ConnectorContext;
use data_connector_api::{
    AnyErrorResult, ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory,
};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use runtime_component::dataset::DatasetSpec;
use runtime_datafusion::dialect::new_duckdb_dialect;
use runtime_parameters::ParameterSpec;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    ParameterSpec::component("aws_session_token")
        .description("The AWS session token for S3 storage. Required with temporary (STS) credentials.")
        .secret(),
    ParameterSpec::component("aws_endpoint")
        .description("Custom S3-compatible endpoint URL (e.g. for MinIO).")
        .secret(),
    ParameterSpec::component("aws_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3."),
    ParameterSpec::component("automatic_migration").description(
        "Automatically migrate an older DuckLake catalog schema to the version required by the ducklake extension on attach. Defaults to false; migration rewrites catalog metadata and cannot be undone.",
    ),
];

fn create_ducklake_factory(
    connection_string: &str,
    catalog_name: &str,
    open_path: Option<&str>,
    automatic_migration: bool,
    params: &ConnectorParams,
) -> AnyErrorResult<(DuckDBTableFactory, Arc<DuckDbConnectionPool>, String)> {
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
        session_token: params
            .parameters
            .get("aws_session_token")
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

    let attach_sql =
        build_ducklake_attach_sql(connection_string, catalog_name, automatic_migration);
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

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = data_connector_api::NewDataConnectorResult> + Send + 'a>> {
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

            let automatic_migration = params
                .parameters
                .get("automatic_migration")
                .expose()
                .ok()
                .is_some_and(|v| v.eq_ignore_ascii_case("true"));

            let params_for_factory = params.clone();
            let (duckdb_factory, pool, catalog_name) = tokio::task::spawn_blocking(move || {
                create_ducklake_factory(
                    &connection_string,
                    &catalog_name,
                    open_path.as_deref(),
                    automatic_migration,
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
    fn resolve_table_reference(&self, dataset: &DatasetSpec) -> TableReference {
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
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> data_connector_api::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_ref = self.resolve_table_reference(dataset);

        Ok(Read::table_provider(&self.duckdb_factory, table_ref)
            .await
            .context(data_connector_api::UnableToGetReadProviderSnafu {
                dataconnector: "ducklake",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }

    async fn read_write_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<data_connector_api::DataConnectorResult<Arc<dyn TableProvider>>> {
        let table_ref = self.resolve_table_reference(dataset);

        let read_provider = match Read::table_provider(&self.duckdb_factory, table_ref.clone())
            .await
            .context(data_connector_api::UnableToGetReadProviderSnafu {
                dataconnector: "ducklake",
                connector_component: ConnectorComponent::from(dataset),
            }) {
            Ok(provider) => provider,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok(DuckDbFederatedTableWriter::create(
            read_provider,
            Arc::clone(&self.pool),
            &table_ref,
            Arc::clone(&self.write_lock),
        )))
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "ducklake";

/// Returns a new instance of the `DuckLake` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    DuckLakeFactory::new_arc()
}

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_ducklake as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_ducklake_connector,
    DUCKLAKE_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    DuckLakeFactory
);

#[cfg(test)]
mod tests {
    use super::PARAMETERS;

    /// The S3 secret built for `DuckDB` only carries `SESSION_TOKEN` when the parameter is
    /// declared here — otherwise `Parameters::try_new` strips it and temporary (STS)
    /// credentials fail with `InvalidAccessKeyId`.
    #[test]
    fn ducklake_parameters_include_aws_session_token() {
        assert!(
            PARAMETERS
                .iter()
                .any(|parameter| parameter.name == "aws_session_token"),
            "missing DuckLake data connector parameter aws_session_token"
        );
    }
}
