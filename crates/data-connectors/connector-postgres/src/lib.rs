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

//! `PostgreSQL` data connector for Spice.ai runtime.
//!
//! This crate provides the `PostgreSQL` connector implementation, allowing
//! Spice.ai to connect to `PostgreSQL` databases as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection;
use datafusion_table_providers::sql::db_connection_pool::{
    Error as DbConnectionPoolError,
    postgrespool::{self, PostgresConnectionPool},
};
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::parameters::ParameterSpec;
use secrecy::SecretBox;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: DbConnectionPoolError },
}

/// `PostgreSQL` data connector.
pub struct Postgres {
    postgres_factory: PostgresTableFactory,
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres").finish_non_exhaustive()
    }
}

/// Factory for creating `PostgreSQL` connector instances.
#[derive(Default, Copy, Clone)]
pub struct PostgresFactory {}

impl PostgresFactory {
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
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("user").secret(),
    ParameterSpec::component("pass").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("db"),
    ParameterSpec::component("sslmode"),
    ParameterSpec::component("sslrootcert"),
    ParameterSpec::component("connection_pool_min_idle")
        .description("The minimum number of idle connections to keep open in the pool.")
        .default("1"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections created in the connection pool.")
        .default("5"),
];

impl DataConnectorFactory for PostgresFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mut param_map = params.parameters.to_secret_map();

            param_map.insert(
                "application_name".to_string(),
                SecretBox::from(format!("Spice.ai {}", env!("CARGO_PKG_VERSION"))),
            );

            match PostgresConnectionPool::new(param_map).await {
                Ok(pool) => {
                    let unsupported_type_action = params
                        .unsupported_type_action
                        .unwrap_or(datafusion_table_providers::UnsupportedTypeAction::String);
                    let pool = pool.with_unsupported_type_action(unsupported_type_action);

                    let postgres_factory = PostgresTableFactory::new(Arc::new(pool));
                    Ok(Arc::new(Postgres { postgres_factory }) as Arc<dyn DataConnector>)
                }
                Err(e) => match e {
                    postgrespool::Error::InvalidUsernameOrPassword { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "postgres".to_string(),
                            connector_component: params.component.clone(),
                        }
                        .into(),
                    ),

                    postgrespool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        host,
                        port: format!("{port}"),
                    }
                    .into()),

                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "pg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Postgres {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(feature = "postgres-write")]
    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self
            .postgres_factory
            .read_write_table_provider(dataset.path().into())
            .await
        {
            Ok(provider) => Some(Ok(provider)),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    match err_source.downcast_ref::<dbconnection::Error>() {
                        Some(dbconnection::Error::UndefinedTable {
                            table_name,
                            source: _,
                        }) => {
                            return Some(Err(DataConnectorError::InvalidTableName {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                table_name: table_name.clone(),
                            }));
                        }
                        Some(dbconnection::Error::UnsupportedDataType {
                            data_type,
                            field_name,
                        }) => {
                            return Some(Err(DataConnectorError::UnsupportedDataType {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                data_type: data_type.clone(),
                                field_name: field_name.clone(),
                            }));
                        }
                        _ => {}
                    }
                }

                Some(Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                }))
            }
        }
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        match self
            .postgres_factory
            .table_provider(dataset.path().into())
            .await
        {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    match err_source.downcast_ref::<dbconnection::Error>() {
                        Some(dbconnection::Error::UndefinedTable {
                            table_name,
                            source: _,
                        }) => {
                            return Err(DataConnectorError::InvalidTableName {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                table_name: table_name.clone(),
                            });
                        }
                        Some(dbconnection::Error::UnsupportedDataType {
                            data_type,
                            field_name,
                        }) => {
                            return Err(DataConnectorError::UnsupportedDataType {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                data_type: data_type.clone(),
                                field_name: field_name.clone(),
                            });
                        }
                        _ => {}
                    }
                }

                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                })
            }
        }
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "postgres";

/// Returns a new instance of the `PostgreSQL` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    PostgresFactory::new_arc()
}
