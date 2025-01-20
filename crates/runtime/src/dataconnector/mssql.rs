/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::mssql::connection_manager::SqlServerConnectionManager;
use data_components::mssql::{
    self, connection_manager::SqlServerConnectionPool, SqlServerTableProvider,
};
use datafusion::datasource::TableProvider;
use snafu::{ResultExt, Snafu};
use std::pin::Pin;
use std::sync::Arc;
use std::{any::Any, future::Future};
use tiberius::{Config, EncryptionLevel};

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
    ParameterSpec, Parameters, UnableToGetReadProviderSnafu,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: '{parameter}'. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/mssql#configuration"))]
    MissingParameter { parameter: String },

    #[snafu(display("Failed to connect to the MS SQL Server.\nVerify your connection configuration, and try again.\n{source}"))]
    UnableToCreateConnectionPool { source: mssql::Error },

    #[snafu(display("Invalid connection string.\nVerify the connection string is valid, and try again.\n{source}"))]
    InvalidConnectionStringError { source: tiberius::error::Error },

    #[snafu(display("Invalid value provided for the 'port' parameter: {port}.\nSpecify a valid port, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/mssql#configuration"))]
    FailedToParsePort { port: String },

    #[snafu(display("Invalid value provided for parameter '{parameter}'\nSpecify a valid value, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/mssql#configuration"))]
    InvalidParameterValue { parameter: String },
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("connection_string").secret(),
    ParameterSpec::connector("username").secret(),
    ParameterSpec::connector("password").secret(),
    ParameterSpec::connector("host"),
    ParameterSpec::connector("port"),
    ParameterSpec::connector("database"),
    ParameterSpec::connector("encrypt"),
    ParameterSpec::connector("trust_server_certificate"),
];

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlServer {
    conn: Arc<SqlServerConnectionPool>,
}

impl SqlServer {
    async fn new(params: &Parameters) -> Result<Self> {
        let conn_config = if let Some(conn_string) = params.get("connection_string").expose().ok() {
            Config::from_ado_string(conn_string).context(InvalidConnectionStringSnafu)?
        } else {
            let mut config = Config::default();

            config.authentication(tiberius::AuthMethod::sql_server(
                params
                    .get("username")
                    .expose()
                    .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
                params
                    .get("password")
                    .expose()
                    .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
            ));

            config.host(
                params
                    .get("host")
                    .expose()
                    .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
            );

            if let Some(port_str) = params.get("port").expose().ok() {
                let port = port_str.parse::<u16>().map_err(|_| {
                    FailedToParsePortSnafu {
                        port: port_str.to_string(),
                    }
                    .build()
                })?;
                config.port(port);
            }

            if let Some(database) = params.get("database").expose().ok() {
                config.database(database);
            }

            if let Some(val) = params.get("encrypt").expose().ok() {
                match val.to_lowercase().as_str() {
                    "true" | "require" => {
                        config.encryption(EncryptionLevel::Required);
                    }
                    "false" | "disable" => {
                        config.encryption(EncryptionLevel::Off);
                    }
                    _ => InvalidParameterValueSnafu {
                        parameter: "encrypt",
                    }
                    .fail()?,
                }
            } else {
                config.encryption(EncryptionLevel::Required);
            }

            if let Some(val) = params.get("trust_server_certificate").expose().ok() {
                match val.to_lowercase().as_str() {
                    "true" => {
                        config.trust_cert();
                    }
                    "false" => (),
                    _ => InvalidParameterValueSnafu {
                        parameter: "trust_server_certificate",
                    }
                    .fail()?,
                }
            }

            config
        };

        let conn = SqlServerConnectionManager::create(conn_config)
            .await
            .context(UnableToCreateConnectionPoolSnafu)?;

        Ok(Self {
            conn: Arc::new(conn),
        })
    }
}

#[derive(Default, Copy, Clone)]
pub struct SqlServerFactory {}

impl SqlServerFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for SqlServerFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(SqlServer::new(&params.parameters).await?) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mssql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for SqlServer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let provider = SqlServerTableProvider::new(Arc::clone(&self.conn), &dataset.path().into())
            .await
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "mssql",
                connector_component: ConnectorComponent::from(dataset),
            })?;

        Ok(Arc::new(provider))
    }
}
