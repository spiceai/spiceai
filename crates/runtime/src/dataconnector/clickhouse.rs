/*
Copyright 2024 The Spice.ai OSS Authors

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
use clickhouse_rs::Options;
use data_components::clickhouse::ClickhouseTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::sql::db_connection_pool::Error as DbConnectionPoolError;
use db_connection_pool::clickhousepool::ClickhouseConnectionPool;
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::str::ParseBoolError;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use super::{DataConnector, DataConnectorError, DataConnectorFactory, Parameters};
use crate::parameters::{ParamLookup, ParameterSpec};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Clickhouse connection pool: {source}"))]
    UnableToCreateClickhouseConnectionPool { source: DbConnectionPoolError },

    #[snafu(display("InvalidConnectionStringError: {source}"))]
    InvalidConnectionStringError {
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("ConnectionTlsError: {source}"))]
    ConnectionTlsError {
        source: clickhouse_rs::errors::ConnectionError,
    },

    #[snafu(display("Unable to parse the connection string as a URL: {source}"))]
    UnableToParseConnectionString { source: url::ParseError },

    #[snafu(display("Unable to sanitize the connection string"))]
    UnableToSanitizeConnectionString,

    #[snafu(display(
        "Authentication failed. Ensure that the username and password are correctly configured."
    ))]
    InvalidUsernameOrPasswordError {
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("Cannot connect to ClickHouse on {host}:{port}. Ensure that the host and port are correctly configured, and that the host is reachable."))]
    InvalidHostOrPortError {
        source: Box<dyn std::error::Error + Sync + Send>,
        host: String,
        port: String,
    },

    #[snafu(display("Missing required parameter: {parameter_name}"))]
    MissingRequiredParameterForConnection { parameter_name: String },

    #[snafu(display("Invalid secure parameter value {parameter_name}"))]
    InvalidSecureParameterValueError {
        parameter_name: String,
        source: ParseBoolError,
    },

    #[snafu(display("Invalid clickhouse_connection_timeout value: {source}"))]
    InvalidConnectionTimeoutValue { source: std::num::ParseIntError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Clickhouse {
    clickhouse_factory: ClickhouseTableFactory,
}

#[derive(Default, Copy, Clone)]
pub struct ClickhouseFactory {}

impl ClickhouseFactory {
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
    // clickhouse_connection_string
    ParameterSpec::connector("connection_string").secret()
        .description("The connection string to use to connect to the Clickhouse server. This can be used instead of providing individual connection parameters."),
    // clickhouse_pass
    ParameterSpec::connector("pass").secret().description("The password to use to connect to the Clickhouse server."),
    // clickhouse_user
    ParameterSpec::connector("user").description("The username to use to connect to the Clickhouse server."),
    // clickhouse_host
    ParameterSpec::connector("host").description("The hostname of the Clickhouse server."),
    // clickhouse_tcp_port
    ParameterSpec::connector("tcp_port").description("The port of the Clickhouse server."),
    // clickhouse_db
    ParameterSpec::connector("db").description("The database to use on the Clickhouse server."),
    // clickhouse_secure
    ParameterSpec::connector("secure").description("Whether to use a secure connection to the Clickhouse server."),
    // connection_timeout
    ParameterSpec::runtime("connection_timeout").description("The connection timeout in milliseconds."),
];

impl DataConnectorFactory for ClickhouseFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match get_config_from_params(params).await {
                Ok((options, compute_context)) => {
                    let pool = ClickhouseConnectionPool::new(options, compute_context);
                    let clickhouse_factory = ClickhouseTableFactory::new(Arc::new(pool));
                    Ok(Arc::new(Clickhouse { clickhouse_factory }) as Arc<dyn DataConnector>)
                }

                Err(e) => match e {
                    Error::InvalidUsernameOrPasswordError { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "clickhouse".to_string(),
                        }
                        .into(),
                    ),
                    Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "clickhouse".to_string(),
                        host,
                        port,
                    }
                    .into()),
                    Error::ConnectionTlsError { source: _ } => {
                        Err(DataConnectorError::UnableToConnectTlsError {
                            dataconnector: "clickhouse".to_string(),
                        }
                        .into())
                    }
                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "clickhouse".to_string(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "clickhouse"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Clickhouse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.clickhouse_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "clickhouse",
        })?)
    }
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Returns a Clickhouse `Options` based on user-provided parameters.
/// Also returns the sanitized connection string for use as a federation `compute_context`.
async fn get_config_from_params(params: Parameters) -> Result<(Options, String)> {
    let connection_string = match params.get("connection_string") {
        ParamLookup::Present(clickhouse_connection_string) => {
            clickhouse_connection_string.expose_secret().to_string()
        }
        ParamLookup::Absent(_) => {
            let user = params.get("user").expose().ok_or_else(|p| {
                Error::MissingRequiredParameterForConnection {
                    parameter_name: p.0,
                }
            })?;
            let password = params
                .get("pass")
                .expose()
                .ok()
                .map(ToString::to_string)
                .unwrap_or_default();
            let host = params.get("host").expose().ok_or_else(|p| {
                Error::MissingRequiredParameterForConnection {
                    parameter_name: p.0,
                }
            })?;
            let port = params.get("tcp_port").expose().ok_or_else(|p| {
                Error::MissingRequiredParameterForConnection {
                    parameter_name: p.0,
                }
            })?;

            let port_in_usize = u16::from_str(port)
                .map_err(std::convert::Into::into)
                .context(InvalidHostOrPortSnafu { host, port })?;
            verify_ns_lookup_and_tcp_connect(host, port_in_usize)
                .await
                .map_err(std::convert::Into::into)
                .context(InvalidHostOrPortSnafu { host, port })?;
            let db = params.get("db").expose().ok_or_else(|p| {
                Error::MissingRequiredParameterForConnection {
                    parameter_name: p.0,
                }
            })?;

            format!("tcp://{user}:{password}@{host}:{port}/{db}")
        }
    };

    let mut sanitized_connection_string =
        Url::parse(&connection_string).context(UnableToParseConnectionStringSnafu)?;
    sanitized_connection_string
        .set_password(None)
        .map_err(|()| Error::UnableToSanitizeConnectionString)?;

    let mut options =
        Options::from_str(&connection_string).context(InvalidConnectionStringSnafu)?;
    if !connection_string.contains("connection_timeout") {
        // Default timeout of 500ms is not enough in some cases.
        options = options.connection_timeout(DEFAULT_CONNECTION_TIMEOUT);
    }

    if let Some(connection_timeout) = params.get("connection_timeout").expose().ok() {
        let connection_timeout = connection_timeout
            .parse::<u64>()
            .context(InvalidConnectionTimeoutValueSnafu)?;
        options = options.connection_timeout(Duration::from_millis(connection_timeout));
    }

    let secure = params
        .get("secure")
        .expose()
        .ok()
        .map(str::parse)
        .transpose()
        .context(InvalidSecureParameterValueSnafu {
            parameter_name: "clickhouse_secure".to_string(),
        })?;
    options = options.secure(secure.unwrap_or(true));

    Ok((options, sanitized_connection_string.to_string()))
}
