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

use super::ConnectorComponent;
use super::ConnectorParams;
use super::{DataConnector, DataConnectorError, DataConnectorFactory, Parameters};
use crate::parameters::{ParamLookup, ParameterSpec};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to ClickHouse.\nVerify your connection configuration, and try again.\n{source}"))]
    UnableToCreateClickhouseConnectionPool { source: DbConnectionPoolError },

    #[snafu(display("An invalid connection string value was provided.\nVerify the connection string is valid, and try again.\n{source}"))]
    InvalidConnectionStringError {
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("Failed to connect to ClickHouse over TLS.\nVerify your TLS configuration, and try again.\n{source}"))]
    ConnectionTlsError {
        source: clickhouse_rs::errors::ConnectionError,
    },

    #[snafu(display("An invalid connection string value was provided.\nVerify the connection string is valid, and try again.\n{source}"))]
    UnableToParseConnectionString { source: url::ParseError },

    // from url::Url: If this URL is cannot-be-a-base or does not have a host, do nothing and return Err.
    // so, this error is only possible if the URL is not a valid URL.
    #[snafu(display("Failed to sanitize the connection string.\nVerify the connection string is valid, and try again."))]
    UnableToSanitizeConnectionString,

    #[snafu(display(
        "Failed to authenticate with the ClickHouse.\nEnsure that the username and password are correctly configured.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/clickhouse#configuration"
    ))]
    InvalidUsernameOrPasswordError {
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("Unable to connect to ClickHouse on {host}:{port}.\nEnsure that the host and port are correctly configured, and that the host is reachable."))]
    InvalidHostOrPortError {
        source: Box<dyn std::error::Error + Sync + Send>,
        host: String,
        port: String,
    },

    #[snafu(display("Missing required parameter: '{parameter_name}'. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/clickhouse#configuration"))]
    MissingRequiredParameterForConnection { parameter_name: String },

    #[snafu(display("An invalid value was provided for the parameter '{parameter_name}'.\nSpecify a value of 'true' or 'false'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/clickhouse#configuration"))]
    InvalidSecureParameterValueError {
        parameter_name: String,
        source: ParseBoolError,
    },

    #[snafu(display("An invalid value was provided for the parameter 'clickhouse_connection_timeout'.\nSpecify a valid integer value.\n{source}"))]
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match get_config_from_params(params.parameters).await {
                Ok(config) => {
                    let pool = ClickhouseConnectionPool::new(
                        config.options,
                        config.db,
                        config.compute_context,
                    );
                    let clickhouse_factory = ClickhouseTableFactory::new(Arc::new(pool));
                    Ok(Arc::new(Clickhouse { clickhouse_factory }) as Arc<dyn DataConnector>)
                }

                Err(e) => match e {
                    Error::InvalidUsernameOrPasswordError { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "clickhouse".to_string(),
                            connector_component: params.component,
                        }
                        .into(),
                    ),
                    Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "clickhouse".to_string(),
                        connector_component: params.component,
                        host,
                        port,
                    }
                    .into()),
                    Error::ConnectionTlsError { source: _ } => {
                        Err(DataConnectorError::UnableToConnectTlsError {
                            dataconnector: "clickhouse".to_string(),
                            connector_component: params.component,
                        }
                        .into())
                    }
                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "clickhouse".to_string(),
                        connector_component: params.component,
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
            connector_component: ConnectorComponent::from(dataset),
        })?)
    }
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct ClickhouseConfig {
    pub(crate) options: Options,
    pub(crate) db: Arc<str>,
    pub(crate) compute_context: String,
}

impl ClickhouseConfig {
    pub fn new(options: Options, db: Arc<str>, compute_context: String) -> Self {
        Self {
            options,
            db,
            compute_context,
        }
    }
}

/// Returns a `ClickhouseConfig` based on user-provided parameters.
async fn get_config_from_params(params: Parameters) -> Result<ClickhouseConfig> {
    let mut db: Arc<str> = "default".into();
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
            let db_param = params.get("db").expose().ok_or_else(|p| {
                Error::MissingRequiredParameterForConnection {
                    parameter_name: p.0,
                }
            })?;

            format!("tcp://{user}:{password}@{host}:{port}/{db_param}")
        }
    };

    if let Some(db_name) = get_database_from_url(&connection_string) {
        db = db_name.into();
    }

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

    Ok(ClickhouseConfig::new(
        options,
        db,
        sanitized_connection_string.to_string(),
    ))
}

/// Extracts the database name from a Clickhouse URL.
///
/// This function parses the URL and attempts to extract the database name from the path.
/// It returns `Some(database_name)` if a valid database name is found, or `None` if no
/// database is specified or the path is empty.
///
/// # Arguments
///
/// * `url` - A reference to a parsed `Url` struct representing the Clickhouse connection URL.
///
/// # Returns
///
/// An `Option<&str>` containing the database name if found, or `None` otherwise.
///
/// # Example
///
/// ```
/// use url::Url;
///
/// let url = Url::parse("tcp://user:pass@host:9000/mydb").unwrap();
/// let database = get_database_from_url(&url);
/// assert_eq!(database, Some("mydb"));
///
/// let url_without_db = Url::parse("tcp://user:pass@host:9000").unwrap();
/// let database = get_database_from_url(&url_without_db);
/// assert_eq!(database, None);
/// ```
fn get_database_from_url(url_str: &str) -> Option<String> {
    let url = Url::parse(url_str).ok()?;
    match url.path_segments() {
        None => None,
        Some(mut segments) => {
            let head = segments.next();

            if segments.next().is_some() {
                return None;
            }

            match head {
                Some(database) if !database.is_empty() => Some(database.to_string()),
                _ => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_database_from_url() {
        let test_cases = vec![
            ("tcp://user:pass@host:9000/mydb", Some("mydb")),
            ("tcp://user:pass@host:9000/", None),
            ("tcp://user:pass@host:9000", None),
            (
                "tcp://user:pass@host:9000/db_name_with_underscores",
                Some("db_name_with_underscores"),
            ),
            (
                "tcp://user:pass@host:9000/db-name-with-hyphens",
                Some("db-name-with-hyphens"),
            ),
            (
                "tcp://user:pass@host:9000/dbNameWithCamelCase",
                Some("dbNameWithCamelCase"),
            ),
            (
                "tcp://user:pass@host:9000/DB_NAME_WITH_UPPERCASE",
                Some("DB_NAME_WITH_UPPERCASE"),
            ),
            (
                "tcp://user:pass@host:9000/db123WithNumbers",
                Some("db123WithNumbers"),
            ),
        ];

        for (url_str, expected_db) in test_cases {
            match get_database_from_url(url_str) {
                Some(db) => assert_eq!(
                    db,
                    expected_db.expect("Expected a database name"),
                    "Unexpected result for URL: {url_str}"
                ),
                None => assert_eq!(expected_db, None, "Unexpected result for URL: {url_str}"),
            }
        }
    }

    #[test]
    fn test_get_database_from_url_with_invalid_path() {
        let result = get_database_from_url("tcp://user:pass@host:9000/db/invalid");
        assert!(result.is_none(), "Expected None for invalid path");
    }
}
