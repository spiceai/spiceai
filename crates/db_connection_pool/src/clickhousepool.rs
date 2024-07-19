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

use std::{
    collections::HashMap,
    str::{FromStr, ParseBoolError},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use clickhouse_rs::{ClientHandle, Options, Pool};
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::DbConnection, DbConnectionPool, JoinPushDown,
};
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::{ResultExt, Snafu};
use url::Url;

use crate::dbconnection::clickhouseconn::ClickhouseConnection;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError {
        source: clickhouse_rs::errors::ConnectionError,
    },

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

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError {
        source: clickhouse_rs::errors::Error,
    },

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

pub struct ClickhouseConnectionPool {
    pool: Arc<Pool>,
    join_push_down: JoinPushDown,
}

impl ClickhouseConnectionPool {
    // Creates a new instance of `ClickhouseConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let (options, compute_context) = get_config_from_params(&params).await?;

        let pool = Pool::new(options);

        Ok(Self {
            pool: Arc::new(pool),
            join_push_down: JoinPushDown::AllowedFor(compute_context),
        })
    }
}

#[async_trait]
impl DbConnectionPool<ClientHandle, &'static (dyn Sync)> for ClickhouseConnectionPool {
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<dyn DbConnection<ClientHandle, &'static (dyn Sync)>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = match pool.get_handle().await {
            Ok(conn) => Ok(conn),
            Err(e) => match e {
                clickhouse_rs::errors::Error::Driver(_)
                | clickhouse_rs::errors::Error::Io(_)
                | clickhouse_rs::errors::Error::Other(_)
                | clickhouse_rs::errors::Error::Url(_)
                | clickhouse_rs::errors::Error::FromSql(_) => {
                    Err(Error::ConnectionPoolRunError { source: e })
                }
                clickhouse_rs::errors::Error::Connection(connection_error) => {
                    match connection_error {
                        // This will be covered by host configuration error
                        clickhouse_rs::ConnectionError::TlsHostNotProvided
                        | clickhouse_rs::ConnectionError::IoError(_)
                        | clickhouse_rs::ConnectionError::Broken
                        | clickhouse_rs::ConnectionError::NoPacketReceived => {
                            Err(Error::ConnectionPoolRunError {
                                source: connection_error.into(),
                            })
                        }
                        clickhouse_rs::ConnectionError::TlsError(_) => {
                            Err(Error::ConnectionTlsError {
                                source: connection_error,
                            })
                        }
                    }
                }
                clickhouse_rs::errors::Error::Server(server_error) => {
                    if server_error.code == 516 {
                        Err(Error::InvalidUsernameOrPasswordError {
                            source: server_error.into(),
                        })
                    } else {
                        Err(Error::ConnectionPoolRunError {
                            source: server_error.into(),
                        })
                    }
                }
            },
        }?;
        Ok(Box::new(ClickhouseConnection::new(
            conn,
            Arc::clone(&self.pool),
        )))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
