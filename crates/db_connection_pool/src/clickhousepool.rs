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
use secrets::{get_secret_or_param, Secret};
use snafu::{ResultExt, Snafu};
use url::Url;

use crate::{
    dbconnection::{clickhouseconn::ClickhouseConnection, DbConnection},
    JoinPushDown,
};

use super::{DbConnectionPool, Result};

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

    #[snafu(display("Unable to parse the connection string as a URL: {source}"))]
    UnableToParseConnectionString { source: url::ParseError },

    #[snafu(display("Unable to sanitize the connection string"))]
    UnableToSanitizeConnectionString,

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError {
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("No parameters specified"))]
    ParametersEmptyError {},

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
    join_push_down_context: String,
}

impl ClickhouseConnectionPool {
    // Creates a new instance of `ClickhouseConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::unused_async)]
    pub async fn new(
        params: Arc<Option<HashMap<String, String>>>,
        secret: Option<Secret>,
    ) -> Result<Self> {
        let params = match params.as_ref() {
            Some(params) => params,
            None => ParametersEmptySnafu {}.fail()?,
        };
        let (options, compute_context) = get_config_from_params(params, &secret)?;

        let pool = Pool::new(options);

        Ok(Self {
            pool: Arc::new(pool),
            join_push_down_context: compute_context,
        })
    }
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Returns a Clickhouse `Options` based on user-provided parameters.
/// Also returns the sanitized connection string for use as a federation compute_context.
fn get_config_from_params<'a>(
    params: &'a HashMap<String, String>,
    secret: &'_ Option<Secret>,
) -> Result<(Options, String)> {
    let mut connection_string: Option<String> = None;
    if let Some(clickhouse_connection_string) = get_secret_or_param(
        Some(params),
        secret,
        "clickhouse_connection_string_key",
        "clickhouse_connection_string",
    ) {
        connection_string = Some(clickhouse_connection_string);
    } else {
        let user =
            params
                .get("clickhouse_user")
                .ok_or(Error::MissingRequiredParameterForConnection {
                    parameter_name: "clickhouse_user".to_string(),
                })?;
        let password = get_secret_or_param(
            Some(params),
            secret,
            "clickhouse_pass_key",
            "clickhouse_pass",
        )
        .ok_or(Error::MissingRequiredParameterForConnection {
            parameter_name: "clickhouse_pass".to_string(),
        })?;
        let host =
            params
                .get("clickhouse_host")
                .ok_or(Error::MissingRequiredParameterForConnection {
                    parameter_name: "clickhouse_tcp_host".to_string(),
                })?;
        let port = params.get("clickhouse_tcp_port").ok_or(
            Error::MissingRequiredParameterForConnection {
                parameter_name: "clickhouse_port".to_string(),
            },
        )?;
        let db =
            params
                .get("clickhouse_db")
                .ok_or(Error::MissingRequiredParameterForConnection {
                    parameter_name: "clickhouse_db".to_string(),
                })?;

        connection_string = Some(format!("tcp://{user}:{password}@{host}:{port}/{db}",));
    }

    let Some(connection_string) = connection_string else {
        return MissingRequiredParameterForConnectionSnafu {
            parameter_name: "clickhouse_connection_string".to_string(),
        }
        .fail()?;
    };

    let mut sanitized_connection_string =
        Url::parse(&connection_string).context(UnableToParseConnectionStringSnafu)?;
    sanitized_connection_string
        .set_password(None)
        .map_err(|_| Error::UnableToSanitizeConnectionString)?;

    let mut options =
        Options::from_str(&connection_string).context(InvalidConnectionStringSnafu)?;
    if !connection_string.contains("connection_timeout") {
        // Default timeout of 500ms is not enough in some cases.
        options = options.connection_timeout(DEFAULT_CONNECTION_TIMEOUT);
    }

    if let Some(connection_timeout) = params.get("clickhouse_connection_timeout") {
        let connection_timeout = connection_timeout
            .parse::<u64>()
            .context(InvalidConnectionTimeoutValueSnafu)?;
        options = options.connection_timeout(Duration::from_millis(connection_timeout));
    }

    let secure = params
        .get("clickhouse_secure")
        .map(|s| s.parse::<bool>())
        .transpose()
        .context(InvalidSecureParameterValueSnafu {
            parameter_name: "clickhouse_secure".to_string(),
        })?;
    options = options.secure(secure.unwrap_or(true));

    Ok((options, sanitized_connection_string.to_string()))
}

#[async_trait]
impl DbConnectionPool<ClientHandle, &'static (dyn Sync)> for ClickhouseConnectionPool {
    async fn connect(&self) -> Result<Box<dyn DbConnection<ClientHandle, &'static (dyn Sync)>>> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_handle().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(ClickhouseConnection::new(
            conn,
            Arc::clone(&self.pool),
        )))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.join_push_down_context.clone())
    }
}
