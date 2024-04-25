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
use secrets::Secret;
use snafu::{ResultExt, Snafu};

use crate::dbconnection::{clickhouseconn::ClickhouseConnection, AsyncDbConnection, DbConnection};

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

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError {
        source: clickhouse_rs::errors::Error,
    },

    #[snafu(display("ParametersEmptyError"))]
    ParametersEmptyError {},

    #[snafu(display("Required parameter was not provided: {parameter_name}"))]
    NotEnoughParametersForConnection { parameter_name: String },

    #[snafu(display("Invalid root cert path: {path}"))]
    InvalidRootCertPathError { path: String },

    #[snafu(display("Invalid secure parameter value {parameter_name}"))]
    InvalidSecureParameterValueError {
        parameter_name: String,
        source: ParseBoolError,
    },
}

pub struct ClickhouseConnectionPool {
    pool: Arc<Pool>,
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
        let options = get_config_from_params(params, &secret)?;

        let pool = Pool::new(options);

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

fn get_config_from_params(
    params: &HashMap<String, String>,
    secret: &Option<Secret>,
) -> Result<Options> {
    let options: Option<Options>;
    if let Some(clickhouse_connection_string) = get_secret_or_param(
        params,
        secret,
        "clickhouse_connection_string_key",
        "clickhouse_connection_string",
    ) {
        let mut new_options = Options::from_str(&clickhouse_connection_string)
            .context(InvalidConnectionStringSnafu)?;
        if !clickhouse_connection_string.contains("connection_timeout") {
            // Default timeout of 500ms is not enough in some cases.
            new_options = new_options.connection_timeout(DEFAULT_CONNECTION_TIMEOUT);
        }
        options = Some(new_options);
    } else {
        let mut connection_opts: Vec<String> = vec![];
        let keys = [
            "clickhouse_user",
            "clickhouse_host",
            "clickhouse_tcp_port",
            "clickhouse_db",
        ];
        for key in &keys {
            let value = params.get(*key);
            match value {
                Some(value) => connection_opts.push(value.clone()),
                None => NotEnoughParametersForConnectionSnafu {
                    parameter_name: (*key).to_string(),
                }
                .fail()?,
            }
        }
        let password =
            get_secret_or_param(params, secret, "clickhouse_pass_key", "clickhouse_pass");
        match password {
            Some(password) => connection_opts.insert(1, password),
            None => NotEnoughParametersForConnectionSnafu {
                parameter_name: "clickhouse_pass".to_string(),
            }
            .fail()?,
        }
        let connection_string = format!(
            "tcp://{}:{}@{}:{}/{}",
            connection_opts[0],
            connection_opts[1],
            connection_opts[2],
            connection_opts[3],
            connection_opts[4]
        );
        // Default timeout of 500ms is not enough
        let new_options = Options::from_str(&connection_string)
            .context(InvalidConnectionStringSnafu)?
            .connection_timeout(DEFAULT_CONNECTION_TIMEOUT);
        options = Some(new_options);
    }
    let mut options = options.ok_or(Error::NotEnoughParametersForConnection {
        parameter_name: "clickhouse_connection_string".to_string(),
    })?;
    let secure = params
        .get("clickhouse_secure")
        .map(|s| s.parse::<bool>())
        .transpose()
        .context(InvalidSecureParameterValueSnafu {
            parameter_name: "clickhouse_secure".to_string(),
        })?;
    options = options.secure(secure.unwrap_or(true));

    if let Some(clickhouse_sslrootcert) = params.get("clickhouse_sslrootcert") {
        if !std::path::Path::new(clickhouse_sslrootcert).exists() {
            InvalidRootCertPathSnafu {
                path: clickhouse_sslrootcert,
            }
            .fail()?;
        }
    }

    Ok(options)
}

#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn get_secret_or_param(
    params: &HashMap<String, String>,
    secret: &Option<Secret>,
    secret_param_key: &str,
    param_key: &str,
) -> Option<String> {
    let clickhouse_secret_param_val = match params.get(secret_param_key) {
        Some(val) => val,
        None => param_key,
    };

    if let Some(secrets) = secret {
        if let Some(clickhouse_secret_val) = secrets.get(clickhouse_secret_param_val) {
            return Some(clickhouse_secret_val.to_string());
        };
    };

    if let Some(clickhouse_secret_val) = params.get(param_key) {
        return Some(clickhouse_secret_val.to_string());
    };

    None
}

#[async_trait]
impl DbConnectionPool<ClientHandle, &'static (dyn Sync)> for ClickhouseConnectionPool {
    async fn connect(&self) -> Result<Box<dyn DbConnection<ClientHandle, &'static (dyn Sync)>>> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_handle().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(ClickhouseConnection::new(conn)))
    }
}
