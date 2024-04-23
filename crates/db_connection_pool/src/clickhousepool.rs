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

use std::{collections::HashMap, str::FromStr, sync::Arc};

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

    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },
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
        let mut options = Options::default();
        let mut host: &str = "localhost";
        let mut user: &str = "default";
        let mut db: &str = "default";
        let mut pass: String = "password".to_string();
        let mut tcp_port: &str = "9000";

        if let Some(params) = params.as_ref() {
            if let Some(clickhouse_connection_string) = get_secret_or_param(
                params,
                &secret,
                "clickhouse_connection_string_key",
                "clickhouse_connection_string",
            ) {
                options = Options::from_str(&clickhouse_connection_string)
                    .context(InvalidConnectionStringSnafu)?;
            } else {
                if let Some(clickhouse_host) = params.get("clickhouse_host") {
                    host = clickhouse_host;
                }
                if let Some(clickhouse_user) = params.get("clickhouse_user") {
                    user = clickhouse_user;
                }
                if let Some(clickhouse_db) = params.get("clickhouse_db") {
                    db = clickhouse_db;
                }
                if let Some(clickhouse_pass) =
                    get_secret_or_param(params, &secret, "clickhouse_pass_key", "clickhouse_pass")
                {
                    pass = clickhouse_pass;
                }
                if let Some(clickhouse_tcp_port) = params.get("clickhouse_tcp_port") {
                    tcp_port = clickhouse_tcp_port;
                }
                let connection_string = format!("tcp://{user}:{pass}@{host}:{tcp_port}/{db}");
                options =
                    Options::from_str(&connection_string).context(InvalidConnectionStringSnafu)?;
            }
        }

        let pool = Pool::new(options);

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
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
