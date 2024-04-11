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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use mysql_async::{prelude::ToValue, SslOpts};
use secrets::Secret;
use snafu::{ResultExt, Snafu};

use crate::dbconnection::{mysqlconn::MySQLConnection, AsyncDbConnection, DbConnection};

use super::{DbConnectionPool, Result};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: mysql_async::UrlError },

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError { source: mysql_async::Error },

    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Invalid root cert path: {path}"))]
    InvalidRootCertPathError { path: String },
}

pub struct MySQLConnectionPool {
    pool: Arc<mysql_async::Pool>,
}

impl MySQLConnectionPool {
    // Creates a new instance of `MySQLConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::unused_async)]
    pub async fn new(
        params: Arc<Option<HashMap<String, String>>>,
        secret: Option<Secret>,
    ) -> Result<Self> {
        let mut connection_string = mysql_async::OptsBuilder::default();
        let mut ssl_mode = "required";
        let mut ssl_rootcert_path: Option<PathBuf> = None;

        if let Some(params) = params.as_ref() {
            if let Some(mysql_connection_string) = get_secret_or_param(
                params,
                &secret,
                "mysql_connection_string_key",
                "mysql_connection_string",
            ) {
                connection_string = mysql_async::OptsBuilder::from_opts(
                    mysql_async::Opts::from_url(mysql_connection_string.as_str())?,
                );
            } else {
                if let Some(mysql_host) = params.get("mysql_host") {
                    connection_string = connection_string.ip_or_hostname(mysql_host.as_str());
                }
                if let Some(mysql_user) = params.get("mysql_user") {
                    connection_string = connection_string.user(Some(mysql_user));
                }
                if let Some(mysql_db) = params.get("mysql_db") {
                    connection_string = connection_string.db_name(Some(mysql_db));
                }
                if let Some(mysql_pass) =
                    get_secret_or_param(params, &secret, "mysql_pass_key", "mysql_pass")
                {
                    connection_string = connection_string.pass(Some(mysql_pass));
                }
                if let Some(mysql_port) = params.get("mysql_port") {
                    connection_string =
                        connection_string.tcp_port(mysql_port.parse::<u16>().unwrap_or(3306));
                }
                if let Some(mysql_sslmode) = params.get("mysql_sslmode") {
                    match mysql_sslmode.to_lowercase().as_str() {
                        "disabled" | "required" | "preferred" => {
                            ssl_mode = mysql_sslmode.as_str();
                        }
                        _ => {
                            InvalidParameterSnafu {
                                parameter_name: "mysql_sslmode".to_string(),
                            }
                            .fail()?;
                        }
                    }
                }
                if let Some(mysql_sslrootcert) = params.get("mysql_sslrootcert") {
                    if !std::path::Path::new(mysql_sslrootcert).exists() {
                        InvalidRootCertPathSnafu {
                            path: mysql_sslrootcert,
                        }
                        .fail()?;
                    }

                    ssl_rootcert_path = Some(PathBuf::from(mysql_sslrootcert));
                }
            }
        }

        let ssl_opts = get_ssl_opts(ssl_mode, ssl_rootcert_path);

        connection_string = connection_string.ssl_opts(ssl_opts);

        let opts = mysql_async::Opts::from(connection_string);

        let pool = mysql_async::Pool::new(opts);

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

fn get_ssl_opts(ssl_mode: &str, rootcert_path: Option<PathBuf>) -> Option<SslOpts> {
    if ssl_mode == "disabled" {
        return None;
    }

    let mut opts = SslOpts::default();

    if let Some(rootcert_path) = rootcert_path {
        let path = rootcert_path;
        opts = opts.with_root_certs(vec![path.into()]);
    }

    // If ssl_mode is "preferred", we will accept invalid certs and skip domain validation
    // mysql_async does not have a "ssl_mode" https://github.com/blackbeam/mysql_async/issues/225#issuecomment-1409922237
    if ssl_mode == "preferred" {
        opts = opts
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }

    Some(opts)
}

#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn get_secret_or_param(
    params: &HashMap<String, String>,
    secret: &Option<Secret>,
    secret_param_key: &str,
    param_key: &str,
) -> Option<String> {
    let mysql_secret_param_val = match params.get(secret_param_key) {
        Some(val) => val,
        None => param_key,
    };

    if let Some(secrets) = secret {
        if let Some(mysql_secret_val) = secrets.get(mysql_secret_param_val) {
            return Some(mysql_secret_val.to_string());
        };
    };

    if let Some(mysql_secret_val) = params.get(param_key) {
        return Some(mysql_secret_val.to_string());
    };

    None
}

#[async_trait]
impl DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> for MySQLConnectionPool {
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<mysql_async::Conn, &'static (dyn ToValue + Sync)>>> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_conn().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(MySQLConnection::new(conn)))
    }
}
