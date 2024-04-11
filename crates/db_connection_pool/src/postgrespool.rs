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

use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use bb8::ErrorSink;
use bb8_postgres::{
    tokio_postgres::{config::Host, types::ToSql, Config},
    PostgresConnectionManager,
};
use native_tls::{Certificate, TlsConnector};
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use postgres_native_tls::MakeTlsConnector;
use secrets::Secret;
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Result};
use crate::dbconnection::{postgresconn::PostgresConnection, AsyncDbConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError {
        source: bb8::RunError<bb8_postgres::tokio_postgres::Error>,
    },

    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Invalid port: {port}"))]
    InvalidPortError { port: String },

    #[snafu(display("Invalid root cert path: {path}"))]
    InvalidRootCertPathError { path: String },

    #[snafu(display("Failed to read cert : {source}"))]
    FailedToReadCertError { source: std::io::Error },

    #[snafu(display("Failed to parse cert : {source}"))]
    FailedToParseCertError { source: pem::PemError },

    #[snafu(display("Failed to load cert : {source}"))]
    FailedToLoadCertError { source: native_tls::Error },
}

pub struct PostgresConnectionPool {
    pool: Arc<bb8::Pool<PostgresConnectionManager<MakeTlsConnector>>>,
}

impl PostgresConnectionPool {
    /// Creates a new instance of `PostgresConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(
        params: Arc<Option<HashMap<String, String>>>,
        secret: Option<Secret>,
    ) -> Result<Self> {
        let mut connection_string = "host=localhost user=postgres dbname=postgres".to_string();
        let mut accept_invalid_certs = false;
        let mut ssl_mode = "require";
        let mut ssl_rootcert_path: Option<PathBuf> = None;

        if let Some(params) = params.as_ref() {
            connection_string = String::new();

            if let Some(pg_connection_string) = get_secret_or_param(
                params,
                &secret,
                "pg_connection_string_key",
                "pg_connection_string",
            ) {
                connection_string.push_str(pg_connection_string.as_str());
            } else {
                if let Some(pg_host) = params.get("pg_host") {
                    connection_string.push_str(format!("host={pg_host} ").as_str());
                }
                if let Some(pg_user) = params.get("pg_user") {
                    connection_string.push_str(format!("user={pg_user} ").as_str());
                }
                if let Some(pg_db) = params.get("pg_db") {
                    connection_string.push_str(format!("dbname={pg_db} ").as_str());
                }
                if let Some(pg_pass) =
                    get_secret_or_param(params, &secret, "pg_pass_key", "pg_pass")
                {
                    connection_string.push_str(format!("password={pg_pass} ").as_str());
                }
                if let Some(pg_port) = params.get("pg_port") {
                    connection_string.push_str(format!("port={pg_port} ").as_str());
                }
                if let Some(pg_sslmode) = params.get("pg_sslmode") {
                    match pg_sslmode.to_lowercase().as_str() {
                        "disable" | "require" | "prefer" => {
                            ssl_mode = pg_sslmode.as_str();
                        }
                        _ => {
                            InvalidParameterSnafu {
                                parameter_name: "pg_sslmode".to_string(),
                            }
                            .fail()?;
                        }
                    }
                }
                if let Some(pg_insecure) = params.get("pg_insecure") {
                    accept_invalid_certs = pg_insecure == "true";
                }
                if let Some(pg_sslrootcert) = params.get("pg_sslrootcert") {
                    if !std::path::Path::new(pg_sslrootcert).exists() {
                        InvalidRootCertPathSnafu {
                            path: pg_sslrootcert,
                        }
                        .fail()?;
                    }

                    ssl_rootcert_path = Some(PathBuf::from(pg_sslrootcert));
                }
            }
        }

        connection_string.push_str(format!("sslmode={ssl_mode} ").as_str());
        let config = Config::from_str(connection_string.as_str()).context(ConnectionPoolSnafu)?;

        for host in config.get_hosts() {
            for port in config.get_ports() {
                if let Host::Tcp(host) = host {
                    if let Err(e) = verify_ns_lookup_and_tcp_connect(host, *port).await {
                        tracing::error!("{e}");
                    }
                }
            }
        }

        let mut cert: Option<Certificate> = None;

        if let Some(path) = ssl_rootcert_path {
            let buf = tokio::fs::read(path).await.context(FailedToReadCertSnafu)?;
            let pem = pem::parse(buf).context(FailedToParseCertSnafu)?;
            cert = Some(
                Certificate::from_pem(pem::encode(&pem).as_bytes())
                    .context(FailedToLoadCertSnafu)?,
            );
        }

        let tls_connector = get_tls_connector(ssl_mode, accept_invalid_certs, cert)?;
        let connector = MakeTlsConnector::new(tls_connector);
        let manager = PostgresConnectionManager::new(config, connector);
        let error_sink = PostgresErrorSink::new();

        let pool = bb8::Pool::builder()
            .error_sink(Box::new(error_sink))
            .build(manager)
            .await
            .context(ConnectionPoolSnafu)?;

        Ok(PostgresConnectionPool {
            pool: Arc::new(pool),
        })
    }
}

fn get_tls_connector(
    ssl_mode: &str,
    allow_insecure: bool,
    rootcert: Option<Certificate>,
) -> native_tls::Result<TlsConnector> {
    if ssl_mode == "disable" {
        return TlsConnector::builder().build();
    }

    if let Some(cert) = rootcert {
        return TlsConnector::builder()
            .danger_accept_invalid_certs(allow_insecure)
            .add_root_certificate(cert)
            .build();
    }

    TlsConnector::builder()
        .danger_accept_invalid_certs(allow_insecure)
        .build()
}

#[derive(Debug, Clone, Copy)]
struct PostgresErrorSink {}

impl PostgresErrorSink {
    pub fn new() -> Self {
        PostgresErrorSink {}
    }
}

impl<E> ErrorSink<E> for PostgresErrorSink
where
    E: std::fmt::Debug,
    E: std::fmt::Display,
{
    fn sink(&self, error: E) {
        tracing::error!("Postgres Connection Error: {:?}", error);
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<E>> {
        Box::new(*self)
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
    let pg_secret_param_val = match params.get(secret_param_key) {
        Some(val) => val,
        None => param_key,
    };

    if let Some(secrets) = secret {
        if let Some(pg_secret_val) = secrets.get(pg_secret_param_val) {
            return Some(pg_secret_val.to_string());
        };
    };

    if let Some(pg_param_val) = params.get(param_key) {
        return Some(pg_param_val.to_string());
    };

    None
}

#[async_trait]
impl
    DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'static (dyn ToSql + Sync),
    > for PostgresConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<
            dyn DbConnection<
                bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
                &'static (dyn ToSql + Sync),
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_owned().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(PostgresConnection::new(conn)))
    }
}
