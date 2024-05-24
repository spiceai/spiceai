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
use secrets::{get_secret_or_param, Secret};
use snafu::{prelude::*, ResultExt};
use tokio_postgres;

use super::DbConnectionPool;
use crate::{
    dbconnection::{postgresconn::PostgresConnection, AsyncDbConnection, DbConnection},
    JoinPushDown,
};

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

    #[snafu(display("Cannot connect to PostgreSQL on {host}:{port}. Ensure that the host and port are correctly configured, and that the host is reachable."))]
    InvalidHostOrPortError {
        source: ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display("Invalid root cert path: {path}"))]
    InvalidRootCertPathError { path: String },

    #[snafu(display("Failed to read cert : {source}"))]
    FailedToReadCertError { source: std::io::Error },

    #[snafu(display("Failed to load cert : {source}"))]
    FailedToLoadCertError { source: native_tls::Error },

    #[snafu(display("Failed to build tls connector : {source}"))]
    FailedToBuildTlsConnectorError { source: native_tls::Error },

    #[snafu(display("Postgres connection error: {source}"))]
    PostgresConnectionError { source: tokio_postgres::Error },

    #[snafu(display(
        "Authentication failed. Ensure that the username and password are correctly configured."
    ))]
    InvalidUsernameOrPassword { source: tokio_postgres::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct PostgresConnectionPool {
    pool: Arc<bb8::Pool<PostgresConnectionManager<MakeTlsConnector>>>,
    join_push_down: JoinPushDown,
}

impl PostgresConnectionPool {
    /// Creates a new instance of `PostgresConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: Arc<HashMap<String, String>>, secret: Option<Secret>) -> Result<Self> {
        let mut connection_string = String::new();
        let mut ssl_mode = "verify-full".to_string();
        let mut ssl_rootcert_path: Option<PathBuf> = None;

        if let Some(pg_connection_string) = get_secret_or_param(
            &params,
            &secret,
            "pg_connection_string_key",
            "pg_connection_string",
        ) {
            let (str, mode, cert_path) = parse_connection_string(pg_connection_string.as_str());
            connection_string = str;
            ssl_mode = mode;
            if let Some(cert_path) = cert_path {
                let sslrootcert = cert_path.as_str();
                ensure!(
                    std::path::Path::new(sslrootcert).exists(),
                    InvalidRootCertPathSnafu { path: cert_path }
                );
                ssl_rootcert_path = Some(PathBuf::from(sslrootcert));
            }
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
            if let Some(pg_pass) = get_secret_or_param(&params, &secret, "pg_pass_key", "pg_pass") {
                connection_string.push_str(format!("password={pg_pass} ").as_str());
            }
            if let Some(pg_port) = params.get("pg_port") {
                connection_string.push_str(format!("port={pg_port} ").as_str());
            }
            if let Some(pg_sslmode) = params.get("pg_sslmode") {
                match pg_sslmode.to_lowercase().as_str() {
                    "disable" | "require" | "prefer" | "verify-ca" | "verify-full" => {
                        ssl_mode = pg_sslmode.to_string();
                    }
                    _ => {
                        InvalidParameterSnafu {
                            parameter_name: "pg_sslmode".to_string(),
                        }
                        .fail()?;
                    }
                }
            }
            if let Some(pg_sslrootcert) = params.get("pg_sslrootcert") {
                ensure!(
                    std::path::Path::new(pg_sslrootcert).exists(),
                    InvalidRootCertPathSnafu {
                        path: pg_sslrootcert,
                    }
                );

                ssl_rootcert_path = Some(PathBuf::from(pg_sslrootcert));
            }
        }

        let mode = match ssl_mode.as_str() {
            "disable" => "disable",
            "prefer" => "prefer",
            // tokio_postgres supports only disable, require and prefer
            _ => "require",
        };

        connection_string.push_str(format!("sslmode={mode} ").as_str());
        let config = Config::from_str(connection_string.as_str()).context(ConnectionPoolSnafu)?;
        verify_postgres_config(&config).await?;

        let mut certs: Option<Vec<Certificate>> = None;

        if let Some(path) = ssl_rootcert_path {
            let buf = tokio::fs::read(path).await.context(FailedToReadCertSnafu)?;
            certs = Some(parse_certs(&buf)?);
        }

        let tls_connector = get_tls_connector(ssl_mode.as_str(), certs)?;
        let connector = MakeTlsConnector::new(tls_connector);
        test_postgres_connection(connection_string.as_str(), connector.clone()).await?;

        let join_push_down = get_join_context(&config);

        let manager = PostgresConnectionManager::new(config, connector);
        let error_sink = PostgresErrorSink::new();

        let pool = bb8::Pool::builder()
            .error_sink(Box::new(error_sink))
            .build(manager)
            .await
            .context(ConnectionPoolSnafu)?;

        // Test the connection
        let conn = pool.get().await.context(ConnectionPoolRunSnafu)?;
        conn.execute("SELECT 1", &[])
            .await
            .context(ConnectionPoolSnafu)?;

        Ok(PostgresConnectionPool {
            pool: Arc::new(pool.clone()),
            join_push_down,
        })
    }
}

fn parse_connection_string(pg_connection_string: &str) -> (String, String, Option<String>) {
    let mut connection_string = String::new();
    let mut ssl_mode = "verify-full".to_string();
    let mut ssl_rootcert_path: Option<String> = None;

    let str = pg_connection_string;
    let str_params: Vec<&str> = str.split_whitespace().collect();
    for param in str_params {
        let param = param.split('=').collect::<Vec<&str>>();
        if let (Some(&name), Some(&value)) = (param.first(), param.get(1)) {
            match name {
                "sslmode" => {
                    ssl_mode = value.to_string();
                }
                "sslrootcert" => {
                    ssl_rootcert_path = Some(value.to_string());
                }
                _ => {
                    connection_string.push_str(format!("{name}={value} ").as_str());
                }
            }
        }
    }

    (connection_string, ssl_mode, ssl_rootcert_path)
}

fn get_join_context(config: &Config) -> JoinPushDown {
    let mut join_push_context_str = String::new();
    for host in config.get_hosts() {
        join_push_context_str.push_str(&format!("host={host:?},"));
    }
    if !config.get_ports().is_empty() {
        join_push_context_str.push_str(&format!("port={port},", port = config.get_ports()[0]));
    }
    if let Some(dbname) = config.get_dbname() {
        join_push_context_str.push_str(&format!("db={dbname},"));
    }
    if let Some(user) = config.get_user() {
        join_push_context_str.push_str(&format!("user={user},"));
    }

    JoinPushDown::AllowedFor(join_push_context_str)
}

async fn test_postgres_connection(
    connection_string: &str,
    connector: MakeTlsConnector,
) -> Result<()> {
    match tokio_postgres::connect(connection_string, connector).await {
        Ok(_) => Ok(()),
        Err(err) => {
            if let Some(code) = err.code() {
                if *code == tokio_postgres::error::SqlState::INVALID_PASSWORD {
                    return Err(Error::InvalidUsernameOrPassword { source: err });
                }
            }

            Err(Error::PostgresConnectionError { source: err })
        }
    }
}

async fn verify_postgres_config(config: &Config) -> Result<()> {
    for host in config.get_hosts() {
        for port in config.get_ports() {
            if let Host::Tcp(host) = host {
                verify_ns_lookup_and_tcp_connect(host, *port)
                    .await
                    .context(InvalidHostOrPortSnafu { host, port: *port })?;
            }
        }
    }

    Ok(())
}

fn get_tls_connector(ssl_mode: &str, rootcerts: Option<Vec<Certificate>>) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    if ssl_mode == "disable" {
        return builder.build().context(FailedToBuildTlsConnectorSnafu);
    }

    if let Some(certs) = rootcerts {
        for cert in certs {
            builder.add_root_certificate(cert);
        }
    }

    builder
        .danger_accept_invalid_hostnames(ssl_mode != "verify-full")
        .danger_accept_invalid_certs(ssl_mode != "verify-full" && ssl_mode != "verify-ca")
        .build()
        .context(FailedToBuildTlsConnectorSnafu)
}

fn parse_certs(buf: &[u8]) -> Result<Vec<Certificate>> {
    Certificate::from_der(buf)
        .map(|x| vec![x])
        .or_else(|_| {
            pem::parse_many(buf)
                .unwrap_or_default()
                .iter()
                .map(pem::encode)
                .map(|s| Certificate::from_pem(s.as_bytes()))
                .collect()
        })
        .context(FailedToLoadCertSnafu)
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

#[async_trait]
impl
    DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'static (dyn ToSql + Sync),
    > for PostgresConnectionPool
{
    async fn connect(
        &self,
    ) -> super::Result<
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

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
