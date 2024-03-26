use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::{config::Host, types::ToSql, Config, NoTls},
    PostgresConnectionManager,
};
use ns_lookup::verify_ns_lookup_and_tcp_connect;
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

    #[snafu(display("Invalid port: {port}"))]
    InvalidPortError { port: String },
}

pub struct PostgresConnectionPool {
    pool: Arc<bb8::Pool<PostgresConnectionManager<NoTls>>>,
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
            }
        }

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

        let manager = PostgresConnectionManager::new(config, NoTls);

        let pool = bb8::Pool::builder()
            .build(manager)
            .await
            .context(ConnectionPoolSnafu)?;

        Ok(PostgresConnectionPool {
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
        bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
        &'static (dyn ToSql + Sync),
    > for PostgresConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<
            dyn DbConnection<
                bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
                &'static (dyn ToSql + Sync),
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_owned().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(PostgresConnection::new(conn)))
    }
}
