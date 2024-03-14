use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::{types::ToSql, NoTls},
    PostgresConnectionManager,
};
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
        let mut host = "localhost";
        let mut user = "postgres";
        let mut dbname = "postgres";
        let mut connection_string = String::new();

        if let Some(params) = params.as_ref() {
            if let Some(pg_host) = params.get("pg_host") {
                host = pg_host;
            }
            if let Some(pg_user) = params.get("pg_user") {
                user = pg_user;
            }
            if let Some(pg_db) = params.get("pg_db") {
                dbname = pg_db;
            }
            if let Some(pg_pass) = get_pg_pass(params, secret) {
                connection_string.push_str(format!("password={pg_pass} ").as_str());
            }
            if let Some(pg_port) = params.get("pg_port") {
                connection_string.push_str(format!("port={pg_port} ").as_str());
            }
        }

        connection_string.push_str(format!("host={host} user={user} dbname={dbname}").as_str());

        let manager = PostgresConnectionManager::new_from_stringlike(connection_string, NoTls)
            .context(ConnectionPoolSnafu)?;

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
pub fn get_pg_pass(params: &HashMap<String, String>, secret: Option<Secret>) -> Option<String> {
    if let Some(pg_pass_val) = params.get("pg_pass_key") {
        if let Some(secrets) = secret {
            if let Some(pg_pass_secret) = secrets.get(pg_pass_val) {
                return Some(pg_pass_secret.to_string());
            };
        };
    };

    if let Some(pg_raw_pass) = params.get("pg_pass") {
        return Some(pg_raw_pass.to_string());
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
