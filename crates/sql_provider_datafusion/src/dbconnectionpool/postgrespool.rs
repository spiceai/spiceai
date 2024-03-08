use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::{types::ToSql, NoTls},
    PostgresConnectionManager,
};
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Mode, Result};
use crate::dbconnection::{postgresconn::PostgresConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

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

#[async_trait]
impl
    DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
        &'static (dyn ToSql + Sync),
    > for PostgresConnectionPool
{
    async fn new(
        _name: &str,
        _mode: Mode,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let mut host = "localhost";
        let mut user = "postrgres";
        let mut dbname = "postgres";
        let mut connection_string = String::new();

        if let Some(params) = _params.as_ref() {
            if let Some(pg_host) = params.get("pg_host") {
                host = pg_host;
            }
            if let Some(pg_user) = params.get("pg_user") {
                user = pg_user;
            }
            if let Some(pg_db) = params.get("pg_db") {
                dbname = pg_db;
            }
            if let Some(pg_pass) = params.get("pg_pass") {
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
