use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bb8_sqlite::RusqliteConnectionManager;
use rusqlite::ToSql;
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Result};
use crate::dbconnection::{sqliteconn::SqliteConnection, DbConnection, SyncDbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: bb8_sqlite::Error },

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError {
        source: bb8::RunError<bb8_sqlite::Error>,
    },

    #[snafu(display("No path provided for SQLite connection"))]
    NoPathError {},
}

pub struct SqliteConnectionPool {
    pool: Arc<bb8::Pool<RusqliteConnectionManager>>,
}

impl SqliteConnectionPool {
    /// Creates a new instance of `SqliteConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: Arc<Option<HashMap<String, String>>>) -> Result<Self> {
        let Some(params) = params.as_ref() else {
            return NoPathSnafu.fail()?;
        };

        let Some(path) = params.get("path") else {
            return NoPathSnafu.fail()?;
        };

        let manager = RusqliteConnectionManager::new(path);
        let pool = bb8::Pool::builder().build(manager).await?;

        Ok(SqliteConnectionPool {
            pool: Arc::new(pool),
        })
    }
}

#[async_trait]
impl DbConnectionPool<bb8::PooledConnection<'static, RusqliteConnectionManager>, &'static dyn ToSql>
    for SqliteConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<
            dyn DbConnection<
                bb8::PooledConnection<'static, RusqliteConnectionManager>,
                &'static dyn ToSql,
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_owned().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(SqliteConnection::new(conn)))
    }
}
