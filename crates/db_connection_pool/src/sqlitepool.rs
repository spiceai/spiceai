use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use snafu::{prelude::*, ResultExt};
use tokio_rusqlite::{Connection, ToSql};

use super::{DbConnectionPool, Result};
use crate::dbconnection::{sqliteconn::SqliteConnection, AsyncDbConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: tokio_rusqlite::Error },

    #[snafu(display("No path provided for SQLite connection"))]
    NoPathError {},
}

pub struct SqliteConnectionPool {
    path: String,
}

impl SqliteConnectionPool {
    /// Creates a new instance of `SqliteConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(params: Arc<Option<HashMap<String, String>>>) -> Result<Self> {
        let Some(params) = params.as_ref() else {
            return NoPathSnafu.fail()?;
        };

        let Some(path) = params.get("path") else {
            return NoPathSnafu.fail()?;
        };

        Ok(SqliteConnectionPool {
            path: path.to_string(),
        })
    }
}

#[async_trait]
impl DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> for SqliteConnectionPool {
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>> {
        let conn = Connection::open(self.path.clone())
            .await
            .context(ConnectionPoolSnafu)?;
        Ok(Box::new(SqliteConnection::new(conn)))
    }
}
