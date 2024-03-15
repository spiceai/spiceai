use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use snafu::{prelude::*, ResultExt};
use tokio_rusqlite::{Connection, ToSql};

use super::{DbConnectionPool, Result};
use crate::{
    dbconnection::{sqliteconn::SqliteConnection, AsyncDbConnection, DbConnection},
    Mode,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: tokio_rusqlite::Error },

    #[snafu(display("No path provided for SQLite connection"))]
    NoPathError {},
}

pub struct SqliteConnectionPool {
    path: String,
    mode: Mode,
}

impl SqliteConnectionPool {
    /// Creates a new instance of `SqliteConnectionPool`.
    ///
    /// NOTE: The `SqliteConnectionPool` currently does no connection pooling, it simply creates a new connection
    /// on each call to `connect()`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(params: Arc<Option<HashMap<String, String>>>, mode: Mode) -> Result<Self> {
        let mut path = String::default();
        if let Some(params) = params.as_ref() {
            if let Some(path_val) = params.get("path") {
                path = path_val.to_string();
            };
        };

        Ok(SqliteConnectionPool { path, mode })
    }
}

#[async_trait]
impl DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> for SqliteConnectionPool {
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>> {
        let conn = match self.mode {
            Mode::Memory => Connection::open_in_memory()
                .await
                .context(ConnectionPoolSnafu)?,
            Mode::File => Connection::open(self.path.clone())
                .await
                .context(ConnectionPoolSnafu)?,
        };
        Ok(Box::new(SqliteConnection::new(conn)))
    }
}
