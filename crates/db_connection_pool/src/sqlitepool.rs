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
    file_name: String,
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
    pub fn new(
        name: &str,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let file_name = params
            .as_ref()
            .as_ref()
            .and_then(|params| params.get("sqlite_file").cloned())
            .unwrap_or(name.to_string());
        let file_name = format!("{file_name}_sqlite.db");

        Ok(SqliteConnectionPool { file_name, mode })
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
            Mode::File => Connection::open(self.file_name.clone())
                .await
                .context(ConnectionPoolSnafu)?,
        };
        Ok(Box::new(SqliteConnection::new(conn)))
    }
}
