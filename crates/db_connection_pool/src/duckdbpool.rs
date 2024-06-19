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

use std::sync::Arc;

use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager, ToSql};
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Result};
use crate::{
    dbconnection::{duckdbconn::DuckDbConnection, DbConnection, SyncDbConnection},
    JoinPushDown,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display("Unable to connect to DuckDB: {source}"))]
    UnableToConnect { source: duckdb::Error },
}

pub struct DuckDbConnectionPool {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    join_push_down: JoinPushDown,
}

impl DuckDbConnectionPool {
    /// Create a new `DuckDbConnectionPool` from memory.
    ///
    /// # Arguments
    ///
    /// * `access_mode` - The access mode for the connection pool
    ///
    /// # Returns
    ///
    /// * A new `DuckDbConnectionPool`
    ///
    /// # Errors
    ///
    /// * `DuckDBSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    /// * `UnableToConnectSnafu` - If there is an error connecting to the database
    pub fn new_memory(access_mode: &AccessMode) -> Result<Self> {
        let config = get_config(access_mode)?;
        let manager = DuckdbConnectionManager::memory_with_flags(config).context(DuckDBSnafu)?;
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            pool,
            // There can't be any other tables that share the same context for an in-memory DuckDB.
            join_push_down: JoinPushDown::Disallow,
        })
    }

    /// Create a new `DuckDbConnectionPool` from a file.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file
    /// * `access_mode` - The access mode for the connection pool
    ///
    /// # Returns
    ///
    /// * A new `DuckDbConnectionPool`
    ///
    /// # Errors
    ///
    /// * `DuckDBSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    /// * `UnableToConnectSnafu` - If there is an error connecting to the database
    pub fn new_file(path: &str, access_mode: &AccessMode) -> Result<Self> {
        let config = get_config(access_mode)?;
        let manager =
            DuckdbConnectionManager::file_with_flags(path, config).context(DuckDBSnafu)?;
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            pool,
            // Allow join-push down for any other instances that connect to the same underlying file.
            join_push_down: JoinPushDown::AllowedFor(path.to_string()),
        })
    }

    /// Create a new `DuckDbConnectionPool` from a database URL.
    ///
    /// # Errors
    ///
    /// * `DuckDBSnafu` - If there is an error creating the connection pool
    pub fn connect_sync(
        self: Arc<Self>,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(DuckDbConnection::new(conn)))
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
    for DuckDbConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(DuckDbConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn test_connection(conn: &r2d2::PooledConnection<DuckdbConnectionManager>) -> Result<()> {
    conn.execute("SELECT 1", []).context(UnableToConnectSnafu)?;
    Ok(())
}

fn get_config(access_mode: &AccessMode) -> Result<duckdb::Config> {
    let config = duckdb::Config::default()
        .access_mode(match access_mode {
            AccessMode::ReadOnly => duckdb::AccessMode::ReadOnly,
            AccessMode::ReadWrite => duckdb::AccessMode::ReadWrite,
            AccessMode::Automatic => duckdb::AccessMode::Automatic,
        })
        .context(DuckDBSnafu)?;

    Ok(config)
}
