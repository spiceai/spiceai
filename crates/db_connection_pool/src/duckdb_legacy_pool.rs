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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use duckdb_0_9_2::{DuckdbConnectionManager, ToSql};
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Result};
use crate::dbconnection::{
    duckdb_legacy_conn::LegacyDuckDbConnection, DbConnection, SyncDbConnection,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb_0_9_2::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display("Missing required parameter: open"))]
    MissingDuckDBFile {},
}

pub struct LegacyDuckDbConnectionPool {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
}

impl LegacyDuckDbConnectionPool {
    /// Create a new `DuckDbConnectionPool` from data connector params.
    ///
    /// # Arguments
    ///
    /// * `params` - Data connector parameters for the connection pool.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub fn new_with_file_mode(params: &Arc<Option<HashMap<String, String>>>) -> Result<Self> {
        let path = params
            .as_ref()
            .as_ref()
            .and_then(|params| params.get("open").cloned())
            .ok_or(Error::MissingDuckDBFile {})?;

        let manager = DuckdbConnectionManager::file(path).context(DuckDBSnafu)?;
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        // TODO SG: review if this is required
        // let conn = pool.get().context(ConnectionPoolSnafu)?;
        // conn.register_table_function::<ArrowVTab>("arrow")
        //     .context(DuckDBSnafu)?;

        Ok(LegacyDuckDbConnectionPool { pool })
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
    for LegacyDuckDbConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(LegacyDuckDbConnection::new(conn)))
    }
}
