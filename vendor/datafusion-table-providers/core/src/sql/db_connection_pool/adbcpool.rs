// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use adbc_core::{Connection, Database};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use r2d2_adbc::AdbcConnectionManager;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use snafu::{prelude::*, ResultExt};
use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::db_connection_pool::dbconnection::{
    adbcconn::AdbcDbConnection, DbConnection, SyncDbConnection,
};

use super::{DbConnectionPool, JoinPushDown, Result};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ADBC Database initialization failed.\n{source}"))]
    AdbcInitializationError { source: adbc_core::error::Error },

    #[snafu(display("ADBC Connection failed to execute.\n{source}"))]
    ConnectionExecutionError { source: adbc_core::error::Error },

    #[snafu(display(
        "ADBC connection failed.\n{source}\nAdjust the connection pool parameters or sufficient capacity."
    ))]
    ConnectionPoolError { source: r2d2::Error },
}

pub struct AdbcConnectionPoolBuilder<D>
where
    D: Database + Send,
    D::ConnectionType: Connection + Send + Sync,
{
    database: D,
    connection_options: Option<HashMap<String, String>>,
    max_size: Option<u32>,
    min_idle: Option<u32>,
}

pub fn hash_db_options(
    driver: &str,
    db_options: &HashMap<String, SecretString>,
    conn_options: &HashMap<String, String>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(driver.as_bytes());
    db_options.iter().for_each(|(key, value)| {
        hasher.update(key.as_bytes());
        hasher.update(value.expose_secret().as_bytes());
    });
    conn_options.iter().for_each(|(key, value)| {
        hasher.update(key.as_bytes());
        hasher.update(value.as_bytes());
    });
    hasher.finalize().iter().fold(String::new(), |mut hash, b| {
        hash.push_str(&format!("{b:02x}"));
        hash
    })
}

impl<D> AdbcConnectionPoolBuilder<D>
where
    D: Database + Send,
    D::ConnectionType: Connection + Send + Sync,
{
    pub fn new(database: D) -> Self {
        Self {
            database,
            connection_options: None,
            max_size: None,
            min_idle: None,
        }
    }

    pub fn with_conn_options<I: IntoIterator<Item = (String, String)>>(
        mut self,
        connection_options: I,
    ) -> Self {
        if let Some(existing) = &mut self.connection_options {
            existing.extend(connection_options);
        } else {
            self.connection_options = Some(connection_options.into_iter().collect());
        }

        self
    }

    pub fn with_max_size(mut self, size: Option<u32>) -> Self {
        self.max_size = size;
        self
    }

    pub fn with_min_idle(mut self, size: Option<u32>) -> Self {
        self.min_idle = size;
        self
    }

    pub fn build(self) -> Result<ADBCPool<D>> {
        let mut pool_builder = r2d2::Pool::builder();

        if let Some(size) = self.max_size {
            pool_builder = pool_builder.max_size(size);
        }
        if self.min_idle.is_some() {
            pool_builder = pool_builder.min_idle(self.min_idle);
        }

        // let opts = ;
        let database = self.database;
        let manager: AdbcConnectionManager<D>;
        if let Some(opts) = self.connection_options {
            manager = AdbcConnectionManager::with_options(database, opts);
        } else {
            manager = AdbcConnectionManager::new(database);
        }
        let pool = Arc::new(pool_builder.build(manager).context(ConnectionPoolSnafu)?);
        Ok(ADBCPool { pool })
    }
}

#[derive(Clone)]
pub struct ADBCPool<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    pool: Arc<r2d2::Pool<AdbcConnectionManager<D>>>,
}

impl<D> std::fmt::Debug for ADBCPool<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ADBCPool")
    }
}

impl<D> ADBCPool<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    pub fn new(db: D, conn_options: Option<HashMap<String, String>>) -> Result<Self> {
        let builder = AdbcConnectionPoolBuilder::new(db);
        let builder = if let Some(opts) = conn_options {
            builder.with_conn_options(opts)
        } else {
            builder
        };
        builder.build()
    }

    pub fn connect_sync(
        self: Arc<Self>,
    ) -> Result<Box<dyn DbConnection<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>>>
    {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<AdbcConnectionManager<D>> =
            pool.get().context(ConnectionPoolSnafu)?;

        Ok(Box::new(AdbcDbConnection::new(conn)))
    }
}

#[async_trait]
impl<D> DbConnectionPool<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>
    for ADBCPool<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>>>
    {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<AdbcConnectionManager<D>> =
            pool.get().context(ConnectionPoolSnafu)?;

        Ok(Box::new(AdbcDbConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        // TODO: make this configurable
        JoinPushDown::Disallow
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use adbc_core::options::{AdbcVersion, OptionDatabase};
    use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
    use adbc_driver_manager::{ManagedDatabase, ManagedDriver};

    fn try_get_db() -> Result<ManagedDatabase, adbc_core::error::Error> {
        let mut driver = ManagedDriver::load_from_name(
            "duckdb",
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )?;

        driver.new_database_with_opts([(
            OptionDatabase::Other("path".to_string()),
            ":memory:".into(),
        )])
    }

    #[tokio::test]
    async fn test_adbc_connection_pool() {
        let db = match try_get_db() {
            Ok(db) => db,
            Err(err) => {
                eprintln!(
                    "Skipping test_adbc_connection_pool because DuckDB ADBC driver was not found: {err}"
                );
                return;
            }
        };

        let pool = ADBCPool::new(db, None).expect("ADBC Connection pool to be created");

        let conn = pool
            .connect()
            .await
            .expect("ADBC Connection should be established");

        let conn = conn
            .as_sync()
            .expect("ADBC Connection should be synchronous");

        conn.execute("CREATE TABLE test (a INTEGER, b VARCHAR)", &[])
            .expect("Table should be created");
        conn.execute("INSERT INTO test VALUES (1, 'a')", &[])
            .expect("Data should be inserted");

        conn.query_arrow("SELECT * FROM test", &[], None)
            .expect("Query should succeed");
    }
}
