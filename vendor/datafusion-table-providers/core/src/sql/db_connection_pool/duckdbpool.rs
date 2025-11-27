use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, AccessMode, DuckdbConnectionManager};
use snafu::{prelude::*, ResultExt};
use std::sync::Arc;

use super::{
    dbconnection::duckdbconn::{DuckDBAttachments, DuckDBParameter},
    DbConnectionPool, Mode, Result,
};
use crate::{
    sql::db_connection_pool::{
        dbconnection::{duckdbconn::DuckDbConnection, DbConnection, SyncDbConnection},
        JoinPushDown,
    },
    UnsupportedTypeAction,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDB connection failed.\n{source}\nFor details, refer to the DuckDB manual: https://duckdb.org/docs/"))]
    DuckDBConnectionError { source: duckdb::Error },

    #[snafu(display(
        "DuckDB connection failed.\n{source}\nAdjust the DuckDB connection pool parameters for sufficient capacity."
    ))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display(
        "Invalid DuckDB file path: {path}. Ensure it contains a valid database name."
    ))]
    UnableToExtractDatabaseNameFromPath { path: Arc<str> },
}

pub struct DuckDbConnectionPoolBuilder {
    path: String,
    max_size: Option<u32>,
    access_mode: AccessMode,
    min_idle: Option<u32>,
    mode: Mode,
    connection_setup_queries: Vec<Arc<str>>,
}

impl DuckDbConnectionPoolBuilder {
    pub fn memory() -> Self {
        Self {
            path: String::default(),
            max_size: None,
            access_mode: AccessMode::ReadWrite,
            min_idle: None,
            mode: Mode::Memory,
            connection_setup_queries: Vec::new(),
        }
    }

    pub fn file(path: &str) -> Self {
        Self {
            path: path.to_string(),
            max_size: None,
            access_mode: AccessMode::ReadWrite,
            min_idle: None,
            mode: Mode::File,
            connection_setup_queries: Vec::new(),
        }
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn get_mode(&self) -> Mode {
        self.mode
    }

    pub fn with_max_size(mut self, size: Option<u32>) -> Self {
        self.max_size = size;
        self
    }

    pub fn with_access_mode(mut self, access_mode: AccessMode) -> Self {
        self.access_mode = access_mode;
        self
    }

    pub fn with_min_idle(mut self, min_idle: Option<u32>) -> Self {
        self.min_idle = min_idle;
        self
    }

    pub fn with_connection_setup_query(mut self, query: impl Into<Arc<str>>) -> Self {
        self.connection_setup_queries.push(query.into());
        self
    }

    fn build_memory_pool(self) -> Result<DuckDbConnectionPool> {
        let config = get_config(&AccessMode::ReadWrite)?;
        let manager =
            DuckdbConnectionManager::memory_with_flags(config).context(DuckDBConnectionSnafu)?;

        tracing::debug!("Creating DuckDB connection pool for memory instance with max_size {:?} and min_idle {:?}", self.max_size, self.min_idle);

        let mut pool_builder = r2d2::Pool::builder();

        if let Some(size) = self.max_size {
            pool_builder = pool_builder.max_size(size)
        }
        if self.min_idle.is_some() {
            pool_builder = pool_builder.min_idle(self.min_idle)
        }

        let pool = Arc::new(pool_builder.build(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBConnectionSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            path: ":memory:".into(),
            pool,
            join_push_down: JoinPushDown::AllowedFor(":memory:".to_string()),
            attached_databases: Vec::new(),
            mode: Mode::Memory,
            unsupported_type_action: UnsupportedTypeAction::Error,
            connection_setup_queries: self.connection_setup_queries,
        })
    }

    fn build_file_pool(self) -> Result<DuckDbConnectionPool> {
        let config = get_config(&self.access_mode)?;
        let manager = DuckdbConnectionManager::file_with_flags(&self.path, config)
            .context(DuckDBConnectionSnafu)?;

        let mut pool_builder = r2d2::Pool::builder();

        tracing::debug!(
            "Creating DuckDB connection pool for path {} with max_size {:?} and min_idle {:?}",
            self.path,
            self.max_size,
            self.min_idle
        );

        if let Some(size) = self.max_size {
            pool_builder = pool_builder.max_size(size)
        }
        if self.min_idle.is_some() {
            pool_builder = pool_builder.min_idle(self.min_idle)
        }

        let pool = Arc::new(pool_builder.build(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBConnectionSnafu)?;

        test_connection(&conn)?;

        Ok(DuckDbConnectionPool {
            path: self.path.as_str().into(),
            pool,
            // Allow join-push down for any other instances that connect to the same underlying file.
            join_push_down: JoinPushDown::AllowedFor(self.path),
            attached_databases: Vec::new(),
            mode: Mode::File,
            unsupported_type_action: UnsupportedTypeAction::Error,
            connection_setup_queries: self.connection_setup_queries,
        })
    }

    pub fn build(self) -> Result<DuckDbConnectionPool> {
        match self.mode {
            Mode::Memory => self.build_memory_pool(),
            Mode::File => self.build_file_pool(),
        }
    }
}

#[derive(Clone)]
pub struct DuckDbConnectionPool {
    path: Arc<str>,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    join_push_down: JoinPushDown,
    attached_databases: Vec<Arc<str>>,
    mode: Mode,
    unsupported_type_action: UnsupportedTypeAction,
    connection_setup_queries: Vec<Arc<str>>,
}

impl std::fmt::Debug for DuckDbConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbConnectionPool")
            .field("path", &self.path)
            .field("join_push_down", &self.join_push_down)
            .field("attached_databases", &self.attached_databases)
            .field("mode", &self.mode)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .finish()
    }
}

impl DuckDbConnectionPool {
    /// Get the dataset path. Returns `:memory:` if the in memory database is used.
    pub fn db_path(&self) -> &str {
        self.path.as_ref()
    }

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
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    pub fn new_memory() -> Result<Self> {
        DuckDbConnectionPoolBuilder::memory().build()
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
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    /// * `ConnectionPoolSnafu` - If there is an error creating the connection pool
    pub fn new_file(path: &str, access_mode: &AccessMode) -> Result<Self> {
        let access_mode = match access_mode {
            AccessMode::Automatic => AccessMode::Automatic,
            AccessMode::ReadOnly => AccessMode::ReadOnly,
            AccessMode::ReadWrite => AccessMode::ReadWrite,
        };
        DuckDbConnectionPoolBuilder::file(path)
            .with_access_mode(access_mode)
            .build()
    }

    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    #[must_use]
    pub fn set_attached_databases(mut self, databases: &[Arc<str>]) -> Self {
        self.attached_databases = databases.to_vec();

        if !databases.is_empty() {
            let mut paths = self.attached_databases.clone();
            paths.push(Arc::clone(&self.path));
            paths.sort();
            let push_down_context = paths.join(";");
            self.join_push_down = JoinPushDown::AllowedFor(push_down_context);
        }

        self
    }

    #[must_use]
    pub fn with_connection_setup_queries(mut self, queries: Vec<Arc<str>>) -> Self {
        self.connection_setup_queries = queries;
        self
    }

    /// Create a new `DuckDbConnectionPool` from a database URL.
    ///
    /// # Errors
    ///
    /// * `DuckDBConnectionSnafu` - If there is an error creating the connection pool
    pub fn connect_sync(
        self: Arc<Self>,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;

        let attachments = self.get_attachments()?;

        for query in self.connection_setup_queries.iter() {
            tracing::debug!("DuckDB connection setup: {}", query);
            conn.execute(query, []).context(DuckDBConnectionSnafu)?;
        }

        Ok(Box::new(
            DuckDbConnection::new(conn)
                .with_attachments(attachments)
                .with_connection_setup_queries(self.connection_setup_queries.clone())
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn get_attachments(&self) -> Result<Option<Arc<DuckDBAttachments>>> {
        if self.attached_databases.is_empty() {
            Ok(None)
        } else {
            #[cfg(not(feature = "duckdb-federation"))]
            return Ok(None);

            #[cfg(feature = "duckdb-federation")]
            Ok(Some(Arc::new(DuckDBAttachments::new(
                &extract_db_name(Arc::clone(&self.path))?,
                &self.attached_databases,
            ))))
        }
    }
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;

        let attachments = self.get_attachments()?;

        for query in self.connection_setup_queries.iter() {
            tracing::debug!("DuckDB connection setup: {}", query);
            conn.execute(query, []).context(DuckDBConnectionSnafu)?;
        }

        Ok(Box::new(
            DuckDbConnection::new(conn)
                .with_attachments(attachments)
                .with_connection_setup_queries(self.connection_setup_queries.clone())
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

fn test_connection(conn: &r2d2::PooledConnection<DuckdbConnectionManager>) -> Result<()> {
    conn.execute("SELECT 1", [])
        .context(DuckDBConnectionSnafu)?;
    Ok(())
}

fn get_config(access_mode: &AccessMode) -> Result<duckdb::Config> {
    let config = duckdb::Config::default()
        .access_mode(match access_mode {
            AccessMode::ReadOnly => duckdb::AccessMode::ReadOnly,
            AccessMode::ReadWrite => duckdb::AccessMode::ReadWrite,
            AccessMode::Automatic => duckdb::AccessMode::Automatic,
        })
        .context(DuckDBConnectionSnafu)?;

    Ok(config)
}

// Helper function to extract the duckdb database name from the duckdb file path
fn extract_db_name(file_path: Arc<str>) -> Result<String> {
    let path = std::path::Path::new(file_path.as_ref());

    let db_name = match path.file_stem().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => {
            return Err(Box::new(Error::UnableToExtractDatabaseNameFromPath {
                path: file_path,
            }))
        }
    };

    Ok(db_name.to_string())
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use super::*;
    use crate::sql::db_connection_pool::DbConnectionPool;

    fn random_db_name() -> String {
        let mut rng = rand::rng();
        let mut name = String::new();

        for _ in 0..10 {
            name.push(rng.random_range(b'a'..=b'z') as char);
        }

        format!("./{name}.duckdb")
    }

    #[tokio::test]
    async fn test_duckdb_connection_pool() {
        let pool =
            DuckDbConnectionPool::new_memory().expect("DuckDB connection pool to be created");
        let conn = pool
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn = conn
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        conn.execute("CREATE TABLE test (a INTEGER, b VARCHAR)", &[])
            .expect("Table should be created");
        conn.execute("INSERT INTO test VALUES (1, 'a')", &[])
            .expect("Data should be inserted");

        conn.query_arrow("SELECT * FROM test", &[], None)
            .expect("Query should be successful");
    }

    #[tokio::test]
    #[cfg(feature = "duckdb-federation")]
    async fn test_duckdb_connection_pool_with_attached_databases() {
        let db_base_name = random_db_name();
        let db_attached_name = random_db_name();
        let pool = DuckDbConnectionPool::new_file(&db_base_name, &AccessMode::ReadWrite)
            .expect("DuckDB connection pool to be created")
            .set_attached_databases(&[Arc::from(db_attached_name.as_str())]);

        let pool_attached =
            DuckDbConnectionPool::new_file(&db_attached_name, &AccessMode::ReadWrite)
                .expect("DuckDB connection pool to be created")
                .set_attached_databases(&[Arc::from(db_base_name.as_str())]);

        let conn = pool
            .pool
            .get()
            .expect("DuckDB connection should be established");

        conn.execute("CREATE TABLE test_one (a INTEGER, b VARCHAR)", [])
            .expect("Table should be created");
        conn.execute("INSERT INTO test_one VALUES (1, 'a')", [])
            .expect("Data should be inserted");

        let conn_attached = pool_attached
            .pool
            .get()
            .expect("DuckDB connection should be established");

        conn_attached
            .execute("CREATE TABLE test_two (a INTEGER, b VARCHAR)", [])
            .expect("Table should be created");
        conn_attached
            .execute("INSERT INTO test_two VALUES (1, 'a')", [])
            .expect("Data should be inserted");

        let conn = pool
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn = conn
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        let conn_attached = pool_attached
            .connect()
            .await
            .expect("DuckDB connection should be established");
        let conn_attached = conn_attached
            .as_sync()
            .expect("DuckDB connection should be synchronous");

        // sleep to let writes clear
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        conn.query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn_attached
            .query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        conn_attached
            .query_arrow("SELECT * FROM test_one", &[], None)
            .expect("Query should be successful");

        conn.query_arrow("SELECT * FROM test_two", &[], None)
            .expect("Query should be successful");

        std::fs::remove_file(&db_base_name).expect("File should be removed");
        std::fs::remove_file(&db_attached_name).expect("File should be removed");
    }
}
