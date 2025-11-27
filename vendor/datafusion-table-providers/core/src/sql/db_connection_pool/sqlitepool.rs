use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use snafu::prelude::*;
use tokio_rusqlite::{Connection, ToSql};

use super::{DbConnectionPool, Result};
use crate::sql::db_connection_pool::{
    dbconnection::{sqliteconn::SqliteConnection, AsyncDbConnection, DbConnection},
    JoinPushDown, Mode,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: rusqlite::Error },

    #[snafu(display("No path provided for SQLite connection"))]
    NoPathError {},

    #[snafu(display("Database to attach does not exist: {path}"))]
    DatabaseDoesNotExist { path: String },
}

pub struct SqliteConnectionPoolFactory {
    path: Arc<str>,
    mode: Mode,
    attach_databases: Option<Vec<Arc<str>>>,
    busy_timeout: Duration,
}

impl SqliteConnectionPoolFactory {
    pub fn new(path: &str, mode: Mode, busy_timeout: Duration) -> Self {
        SqliteConnectionPoolFactory {
            path: path.into(),
            mode,
            attach_databases: None,
            busy_timeout,
        }
    }

    #[must_use]
    pub fn with_databases(mut self, attach_databases: Option<Vec<Arc<str>>>) -> Self {
        self.attach_databases = attach_databases;
        self
    }

    pub async fn build(&self) -> Result<SqliteConnectionPool> {
        let join_push_down = match (self.mode, &self.attach_databases) {
            (Mode::File, Some(attach_databases)) => {
                if attach_databases.is_empty() {
                    JoinPushDown::AllowedFor(self.path.to_string())
                } else {
                    let mut attach_databases = attach_databases.clone();

                    for database in &attach_databases {
                        // check if the database file exists
                        if std::fs::metadata(database.as_ref()).is_err() {
                            return Err(Error::DatabaseDoesNotExist {
                                path: database.to_string(),
                            }
                            .into());
                        }
                    }

                    if !attach_databases.contains(&self.path) {
                        attach_databases.push(Arc::clone(&self.path));
                    }

                    attach_databases.sort();

                    JoinPushDown::AllowedFor(attach_databases.join(";")) // push down is allowed cross-database when they're attached together
                }
            }
            (Mode::File, None) => JoinPushDown::AllowedFor(self.path.to_string()),
            (Mode::Memory, _) => JoinPushDown::AllowedFor("memory".to_string()),
        };

        let attach_databases = if let Some(attach_databases) = &self.attach_databases {
            attach_databases.clone()
        } else {
            vec![]
        };

        let pool = SqliteConnectionPool::new(
            &self.path,
            self.mode,
            join_push_down,
            attach_databases,
            self.busy_timeout,
        )
        .await?;

        pool.setup().await?;

        Ok(pool)
    }
}

#[derive(Debug)]
pub struct SqliteConnectionPool {
    conn: Connection,
    join_push_down: JoinPushDown,
    mode: Mode,
    path: Arc<str>,
    attach_databases: Vec<Arc<str>>,
    busy_timeout: Duration,
}

impl SqliteConnectionPool {
    /// Creates a new instance of `SqliteConnectionPool`.
    ///
    /// NOTE: The `SqliteConnectionPool` currently does no connection pooling, it simply creates a new connection
    /// and clones it on each call to `connect()`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        path: &str,
        mode: Mode,
        join_push_down: JoinPushDown,
        attach_databases: Vec<Arc<str>>,
        busy_timeout: Duration,
    ) -> Result<Self> {
        let conn = match mode {
            Mode::Memory => Connection::open_in_memory()
                .await
                .map_err(|e| Error::ConnectionPoolError { source: e })?,

            Mode::File => Connection::open(path.to_string())
                .await
                .map_err(|e| Error::ConnectionPoolError { source: e })?,
        };

        Ok(SqliteConnectionPool {
            conn,
            join_push_down,
            mode,
            attach_databases,
            path: path.into(),
            busy_timeout,
        })
    }

    /// Initializes an SQLite database on-disk without creating a connection pool.
    /// No-op if the database is in-memory.
    pub async fn init(path: &str, mode: Mode) -> Result<()> {
        if mode == Mode::File {
            Connection::open(path.to_string())
                .await
                .map_err(|e| Error::ConnectionPoolError { source: e })?;
        }

        Ok(())
    }

    pub async fn setup(&self) -> Result<()> {
        let conn = self.conn.clone();
        let busy_timeout = self.busy_timeout;

        // these configuration options are only applicable for file-mode databases
        if self.mode == Mode::File {
            // change transaction mode to Write-Ahead log instead of default atomic rollback journal: https://www.sqlite.org/wal.html
            // NOTE: This is a no-op if the database is in-memory, as only MEMORY or OFF are supported: https://www.sqlite.org/pragma.html#pragma_journal_mode
            conn.call(move |conn| {
                conn.pragma_update(None, "journal_mode", "WAL")?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                conn.pragma_update(None, "cache_size", "-20000")?;
                conn.pragma_update(None, "foreign_keys", "true")?;
                conn.pragma_update(None, "temp_store", "memory")?;
                // conn.set_transaction_behavior(TransactionBehavior::Immediate); introduced in rustqlite 0.32.1, but tokio-rusqlite is still on 0.31.0

                // Set user configurable connection timeout
                conn.busy_timeout(busy_timeout)?;

                Ok(())
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Error(e) => Error::ConnectionPoolError { source: e },
                tokio_rusqlite::Error::ConnectionClosed => Error::ConnectionPoolError { source: rusqlite::Error::InvalidQuery },
                tokio_rusqlite::Error::Close((_, e)) => Error::ConnectionPoolError { source: e },
                _ => Error::ConnectionPoolError { source: rusqlite::Error::InvalidQuery },
            })?;

            // database attachments are only supported for file-mode databases
            #[cfg(feature = "sqlite-federation")]
            {
                let attach_databases = self
                    .attach_databases
                    .iter()
                    .enumerate()
                    .map(|(i, db)| format!("ATTACH DATABASE '{db}' AS attachment_{i}"));

                for attachment in attach_databases {
                    if attachment == *self.path {
                        continue;
                    }

                    conn.call(move |conn| {
                        conn.execute(&attachment, [])?;
                        Ok(())
                    })
                    .await
                    .map_err(|e| match e {
                        tokio_rusqlite::Error::Error(e) => Error::ConnectionPoolError { source: e },
                        tokio_rusqlite::Error::ConnectionClosed => Error::ConnectionPoolError { source: rusqlite::Error::InvalidQuery },
                        tokio_rusqlite::Error::Close((_, e)) => Error::ConnectionPoolError { source: e },
                        _ => Error::ConnectionPoolError { source: rusqlite::Error::InvalidQuery },
                    })?;
                }

                Ok::<(), super::Error>(())
            }?;
        }

        Ok(())
    }

    #[must_use]
    pub fn connect_sync(&self) -> Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>> {
        Box::new(SqliteConnection::new(self.conn.clone()))
    }

    /// Will attempt to clone the connection pool. This will always succeed for in-memory mode.
    /// For file-mode, it will attempt to create a new connection pool with the same configuration.
    ///
    /// Due to the way the connection pool is implemented, it doesn't allow multiple concurrent reads/writes
    /// using the same connection pool instance.
    pub async fn try_clone(&self) -> Result<Self> {
        match self.mode {
            Mode::Memory => Ok(SqliteConnectionPool {
                conn: self.conn.clone(),
                join_push_down: self.join_push_down.clone(),
                mode: self.mode,
                path: Arc::clone(&self.path),
                attach_databases: self.attach_databases.clone(),
                busy_timeout: self.busy_timeout,
            }),
            Mode::File => {
                let attach_databases = if self.attach_databases.is_empty() {
                    None
                } else {
                    Some(self.attach_databases.clone())
                };

                SqliteConnectionPoolFactory::new(&self.path, self.mode, self.busy_timeout)
                    .with_databases(attach_databases)
                    .build()
                    .await
            }
        }
    }
}

#[async_trait]
impl DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> for SqliteConnectionPool {
    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>> {
        let conn = self.conn.clone();

        Ok(Box::new(SqliteConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::db_connection_pool::Mode;
    use rand::Rng;
    use rstest::rstest;
    use std::time::Duration;

    fn random_db_name() -> String {
        let mut rng = rand::rng();
        let mut name = String::new();

        for _ in 0..10 {
            name.push(rng.random_range(b'a'..=b'z') as char);
        }

        format!("./{name}.sqlite")
    }

    #[rstest]
    #[tokio::test]
    async fn test_sqlite_connection_pool_factory() {
        let db_name = random_db_name();
        let factory =
            SqliteConnectionPoolFactory::new(&db_name, Mode::File, Duration::from_secs(5));
        let pool = factory.build().await.unwrap();

        assert!(pool.join_push_down == JoinPushDown::AllowedFor(db_name.clone()));
        assert!(pool.mode == Mode::File);
        assert_eq!(pool.path, db_name.clone().into());

        pool.conn.close().await.unwrap();

        // cleanup
        std::fs::remove_file(&db_name).unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_with_attachments() {
        let mut db_names = [random_db_name(), random_db_name(), random_db_name()];
        db_names.sort();

        let factory =
            SqliteConnectionPoolFactory::new(&db_names[0], Mode::File, Duration::from_millis(5000))
                .with_databases(Some(vec![
                    db_names[1].clone().into(),
                    db_names[2].clone().into(),
                ]));

        SqliteConnectionPool::init(&db_names[1], Mode::File)
            .await
            .unwrap();
        SqliteConnectionPool::init(&db_names[2], Mode::File)
            .await
            .unwrap();

        let pool = factory.build().await.unwrap();

        let push_down = db_names.join(";");

        assert!(pool.join_push_down == JoinPushDown::AllowedFor(push_down));
        assert!(pool.mode == Mode::File);
        assert_eq!(pool.path, db_names[0].clone().into());

        pool.conn.close().await.unwrap();

        // cleanup
        for db in &db_names {
            std::fs::remove_file(db).unwrap();
        }
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_with_empty_attachments() {
        let db_name = random_db_name();
        let factory =
            SqliteConnectionPoolFactory::new(&db_name, Mode::File, Duration::from_millis(5000))
                .with_databases(Some(vec![]));

        let pool = factory.build().await.unwrap();

        assert!(pool.join_push_down == JoinPushDown::AllowedFor(db_name.clone()));
        assert!(pool.mode == Mode::File);
        assert_eq!(pool.path, db_name.clone().into());

        pool.conn.close().await.unwrap();

        // cleanup
        std::fs::remove_file(&db_name).unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_memory_with_attachments() {
        let factory = SqliteConnectionPoolFactory::new(
            "./test.sqlite",
            Mode::Memory,
            Duration::from_millis(5000),
        )
        .with_databases(Some(vec!["./test1.sqlite".into(), "./test2.sqlite".into()]));
        let pool = factory.build().await.unwrap();

        assert!(pool.join_push_down == JoinPushDown::AllowedFor("memory".to_string()));
        assert!(pool.mode == Mode::Memory);
        assert_eq!(pool.path, "./test.sqlite".into());

        pool.conn.close().await.unwrap();

        // in memory mode, attachments are not created and nothing happens
        assert!(std::fs::metadata("./test.sqlite").is_err());
        assert!(std::fs::metadata("./test1.sqlite").is_err());
        assert!(std::fs::metadata("./test2.sqlite").is_err());
    }

    #[tokio::test]
    async fn test_sqlite_connection_pool_factory_errors_with_missing_attachments() {
        let mut db_names = [random_db_name(), random_db_name(), random_db_name()];
        db_names.sort();

        let factory =
            SqliteConnectionPoolFactory::new(&db_names[0], Mode::File, Duration::from_millis(5000))
                .with_databases(Some(vec![
                    db_names[1].clone().into(),
                    db_names[2].clone().into(),
                ]));
        let pool = factory.build().await;

        assert!(pool.is_err());

        let err = pool.err().unwrap();
        assert!(err.to_string().contains(&format!(
            "Database to attach does not exist: {}",
            db_names[1]
        )));
    }
}
