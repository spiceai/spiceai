/*
Copyright 2025 The Spice.ai OSS Authors

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

//! `SQLite` implementation of the metastore backend.

use super::{
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreValue, QueryParams,
    QueryRowParams,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::path::Path;

/// `SQLite`-based metastore backend.
#[derive(Debug)]
pub struct SqliteMetastore {
    connection_string: String,
}

impl SqliteMetastore {
    /// Create a new `SQLite` metastore.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("sqlite://")
            .unwrap_or(&self.connection_string)
    }

    /// Open a `SQLite` connection configured for concurrent access.
    ///
    /// Applies performance optimizations based on `SQLite` best practices:
    /// - WAL mode for non-blocking reads/writes
    /// - Busy timeout to reduce lock contention errors
    /// - NORMAL synchronous mode (safe with WAL)
    /// - Memory cache and temp storage for performance
    /// - Foreign keys enabled
    fn open_connection(db_path: &str, read_only: bool) -> CatalogResult<rusqlite::Connection> {
        let flags = if read_only {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
        };

        let conn = rusqlite::Connection::open_with_flags(db_path, flags)?;

        // Enable WAL mode for better concurrent access (allows multiple readers with one writer)
        if !read_only {
            conn.pragma_update(None, "journal_mode", "WAL")?;
        }

        // SQLite will wait 5 seconds to obtain a lock before returning SQLITE_BUSY errors
        conn.busy_timeout(std::time::Duration::from_secs(5))?;

        // NORMAL synchronous mode is safe with WAL and more performant than FULL
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        // 32MB cache size (negative number means kilobytes)
        conn.pragma_update(None, "cache_size", -32000)?;

        // Enable foreign keys (disabled by default for historical reasons)
        conn.pragma_update(None, "foreign_keys", true)?;

        // Store temporary tables in memory for better performance
        conn.pragma_update(None, "temp_store", "memory")?;

        Ok(conn)
    }

    /// Handle the result of a `spawn_blocking` task with explicit error messages.
    fn handle_blocking_result<T>(
        result: Result<CatalogResult<T>, tokio::task::JoinError>,
        operation: &str,
    ) -> CatalogResult<T> {
        result.map_err(|err| {
            let message = if err.is_panic() {
                format!("{operation} task panicked: {err}")
            } else if err.is_cancelled() {
                format!("{operation} task was cancelled: {err}")
            } else {
                format!("{operation} task failed: {err}")
            };
            CatalogError::InvalidOperation {
                message,
                source: Box::new(err),
            }
        })?
    }

    /// Schema for the `cayenne_table` table.
    /// Using INTEGER for AUTOINCREMENT is required
    /// It is unlikely someone will have more than `9223372036854775807` tables (`SQLite` INTEGER max)
    const TABLE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_table (
            table_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_uuid TEXT NOT NULL,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT,
            vortex_config_json TEXT
        )
    ";

    /// Schema for the `cayenne_delete_file` table.
    const DELETE_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_delete_file (
            delete_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            format TEXT NOT NULL,
            delete_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    /// Schema for the `cayenne_partition` table.
    const PARTITION_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_partition (
            partition_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            partition_column TEXT NOT NULL,
            partition_value TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            record_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            UNIQUE(table_id, partition_column, partition_value)
        )
    ";
}

/// `SQLite` row wrapper implementing `MetastoreRow`.
struct SqliteRow {
    values: Vec<MetastoreValue>,
}

impl MetastoreRow for SqliteRow {
    fn get_i64(&self, index: usize) -> CatalogResult<i64> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        i64::from_value(value)
    }

    fn get_string(&self, index: usize) -> CatalogResult<String> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        String::from_value(value)
    }

    fn get_bool(&self, index: usize) -> CatalogResult<bool> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        bool::from_value(value)
    }

    fn get_optional_i64(&self, index: usize) -> CatalogResult<Option<i64>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Option::<i64>::from_value(value)
    }

    fn get_optional_string(&self, index: usize) -> CatalogResult<Option<String>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Option::<String>::from_value(value)
    }
}

/// Convert `rusqlite::Value` to `MetastoreValue`.
fn convert_sqlite_value(value: rusqlite::types::ValueRef<'_>) -> MetastoreValue {
    match value {
        rusqlite::types::ValueRef::Null => MetastoreValue::Null,
        rusqlite::types::ValueRef::Integer(i) => MetastoreValue::Integer(i),
        rusqlite::types::ValueRef::Real(_) => {
            // We don't use real numbers in metadata, treat as error
            MetastoreValue::Null
        }
        rusqlite::types::ValueRef::Text(t) => {
            MetastoreValue::Text(String::from_utf8_lossy(t).to_string())
        }
        rusqlite::types::ValueRef::Blob(_) => {
            // We don't use blobs in metadata
            MetastoreValue::Null
        }
    }
}

/// Convert `MetastoreValue` to a `rusqlite` parameter.
fn to_sqlite_param(value: &MetastoreValue) -> Box<dyn rusqlite::ToSql> {
    match value {
        MetastoreValue::Integer(i) => Box::new(*i),
        MetastoreValue::Text(s) => Box::new(s.clone()),
        MetastoreValue::Bool(b) => Box::new(*b),
        MetastoreValue::Null => Box::new(rusqlite::types::Null),
    }
}

#[async_trait]
impl MetastoreBackend for SqliteMetastore {
    async fn init_schema(&self) -> CatalogResult<()> {
        // Create database file if it doesn't exist
        let db_path = self.db_path();
        let db_dir =
            Path::new(db_path)
                .parent()
                .ok_or_else(|| CatalogError::InvalidDatabasePath {
                    path: db_path.to_string(),
                })?;

        if !db_dir.exists() {
            tokio::fs::create_dir_all(db_dir).await?;
        }

        // Initialize schema using connection with WAL mode
        let db_path_owned = self.db_path().to_string();
        let result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            // Create tables in a transaction
            conn.execute_batch(&format!(
                "{}; {}; {};",
                Self::TABLE_TABLE_DDL,
                Self::DELETE_FILE_TABLE_DDL,
                Self::PARTITION_TABLE_DDL
            ))?;

            Ok::<(), CatalogError>(())
        })
        .await;

        Self::handle_blocking_result(result, "Schema initialization")?;

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let db_path_owned = self.db_path().to_string();
        let sql = params.sql.to_string();
        let param_values = params.params.clone();

        let result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;

            let params_refs: Vec<Box<dyn rusqlite::ToSql>> =
                param_values.iter().map(to_sqlite_param).collect();

            let params_slice: Vec<&dyn rusqlite::ToSql> = params_refs
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect();

            conn.execute(&sql, params_slice.as_slice())?;

            Ok::<(), CatalogError>(())
        })
        .await;

        Self::handle_blocking_result(result, "Execute statement")?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let db_path_owned = self.db_path().to_string();
        let sql_owned = sql.to_string();

        let result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, false)?;
            conn.execute_batch(&sql_owned)?;
            Ok::<(), CatalogError>(())
        })
        .await;

        Self::handle_blocking_result(result, "Execute batch")?;

        Ok(())
    }

    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let db_path_owned = self.db_path().to_string();
        let sql = params.sql.to_string();
        let param_values = params.params.clone();

        let result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let params_refs: Vec<Box<dyn rusqlite::ToSql>> =
                param_values.iter().map(to_sqlite_param).collect();

            let params_slice: Vec<&dyn rusqlite::ToSql> = params_refs
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect();

            conn.query_row(&sql, params_slice.as_slice(), |row| {
                let column_count = row.as_ref().column_count();
                let mut values = Vec::with_capacity(column_count);

                for i in 0..column_count {
                    let value = row.get_ref(i)?;
                    values.push(convert_sqlite_value(value));
                }

                Ok(SqliteRow { values })
            })
            .map_err(CatalogError::from)
            .and_then(|row| f(&row))
        })
        .await;

        Self::handle_blocking_result(result, "Query row")
    }

    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let db_path_owned = self.db_path().to_string();
        let sql = params.sql.to_string();
        let param_values = params.params.clone();

        let result = tokio::task::spawn_blocking(move || {
            let conn = Self::open_connection(&db_path_owned, true)?;

            let params_refs: Vec<Box<dyn rusqlite::ToSql>> =
                param_values.iter().map(to_sqlite_param).collect();

            let params_slice: Vec<&dyn rusqlite::ToSql> = params_refs
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect();

            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_slice.as_slice(), |row| {
                let column_count = row.as_ref().column_count();
                let mut values = Vec::with_capacity(column_count);

                for i in 0..column_count {
                    let value = row.get_ref(i)?;
                    values.push(convert_sqlite_value(value));
                }

                Ok(SqliteRow { values })
            })?;

            let mut results = Vec::new();
            for row_result in rows {
                let row = row_result?;
                results.push(f(&row)?);
            }

            Ok::<Vec<T>, CatalogError>(results)
        })
        .await;

        Self::handle_blocking_result(result, "Query rows")
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        let db_path_owned = self.db_path().to_string();

        let result = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&db_path_owned)?;

            // Check if WAL mode is enabled
            let journal_mode: String =
                conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;

            if journal_mode.eq_ignore_ascii_case("wal") {
                tracing::info!("Truncating Cayenne catalog WAL log");
                // Truncate the WAL log to persist changes and reduce file size
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", [])?;
            }

            // Run optimize to improve query performance for future connections
            tracing::info!("Running optimize on Cayenne catalog");
            conn.execute("PRAGMA optimize", [])?;

            Ok::<(), CatalogError>(())
        })
        .await;

        Self::handle_blocking_result(result, "Catalog shutdown")?;

        Ok(())
    }
}
