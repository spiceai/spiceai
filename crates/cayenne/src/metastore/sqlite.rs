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
//!
//! Uses `tokio-rusqlite` for a persistent connection managed by a background thread,
//! avoiding the overhead of opening a new connection for each operation.

use super::{
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreValue, QueryParams,
    QueryRowParams,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// `SQLite`-based metastore backend with a persistent connection.
///
/// Uses `tokio-rusqlite` to maintain a long-lived connection to the database,
/// eliminating the overhead of opening/closing connections for each operation.
pub struct SqliteMetastore {
    connection_string: String,
    /// Cached connection - lazily initialized on first use
    conn: Arc<Mutex<Option<tokio_rusqlite::Connection>>>,
}

impl std::fmt::Debug for SqliteMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteMetastore")
            .field("connection_string", &self.connection_string)
            .finish_non_exhaustive()
    }
}

impl SqliteMetastore {
    /// Create a new `SQLite` metastore.
    #[must_use]
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("sqlite://")
            .unwrap_or(&self.connection_string)
    }

    /// Get or create the persistent connection.
    ///
    /// The connection is configured with performance optimizations:
    /// - WAL mode for non-blocking reads/writes
    /// - Busy timeout to reduce lock contention errors
    /// - NORMAL synchronous mode (safe with WAL)
    /// - Memory cache and temp storage for performance
    /// - Foreign keys enabled
    ///
    /// Uses double-checked locking to avoid holding the mutex across `.await` points.
    async fn get_conn(&self) -> CatalogResult<tokio_rusqlite::Connection> {
        // First check: try to get existing connection without holding lock across await
        {
            let conn_guard = self.conn.lock().await;
            if let Some(conn) = conn_guard.as_ref() {
                return Ok(conn.clone());
            }
        } // Lock dropped here before any async operations

        // Create parent directory if it doesn't exist
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

        // Open connection with tokio-rusqlite (no lock held)
        let conn = tokio_rusqlite::Connection::open(db_path)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to open SQLite database: {e}"),
            })?;

        // Configure pragmas for performance (no lock held)
        conn.call(|conn| {
            // Enable WAL mode for better concurrent access
            conn.pragma_update(None, "journal_mode", "WAL")?;

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

            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to configure SQLite pragmas: {e}"),
        })?;

        // Second check: re-acquire lock and store connection (or use existing if another task created one)
        let mut conn_guard = self.conn.lock().await;
        if let Some(existing_conn) = conn_guard.as_ref() {
            // Another task initialized the connection while we were setting up
            return Ok(existing_conn.clone());
        }
        *conn_guard = Some(conn.clone());
        Ok(conn)
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
            on_conflict_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT,
            vortex_config_json TEXT,
            current_sequence_number BIGINT NOT NULL DEFAULT 0
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
            source_data_file_path TEXT,
            sequence_number BIGINT NOT NULL DEFAULT 0,
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

    /// Schema for the `cayenne_insert_record` table.
    ///
    /// Insert records track PKs that were re-inserted after being deleted.
    /// Each record stores the sequence number when the insert occurred.
    /// Combined with the delete's sequence number, this enables ordering:
    /// - If `insert_sequence` > `delete_sequence` for a PK, the row is visible
    /// - If `delete_sequence` > `insert_sequence`, the row is filtered out
    const INSERT_RECORD_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_insert_record (
            insert_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            pk_bytes BLOB NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            UNIQUE(table_id, pk_bytes)
        )
    ";

    /// Schema for the `cayenne_snapshot_sequence` table.
    ///
    /// Tracks the sequence number for each snapshot. This enables Iceberg-style
    /// sequence ordering: a deletion only applies to snapshots with `sequence_number`
    /// <= the delete file's `sequence_number`.
    const SNAPSHOT_SEQUENCE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_snapshot_sequence (
            table_id INTEGER NOT NULL,
            snapshot_id TEXT NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id)
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

    fn get_blob(&self, index: usize) -> CatalogResult<Vec<u8>> {
        let value = self
            .values
            .get(index)
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })?;
        Vec::<u8>::from_value(value)
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
        rusqlite::types::ValueRef::Blob(b) => MetastoreValue::Blob(b.to_vec()),
    }
}

/// Convert `MetastoreValue` to a `rusqlite::types::Value`.
fn to_sqlite_value(value: &MetastoreValue) -> rusqlite::types::Value {
    match value {
        MetastoreValue::Integer(i) => rusqlite::types::Value::Integer(*i),
        MetastoreValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
        MetastoreValue::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
        MetastoreValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
        MetastoreValue::Null => rusqlite::types::Value::Null,
    }
}

#[async_trait]
impl MetastoreBackend for SqliteMetastore {
    async fn init_schema(&self) -> CatalogResult<()> {
        let conn = self.get_conn().await?;

        conn.call(|conn| {
            // Create tables in a transaction
            conn.execute_batch(&format!(
                "{}; {}; {}; {}; {};",
                Self::TABLE_TABLE_DDL,
                Self::DELETE_FILE_TABLE_DDL,
                Self::PARTITION_TABLE_DDL,
                Self::INSERT_RECORD_TABLE_DDL,
                Self::SNAPSHOT_SEQUENCE_TABLE_DDL
            ))?;

            // Backfill new columns for existing deployments (SQLite doesn't support IF NOT EXISTS for ALTER TABLE until v3.35)
            // Ignore errors when the column already exists to keep init idempotent.
            let _ = conn.execute(
                "ALTER TABLE cayenne_table ADD COLUMN on_conflict_json TEXT",
                [],
            );

            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to initialize schema: {e}"),
        })?;

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn = self.get_conn().await?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.iter().map(to_sqlite_value).collect();

        conn.call(move |conn| {
            let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect();
            conn.execute(&sql, params_refs.as_slice())?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to execute statement: {e}"),
        })?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.get_conn().await?;
        let sql_owned = sql.to_string();

        conn.call(move |conn| {
            conn.execute_batch(&sql_owned)?;
            Ok::<_, rusqlite::Error>(())
        })
        .await
        .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to execute batch: {e}"),
        })?;

        Ok(())
    }

    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.get_conn().await?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.iter().map(to_sqlite_value).collect();

        conn.call(move |conn| {
            let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect();

            let row_values = conn.query_row(&sql, params_refs.as_slice(), |row| {
                let column_count = row.as_ref().column_count();
                let mut values = Vec::with_capacity(column_count);

                for i in 0..column_count {
                    let value = row.get_ref(i)?;
                    values.push(convert_sqlite_value(value));
                }

                Ok(values)
            })?;

            let sqlite_row = SqliteRow { values: row_values };
            f(&sqlite_row).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
        })
        .await
        .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to query row: {e}"),
        })
    }

    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.get_conn().await?;
        let sql = params.sql.to_string();
        let param_values: Vec<rusqlite::types::Value> =
            params.params.iter().map(to_sqlite_value).collect();

        conn.call(move |conn| {
            let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
                .iter()
                .map(|v| v as &dyn rusqlite::ToSql)
                .collect();

            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
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
                results.push(
                    f(&row).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                );
            }

            Ok::<Vec<T>, rusqlite::Error>(results)
        })
        .await
        .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
            message: format!("Failed to query rows: {e}"),
        })
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        // Clone the connection before dropping the lock to avoid holding it across await
        let conn = {
            let conn_guard = self.conn.lock().await;
            conn_guard.clone()
        };

        if let Some(conn) = conn.as_ref() {
            conn.call(|conn| {
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

                Ok::<_, rusqlite::Error>(())
            })
            .await
            .map_err(|e: tokio_rusqlite::Error<rusqlite::Error>| CatalogError::Database {
                message: format!("Failed to shutdown catalog: {e}"),
            })?;
        }

        Ok(())
    }
}
