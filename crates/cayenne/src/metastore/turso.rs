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

//! Turso implementation of the metastore backend.

use super::{
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreTransaction,
    MetastoreValue, QueryParams, QueryRowParams, duplicate_delete_file_index_error_message,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::sync::Arc;
use std::{fmt::Debug, path::Path};
use tokio::sync::{Mutex, OwnedMutexGuard};
use turso::{Builder, Connection, Value as TursoValue};
use turso_shared::JOURNAL_MODE_SQL_LITERAL;

const DELETE_FILE_TABLE_UNIQUE_INDEX_DDL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_delete_file_table_path ON cayenne_delete_file(table_id, path)";

/// Turso-based metastore backend.
///
/// The connection is behind a [`Mutex`] to ensure exclusive access during
/// multi-statement transactions, matching the `SQLite` metastore design.
pub struct TursoMetastore {
    conn: Arc<Mutex<Option<Connection>>>,
    connection_string: String,
}

impl Debug for TursoMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoMetastore")
            .field("connection_string", &self.connection_string)
            .finish_non_exhaustive()
    }
}

impl TursoMetastore {
    /// Create a new Turso metastore.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(None)),
            connection_string: connection_string.into(),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("libsql://")
            .unwrap_or(&self.connection_string)
    }

    /// Get or create the database connection, returning the mutex-guarded wrapper.
    ///
    /// Callers acquire the lock before using the connection.
    async fn get_conn(&self) -> CatalogResult<Arc<Mutex<Option<Connection>>>> {
        // Initialize connection if needed
        {
            let mut conn_guard = self.conn.lock().await;
            if conn_guard.is_none() {
                let db_path = self.db_path();

                // Create parent directory if it doesn't exist
                let db_dir = Path::new(db_path).parent().ok_or_else(|| {
                    CatalogError::InvalidDatabasePath {
                        path: db_path.to_string(),
                    }
                })?;

                if !db_dir.exists() {
                    tokio::fs::create_dir_all(db_dir).await?;

                    // Best-effort parent directory sync (defense-in-depth with
                    // the sync already performed in CayenneCatalog::init).
                    // Ensures the db_dir entry is durable before opening the
                    // Turso connection and initializing the schema.
                    //
                    // We keep this best-effort (with warning on failure) for
                    // the same reasons as in CayenneCatalog::init: one-time
                    // initialization, followed by DB file + schema creation,
                    // and the parent is often a stable operator-managed
                    // volume root.
                    if let Some(parent) = db_dir.parent() {
                        let parent_for_sync = parent.to_path_buf();
                        let parent_display = parent_for_sync.display().to_string();
                        let db_dir_display = db_dir.display().to_string();
                        match tokio::task::spawn_blocking(move || {
                            std::fs::File::open(&parent_for_sync).and_then(|f| f.sync_all())
                        })
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => tracing::warn!(
                                "Failed to sync parent directory {parent_display} after creating Turso catalog DB directory {db_dir_display} (subsequent DB writes will still be durable): {error}"
                            ),
                            Err(error) => tracing::warn!(
                                "Failed to join Turso catalog DB parent directory sync task for {parent_display}: {error}"
                            ),
                        }
                    }
                }

                let db = Builder::new_local(db_path).build().await.map_err(|e| {
                    CatalogError::Database {
                        message: format!("Failed to open Turso database: {e}"),
                    }
                })?;

                let conn = db.connect().map_err(|e| CatalogError::Database {
                    message: format!("Failed to connect to Turso database: {e}"),
                })?;

                // Set busy timeout to wait for locks instead of immediately returning SQLITE_BUSY.
                conn.busy_timeout(std::time::Duration::from_secs(5))
                    .map_err(|e| CatalogError::Database {
                        message: format!("Failed to set busy timeout: {e}"),
                    })?;

                // BEGIN CONCURRENT requires MVCC journal mode for concurrent writers.
                conn.pragma_update("journal_mode", JOURNAL_MODE_SQL_LITERAL)
                    .await
                    .map_err(|e| CatalogError::Database {
                        message: format!("Failed to set journal mode: {e}"),
                    })?;

                conn.execute("PRAGMA foreign_keys = ON", ())
                    .await
                    .map_err(|e| CatalogError::Database {
                        message: format!("Failed to enable foreign keys: {e}"),
                    })?;

                // NORMAL synchronous mode: safe with MVCC, more performant than FULL
                conn.execute("PRAGMA synchronous = NORMAL", ())
                    .await
                    .map_err(|e| CatalogError::Database {
                        message: format!("Failed to set synchronous mode: {e}"),
                    })?;

                // 32MB cache size (negative value = kilobytes in SQLite/libSQL)
                conn.execute("PRAGMA cache_size = -32768", ())
                    .await
                    .map_err(|e| CatalogError::Database {
                        message: format!("Failed to set cache size: {e}"),
                    })?;

                *conn_guard = Some(conn);
            }
        }

        Ok(Arc::clone(&self.conn))
    }

    /// Schema for the `cayenne_table` table.
    const TABLE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_table (
            table_id TEXT PRIMARY KEY,
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

    const TABLE_NAME_UNIQUE_INDEX_DDL: &'static str = r"
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_table_name_unique
        ON cayenne_table(table_name)
    ";

    /// Schema for the `cayenne_delete_file` table.
    const DELETE_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_delete_file (
            delete_file_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
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
    ///
    /// Supports composite partition keys by storing column names and values as JSON arrays.
    /// The `partition_key` column stores a unique composite key (slash-separated values)
    /// for efficient lookups and uniqueness constraints.
    const PARTITION_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_partition (
            partition_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_columns_json TEXT NOT NULL,
            partition_values_json TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            record_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            UNIQUE(table_id, partition_key)
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
            insert_record_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
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
            table_id TEXT NOT NULL,
            snapshot_id TEXT NOT NULL,
            sequence_number BIGINT NOT NULL,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
            PRIMARY KEY (table_id, snapshot_id)
        )
    ";

    /// Schema for the `cayenne_table_statistics` table.
    const TABLE_STATISTICS_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
            table_id TEXT NOT NULL PRIMARY KEY,
            statistics_blob BLOB NOT NULL,
            num_rows BIGINT NOT NULL DEFAULT 0,
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DATA_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_inlined_data (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            partition_key TEXT,
            data_ipc BLOB NOT NULL,
            record_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DELETE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS cayenne_inlined_delete (
            inlined_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            delete_ipc BLOB NOT NULL,
            delete_count BIGINT NOT NULL,
            sequence_number BIGINT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
        )
    ";

    const INLINED_DATA_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_data_table_seq ON cayenne_inlined_data(table_id, sequence_number)";
    const INLINED_DELETE_INDEX_DDL: &'static str = "CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_delete_table_seq ON cayenne_inlined_delete(table_id, sequence_number)";
}

/// Turso row wrapper implementing `MetastoreRow`.
struct TursoRow {
    values: Vec<MetastoreValue>,
}

impl MetastoreRow for TursoRow {
    fn get_value(&self, index: usize) -> CatalogResult<MetastoreValue> {
        self.values
            .get(index)
            .cloned()
            .ok_or_else(|| CatalogError::Database {
                message: format!("Column index {index} out of bounds"),
            })
    }

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

/// Convert Turso Value to `MetastoreValue`.
fn convert_turso_value(value: &TursoValue) -> MetastoreValue {
    match value {
        TursoValue::Null => MetastoreValue::Null,
        TursoValue::Integer(i) => MetastoreValue::Integer(*i),
        TursoValue::Real(_) => {
            // We don't use real numbers in metadata
            MetastoreValue::Null
        }
        TursoValue::Text(t) => MetastoreValue::Text(t.clone()),
        TursoValue::Blob(b) => MetastoreValue::Blob(b.clone()),
    }
}

/// Convert `MetastoreValue` to Turso Value.
fn to_turso_value(value: &MetastoreValue) -> TursoValue {
    match value {
        MetastoreValue::Integer(i) => TursoValue::Integer(*i),
        MetastoreValue::Text(s) => TursoValue::Text(s.clone()),
        MetastoreValue::Bool(b) => TursoValue::Integer(i64::from(*b)),
        MetastoreValue::Blob(b) => TursoValue::Blob(b.clone()),
        MetastoreValue::Null => TursoValue::Null,
    }
}

/// Convert Turso errors to `CatalogError`, distinguishing constraint violations.
fn convert_turso_error(e: turso::Error) -> CatalogError {
    match e {
        // turso exposes a dedicated Constraint variant for constraint violations
        turso::Error::Constraint(ref msg) => CatalogError::ConstraintViolation {
            message: msg.clone(),
        },
        other => CatalogError::Database {
            message: format!("Failed to execute statement: {other}"),
        },
    }
}

#[async_trait]
impl MetastoreBackend for TursoMetastore {
    async fn init_schema(&self) -> CatalogResult<()> {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock().await;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        // Create tables
        let schema_sql = format!(
            "{}; {}; {}; {}; {}; {}; {}; {}; {};",
            Self::TABLE_TABLE_DDL,
            Self::TABLE_NAME_UNIQUE_INDEX_DDL,
            Self::DELETE_FILE_TABLE_DDL,
            Self::PARTITION_TABLE_DDL,
            Self::INSERT_RECORD_TABLE_DDL,
            Self::SNAPSHOT_SEQUENCE_TABLE_DDL,
            Self::TABLE_STATISTICS_DDL,
            Self::INLINED_DATA_TABLE_DDL,
            Self::INLINED_DELETE_TABLE_DDL
        );

        conn.execute_batch(&schema_sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to initialize schema: {e}"),
            })?;

        conn.execute(DELETE_FILE_TABLE_UNIQUE_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: duplicate_delete_file_index_error_message("Turso/libSQL", e),
            })?;

        conn.execute(Self::INLINED_DATA_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to create inlined_data index: {e}"),
            })?;
        conn.execute(Self::INLINED_DELETE_INDEX_DDL, ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to create inlined_delete index: {e}"),
            })?;

        // Attempt to backfill newly added columns for existing deployments. Errors are ignored
        // because the column may already exist (libSQL doesn't support IF NOT EXISTS for ALTER).
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_table ADD COLUMN on_conflict_json TEXT",
                (),
            )
            .await;

        // Validate that existing tables match the expected schema.
        // This catches incompatible metadata databases from previous versions.
        for expected in super::EXPECTED_TABLES {
            let mut rows = conn
                .query(&format!("PRAGMA table_info('{}')", expected.name), ())
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to read table schema for validation: {e}"),
                })?;

            let mut actual_columns = Vec::new();
            loop {
                match rows.next().await {
                    Ok(Some(row)) => {
                        // PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
                        // Column name is at index 1.
                        if let Ok(turso::Value::Text(name)) = row.get_value(1) {
                            actual_columns.push(name);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(CatalogError::Database {
                            message: format!("Failed to read table schema for validation: {e}"),
                        });
                    }
                }
            }

            // Skip validation for freshly created tables (no columns = table didn't exist before).
            if actual_columns.is_empty() {
                continue;
            }

            let expected_columns: Vec<&str> = expected.columns.to_vec();
            let actual_refs: Vec<&str> = actual_columns.iter().map(String::as_str).collect();

            if expected_columns != actual_refs {
                tracing::debug!(
                    "Cayenne schema mismatch for '{}': expected columns [{}], found [{}]",
                    expected.name,
                    expected_columns.join(", "),
                    actual_refs.join(", ")
                );
                return Err(CatalogError::SchemaMismatch {
                    table: expected.name.to_string(),
                });
            }
        }

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock().await;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        let mut stmt = conn
            .prepare_cached(params.sql)
            .await
            .map_err(convert_turso_error)?;
        stmt.execute(turso_params)
            .await
            .map_err(convert_turso_error)?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock().await;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        conn.execute_batch(sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to execute batch: {e}"),
            })?;

        Ok(())
    }

    async fn execute_transaction_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock().await;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;
        let batch_sql = format!("BEGIN CONCURRENT; {sql}; COMMIT;");

        if let Err(e) = conn.execute_batch(&batch_sql).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(CatalogError::Database {
                message: format!("Failed to execute transaction batch: {e}"),
            });
        }

        Ok(())
    }

    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock().await;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        let mut stmt =
            conn.prepare_cached(params.sql)
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to query row: {e}"),
                })?;
        let mut rows = stmt
            .query(turso_params)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to query row: {e}"),
            })?;

        let row = rows.next().await.map_err(|e| CatalogError::Database {
            message: format!("Failed to fetch row: {e}"),
        })?;

        let row = row.ok_or_else(|| CatalogError::Database {
            message: "Query returned no rows".to_string(),
        })?;

        // Convert row values
        let values: Vec<MetastoreValue> = (0..row.column_count())
            .map(|i| {
                row.get_value(i)
                    .map(|v| convert_turso_value(&v))
                    .unwrap_or(MetastoreValue::Null)
            })
            .collect();

        let turso_row = TursoRow { values };
        f(&turso_row)
    }

    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock().await;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        let mut stmt =
            conn.prepare_cached(params.sql)
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to query rows: {e}"),
                })?;
        let mut rows = stmt
            .query(turso_params)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to query rows: {e}"),
            })?;

        let mut results = Vec::new();

        loop {
            match rows.next().await {
                Ok(Some(row)) => {
                    // Convert row values
                    let values: Vec<MetastoreValue> = (0..row.column_count())
                        .map(|i| {
                            row.get_value(i)
                                .map(|v| convert_turso_value(&v))
                                .unwrap_or(MetastoreValue::Null)
                        })
                        .collect();

                    let turso_row = TursoRow { values };
                    results.push(f(&turso_row)?);
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(CatalogError::Database {
                        message: format!("Failed to fetch row: {e}"),
                    });
                }
            }
        }

        Ok(results)
    }

    async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>> {
        let conn_arc = self.get_conn().await?;
        let guard = conn_arc.lock_owned().await;

        {
            let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
                message: "Turso connection not initialized".to_string(),
            })?;
            conn.execute("BEGIN CONCURRENT", ())
                .await
                .map_err(|e| CatalogError::Database {
                    message: format!("Failed to begin concurrent transaction: {e}"),
                })?;
        }

        Ok(Box::new(TursoTransaction { conn: Some(guard) }))
    }

    async fn shutdown(&self) -> CatalogResult<()> {
        // Turso handles cleanup automatically
        tracing::info!("Shutting down Turso metastore");
        Ok(())
    }
}

/// A transaction on a Turso metastore connection.
///
/// Holds an [`OwnedMutexGuard`] on the underlying connection, ensuring
/// exclusive access for the lifetime of the transaction.
///
/// If neither [`commit`](MetastoreTransaction::commit) nor
/// [`rollback`](MetastoreTransaction::rollback) is called, the transaction
/// is automatically rolled back on drop via a best-effort `ROLLBACK`.
pub struct TursoTransaction {
    /// Exclusive lock on the connection. `None` after commit/rollback.
    conn: Option<OwnedMutexGuard<Option<Connection>>>,
}

impl Drop for TursoTransaction {
    fn drop(&mut self) {
        if let Some(guard) = self.conn.take() {
            // Spawn a best-effort async rollback while holding the owned guard.
            // The guard (and its mutex lock) is moved into the spawned task and
            // released after the ROLLBACK completes or fails.
            tokio::spawn(async move {
                tracing::debug!(
                    "TursoTransaction dropped without explicit commit or rollback; \
                     attempting auto-rollback"
                );
                if let Some(conn) = guard.as_ref()
                    && let Err(err) = conn.execute("ROLLBACK", ()).await
                {
                    tracing::error!("Failed to auto-rollback TursoTransaction on drop: {err}");
                }
                // `guard` is dropped here, releasing the connection lock.
            });
        }
    }
}

#[async_trait]
impl MetastoreTransaction for TursoTransaction {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let guard = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        let mut stmt = conn
            .prepare_cached(params.sql)
            .await
            .map_err(convert_turso_error)?;
        stmt.execute(turso_params)
            .await
            .map_err(convert_turso_error)?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let guard = self.conn.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        conn.execute_batch(sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to execute batch in transaction: {e}"),
            })?;

        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> CatalogResult<()> {
        let guard = self.conn.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        if let Err(e) = conn.execute("COMMIT", ()).await {
            // Best-effort rollback to leave the connection in a clean state.
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(CatalogError::Database {
                message: format!("Failed to commit transaction: {e}"),
            });
        }

        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> CatalogResult<()> {
        let guard = self.conn.take().ok_or_else(|| CatalogError::Database {
            message: "Transaction already completed".to_string(),
        })?;
        let conn = guard.as_ref().ok_or_else(|| CatalogError::Database {
            message: "Turso connection not initialized".to_string(),
        })?;

        conn.execute("ROLLBACK", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to rollback transaction: {e}"),
            })?;

        Ok(())
    }
}
