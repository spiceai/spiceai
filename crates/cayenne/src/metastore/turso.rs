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
    ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow, MetastoreValue, QueryParams,
    QueryRowParams,
};
use crate::catalog::{CatalogError, CatalogResult};
use async_trait::async_trait;
use std::sync::Arc;
use std::{fmt::Debug, path::Path};
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value as TursoValue};

/// Turso-based metastore backend.
pub struct TursoMetastore {
    db: Arc<Mutex<Option<Database>>>,
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
            db: Arc::new(Mutex::new(None)),
            connection_string: connection_string.into(),
        }
    }

    /// Get the database file path from the connection string.
    fn db_path(&self) -> &str {
        self.connection_string
            .strip_prefix("libsql://")
            .unwrap_or(&self.connection_string)
    }

    /// Get or create the database connection.
    async fn get_db(&self) -> CatalogResult<Database> {
        let mut db_guard = self.db.lock().await;

        if let Some(db) = db_guard.as_ref() {
            return Ok(db.clone());
        }

        // Create the database
        let db_path = self.db_path();

        // Create parent directory if it doesn't exist
        let db_dir =
            Path::new(db_path)
                .parent()
                .ok_or_else(|| CatalogError::InvalidDatabasePath {
                    path: db_path.to_string(),
                })?;

        if !db_dir.exists() {
            tokio::fs::create_dir_all(db_dir).await?;
        }

        let db = Builder::new_local(db_path)
            .build()
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to open Turso database: {e}"),
            })?;

        *db_guard = Some(db.clone());
        Ok(db)
    }

    /// Get a connection from the database.
    async fn get_conn(&self) -> CatalogResult<Connection> {
        let db = self.get_db().await?;
        let conn = db.connect().map_err(|e| CatalogError::Database {
            message: format!("Failed to connect to Turso database: {e}"),
        })?;

        // Set busy timeout to wait for locks instead of immediately returning SQLITE_BUSY.
        // This fixes issue #8826 where concurrent transactions to the same database fail.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set busy timeout: {e}"),
            })?;

        Ok(conn)
    }

    /// Schema for the `cayenne_table` table.
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

/// Turso row wrapper implementing `MetastoreRow`.
struct TursoRow {
    values: Vec<MetastoreValue>,
}

impl MetastoreRow for TursoRow {
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
        // turso 0.4.x uses dedicated Constraint variant for constraint violations
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
        let conn = self.get_conn().await?;

        // NORMAL synchronous mode: safe with WAL, more performant than FULL
        // With WAL mode, NORMAL only syncs at checkpoints, not on every commit
        conn.execute("PRAGMA synchronous = NORMAL", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set synchronous mode: {e}"),
            })?;

        // 32MB cache size (negative value = kilobytes in SQLite/libSQL)
        // Larger cache reduces disk I/O for frequently accessed metadata
        conn.execute("PRAGMA cache_size = -32768", ())
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to set cache size: {e}"),
            })?;

        // Create tables
        let schema_sql = format!(
            "{}; {}; {}; {}; {};",
            Self::TABLE_TABLE_DDL,
            Self::DELETE_FILE_TABLE_DDL,
            Self::PARTITION_TABLE_DDL,
            Self::INSERT_RECORD_TABLE_DDL,
            Self::SNAPSHOT_SEQUENCE_TABLE_DDL
        );

        conn.execute_batch(&schema_sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to initialize schema: {e}"),
            })?;

        // Attempt to backfill newly added columns for existing deployments. Errors are ignored
        // because the column may already exist (libSQL doesn't support IF NOT EXISTS for ALTER).
        let _ = conn
            .execute(
                "ALTER TABLE cayenne_table ADD COLUMN on_conflict_json TEXT",
                (),
            )
            .await;

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn = self.get_conn().await?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        conn.execute(params.sql, turso_params)
            .await
            .map_err(convert_turso_error)?;

        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        let conn = self.get_conn().await?;

        conn.execute_batch(sql)
            .await
            .map_err(|e| CatalogError::Database {
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

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        let mut rows =
            conn.query(params.sql, turso_params)
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
        let conn = self.get_conn().await?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        let mut rows =
            conn.query(params.sql, turso_params)
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

    async fn shutdown(&self) -> CatalogResult<()> {
        // Turso handles cleanup automatically
        tracing::info!("Shutting down Turso metastore");
        Ok(())
    }
}
