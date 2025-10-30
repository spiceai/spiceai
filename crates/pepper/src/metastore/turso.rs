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
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value as TursoValue};

/// Turso-based metastore backend.
pub struct TursoMetastore {
    db: Arc<Mutex<Option<Database>>>,
    connection_string: String,
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
        let db_dir = Path::new(db_path)
            .parent()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid database path".to_string(),
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
        db.connect().map_err(|e| CatalogError::Database {
            message: format!("Failed to connect to Turso database: {e}"),
        })
    }

    /// Schema for the `pepper_metadata` table that tracks next IDs.
    const METADATA_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_metadata (
            key TEXT PRIMARY KEY,
            value BIGINT NOT NULL
        )
    ";

    /// Schema for the `pepper_table` table.
    const TABLE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_table (
            table_id BIGINT PRIMARY KEY,
            table_uuid TEXT NOT NULL,
            table_name TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            schema_json TEXT NOT NULL,
            primary_key_json TEXT,
            current_snapshot_id TEXT NOT NULL DEFAULT '',
            partition_column TEXT
        )
    ";

    /// Schema for the `pepper_data_file` table.
    const DATA_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_data_file (
            data_file_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            partition_id BIGINT,
            file_order BIGINT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            file_format TEXT NOT NULL,
            record_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            row_id_start BIGINT NOT NULL,
            FOREIGN KEY(partition_id) REFERENCES pepper_partition(partition_id) ON DELETE SET NULL
        )
    ";

    /// Schema for the `pepper_delete_file` table.
    const DELETE_FILE_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_delete_file (
            delete_file_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            data_file_id BIGINT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            format TEXT NOT NULL,
            delete_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL
        )
    ";

    /// Schema for the `pepper_partition` table.
    const PARTITION_TABLE_DDL: &'static str = r"
        CREATE TABLE IF NOT EXISTS pepper_partition (
            partition_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            partition_column TEXT NOT NULL,
            partition_value TEXT NOT NULL,
            path TEXT NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            record_count BIGINT NOT NULL DEFAULT 0,
            file_size_bytes BIGINT NOT NULL DEFAULT 0,
            UNIQUE(table_id, partition_value)
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
        TursoValue::Blob(_) => {
            // We don't use blobs in metadata
            MetastoreValue::Null
        }
    }
}

/// Convert `MetastoreValue` to Turso Value.
fn to_turso_value(value: &MetastoreValue) -> TursoValue {
    match value {
        MetastoreValue::Integer(i) => TursoValue::Integer(*i),
        MetastoreValue::Text(s) => TursoValue::Text(s.clone()),
        MetastoreValue::Bool(b) => TursoValue::Integer(i64::from(*b)),
        MetastoreValue::Null => TursoValue::Null,
    }
}

#[async_trait]
impl MetastoreBackend for TursoMetastore {
    async fn init_schema(&self) -> CatalogResult<()> {
        let conn = self.get_conn().await?;

        // Create tables
        let schema_sql = format!(
            "{}; {}; {}; {}; {};",
            Self::METADATA_TABLE_DDL,
            Self::TABLE_TABLE_DDL,
            Self::DATA_FILE_TABLE_DDL,
            Self::DELETE_FILE_TABLE_DDL,
            Self::PARTITION_TABLE_DDL
        );

        conn.execute_batch(&schema_sql)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to initialize schema: {e}"),
            })?;

        // Initialize metadata with next IDs if not exists
        conn.execute(
            "INSERT OR IGNORE INTO pepper_metadata (key, value) VALUES ('next_catalog_id', 1)",
            (),
        )
        .await
        .map_err(|e| CatalogError::Database {
            message: format!("Failed to initialize metadata: {e}"),
        })?;

        conn.execute(
            "INSERT OR IGNORE INTO pepper_metadata (key, value) VALUES ('next_file_id', 1)",
            (),
        )
        .await
        .map_err(|e| CatalogError::Database {
            message: format!("Failed to initialize metadata: {e}"),
        })?;

        conn.execute(
            "INSERT OR IGNORE INTO pepper_metadata (key, value) VALUES ('next_partition_id', 1)",
            (),
        )
        .await
        .map_err(|e| CatalogError::Database {
            message: format!("Failed to initialize metadata: {e}"),
        })?;

        Ok(())
    }

    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        let conn = self.get_conn().await?;

        let turso_params: Vec<TursoValue> = params.params.iter().map(to_turso_value).collect();

        conn.execute(params.sql, turso_params)
            .await
            .map_err(|e| CatalogError::Database {
                message: format!("Failed to execute statement: {e}"),
            })?;

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
