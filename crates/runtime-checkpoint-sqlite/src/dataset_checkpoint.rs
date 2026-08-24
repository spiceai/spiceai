/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! The dataset schema / refresh-SQL checkpoint table.
//!
//! ```sql
//! CREATE TABLE spice_sys_dataset_checkpoint (
//!     dataset_name TEXT PRIMARY KEY,
//!     schema_json TEXT,
//!     refresh_sql TEXT,
//!     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//!     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//! );
//! ```

use std::{sync::Arc, time::SystemTime};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};
use runtime_acceleration::dataset_checkpoint::{
    DatasetCheckpointer, deserialize_schema, serialize_schema,
};
use runtime_checkpoint_api::CheckpointError;

use crate::{downcast_failed, store_error};

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";

/// Dataset schema/refresh-SQL checkpoint backed by a `SQLite` accelerator.
pub struct SqliteDatasetCheckpointer {
    pool: Arc<SqliteConnectionPool>,
    dataset_name: String,
}

impl SqliteDatasetCheckpointer {
    /// Opens the checkpoint table for `dataset_name`, creating and migrating it if
    /// needed.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Store`] when the table cannot be created or migrated.
    pub async fn try_new(
        pool: Arc<SqliteConnectionPool>,
        dataset_name: String,
    ) -> Result<Self, CheckpointError> {
        Self::init_sqlite(&pool).await?;
        Self::migrate_sqlite(&pool).await?;
        Ok(Self::new(pool, dataset_name))
    }

    /// The checkpointer over an already-initialized table.
    ///
    /// [`Self::try_new`] is the constructor callers want; this one exists for the
    /// migration tests, which have to observe a pre-migration table.
    fn new(pool: Arc<SqliteConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }

    async fn init_sqlite(pool: &SqliteConnectionPool) -> Result<(), CheckpointError> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                let create_table = format!(
                    "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                        dataset_name TEXT PRIMARY KEY,
                        schema_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )"
                );
                conn.execute(&create_table, [])?;

                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(store_error)
    }

    async fn migrate_sqlite(pool: &SqliteConnectionPool) -> Result<(), CheckpointError> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                // Check if schema_json column exists
                let columns: Vec<String> = conn
                    .prepare(&format!("PRAGMA table_info({CHECKPOINT_TABLE_NAME})"))?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                if !columns.contains(&"schema_json".to_string()) {
                    conn.execute(
                        &format!("ALTER TABLE {CHECKPOINT_TABLE_NAME} ADD COLUMN schema_json TEXT"),
                        [],
                    )?;
                }

                if !columns.contains(&"refresh_sql".to_string()) {
                    conn.execute(
                        &format!("ALTER TABLE {CHECKPOINT_TABLE_NAME} ADD COLUMN refresh_sql TEXT"),
                        [],
                    )?;
                }

                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(store_error)
    }

    async fn exists_inner(&self) -> Result<bool, CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                let query =
                    format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;
                Ok::<bool, rusqlite::Error>(rows.next()?.is_some())
            })
            .await
            .map_err(store_error)
    }

    async fn last_checkpoint_time_inner(&self) -> Result<Option<SystemTime>, CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        let query = format!(
            "SELECT updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1"
        );
        let checkpoint_time: Option<DateTime<Utc>> = conn
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([&dataset_name])?;
                Ok::<Option<std::result::Result<DateTime<Utc>, rusqlite::Error>>, rusqlite::Error>(
                    rows.next()?.map(|row| row.get(0)),
                )
            })
            .await
            .map_err(store_error)?
            .transpose()
            .map_err(store_error)?;

        let checkpoint_time = checkpoint_time.map(Into::into);
        Ok(checkpoint_time)
    }

    async fn checkpoint_inner(
        &self,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();
        let schema_json = serialize_schema(schema).map_err(store_error)?;
        let refresh_sql_owned = refresh_sql.map(ToString::to_string);

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                let upsert = format!(
                    "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, schema_json, refresh_sql, updated_at)
                     VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)
                     ON CONFLICT (dataset_name) DO UPDATE
                     SET schema_json = ?2, refresh_sql = ?3, updated_at = CURRENT_TIMESTAMP"
                );
                conn.execute(&upsert, rusqlite::params![&dataset_name, &schema_json, &refresh_sql_owned])?;

                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(store_error)
    }

    async fn get_refresh_sql_inner(&self) -> Result<Option<String>, CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                let query = format!(
                    "SELECT refresh_sql FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    Ok::<Option<String>, rusqlite::Error>(row.get(0)?)
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(store_error)
    }

    async fn get_schema_inner(&self) -> Result<Option<SchemaRef>, CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        let schema_json: Option<String> = conn
            .conn
            .call(move |conn| {
                let query = format!(
                    "SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    Ok::<Option<String>, rusqlite::Error>(Some(row.get(0)?))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(store_error)?;

        match schema_json {
            Some(json) => Ok(Some(deserialize_schema(&json).map_err(store_error)?)),
            None => Ok(None),
        }
    }

    async fn delete_inner(&self) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                let delete = format!("DELETE FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?1");
                conn.execute(&delete, [&dataset_name])?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(store_error)
    }
}

#[async_trait]
impl DatasetCheckpointer for SqliteDatasetCheckpointer {
    async fn exists(&self) -> bool {
        self.exists_inner().await.unwrap_or(false)
    }

    async fn checkpoint(
        &self,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        self.checkpoint_inner(schema, refresh_sql)
            .await
            .map_err(Into::into)
    }

    async fn get_schema(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SchemaRef>> {
        self.get_schema_inner().await.map_err(Into::into)
    }

    async fn last_checkpoint_time(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SystemTime>> {
        self.last_checkpoint_time_inner().await.map_err(Into::into)
    }

    async fn get_refresh_sql(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<String>> {
        self.get_refresh_sql_inner().await.map_err(Into::into)
    }

    async fn delete(&self) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        self.delete_inner().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_table_providers::sql::db_connection_pool::{
        Mode, sqlitepool::SqliteConnectionPoolFactory,
    };
    use std::sync::Arc;

    async fn create_in_memory_sqlite_checkpoint() -> SqliteDatasetCheckpointer {
        let pool =
            SqliteConnectionPoolFactory::new("", Mode::Memory, std::time::Duration::from_secs(5))
                .build()
                .await
                .expect("to build in-memory sqlite connection pool");
        SqliteDatasetCheckpointer::init_sqlite(&pool)
            .await
            .expect("Failed to initialize SQLite");
        SqliteDatasetCheckpointer::migrate_sqlite(&pool)
            .await
            .expect("Failed to migrate SQLite");
        SqliteDatasetCheckpointer::new(Arc::new(pool), "test_dataset".to_string())
    }

    async fn create_legacy_sqlite_checkpoint()
    -> (SqliteDatasetCheckpointer, Arc<SqliteConnectionPool>) {
        let pool =
            SqliteConnectionPoolFactory::new("", Mode::Memory, std::time::Duration::from_secs(5))
                .build()
                .await
                .expect("to build in-memory sqlite connection pool");

        // Create legacy table without schema_json column
        let conn_sync = pool.connect_sync();
        let conn = conn_sync
            .as_any()
            .downcast_ref::<SqliteConnection>()
            .expect("sqlite connection");

        conn.conn
            .call(move |conn| {
                conn.execute(
                    &format!(
                        "CREATE TABLE {CHECKPOINT_TABLE_NAME} (
                        dataset_name TEXT PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )"
                    ),
                    [],
                )?;

                // Insert legacy data
                conn.execute(
                    &format!("INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name) VALUES (?)"),
                    ["legacy_dataset"],
                )?;

                Ok::<(), rusqlite::Error>(())
            })
            .await
            .expect("Failed to create legacy table");

        (
            SqliteDatasetCheckpointer::new(
                Arc::new(pool.try_clone().await.expect("to clone pool")),
                "legacy_dataset".to_string(),
            ),
            Arc::new(pool),
        )
    }

    #[tokio::test]
    async fn test_sqlite_migration() {
        let (checkpoint, pool) = create_legacy_sqlite_checkpoint().await;

        // Run migration
        SqliteDatasetCheckpointer::migrate_sqlite(&pool)
            .await
            .expect("Migration failed");

        // Verify schema column exists by trying to use it
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema_ref = std::sync::Arc::new(schema.clone());

        checkpoint
            .checkpoint(&schema_ref, None)
            .await
            .expect("Failed to save schema after migration");

        let retrieved_schema = checkpoint
            .get_schema()
            .await
            .expect("Failed to get schema")
            .expect("Schema should exist");

        assert_eq!(&schema, retrieved_schema.as_ref());

        // Verify old data still exists
        assert!(checkpoint.exists().await);
    }

    #[tokio::test]
    async fn test_sqlite_schema_roundtrip() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Create a test schema
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema_ref = std::sync::Arc::new(schema.clone());

        // Save the schema
        checkpoint
            .checkpoint(&schema_ref, None)
            .await
            .expect("Failed to save schema");

        // Retrieve the schema
        let retrieved_schema = checkpoint
            .get_schema()
            .await
            .expect("Failed to get schema")
            .expect("Schema should exist");

        assert_eq!(&schema, retrieved_schema.as_ref());
    }

    #[tokio::test]
    async fn test_sqlite_checkpoint_exists() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Initially, the checkpoint should not exist
        assert!(!checkpoint.exists().await);

        // Create a test schema
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema_ref = std::sync::Arc::new(schema.clone());

        // Create the checkpoint with schema
        checkpoint
            .checkpoint(&schema_ref, None)
            .await
            .expect("Failed to create checkpoint");

        // Now the checkpoint should exist
        assert!(checkpoint.exists().await);

        // Verify schema was saved
        let retrieved_schema = checkpoint
            .get_schema()
            .await
            .expect("Failed to get schema")
            .expect("Schema should exist");
        assert_eq!(&schema, retrieved_schema.as_ref());
    }

    #[tokio::test]
    async fn test_sqlite_checkpoint_update() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Create initial schema
        let schema1 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema_ref1 = std::sync::Arc::new(schema1.clone());

        // Create the initial checkpoint
        checkpoint
            .checkpoint(&schema_ref1, None)
            .await
            .expect("Failed to create initial checkpoint");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Create updated schema
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema_ref2 = std::sync::Arc::new(schema2.clone());

        // Update the checkpoint with new schema
        checkpoint
            .checkpoint(&schema_ref2, None)
            .await
            .expect("Failed to update checkpoint");

        // Verify the schema was updated
        let retrieved_schema = checkpoint
            .get_schema()
            .await
            .expect("Failed to get schema")
            .expect("Schema should exist");
        assert_eq!(&schema2, retrieved_schema.as_ref());

        // Verify that the updated_at timestamp has changed
        let conn_sync = checkpoint.pool.connect_sync();
        let conn = conn_sync
            .as_any()
            .downcast_ref::<SqliteConnection>()
            .expect("sqlite connection");
        let result = conn.conn
            .call(move |conn| {
                let query = format!(
                    "SELECT created_at, updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?",
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([&checkpoint.dataset_name])?;

                if let Some(row) = rows.next()? {
                    let created_at: String = row.get(0)?;
                    let updated_at: String = row.get(1)?;
                    Ok((created_at, updated_at))
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows)
                }
            })
            .await
            .expect("Failed to fetch checkpoint data");

        let (created_at, updated_at) = result;
        assert_ne!(
            created_at, updated_at,
            "created_at and updated_at should be different"
        );
    }

    #[tokio::test]
    async fn test_sqlite_refresh_sql_roundtrip() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema_ref = std::sync::Arc::new(schema);

        // Store a refresh_sql
        checkpoint
            .checkpoint(&schema_ref, Some("SELECT * FROM source_table"))
            .await
            .expect("Failed to store checkpoint with refresh_sql");

        let stored = checkpoint
            .get_refresh_sql()
            .await
            .expect("Failed to get refresh_sql")
            .expect("refresh_sql should be Some");
        assert_eq!(stored, "SELECT * FROM source_table");

        // Update to a different refresh_sql
        checkpoint
            .checkpoint(
                &schema_ref,
                Some("SELECT id FROM source_table WHERE id > 10"),
            )
            .await
            .expect("Failed to update refresh_sql");

        let updated = checkpoint
            .get_refresh_sql()
            .await
            .expect("Failed to get updated refresh_sql")
            .expect("refresh_sql should still be Some");
        assert_eq!(updated, "SELECT id FROM source_table WHERE id > 10");

        // Clear refresh_sql by passing None — should overwrite (no COALESCE)
        checkpoint
            .checkpoint(&schema_ref, None)
            .await
            .expect("Failed to clear refresh_sql");

        let cleared = checkpoint
            .get_refresh_sql()
            .await
            .expect("Failed to get cleared refresh_sql");
        assert!(
            cleared.is_none(),
            "refresh_sql should be None after passing None (no COALESCE)"
        );
    }

    #[tokio::test]
    async fn test_sqlite_last_checkpoint_time() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Initially, there should be no checkpoint time
        assert!(
            checkpoint
                .last_checkpoint_time()
                .await
                .expect("Unexpected checkpoint failure")
                .is_none()
        );

        // Create a test schema
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema_ref = std::sync::Arc::new(schema);

        // Create the checkpoint
        checkpoint
            .checkpoint(&schema_ref, None)
            .await
            .expect("Failed to create checkpoint");

        // Now there should be a checkpoint time
        let checkpoint_time = checkpoint
            .last_checkpoint_time()
            .await
            .expect("Failed to get checkpoint time")
            .expect("Checkpoint time should exist");

        // Verify the checkpoint time is recent
        let now = SystemTime::now();
        let time_diff = now
            .duration_since(checkpoint_time)
            .expect("Time difference should be positive");
        assert!(time_diff.as_secs() < 5, "Checkpoint time should be recent");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Update the checkpoint
        checkpoint
            .checkpoint(&schema_ref, None)
            .await
            .expect("Failed to update checkpoint");

        // Get the new checkpoint time
        let new_checkpoint_time = checkpoint
            .last_checkpoint_time()
            .await
            .expect("Failed to get new checkpoint time")
            .expect("New checkpoint time should exist");

        // Verify the new checkpoint time is more recent than the old one
        assert!(
            new_checkpoint_time > checkpoint_time,
            "New checkpoint time should be more recent"
        );
    }
}
