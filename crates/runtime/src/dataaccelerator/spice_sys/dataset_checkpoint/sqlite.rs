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

use super::{DatasetCheckpoint, Result, CHECKPOINT_TABLE_NAME};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

impl DatasetCheckpoint {
    pub(super) async fn init_sqlite(pool: &SqliteConnectionPool) -> Result<()> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
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

                Ok(())
            })
            .await
            .map_err(|e| e.to_string().into())
    }

    pub(super) async fn migrate_sqlite(pool: &SqliteConnectionPool) -> Result<()> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
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

                Ok(())
            })
            .await
            .map_err(|e| e.to_string().into())
    }

    pub(super) async fn exists_sqlite(&self, pool: &SqliteConnectionPool) -> Result<bool> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
        };
        let dataset_name = self.dataset_name.clone();
        conn.conn
            .call(move |conn| {
                let query =
                    format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;
                Ok(rows.next()?.is_some())
            })
            .await
            .map_err(|e| e.to_string().into())
    }

    pub(super) async fn checkpoint_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        schema: &SchemaRef,
    ) -> Result<()> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
        };
        let dataset_name = self.dataset_name.clone();
        let schema_json = Self::serialize_schema(schema)?;

        conn.conn
            .call(move |conn| {
                let upsert = format!(
                    "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, schema_json, updated_at)
                     VALUES (?1, ?2, CURRENT_TIMESTAMP)
                     ON CONFLICT (dataset_name) DO UPDATE 
                     SET schema_json = ?2, updated_at = CURRENT_TIMESTAMP"
                );
                conn.execute(&upsert, [&dataset_name, &schema_json])?;

                Ok(())
            })
            .await
            .map_err(|e| e.to_string().into())
    }

    pub(super) async fn get_schema_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Result<Option<SchemaRef>> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
        };
        let dataset_name = self.dataset_name.clone();

        let schema_json: Option<String> = conn
            .conn
            .call(move |conn| {
                let query = format!(
                    "SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    Ok(row.get(0)?)
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(Box::new)?;

        match schema_json {
            Some(json) => Ok(Some(Self::deserialize_schema(&json)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataaccelerator::spice_sys::AccelerationConnection;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_table_providers::sql::db_connection_pool::{
        sqlitepool::SqliteConnectionPoolFactory, Mode,
    };

    async fn create_in_memory_sqlite_checkpoint() -> DatasetCheckpoint {
        let pool = SqliteConnectionPoolFactory::new(
            "",
            Mode::Memory,
            std::time::Duration::from_millis(5000),
        )
        .build()
        .await
        .expect("to build in-memory sqlite connection pool");
        DatasetCheckpoint::init_sqlite(&pool)
            .await
            .expect("Failed to initialize SQLite");
        DatasetCheckpoint::migrate_sqlite(&pool)
            .await
            .expect("Failed to migrate SQLite");
        DatasetCheckpoint {
            dataset_name: "test_dataset".to_string(),
            acceleration_connection: AccelerationConnection::SQLite(pool),
        }
    }

    async fn create_legacy_sqlite_checkpoint() -> (DatasetCheckpoint, SqliteConnectionPool) {
        let pool = SqliteConnectionPoolFactory::new(
            "",
            Mode::Memory,
            std::time::Duration::from_millis(5000),
        )
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

                Ok(())
            })
            .await
            .expect("Failed to create legacy table");

        (
            DatasetCheckpoint {
                dataset_name: "legacy_dataset".to_string(),
                acceleration_connection: AccelerationConnection::SQLite(
                    pool.try_clone().await.expect("to clone pool"),
                ),
            },
            pool,
        )
    }

    #[tokio::test]
    async fn test_sqlite_migration() {
        let (checkpoint, pool) = create_legacy_sqlite_checkpoint().await;

        // Run migration
        DatasetCheckpoint::migrate_sqlite(&pool)
            .await
            .expect("Migration failed");

        // Verify schema column exists by trying to use it
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema_ref = std::sync::Arc::new(schema.clone());

        checkpoint
            .checkpoint(&schema_ref)
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
            .checkpoint(&schema_ref)
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
            .checkpoint(&schema_ref)
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
            .checkpoint(&schema_ref1)
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
            .checkpoint(&schema_ref2)
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
        let AccelerationConnection::SQLite(pool) = &checkpoint.acceleration_connection else {
            panic!("Unexpected acceleration connection type");
        };
        let conn_sync = pool.connect_sync();
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
                    Err(tokio_rusqlite::Error::Other(
                        "No checkpoint found".into(),
                    ))
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
}
