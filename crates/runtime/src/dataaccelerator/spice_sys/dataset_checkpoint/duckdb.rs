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

use datafusion::arrow::datatypes::SchemaRef;
use std::sync::Arc;

use super::{DatasetCheckpoint, Result, CHECKPOINT_TABLE_NAME, SCHEMA_MIGRATION_01_STMT};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;

impl DatasetCheckpoint {
    pub(super) fn init_duckdb(pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(super) fn exists_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<bool> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let query = format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1");
        let mut stmt = duckdb_conn.prepare(&query).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query([&self.dataset_name])
            .map_err(|e| e.to_string())?;

        Ok(rows.next().map_err(|e| e.to_string())?.is_some())
    }

    pub(super) fn checkpoint_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        schema: &SchemaRef,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let schema_json = Self::serialize_schema(schema)?;
        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, created_at, updated_at, schema_json)
             VALUES (?, now(), now(), ?)
             ON CONFLICT (dataset_name) DO UPDATE 
             SET updated_at = now(), schema_json = excluded.schema_json"
        );
        duckdb_conn
            .execute(&upsert, [&self.dataset_name, &schema_json])
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(super) fn migrate_duckdb(pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        duckdb_conn
            .execute(SCHEMA_MIGRATION_01_STMT, [])
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(super) fn get_schema_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Result<Option<SchemaRef>> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let query = format!(
            "SELECT schema_json FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ? LIMIT 1"
        );
        let mut stmt = duckdb_conn.prepare(&query).map_err(|e| e.to_string())?;
        let mut rows = stmt
            .query([&self.dataset_name])
            .map_err(|e| e.to_string())?;

        if let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let schema_json: Option<String> = row.get(0).map_err(|e| e.to_string())?;
            match schema_json {
                Some(json) => Ok(Some(Self::deserialize_schema(&json)?)),
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dataaccelerator::spice_sys::AccelerationConnection;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use std::sync::Arc;

    fn create_in_memory_duckdb_checkpoint() -> (DatasetCheckpoint, Arc<DuckDbConnectionPool>) {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory().expect("Failed to create in-memory DuckDB database"),
        );
        DatasetCheckpoint::init_duckdb(&pool).expect("Failed to initialize DuckDB");
        DatasetCheckpoint::migrate_duckdb(&pool).expect("Failed to migrate DuckDB");
        (
            DatasetCheckpoint {
                dataset_name: "test_dataset".to_string(),
                acceleration_connection: AccelerationConnection::DuckDB(Arc::clone(&pool)),
            },
            pool,
        )
    }

    fn create_legacy_table(pool: &Arc<DuckDbConnectionPool>) {
        let mut db_conn = Arc::clone(pool).connect_sync().expect("Failed to connect");
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .expect("Failed to get conn")
            .get_underlying_conn_mut();

        // Create table without schema_json column
        duckdb_conn
            .execute(
                &format!(
                    "CREATE TABLE {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
                ),
                [],
            )
            .expect("Failed to create legacy table");

        // Insert some test data
        duckdb_conn
            .execute(
                &format!(
                    "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, created_at, updated_at) 
             VALUES ('legacy_dataset', now(), now())"
                ),
                [],
            )
            .expect("Failed to insert legacy data");
    }

    #[tokio::test]
    async fn test_duckdb_schema_roundtrip() {
        let (checkpoint, _) = create_in_memory_duckdb_checkpoint();

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
    async fn test_duckdb_migration() {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory().expect("Failed to create in-memory DuckDB database"),
        );

        // Create legacy table format
        create_legacy_table(&pool);

        // Create checkpoint which should trigger migration
        let checkpoint = DatasetCheckpoint {
            dataset_name: "legacy_dataset".to_string(),
            acceleration_connection: AccelerationConnection::DuckDB(Arc::clone(&pool)),
        };

        // Run migration
        DatasetCheckpoint::migrate_duckdb(&pool).expect("Migration failed");

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
    async fn test_duckdb_checkpoint_exists() {
        let (checkpoint, _) = create_in_memory_duckdb_checkpoint();

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
    async fn test_duckdb_checkpoint_update() {
        let (checkpoint, _) = create_in_memory_duckdb_checkpoint();

        // Create initial schema
        let schema1 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema_ref1 = std::sync::Arc::new(schema1.clone());

        // Create the initial checkpoint
        checkpoint
            .checkpoint(&schema_ref1)
            .await
            .expect("Failed to create initial checkpoint");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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
        if let AccelerationConnection::DuckDB(pool) = &checkpoint.acceleration_connection {
            let mut db_conn = Arc::clone(pool)
                .connect_sync()
                .expect("Failed to connect to DuckDB");
            let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
                .expect("Failed to get DuckDB connection")
                .get_underlying_conn_mut();

            let query = format!(
                "SELECT created_at, updated_at FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = ?",
            );
            let mut stmt = duckdb_conn
                .prepare(&query)
                .expect("Failed to prepare SQL statement");
            let mut rows = stmt
                .query([&checkpoint.dataset_name])
                .expect("Failed to execute query");

            if let Some(row) = rows.next().expect("Failed to fetch row") {
                let created_at: duckdb::types::Value =
                    row.get(0).expect("Failed to get created_at");
                let duckdb::types::Value::Timestamp(_, created_at) = created_at else {
                    panic!("created_at is not a timestamp");
                };
                let updated_at: duckdb::types::Value =
                    row.get(1).expect("Failed to get updated_at");
                let duckdb::types::Value::Timestamp(_, updated_at) = updated_at else {
                    panic!("updated_at is not a timestamp");
                };
                assert_ne!(
                    created_at, updated_at,
                    "created_at and updated_at should be different"
                );
            } else {
                panic!("No checkpoint found");
            }
        } else {
            panic!("Unexpected acceleration connection type");
        }
    }
}
