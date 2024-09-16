/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use super::{DatasetCheckpoint, Result, CHECKPOINT_TABLE_NAME};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;

impl DatasetCheckpoint {
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

    pub(super) fn checkpoint_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Result<()> {
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

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, created_at, updated_at)
             VALUES (?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET updated_at = now()"
        );
        duckdb_conn
            .execute(&upsert, [&self.dataset_name])
            .map_err(|e| e.to_string())?;

        duckdb_conn
            .execute("CHECKPOINT", [])
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::dataaccelerator::spice_sys::AccelerationConnection;

    use super::*;
    use std::sync::Arc;

    fn create_in_memory_duckdb_checkpoint() -> DatasetCheckpoint {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory().expect("Failed to create in-memory DuckDB database"),
        );
        DatasetCheckpoint {
            dataset_name: "test_dataset".to_string(),
            acceleration_connection: AccelerationConnection::DuckDB(pool),
        }
    }

    #[tokio::test]
    async fn test_duckdb_checkpoint_exists() {
        let checkpoint = create_in_memory_duckdb_checkpoint();

        // Initially, the checkpoint should not exist
        assert!(!checkpoint.exists().await);

        // Create the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create checkpoint");

        // Now the checkpoint should exist
        assert!(checkpoint.exists().await);
    }

    #[tokio::test]
    async fn test_duckdb_checkpoint_update() {
        let checkpoint = create_in_memory_duckdb_checkpoint();

        // Create the initial checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create initial checkpoint");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Update the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to update checkpoint");

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
