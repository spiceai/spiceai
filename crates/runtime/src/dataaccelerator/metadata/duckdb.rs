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

use std::{path::Path, sync::Arc};

use datafusion_table_providers::duckdb::DuckDB;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;

use super::AcceleratedMetadataProvider;
use super::{
    METADATA_DATASET_COLUMN, METADATA_KEY_COLUMN, METADATA_METADATA_COLUMN, METADATA_TABLE_NAME,
};
use crate::component::dataset::{acceleration::Engine, Dataset};
use crate::dataaccelerator::{get_accelerator_engine, DuckDBAccelerator};

pub struct AcceleratedMetadataDuckDB {
    pool: Arc<DuckDbConnectionPool>,
}

impl AcceleratedMetadataDuckDB {
    pub async fn try_new(
        dataset: &Dataset,
        create_if_file_not_exists: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let accelerator = get_accelerator_engine(Engine::DuckDB)
            .await
            .ok_or("DuckDB accelerator engine not available")?;
        let duckdb_accelerator = accelerator
            .as_any()
            .downcast_ref::<DuckDBAccelerator>()
            .ok_or("Accelerator is not a DuckDBAccelerator")?;

        let duckdb_file = duckdb_accelerator
            .duckdb_file_path(dataset)
            .ok_or("Acceleration mode is not file-based.")?;
        if !create_if_file_not_exists && !Path::new(&duckdb_file).exists() {
            return Err("DuckDB file does not exist.".into());
        }

        let pool = DuckDbConnectionPool::new_file(&duckdb_file, &AccessMode::ReadWrite)
            .map_err(|e| e.to_string())?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

#[async_trait::async_trait]
impl AcceleratedMetadataProvider for AcceleratedMetadataDuckDB {
    async fn get_metadata(&self, dataset: &str, key: &str) -> Option<String> {
        let mut db_conn = Arc::clone(&self.pool).connect_sync().ok()?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();
        let query = format!(
            "SELECT {METADATA_METADATA_COLUMN} FROM {METADATA_TABLE_NAME} WHERE {METADATA_DATASET_COLUMN} = ? AND {METADATA_KEY_COLUMN} = ?",
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([dataset, key]).ok()?;

        let metadata: Option<String> = if let Some(row) = rows.next().ok()? {
            Some(row.get(0).ok()?)
        } else {
            None
        };

        metadata
    }

    async fn set_metadata(
        &self,
        dataset: &str,
        key: &str,
        metadata: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .map_err(|e| e.to_string())?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let create_if_not_exists = format!(
            "CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                {METADATA_DATASET_COLUMN} TEXT,
                {METADATA_KEY_COLUMN} TEXT,
                {METADATA_METADATA_COLUMN} TEXT,
                PRIMARY KEY ({METADATA_DATASET_COLUMN}, {METADATA_KEY_COLUMN})
            );",
        );

        duckdb_conn
            .execute(&create_if_not_exists, [])
            .map_err(|e| e.to_string())?;

        let query = format!(
            "INSERT INTO {METADATA_TABLE_NAME} ({METADATA_DATASET_COLUMN}, {METADATA_KEY_COLUMN}, {METADATA_METADATA_COLUMN}) VALUES (?, ?, ?) ON CONFLICT ({METADATA_DATASET_COLUMN}, {METADATA_KEY_COLUMN}) DO UPDATE SET {METADATA_METADATA_COLUMN} = ?",
        );

        duckdb_conn
            .execute(&query, [dataset, key, metadata, metadata])
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn create_in_memory_provider() -> AcceleratedMetadataDuckDB {
        let pool =
            Arc::new(DuckDbConnectionPool::new_memory().expect("to create in-memory database"));
        AcceleratedMetadataDuckDB { pool }
    }

    #[tokio::test]
    async fn test_set_and_get_metadata() {
        let provider = create_in_memory_provider();

        // Set metadata
        provider
            .set_metadata("test_dataset", "test_key", "test_value")
            .await
            .expect("to set metadata");

        // Get metadata
        let result = provider.get_metadata("test_dataset", "test_key").await;
        assert_eq!(result, Some("test_value".to_string()));
    }

    #[tokio::test]
    async fn test_update_metadata() {
        let provider = create_in_memory_provider();

        // Set initial metadata
        provider
            .set_metadata("test_dataset", "test_key", "initial_value")
            .await
            .expect("to set metadata");

        // Update metadata
        provider
            .set_metadata("test_dataset", "test_key", "updated_value")
            .await
            .expect("to set metadata");

        // Get updated metadata
        let result = provider.get_metadata("test_dataset", "test_key").await;
        assert_eq!(result, Some("updated_value".to_string()));
    }

    #[tokio::test]
    async fn test_get_nonexistent_metadata() {
        let provider = create_in_memory_provider();

        // Get metadata for a key that doesn't exist
        let result = provider
            .get_metadata("test_dataset", "nonexistent_key")
            .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_set_multiple_keys() {
        let provider = create_in_memory_provider();

        // Set multiple keys
        provider
            .set_metadata("test_dataset", "key1", "value1")
            .await
            .expect("to set metadata");
        provider
            .set_metadata("test_dataset", "key2", "value2")
            .await
            .expect("to set metadata");
        provider
            .set_metadata("another_dataset", "key3", "value3")
            .await
            .expect("to set metadata");

        // Get metadata for multiple keys
        let result1 = provider.get_metadata("test_dataset", "key1").await;
        let result2 = provider.get_metadata("test_dataset", "key2").await;
        let result3 = provider.get_metadata("another_dataset", "key3").await;

        assert_eq!(result1, Some("value1".to_string()));
        assert_eq!(result2, Some("value2".to_string()));
        assert_eq!(result3, Some("value3".to_string()));
    }

    #[tokio::test]
    async fn test_update_multiple_keys() {
        let provider = create_in_memory_provider();

        // Set initial values
        provider
            .set_metadata("test_dataset", "key1", "initial1")
            .await
            .expect("to set metadata");
        provider
            .set_metadata("test_dataset", "key2", "initial2")
            .await
            .expect("to set metadata");

        // Update multiple keys
        provider
            .set_metadata("test_dataset", "key1", "updated1")
            .await
            .expect("to set metadata");
        provider
            .set_metadata("test_dataset", "key2", "updated2")
            .await
            .expect("to set metadata");

        // Get updated metadata
        let result1 = provider.get_metadata("test_dataset", "key1").await;
        let result2 = provider.get_metadata("test_dataset", "key2").await;

        assert_eq!(result1, Some("updated1".to_string()));
        assert_eq!(result2, Some("updated2".to_string()));
    }
}
