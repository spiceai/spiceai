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

use super::{DatasetCheckpoint, Result, CHECKPOINT_TABLE_NAME};
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

    pub(super) async fn checkpoint_sqlite(&self, pool: &SqliteConnectionPool) -> Result<()> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
        };
        let dataset_name = self.dataset_name.clone();
        conn.conn
            .call(move |conn| {
                let upsert = format!(
                    "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, updated_at)
                 VALUES (?1, CURRENT_TIMESTAMP)
                 ON CONFLICT (dataset_name) DO UPDATE SET updated_at = CURRENT_TIMESTAMP"
                );
                conn.execute(&upsert, [dataset_name])?;

                Ok(())
            })
            .await
            .map_err(|e| e.to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use datafusion_table_providers::sql::db_connection_pool::{
        sqlitepool::SqliteConnectionPoolFactory, Mode,
    };

    use crate::dataaccelerator::spice_sys::AccelerationConnection;

    use super::*;

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
        DatasetCheckpoint {
            dataset_name: "test_dataset".to_string(),
            acceleration_connection: AccelerationConnection::SQLite(pool),
        }
    }

    #[tokio::test]
    async fn test_sqlite_checkpoint_exists() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

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
    async fn test_sqlite_checkpoint_update() {
        let checkpoint = create_in_memory_sqlite_checkpoint().await;

        // Create the initial checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to create initial checkpoint");

        // Sleep for a short time to ensure the timestamp changes
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Update the checkpoint
        checkpoint
            .checkpoint()
            .await
            .expect("Failed to update checkpoint");

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
