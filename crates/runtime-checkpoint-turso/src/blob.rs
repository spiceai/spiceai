/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The opaque single-value checkpoint table.

use std::sync::Arc;

use async_trait::async_trait;
use data_components::turso::TursoConnectionPool;
use runtime_checkpoint_api::{BlobCheckpoint, BlobCheckpointStore, CheckpointError};

/// Blob checkpoint store backed by a `Turso` accelerator.
pub struct TursoBlobCheckpointStore {
    pool: Arc<TursoConnectionPool>,
    dataset_name: String,
    table_name: &'static str,
}

impl TursoBlobCheckpointStore {
    #[must_use]
    pub fn new(
        pool: Arc<TursoConnectionPool>,
        dataset_name: String,
        table_name: &'static str,
    ) -> Self {
        Self {
            pool,
            dataset_name,
            table_name,
        }
    }
}

#[async_trait]
impl BlobCheckpointStore for TursoBlobCheckpointStore {
    async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
        let dataset_name = self.dataset_name.clone();
        let conn = self
            .pool
            .connect()
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;
        let table = self.table_name;

        // Ensure the sidecar table exists so a fresh accelerator reads as "no
        // checkpoint yet" (Ok(None)) rather than a missing-table store error.
        {
            let _schema_guard = self.pool.acquire_schema_write_lock().await;
            let create_table = format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    dataset_name TEXT PRIMARY KEY,
                    checkpoint_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            );
            conn.execute(&create_table, ())
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;
        }

        let _schema_guard = self.pool.acquire_schema_read_lock().await;
        let query = format!(
            "SELECT checkpoint_data, strftime('%s', updated_at) FROM {table} WHERE dataset_name = ?"
        );

        let mut rows = conn
            .query(&query, turso::params![dataset_name])
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;
        let Some(row) = rows.next().await.map_err(|source| CheckpointError::Store {
            source: Box::new(source),
        })?
        else {
            return Ok(None);
        };

        let data = row
            .get::<String>(0)
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;
        let updated_at_epoch: Option<i64> = row.get::<i64>(1).ok();
        let updated_at = updated_at_epoch.and_then(|epoch| {
            u64::try_from(epoch)
                .ok()
                .and_then(|e| std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(e)))
        });
        Ok(Some(BlobCheckpoint { data, updated_at }))
    }

    async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
        let dataset_name = self.dataset_name.clone();
        let checkpoint_data = data.to_string();
        let table = self.table_name;

        let conn = self
            .pool
            .connect()
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;

        {
            let _schema_guard = self.pool.acquire_schema_write_lock().await;
            let create_table = format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    dataset_name TEXT PRIMARY KEY,
                    checkpoint_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            );
            conn.execute(&create_table, ())
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;
        }

        let _schema_guard = self.pool.acquire_schema_read_lock().await;
        let upsert = format!(
            "INSERT INTO {table} (dataset_name, checkpoint_data, updated_at)
             VALUES (?1, ?2, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                checkpoint_data = ?2,
                updated_at = CURRENT_TIMESTAMP"
        );
        conn.execute(&upsert, turso::params![dataset_name, checkpoint_data])
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;
        Ok(())
    }
}
