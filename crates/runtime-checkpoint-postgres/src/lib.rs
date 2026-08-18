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

//! Per-dataset **blob** checkpoint store backed by a `PostgreSQL` accelerator.
//!
//! Persists one opaque `String` payload keyed by `dataset_name` into a
//! `(dataset_name PK, checkpoint_data TEXT, created_at, updated_at)` sidecar table
//! whose name the caller chooses. Implements
//! [`runtime_checkpoint_api::BlobCheckpointStore`]; the `runtime` crate resolves a
//! dataset's accelerator connection and constructs it.

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use runtime_checkpoint_api::{BlobCheckpoint, BlobCheckpointStore, CheckpointError};

/// Blob checkpoint store backed by a `PostgreSQL` accelerator.
pub struct PostgresBlobCheckpointStore {
    pool: PostgresConnectionPool,
    dataset_name: String,
    table_name: &'static str,
}

impl PostgresBlobCheckpointStore {
    #[must_use]
    pub fn new(
        pool: PostgresConnectionPool,
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
impl BlobCheckpointStore for PostgresBlobCheckpointStore {
    async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .map_err(|source| CheckpointError::Store { source })?;
        let table = self.table_name;

        // Ensure the sidecar table exists so a fresh accelerator reads as "no
        // checkpoint yet" (Ok(None)) rather than a missing-table store error.
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;

        let query = format!(
            "SELECT checkpoint_data, EXTRACT(EPOCH FROM updated_at) FROM {table} WHERE dataset_name = $1"
        );
        let stmt = conn
            .conn
            .prepare(&query)
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;
        let Some(row) = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?
        else {
            return Ok(None);
        };

        let data: String = row.get(0);
        let updated_at_epoch: Option<f64> = row.get(1);
        let updated_at = updated_at_epoch.and_then(|epoch| {
            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs_f64(epoch))
        });
        Ok(Some(BlobCheckpoint { data, updated_at }))
    }

    async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
        let conn = self
            .pool
            .connect_direct()
            .await
            .map_err(|source| CheckpointError::Store { source })?;
        let table = self.table_name;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;

        let upsert = format!(
            "INSERT INTO {table} (dataset_name, checkpoint_data, updated_at)
             VALUES ($1, $2, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                checkpoint_data = EXCLUDED.checkpoint_data,
                updated_at = CURRENT_TIMESTAMP"
        );
        let checkpoint_data = data.to_string();
        conn.conn
            .execute(&upsert, &[&self.dataset_name, &checkpoint_data])
            .await
            .map_err(|source| CheckpointError::Store {
                source: Box::new(source),
            })?;
        Ok(())
    }
}
