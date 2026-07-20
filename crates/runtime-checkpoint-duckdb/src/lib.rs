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

//! Per-dataset **blob** checkpoint store backed by a `DuckDB` accelerator.
//!
//! Persists one opaque `String` payload keyed by `dataset_name` into a
//! `(dataset_name PK, checkpoint_data TEXT, created_at, updated_at)` sidecar table
//! whose name the caller chooses, so a single implementation serves *any* CDC
//! connector. Implements [`runtime_checkpoint_api::BlobCheckpointStore`]; the
//! `runtime` crate resolves a dataset's accelerator connection and constructs it.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use runtime_checkpoint_api::{BlobCheckpoint, BlobCheckpointStore, CheckpointError};

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Blob checkpoint store backed by a `DuckDB` accelerator.
pub struct DuckDbBlobCheckpointStore {
    pool: Arc<DuckDbConnectionPool>,
    dataset_name: String,
    table_name: &'static str,
}

impl DuckDbBlobCheckpointStore {
    #[must_use]
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        dataset_name: String,
        table_name: &'static str,
    ) -> Self {
        Self {
            pool,
            dataset_name,
            table_name,
        }
    }

    fn upsert_blocking(&self, data: &str) -> Result<(), BoxedError> {
        let mut db_conn = Arc::clone(&self.pool).connect_sync()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)?
            .get_underlying_conn_mut();
        let table = self.table_name;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn.execute(&create_table, [])?;

        let upsert = format!(
            "INSERT INTO {table} (dataset_name, checkpoint_data, created_at, updated_at)
             VALUES (?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                checkpoint_data = excluded.checkpoint_data,
                updated_at = now()"
        );
        let data = data.to_string();
        duckdb_conn.execute(&upsert, [&self.dataset_name, &data])?;
        Ok(())
    }

    fn get_blocking(&self) -> Result<Option<BlobCheckpoint>, BoxedError> {
        let mut db_conn = Arc::clone(&self.pool).connect_sync()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)?
            .get_underlying_conn_mut();
        let table = self.table_name;

        let query = format!(
            "SELECT checkpoint_data, epoch(updated_at) FROM {table} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query)?;
        let mut rows = stmt.query([&self.dataset_name])?;

        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let data: String = row.get(0)?;
        let updated_at_epoch: Option<f64> = row.get(1).ok();
        let updated_at = updated_at_epoch
            .and_then(|epoch| UNIX_EPOCH.checked_add(Duration::from_secs_f64(epoch)));
        Ok(Some(BlobCheckpoint { data, updated_at }))
    }
}

#[async_trait]
impl BlobCheckpointStore for DuckDbBlobCheckpointStore {
    // The DuckDB pool is synchronous/blocking; this mirrors the previous behavior of
    // the per-connector sidecars (they called the sync path inline).
    async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
        self.get_blocking()
            .map_err(|source| CheckpointError::Store { source })
    }

    async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
        self.upsert_blocking(data)
            .map_err(|source| CheckpointError::Store { source })
    }
}
