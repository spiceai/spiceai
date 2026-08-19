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

//! The `MongoDB` Change Stream resume-token table.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use runtime_checkpoint_api::{
    CheckpointError,
    mongodb::{MongoCheckpointMetadata, MongoCheckpointStore},
};

use crate::{spawn_checkpoint_blocking, spawn_checkpoint_blocking_opt, store_error};

const MONGODB_TABLE_NAME: &str = "spice_sys_mongodb";

/// `MongoDB` resume-token store backed by a `DuckDB` accelerator.
pub struct DuckDbMongoCheckpointStore {
    pool: Arc<DuckDbConnectionPool>,
    dataset_name: String,
}

impl DuckDbMongoCheckpointStore {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn upsert_duckdb(
        dataset_name: &str,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &MongoCheckpointMetadata,
    ) -> Result<(), CheckpointError> {
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {MONGODB_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                resume_token_json TEXT NOT NULL,
                cluster_time_ts BIGINT,
                schema_json TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(store_error)?;

        let upsert = format!(
            "INSERT INTO {MONGODB_TABLE_NAME}
                (dataset_name, resume_token_json, cluster_time_ts, schema_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                resume_token_json = excluded.resume_token_json,
                cluster_time_ts = excluded.cluster_time_ts,
                schema_json = excluded.schema_json,
                updated_at = now()"
        );

        duckdb_conn
            .execute(
                &upsert,
                duckdb::params![
                    dataset_name,
                    metadata.resume_token_json,
                    metadata.cluster_time_ts,
                    metadata.schema_json,
                ],
            )
            .map_err(store_error)?;

        Ok(())
    }

    /// Blocking `DuckDB` I/O. Callers must reach this through
    /// `spawn_duckdb_blocking_opt`, never directly from an async worker.
    fn get_duckdb(
        dataset_name: &str,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Option<MongoCheckpointMetadata> {
        use std::time::{Duration, UNIX_EPOCH};

        let mut db_conn = Arc::clone(pool).connect_sync().ok()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();

        let query = format!(
            "SELECT resume_token_json, cluster_time_ts, schema_json, CAST(epoch(updated_at) AS DOUBLE) FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let resume_token_json: String = row.get(0).ok()?;
            let cluster_time_ts: Option<i64> = row.get(1).ok();
            let schema_json: Option<String> = row.get(2).ok();
            let updated_at_epoch: Option<f64> = row.get(3).ok();
            let updated_at = updated_at_epoch
                .and_then(|epoch| UNIX_EPOCH.checked_add(Duration::from_secs_f64(epoch)));

            Some(MongoCheckpointMetadata {
                resume_token_json,
                cluster_time_ts,
                schema_json,
                updated_at,
            })
        } else {
            None
        }
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn delete_duckdb(
        dataset_name: &str,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Result<(), CheckpointError> {
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        let delete = format!("DELETE FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?");
        duckdb_conn
            .execute(&delete, [dataset_name])
            .map_err(store_error)?;

        Ok(())
    }
}

#[async_trait]
impl MongoCheckpointStore for DuckDbMongoCheckpointStore {
    async fn get(&self) -> Option<MongoCheckpointMetadata> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        spawn_checkpoint_blocking_opt(move || Self::get_duckdb(&dataset_name, &pool)).await
    }

    async fn upsert(&self, metadata: &MongoCheckpointMetadata) -> Result<(), CheckpointError> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        let metadata = metadata.clone();
        spawn_checkpoint_blocking(move || Self::upsert_duckdb(&dataset_name, &pool, &metadata))
            .await
    }

    async fn delete(&self) -> Result<(), CheckpointError> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        spawn_checkpoint_blocking(move || Self::delete_duckdb(&dataset_name, &pool)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_pool;

    fn create_test_metadata() -> MongoCheckpointMetadata {
        MongoCheckpointMetadata {
            resume_token_json: r#"{"_data":"82650000000000000001"}"#.to_string(),
            cluster_time_ts: Some(1_700_000_000),
            schema_json: Some(r#"{"fields":[]}"#.to_string()),
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn test_duckdb_roundtrip() {
        let (pool, _tmp) = temp_pool("test_mongodb_duckdb_roundtrip");
        let dataset_name = "test_mongodb_duckdb_roundtrip".to_string();
        let mongo_sys = DuckDbMongoCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        let metadata = create_test_metadata();
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to upsert metadata");

        let retrieved = mongo_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.resume_token_json, metadata.resume_token_json);
        assert_eq!(retrieved.cluster_time_ts, metadata.cluster_time_ts);
        assert_eq!(retrieved.schema_json, metadata.schema_json);
    }

    #[tokio::test]
    async fn test_duckdb_metadata_overwrite() {
        let (pool, _tmp) = temp_pool("test_mongodb_duckdb_overwrite");
        let dataset_name = "test_mongodb_duckdb_overwrite".to_string();
        let mongo_sys = DuckDbMongoCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());
        let mut metadata = create_test_metadata();

        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to upsert metadata");

        metadata.resume_token_json = r#"{"_data":"82650000000000000002"}"#.to_string();
        mongo_sys
            .upsert(&metadata)
            .await
            .expect("to overwrite metadata");

        let retrieved = mongo_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.resume_token_json, metadata.resume_token_json);
    }

    #[tokio::test]
    async fn test_duckdb_get_nonexistent() {
        let (pool, _tmp) = temp_pool("test_mongodb_duckdb_get_nonexistent");
        let dataset_name = "test_mongodb_duckdb_get_nonexistent".to_string();
        let mongo_sys = DuckDbMongoCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        assert!(mongo_sys.get().await.is_none());
    }

    #[tokio::test]
    async fn test_duckdb_delete() {
        let (pool, _tmp) = temp_pool("test_mongodb_duckdb_delete");
        let dataset_name = "test_mongodb_duckdb_delete".to_string();
        let mongo_sys = DuckDbMongoCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        mongo_sys
            .upsert(&create_test_metadata())
            .await
            .expect("to upsert metadata");
        assert!(mongo_sys.get().await.is_some());

        mongo_sys.delete().await.expect("to delete");
        assert!(mongo_sys.get().await.is_none());
    }
}
