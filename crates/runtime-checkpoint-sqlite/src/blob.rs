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
//!
//! Persists one `String` payload keyed by `dataset_name` into a
//! `(dataset_name PK, checkpoint_data TEXT, created_at, updated_at)` sidecar table
//! whose name the caller chooses.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};
use runtime_checkpoint_api::{BlobCheckpoint, BlobCheckpointStore, CheckpointError};

/// Blob checkpoint store backed by a `SQLite` accelerator (also used for the Cayenne
/// metastore, which is a `SQLite` connection).
pub struct SqliteBlobCheckpointStore {
    pool: Arc<SqliteConnectionPool>,
    dataset_name: String,
    table_name: &'static str,
}

impl SqliteBlobCheckpointStore {
    #[must_use]
    pub fn new(
        pool: Arc<SqliteConnectionPool>,
        dataset_name: String,
        table_name: &'static str,
    ) -> Self {
        Self {
            pool,
            dataset_name,
            table_name,
        }
    }

    fn create_table_sql(table: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        )
    }
}

#[async_trait]
impl BlobCheckpointStore for SqliteBlobCheckpointStore {
    async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
        let dataset_name = self.dataset_name.clone();
        let table = self.table_name;

        let conn_sync = self.pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(crate::downcast_failed());
        };

        conn.conn
            .call(move |conn: &mut rusqlite::Connection| -> Result<Option<BlobCheckpoint>, rusqlite::Error> {
                // Ensure the sidecar table exists so a fresh accelerator reads as
                // "no checkpoint yet" (Ok(None)) rather than a missing-table error.
                conn.execute(&Self::create_table_sql(table), [])?;

                let query = format!(
                    "SELECT checkpoint_data, CAST(strftime('%s', updated_at) AS INTEGER) FROM {table} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    let data: String = row.get(0)?;
                    let updated_at_epoch: Option<i64> = row.get(1).ok();
                    let updated_at = updated_at_epoch.and_then(|epoch| {
                        u64::try_from(epoch).ok().and_then(|e| {
                            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(e))
                        })
                    });
                    Ok(Some(BlobCheckpoint { data, updated_at }))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(crate::store_error)
    }

    async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
        let dataset_name = self.dataset_name.clone();
        let checkpoint_data = data.to_string();
        let table = self.table_name;

        let conn_sync = self.pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(crate::downcast_failed());
        };

        conn.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> Result<(), rusqlite::Error> {
                    conn.execute(&Self::create_table_sql(table), [])?;

                    let upsert = format!(
                        "INSERT INTO {table} (dataset_name, checkpoint_data, updated_at)
                         VALUES (?1, ?2, CURRENT_TIMESTAMP)
                         ON CONFLICT (dataset_name) DO UPDATE SET
                            checkpoint_data = ?2,
                            updated_at = CURRENT_TIMESTAMP"
                    );
                    conn.execute(&upsert, [dataset_name, checkpoint_data])?;
                    Ok(())
                },
            )
            .await
            .map_err(crate::store_error)
    }

    async fn delete(&self) -> Result<(), CheckpointError> {
        let dataset_name = self.dataset_name.clone();
        let table = self.table_name;

        let conn_sync = self.pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(crate::downcast_failed());
        };

        conn.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> Result<(), rusqlite::Error> {
                    // The table may not exist yet: a dataset that never checkpointed
                    // has nothing to discard, and that is not an error.
                    conn.execute(&Self::create_table_sql(table), [])?;
                    let delete = format!("DELETE FROM {table} WHERE dataset_name = ?1");
                    conn.execute(&delete, [dataset_name])?;
                    Ok(())
                },
            )
            .await
            .map_err(crate::store_error)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::test_support::temp_pool;

    const TABLE: &str = "spice_sys_blob_test";

    /// A connector decides whether a checkpoint is too old to resume from by its
    /// `updated_at`, so the store must report when the checkpoint was written, not
    /// `None`: `SQLite`'s `strftime` returns text, which does not read as an integer.
    #[tokio::test]
    async fn get_reports_when_the_checkpoint_was_written() {
        let (pool, _dir) = temp_pool("blob_updated_at").await;
        let store = SqliteBlobCheckpointStore::new(Arc::clone(&pool), "orders".to_string(), TABLE);
        store.upsert("{}").await.expect("upsert the checkpoint");

        let conn_sync = pool.connect_sync();
        let conn = conn_sync
            .as_any()
            .downcast_ref::<SqliteConnection>()
            .expect("sqlite connection");
        conn.conn
            .call(|conn| {
                conn.execute(
                    &format!("UPDATE {TABLE} SET updated_at = '2026-11-07 10:00:00'"),
                    [],
                )?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .expect("set updated_at");

        let checkpoint = store
            .get()
            .await
            .expect("read the checkpoint")
            .expect("the checkpoint exists");
        assert_eq!(checkpoint.data, "{}");
        // 2026-11-07T10:00:00Z
        assert_eq!(
            checkpoint.updated_at,
            Some(UNIX_EPOCH + Duration::from_hours(498_346))
        );
    }
}
