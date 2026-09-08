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

//! The Kafka consumer-group / per-partition-offset tables.
//!
//! ```sql
//! CREATE TABLE spice_sys_kafka (
//!     dataset_name TEXT PRIMARY KEY,
//!     consumer_group_id TEXT,
//!     topic TEXT,
//!     schema_json TEXT,
//!     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//!     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//! );
//!
//! CREATE TABLE spice_sys_kafka_offsets (
//!     dataset_name TEXT NOT NULL,
//!     topic TEXT NOT NULL,
//!     partition_id INTEGER NOT NULL,
//!     partition_offset BIGINT NOT NULL,
//!     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//!     PRIMARY KEY (dataset_name, topic, partition_id)
//! );
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};
use runtime_checkpoint_api::{
    CheckpointError,
    kafka::{KafkaCheckpoint, KafkaCheckpointStore, KafkaOffset},
    offsets::{OffsetSchemaState, merge_offsets, sort_offsets},
};
use rusqlite::OptionalExtension;

use crate::{downcast_failed, store_error};

const KAFKA_TABLE_NAME: &str = "spice_sys_kafka";
const KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_kafka_offsets";

/// Kafka checkpoint store backed by a `SQLite` accelerator.
pub struct SqliteKafkaCheckpointStore {
    pool: Arc<SqliteConnectionPool>,
    dataset_name: String,
    schema_ensured: Arc<OffsetSchemaState>,
}

impl SqliteKafkaCheckpointStore {
    #[must_use]
    pub fn new(pool: Arc<SqliteConnectionPool>, dataset_name: String) -> Self {
        Self {
            pool,
            dataset_name,
            schema_ensured: Arc::default(),
        }
    }
}

#[async_trait]
impl KafkaCheckpointStore for SqliteKafkaCheckpointStore {
    async fn upsert(&self, checkpoint: &KafkaCheckpoint) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let metadata = checkpoint;
        let schema_json = metadata.schema_json.clone();
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let seed_offsets = metadata.offsets.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                ensure_kafka_tables(conn)?;

                let tx = conn.transaction()?;
                let upsert = format!(
                    "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                     ON CONFLICT (dataset_name) DO UPDATE SET
                        consumer_group_id = ?2,
                        topic = ?3,
                        schema_json = ?4,
                        updated_at = CURRENT_TIMESTAMP"
                );
                tx.execute(
                    &upsert,
                    rusqlite::params![dataset_name, consumer_group_id, topic, schema_json],
                )?;
                upsert_offsets_into(&tx, &dataset_name, &seed_offsets)?;
                tx.commit()?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(store_error)?;

        self.schema_ensured.mark_ensured();
        Ok(())
    }

    async fn get(&self) -> Result<Option<KafkaCheckpoint>, CheckpointError> {
        type MetadataRow = (String, String, String);
        let pool = &self.pool;

        let dataset_name = self.dataset_name.clone();
        let schema_needs_ensure = self.schema_ensured.needs_ensure();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        let result = conn
            .conn
            .call(move |conn| {
                if schema_needs_ensure {
                    ensure_kafka_tables(conn)?;
                }

                let metadata_query = format!(
                    "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?1"
                );
                let metadata: Option<MetadataRow> = conn
                    .query_row(&metadata_query, [&dataset_name], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .optional()?;

                let Some((consumer_group_id, topic, schema_json)) = metadata else {
                    return Ok::<Option<(String, String, String, Vec<KafkaOffset>)>, rusqlite::Error>(None);
                };

                let offsets = load_offsets(conn, &dataset_name)?;
                Ok(Some((consumer_group_id, topic, schema_json, offsets)))
            })
            .await
            .map_err(store_error)?;

        if schema_needs_ensure {
            self.schema_ensured.mark_ensured();
        }

        let Some((consumer_group_id, topic, schema_json, offsets)) = result else {
            return Ok(None);
        };

        Ok(Some(KafkaCheckpoint {
            consumer_group_id,
            topic,
            schema_json,
            offsets,
        }))
    }

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();
        let new_offsets = offsets.to_vec();
        let warn_dataset = self.dataset_name.clone();
        let schema_needs_ensure = self.schema_ensured.needs_ensure();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(downcast_failed());
        };

        conn.conn
            .call(move |conn| {
                if schema_needs_ensure {
                    ensure_kafka_tables(conn)?;
                }

                // Diagnostic-only: surface a warn log when an offset regresses.
                // The SQL MAX() in upsert_offsets_into is the source of truth.
                if let Ok(prior) = load_offsets(conn, &dataset_name) {
                    let _ = merge_offsets(&warn_dataset, prior, &new_offsets);
                }

                let tx = conn.transaction()?;
                upsert_offsets_into(&tx, &dataset_name, &new_offsets)?;
                tx.commit()?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(store_error)?;

        if schema_needs_ensure {
            self.schema_ensured.mark_ensured();
        }

        Ok(())
    }
}

fn ensure_kafka_tables(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let create_metadata = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    );
    conn.execute(&create_metadata, [])?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_OFFSETS_TABLE_NAME} (
            dataset_name TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            partition_offset BIGINT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (dataset_name, topic, partition_id)
        )"
    );
    conn.execute(&create_offsets, [])?;
    Ok(())
}

fn upsert_offsets_into(
    tx: &rusqlite::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> rusqlite::Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = MAX(excluded.partition_offset, partition_offset),
            updated_at = CURRENT_TIMESTAMP"
    );
    let mut stmt = tx.prepare(&stmt_sql)?;
    for offset in offsets {
        stmt.execute(rusqlite::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])?;
    }
    Ok(())
}

fn load_offsets(
    conn: &rusqlite::Connection,
    dataset_name: &str,
) -> rusqlite::Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?1"
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([dataset_name], |row| {
        Ok(KafkaOffset {
            topic: row.get(0)?,
            partition: row.get(1)?,
            offset: row.get(2)?,
        })
    })?;
    let mut out: Vec<KafkaOffset> = rows.collect::<rusqlite::Result<_>>()?;
    sort_offsets(&mut out);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_pool;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_metadata() -> KafkaCheckpoint {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        KafkaCheckpoint {
            consumer_group_id: "test-group-123".to_string(),
            topic: "test-topic".to_string(),
            schema_json: arrow_tools::schema::schema_to_json(&schema).expect("schema serializes"),
            offsets: vec![KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 42,
            }],
        }
    }

    #[tokio::test]
    async fn test_sqlite_roundtrip() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_roundtrip").await;
        let dataset_name = "test_sqlite_roundtrip".to_string();
        let kafka_sys = SqliteKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        let test_metadata = create_test_metadata();

        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to upsert metadata");
        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");

        assert_eq!(retrieved.consumer_group_id, test_metadata.consumer_group_id);
        assert_eq!(retrieved.topic, test_metadata.topic);
        assert_eq!(retrieved.schema_json, test_metadata.schema_json);
        assert_eq!(retrieved.offsets, test_metadata.offsets);
    }

    #[tokio::test]
    async fn test_sqlite_metadata_overwrite() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_metadata_overwrite").await;
        let dataset_name = "test_sqlite_metadata_overwrite".to_string();
        let kafka_sys = SqliteKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());
        let mut test_metadata = create_test_metadata();

        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to upsert metadata");

        test_metadata.consumer_group_id = "updated-group-456".to_string();
        test_metadata.topic = "updated-topic".to_string();
        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to overwrite metadata");

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");
        assert_eq!(retrieved.consumer_group_id, "updated-group-456");
        assert_eq!(retrieved.topic, "updated-topic");
        assert_eq!(retrieved.schema_json, test_metadata.schema_json);
        assert_eq!(retrieved.offsets, test_metadata.offsets);
    }

    #[tokio::test]
    async fn test_sqlite_offsets_update() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_offsets_update").await;
        let dataset_name = "test_sqlite_offsets_update".to_string();
        let kafka_sys = SqliteKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());
        let test_metadata = create_test_metadata();

        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to upsert metadata");

        let offsets = vec![KafkaOffset {
            topic: "test-topic".to_string(),
            partition: 1,
            offset: 99,
        }];
        kafka_sys
            .upsert_offsets(&offsets)
            .await
            .expect("to upsert offsets");

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");
        let mut expected_offsets = test_metadata.offsets.clone();
        expected_offsets.extend(offsets);
        assert_eq!(retrieved.offsets, expected_offsets);
    }

    #[tokio::test]
    async fn test_sqlite_get_nonexistent() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_get_nonexistent").await;
        let dataset_name = "test_sqlite_get_nonexistent".to_string();
        let kafka_sys = SqliteKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        let result = kafka_sys.get().await;
        assert!(
            result.expect("to get empty metadata").is_none(),
            "Should return None for nonexistent dataset"
        );
    }

    /// Regression for finding #2: `upsert_offsets` used to fail with "Kafka
    /// sidecar metadata for dataset X does not exist" when no metadata row
    /// existed yet. With per-partition storage the offsets always land.
    #[tokio::test]
    async fn test_sqlite_offsets_update_succeeds_without_metadata_row() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_offsets_no_metadata").await;
        let dataset_name = "test_sqlite_offsets_no_metadata".to_string();
        let kafka_sys = SqliteKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        let offsets = vec![KafkaOffset {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
        }];
        kafka_sys
            .upsert_offsets(&offsets)
            .await
            .expect("upsert_offsets should succeed without a prior metadata row");

        // `get()` returns None because the metadata row is missing, but the
        // offset is durably persisted; a later `upsert(metadata)` will
        // surface it.
        let result = kafka_sys.get().await.expect("to query metadata");
        assert!(result.is_none(), "metadata row was never written");
    }

    /// Regression for finding #1 part a: concurrent `upsert_offsets` over
    /// disjoint partitions must keep every writer's data.
    #[tokio::test]
    async fn test_sqlite_concurrent_upserts_do_not_lose_partitions() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_concurrent_upserts").await;
        let dataset_name = "test_sqlite_concurrent_upserts".to_string();
        let kafka_sys = Arc::new(SqliteKafkaCheckpointStore::new(
            Arc::clone(&pool),
            dataset_name.clone(),
        ));

        let num_tasks = 8_i32;
        let partitions_per_task = 8_i32;

        let mut handles = Vec::new();
        for task_idx in 0..num_tasks {
            let kafka_sys = Arc::clone(&kafka_sys);
            handles.push(tokio::spawn(async move {
                let offsets: Vec<KafkaOffset> = (0..partitions_per_task)
                    .map(|partition_index| KafkaOffset {
                        topic: "concurrent-topic".to_string(),
                        partition: task_idx * partitions_per_task + partition_index,
                        offset: 100 + i64::from(partition_index),
                    })
                    .collect();
                kafka_sys
                    .upsert_offsets(&offsets)
                    .await
                    .expect("concurrent upsert_offsets should succeed");
            }));
        }
        for h in handles {
            h.await.expect("task join");
        }

        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("to upsert metadata after concurrent offset writes");

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");

        let expected_count = usize::try_from(num_tasks * partitions_per_task)
            .expect("test offset count should fit in usize");
        let concurrent_count = retrieved
            .offsets
            .iter()
            .filter(|o| o.topic == "concurrent-topic")
            .count();
        assert_eq!(
            concurrent_count, expected_count,
            "all per-partition offsets must land after concurrent writes"
        );
    }

    /// Regression for finding #1 part b: when two writers race on the same
    /// (topic, partition), the storage layer must keep the highest offset.
    #[tokio::test]
    async fn test_sqlite_concurrent_same_partition_keeps_max() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_concurrent_same_partition").await;
        let dataset_name = "test_sqlite_concurrent_same_partition".to_string();
        let kafka_sys = Arc::new(SqliteKafkaCheckpointStore::new(
            Arc::clone(&pool),
            dataset_name.clone(),
        ));
        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("seed metadata");

        let mut handles = Vec::new();
        for off in [10_i64, 50, 30, 100, 70, 5] {
            let kafka_sys = Arc::clone(&kafka_sys);
            handles.push(tokio::spawn(async move {
                kafka_sys
                    .upsert_offsets(&[KafkaOffset {
                        topic: "test-topic".to_string(),
                        partition: 0,
                        offset: off,
                    }])
                    .await
                    .expect("upsert");
            }));
        }
        for h in handles {
            h.await.expect("task join");
        }

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve")
            .expect("to exist");
        let p0 = retrieved
            .offsets
            .iter()
            .find(|o| o.partition == 0 && o.topic == "test-topic")
            .expect("partition 0 present");
        assert_eq!(p0.offset, 100, "must keep the highest concurrent offset");
    }

    /// Regression for finding #5: a backward offset must NOT overwrite a
    /// higher stored offset.
    #[tokio::test]
    async fn test_sqlite_backward_offset_does_not_regress() {
        let (pool, _temp_dir) = temp_pool("test_sqlite_backward_offset").await;
        let dataset_name = "test_sqlite_backward_offset".to_string();
        let kafka_sys = SqliteKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());
        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("seed metadata");

        kafka_sys
            .upsert_offsets(&[KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 500,
            }])
            .await
            .expect("forward upsert");
        kafka_sys
            .upsert_offsets(&[KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 100,
            }])
            .await
            .expect("backward upsert");

        let retrieved = kafka_sys.get().await.expect("retrieve").expect("exist");
        let p0 = retrieved
            .offsets
            .iter()
            .find(|o| o.partition == 0)
            .expect("partition 0 present");
        assert_eq!(p0.offset, 500, "backward offset must not overwrite");
    }
}
