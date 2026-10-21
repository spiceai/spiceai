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

//! The Kafka consumer-group / per-partition-offset tables.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use runtime_checkpoint_api::{
    CheckpointError,
    kafka::{KafkaCheckpoint, KafkaCheckpointStore, KafkaOffset},
    offsets::{OffsetSchemaState, merge_offsets, sort_offsets},
    retry::retry_on_write_conflict,
};

use crate::{spawn_checkpoint_blocking, store_error};

const KAFKA_TABLE_NAME: &str = "spice_sys_kafka";
const KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_kafka_offsets";

/// Kafka checkpoint store backed by a `DuckDB` accelerator.
pub struct DuckDbKafkaCheckpointStore {
    pool: Arc<DuckDbConnectionPool>,
    dataset_name: String,
    schema_ensured: Arc<OffsetSchemaState>,
    /// Serializes this instance's own sidecar writes.
    ///
    /// `DuckDB` resolves concurrent writes to one row optimistically — the loser gets
    /// `Conflict on update!` instead of waiting — and the sidecar writers hold the
    /// pool's write gate with `read()`, so they do not exclude each other. Two commits
    /// for the same dataset therefore conflict rather than queue, and a burst would
    /// fail instead of resolving to the max offset.
    ///
    /// Scoped to one instance: writers for different datasets key on distinct rows and
    /// do not conflict. `retry_on_write_conflict` still covers contention this lock
    /// cannot see.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DuckDbKafkaCheckpointStore {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>, dataset_name: String) -> Self {
        Self {
            pool,
            dataset_name,
            schema_ensured: Arc::default(),
            write_lock: Arc::default(),
        }
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn upsert_duckdb(
        dataset_name: &str,
        schema_ensured: &OffsetSchemaState,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &KafkaCheckpoint,
    ) -> Result<(), CheckpointError> {
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        ensure_kafka_tables(duckdb_conn)?;
        schema_ensured.mark_ensured();

        let schema_json = metadata.schema_json.clone();

        let tx = duckdb_conn.transaction().map_err(store_error)?;
        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = excluded.consumer_group_id,
                topic = excluded.topic,
                schema_json = excluded.schema_json,
                updated_at = now()"
        );
        tx.execute(
            &upsert,
            duckdb::params![
                dataset_name,
                metadata.consumer_group_id,
                metadata.topic,
                schema_json,
            ],
        )
        .map_err(store_error)?;
        upsert_offsets_tx(&tx, dataset_name, &metadata.offsets)?;
        tx.commit().map_err(store_error)?;

        Ok(())
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn get_duckdb(
        dataset_name: &str,
        schema_ensured: &OffsetSchemaState,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Result<Option<KafkaCheckpoint>, CheckpointError> {
        // `ensure_kafka_tables` below issues DDL, so this read path is also a
        // writer to the shared acceleration file and takes the write gate.
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        if schema_ensured.needs_ensure() {
            ensure_kafka_tables(duckdb_conn)?;
            schema_ensured.mark_ensured();
        }

        let query = format!(
            "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).map_err(store_error)?;
        let mut rows = stmt.query([dataset_name]).map_err(store_error)?;

        let Some(row) = rows.next().map_err(store_error)? else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0).map_err(store_error)?;
        let topic: String = row.get(1).map_err(store_error)?;
        let schema_json: String = row.get(2).map_err(store_error)?;
        drop(rows);
        drop(stmt);

        let offsets = load_offsets(duckdb_conn, dataset_name)?;

        Ok(Some(KafkaCheckpoint {
            consumer_group_id,
            topic,
            schema_json,
            offsets,
        }))
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn upsert_offsets_duckdb(
        dataset_name: &str,
        schema_ensured: &OffsetSchemaState,
        pool: &Arc<DuckDbConnectionPool>,
        offsets: &[KafkaOffset],
    ) -> Result<(), CheckpointError> {
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        if schema_ensured.needs_ensure() {
            ensure_kafka_tables(duckdb_conn)?;
            schema_ensured.mark_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(prior) = load_offsets(duckdb_conn, dataset_name) {
            let _ = merge_offsets(dataset_name, prior, offsets);
        }

        let tx = duckdb_conn.transaction().map_err(store_error)?;
        upsert_offsets_tx(&tx, dataset_name, offsets)?;
        tx.commit().map_err(store_error)?;
        Ok(())
    }
}

#[async_trait]
impl KafkaCheckpointStore for DuckDbKafkaCheckpointStore {
    async fn get(&self) -> Result<Option<KafkaCheckpoint>, CheckpointError> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        let schema_ensured = Arc::clone(&self.schema_ensured);
        spawn_checkpoint_blocking(move || Self::get_duckdb(&dataset_name, &schema_ensured, &pool))
            .await
    }

    async fn upsert(&self, checkpoint: &KafkaCheckpoint) -> Result<(), CheckpointError> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        let schema_ensured = Arc::clone(&self.schema_ensured);
        let checkpoint = checkpoint.clone();
        spawn_checkpoint_blocking(move || {
            Self::upsert_duckdb(&dataset_name, &schema_ensured, &pool, &checkpoint)
        })
        .await
    }

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        let _serialize = self.write_lock.lock().await;
        retry_on_write_conflict(&self.dataset_name, || {
            let pool = Arc::clone(&self.pool);
            let dataset_name = self.dataset_name.clone();
            let schema_ensured = Arc::clone(&self.schema_ensured);
            let offsets = offsets.to_vec();
            async move {
                spawn_checkpoint_blocking(move || {
                    Self::upsert_offsets_duckdb(&dataset_name, &schema_ensured, &pool, &offsets)
                })
                .await
            }
        })
        .await
    }
}
fn ensure_kafka_tables(conn: &mut duckdb::Connection) -> Result<(), CheckpointError> {
    let create_metadata = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )"
    );
    conn.execute(&create_metadata, []).map_err(store_error)?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_OFFSETS_TABLE_NAME} (
            dataset_name TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            partition_offset BIGINT NOT NULL,
            updated_at TIMESTAMP,
            PRIMARY KEY (dataset_name, topic, partition_id)
        )"
    );
    conn.execute(&create_offsets, []).map_err(store_error)?;
    Ok(())
}

fn upsert_offsets_tx(
    tx: &duckdb::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<(), CheckpointError> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES (?, ?, ?, ?, now())
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = GREATEST(excluded.partition_offset, partition_offset),
            updated_at = now()"
    );
    let mut stmt = tx.prepare(&stmt_sql).map_err(store_error)?;
    for offset in offsets {
        stmt.execute(duckdb::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])
        .map_err(store_error)?;
    }
    Ok(())
}

fn load_offsets(
    conn: &duckdb::Connection,
    dataset_name: &str,
) -> Result<Vec<KafkaOffset>, CheckpointError> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?"
    );
    let mut stmt = conn.prepare(&query).map_err(store_error)?;
    let rows = stmt
        .query_map([dataset_name], |row| {
            Ok(KafkaOffset {
                topic: row.get(0)?,
                partition: row.get(1)?,
                offset: row.get(2)?,
            })
        })
        .map_err(store_error)?;
    let mut out: Vec<KafkaOffset> = rows.collect::<duckdb::Result<_>>().map_err(store_error)?;
    sort_offsets(&mut out);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_pool;
    use arrow::datatypes::{DataType, Field, Schema};
    use runtime_checkpoint_api::kafka::{KafkaCheckpoint, KafkaCheckpointStore};
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
    async fn test_duckdb_roundtrip() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_roundtrip");
        let dataset_name = "test_duckdb_roundtrip".to_string();
        let kafka_sys = DuckDbKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

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
    async fn test_duckdb_metadata_overwrite() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_metadata_overwrite");
        let dataset_name = "test_duckdb_metadata_overwrite".to_string();
        let kafka_sys = DuckDbKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());
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
    async fn test_duckdb_offsets_update() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_offsets_update");
        let dataset_name = "test_duckdb_offsets_update".to_string();
        let kafka_sys = DuckDbKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());
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
    async fn test_duckdb_get_nonexistent() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_get_nonexistent");
        let dataset_name = "test_duckdb_get_nonexistent".to_string();
        let kafka_sys = DuckDbKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        let result = kafka_sys.get().await;
        assert!(
            result.expect("to get empty metadata").is_none(),
            "Should return None for nonexistent dataset"
        );
    }

    /// Regression for finding #2: `upsert_offsets` must succeed even when no
    /// metadata row exists.
    #[tokio::test]
    async fn test_duckdb_offsets_update_succeeds_without_metadata_row() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_offsets_no_metadata");
        let dataset_name = "test_duckdb_offsets_no_metadata".to_string();
        let kafka_sys = DuckDbKafkaCheckpointStore::new(Arc::clone(&pool), dataset_name.clone());

        kafka_sys
            .upsert_offsets(&[KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 42,
            }])
            .await
            .expect("upsert_offsets should succeed without metadata row");
    }

    /// Regression for finding #1 part b: per-partition INSERT ON CONFLICT
    /// keeps the highest offset when racing concurrent writers on the same
    /// partition.
    #[tokio::test]
    async fn test_duckdb_concurrent_same_partition_keeps_max() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_concurrent_same_partition");
        let dataset_name = "test_duckdb_concurrent_same_partition".to_string();
        let kafka_sys = Arc::new(DuckDbKafkaCheckpointStore::new(
            Arc::clone(&pool),
            dataset_name.clone(),
        ));
        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("seed");

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
            h.await.expect("join");
        }

        let retrieved = kafka_sys.get().await.expect("retrieve").expect("exist");
        let p0 = retrieved
            .offsets
            .iter()
            .find(|o| o.partition == 0 && o.topic == "test-topic")
            .expect("partition 0 present");
        assert_eq!(p0.offset, 100);
    }

    /// The connector reaches this store as `dyn KafkaCheckpointStore`, whose checkpoint
    /// carries the schema as JSON rather than as an Arrow schema. Everything the
    /// connector persists therefore crosses one extra conversion, so round-trip it
    /// through the trait rather than through the inherent helpers.
    #[tokio::test]
    async fn trait_roundtrip_preserves_the_schema_and_offsets() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_trait_roundtrip");
        let dataset_name = "test_duckdb_trait_roundtrip".to_string();
        let store = trait_store(&pool, &dataset_name);

        let metadata = create_test_metadata();
        let written = metadata.clone();
        store.upsert(&written).await.expect("to upsert checkpoint");

        let read = store
            .get()
            .await
            .expect("to read the checkpoint back")
            .expect("a checkpoint to exist");

        assert_eq!(read.consumer_group_id, written.consumer_group_id);
        assert_eq!(read.topic, written.topic);
        assert_eq!(read.offsets, written.offsets);
        // The engine layer parses the stored JSON and the seam re-encodes it, so this
        // asserts that round trip is byte-lossless and not merely semantically equal.
        assert_eq!(read.schema_json, written.schema_json);
    }

    /// `upsert_offsets` is the hot path and must never move a partition backwards, which
    /// is the whole reason offsets are rows rather than a field of the metadata blob.
    #[tokio::test]
    async fn trait_offset_upsert_keeps_the_higher_offset() {
        let (pool, _temp_dir) = temp_pool("test_duckdb_trait_offsets");
        let dataset_name = "test_duckdb_trait_offsets".to_string();
        let store = trait_store(&pool, &dataset_name);
        store.upsert(&create_test_metadata()).await.expect("seed");

        let at = |offset: i64| {
            vec![KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset,
            }]
        };
        store.upsert_offsets(&at(80)).await.expect("advance");
        store
            .upsert_offsets(&at(20))
            .await
            .expect("a late, lower commit is accepted rather than rejected");

        let read = store.get().await.expect("read").expect("exist");
        let p0 = read
            .offsets
            .iter()
            .find(|o| o.partition == 0 && o.topic == "test-topic")
            .expect("partition 0 present");
        assert_eq!(
            p0.offset, 80,
            "a lower offset must not overwrite a higher one"
        );
    }

    /// The sidecar as a connector sees it: behind the checkpoint-store trait.
    fn trait_store(
        pool: &Arc<DuckDbConnectionPool>,
        dataset_name: &str,
    ) -> Arc<dyn KafkaCheckpointStore> {
        Arc::new(DuckDbKafkaCheckpointStore::new(
            Arc::clone(pool),
            dataset_name.to_string(),
        ))
    }
}
