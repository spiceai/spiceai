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
use data_components::turso::TursoConnectionPool;
use runtime_checkpoint_api::{
    CheckpointError,
    kafka::{KafkaCheckpoint, KafkaCheckpointStore, KafkaOffset},
    offsets::{OffsetSchemaState, merge_offsets, sort_offsets},
};

use crate::store_error;

const KAFKA_TABLE_NAME: &str = "spice_sys_kafka";
const KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_kafka_offsets";

/// Kafka checkpoint store backed by a `Turso` accelerator.
pub struct TursoKafkaCheckpointStore {
    pool: Arc<TursoConnectionPool>,
    dataset_name: String,
    schema_ensured: Arc<OffsetSchemaState>,
}

impl TursoKafkaCheckpointStore {
    #[must_use]
    pub fn new(pool: Arc<TursoConnectionPool>, dataset_name: String) -> Self {
        Self {
            pool,
            dataset_name,
            schema_ensured: Arc::default(),
        }
    }
}

#[async_trait]
impl KafkaCheckpointStore for TursoKafkaCheckpointStore {
    async fn upsert(&self, checkpoint: &KafkaCheckpoint) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let metadata = checkpoint;
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let schema_json = metadata.schema_json.clone();

        let conn = pool.connect().await.map_err(store_error)?;

        {
            let _schema_guard = pool.acquire_schema_write_lock().await;
            ensure_kafka_tables(&conn).await?;
            self.schema_ensured.mark_ensured();
        }

        // Turso lacks explicit transactions in its current Rust binding; the
        // metadata upsert is one statement and each per-partition upsert is
        // also one statement (idempotent via ON CONFLICT). Per-row atomicity
        // is what matters for resumability.
        let _schema_guard = pool.acquire_schema_read_lock().await;
        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = ?2,
                topic = ?3,
                schema_json = ?4,
                updated_at = CURRENT_TIMESTAMP"
        );
        conn.execute(
            &upsert,
            turso::params![dataset_name, consumer_group_id, topic, schema_json],
        )
        .await
        .map_err(store_error)?;

        upsert_offsets_each(&conn, &self.dataset_name, &metadata.offsets).await?;
        Ok(())
    }

    async fn get(&self) -> Result<Option<KafkaCheckpoint>, CheckpointError> {
        let pool = &self.pool;
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.map_err(store_error)?;
        if self.schema_ensured.needs_ensure() {
            let _schema_guard = pool.acquire_schema_write_lock().await;
            ensure_kafka_tables(&conn).await?;
            self.schema_ensured.mark_ensured();
        }

        let query = format!(
            "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut rows = conn
            .query(&query, turso::params![dataset_name.clone()])
            .await
            .map_err(store_error)?;
        let Some(row) = rows.next().await.map_err(store_error)? else {
            return Ok(None);
        };

        let consumer_group_id = row.get::<String>(0).map_err(store_error)?;
        let topic = row.get::<String>(1).map_err(store_error)?;
        let schema_json = row.get::<String>(2).map_err(store_error)?;
        drop(rows);

        let offsets = load_offsets(&conn, &dataset_name).await?;

        Ok(Some(KafkaCheckpoint {
            consumer_group_id,
            topic,
            schema_json,
            offsets,
        }))
    }

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let conn = pool.connect().await.map_err(store_error)?;
        if self.schema_ensured.needs_ensure() {
            let _schema_guard = pool.acquire_schema_write_lock().await;
            ensure_kafka_tables(&conn).await?;
            self.schema_ensured.mark_ensured();
        }

        let _schema_guard = pool.acquire_schema_read_lock().await;

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(prior) = load_offsets(&conn, &self.dataset_name).await {
            let _ = merge_offsets(&self.dataset_name, prior, offsets);
        }

        upsert_offsets_each(&conn, &self.dataset_name, offsets).await?;
        Ok(())
    }
}

async fn ensure_kafka_tables(conn: &turso::Connection) -> Result<(), CheckpointError> {
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
    conn.execute(&create_metadata, ())
        .await
        .map_err(store_error)?;

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
    conn.execute(&create_offsets, ())
        .await
        .map_err(store_error)?;
    Ok(())
}

async fn upsert_offsets_each(
    conn: &turso::Connection,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<(), CheckpointError> {
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
    for offset in offsets {
        conn.execute(
            &stmt_sql,
            turso::params![
                dataset_name.to_string(),
                offset.topic.clone(),
                offset.partition,
                offset.offset,
            ],
        )
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

async fn load_offsets(
    conn: &turso::Connection,
    dataset_name: &str,
) -> Result<Vec<KafkaOffset>, CheckpointError> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?1"
    );
    let mut rows = conn
        .query(&query, turso::params![dataset_name.to_string()])
        .await
        .map_err(store_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_error)? {
        out.push(KafkaOffset {
            topic: row.get::<String>(0).map_err(store_error)?,
            partition: row.get::<i32>(1).map_err(store_error)?,
            offset: row.get::<i64>(2).map_err(store_error)?,
        });
    }
    sort_offsets(&mut out);
    Ok(out)
}
