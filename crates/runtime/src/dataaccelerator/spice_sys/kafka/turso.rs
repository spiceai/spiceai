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

use std::sync::Arc;

use super::super::offsets::{self, sort_offsets};
use super::{Error, KAFKA_OFFSETS_TABLE_NAME, KAFKA_TABLE_NAME, KafkaSys, Result};
use crate::dataaccelerator::turso::TursoConnectionPool;
use data_components::kafka::KafkaMetadata;
use data_components::kafka::KafkaOffset;

impl KafkaSys {
    pub(super) async fn upsert_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        metadata: &KafkaMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let schema_json = Self::serialize_schema(&metadata.schema)?;

        let conn = pool.connect().await.map_err(Error::external)?;

        ensure_kafka_tables(&conn).await?;
        self.mark_schema_ensured();

        // Turso lacks explicit transactions in its current Rust binding; the
        // metadata upsert is one statement and each per-partition upsert is
        // also one statement (idempotent via ON CONFLICT). Per-row atomicity
        // is what matters for resumability.
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
        .map_err(Error::external)?;

        upsert_offsets_each(&conn, &self.dataset_name, &metadata.offsets).await?;
        Ok(())
    }

    pub(super) async fn get_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
    ) -> Result<Option<KafkaMetadata>> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.map_err(Error::external)?;
        if self.schema_needs_ensure() {
            ensure_kafka_tables(&conn).await?;
            self.mark_schema_ensured();
        }

        let query = format!(
            "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut rows = conn
            .query(&query, turso::params![dataset_name.clone()])
            .await
            .map_err(Error::external)?;
        let Some(row) = rows.next().await.map_err(Error::external)? else {
            return Ok(None);
        };

        let consumer_group_id = row.get::<String>(0).map_err(Error::external)?;
        let topic = row.get::<String>(1).map_err(Error::external)?;
        let schema_json = row.get::<String>(2).map_err(Error::external)?;
        drop(rows);

        let schema = Self::deserialize_schema(&schema_json)?;
        let offsets = load_offsets(&conn, &dataset_name).await?;

        Ok(Some(KafkaMetadata {
            consumer_group_id,
            topic,
            schema,
            offsets,
        }))
    }

    pub(super) async fn upsert_offsets_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let conn = pool.connect().await.map_err(Error::external)?;
        if self.schema_needs_ensure() {
            ensure_kafka_tables(&conn).await?;
            self.mark_schema_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(prior) = load_offsets(&conn, &self.dataset_name).await {
            let _ = offsets::merge_offsets(&self.dataset_name, prior, offsets);
        }

        upsert_offsets_each(&conn, &self.dataset_name, offsets).await?;
        Ok(())
    }
}

async fn ensure_kafka_tables(conn: &turso::Connection) -> Result<()> {
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
        .map_err(Error::external)?;

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
        .map_err(Error::external)?;
    Ok(())
}

async fn upsert_offsets_each(
    conn: &turso::Connection,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<()> {
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
        .map_err(Error::external)?;
    }
    Ok(())
}

async fn load_offsets(conn: &turso::Connection, dataset_name: &str) -> Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?1"
    );
    let mut rows = conn
        .query(&query, turso::params![dataset_name.to_string()])
        .await
        .map_err(Error::external)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(Error::external)? {
        out.push(KafkaOffset {
            topic: row.get::<String>(0).map_err(Error::external)?,
            partition: row.get::<i32>(1).map_err(Error::external)?,
            offset: row.get::<i64>(2).map_err(Error::external)?,
        });
    }
    sort_offsets(&mut out);
    Ok(out)
}
