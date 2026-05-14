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

use super::{DEBEZIUM_KAFKA_TABLE_NAME, DebeziumKafkaMetadata, DebeziumKafkaSys, Error, Result};
use crate::dataaccelerator::turso::TursoConnectionPool;
use data_components::kafka::KafkaOffset;

impl DebeziumKafkaSys {
    pub(super) async fn upsert_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(Error::external)?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(Error::external)?;
        let offsets_json = Self::serialize_offsets(&metadata.offsets)?;

        let conn = pool.connect().await.map_err(Error::external)?;

        ensure_debezium_kafka_table(&conn).await?;
        self.mark_schema_ensured();

        let upsert = format!(
            "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME}
             (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, offsets_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = ?2,
                topic = ?3,
                primary_keys = ?4,
                schema_fields = ?5,
                offsets_json = ?6,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.execute(
            &upsert,
            turso::params![
                dataset_name,
                consumer_group_id,
                topic,
                primary_keys,
                schema_fields,
                offsets_json,
            ],
        )
        .await
        .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
    ) -> Option<DebeziumKafkaMetadata> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.ok()?;
        ensure_debezium_kafka_table(&conn).await.ok()?;
        self.mark_schema_ensured();
        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields, offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );

        let mut rows = conn
            .query(&query, turso::params![dataset_name])
            .await
            .ok()?;
        let row = rows.next().await.ok()??;

        let consumer_group_id = row.get::<String>(0).ok()?;
        let topic = row.get::<String>(1).ok()?;
        let primary_keys_json = row.get::<String>(2).ok()?;
        let schema_fields_json = row.get::<String>(3).ok()?;
        let offsets_json = row.get::<Option<String>>(4).ok()?;

        let primary_keys = serde_json::from_str(&primary_keys_json).ok()?;
        let schema_fields = serde_json::from_str(&schema_fields_json).ok()?;

        Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields,
            offsets: Self::deserialize_offsets(offsets_json.as_deref()).ok()?,
        })
    }

    pub(super) async fn upsert_offsets_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.map_err(Error::external)?;
        if self.schema_needs_ensure() {
            ensure_debezium_kafka_table(&conn).await?;
            self.mark_schema_ensured();
        }

        let query =
            format!("SELECT offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?1");
        let mut rows = conn
            .query(&query, turso::params![dataset_name.clone()])
            .await
            .map_err(Error::external)?;
        let row = rows.next().await.map_err(Error::external)?.ok_or_else(|| {
            Error::external(format!(
                "Debezium Kafka sidecar metadata for dataset {} does not exist",
                self.dataset_name
            ))
        })?;
        let existing_offsets_json = row.get::<Option<String>>(0).map_err(Error::external)?;
        let offsets_json =
            Self::serialize_merged_offsets(existing_offsets_json.as_deref(), offsets)?;

        let update = format!(
            "UPDATE {DEBEZIUM_KAFKA_TABLE_NAME} SET offsets_json = ?1, updated_at = CURRENT_TIMESTAMP WHERE dataset_name = ?2"
        );
        let changed = conn
            .execute(&update, turso::params![offsets_json, dataset_name])
            .await
            .map_err(Error::external)?;

        if changed == 0 {
            return Err(Error::external(format!(
                "Debezium Kafka sidecar metadata for dataset {} does not exist",
                self.dataset_name
            )));
        }

        Ok(())
    }
}

async fn ensure_debezium_kafka_table(conn: &turso::Connection) -> Result<()> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            primary_keys TEXT,
            schema_fields TEXT,
            offsets_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    );
    conn.execute(&create_table, ())
        .await
        .map_err(Error::external)?;

    let table_info = format!("PRAGMA table_info({DEBEZIUM_KAFKA_TABLE_NAME})");
    let mut rows = conn.query(&table_info, ()).await.map_err(Error::external)?;
    let mut has_offsets_json = false;
    while let Some(row) = rows.next().await.map_err(Error::external)? {
        if row.get::<String>(1).ok().as_deref() == Some("offsets_json") {
            has_offsets_json = true;
            break;
        }
    }

    if !has_offsets_json {
        let add_offsets =
            format!("ALTER TABLE {DEBEZIUM_KAFKA_TABLE_NAME} ADD COLUMN offsets_json TEXT");
        conn.execute(&add_offsets, ())
            .await
            .map_err(Error::external)?;
    }

    Ok(())
}
