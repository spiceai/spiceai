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

use super::{Error, KAFKA_TABLE_NAME, KafkaMetadata, KafkaSys, Result};
use crate::dataaccelerator::turso::TursoConnectionPool;
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
        let offsets_json = Self::serialize_offsets(&metadata.offsets)?;

        let conn = pool.connect().await.map_err(Error::external)?;

        ensure_kafka_table(&conn).await?;
        self.mark_schema_ensured();

        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, offsets_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = ?2,
                topic = ?3,
                schema_json = ?4,
                offsets_json = ?5,
                updated_at = CURRENT_TIMESTAMP"
        );
        conn.execute(
            &upsert,
            turso::params![
                dataset_name,
                consumer_group_id,
                topic,
                schema_json,
                offsets_json,
            ],
        )
        .await
        .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_turso(&self, pool: &Arc<TursoConnectionPool>) -> Option<KafkaMetadata> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.ok()?;
        ensure_kafka_table(&conn).await.ok()?;
        self.mark_schema_ensured();
        let query = format!(
            "SELECT consumer_group_id, topic, schema_json, offsets_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );

        let mut rows = conn
            .query(&query, turso::params![dataset_name])
            .await
            .ok()?;
        let row = rows.next().await.ok()??;

        let consumer_group_id = row.get::<String>(0).ok()?;
        let topic = row.get::<String>(1).ok()?;
        let schema_json = row.get::<String>(2).ok()?;
        let offsets_json = row.get::<Option<String>>(3).ok()?;

        let schema = Self::deserialize_schema(&schema_json).ok()?;

        Some(KafkaMetadata {
            consumer_group_id,
            topic,
            schema,
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
            ensure_kafka_table(&conn).await?;
            self.mark_schema_ensured();
        }

        let query = format!("SELECT offsets_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?1");
        let mut rows = conn
            .query(&query, turso::params![dataset_name.clone()])
            .await
            .map_err(Error::external)?;
        let row = rows.next().await.map_err(Error::external)?.ok_or_else(|| {
            Error::external(format!(
                "Kafka sidecar metadata for dataset {} does not exist",
                self.dataset_name
            ))
        })?;
        let existing_offsets_json = row.get::<Option<String>>(0).map_err(Error::external)?;
        let offsets_json =
            Self::serialize_merged_offsets(existing_offsets_json.as_deref(), offsets)?;

        let update = format!(
            "UPDATE {KAFKA_TABLE_NAME} SET offsets_json = ?1, updated_at = CURRENT_TIMESTAMP WHERE dataset_name = ?2"
        );
        let changed = conn
            .execute(&update, turso::params![offsets_json, dataset_name])
            .await
            .map_err(Error::external)?;

        if changed == 0 {
            return Err(Error::external(format!(
                "Kafka sidecar metadata for dataset {} does not exist",
                self.dataset_name
            )));
        }

        Ok(())
    }
}

async fn ensure_kafka_table(conn: &turso::Connection) -> Result<()> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            offsets_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    );
    conn.execute(&create_table, ())
        .await
        .map_err(Error::external)?;

    if !has_offsets_json_column(conn).await? {
        let add_offsets = format!("ALTER TABLE {KAFKA_TABLE_NAME} ADD COLUMN offsets_json TEXT");
        match conn.execute(&add_offsets, ()).await {
            Ok(_) => {}
            Err(err) if is_duplicate_offsets_column_error(&err) => {}
            Err(err) => return Err(Error::external(err)),
        }
    }

    Ok(())
}

async fn has_offsets_json_column(conn: &turso::Connection) -> Result<bool> {
    let table_info = format!("PRAGMA table_info({KAFKA_TABLE_NAME})");
    let mut rows = conn.query(&table_info, ()).await.map_err(Error::external)?;
    while let Some(row) = rows.next().await.map_err(Error::external)? {
        if row.get::<String>(1).ok().as_deref() == Some("offsets_json") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_duplicate_offsets_column_error(err: &turso::Error) -> bool {
    let message = err.to_string();
    message.contains("duplicate column name") && message.contains("offsets_json")
}
