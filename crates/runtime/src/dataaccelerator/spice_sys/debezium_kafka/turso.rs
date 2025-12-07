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

        let conn = pool.connect().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                consumer_group_id TEXT,
                topic TEXT,
                primary_keys TEXT,
                schema_fields TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.execute(&create_table, ())
            .await
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME}
             (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = ?2,
                topic = ?3,
                primary_keys = ?4,
                schema_fields = ?5,
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
        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
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

        let primary_keys = serde_json::from_str(&primary_keys_json).ok()?;
        let schema_fields = serde_json::from_str(&schema_fields_json).ok()?;

        Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields,
        })
    }
}
