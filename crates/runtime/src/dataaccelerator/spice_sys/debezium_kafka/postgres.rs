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

use super::{DebeziumKafkaMetadata, DebeziumKafkaSys, Result, DEBEZIUM_KAFKA_TABLE_NAME};
use data_components::debezium::change_event;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

impl DebeziumKafkaSys {
    pub(super) async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;

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
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(|e| e.to_string())?;

        let upsert = format!(
            "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME}
             (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, updated_at)
             VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = EXCLUDED.consumer_group_id,
                topic = EXCLUDED.topic,
                primary_keys = EXCLUDED.primary_keys,
                schema_fields = EXCLUDED.schema_fields,
                updated_at = CURRENT_TIMESTAMP"
        );

        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(|e| e.to_string())?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(|e| e.to_string())?;

        conn.conn
            .execute(
                &upsert,
                &[
                    &self.dataset_name,
                    &metadata.consumer_group_id,
                    &metadata.topic,
                    &primary_keys,
                    &schema_fields,
                ],
            )
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(super) async fn get_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Option<DebeziumKafkaMetadata> {
        let conn = pool.connect_direct().await.ok()?;
        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = $1"
        );
        let stmt = conn.conn.prepare(&query).await.ok()?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .ok()??;

        let consumer_group_id: String = row.get(0);
        let topic: String = row.get(1);
        let primary_keys: String = row.get(2);
        let schema_fields: String = row.get(3);

        let primary_keys: Vec<String> = serde_json::from_str(&primary_keys).ok()?;
        let schema_fields: Vec<change_event::Field> = serde_json::from_str(&schema_fields).ok()?;

        Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields,
        })
    }
}
