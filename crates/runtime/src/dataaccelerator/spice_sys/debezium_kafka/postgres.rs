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

use super::super::offsets::{deserialize_offsets, serialize_merged_offsets, serialize_offsets};
use super::{DEBEZIUM_KAFKA_TABLE_NAME, DebeziumKafkaMetadata, DebeziumKafkaSys, Error, Result};
use data_components::debezium::change_event;
use data_components::kafka::KafkaOffset;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

impl DebeziumKafkaSys {
    pub(super) async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

        ensure_debezium_kafka_table(pool).await?;
        self.schema_ensured.mark_ensured();

        let upsert = format!(
            "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME}
             (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, offsets_json, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = EXCLUDED.consumer_group_id,
                topic = EXCLUDED.topic,
                primary_keys = EXCLUDED.primary_keys,
                schema_fields = EXCLUDED.schema_fields,
                offsets_json = EXCLUDED.offsets_json,
                updated_at = CURRENT_TIMESTAMP"
        );

        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(Error::external)?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(Error::external)?;
        let offsets_json = serialize_offsets(&metadata.offsets)?;

        conn.conn
            .execute(
                &upsert,
                &[
                    &self.dataset_name,
                    &metadata.consumer_group_id,
                    &metadata.topic,
                    &primary_keys,
                    &schema_fields,
                    &offsets_json,
                ],
            )
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Result<Option<DebeziumKafkaMetadata>> {
        ensure_debezium_kafka_table(pool).await?;
        self.schema_ensured.mark_ensured();
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields, offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = $1"
        );
        let stmt = conn.conn.prepare(&query).await.map_err(Error::external)?;
        let Some(row) = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .map_err(Error::external)?
        else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0);
        let topic: String = row.get(1);
        let primary_keys: String = row.get(2);
        let schema_fields: String = row.get(3);
        let offsets_json: Option<String> = row.get(4);

        let primary_keys: Vec<String> =
            serde_json::from_str(&primary_keys).map_err(Error::external)?;
        let schema_fields: Vec<change_event::Field> =
            serde_json::from_str(&schema_fields).map_err(Error::external)?;

        Ok(Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields,
            offsets: deserialize_offsets(offsets_json.as_deref())?,
        }))
    }

    pub(super) async fn upsert_offsets_postgres(
        &self,
        pool: &PostgresConnectionPool,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        if self.schema_ensured.needs_ensure() {
            ensure_debezium_kafka_table(pool).await?;
            self.schema_ensured.mark_ensured();
        }
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let query =
            format!("SELECT offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = $1");
        let row = conn
            .conn
            .query_opt(&query, &[&self.dataset_name])
            .await
            .map_err(Error::external)?
            .ok_or_else(|| {
                Error::external(format!(
                    "Debezium Kafka sidecar metadata for dataset {} does not exist",
                    self.dataset_name
                ))
            })?;
        let existing_offsets_json: Option<String> = row.get(0);
        let offsets_json = serialize_merged_offsets(existing_offsets_json.as_deref(), offsets)?;
        let update = format!(
            "UPDATE {DEBEZIUM_KAFKA_TABLE_NAME} SET offsets_json = $1, updated_at = CURRENT_TIMESTAMP WHERE dataset_name = $2"
        );
        let changed = conn
            .conn
            .execute(&update, &[&offsets_json, &self.dataset_name])
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

async fn ensure_debezium_kafka_table(pool: &PostgresConnectionPool) -> Result<()> {
    let conn = pool.connect_direct().await.map_err(Error::external)?;

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
    conn.conn
        .execute(&create_table, &[])
        .await
        .map_err(Error::external)?;

    let add_offsets = format!(
        "ALTER TABLE {DEBEZIUM_KAFKA_TABLE_NAME} ADD COLUMN IF NOT EXISTS offsets_json TEXT"
    );
    conn.conn
        .execute(&add_offsets, &[])
        .await
        .map_err(Error::external)?;

    Ok(())
}
