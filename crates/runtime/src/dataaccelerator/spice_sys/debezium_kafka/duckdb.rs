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
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use std::sync::Arc;

impl DebeziumKafkaSys {
    pub(super) fn upsert_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(|e| e.to_string())?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                consumer_group_id TEXT,
                topic TEXT,
                primary_keys TEXT,
                schema_fields TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(|e| e.to_string())?;

        let upsert = format!(
            "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = excluded.consumer_group_id,
                topic = excluded.topic,
                primary_keys = excluded.primary_keys,
                schema_fields = excluded.schema_fields,
                updated_at = now()"
        );

        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(|e| e.to_string())?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(|e| e.to_string())?;

        duckdb_conn
            .execute(
                &upsert,
                [
                    &self.dataset_name,
                    &metadata.consumer_group_id,
                    &metadata.topic,
                    &primary_keys,
                    &schema_fields,
                ],
            )
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(super) fn get_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Option<DebeziumKafkaMetadata> {
        let mut db_conn = Arc::clone(pool).connect_sync().ok()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();

        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([&self.dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let consumer_group_id: String = row.get(0).ok()?;
            let topic: String = row.get(1).ok()?;
            let primary_keys: String = row.get(2).ok()?;
            let schema_fields: String = row.get(3).ok()?;

            let primary_keys: Vec<String> = serde_json::from_str(&primary_keys).ok()?;
            let schema_fields: Vec<change_event::Field> =
                serde_json::from_str(&schema_fields).ok()?;

            Some(DebeziumKafkaMetadata {
                consumer_group_id,
                topic,
                primary_keys,
                schema_fields,
            })
        } else {
            None
        }
    }
}
