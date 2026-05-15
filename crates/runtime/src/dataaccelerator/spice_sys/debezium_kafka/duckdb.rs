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
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use std::sync::Arc;

impl DebeziumKafkaSys {
    pub(super) fn upsert_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        ensure_debezium_kafka_table(duckdb_conn)?;
        self.schema_ensured.mark_ensured();

        let upsert = format!(
            "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, offsets_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = excluded.consumer_group_id,
                topic = excluded.topic,
                primary_keys = excluded.primary_keys,
                schema_fields = excluded.schema_fields,
                offsets_json = excluded.offsets_json,
                updated_at = now()"
        );

        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(Error::external)?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(Error::external)?;
        let offsets_json = serialize_offsets(&metadata.offsets)?;

        duckdb_conn
            .execute(
                &upsert,
                [
                    &self.dataset_name,
                    &metadata.consumer_group_id,
                    &metadata.topic,
                    &primary_keys,
                    &schema_fields,
                    &offsets_json,
                ],
            )
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) fn get_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Result<Option<DebeziumKafkaMetadata>> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        ensure_debezium_kafka_table(duckdb_conn)?;
        self.schema_ensured.mark_ensured();

        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields, offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).map_err(Error::external)?;
        let mut rows = stmt.query([&self.dataset_name]).map_err(Error::external)?;

        if let Some(row) = rows.next().map_err(Error::external)? {
            let consumer_group_id: String = row.get(0).map_err(Error::external)?;
            let topic: String = row.get(1).map_err(Error::external)?;
            let primary_keys: String = row.get(2).map_err(Error::external)?;
            let schema_fields: String = row.get(3).map_err(Error::external)?;
            let offsets_json: Option<String> = row.get(4).map_err(Error::external)?;

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
        } else {
            Ok(None)
        }
    }

    pub(super) fn upsert_offsets_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        if self.schema_ensured.needs_ensure() {
            ensure_debezium_kafka_table(duckdb_conn)?;
            self.schema_ensured.mark_ensured();
        }

        let query =
            format!("SELECT offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?");
        let existing_offsets_json: Option<String> = duckdb_conn
            .query_row(&query, [&self.dataset_name], |row| row.get(0))
            .map_err(Error::external)?;
        let offsets_json = serialize_merged_offsets(existing_offsets_json.as_deref(), offsets)?;
        let update = format!(
            "UPDATE {DEBEZIUM_KAFKA_TABLE_NAME} SET offsets_json = ?, updated_at = now() WHERE dataset_name = ?"
        );
        let changed = duckdb_conn
            .execute(&update, [&offsets_json, &self.dataset_name])
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

fn ensure_debezium_kafka_table(conn: &mut duckdb::Connection) -> Result<()> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            primary_keys TEXT,
            schema_fields TEXT,
            offsets_json TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )"
    );
    conn.execute(&create_table, []).map_err(Error::external)?;

    let add_offsets = format!(
        "ALTER TABLE {DEBEZIUM_KAFKA_TABLE_NAME} ADD COLUMN IF NOT EXISTS offsets_json TEXT"
    );
    conn.execute(&add_offsets, []).map_err(Error::external)?;

    Ok(())
}
