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

use super::{DEBEZIUM_KAFKA_TABLE_NAME, DebeziumKafkaMetadata, DebeziumKafkaSys, Error, Result};
use data_components::debezium::change_event;
use data_components::kafka::KafkaOffset;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

impl DebeziumKafkaSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
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

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                ensure_debezium_kafka_table(conn)?;

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
                    [
                        dataset_name,
                        consumer_group_id,
                        topic,
                        primary_keys,
                        schema_fields,
                        offsets_json,
                    ],
                )?;

                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)
    }

    pub(super) async fn get_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Option<DebeziumKafkaMetadata> {
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let conn = conn_sync.as_any().downcast_ref::<SqliteConnection>()?;

        conn.conn
            .call(move |conn| {
                ensure_debezium_kafka_table(conn)?;

                let query = format!(
                    "SELECT consumer_group_id, topic, primary_keys, schema_fields, offsets_json FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    let consumer_group_id: String = row.get(0)?;
                    let topic: String = row.get(1)?;
                    let primary_keys: String = row.get(2)?;
                    let schema_fields: String = row.get(3)?;
                    let offsets_json: Option<String> = row.get(4)?;

                    let primary_keys: Vec<String> = serde_json::from_str(&primary_keys)
                        .map_err(|err| {
                            tracing::warn!("Failed to deserialize primary_keys from SQLite: {err}");
                            rusqlite::Error::InvalidQuery
                        })?;
                    let schema_fields: Vec<change_event::Field> = serde_json::from_str(&schema_fields)
                        .map_err(|err| {
                            tracing::warn!("Failed to deserialize schema_fields from SQLite: {err}");
                            rusqlite::Error::InvalidQuery
                        })?;

                    Ok(DebeziumKafkaMetadata {
                        consumer_group_id,
                        topic,
                        primary_keys,
                        schema_fields,
                        offsets: DebeziumKafkaSys::deserialize_offsets(offsets_json.as_deref())
                            .map_err(|err| {
                                tracing::warn!("Failed to deserialize Debezium Kafka offsets from SQLite: {err}");
                                rusqlite::Error::InvalidQuery
                            })?,
                    })
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows)
                }
            })
            .await
            .ok()
    }

    pub(super) async fn upsert_offsets_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let offsets_json = Self::serialize_offsets(offsets)?;

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                ensure_debezium_kafka_table(conn)?;
                let update = format!(
                    "UPDATE {DEBEZIUM_KAFKA_TABLE_NAME} SET offsets_json = ?1, updated_at = CURRENT_TIMESTAMP WHERE dataset_name = ?2"
                );
                let changed = conn.execute(&update, [offsets_json, dataset_name])?;
                if changed == 0 {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)
    }
}

fn ensure_debezium_kafka_table(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
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
    conn.execute(&create_table, [])?;

    let table_info = format!("PRAGMA table_info({DEBEZIUM_KAFKA_TABLE_NAME})");
    let mut stmt = conn.prepare(&table_info)?;
    let mut columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_offsets_json = false;
    while let Some(column) = columns.next() {
        if column? == "offsets_json" {
            has_offsets_json = true;
            break;
        }
    }

    if !has_offsets_json {
        let add_offsets =
            format!("ALTER TABLE {DEBEZIUM_KAFKA_TABLE_NAME} ADD COLUMN offsets_json TEXT");
        conn.execute(&add_offsets, [])?;
    }

    Ok(())
}
