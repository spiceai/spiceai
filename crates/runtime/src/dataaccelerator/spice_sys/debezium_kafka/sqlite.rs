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

use rusqlite::OptionalExtension;

use super::super::offsets::{self, sort_offsets};
use super::{
    DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME, DEBEZIUM_KAFKA_TABLE_NAME, DebeziumKafkaMetadata,
    DebeziumKafkaSys, Error, Result,
};
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
        let seed_offsets = metadata.offsets.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                ensure_debezium_kafka_tables(conn)?;

                let tx = conn.transaction()?;
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

                tx.execute(
                    &upsert,
                    rusqlite::params![
                        dataset_name,
                        consumer_group_id,
                        topic,
                        primary_keys,
                        schema_fields,
                    ],
                )?;
                upsert_offsets_into(&tx, &dataset_name, &seed_offsets)?;
                tx.commit()?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)?;

        self.schema_ensured.mark_ensured();
        Ok(())
    }

    pub(super) async fn get_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Result<Option<DebeziumKafkaMetadata>> {
        type MetadataRow = (String, String, String, String);

        let dataset_name = self.dataset_name.clone();
        let schema_needs_ensure = self.schema_needs_ensure();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        let result = conn
            .conn
            .call(move |conn| {
                if schema_needs_ensure {
                    ensure_debezium_kafka_tables(conn)?;
                }

                let metadata_query = format!(
                    "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?1"
                );
                let metadata: Option<MetadataRow> = conn
                    .query_row(&metadata_query, [&dataset_name], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .optional()?;

                let Some((consumer_group_id, topic, primary_keys, schema_fields)) = metadata
                else {
                    return Ok::<Option<(String, String, String, String, Vec<KafkaOffset>)>, rusqlite::Error>(None);
                };

                let offsets = load_offsets(conn, &dataset_name)?;
                Ok(Some((
                    consumer_group_id,
                    topic,
                    primary_keys,
                    schema_fields,
                    offsets,
                )))
            })
            .await
            .map_err(Error::external)?;

        if schema_needs_ensure {
            self.mark_schema_ensured();
        }

        let Some((consumer_group_id, topic, primary_keys_json, schema_fields_json, offsets)) =
            result
        else {
            return Ok(None);
        };

        Ok(Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys: serde_json::from_str(&primary_keys_json).map_err(Error::external)?,
            schema_fields: serde_json::from_str::<Vec<change_event::Field>>(&schema_fields_json)
                .map_err(Error::external)?,
            offsets,
        }))
    }

    pub(super) async fn upsert_offsets_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let new_offsets = offsets.to_vec();
        let warn_dataset = self.dataset_name.clone();
        let schema_needs_ensure = self.schema_needs_ensure();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                if schema_needs_ensure {
                    ensure_debezium_kafka_tables(conn)?;
                }

                // Diagnostic-only: surface a warn log when an offset regresses.
                if let Ok(prior) = load_offsets(conn, &dataset_name) {
                    let _ = offsets::merge_offsets(&warn_dataset, prior, &new_offsets);
                }

                let tx = conn.transaction()?;
                upsert_offsets_into(&tx, &dataset_name, &new_offsets)?;
                tx.commit()?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)?;

        if schema_needs_ensure {
            self.mark_schema_ensured();
        }

        Ok(())
    }
}

fn ensure_debezium_kafka_tables(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let create_metadata = format!(
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
    conn.execute(&create_metadata, [])?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME} (
            dataset_name TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            partition_offset BIGINT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (dataset_name, topic, partition_id)
        )"
    );
    conn.execute(&create_offsets, [])?;
    Ok(())
}

fn upsert_offsets_into(
    tx: &rusqlite::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> rusqlite::Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = MAX(excluded.partition_offset, partition_offset),
            updated_at = CURRENT_TIMESTAMP"
    );
    let mut stmt = tx.prepare(&stmt_sql)?;
    for offset in offsets {
        stmt.execute(rusqlite::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])?;
    }
    Ok(())
}

fn load_offsets(
    conn: &rusqlite::Connection,
    dataset_name: &str,
) -> rusqlite::Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?1"
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([dataset_name], |row| {
        Ok(KafkaOffset {
            topic: row.get(0)?,
            partition: row.get(1)?,
            offset: row.get(2)?,
        })
    })?;
    let mut out: Vec<KafkaOffset> = rows.collect::<rusqlite::Result<_>>()?;
    sort_offsets(&mut out);
    Ok(out)
}
