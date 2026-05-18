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

use super::super::offsets::{self, sort_offsets};
use super::{
    DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME, DEBEZIUM_KAFKA_TABLE_NAME, DebeziumKafkaMetadata,
    DebeziumKafkaSys, Error, Result,
};
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

        ensure_debezium_kafka_tables(duckdb_conn)?;
        self.schema_ensured.mark_ensured();

        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(Error::external)?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(Error::external)?;

        let tx = duckdb_conn.transaction().map_err(Error::external)?;
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
        tx.execute(
            &upsert,
            duckdb::params![
                self.dataset_name,
                metadata.consumer_group_id,
                metadata.topic,
                primary_keys,
                schema_fields,
            ],
        )
        .map_err(Error::external)?;
        upsert_offsets_tx(&tx, &self.dataset_name, &metadata.offsets)?;
        tx.commit().map_err(Error::external)?;

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

        if self.schema_needs_ensure() {
            ensure_debezium_kafka_tables(duckdb_conn)?;
            self.mark_schema_ensured();
        }

        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).map_err(Error::external)?;
        let mut rows = stmt.query([&self.dataset_name]).map_err(Error::external)?;

        let Some(row) = rows.next().map_err(Error::external)? else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0).map_err(Error::external)?;
        let topic: String = row.get(1).map_err(Error::external)?;
        let primary_keys_json: String = row.get(2).map_err(Error::external)?;
        let schema_fields_json: String = row.get(3).map_err(Error::external)?;
        drop(rows);
        drop(stmt);

        let offsets = load_offsets(duckdb_conn, &self.dataset_name)?;

        let primary_keys: Vec<String> =
            serde_json::from_str(&primary_keys_json).map_err(Error::external)?;
        let schema_fields: Vec<change_event::Field> =
            serde_json::from_str(&schema_fields_json).map_err(Error::external)?;

        Ok(Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields,
            offsets,
        }))
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

        if self.schema_needs_ensure() {
            ensure_debezium_kafka_tables(duckdb_conn)?;
            self.mark_schema_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(prior) = load_offsets(duckdb_conn, &self.dataset_name) {
            let _ = offsets::merge_offsets(&self.dataset_name, prior, offsets);
        }

        let tx = duckdb_conn.transaction().map_err(Error::external)?;
        upsert_offsets_tx(&tx, &self.dataset_name, offsets)?;
        tx.commit().map_err(Error::external)?;
        Ok(())
    }
}

fn ensure_debezium_kafka_tables(conn: &mut duckdb::Connection) -> Result<()> {
    let create_metadata = format!(
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
    conn.execute(&create_metadata, [])
        .map_err(Error::external)?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME} (
            dataset_name TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            partition_offset BIGINT NOT NULL,
            updated_at TIMESTAMP,
            PRIMARY KEY (dataset_name, topic, partition_id)
        )"
    );
    conn.execute(&create_offsets, []).map_err(Error::external)?;
    Ok(())
}

fn upsert_offsets_tx(
    tx: &duckdb::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES (?, ?, ?, ?, now())
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = GREATEST(excluded.partition_offset, partition_offset),
            updated_at = now()"
    );
    let mut stmt = tx.prepare(&stmt_sql).map_err(Error::external)?;
    for offset in offsets {
        stmt.execute(duckdb::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])
        .map_err(Error::external)?;
    }
    Ok(())
}

fn load_offsets(conn: &duckdb::Connection, dataset_name: &str) -> Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?"
    );
    let mut stmt = conn.prepare(&query).map_err(Error::external)?;
    let rows = stmt
        .query_map([dataset_name], |row| {
            Ok(KafkaOffset {
                topic: row.get(0)?,
                partition: row.get(1)?,
                offset: row.get(2)?,
            })
        })
        .map_err(Error::external)?;
    let mut out: Vec<KafkaOffset> = rows
        .collect::<duckdb::Result<_>>()
        .map_err(Error::external)?;
    sort_offsets(&mut out);
    Ok(out)
}
