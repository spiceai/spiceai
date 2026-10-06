/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The Debezium consumer-group / per-partition-offset tables.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use runtime_checkpoint_api::{
    CheckpointError,
    debezium::{DebeziumCheckpoint, DebeziumCheckpointStore},
    kafka::KafkaOffset,
    offsets::{OffsetSchemaState, merge_offsets, sort_offsets},
    retry::retry_on_write_conflict,
};

use crate::{spawn_checkpoint_blocking, store_error};

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";
const DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_debezium_kafka_offsets";

/// Debezium checkpoint store backed by a `DuckDB` accelerator.
pub struct DuckDbDebeziumCheckpointStore {
    pool: Arc<DuckDbConnectionPool>,
    dataset_name: String,
    schema_ensured: Arc<OffsetSchemaState>,
    /// Serializes this instance's own sidecar writes — see the Kafka store for why.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DuckDbDebeziumCheckpointStore {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>, dataset_name: String) -> Self {
        Self {
            pool,
            dataset_name,
            schema_ensured: Arc::default(),
            write_lock: Arc::default(),
        }
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn upsert_duckdb(
        dataset_name: &str,
        schema_ensured: &OffsetSchemaState,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &DebeziumCheckpoint,
    ) -> Result<(), CheckpointError> {
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        ensure_debezium_kafka_tables(duckdb_conn)?;
        schema_ensured.mark_ensured();

        let primary_keys = serde_json::to_string(&metadata.primary_keys).map_err(store_error)?;
        let schema_fields = metadata.schema_fields_json.clone();

        let tx = duckdb_conn.transaction().map_err(store_error)?;
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
                dataset_name,
                metadata.consumer_group_id,
                metadata.topic,
                primary_keys,
                schema_fields,
            ],
        )
        .map_err(store_error)?;
        upsert_offsets_tx(&tx, dataset_name, &metadata.offsets)?;
        tx.commit().map_err(store_error)?;

        Ok(())
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn get_duckdb(
        dataset_name: &str,
        schema_ensured: &OffsetSchemaState,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Result<Option<DebeziumCheckpoint>, CheckpointError> {
        // `ensure_debezium_kafka_tables` below issues DDL, so this read path is
        // also a writer to the shared acceleration file and takes the write gate.
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        if schema_ensured.needs_ensure() {
            ensure_debezium_kafka_tables(duckdb_conn)?;
            schema_ensured.mark_ensured();
        }

        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).map_err(store_error)?;
        let mut rows = stmt.query([dataset_name]).map_err(store_error)?;

        let Some(row) = rows.next().map_err(store_error)? else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0).map_err(store_error)?;
        let topic: String = row.get(1).map_err(store_error)?;
        let primary_keys_json: String = row.get(2).map_err(store_error)?;
        let schema_fields_json: String = row.get(3).map_err(store_error)?;
        drop(rows);
        drop(stmt);

        let offsets = load_offsets(duckdb_conn, dataset_name)?;

        let primary_keys: Vec<String> =
            serde_json::from_str(&primary_keys_json).map_err(store_error)?;

        Ok(Some(DebeziumCheckpoint {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields_json,
            offsets,
        }))
    }

    /// Blocking: takes the pool's write gate. Callers must reach this through
    /// `spawn_duckdb_blocking`, never directly from an async worker.
    fn upsert_offsets_duckdb(
        dataset_name: &str,
        schema_ensured: &OffsetSchemaState,
        pool: &Arc<DuckDbConnectionPool>,
        offsets: &[KafkaOffset],
    ) -> Result<(), CheckpointError> {
        let write_gate = pool.write_gate();
        let _write_guard = write_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut db_conn = Arc::clone(pool).connect_sync().map_err(store_error)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(store_error)?
            .get_underlying_conn_mut();

        if schema_ensured.needs_ensure() {
            ensure_debezium_kafka_tables(duckdb_conn)?;
            schema_ensured.mark_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(prior) = load_offsets(duckdb_conn, dataset_name) {
            let _ = merge_offsets(dataset_name, prior, offsets);
        }

        let tx = duckdb_conn.transaction().map_err(store_error)?;
        upsert_offsets_tx(&tx, dataset_name, offsets)?;
        tx.commit().map_err(store_error)?;
        Ok(())
    }
}

#[async_trait]
impl DebeziumCheckpointStore for DuckDbDebeziumCheckpointStore {
    async fn get(&self) -> Result<Option<DebeziumCheckpoint>, CheckpointError> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        let schema_ensured = Arc::clone(&self.schema_ensured);
        spawn_checkpoint_blocking(move || Self::get_duckdb(&dataset_name, &schema_ensured, &pool))
            .await
    }

    async fn upsert(&self, checkpoint: &DebeziumCheckpoint) -> Result<(), CheckpointError> {
        let pool = Arc::clone(&self.pool);
        let dataset_name = self.dataset_name.clone();
        let schema_ensured = Arc::clone(&self.schema_ensured);
        let checkpoint = checkpoint.clone();
        spawn_checkpoint_blocking(move || {
            Self::upsert_duckdb(&dataset_name, &schema_ensured, &pool, &checkpoint)
        })
        .await
    }

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        let _serialize = self.write_lock.lock().await;
        retry_on_write_conflict(&self.dataset_name, || {
            let pool = Arc::clone(&self.pool);
            let dataset_name = self.dataset_name.clone();
            let schema_ensured = Arc::clone(&self.schema_ensured);
            let offsets = offsets.to_vec();
            async move {
                spawn_checkpoint_blocking(move || {
                    Self::upsert_offsets_duckdb(&dataset_name, &schema_ensured, &pool, &offsets)
                })
                .await
            }
        })
        .await
    }
}
fn ensure_debezium_kafka_tables(conn: &mut duckdb::Connection) -> Result<(), CheckpointError> {
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
    conn.execute(&create_metadata, []).map_err(store_error)?;

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
    conn.execute(&create_offsets, []).map_err(store_error)?;
    Ok(())
}

fn upsert_offsets_tx(
    tx: &duckdb::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<(), CheckpointError> {
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
    let mut stmt = tx.prepare(&stmt_sql).map_err(store_error)?;
    for offset in offsets {
        stmt.execute(duckdb::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])
        .map_err(store_error)?;
    }
    Ok(())
}

fn load_offsets(
    conn: &duckdb::Connection,
    dataset_name: &str,
) -> Result<Vec<KafkaOffset>, CheckpointError> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?"
    );
    let mut stmt = conn.prepare(&query).map_err(store_error)?;
    let rows = stmt
        .query_map([dataset_name], |row| {
            Ok(KafkaOffset {
                topic: row.get(0)?,
                partition: row.get(1)?,
                offset: row.get(2)?,
            })
        })
        .map_err(store_error)?;
    let mut out: Vec<KafkaOffset> = rows.collect::<duckdb::Result<_>>().map_err(store_error)?;
    sort_offsets(&mut out);
    Ok(out)
}
