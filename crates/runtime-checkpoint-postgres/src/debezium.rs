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
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::postgresconn::PostgresConnection, postgrespool::PostgresConnectionPool,
};
use runtime_checkpoint_api::{
    CheckpointError,
    debezium::{DebeziumCheckpoint, DebeziumCheckpointStore},
    kafka::KafkaOffset,
    offsets::{OffsetSchemaState, merge_offsets, sort_offsets},
};
use tokio_postgres::{Transaction, types::ToSql};

use crate::store_error;

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";
const DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_debezium_kafka_offsets";

/// Debezium checkpoint store backed by a `PostgreSQL` accelerator.
pub struct PostgresDebeziumCheckpointStore {
    pool: Arc<PostgresConnectionPool>,
    dataset_name: String,
    schema_ensured: Arc<OffsetSchemaState>,
}

impl PostgresDebeziumCheckpointStore {
    #[must_use]
    pub fn new(pool: Arc<PostgresConnectionPool>, dataset_name: String) -> Self {
        Self {
            pool,
            dataset_name,
            schema_ensured: Arc::default(),
        }
    }
}

#[async_trait]
impl DebeziumCheckpointStore for PostgresDebeziumCheckpointStore {
    async fn upsert(&self, checkpoint: &DebeziumCheckpoint) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        let metadata = checkpoint;
        ensure_debezium_kafka_tables(pool).await?;
        self.schema_ensured.mark_ensured();

        let mut conn = pool.connect_direct().await.map_err(store_error)?;
        let tx = conn.conn.transaction().await.map_err(store_error)?;

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

        let primary_keys = serde_json::to_string(&metadata.primary_keys).map_err(store_error)?;
        let schema_fields = metadata.schema_fields_json.clone();

        tx.execute(
            upsert.as_str(),
            &[
                &self.dataset_name,
                &metadata.consumer_group_id,
                &metadata.topic,
                &primary_keys,
                &schema_fields,
            ],
        )
        .await
        .map_err(store_error)?;

        upsert_offsets_tx(&tx, &self.dataset_name, &metadata.offsets).await?;
        tx.commit().await.map_err(store_error)?;
        Ok(())
    }

    async fn get(&self) -> Result<Option<DebeziumCheckpoint>, CheckpointError> {
        let pool = &self.pool;
        if self.schema_ensured.needs_ensure() {
            ensure_debezium_kafka_tables(pool).await?;
            self.schema_ensured.mark_ensured();
        }
        let conn = pool.connect_direct().await.map_err(store_error)?;
        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = $1"
        );
        let Some(row) = conn
            .conn
            .query_opt(query.as_str(), &[&self.dataset_name])
            .await
            .map_err(store_error)?
        else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0);
        let topic: String = row.get(1);
        let primary_keys_json: String = row.get(2);
        let schema_fields_json: String = row.get(3);
        let offsets = load_offsets(&conn, &self.dataset_name).await?;

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

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        let pool = &self.pool;
        if self.schema_ensured.needs_ensure() {
            ensure_debezium_kafka_tables(pool).await?;
            self.schema_ensured.mark_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(read_conn) = pool.connect_direct().await
            && let Ok(prior) = load_offsets(&read_conn, &self.dataset_name).await
        {
            let _ = merge_offsets(&self.dataset_name, prior, offsets);
        }

        let mut conn = pool.connect_direct().await.map_err(store_error)?;
        let tx = conn.conn.transaction().await.map_err(store_error)?;
        upsert_offsets_tx(&tx, &self.dataset_name, offsets).await?;
        tx.commit().await.map_err(store_error)?;
        Ok(())
    }
}

async fn ensure_debezium_kafka_tables(
    pool: &PostgresConnectionPool,
) -> Result<(), CheckpointError> {
    let conn = pool.connect_direct().await.map_err(store_error)?;

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
    conn.conn
        .execute(create_metadata.as_str(), &[])
        .await
        .map_err(store_error)?;

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
    conn.conn
        .execute(create_offsets.as_str(), &[])
        .await
        .map_err(store_error)?;
    Ok(())
}

async fn upsert_offsets_tx(
    tx: &Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<(), CheckpointError> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = GREATEST(EXCLUDED.partition_offset, {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME}.partition_offset),
            updated_at = CURRENT_TIMESTAMP"
    );
    let stmt = tx.prepare(stmt_sql.as_str()).await.map_err(store_error)?;
    for offset in offsets {
        let params: [&(dyn ToSql + Sync); 4] = [
            &dataset_name,
            &offset.topic,
            &offset.partition,
            &offset.offset,
        ];
        tx.execute(&stmt, &params).await.map_err(store_error)?;
    }
    Ok(())
}

async fn load_offsets(
    conn: &PostgresConnection,
    dataset_name: &str,
) -> Result<Vec<KafkaOffset>, CheckpointError> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = $1"
    );
    let rows = conn
        .conn
        .query(query.as_str(), &[&dataset_name])
        .await
        .map_err(store_error)?;
    let mut out: Vec<KafkaOffset> = rows
        .into_iter()
        .map(|row| KafkaOffset {
            topic: row.get(0),
            partition: row.get(1),
            offset: row.get(2),
        })
        .collect();
    sort_offsets(&mut out);
    Ok(out)
}
