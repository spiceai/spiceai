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
use super::{Error, KAFKA_OFFSETS_TABLE_NAME, KAFKA_TABLE_NAME, KafkaMetadata, KafkaSys, Result};
use data_components::kafka::KafkaOffset;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::postgresconn::PostgresConnection, postgrespool::PostgresConnectionPool,
};
use tokio_postgres::{Transaction, types::ToSql};

impl KafkaSys {
    pub(super) async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        metadata: &KafkaMetadata,
    ) -> Result<()> {
        ensure_kafka_tables(pool).await?;
        self.mark_schema_ensured();

        let mut conn = pool.connect_direct().await.map_err(Error::external)?;
        let tx = conn.conn.transaction().await.map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME}
             (dataset_name, consumer_group_id, topic, schema_json, updated_at)
             VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = EXCLUDED.consumer_group_id,
                topic = EXCLUDED.topic,
                schema_json = EXCLUDED.schema_json,
                updated_at = CURRENT_TIMESTAMP"
        );

        let schema_json = Self::serialize_schema(&metadata.schema)?;

        tx.execute(
            upsert.as_str(),
            &[
                &self.dataset_name,
                &metadata.consumer_group_id,
                &metadata.topic,
                &schema_json,
            ],
        )
        .await
        .map_err(Error::external)?;

        upsert_offsets_tx(&tx, &self.dataset_name, &metadata.offsets).await?;
        tx.commit().await.map_err(Error::external)?;
        Ok(())
    }

    pub(super) async fn get_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Result<Option<KafkaMetadata>> {
        if self.schema_needs_ensure() {
            ensure_kafka_tables(pool).await?;
            self.mark_schema_ensured();
        }
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let query = format!(
            "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = $1"
        );
        let Some(row) = conn
            .conn
            .query_opt(query.as_str(), &[&self.dataset_name])
            .await
            .map_err(Error::external)?
        else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0);
        let topic: String = row.get(1);
        let schema_json: String = row.get(2);
        let offsets = load_offsets(&conn, &self.dataset_name).await?;

        Ok(Some(KafkaMetadata {
            consumer_group_id,
            topic,
            schema: KafkaSys::deserialize_schema(&schema_json)?,
            offsets,
        }))
    }

    pub(super) async fn upsert_offsets_postgres(
        &self,
        pool: &PostgresConnectionPool,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        if self.schema_needs_ensure() {
            ensure_kafka_tables(pool).await?;
            self.mark_schema_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(read_conn) = pool.connect_direct().await
            && let Ok(prior) = load_offsets(&read_conn, &self.dataset_name).await
        {
            let _ = offsets::merge_offsets(&self.dataset_name, prior, offsets);
        }

        let mut conn = pool.connect_direct().await.map_err(Error::external)?;
        let tx = conn.conn.transaction().await.map_err(Error::external)?;
        upsert_offsets_tx(&tx, &self.dataset_name, offsets).await?;
        tx.commit().await.map_err(Error::external)?;
        Ok(())
    }
}

async fn ensure_kafka_tables(pool: &PostgresConnectionPool) -> Result<()> {
    let conn = pool.connect_direct().await.map_err(Error::external)?;

    let create_metadata = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    );
    conn.conn
        .execute(create_metadata.as_str(), &[])
        .await
        .map_err(Error::external)?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_OFFSETS_TABLE_NAME} (
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
        .map_err(Error::external)?;
    Ok(())
}

async fn upsert_offsets_tx(
    tx: &Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = GREATEST(EXCLUDED.partition_offset, {KAFKA_OFFSETS_TABLE_NAME}.partition_offset),
            updated_at = CURRENT_TIMESTAMP"
    );
    let stmt = tx
        .prepare(stmt_sql.as_str())
        .await
        .map_err(Error::external)?;
    for offset in offsets {
        let params: [&(dyn ToSql + Sync); 4] = [
            &dataset_name,
            &offset.topic,
            &offset.partition,
            &offset.offset,
        ];
        tx.execute(&stmt, &params).await.map_err(Error::external)?;
    }
    Ok(())
}

async fn load_offsets(conn: &PostgresConnection, dataset_name: &str) -> Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = $1"
    );
    let rows = conn
        .conn
        .query(query.as_str(), &[&dataset_name])
        .await
        .map_err(Error::external)?;
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
