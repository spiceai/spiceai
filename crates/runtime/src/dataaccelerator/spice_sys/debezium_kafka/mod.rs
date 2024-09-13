/*
Copyright 2024 The Spice.ai OSS Authors

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

//! CREATE TABLE `spice_sys_debezium_kafka` (
//!     `dataset_name` TEXT PRIMARY KEY,
//!     `consumer_group_id` TEXT,
//!     `topic` TEXT,
//!     `primary_keys` TEXT,
//!     `schema_fields` TEXT,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );

use super::{acceleration_connection, AccelerationConnection, Result};
use crate::component::dataset::Dataset;
use data_components::debezium::change_event;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_rusqlite::Connection;

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";

#[derive(Serialize, Deserialize)]
struct DebeziumKafkaMetadata {
    consumer_group_id: String,
    topic: String,
    primary_keys: Vec<String>,
    schema_fields: Vec<change_event::Field>,
}

pub struct DebeziumKafka {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DebeziumKafka {
    pub async fn try_new(dataset: &Dataset) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, false).await?,
        })
    }

    pub async fn try_new_create_if_not_exists(dataset: &Dataset) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, true).await?,
        })
    }

    pub async fn exists(&self) -> bool {
        match &self.acceleration_connection {
            AccelerationConnection::DuckDB(pool) => self.exists_duckdb(pool).ok().unwrap_or(false),
            AccelerationConnection::Postgres(pool) => {
                self.exists_postgres(pool).await.ok().unwrap_or(false)
            }
            AccelerationConnection::SQLite(conn) => {
                self.exists_sqlite(conn).await.ok().unwrap_or(false)
            }
        }
    }

    pub async fn upsert(&self, metadata: &DebeziumKafkaMetadata) -> Result<()> {
        match &self.acceleration_connection {
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
            AccelerationConnection::Postgres(pool) => self.upsert_postgres(pool, metadata).await,
            AccelerationConnection::SQLite(conn) => self.upsert_sqlite(conn, metadata).await,
        }
    }

    // Implementation for exists_duckdb, exists_postgres, exists_sqlite
    // ... (similar to DatasetCheckpoint)

    fn upsert_duckdb(
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

    // Implementation for upsert_postgres and upsert_sqlite
    // ... (similar to DatasetCheckpoint, but with the DebeziumKafkaMetadata fields)
}

#[cfg(test)]
mod tests {
    // ... (similar to DatasetCheckpoint tests, but adapted for DebeziumKafka)
}
