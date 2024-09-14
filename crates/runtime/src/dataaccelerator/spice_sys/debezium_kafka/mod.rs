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
use crate::{component::dataset::Dataset, dataconnector::debezium::DebeziumKafkaMetadata};
use data_components::debezium::change_event;
#[cfg(feature = "duckdb")]
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
#[cfg(feature = "postgres")]
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::sync::Arc;
#[cfg(feature = "sqlite")]
use tokio_rusqlite::Connection;

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";

pub struct DebeziumKafkaSys {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DebeziumKafkaSys {
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

    #[allow(unreachable_patterns)]
    pub(crate) async fn get(&self) -> Option<DebeziumKafkaMetadata> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_duckdb(pool),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => self.get_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.get_sqlite(conn).await,
            _ => unreachable!(),
        }
    }

    #[allow(unreachable_patterns)]
    pub(crate) async fn upsert(&self, metadata: &DebeziumKafkaMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => self.upsert_postgres(pool, metadata).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.upsert_sqlite(conn, metadata).await,
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "duckdb")]
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

    #[cfg(feature = "postgres")]
    async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;

        let create_table = format!(
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
            .execute(&create_table, &[])
            .await
            .map_err(|e| e.to_string())?;

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

        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(|e| e.to_string())?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(|e| e.to_string())?;

        conn.conn
            .execute(
                &upsert,
                &[
                    &self.dataset_name,
                    &metadata.consumer_group_id,
                    &metadata.topic,
                    &primary_keys,
                    &schema_fields,
                ],
            )
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    #[cfg(feature = "sqlite")]
    async fn upsert_sqlite(
        &self,
        conn: &Connection,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(|e| e.to_string())?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(|e| e.to_string())?;

        conn.call(move |conn| {
            let create_table = format!(
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
            conn.execute(&create_table, [])?;

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

            conn.execute(
                &upsert,
                [
                    dataset_name,
                    consumer_group_id,
                    topic,
                    primary_keys,
                    schema_fields,
                ],
            )?;

            Ok(())
        })
        .await
        .map_err(|e| e.to_string().into())
    }

    #[cfg(feature = "duckdb")]
    fn get_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Option<DebeziumKafkaMetadata> {
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

    #[cfg(feature = "postgres")]
    async fn get_postgres(&self, pool: &PostgresConnectionPool) -> Option<DebeziumKafkaMetadata> {
        let conn = pool.connect_direct().await.ok()?;
        let query = format!(
            "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = $1"
        );
        let stmt = conn.conn.prepare(&query).await.ok()?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .ok()??;

        let consumer_group_id: String = row.get(0);
        let topic: String = row.get(1);
        let primary_keys: String = row.get(2);
        let schema_fields: String = row.get(3);

        let primary_keys: Vec<String> = serde_json::from_str(&primary_keys).ok()?;
        let schema_fields: Vec<change_event::Field> = serde_json::from_str(&schema_fields).ok()?;

        Some(DebeziumKafkaMetadata {
            consumer_group_id,
            topic,
            primary_keys,
            schema_fields,
        })
    }

    #[cfg(feature = "sqlite")]
    async fn get_sqlite(&self, conn: &Connection) -> Option<DebeziumKafkaMetadata> {
        let dataset_name = self.dataset_name.clone();
        conn.call(move |conn| {
            let query = format!(
                "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
            );
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query([dataset_name])?;

            if let Some(row) = rows.next()? {
                let consumer_group_id: String = row.get(0)?;
                let topic: String = row.get(1)?;
                let primary_keys: String = row.get(2)?;
                let schema_fields: String = row.get(3)?;

                let primary_keys: Vec<String> = serde_json::from_str(&primary_keys).map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;
                let schema_fields: Vec<change_event::Field> = serde_json::from_str(&schema_fields).map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;

                Ok(DebeziumKafkaMetadata {
                    consumer_group_id,
                    topic,
                    primary_keys,
                    schema_fields,
                })
            } else {
                Err(tokio_rusqlite::Error::Other("No row found".into()))
            }
        })
        .await
        .ok()
    }
}
