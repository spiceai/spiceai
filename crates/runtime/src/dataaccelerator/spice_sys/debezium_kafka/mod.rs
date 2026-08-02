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

//! CREATE TABLE `spice_sys_debezium_kafka` (
//!     `dataset_name` TEXT PRIMARY KEY,
//!     `consumer_group_id` TEXT,
//!     `topic` TEXT,
//!     `primary_keys` TEXT,
//!     `schema_fields` TEXT,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );
//!
//! CREATE TABLE `spice_sys_debezium_kafka_offsets` (
//!     `dataset_name` TEXT NOT NULL,
//!     `topic` TEXT NOT NULL,
//!     `partition_id` INTEGER NOT NULL,
//!     `partition_offset` BIGINT NOT NULL,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     PRIMARY KEY (`dataset_name`, `topic`, `partition_id`),
//! );

use super::{
    AccelerationConnection, Error, Result, acceleration_connection, offsets::OffsetSchemaState,
};
use crate::{
    component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption,
    dataconnector::debezium::DebeziumKafkaMetadata,
};
use data_components::kafka::KafkaOffset;
use std::sync::Arc;

#[cfg(feature = "duckdb")]
use super::retry_on_write_conflict;

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";
const DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_debezium_kafka_offsets";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres-accel")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

pub struct DebeziumKafkaSys {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
    schema_ensured: Arc<OffsetSchemaState>,
    /// Serializes this instance's own `DuckDB` sidecar writes.
    ///
    /// `DuckDB` resolves concurrent writes to one row optimistically — the loser
    /// gets `Conflict on update!` instead of waiting — and the sidecar writers hold
    /// the pool's write gate with `read()`, so they do not exclude each other. Two
    /// commits for the same dataset therefore conflict rather than queue. Before the
    /// writes moved to the blocking pool, the async worker serialized them by
    /// accident; this keeps that ordering on purpose, so a burst of commits for one
    /// dataset still resolves to the max offset instead of failing.
    ///
    /// Scoped to one instance: writers for different datasets key on distinct rows
    /// and do not conflict. `retry_on_write_conflict` still covers contention this
    /// lock cannot see.
    #[cfg(feature = "duckdb")]
    duckdb_write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DebeziumKafkaSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        let registry = dataset.runtime.accelerator_engine_registry();
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, registry, open_option)
                .await?,
            schema_ensured: Arc::default(),
            #[cfg(feature = "duckdb")]
            duckdb_write_lock: Arc::default(),
        })
    }

    pub(crate) async fn get(&self) -> Result<Option<DebeziumKafkaMetadata>> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                let schema_ensured = Arc::clone(&self.schema_ensured);
                super::spawn_duckdb_blocking(move || {
                    Self::get_duckdb(&dataset_name, &schema_ensured, &pool)
                })
                .await
            }
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.get_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.get_sqlite(conn).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.get_turso(pool).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.get_sqlite(conn).await,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres-accel",
                feature = "turso"
            )))]
            _ => Ok(None),
        }
    }

    pub(crate) async fn upsert(&self, metadata: &DebeziumKafkaMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                let schema_ensured = Arc::clone(&self.schema_ensured);
                let metadata = metadata.clone();
                let _serialized = self.duckdb_write_lock.lock().await;
                super::spawn_duckdb_blocking(move || {
                    Self::upsert_duckdb(&dataset_name, &schema_ensured, &pool, &metadata)
                })
                .await
            }
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.upsert_postgres(pool, metadata).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.upsert_sqlite(conn, metadata).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.upsert_turso(pool, metadata).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.upsert_sqlite(conn, metadata).await,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres-accel",
                feature = "turso"
            )))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }

    pub(crate) async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                let schema_ensured = Arc::clone(&self.schema_ensured);
                let offsets = offsets.to_vec();
                let _serialized = self.duckdb_write_lock.lock().await;
                retry_on_write_conflict(&dataset_name, || {
                    let pool = Arc::clone(&pool);
                    let dataset_name = dataset_name.clone();
                    let schema_ensured = Arc::clone(&schema_ensured);
                    let offsets = offsets.clone();
                    super::spawn_duckdb_blocking(move || {
                        Self::upsert_offsets_duckdb(&dataset_name, &schema_ensured, &pool, &offsets)
                    })
                })
                .await
            }
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => {
                self.upsert_offsets_postgres(pool, offsets).await
            }
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.upsert_offsets_sqlite(conn, offsets).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.upsert_offsets_turso(pool, offsets).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => {
                self.upsert_offsets_sqlite(conn, offsets).await
            }
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres-accel",
                feature = "turso"
            )))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }
}
