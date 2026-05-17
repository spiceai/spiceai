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
//!     `offsets_json` TEXT,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );

use super::{
    AccelerationConnection, Error, Result, acceleration_connection, offsets::OffsetSchemaState,
};
use crate::{
    component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption,
    dataconnector::debezium::DebeziumKafkaMetadata,
};
use data_components::kafka::KafkaOffset;

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";

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
    schema_ensured: OffsetSchemaState,
}

impl DebeziumKafkaSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
            schema_ensured: OffsetSchemaState::default(),
        })
    }

    pub(crate) async fn get(&self) -> Result<Option<DebeziumKafkaMetadata>> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_duckdb(pool),
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
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
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
            AccelerationConnection::DuckDB(pool) => self.upsert_offsets_duckdb(pool, offsets),
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

    fn schema_needs_ensure(&self) -> bool {
        self.schema_ensured.needs_ensure()
    }

    fn mark_schema_ensured(&self) {
        self.schema_ensured.mark_ensured();
    }
}
