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

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::{
    component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption,
    dataconnector::debezium::DebeziumKafkaMetadata,
};

const DEBEZIUM_KAFKA_TABLE_NAME: &str = "spice_sys_debezium_kafka";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

pub struct DebeziumKafkaSys {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DebeziumKafkaSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
        })
    }

    pub(crate) async fn get(&self) -> Option<DebeziumKafkaMetadata> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_duckdb(pool),
            #[cfg(feature = "postgres")]
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
                feature = "postgres",
                feature = "turso"
            )))]
            _ => None,
        }
    }

    pub(crate) async fn upsert(&self, metadata: &DebeziumKafkaMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
            #[cfg(feature = "postgres")]
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
                feature = "postgres",
                feature = "turso"
            )))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }
}
