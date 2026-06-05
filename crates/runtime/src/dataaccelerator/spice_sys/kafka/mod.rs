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

//! CREATE TABLE `spice_sys_kafka` (
//!     `dataset_name` TEXT PRIMARY KEY,
//!     `consumer_group_id` TEXT,
//!     `topic` TEXT,
//!     `schema_json` TEXT,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );
//!
//! CREATE TABLE `spice_sys_kafka_offsets` (
//!     `dataset_name` TEXT NOT NULL,
//!     `topic` TEXT NOT NULL,
//!     `partition_id` INTEGER NOT NULL,
//!     `partition_offset` BIGINT NOT NULL,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     PRIMARY KEY (`dataset_name`, `topic`, `partition_id`),
//! );

use datafusion::arrow::datatypes::{Schema, SchemaRef};

use super::{
    AccelerationConnection, Error, Result, acceleration_connection, offsets::OffsetSchemaState,
};
use crate::{component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption};
use data_components::kafka::{KafkaMetadata, KafkaOffset};

const KAFKA_TABLE_NAME: &str = "spice_sys_kafka";
const KAFKA_OFFSETS_TABLE_NAME: &str = "spice_sys_kafka_offsets";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres-accel")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

pub struct KafkaSys {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
    schema_ensured: OffsetSchemaState,
}

impl KafkaSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
            schema_ensured: OffsetSchemaState::default(),
        })
    }

    pub(crate) async fn get(&self) -> Result<Option<KafkaMetadata>> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_duckdb(pool),
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.get_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(pool) => self.get_sqlite(pool).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.get_turso(pool).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(pool) => self.get_sqlite(pool).await,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres-accel",
                feature = "turso"
            )))]
            _ => Ok(None),
        }
    }

    pub(crate) async fn upsert(&self, metadata: &KafkaMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.upsert_postgres(pool, metadata).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(pool) => self.upsert_sqlite(pool, metadata).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.upsert_turso(pool, metadata).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(pool) => self.upsert_sqlite(pool, metadata).await,
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
            AccelerationConnection::SQLite(pool) => self.upsert_offsets_sqlite(pool, offsets).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.upsert_offsets_turso(pool, offsets).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(pool) => {
                self.upsert_offsets_sqlite(pool, offsets).await
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

    fn serialize_schema(schema: &SchemaRef) -> Result<String> {
        serde_json::to_string(schema).map_err(Error::external)
    }

    fn deserialize_schema(schema_json: &str) -> Result<SchemaRef> {
        let schema: Schema = serde_json::from_str(schema_json).map_err(Error::external)?;
        Ok(std::sync::Arc::new(schema))
    }

    fn schema_needs_ensure(&self) -> bool {
        self.schema_ensured.needs_ensure()
    }

    fn mark_schema_ensured(&self) {
        self.schema_ensured.mark_ensured();
    }
}
