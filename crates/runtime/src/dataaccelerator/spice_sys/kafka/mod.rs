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

use datafusion::arrow::datatypes::{Schema, SchemaRef};

use super::{AccelerationConnection, Result, acceleration_connection};
use crate::{component::dataset::Dataset, dataconnector::kafka::KafkaMetadata};

const KAFKA_TABLE_NAME: &str = "spice_sys_kafka";

#[cfg(feature = "duckdb")]
mod duckdb;

pub struct KafkaSys {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl KafkaSys {
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

    pub(crate) fn get(&self) -> Option<KafkaMetadata> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_duckdb(pool),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(_) => {
                tracing::warn!(
                    "Persisting Kafka metadata in Postgres for state retention across restarts is not currently supported"
                );
                None
            }
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(_) => {
                tracing::warn!(
                    "Persisting Kafka metadata in SQLite for state retention across restarts is not currently supported"
                );
                None
            }
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
            _ => None,
        }
    }

    pub(crate) fn upsert(&self, metadata: &KafkaMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(_) => {
                tracing::warn!(
                    "Persisting Kafka metadata in Postgres for state retention across restarts is not currently supported"
                );
                Ok(())
            }
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(_) => {
                tracing::warn!(
                    "Persisting Kafka metadata in SQLite for state retention across restarts is not currently supported"
                );
                Ok(())
            }
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
            _ => Err("No acceleration connection available".into()),
        }
    }

    fn serialize_schema(schema: &SchemaRef) -> Result<String> {
        Ok(serde_json::to_string(schema).map_err(Box::new)?)
    }

    fn deserialize_schema(schema_json: &str) -> Result<SchemaRef> {
        let schema: Schema = serde_json::from_str(schema_json).map_err(Box::new)?;
        Ok(std::sync::Arc::new(schema))
    }
}
