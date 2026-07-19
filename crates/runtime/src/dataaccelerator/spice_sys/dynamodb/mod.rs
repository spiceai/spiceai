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

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::{component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption};
use async_trait::async_trait;
use runtime_checkpoint_api::{CheckpointError, CheckpointRecord, CheckpointStore};
use serde::{Deserialize, Serialize};

#[cfg_attr(
    not(any(
        feature = "duckdb",
        feature = "sqlite",
        feature = "postgres-accel",
        feature = "turso"
    )),
    expect(dead_code, reason = "only referenced by the accelerator backend modules")
)]
const DYNAMODB_STREAMS_TABLE_NAME: &str = "spice_sys_dynamodb_streams";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres-accel")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DynamoDBCheckpointMetadata {
    pub checkpoint_data: String,
    /// When the checkpoint was last updated. Used to determine if shards may have expired.
    #[serde(default)]
    pub updated_at: Option<std::time::SystemTime>,
}

pub struct DynamoDBSys {
    #[cfg_attr(
        not(any(
            feature = "duckdb",
            feature = "sqlite",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(dead_code, reason = "only read by the accelerator backend modules")
    )]
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DynamoDBSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        let registry = dataset.runtime.accelerator_engine_registry();
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, registry, open_option)
                .await?,
        })
    }
}

#[async_trait]
impl CheckpointStore for DynamoDBSys {
    async fn get(&self) -> Option<CheckpointRecord> {
        // Annotate the type explicitly: with no accelerator backend feature
        // compiled in, only the `_ => None` arm survives and the type can't be
        // inferred from the arms alone.
        let metadata: Option<DynamoDBCheckpointMetadata> = match &self.acceleration_connection {
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
            _ => None,
        };
        metadata.map(|m| CheckpointRecord {
            data: m.checkpoint_data,
            updated_at: m.updated_at,
        })
    }

    async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
        // Consumed only by the accelerator-backed arms below; with no backend
        // feature compiled in, the `_ => Err` arm leaves it unused.
        #[cfg_attr(
            not(any(
                feature = "duckdb",
                feature = "sqlite",
                feature = "postgres-accel",
                feature = "turso"
            )),
            expect(unused_variables, reason = "no accelerator backend compiled in")
        )]
        let metadata = DynamoDBCheckpointMetadata {
            checkpoint_data: data.to_string(),
            updated_at: None,
        };
        let result = match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, &metadata),
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.upsert_postgres(pool, &metadata).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.upsert_sqlite(conn, &metadata).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.upsert_turso(pool, &metadata).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.upsert_sqlite(conn, &metadata).await,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres-accel",
                feature = "turso"
            )))]
            _ => Err(Error::NoAccelerationConnection),
        };
        result.map_err(|e| CheckpointError::Store {
            source: Box::new(e),
        })
    }
}
