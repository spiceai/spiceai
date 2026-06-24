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

#[cfg(feature = "duckdb")]
use std::sync::Arc;

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::{component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption};
use serde::{Deserialize, Serialize};

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
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DynamoDBSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
        })
    }

    pub async fn get(&self) -> Option<DynamoDBCheckpointMetadata> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                // DuckDB is synchronous; offload to the blocking pool so the
                // async runtime thread isn't stalled.
                let pool = Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                tokio::task::spawn_blocking(move || Self::get_duckdb(&pool, &dataset_name))
                    .await
                    .ok()
                    .flatten()
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
            _ => None,
        }
    }

    pub async fn upsert(&self, metadata: &DynamoDBCheckpointMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                let metadata = metadata.clone();
                tokio::task::spawn_blocking(move || {
                    Self::upsert_duckdb(&pool, &dataset_name, &metadata)
                })
                .await
                .map_err(Error::external)?
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
}
