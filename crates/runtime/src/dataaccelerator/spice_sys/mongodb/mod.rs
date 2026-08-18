/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Durable sidecar storage for `MongoDB` Change Stream resume tokens.
//!
//! Mirrors `DynamoDBSys` and `KafkaSys`: one row per dataset in
//! `spice_sys_mongodb`, holding the most recent resume token (canonical
//! extended JSON of the raw BSON `ResumeToken`), an optional cluster time for
//! `startAtOperationTime` fallback, and an optional Arrow schema snapshot for
//! drift detection.
//!
//! ```sql
//! CREATE TABLE spice_sys_mongodb (
//!     dataset_name TEXT PRIMARY KEY,
//!     resume_token_json TEXT NOT NULL,
//!     cluster_time_ts INTEGER,
//!     schema_json TEXT,
//!     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//!     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//! );
//! ```

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::{component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption};

#[cfg_attr(
    not(any(
        feature = "sqlite",
        feature = "duckdb",
        feature = "postgres-accel",
        feature = "turso"
    )),
    expect(dead_code)
)]
const MONGODB_TABLE_NAME: &str = "spice_sys_mongodb";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres-accel")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

// The checkpoint's shape is the connector-facing contract, so it lives in
// `runtime-checkpoint-api` below both sides. The engine modules here name it through
// this path.
pub use runtime_checkpoint_api::mongodb::MongoCheckpointMetadata;

pub struct MongoSys {
    pub dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl MongoSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        let registry = dataset.runtime.accelerator_engine_registry();
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, registry, open_option)
                .await?,
        })
    }

    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(clippy::unused_async)
    )]
    pub async fn get(&self) -> Option<MongoCheckpointMetadata> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = std::sync::Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                super::spawn_duckdb_blocking_opt(move || Self::get_duckdb(&dataset_name, &pool))
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
            _ => {
                // Referenced so the field is never dead code when no accelerator backend is
                // compiled in (backends read it to key the sidecar row by dataset).
                let _ = &self.dataset_name;
                None
            }
        }
    }

    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(clippy::unused_async)
    )]
    pub async fn upsert(
        &self,
        #[cfg_attr(
            not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres-accel",
                feature = "turso"
            )),
            expect(unused_variables)
        )]
        metadata: &MongoCheckpointMetadata,
    ) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = std::sync::Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                let metadata = metadata.clone();
                super::spawn_duckdb_blocking(move || {
                    Self::upsert_duckdb(&dataset_name, &pool, &metadata)
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

    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(clippy::unused_async)
    )]
    pub async fn delete(&self) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => {
                let pool = std::sync::Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                super::spawn_duckdb_blocking(move || Self::delete_duckdb(&dataset_name, &pool))
                    .await
            }
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.delete_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.delete_sqlite(conn).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.delete_turso(pool).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.delete_sqlite(conn).await,
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

#[async_trait::async_trait]
impl runtime_checkpoint_api::mongodb::MongoCheckpointStore for MongoSys {
    async fn get(&self) -> Option<MongoCheckpointMetadata> {
        MongoSys::get(self).await
    }

    async fn upsert(
        &self,
        metadata: &MongoCheckpointMetadata,
    ) -> std::result::Result<(), runtime_checkpoint_api::CheckpointError> {
        MongoSys::upsert(self, metadata).await.map_err(Into::into)
    }

    async fn delete(&self) -> std::result::Result<(), runtime_checkpoint_api::CheckpointError> {
        MongoSys::delete(self).await.map_err(Into::into)
    }
}
