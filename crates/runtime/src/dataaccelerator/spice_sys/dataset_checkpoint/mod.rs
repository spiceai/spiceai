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

//! CREATE TABLE `spice_sys_dataset_checkpoint` (
//!     `dataset_name` TEXT PRIMARY KEY,
//!     `schema_json` TEXT,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );

use std::{sync::Arc, time::SystemTime};

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::dataaccelerator::{AccelerationSource, spice_sys::OpenOption};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use runtime_acceleration::{dataset_checkpoint::DatasetCheckpointer, snapshot::SnapshotBehavior};
use serde_json;
use snafu::ResultExt;

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";
const SCHEMA_MIGRATION_01_STMT: &str =
    "ALTER TABLE spice_sys_dataset_checkpoint ADD COLUMN IF NOT EXISTS schema_json TEXT";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

#[async_trait]
impl DatasetCheckpointer for DatasetCheckpoint {
    async fn exists(&self) -> bool {
        self.exists().await
    }

    async fn checkpoint(
        &self,
        schema: &SchemaRef,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.checkpoint(schema).await.boxed()
    }

    async fn get_schema(
        &self,
    ) -> Result<Option<SchemaRef>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_schema().await.boxed()
    }

    async fn last_checkpoint_time(
        &self,
    ) -> Result<Option<SystemTime>, Box<dyn std::error::Error + Send + Sync>> {
        self.last_checkpoint_time().await.boxed()
    }
}

pub struct DatasetCheckpoint {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
    snapshot_behavior: SnapshotBehavior,
}

impl DatasetCheckpoint {
    pub async fn try_new(source: &dyn AccelerationSource, open_option: OpenOption) -> Result<Self> {
        let acceleration_connection = acceleration_connection(source, open_option).await?;
        Self::init(&acceleration_connection).await?;
        Ok(Self {
            dataset_name: source.name().to_string(),
            acceleration_connection,
            snapshot_behavior: SnapshotBehavior::Disabled,
        })
    }

    #[must_use]
    pub fn with_snapshot_behavior(self, snapshot_behavior: SnapshotBehavior) -> Self {
        Self {
            dataset_name: self.dataset_name,
            acceleration_connection: self.acceleration_connection,
            snapshot_behavior,
        }
    }

    #[must_use]
    pub fn to_arc(self) -> Arc<dyn DatasetCheckpointer> {
        Arc::new(self)
    }

    async fn init(connection: &AccelerationConnection) -> Result<()> {
        // First create the initial table
        match connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => Self::init_duckdb(pool)?,
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => Self::init_postgres(pool).await?,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => Self::init_sqlite(conn).await?,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => Self::init_turso(pool).await?,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => Self::init_sqlite(conn).await?,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres",
                feature = "turso"
            )))]
            _ => return Err(Error::NoAccelerationConnection),
        }

        // Then add the schema column if it doesn't exist
        match connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => Self::migrate_duckdb(pool)?,
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => Self::migrate_postgres(pool).await?,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => Self::migrate_sqlite(conn).await?,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => Self::migrate_turso(pool).await?,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => Self::migrate_sqlite(conn).await?,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres",
                feature = "turso"
            )))]
            _ => return Err(Error::NoAccelerationConnection),
        }

        Ok(())
    }

    fn serialize_schema(schema: &SchemaRef) -> Result<String> {
        serde_json::to_string(schema).map_err(Error::external)
    }

    fn deserialize_schema(schema_json: &str) -> Result<SchemaRef> {
        let schema: Schema = serde_json::from_str(schema_json).map_err(Error::external)?;
        Ok(std::sync::Arc::new(schema))
    }

    pub async fn exists(&self) -> bool {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.exists_duckdb(pool).ok().unwrap_or(false),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => {
                self.exists_postgres(pool).await.ok().unwrap_or(false)
            }
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => {
                self.exists_sqlite(conn).await.ok().unwrap_or(false)
            }
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => {
                self.exists_turso(pool).await.ok().unwrap_or(false)
            }
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => {
                self.exists_sqlite(conn).await.ok().unwrap_or(false)
            }
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres",
                feature = "turso"
            )))]
            _ => false,
        }
    }

    pub async fn last_checkpoint_time(&self) -> Result<Option<SystemTime>> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.last_checkpoint_time_duckdb(pool),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => {
                self.last_checkpoint_time_postgres(pool).await
            }
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.last_checkpoint_time_sqlite(conn).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.last_checkpoint_time_turso(pool).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.last_checkpoint_time_sqlite(conn).await,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres",
                feature = "turso"
            )))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }

    pub async fn checkpoint(&self, schema: &SchemaRef) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.checkpoint_duckdb(pool, schema),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => self.checkpoint_postgres(pool, schema).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.checkpoint_sqlite(conn, schema).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.checkpoint_turso(pool, schema).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.checkpoint_sqlite(conn, schema).await,
            #[cfg(not(any(
                feature = "sqlite",
                feature = "duckdb",
                feature = "postgres",
                feature = "turso"
            )))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }

    pub async fn get_schema(&self) -> Result<Option<SchemaRef>> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_schema_duckdb(pool),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => self.get_schema_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.get_schema_sqlite(conn).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.get_schema_turso(pool).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.get_schema_sqlite(conn).await,
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
