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
use serde_json;

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";
const SCHEMA_MIGRATION_01_STMT: &str =
    "ALTER TABLE spice_sys_dataset_checkpoint ADD COLUMN IF NOT EXISTS schema_json TEXT";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[async_trait]
pub trait DatasetCheckpointer: Send + Sync {
    async fn exists(&self) -> bool;
    async fn checkpoint(&self, schema: &SchemaRef) -> Result<()>;
    async fn get_schema(&self) -> Result<Option<SchemaRef>>;
    async fn last_checkpoint_time(&self) -> Result<Option<SystemTime>>;
}

#[async_trait]
impl DatasetCheckpointer for DatasetCheckpoint {
    async fn exists(&self) -> bool {
        self.exists().await
    }

    async fn checkpoint(&self, schema: &SchemaRef) -> Result<()> {
        self.checkpoint(schema).await
    }

    async fn get_schema(&self) -> Result<Option<SchemaRef>> {
        self.get_schema().await
    }

    async fn last_checkpoint_time(&self) -> Result<Option<SystemTime>> {
        self.last_checkpoint_time().await
    }
}

pub struct DatasetCheckpoint {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DatasetCheckpoint {
    pub async fn try_new(
        source: &dyn AccelerationSource,
        open_option: OpenOption,
    ) -> Result<Arc<dyn DatasetCheckpointer>> {
        let acceleration_connection = acceleration_connection(source, open_option).await?;
        Self::init(&acceleration_connection).await?;
        Ok(Arc::new(Self {
            dataset_name: source.name().to_string(),
            acceleration_connection,
        }) as Arc<dyn DatasetCheckpointer>)
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
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
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
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
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
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
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
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
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
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
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
            #[cfg(not(any(feature = "sqlite", feature = "duckdb", feature = "postgres")))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }
}
