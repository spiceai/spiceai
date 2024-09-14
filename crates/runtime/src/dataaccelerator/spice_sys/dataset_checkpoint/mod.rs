/*
Copyright 2024 The Spice.ai OSS Authors

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
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );

use super::{acceleration_connection, AccelerationConnection, Result};
use crate::component::dataset::Dataset;

const CHECKPOINT_TABLE_NAME: &str = "spice_sys_dataset_checkpoint";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

pub struct DatasetCheckpoint {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DatasetCheckpoint {
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
        }
    }

    pub async fn checkpoint(&self) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.checkpoint_duckdb(pool),
            #[cfg(feature = "postgres")]
            AccelerationConnection::Postgres(pool) => self.checkpoint_postgres(pool).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.checkpoint_sqlite(conn).await,
        }
    }
}
