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

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MongoCheckpointMetadata {
    /// Canonical extended JSON of the most recent `ResumeToken`'s raw BSON.
    pub resume_token_json: String,
    /// Optional cluster operation time (seconds since epoch) used as the
    /// `startAtOperationTime` fallback when the resume token is past the
    /// oplog window.
    #[serde(default)]
    pub cluster_time_ts: Option<i64>,
    /// Optional serialized Arrow schema snapshot for detecting drift between
    /// runs.
    #[serde(default)]
    pub schema_json: Option<String>,
    /// When the row was last updated. Populated by the database layer on read.
    #[serde(default)]
    pub updated_at: Option<std::time::SystemTime>,
}

pub struct MongoSys {
    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(dead_code)
    )]
    pub dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl MongoSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
        })
    }

    pub async fn get(&self) -> Option<MongoCheckpointMetadata> {
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
            _ => None,
        }
    }

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

    pub async fn delete(&self) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.delete_duckdb(pool),
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

    /// Serialize an Arrow schema to a JSON string for `schema_json` storage.
    pub fn serialize_schema(schema: &SchemaRef) -> Result<String> {
        serde_json::to_string(schema).map_err(Error::external)
    }

    /// Deserialize an Arrow schema from a `schema_json` string.
    pub fn deserialize_schema(schema_json: &str) -> Result<SchemaRef> {
        let schema: Schema = serde_json::from_str(schema_json).map_err(Error::external)?;
        Ok(std::sync::Arc::new(schema))
    }
}
