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

//! Durable sidecar storage for `MySQL` binlog replication positions.
//!
//! Mirrors `MongoSys` and `DynamoDBSys`: one row per dataset in
//! `spice_sys_mysql_binlog`, holding the binlog file + offset that the
//! dataset's change stream resumes from, and an optional Arrow schema
//! snapshot for drift detection. This is the client-side replacement for a
//! Postgres replication slot's server-tracked `confirmed_flush_lsn`.
//!
//! ```sql
//! CREATE TABLE spice_sys_mysql_binlog (
//!     dataset_name TEXT PRIMARY KEY,
//!     binlog_file TEXT NOT NULL,
//!     binlog_pos BIGINT NOT NULL,
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
const MYSQL_BINLOG_TABLE_NAME: &str = "spice_sys_mysql_binlog";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres-accel")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

#[derive(Clone, Debug, Default)]
pub struct MySqlBinlogCheckpoint {
    /// Binlog file name to resume from, e.g. `binlog.000042`.
    pub binlog_file: String,
    /// Byte offset of the next event to read within `binlog_file`.
    pub binlog_pos: u64,
    /// Optional serialized Arrow schema snapshot for detecting drift between
    /// runs.
    pub schema_json: Option<String>,
    /// When the row was last updated. Populated by the database layer on read.
    pub updated_at: Option<std::time::SystemTime>,
}

pub struct MySqlBinlogSys {
    pub dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl MySqlBinlogSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
        })
    }

    pub async fn get(&self) -> Option<MySqlBinlogCheckpoint> {
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
        checkpoint: &MySqlBinlogCheckpoint,
    ) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, checkpoint),
            #[cfg(feature = "postgres-accel")]
            AccelerationConnection::Postgres(pool) => self.upsert_postgres(pool, checkpoint).await,
            #[cfg(feature = "sqlite")]
            AccelerationConnection::SQLite(conn) => self.upsert_sqlite(conn, checkpoint).await,
            #[cfg(feature = "turso")]
            AccelerationConnection::Turso(pool) => self.upsert_turso(pool, checkpoint).await,
            #[cfg(all(not(windows), feature = "sqlite"))]
            AccelerationConnection::Cayenne(conn) => self.upsert_sqlite(conn, checkpoint).await,
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
    pub fn serialize_schema(schema: &datafusion::arrow::datatypes::SchemaRef) -> Result<String> {
        serde_json::to_string(schema).map_err(Error::external)
    }

    /// Convert a stored position (`BIGINT`) back to the u64 the replication
    /// layer speaks. Negative stored values (impossible via [`Self::upsert`])
    /// clamp to 0.
    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(dead_code)
    )]
    fn position_from_i64(pos: i64) -> u64 {
        u64::try_from(pos).unwrap_or(0)
    }

    /// Convert the replication layer's u64 offset into the `BIGINT` the
    /// sidecar stores. Positions beyond `i64::MAX` cannot occur (binlog files
    /// cap at 1 GiB), but clamp defensively rather than wrap.
    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(dead_code)
    )]
    fn position_to_i64(pos: u64) -> i64 {
        i64::try_from(pos).unwrap_or(i64::MAX)
    }
}
