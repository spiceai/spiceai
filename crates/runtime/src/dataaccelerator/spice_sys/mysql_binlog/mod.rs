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
//! dataset's change stream resumes from, and an optional schema/layout
//! snapshot for drift detection. This is the client-side replacement for a
//! Postgres replication slot's server-tracked `confirmed_flush_lsn`.
//!
//! `schema_json` stores a versioned checkpoint meta envelope (dataset Arrow
//! schema + source ordinal-layout fingerprint). Legacy rows may still hold a
//! bare Arrow schema JSON object; the replication layer treats those as
//! unknown layout and refuses unsafe resume.
//!
//! `gtid_executed` holds the source's executed GTID set for failover-safe
//! resume (`COM_BINLOG_DUMP_GTID`); it is `NULL` for file+offset positioning.
//! `cursor_type` (`file`|`gtid`) records the checkpoint's positioning type
//! explicitly, so resume never has to *infer* it from whether `gtid_executed`
//! is set — an empty GTID set (`gtid_mode = ON`, zero transactions yet) must
//! still reload as `gtid`, and an engine that maps an empty string to `NULL`
//! must not silently reclassify it as `file`. Both columns were added after the
//! initial schema, so each is created lazily via `ALTER TABLE ... ADD COLUMN`
//! on tables that predate it.
//!
//! ```sql
//! CREATE TABLE spice_sys_mysql_binlog (
//!     dataset_name TEXT PRIMARY KEY,
//!     binlog_file TEXT NOT NULL,
//!     binlog_pos BIGINT NOT NULL,
//!     schema_json TEXT,
//!     gtid_executed TEXT,
//!     cursor_type TEXT,
//!     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//!     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//! );
//! ```

use super::{
    AccelerationConnection, Error, Result, UPSERT_MAX_RETRIES, UPSERT_MAX_RETRY_DELAY,
    acceleration_connection, is_retryable_lock_error,
};
use crate::{component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption};
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

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
    /// Optional executed GTID set (`uuid:range` text) for failover-safe resume
    /// via `COM_BINLOG_DUMP_GTID`. `None` for file+offset positioning; may be an
    /// empty string when `gtid_mode = ON` but no transactions have committed.
    pub gtid_executed: Option<String>,
    /// The checkpoint's positioning type (`file`|`gtid`), stored explicitly so
    /// resume doesn't need to infer it from `gtid_executed`. `Option` only
    /// because the column is nullable; this connector always writes it (the
    /// feature has never shipped, so there are no legacy typeless checkpoints),
    /// and the sidecar loader defensively resolves any `None` read by inferring
    /// the type from `gtid_executed`.
    pub cursor_type: Option<String>,
    /// When the row was last updated. Populated by the database layer on read.
    pub updated_at: Option<std::time::SystemTime>,
}

pub struct MySqlBinlogSys {
    pub dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl MySqlBinlogSys {
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
        expect(
            clippy::unused_async,
            reason = "async only when an accelerator backend is compiled in; with none, every arm errors immediately"
        )
    )]
    pub async fn get(&self) -> Option<MySqlBinlogCheckpoint> {
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
            _ => None,
        }
    }

    /// Persist a checkpoint, retrying briefly on a transient accelerator write
    /// lock.
    ///
    /// The sidecar shares the accelerator's connection pool, so a checkpoint
    /// upsert contends with the accelerator's own CDC-apply transactions for
    /// the single writer lock. A large batch commit can hold that lock past the
    /// engine's `busy_timeout`, surfacing as `SQLITE_BUSY` / "database is
    /// locked" (and equivalents on the other file engines). Without a retry a
    /// single contention drops the whole checkpoint interval and widens the
    /// crash-replay window; a few short retries convert most of these into a
    /// successful persist. Still best-effort — a persistent lock returns the
    /// error and the replication layer retries on the next interval.
    pub async fn upsert(&self, checkpoint: &MySqlBinlogCheckpoint) -> Result<()> {
        let backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(UPSERT_MAX_RETRIES))
            .max_duration(Some(UPSERT_MAX_RETRY_DELAY))
            .build();

        retry(backoff, || async {
            self.upsert_once(checkpoint).await.map_err(|e| {
                if is_retryable_lock_error(&e) {
                    tracing::debug!(
                        dataset = %self.dataset_name,
                        error = %e,
                        "binlog checkpoint upsert hit a transient accelerator write lock"
                    );
                    RetryError::transient(e)
                } else {
                    RetryError::permanent(e)
                }
            })
        })
        .await
    }

    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(
            clippy::unused_async,
            reason = "async only when an accelerator backend is compiled in; with none, every arm errors immediately"
        )
    )]
    async fn upsert_once(
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
            AccelerationConnection::DuckDB(pool) => {
                let pool = std::sync::Arc::clone(pool);
                let dataset_name = self.dataset_name.clone();
                let checkpoint = checkpoint.clone();
                super::spawn_duckdb_blocking(move || {
                    Self::upsert_duckdb(&dataset_name, &pool, &checkpoint)
                })
                .await
            }
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

    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "duckdb",
            feature = "postgres-accel",
            feature = "turso"
        )),
        expect(
            clippy::unused_async,
            reason = "async only when an accelerator backend is compiled in; with none, every arm errors immediately"
        )
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
