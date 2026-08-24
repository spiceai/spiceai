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

//! The `spice_sys_*` sidecar tables as stored by a `SQLite` accelerator.
//!
//! One module per checkpoint shape, each implementing the matching
//! `runtime-checkpoint-api` trait against a [`SqliteConnectionPool`]. The Cayenne
//! metastore is also a `SQLite` connection, so it reuses every store here.
//!
//! [`SqliteSidecar`] binds a pool to a dataset name and hands out those stores; the
//! `SQLite` accelerator returns one from `DataAccelerator::sidecar`, which is how the
//! runtime reaches this engine's sidecar tables without naming the engine.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool;
use runtime_acceleration::{
    dataset_checkpoint::DatasetCheckpointer,
    sidecar::{AcceleratorSidecar, unsupported_sidecar},
    snapshot::SnapshotBehavior,
};
use runtime_checkpoint_api::{
    BlobCheckpointStore, CheckpointError, debezium::DebeziumCheckpointStore,
    kafka::KafkaCheckpointStore, mongodb::MongoCheckpointStore, mysql_binlog::MySqlBinlogStore,
};

mod blob;
mod dataset_checkpoint;
mod debezium;
mod kafka;
mod mongodb;
mod mysql_binlog;
#[cfg(test)]
mod test_support;

pub use blob::SqliteBlobCheckpointStore;
pub use dataset_checkpoint::SqliteDatasetCheckpointer;
pub use debezium::SqliteDebeziumCheckpointStore;
pub use kafka::SqliteKafkaCheckpointStore;
pub use mongodb::SqliteMongoCheckpointStore;
pub use mysql_binlog::SqliteMySqlBinlogStore;

/// Reports that the pool handed back a connection that is not a `SQLite` one.
///
/// Structurally unreachable — a `SqliteConnectionPool` only ever yields a
/// `SqliteConnection` — but the pool's `connect_sync` is typed as the generic
/// connection, so the downcast has to be answered.
pub(crate) fn downcast_failed() -> CheckpointError {
    CheckpointError::Store {
        source: "expected a SqliteConnection from the sqlite pool".into(),
    }
}

/// Wraps an engine-level failure as a store failure.
pub(crate) fn store_error(
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CheckpointError {
    CheckpointError::Store {
        source: source.into(),
    }
}

/// One dataset's sidecar tables inside a `SQLite` accelerator.
pub struct SqliteSidecar {
    pool: Arc<SqliteConnectionPool>,
    dataset_name: String,
}

impl SqliteSidecar {
    #[must_use]
    pub fn new(pool: Arc<SqliteConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }
}

#[async_trait]
impl AcceleratorSidecar for SqliteSidecar {
    fn blob_checkpoint_store(
        &self,
        table_name: &'static str,
    ) -> Result<Arc<dyn BlobCheckpointStore>, CheckpointError> {
        Ok(Arc::new(SqliteBlobCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
            table_name,
        )))
    }

    fn kafka_checkpoint_store(&self) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError> {
        Ok(Arc::new(SqliteKafkaCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn debezium_checkpoint_store(
        &self,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError> {
        Ok(Arc::new(SqliteDebeziumCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn mysql_binlog_store(&self) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        Ok(Arc::new(SqliteMySqlBinlogStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn mongo_checkpoint_store(&self) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        Ok(Arc::new(SqliteMongoCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    async fn dataset_checkpointer(
        &self,
        _snapshot_behavior: SnapshotBehavior,
    ) -> Result<Arc<dyn DatasetCheckpointer>, CheckpointError> {
        // `SQLite` has no snapshot support, so the behavior is not consulted.
        Ok(Arc::new(
            SqliteDatasetCheckpointer::try_new(Arc::clone(&self.pool), self.dataset_name.clone())
                .await?,
        ))
    }

    async fn update_caching_engine_fetched_at(&self) -> Result<(), CheckpointError> {
        Err(unsupported_sidecar("sqlite", "caching-engine"))
    }
}
