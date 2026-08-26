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

//! The `spice_sys_*` sidecar tables as stored by a `PostgreSQL` accelerator.
//!
//! One module per checkpoint shape, each implementing the matching
//! `runtime-checkpoint-api` trait against a [`PostgresConnectionPool`].
//!
//! [`PostgresSidecar`] binds a pool to a dataset name and hands out those stores; the
//! `PostgreSQL` accelerator returns one from `DataAccelerator::sidecar`, which is how
//! the runtime reaches this engine's sidecar tables without naming the engine.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
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

pub use blob::PostgresBlobCheckpointStore;
pub use dataset_checkpoint::PostgresDatasetCheckpointer;
pub use debezium::PostgresDebeziumCheckpointStore;
pub use kafka::PostgresKafkaCheckpointStore;
pub use mongodb::PostgresMongoCheckpointStore;
pub use mysql_binlog::PostgresMySqlBinlogStore;

/// Wraps an engine-level failure as a store failure.
pub(crate) fn store_error(
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CheckpointError {
    CheckpointError::Store {
        source: source.into(),
    }
}

/// One dataset's sidecar tables inside a `PostgreSQL` accelerator.
pub struct PostgresSidecar {
    pool: Arc<PostgresConnectionPool>,
    dataset_name: String,
}

impl PostgresSidecar {
    #[must_use]
    pub fn new(pool: Arc<PostgresConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }
}

#[async_trait]
impl AcceleratorSidecar for PostgresSidecar {
    fn blob_checkpoint_store(
        &self,
        table_name: &'static str,
    ) -> Result<Arc<dyn BlobCheckpointStore>, CheckpointError> {
        Ok(Arc::new(PostgresBlobCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
            table_name,
        )))
    }

    fn kafka_checkpoint_store(&self) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError> {
        Ok(Arc::new(PostgresKafkaCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn debezium_checkpoint_store(
        &self,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError> {
        Ok(Arc::new(PostgresDebeziumCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn mysql_binlog_store(&self) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        Ok(Arc::new(PostgresMySqlBinlogStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn mongo_checkpoint_store(&self) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        Ok(Arc::new(PostgresMongoCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    async fn dataset_checkpointer(
        &self,
        _snapshot_behavior: SnapshotBehavior,
    ) -> Result<Arc<dyn DatasetCheckpointer>, CheckpointError> {
        // `PostgreSQL` has no snapshot support, so the behavior is not consulted.
        Ok(Arc::new(
            PostgresDatasetCheckpointer::try_new(Arc::clone(&self.pool), self.dataset_name.clone())
                .await?,
        ))
    }

    async fn update_caching_engine_fetched_at(&self) -> Result<(), CheckpointError> {
        Err(unsupported_sidecar("postgres", "caching-engine"))
    }
}
