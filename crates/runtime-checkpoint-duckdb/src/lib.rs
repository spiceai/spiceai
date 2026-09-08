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

//! The `spice_sys_*` sidecar tables as stored by a `DuckDB` accelerator.
//!
//! One module per checkpoint shape, each implementing the matching
//! `runtime-checkpoint-api` trait against a [`DuckDbConnectionPool`].
//!
//! [`DuckDbSidecar`] binds a pool to a dataset name and hands out those stores; the
//! `DuckDB` accelerator returns one from `DataAccelerator::sidecar`, which is how the
//! runtime reaches this engine's sidecar tables without naming the engine.
//!
//! # Everything here runs on the blocking pool
//!
//! The `DuckDB` pool is synchronous, and every sidecar helper takes the pool's write
//! gate. A `duckdb_on_full_refresh: replace_file` refresh holds that gate
//! *exclusively* while it copies every co-resident table into a staging file and
//! checkpoints it — a window that scales with the size of the *other* datasets
//! sharing the file. Waiting on it from an async worker would park that worker,
//! including `/health`, which Kubernetes reads to decide the pod is dead. So each
//! trait method below is an async wrapper over a synchronous body dispatched through
//! [`spawn_checkpoint_blocking`].

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use runtime_acceleration::{
    dataset_checkpoint::DatasetCheckpointer, sidecar::AcceleratorSidecar,
    snapshot::SnapshotBehavior,
};
use runtime_checkpoint_api::{
    BlobCheckpointStore, CheckpointError, debezium::DebeziumCheckpointStore,
    kafka::KafkaCheckpointStore, mongodb::MongoCheckpointStore, mysql_binlog::MySqlBinlogStore,
};

mod blob;
mod caching_engine;
mod dataset_checkpoint;
mod debezium;
mod kafka;
mod mongodb;
mod mysql_binlog;
#[cfg(test)]
mod test_support;

pub use blob::DuckDbBlobCheckpointStore;
pub use caching_engine::DuckDbCachingEngine;
pub use dataset_checkpoint::DuckDbDatasetCheckpointer;
pub use debezium::DuckDbDebeziumCheckpointStore;
pub use kafka::DuckDbKafkaCheckpointStore;
pub use mongodb::DuckDbMongoCheckpointStore;
pub use mysql_binlog::DuckDbMySqlBinlogStore;

/// Wraps an engine-level failure as a store failure.
pub(crate) fn store_error(
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CheckpointError {
    CheckpointError::Store {
        source: source.into(),
    }
}

/// Runs a synchronous `DuckDB` sidecar operation on the blocking pool.
pub(crate) async fn spawn_checkpoint_blocking<T, F>(f: F) -> Result<T, CheckpointError>
where
    F: FnOnce() -> Result<T, CheckpointError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(store_error)?
}

/// [`spawn_checkpoint_blocking`] for the read paths that report a failure as `None`
/// rather than as an error.
///
/// A panic is re-raised on this task rather than reported as `None`: these reads
/// answer "where did this dataset get to", and a `None` the caller believes means the
/// dataset re-bootstraps from the beginning of the change stream. Running the read on
/// the blocking pool must not turn a bug into that answer.
pub(crate) async fn spawn_checkpoint_blocking_opt<T, F>(f: F) -> Option<T>
where
    F: FnOnce() -> Option<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(value) => value,
        Err(join_error) if join_error.is_panic() => {
            std::panic::resume_unwind(join_error.into_panic())
        }
        // Cancellation, i.e. the runtime is shutting down under the read. Below the
        // default level, but not silent: it must not pass unremarked for "no
        // checkpoint".
        Err(join_error) => {
            tracing::debug!(
                "Did not read the sidecar checkpoint: the runtime is shutting down ({join_error})"
            );
            None
        }
    }
}

/// One dataset's sidecar tables inside a `DuckDB` accelerator.
pub struct DuckDbSidecar {
    pool: Arc<DuckDbConnectionPool>,
    dataset_name: String,
}

impl DuckDbSidecar {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>, dataset_name: String) -> Self {
        Self { pool, dataset_name }
    }
}

#[async_trait]
impl AcceleratorSidecar for DuckDbSidecar {
    fn blob_checkpoint_store(
        &self,
        table_name: &'static str,
    ) -> Result<Arc<dyn BlobCheckpointStore>, CheckpointError> {
        Ok(Arc::new(DuckDbBlobCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
            table_name,
        )))
    }

    fn kafka_checkpoint_store(&self) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError> {
        Ok(Arc::new(DuckDbKafkaCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn debezium_checkpoint_store(
        &self,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError> {
        Ok(Arc::new(DuckDbDebeziumCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn mysql_binlog_store(&self) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        Ok(Arc::new(DuckDbMySqlBinlogStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    fn mongo_checkpoint_store(&self) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        Ok(Arc::new(DuckDbMongoCheckpointStore::new(
            Arc::clone(&self.pool),
            self.dataset_name.clone(),
        )))
    }

    async fn dataset_checkpointer(
        &self,
        snapshot_behavior: SnapshotBehavior,
    ) -> Result<Arc<dyn DatasetCheckpointer>, CheckpointError> {
        Ok(Arc::new(
            DuckDbDatasetCheckpointer::try_new(
                Arc::clone(&self.pool),
                self.dataset_name.clone(),
                snapshot_behavior,
            )
            .await?,
        ))
    }

    async fn update_caching_engine_fetched_at(&self) -> Result<(), CheckpointError> {
        DuckDbCachingEngine::new(Arc::clone(&self.pool), self.dataset_name.clone())
            .update_fetched_at()
            .await
    }
}
