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

//! Resolving a dataset to the sidecar stores its accelerator keeps for it.
//!
//! The `spice_sys_*` tables — CDC stream positions, the dataset schema checkpoint, the
//! caching engine's fetch marker — live inside the dataset's own accelerator database.
//! The per-engine SQL for them lives in the `runtime-checkpoint-{duckdb,sqlite,turso,
//! postgres}` crates, behind [`AcceleratorSidecar`].
//!
//! What is left here is only the *resolution*: a dataset names an engine, the registry
//! hands back that engine, and the engine hands back its sidecar. Nothing in this
//! module names a connection pool or a driver, which is what lets the engines move
//! below `runtime`.

use std::sync::Arc;

use runtime_acceleration::{
    dataset_checkpoint::{
        DatasetCheckpointer, DatasetCheckpointerFactory, make_checkpointer_factory,
    },
    sidecar::{AcceleratorSidecar, OpenOption},
    snapshot::SnapshotBehavior,
};
use runtime_checkpoint_api::{BlobCheckpointStore, CheckpointError};

use super::AccelerationSource;
use crate::dataaccelerator::AcceleratorEngineRegistry;

/// Resolves `source`'s sidecar through its configured accelerator engine.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when acceleration is disabled, the engine is not
/// registered (not compiled in), or the engine cannot open its sidecar.
pub async fn sidecar_for(
    source: &dyn AccelerationSource,
    registry: Arc<AcceleratorEngineRegistry>,
    open_option: OpenOption,
) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
    let acceleration = source
        .acceleration()
        .ok_or_else(|| CheckpointError::Store {
            source: "Acceleration is not enabled".into(),
        })?;
    let engine = acceleration.engine;

    let accelerator = registry
        .get_accelerator_engine(engine)
        .await
        .ok_or_else(|| CheckpointError::Store {
            source: format!("{engine} accelerator engine not available").into(),
        })?;

    accelerator
        .sidecar(source, Arc::clone(&registry), open_option)
        .await
}

/// The dataset's sidecar, resolved from the runtime handle it carries.
async fn dataset_sidecar(
    dataset: &crate::component::dataset::Dataset,
    open_option: OpenOption,
) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
    let registry = dataset.runtime.accelerator_engine_registry();
    sidecar_for(dataset, registry, open_option).await
}

/// Construct the per-dataset **blob** checkpoint store backed by the dataset's own
/// accelerator, writing into the sidecar `table_name`.
///
/// Returns `None` when the dataset has no usable accelerator connection (acceleration
/// disabled, or the engine isn't compiled in), so a CDC connector degrades to
/// re-bootstrapping from scratch rather than failing.
pub async fn checkpoint_store(
    dataset: &crate::component::dataset::Dataset,
    table_name: &'static str,
) -> Option<Arc<dyn BlobCheckpointStore>> {
    let sidecar = match dataset_sidecar(dataset, OpenOption::CreateIfNotExists).await {
        Ok(sidecar) => sidecar,
        Err(e) => {
            // Surface *why* checkpointing is unavailable (missing engine feature,
            // missing file, pool-init failure, …) instead of a silent `None`.
            tracing::warn!(
                dataset = %dataset.name,
                error = %e,
                "Could not resolve the dataset's accelerator connection for checkpoint storage; the connector will run without a persisted checkpoint"
            );
            return None;
        }
    };

    match sidecar.blob_checkpoint_store(table_name) {
        Ok(store) => Some(store),
        Err(e) => {
            tracing::warn!(
                dataset = %dataset.name,
                error = %e,
                "The dataset's accelerator does not store connector checkpoints; the connector will run without a persisted checkpoint"
            );
            None
        }
    }
}

/// Where `dataset`'s append refresh keeps the source snapshot it last loaded.
///
/// A file-backed acceleration keeps it in its sidecar, beside the data it describes.
/// One with no sidecar (memory mode, Arrow) keeps it for the process: its data lives no
/// longer, so the next start takes an initial load either way. `checkpoint_store` logs
/// why a sidecar could not be resolved, which a memory-mode dataset must not trigger.
pub async fn append_cursor_store(
    dataset: &crate::component::dataset::Dataset,
) -> crate::accelerated::refresh_task::append_cursor::AppendCursorStore {
    use crate::accelerated::refresh_task::append_cursor::{APPEND_CURSOR_TABLE, AppendCursorStore};
    let persisted = if dataset.is_file_accelerated() {
        checkpoint_store(dataset, APPEND_CURSOR_TABLE)
            .await
            .map(AppendCursorStore::persisted)
    } else {
        None
    };
    persisted.unwrap_or_else(AppendCursorStore::in_memory)
}

/// [`append_cursor_store`] for any acceleration source, through its engine registry:
/// the recreate path, which has no dataset handle. `open_option` says whether a
/// sidecar that does not exist yet is created; a caller that only clears passes
/// [`OpenOption::OpenExisting`] and gets the in-memory store when there is nothing to
/// clear, so a sidecar is never created just to delete from it.
pub async fn append_cursor_store_for(
    source: &dyn AccelerationSource,
    registry: Arc<AcceleratorEngineRegistry>,
    open_option: OpenOption,
) -> crate::accelerated::refresh_task::append_cursor::AppendCursorStore {
    use crate::accelerated::refresh_task::append_cursor::{APPEND_CURSOR_TABLE, AppendCursorStore};
    if !source.is_file_accelerated() {
        return AppendCursorStore::in_memory();
    }
    let store = match sidecar_for(source, registry, open_option).await {
        Ok(sidecar) => sidecar.blob_checkpoint_store(APPEND_CURSOR_TABLE),
        Err(e) => Err(e),
    };
    match store {
        Ok(store) => AppendCursorStore::persisted(store),
        Err(e) => {
            tracing::debug!(
                "Dataset '{}' has no sidecar for its append cursor ({e}); it is kept in memory",
                source.name()
            );
            AppendCursorStore::in_memory()
        }
    }
}

/// Construct the **Kafka** checkpoint store over this dataset's accelerator.
///
/// Unlike [`checkpoint_store`] this reports a resolution failure as an error: the Kafka
/// connector refuses to register a dataset whose offsets it cannot persist, because
/// replaying a topic from the beginning into an append accelerator duplicates rows.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when the sidecar cannot be resolved.
pub async fn kafka_checkpoint_store(
    dataset: &crate::component::dataset::Dataset,
) -> Result<Arc<dyn runtime_checkpoint_api::kafka::KafkaCheckpointStore>, CheckpointError> {
    dataset_sidecar(dataset, OpenOption::CreateIfNotExists)
        .await?
        .kafka_checkpoint_store()
}

/// Construct the **Debezium** checkpoint store over this dataset's accelerator.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when the sidecar cannot be resolved.
pub async fn debezium_checkpoint_store(
    dataset: &crate::component::dataset::Dataset,
) -> Result<Arc<dyn runtime_checkpoint_api::debezium::DebeziumCheckpointStore>, CheckpointError> {
    dataset_sidecar(dataset, OpenOption::CreateIfNotExists)
        .await?
        .debezium_checkpoint_store()
}

/// Construct the **`MySQL` binlog** position store over this dataset's accelerator.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when the sidecar cannot be resolved.
pub async fn mysql_binlog_store(
    dataset: &crate::component::dataset::Dataset,
) -> Result<Arc<dyn runtime_checkpoint_api::mysql_binlog::MySqlBinlogStore>, CheckpointError> {
    dataset_sidecar(dataset, OpenOption::CreateIfNotExists)
        .await?
        .mysql_binlog_store()
}

/// Construct the **`MongoDB`** resume-token store over this dataset's accelerator.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when the sidecar cannot be resolved.
pub async fn mongo_checkpoint_store(
    dataset: &crate::component::dataset::Dataset,
) -> Result<Arc<dyn runtime_checkpoint_api::mongodb::MongoCheckpointStore>, CheckpointError> {
    dataset_sidecar(dataset, OpenOption::CreateIfNotExists)
        .await?
        .mongo_checkpoint_store()
}

/// Records that the caching engine fetched this dataset just now.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when the sidecar cannot be resolved, or when the
/// dataset's engine does not serve cached results.
pub async fn update_caching_engine_fetched_at(
    dataset: &crate::component::dataset::Dataset,
) -> Result<(), CheckpointError> {
    dataset_sidecar(dataset, OpenOption::OpenExisting)
        .await?
        .update_caching_engine_fetched_at()
        .await
}

/// A [`DatasetCheckpointerFactory`] over `source`'s acceleration checkpoint, opened
/// read-only.
///
/// This is what satisfies `AccelerationSource::checkpointer_factory` for every source
/// the runtime owns. It exists so the snapshot bootstrap — which lives below `runtime`
/// — can reconcile a downloaded snapshot against the stored checkpoint without naming
/// an engine.
///
/// A factory rather than the checkpointer itself because opening one touches the
/// accelerator, and the caller decides whether it needs to.
pub(crate) fn checkpointer_factory(
    source: &dyn AccelerationSource,
    registry: Arc<AcceleratorEngineRegistry>,
    snapshot_behavior: SnapshotBehavior,
) -> DatasetCheckpointerFactory {
    let source = source.clone_arc();
    make_checkpointer_factory(move || {
        let source = Arc::clone(&source);
        let registry = Arc::clone(&registry);
        let snapshot_behavior = snapshot_behavior.clone();
        async move {
            dataset_checkpointer(
                source.as_ref(),
                registry,
                OpenOption::OpenExisting,
                snapshot_behavior,
            )
            .await
            .map_err(Into::into)
        }
    })
}

/// The dataset schema/refresh-SQL checkpoint held by `source`'s accelerator.
///
/// # Errors
///
/// Returns [`CheckpointError::Store`] when the sidecar cannot be resolved, or when the
/// engine cannot host a checkpoint.
pub async fn dataset_checkpointer(
    source: &dyn AccelerationSource,
    registry: Arc<AcceleratorEngineRegistry>,
    open_option: OpenOption,
    snapshot_behavior: SnapshotBehavior,
) -> Result<Arc<dyn DatasetCheckpointer>, CheckpointError> {
    sidecar_for(source, registry, open_option)
        .await?
        .dataset_checkpointer(snapshot_behavior)
        .await
}
