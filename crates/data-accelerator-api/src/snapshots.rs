/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Snapshot bootstrap for a file-backed acceleration: download one before the engine
//! creates its table, and create one before an engine recreates it.
//!
//! Lives beside the accelerator contract rather than in `runtime` because the engines
//! that call it do — they are linked by the binary, not by the orchestrator. The one
//! thing it needs from the runtime, the acceleration checkpoint to reconcile a
//! downloaded snapshot against, arrives through
//! [`AccelerationSource::checkpointer_factory`].

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use runtime_acceleration::BootstrapStatus;
use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
use runtime_acceleration::acceleration_source::AccelerationSource;
use runtime_acceleration::snapshot::engine::SnapshotEngine;
use runtime_acceleration::snapshot::{
    AccelerationEngine, AccelerationLayout, ForceCreate, SnapshotBehavior, SnapshotManager, metrics,
};
use snafu::Snafu;

use crate::{AcceleratorEngineRegistry, acceleration_file_path};

/// Whether `mode: file_create` still permits bootstrapping from a snapshot.
///
/// `file_create` snapshots the outgoing acceleration and deletes it so the next refresh
/// rebuilds from the source. Bootstrapping straight back from that snapshot would undo
/// the delete, so a refresh that replays everything must not bootstrap. A refresh that
/// does *not* replay from the beginning still needs it, or rows nothing can re-send are
/// gone.
///
/// `refresh_mode` arrives resolved: resolving it consults the connector, which this
/// crate cannot reach. Erring toward keeping the bootstrap costs `file_create` some of
/// its effect on a CDC dataset; erring the other way destroys rows nothing can re-send.
fn mode_allows_snapshot_bootstrap(acceleration: &Acceleration, refresh_mode: RefreshMode) -> bool {
    if acceleration.mode != Mode::FileCreate {
        return true;
    }

    !matches!(refresh_mode, RefreshMode::Full | RefreshMode::Caching)
}

pub async fn download_snapshot_if_needed(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
    refresh_mode: RefreshMode,
) -> BootstrapStatus {
    if !acceleration.snapshot_behavior.bootstrap_enabled() {
        return BootstrapStatus::none();
    }

    if !mode_allows_snapshot_bootstrap(acceleration, refresh_mode) {
        tracing::info!(
            "Acceleration mode is 'file_create' for dataset {}, skipping snapshot bootstrap so the next refresh rebuilds the acceleration from the source",
            source.name()
        );
        return BootstrapStatus::none();
    }

    let Some(primary_path) = layout.primary_path().cloned() else {
        tracing::debug!("No primary path for acceleration layout, skipping download");
        return BootstrapStatus::none();
    };

    if primary_path.exists() {
        tracing::info!(
            "Acceleration already exists at {}, skipping snapshot download",
            primary_path.display()
        );
        return BootstrapStatus::none();
    }

    let dataset_name = source.name().to_string();
    // The source opens its own checkpoint: the concrete `DatasetCheckpoint` carries
    // per-engine sidecar SQL and stays in `runtime`, so it reaches here as a factory
    // behind the `AccelerationSource` contract rather than as a type this crate names.
    let checkpoint_factory = source.checkpointer_factory(acceleration.snapshot_behavior.clone());
    if let Some(manager) = SnapshotManager::try_new(
        dataset_name.clone(),
        acceleration.snapshot_behavior.clone(),
        layout,
        engine,
    )
    .await
    {
        let mut manager = manager.with_checkpointer_factory(checkpoint_factory);
        if let Some(engine_override) = engine_override {
            manager = manager.with_snapshot_engine(engine_override);
        }
        let start_time = Instant::now();
        match manager.download_latest_snapshot().await {
            Ok(Some(info)) => {
                let duration_ms = start_time.elapsed().as_secs_f64() * 1000.0;
                metrics::record_bootstrap_metrics(
                    &dataset_name,
                    duration_ms,
                    info.bytes_downloaded,
                    &info.checksum,
                );
                BootstrapStatus::bootstrapped(info)
            }
            Ok(None) => BootstrapStatus::none(),
            Err(e) => {
                tracing::error!(dataset = %dataset_name, error = %e, "Failed to download snapshot");
                BootstrapStatus::none()
            }
        }
    } else {
        BootstrapStatus::none()
    }
}

/// Creates a snapshot of the existing acceleration file before it is deleted or recreated.
///
/// Called during `file_create` and `file_update` (on schema mismatch) modes to preserve
/// a copy of the current acceleration data before it is destroyed.
///
/// This is a best-effort operation: if snapshotting fails, a warning is logged and the
/// caller proceeds with recreation.
///
/// `engine_override` parallels [`download_snapshot_if_needed`].
pub async fn snapshot_before_recreate(
    acceleration: &Acceleration,
    dataset_name: &str,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    schema: Arc<arrow_schema::Schema>,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
    refresh_mode: RefreshMode,
) {
    if !acceleration.snapshot_behavior.create_enabled() {
        return;
    }

    // `refresh_mode: snapshot` is a read-only consumer of the snapshot store, so it must
    // never publish. Its local acceleration is a copy of a snapshot someone else owns,
    // and creating one makes the uploaded bytes the store's `current-snapshot-id`: a
    // replica lagging behind would publish its stale copy under a higher id and roll
    // every other reader back onto it.
    if refresh_mode == RefreshMode::Snapshot {
        tracing::debug!(
            dataset = %dataset_name,
            "refresh_mode: snapshot consumes snapshots without publishing them; skipping pre-recreation snapshot"
        );
        return;
    }

    // A Cayenne bootstrap needs the per-dataset metastore slice that only
    // `CayenneSnapshotEngine` writes, and creating a snapshot makes whatever it uploads
    // the store's `current-snapshot-id`. Publishing a default-engine archive (a raw
    // `cayenne.db`, no slice) would replace a restorable current snapshot with one
    // nothing can load, which is worse than keeping no backup of this wipe. The caller
    // still recreates the acceleration either way.
    if engine == AccelerationEngine::Cayenne && engine_override.is_none() {
        tracing::warn!(
            dataset = %dataset_name,
            "Skipping the pre-recreation snapshot: this dataset's Cayenne metastore catalog is unavailable, and an archive without its metastore slice could not be restored"
        );
        return;
    }

    let Some(manager) = SnapshotManager::try_new(
        dataset_name.to_string(),
        acceleration.snapshot_behavior.clone(),
        layout,
        engine,
    )
    .await
    else {
        return;
    };
    let manager = if let Some(engine_override) = engine_override {
        manager.with_snapshot_engine(engine_override)
    } else {
        manager
    };

    // If the caller provided an empty schema (e.g. during file_create init when the table
    // provider isn't available yet), try to read the real schema from existing snapshot
    // metadata. If no stored schema exists either, skip the snapshot to avoid storing an
    // empty schema that would make this snapshot unrestorable.
    let snapshot_schema = if schema.fields().is_empty() {
        let Some(stored) = manager.current_stored_schema().await else {
            tracing::debug!(dataset = %dataset_name, "No stored schema available for pre-recreation snapshot; skipping");
            return;
        };
        stored
    } else {
        Arc::clone(&schema)
    };

    // Create a mutex just for this one-off snapshot; no other operations are concurrent at init time.
    let mutex = Arc::new(tokio::sync::Mutex::new(()));
    let lock_guard = mutex.lock_owned().await;

    match manager
        .create_snapshot(&snapshot_schema, lock_guard, None, None, ForceCreate(true))
        .await
    {
        Ok(Some(path)) => {
            tracing::info!(dataset = %dataset_name, snapshot = %path, "Created pre-recreation snapshot");
        }
        Ok(None) => {
            tracing::debug!(dataset = %dataset_name, "No snapshot created before recreation");
        }
        Err(e) => {
            tracing::warn!(dataset = %dataset_name, error = %e, "Failed to create pre-recreation snapshot; proceeding with recreation");
        }
    }
}

/// Rejects a configuration in which two datasets snapshot to the same path.
///
/// # Errors
///
/// Returns [`SharedAccelerationSnapshotError`] naming the datasets that collide.
pub async fn validate_snapshot_paths(
    sources: Vec<Arc<dyn AccelerationSource>>,
    registry: &AcceleratorEngineRegistry,
) -> Result<(), SharedAccelerationSnapshotError> {
    let mut paths: HashMap<PathBuf, Vec<String>> = HashMap::new();

    for source in sources {
        let Some(acceleration) = source.acceleration() else {
            continue;
        };

        if matches!(acceleration.snapshot_behavior, SnapshotBehavior::Disabled) {
            continue;
        }

        if !source.is_file_accelerated() {
            continue;
        }

        match acceleration_file_path(source.as_ref(), registry).await {
            Ok(path) => {
                paths
                    .entry(path)
                    .or_default()
                    .push(source.name().to_string());
            }
            Err(err) => {
                tracing::warn!(
                    "Unable to determine acceleration file path for dataset {} while validating snapshot configuration: {err}",
                    source.name()
                );
            }
        }
    }

    if let Some((path, datasets)) = paths.into_iter().find(|(_, ds)| ds.len() > 1) {
        return Err(SharedAccelerationSnapshotError::DuckDbSharedFile {
            datasets: datasets.join(", "),
            path: path.display().to_string(),
        });
    }

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum SharedAccelerationSnapshotError {
    #[snafu(display(
        "DuckDB doesn't support snapshots for shared acceleration. \
        Datasets [{datasets}] share the same file '{path}'. \
        Configure datasets to point to different location using duckdb_file"
    ))]
    DuckDbSharedFile { datasets: String, path: String },
}

#[derive(Debug, Snafu)]
pub enum CayenneSnapshotValidationError {
    #[snafu(display(
        "Cayenne datasets sharing metadata directory '{metadata_dir}' have inconsistent snapshot settings. \
        Datasets with snapshots enabled: [{enabled_datasets}]. Datasets with snapshots disabled: [{disabled_datasets}]. \
        All Cayenne datasets sharing the same metadata directory must have the same snapshot \
        configuration (either all enabled or all disabled). \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne#snapshots"
    ))]
    InconsistentSnapshotSettings {
        metadata_dir: String,
        enabled_datasets: String,
        disabled_datasets: String,
    },

    #[snafu(display(
        "Cayenne doesn't support snapshots for shared acceleration. \
        Datasets [{datasets}] share metadata directory '{metadata_dir}'. \
        Only single dataset per spicepod is supported when snapshots are enabled"
    ))]
    SharedAcceleration {
        metadata_dir: String,
        datasets: String,
    },
}
