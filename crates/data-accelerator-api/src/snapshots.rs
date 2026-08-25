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

    if !layout.is_enabled() {
        tracing::debug!("No storage paths for the acceleration layout, skipping download");
        return BootstrapStatus::none();
    }

    // Asks the layout whether an acceleration is already present, rather than testing one
    // path for existence. For a directory-layout engine the accelerator has already
    // created its directories by the time this runs — Cayenne's metastore creates the
    // metadata directory the moment it opens, and that directory is shared by every
    // Cayenne dataset in the pod — so an existence test on a path would answer "yes"
    // unconditionally and silently skip every bootstrap.
    if layout.has_existing_acceleration() {
        tracing::info!(
            "Acceleration for '{}' already exists at {}, skipping snapshot download",
            source.name(),
            layout.data_path().map_or_else(
                || "the configured location".to_string(),
                |p| p.display().to_string()
            )
        );
        return BootstrapStatus::none();
    }

    let dataset_name = source.name().to_string();
    // The source opens its own checkpoint: each engine's checkpointer carries that
    // engine's sidecar SQL and lives in its own `runtime-checkpoint-*` crate, so it
    // reaches here as a factory behind the `AccelerationSource` contract rather than
    // as a type this crate names.
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
        // A source whose rows are the result of a definition (a view's SQL) must not
        // bootstrap an archive materialized from a different one: the rows would be
        // wrong rather than merely old, and no schema check would catch it.
        if let Some(definition) = source.definition_fingerprint() {
            manager = manager.with_source_definition(definition);
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
    source: &dyn AccelerationSource,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    schema: Arc<arrow_schema::Schema>,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
    refresh_mode: RefreshMode,
) {
    if !acceleration.snapshot_behavior.create_enabled() {
        return;
    }

    let dataset_name = source.name().to_string();

    // A source whose rows are the result of a definition (a view's SQL) cannot publish
    // from here. This runs inside the accelerator's `init`, before the runtime has
    // planned the definition, so there is no way to establish that the outgoing
    // materialization came from a single read — and publishing makes whatever it holds
    // the store's current snapshot. The live publish path decides that question with the
    // compiled plan in hand; this one would be guessing, and the cost of guessing wrong
    // is a durable wrong answer rather than a missing backup.
    if source.definition_fingerprint().is_some() {
        tracing::warn!(
            "Skipped snapshotting the outgoing acceleration of '{dataset_name}' before recreating it, so the snapshot series keeps its previously published contents: Spice cannot confirm from here that those rows came from a single consistent read of this view's sources"
        );
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

    // A partitioned Cayenne dataset's metastore slice is incomplete: `export_dataset`
    // selects every dependent table by the parent's table id, so the partition child
    // `cayenne_table` rows (and each child's metadata) are not exported, and a restore
    // fails at `infer_existing_partitions` with `TableNotFound` once the drop cascade has
    // removed the live child rows. Publishing would make that unrestorable archive the
    // store's `current-snapshot-id`, so skip until the slice covers child tables.
    // `build_snapshot_creation_config` applies the same gate to the periodic publish path.
    if engine == AccelerationEngine::Cayenne && !acceleration.partition_by.is_empty() {
        tracing::warn!(
            dataset = %dataset_name,
            "Skipping the pre-recreation snapshot: snapshots of a partitioned Cayenne acceleration are not yet supported, and an archive without the partitions' metadata could not be restored"
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
        dataset_name.clone(),
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
