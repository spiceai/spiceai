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

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use crate::component::dataset::acceleration::Engine;
use crate::dataaccelerator::BootstrapStatus;
#[cfg(not(windows))]
use crate::dataaccelerator::cayenne::CayenneAccelerator;
use crate::{
    component::dataset::acceleration::Acceleration,
    dataaccelerator::{
        AccelerationSource, acceleration_file_path,
        spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
    },
};
use runtime_acceleration::snapshot::AccelerationEngine;
use runtime_acceleration::snapshot::AccelerationLayout;
use runtime_acceleration::snapshot::ForceCreate;
use runtime_acceleration::snapshot::engine::SnapshotEngine;
use runtime_acceleration::{
    dataset_checkpoint::make_checkpointer_factory,
    snapshot::{SnapshotBehavior, SnapshotManager, metrics},
};
use snafu::{ResultExt, Snafu};

/// Downloads a snapshot if needed for bootstrapping.
/// Returns `BootstrapStatus`::`Bootstrapped` if a snapshot was successfully downloaded.
///
/// `engine_override`, when set, replaces the engine that the resulting
/// `SnapshotManager` would otherwise build via
/// `runtime_acceleration::snapshot::engine::create_snapshot_engine`. Used by
/// the Cayenne accelerator to inject a `CayenneSnapshotEngine` that knows
/// how to import a per-dataset metastore slice on extract.
pub(super) async fn download_snapshot_if_needed(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
) -> BootstrapStatus {
    if !acceleration.snapshot_behavior.bootstrap_enabled() {
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
    let source = source.clone_arc();
    let snapshot_behavior = acceleration.snapshot_behavior.clone();
    let checkpoint_factory = make_checkpointer_factory(move || {
        let source = Arc::clone(&source);
        let snapshot_behavior = snapshot_behavior.clone();
        async move {
            DatasetCheckpoint::try_new(source.as_ref(), OpenOption::OpenExisting)
                .await
                .boxed()
                .map(|checkpoint| {
                    checkpoint
                        .with_snapshot_behavior(snapshot_behavior)
                        .to_arc()
                })
        }
    });
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
pub(crate) async fn snapshot_before_recreate(
    acceleration: &Acceleration,
    dataset_name: &str,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
    schema: Arc<arrow_schema::Schema>,
    engine_override: Option<Arc<dyn SnapshotEngine>>,
) {
    if !acceleration.snapshot_behavior.create_enabled() {
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

pub(crate) async fn validate_snapshot_paths(
    sources: Vec<Arc<dyn AccelerationSource>>,
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

        match acceleration_file_path(source.as_ref()).await {
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

/// Validates that all Cayenne datasets sharing the same metadata directory have consistent
/// snapshot settings (either all enabled or all disabled).
///
/// This validation is necessary because Cayenne uses a shared `SQLite` metadata catalog for
/// all datasets in the same metadata directory. When snapshots are enabled, the metadata
/// database must be included in the snapshot archive. To ensure consistency and avoid
/// conflicts during snapshot restoration, all datasets sharing the metadata directory
/// must have the same snapshot configuration.
///
/// Returns `Ok(())` if the configuration is valid, or an error describing which datasets
/// have mismatched settings.
#[cfg(not(windows))]
pub fn validate_cayenne_snapshot_consistency(
    sources: &[Arc<dyn AccelerationSource>],
) -> Result<(), CayenneSnapshotValidationError> {
    // Group Cayenne datasets by their resolved metadata directory
    let mut metadata_dir_groups: HashMap<String, Vec<(String, bool)>> = HashMap::new();

    for source in sources {
        let Some(acceleration) = source.acceleration() else {
            continue;
        };

        // Only check Cayenne datasets
        if acceleration.engine != Engine::Cayenne {
            continue;
        }

        let metadata_dir = CayenneAccelerator::resolve_metadata_dir(Some(acceleration));
        let snapshots_enabled =
            !matches!(acceleration.snapshot_behavior, SnapshotBehavior::Disabled);
        let dataset_name = source.name().to_string();

        metadata_dir_groups
            .entry(metadata_dir)
            .or_default()
            .push((dataset_name, snapshots_enabled));
    }

    // Check each group for consistency
    for (metadata_dir, datasets) in metadata_dir_groups {
        if datasets.len() <= 1 {
            continue; // Single dataset, no conflict possible
        }

        let enabled: Vec<&str> = datasets
            .iter()
            .filter_map(|(name, enabled)| if *enabled { Some(name.as_str()) } else { None })
            .collect();
        let disabled: Vec<&str> = datasets
            .iter()
            .filter_map(|(name, enabled)| if *enabled { None } else { Some(name.as_str()) })
            .collect();

        // If we have both enabled and disabled datasets, that's an error
        if !enabled.is_empty() && !disabled.is_empty() {
            return Err(
                CayenneSnapshotValidationError::InconsistentSnapshotSettings {
                    metadata_dir,
                    enabled_datasets: enabled.join(", "),
                    disabled_datasets: disabled.join(", "),
                },
            );
        }

        // Multiple datasets sharing the metadata directory with snapshots all
        // enabled is supported: each dataset's snapshot ships a per-dataset
        // metastore-slice JSON via `CayenneSnapshotEngine`. That engine is
        // wired in by `Cayenne::snapshot_engine_for_source` and threaded
        // through both the snapshot-creation pipeline
        // (`build_snapshot_creation_config`) and the snapshot-refresh-mode
        // pipeline (`build_snapshot_refresh_state`), so per-dataset slices
        // never clobber each other on extract. The previous restriction
        // (single-dataset-per-metadata-dir) is therefore lifted.
    }

    Ok(())
}

/// No-op validation on Windows where Cayenne is not supported.
#[cfg(windows)]
pub fn validate_cayenne_snapshot_consistency(
    _sources: &[Arc<dyn AccelerationSource>],
) -> Result<(), CayenneSnapshotValidationError> {
    Ok(())
}

#[cfg(test)]
#[cfg(not(windows))]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::{Acceleration, Engine, Mode};
    use datafusion::sql::TableReference;
    use runtime_acceleration::snapshot::SnapshotBehavior;
    use spicepod::acceleration::SnapshotsCompaction;
    use spicepod::component::snapshot::Snapshots;
    use std::sync::Weak;

    struct MockSource {
        name: TableReference,
        acceleration: Option<Acceleration>,
    }

    impl MockSource {
        fn cayenne_with_metadata_dir(
            name: &str,
            metadata_dir: &str,
            snapshots_enabled: bool,
        ) -> Arc<dyn AccelerationSource> {
            let mut accel = Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                ..Default::default()
            };
            accel
                .params
                .insert("cayenne_metadata_dir".to_string(), metadata_dir.to_string());
            if snapshots_enabled {
                let snapshots = Arc::new(Snapshots::default());
                let secrets = Weak::new();
                let handle = tokio::runtime::Handle::current();
                accel.snapshot_behavior = SnapshotBehavior::Enabled(
                    snapshots,
                    secrets,
                    handle,
                    SnapshotsCompaction::Disabled,
                );
            }
            Arc::new(MockSource {
                name: TableReference::bare(name),
                acceleration: Some(accel),
            })
        }
    }

    impl AccelerationSource for MockSource {
        fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
            Arc::new(MockSource {
                name: self.name.clone(),
                acceleration: self.acceleration.clone(),
            })
        }

        fn is_file_accelerated(&self) -> bool {
            self.acceleration
                .as_ref()
                .is_some_and(|a| matches!(a.mode, Mode::File | Mode::FileCreate | Mode::FileUpdate))
        }

        fn app(&self) -> Arc<app::App> {
            unimplemented!("not needed for validation tests")
        }

        fn runtime(&self) -> Arc<crate::Runtime> {
            unimplemented!("not needed for validation tests")
        }

        fn acceleration(&self) -> Option<&Acceleration> {
            self.acceleration.as_ref()
        }

        fn name(&self) -> &TableReference {
            &self.name
        }

        fn time_column(&self) -> Option<&str> {
            None
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[tokio::test]
    async fn test_cayenne_shared_acceleration_with_snapshots_now_supported() {
        // Multi-dataset shared metastore + snapshots-enabled used to error
        // with SharedAcceleration. With per-dataset metastore-slice snapshots
        // (`CayenneSnapshotEngine`), this configuration is now supported.
        let sources: Vec<Arc<dyn AccelerationSource>> = vec![
            MockSource::cayenne_with_metadata_dir("ds1", "/tmp/meta", true),
            MockSource::cayenne_with_metadata_dir("ds2", "/tmp/meta", true),
        ];

        validate_cayenne_snapshot_consistency(&sources)
            .expect("shared metastore + snapshots is now valid");
    }

    #[tokio::test]
    async fn test_cayenne_inconsistent_snapshot_settings_errors() {
        let sources: Vec<Arc<dyn AccelerationSource>> = vec![
            MockSource::cayenne_with_metadata_dir("ds1", "/tmp/meta", true),
            MockSource::cayenne_with_metadata_dir("ds2", "/tmp/meta", false),
        ];

        let result = validate_cayenne_snapshot_consistency(&sources);
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        assert!(
            matches!(
                err,
                CayenneSnapshotValidationError::InconsistentSnapshotSettings { .. }
            ),
            "Expected InconsistentSnapshotSettings error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_cayenne_single_dataset_with_snapshots_ok() {
        let sources: Vec<Arc<dyn AccelerationSource>> =
            vec![MockSource::cayenne_with_metadata_dir(
                "ds1",
                "/tmp/meta",
                true,
            )];

        let result = validate_cayenne_snapshot_consistency(&sources);
        result.expect("expected Ok");
    }

    #[tokio::test]
    async fn test_cayenne_different_metadata_dirs_ok() {
        let sources: Vec<Arc<dyn AccelerationSource>> = vec![
            MockSource::cayenne_with_metadata_dir("ds1", "/tmp/meta1", true),
            MockSource::cayenne_with_metadata_dir("ds2", "/tmp/meta2", true),
        ];

        let result = validate_cayenne_snapshot_consistency(&sources);
        result.expect("expected Ok");
    }

    #[tokio::test]
    async fn test_cayenne_shared_dir_all_disabled_ok() {
        let sources: Vec<Arc<dyn AccelerationSource>> = vec![
            MockSource::cayenne_with_metadata_dir("ds1", "/tmp/meta", false),
            MockSource::cayenne_with_metadata_dir("ds2", "/tmp/meta", false),
        ];

        let result = validate_cayenne_snapshot_consistency(&sources);
        result.expect("expected Ok");
    }
}
