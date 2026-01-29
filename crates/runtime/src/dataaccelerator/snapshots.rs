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
use runtime_acceleration::{
    dataset_checkpoint::make_checkpointer_factory,
    snapshot::{SnapshotBehavior, SnapshotManager, metrics},
};
use snafu::{ResultExt, Snafu};

/// Downloads a snapshot if needed for bootstrapping.
/// Returns `BootstrapStatus`::`Bootstrapped` if a snapshot was successfully downloaded.
pub(super) async fn download_snapshot_if_needed(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    layout: AccelerationLayout,
    engine: AccelerationEngine,
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
        let manager = manager.with_checkpointer_factory(checkpoint_factory);
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

pub(crate) async fn validate_snapshot_paths(sources: Vec<Arc<dyn AccelerationSource>>) {
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

    for (path, datasets) in paths.into_iter().filter(|(_, ds)| ds.len() > 1) {
        tracing::warn!(
            "Datasets [{}] are configured to use the same acceleration file path '{}' while snapshots are enabled. Each dataset must use a unique file path to prevent snapshot conflicts.",
            datasets.join(", "),
            path.display()
        );
    }
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
