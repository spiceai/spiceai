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

//! Cayenne's snapshot-consistency validation.
//!
//! Stays in `runtime` while the Cayenne accelerator does: it is the only part of the
//! snapshot code that resolves a Cayenne metadata directory, and that resolution lives
//! on `CayenneAccelerator`. The engine-facing half of the snapshot bootstrap moved to
//! `data-accelerator-api` so the accelerator crates can call it.

use runtime_acceleration::snapshot::SnapshotBehavior;
use std::{collections::HashMap, sync::Arc};

use crate::component::dataset::acceleration::Engine;
use crate::dataaccelerator::AccelerationSource;
#[cfg(not(windows))]
use crate::dataaccelerator::cayenne::CayenneAccelerator;
use data_accelerator_api::snapshots::CayenneSnapshotValidationError;

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

        fn secrets(&self) -> Arc<tokio::sync::RwLock<crate::secrets::Secrets>> {
            unimplemented!("not needed for validation tests")
        }

        fn acceleration(&self) -> Option<&Acceleration> {
            self.acceleration.as_ref()
        }

        fn name(&self) -> &TableReference {
            &self.name
        }

        fn connector_name(&self) -> Option<&str> {
            // These tests exercise snapshot-consistency validation, which never
            // consults the connector default; `None` resolves to `full`.
            None
        }

        fn on_schema_change(&self) -> Option<runtime_acceleration::OnSchemaChange> {
            // Snapshot-consistency validation never consults the schema-change policy.
            None
        }

        fn allows_write(&self) -> bool {
            true
        }

        fn time_column(&self) -> Option<&str> {
            None
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn checkpointer_factory(
            &self,
            _snapshot_behavior: runtime_acceleration::snapshot::SnapshotBehavior,
        ) -> runtime_acceleration::dataset_checkpoint::DatasetCheckpointerFactory {
            runtime_acceleration::dataset_checkpoint::make_checkpointer_factory(|| async {
                Err("test source has no checkpoint".into())
            })
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
