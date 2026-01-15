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

use crate::{
    component::dataset::acceleration::Acceleration,
    dataaccelerator::{
        AccelerationSource, acceleration_file_path,
        spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
    },
};
use runtime_acceleration::snapshot::AccelerationEngine;
use runtime_acceleration::{
    dataset_checkpoint::make_checkpointer_factory,
    snapshot::{SnapshotBehavior, SnapshotDownloadInfo, SnapshotManager, metrics},
};
use snafu::ResultExt;

pub(super) async fn download_snapshot_if_needed(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    path: PathBuf,
    engine: AccelerationEngine,
) {
    if !acceleration.snapshot_behavior.bootstrap_enabled() {
        return;
    }

    if path.exists() {
        tracing::info!(
            "Acceleration already exists at {}, skipping snapshot download",
            path.display()
        );
        return;
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
        path,
        engine,
    )
    .await
    {
        let manager = manager.with_checkpointer_factory(checkpoint_factory);
        let start_time = Instant::now();
        match manager.download_latest_snapshot().await {
            Ok(Some(SnapshotDownloadInfo {
                schema: _,
                bytes_downloaded,
                checksum,
            })) => {
                let duration_ms = start_time.elapsed().as_secs_f64() * 1000.0;
                metrics::record_bootstrap_metrics(
                    &dataset_name,
                    duration_ms,
                    bytes_downloaded,
                    &checksum,
                );
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("Failed to download snapshot: {}", e);
            }
        }
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
