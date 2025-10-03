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

use std::{path::PathBuf, sync::Arc};

use runtime_acceleration::{
    dataset_checkpoint::make_checkpointer_factory, snapshot::SnapshotManager,
};
use snafu::ResultExt;

use crate::{
    component::dataset::acceleration::Acceleration,
    dataaccelerator::{
        AccelerationSource,
        spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
    },
};

pub(super) async fn download_snapshot_if_needed(
    acceleration: &Acceleration,
    source: &dyn AccelerationSource,
    path: PathBuf,
) {
    if !acceleration.snapshots.bootstrap_enabled() {
        return;
    }

    let source_name = source.name().to_string();
    let source = source.clone_arc();
    let snapshot_behavior = acceleration.snapshots.clone();
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
    if let Some(manager) =
        SnapshotManager::try_new(source_name, acceleration.snapshots.clone(), path).await
    {
        let manager = manager.with_checkpointer_factory(checkpoint_factory);
        let _ = manager.download_latest_snapshot().await.inspect_err(|e| {
            tracing::error!("Failed to download snapshot: {}", e);
        });
    }
}
