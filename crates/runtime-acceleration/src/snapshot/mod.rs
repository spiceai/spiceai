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

//! Supports loading and saving snapshots of accelerated database files to and from object storage.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use object_store::ObjectStore;
use snafu::prelude::*;
use url::Url;

use crate::dataset_checkpoint::{DatasetCheckpointer, DatasetCheckpointerFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid snapshot bootstrap failure behavior: {s}. Valid values are: warn, retry, fallback"
    ))]
    InvalidSnapshotBootstrapBehavior { s: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SnapshotBootstrapManager {
    snapshots_location: object_store::path::Path,
    object_store: Box<dyn ObjectStore>,
    snapshot_bootstrap_failure_behavior: SnapshotBootstrapFailureBehavior,
    checkpointer_factory: DatasetCheckpointerFactory,
}

#[derive(Debug, Clone, Copy, Default)]
enum SnapshotBootstrapFailureBehavior {
    /// Logs a warning if the snapshot fails to load and continues as if no snapshot exists.
    Warn,
    /// Logs an error and retries loading the snapshot indefinitely. Only covers errors
    /// loading the snapshot (i.e. insufficient permissions, network issues, etc), if no
    /// snapshots exists then it continues as normal.
    Retry,
    /// If the checkpoint cannot be loaded for a downloaded snapshot (i.e. corrupted or otherwise errors out) then try loading
    /// an older snapshot.
    #[default]
    Fallback,
}

impl FromStr for SnapshotBootstrapFailureBehavior {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "warn" => Ok(SnapshotBootstrapFailureBehavior::Warn),
            "retry" => Ok(SnapshotBootstrapFailureBehavior::Retry),
            "fallback" => Ok(SnapshotBootstrapFailureBehavior::Fallback),
            _ => Err(InvalidSnapshotBootstrapBehaviorSnafu { s: s.to_string() }.build()),
        }
    }
}

impl SnapshotBootstrapManager {
    pub async fn from_params(
        dataset_name: &str,
        params: HashMap<String, String>,
        checkpointer_factory: DatasetCheckpointerFactory,
    ) -> Option<Self> {
        let snapshots_enabled = params
            .get("snapshots_enabled")
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);
        if !snapshots_enabled {
            tracing::debug!("Snapshots are explicitly disabled for {dataset_name}");
            return None;
        }
        tracing::debug!("Snapshots are enabled for {dataset_name}");

        let snapshots_location_url: Url = params
            .get("snapshots_location")
            .and_then(|s| s.parse().ok())?;

        let snapshot_bootstrap_failure_behavior: SnapshotBootstrapFailureBehavior = params
            .get("snapshots_bootstrap_on_failure_behavior")
            .and_then(|s| s.parse().inspect_err(|e| tracing::error!("{e}")).ok())
            .unwrap_or_default();

        let (store, path) = match (
            snapshots_location_url.scheme(),
            snapshots_location_url.path(),
        ) {
            ("s3", path) => {
                let store = aws_sdk_credential_bridge::from_s3_url(&snapshots_location_url, None)
                    .await
                    .ok()?;
                let path = object_store::path::Path::from(path);
                (store, path)
            }
            _ => object_store::parse_url(&snapshots_location_url).ok()?,
        };

        Some(Self {
            snapshots_location: path,
            object_store: store,
            checkpointer_factory,
            snapshot_bootstrap_failure_behavior,
        })
    }

    pub fn download_latest_snapshot
}
