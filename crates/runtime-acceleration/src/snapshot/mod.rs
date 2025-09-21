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

use std::{collections::HashMap, path::PathBuf, str::FromStr};

use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore, path::Path as ObjectPath};
use snafu::prelude::*;
use url::Url;

use arrow::datatypes::SchemaRef;

use crate::dataset_checkpoint::{
    DatasetCheckpointer, DatasetCheckpointerFactory, Result as CheckpointerResult,
};
use tokio::fs;
use util::{RetryError, fibonacci_backoff::FibonacciBackoff, retry};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid snapshot bootstrap failure behavior: {s}. Valid values are: warn, retry, fallback"
    ))]
    InvalidSnapshotBootstrapBehavior { s: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SnapshotBootstrapManager {
    dataset_name: String,
    snapshots_location: object_store::path::Path,
    local_path: PathBuf,
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

#[derive(Debug, Snafu)]
enum SnapshotDownloadError {
    #[snafu(display("Failed to list snapshots at {path}: {source}"))]
    ListSnapshots {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to download snapshot {path}: {source}"))]
    Download {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to read snapshot bytes for {path}: {source}"))]
    DownloadBytes {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to ensure local snapshot directory {path}: {source}"))]
    CreateLocalDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to write snapshot to {path}: {source}"))]
    WriteLocal {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to initialize dataset checkpointer: {source}"))]
    CheckpointerInit {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Failed to fetch schema from dataset checkpointer: {source}"))]
    CheckpointerSchema {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Snapshot {path} is missing a schema in its checkpoint"))]
    MissingSchema { path: String },
}

#[derive(Debug, Clone)]
struct SnapshotCandidate {
    location: ObjectPath,
    timestamp: String,
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

#[cfg(feature = "snapshots")]
const SNAPSHOTS_ENABLED: bool = true;
#[cfg(not(feature = "snapshots"))]
const SNAPSHOTS_ENABLED: bool = false;

impl SnapshotBootstrapManager {
    pub fn enabled(params: &HashMap<String, String>) -> bool {
        if !SNAPSHOTS_ENABLED {
            return false;
        }
        params
            .get("snapshots_enabled")
            .and_then(|s| {
                s.parse()
                    .inspect_err(|e| {
                        tracing::error!(
                            "Couldn't parse `snapshots_enabled`, defaulting to false: {e}"
                        )
                    })
                    .ok()
            })
            .unwrap_or(false)
    }

    pub async fn from_params(
        dataset_name: &str,
        params: HashMap<String, String>,
        checkpointer_factory: DatasetCheckpointerFactory,
        local_path: PathBuf,
    ) -> Option<Self> {
        if !SnapshotBootstrapManager::enabled(&params) {
            tracing::debug!("Snapshots are disabled for {dataset_name}");
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
            dataset_name: dataset_name.to_string(),
            snapshots_location: path,
            local_path,
            object_store: store,
            checkpointer_factory,
            snapshot_bootstrap_failure_behavior,
        })
    }

    pub async fn download_latest_snapshot(&self) -> CheckpointerResult<Option<SchemaRef>> {
        match self.snapshot_bootstrap_failure_behavior {
            SnapshotBootstrapFailureBehavior::Warn => match self.download_latest_once().await {
                Ok(result) => Ok(result),
                Err(err) => {
                    let location = self.snapshots_location.to_string();
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        location = %location,
                        error = %err,
                        "Failed to bootstrap snapshot; continuing without a downloaded snapshot."
                    );
                    Ok(None)
                }
            },
            SnapshotBootstrapFailureBehavior::Retry => {
                let retry_strategy = FibonacciBackoff::default();
                let dataset_name = self.dataset_name.clone();
                let location = self.snapshots_location.to_string();

                let result = retry(retry_strategy, || async {
                    match self.download_latest_once().await {
                        Ok(result) => Ok(result),
                        Err(err) => {
                            tracing::error!(
                                dataset = %dataset_name,
                                location = %location,
                                error = %err,
                                "Failed to bootstrap snapshot; retrying."
                            );
                            Err(RetryError::transient(err))
                        }
                    }
                })
                .await;

                match result {
                    Ok(result) => Ok(result),
                    Err(RetryError::Permanent(err)) => Err(Box::new(err)),
                    Err(RetryError::Transient { err, .. }) => Err(Box::new(err)),
                }
            }
            SnapshotBootstrapFailureBehavior::Fallback => {
                match self.download_with_fallback().await {
                    Ok(result) => Ok(result),
                    Err(err) => {
                        let location = self.snapshots_location.to_string();
                        tracing::warn!(
                            dataset = %self.dataset_name,
                            location = %location,
                            error = %err,
                            "Failed to bootstrap snapshot even after fallback attempts; continuing."
                        );
                        Ok(None)
                    }
                }
            }
        }
    }

    async fn download_latest_once(&self) -> Result<Option<SchemaRef>, SnapshotDownloadError> {
        let mut candidates = self.list_snapshot_candidates().await?;
        if let Some(candidate) = candidates.into_iter().next() {
            self.download_snapshot(&candidate.location).await.map(Some)
        } else {
            Ok(None)
        }
    }

    async fn download_with_fallback(&self) -> Result<Option<SchemaRef>, SnapshotDownloadError> {
        let candidates = self.list_snapshot_candidates().await?;
        if candidates.is_empty() {
            return Ok(None);
        }

        for candidate in candidates {
            let path_display = candidate.location.to_string();
            match self.download_snapshot(&candidate.location).await {
                Ok(schema) => return Ok(Some(schema)),
                Err(SnapshotDownloadError::MissingSchema { path }) => {
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        snapshot = %path,
                        "Snapshot missing schema; attempting to download the next available snapshot."
                    );
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        snapshot = %path_display,
                        error = %err,
                        "Failed to download snapshot while attempting fallback."
                    );
                    return Err(err);
                }
            }
        }

        tracing::warn!(
            dataset = %self.dataset_name,
            location = %self.snapshots_location.to_string(),
            "All available snapshots are missing schemas; continuing without bootstrapping."
        );

        Ok(None)
    }

    async fn list_snapshot_candidates(
        &self,
    ) -> Result<Vec<SnapshotCandidate>, SnapshotDownloadError> {
        let mut stream = self.object_store.list(Some(&self.snapshots_location));
        let mut snapshots: Vec<SnapshotCandidate> = Vec::new();
        let listing_path = self.snapshots_location.to_string();

        while let Some(meta_result) = stream.next().await {
            let meta: ObjectMeta =
                meta_result.map_err(|source| SnapshotDownloadError::ListSnapshots {
                    path: listing_path.clone(),
                    source,
                })?;

            if let Some(candidate) = Self::snapshot_candidate_from_meta(meta, &self.dataset_name) {
                snapshots.push(candidate);
            }
        }

        snapshots.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(snapshots)
    }

    async fn download_snapshot(
        &self,
        location: &ObjectPath,
    ) -> Result<SchemaRef, SnapshotDownloadError> {
        let path_display = location.to_string();

        let reader = self.object_store.get(location).await.map_err(|source| {
            SnapshotDownloadError::Download {
                path: path_display.clone(),
                source,
            }
        })?;

        let bytes =
            reader
                .bytes()
                .await
                .map_err(|source| SnapshotDownloadError::DownloadBytes {
                    path: path_display.clone(),
                    source,
                })?;

        if let Some(parent) = self.local_path.parent() {
            fs::create_dir_all(parent).await.map_err(|source| {
                SnapshotDownloadError::CreateLocalDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }

        fs::write(&self.local_path, bytes).await.map_err(|source| {
            SnapshotDownloadError::WriteLocal {
                path: self.local_path.clone(),
                source,
            }
        })?;

        let checkpointer = (self.checkpointer_factory)()
            .await
            .map_err(|source| SnapshotDownloadError::CheckpointerInit { source })?;

        match checkpointer
            .get_schema()
            .await
            .map_err(|source| SnapshotDownloadError::CheckpointerSchema { source })?
        {
            Some(schema) => Ok(schema),
            None => Err(SnapshotDownloadError::MissingSchema { path: path_display }),
        }
    }

    fn snapshot_candidate_from_meta(
        meta: ObjectMeta,
        dataset_name: &str,
    ) -> Option<SnapshotCandidate> {
        let location = meta.location;
        let filename = location.filename()?;
        let timestamp = Self::parse_snapshot_timestamp(filename, dataset_name)?;

        Some(SnapshotCandidate {
            location,
            timestamp,
        })
    }

    fn parse_snapshot_timestamp(filename: &str, dataset_name: &str) -> Option<String> {
        let Some(name_without_ext) = filename.strip_suffix(".db") else {
            return None;
        };

        let (name_part, timestamp) = name_without_ext.rsplit_once('_')?;
        if name_part != dataset_name {
            return None;
        }

        if timestamp.len() != 16 {
            return None;
        }

        Some(timestamp.to_string())
    }
}
