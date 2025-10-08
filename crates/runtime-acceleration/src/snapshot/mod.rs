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

use std::{cmp::Ordering, path::PathBuf, str::FromStr, sync::Arc};

use arrow::datatypes::SchemaRef;
use bytes::BytesMut;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use futures::StreamExt;
use object_store::{
    ObjectMeta, ObjectStore,
    path::{Path as ObjectPath, PathPart},
};
use snafu::prelude::*;
use spicepod::{component::snapshot::BootstrapOnFailureBehavior, param::ParamValue};
use tokio::{
    fs,
    io::{AsyncReadExt, BufReader},
};
use url::Url;
use util::{RetryError, fibonacci_backoff::FibonacciBackoff, retry};

use crate::dataset_checkpoint::DatasetCheckpointerFactory;

mod behavior;
pub use behavior::SnapshotBehavior;

const SNAPSHOT_TIMESTAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";
const SNAPSHOT_MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Snafu)]
pub enum SnapshotDownloadError {
    #[snafu(display("Dataset checkpointer factory not set for snapshot manager"))]
    CheckpointerFactoryNotSet,
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
    #[snafu(display("Failed to ensure local snapshot directory {}: {source}", path.display()))]
    CreateLocalDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to write snapshot to {}: {source}", path.display()))]
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

#[derive(Debug, Snafu)]
pub enum SnapshotUploadError {
    #[snafu(display("Failed to open local snapshot file {}: {source}", path.display()))]
    OpenLocal {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to read local snapshot file {}: {source}", path.display()))]
    ReadLocal {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to start snapshot upload to {path}: {source}"))]
    StartUpload {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to upload snapshot part to {path}: {source}"))]
    UploadPart {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to complete snapshot upload to {path}: {source}"))]
    CompleteUpload {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to abort snapshot upload to {path}: {source}"))]
    AbortUpload {
        path: String,
        source: object_store::Error,
    },
}

#[derive(Debug, Clone)]
struct SnapshotCandidate {
    location: ObjectPath,
    timestamp: DateTime<Utc>,
    display_timestamp: String,
}

impl SnapshotCandidate {
    fn location(&self) -> &ObjectPath {
        &self.location
    }
}

impl PartialEq for SnapshotCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location && self.timestamp == other.timestamp
    }
}

impl Eq for SnapshotCandidate {}

impl PartialOrd for SnapshotCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SnapshotCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.display_timestamp.cmp(&other.display_timestamp))
            .then_with(|| self.location.as_ref().cmp(other.location.as_ref()))
    }
}

#[derive(Debug, Clone, Copy)]
struct SnapshotPathLayout<'a> {
    dataset_name: &'a str,
}

impl<'a> SnapshotPathLayout<'a> {
    fn new(dataset_name: &'a str) -> Self {
        Self { dataset_name }
    }

    fn dataset_name_path(&'a self) -> PathPart<'a> {
        PathPart::from(self.dataset_name)
    }

    fn snapshot_filename(&self, instant: DateTime<Utc>) -> String {
        format!(
            "{}_{}.db",
            self.dataset_name,
            instant.format(SNAPSHOT_TIMESTAMP_FORMAT)
        )
    }

    fn dataset_partition_raw(&self) -> String {
        format!("dataset={}", self.dataset_name)
    }

    fn dataset_partition_expected(&self) -> String {
        format!("dataset={}", self.dataset_name_path().as_ref())
    }

    fn build_location(&self, base: &ObjectPath, instant: DateTime<Utc>) -> ObjectPath {
        let month_partition = format!("month={}", instant.format("%Y-%m"));
        let day_partition = format!("day={}", instant.format("%Y-%m-%d"));
        let dataset_partition = self.dataset_partition_raw();
        base.child(month_partition)
            .child(day_partition)
            .child(dataset_partition)
            .child(self.snapshot_filename(instant))
    }

    fn parse_filename_timestamp(&self, filename: &str) -> Option<(String, DateTime<Utc>)> {
        let name_without_ext = filename.strip_suffix(".db")?;
        let (name_part, timestamp_str) = name_without_ext.rsplit_once('_')?;
        if name_part != self.dataset_name_path().as_ref() {
            return None;
        }
        if timestamp_str.len() != 16 {
            return None;
        }
        let parsed = parse_snapshot_timestamp(timestamp_str)?;
        Some((timestamp_str.to_string(), parsed))
    }

    fn candidate_from_meta(&self, meta: ObjectMeta) -> Option<SnapshotCandidate> {
        let location = meta.location;
        let parts: Vec<_> = location.parts().collect();
        if parts.len() < 4 {
            return None;
        }

        let mut parts_rev = parts.iter().rev();
        let filename = parts_rev.next()?.as_ref();
        let dataset_part = parts_rev.next()?;
        let day_part = parts_rev.next()?.as_ref();
        let month_part = parts_rev.next()?.as_ref();

        if !month_part.starts_with("month=") || !day_part.starts_with("day=") {
            return None;
        }

        let expected_dataset_part = self.dataset_partition_expected();
        if dataset_part.as_ref() != expected_dataset_part {
            tracing::trace!(
                "Dataset partition mismatch while parsing snapshot path. expected={expected_dataset_part} actual={}",
                dataset_part.as_ref()
            );
            return None;
        }

        let (display_timestamp, timestamp) = self.parse_filename_timestamp(filename)?;

        let expected_month_part = format!("month={}", timestamp.format("%Y-%m"));
        if month_part != expected_month_part {
            tracing::trace!(
                "Month partition mismatch while parsing snapshot path. expected={expected_month_part} actual={month_part}"
            );
            return None;
        }

        let expected_day_part = format!("day={}", timestamp.format("%Y-%m-%d"));
        if day_part != expected_day_part {
            tracing::trace!(
                "Day partition mismatch while parsing snapshot path. expected={expected_day_part} actual={day_part}"
            );
            return None;
        }

        Some(SnapshotCandidate {
            location,
            timestamp,
            display_timestamp,
        })
    }
}

fn parse_snapshot_timestamp(timestamp: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(timestamp, SNAPSHOT_TIMESTAMP_FORMAT)
        .map(|naive| Utc.from_utc_datetime(&naive))
        .or_else(|_| DateTime::parse_from_rfc3339(timestamp).map(|dt| dt.with_timezone(&Utc)))
        .ok()
}

/// Manages snapshots for a specific accelerated dataset.
#[derive(Clone)]
pub struct SnapshotManager {
    dataset_name: String,
    snapshots_location: object_store::path::Path,
    local_path: PathBuf,
    object_store: Arc<dyn ObjectStore>,
    bootstrap_failure_behavior: BootstrapOnFailureBehavior,
    checkpointer_factory: Option<DatasetCheckpointerFactory>,
}

impl std::fmt::Debug for SnapshotManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotManager")
            .field("dataset_name", &self.dataset_name)
            .field("snapshots_location", &self.snapshots_location)
            .field("local_path", &self.local_path)
            .field(
                "bootstrap_failure_behavior",
                &self.bootstrap_failure_behavior,
            )
            .field("object_store", &self.object_store)
            .finish_non_exhaustive()
    }
}

impl SnapshotManager {
    pub async fn try_new(
        dataset_name: String,
        snapshots: SnapshotBehavior,
        local_path: PathBuf,
    ) -> Option<Self> {
        let snapshot_config = match snapshots {
            SnapshotBehavior::Disabled => {
                tracing::debug!("Snapshots are disabled for {dataset_name}");
                return None;
            }
            SnapshotBehavior::Enabled(s)
            | SnapshotBehavior::BootstrapOnly(s)
            | SnapshotBehavior::CreateOnly(s) => s,
        };
        tracing::debug!("Snapshots are enabled for {dataset_name}");

        let Some(snapshot_location) = &snapshot_config.location else {
            tracing::warn!(
                "Snapshots are enabled for dataset {dataset_name} but no location is configured"
            );
            return None;
        };

        let snapshots_location_url = match Url::from_str(snapshot_location) {
            Ok(url) => url,
            Err(e) => {
                tracing::error!(
                    "Failed to parse snapshot location URL: {snapshot_location}, error: {e}"
                );
                return None;
            }
        };

        let s3_region = snapshot_config
            .as_ref()
            .params
            .as_ref()
            .and_then(|params| params.data.get("s3_region").map(ParamValue::as_string));

        let (store, path) = match (
            snapshots_location_url.scheme(),
            snapshots_location_url.path(),
        ) {
            ("s3", path) => {
                let store =
                    aws_sdk_credential_bridge::from_s3_url(&snapshots_location_url, s3_region)
                        .await
                        .ok()?;
                let path = object_store::path::Path::from(path);
                (store, path)
            }
            _ => object_store::parse_url(&snapshots_location_url).ok()?,
        };

        Some(Self {
            dataset_name,
            snapshots_location: path,
            local_path,
            object_store: store.into(),
            checkpointer_factory: None,
            bootstrap_failure_behavior: snapshot_config.bootstrap_on_failure_behavior,
        })
    }

    /// Sets a factory function to create a new dataset checkpointer for this snapshot manager.
    #[must_use]
    pub fn with_checkpointer_factory(mut self, factory: DatasetCheckpointerFactory) -> Self {
        self.checkpointer_factory = Some(factory);
        self
    }

    /// Creates a new snapshot by streaming the local acceleration file to object storage.
    ///
    /// # Errors
    ///
    /// - If the local acceleration file cannot be opened or read.
    /// - If communicating with the backing object store fails at any stage of the upload.
    #[allow(clippy::too_many_lines)]
    pub async fn create_snapshot(&self) -> Result<ObjectPath, SnapshotUploadError> {
        let now = Utc::now();
        let layout = SnapshotPathLayout::new(&self.dataset_name);
        let location = layout.build_location(&self.snapshots_location, now);
        let location_path = location.to_string();
        let local_path = self.local_path.clone();

        tracing::info!(
            "Uploading snapshot. dataset={} snapshot={location}",
            self.dataset_name
        );

        let file = fs::File::open(&local_path).await.context(OpenLocalSnafu {
            path: local_path.clone(),
        })?;

        let mut reader = BufReader::with_capacity(SNAPSHOT_MULTIPART_CHUNK_SIZE, file);

        let mut upload =
            self.object_store
                .put_multipart(&location)
                .await
                .context(StartUploadSnafu {
                    path: location_path.clone(),
                })?;

        let mut buffer = BytesMut::with_capacity(SNAPSHOT_MULTIPART_CHUNK_SIZE);
        let mut eof = false;
        let mut total_bytes: u64 = 0;

        while !eof || !buffer.is_empty() {
            while buffer.len() < SNAPSHOT_MULTIPART_CHUNK_SIZE && !eof {
                match reader.read_buf(&mut buffer).await {
                    Ok(0) => {
                        eof = true;
                    }
                    Ok(read) => {
                        total_bytes += read as u64;
                    }
                    Err(source) => {
                        tracing::error!(
                            "Failed to read local snapshot file while uploading. dataset={} snapshot={location} error={source}",
                            self.dataset_name
                        );
                        if let Err(abort_source) = upload.abort().await {
                            tracing::warn!(
                                "Failed to abort snapshot upload after read failure. dataset={} snapshot={location} error={abort_source}",
                                self.dataset_name
                            );
                            return Err(SnapshotUploadError::AbortUpload {
                                path: location_path.clone(),
                                source: abort_source,
                            });
                        }
                        return Err(SnapshotUploadError::ReadLocal {
                            path: local_path,
                            source,
                        });
                    }
                }
            }

            if buffer.is_empty() {
                break;
            }

            let chunk_len = buffer.len().min(SNAPSHOT_MULTIPART_CHUNK_SIZE);
            let chunk = buffer.split_to(chunk_len).freeze();

            if let Err(source) = upload.put_part(chunk.into()).await {
                tracing::error!(
                    "Snapshot upload part failed. dataset={} snapshot={location} error={source}",
                    self.dataset_name
                );
                if let Err(abort_source) = upload.abort().await {
                    tracing::warn!(
                        "Failed to abort snapshot upload after part failure. dataset={} snapshot={location} error={abort_source}",
                        self.dataset_name
                    );
                    return Err(SnapshotUploadError::AbortUpload {
                        path: location_path.clone(),
                        source: abort_source,
                    });
                }
                return Err(SnapshotUploadError::UploadPart {
                    path: location_path.clone(),
                    source,
                });
            }
        }

        match upload.complete().await {
            Ok(_) => {
                tracing::info!(
                    "Snapshot uploaded. dataset={} snapshot={location} size={total_bytes}",
                    self.dataset_name
                );
                Ok(location)
            }
            Err(source) => {
                tracing::error!(
                    "Failed to finalize snapshot upload. dataset={} snapshot={location} error={source}",
                    self.dataset_name
                );
                if let Err(abort_source) = upload.abort().await {
                    tracing::warn!(
                        "Failed to abort snapshot upload after completion failure. dataset={} snapshot={location} error={abort_source}",
                        self.dataset_name
                    );
                    return Err(SnapshotUploadError::AbortUpload {
                        path: location_path,
                        source: abort_source,
                    });
                }
                Err(SnapshotUploadError::CompleteUpload {
                    path: location_path,
                    source,
                })
            }
        }
    }

    /// Attempts to download the latest snapshot, returning the schema if successful.
    ///
    /// # Errors
    ///
    /// - If there is an error communicating with the object store.
    /// - If there is an error writing the snapshot to the local filesystem.
    /// - If there is an error initializing the dataset checkpointer.
    /// - If there is an error fetching the schema from the dataset checkpointer.
    pub async fn download_latest_snapshot(
        &self,
    ) -> Result<Option<SchemaRef>, SnapshotDownloadError> {
        let checkpointer_factory = Arc::clone(
            self.checkpointer_factory
                .as_ref()
                .context(CheckpointerFactoryNotSetSnafu)?,
        );
        match self.bootstrap_failure_behavior {
            BootstrapOnFailureBehavior::Warn => {
                match self.download_latest_once(checkpointer_factory).await {
                    Ok(result) => Ok(result),
                    Err(err) => {
                        let location = self.snapshots_location.to_string();
                        tracing::warn!(
                            "Failed to bootstrap snapshot; continuing without a downloaded snapshot. dataset={} location={} error={err}",
                            self.dataset_name,
                            location
                        );
                        Ok(None)
                    }
                }
            }
            BootstrapOnFailureBehavior::Retry => {
                let retry_strategy = FibonacciBackoff::default();
                let dataset_name = self.dataset_name.clone();
                let location = self.snapshots_location.to_string();

                retry(retry_strategy, || async {
                    match self
                        .download_latest_once(Arc::clone(&checkpointer_factory))
                        .await
                    {
                        Ok(result) => Ok(result),
                        Err(err) => {
                            tracing::error!(
                                "Failed to bootstrap snapshot; retrying. dataset={} location={} error={err}",
                                dataset_name,
                                location
                            );
                            Err(RetryError::transient(err))
                        }
                    }
                })
                .await
            }
            BootstrapOnFailureBehavior::Fallback => {
                match self.download_with_fallback(checkpointer_factory).await {
                    Ok(result) => Ok(result),
                    Err(err) => {
                        let location = self.snapshots_location.to_string();
                        tracing::warn!(
                            "Failed to bootstrap snapshot even after fallback attempts; continuing. dataset={} location={location} error={err}",
                            self.dataset_name,
                        );
                        Ok(None)
                    }
                }
            }
        }
    }

    async fn download_latest_once(
        &self,
        checkpointer_factory: DatasetCheckpointerFactory,
    ) -> Result<Option<SchemaRef>, SnapshotDownloadError> {
        let candidates = self.list_snapshot_candidates().await?;
        if let Some(candidate) = candidates.into_iter().next() {
            let snapshot_display = candidate.location.to_string();
            let timestamp_display = candidate.display_timestamp.clone();
            tracing::info!(
                "Downloading latest snapshot. dataset={} snapshot={snapshot_display} timestamp={timestamp_display}",
                self.dataset_name
            );
            self.download_snapshot(candidate.location(), checkpointer_factory)
                .await
                .map(Some)
        } else {
            let location_display = self.snapshots_location.to_string();
            tracing::debug!(
                "No snapshots found; continuing without bootstrapping. dataset={} location={location_display}",
                self.dataset_name
            );
            Ok(None)
        }
    }

    async fn download_with_fallback(
        &self,
        checkpointer_factory: DatasetCheckpointerFactory,
    ) -> Result<Option<SchemaRef>, SnapshotDownloadError> {
        let candidates = self.list_snapshot_candidates().await?;
        if candidates.is_empty() {
            return Ok(None);
        }

        for candidate in candidates {
            let path_display = candidate.location().to_string();
            match self
                .download_snapshot(candidate.location(), Arc::clone(&checkpointer_factory))
                .await
            {
                Ok(schema) => return Ok(Some(schema)),
                Err(SnapshotDownloadError::MissingSchema { path }) => {
                    tracing::warn!(
                        "Snapshot missing schema; attempting to download the next available snapshot. dataset={} snapshot={path}",
                        self.dataset_name,
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to download snapshot while attempting fallback. dataset={} snapshot={path_display} error={err}",
                        self.dataset_name,
                    );
                    return Err(err);
                }
            }
        }

        let location_display = self.snapshots_location.to_string();
        tracing::warn!(
            "All available snapshots are missing schemas; continuing without bootstrapping. dataset={} location={location_display}",
            self.dataset_name
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
            let meta: ObjectMeta = meta_result
                .map_err(|source| SnapshotDownloadError::ListSnapshots {
                    path: listing_path.clone(),
                    source,
                })
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to list snapshots while iterating object store listing. path={listing_path} error={e}"
                    );
                })?;

            if let Some(candidate) = Self::snapshot_candidate_from_meta(meta, &self.dataset_name) {
                snapshots.push(candidate);
            }
        }

        snapshots.sort_by(|a, b| b.cmp(a));
        let location_display = self.snapshots_location.to_string();
        let count = snapshots.len();
        tracing::info!(
            "Found {count} snapshot candidates. dataset={} location={location_display}",
            self.dataset_name
        );
        Ok(snapshots)
    }

    async fn download_snapshot(
        &self,
        location: &ObjectPath,
        checkpointer_factory: DatasetCheckpointerFactory,
    ) -> Result<SchemaRef, SnapshotDownloadError> {
        let path_display = location.to_string();

        let reader = self.object_store.get(location).await.map_err(|source| {
            SnapshotDownloadError::Download {
                path: path_display.clone(),
                source,
            }
        })?;

        tracing::info!(
            "Downloading snapshot. dataset={} snapshot={location}",
            self.dataset_name
        );

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

        let bytes_len = bytes.len();
        fs::write(&self.local_path, bytes).await.map_err(|source| {
            SnapshotDownloadError::WriteLocal {
                path: self.local_path.clone(),
                source,
            }
        })?;

        let local_path_display = self.local_path.display();
        tracing::info!(
            "Snapshot downloaded to {local_path_display}. dataset={} snapshot={location} size={bytes_len}",
            self.dataset_name
        );

        let checkpointer = (checkpointer_factory)()
            .await
            .map_err(|source| SnapshotDownloadError::CheckpointerInit { source })?;

        if let Some(schema) = checkpointer
            .get_schema()
            .await
            .map_err(|source| SnapshotDownloadError::CheckpointerSchema { source })?
        {
            tracing::info!(
                "Snapshot schema verified. dataset={} snapshot={location}",
                self.dataset_name
            );
            Ok(schema)
        } else {
            tracing::warn!(
                "Snapshot schema not found. dataset={} snapshot={location}",
                self.dataset_name
            );
            Err(SnapshotDownloadError::MissingSchema { path: path_display })
        }
    }

    fn snapshot_candidate_from_meta(
        meta: ObjectMeta,
        dataset_name: &str,
    ) -> Option<SnapshotCandidate> {
        let layout = SnapshotPathLayout::new(dataset_name);
        let candidate = layout.candidate_from_meta(meta)?;

        let snapshot_display = candidate.location.to_string();
        let timestamp = &candidate.display_timestamp;
        tracing::debug!(
            "Found snapshot candidate. dataset={} snapshot={snapshot_display} timestamp={timestamp}",
            dataset_name
        );

        Some(candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset_checkpoint::{DatasetCheckpointer, Result as DatasetCheckpointResult};
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use futures::executor::block_on;
    use object_store::{memory::InMemory, path::Path};
    use std::{io::Write, path::PathBuf, sync::Arc, time::SystemTime};
    use tempfile::NamedTempFile;

    struct NoopCheckpointer;

    #[async_trait]
    impl DatasetCheckpointer for NoopCheckpointer {
        async fn exists(&self) -> bool {
            false
        }

        async fn checkpoint(&self, _schema: &SchemaRef) -> DatasetCheckpointResult<()> {
            Ok(())
        }

        async fn get_schema(&self) -> DatasetCheckpointResult<Option<SchemaRef>> {
            Ok(None)
        }

        async fn last_checkpoint_time(&self) -> DatasetCheckpointResult<Option<SystemTime>> {
            Ok(None)
        }
    }

    fn test_meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: ObjectPath::from(path),
            last_modified: Utc
                .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            size: 1,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn snapshot_path_layout_encodes_and_parses_dataset() {
        let layout = SnapshotPathLayout::new("dataset with spaces/slash");
        let base = ObjectPath::from("snapshots/prefix");
        let instant = Utc
            .with_ymd_and_hms(2025, 1, 2, 3, 4, 5)
            .single()
            .expect("valid timestamp");

        let location = layout.build_location(&base, instant);
        let location_string = location.to_string();
        assert!(
            location_string.contains("dataset=dataset with spaces%2Fslash"),
            "dataset partition should be percent encoded"
        );
        assert!(
            !location_string.contains("dataset=dataset with spaces/slash"),
            "dataset partition should not contain raw dataset name"
        );

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc
                .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            size: 1,
            e_tag: None,
            version: None,
        };
        let candidate = layout
            .candidate_from_meta(meta)
            .expect("expected valid candidate");

        assert_eq!(candidate.display_timestamp, "20250102T030405Z");
    }

    #[test]
    fn snapshot_candidate_from_meta_filters_by_dataset() {
        let meta = test_meta(
            "snapshots/month=2025-01/day=2025-01-02/dataset=dataset/dataset_20250102T030405Z.db",
        );
        let candidate = SnapshotManager::snapshot_candidate_from_meta(meta, "dataset")
            .expect("expected valid snapshot candidate");

        assert_eq!(
            candidate.location.filename(),
            Some("dataset_20250102T030405Z.db")
        );
        assert_eq!(candidate.display_timestamp, "20250102T030405Z");
    }

    #[test]
    fn snapshot_candidate_from_meta_rejects_invalid_file() {
        let meta =
            test_meta("snapshots/month=2025-01/day=2025-01-02/dataset=dataset/dataset_invalid.db");
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());
    }

    #[test]
    fn snapshot_candidate_from_meta_rejects_mismatched_dataset_partition() {
        let meta = test_meta(
            "snapshots/month=2025-01/day=2025-01-02/dataset=other/dataset_20250102T030405Z.db",
        );
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());
    }

    #[test]
    fn snapshot_candidate_from_meta_rejects_mismatched_month_partition() {
        let meta = test_meta(
            "snapshots/month=2025-02/day=2025-01-02/dataset=dataset/dataset_20250102T030405Z.db",
        );
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());
    }

    #[test]
    fn snapshot_candidate_from_meta_rejects_mismatched_day_partition() {
        let meta = test_meta(
            "snapshots/month=2025-01/day=2025-01-03/dataset=dataset/dataset_20250102T030405Z.db",
        );
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());
    }

    #[test]
    fn snapshot_candidate_from_meta_rejects_missing_partitions() {
        let meta = test_meta("snapshots/dataset=dataset/dataset_20250102T030405Z.db");
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());

        let meta =
            test_meta("snapshots/day=2025-01-02/dataset=dataset/dataset_20250102T030405Z.db");
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());

        let meta = test_meta("snapshots/month=2025-01/dataset=dataset/dataset_20250102T030405Z.db");
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());

        let meta = test_meta("snapshots/dataset_20250102T030405Z.db");
        assert!(SnapshotManager::snapshot_candidate_from_meta(meta, "dataset").is_none());
    }

    #[test]
    fn list_snapshot_candidates_sorts_descending() {
        let store = InMemory::new();

        block_on(async {
            store
                .put(
                    &Path::from("snapshots/month=2025-01/day=2025-01-01/dataset=dataset/dataset_20250101T000000Z.db"),
                    Bytes::from_static(b"a").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from("snapshots/month=2025-02/day=2025-02-01/dataset=dataset/dataset_20250201T000000Z.db"),
                    Bytes::from_static(b"b").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from("snapshots/month=2025-03/day=2025-03-01/dataset=other/other_20250301T000000Z.db"),
                    Bytes::from_static(b"c").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from(
                        "snapshots/month=2025-01/day=2025-01-01/dataset=dataset/dataset_invalid.db",
                    ),
                    Bytes::from_static(b"d").into(),
                )
                .await
                .expect("write snapshot file");
        });

        let manager = SnapshotManager {
            dataset_name: "dataset".to_string(),
            snapshots_location: Path::from("snapshots"),
            local_path: PathBuf::from("/tmp/unused.db"),
            object_store: Arc::new(store),
            bootstrap_failure_behavior: BootstrapOnFailureBehavior::Fallback,
            checkpointer_factory: Some(Arc::new(|| {
                Box::pin(async {
                    Ok::<Arc<dyn DatasetCheckpointer>, _>(Arc::new(NoopCheckpointer))
                })
            })),
        };

        let candidates =
            block_on(manager.list_snapshot_candidates()).expect("list snapshot candidates");
        let filenames: Vec<_> = candidates
            .iter()
            .map(|candidate| {
                candidate
                    .location
                    .filename()
                    .expect("snapshot object should have filename")
                    .to_string()
            })
            .collect();

        assert_eq!(
            filenames,
            vec![
                "dataset_20250201T000000Z.db".to_string(),
                "dataset_20250101T000000Z.db".to_string()
            ]
        );
    }

    #[test]
    fn list_snapshot_candidates_returns_latest_first() {
        let store = InMemory::new();

        block_on(async {
            store
                .put(
                    &Path::from("snapshots/month=2025-05/day=2025-05-10/dataset=dataset/dataset_20250510T120000Z.db"),
                    Bytes::from_static(b"latest").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from("snapshots/month=2025-04/day=2025-04-10/dataset=dataset/dataset_20250410T120000Z.db"),
                    Bytes::from_static(b"older").into(),
                )
                .await
                .expect("write snapshot file");
        });

        let manager = SnapshotManager {
            dataset_name: "dataset".to_string(),
            snapshots_location: Path::from("snapshots"),
            local_path: PathBuf::from("/tmp/unused.db"),
            object_store: Arc::new(store),
            bootstrap_failure_behavior: BootstrapOnFailureBehavior::Fallback,
            checkpointer_factory: Some(Arc::new(|| {
                Box::pin(async {
                    Ok::<Arc<dyn DatasetCheckpointer>, _>(Arc::new(NoopCheckpointer))
                })
            })),
        };

        let candidates =
            block_on(manager.list_snapshot_candidates()).expect("list snapshot candidates");

        let first = candidates.first().expect("expected at least one candidate");
        assert_eq!(first.display_timestamp, "20250510T120000Z");
    }

    #[test]
    fn list_snapshot_candidates_ignores_unparsable() {
        let store = InMemory::new();

        block_on(async {
            store
                .put(
                    &Path::from("snapshots/month=2025-10/day=2025-10-03/dataset=dataset/dataset_20251003T123312Z.db"),
                    Bytes::from_static(b"a").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from("snapshots/month=2025-10/day=2025-10-03/dataset=dataset/dataset_20251003T123421Z.db"),
                    Bytes::from_static(b"b").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from("snapshots/month=2025-09/day=2025-09-27/dataset=dataset/dataset_250927T13340914Z.db"),
                    Bytes::from_static(b"c").into(),
                )
                .await
                .expect("write snapshot file");
            store
                .put(
                    &Path::from("snapshots/month=2025-10/day=2025-10-03/dataset=other/other_20251003T123421Z.db"),
                    Bytes::from_static(b"d").into(),
                )
                .await
                .expect("write snapshot file");
        });

        let manager = SnapshotManager {
            dataset_name: "dataset".to_string(),
            snapshots_location: Path::from("snapshots"),
            local_path: PathBuf::from("/tmp/unused.db"),
            object_store: Arc::new(store),
            bootstrap_failure_behavior: BootstrapOnFailureBehavior::Fallback,
            checkpointer_factory: Some(Arc::new(|| {
                Box::pin(async {
                    Ok::<Arc<dyn DatasetCheckpointer>, _>(Arc::new(NoopCheckpointer))
                })
            })),
        };

        let candidates =
            block_on(manager.list_snapshot_candidates()).expect("list snapshot candidates");
        let filenames: Vec<_> = candidates
            .iter()
            .map(|candidate| {
                candidate
                    .location
                    .filename()
                    .expect("snapshot object should have filename")
                    .to_string()
            })
            .collect();

        assert_eq!(
            filenames,
            vec![
                "dataset_20251003T123421Z.db".to_string(),
                "dataset_20251003T123312Z.db".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn create_snapshot_streams_file_to_store() {
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        let contents = b"snapshot-bytes".to_vec();
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let manager = SnapshotManager {
            dataset_name: "dataset".to_string(),
            snapshots_location: Path::from("snapshots"),
            local_path: local_path.clone(),
            object_store: Arc::new(InMemory::new()),
            bootstrap_failure_behavior: BootstrapOnFailureBehavior::Fallback,
            checkpointer_factory: Some(Arc::new(|| {
                Box::pin(async {
                    Ok::<Arc<dyn DatasetCheckpointer>, _>(Arc::new(NoopCheckpointer))
                })
            })),
        };

        let uploaded_path = manager.create_snapshot().await.expect("upload snapshot");

        let filename = uploaded_path
            .filename()
            .expect("snapshot path includes filename");
        assert!(
            std::path::Path::new(filename)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("db"))
        );
        let layout = SnapshotPathLayout::new("dataset");
        assert!(
            layout.parse_filename_timestamp(filename).is_some(),
            "snapshot filename should contain a parsable timestamp"
        );

        let path_parts: Vec<_> = uploaded_path
            .parts()
            .map(|part| part.as_ref().to_string())
            .collect();
        assert!(
            path_parts.iter().any(|part| part.starts_with("month=")),
            "snapshot path missing month partition"
        );
        assert!(
            path_parts.iter().any(|part| part.starts_with("day=")),
            "snapshot path missing day partition"
        );
        assert!(
            path_parts.iter().any(|part| part == "dataset=dataset"),
            "snapshot path missing dataset partition"
        );

        let stored_bytes = manager
            .object_store
            .get(&uploaded_path)
            .await
            .expect("snapshot should exist")
            .bytes()
            .await
            .expect("read snapshot bytes");

        assert_eq!(stored_bytes.as_ref(), contents.as_slice());

        // Ensure the temp file path isn't dropped until the end of the test.
        drop(temp_path);
    }
}
