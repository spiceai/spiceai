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

use std::{
    collections::HashMap,
    fmt::Write,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Instant,
};

use arrow_schema::{Schema, SchemaRef};
use aws_sdk_credential_bridge::{S3CredentialProvider, get_bucket_name};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{
    ClientOptions, ObjectStore, PutMode, PutPayload, UpdateVersion, aws::AmazonS3Builder,
    client::SpawnedReqwestConnector, path::Path as ObjectPath,
};
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_secrets::{Secrets, get_params_with_secrets};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use spicepod::{component::snapshot::BootstrapOnFailureBehavior, param::Params};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    runtime::Handle,
    sync::RwLock,
};
use url::Url;
use util::{RetryError, fibonacci_backoff::FibonacciBackoff, retry};

use crate::dataset_checkpoint::DatasetCheckpointerFactory;

mod behavior;
pub mod metrics;
pub use behavior::SnapshotBehavior;

const SNAPSHOT_TIMESTAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";
const SNAPSHOT_MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const SNAPSHOT_METADATA_FORMAT_VERSION: u32 = 1;
const METADATA_FILE_NAME: &str = "metadata.json";
const SNAPSHOT_CHECKSUM_ALGORITHM: &str = "SHA256";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SnapshotMetadata {
    #[serde(rename = "format-version")]
    format_version: u32,
    location: String,
    #[serde(rename = "last-updated-ms")]
    last_updated_ms: i64,
    #[serde(flatten)]
    datasets: HashMap<String, DatasetMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DatasetMetadata {
    name: String,
    #[serde(default)]
    schemas: Vec<SchemaMetadata>,
    #[serde(rename = "current-schema-id")]
    current_schema_id: u64,
    #[serde(default)]
    snapshots: Vec<SnapshotEntry>,
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<u64>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaMetadata {
    #[serde(rename = "schema-id")]
    schema_id: u64,
    schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEntry {
    #[serde(rename = "snapshot-id")]
    snapshot_id: u64,
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    snapshot: String,
    #[serde(rename = "snapshot-checksum")]
    snapshot_checksum: String,
    #[serde(rename = "snapshot-checksum-algorithm")]
    snapshot_checksum_algorithm: String,
    #[serde(rename = "snapshot-size")]
    snapshot_size: u64,
}

impl SnapshotMetadata {
    fn empty(location: String, now_ms: i64) -> Self {
        Self {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location,
            last_updated_ms: now_ms,
            datasets: HashMap::new(),
        }
    }
}

impl DatasetMetadata {
    fn current_snapshot(&self) -> Option<&SnapshotEntry> {
        let current_id = self.current_snapshot_id?;
        self.snapshots
            .iter()
            .find(|entry| entry.snapshot_id == current_id)
    }

    fn current_schema(&self) -> Option<&SchemaMetadata> {
        self.schemas
            .iter()
            .find(|schema| schema.schema_id == self.current_schema_id)
    }
}

/// Details captured when downloading a snapshot for bootstrapping.
pub struct SnapshotDownloadInfo {
    pub schema: SchemaRef,
    pub bytes_downloaded: u64,
    pub checksum: String,
}

#[derive(Debug, Clone)]
struct MetadataHandle {
    metadata: SnapshotMetadata,
    version: Option<UpdateVersion>,
}

#[derive(Debug)]
enum MetadataLoadError {
    Read {
        path: String,
        source: object_store::Error,
    },
    Parse {
        path: String,
        source: serde_json::Error,
    },
    UnsupportedVersion {
        path: String,
        version: u32,
    },
}

impl SchemaMetadata {
    fn to_schema_ref(&self) -> Result<SchemaRef, serde_json::Error> {
        let schema: Schema = serde_json::from_value(self.schema.clone())?;
        Ok(Arc::new(schema))
    }

    fn from_schema(schema_id: u64, schema: &SchemaRef) -> Result<Self, serde_json::Error> {
        let schema_json = serde_json::to_value(schema)?;
        Ok(Self {
            schema_id,
            schema: schema_json,
        })
    }
}

#[derive(Debug, Snafu)]
pub enum SnapshotDownloadError {
    #[snafu(display("Dataset checkpointer factory not set for snapshot manager"))]
    CheckpointerFactoryNotSet,
    #[snafu(display("Failed to read snapshot metadata at {path}: {source}"))]
    ReadMetadata {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Snapshot metadata at {path} is invalid: {source}"))]
    ParseMetadata {
        path: String,
        source: serde_json::Error,
    },
    #[snafu(display("Snapshot metadata at {path} has unsupported format version {version}"))]
    UnsupportedMetadataVersion { path: String, version: u32 },
    #[snafu(display("Dataset {dataset} not present in snapshot metadata at {path}"))]
    DatasetNotFound { path: String, dataset: String },
    #[snafu(display("Dataset {dataset} has no current snapshot configured"))]
    CurrentSnapshotMissing { dataset: String },
    #[snafu(display("Dataset {dataset} snapshot id {snapshot_id} not found in metadata"))]
    SnapshotNotFound { dataset: String, snapshot_id: u64 },
    #[snafu(display("Snapshot URI {uri} is invalid: {source}"))]
    InvalidSnapshotUri {
        uri: String,
        source: url::ParseError,
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
    #[snafu(display("Snapshot {path} checksum mismatch. expected={expected} actual={actual}"))]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[snafu(display("Snapshot {path} uses unsupported checksum algorithm {algorithm}"))]
    UnsupportedChecksumAlgorithm { path: String, algorithm: String },
    #[snafu(display("Snapshot {path} size mismatch. expected={expected} actual={actual}"))]
    SizeMismatch {
        path: String,
        expected: u64,
        actual: u64,
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
    #[snafu(display("Snapshot schema for dataset {dataset} is missing in metadata"))]
    MetadataSchemaMissing { dataset: String },
    #[snafu(display("Failed to deserialize schema for dataset {dataset} from metadata: {source}"))]
    MetadataSchemaDeserialize {
        dataset: String,
        source: serde_json::Error,
    },
    #[snafu(display("Snapshot schema mismatch for dataset {dataset}"))]
    SchemaMismatch { dataset: String },
}

impl From<MetadataLoadError> for SnapshotDownloadError {
    fn from(err: MetadataLoadError) -> Self {
        match err {
            MetadataLoadError::Read { path, source } => {
                SnapshotDownloadError::ReadMetadata { path, source }
            }
            MetadataLoadError::Parse { path, source } => {
                SnapshotDownloadError::ParseMetadata { path, source }
            }
            MetadataLoadError::UnsupportedVersion { path, version } => {
                SnapshotDownloadError::UnsupportedMetadataVersion { path, version }
            }
        }
    }
}

impl From<MetadataLoadError> for SnapshotUploadError {
    fn from(err: MetadataLoadError) -> Self {
        match err {
            MetadataLoadError::Read { path, source } => {
                SnapshotUploadError::UploadReadMetadata { path, source }
            }
            MetadataLoadError::Parse { path, source } => {
                SnapshotUploadError::UploadParseMetadata { path, source }
            }
            MetadataLoadError::UnsupportedVersion { path, version } => {
                SnapshotUploadError::UploadUnsupportedMetadataVersion { path, version }
            }
        }
    }
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
    #[snafu(display("Failed to serialize schema for dataset {dataset}: {source}"))]
    UploadSchemaSerialize {
        dataset: String,
        source: serde_json::Error,
    },
    #[snafu(display("Failed to read snapshot metadata at {path}: {source}"))]
    UploadReadMetadata {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Snapshot metadata at {path} is invalid: {source}"))]
    UploadParseMetadata {
        path: String,
        source: serde_json::Error,
    },
    #[snafu(display("Snapshot metadata at {path} has unsupported format version {version}"))]
    UploadUnsupportedMetadataVersion { path: String, version: u32 },
    #[snafu(display("Failed to write snapshot metadata to {path}: {source}"))]
    UploadWriteMetadata {
        path: String,
        source: object_store::Error,
    },
    #[snafu(display("Failed to serialize snapshot metadata at {path}: {source}"))]
    UploadSerializeMetadata {
        path: String,
        source: serde_json::Error,
    },
    #[snafu(display("Snapshot metadata schema for dataset {dataset} is invalid: {source}"))]
    UploadMetadataSchemaDeserialize {
        dataset: String,
        source: serde_json::Error,
    },
    #[snafu(display("Snapshot metadata schema for dataset {dataset} is missing"))]
    UploadMetadataSchemaMissing { dataset: String },
    #[snafu(display("Snapshot metadata schema conflict for dataset {dataset}"))]
    UploadSchemaMismatch { dataset: String },
}

#[derive(Debug, Clone, Copy)]
struct SnapshotPathLayout<'a> {
    dataset_name: &'a str,
}

impl<'a> SnapshotPathLayout<'a> {
    fn new(dataset_name: &'a str) -> Self {
        Self { dataset_name }
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

    fn build_location(&self, base: &ObjectPath, instant: DateTime<Utc>) -> ObjectPath {
        let month_partition = format!("month={}", instant.format("%Y-%m"));
        let day_partition = format!("day={}", instant.format("%Y-%m-%d"));
        let dataset_partition = self.dataset_partition_raw();
        base.child(month_partition)
            .child(day_partition)
            .child(dataset_partition)
            .child(self.snapshot_filename(instant))
    }
}

/// Manages snapshots for a specific accelerated dataset.
#[derive(Clone)]
pub struct SnapshotManager {
    dataset_name: String,
    snapshots_location: object_store::path::Path,
    snapshot_location_uri: String,
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
            .field("snapshot_location_uri", &self.snapshot_location_uri)
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
    fn metadata_path(&self) -> ObjectPath {
        self.snapshots_location.child(METADATA_FILE_NAME)
    }

    fn metadata_path_display(&self) -> String {
        self.metadata_path().to_string()
    }

    fn snapshot_uri_for_location(&self, location: &ObjectPath) -> String {
        let base = self.snapshot_location_uri.trim_end_matches('/').to_string();
        let relative = location
            .prefix_match(&self.snapshots_location)
            .map(|parts| {
                parts
                    .map(|p| p.as_ref().to_string())
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .filter(|rel| !rel.is_empty());

        match (base.is_empty(), relative) {
            (true, Some(rel)) => rel,
            (true, None) => location.to_string(),
            (false, Some(rel)) => format!("{base}/{rel}"),
            (false, None) => base,
        }
    }

    async fn load_metadata(&self) -> Result<Option<MetadataHandle>, MetadataLoadError> {
        let metadata_path = self.metadata_path();
        let metadata_path_display = metadata_path.to_string();

        let get_result = match self.object_store.get(&metadata_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(source) => {
                return Err(MetadataLoadError::Read {
                    path: metadata_path_display,
                    source,
                });
            }
        };

        let meta = get_result.meta.clone();
        let bytes = get_result
            .bytes()
            .await
            .map_err(|source| MetadataLoadError::Read {
                path: metadata_path_display.clone(),
                source,
            })?;

        let metadata: SnapshotMetadata =
            serde_json::from_slice(&bytes).map_err(|source| MetadataLoadError::Parse {
                path: metadata_path_display.clone(),
                source,
            })?;

        if metadata.format_version != SNAPSHOT_METADATA_FORMAT_VERSION {
            return Err(MetadataLoadError::UnsupportedVersion {
                path: metadata_path_display,
                version: metadata.format_version,
            });
        }

        let version = if meta.e_tag.is_some() || meta.version.is_some() {
            Some(UpdateVersion {
                e_tag: meta.e_tag.clone(),
                version: meta.version.clone(),
            })
        } else {
            None
        };

        Ok(Some(MetadataHandle { metadata, version }))
    }

    pub async fn try_new(
        dataset_name: String,
        snapshots: SnapshotBehavior,
        local_path: PathBuf,
    ) -> Option<Self> {
        let (snapshot_config, secrets, io_runtime) = match snapshots {
            SnapshotBehavior::Disabled => {
                tracing::debug!("Snapshots are disabled for {dataset_name}");
                return None;
            }
            SnapshotBehavior::Enabled(s, secrets, io_runtime)
            | SnapshotBehavior::BootstrapOnly(s, secrets, io_runtime)
            | SnapshotBehavior::CreateOnly(s, secrets, io_runtime) => {
                (s, secrets.upgrade()?, io_runtime)
            }
        };
        tracing::debug!("Snapshots are enabled for {dataset_name}");

        let Some(snapshot_location) = &snapshot_config.location else {
            tracing::warn!(
                "Snapshots are enabled for dataset {dataset_name} but no location is configured"
            );
            return None;
        };
        let snapshot_location_uri = snapshot_location.clone();

        let snapshots_location_url = match Url::from_str(snapshot_location) {
            Ok(url) => url,
            Err(e) => {
                tracing::error!(
                    "Failed to parse snapshot location URL: {snapshot_location}, error: {e}"
                );
                return None;
            }
        };

        let (store, path) = match (
            snapshots_location_url.scheme(),
            snapshots_location_url.path(),
        ) {
            ("s3", path) => {
                let store = build_s3_object_store(
                    &snapshots_location_url,
                    secrets,
                    snapshot_config.params.as_ref().map(Params::as_string_map),
                    io_runtime,
                )
                .await
                .inspect_err(|e| {
                    tracing::error!("Error connecting to S3 snapshot location: {e}");
                })
                .ok()?;
                let path = object_store::path::Path::from(path);
                (store, path)
            }
            _ => object_store::parse_url(&snapshots_location_url).ok()?,
        };

        Some(Self {
            dataset_name,
            snapshots_location: path,
            snapshot_location_uri,
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
    pub async fn create_snapshot(
        &self,
        schema: &SchemaRef,
    ) -> Result<ObjectPath, SnapshotUploadError> {
        let start_time = Instant::now();
        let now = Utc::now();
        let layout = SnapshotPathLayout::new(&self.dataset_name);
        let location = layout.build_location(&self.snapshots_location, now);
        let location_path = location.to_string();
        let local_path = self.local_path.clone();
        let timestamp_ms = now.timestamp_millis();

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
        let mut hasher = Sha256::new();

        while !eof || !buffer.is_empty() {
            while buffer.len() < SNAPSHOT_MULTIPART_CHUNK_SIZE && !eof {
                let previous_len = buffer.len();
                match reader.read_buf(&mut buffer).await {
                    Ok(0) => {
                        eof = true;
                    }
                    Ok(read) => {
                        total_bytes += read as u64;
                        let new_len = buffer.len();
                        hasher.update(&buffer[previous_len..new_len]);
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
                let checksum_bytes = hasher.finalize();
                let checksum = encode_hex_lower(checksum_bytes.as_ref());

                self.update_metadata_after_upload(
                    &location,
                    checksum.clone(),
                    total_bytes,
                    timestamp_ms,
                    schema,
                )
                .await?;

                let duration_ms = start_time.elapsed().as_secs_f64() * 1000.0;
                metrics::record_write_metrics(
                    &self.dataset_name,
                    timestamp_ms / 1000,
                    duration_ms,
                    total_bytes,
                    &checksum,
                );

                tracing::info!(
                    "Snapshot uploaded. dataset={} snapshot={location} size={total_bytes} sha={checksum}",
                    self.dataset_name,
                    checksum = checksum.as_str(),
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

    /// Attempts to download the latest snapshot, returning details if successful.
    ///
    /// # Errors
    ///
    /// - If there is an error communicating with the object store.
    /// - If there is an error writing the snapshot to the local filesystem.
    /// - If there is an error initializing the dataset checkpointer.
    /// - If there is an error fetching the schema from the dataset checkpointer.
    pub async fn download_latest_snapshot(
        &self,
    ) -> Result<Option<SnapshotDownloadInfo>, SnapshotDownloadError> {
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
    ) -> Result<Option<SnapshotDownloadInfo>, SnapshotDownloadError> {
        let metadata_handle = match self.load_metadata().await {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                let location_display = self.snapshots_location.to_string();
                tracing::debug!(
                    "No snapshot metadata found; continuing without bootstrapping. dataset={} location={location_display}",
                    self.dataset_name
                );
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };

        let metadata_path_display = self.metadata_path_display();
        let Some(dataset_metadata) = metadata_handle
            .metadata
            .datasets
            .get(&self.dataset_name)
            .cloned()
        else {
            tracing::debug!(
                "Dataset not present in snapshot metadata; continuing without bootstrapping. dataset={} metadata={metadata_path_display}",
                self.dataset_name
            );
            return Ok(None);
        };

        let Some(current_entry) = dataset_metadata.current_snapshot().cloned() else {
            tracing::debug!(
                "Dataset metadata missing current snapshot pointer; continuing without bootstrapping. dataset={} metadata={metadata_path_display}",
                self.dataset_name
            );
            return Ok(None);
        };

        tracing::info!(
            "Downloading current snapshot. dataset={} snapshot={} sha={sha}",
            self.dataset_name,
            current_entry.snapshot,
            sha = current_entry.snapshot_checksum.as_str(),
        );
        self.download_snapshot_entry(&current_entry, &dataset_metadata, checkpointer_factory)
            .await
            .map(Some)
    }

    async fn download_with_fallback(
        &self,
        checkpointer_factory: DatasetCheckpointerFactory,
    ) -> Result<Option<SnapshotDownloadInfo>, SnapshotDownloadError> {
        let metadata_handle = match self.load_metadata().await {
            Ok(Some(handle)) => handle,
            Ok(None) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let Some(dataset_metadata) = metadata_handle
            .metadata
            .datasets
            .get(&self.dataset_name)
            .cloned()
        else {
            return Ok(None);
        };

        if dataset_metadata.snapshots.is_empty() {
            return Ok(None);
        }

        let mut ordered_snapshots = Vec::new();
        if let Some(current) = dataset_metadata.current_snapshot().cloned() {
            ordered_snapshots.push(current);
        }
        let mut remaining: Vec<_> = dataset_metadata
            .snapshots
            .iter()
            .filter(|entry| Some(entry.snapshot_id) != dataset_metadata.current_snapshot_id)
            .cloned()
            .collect();
        remaining.sort_by(|a, b| b.snapshot_id.cmp(&a.snapshot_id));
        ordered_snapshots.extend(remaining);

        for snapshot in ordered_snapshots {
            match self
                .download_snapshot_entry(
                    &snapshot,
                    &dataset_metadata,
                    Arc::clone(&checkpointer_factory),
                )
                .await
            {
                Ok(schema) => return Ok(Some(schema)),
                Err(err) => match err {
                    SnapshotDownloadError::MissingSchema { ref path }
                    | SnapshotDownloadError::ChecksumMismatch { ref path, .. }
                    | SnapshotDownloadError::SizeMismatch { ref path, .. }
                    | SnapshotDownloadError::UnsupportedChecksumAlgorithm { ref path, .. } => {
                        tracing::warn!(
                            "Snapshot integrity issue; attempting next available snapshot. dataset={} snapshot={path} sha={sha}",
                            self.dataset_name,
                            sha = snapshot.snapshot_checksum.as_str(),
                        );
                    }
                    SnapshotDownloadError::SchemaMismatch { .. } => {
                        tracing::warn!(
                            "Snapshot schema mismatch; attempting next available snapshot. dataset={} snapshot={} sha={sha}",
                            self.dataset_name,
                            snapshot.snapshot,
                            sha = snapshot.snapshot_checksum.as_str(),
                        );
                    }
                    SnapshotDownloadError::InvalidSnapshotUri { ref uri, .. } => {
                        tracing::warn!(
                            "Snapshot URI invalid; attempting next available snapshot. dataset={} snapshot={uri} sha={sha}",
                            self.dataset_name,
                            sha = snapshot.snapshot_checksum.as_str(),
                        );
                    }
                    other => {
                        tracing::warn!(
                            "Failed to download snapshot while attempting fallback. dataset={} snapshot={} sha={sha} error={other}",
                            self.dataset_name,
                            snapshot.snapshot,
                            sha = snapshot.snapshot_checksum.as_str(),
                        );
                        return Err(other);
                    }
                },
            }
        }

        tracing::warn!(
            "All available snapshots failed validation; continuing without bootstrapping. dataset={}",
            self.dataset_name
        );

        Ok(None)
    }

    fn snapshot_uri_to_object_path(&self, uri: &str) -> Result<ObjectPath, SnapshotDownloadError> {
        let base_uri = self.snapshot_location_uri.trim_end_matches('/');
        if let Some(relative) = uri.strip_prefix(base_uri) {
            let relative = relative.trim_start_matches('/');
            let combined = if relative.is_empty() {
                self.snapshots_location.to_string()
            } else {
                format!("{}/{}", self.snapshots_location, relative)
            };
            return Ok(ObjectPath::from(combined));
        }

        match Url::parse(uri) {
            Ok(parsed_uri) => {
                let mut combined = self.snapshots_location.to_string();
                if let Some(host) = parsed_uri.host_str() {
                    combined = format!("{combined}/{host}");
                }
                let path = parsed_uri.path().trim_start_matches('/').trim();
                if path.is_empty() {
                    Ok(ObjectPath::from(combined))
                } else {
                    Ok(ObjectPath::from(format!("{combined}/{path}")))
                }
            }
            Err(parse_err) => {
                if uri.contains("://") {
                    Err(SnapshotDownloadError::InvalidSnapshotUri {
                        uri: uri.to_string(),
                        source: parse_err,
                    })
                } else {
                    Ok(ObjectPath::from(uri))
                }
            }
        }
    }

    async fn download_snapshot_entry(
        &self,
        entry: &SnapshotEntry,
        dataset_metadata: &DatasetMetadata,
        checkpointer_factory: DatasetCheckpointerFactory,
    ) -> Result<SnapshotDownloadInfo, SnapshotDownloadError> {
        let object_path = self.snapshot_uri_to_object_path(&entry.snapshot)?;
        let path_display = object_path.to_string();

        let get_result = self
            .object_store
            .get(&object_path)
            .await
            .map_err(|source| SnapshotDownloadError::Download {
                path: path_display.clone(),
                source,
            })?;

        tracing::info!(
            "Downloading snapshot. dataset={} snapshot={} snapshot_id={} sha={sha}",
            self.dataset_name,
            entry.snapshot,
            entry.snapshot_id,
            sha = entry.snapshot_checksum.as_str(),
        );

        if let Some(parent) = self.local_path.parent() {
            fs::create_dir_all(parent).await.map_err(|source| {
                SnapshotDownloadError::CreateLocalDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }

        let mut stream = get_result.into_stream();
        let mut file = fs::File::create(&self.local_path).await.map_err(|source| {
            SnapshotDownloadError::WriteLocal {
                path: self.local_path.clone(),
                source,
            }
        })?;

        let mut hasher = Sha256::new();
        let mut actual_size: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(chunk) => chunk,
                Err(source) => {
                    let _ = fs::remove_file(&self.local_path).await;
                    return Err(SnapshotDownloadError::DownloadBytes {
                        path: path_display.clone(),
                        source,
                    });
                }
            };

            actual_size += chunk.len() as u64;
            hasher.update(&chunk);

            if let Err(source) = file.write_all(&chunk).await {
                let _ = fs::remove_file(&self.local_path).await;
                return Err(SnapshotDownloadError::WriteLocal {
                    path: self.local_path.clone(),
                    source,
                });
            }
        }

        if let Err(source) = file.flush().await {
            let _ = fs::remove_file(&self.local_path).await;
            return Err(SnapshotDownloadError::WriteLocal {
                path: self.local_path.clone(),
                source,
            });
        }
        drop(file);

        if entry.snapshot_size != actual_size {
            let _ = fs::remove_file(&self.local_path).await;
            return Err(SnapshotDownloadError::SizeMismatch {
                path: path_display.clone(),
                expected: entry.snapshot_size,
                actual: actual_size,
            });
        }

        if !entry
            .snapshot_checksum_algorithm
            .eq_ignore_ascii_case(SNAPSHOT_CHECKSUM_ALGORITHM)
        {
            let _ = fs::remove_file(&self.local_path).await;
            return Err(SnapshotDownloadError::UnsupportedChecksumAlgorithm {
                path: path_display.clone(),
                algorithm: entry.snapshot_checksum_algorithm.clone(),
            });
        }

        let checksum_bytes = hasher.finalize();
        let actual_checksum = encode_hex_lower(checksum_bytes.as_ref());
        let expected_checksum = entry.snapshot_checksum.to_lowercase();
        if expected_checksum != actual_checksum {
            let _ = fs::remove_file(&self.local_path).await;
            return Err(SnapshotDownloadError::ChecksumMismatch {
                path: path_display.clone(),
                expected: entry.snapshot_checksum.clone(),
                actual: actual_checksum,
            });
        }

        let checkpointer = (checkpointer_factory)()
            .await
            .map_err(|source| SnapshotDownloadError::CheckpointerInit { source })?;

        let metadata_schema = dataset_metadata
            .current_schema()
            .ok_or_else(|| SnapshotDownloadError::MetadataSchemaMissing {
                dataset: self.dataset_name.clone(),
            })?
            .to_schema_ref()
            .map_err(|source| SnapshotDownloadError::MetadataSchemaDeserialize {
                dataset: self.dataset_name.clone(),
                source,
            })?;

        if let Some(schema) = checkpointer
            .get_schema()
            .await
            .map_err(|source| SnapshotDownloadError::CheckpointerSchema { source })?
        {
            if schema.as_ref() != metadata_schema.as_ref() {
                return Err(SnapshotDownloadError::SchemaMismatch {
                    dataset: self.dataset_name.clone(),
                });
            }

            let local_path_display = self.local_path.display();
            tracing::info!(
                "Snapshot downloaded to {local_path_display}. dataset={} snapshot={} size={actual_size} sha={sha}",
                self.dataset_name,
                entry.snapshot,
                sha = actual_checksum.as_str(),
            );
            Ok(SnapshotDownloadInfo {
                schema,
                bytes_downloaded: actual_size,
                checksum: actual_checksum,
            })
        } else {
            tracing::warn!(
                "Snapshot schema not found. dataset={} snapshot={} sha={sha}",
                self.dataset_name,
                entry.snapshot,
                sha = entry.snapshot_checksum.as_str(),
            );
            Err(SnapshotDownloadError::MissingSchema { path: path_display })
        }
    }

    async fn update_metadata_after_upload(
        &self,
        location: &ObjectPath,
        checksum: String,
        size: u64,
        timestamp_ms: i64,
        schema: &SchemaRef,
    ) -> Result<(), SnapshotUploadError> {
        let metadata_path = self.metadata_path();
        let metadata_path_display = metadata_path.to_string();
        let dataset_name = self.dataset_name.clone();
        let snapshot_uri = self.snapshot_uri_for_location(location);

        // Retry loop to handle precondition failures due to concurrent updates.
        loop {
            let handle = self
                .load_metadata()
                .await
                .map_err(SnapshotUploadError::from)?;

            let now_ms = Utc::now().timestamp_millis();
            let mut metadata = if let Some(existing) = handle.as_ref() {
                existing.metadata.clone()
            } else {
                SnapshotMetadata::empty(self.snapshot_location_uri.clone(), now_ms)
            };

            if metadata.location.is_empty() {
                metadata.location.clone_from(&self.snapshot_location_uri);
            }
            metadata.last_updated_ms = now_ms;

            let dataset_entry = metadata
                .datasets
                .entry(dataset_name.clone())
                .or_insert_with(|| DatasetMetadata {
                    name: dataset_name.clone(),
                    ..Default::default()
                });
            dataset_entry.name.clone_from(&dataset_name);

            if dataset_entry.schemas.is_empty() {
                let schema_metadata = SchemaMetadata::from_schema(0, schema).map_err(|source| {
                    SnapshotUploadError::UploadSchemaSerialize {
                        dataset: dataset_name.clone(),
                        source,
                    }
                })?;
                dataset_entry.schemas.push(schema_metadata);
                dataset_entry.current_schema_id = 0;
            } else {
                let metadata_schema = dataset_entry
                    .current_schema()
                    .ok_or_else(|| SnapshotUploadError::UploadMetadataSchemaMissing {
                        dataset: dataset_name.clone(),
                    })?
                    .to_schema_ref()
                    .map_err(
                        |source| SnapshotUploadError::UploadMetadataSchemaDeserialize {
                            dataset: dataset_name.clone(),
                            source,
                        },
                    )?;

                if metadata_schema.as_ref() != schema.as_ref() {
                    return Err(SnapshotUploadError::UploadSchemaMismatch {
                        dataset: dataset_name.clone(),
                    });
                }
            }

            let next_snapshot_id = dataset_entry
                .snapshots
                .iter()
                .map(|entry| entry.snapshot_id)
                .max()
                .map_or(0, |max_id| max_id + 1);

            let checksum_for_metadata = checksum.clone();
            let snapshot_entry = SnapshotEntry {
                snapshot_id: next_snapshot_id,
                timestamp_ms,
                snapshot: snapshot_uri.clone(),
                snapshot_checksum: checksum_for_metadata,
                snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
                snapshot_size: size,
            };

            dataset_entry.snapshots.push(snapshot_entry);
            dataset_entry.current_snapshot_id = Some(next_snapshot_id);

            let serialized = serde_json::to_vec_pretty(&metadata).map_err(|source| {
                SnapshotUploadError::UploadSerializeMetadata {
                    path: metadata_path_display.clone(),
                    source,
                }
            })?;

            let version = handle.as_ref().and_then(|h| h.version.clone());
            let put_mode = match (handle.is_some(), version) {
                (false, _) => PutMode::Create,
                (true, Some(version)) => PutMode::Update(version),
                (true, None) => PutMode::Overwrite,
            };

            let payload = PutPayload::from(serialized);

            match self
                .object_store
                .put_opts(&metadata_path, payload.clone(), put_mode.clone().into())
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::AlreadyExists { .. })
                    if matches!(put_mode, PutMode::Create) => {}
                Err(object_store::Error::Precondition { .. }) => {}
                Err(object_store::Error::NotSupported { .. })
                    if matches!(put_mode, PutMode::Update(_)) =>
                {
                    match self
                        .object_store
                        .put_opts(&metadata_path, payload, PutMode::Overwrite.into())
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(err) => {
                            return Err(SnapshotUploadError::UploadWriteMetadata {
                                path: metadata_path_display.clone(),
                                source: err,
                            });
                        }
                    }
                }
                Err(err) => {
                    return Err(SnapshotUploadError::UploadWriteMetadata {
                        path: metadata_path_display.clone(),
                        source: err,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
fn compute_sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    encode_hex_lower(digest.as_ref())
}

fn encode_hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

static S3_PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    vec![
        ParameterSpec::component("region").secret(),
        ParameterSpec::component("endpoint").secret(),
        ParameterSpec::component("key").secret(),
        ParameterSpec::component("secret").secret(),
        ParameterSpec::component("session_token").secret(),
        ParameterSpec::component("auth")
            .description("Configures the authentication method for S3. Supported methods are: iam_role, key.")
            .default("iam_role")
            .one_of(&["iam_role", "key"])
            .secret(),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for S3 client."),
        ParameterSpec::runtime("allow_http")
            .description("Allow HTTP protocol for S3 endpoint.")
    ]
});

#[derive(Debug, Snafu)]
enum S3ObjectStoreError {
    #[snafu(transparent)]
    InvalidBucketName {
        source: aws_sdk_credential_bridge::Error,
    },

    #[snafu(display("Unable to parse client_timeout: {source}"))]
    ClientTimeoutParse { source: fundu::ParseError },

    #[snafu(display("Unexpected S3 auth method: {method}"))]
    UnexpectedS3AuthMethod { method: String },

    #[snafu(display("Unable to load S3 credentials from environment: {source}"))]
    EnvLoad {
        source: aws_sdk_credential_bridge::Error,
    },

    #[snafu(transparent)]
    ObjectStore { source: object_store::Error },
}

async fn build_s3_object_store(
    snapshots_url: &Url,
    secrets: Arc<RwLock<Secrets>>,
    params: Option<HashMap<String, String>>,
    io_runtime: Handle,
) -> Result<Box<dyn ObjectStore>, S3ObjectStoreError> {
    let s3_params = build_s3_parameters(Arc::clone(&secrets), params.as_ref()).await;

    let s3_region = s3_params.get("region").expose().ok();
    let allow_http = s3_params
        .get("allow_http")
        .expose()
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(false);
    let s3_endpoint = s3_params.get("endpoint").expose().ok();
    let client_timeout = s3_params.get("client_timeout").expose().ok();
    let bucket_name = get_bucket_name(snapshots_url)?;

    let mut s3_builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
        .with_allow_http(allow_http);
    let mut client_options = ClientOptions::default();

    if let Some(region) = s3_region {
        s3_builder = s3_builder.with_region(region);
    }
    if let Some(endpoint) = s3_endpoint {
        s3_builder = s3_builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            client_options = client_options.with_allow_http(true);
        }
    }
    if let Some(timeout) = client_timeout {
        client_options = client_options
            .with_timeout(fundu::parse_duration(timeout).context(ClientTimeoutParseSnafu)?);
    }
    let mut load_credentials_from_environment = true;

    if let (Some(key), Some(secret)) = (
        s3_params.get("key").expose().ok(),
        s3_params.get("secret").expose().ok(),
    ) {
        s3_builder = s3_builder.with_access_key_id(key);
        s3_builder = s3_builder.with_secret_access_key(secret);
        if let Some(token) = s3_params.get("session_token").expose().ok() {
            s3_builder = s3_builder.with_token(token);
        }
        load_credentials_from_environment = false;
    }
    s3_builder = s3_builder.with_client_options(client_options);

    if load_credentials_from_environment {
        tracing::trace!("Loading S3 credentials from environment");
        match aws_sdk_credential_bridge::get_or_init_sdk_config().await {
            Ok(Some(sdk_config)) => {
                if sdk_config.credentials_provider().is_some() {
                    tracing::trace!("Using S3 credentials provider from SDK config");
                    s3_builder = s3_builder.with_credentials(Arc::new(
                        S3CredentialProvider::from_config(sdk_config.as_ref())
                            .context(EnvLoadSnafu)?,
                    ));
                }
            }
            Ok(None) => {
                tracing::trace!(
                    "No AWS SDK credentials available for snapshot store; assuming public access"
                );
            }
            Err(err) => {
                tracing::warn!("Unable to initialize AWS credentials for snapshot store: {err}");
            }
        }
    }

    Ok(Box::new(s3_builder.build()?))
}

async fn build_s3_parameters(
    secrets: Arc<RwLock<Secrets>>,
    params: Option<&HashMap<String, String>>,
) -> Parameters {
    let default_params = || Parameters::new(vec![], "s3", &S3_PARAMETERS);
    match params {
        Some(p) => {
            let secret_params = get_params_with_secrets(Arc::clone(&secrets), p).await;
            Parameters::try_new(
                "snapshot",
                secret_params.into_iter().collect(),
                "s3",
                secrets,
                &S3_PARAMETERS,
            )
            .await
            .unwrap_or_else(|_| default_params())
        }
        None => default_params(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset_checkpoint::{DatasetCheckpointer, Result as DatasetCheckpointResult};
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::{memory::InMemory, path::Path};
    use std::{io::Write, path::PathBuf, sync::Arc, time::SystemTime};
    use tempfile::{NamedTempFile, TempDir};
    use tokio::fs;

    const DATASET_NAME: &str = "dataset";
    const SNAPSHOT_URI_PREFIX: &str = "memory://snapshots";
    const SNAPSHOT_BASE_PATH: &str = "snapshots";

    #[derive(Clone)]
    struct StaticSchemaCheckpointer {
        schema: SchemaRef,
    }

    #[async_trait]
    impl DatasetCheckpointer for StaticSchemaCheckpointer {
        async fn exists(&self) -> bool {
            true
        }

        async fn checkpoint(&self, _schema: &SchemaRef) -> DatasetCheckpointResult<()> {
            Ok(())
        }

        async fn get_schema(&self) -> DatasetCheckpointResult<Option<SchemaRef>> {
            Ok(Some(Arc::clone(&self.schema)))
        }

        async fn last_checkpoint_time(&self) -> DatasetCheckpointResult<Option<SystemTime>> {
            Ok(None)
        }
    }

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn build_manager(
        store: Arc<InMemory>,
        local_path: PathBuf,
        behavior: BootstrapOnFailureBehavior,
        schema: &SchemaRef,
    ) -> SnapshotManager {
        let schema_for_factory = Arc::clone(schema);
        let factory: DatasetCheckpointerFactory = Arc::new(move || {
            let schema = Arc::clone(&schema_for_factory);
            Box::pin(async move {
                Ok::<Arc<dyn DatasetCheckpointer>, _>(Arc::new(StaticSchemaCheckpointer { schema }))
            })
        });

        let object_store: Arc<dyn ObjectStore> = store;

        SnapshotManager {
            dataset_name: DATASET_NAME.to_string(),
            snapshots_location: Path::from(SNAPSHOT_BASE_PATH),
            snapshot_location_uri: SNAPSHOT_URI_PREFIX.to_string(),
            local_path,
            object_store,
            bootstrap_failure_behavior: behavior,
            checkpointer_factory: Some(factory),
        }
    }

    async fn write_metadata(store: &InMemory, metadata_path: &Path, metadata: &SnapshotMetadata) {
        let bytes = serde_json::to_vec_pretty(metadata).expect("serialize metadata");
        store
            .put(metadata_path, bytes.into())
            .await
            .expect("write metadata");
    }

    fn snapshot_uri(location: &ObjectPath) -> String {
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let relative = location.prefix_match(&base).map_or_else(
            || location.to_string(),
            |parts| {
                parts
                    .map(|p| p.as_ref().to_owned())
                    .collect::<Vec<_>>()
                    .join("/")
            },
        );
        format!("{SNAPSHOT_URI_PREFIX}/{relative}")
    }

    fn dataset_metadata(
        schema: &SchemaRef,
        snapshots: Vec<SnapshotEntry>,
        current_snapshot_id: Option<u64>,
    ) -> DatasetMetadata {
        DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots,
            current_snapshot_id,
            properties: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn download_latest_snapshot_returns_none_without_metadata() {
        let store = Arc::new(InMemory::new());
        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");
        let schema = sample_schema();

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );

        let result = manager
            .download_latest_snapshot()
            .await
            .expect("download should succeed");

        assert!(result.is_none());
        assert!(!local_path.exists());
    }

    #[tokio::test]
    async fn download_latest_snapshot_downloads_current_snapshot() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);
        let instant = Utc
            .with_ymd_and_hms(2025, 1, 2, 3, 4, 5)
            .single()
            .expect("valid time");
        let location = layout.build_location(&base, instant);

        let contents = Bytes::from_static(b"snapshot-bytes");
        store
            .put(&location, contents.clone().into())
            .await
            .expect("write snapshot");

        let checksum = compute_sha256_hex(contents.as_ref());
        let snapshot_entry = SnapshotEntry {
            snapshot_id: 0,
            timestamp_ms: instant.timestamp_millis(),
            snapshot: snapshot_uri(&location),
            snapshot_checksum: checksum.clone(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: contents.len() as u64,
        };

        let schema = sample_schema();
        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: Utc::now().timestamp_millis(),
            datasets: HashMap::from([(
                DATASET_NAME.to_string(),
                dataset_metadata(&schema, vec![snapshot_entry], Some(0)),
            )]),
        };

        let metadata_path = base.child(METADATA_FILE_NAME);
        write_metadata(&store, &metadata_path, &metadata).await;

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );

        let info = manager
            .download_latest_snapshot()
            .await
            .expect("download should succeed")
            .expect("expected snapshot");

        assert_eq!(info.schema.as_ref(), schema.as_ref());
        assert_eq!(info.bytes_downloaded, contents.len() as u64);
        assert_eq!(info.checksum, checksum);
        let downloaded = fs::read(&local_path)
            .await
            .expect("read downloaded snapshot");
        assert_eq!(downloaded.as_slice(), contents.as_ref());
    }

    #[tokio::test]
    async fn download_with_fallback_uses_next_snapshot_on_integrity_failure() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);

        let first_instant = Utc
            .with_ymd_and_hms(2025, 2, 1, 0, 0, 0)
            .single()
            .expect("valid time");
        let first_location = layout.build_location(&base, first_instant);
        let first_contents = Bytes::from_static(b"invalid-bytes");
        store
            .put(&first_location, first_contents.clone().into())
            .await
            .expect("write snapshot");

        let second_instant = Utc
            .with_ymd_and_hms(2025, 1, 20, 0, 0, 0)
            .single()
            .expect("valid time");
        let second_location = layout.build_location(&base, second_instant);
        let second_contents = Bytes::from_static(b"valid-snapshot");
        store
            .put(&second_location, second_contents.clone().into())
            .await
            .expect("write snapshot");

        let broken_snapshot = SnapshotEntry {
            snapshot_id: 1,
            timestamp_ms: first_instant.timestamp_millis(),
            snapshot: snapshot_uri(&first_location),
            snapshot_checksum: "0000".to_string(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: first_contents.len() as u64,
        };

        let valid_checksum = compute_sha256_hex(second_contents.as_ref());
        let valid_snapshot = SnapshotEntry {
            snapshot_id: 0,
            timestamp_ms: second_instant.timestamp_millis(),
            snapshot: snapshot_uri(&second_location),
            snapshot_checksum: valid_checksum.clone(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: second_contents.len() as u64,
        };

        let schema = sample_schema();
        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: Utc::now().timestamp_millis(),
            datasets: HashMap::from([(
                DATASET_NAME.to_string(),
                dataset_metadata(&schema, vec![broken_snapshot, valid_snapshot], Some(1)),
            )]),
        };

        let metadata_path = base.child(METADATA_FILE_NAME);
        write_metadata(&store, &metadata_path, &metadata).await;

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Fallback,
            &schema,
        );

        let info = manager
            .download_latest_snapshot()
            .await
            .expect("download should succeed")
            .expect("expected snapshot");

        assert_eq!(info.schema.as_ref(), schema.as_ref());
        assert_eq!(info.bytes_downloaded, second_contents.len() as u64);
        assert_eq!(info.checksum, valid_checksum);
        let downloaded = fs::read(&local_path)
            .await
            .expect("read downloaded snapshot");
        assert_eq!(downloaded.as_slice(), second_contents.as_ref());
    }

    #[tokio::test]
    async fn create_snapshot_streams_file_and_updates_metadata() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-bytes".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();
        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Fallback,
            &schema,
        );

        let uploaded_path = manager
            .create_snapshot(&schema)
            .await
            .expect("create snapshot");

        let stored_bytes = store
            .get(&uploaded_path)
            .await
            .expect("snapshot stored")
            .bytes()
            .await
            .expect("read stored snapshot");
        assert_eq!(stored_bytes.as_ref(), contents.as_slice());

        let metadata_path = Path::from(SNAPSHOT_BASE_PATH).child(METADATA_FILE_NAME);
        let metadata_bytes = store
            .get(&metadata_path)
            .await
            .expect("metadata stored")
            .bytes()
            .await
            .expect("read metadata");
        let metadata: SnapshotMetadata =
            serde_json::from_slice(&metadata_bytes).expect("parse metadata");

        let dataset = metadata
            .datasets
            .get(DATASET_NAME)
            .expect("dataset metadata present");
        assert_eq!(dataset.snapshots.len(), 1);
        assert_eq!(dataset.current_snapshot_id, Some(0));

        let entry = dataset.snapshots.first().expect("snapshot entry");
        assert_eq!(entry.snapshot_size, contents.len() as u64);
        assert_eq!(entry.snapshot_checksum, compute_sha256_hex(&contents));
        assert_eq!(
            entry.snapshot_checksum_algorithm,
            SNAPSHOT_CHECKSUM_ALGORITHM
        );
        assert_eq!(entry.snapshot, snapshot_uri(&uploaded_path));

        let metadata_schema = dataset
            .current_schema()
            .expect("current schema")
            .to_schema_ref()
            .expect("deserialize schema");
        assert_eq!(metadata_schema.as_ref(), schema.as_ref());
    }

    #[tokio::test]
    async fn download_snapshot_entry_rejects_checksum_mismatch() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);
        let instant = Utc
            .with_ymd_and_hms(2025, 3, 1, 12, 0, 0)
            .single()
            .expect("valid time");
        let location = layout.build_location(&base, instant);

        let contents = Bytes::from_static(b"correct-bytes");
        store
            .put(&location, contents.clone().into())
            .await
            .expect("write snapshot");

        let schema = sample_schema();
        let checksum = compute_sha256_hex(b"other-bytes");
        let entry = SnapshotEntry {
            snapshot_id: 0,
            timestamp_ms: instant.timestamp_millis(),
            snapshot: snapshot_uri(&location),
            snapshot_checksum: checksum,
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: contents.len() as u64,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, &schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(0),
            properties: HashMap::new(),
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );
        let factory = Arc::clone(
            manager
                .checkpointer_factory
                .as_ref()
                .expect("factory present"),
        );

        let result = manager
            .download_snapshot_entry(&entry, &metadata, factory)
            .await;

        assert!(matches!(
            result,
            Err(SnapshotDownloadError::ChecksumMismatch { .. })
        ));
        assert!(!local_path.exists());
    }

    #[tokio::test]
    async fn download_snapshot_entry_rejects_size_mismatch() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);
        let instant = Utc
            .with_ymd_and_hms(2025, 4, 1, 12, 0, 0)
            .single()
            .expect("valid time");
        let location = layout.build_location(&base, instant);

        let contents = Bytes::from_static(b"size-bytes");
        store
            .put(&location, contents.clone().into())
            .await
            .expect("write snapshot");

        let schema = sample_schema();
        let checksum = compute_sha256_hex(contents.as_ref());
        let entry = SnapshotEntry {
            snapshot_id: 1,
            timestamp_ms: instant.timestamp_millis(),
            snapshot: snapshot_uri(&location),
            snapshot_checksum: checksum,
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: contents.len() as u64 + 1,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, &schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(1),
            properties: HashMap::new(),
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );
        let factory = Arc::clone(
            manager
                .checkpointer_factory
                .as_ref()
                .expect("factory present"),
        );

        let result = manager
            .download_snapshot_entry(&entry, &metadata, factory)
            .await;

        assert!(matches!(
            result,
            Err(SnapshotDownloadError::SizeMismatch { .. })
        ));
        assert!(!local_path.exists());
    }

    #[tokio::test]
    async fn download_snapshot_entry_rejects_unsupported_checksum_algorithm() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);
        let instant = Utc
            .with_ymd_and_hms(2025, 5, 1, 12, 0, 0)
            .single()
            .expect("valid time");
        let location = layout.build_location(&base, instant);

        let contents = Bytes::from_static(b"alg-bytes");
        store
            .put(&location, contents.clone().into())
            .await
            .expect("write snapshot");

        let schema = sample_schema();
        let checksum = compute_sha256_hex(contents.as_ref());
        let entry = SnapshotEntry {
            snapshot_id: 2,
            timestamp_ms: instant.timestamp_millis(),
            snapshot: snapshot_uri(&location),
            snapshot_checksum: checksum,
            snapshot_checksum_algorithm: "MD5".to_string(),
            snapshot_size: contents.len() as u64,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, &schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(2),
            properties: HashMap::new(),
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );
        let factory = Arc::clone(
            manager
                .checkpointer_factory
                .as_ref()
                .expect("factory present"),
        );

        let result = manager
            .download_snapshot_entry(&entry, &metadata, factory)
            .await;

        assert!(matches!(
            result,
            Err(SnapshotDownloadError::UnsupportedChecksumAlgorithm { .. })
        ));
        assert!(!local_path.exists());
    }

    #[tokio::test]
    async fn download_snapshot_entry_rejects_schema_mismatch() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);
        let instant = Utc
            .with_ymd_and_hms(2025, 6, 1, 12, 0, 0)
            .single()
            .expect("valid time");
        let location = layout.build_location(&base, instant);

        let contents = Bytes::from_static(b"schema-bytes");
        store
            .put(&location, contents.clone().into())
            .await
            .expect("write snapshot");

        let runtime_schema = sample_schema();
        let metadata_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Utf8,
            false,
        )]));
        let checksum = compute_sha256_hex(contents.as_ref());
        let entry = SnapshotEntry {
            snapshot_id: 3,
            timestamp_ms: instant.timestamp_millis(),
            snapshot: snapshot_uri(&location),
            snapshot_checksum: checksum,
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: contents.len() as u64,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![
                SchemaMetadata::from_schema(0, &metadata_schema).expect("serialize schema"),
            ],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(3),
            properties: HashMap::new(),
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &runtime_schema,
        );
        let factory = Arc::clone(
            manager
                .checkpointer_factory
                .as_ref()
                .expect("factory present"),
        );

        let result = manager
            .download_snapshot_entry(&entry, &metadata, factory)
            .await;

        assert!(matches!(
            result,
            Err(SnapshotDownloadError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn snapshot_uri_to_object_path_handles_relative_uris() {
        let store = Arc::new(InMemory::new());
        let schema = sample_schema();
        let manager = build_manager(
            Arc::clone(&store),
            PathBuf::from("/tmp/unused"),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );

        let uri = format!("{SNAPSHOT_URI_PREFIX}/month=2025-01/day=01/dataset=dataset/file.db");
        let path = manager
            .snapshot_uri_to_object_path(&uri)
            .expect("convert uri to path");

        assert_eq!(
            path.to_string(),
            "snapshots/month=2025-01/day=01/dataset=dataset/file.db"
        );
    }

    #[test]
    fn snapshot_uri_to_object_path_preserves_absolute_paths() {
        let store = Arc::new(InMemory::new());
        let schema = sample_schema();
        let manager = build_manager(
            Arc::clone(&store),
            PathBuf::from("/tmp/unused"),
            BootstrapOnFailureBehavior::Warn,
            &schema,
        );

        let uri = "memory://other-prefix/path/to/file.db";
        let path = manager
            .snapshot_uri_to_object_path(uri)
            .expect("convert uri to path");

        assert_eq!(path.to_string(), "snapshots/other-prefix/path/to/file.db");
    }
}
