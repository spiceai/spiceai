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

use arrow_schema::{Schema, SchemaRef};
use aws_sdk_credential_bridge::object_store_builder::{
    S3ObjectStoreBuilder, S3ObjectStoreBuilderError,
};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{
    GetResult, ObjectStore, PutMode, PutPayload, UpdateVersion, path::Path as ObjectPath,
};
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_secrets::{Secrets, get_params_with_secrets};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use spicepod::{component::snapshot::BootstrapOnFailureBehavior, param::Params};
use std::{
    collections::HashMap,
    fmt::Write,
    ops::Not,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Instant,
};
use tokio::sync::OwnedMutexGuard;
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
pub mod directory_archive;
mod engine;
pub mod metrics;
pub use crate::layout::AccelerationLayout;
pub use behavior::SnapshotBehavior;
use engine::{SnapshotEngine, create_snapshot_engine};

/// Public API types for snapshot information exposed via HTTP endpoints.
pub mod api {
    use serde::{Deserialize, Serialize};

    /// Summary of all snapshots for a dataset, returned by the list snapshots API.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SnapshotSummary {
        pub dataset_name: String,
        pub location: String,
        pub last_updated_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub current_snapshot_id: Option<u64>,
        pub snapshots: Vec<SnapshotInfo>,
    }

    /// Information about a single snapshot.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SnapshotInfo {
        pub snapshot_id: u64,
        pub timestamp_ms: i64,
        pub location: String,
        pub checksum: String,
        pub checksum_algorithm: String,
        pub size_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub row_count: Option<u64>,
        pub is_current: bool,
    }

    /// Request body for setting the current snapshot.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SetCurrentSnapshotRequest {
        pub snapshot_id: u64,
    }
}
use spicepod::acceleration::{SnapshotsCompaction, SnapshotsCreationPolicy};

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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "snapshot-last-updated-at-ms"
    )]
    snapshot_last_updated_at_ms: Option<i64>,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDownloadInfo {
    pub schema: SchemaRef,
    pub bytes_downloaded: u64,
    pub checksum: String,
    pub last_updated_at: Option<i64>,
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
    #[snafu(display("Snapshots are disabled for dataset {dataset}"))]
    SnapshotDisabled { dataset: String },
    #[snafu(display("Failed to extract snapshot archive at {}: {source}", path.display()))]
    ArchiveExtract {
        path: PathBuf,
        source: std::io::Error,
    },
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
    #[snafu(display("Failed to copy local file from {source_path:?} to {dest_path:?}"))]
    CopyLocal {
        source_path: PathBuf,
        dest_path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to prepare snapshot for upload: {source}"))]
    PrepareUpload { source: engine::SnapshotEngineError },
    #[snafu(display("Snapshots are disabled for dataset {dataset}"))]
    AdapterDisabled { dataset: String },
    #[snafu(display("Failed to create snapshot archive at {}: {source}", path.display()))]
    ArchiveCreate {
        path: PathBuf,
        source: std::io::Error,
    },
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

#[derive(Clone, PartialEq, Debug)]
pub enum AccelerationEngine {
    Cayenne,
    #[cfg(feature = "duckdb")]
    DuckDB,
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

/// Manages snapshots for a specific accelerated dataset.
#[derive(Clone)]
pub struct SnapshotManager {
    dataset_name: String,
    snapshots_location: object_store::path::Path,
    snapshot_location_uri: String,
    /// The acceleration layout defining the storage paths for this accelerator.
    layout: AccelerationLayout,
    snapshot_engine: Arc<dyn SnapshotEngine>,
    object_store: Arc<dyn ObjectStore>,
    bootstrap_failure_behavior: BootstrapOnFailureBehavior,
    checkpointer_factory: Option<DatasetCheckpointerFactory>,
    snapshots_creation_policy: SnapshotsCreationPolicy,
}

impl std::fmt::Debug for SnapshotManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotManager")
            .field("dataset_name", &self.dataset_name)
            .field("snapshots_location", &self.snapshots_location)
            .field("snapshot_location_uri", &self.snapshot_location_uri)
            .field("layout", &self.layout)
            .field(
                "bootstrap_failure_behavior",
                &self.bootstrap_failure_behavior,
            )
            .field("object_store", &self.object_store)
            .finish_non_exhaustive()
    }
}

pub struct ForceCreate(pub bool);

impl Not for ForceCreate {
    type Output = bool;

    fn not(self) -> Self::Output {
        !self.0
    }
}

impl Not for &ForceCreate {
    type Output = bool;

    fn not(self) -> Self::Output {
        !self.0
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

    /// Checks if there are any existing snapshots for this dataset.
    ///
    /// Returns `true` if both:
    /// - Metadata exists and contains at least one snapshot entry for this dataset
    /// - At least one actual snapshot file exists in the object store
    ///
    /// Returns `false` if either condition is not met.
    async fn has_existing_snapshots(&self) -> bool {
        // Check metadata for snapshot entries
        let metadata_has_snapshots = match self.load_metadata().await {
            Ok(Some(handle)) => handle
                .metadata
                .datasets
                .get(&self.dataset_name)
                .is_some_and(|meta| !meta.snapshots.is_empty()),
            Ok(None) | Err(_) => false,
        };

        if !metadata_has_snapshots {
            return false;
        }

        // Check if actual snapshot files exist in object store
        let layout = SnapshotPathLayout::new(&self.dataset_name);
        let dataset_partition = layout.dataset_partition_raw();

        let mut list_stream = self.object_store.list(Some(&self.snapshots_location));
        while let Some(result) = list_stream.next().await {
            if let Ok(meta) = result {
                // Check if this file belongs to our dataset partition by matching exact path segment.
                // Using contains() would incorrectly match prefix names (e.g., dataset=foo matches dataset=foobar).
                if meta
                    .location
                    .as_ref()
                    .split('/')
                    .any(|segment| segment == dataset_partition)
                {
                    return true;
                }
            }
        }

        false
    }

    pub async fn try_new(
        dataset_name: String,
        snapshots: SnapshotBehavior,
        layout: AccelerationLayout,
        engine: AccelerationEngine,
    ) -> Option<Self> {
        if !layout.is_enabled() {
            tracing::debug!("Acceleration layout is not enabled for {dataset_name}");
            return None;
        }

        let (snapshot_config, secrets, io_runtime, compaction_enabled) = match snapshots {
            SnapshotBehavior::Disabled => {
                tracing::debug!("Snapshots are disabled for {dataset_name}");
                return None;
            }
            SnapshotBehavior::Enabled(s, secrets, io_runtime, compaction)
            | SnapshotBehavior::CreateOnly(s, secrets, io_runtime, compaction) => (
                s,
                secrets.upgrade()?,
                io_runtime,
                matches!(compaction, SnapshotsCompaction::Enabled),
            ),
            SnapshotBehavior::BootstrapOnly(s, secrets, io_runtime) => {
                (s, secrets.upgrade()?, io_runtime, false)
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

        let (store, path): (Arc<dyn ObjectStore>, _) = if let ("s3", path) = (
            snapshots_location_url.scheme(),
            snapshots_location_url.path(),
        ) {
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
        } else {
            let (store, path) = object_store::parse_url(&snapshots_location_url).ok()?;
            (store.into(), path)
        };

        let snapshot_engine = create_snapshot_engine(&engine, compaction_enabled);

        if compaction_enabled {
            if snapshot_engine.supports_compaction() {
                tracing::info!("Snapshot compaction is enabled for dataset {dataset_name}");
            } else {
                tracing::warn!(
                    "Snapshot compaction is enabled for dataset {dataset_name} but engine does not support compaction"
                );
            }
        }

        Some(Self {
            dataset_name,
            snapshots_location: path,
            snapshot_location_uri,
            layout,
            snapshot_engine,
            object_store: store,
            checkpointer_factory: None,
            bootstrap_failure_behavior: snapshot_config.bootstrap_on_failure_behavior,
            snapshots_creation_policy: SnapshotsCreationPolicy::default(),
        })
    }

    /// Creates a `SnapshotManager` for metadata-only queries (list/get/set snapshots).
    ///
    /// Unlike `try_new`, this constructor does not require an enabled `SnapshotAdapter`
    /// because it only accesses the metadata file, not the actual snapshot files.
    /// This is used by HTTP endpoints to query and manage snapshot metadata.
    ///
    /// # Arguments
    ///
    /// * `dataset_name` - The name of the dataset.
    /// * `snapshots` - The snapshot behavior configuration.
    ///
    /// # Returns
    ///
    /// Returns `Some(SnapshotManager)` if snapshots are configured with a valid location,
    /// or `None` if snapshots are disabled or misconfigured.
    pub async fn try_new_for_metadata_queries(
        dataset_name: String,
        snapshots: SnapshotBehavior,
    ) -> Option<Self> {
        let (snapshot_config, secrets, io_runtime) = match snapshots {
            SnapshotBehavior::Disabled => {
                tracing::debug!("Snapshots are disabled for {dataset_name}");
                return None;
            }
            SnapshotBehavior::Enabled(s, secrets, io_runtime, _)
            | SnapshotBehavior::CreateOnly(s, secrets, io_runtime, _)
            | SnapshotBehavior::BootstrapOnly(s, secrets, io_runtime) => {
                (s, secrets.upgrade()?, io_runtime)
            }
        };

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

        let (store, path): (Arc<dyn ObjectStore>, _) = if let ("s3", path) = (
            snapshots_location_url.scheme(),
            snapshots_location_url.path(),
        ) {
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
        } else {
            let (store, path) = object_store::parse_url(&snapshots_location_url).ok()?;
            (store.into(), path)
        };

        // Use a no-op layout and engine for metadata-only queries
        let snapshot_engine = create_snapshot_engine(&AccelerationEngine::Cayenne, false);

        Some(Self {
            dataset_name,
            snapshots_location: path,
            snapshot_location_uri,
            layout: AccelerationLayout::None,
            snapshot_engine,
            object_store: store,
            checkpointer_factory: None,
            bootstrap_failure_behavior: snapshot_config.bootstrap_on_failure_behavior,
            snapshots_creation_policy: SnapshotsCreationPolicy::default(),
        })
    }

    /// Sets a factory function to create a new dataset checkpointer for this snapshot manager.
    #[must_use]
    pub fn with_checkpointer_factory(mut self, factory: DatasetCheckpointerFactory) -> Self {
        self.checkpointer_factory = Some(factory);
        self
    }

    /// Sets the policy for snapshot creation.
    #[must_use]
    pub fn with_snapshots_creation_policy(
        mut self,
        snapshots_creation_policy: SnapshotsCreationPolicy,
    ) -> Self {
        self.snapshots_creation_policy = snapshots_creation_policy;
        self
    }

    /// Creates a new snapshot by streaming the local acceleration file to object storage.
    ///
    /// For file-based accelerators (`DuckDB`, `SQLite`), this copies and uploads the database file.
    /// For directory-based accelerators (Cayenne), this archives the directories into a tar file
    /// before uploading.
    ///
    /// # Arguments
    /// * `schema` - The schema of the dataset.
    /// * `lock_guard` - Lock guard protecting accelerator writes during snapshot.
    /// * `last_updated_at` - Optional timestamp (ms since epoch) of the last `insert_into`.
    ///
    /// # Returns
    /// * `Ok(Some(path))` - Snapshot was created at the given path.
    /// * `Ok(None)` - Snapshot was skipped (no updates since last snapshot).
    ///
    /// # Errors
    ///
    /// - If the local acceleration file cannot be opened or read.
    /// - If communicating with the backing object store fails at any stage of the upload.
    pub async fn create_snapshot(
        &self,
        schema: &SchemaRef,
        lock_guard: OwnedMutexGuard<()>,
        last_updated_at: Option<i64>,
        force_create: ForceCreate,
    ) -> Result<Option<ObjectPath>, SnapshotUploadError> {
        // If no existing snapshots (in metadata or as actual files), treat as force_create.
        // This ensures at least one snapshot exists at all times.
        let force_create = if force_create.0 {
            force_create
        } else if !self.has_existing_snapshots().await {
            tracing::info!(
                "No existing snapshots found (metadata or files), forcing snapshot creation. dataset={}",
                self.dataset_name
            );
            ForceCreate(true)
        } else {
            force_create
        };

        // Check if we should skip due to no updates (on_change policy)
        if matches!(
            self.snapshots_creation_policy,
            SnapshotsCreationPolicy::OnChange
        ) && !force_create
        {
            // Skip if no writes have occurred in this session (last_updated_at is None/0).
            // This avoids creating snapshots before the first refresh completes.
            if last_updated_at.is_none() {
                tracing::info!(
                    "Skipping snapshot creation - no data writes have occurred yet. dataset={}",
                    self.dataset_name
                );
                metrics::record_snapshot_skipped(&self.dataset_name);
                return Ok(None);
            }

            // Check if the timestamp matches the last stored snapshot to avoid duplicates.
            if let Some(last_updated_at) = last_updated_at
                && let Ok(Some(handle)) = self.load_metadata().await
                && let Some(dataset_meta) = handle.metadata.datasets.get(&self.dataset_name)
                && let Some(snapshot_entry) = dataset_meta.snapshots.last()
                && snapshot_entry.snapshot_last_updated_at_ms == Some(last_updated_at)
            {
                tracing::info!(
                    "Skipping snapshot creation - no updates since last snapshot. dataset={} last_updated_at_ms={}",
                    self.dataset_name,
                    last_updated_at
                );
                metrics::record_snapshot_skipped(&self.dataset_name);
                return Ok(None);
            }
        }

        let start_time = Instant::now();
        let now = Utc::now();
        let layout = SnapshotPathLayout::new(&self.dataset_name);
        let destination_location = layout.build_location(&self.snapshots_location, now);
        let timestamp_ms = now.timestamp_millis();

        tracing::info!(
            "Uploading snapshot. dataset={} snapshot={destination_location}",
            self.dataset_name
        );

        let (total_bytes, checksum) = match &self.layout {
            AccelerationLayout::None => {
                return Err(SnapshotUploadError::AdapterDisabled {
                    dataset: self.dataset_name.clone(),
                });
            }
            AccelerationLayout::File { path } => {
                self.create_file_snapshot(path, &destination_location, lock_guard)
                    .await?
            }
            AccelerationLayout::Directories { dirs } => {
                self.create_directory_snapshot(dirs, &destination_location, lock_guard)
                    .await?
            }
        };

        self.update_metadata_after_upload(
            &destination_location,
            checksum.clone(),
            total_bytes,
            timestamp_ms,
            schema,
            last_updated_at,
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
            "Snapshot uploaded. dataset={} snapshot={destination_location} size={total_bytes} sha={checksum}",
            self.dataset_name,
        );

        Ok(Some(destination_location))
    }

    /// Creates a snapshot from a single file-based accelerator.
    async fn create_file_snapshot(
        &self,
        source_local_path: &PathBuf,
        destination_location: &ObjectPath,
        lock_guard: OwnedMutexGuard<()>,
    ) -> Result<(u64, String), SnapshotUploadError> {
        // Step 1: Copy the database file locally (lock is held)
        let temp_copy_path = source_local_path.with_extension("snapshot_tmp");
        fs::copy(source_local_path, &temp_copy_path)
            .await
            .context(CopyLocalSnafu {
                source_path: source_local_path.clone(),
                dest_path: temp_copy_path.clone(),
            })?;

        // Step 2: Release the lock - queries can resume
        drop(lock_guard);
        tracing::debug!(
            "Lock released after file copy. dataset={}",
            self.dataset_name
        );

        // Step 3: Prepare a snapshot using engine-specific logic
        let final_source_local_path = self
            .snapshot_engine
            .prepare_for_upload(&temp_copy_path, &self.dataset_name)
            .await
            .context(PrepareUploadSnafu)?;

        // Step 4: Upload the file
        let upload_result = self
            .upload_snapshot_file(&final_source_local_path, destination_location)
            .await;

        // Step 5: Cleanup temp files
        let _ = fs::remove_file(&temp_copy_path).await;
        if final_source_local_path != temp_copy_path {
            let _ = fs::remove_file(&final_source_local_path).await;
        }

        upload_result
    }

    /// Creates a snapshot from directory-based accelerator (e.g., Cayenne).
    ///
    /// Archives multiple directories into a tar file before upload.
    async fn create_directory_snapshot(
        &self,
        dirs: &[(PathBuf, String)],
        destination_location: &ObjectPath,
        lock_guard: OwnedMutexGuard<()>,
    ) -> Result<(u64, String), SnapshotUploadError> {
        use crate::snapshot::directory_archive::archive_directories;

        // Step 1: Create a temporary tar archive of all directories
        let temp_archive_path = std::env::temp_dir().join(format!(
            "snapshot_{}_{}_{}.tar",
            self.dataset_name,
            chrono::Utc::now().format("%Y%m%dT%H%M%S"),
            uuid::Uuid::now_v7()
        ));

        let archive_file = fs::File::create(&temp_archive_path)
            .await
            .map_err(|source| SnapshotUploadError::ArchiveCreate {
                path: temp_archive_path.clone(),
                source,
            })?;

        let total_archived = archive_directories(dirs, archive_file)
            .await
            .map_err(|source| SnapshotUploadError::ArchiveCreate {
                path: temp_archive_path.clone(),
                source: std::io::Error::other(source.to_string()),
            })?;

        tracing::debug!(
            "Created tar archive for snapshot. dataset={} archive_size={}",
            self.dataset_name,
            total_archived
        );

        // Step 2: Release the lock - queries can resume
        drop(lock_guard);
        tracing::debug!(
            "Lock released after archive creation. dataset={}",
            self.dataset_name
        );

        // Step 3: Upload the tar archive
        let upload_result = self
            .upload_snapshot_file(&temp_archive_path, destination_location)
            .await;

        // Step 4: Cleanup temp archive
        let _ = fs::remove_file(&temp_archive_path).await;

        upload_result
    }

    /// Uploads a file to object storage, returning (size, checksum).
    async fn upload_snapshot_file(
        &self,
        source_local_path: &PathBuf,
        destination_location: &ObjectPath,
    ) -> Result<(u64, String), SnapshotUploadError> {
        let destination_location_path = destination_location.to_string();

        let file = fs::File::open(source_local_path)
            .await
            .context(OpenLocalSnafu {
                path: source_local_path.clone(),
            })?;

        let mut reader = BufReader::with_capacity(SNAPSHOT_MULTIPART_CHUNK_SIZE, file);

        let mut upload = self
            .object_store
            .put_multipart(destination_location)
            .await
            .context(StartUploadSnafu {
                path: destination_location_path.clone(),
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
                            "Failed to read local snapshot file. dataset={} snapshot={destination_location} error={source}",
                            self.dataset_name
                        );
                        if let Err(abort_source) = upload.abort().await {
                            tracing::warn!(
                                "Failed to abort snapshot upload after read failure. dataset={} snapshot={destination_location} error={abort_source}",
                                self.dataset_name
                            );
                            return Err(SnapshotUploadError::AbortUpload {
                                path: destination_location_path.clone(),
                                source: abort_source,
                            });
                        }
                        return Err(SnapshotUploadError::ReadLocal {
                            path: source_local_path.clone(),
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
                    "Snapshot upload part failed. dataset={} snapshot={destination_location} error={source}",
                    self.dataset_name
                );
                if let Err(abort_source) = upload.abort().await {
                    tracing::warn!(
                        "Failed to abort snapshot upload after part failure. dataset={} snapshot={destination_location} error={abort_source}",
                        self.dataset_name
                    );
                    return Err(SnapshotUploadError::AbortUpload {
                        path: destination_location_path.clone(),
                        source: abort_source,
                    });
                }
                return Err(SnapshotUploadError::UploadPart {
                    path: destination_location_path.clone(),
                    source,
                });
            }
        }

        match upload.complete().await {
            Ok(_) => {
                let checksum_bytes = hasher.finalize();
                let checksum = encode_hex_lower(checksum_bytes.as_ref());
                Ok((total_bytes, checksum))
            }
            Err(source) => {
                tracing::error!(
                    "Failed to finalize snapshot upload. dataset={} snapshot={destination_location} error={source}",
                    self.dataset_name
                );
                if let Err(abort_source) = upload.abort().await {
                    tracing::warn!(
                        "Failed to abort upload after completion failure. dataset={} error={abort_source}",
                        self.dataset_name
                    );
                    return Err(SnapshotUploadError::AbortUpload {
                        path: destination_location_path,
                        source: abort_source,
                    });
                }
                Err(SnapshotUploadError::CompleteUpload {
                    path: destination_location_path.clone(),
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

        let (actual_size, actual_checksum) = match &self.layout {
            AccelerationLayout::None => {
                return Err(SnapshotDownloadError::SnapshotDisabled {
                    dataset: self.dataset_name.clone(),
                });
            }
            AccelerationLayout::File { path } => {
                self.download_to_file(path, get_result, entry, &path_display)
                    .await?
            }
            AccelerationLayout::Directories { dirs } => {
                self.download_to_directories(dirs, get_result, entry, &path_display)
                    .await?
            }
        };

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

            let local_path_display = self
                .layout
                .primary_path()
                .map_or_else(|| "<directories>".to_string(), |p| p.display().to_string());
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
                last_updated_at: entry.snapshot_last_updated_at_ms,
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

    /// Downloads a snapshot directly to a single file (for file-based accelerators).
    async fn download_to_file(
        &self,
        local_path: &PathBuf,
        get_result: GetResult,
        entry: &SnapshotEntry,
        path_display: &str,
    ) -> Result<(u64, String), SnapshotDownloadError> {
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent).await.map_err(|source| {
                SnapshotDownloadError::CreateLocalDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }

        let mut stream = get_result.into_stream();
        let mut file = fs::File::create(local_path).await.map_err(|source| {
            SnapshotDownloadError::WriteLocal {
                path: local_path.clone(),
                source,
            }
        })?;

        let mut hasher = Sha256::new();
        let mut actual_size: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(chunk) => chunk,
                Err(source) => {
                    let _ = fs::remove_file(local_path).await;
                    return Err(SnapshotDownloadError::DownloadBytes {
                        path: path_display.to_string(),
                        source,
                    });
                }
            };

            actual_size += chunk.len() as u64;
            hasher.update(&chunk);

            if let Err(source) = file.write_all(&chunk).await {
                let _ = fs::remove_file(local_path).await;
                return Err(SnapshotDownloadError::WriteLocal {
                    path: local_path.clone(),
                    source,
                });
            }
        }

        if let Err(source) = file.flush().await {
            let _ = fs::remove_file(local_path).await;
            return Err(SnapshotDownloadError::WriteLocal {
                path: local_path.clone(),
                source,
            });
        }
        drop(file);

        let actual_checksum = self
            .validate_snapshot(entry, actual_size, hasher, local_path, path_display)
            .await?;

        Ok((actual_size, actual_checksum))
    }

    /// Downloads a snapshot archive and extracts it to multiple directories (for Cayenne).
    ///
    /// Uses skip-if-exists extraction to handle multiple Cayenne datasets that share a
    /// metadata directory. The first dataset's snapshot extracts the metadata files,
    /// and subsequent datasets skip those files since they already exist.
    async fn download_to_directories(
        &self,
        dirs: &[(PathBuf, String)],
        get_result: GetResult,
        entry: &SnapshotEntry,
        path_display: &str,
    ) -> Result<(u64, String), SnapshotDownloadError> {
        use crate::snapshot::directory_archive::{ExtractOptions, extract_archive_with_options};

        // Download to a temporary file first
        let temp_archive_path = std::env::temp_dir().join(format!(
            "snapshot_download_{}_{}_{}.tar",
            self.dataset_name,
            chrono::Utc::now().format("%Y%m%dT%H%M%S"),
            uuid::Uuid::now_v7()
        ));

        // Ensure temp dir exists
        if let Some(parent) = temp_archive_path.parent() {
            fs::create_dir_all(parent).await.map_err(|source| {
                SnapshotDownloadError::CreateLocalDir {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }

        let mut stream = get_result.into_stream();
        let mut file = fs::File::create(&temp_archive_path)
            .await
            .map_err(|source| SnapshotDownloadError::WriteLocal {
                path: temp_archive_path.clone(),
                source,
            })?;

        let mut hasher = Sha256::new();
        let mut actual_size: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(chunk) => chunk,
                Err(source) => {
                    let _ = fs::remove_file(&temp_archive_path).await;
                    return Err(SnapshotDownloadError::DownloadBytes {
                        path: path_display.to_string(),
                        source,
                    });
                }
            };

            actual_size += chunk.len() as u64;
            hasher.update(&chunk);

            if let Err(source) = file.write_all(&chunk).await {
                let _ = fs::remove_file(&temp_archive_path).await;
                return Err(SnapshotDownloadError::WriteLocal {
                    path: temp_archive_path.clone(),
                    source,
                });
            }
        }

        if let Err(source) = file.flush().await {
            let _ = fs::remove_file(&temp_archive_path).await;
            return Err(SnapshotDownloadError::WriteLocal {
                path: temp_archive_path.clone(),
                source,
            });
        }
        drop(file);

        // Validate size and checksum before extraction, consuming the hasher to get the checksum
        let actual_checksum = self
            .validate_snapshot(entry, actual_size, hasher, &temp_archive_path, path_display)
            .await?;

        // Find the common parent directory to extract the archive to.
        // The archive contains prefixed paths like "metadata/..." and "data/...",
        // so we need to extract to the parent of these directories.
        let extract_target = dirs
            .first()
            .and_then(|(dir, _)| dir.parent())
            .ok_or_else(|| {
                let _ = std::fs::remove_file(&temp_archive_path);
                SnapshotDownloadError::CreateLocalDir {
                    path: temp_archive_path.clone(),
                    source: std::io::Error::other(
                        "Cannot determine extraction target: no directories specified or directory has no parent",
                    ),
                }
            })?;

        // Extract the tar archive to the target directory.
        // Use skip_if_exists to handle shared metadata directories across multiple
        // Cayenne datasets. The first dataset's snapshot extracts the metadata,
        // and subsequent datasets skip those files since they already exist.
        let archive_file = fs::File::open(&temp_archive_path).await.map_err(|source| {
            SnapshotDownloadError::WriteLocal {
                path: temp_archive_path.clone(),
                source,
            }
        })?;

        extract_archive_with_options(
            archive_file,
            extract_target,
            ExtractOptions::skip_existing(),
        )
        .await
        .map_err(|source| SnapshotDownloadError::ArchiveExtract {
            path: temp_archive_path.clone(),
            source: std::io::Error::other(source.to_string()),
        })?;

        // Cleanup temp archive
        let _ = fs::remove_file(&temp_archive_path).await;

        tracing::debug!(
            "Extracted snapshot archive to {} directories. dataset={}",
            dirs.len(),
            self.dataset_name
        );

        Ok((actual_size, actual_checksum))
    }

    /// Validates snapshot size and checksum, cleaning up the local file on failure.
    /// Consumes the hasher and returns the hex-encoded checksum on success.
    async fn validate_snapshot(
        &self,
        entry: &SnapshotEntry,
        actual_size: u64,
        hasher: Sha256,
        local_path: &PathBuf,
        path_display: &str,
    ) -> Result<String, SnapshotDownloadError> {
        if entry.snapshot_size != actual_size {
            let _ = fs::remove_file(local_path).await;
            return Err(SnapshotDownloadError::SizeMismatch {
                path: path_display.to_string(),
                expected: entry.snapshot_size,
                actual: actual_size,
            });
        }

        if !entry
            .snapshot_checksum_algorithm
            .eq_ignore_ascii_case(SNAPSHOT_CHECKSUM_ALGORITHM)
        {
            let _ = fs::remove_file(local_path).await;
            return Err(SnapshotDownloadError::UnsupportedChecksumAlgorithm {
                path: path_display.to_string(),
                algorithm: entry.snapshot_checksum_algorithm.clone(),
            });
        }

        let checksum_bytes = hasher.finalize();
        let actual_checksum = encode_hex_lower(checksum_bytes.as_ref());
        let expected_checksum = entry.snapshot_checksum.to_lowercase();
        if expected_checksum != actual_checksum {
            let _ = fs::remove_file(local_path).await;
            return Err(SnapshotDownloadError::ChecksumMismatch {
                path: path_display.to_string(),
                expected: entry.snapshot_checksum.clone(),
                actual: actual_checksum,
            });
        }

        Ok(actual_checksum)
    }

    async fn update_metadata_after_upload(
        &self,
        location: &ObjectPath,
        checksum: String,
        size: u64,
        timestamp_ms: i64,
        schema: &SchemaRef,
        last_updated_at: Option<i64>,
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
                snapshot_last_updated_at_ms: last_updated_at,
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

    /// Returns the snapshot location URI for this dataset.
    #[must_use]
    pub fn snapshot_location(&self) -> &str {
        &self.snapshot_location_uri
    }

    /// Returns the dataset name for this snapshot manager.
    #[must_use]
    pub fn dataset_name(&self) -> &str {
        &self.dataset_name
    }

    /// Retrieves the snapshot summary for this dataset, including all available snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or parsing the metadata fails.
    pub async fn get_snapshot_summary(&self) -> Result<api::SnapshotSummary, SnapshotApiError> {
        let handle = self.load_metadata().await.map_err(|e| match e {
            MetadataLoadError::Read { path, source } => SnapshotApiError::ReadMetadata {
                path,
                reason: source.to_string(),
            },
            MetadataLoadError::Parse { path, source } => SnapshotApiError::ParseMetadata {
                path,
                reason: source.to_string(),
            },
            MetadataLoadError::UnsupportedVersion { path, version } => {
                SnapshotApiError::UnsupportedVersion { path, version }
            }
        })?;

        let (location, last_updated_ms, dataset_metadata) = match handle {
            Some(h) => {
                let ds_meta = h.metadata.datasets.get(&self.dataset_name).cloned();
                (h.metadata.location, h.metadata.last_updated_ms, ds_meta)
            }
            None => (self.snapshot_location_uri.clone(), 0, None),
        };

        let dataset_metadata = dataset_metadata.unwrap_or_else(|| DatasetMetadata {
            name: self.dataset_name.clone(),
            ..Default::default()
        });

        let current_snapshot_id = dataset_metadata.current_snapshot_id;
        let snapshots: Vec<api::SnapshotInfo> = dataset_metadata
            .snapshots
            .iter()
            .map(|entry| api::SnapshotInfo {
                snapshot_id: entry.snapshot_id,
                timestamp_ms: entry.timestamp_ms,
                location: entry.snapshot.clone(),
                checksum: entry.snapshot_checksum.clone(),
                checksum_algorithm: entry.snapshot_checksum_algorithm.clone(),
                size_bytes: entry.snapshot_size,
                row_count: None, // Not stored in current metadata format
                is_current: Some(entry.snapshot_id) == current_snapshot_id,
            })
            .collect();

        Ok(api::SnapshotSummary {
            dataset_name: self.dataset_name.clone(),
            location,
            last_updated_ms,
            current_snapshot_id,
            snapshots,
        })
    }

    /// Retrieves information about a specific snapshot by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if reading the metadata fails or the snapshot is not found.
    pub async fn get_snapshot(
        &self,
        snapshot_id: u64,
    ) -> Result<api::SnapshotInfo, SnapshotApiError> {
        let handle = self.load_metadata().await.map_err(|e| match e {
            MetadataLoadError::Read { path, source } => SnapshotApiError::ReadMetadata {
                path,
                reason: source.to_string(),
            },
            MetadataLoadError::Parse { path, source } => SnapshotApiError::ParseMetadata {
                path,
                reason: source.to_string(),
            },
            MetadataLoadError::UnsupportedVersion { path, version } => {
                SnapshotApiError::UnsupportedVersion { path, version }
            }
        })?;

        let Some(h) = handle else {
            return Err(SnapshotApiError::SnapshotNotFound {
                dataset: self.dataset_name.clone(),
                snapshot_id,
            });
        };

        let Some(dataset_metadata) = h.metadata.datasets.get(&self.dataset_name) else {
            return Err(SnapshotApiError::SnapshotNotFound {
                dataset: self.dataset_name.clone(),
                snapshot_id,
            });
        };

        let Some(entry) = dataset_metadata
            .snapshots
            .iter()
            .find(|e| e.snapshot_id == snapshot_id)
        else {
            return Err(SnapshotApiError::SnapshotNotFound {
                dataset: self.dataset_name.clone(),
                snapshot_id,
            });
        };

        Ok(api::SnapshotInfo {
            snapshot_id: entry.snapshot_id,
            timestamp_ms: entry.timestamp_ms,
            location: entry.snapshot.clone(),
            checksum: entry.snapshot_checksum.clone(),
            checksum_algorithm: entry.snapshot_checksum_algorithm.clone(),
            size_bytes: entry.snapshot_size,
            row_count: None,
            is_current: Some(entry.snapshot_id) == dataset_metadata.current_snapshot_id,
        })
    }

    /// Sets the current snapshot ID for this dataset.
    ///
    /// This updates the metadata to point to the specified snapshot, which will be used
    /// for bootstrapping on the next runtime restart.
    ///
    /// # Errors
    ///
    /// Returns an error if reading/writing the metadata fails or the snapshot is not found.
    pub async fn set_current_snapshot(&self, snapshot_id: u64) -> Result<(), SnapshotApiError> {
        loop {
            let handle = self.load_metadata().await.map_err(|e| match e {
                MetadataLoadError::Read { path, source } => SnapshotApiError::ReadMetadata {
                    path,
                    reason: source.to_string(),
                },
                MetadataLoadError::Parse { path, source } => SnapshotApiError::ParseMetadata {
                    path,
                    reason: source.to_string(),
                },
                MetadataLoadError::UnsupportedVersion { path, version } => {
                    SnapshotApiError::UnsupportedVersion { path, version }
                }
            })?;

            let Some(h) = handle.as_ref() else {
                return Err(SnapshotApiError::SnapshotNotFound {
                    dataset: self.dataset_name.clone(),
                    snapshot_id,
                });
            };

            let mut metadata = h.metadata.clone();

            let dataset_entry = metadata
                .datasets
                .get_mut(&self.dataset_name)
                .ok_or_else(|| SnapshotApiError::SnapshotNotFound {
                    dataset: self.dataset_name.clone(),
                    snapshot_id,
                })?;

            // Verify the snapshot exists
            if !dataset_entry
                .snapshots
                .iter()
                .any(|e| e.snapshot_id == snapshot_id)
            {
                return Err(SnapshotApiError::SnapshotNotFound {
                    dataset: self.dataset_name.clone(),
                    snapshot_id,
                });
            }

            dataset_entry.current_snapshot_id = Some(snapshot_id);
            metadata.last_updated_ms = Utc::now().timestamp_millis();

            let metadata_path = self.metadata_path();
            let metadata_path_display = metadata_path.to_string();

            let serialized = serde_json::to_vec_pretty(&metadata).map_err(|err| {
                SnapshotApiError::WriteMetadata {
                    path: metadata_path_display.clone(),
                    reason: err.to_string(),
                }
            })?;

            let version = h.version.clone();
            let put_mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Overwrite,
            };

            let payload = PutPayload::from(serialized);

            match self
                .object_store
                .put_opts(&metadata_path, payload.clone(), put_mode.clone().into())
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. }) => {
                    // Concurrent update, retry
                }
                Err(object_store::Error::NotSupported { .. })
                    if matches!(put_mode, PutMode::Update(_)) =>
                {
                    // Object store doesn't support conditional updates, fall back to overwrite
                    match self
                        .object_store
                        .put_opts(&metadata_path, payload, PutMode::Overwrite.into())
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(err) => {
                            return Err(SnapshotApiError::WriteMetadata {
                                path: metadata_path_display,
                                reason: err.to_string(),
                            });
                        }
                    }
                }
                Err(err) => {
                    return Err(SnapshotApiError::WriteMetadata {
                        path: metadata_path_display,
                        reason: err.to_string(),
                    });
                }
            }
        }
    }
}

/// Errors that can occur when using the snapshot API.
#[derive(Debug, Snafu)]
#[snafu(module(snapshot_api_error))]
pub enum SnapshotApiError {
    #[snafu(display("Failed to read snapshot metadata at {path}: {reason}"))]
    ReadMetadata { path: String, reason: String },

    #[snafu(display("Snapshot metadata at {path} is invalid: {reason}"))]
    ParseMetadata { path: String, reason: String },

    #[snafu(display("Snapshot metadata at {path} has unsupported format version {version}"))]
    UnsupportedVersion { path: String, version: u32 },

    #[snafu(display("Snapshot {snapshot_id} not found for dataset {dataset}"))]
    SnapshotNotFound { dataset: String, snapshot_id: u64 },

    #[snafu(display("Failed to write snapshot metadata to {path}: {reason}"))]
    WriteMetadata { path: String, reason: String },
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
    #[snafu(display("Failed to build S3 object store: {source}"))]
    BuilderError { source: S3ObjectStoreBuilderError },
}

async fn build_s3_object_store(
    snapshots_url: &Url,
    secrets: Arc<RwLock<Secrets>>,
    params: Option<HashMap<String, String>>,
    io_runtime: Handle,
) -> Result<Arc<dyn ObjectStore>, S3ObjectStoreError> {
    let s3_params = build_s3_parameters(Arc::clone(&secrets), params.as_ref()).await;

    S3ObjectStoreBuilder::from_url(snapshots_url, io_runtime)
        .context(BuilderSnafu)?
        .with_secret_params(&s3_params.to_secret_map())
        .context(BuilderSnafu)?
        .build()
        .await
        .context(BuilderSnafu)
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
    use crate::snapshot::engine::create_snapshot_engine;
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::{memory::InMemory, path::Path};
    use std::{io::Write, path::PathBuf, sync::Arc, time::SystemTime};
    use tempfile::{NamedTempFile, TempDir};
    use tokio::fs;
    use tokio::sync::Mutex;

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

    /// Builds a `SnapshotManager` for the specified engine type.
    /// This enables testing snapshot functionality across all accelerator backends.
    fn build_manager_for_engine(
        store: Arc<InMemory>,
        local_path: PathBuf,
        behavior: BootstrapOnFailureBehavior,
        schema: &SchemaRef,
        engine: &AccelerationEngine,
        compaction_enabled: bool,
    ) -> SnapshotManager {
        let schema_for_factory = Arc::clone(schema);
        let factory: DatasetCheckpointerFactory = Arc::new(move || {
            let schema = Arc::clone(&schema_for_factory);
            Box::pin(async move {
                Ok::<Arc<dyn DatasetCheckpointer>, _>(Arc::new(StaticSchemaCheckpointer { schema }))
            })
        });

        let object_store: Arc<dyn ObjectStore> = store;
        let snapshot_engine = create_snapshot_engine(engine, compaction_enabled);

        SnapshotManager {
            dataset_name: DATASET_NAME.to_string(),
            snapshots_location: Path::from(SNAPSHOT_BASE_PATH),
            snapshot_location_uri: SNAPSHOT_URI_PREFIX.to_string(),
            layout: AccelerationLayout::File { path: local_path },
            snapshot_engine,
            object_store,
            bootstrap_failure_behavior: behavior,
            checkpointer_factory: Some(factory),
            snapshots_creation_policy: SnapshotsCreationPolicy::Always,
        }
    }

    #[cfg(feature = "duckdb")]
    fn build_manager(
        store: Arc<InMemory>,
        local_path: PathBuf,
        behavior: BootstrapOnFailureBehavior,
        schema: &SchemaRef,
        compaction_enabled: bool,
    ) -> SnapshotManager {
        build_manager_for_engine(
            store,
            local_path,
            behavior,
            schema,
            &AccelerationEngine::DuckDB,
            compaction_enabled,
        )
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
            false,
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
            snapshot_last_updated_at_ms: None,
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
            false,
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
            snapshot_last_updated_at_ms: None,
        };

        let valid_checksum = compute_sha256_hex(second_contents.as_ref());
        let valid_snapshot = SnapshotEntry {
            snapshot_id: 0,
            timestamp_ms: second_instant.timestamp_millis(),
            snapshot: snapshot_uri(&second_location),
            snapshot_checksum: valid_checksum.clone(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: second_contents.len() as u64,
            snapshot_last_updated_at_ms: None,
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
            false,
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
            false,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        let uploaded_path = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(true))
            .await
            .expect("create snapshot")
            .expect("snapshot should be created");

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
        assert_eq!(entry.snapshot_last_updated_at_ms, None);

        let metadata_schema = dataset
            .current_schema()
            .expect("current schema")
            .to_schema_ref()
            .expect("deserialize schema");
        assert_eq!(metadata_schema.as_ref(), schema.as_ref());
    }

    #[tokio::test]
    async fn create_snapshot_stores_timestamp_metadata() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-with-timestamps".to_vec();
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
            false,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        // Create snapshot with timestamp metadata
        let last_updated_at = Some(1_704_153_600_000_i64); // 2024-01-02 00:00:00 UTC

        let _uploaded_path = manager
            .create_snapshot(&schema, lock_guard, last_updated_at, ForceCreate(true))
            .await
            .expect("create snapshot")
            .expect("snapshot should be created");

        // Read and verify metadata
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

        // Verify timestamp field is stored
        let snapshot_entry = dataset.snapshots.last().expect("snapshot");
        assert_eq!(
            snapshot_entry.snapshot_last_updated_at_ms,
            Some(1_704_153_600_000)
        );
    }

    #[tokio::test]
    async fn create_snapshot_omits_zero_timestamps() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-zero-timestamps".to_vec();
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
            false,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        // Create snapshot with None updated_at
        let _uploaded_path = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(true))
            .await
            .expect("create snapshot")
            .expect("snapshot should be created");

        // Read and verify metadata
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

        let snapshot_entry = dataset.snapshots.last().expect("snapshot");
        assert_eq!(snapshot_entry.snapshot_last_updated_at_ms, None);
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
            snapshot_last_updated_at_ms: None,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, &schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(0),
            ..Default::default()
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            false,
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
            snapshot_last_updated_at_ms: None,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, &schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(1),
            ..Default::default()
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            false,
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
            snapshot_last_updated_at_ms: None,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![SchemaMetadata::from_schema(0, &schema).expect("serialize schema")],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(2),
            ..Default::default()
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            false,
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
            snapshot_last_updated_at_ms: None,
        };
        let metadata = DatasetMetadata {
            name: DATASET_NAME.to_string(),
            schemas: vec![
                SchemaMetadata::from_schema(0, &metadata_schema).expect("serialize schema"),
            ],
            current_schema_id: 0,
            snapshots: vec![entry.clone()],
            current_snapshot_id: Some(3),
            ..Default::default()
        };

        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");

        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &runtime_schema,
            false,
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
            false,
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
            false,
        );

        let uri = "memory://other-prefix/path/to/file.db";
        let path = manager
            .snapshot_uri_to_object_path(uri)
            .expect("convert uri to path");

        assert_eq!(path.to_string(), "snapshots/other-prefix/path/to/file.db");
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn create_snapshot_with_compaction() {
        use duckdb::Connection;

        let store = Arc::new(InMemory::new());
        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.duckdb");

        // Create a fragmented DuckDB database
        {
            let conn = Connection::open(&local_path).expect("open duckdb");

            conn.execute("CREATE TABLE test_data (id INTEGER, padding VARCHAR)", [])
                .expect("create table");

            // Insert data with padding to make file size significant
            conn.execute(
                "INSERT INTO test_data
                 SELECT i, REPEAT('x', 500)
                 FROM generate_series(1, 2000000) AS t(i)",
                [],
            )
            .expect("insert data");

            // Delete most rows to create dead tuples (fragmentation)
            conn.execute("DELETE FROM test_data WHERE id > 50", [])
                .expect("delete data");

            conn.execute("CHECKPOINT", []).expect("checkpoint");
        }

        let fragmented_size = std::fs::metadata(&local_path)
            .expect("get fragmented size")
            .len();

        let schema = sample_schema();
        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            true,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        let uploaded_path = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(true))
            .await
            .expect("create snapshot")
            .expect("snapshot should be created");

        // Verify snapshot was uploaded to object store
        let stored = store
            .get(&uploaded_path)
            .await
            .expect("snapshot stored")
            .bytes()
            .await
            .expect("read stored snapshot");

        let uploaded_size = stored.len() as u64;

        // Compacted snapshot should be smaller than fragmented source
        assert!(
            uploaded_size < fragmented_size,
            "compacted snapshot ({uploaded_size} bytes) should be smaller than \
             fragmented source ({fragmented_size} bytes)"
        );

        // Verify metadata was updated correctly
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
        assert_eq!(entry.snapshot_size, uploaded_size);
        assert_eq!(entry.snapshot_checksum, compute_sha256_hex(&stored));
        assert_eq!(
            entry.snapshot_checksum_algorithm,
            SNAPSHOT_CHECKSUM_ALGORITHM
        );
        assert_eq!(entry.snapshot, snapshot_uri(&uploaded_path));

        // Verify uploaded snapshot contains valid data
        let verify_path = temp_dir.path().join("verify.duckdb");
        std::fs::write(&verify_path, &stored).expect("write verify file");

        {
            let conn = Connection::open(&verify_path).expect("open verify db");

            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM test_data", [], |row| row.get(0))
                .expect("count rows");

            assert_eq!(count, 50, "compacted snapshot should have 50 rows");
        }
    }

    // ==================== Generic Engine Tests ====================
    // These tests verify snapshot functionality works across all accelerator backends.

    /// Generic test: Download returns None when no metadata exists (for any engine).
    async fn generic_download_returns_none_without_metadata(engine: &AccelerationEngine) {
        let store = Arc::new(InMemory::new());
        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");
        let schema = sample_schema();

        let manager = build_manager_for_engine(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            engine,
            false,
        );

        let result = manager
            .download_latest_snapshot()
            .await
            .expect("download should succeed");

        assert!(result.is_none());
        assert!(!local_path.exists());
    }

    /// Generic test: Creates snapshot and updates metadata (for any engine).
    async fn generic_create_snapshot_updates_metadata(engine: &AccelerationEngine) {
        let store = Arc::new(InMemory::new());
        let temp_dir = TempDir::new().expect("create temp dir");
        let local_path = temp_dir.path().join("snapshot.db");
        std::fs::write(&local_path, b"test snapshot content").expect("write test file");

        let schema = sample_schema();
        let manager = build_manager_for_engine(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            engine,
            false, // no compaction for generic test
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        let uploaded_path = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(true))
            .await
            .expect("create snapshot")
            .expect("path");

        // Verify snapshot was uploaded
        let stored = store
            .get(&uploaded_path)
            .await
            .expect("snapshot stored")
            .bytes()
            .await
            .expect("read stored snapshot");

        // Verify metadata was created
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
        assert_eq!(entry.snapshot_size, stored.len() as u64);
        assert_eq!(entry.snapshot_checksum, compute_sha256_hex(&stored));
    }

    /// Generic test: Download snapshot succeeds with valid metadata (for any engine).
    async fn generic_download_snapshot_with_valid_metadata(engine: &AccelerationEngine) {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let layout = SnapshotPathLayout::new(DATASET_NAME);
        let instant = Utc
            .with_ymd_and_hms(2025, 3, 15, 10, 30, 0)
            .single()
            .expect("valid time");
        let location = layout.build_location(&base, instant);

        let contents = Bytes::from_static(b"engine-agnostic-snapshot-bytes");
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
            snapshot_last_updated_at_ms: None,
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

        let manager = build_manager_for_engine(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Warn,
            &schema,
            engine,
            false,
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

    /// Generic test: `SnapshotEngine` reports correct compaction support.
    fn generic_engine_compaction_support(engine: &AccelerationEngine, expected_support: bool) {
        let snapshot_engine = create_snapshot_engine(engine, true);
        assert_eq!(
            snapshot_engine.supports_compaction(),
            expected_support,
            "Engine {:?} should {} compaction",
            engine,
            if expected_support {
                "support"
            } else {
                "not support"
            }
        );
    }

    // ==================== Cayenne Engine Tests ====================

    #[tokio::test]
    async fn cayenne_download_returns_none_without_metadata() {
        generic_download_returns_none_without_metadata(&AccelerationEngine::Cayenne).await;
    }

    #[tokio::test]
    async fn cayenne_create_snapshot_updates_metadata() {
        generic_create_snapshot_updates_metadata(&AccelerationEngine::Cayenne).await;
    }

    #[tokio::test]
    async fn cayenne_download_snapshot_with_valid_metadata() {
        generic_download_snapshot_with_valid_metadata(&AccelerationEngine::Cayenne).await;
    }

    #[test]
    fn cayenne_engine_does_not_support_compaction() {
        generic_engine_compaction_support(&AccelerationEngine::Cayenne, false);
    }

    // ==================== SQLite Engine Tests ====================

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_download_returns_none_without_metadata() {
        generic_download_returns_none_without_metadata(&AccelerationEngine::Sqlite).await;
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_create_snapshot_updates_metadata() {
        generic_create_snapshot_updates_metadata(&AccelerationEngine::Sqlite).await;
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_download_snapshot_with_valid_metadata() {
        generic_download_snapshot_with_valid_metadata(&AccelerationEngine::Sqlite).await;
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_engine_does_not_support_compaction() {
        generic_engine_compaction_support(&AccelerationEngine::Sqlite, false);
    }

    // ==================== DuckDB Engine Tests ====================

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_download_returns_none_without_metadata() {
        generic_download_returns_none_without_metadata(&AccelerationEngine::DuckDB).await;
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_create_snapshot_updates_metadata() {
        generic_create_snapshot_updates_metadata(&AccelerationEngine::DuckDB).await;
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_download_snapshot_with_valid_metadata() {
        generic_download_snapshot_with_valid_metadata(&AccelerationEngine::DuckDB).await;
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn duckdb_engine_supports_compaction() {
        generic_engine_compaction_support(&AccelerationEngine::DuckDB, true);
    }

    // ==================== Turso Engine Tests ====================

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_download_returns_none_without_metadata() {
        generic_download_returns_none_without_metadata(&AccelerationEngine::Turso).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_create_snapshot_updates_metadata() {
        generic_create_snapshot_updates_metadata(&AccelerationEngine::Turso).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_download_snapshot_with_valid_metadata() {
        generic_download_snapshot_with_valid_metadata(&AccelerationEngine::Turso).await;
    }

    #[cfg(feature = "turso")]
    #[test]
    fn turso_engine_does_not_support_compaction() {
        generic_engine_compaction_support(&AccelerationEngine::Turso, false);
    }

    // ==================== OnChange Policy Tests ====================

    #[cfg(feature = "duckdb")]
    fn build_manager_with_on_change_policy(
        store: &Arc<InMemory>,
        local_path: PathBuf,
        schema: &SchemaRef,
    ) -> SnapshotManager {
        build_manager(
            Arc::clone(store),
            local_path,
            BootstrapOnFailureBehavior::Fallback,
            schema,
            false,
        )
        .with_snapshots_creation_policy(SnapshotsCreationPolicy::OnChange)
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn on_change_policy_skips_when_no_writes_occurred() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-on-change-no-writes".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();
        let manager = build_manager_with_on_change_policy(&store, local_path.clone(), &schema);

        let mutex = Arc::new(Mutex::new(()));

        // First, create an initial snapshot to establish existing snapshots
        let lock_guard = Arc::clone(&mutex).lock_owned().await;
        let first_result = manager
            .create_snapshot(
                &schema,
                lock_guard,
                Some(1_704_153_600_000_i64),
                ForceCreate(true),
            )
            .await
            .expect("first snapshot should succeed");
        assert!(first_result.is_some(), "First snapshot should be created");

        // Now with OnChange policy and None last_updated_at, snapshot should be skipped
        // because there are existing snapshots but no new writes
        let lock_guard2 = Arc::clone(&mutex).lock_owned().await;
        let result = manager
            .create_snapshot(&schema, lock_guard2, None, ForceCreate(false))
            .await
            .expect("create_snapshot should not error");

        assert!(
            result.is_none(),
            "Snapshot should be skipped when no writes occurred (last_updated_at is None)"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn on_change_policy_skips_when_timestamp_matches_previous() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-on-change-duplicate".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();

        // First, create a snapshot with Always policy to establish baseline
        let manager_always = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Fallback,
            &schema,
            false,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = Arc::clone(&mutex).lock_owned().await;
        let timestamp = 1_704_153_600_000_i64; // 2024-01-02 00:00:00 UTC

        let first_result = manager_always
            .create_snapshot(&schema, lock_guard, Some(timestamp), ForceCreate(true))
            .await
            .expect("first snapshot");
        assert!(first_result.is_some(), "First snapshot should be created");

        // Now try with OnChange policy and same timestamp - should skip
        let manager_on_change = build_manager_with_on_change_policy(&store, local_path, &schema);

        let lock_guard2 = Arc::clone(&mutex).lock_owned().await;
        let second_result = manager_on_change
            .create_snapshot(&schema, lock_guard2, Some(timestamp), ForceCreate(false))
            .await
            .expect("second snapshot should not error");

        assert!(
            second_result.is_none(),
            "Snapshot should be skipped when timestamp matches previous snapshot"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn on_change_policy_creates_first_snapshot_with_valid_timestamp() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-on-change-first".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();
        // Use OnChange policy directly - no previous snapshot exists
        let manager = build_manager_with_on_change_policy(&store, local_path, &schema);

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;
        let timestamp = 1_704_153_600_000_i64;

        // First snapshot with valid timestamp should be created even with OnChange policy
        // (no existing snapshots means force_create is automatically set to true)
        let result = manager
            .create_snapshot(&schema, lock_guard, Some(timestamp), ForceCreate(false))
            .await
            .expect("create_snapshot should not error");

        assert!(
            result.is_some(),
            "First snapshot should be created with OnChange policy when timestamp is valid and no previous snapshot exists"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn on_change_policy_creates_snapshot_when_timestamp_differs() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-on-change-new-update".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();

        // First, create a snapshot to establish baseline
        let manager = build_manager(
            Arc::clone(&store),
            local_path.clone(),
            BootstrapOnFailureBehavior::Fallback,
            &schema,
            false,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = Arc::clone(&mutex).lock_owned().await;
        let first_timestamp = 1_704_153_600_000_i64;

        let first_result = manager
            .create_snapshot(
                &schema,
                lock_guard,
                Some(first_timestamp),
                ForceCreate(true),
            )
            .await
            .expect("first snapshot");
        assert!(first_result.is_some(), "First snapshot should be created");

        // Now try with OnChange policy and different timestamp - should create
        let manager_on_change = build_manager_with_on_change_policy(&store, local_path, &schema);

        let lock_guard2 = Arc::clone(&mutex).lock_owned().await;
        let second_timestamp = 1_704_240_000_000_i64; // Next day

        let second_result = manager_on_change
            .create_snapshot(
                &schema,
                lock_guard2,
                Some(second_timestamp),
                ForceCreate(false),
            )
            .await
            .expect("second snapshot");

        assert!(
            second_result.is_some(),
            "Snapshot should be created when timestamp differs from previous"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn always_policy_creates_snapshot_even_when_no_writes() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-always-no-writes".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();
        let manager = build_manager(
            Arc::clone(&store),
            local_path,
            BootstrapOnFailureBehavior::Fallback,
            &schema,
            false,
        );

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        // With Always policy, snapshot should be created even with None last_updated_at
        // (also, no existing snapshots means force_create is automatically set)
        let result = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
            .await
            .expect("create_snapshot should not error");

        assert!(
            result.is_some(),
            "With Always policy, snapshot should be created even when no writes occurred"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn force_creates_snapshot_when_no_metadata_exists() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-force-no-metadata".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        let schema = sample_schema();
        // Use OnChange policy which would normally skip when last_updated_at is None
        let manager = build_manager_with_on_change_policy(&store, local_path, &schema);

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        // Even with OnChange policy and None last_updated_at, snapshot should be created
        // because no metadata exists (no prior snapshots)
        let result = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
            .await
            .expect("create_snapshot should not error");

        assert!(
            result.is_some(),
            "Snapshot should be force-created when no metadata exists, even with OnChange policy"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn force_creates_snapshot_when_metadata_exists_but_no_snapshots_for_dataset() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-force-no-dataset-snapshots".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        // Create metadata with a different dataset (not our test dataset)
        let metadata_path = Path::from(SNAPSHOT_BASE_PATH).child(METADATA_FILE_NAME);
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);
        metadata.datasets.insert(
            "other_dataset".to_string(),
            DatasetMetadata {
                name: "other_dataset".to_string(),
                ..Default::default()
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;

        let schema = sample_schema();
        let manager = build_manager_with_on_change_policy(&store, local_path, &schema);

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        // Even with OnChange policy and None last_updated_at, snapshot should be created
        // because metadata exists but has no snapshots for THIS dataset
        let result = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
            .await
            .expect("create_snapshot should not error");

        assert!(
            result.is_some(),
            "Snapshot should be force-created when metadata has no snapshots for this dataset"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn force_creates_snapshot_when_metadata_has_snapshots_but_no_files_exist() {
        let store = Arc::new(InMemory::new());
        let contents = b"snapshot-force-no-files".to_vec();
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        temp_file.write_all(&contents).expect("write temp snapshot");
        temp_file.flush().expect("flush temp snapshot");
        let temp_path = temp_file.into_temp_path();
        let local_path = temp_path.to_path_buf();

        // Create metadata that claims snapshots exist, but don't actually create the files
        let schema = sample_schema();
        let metadata_path = Path::from(SNAPSHOT_BASE_PATH).child(METADATA_FILE_NAME);
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);
        let schema_metadata =
            SchemaMetadata::from_schema(0, &schema).expect("schema serialization");
        metadata.datasets.insert(
            DATASET_NAME.to_string(),
            DatasetMetadata {
                name: DATASET_NAME.to_string(),
                schemas: vec![schema_metadata],
                current_schema_id: 0,
                snapshots: vec![SnapshotEntry {
                    snapshot_id: 0,
                    timestamp_ms: 1_704_153_600_000,
                    snapshot: format!("{SNAPSHOT_URI_PREFIX}/fake_snapshot.db"),
                    snapshot_checksum: "fake_checksum".to_string(),
                    snapshot_checksum_algorithm: "sha256".to_string(),
                    snapshot_size: 1000,
                    snapshot_last_updated_at_ms: Some(1_704_153_600_000),
                }],
                current_snapshot_id: Some(0),
                properties: HashMap::default(),
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;
        // Note: We intentionally don't create the actual snapshot file

        let manager = build_manager_with_on_change_policy(&store, local_path, &schema);

        let mutex = Arc::new(Mutex::new(()));
        let lock_guard = mutex.lock_owned().await;

        // Even with OnChange policy and None last_updated_at, snapshot should be created
        // because metadata has snapshots but actual files don't exist
        let result = manager
            .create_snapshot(&schema, lock_guard, None, ForceCreate(false))
            .await
            .expect("create_snapshot should not error");

        assert!(
            result.is_some(),
            "Snapshot should be force-created when metadata has snapshots but no actual files exist"
        );
    }

    // ========== Tests for Snapshot Metadata API ==========

    /// Builds a `SnapshotManager` for metadata-only API tests.
    /// Uses `AccelerationLayout::None` since API tests only read/write metadata.
    fn build_manager_for_api_tests(store: Arc<InMemory>) -> SnapshotManager {
        let object_store: Arc<dyn ObjectStore> = store;
        let snapshot_engine = create_snapshot_engine(&AccelerationEngine::Cayenne, false);

        SnapshotManager {
            dataset_name: DATASET_NAME.to_string(),
            snapshots_location: Path::from(SNAPSHOT_BASE_PATH),
            snapshot_location_uri: SNAPSHOT_URI_PREFIX.to_string(),
            layout: AccelerationLayout::None,
            snapshot_engine,
            object_store,
            bootstrap_failure_behavior: BootstrapOnFailureBehavior::Warn,
            checkpointer_factory: None,
            snapshots_creation_policy: SnapshotsCreationPolicy::default(),
        }
    }

    #[tokio::test]
    async fn get_snapshot_summary_returns_empty_when_no_metadata() {
        let store = Arc::new(InMemory::new());
        let manager = build_manager_for_api_tests(Arc::clone(&store));

        let summary = manager
            .get_snapshot_summary()
            .await
            .expect("get_snapshot_summary should succeed");

        assert_eq!(summary.dataset_name, DATASET_NAME);
        assert_eq!(summary.location, SNAPSHOT_URI_PREFIX);
        assert_eq!(summary.last_updated_ms, 0);
        assert!(summary.current_snapshot_id.is_none());
        assert!(summary.snapshots.is_empty());
    }

    #[tokio::test]
    async fn get_snapshot_summary_returns_snapshots_from_metadata() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        let snapshot_entry = SnapshotEntry {
            snapshot_id: 100,
            timestamp_ms: 1_704_153_600_000,
            snapshot: "snapshots/test_snapshot.db".to_string(),
            snapshot_checksum: "abc123".to_string(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: 1024,
            snapshot_last_updated_at_ms: Some(1_704_153_600_000),
        };

        let mut datasets = HashMap::new();
        datasets.insert(
            DATASET_NAME.to_string(),
            DatasetMetadata {
                name: DATASET_NAME.to_string(),
                schemas: vec![],
                current_schema_id: 0,
                snapshots: vec![snapshot_entry],
                current_snapshot_id: Some(100),
                properties: HashMap::new(),
            },
        );

        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: 1_704_240_000_000,
            datasets,
        };

        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_for_api_tests(Arc::clone(&store));
        let summary = manager
            .get_snapshot_summary()
            .await
            .expect("get_snapshot_summary should succeed");

        assert_eq!(summary.dataset_name, DATASET_NAME);
        assert_eq!(summary.location, SNAPSHOT_URI_PREFIX);
        assert_eq!(summary.last_updated_ms, 1_704_240_000_000);
        assert_eq!(summary.current_snapshot_id, Some(100));
        assert_eq!(summary.snapshots.len(), 1);

        let snapshot_info = &summary.snapshots[0];
        assert_eq!(snapshot_info.snapshot_id, 100);
        assert_eq!(snapshot_info.timestamp_ms, 1_704_153_600_000);
        assert_eq!(snapshot_info.checksum, "abc123");
        assert!(snapshot_info.is_current);
    }

    #[tokio::test]
    async fn get_snapshot_returns_snapshot_when_exists() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        let snapshot_entry = SnapshotEntry {
            snapshot_id: 200,
            timestamp_ms: 1_704_153_600_000,
            snapshot: "snapshots/test_snapshot.db".to_string(),
            snapshot_checksum: "def456".to_string(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: 2048,
            snapshot_last_updated_at_ms: None,
        };

        let mut datasets = HashMap::new();
        datasets.insert(
            DATASET_NAME.to_string(),
            DatasetMetadata {
                name: DATASET_NAME.to_string(),
                schemas: vec![],
                current_schema_id: 0,
                snapshots: vec![snapshot_entry],
                current_snapshot_id: Some(200),
                properties: HashMap::new(),
            },
        );

        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: 1_704_240_000_000,
            datasets,
        };

        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_for_api_tests(Arc::clone(&store));
        let snapshot_info = manager
            .get_snapshot(200)
            .await
            .expect("get_snapshot should succeed");

        assert_eq!(snapshot_info.snapshot_id, 200);
        assert_eq!(snapshot_info.checksum, "def456");
        assert_eq!(snapshot_info.size_bytes, 2048);
        assert!(snapshot_info.is_current);
    }

    #[tokio::test]
    async fn get_snapshot_returns_error_when_snapshot_not_found() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        let mut datasets = HashMap::new();
        datasets.insert(
            DATASET_NAME.to_string(),
            DatasetMetadata {
                name: DATASET_NAME.to_string(),
                schemas: vec![],
                current_schema_id: 0,
                snapshots: vec![],
                current_snapshot_id: None,
                properties: HashMap::new(),
            },
        );

        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: 1_704_240_000_000,
            datasets,
        };

        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_for_api_tests(Arc::clone(&store));
        let result = manager.get_snapshot(999).await;

        assert!(result.is_err());
        let err = result.expect_err("should return error");
        assert!(matches!(err, SnapshotApiError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn get_snapshot_returns_error_when_no_metadata() {
        let store = Arc::new(InMemory::new());
        let manager = build_manager_for_api_tests(Arc::clone(&store));

        let result = manager.get_snapshot(100).await;

        assert!(result.is_err());
        let err = result.expect_err("should return error");
        assert!(matches!(err, SnapshotApiError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn set_current_snapshot_updates_metadata() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        let snapshot_entry1 = SnapshotEntry {
            snapshot_id: 100,
            timestamp_ms: 1_704_153_600_000,
            snapshot: "snapshots/snapshot1.db".to_string(),
            snapshot_checksum: "abc123".to_string(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: 1024,
            snapshot_last_updated_at_ms: None,
        };

        let snapshot_entry2 = SnapshotEntry {
            snapshot_id: 200,
            timestamp_ms: 1_704_240_000_000,
            snapshot: "snapshots/snapshot2.db".to_string(),
            snapshot_checksum: "def456".to_string(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: 2048,
            snapshot_last_updated_at_ms: None,
        };

        let mut datasets = HashMap::new();
        datasets.insert(
            DATASET_NAME.to_string(),
            DatasetMetadata {
                name: DATASET_NAME.to_string(),
                schemas: vec![],
                current_schema_id: 0,
                snapshots: vec![snapshot_entry1, snapshot_entry2],
                current_snapshot_id: Some(200), // Initially set to 200
                properties: HashMap::new(),
            },
        );

        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: 1_704_240_000_000,
            datasets,
        };

        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_for_api_tests(Arc::clone(&store));

        // Set current snapshot to 100
        manager
            .set_current_snapshot(100)
            .await
            .expect("set_current_snapshot should succeed");

        // Verify the change by reading the summary
        let summary = manager
            .get_snapshot_summary()
            .await
            .expect("get_snapshot_summary should succeed");

        assert_eq!(summary.current_snapshot_id, Some(100));

        // Verify is_current flags are correct
        let snapshot_100 = summary
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == 100)
            .expect("snapshot 100 should exist");
        let snapshot_200 = summary
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == 200)
            .expect("snapshot 200 should exist");

        assert!(snapshot_100.is_current);
        assert!(!snapshot_200.is_current);
    }

    #[tokio::test]
    async fn set_current_snapshot_returns_error_when_snapshot_not_found() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        let snapshot_entry = SnapshotEntry {
            snapshot_id: 100,
            timestamp_ms: 1_704_153_600_000,
            snapshot: "snapshots/snapshot1.db".to_string(),
            snapshot_checksum: "abc123".to_string(),
            snapshot_checksum_algorithm: SNAPSHOT_CHECKSUM_ALGORITHM.to_string(),
            snapshot_size: 1024,
            snapshot_last_updated_at_ms: None,
        };

        let mut datasets = HashMap::new();
        datasets.insert(
            DATASET_NAME.to_string(),
            DatasetMetadata {
                name: DATASET_NAME.to_string(),
                schemas: vec![],
                current_schema_id: 0,
                snapshots: vec![snapshot_entry],
                current_snapshot_id: Some(100),
                properties: HashMap::new(),
            },
        );

        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: 1_704_240_000_000,
            datasets,
        };

        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_for_api_tests(Arc::clone(&store));

        // Try to set a non-existent snapshot
        let result = manager.set_current_snapshot(999).await;

        assert!(result.is_err());
        let err = result.expect_err("should return error");
        assert!(matches!(err, SnapshotApiError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn set_current_snapshot_returns_error_when_no_metadata() {
        let store = Arc::new(InMemory::new());
        let manager = build_manager_for_api_tests(Arc::clone(&store));

        let result = manager.set_current_snapshot(100).await;

        assert!(result.is_err());
        let err = result.expect_err("should return error");
        assert!(matches!(err, SnapshotApiError::SnapshotNotFound { .. }));
    }

    #[tokio::test]
    async fn get_snapshot_summary_returns_error_on_unsupported_version() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create metadata with unsupported version
        let metadata_json = serde_json::json!({
            "format-version": 999,
            "location": SNAPSHOT_URI_PREFIX,
            "last-updated-ms": 1_704_240_000_000_i64
        });

        let bytes = serde_json::to_vec_pretty(&metadata_json).expect("serialize metadata");
        store
            .put(&metadata_path, bytes.into())
            .await
            .expect("write metadata");

        let manager = build_manager_for_api_tests(Arc::clone(&store));
        let result = manager.get_snapshot_summary().await;

        assert!(result.is_err());
        let err = result.expect_err("should return error");
        assert!(matches!(err, SnapshotApiError::UnsupportedVersion { .. }));
    }

    #[tokio::test]
    async fn get_snapshot_summary_returns_empty_when_dataset_not_in_metadata() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create metadata without our dataset
        let metadata = SnapshotMetadata {
            format_version: SNAPSHOT_METADATA_FORMAT_VERSION,
            location: SNAPSHOT_URI_PREFIX.to_string(),
            last_updated_ms: 1_704_240_000_000,
            datasets: HashMap::new(), // No datasets
        };

        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_for_api_tests(Arc::clone(&store));
        let summary = manager
            .get_snapshot_summary()
            .await
            .expect("get_snapshot_summary should succeed");

        assert_eq!(summary.dataset_name, DATASET_NAME);
        assert!(summary.snapshots.is_empty());
        assert!(summary.current_snapshot_id.is_none());
    }

    // ========== Tests for has_existing_snapshots path matching ==========

    /// Creates a `SnapshotManager` with a specific dataset name for testing path matching.
    fn build_manager_with_dataset_name(
        store: Arc<InMemory>,
        dataset_name: &str,
    ) -> SnapshotManager {
        let object_store: Arc<dyn ObjectStore> = store;
        let snapshot_engine = create_snapshot_engine(&AccelerationEngine::Cayenne, false);

        SnapshotManager {
            dataset_name: dataset_name.to_string(),
            snapshots_location: Path::from(SNAPSHOT_BASE_PATH),
            snapshot_location_uri: SNAPSHOT_URI_PREFIX.to_string(),
            layout: AccelerationLayout::None,
            snapshot_engine,
            object_store,
            bootstrap_failure_behavior: BootstrapOnFailureBehavior::Warn,
            checkpointer_factory: None,
            snapshots_creation_policy: SnapshotsCreationPolicy::default(),
        }
    }

    #[tokio::test]
    async fn has_existing_snapshots_returns_false_when_no_metadata() {
        let store = Arc::new(InMemory::new());
        let manager = build_manager_with_dataset_name(Arc::clone(&store), "foo");

        let result = manager.has_existing_snapshots().await;

        assert!(!result, "Should return false when no metadata exists");
    }

    #[tokio::test]
    async fn has_existing_snapshots_returns_false_when_metadata_has_no_snapshots() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create metadata with dataset but no snapshots
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);
        metadata.datasets.insert(
            "foo".to_string(),
            DatasetMetadata {
                name: "foo".to_string(),
                snapshots: vec![], // Empty snapshots
                ..Default::default()
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_with_dataset_name(Arc::clone(&store), "foo");
        let result = manager.has_existing_snapshots().await;

        assert!(
            !result,
            "Should return false when metadata exists but has no snapshots"
        );
    }

    #[tokio::test]
    async fn has_existing_snapshots_returns_false_when_metadata_has_snapshots_but_files_missing() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create metadata claiming snapshots exist
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);
        metadata.datasets.insert(
            "foo".to_string(),
            DatasetMetadata {
                name: "foo".to_string(),
                snapshots: vec![SnapshotEntry {
                    snapshot_id: 0,
                    timestamp_ms: 1_704_153_600_000,
                    snapshot: "snapshots/month=2025-01/day=2025-01-01/dataset=foo/foo.db"
                        .to_string(),
                    snapshot_checksum: "abc123".to_string(),
                    snapshot_checksum_algorithm: "SHA256".to_string(),
                    snapshot_size: 1024,
                    snapshot_last_updated_at_ms: None,
                }],
                current_snapshot_id: Some(0),
                ..Default::default()
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;
        // Note: We intentionally don't create the actual snapshot file

        let manager = build_manager_with_dataset_name(Arc::clone(&store), "foo");
        let result = manager.has_existing_snapshots().await;

        assert!(
            !result,
            "Should return false when metadata has snapshots but actual files don't exist"
        );
    }

    #[tokio::test]
    async fn has_existing_snapshots_returns_true_when_metadata_and_files_exist() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create the actual snapshot file
        let snapshot_path = base
            .child("month=2025-01")
            .child("day=2025-01-01")
            .child("dataset=foo")
            .child("foo.db");
        store
            .put(&snapshot_path, Bytes::from_static(b"snapshot data").into())
            .await
            .expect("write snapshot file");

        // Create metadata referencing the snapshot
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);
        metadata.datasets.insert(
            "foo".to_string(),
            DatasetMetadata {
                name: "foo".to_string(),
                snapshots: vec![SnapshotEntry {
                    snapshot_id: 0,
                    timestamp_ms: 1_704_153_600_000,
                    snapshot: "snapshots/month=2025-01/day=2025-01-01/dataset=foo/foo.db"
                        .to_string(),
                    snapshot_checksum: "abc123".to_string(),
                    snapshot_checksum_algorithm: "SHA256".to_string(),
                    snapshot_size: 1024,
                    snapshot_last_updated_at_ms: None,
                }],
                current_snapshot_id: Some(0),
                ..Default::default()
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;

        let manager = build_manager_with_dataset_name(Arc::clone(&store), "foo");
        let result = manager.has_existing_snapshots().await;

        assert!(
            result,
            "Should return true when both metadata and actual files exist"
        );
    }

    /// This test verifies that `has_existing_snapshots` correctly distinguishes between
    /// datasets with similar prefixes (e.g., "foo" vs "foobar").
    ///
    /// Previously, the code used `.contains()` for substring matching which would
    /// incorrectly match "dataset=foo" against paths containing "dataset=foobar".
    /// The fix uses exact path segment matching with `.split('/').any(|s| s == partition)`.
    #[tokio::test]
    async fn has_existing_snapshots_does_not_match_dataset_name_prefix() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create a snapshot file for "foobar" dataset (NOT "foo")
        let snapshot_path = base
            .child("month=2025-01")
            .child("day=2025-01-01")
            .child("dataset=foobar") // Note: "foobar", not "foo"
            .child("foobar.db");
        store
            .put(&snapshot_path, Bytes::from_static(b"snapshot data").into())
            .await
            .expect("write snapshot file");

        // Create metadata for BOTH datasets, but only foobar has actual files
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);

        // "foo" dataset - metadata exists but no actual file
        metadata.datasets.insert(
            "foo".to_string(),
            DatasetMetadata {
                name: "foo".to_string(),
                snapshots: vec![SnapshotEntry {
                    snapshot_id: 0,
                    timestamp_ms: 1_704_153_600_000,
                    snapshot: "snapshots/month=2025-01/day=2025-01-01/dataset=foo/foo.db"
                        .to_string(),
                    snapshot_checksum: "abc123".to_string(),
                    snapshot_checksum_algorithm: "SHA256".to_string(),
                    snapshot_size: 1024,
                    snapshot_last_updated_at_ms: None,
                }],
                current_snapshot_id: Some(0),
                ..Default::default()
            },
        );

        // "foobar" dataset - both metadata and actual file exist
        metadata.datasets.insert(
            "foobar".to_string(),
            DatasetMetadata {
                name: "foobar".to_string(),
                snapshots: vec![SnapshotEntry {
                    snapshot_id: 0,
                    timestamp_ms: 1_704_153_600_000,
                    snapshot: "snapshots/month=2025-01/day=2025-01-01/dataset=foobar/foobar.db"
                        .to_string(),
                    snapshot_checksum: "def456".to_string(),
                    snapshot_checksum_algorithm: "SHA256".to_string(),
                    snapshot_size: 2048,
                    snapshot_last_updated_at_ms: None,
                }],
                current_snapshot_id: Some(0),
                ..Default::default()
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;

        // Check "foo" dataset - should return FALSE because:
        // - metadata exists with snapshots
        // - BUT no actual file exists for "dataset=foo" (only "dataset=foobar" exists)
        let manager_foo = build_manager_with_dataset_name(Arc::clone(&store), "foo");
        let result_foo = manager_foo.has_existing_snapshots().await;

        assert!(
            !result_foo,
            "Should return false for 'foo' dataset - must not match 'dataset=foobar' path. \
             This test catches the substring matching bug where 'dataset=foo' incorrectly \
             matches paths containing 'dataset=foobar'."
        );

        // Check "foobar" dataset - should return TRUE
        let manager_foobar = build_manager_with_dataset_name(Arc::clone(&store), "foobar");
        let result_foobar = manager_foobar.has_existing_snapshots().await;

        assert!(
            result_foobar,
            "Should return true for 'foobar' dataset - actual file exists"
        );
    }

    /// Additional test: Verify suffix matching doesn't cause false positives either.
    /// E.g., "bar" should not match "dataset=foobar".
    #[tokio::test]
    async fn has_existing_snapshots_does_not_match_dataset_name_suffix() {
        let store = Arc::new(InMemory::new());
        let base = Path::from(SNAPSHOT_BASE_PATH);
        let metadata_path = base.child(METADATA_FILE_NAME);

        // Create a snapshot file for "foobar" dataset
        let snapshot_path = base
            .child("month=2025-01")
            .child("day=2025-01-01")
            .child("dataset=foobar")
            .child("foobar.db");
        store
            .put(&snapshot_path, Bytes::from_static(b"snapshot data").into())
            .await
            .expect("write snapshot file");

        // Create metadata for "bar" dataset (which is a suffix of "foobar")
        let mut metadata = SnapshotMetadata::empty(SNAPSHOT_URI_PREFIX.to_string(), 0);
        metadata.datasets.insert(
            "bar".to_string(),
            DatasetMetadata {
                name: "bar".to_string(),
                snapshots: vec![SnapshotEntry {
                    snapshot_id: 0,
                    timestamp_ms: 1_704_153_600_000,
                    snapshot: "snapshots/month=2025-01/day=2025-01-01/dataset=bar/bar.db"
                        .to_string(),
                    snapshot_checksum: "abc123".to_string(),
                    snapshot_checksum_algorithm: "SHA256".to_string(),
                    snapshot_size: 1024,
                    snapshot_last_updated_at_ms: None,
                }],
                current_snapshot_id: Some(0),
                ..Default::default()
            },
        );
        write_metadata(&store, &metadata_path, &metadata).await;

        let manager_bar = build_manager_with_dataset_name(Arc::clone(&store), "bar");
        let result_bar = manager_bar.has_existing_snapshots().await;

        assert!(
            !result_bar,
            "Should return false for 'bar' dataset - must not match 'dataset=foobar' path"
        );
    }
}
