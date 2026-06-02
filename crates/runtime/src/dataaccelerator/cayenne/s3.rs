/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use aws_sdk_credential_bridge::{S3CredentialProvider, get_bucket_name};
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::BoxStream;
use object_store::{
    ClientOptions, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RetryConfig, aws::AmazonS3Builder, client::SpawnedReqwestConnector, path::Path,
};
use runtime_parameters::ParameterSpec;
use runtime_secrets::get_params_with_secrets;
use secrecy::ExposeSecret;
use snafu::{ResultExt, Snafu};
use tokio::sync::Mutex;
use url::Url;

use super::AccelerationSource;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create S3 Express One Zone object store: {source}"))]
    ObjectStoreCreation {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid S3 Express One Zone URL '{url}': {source}"))]
    InvalidS3Url {
        url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Standard S3 paths are not supported for Cayenne acceleration. Only S3 Express One Zone is supported. \
        S3 Express One Zone buckets use the naming convention: 's3://{{bucket-name}}--{{zone-id}}--x-s3/'. \
        Received: '{path}'"
    ))]
    StandardS3NotSupported { path: String },

    #[snafu(display("Failed to create S3 Express One Zone directory bucket '{bucket}': {source}"))]
    DirectoryBucketCreation {
        bucket: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "AWS authentication failed for S3 Express One Zone. Your AWS credentials may be expired or invalid."
    ))]
    S3AuthenticationFailed,

    #[snafu(display(
        "Cannot determine S3 Express One Zone bucket info: {reason}. \
        Either provide a valid 'cayenne_file_path' with an S3 Express bucket, or specify 'cayenne_s3_zone_ids' to auto-generate the bucket name."
    ))]
    CannotAutoCreateBucket { reason: String },

    #[snafu(display(
        "Multi-zone S3 Express One Zone write failed: {failed_count} of {total_zones} zone(s) failed. \
        Failure details: {failed_zones}. Rollback was attempted for successful zones."
    ))]
    MultiZoneWriteFailed {
        failed_count: usize,
        total_zones: usize,
        failed_zones: String,
    },

    #[snafu(display(
        "Multi-zone S3 Express One Zone delete failed for {failed_count} of {total_zones} zone(s). \
        Failure details: {failed_zones}. Some replicas may still retain the object."
    ))]
    MultiZoneDeleteFailed {
        failed_count: usize,
        total_zones: usize,
        failed_zones: String,
    },

    #[snafu(display(
        "Multi-zone S3 Express rollback failed for zone '{zone}': {reason}. \
        Manual cleanup may be required."
    ))]
    MultiZoneRollbackFailed { zone: String, reason: String },

    #[snafu(display("Invalid S3 Express One Zone bucket name: {reason}"))]
    InvalidBucketName { reason: String },

    #[snafu(display("Invalid S3 configuration for Cayenne acceleration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },
}
type Result<T, E = Error> = std::result::Result<T, E>;

const MULTI_ZONE_STORE_NAME: &str = "S3ExpressMultiZone";

#[derive(Debug, Clone)]
struct ZoneStore {
    zone_id: String,
    store: Arc<dyn ObjectStore>,
}

#[derive(Debug, Clone)]
struct MultiZoneS3ExpressStore {
    zones: Vec<ZoneStore>,
}

#[derive(Clone)]
enum ListMode {
    Prefix(Option<Path>),
    Offset { prefix: Option<Path>, offset: Path },
}

struct ListFallbackState {
    zones: Vec<ZoneStore>,
    mode: ListMode,
    current_zone_idx: usize,
    current_stream: Option<BoxStream<'static, object_store::Result<ObjectMeta>>>,
    emitted_from_current: bool,
}

impl MultiZoneS3ExpressStore {
    fn new(zones: Vec<ZoneStore>) -> Self {
        Self { zones }
    }

    fn stores_len(&self) -> usize {
        self.zones.len()
    }

    /// Returns a deterministic read order for all read operations.
    ///
    /// Always prefers zone 0 (the listing primary) and falls back to other zones on
    /// read failures. Keeping list/get/head aligned to the same primary zone avoids
    /// potential metadata/content mismatches if replicas drift.
    fn read_order(&self) -> Vec<usize> {
        if self.zones.is_empty() {
            return Vec::new();
        }
        let mut order = Vec::with_capacity(self.zones.len());
        order.push(0);
        for idx in 0..self.zones.len() {
            if idx != 0 {
                order.push(idx);
            }
        }
        order
    }

    fn multi_zone_write_error(&self, failed_zone_details: &[String]) -> object_store::Error {
        object_store::Error::Generic {
            store: MULTI_ZONE_STORE_NAME,
            source: Box::new(Error::MultiZoneWriteFailed {
                failed_count: failed_zone_details.len(),
                total_zones: self.stores_len(),
                failed_zones: failed_zone_details.join(", "),
            }),
        }
    }

    fn multi_zone_delete_error(&self, failed_zone_details: &[String]) -> object_store::Error {
        object_store::Error::Generic {
            store: MULTI_ZONE_STORE_NAME,
            source: Box::new(Error::MultiZoneDeleteFailed {
                failed_count: failed_zone_details.len(),
                total_zones: self.stores_len(),
                failed_zones: failed_zone_details.join(", "),
            }),
        }
    }

    async fn rollback_put(&self, location: &Path, successful_zone_ids: &[String]) -> Vec<String> {
        let successful_set: HashSet<&str> = successful_zone_ids
            .iter()
            .map(std::string::String::as_str)
            .collect();

        let mut rollback_failures = Vec::new();

        for zone in &self.zones {
            if !successful_set.contains(zone.zone_id.as_str()) {
                continue;
            }
            if let Err(e) = zone.store.delete(location).await {
                rollback_failures.push(format!("rollback {}: {e}", zone.zone_id));
                tracing::warn!(
                    "{}",
                    Error::MultiZoneRollbackFailed {
                        zone: zone.zone_id.clone(),
                        reason: e.to_string(),
                    }
                );
            }
        }

        rollback_failures
    }

    async fn rollback_copy(&self, to: &Path, successful_zone_ids: &[String]) -> Vec<String> {
        let successful_set: HashSet<&str> = successful_zone_ids
            .iter()
            .map(std::string::String::as_str)
            .collect();

        let mut rollback_failures = Vec::new();

        for zone in &self.zones {
            if !successful_set.contains(zone.zone_id.as_str()) {
                continue;
            }
            if let Err(e) = zone.store.delete(to).await {
                rollback_failures.push(format!("rollback {}: {e}", zone.zone_id));
                tracing::warn!(
                    "{}",
                    Error::MultiZoneRollbackFailed {
                        zone: zone.zone_id.clone(),
                        reason: e.to_string(),
                    }
                );
            }
        }

        rollback_failures
    }

    async fn delete_one(&self, location: &Path) -> object_store::Result<()> {
        let mut all_not_found = true;
        let mut last_not_found: Option<object_store::Error> = None;
        let mut failed_zone_details = Vec::new();
        for zone in &self.zones {
            match zone.store.delete(location).await {
                Ok(()) => {
                    all_not_found = false;
                }
                Err(err) => {
                    if let object_store::Error::NotFound { .. } = err {
                        tracing::debug!(
                            "Object '{}' not found in zone '{}' during delete: {err}",
                            location,
                            zone.zone_id
                        );
                        last_not_found = Some(err);
                    } else {
                        tracing::warn!(
                            "Failed to delete object '{}' from zone '{}': {err}",
                            location,
                            zone.zone_id
                        );
                        failed_zone_details.push(format!("{}: {err}", zone.zone_id));
                        all_not_found = false;
                    }
                }
            }
        }

        if !failed_zone_details.is_empty() {
            Err(self.multi_zone_delete_error(&failed_zone_details))
        } else if all_not_found {
            Err(
                last_not_found.unwrap_or_else(|| object_store::Error::NotFound {
                    path: location.to_string(),
                    source: "No configured S3 zones".into(),
                }),
            )
        } else {
            Ok(())
        }
    }

    fn build_list_stream_for_zone(
        zone: &ZoneStore,
        mode: &ListMode,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match mode {
            ListMode::Prefix(prefix) => zone.store.list(prefix.as_ref()),
            ListMode::Offset { prefix, offset } => {
                zone.store.list_with_offset(prefix.as_ref(), offset)
            }
        }
    }

    fn list_with_fallback(
        &self,
        mode: ListMode,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.zones.is_empty() {
            let path = match &mode {
                ListMode::Prefix(prefix) => prefix
                    .as_ref()
                    .map_or_else(String::new, std::string::ToString::to_string),
                ListMode::Offset { offset, .. } => offset.to_string(),
            };
            return futures::stream::once(async {
                Err(object_store::Error::NotFound {
                    path,
                    source: "No configured S3 zones".into(),
                })
            })
            .boxed();
        }

        let initial_state = ListFallbackState {
            zones: self.zones.clone(),
            mode,
            current_zone_idx: 0,
            current_stream: None,
            emitted_from_current: false,
        };

        futures::stream::unfold(initial_state, |mut state| async move {
            loop {
                if state.current_stream.is_none() {
                    if state.current_zone_idx >= state.zones.len() {
                        return None;
                    }

                    let zone = state.zones[state.current_zone_idx].clone();
                    state.current_stream = Some(Self::build_list_stream_for_zone(&zone, &state.mode));
                    state.emitted_from_current = false;
                }

                let next_item = {
                    let Some(stream) = state.current_stream.as_mut() else {
                        unreachable!("current_stream should be initialized before polling");
                    };
                    stream.next().await
                };

                match next_item {
                    Some(Ok(meta)) => {
                        state.emitted_from_current = true;
                        return Some((Ok(meta), state));
                    }
                    Some(Err(err)) if !state.emitted_from_current => {
                        let zone_id = state.zones[state.current_zone_idx].zone_id.clone();
                        tracing::debug!(
                            "S3 Express list stream failed for zone '{}' before yielding rows: {err}",
                            zone_id
                        );
                        state.current_zone_idx += 1;
                        state.current_stream = None;
                    }
                    Some(Err(err)) => return Some((Err(err), state)),
                    None => return None,
                }
            }
        })
        .boxed()
    }
}

impl Display for MultiZoneS3ExpressStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{MULTI_ZONE_STORE_NAME}")
    }
}

#[async_trait::async_trait]
impl ObjectStore for MultiZoneS3ExpressStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mut put_futures = Vec::with_capacity(self.zones.len());
        for zone in &self.zones {
            put_futures.push(async {
                let result = zone
                    .store
                    .put_opts(location, payload.clone(), opts.clone())
                    .await;
                (zone.zone_id.clone(), result)
            });
        }

        let results = join_all(put_futures).await;

        let mut first_success: Option<PutResult> = None;
        let mut successful_zone_ids = Vec::new();
        let mut failed_zone_details = Vec::new();

        for (zone_id, result) in results {
            match result {
                Ok(put_result) => {
                    if first_success.is_none() {
                        first_success = Some(put_result);
                    }
                    successful_zone_ids.push(zone_id);
                }
                Err(err) => {
                    tracing::warn!("Multi-zone put failed for zone '{zone_id}': {err}");
                    failed_zone_details.push(format!("{zone_id}: {err}"));
                }
            }
        }

        if failed_zone_details.is_empty() {
            return first_success.ok_or_else(|| object_store::Error::Generic {
                store: MULTI_ZONE_STORE_NAME,
                source: "No successful multi-zone write result".into(),
            });
        }

        let rollback_failures = self.rollback_put(location, &successful_zone_ids).await;
        failed_zone_details.extend(rollback_failures);
        Err(self.multi_zone_write_error(&failed_zone_details))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let mut uploads: Vec<Arc<Mutex<ZoneMultipartUpload>>> =
            Vec::with_capacity(self.zones.len());
        for zone in &self.zones {
            match zone.store.put_multipart_opts(location, opts.clone()).await {
                Ok(upload) => {
                    uploads.push(Arc::new(Mutex::new(ZoneMultipartUpload {
                        zone_id: zone.zone_id.clone(),
                        store: Arc::clone(&zone.store),
                        upload,
                    })));
                }
                Err(err) => {
                    // Abort any already-started uploads to avoid leaked multipart state.
                    for started in uploads {
                        let mut started = started.lock().await;
                        if let Err(abort_err) = started.upload.abort().await {
                            tracing::warn!(
                                "Failed to abort multipart upload for zone '{}' after zone '{}' init failure: {abort_err}",
                                started.zone_id,
                                zone.zone_id,
                            );
                        }
                    }
                    return Err(err);
                }
            }
        }

        Ok(Box::new(MultiZoneMultipartUpload {
            location: location.clone(),
            total_zones: self.stores_len(),
            uploads,
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let read_order = self.read_order();
        let mut last_err: Option<object_store::Error> = None;

        for idx in read_order {
            let zone = &self.zones[idx];
            match zone.store.get_opts(location, options.clone()).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    tracing::debug!(
                        "S3 Express read failed for zone '{}' and object '{}': {err}",
                        zone.zone_id,
                        location
                    );
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| object_store::Error::NotFound {
            path: location.to_string(),
            source: "No configured S3 zones".into(),
        }))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store.delete_one(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_with_fallback(ListMode::Prefix(prefix.cloned()))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_with_fallback(ListMode::Offset {
            prefix: prefix.cloned(),
            offset: offset.clone(),
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut last_err: Option<object_store::Error> = None;
        for zone in &self.zones {
            match zone.store.list_with_delimiter(prefix).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    tracing::debug!(
                        "S3 Express list_with_delimiter failed for zone '{}': {err}",
                        zone.zone_id
                    );
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| object_store::Error::NotFound {
            path: prefix.map_or_else(String::new, std::string::ToString::to_string),
            source: "No configured S3 zones".into(),
        }))
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let mut successful_zone_ids = Vec::new();
        let mut failed_zone_details = Vec::new();

        for zone in &self.zones {
            match zone.store.copy_opts(from, to, options.clone()).await {
                Ok(()) => successful_zone_ids.push(zone.zone_id.clone()),
                Err(err) => {
                    tracing::warn!(
                        "Failed to copy object '{}' -> '{}' in zone '{}': {err}",
                        from,
                        to,
                        zone.zone_id
                    );
                    failed_zone_details.push(format!("{}: {err}", zone.zone_id));
                }
            }
        }

        if failed_zone_details.is_empty() {
            return Ok(());
        }

        let rollback_failures = self.rollback_copy(to, &successful_zone_ids).await;
        failed_zone_details.extend(rollback_failures);
        Err(self.multi_zone_write_error(&failed_zone_details))
    }
}

#[derive(Debug)]
struct ZoneMultipartUpload {
    zone_id: String,
    store: Arc<dyn ObjectStore>,
    upload: Box<dyn MultipartUpload>,
}

#[derive(Debug)]
struct MultiZoneMultipartUpload {
    location: Path,
    total_zones: usize,
    uploads: Vec<Arc<Mutex<ZoneMultipartUpload>>>,
}

#[async_trait::async_trait]
impl MultipartUpload for MultiZoneMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let uploads = self.uploads.iter().map(Arc::clone).collect::<Vec<_>>();
        let mut part_futures = Vec::with_capacity(uploads.len());
        for zone_upload in &uploads {
            let zone_upload = Arc::clone(zone_upload);
            let part_data = data.clone();
            part_futures.push(async move {
                let (zone_id, upload_part) = {
                    let mut zone_upload = zone_upload.lock().await;
                    (
                        zone_upload.zone_id.clone(),
                        zone_upload.upload.put_part(part_data),
                    )
                };
                (zone_id, upload_part.await)
            });
        }

        Box::pin(async move {
            let results = join_all(part_futures).await;
            let mut first_error: Option<object_store::Error> = None;

            for (zone_id, result) in results {
                if let Err(err) = result {
                    tracing::warn!("Failed multipart part upload in zone '{zone_id}': {err}");
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }

            if let Some(err) = first_error {
                // Best-effort abort on part upload failure to avoid leaked multipart uploads.
                for zone_upload in uploads {
                    let mut zone_upload = zone_upload.lock().await;
                    if let Err(abort_err) = zone_upload.upload.abort().await {
                        tracing::warn!(
                            "Failed multipart abort in zone '{}' after part upload failure: {abort_err}",
                            zone_upload.zone_id
                        );
                    }
                }
                return Err(err);
            }
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut first_success: Option<PutResult> = None;
        let mut successful_zone_ids = Vec::new();
        let mut failed_zone_details = Vec::new();

        for zone_upload in &self.uploads {
            let mut zone_upload = zone_upload.lock().await;
            match zone_upload.upload.complete().await {
                Ok(put_result) => {
                    if first_success.is_none() {
                        first_success = Some(put_result);
                    }
                    successful_zone_ids.push(zone_upload.zone_id.clone());
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed multipart completion in zone '{}': {err}",
                        zone_upload.zone_id
                    );
                    failed_zone_details.push(format!("{}: {err}", zone_upload.zone_id));
                    let _ = zone_upload.upload.abort().await;
                }
            }
        }

        if failed_zone_details.is_empty() {
            return first_success.ok_or_else(|| object_store::Error::Generic {
                store: MULTI_ZONE_STORE_NAME,
                source: "No successful multi-zone multipart completion result".into(),
            });
        }

        let successful_set: HashSet<&str> = successful_zone_ids
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let mut rollback_targets: Vec<(String, Arc<dyn ObjectStore>)> = Vec::new();
        for zone_upload in &self.uploads {
            let zone_upload = zone_upload.lock().await;
            if successful_set.contains(zone_upload.zone_id.as_str()) {
                rollback_targets
                    .push((zone_upload.zone_id.clone(), Arc::clone(&zone_upload.store)));
            }
        }

        for (zone_id, store) in rollback_targets {
            if let Err(err) = store.delete(&self.location).await {
                tracing::warn!(
                    "{}",
                    Error::MultiZoneRollbackFailed {
                        zone: zone_id,
                        reason: err.to_string(),
                    }
                );
            }
        }

        Err(object_store::Error::Generic {
            store: MULTI_ZONE_STORE_NAME,
            source: Box::new(Error::MultiZoneWriteFailed {
                failed_count: failed_zone_details.len(),
                total_zones: self.total_zones,
                failed_zones: failed_zone_details.join(", "),
            }),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut first_error: Option<object_store::Error> = None;
        for zone_upload in &self.uploads {
            let mut zone_upload = zone_upload.lock().await;
            if let Err(err) = zone_upload.upload.abort().await {
                tracing::warn!(
                    "Failed multipart abort in zone '{}': {err}",
                    zone_upload.zone_id
                );
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }
}

pub(crate) const S3_PARAMS_LEN: usize = 10;
pub(crate) const S3_PARAMETERS: [ParameterSpec; S3_PARAMS_LEN] = [
    ParameterSpec::component("s3_region")
        .description("AWS region for S3 Express One Zone storage. If not specified, derived from cayenne_s3_zone_ids."),
    ParameterSpec::component("s3_endpoint")
        .description("Custom S3 endpoint URL for S3 Express One Zone."),
    ParameterSpec::component("s3_key")
        .description("AWS access key ID for S3 authentication.")
        .secret(),
    ParameterSpec::component("s3_secret")
        .description("AWS secret access key for S3 authentication.")
        .secret(),
    ParameterSpec::component("s3_session_token")
        .description("AWS session token for temporary credentials (optional).")
        .secret(),
    ParameterSpec::component("s3_auth")
        .description("Authentication method for S3 Express One Zone. Options: 'iam_role' (default, uses environment credentials), 'key' (uses explicit cayenne_s3_key/cayenne_s3_secret).")
        .default("iam_role")
        .one_of(&["iam_role", "key"]),
    ParameterSpec::component("s3_client_timeout")
        .description("Timeout for S3 client operations (e.g., '30s', '5m'). Default: 120s.")
        .default("120s"),
    ParameterSpec::component("s3_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3. Default: false.")
        .default("false"),
    ParameterSpec::component("s3_unsigned_payload")
        .description("Use unsigned payload for S3 Express One Zone requests. Only applies when S3 Express mode is enabled (via cayenne_s3_zone_ids or directory bucket path). Skips SHA-256 computation for request body, improving upload performance. S3 Express One Zone uses session-based auth, making payload signing unnecessary. Default: true.")
        .default("true"),
    // S3 Express One Zone auto-generation parameter
    ParameterSpec::component("s3_zone_ids")
        .description("Comma-separated list of Availability Zone IDs for S3 Express One Zone storage (e.g., 'usw2-az1' or 'usw2-az1,usw2-az2'). When specified without 'cayenne_file_path', auto-generates bucket name from app and dataset name, and creates the bucket if needed. For multi-zone redundancy, specify multiple zones. Data is written to all zones with ACID guarantees - writes succeed only if all zones succeed. Reads are served from the primary (first) zone with fallback to replicas."),
];

/// Returns true if the path is an S3 Express One Zone path.
///
/// S3 Express One Zone buckets have the naming convention: `{base-name}--{zone-id}--x-s3`
/// Example: `s3://mybucket--usw2-az1--x-s3/prefix/`
#[must_use]
pub fn is_s3_express_path(path: &str) -> bool {
    path.starts_with("s3://") && path.contains("--x-s3")
}

/// Validates that the path is either a local path or an S3 Express One Zone path.
/// Standard S3 paths are not supported.
pub fn validate_file_path(path: &str) -> Result<()> {
    if path.starts_with("s3://") && !is_s3_express_path(path) {
        return Err(Error::StandardS3NotSupported {
            path: path.to_string(),
        });
    }
    Ok(())
}

/// Returns true if the data path for this source is an S3 Express One Zone path.
///
/// This returns true if either:
/// - `cayenne_file_path` is set to an S3 Express path, or
/// - `cayenne_s3_zone_ids` is set (which means we'll auto-generate S3 Express paths)
pub fn is_s3_express_data_path(source: &dyn AccelerationSource) -> bool {
    source.acceleration().is_some_and(|a| {
        // Check for explicit S3 Express path
        if a.params
            .get("cayenne_file_path")
            .is_some_and(|path| is_s3_express_path(path))
        {
            return true;
        }
        // Check for auto-generated path via zone_ids
        a.params.contains_key("cayenne_s3_zone_ids")
    })
}

/// Returns true if multi-zone S3 Express One Zone storage is configured.
///
/// Multi-zone is enabled when `cayenne_s3_zone_ids` contains multiple zone IDs.
pub fn is_multi_zone_s3_express(source: &dyn AccelerationSource) -> bool {
    source.acceleration().is_some_and(|a| {
        a.params
            .get("cayenne_s3_zone_ids")
            .is_some_and(|zone_ids| zone_ids.contains(','))
    })
}

/// Returns the list of zone IDs for S3 Express One Zone storage.
///
/// Parses the `cayenne_s3_zone_ids` parameter as a comma-separated list of zone IDs.
/// Normalizes entries (trim + lowercase) and deduplicates, preserving first-seen order.
/// Returns an empty vector if the parameter is not set.
pub fn get_s3_zone_ids(source: &dyn AccelerationSource) -> Result<Vec<String>> {
    source
        .acceleration()
        .and_then(|a| a.params.get("cayenne_s3_zone_ids"))
        .map_or_else(
            || Ok(Vec::new()),
            |zone_ids| {
            let mut seen = HashSet::new();
            let mut ordered = Vec::new();
            let mut duplicates = Vec::new();
            zone_ids
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .for_each(|zone_id| {
                    if seen.insert(zone_id.clone()) {
                        ordered.push(zone_id);
                    } else {
                        duplicates.push(zone_id);
                    }
                });

            if !duplicates.is_empty() {
                return Err(Error::InvalidConfiguration {
                    detail: Arc::from(format!(
                        "Duplicate values found in 'cayenne_s3_zone_ids': {}. Use a unique, comma-separated zone list.",
                        duplicates.join(", ")
                    )),
                });
            }

                Ok(ordered)
            },
        )
}

/// Extracts the zone ID from an S3 Express One Zone bucket name.
///
/// S3 Express One Zone bucket names have the format: `{base-name}--{zone-id}--x-s3`
/// Example: `mybucket--usw2-az1--x-s3` returns `Some("usw2-az1")`
#[must_use]
pub fn extract_zone_id_from_bucket(bucket_name: &str) -> Option<&str> {
    // Find the last occurrence of "--x-s3"
    let suffix_start = bucket_name.rfind("--x-s3")?;
    let before_suffix = &bucket_name[..suffix_start];

    // Find the second-to-last "--" which separates the base name from zone id
    let zone_start = before_suffix.rfind("--")?;
    Some(&before_suffix[zone_start + 2..])
}

/// Generates an S3 Express One Zone bucket name from the app name and dataset name.
///
/// Format: `spice-{app_name}-{dataset_name}--{zone_id}--x-s3`
///
/// The names are sanitized to comply with S3 bucket naming rules:
/// - Lowercase only
/// - Only alphanumeric and hyphens allowed
/// - Max 63 characters total (we leave room for the suffix)
pub fn generate_bucket_name(app_name: &str, dataset_name: &str, zone_id: &str) -> Result<String> {
    // Sanitize names for S3 bucket naming requirements
    fn sanitize(s: &str) -> String {
        s.to_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>()
            // Collapse multiple consecutive hyphens into one
            .split('-')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("-")
    }

    let sanitized_app = sanitize(app_name);
    let sanitized_dataset = sanitize(dataset_name);

    if sanitized_app.is_empty() || sanitized_dataset.is_empty() {
        return Err(Error::InvalidBucketName {
            reason: "App or dataset name is empty after sanitization".to_string(),
        });
    }

    // S3 bucket names can be max 63 chars
    // We need room for: "spice-" (6) + "--" (2) + zone_id + "--x-s3" (6) = 14 + zone_id.len()
    let suffix_len = 2_usize.saturating_add(zone_id.len()).saturating_add(6); // "--{zone_id}--x-s3"
    let prefix_len = 6_usize; // "spice-"

    // Check if zone_id is too long (max 49 chars to leave at least 1 char for name)
    // 63 (max) - 6 (prefix) - 1 (hyphen) - 6 (--x-s3) - 2 (--) = 48 max for zone_id + 1 for name
    let required_fixed_len = prefix_len.saturating_add(1).saturating_add(suffix_len);
    if required_fixed_len >= 63 {
        return Err(Error::InvalidBucketName {
            reason: format!(
                "Zone ID '{zone_id}' is too long ({} chars). Maximum zone ID length is {} characters to fit S3 bucket naming constraints.",
                zone_id.len(),
                63_usize
                    .saturating_sub(prefix_len)
                    .saturating_sub(1)
                    .saturating_sub(8) // 8 = 2 + 6 (-- + --x-s3)
            ),
        });
    }

    let max_name_len = 63_usize.saturating_sub(required_fixed_len);

    if max_name_len == 0 {
        return Err(Error::InvalidBucketName {
            reason: format!(
                "Zone ID '{zone_id}' leaves no room for bucket naming (max length <= 0)"
            ),
        });
    }

    let base_name = format!("{sanitized_app}-{sanitized_dataset}");
    let truncated_name = if base_name.len() > max_name_len {
        base_name[..max_name_len].trim_end_matches('-').to_string()
    } else {
        base_name
    };

    if truncated_name.is_empty() {
        return Err(Error::InvalidBucketName {
            reason: "Bucket name is empty after truncation".to_string(),
        });
    }

    Ok(format!("spice-{truncated_name}--{zone_id}--x-s3"))
}

/// Creates an S3 Express One Zone directory bucket if it doesn't exist.
///
/// Uses `initiate_config_with_credentials` from `aws_sdk_credential_bridge` for credential handling,
/// supporting both explicit credentials and IAM role-based authentication.
///
/// Returns `Ok(true)` if a new bucket was created, `Ok(false)` if the bucket already existed.
/// Returns `Err` if the bucket creation or verification fails.
pub async fn create_s3_express_bucket_if_needed(
    bucket_name: &str,
    zone_id: &str,
    region: &str,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
) -> Result<bool> {
    use aws_sdk_s3::types::{
        BucketInfo, BucketType, CreateBucketConfiguration, DataRedundancy, LocationInfo,
        LocationType,
    };

    // Use the credential bridge to build config with proper credential handling
    let config_loader = aws_sdk_credential_bridge::initiate_config_with_credentials(
        "cayenne-s3-express",
        region.to_string(),
        access_key_id.clone(),
        secret_access_key.clone(),
        session_token.clone(),
        None,
    )
    .await;

    let sdk_config = config_loader.load().await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    // Check if bucket already exists by trying to head it
    let bucket_exists = match s3_client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => true,
        Err(e) => {
            // Check if it's a "not found" error vs. a permissions error
            let service_error = e.into_service_error();
            if !service_error.is_not_found() {
                // Could be access denied or other error - log but continue to try creation
                tracing::debug!(
                    "Head bucket check returned error (will attempt creation): {:?}",
                    service_error
                );
            }
            false
        }
    };

    // If bucket exists, skip creation but still validate write/read access
    let bucket_created = if bucket_exists {
        tracing::debug!(
            "S3 Express bucket '{}' already exists, validating access...",
            bucket_name
        );
        false
    } else {
        tracing::info!(
            "Creating S3 Express One Zone bucket '{}' in zone '{}' (region: {})",
            bucket_name,
            zone_id,
            region
        );

        // Create the bucket configuration for S3 Express One Zone (directory bucket)
        // Note: LocationConstraint is NOT supported for directory buckets - only Location and Bucket are used
        let bucket_config = CreateBucketConfiguration::builder()
            .location(
                LocationInfo::builder()
                    .r#type(LocationType::AvailabilityZone)
                    .name(zone_id)
                    .build(),
            )
            .bucket(
                BucketInfo::builder()
                    .r#type(BucketType::Directory)
                    .data_redundancy(DataRedundancy::SingleAvailabilityZone)
                    .build(),
            )
            .build();

        // Attempt to create the bucket
        match s3_client
            .create_bucket()
            .bucket(bucket_name)
            .create_bucket_configuration(bucket_config)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                let service_error = e.into_service_error();
                // Check if bucket already exists (race condition or created by another process)
                if service_error.is_bucket_already_exists()
                    || service_error.is_bucket_already_owned_by_you()
                {
                    tracing::debug!(
                        "S3 Express bucket '{}' already exists (concurrent creation)",
                        bucket_name
                    );
                    false
                } else {
                    // Check for authentication errors using structured error codes first
                    let code = service_error.meta().code();
                    if matches!(
                        code,
                        Some(
                            "UnauthorizedException"
                                | "ExpiredToken"
                                | "InvalidAccessKeyId"
                                | "SignatureDoesNotMatch"
                                | "AccessDenied"
                        )
                    ) || is_auth_error(&service_error)
                    {
                        return Err(Error::S3AuthenticationFailed);
                    }
                    return Err(Error::DirectoryBucketCreation {
                        bucket: bucket_name.to_string(),
                        source: Box::new(service_error),
                    });
                }
            }
        }
    };

    // Verify bucket access with a write/read test using object_store
    // This ensures the same client configuration used for actual data uploads is validated
    tracing::info!(
        "Validating S3 Express One Zone bucket access for '{}' using object_store...",
        bucket_name
    );

    // Note: Validation uses default timeout/unsigned_payload settings because this runs
    // during bucket creation/verification, before user parameters are parsed. The defaults
    // (120s timeout, unsigned payload enabled) are appropriate for validation requests.
    let object_store = build_s3_object_store_for_validation(
        bucket_name,
        zone_id,
        region,
        access_key_id.clone(),
        secret_access_key.clone(),
        session_token.clone(),
        None,
        None,
    )
    .await?;

    let test_path = object_store::path::Path::from(".cayenne_write_test");
    let test_content = bytes::Bytes::from_static(b"cayenne_s3_express_verification");

    // Write test object using object_store
    object_store
        .put(&test_path, test_content.clone().into())
        .await
        .map_err(|e| {
            if is_auth_error(&e) {
                Error::S3AuthenticationFailed
            } else {
                Error::DirectoryBucketCreation {
                    bucket: bucket_name.to_string(),
                    source: Box::new(e),
                }
            }
        })?;

    // Read test object back using object_store
    let get_result = object_store.get(&test_path).await.map_err(|e| {
        if is_auth_error(&e) {
            Error::S3AuthenticationFailed
        } else {
            Error::DirectoryBucketCreation {
                bucket: bucket_name.to_string(),
                source: Box::new(e),
            }
        }
    })?;

    let body = get_result
        .bytes()
        .await
        .boxed()
        .context(DirectoryBucketCreationSnafu {
            bucket: bucket_name.to_string(),
        })?;

    if body.as_ref() != test_content.as_ref() {
        return Err(Error::DirectoryBucketCreation {
            bucket: bucket_name.to_string(),
            source: "S3 write/read verification failed: content mismatch".into(),
        });
    }

    // Clean up test object
    let _ = object_store.delete(&test_path).await;

    if bucket_created {
        tracing::info!(
            "Created and verified S3 Express One Zone bucket: {}",
            bucket_name
        );
    } else {
        tracing::info!(
            "Validated S3 Express One Zone bucket access: {}",
            bucket_name
        );
    }

    Ok(bucket_created)
}

/// Builds an S3 object store for validation using the same configuration as data uploads.
///
/// This ensures validation uses the exact same client configuration (credentials, endpoint, S3 Express mode)
/// that will be used for actual data uploads, preventing configuration mismatches.
///
/// Optional parameters allow callers to match the main client configuration:
/// - `timeout`: Client timeout (defaults to 120s if None)
/// - `unsigned_payload`: Whether to skip payload signing (defaults to true if None)
#[expect(clippy::too_many_arguments)]
pub async fn build_s3_object_store_for_validation(
    bucket_name: &str,
    zone_id: &str,
    region: &str,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    timeout: Option<std::time::Duration>,
    unsigned_payload: Option<bool>,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    let io_runtime = tokio::runtime::Handle::current();
    let mut s3_builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
        .with_region(region);

    // Use provided settings or defaults
    let effective_unsigned_payload = unsigned_payload.unwrap_or(true);
    let effective_timeout = timeout.unwrap_or(std::time::Duration::from_secs(120));

    // Configure S3 Express One Zone mode
    tracing::debug!(
        "Building validation object store for S3 Express bucket (zone: {}, unsigned_payload: {})",
        zone_id,
        effective_unsigned_payload
    );
    s3_builder = s3_builder
        .with_s3_express(true)
        .with_virtual_hosted_style_request(true)
        .with_unsigned_payload(effective_unsigned_payload);

    // Build the S3 Express endpoint with virtual-hosted-style format
    let express_endpoint =
        format!("https://{bucket_name}.s3express-{zone_id}.{region}.amazonaws.com");
    tracing::debug!("Validation using S3 Express endpoint: {express_endpoint}");
    s3_builder = s3_builder.with_endpoint(&express_endpoint);

    // Set timeout for S3 Express validation requests
    let client_options = ClientOptions::default().with_timeout(effective_timeout);
    s3_builder = s3_builder.with_client_options(client_options);

    // Handle credentials
    let mut load_credentials_from_environment = true;
    if let (Some(key), Some(secret)) = (access_key_id, secret_access_key) {
        s3_builder = s3_builder.with_access_key_id(key);
        s3_builder = s3_builder.with_secret_access_key(secret);
        if let Some(token) = session_token {
            s3_builder = s3_builder.with_token(token);
        }
        load_credentials_from_environment = false;
    }

    // Load credentials from environment using our credential bridge
    if load_credentials_from_environment {
        tracing::debug!("Loading S3 credentials from environment for validation");
        match aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(Some(region)).await {
            Ok(Some(sdk_config)) => {
                if sdk_config.credentials_provider().is_some() {
                    tracing::debug!("Using S3 credentials provider from SDK config for validation");
                    s3_builder = s3_builder.with_credentials(Arc::new(
                        S3CredentialProvider::from_config(sdk_config.as_ref())
                            .boxed()
                            .context(ObjectStoreCreationSnafu)?,
                    ));
                }
            }
            Ok(None) => {
                tracing::warn!(
                    "No AWS SDK credentials available for S3 Express validation; assuming public access"
                );
            }
            Err(err) => {
                tracing::warn!("Unable to initialize AWS credentials for validation: {err}");
            }
        }
    }

    let store = s3_builder
        .build()
        .boxed()
        .context(ObjectStoreCreationSnafu)?;

    Ok(Arc::new(store))
}

/// Extracts S3 bucket information (bucket name, zone ID, region, credentials) from the source configuration.
///
/// If `cayenne_file_path` is provided as an S3 Express path, extracts info from that.
/// Otherwise, generates a bucket name from the app name and dataset name using `cayenne_s3_zone_ids`.
///
/// # Returns
///
/// A tuple of (bucket name, zone ID, region, access key, secret key, session token)
#[expect(
    clippy::type_complexity,
    reason = "Return type represents distinct S3 configuration fields"
)]
pub fn get_s3_bucket_info(
    source: &dyn AccelerationSource,
    data_path: &str,
) -> Result<(
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    let acceleration = source
        .acceleration()
        .ok_or_else(|| Error::InvalidConfiguration {
            detail: Arc::from("Acceleration settings required for S3 bucket info"),
        })?;

    // Try to extract zone ID from the bucket name in the path
    let url = Url::parse(data_path).boxed().context(InvalidS3UrlSnafu {
        url: data_path.to_string(),
    })?;

    let bucket_name = get_bucket_name(&url).boxed().context(InvalidS3UrlSnafu {
        url: data_path.to_string(),
    })?;

    // Extract zone ID from bucket name (e.g., "mybucket--usw2-az1--x-s3" -> "usw2-az1")
    let zone_id = extract_zone_id_from_bucket(bucket_name)
        .or_else(|| {
            acceleration
                .params
                .get("cayenne_s3_zone_ids")
                .and_then(|ids| ids.split(',').next())
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .ok_or_else(|| Error::CannotAutoCreateBucket {
            reason: "Could not determine zone ID. Either use a valid S3 Express bucket name format (bucket--zone-id--x-s3) or specify 'cayenne_s3_zone_ids' parameter".to_string(),
        })?
        .to_string();

    // Get region from params or derive from zone ID
    let region = acceleration
        .params
        .get("cayenne_s3_region")
        .cloned()
        .or_else(|| derive_region_from_zone(&zone_id))
        .ok_or_else(|| Error::CannotAutoCreateBucket {
            reason: format!(
                "Could not determine region. Specify 'cayenne_s3_region' parameter. Zone ID: {zone_id}"
            ),
        })?;

    // Get optional credentials from params
    let s3_auth = acceleration
        .params
        .get("cayenne_s3_auth")
        .map_or("iam_role", String::as_str);

    let (access_key, secret_key, session_token) = if s3_auth == "key" {
        (
            acceleration.params.get("cayenne_s3_key").cloned(),
            acceleration.params.get("cayenne_s3_secret").cloned(),
            acceleration.params.get("cayenne_s3_session_token").cloned(),
        )
    } else {
        (None, None, None)
    };

    Ok((
        bucket_name.to_string(),
        zone_id,
        region,
        access_key,
        secret_key,
        session_token,
    ))
}

/// Derives the AWS region from a zone ID.
///
/// Zone IDs follow the pattern: `{region-code}-az{n}` (e.g., `usw2-az1`, `use1-az4`)
/// We need to map the abbreviated region code to the full AWS region name.
#[must_use]
pub fn derive_region_from_zone(zone_id: &str) -> Option<String> {
    // Extract the region prefix from zone ID (e.g., "usw2" from "usw2-az1")
    let region_prefix = zone_id.split("-az").next()?;

    // Map abbreviated region codes to full AWS region names
    let region = match region_prefix {
        // US regions
        "use1" => "us-east-1",
        "use2" => "us-east-2",
        "usw1" => "us-west-1",
        "usw2" => "us-west-2",
        // EU regions
        "euw1" => "eu-west-1",
        "euw2" => "eu-west-2",
        "euw3" => "eu-west-3",
        "euc1" => "eu-central-1",
        "euc2" => "eu-central-2",
        "eun1" => "eu-north-1",
        "eus1" => "eu-south-1",
        "eus2" => "eu-south-2",
        // AP regions
        "apne1" => "ap-northeast-1",
        "apne2" => "ap-northeast-2",
        "apne3" => "ap-northeast-3",
        "apse1" => "ap-southeast-1",
        "apse2" => "ap-southeast-2",
        "apse3" => "ap-southeast-3",
        "apse4" => "ap-southeast-4",
        "apse5" => "ap-southeast-5",
        "aps1" => "ap-south-1",
        "aps2" => "ap-south-2",
        "ape1" => "ap-east-1",
        // Other regions
        "sae1" => "sa-east-1",
        "cac1" => "ca-central-1",
        "caw1" => "ca-west-1",
        "afs1" => "af-south-1",
        "mes1" => "me-south-1",
        "mec1" => "me-central-1",
        "ilc1" => "il-central-1",
        _ => return None,
    };

    Some(region.to_string())
}

#[derive(Debug, Clone)]
struct ResolvedS3Params {
    region: Option<String>,
    endpoint: Option<String>,
    key: Option<String>,
    secret: Option<String>,
    session_token: Option<String>,
    auth: String,
    client_timeout: Option<String>,
    allow_http: bool,
    unsigned_payload: bool,
}

async fn resolve_s3_params(source: &dyn AccelerationSource) -> ResolvedS3Params {
    let raw_params = source
        .acceleration()
        .map(|a| a.params.clone())
        .unwrap_or_default();
    let secrets = source.runtime().secrets();
    let params = get_params_with_secrets(secrets, &raw_params).await;

    let get_param =
        |key: &str| -> Option<String> { params.get(key).map(|v| v.expose_secret().to_string()) };

    let s3_region = get_param("cayenne_s3_region");
    let s3_endpoint = get_param("cayenne_s3_endpoint");
    let s3_key = get_param("cayenne_s3_key");
    let s3_secret = get_param("cayenne_s3_secret");
    let s3_session_token = get_param("cayenne_s3_session_token");
    let s3_auth = get_param("cayenne_s3_auth").unwrap_or_else(|| "iam_role".to_string());
    let s3_client_timeout = get_param("cayenne_s3_client_timeout");
    let s3_allow_http =
        get_param("cayenne_s3_allow_http").is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let s3_unsigned_payload =
        get_param("cayenne_s3_unsigned_payload").is_none_or(|v| !v.eq_ignore_ascii_case("false"));

    ResolvedS3Params {
        region: s3_region,
        endpoint: s3_endpoint,
        key: s3_key,
        secret: s3_secret,
        session_token: s3_session_token,
        auth: s3_auth,
        client_timeout: s3_client_timeout,
        allow_http: s3_allow_http,
        unsigned_payload: s3_unsigned_payload,
    }
}

async fn build_single_s3_store_for_path(
    data_path: &str,
    params: &ResolvedS3Params,
) -> Result<(Url, Arc<dyn ObjectStore>, String)> {
    let url = Url::parse(data_path).boxed().context(InvalidS3UrlSnafu {
        url: data_path.to_string(),
    })?;

    let bucket_name = get_bucket_name(&url).boxed().context(InvalidS3UrlSnafu {
        url: data_path.to_string(),
    })?;

    let zone_id = extract_zone_id_from_bucket(bucket_name)
        .ok_or_else(|| Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "S3 Express bucket name '{bucket_name}' does not contain a valid zone ID"
            )),
        })?
        .to_string();

    let effective_region = params
        .region
        .clone()
        .or_else(|| derive_region_from_zone(&zone_id))
        .ok_or_else(|| Error::InvalidConfiguration {
            detail: Arc::from(
                "Cannot determine AWS region for S3 Express One Zone. Specify 'cayenne_s3_region' or use an S3 Express bucket name that encodes the zone (bucket--<zone>--x-s3).",
            ),
        })?;

    let io_runtime = tokio::runtime::Handle::current();
    let mut s3_builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
        .with_allow_http(params.allow_http)
        .with_region(effective_region.clone());

    let retry_config = RetryConfig {
        max_retries: 3,
        retry_timeout: std::time::Duration::from_secs(600),
        ..Default::default()
    };
    s3_builder = s3_builder.with_retry(retry_config);

    let mut client_options =
        ClientOptions::default().with_timeout(std::time::Duration::from_secs(120));

    s3_builder = s3_builder
        .with_s3_express(true)
        .with_virtual_hosted_style_request(true)
        .with_unsigned_payload(params.unsigned_payload);

    if params.endpoint.is_none() {
        let express_endpoint =
            format!("https://{bucket_name}.s3express-{zone_id}.{effective_region}.amazonaws.com");
        s3_builder = s3_builder.with_endpoint(&express_endpoint);
    }

    if let Some(endpoint) = &params.endpoint {
        s3_builder = s3_builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            client_options = client_options.with_allow_http(true);
        }
    }

    if let Some(timeout) = &params.client_timeout {
        client_options = client_options.with_timeout(
            fundu::parse_duration(timeout)
                .boxed()
                .context(ObjectStoreCreationSnafu)?,
        );
    }

    let mut load_credentials_from_environment = true;
    if params.auth == "key" {
        if let (Some(key), Some(secret)) = (params.key.clone(), params.secret.clone()) {
            s3_builder = s3_builder.with_access_key_id(key);
            s3_builder = s3_builder.with_secret_access_key(secret);
            if let Some(token) = params.session_token.clone() {
                s3_builder = s3_builder.with_token(token);
            }
            load_credentials_from_environment = false;
        } else {
            return Err(Error::InvalidConfiguration {
                detail: Arc::from(
                    "S3 auth method 'key' requires both 'cayenne_s3_key' and 'cayenne_s3_secret' parameters",
                ),
            });
        }
    }

    s3_builder = s3_builder.with_client_options(client_options);

    if load_credentials_from_environment {
        match aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(Some(
            effective_region.as_str(),
        ))
        .await
        {
            Ok(Some(sdk_config)) => {
                if sdk_config.credentials_provider().is_some() {
                    s3_builder = s3_builder.with_credentials(Arc::new(
                        S3CredentialProvider::from_config(sdk_config.as_ref())
                            .boxed()
                            .context(ObjectStoreCreationSnafu)?,
                    ));
                }
            }
            Ok(None) => {
                tracing::warn!(
                    "No AWS SDK credentials available for Cayenne S3 Express storage; assuming public access"
                );
            }
            Err(err) => {
                tracing::warn!("Unable to initialize AWS credentials for Cayenne: {err}");
            }
        }
    }

    let store = s3_builder
        .build()
        .boxed()
        .context(ObjectStoreCreationSnafu)?;

    Ok((url, Arc::new(store), zone_id))
}

/// Build an S3 object store for S3 Express One Zone storage.
///
/// Returns `None` if the path is not an S3 path, or an error if S3 configuration is invalid.
pub async fn build_s3_object_store(
    source: &dyn AccelerationSource,
    data_path: String,
) -> Result<Option<cayenne::metadata::ObjectStoreConfig>> {
    // Check if this is S3 Express One Zone storage
    if !is_s3_express_data_path(source) {
        return Ok(None);
    }

    if !is_s3_express_path(&data_path) {
        return Ok(None);
    }

    tracing::debug!(
        "Building S3 Express One Zone object store for path: {}",
        data_path
    );

    let resolved = resolve_s3_params(source).await;

    let zone_ids = get_s3_zone_ids(source)?;
    if zone_ids.len() > 1 {
        let dataset_name = source.name().to_string().replace(['.', '/'], "_");
        let app_name = source.app().name.clone();

        let mut zone_stores = Vec::with_capacity(zone_ids.len());
        let mut primary_url: Option<Url> = None;

        for zone_id in &zone_ids {
            let bucket_name = generate_bucket_name(&app_name, &dataset_name, zone_id)?;
            let zone_data_path = format!("s3://{bucket_name}/{dataset_name}/");
            let (url, store, resolved_zone_id) =
                build_single_s3_store_for_path(&zone_data_path, &resolved).await?;

            if primary_url.is_none() {
                primary_url = Some(url.clone());
            }

            zone_stores.push(ZoneStore {
                zone_id: resolved_zone_id,
                store,
            });
        }

        let primary_url = primary_url.ok_or_else(|| Error::InvalidConfiguration {
            detail: Arc::from("Failed to determine primary S3 Express URL for multi-zone setup"),
        })?;

        tracing::info!(
            "Configured multi-zone S3 Express object store with {} zone(s)",
            zone_stores.len()
        );

        return Ok(Some(cayenne::metadata::ObjectStoreConfig {
            url: primary_url,
            store: Arc::new(MultiZoneS3ExpressStore::new(zone_stores)),
        }));
    }

    let (url, store, _) = build_single_s3_store_for_path(&data_path, &resolved).await?;

    tracing::info!(
        "S3 Express One Zone object store configured for data path: {}",
        data_path
    );

    Ok(Some(cayenne::metadata::ObjectStoreConfig { url, store }))
}

/// Returns true if the provided error or any of its sources indicates an authentication failure.
///
/// This walks the error chain and inspects both structured error sources (when available) and
/// a last-resort string check. This keeps the string matching centralized instead of duplicating
/// it at each call site.
pub fn is_auth_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        // If the object store wrapped another error (e.g., AWS SDK), inspect the source chain
        if let Some(store_err) = err.downcast_ref::<object_store::Error>()
            && let object_store::Error::Generic { source, .. } = store_err
            && is_auth_error(source.as_ref())
        {
            return true;
        }

        let error_str = format!("{err:?}");
        if error_str.contains("UnauthorizedException")
            || error_str.contains("ExpiredToken")
            || error_str.contains("InvalidAccessKeyId")
            || error_str.contains("SignatureDoesNotMatch")
            || error_str.contains("AccessDenied")
            || error_str.contains("403")
        {
            return true;
        }

        current = err.source();
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::{Error as ObjectStoreError, ListResult};
    use object_store::{PutMultipartOptions, PutOptions, PutPayload, PutResult};
    use object_store::{memory::InMemory, path::Path};

    #[derive(Debug)]
    struct FailingPutStore {
        inner: Arc<InMemory>,
    }

    impl Display for FailingPutStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingPutStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            Err(ObjectStoreError::Generic {
                store: "FailingPutStore",
                source: format!("forced put failure for {location}").into(),
            })
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Err(ObjectStoreError::NotSupported {
                source: "multipart not supported in FailingPutStore test double".into(),
            })
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn test_is_s3_express_path() {
        // Valid S3 Express One Zone paths
        assert!(is_s3_express_path("s3://mybucket--usw2-az1--x-s3/prefix/"));
        assert!(is_s3_express_path("s3://data-bucket--use1-az4--x-s3/"));
        assert!(is_s3_express_path(
            "s3://my-bucket-name--euw1-az2--x-s3/some/nested/path/"
        ));

        // Standard S3 paths (not Express)
        assert!(!is_s3_express_path("s3://mybucket/prefix/"));
        assert!(!is_s3_express_path("s3://mybucket-with-dashes/prefix/"));
        assert!(!is_s3_express_path("s3://mybucket--partial/prefix/"));

        // Non-S3 paths
        assert!(!is_s3_express_path("/local/path/"));
    }

    #[test]
    fn test_validate_file_path_accepts_local_paths() {
        validate_file_path("/local/path/data/").expect("local absolute path should be valid");
        validate_file_path("./relative/path/").expect("relative path should be valid");
        validate_file_path("/var/spice/data/").expect("another local path should be valid");
    }

    #[test]
    fn test_validate_file_path_accepts_s3_express() {
        validate_file_path("s3://mybucket--usw2-az1--x-s3/prefix/")
            .expect("S3 Express One Zone path should be valid");
        validate_file_path("s3://data--use1-az4--x-s3/cayenne/")
            .expect("another S3 Express One Zone path should be valid");
    }

    #[test]
    fn test_validate_file_path_rejects_standard_s3() {
        // Standard S3 paths should be rejected
        let result = validate_file_path("s3://mybucket/prefix/");
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        assert!(
            matches!(err, Error::StandardS3NotSupported { .. }),
            "Expected StandardS3NotSupported error, got: {err:?}"
        );

        let result = validate_file_path("s3://my-data-bucket/cayenne/data/");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_file_path_error_message() {
        let result = validate_file_path("s3://regular-bucket/data/");
        let err = result.expect_err("expected error");
        let error_message = err.to_string();

        assert!(
            error_message.contains("Standard S3 paths are not supported"),
            "Error message should mention standard S3 not supported: {error_message}"
        );
        assert!(
            error_message.contains("S3 Express One Zone"),
            "Error message should mention S3 Express One Zone: {error_message}"
        );
        assert!(
            error_message.contains("--x-s3"),
            "Error message should show the bucket naming convention: {error_message}"
        );
        assert!(
            error_message.contains("s3://regular-bucket/data/"),
            "Error message should include the invalid path: {error_message}"
        );
    }

    #[test]
    fn test_extract_zone_id_from_bucket() {
        // Valid S3 Express bucket names
        assert_eq!(
            extract_zone_id_from_bucket("mybucket--usw2-az1--x-s3"),
            Some("usw2-az1")
        );
        assert_eq!(
            extract_zone_id_from_bucket("data-bucket--use1-az4--x-s3"),
            Some("use1-az4")
        );
        assert_eq!(
            extract_zone_id_from_bucket("spice-myapp-dataset--euw1-az2--x-s3"),
            Some("euw1-az2")
        );

        // Invalid bucket names
        assert_eq!(extract_zone_id_from_bucket("mybucket"), None);
        assert_eq!(extract_zone_id_from_bucket("mybucket--partial"), None);
        assert_eq!(extract_zone_id_from_bucket("mybucket--x-s3"), None);
    }

    #[test]
    fn test_generate_bucket_name() {
        // Basic bucket name generation
        assert_eq!(
            generate_bucket_name("myapp", "orders", "usw2-az1").expect("bucket name"),
            "spice-myapp-orders--usw2-az1--x-s3"
        );

        // Special characters are sanitized
        assert_eq!(
            generate_bucket_name("My.App", "order_items", "use1-az4").expect("bucket name"),
            "spice-my-app-order-items--use1-az4--x-s3"
        );

        // Uppercase is converted to lowercase
        assert_eq!(
            generate_bucket_name("MyApp", "MyDataset", "euw1-az2").expect("bucket name"),
            "spice-myapp-mydataset--euw1-az2--x-s3"
        );

        // Names with multiple special chars
        assert_eq!(
            generate_bucket_name("my--app", "data..set", "aps1-az1").expect("bucket name"),
            "spice-my-app-data-set--aps1-az1--x-s3"
        );
    }

    #[test]
    fn test_generate_bucket_name_truncation() {
        // Very long names should be truncated to fit within S3 bucket name limits
        let long_app = "a".repeat(50);
        let long_dataset = "b".repeat(50);
        let bucket =
            generate_bucket_name(&long_app, &long_dataset, "usw2-az1").expect("bucket name");

        // S3 bucket names can be max 63 chars
        assert!(
            bucket.len() <= 63,
            "Bucket name should be <= 63 chars, got {} chars: {}",
            bucket.len(),
            bucket
        );
        assert!(bucket.ends_with("--usw2-az1--x-s3"));
        assert!(bucket.starts_with("spice-"));
    }

    #[test]
    fn test_derive_region_from_zone() {
        // US regions
        assert_eq!(
            derive_region_from_zone("use1-az1"),
            Some("us-east-1".to_string())
        );
        assert_eq!(
            derive_region_from_zone("use2-az2"),
            Some("us-east-2".to_string())
        );
        assert_eq!(
            derive_region_from_zone("usw1-az1"),
            Some("us-west-1".to_string())
        );
        assert_eq!(
            derive_region_from_zone("usw2-az1"),
            Some("us-west-2".to_string())
        );

        // EU regions
        assert_eq!(
            derive_region_from_zone("euw1-az1"),
            Some("eu-west-1".to_string())
        );
        assert_eq!(
            derive_region_from_zone("euc1-az2"),
            Some("eu-central-1".to_string())
        );

        // AP regions
        assert_eq!(
            derive_region_from_zone("apne1-az1"),
            Some("ap-northeast-1".to_string())
        );
        assert_eq!(
            derive_region_from_zone("apse1-az2"),
            Some("ap-southeast-1".to_string())
        );

        // Unknown zone format
        assert_eq!(derive_region_from_zone("unknown-az1"), None);
        assert_eq!(derive_region_from_zone("invalid"), None);
    }

    #[tokio::test]
    async fn test_multi_zone_put_rolls_back_on_partial_failure() {
        let healthy_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let failing_store = Arc::new(FailingPutStore {
            inner: Arc::new(InMemory::new()),
        }) as Arc<dyn ObjectStore>;

        let multi_zone_store = MultiZoneS3ExpressStore::new(vec![
            ZoneStore {
                zone_id: "use1-az1".to_string(),
                store: Arc::clone(&healthy_store),
            },
            ZoneStore {
                zone_id: "use1-az2".to_string(),
                store: failing_store,
            },
        ]);

        let location = Path::from("table/snapshot/data.vtx");
        let write_result = multi_zone_store
            .put_opts(
                &location,
                PutPayload::from("test-data".to_string()),
                PutOptions::default(),
            )
            .await;
        assert!(write_result.is_err(), "multi-zone write should fail");

        let head_result = healthy_store.head(&location).await;
        assert!(
            matches!(head_result, Err(ObjectStoreError::NotFound { .. })),
            "successful zone should be rolled back when any replica write fails"
        );
    }

    #[tokio::test]
    async fn test_striped_read_with_cross_zone_fallback() {
        let zone_a = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let zone_b = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let multi_zone_store = MultiZoneS3ExpressStore::new(vec![
            ZoneStore {
                zone_id: "usw2-az1".to_string(),
                store: Arc::clone(&zone_a),
            },
            ZoneStore {
                zone_id: "usw2-az2".to_string(),
                store: Arc::clone(&zone_b),
            },
        ]);

        let location = Path::from("table/snapshot/primary-fallback-key.vtx");

        zone_b
            .put_opts(
                &location,
                PutPayload::from("fallback-zone-data".to_string()),
                PutOptions::default(),
            )
            .await
            .expect("fallback zone write should succeed");

        let bytes: Bytes = multi_zone_store
            .get_opts(&location, GetOptions::default())
            .await
            .expect("read should succeed via cross-zone fallback")
            .bytes()
            .await
            .expect("read bytes should succeed");

        assert_eq!(bytes.as_ref(), b"fallback-zone-data");
    }
}
