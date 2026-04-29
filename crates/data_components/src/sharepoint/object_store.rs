/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! [`object_store::ObjectStore`] implementation backed by Microsoft Graph's
//! Drive APIs. Enables `DataFusion` to read, write, list, and delete files on
//! `SharePoint`/`OneDrive` via the standard object-store abstraction.
//!
//! Files are addressed as `sharepoint://{drive-ref}/{path}` — see [`super::url`].
//!
//! Write path semantics:
//! - Files ≤ [`INLINE_PUT_THRESHOLD`] go through `PUT /items/{id}/content`.
//! - Larger files use a resumable upload session (`POST createUploadSession`
//!   → chunked `PUT`s to the returned upload URL).
//! - [`ConflictBehavior`] controls overwrite semantics; the default `Replace`
//!   preserves `SharePoint`'s version-on-overwrite behavior.

#![expect(
    clippy::doc_markdown,
    reason = "prose-frequent identifiers (SharePoint, OneDrive, DataFusion, OAuth2) are clearer without backticks"
)]

use std::{
    fmt::{self, Debug, Display},
    io::Cursor,
    ops::Range,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::BoxStream};
use graph_rs_sdk::{
    GraphClient, GraphFailure,
    default_drive::DefaultDriveApiClient,
    drives::DrivesIdApiClient,
    http::{AsyncIterator, ResponseExt},
};
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult, UploadPart, path::Path,
};
use serde::Deserialize;
use tokio::sync::Mutex;

use super::url::DriveRef;
use crate::resilient_http::read_bounded_error_body;

/// SharePoint cap on a single `PUT /content` is 4 MiB. We use a slightly lower
/// threshold to leave headroom for request headers/overhead.
const INLINE_PUT_THRESHOLD: usize = 4 * 1024 * 1024 - 4096;

const STORE_TAG: &str = "SharepointObjectStore";

/// Controls what happens on write when a file with the same path exists.
/// Maps to SharePoint's `@microsoft.graph.conflictBehavior` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ConflictBehavior {
    /// Overwrite the existing file; SharePoint retains the prior content as a
    /// prior version. This is the default and matches SharePoint's standard
    /// versioning-on-overwrite behavior.
    #[default]
    Replace,
    /// Refuse to overwrite; return an error if the path exists.
    Fail,
    /// Write under a new name chosen by SharePoint (e.g. `file (1).csv`).
    Rename,
}

impl ConflictBehavior {
    fn as_graph_header(self) -> &'static str {
        match self {
            ConflictBehavior::Replace => "replace",
            ConflictBehavior::Fail => "fail",
            ConflictBehavior::Rename => "rename",
        }
    }
}

impl FromStr for ConflictBehavior {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "replace" => Ok(ConflictBehavior::Replace),
            "fail" => Ok(ConflictBehavior::Fail),
            "rename" => Ok(ConflictBehavior::Rename),
            other => Err(format!(
                "invalid conflict_behavior '{other}' (expected 'replace', 'fail', or 'rename')"
            )),
        }
    }
}

/// Hard cap on the total bytes a single `put`/`put_multipart` write may
/// materialize in memory before the store rejects the request. Large
/// writes go through `resumable_put`, which still buffers the whole
/// object in RAM (SharePoint's upload-session protocol requires the
/// total size up-front). Default: 1 GiB — big enough for typical
/// Parquet/CSV writes, small enough to fail loudly instead of OOM-ing
/// the runtime on a pathological `COPY TO`.
const DEFAULT_MAX_PUT_BYTES: usize = 1024 * 1024 * 1024;

/// Configuration applied to all operations against a [`SharepointObjectStore`].
#[derive(Debug, Clone, Copy)]
pub struct SharepointObjectStoreConfig {
    pub conflict_behavior: ConflictBehavior,
    /// Reject `put` / `put_multipart` writes larger than this many bytes
    /// instead of silently buffering them all in memory. Defaults to
    /// [`DEFAULT_MAX_PUT_BYTES`] (1 GiB).
    pub max_put_bytes: usize,
}

impl Default for SharepointObjectStoreConfig {
    fn default() -> Self {
        Self {
            conflict_behavior: ConflictBehavior::default(),
            max_put_bytes: DEFAULT_MAX_PUT_BYTES,
        }
    }
}

/// Kind of drive handled by a non-`me` [`SharepointObjectStore`]. Stores
/// registered under `sharepoint://drives`, `sharepoint://sites`, etc. are
/// shared by DataFusion across every dataset that resolves to the same
/// scheme+authority, so the drive ID must come from the first path segment
/// on each operation rather than from construction-time state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveKind {
    Drives,
    Sites,
    Users,
    Groups,
}

impl DriveKind {
    fn authority(self) -> &'static str {
        match self {
            Self::Drives => "drives",
            Self::Sites => "sites",
            Self::Users => "users",
            Self::Groups => "groups",
        }
    }

    fn with_id(self, id: String) -> DriveRef {
        match self {
            Self::Drives => DriveRef::Drive(id),
            Self::Sites => DriveRef::Site(id),
            Self::Users => DriveRef::User(id),
            Self::Groups => DriveRef::Group(id),
        }
    }
}

pub struct SharepointObjectStore {
    client: Arc<GraphClient>,
    /// `None` for the `sharepoint://me` store (drive fixed to
    /// [`DriveRef::Me`], paths are drive-relative). `Some(kind)` for
    /// `sharepoint://{drives,sites,users,groups}` stores — the drive ID
    /// is encoded as the first path segment of every `Path` argument,
    /// matching `DefaultObjectStoreRegistry`'s scheme+authority keying.
    kind: Option<DriveKind>,
    config: SharepointObjectStoreConfig,
}

impl SharepointObjectStore {
    #[must_use]
    pub fn new(
        client: Arc<GraphClient>,
        kind: Option<DriveKind>,
        config: SharepointObjectStoreConfig,
    ) -> Self {
        Self {
            client,
            kind,
            config,
        }
    }

    /// Resolve a path from an object-store [`Path`] to a SharePoint API path
    /// component in the form `:/full/path:`. Root returns `""` so callers can
    /// use `item_by_path("")` directly.
    fn graph_path(p: &Path) -> String {
        let s = p.as_ref();
        if s.is_empty() {
            String::new()
        } else {
            format!(":/{s}:")
        }
    }

    /// Split a `Path` argument from DataFusion into the drive target and
    /// the drive-relative item path. For the `me` store the drive is fixed;
    /// for kinded stores the ID is the first path segment so every
    /// `sharepoint://{kind}/{id}/...` dataset can share a single registered
    /// store without cross-drive confusion.
    fn resolve(&self, location: &Path) -> ObjectStoreResult<(DriveRef, Path)> {
        resolve_static(self.kind, location)
    }
}

fn resolve_static(kind: Option<DriveKind>, location: &Path) -> ObjectStoreResult<(DriveRef, Path)> {
    match kind {
        None => Ok((DriveRef::Me, location.clone())),
        Some(kind) => {
            let mut parts = location.parts();
            let Some(first) = parts.next() else {
                return Err(object_store::Error::Generic {
                    store: STORE_TAG,
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "missing drive id — expected path prefix '{{{id}-id}}/...' for sharepoint://{auth}",
                            id = kind.authority().trim_end_matches('s'),
                            auth = kind.authority(),
                        ),
                    )),
                });
            };
            let id = first.as_ref().to_string();
            let rest: Path = parts
                .map(|p| p.as_ref().to_string())
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str)
                .collect();
            Ok((kind.with_id(id), rest))
        }
    }
}

impl Debug for SharepointObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharepointObjectStore")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Display for SharepointObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(STORE_TAG)
    }
}

#[async_trait]
impl ObjectStore for SharepointObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        // `PutMode::Update(expected)` is a conditional write — verify the
        // current object still matches `expected` before overwriting.
        // graph-http's cross-crate `http` types make setting `If-Match`
        // on the PUT awkward, so we do a head-then-put check. Small TOCTOU
        // window is acceptable for the typical acceleration-writer use case.
        let (drive, in_drive) = self.resolve(location)?;
        if let PutMode::Update(expected) = &opts.mode {
            let current = with_original_location(
                head_drive_item(&self.client, &drive, &in_drive).await,
                location,
            )?;
            let e_tag_matches = expected.e_tag.is_none() || expected.e_tag == current.e_tag;
            let version_matches = expected.version.is_none() || expected.version == current.version;
            if !(e_tag_matches && version_matches) {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: format!(
                        "object changed: expected e_tag={:?} version={:?}, current e_tag={:?} version={:?}",
                        expected.e_tag, expected.version, current.e_tag, current.version
                    )
                    .into(),
                });
            }
        }

        let bytes = payload_to_bytes(&payload, self.config.max_put_bytes)?;

        // Map `PutMode` to SharePoint conflict behavior without violating the
        // `ObjectStore` contract. The contract requires that a successful
        // `PutMode::Overwrite` / `PutMode::Update` make the object at
        // `location` contain the new payload. SharePoint's `rename`
        // (succeed-with-different-name) and `fail` (reject on conflict)
        // cannot satisfy that, so we reject them here rather than silently
        // writing to a different path or failing a user who explicitly asked
        // to overwrite.
        //
        // - `Create`   → `Fail` (that's exactly "create, don't overwrite").
        // - `Overwrite`→ `Replace` only; reject `Rename`/`Fail` configs.
        // - `Update`   → `Replace` unconditionally. The precondition has
        //   already been verified above; the config must not force a rename
        //   or fail an OCC write that matched its precondition.
        let effective_conflict = match opts.mode {
            PutMode::Create => ConflictBehavior::Fail,
            PutMode::Overwrite => match self.config.conflict_behavior {
                ConflictBehavior::Replace => ConflictBehavior::Replace,
                other @ (ConflictBehavior::Rename | ConflictBehavior::Fail) => {
                    return Err(object_store::Error::Generic {
                        store: STORE_TAG,
                        source: Box::new(std::io::Error::other(format!(
                            "SharePoint put rejected for {location}: configured conflict_behavior={other:?} cannot satisfy PutMode::Overwrite; set sharepoint_conflict_behavior=replace"
                        ))),
                    });
                }
            },
            PutMode::Update(_) => ConflictBehavior::Replace,
        };

        let result = if bytes.len() <= INLINE_PUT_THRESHOLD {
            inline_put(&self.client, &drive, &in_drive, bytes, effective_conflict).await
        } else {
            resumable_put(&self.client, &drive, &in_drive, bytes, effective_conflict).await
        }?;

        Ok(PutResult {
            e_tag: result.e_tag,
            version: result.version,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let (drive, in_drive) = self.resolve(location)?;
        // Same contract issue as `put_opts`: `put_multipart` is implicitly
        // overwrite semantics, which `rename` / `fail` configs cannot satisfy.
        match self.config.conflict_behavior {
            ConflictBehavior::Replace => {}
            other @ (ConflictBehavior::Rename | ConflictBehavior::Fail) => {
                return Err(object_store::Error::Generic {
                    store: STORE_TAG,
                    source: Box::new(std::io::Error::other(format!(
                        "SharePoint multipart upload rejected for {location}: configured conflict_behavior={other:?} cannot satisfy overwrite; set sharepoint_conflict_behavior=replace"
                    ))),
                });
            }
        }
        Ok(Box::new(BufferedMultipart::new(
            Arc::clone(&self.client),
            drive,
            in_drive,
            ConflictBehavior::Replace,
            self.config.max_put_bytes,
        )))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let (drive, in_drive) = self.resolve(location)?;
        // Fetch metadata first — needed both for the ObjectMeta in GetResult
        // and to respect If-Match / If-None-Match via object_store's helper.
        let meta = with_original_location(
            head_drive_item(&self.client, &drive, &in_drive).await,
            location,
        )?;
        let object_meta = ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        };
        options.check_preconditions(&object_meta)?;

        // Slice in-memory after fetch (see note in `get_content`).
        // `get_content` returns the authoritative total size from the
        // fetched response body, so `range` / `meta.size` always agree
        // with the data we actually return — even if the Graph `head`
        // response reported a stale or missing `size`.
        let (bytes, total_size) = with_original_location(
            get_content(&self.client, &drive, &in_drive, options.range.as_ref()).await,
            location,
        )?;
        let range = options
            .range
            .as_ref()
            .map_or(0..total_size, |r| resolve_range(r, total_size));

        let stream = futures::stream::once(async move { Ok(bytes) });
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            attributes: Attributes::default(),
            range,
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: meta.last_modified,
                size: total_size,
                e_tag: meta.e_tag.clone(),
                version: meta.version.clone(),
            },
        })
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let (drive, in_drive) = self.resolve(location)?;
        let meta = with_original_location(
            head_drive_item(&self.client, &drive, &in_drive).await,
            location,
        )?;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        })
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let (drive, in_drive) = self.resolve(location)?;
        with_original_location(delete_item(&self.client, &drive, &in_drive).await, location)
    }

    /// Lists objects recursively below `prefix`, matching the `ObjectStore`
    /// trait contract: "all objects whose key starts with `prefix`"
    /// (S3/GCS semantics), not "all objects inside a folder". Since
    /// `object_store::Path` drops trailing slashes, we can't tell whether
    /// the last segment of `prefix` is a folder name or a partial file
    /// name — so we walk the parent folder, apply the last segment as a
    /// name-prefix filter at that level only, and let the full-path
    /// `starts_with` check drive the yield decision. Folders that match
    /// the name-prefix are descended into recursively (BFS), with no
    /// further name filtering at deeper levels.
    ///
    /// Uses `async_stream::stream!` for readability: the generator has
    /// multiple early-exit branches on errors and a nested page loop,
    /// which `futures::stream::unfold` would turn into an explicit
    /// state-machine with ~3× the boilerplate. Consistent with
    /// `runtime-object-store/src/store/github.rs`'s `list` impl.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let client = Arc::clone(&self.client);
        let kind = self.kind;
        let full_prefix = prefix.cloned().unwrap_or_else(|| Path::from(""));
        let (parent, name_filter) = split_prefix(&full_prefix, kind);
        Box::pin(async_stream::stream! {
            let mut queue: std::collections::VecDeque<(Path, Option<String>)> =
                std::collections::VecDeque::new();
            queue.push_back((parent, name_filter));
            while let Some((current, filter)) = queue.pop_front() {
                let (drive, in_drive) = match resolve_static(kind, &current) {
                    Ok(r) => r,
                    Err(e) => { yield Err(e); return; }
                };
                let mut pages = list_children(&client, &drive, &in_drive);
                while let Some(page) = pages.next().await {
                    match page {
                        Ok(batch) => {
                            for item in batch {
                                if let Some(f) = filter.as_deref()
                                    && !item.name.starts_with(f)
                                {
                                    continue;
                                }
                                let child = child_location(&current, &item.name);
                                if item.is_folder {
                                    // Once we've descended past the name-
                                    // filter level, the full-path starts_with
                                    // check below is all we need.
                                    queue.push_back((child, None));
                                } else if child.as_ref().starts_with(full_prefix.as_ref())
                                    && let Some(meta) = item.into_object_meta(&current)
                                {
                                    yield Ok(meta);
                                }
                            }
                        }
                        Err(e) => yield Err(e),
                    }
                }
            }
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix = prefix.cloned().unwrap_or_else(|| Path::from(""));
        let (drive, in_drive) = self.resolve(&prefix)?;
        let mut pages = list_children(&self.client, &drive, &in_drive);

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        while let Some(res) = pages.next().await {
            let batch = res?;
            for item in batch {
                if item.is_folder {
                    if let Some(p) = item.as_prefix(&prefix) {
                        common_prefixes.push(p);
                    }
                } else if let Some(meta) = item.into_object_meta(&prefix) {
                    objects.push(meta);
                }
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Err(object_store::Error::NotImplemented)
    }
}

/// Concatenate a [`PutPayload`] into a single owned buffer, refusing
/// payloads larger than `max_put_bytes`. Uses checked addition so a
/// payload whose chunk lengths sum past `usize::MAX` is rejected up
/// front rather than wrapping into an undersized allocation.
fn payload_to_bytes(payload: &PutPayload, max_put_bytes: usize) -> ObjectStoreResult<Vec<u8>> {
    let mut total: usize = 0;
    for chunk in payload {
        total = total
            .checked_add(chunk.len())
            .ok_or_else(|| object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other(
                    "SharePoint put rejected: payload size overflowed usize",
                )),
            })?;
        if total > max_put_bytes {
            return Err(object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other(format!(
                    "SharePoint put rejected: payload exceeds max_put_bytes={max_put_bytes} (raise sharepoint_max_put_bytes or stage writes in smaller pieces)"
                ))),
            });
        }
    }
    let mut buf = Vec::with_capacity(total);
    for chunk in payload {
        buf.extend_from_slice(chunk);
    }
    Ok(buf)
}

/// Split an `ObjectStore::list` prefix into `(parent_folder, name_filter)`
/// so the driver can walk the parent and apply a starts-with filter on
/// the first-level children. Empty prefix returns `(root, None)`.
///
/// For kinded stores (`DriveKind::Drives`/`Sites`/`Users`/`Groups`), the
/// first path segment is the drive/site/etc. id — it's not a filterable
/// name. Listing `Path::from("{drive-id}")` (a single segment that *is*
/// the drive root) must yield `(parent="{drive-id}", filter=None)` so
/// `resolve_static` can find the drive id; otherwise the drive root
/// would resolve as "missing drive id".
fn split_prefix(prefix: &Path, kind: Option<DriveKind>) -> (Path, Option<String>) {
    let parts: Vec<String> = prefix.parts().map(|p| p.as_ref().to_string()).collect();
    let reserved_segments = usize::from(kind.is_some());
    if parts.len() <= reserved_segments {
        // Prefix is at or above the drive-root level — list everything
        // under that drive (or under root for `Me`).
        let parent: Path = parts.iter().map(String::as_str).collect();
        return (parent, None);
    }
    let Some((last, head)) = parts.split_last() else {
        // Unreachable: `parts.len() > reserved_segments >= 0` above
        // guarantees at least one element, but avoid `expect()` to
        // satisfy `clippy::expect_used`.
        let parent: Path = parts.iter().map(String::as_str).collect();
        return (parent, None);
    };
    let parent: Path = head.iter().map(String::as_str).collect();
    (parent, Some(last.clone()))
}

fn resolve_range(range: &GetRange, total_size: u64) -> std::ops::Range<u64> {
    match range {
        // Clamp Bounded ranges to [0, total_size] and ensure `end >= start`
        // so the returned `GetResult.range` always matches the sliced
        // payload length — even when the caller passes an inverted range
        // like `GetRange::Bounded(200..50)`.
        GetRange::Bounded(r) => {
            let start = r.start.min(total_size);
            let end = r.end.min(total_size).max(start);
            start..end
        }
        GetRange::Offset(off) => (*off).min(total_size)..total_size,
        GetRange::Suffix(n) => total_size.saturating_sub(*n)..total_size,
    }
}

/// A [`MultipartUpload`] that buffers all parts in memory, then uploads at
/// `complete()` using either an inline PUT or a resumable upload session.
/// Simpler and more robust than trying to stream SharePoint's upload-session
/// chunks directly (which requires the total size up-front anyway).
#[derive(Debug)]
struct BufferedMultipart {
    client: Arc<GraphClient>,
    drive: DriveRef,
    item_path: Path,
    conflict: ConflictBehavior,
    max_put_bytes: usize,
    buffer: Arc<Mutex<Vec<u8>>>,
    completed: bool,
}

impl BufferedMultipart {
    fn new(
        client: Arc<GraphClient>,
        drive: DriveRef,
        item_path: Path,
        conflict: ConflictBehavior,
        max_put_bytes: usize,
    ) -> Self {
        Self {
            client,
            drive,
            item_path,
            conflict,
            max_put_bytes,
            buffer: Arc::new(Mutex::new(Vec::new())),
            completed: false,
        }
    }
}

#[async_trait]
impl MultipartUpload for BufferedMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let buffer = Arc::clone(&self.buffer);
        let max = self.max_put_bytes;
        Box::pin(async move {
            let bytes = payload_to_bytes(&data, max)?;
            let mut buf = buffer.lock().await;
            if buf.len().saturating_add(bytes.len()) > max {
                return Err(object_store::Error::Generic {
                    store: STORE_TAG,
                    source: Box::new(std::io::Error::other(format!(
                        "SharePoint multipart upload rejected: buffered size would exceed max_put_bytes={max} (raise sharepoint_max_put_bytes or split the write)"
                    ))),
                });
            }
            buf.extend_from_slice(&bytes);
            Ok(())
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        if self.completed {
            return Err(object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other("multipart upload already completed")),
            });
        }
        self.completed = true;

        let bytes = {
            let mut buf = self.buffer.lock().await;
            std::mem::take(&mut *buf)
        };
        let result = if bytes.len() <= INLINE_PUT_THRESHOLD {
            inline_put(
                &self.client,
                &self.drive,
                &self.item_path,
                bytes,
                self.conflict,
            )
            .await?
        } else {
            resumable_put(
                &self.client,
                &self.drive,
                &self.item_path,
                bytes,
                self.conflict,
            )
            .await?
        };
        Ok(PutResult {
            e_tag: result.e_tag,
            version: result.version,
        })
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        self.completed = true;
        let mut buf = self.buffer.lock().await;
        buf.clear();
        Ok(())
    }
}

/// Minimal metadata extracted from a DriveItem JSON response for head/list.
#[derive(Debug, Clone)]
struct DriveItemMeta {
    name: String,
    last_modified: DateTime<Utc>,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    is_folder: bool,
}

impl DriveItemMeta {
    fn into_object_meta(self, parent: &Path) -> Option<ObjectMeta> {
        if self.is_folder {
            return None;
        }
        Some(ObjectMeta {
            location: child_location(parent, &self.name),
            last_modified: self.last_modified,
            size: self.size,
            e_tag: self.e_tag,
            version: self.version,
        })
    }

    fn as_prefix(&self, parent: &Path) -> Option<Path> {
        if self.is_folder {
            Some(child_location(parent, &self.name))
        } else {
            None
        }
    }
}

/// Build a drive-relative location by appending `name` to `parent`.
/// Returned paths are relative to the store's drive root — the drive target
/// (me / drives / sites / ...) is held on the store instance, not the path.
fn child_location(parent: &Path, name: &str) -> Path {
    let mut segments: Vec<String> = Vec::new();
    for seg in parent.parts() {
        segments.push(seg.as_ref().to_string());
    }
    segments.push(name.to_string());
    segments.iter().map(String::as_str).collect::<Path>()
}

/// Parsed subset of a DriveItem JSON response.
#[derive(Debug, Deserialize)]
struct RawDriveItem {
    name: String,
    size: Option<i64>,
    #[serde(rename = "eTag")]
    e_tag: Option<String>,
    #[serde(rename = "cTag")]
    c_tag: Option<String>,
    #[serde(rename = "lastModifiedDateTime")]
    last_modified: Option<String>,
    folder: Option<serde_json::Value>,
}

impl RawDriveItem {
    /// Convert the raw Graph drive-item JSON into a [`DriveItemMeta`].
    ///
    /// Returns an error when required metadata is missing or malformed:
    /// - `lastModifiedDateTime` is missing, or present but not RFC3339-parseable.
    /// - `size` is present but negative.
    ///
    /// Folders are permitted to omit `size` (defaults to 0) because Graph
    /// sometimes omits size on folder-only responses. For files, a missing
    /// `size` is also tolerated (defaults to 0) — DataFusion's ListingTable
    /// treats `size` as advisory and will re-read via the `GET` response,
    /// so surfacing an error here would prevent otherwise-valid queries.
    /// We still surface structural/parse errors so callers don't silently
    /// operate on corrupted metadata (which could confuse cache invalidation).
    fn into_meta(self) -> ObjectStoreResult<DriveItemMeta> {
        let is_folder = self.folder.is_some();
        let last_modified_str = self.last_modified.as_deref().ok_or_else(|| {
            object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other(format!(
                    "SharePoint drive item '{}' is missing required field 'lastModifiedDateTime'",
                    self.name
                ))),
            }
        })?;
        let last_modified = DateTime::parse_from_rfc3339(last_modified_str)
            .map_err(|e| object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other(format!(
                    "SharePoint drive item '{}' has unparseable 'lastModifiedDateTime' '{last_modified_str}': {e}",
                    self.name
                ))),
            })?
            .with_timezone(&Utc);
        let size = match self.size {
            Some(s) => u64::try_from(s).map_err(|_| object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other(format!(
                    "SharePoint drive item '{}' reported negative size {s}",
                    self.name
                ))),
            })?,
            None => 0,
        };
        Ok(DriveItemMeta {
            name: self.name,
            last_modified,
            size,
            e_tag: self.e_tag,
            version: self.c_tag,
            is_folder,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawChildrenPage {
    value: Vec<RawDriveItem>,
}

struct PutOutcome {
    e_tag: Option<String>,
    version: Option<String>,
}

/// `Drive` API chain that abstracts over the two SDK-exposed types
/// ([`DrivesIdApiClient`] and [`DefaultDriveApiClient`]) which happen to expose
/// identical method names but aren't unified by a trait.
enum DriveChain {
    ById(DrivesIdApiClient),
    Default(DefaultDriveApiClient),
}

fn drive_chain(client: &GraphClient, drive: &DriveRef) -> DriveChain {
    match drive {
        DriveRef::Me => DriveChain::Default(client.me().drive()),
        DriveRef::Drive(id) => DriveChain::ById(client.drive(id)),
        DriveRef::Site(id) => DriveChain::Default(client.site(id).drive()),
        DriveRef::User(id) => DriveChain::Default(client.user(id).drive()),
        DriveRef::Group(id) => DriveChain::Default(client.group(id).drive()),
    }
}

async fn inline_put(
    client: &GraphClient,
    drive: &DriveRef,
    item_path: &Path,
    bytes: Vec<u8>,
    conflict: ConflictBehavior,
) -> ObjectStoreResult<PutOutcome> {
    // `PUT /content` doesn't cleanly support `@microsoft.graph.conflictBehavior`
    // for non-replace semantics, so route any non-default conflict behavior
    // through a resumable upload session (which takes conflictBehavior in the
    // JSON body). For the default `Replace`, SharePoint already preserves
    // history by versioning on overwrite.
    if conflict != ConflictBehavior::Replace {
        return resumable_put(client, drive, item_path, bytes, conflict).await;
    }
    let graph_path = SharepointObjectStore::graph_path(item_path);
    let body = reqwest::Body::from(bytes);
    let response = match drive_chain(client, drive) {
        DriveChain::ById(c) => c
            .item_by_path(&graph_path)
            .update_items_content(body)
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
        DriveChain::Default(c) => c
            .item_by_path(&graph_path)
            .update_items_content(body)
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
    };
    parse_put_response(response).await
}

async fn resumable_put(
    client: &GraphClient,
    drive: &DriveRef,
    item_path: &Path,
    bytes: Vec<u8>,
    conflict: ConflictBehavior,
) -> ObjectStoreResult<PutOutcome> {
    let graph_path = SharepointObjectStore::graph_path(item_path);
    let body = serde_json::json!({
        "item": { "@microsoft.graph.conflictBehavior": conflict.as_graph_header() },
    });

    let response = match drive_chain(client, drive) {
        DriveChain::ById(c) => c
            .item_by_path(&graph_path)
            .create_upload_session(&body)
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
        DriveChain::Default(c) => c
            .item_by_path(&graph_path)
            .create_upload_session(&body)
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
    };

    let mut session = response
        .into_upload_session(Cursor::new(bytes))
        .await
        .map_err(|e| graph_err(&e))?;

    let mut last_response: Option<reqwest::Response> = None;
    while let Some(chunk_result) = session.next().await {
        let resp = chunk_result.map_err(|e| graph_err(&e))?;
        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: STORE_TAG,
                source: Box::new(std::io::Error::other(format!(
                    "upload session chunk returned status {}",
                    resp.status()
                ))),
            });
        }
        last_response = Some(resp);
    }

    match last_response {
        Some(resp) => parse_put_response(resp).await,
        None => Err(object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(std::io::Error::other(
                "upload session produced no responses",
            )),
        }),
    }
}

async fn parse_put_response(response: reqwest::Response) -> ObjectStoreResult<PutOutcome> {
    if !response.status().is_success() {
        let status = response.status();
        let body = read_bounded_error_body(response, 256).await;
        return Err(object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(std::io::Error::other(format!(
                "SharePoint upload failed: HTTP {status}: {body}"
            ))),
        });
    }
    let raw: RawDriveItem = response
        .json()
        .await
        .map_err(|e| object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(e),
        })?;
    let meta = raw.into_meta()?;
    Ok(PutOutcome {
        e_tag: meta.e_tag,
        version: meta.version,
    })
}

async fn head_drive_item(
    client: &GraphClient,
    drive: &DriveRef,
    item_path: &Path,
) -> ObjectStoreResult<DriveItemMeta> {
    let graph_path = SharepointObjectStore::graph_path(item_path);
    let response = match drive_chain(client, drive) {
        DriveChain::ById(c) => c
            .item_by_path(&graph_path)
            .get_items()
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
        DriveChain::Default(c) => c
            .item_by_path(&graph_path)
            .get_items()
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
    };
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(object_store::Error::NotFound {
            path: item_path.to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "drive item not found",
            )),
        });
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = read_bounded_error_body(response, 256).await;
        return Err(object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(std::io::Error::other(format!(
                "head failed: HTTP {status}: {body}"
            ))),
        });
    }
    let raw: RawDriveItem = response
        .json()
        .await
        .map_err(|e| object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(e),
        })?;
    let meta = raw.into_meta()?;
    // Treat folders as NotFound so callers (e.g. DataFusion's ListingTableUrl)
    // fall back to listing the path as a collection rather than treating it as
    // a 0-byte file — which would suppress the list() call and cause schema
    // inference to find no files with the expected extension.
    if meta.is_folder {
        return Err(object_store::Error::NotFound {
            path: item_path.to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "path is a folder, not a file",
            )),
        });
    }
    Ok(meta)
}

async fn get_content(
    client: &GraphClient,
    drive: &DriveRef,
    item_path: &Path,
    range: Option<&GetRange>,
) -> ObjectStoreResult<(Bytes, u64)> {
    let graph_path = SharepointObjectStore::graph_path(item_path);
    let request = match drive_chain(client, drive) {
        DriveChain::ById(c) => c.item_by_path(&graph_path).get_items_content(),
        DriveChain::Default(c) => c.item_by_path(&graph_path).get_items_content(),
    };
    let response = request.send().await.map_err(|e| graph_err(&e))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(object_store::Error::NotFound {
            path: item_path.to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "drive item not found",
            )),
        });
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = read_bounded_error_body(response, 256).await;
        return Err(object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(std::io::Error::other(format!(
                "get content failed: HTTP {status}: {body}"
            ))),
        });
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(e),
        })?;
    // Slice client-side: graph_http's RequestHandler doesn't accept cross-crate
    // header types, so instead of the HTTP `Range` header we fetch the full
    // body and slice. For large files this is wasteful — revisit once the SDK
    // exposes a typed header API.
    //
    // Use `bytes.len()` as the authoritative size when resolving the range,
    // not the HEAD-reported size: Graph occasionally omits or misreports the
    // `size` field, which would otherwise cause suffix/offset reads (e.g.
    // Parquet footer reads via `GetRange::Suffix`) to resolve to an empty
    // slice even though we have the full body in memory.
    let total_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    match range {
        None => Ok((bytes, total_size)),
        Some(r) => {
            let Range { start, end } = resolve_range(r, total_size);
            // Clamp both bounds to the actual buffer length so a
            // requested range past EOF produces an empty slice instead of
            // panicking in `Bytes::slice`.
            let start = usize::try_from(start).unwrap_or(0).min(bytes.len());
            let end = usize::try_from(end).unwrap_or(bytes.len()).min(bytes.len());
            let end = end.max(start);
            Ok((bytes.slice(start..end), total_size))
        }
    }
}

async fn delete_item(
    client: &GraphClient,
    drive: &DriveRef,
    item_path: &Path,
) -> ObjectStoreResult<()> {
    let graph_path = SharepointObjectStore::graph_path(item_path);
    let response = match drive_chain(client, drive) {
        DriveChain::ById(c) => c
            .item_by_path(&graph_path)
            .delete_items()
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
        DriveChain::Default(c) => c
            .item_by_path(&graph_path)
            .delete_items()
            .send()
            .await
            .map_err(|e| graph_err(&e))?,
    };
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(object_store::Error::NotFound {
            path: item_path.to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "drive item not found",
            )),
        });
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = read_bounded_error_body(response, 256).await;
        return Err(object_store::Error::Generic {
            store: STORE_TAG,
            source: Box::new(std::io::Error::other(format!(
                "delete failed: HTTP {status}: {body}"
            ))),
        });
    }
    Ok(())
}

/// Pages through `list_children` for a given drive+path, yielding batches via
/// the SDK's built-in `.paging().stream()` helper (same pattern as
/// [`super::client::SharepointClient::stream_drive_items`]).
///
/// `async_stream::stream!` is used here (rather than `futures::stream::unfold`)
/// because the generator threads two error-translation sites through a page
/// loop; the equivalent `unfold` state machine would roughly triple the LOC
/// without changing behavior.
fn list_children(
    client: &GraphClient,
    drive: &DriveRef,
    item_path: &Path,
) -> BoxStream<'static, ObjectStoreResult<Vec<DriveItemMeta>>> {
    let graph_path = SharepointObjectStore::graph_path(item_path);
    let req_result = match drive_chain(client, drive) {
        DriveChain::ById(c) => c
            .item_by_path(&graph_path)
            .list_children()
            .paging()
            .stream::<RawChildrenPage>(),
        DriveChain::Default(c) => c
            .item_by_path(&graph_path)
            .list_children()
            .paging()
            .stream::<RawChildrenPage>(),
    };

    let paging_stream = match req_result {
        Ok(s) => s,
        Err(e) => {
            return Box::pin(async_stream::stream! {
                yield Err(graph_err(&e));
            });
        }
    };

    Box::pin(async_stream::stream! {
        let mut paging_stream = Box::pin(paging_stream);
        while let Some(resp_result) = paging_stream.next().await {
            let response = match resp_result {
                Ok(r) => r,
                Err(e) => {
                    yield Err(graph_err(&e));
                    return;
                }
            };
            let page: RawChildrenPage = match response.into_body() {
                Ok(p) => p,
                Err(e) => {
                    yield Err(object_store::Error::Generic {
                        store: STORE_TAG,
                        source: Box::new(std::io::Error::other(format!(
                            "list_children response parse failed: {}",
                            GraphFailure::ErrorMessage(e)
                        ))),
                    });
                    return;
                }
            };
            let metas: Vec<DriveItemMeta> = match page
                .value
                .into_iter()
                .map(RawDriveItem::into_meta)
                .collect::<ObjectStoreResult<Vec<_>>>()
            {
                Ok(v) => v,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            yield Ok(metas);
        }
    })
}

/// Rewrite any `Error::NotFound` path to the caller's original object-store
/// `location` (which for kinded stores includes the drive-ID prefix that
/// `resolve()` stripped before the Graph call). Keeps the error path
/// consistent with the input the caller passed in.
fn with_original_location<T>(
    result: ObjectStoreResult<T>,
    location: &Path,
) -> ObjectStoreResult<T> {
    result.map_err(|e| match e {
        object_store::Error::NotFound { source, .. } => object_store::Error::NotFound {
            path: location.to_string(),
            source,
        },
        other => other,
    })
}

fn graph_err(e: &graph_rs_sdk::GraphFailure) -> object_store::Error {
    // Delegate to the shared Graph-specific error formatter in
    // `sharepoint::error` so permission / token errors get their
    // structured inner_error surfaced instead of just `Display`.
    object_store::Error::Generic {
        store: STORE_TAG,
        source: Box::new(std::io::Error::other(super::error::resolve_graph_failure(
            e,
        ))),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests use unwrap to assert happy paths")]
mod tests {
    use super::*;

    #[test]
    fn graph_path_root_is_empty() {
        assert_eq!(SharepointObjectStore::graph_path(&Path::from("")), "");
    }

    #[test]
    fn graph_path_wraps_with_colons() {
        assert_eq!(
            SharepointObjectStore::graph_path(&Path::from("Documents/foo.csv")),
            ":/Documents/foo.csv:"
        );
    }

    #[test]
    fn conflict_behavior_from_str() {
        assert_eq!(
            "replace".parse::<ConflictBehavior>().unwrap(),
            ConflictBehavior::Replace
        );
        assert_eq!(
            "fail".parse::<ConflictBehavior>().unwrap(),
            ConflictBehavior::Fail
        );
        assert_eq!(
            "rename".parse::<ConflictBehavior>().unwrap(),
            ConflictBehavior::Rename
        );
        "bogus".parse::<ConflictBehavior>().unwrap_err();
    }

    #[test]
    fn child_location_empty_parent() {
        let p = child_location(&Path::from(""), "a.csv");
        assert_eq!(p.as_ref(), "a.csv");
    }

    #[test]
    fn child_location_nested() {
        let p = child_location(&Path::from("Documents/2026"), "report.parquet");
        assert_eq!(p.as_ref(), "Documents/2026/report.parquet");
    }

    #[test]
    fn resolve_range_clamps_bounded() {
        let r = resolve_range(&GetRange::Bounded(10..1000), 100);
        assert_eq!(r, 10..100);
    }

    #[test]
    fn resolve_range_offset_past_end_is_empty() {
        let r = resolve_range(&GetRange::Offset(500), 100);
        assert_eq!(r, 100..100);
    }

    // ---- End-to-end object-store tests against a mocked Graph endpoint. ----
    //
    // Gated behind:
    //  - `sharepoint-mock-host` feature → enables `graph-rs-sdk/test-util` so
    //    `GraphClient::use_test_endpoint` accepts a localhost URL.
    //
    // A subset of `#[test]`s in this module are `#[ignore]`-d because debug
    // builds of certain graph-rs-sdk drive-item paths (reqwest + tokio +
    // async-stream) produce async state machines that overflow tokio worker
    // stacks at runtime; release builds inline/elide those intermediate
    // frames. The remaining tests use shallower paths and run cleanly in
    // debug. We typecheck the entire module in debug builds so refactors
    // don't silently break the ignored tests.
    //
    // Run the full suite (including ignored ones) via:
    //
    //     cargo test --release -p data_components \
    //         --features sharepoint,sharepoint-mock-host \
    //         --no-default-features \
    //         sharepoint::object_store::tests::mock_http -- --include-ignored
    #[cfg(feature = "sharepoint-mock-host")]
    mod mock_http {
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures::StreamExt;
        use graph_rs_sdk::GraphClient;
        use url::Url;

        use super::*;

        struct MockResp {
            status: &'static str,
            body_bytes: Vec<u8>,
            content_type: &'static str,
        }

        impl MockResp {
            fn ok_json(body: &str) -> Self {
                Self {
                    status: "200 OK",
                    body_bytes: body.as_bytes().to_vec(),
                    content_type: "application/json",
                }
            }
            fn ok_bytes(bytes: Vec<u8>) -> Self {
                Self {
                    status: "200 OK",
                    body_bytes: bytes,
                    content_type: "application/octet-stream",
                }
            }
            fn empty(status: &'static str) -> Self {
                Self {
                    status,
                    body_bytes: Vec::new(),
                    content_type: "application/json",
                }
            }
        }

        async fn start_mock(
            responses: Vec<MockResp>,
        ) -> (
            String,
            Arc<AtomicUsize>,
            Arc<tokio::sync::Mutex<Vec<String>>>,
        ) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock");
            let addr = listener.local_addr().unwrap();
            let responses = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));
            let count = Arc::new(AtomicUsize::new(0));
            let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
            let count_srv = Arc::clone(&count);
            let captured_srv = Arc::clone(&captured);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        break;
                    };
                    let responses = Arc::clone(&responses);
                    let count = Arc::clone(&count_srv);
                    let captured = Arc::clone(&captured_srv);
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut buf = [0u8; 8192];
                        let mut raw = Vec::new();
                        let mut header_end: Option<usize> = None;
                        let mut content_length = 0usize;
                        loop {
                            let n = match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            raw.extend_from_slice(&buf[..n]);
                            if header_end.is_none()
                                && let Some(i) =
                                    raw.windows(4).position(|w| w == b"\r\n\r\n").map(|j| j + 4)
                            {
                                header_end = Some(i);
                                let ht = String::from_utf8_lossy(&raw[..i]);
                                content_length = ht
                                    .lines()
                                    .find_map(|line| {
                                        let (k, v) = line.split_once(':')?;
                                        k.trim()
                                            .eq_ignore_ascii_case("Content-Length")
                                            .then(|| v.trim().parse::<usize>().ok())
                                            .flatten()
                                    })
                                    .unwrap_or(0);
                            }
                            if let Some(end) = header_end
                                && raw.len() >= end + content_length
                            {
                                break;
                            }
                        }
                        count.fetch_add(1, Ordering::SeqCst);
                        captured
                            .lock()
                            .await
                            .push(String::from_utf8_lossy(&raw).into_owned());
                        let resp = responses.lock().await.pop_front().unwrap_or(MockResp {
                            status: "500 Internal Server Error",
                            body_bytes: b"{}".to_vec(),
                            content_type: "application/json",
                        });
                        let mut hdr = format!(
                            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n",
                            resp.status,
                            resp.content_type,
                            resp.body_bytes.len()
                        )
                        .into_bytes();
                        hdr.extend_from_slice(&resp.body_bytes);
                        let _ = stream.write_all(&hdr).await;
                    });
                }
            });
            (format!("http://{addr}"), count, captured)
        }

        fn mock_store(endpoint: &str) -> SharepointObjectStore {
            let mut client = GraphClient::new("unused-test-token");
            client.use_test_endpoint(&Url::parse(&format!("{endpoint}/v1.0")).unwrap());
            SharepointObjectStore::new(
                Arc::new(client),
                None,
                SharepointObjectStoreConfig::default(),
            )
        }

        /// Run a test future on a tokio runtime with a generous thread stack —
        /// debug builds of deep async chains (graph-rs-sdk + reqwest + tokio +
        /// async-stream) exceed both the default 2 MiB test-runner stack and
        /// tokio's default worker stack. We jump onto a fresh thread with
        /// 64 MiB, then into a multi-thread runtime with 64 MiB workers, so
        /// the driving future and its spawned tasks all have headroom.
        fn run_async<F>(f: F)
        where
            F: std::future::Future<Output = ()> + Send + 'static,
        {
            std::thread::Builder::new()
                .stack_size(64 * 1024 * 1024)
                .spawn(move || {
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .thread_stack_size(64 * 1024 * 1024)
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(f);
                })
                .unwrap()
                .join()
                .unwrap();
        }

        const HEAD_JSON: &str = r#"{
            "id": "01ABC", "name": "file.csv", "size": 42,
            "eTag": "\"abc\"", "cTag": "\"def\"",
            "lastModifiedDateTime": "2026-04-22T10:00:00Z"
        }"#;

        #[test]
        #[ignore = "graph-rs-sdk + reqwest debug-build async state machines blow the tokio worker stack; run with --release: cargo test --release -p data_components --features sharepoint,sharepoint-mock-host --no-default-features -- --ignored sharepoint::object_store::tests::mock_http"]
        fn head_parses_drive_item_metadata() {
            run_async(async {
                let (url, count, _captured) = start_mock(vec![MockResp::ok_json(HEAD_JSON)]).await;
                let store = mock_store(&url);
                let meta = store.head(&Path::from("Documents/file.csv")).await.unwrap();
                assert_eq!(meta.size, 42);
                assert_eq!(meta.e_tag.as_deref(), Some("\"abc\""));
                assert_eq!(meta.version.as_deref(), Some("\"def\""));
                assert_eq!(count.load(Ordering::SeqCst), 1);
            });
        }

        #[test]
        #[ignore = "graph-rs-sdk debug-build async recursion; run --release with --ignored"]
        fn head_returns_not_found_on_404() {
            run_async(async {
                let (url, _count, _captured) =
                    start_mock(vec![MockResp::empty("404 Not Found")]).await;
                let store = mock_store(&url);
                let err = store
                    .head(&Path::from("Documents/missing.csv"))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotFound { .. }),
                    "expected NotFound, got {err:?}"
                );
            });
        }

        #[test]
        fn get_returns_bytes_after_head() {
            run_async(async {
                // get_opts does a head first, then fetches /content.
                let (url, count, _captured) = start_mock(vec![
                    MockResp::ok_json(HEAD_JSON),
                    MockResp::ok_bytes(b"hello, world".to_vec()),
                ])
                .await;
                let store = mock_store(&url);
                let result = store
                    .get_opts(&Path::from("Documents/file.csv"), GetOptions::default())
                    .await
                    .unwrap();
                // get_opts uses the fetched payload length as the authoritative
                // size, not the HEAD-reported size — Graph occasionally
                // misreports `size`. Body is "hello, world" (12 bytes).
                assert_eq!(result.meta.size, 12);
                let bytes = match result.payload {
                    GetResultPayload::Stream(mut s) => {
                        let mut all = Vec::new();
                        while let Some(chunk) = s.next().await {
                            all.extend_from_slice(&chunk.unwrap());
                        }
                        all
                    }
                    _ => panic!("expected stream payload"),
                };
                assert_eq!(bytes, b"hello, world");
                assert_eq!(count.load(Ordering::SeqCst), 2);
            });
        }

        #[test]
        fn delete_returns_ok_on_success() {
            run_async(async {
                let (url, count, _captured) = start_mock(vec![MockResp::ok_json("{}")]).await;
                let store = mock_store(&url);
                store
                    .delete(&Path::from("Documents/file.csv"))
                    .await
                    .unwrap();
                assert_eq!(count.load(Ordering::SeqCst), 1);
            });
        }

        #[test]
        fn delete_returns_not_found_on_404() {
            run_async(async {
                let (url, _count, _captured) = start_mock(vec![MockResp {
                    status: "404 Not Found",
                    body_bytes: b"{}".to_vec(),
                    content_type: "application/json",
                }])
                .await;
                let store = mock_store(&url);
                let err = store
                    .delete(&Path::from("Documents/gone.csv"))
                    .await
                    .unwrap_err();
                assert!(matches!(err, object_store::Error::NotFound { .. }));
            });
        }

        #[test]
        fn put_small_returns_etag_from_response() {
            run_async(async {
                let (url, count, captured) = start_mock(vec![MockResp::ok_json(HEAD_JSON)]).await;
                let store = mock_store(&url);
                let result = store
                    .put_opts(
                        &Path::from("Documents/file.csv"),
                        PutPayload::from(bytes::Bytes::from_static(b"abc123")),
                        PutOptions::default(),
                    )
                    .await
                    .unwrap();
                assert_eq!(result.e_tag.as_deref(), Some("\"abc\""));
                assert_eq!(result.version.as_deref(), Some("\"def\""));
                assert_eq!(count.load(Ordering::SeqCst), 1);
                let req = &captured.lock().await[0];
                assert!(
                    req.starts_with("PUT "),
                    "expected PUT request, got: {}",
                    req.lines().next().unwrap_or("")
                );
                assert!(
                    req.contains("/content"),
                    "expected /content path in PUT request"
                );
            });
        }

        #[test]
        fn list_paginates_via_odata_next_link() {
            run_async(async {
                let page1 = r#"{
                    "value": [
                        {"id":"1","name":"a.csv","size":10,"eTag":"\"e1\"","cTag":"\"c1\"",
                         "lastModifiedDateTime":"2026-04-22T10:00:00Z"},
                        {"id":"2","name":"subfolder","size":0,"eTag":"\"e2\"","cTag":"\"c2\"",
                         "lastModifiedDateTime":"2026-04-22T10:00:00Z",
                         "folder":{"childCount":0}}
                    ],
                    "@odata.nextLink":"PAGE2"
                }"#;
                let page2 = r#"{
                    "value": [
                        {"id":"3","name":"b.parquet","size":20,"eTag":"\"e3\"","cTag":"\"c3\"",
                         "lastModifiedDateTime":"2026-04-22T10:00:00Z"}
                    ]
                }"#;
                let (url, count, _captured) =
                    start_mock(vec![MockResp::ok_json(page1), MockResp::ok_json(page2)]).await;
                let store = mock_store(&url);
                let mut stream = store.list(Some(&Path::from("Documents")));
                let mut names = Vec::new();
                while let Some(item) = stream.next().await {
                    let meta = item.unwrap();
                    names.push(meta.location.to_string());
                }
                // Folders skipped from files-only list; two files across two pages.
                assert_eq!(names.len(), 2, "got names={names:?}");
                assert!(names[0].ends_with("a.csv"));
                assert!(names[1].ends_with("b.parquet"));
                assert_eq!(count.load(Ordering::SeqCst), 2);
            });
        }

        #[test]
        fn list_with_delimiter_separates_files_and_folders() {
            run_async(async {
                let page = r#"{
                    "value": [
                        {"id":"1","name":"a.csv","size":10,"eTag":"\"e1\"","cTag":"\"c1\"",
                         "lastModifiedDateTime":"2026-04-22T10:00:00Z"},
                        {"id":"2","name":"sub","size":0,"eTag":"\"e2\"","cTag":"\"c2\"",
                         "lastModifiedDateTime":"2026-04-22T10:00:00Z",
                         "folder":{"childCount":0}}
                    ]
                }"#;
                let (url, _count, _captured) = start_mock(vec![MockResp::ok_json(page)]).await;
                let store = mock_store(&url);
                let result = store
                    .list_with_delimiter(Some(&Path::from("Documents")))
                    .await
                    .unwrap();
                assert_eq!(result.objects.len(), 1);
                assert!(result.objects[0].location.to_string().ends_with("a.csv"));
                assert_eq!(result.common_prefixes.len(), 1);
                assert!(result.common_prefixes[0].to_string().ends_with("sub"));
            });
        }
    }
}
