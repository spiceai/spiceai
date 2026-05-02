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

//! SMB `ObjectStore` backed by the internal `smb` crate.
//!
//! Supports read, head, list, put, and delete. Multipart uploads are
//! streamed part-by-part into a WAL temp file on the share and atomically
//! renamed on `complete`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use smb::{ShareSession, SmbConfig, SmbPool, WalWriter};
use std::sync::Mutex as StdMutex;
use tokio::sync::OnceCell;

use super::common::{
    DirEntry, build_byte_range, build_object_meta, generic_error, process_directory_entries,
    process_directory_entries_shallow, resolve_range,
};

const STORE_NAME: &str = "SMB";
/// Default connection pool size.
const DEFAULT_POOL_SIZE: usize = 4;
/// Hard cap on the in-memory buffer used by `get_opts` / `head` helpers.
/// Prevents a pathological server from triggering OOM on a gigantic file.
const MAX_BUFFERED_READ: u64 = 2 * 1024 * 1024 * 1024;

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    generic_error(STORE_NAME, error)
}

/// Map an `io::Error` from a head/get/delete into an `object_store::Error`.
/// `NotFound` → `object_store::Error::NotFound`; everything else → `Generic`.
/// This keeps permission/timeout failures from being misreported as 404s.
fn map_head_error(err: std::io::Error, path: String) -> object_store::Error {
    if err.kind() == std::io::ErrorKind::NotFound {
        object_store::Error::NotFound {
            path,
            source: err.into(),
        }
    } else {
        handle_error(err)
    }
}

/// Check whether the destination exists for create-exclusive semantics.
/// `Ok(true)` → exists; `Ok(false)` → confirmed not found; `Err` → head
/// failed for some other reason and should be surfaced as a real error.
async fn destination_exists(share: &ShareSession, key: &str) -> object_store::Result<bool> {
    match share.head_object(key).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(handle_error(e)),
    }
}

fn unix_epoch_datetime() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0)
        .unwrap_or_else(|| unreachable!("Unix epoch is always representable"))
}

fn epoch_secs_to_datetime(epoch_secs: u64) -> DateTime<Utc> {
    let secs_i64 = i64::try_from(epoch_secs).unwrap_or(i64::MAX);
    DateTime::<Utc>::from_timestamp(secs_i64, 0).unwrap_or_else(unix_epoch_datetime)
}

/// Default SMB-over-TCP port per [MS-SMB2] §2.1.
const DEFAULT_SMB_PORT: u16 = 445;

struct SMBConfig {
    server: String,
    port: u16,
    share: String,
    username: String,
    password: String,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for SMBConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBConfig")
            .field("server", &self.server)
            .field("port", &self.port)
            .field("share", &self.share)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl SMBConfig {
    fn to_smb_config(&self) -> SmbConfig {
        SmbConfig {
            server: self.server.clone(),
            port: self.port,
            username: self.username.clone(),
            password: self.password.clone(),
            domain: String::new(),
            workstation: String::new(),
            max_io_size: 0,
            read_timeout: self.timeout,
        }
    }

    fn display_path(&self, subpath: &str) -> String {
        let without_share = self.normalize_subpath(subpath);
        if without_share.is_empty() {
            format!("smb://{}/{}", self.server, self.share)
        } else {
            format!("smb://{}/{}/{}", self.server, self.share, without_share)
        }
    }

    /// `DataFusion` emits paths that include the share name as the first segment.
    /// This strips that prefix so we forward the share-relative portion to the
    /// internal SMB client. Only strips when the share name occupies a full
    /// path segment — `share="data"` against `"database/file"` is left alone.
    fn normalize_subpath<'a>(&self, subpath: &'a str) -> &'a str {
        let trimmed = subpath.trim_start_matches('/');
        let share = self.share.as_str();
        if trimmed == share {
            return "";
        }
        if let Some(rest) = trimmed.strip_prefix(share)
            && matches!(rest.as_bytes().first().copied(), Some(b'/' | b'\\'))
        {
            return rest.trim_start_matches(['/', '\\']);
        }
        trimmed
    }

    fn key_for(&self, path: &Path) -> String {
        self.normalize_subpath(path.as_ref()).to_string()
    }
}

/// Inner state shared across all `Clone`s of a given `SMBObjectStore`.
/// Wrapping the `OnceCell` in an `Arc` ensures that clones reuse the cached
/// `ShareSession` rather than each establishing their own connection pool.
struct Inner {
    config: SMBConfig,
    share: OnceCell<Arc<ShareSession>>,
}

#[derive(Clone)]
pub struct SMBObjectStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SMBObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBObjectStore")
            .field("config", &self.inner.config)
            .field("share_initialized", &self.inner.share.initialized())
            .finish()
    }
}

impl std::fmt::Display for SMBObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SMB")
    }
}

impl SMBObjectStore {
    /// Create a new SMB object store with lazy connection setup.
    /// `port` defaults to 445 when `None`.
    #[must_use]
    pub fn new(
        server: String,
        port: Option<u16>,
        share: String,
        username: String,
        password: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                config: SMBConfig {
                    server,
                    port: port.unwrap_or(DEFAULT_SMB_PORT),
                    share,
                    username,
                    password,
                    timeout,
                },
                share: OnceCell::new(),
            }),
        }
    }

    fn config(&self) -> &SMBConfig {
        &self.inner.config
    }

    async fn get_share(&self) -> object_store::Result<Arc<ShareSession>> {
        let share = self
            .inner
            .share
            .get_or_try_init(|| async {
                let pool = SmbPool::connect(self.config().to_smb_config(), DEFAULT_POOL_SIZE)
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: STORE_NAME,
                        source: format!(
                            "Failed to connect to SMB server smb://{}/{}. Verify host/credentials. Details: {e}",
                            self.config().server, self.config().share
                        )
                        .into(),
                    })?;
                let session = ShareSession::connect(pool, &self.config().share).await.map_err(|e| {
                    object_store::Error::Generic {
                        store: STORE_NAME,
                        source: format!(
                            "Failed to connect to SMB share smb://{}/{}. Details: {e}",
                            self.config().server, self.config().share
                        )
                        .into(),
                    }
                })?;
                Ok::<_, object_store::Error>(Arc::new(session))
            })
            .await?;
        Ok(Arc::clone(share))
    }

    /// Test the connection to the SMB share.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established or the share is not accessible.
    pub async fn test_connection(&self) -> object_store::Result<()> {
        self.get_share().await.map(|_| ())
    }

    async fn list_dir_entries(
        share: &ShareSession,
        config: &SMBConfig,
        dir_path: &str,
    ) -> Vec<DirEntry> {
        match share.list_directory(dir_path).await {
            Ok((files, dirs)) => {
                let mut entries = Vec::with_capacity(files.len() + dirs.len());
                for file in files {
                    entries.push(DirEntry::file(
                        leaf_name(&file.key),
                        file.size,
                        epoch_secs_to_datetime(file.last_modified),
                    ));
                }
                for d in dirs {
                    entries.push(DirEntry::directory(leaf_name(&d)));
                }
                entries
            }
            Err(e) => {
                let display_path = config.display_path(dir_path);
                if dir_path.contains('.') && !dir_path.ends_with('/') {
                    tracing::debug!(
                        "Path {display_path} appears to be a file, not a directory. Skipping directory listing."
                    );
                } else {
                    tracing::warn!("Failed to list SMB directory {display_path}: {e}");
                }
                Vec::new()
            }
        }
    }

    async fn list_all_files(
        &self,
        prefix: Option<String>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let share = self.get_share().await?;
        let config = self.config();
        let prefix_str = prefix.unwrap_or_default();
        let normalized = config.normalize_subpath(&prefix_str).to_string();

        let mut results = Vec::new();
        let mut queue = vec![normalized];

        while let Some(dir_path) = queue.pop() {
            let entries = Self::list_dir_entries(&share, config, &dir_path).await;
            let (files, dirs) = process_directory_entries(&dir_path, entries);
            results.extend(files);
            queue.extend(dirs);
        }

        Ok(results)
    }

    async fn list_directory_shallow(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let share = self.get_share().await?;
        let prefix_str = prefix.map_or(String::new(), Path::to_string);
        let normalized = self.config().normalize_subpath(&prefix_str).to_string();

        let entries = Self::list_dir_entries(&share, self.config(), &normalized).await;
        Ok(process_directory_entries_shallow(&normalized, entries))
    }

    /// Put the payload to the SMB share without a concat-copy.
    ///
    /// `PutPayload` is backed by a `Vec<Bytes>`; converting via `.into()` would
    /// concatenate all chunks into one contiguous buffer. Instead we iterate
    /// the chunks and feed them through the SMB WAL writer, which pipelines
    /// writes to the share.
    async fn put_streaming(
        &self,
        share: &Arc<ShareSession>,
        key: &str,
        payload: PutPayload,
    ) -> object_store::Result<PutResult> {
        let chunks = payload.as_ref();
        if chunks.len() == 1 {
            // Single chunk — hand the slice straight to put_object for the
            // small-file compound fast path.
            let meta = share
                .put_object(key, chunks[0].as_ref())
                .await
                .map_err(handle_error)?;
            return Ok(PutResult {
                e_tag: Some(meta.etag),
                version: None,
            });
        }

        let mut writer = share.open_wal_write(key).await.map_err(handle_error)?;
        for chunk in chunks {
            // On any write failure we must call `abort()` ourselves before
            // returning — `WalWriter::abort` is async and so can't run from
            // `Drop`, which would leave the `.spice-smb-wal/...` temp file
            // and its open server-side handle behind on the share.
            if let Err(e) = writer.write(chunk.as_ref()).await {
                writer.abort().await;
                return Err(handle_error(e));
            }
        }
        let meta = writer.commit(share.as_ref()).await.map_err(handle_error)?;
        Ok(PutResult {
            e_tag: Some(meta.etag),
            version: None,
        })
    }
}

/// Return the final path component (filename) from a forward-slash key.
fn leaf_name(key: &str) -> String {
    key.rsplit_once('/')
        .map_or_else(|| key.to_string(), |(_, name)| name.to_string())
}

#[async_trait]
impl ObjectStore for SMBObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !matches!(opts.mode, PutMode::Overwrite | PutMode::Create) {
            return Err(object_store::Error::NotSupported {
                source: "SMB put_opts: only Overwrite and Create modes are supported".into(),
            });
        }

        let share = self.get_share().await?;
        let key = self.config().key_for(location);

        // SMB has no atomic "create-exclusive" primitive that maps cleanly
        // across dialects; best-effort: head first, reject if the object
        // already exists. This is a TOCTOU race, same as `copy_if_not_exists`.
        if matches!(opts.mode, PutMode::Create) && destination_exists(&share, &key).await? {
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "put_opts(Create): destination already exists".into(),
            });
        }

        self.put_streaming(&share, &key, payload).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let share = self.get_share().await?;
        let key = self.config().key_for(location);

        let writer = share.open_wal_write(&key).await.map_err(handle_error)?;

        Ok(Box::new(SMBMultipartUpload::new(share, writer)))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let share = self.get_share().await?;
        let key = self.config().key_for(location);

        // Head first so we can size-gate the read *before* buffering anything.
        // This costs one extra round trip vs. the unbounded compound path but
        // eliminates the OOM risk on oversized files.
        let meta = share
            .head_object(&key)
            .await
            .map_err(|e| map_head_error(e, location.to_string()))?;

        let (start, end, _to_read) = resolve_range(options.range.as_ref(), meta.size);
        guard_read_size(end.saturating_sub(start))?;

        let data = share
            .get_object_range(&key, start, end)
            .await
            .map_err(handle_error)?;
        let size = meta.size;
        let last_modified = meta.last_modified;

        let object_meta = build_object_meta(
            location.clone(),
            size,
            epoch_secs_to_datetime(last_modified),
        );

        let bytes_data = Bytes::from(data);
        let stream = futures::stream::once(async move { Ok(bytes_data) });

        Ok(GetResult {
            meta: object_meta,
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: build_byte_range(start, end),
            attributes: Attributes::default(),
        })
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let share = self.get_share().await?;
        let key = self.config().key_for(location);

        let meta = share
            .head_object(&key)
            .await
            .map_err(|e| map_head_error(e, location.to_string()))?;

        Ok(build_object_meta(
            location.clone(),
            meta.size,
            epoch_secs_to_datetime(meta.last_modified),
        ))
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let share = self.get_share().await?;
        let key = self.config().key_for(location);

        share.delete_object(&key).await.map_err(handle_error)
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        locations
            .then(move |res| async move {
                let location = res?;
                self.delete(&location).await?;
                Ok(location)
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix_str = prefix.map(ToString::to_string);

        let fut = async move {
            match store.list_all_files(prefix_str).await {
                Ok(files) => futures::stream::iter(files.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "SMB list_with_offset not implemented".into(),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.list_directory_shallow(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let share = self.get_share().await?;
        let src_key = self.config().key_for(from);
        let dst_key = self.config().key_for(to);

        share
            .copy_object(&src_key, &dst_key)
            .await
            .map(|_| ())
            .map_err(handle_error)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        // TOCTOU race: SMB has no atomic copy-if-not-exists primitive.
        // A head-then-copy sequence is the best we can do.
        let share = self.get_share().await?;
        let dst_key = self.config().key_for(to);

        if destination_exists(&share, &dst_key).await? {
            return Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "copy_if_not_exists: destination already exists".into(),
            });
        }

        let src_key = self.config().key_for(from);
        share
            .copy_object(&src_key, &dst_key)
            .await
            .map(|_| ())
            .map_err(handle_error)
    }
}

fn guard_read_size(size: u64) -> object_store::Result<()> {
    if size > MAX_BUFFERED_READ {
        return Err(object_store::Error::Generic {
            store: STORE_NAME,
            source: format!(
                "SMB read of {size} bytes exceeds {MAX_BUFFERED_READ}-byte cap; reduce range or stream"
            )
            .into(),
        });
    }
    Ok(())
}

/// A multipart upload implementation backed by the internal SMB WAL writer.
///
/// Parts are appended in order to a temp file on the SMB share. `complete`
/// atomically renames the temp file into place.
///
/// Uses a synchronous `std::sync::Mutex` (not `tokio::sync::Mutex`) so the
/// guard is never held across `.await`. `put_part`/`complete`/`abort` each
/// briefly lock to `take()` the `WalWriter`, do their async I/O without the
/// lock held, then briefly lock again to put the writer back. The mutex is
/// only required to share state with the `'static` future returned by
/// `put_part`; `&mut self` already serializes concurrent calls.
struct SMBMultipartUpload {
    share: Arc<ShareSession>,
    writer: Arc<StdMutex<Option<WalWriter>>>,
}

impl SMBMultipartUpload {
    fn new(share: Arc<ShareSession>, writer: WalWriter) -> Self {
        Self {
            share,
            writer: Arc::new(StdMutex::new(Some(writer))),
        }
    }
}

impl std::fmt::Debug for SMBMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBMultipartUpload").finish()
    }
}

fn upload_already_finalized() -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: "multipart upload already completed or aborted".into(),
    }
}

/// Take the value out of a shared `Option<T>` slot under a sync mutex.
/// Returns an error if the slot is empty (caller has already taken it,
/// e.g. via `complete()` or `abort()`) or if the mutex was poisoned.
///
/// Generic over `T` so the state-machine semantics — single-take, idempotent
/// after finalize — can be exercised in unit tests without constructing a
/// real `WalWriter`.
fn take_slot<T>(slot: &StdMutex<Option<T>>) -> object_store::Result<T> {
    slot.lock()
        .map_err(|_| object_store::Error::Generic {
            store: STORE_NAME,
            source: "multipart upload mutex poisoned".into(),
        })?
        .take()
        .ok_or_else(upload_already_finalized)
}

/// Put a value back into a shared `Option<T>` slot after async I/O.
fn replace_slot<T>(slot: &StdMutex<Option<T>>, value: T) {
    if let Ok(mut guard) = slot.lock() {
        *guard = Some(value);
    }
}

#[async_trait]
impl MultipartUpload for SMBMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let writer_slot = Arc::clone(&self.writer);
        Box::pin(async move {
            let mut writer = take_slot(&writer_slot)?;
            let mut io_result: Result<(), std::io::Error> = Ok(());
            for chunk in data.as_ref() {
                if let Err(e) = writer.write(chunk.as_ref()).await {
                    io_result = Err(e);
                    break;
                }
            }
            // On error the writer is still valid for `abort()`; on success it
            // is valid for the next `put_part()`/`complete()`.
            replace_slot(&writer_slot, writer);
            io_result.map_err(handle_error)
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let writer = take_slot(&self.writer)?;
        let meta = writer
            .commit(self.share.as_ref())
            .await
            .map_err(handle_error)?;
        Ok(PutResult {
            e_tag: Some(meta.etag),
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        // Take the writer out under the sync lock, then drop the lock before
        // awaiting the asynchronous abort.
        let writer = match self.writer.lock() {
            Ok(mut guard) => guard.take(),
            Err(_) => return Ok(()),
        };
        if let Some(w) = writer {
            w.abort().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smb_object_store_display() {
        let store = SMBObjectStore::new(
            "server.local".to_string(),
            None,
            "share".to_string(),
            "user".to_string(),
            "pass".to_string(),
            None,
        );
        assert_eq!(format!("{store}"), "SMB");
    }

    #[test]
    fn test_smb_object_store_default_port() {
        let store = SMBObjectStore::new(
            "host".to_string(),
            None,
            "share".to_string(),
            "u".to_string(),
            "p".to_string(),
            None,
        );
        assert_eq!(store.config().port, DEFAULT_SMB_PORT);
    }

    #[test]
    fn test_smb_object_store_custom_port() {
        let store = SMBObjectStore::new(
            "host".to_string(),
            Some(1445),
            "share".to_string(),
            "u".to_string(),
            "p".to_string(),
            None,
        );
        assert_eq!(store.config().port, 1445);
    }

    #[test]
    fn test_normalize_subpath_strips_share_prefix() {
        let config = SMBConfig {
            server: "192.168.1.100".to_string(),
            port: DEFAULT_SMB_PORT,
            share: "myshare".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            timeout: None,
        };

        assert_eq!(
            config.normalize_subpath("myshare/data/file.parquet"),
            "data/file.parquet"
        );
        assert_eq!(
            config.normalize_subpath("data/file.parquet"),
            "data/file.parquet"
        );
        assert_eq!(config.normalize_subpath(""), "");
        assert_eq!(config.normalize_subpath("myshare"), "");
        assert_eq!(
            config.normalize_subpath("/myshare/data/file.parquet"),
            "data/file.parquet"
        );
    }

    #[test]
    fn test_leaf_name() {
        assert_eq!(leaf_name("foo/bar/baz.txt"), "baz.txt");
        assert_eq!(leaf_name("bare.txt"), "bare.txt");
        assert_eq!(leaf_name(""), "");
    }

    #[test]
    fn test_display_path_formats() {
        let config = SMBConfig {
            server: "server".to_string(),
            port: DEFAULT_SMB_PORT,
            share: "share".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            timeout: None,
        };
        assert_eq!(config.display_path(""), "smb://server/share");
        assert_eq!(config.display_path("share"), "smb://server/share");
        assert_eq!(
            config.display_path("share/dir/file"),
            "smb://server/share/dir/file"
        );
    }

    #[test]
    fn test_guard_read_size() {
        guard_read_size(1024).expect("small reads are allowed");
        guard_read_size(MAX_BUFFERED_READ).expect("exactly at cap is allowed");
        assert!(guard_read_size(MAX_BUFFERED_READ + 1).is_err());
    }

    // ── Multipart upload state-machine ───────────────────────────────────
    //
    // The full WAL flow (put_part → flush → rename → complete | abort)
    // exercises a real SMB server; that lives in the runtime integration
    // suite under a Samba container. The tests below exercise the parts
    // of `SMBMultipartUpload` that don't require I/O — specifically the
    // single-take / replace state machine that backs the take_writer /
    // replace_writer helpers, and the "already finalized" error semantics
    // that protect against double-complete or use-after-abort.
    //
    // The helpers are generic over `T` precisely to make these tests
    // possible without constructing a `WalWriter`.

    #[test]
    fn test_take_slot_extracts_value_once() {
        let slot: StdMutex<Option<u32>> = StdMutex::new(Some(7));
        let v = take_slot(&slot).expect("first take should succeed");
        assert_eq!(v, 7);
        // Slot is now empty — a second take must fail with "already finalized".
        let err = take_slot(&slot).expect_err("second take must fail");
        assert!(matches!(err, object_store::Error::Generic { .. }));
        assert!(err.to_string().contains("already completed or aborted"));
    }

    #[test]
    fn test_replace_slot_round_trips_for_next_part() {
        let slot: StdMutex<Option<String>> = StdMutex::new(Some("part-1".into()));
        let v = take_slot(&slot).expect("take part-1");
        // Mid-flight: slot is empty; concurrent put_part would see "finalized".
        let _ = take_slot(&slot).expect_err("mid-flight take must reject");
        // After async I/O completes the writer is returned to the slot —
        // this is the put_part success path that must allow further parts.
        replace_slot(&slot, format!("{v}-resumed"));
        let v2 = take_slot(&slot).expect("take after replace");
        assert_eq!(v2, "part-1-resumed");
    }

    #[test]
    fn test_take_slot_idempotent_after_finalize() {
        // Empty slot models the post-complete or post-abort state.
        let slot: StdMutex<Option<u32>> = StdMutex::new(None);
        // Both finalize paths (complete, abort) and any stray put_part must
        // see the "already finalized" error rather than panicking or silently
        // succeeding against missing state.
        for _ in 0..3 {
            let err = take_slot(&slot).expect_err("must reject after finalize");
            assert!(err.to_string().contains("already completed or aborted"));
        }
    }
}
