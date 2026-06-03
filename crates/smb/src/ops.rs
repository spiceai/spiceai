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

//! High-level SMB file operations.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::client::SmbClient;
use crate::pool::SmbPool;
use crate::protocol::{
    CREATE_OPTION_DELETE_ON_CLOSE, CreateDisposition, CreateOptions, DesiredAccess, ShareAccess,
};

/// Maximum size for a single `get_object` / `copy_object` buffered read.
/// Operations exceeding this must be driven through a streaming handle. Set
/// conservatively to avoid accidentally OOM'ing on a terabyte-scale file.
pub const MAX_BUFFERED_OBJECT_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// A connected share session backed by a pool of SMB connections.
pub struct ShareSession {
    pool: Arc<SmbPool>,
    /// Share name (e.g. `"data"`); kept so we can re-issue `tree_connect`
    /// against a freshly-reconnected slot.
    share: String,
    /// Per-slot tree id, async-mutex-protected so a poison-detect /
    /// reconnect / re-tree-connect sequence is serialized atomically per
    /// slot without holding a sync lock across `.await` points.
    tree_ids: Vec<tokio::sync::Mutex<u32>>,
}

/// An open file handle for streaming reads or writes.
pub struct FileHandle {
    client: Arc<SmbClient>,
    tree_id: u32,
    file_id: [u8; 16],
    pub meta: ObjectMeta,
    pub file_size: u64,
    pub max_chunk: u32,
}

impl ShareSession {
    /// Connect to a share on every connection in the pool.
    ///
    /// Each pool client issues its own `tree_connect` in parallel — this
    /// turns a 4-connection cold start from `4 * RTT` into `1 * RTT`.
    pub async fn connect(pool: Arc<SmbPool>, share: &str) -> io::Result<Self> {
        let n = pool.size();
        let mut joins = Vec::with_capacity(n);
        for i in 0..n {
            let client = pool.client(i);
            let s = share.to_string();
            joins.push(tokio::spawn(async move { client.tree_connect(&s).await }));
        }

        let mut tree_ids = Vec::with_capacity(n);
        for join in joins {
            let tree_id = join
                .await
                .map_err(|e| io::Error::other(format!("tree_connect spawn failed: {e}")))??;
            tree_ids.push(tokio::sync::Mutex::new(tree_id));
        }
        Ok(Self {
            pool,
            share: share.to_string(),
            tree_ids,
        })
    }

    /// Pick a healthy `(client, tree_id)` pair, transparently reconnecting
    /// poisoned slots and re-issuing `tree_connect` against the new client.
    /// Falls through to subsequent slots if a reconnect attempt fails, so a
    /// brief outage on one connection does not block progress on the others.
    async fn pick(&self) -> io::Result<(Arc<SmbClient>, u32)> {
        let n = self.pool.size();
        let start = self.pool.next_index();

        let mut last_err: Option<io::Error> = None;
        for i in 0..n {
            let idx = (start + i) % n;
            let mut tree_lock = self.tree_ids[idx].lock().await;
            let client = self.pool.client(idx);

            if !client.is_poisoned() {
                return Ok((client, *tree_lock));
            }

            // Slot poisoned — reconnect under the per-slot tree lock so
            // concurrent picks for this slot serialize on a single attempt.
            match self.pool.reconnect(idx).await {
                Ok(new_client) => match new_client.tree_connect(&self.share).await {
                    Ok(new_tree) => {
                        *tree_lock = new_tree;
                        return Ok((new_client, new_tree));
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "smb",
                            "slot {idx} re-tree-connect failed: {e}",
                        );
                        last_err = Some(e);
                    }
                },
                Err(e) => {
                    tracing::warn!(target: "smb", "slot {idx} reconnect failed: {e}");
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "all SMB pool connections poisoned and reconnect failed",
            )
        }))
    }

    #[must_use]
    pub fn compound_max_read_size(&self) -> u32 {
        self.pool.compound_max_read_size
    }

    #[must_use]
    pub fn compound_max_write_size(&self) -> u32 {
        self.pool.compound_max_write_size
    }

    /// Compound Create+Read+Close. Returns metadata and data bytes.
    pub async fn get_object_compound(
        &self,
        key: &str,
        max_read: u32,
    ) -> io::Result<(ObjectMeta, Bytes)> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        let (cr, data) = client
            .create_read_close(tree_id, &smb_path, max_read)
            .await?;

        let meta = ObjectMeta {
            size: cr.file_size,
            last_modified: filetime_to_epoch_secs(cr.last_write_time),
            etag: format!("{:016x}", cr.last_write_time),
        };

        Ok((meta, data))
    }

    // ── Streaming file operations ───────────────────────────────────────

    /// Open a file for streaming reads. Returns a handle pinned to one connection.
    pub async fn open_read(&self, key: &str) -> io::Result<FileHandle> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        let file = client
            .create(
                tree_id,
                &smb_path,
                DesiredAccess::GenericRead as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        let meta = ObjectMeta {
            size: file.file_size,
            last_modified: filetime_to_epoch_secs(file.last_write_time),
            etag: format!("{:016x}", file.last_write_time),
        };

        Ok(FileHandle {
            client: Arc::clone(&client),
            tree_id,
            file_id: file.file_id,
            file_size: file.file_size,
            max_chunk: self.pool.max_read_size,
            meta,
        })
    }

    /// Open (or create) a file for streaming writes.
    pub async fn open_write(&self, key: &str) -> io::Result<FileHandle> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        self.ensure_parent_dirs_on(&client, tree_id, &smb_path)
            .await?;

        let file = client
            .create(
                tree_id,
                &smb_path,
                DesiredAccess::GenericWrite as u32,
                ShareAccess::Read as u32,
                CreateDisposition::OverwriteIf as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        let meta = ObjectMeta {
            size: 0,
            last_modified: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            etag: String::new(),
        };

        Ok(FileHandle {
            client: Arc::clone(&client),
            tree_id,
            file_id: file.file_id,
            file_size: 0,
            max_chunk: self.pool.max_write_size,
            meta,
        })
    }

    // ── Buffered file operations ────────────────────────────────────────

    /// List objects in a directory. `prefix` uses forward-slash separators.
    /// When `delimiter` is `Some`, directories are returned as common prefixes.
    pub async fn list_objects(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> io::Result<(Vec<ObjectInfo>, Vec<String>)> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(prefix);
        let (dir_path, pattern) = split_dir_pattern(&smb_path);

        let dir = client
            .create(
                tree_id,
                &dir_path,
                DesiredAccess::GenericRead as u32 | DesiredAccess::ReadAttributes as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::DirectoryFile as u32,
            )
            .await?;

        let entries = client
            .query_directory(tree_id, &dir.file_id, &pattern)
            .await;

        let _ = client.close(tree_id, &dir.file_id).await;

        let entries = entries?;

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();

        for entry in entries {
            let key = if dir_path.is_empty() {
                entry.file_name.replace('\\', "/")
            } else {
                format!(
                    "{}/{}",
                    dir_path.replace('\\', "/"),
                    entry.file_name.replace('\\', "/")
                )
            };

            if entry.is_directory() {
                if delimiter.is_some() {
                    common_prefixes.push(format!("{key}/"));
                }
            } else {
                objects.push(ObjectInfo {
                    key,
                    size: entry.file_size,
                    last_modified: filetime_to_epoch_secs(entry.last_write_time),
                    etag: format!("{:016x}", entry.last_write_time),
                });
            }
        }

        Ok((objects, common_prefixes))
    }

    /// Shallow directory listing — returns both files and sub-directory names
    /// under `dir_path`. Forward-slash separators on input; backslash-aware
    /// internally. Unlike [`list_objects`] this returns entries in a flat
    /// `(files, dirs)` pair without building common-prefix paths.
    pub async fn list_directory(
        &self,
        dir_path: &str,
    ) -> io::Result<(Vec<ObjectInfo>, Vec<String>)> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(dir_path);

        let dir = client
            .create(
                tree_id,
                &smb_path,
                DesiredAccess::GenericRead as u32 | DesiredAccess::ReadAttributes as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::DirectoryFile as u32,
            )
            .await?;

        let entries = client.query_directory(tree_id, &dir.file_id, "*").await;
        let _ = client.close(tree_id, &dir.file_id).await;

        let entries = entries?;

        let prefix = dir_path.trim_end_matches('/');

        let mut files = Vec::new();
        let mut dirs = Vec::new();
        for entry in entries {
            let joined = if prefix.is_empty() {
                entry.file_name.clone()
            } else {
                format!("{prefix}/{}", entry.file_name)
            };
            if entry.is_directory() {
                dirs.push(joined);
            } else {
                files.push(ObjectInfo {
                    key: joined,
                    size: entry.file_size,
                    last_modified: filetime_to_epoch_secs(entry.last_write_time),
                    etag: format!("{:016x}", entry.last_write_time),
                });
            }
        }
        Ok((files, dirs))
    }

    /// Get object (file) content. Uses compound Create+Read+Close for files
    /// that fit in one read chunk, falling back to sequential for larger files.
    ///
    /// Rejects files larger than [`MAX_BUFFERED_OBJECT_SIZE`] — callers that
    /// need to handle larger objects should use [`open_read`] and stream
    /// chunks via [`FileHandle::read_chunk`] / [`read_pipeline`].
    pub async fn get_object(&self, key: &str) -> io::Result<(ObjectMeta, Vec<u8>)> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        let compound_max = self.pool.compound_max_read_size;
        let max_read = self.pool.max_read_size;

        let (cr, first_chunk) = client
            .create_read_close(tree_id, &smb_path, compound_max)
            .await?;

        if cr.file_size > MAX_BUFFERED_OBJECT_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "SMB object {} bytes exceeds {} byte buffered-read cap; use open_read() to stream",
                    cr.file_size, MAX_BUFFERED_OBJECT_SIZE
                ),
            ));
        }

        let meta = ObjectMeta {
            size: cr.file_size,
            last_modified: filetime_to_epoch_secs(cr.last_write_time),
            etag: format!("{:016x}", cr.last_write_time),
        };

        if cr.file_size <= first_chunk.len() as u64 {
            return Ok((meta, first_chunk.to_vec()));
        }

        let file = client
            .create(
                tree_id,
                &smb_path,
                DesiredAccess::GenericRead as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        let mut data = Vec::with_capacity(cr.file_size as usize);
        let mut offset = 0u64;
        loop {
            let chunk = client
                .read(tree_id, &file.file_id, offset, max_read)
                .await?;
            if chunk.is_empty() {
                break;
            }
            offset += chunk.len() as u64;
            data.extend_from_slice(&chunk);
            if offset >= cr.file_size {
                break;
            }
        }

        let _ = client.close(tree_id, &file.file_id).await;

        // The caller's `meta.size` came from the initial CREATE response;
        // if the file shrank between that metadata read and the EOF the
        // chunk loop hit, we'd otherwise return a `(meta, data)` pair where
        // `data.len() < meta.size`, silently handing back a truncated
        // object. Surface the short read as `UnexpectedEof` instead.
        if (data.len() as u64) < cr.file_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "SMB object {smb_path}: read returned EOF after {} bytes (expected {})",
                    data.len(),
                    cr.file_size
                ),
            ));
        }

        Ok((meta, data))
    }

    /// Read a byte range of an object using sequential reads. The caller is
    /// responsible for clamping `end` to the file size.
    pub async fn get_object_range(&self, key: &str, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if start >= end {
            return Ok(Vec::new());
        }

        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        let max_read = self.pool.max_read_size;

        let file = client
            .create(
                tree_id,
                &smb_path,
                DesiredAccess::GenericRead as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        let total = end - start;
        let mut data = Vec::with_capacity(total as usize);
        let mut offset = start;
        while offset < end {
            let remaining = end - offset;
            let request = u32::try_from(remaining.min(u64::from(max_read))).unwrap_or(max_read);
            let chunk = client.read(tree_id, &file.file_id, offset, request).await?;
            if chunk.is_empty() {
                break;
            }
            offset += chunk.len() as u64;
            data.extend_from_slice(&chunk);
        }

        let _ = client.close(tree_id, &file.file_id).await;

        // The caller asked for `[start, end)` and the object-store contract
        // requires returning exactly that many bytes — surface a short read
        // as `UnexpectedEof` instead of returning the partial buffer (which
        // would silently truncate query results if the file shrinks during
        // the read).
        if (data.len() as u64) < total {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "SMB object {smb_path}: range read returned {} of {total} requested bytes",
                    data.len()
                ),
            ));
        }

        Ok(data)
    }

    /// Put object (write file). Uses compound Create+Write+Close for small
    /// files, falling back to sequential for larger files. Always overwrites
    /// existing files; use [`Self::put_object_create`] for create-exclusive
    /// semantics.
    pub async fn put_object(&self, key: &str, data: &[u8]) -> io::Result<ObjectMeta> {
        self.put_object_inner(key, data, CreateDisposition::OverwriteIf as u32)
            .await
    }

    /// Put object atomically only if it does not already exist
    /// (`PutMode::Create` semantics). Maps to SMB `FILE_CREATE` disposition,
    /// which the server enforces atomically and returns
    /// `STATUS_OBJECT_NAME_COLLISION` (→ `io::ErrorKind::AlreadyExists`) on
    /// collision — no client-side TOCTOU window between an existence check
    /// and the actual write.
    pub async fn put_object_create(&self, key: &str, data: &[u8]) -> io::Result<ObjectMeta> {
        self.put_object_inner(key, data, CreateDisposition::Create as u32)
            .await
    }

    async fn put_object_inner(
        &self,
        key: &str,
        data: &[u8],
        disposition: u32,
    ) -> io::Result<ObjectMeta> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        self.ensure_parent_dirs_on(&client, tree_id, &smb_path)
            .await?;

        let compound_max = self.pool.compound_max_write_size as usize;
        let chunk_size = self.pool.max_write_size as usize;

        if data.len() <= compound_max && disposition == CreateDisposition::OverwriteIf as u32 {
            // Compound fast path uses the spec-defined OverwriteIf
            // disposition; create-exclusive falls through to the explicit
            // open-write-close sequence below so we control the disposition.
            let cl = client.create_write_close(tree_id, &smb_path, data).await?;
            return Ok(ObjectMeta {
                size: data.len() as u64,
                last_modified: filetime_to_epoch_secs(cl.last_write_time),
                etag: format!("{:016x}", cl.last_write_time),
            });
        }

        let file = client
            .create(
                tree_id,
                &smb_path,
                DesiredAccess::GenericWrite as u32,
                ShareAccess::Read as u32,
                CreateDisposition::OverwriteIf as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        let mut offset = 0u64;
        let mut write_err: Option<io::Error> = None;
        for chunk in data.chunks(chunk_size) {
            // SmbClient::write returns an error on short writes, so a
            // successful response means the full chunk was persisted.
            // Advance `offset` by the server-reported byte count for an
            // additional layer of defense against silent data corruption.
            match client.write(tree_id, &file.file_id, offset, chunk).await {
                Ok(bytes_written) => {
                    if usize::try_from(bytes_written).ok() != Some(chunk.len()) {
                        write_err = Some(io::Error::new(
                            io::ErrorKind::WriteZero,
                            format!(
                                "short SMB write to {smb_path}: wrote {bytes_written} of {} bytes",
                                chunk.len()
                            ),
                        ));
                        break;
                    }
                    offset += u64::from(bytes_written);
                }
                Err(e) => {
                    write_err = Some(e);
                    break;
                }
            }
        }

        // Always attempt to close, even on write failure, to release the
        // server-side handle. Prefer write errors over close errors.
        let close_result = client.close_with_attrs(tree_id, &file.file_id).await;
        if let Some(e) = write_err {
            return Err(e);
        }
        let close_meta = close_result?
            .ok_or_else(|| io::Error::other("SMB close did not return post-query attributes"))?;
        Ok(ObjectMeta {
            size: data.len() as u64,
            last_modified: filetime_to_epoch_secs(close_meta.last_write_time),
            etag: format!("{:016x}", close_meta.last_write_time),
        })
    }

    /// Delete an object.
    pub async fn delete_object(&self, key: &str) -> io::Result<()> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        let _ = client
            .create_close(
                tree_id,
                &smb_path,
                DesiredAccess::Delete as u32,
                ShareAccess::Delete as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32 | CREATE_OPTION_DELETE_ON_CLOSE,
            )
            .await?;
        Ok(())
    }

    /// Head object (metadata only). Compound Create+Close in 1 round trip.
    pub async fn head_object(&self, key: &str) -> io::Result<ObjectMeta> {
        let (client, tree_id) = self.pick().await?;
        let smb_path = to_smb_path(key);
        let (cr, _) = client
            .create_close(
                tree_id,
                &smb_path,
                DesiredAccess::ReadAttributes as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        Ok(ObjectMeta {
            size: cr.file_size,
            last_modified: filetime_to_epoch_secs(cr.last_write_time),
            etag: format!("{:016x}", cr.last_write_time),
        })
    }

    /// Copy a file on the SMB share by streaming source reads into a WAL
    /// writer. Memory footprint is one pipeline window (`max_read_size *
    /// PIPELINE_DEPTH`) regardless of source size. Always overwrites an
    /// existing destination; use [`Self::copy_object_create_only`] for
    /// `copy_if_not_exists` semantics.
    pub async fn copy_object(&self, src_key: &str, dst_key: &str) -> io::Result<ObjectMeta> {
        self.copy_object_inner(src_key, dst_key, true).await
    }

    /// Streaming copy that fails atomically if the destination already
    /// exists. The WAL temp file is renamed with `replace_if_exists=false`,
    /// which the SMB server enforces atomically — no client-side TOCTOU
    /// between a head check and the rename.
    pub async fn copy_object_create_only(
        &self,
        src_key: &str,
        dst_key: &str,
    ) -> io::Result<ObjectMeta> {
        self.copy_object_inner(src_key, dst_key, false).await
    }

    async fn copy_object_inner(
        &self,
        src_key: &str,
        dst_key: &str,
        replace_if_exists: bool,
    ) -> io::Result<ObjectMeta> {
        let src = self.open_read(src_key).await?;
        let src_size = src.file_size;
        let mut writer = self.open_wal_write(dst_key).await?;

        let chunk_size = src.max_chunk;
        let mut offset = 0u64;
        let mut result: io::Result<()> = Ok(());
        while offset < src_size {
            let remaining = src_size - offset;
            match src.read_pipeline(offset, chunk_size, remaining).await {
                Ok(chunks) => {
                    if chunks.is_empty() {
                        // EOF before `src_size` — the source shrank during
                        // the copy. Treat as a short read so we don't
                        // commit a truncated destination.
                        result = Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "copy_object: source {src_key} EOF after {offset} of {src_size} bytes",
                            ),
                        ));
                        break;
                    }
                    for chunk in &chunks {
                        if let Err(e) = writer.write(chunk).await {
                            result = Err(e);
                            break;
                        }
                        offset += chunk.len() as u64;
                    }
                    if result.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }

        // Final guard: make sure we read exactly `src_size` bytes before
        // committing. If the loop above terminated cleanly but ended with
        // `offset != src_size` (e.g. a truncating server returning a
        // partial chunk that exactly hit the boundary above), refuse to
        // publish a truncated destination.
        if result.is_ok() && offset != src_size {
            result = Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("copy_object: copied {offset} of {src_size} bytes from {src_key}",),
            ));
        }

        let _ = src.close().await;
        match result {
            Ok(()) => {
                if replace_if_exists {
                    writer.commit(self).await
                } else {
                    writer.commit_create_only(self).await
                }
            }
            Err(e) => {
                writer.abort().await;
                Err(e)
            }
        }
    }

    /// Try to remove an empty directory (best effort).
    pub async fn remove_dir(&self, smb_path: &str) {
        let Ok((client, tree_id)) = self.pick().await else {
            return;
        };
        let _ = client
            .create_close(
                tree_id,
                smb_path,
                DesiredAccess::Delete as u32,
                ShareAccess::Delete as u32,
                CreateDisposition::Open as u32,
                CreateOptions::DirectoryFile as u32 | CREATE_OPTION_DELETE_ON_CLOSE,
            )
            .await;
    }

    // ── WAL buffered write operations ─────────────────────────────────────

    /// Open a WAL writer for a streaming PutObject. Writes are buffered in
    /// memory and flushed to a temp file via pipelined SMB writes. Call
    /// `commit()` to atomically rename to the final path.
    pub async fn open_wal_write(&self, key: &str) -> io::Result<WalWriter> {
        let (client, tree_id) = self.pick().await?;
        let final_path = to_smb_path(key);

        self.ensure_parent_dirs_on(&client, tree_id, &final_path)
            .await?;

        let wal_path = wal_temp_path();
        self.ensure_parent_dirs_on(&client, tree_id, &wal_path)
            .await?;

        let file = client
            .create(
                tree_id,
                &wal_path,
                DesiredAccess::GenericWrite as u32 | DesiredAccess::Delete as u32,
                ShareAccess::Read as u32 | ShareAccess::Delete as u32,
                CreateDisposition::OverwriteIf as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        let chunk_size = self.pool.max_write_size as usize;
        Ok(WalWriter {
            client: Arc::clone(&client),
            tree_id,
            file_id: file.file_id,
            wal_path,
            final_path,
            buf: Vec::with_capacity(chunk_size * WRITE_PIPELINE_DEPTH),
            chunk_size,
            offset: 0,
            total_size: 0,
        })
    }

    /// Head object by raw SMB path (no forward-slash conversion).
    async fn head_object_smb(&self, smb_path: &str) -> io::Result<ObjectMeta> {
        let (client, tree_id) = self.pick().await?;
        let (cr, _) = client
            .create_close(
                tree_id,
                smb_path,
                DesiredAccess::ReadAttributes as u32,
                ShareAccess::All as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32,
            )
            .await?;

        Ok(ObjectMeta {
            size: cr.file_size,
            last_modified: filetime_to_epoch_secs(cr.last_write_time),
            etag: format!("{:016x}", cr.last_write_time),
        })
    }

    /// Ensure parent directories exist for a given path on a specific connection.
    async fn ensure_parent_dirs_on(
        &self,
        client: &SmbClient,
        tree_id: u32,
        smb_path: &str,
    ) -> io::Result<()> {
        let parts: Vec<&str> = smb_path.split('\\').collect();
        if parts.len() <= 1 {
            return Ok(());
        }

        let mut dirs = Vec::with_capacity(parts.len() - 1);
        let mut current = String::new();
        for part in &parts[..parts.len() - 1] {
            if !current.is_empty() {
                current.push('\\');
            }
            current.push_str(part);
            dirs.push(current.clone());
        }

        client.ensure_dirs(tree_id, &dirs).await
    }
}

const PIPELINE_DEPTH: usize = 64;

impl FileHandle {
    /// Read a chunk at the given offset. Returns empty bytes at EOF.
    pub async fn read_chunk(&self, offset: u64, len: u32) -> io::Result<Bytes> {
        self.client
            .read(self.tree_id, &self.file_id, offset, len)
            .await
    }

    /// Pipelined read: send multiple read requests in one batch.
    pub async fn read_pipeline(
        &self,
        offset: u64,
        chunk_size: u32,
        remaining: u64,
    ) -> io::Result<Vec<Bytes>> {
        let count = remaining
            .div_ceil(u64::from(chunk_size))
            .min(PIPELINE_DEPTH as u64) as usize;
        self.client
            .pipelined_read(self.tree_id, &self.file_id, offset, chunk_size, count)
            .await
    }

    /// Write a chunk at the given offset. Returns bytes written.
    pub async fn write_chunk(&self, offset: u64, data: &[u8]) -> io::Result<u32> {
        self.client
            .write(self.tree_id, &self.file_id, offset, data)
            .await
    }

    /// Close the file handle.
    pub async fn close(self) -> io::Result<()> {
        self.client.close(self.tree_id, &self.file_id).await
    }
}

// ── WAL (Write-Ahead Log) buffered writer ──────────────────────────────────

const WAL_DIR: &str = ".spice-smb-wal";
const WRITE_PIPELINE_DEPTH: usize = 64;

static WAL_COUNTER: AtomicU64 = AtomicU64::new(0);

fn wal_temp_path() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = WAL_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{WAL_DIR}\\{ts:020}-{seq:04}")
}

/// A buffered write-ahead-log writer for streaming `PutObject`.
pub struct WalWriter {
    client: Arc<SmbClient>,
    tree_id: u32,
    file_id: [u8; 16],
    wal_path: String,
    final_path: String,
    buf: Vec<u8>,
    chunk_size: usize,
    offset: u64,
    pub total_size: u64,
}

impl WalWriter {
    /// Append data to the write buffer. Flushes automatically when buffered.
    pub async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        let pipeline_cap = self.chunk_size * WRITE_PIPELINE_DEPTH;
        let mut pos = 0;

        while pos < data.len() {
            let space = pipeline_cap - self.buf.len();
            let take = space.min(data.len() - pos);
            self.buf.extend_from_slice(&data[pos..pos + take]);
            pos += take;
            self.total_size += take as u64;

            if self.buf.len() >= pipeline_cap {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        let expected = self.buf.len() as u64;
        let chunks: Vec<&[u8]> = self.buf.chunks(self.chunk_size).collect();
        let written = self
            .client
            .pipelined_write(self.tree_id, &self.file_id, self.offset, &chunks)
            .await?;
        // `pipelined_write` already errors on per-chunk short writes, but
        // verify the aggregate against the buffered byte count before
        // discarding the buffer to guard against silent data loss.
        if written != expected {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("WAL flush short write: expected {expected} bytes but wrote {written}"),
            ));
        }
        self.offset += written;
        self.buf.clear();
        Ok(())
    }

    /// Flush remaining data, close the WAL file, and rename it to the final
    /// path, replacing any existing object at that path.
    pub async fn commit(self, share: &ShareSession) -> io::Result<ObjectMeta> {
        self.commit_inner(share, true).await
    }

    /// Like [`Self::commit`] but the rename only succeeds when no object
    /// already exists at the final path (atomic create-exclusive). Used by
    /// `PutMode::Create` and `copy_if_not_exists` to avoid a TOCTOU window
    /// between a head check and the rename.
    pub async fn commit_create_only(self, share: &ShareSession) -> io::Result<ObjectMeta> {
        self.commit_inner(share, false).await
    }

    async fn commit_inner(
        mut self,
        share: &ShareSession,
        replace_if_exists: bool,
    ) -> io::Result<ObjectMeta> {
        // Flush before renaming so no buffered bytes are lost.
        let flush_result = self.flush().await;
        let rename_result = if flush_result.is_ok() {
            self.client
                .rename(
                    self.tree_id,
                    &self.file_id,
                    &self.final_path,
                    replace_if_exists,
                )
                .await
        } else {
            Ok(())
        };

        // Always close the handle, regardless of whether flush/rename
        // succeeded — otherwise the server retains the open file.
        let _ = self.client.close(self.tree_id, &self.file_id).await;

        // Best-effort delete the WAL temp file on ANY failure path
        // (flush *or* rename), so a failed upload never leaves an orphan
        // `.spice-smb-wal/...` file behind on the share.
        if flush_result.is_err() || rename_result.is_err() {
            let _ = self
                .client
                .create_close(
                    self.tree_id,
                    &self.wal_path,
                    DesiredAccess::Delete as u32,
                    ShareAccess::Delete as u32,
                    CreateDisposition::Open as u32,
                    CreateOptions::NonDirectoryFile as u32 | CREATE_OPTION_DELETE_ON_CLOSE,
                )
                .await;
        }

        flush_result?;
        rename_result?;

        share.head_object_smb(&self.final_path).await
    }

    /// Abort the WAL write — close and delete the temp file.
    pub async fn abort(self) {
        let _ = self.client.close(self.tree_id, &self.file_id).await;
        let _ = self
            .client
            .create_close(
                self.tree_id,
                &self.wal_path,
                DesiredAccess::Delete as u32,
                ShareAccess::Delete as u32,
                CreateDisposition::Open as u32,
                CreateOptions::NonDirectoryFile as u32 | CREATE_OPTION_DELETE_ON_CLOSE,
            )
            .await;
    }
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: u64,
    pub etag: String,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectMeta {
    pub size: u64,
    pub last_modified: u64,
    pub etag: String,
}

// ── Path conversion ─────────────────────────────────────────────────────────

/// Convert forward-slash key to SMB path (backslash).
fn to_smb_path(key: &str) -> String {
    key.trim_start_matches('/').replace('/', "\\")
}

/// Split an SMB path into (directory, file-pattern) for `QueryDirectory`.
fn split_dir_pattern(path: &str) -> (String, String) {
    if path.is_empty() {
        return (String::new(), "*".into());
    }
    if path.ends_with('\\') || path.contains('*') {
        (path.trim_end_matches('\\').to_string(), "*".into())
    } else if let Some(pos) = path.rfind('\\') {
        let dir = &path[..pos];
        let pattern = &path[pos + 1..];
        if pattern.is_empty() {
            (dir.to_string(), "*".into())
        } else {
            (dir.to_string(), format!("{pattern}*"))
        }
    } else {
        (String::new(), format!("{path}*"))
    }
}

/// Convert Windows FILETIME (100ns since 1601) to Unix epoch seconds.
#[must_use]
pub fn filetime_to_epoch_secs(ft: u64) -> u64 {
    const EPOCH_DIFF: u64 = 116_444_736_000_000_000;
    if ft <= EPOCH_DIFF {
        return 0;
    }
    (ft - EPOCH_DIFF) / 10_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_smb_path_simple() {
        assert_eq!(to_smb_path("a/b/c.txt"), "a\\b\\c.txt");
    }

    #[test]
    fn to_smb_path_strips_leading_slash() {
        assert_eq!(to_smb_path("/dir/file"), "dir\\file");
    }

    #[test]
    fn to_smb_path_root() {
        assert_eq!(to_smb_path("file.txt"), "file.txt");
    }

    #[test]
    fn to_smb_path_empty() {
        assert_eq!(to_smb_path(""), "");
    }

    #[test]
    fn split_dir_pattern_empty() {
        assert_eq!(split_dir_pattern(""), (String::new(), "*".into()));
    }

    #[test]
    fn split_dir_pattern_directory_trailing() {
        assert_eq!(
            split_dir_pattern("foo\\bar\\"),
            ("foo\\bar".into(), "*".into())
        );
    }

    #[test]
    fn split_dir_pattern_with_prefix() {
        assert_eq!(split_dir_pattern("foo\\bar"), ("foo".into(), "bar*".into()));
    }

    #[test]
    fn split_dir_pattern_single_component() {
        assert_eq!(
            split_dir_pattern("prefix"),
            (String::new(), "prefix*".into())
        );
    }

    #[test]
    fn filetime_epoch() {
        const EPOCH_FT: u64 = 116_444_736_000_000_000;
        assert_eq!(filetime_to_epoch_secs(EPOCH_FT), 0);
    }

    #[test]
    fn filetime_known_date() {
        const FT: u64 = 1_704_067_200 * 10_000_000 + 116_444_736_000_000_000;
        assert_eq!(filetime_to_epoch_secs(FT), 1_704_067_200);
    }

    #[test]
    fn filetime_zero() {
        assert_eq!(filetime_to_epoch_secs(0), 0);
    }

    #[test]
    fn wal_temp_path_under_wal_dir() {
        let path = wal_temp_path();
        let expected_prefix = format!("{WAL_DIR}\\");
        assert!(path.starts_with(&expected_prefix), "got: {path}");
    }

    #[test]
    fn wal_temp_path_unique() {
        let p1 = wal_temp_path();
        let p2 = wal_temp_path();
        assert_ne!(p1, p2);
    }
}
