/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Custom local-FS writer for COMPACTION OUTPUT (`provider/table.rs`
//! `write_to_snapshot`, `WriteClass::Maintenance`) — Tier 2/3 of the compaction
//! IO-hygiene work.
//!
//! # Why this exists
//!
//! `object_store`'s `LocalFileSystem` writer never lets Cayenne touch the output
//! fd: it streams parts into a staging file and renames on complete, fsyncing
//! neither the contents (Cayenne fsyncs the snapshot *directory* separately) nor
//! preallocating. That blocks three durability/throughput levers that only exist
//! during the write:
//!
//! - **`fallocate`** (Tier 2) — preallocate the output in large chunks so the
//!   `O(table)` sequential write lands in few extents instead of fragmenting the
//!   file and churning ext4/xfs delayed-allocation metadata; also avoids a
//!   mid-merge `ENOSPC`.
//! - **`sync_file_range` rate-smoothing** (Tier 2, "`bytes_per_sync`") — kick
//!   async writeback every N bytes so the dirty-page set stays bounded and the
//!   final sync isn't a writeback storm that throttles foreground query I/O via
//!   `dirty_ratio`. (`RocksDB`'s `bytes_per_sync`.)
//! - **`O_DIRECT`** (Tier 3) — bypass the page cache entirely so the compaction
//!   write never populates (hence never evicts) the hot query working set. The
//!   structural version of what the `fadvise_tier` `DONTNEED` hint approximates
//!   after the fact.
//!
//! # Owning the fd without changing Vortex
//!
//! This is a thin [`object_store::ObjectStore`] that wraps the real local store
//! and replaces only `put_multipart_opts` with [`CompactionUpload`]; every other
//! method delegates. It is installed ONLY for compaction-output writes on the
//! local tier (see the call site), so every multipart write it sees is a
//! compaction output. All reads/list/delete/copy still go straight to the inner
//! `LocalFileSystem`.
//!
//! `O_DIRECT` needs block-aligned offset, length, AND buffer. `object_store` calls
//! `put_part` in submission order but polls the returned futures in PARALLEL, and
//! this codebase must never block the async runtime. So each upload owns a single
//! **dedicated writer thread**: `put_part` hands the bytes to it over a channel
//! (returning a future that resolves on ack), and the thread does all I/O
//! sequentially and off-reactor. The ack future is the backpressure handle: it
//! resolves only once the writer thread has consumed the part, so a driver that
//! awaits it (`object_store`'s `WriteMultipart` / `BufWriter`, which `DataFusion`'s
//! file sink uses, caps outstanding parts at `max_concurrency`) keeps at most its
//! concurrency window of parts queued — the channel depth is bounded by the
//! driver, not by the file size. Sequential processing lets the `O_DIRECT` path
//! re-chunk arbitrary part sizes into a 4 KiB-aligned bounce buffer and pad +
//! `ftruncate` the final partial block — so true `O_DIRECT` works for any part
//! size with no Vortex-emission change.
//!
//! # When it is installed (storage-tier gated)
//!
//! Installed automatically for compaction output on the **network-attached
//! tier** (`StorageClass::Ebs` — AWS EBS / Azure managed block disks, or an
//! NFS/SMB network filesystem; see [`use_direct_writer_for`]), where bypassing
//! the page cache pays off. NOT
//! installed on local SSD/NVMe (**including AWS EC2 `NVMe` instance storage**,
//! which the detector classifies `LocalSsd`, not `Ebs`), tmpfs, undetected
//! storage, or S3 — a memory-capped HTAP A/B showed `O_DIRECT` is a net loss on
//! local, where the buffered `LocalFileSystem` writer plus the Tier-1 `fadvise`
//! eviction already win. The tier is detected at registration and overridable
//! via the `storage` acceleration param, so there is no separate env/bool knob.
//!
//! # Safety posture
//!
//! Linux + local-FS + compaction-output only; S3 is untouched. Atomic semantics
//! mirror `LocalFileSystem`: write to a same-dir staging file, fsync contents
//! (new — closes the long-standing local-FS content-durability gap), rename into
//! place, then fsync the parent dir. An `O_DIRECT` open that the filesystem
//! rejects (`EINVAL` on tmpfs/overlay) transparently falls back to buffered. On
//! macOS the direct knob maps to `F_NOCACHE` + `F_PREALLOCATE` — uncached,
//! preallocated I/O without `O_DIRECT`'s alignment demands. Other
//! non-Linux/non-macOS targets compile to a buffered fallback.
//!
//! # Completing the write levers
//!
//! - The output is preallocated with ONE up-front `fallocate` seeded from the
//!   caller's target file size (`min(target, 1 GiB)`) instead of growing 64 MiB
//!   at a time; on-demand growth still covers files that exceed the estimate, and
//!   a final truncate releases any unused tail so the file occupies exactly its
//!   logical bytes.
//! - The writer drops its OWN output pages in `finish` using the fd it already
//!   holds — the structural completion of the external `fadvise_tier` `DONTNEED`
//!   hint, which then skips its now-redundant re-open + re-hint for this path.

use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

use async_trait::async_trait;
use futures::FutureExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::metadata::StorageClass;
use crate::provider::delta_encoding::WriteClass;

/// The `MultipartUpload::put_part` return type (`object_store`'s `UploadPart`
/// alias), written explicitly to avoid depending on its crate-root re-export.
type UploadPart = futures::future::BoxFuture<'static, object_store::Result<()>>;

/// Logical block size for `O_DIRECT` alignment. 4 KiB is the safe superset of
/// every common device/filesystem logical block size (512/4096).
const BLOCK: usize = 4096;
/// Aligned bounce-buffer capacity for the `O_DIRECT` path (a `BLOCK` multiple).
/// 4 MiB amortizes the per-`pwrite` syscall over more bytes than any common
/// device I/O size while staying small enough that N concurrent shard writers
/// (one dedicated buffer each) don't balloon RSS.
const ODIRECT_BUF_CAP: usize = 4 << 20; // 4 MiB
/// On-demand `fallocate` growth granularity — preallocate this much past the
/// write frontier at a time once the up-front reservation is exhausted
/// (`FALLOC_FL_KEEP_SIZE`, so the file size is unchanged).
const FALLOC_CHUNK: u64 = 64 << 20; // 64 MiB
/// Upper bound on the single up-front `fallocate` seeded from the caller's target
/// file size. Caps worst-case transient over-reservation (the target can be an
/// over-estimate, and many shard writers preallocate at once) while still
/// collapsing the common ≤cap output to ONE reservation syscall; larger files
/// grow on demand in `FALLOC_CHUNK` steps.
const MAX_UPFRONT_PREALLOC: u64 = 1 << 30; // 1 GiB

/// Fixed configuration for the custom compaction-output writer. The writer is
/// installed automatically by storage tier (see [`use_direct_writer_for`]) rather
/// than a knob; when installed it always uses `O_DIRECT` + `fallocate` +
/// `bytes_per_sync` rate-smoothing + a final content fsync — the combination the
/// module is designed around for the network-attached (EBS) tier.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CompactionWriterConfig {
    direct_io: bool,
    fallocate: bool,
    bytes_per_sync: u64,
    final_fsync: bool,
}

impl CompactionWriterConfig {
    /// The configuration installed on the network-attached block-storage
    /// (EBS/Azure managed disks) tier: bypass the page cache (`O_DIRECT`),
    /// preallocate the output (`fallocate`), rate-smooth writeback
    /// (`bytes_per_sync`), and fsync contents before the publishing rename.
    pub(crate) fn for_ebs_tier() -> Self {
        Self {
            direct_io: true,
            fallocate: true,
            bytes_per_sync: 8 << 20, // 8 MiB
            final_fsync: true,
        }
    }
}

/// Whether compaction OUTPUT for a table should be routed through the custom
/// `O_DIRECT` writer, decided by the detected storage tier. Installed ONLY on the
/// network-attached block-storage tier ([`StorageClass::Ebs`] — AWS EBS, Azure
/// managed disks), where bypassing the page cache pays off. Deliberately NOT
/// installed on:
/// - [`StorageClass::LocalSsd`] — local SSD/NVMe, **including AWS EC2 `NVMe`
///   instance storage** (the detector maps that to `LocalSsd`, not `Ebs`): a
///   memory-capped HTAP A/B showed `O_DIRECT` is a net loss there, where the
///   buffered `LocalFileSystem` writer + the Tier-1 `fadvise` eviction already
///   win;
/// - [`StorageClass::Tmpfs`] — RAM-backed; no device to bypass;
/// - [`StorageClass::Unknown`] — no positive evidence of the networked tier, so
///   keep the safe buffered default (never enable on a guess);
/// - S3 paths — object store, no page cache, its own writer.
///
/// The tier is auto-detected at registration (overridable via the `storage`
/// acceleration param), so this is non-optional — there is no separate env/bool
/// knob. `Maintenance`-class (compaction) writes on a local filesystem only.
pub(crate) fn use_direct_writer_for(
    storage_class: StorageClass,
    write_class: WriteClass,
    table_path: &str,
) -> bool {
    matches!(storage_class, StorageClass::Ebs)
        && matches!(write_class, WriteClass::Maintenance)
        && !table_path.starts_with("s3://")
}

/// A [`object_store::ObjectStore`] that delegates everything to `inner` (the real
/// local store) except multipart writes, which it routes through
/// [`CompactionUpload`]. Install only for compaction-output writes on the local
/// tier — every multipart write it receives is then a compaction output.
#[derive(Debug)]
pub(crate) struct CompactionLocalStore {
    inner: Arc<dyn ObjectStore>,
    /// Filesystem root the object-store `Path`s are resolved against, so the
    /// upload can open the real file. Mirrors `LocalFileSystem`'s prefix.
    root: PathBuf,
    cfg: CompactionWriterConfig,
    /// Target per-output-file size (bytes) from the caller — the on-disk Vortex
    /// file size the compaction is rolling to. Seeds a single up-front
    /// `fallocate` per output. `0` = unknown (size-rolling disabled, one file per
    /// shard) → the writer grows the reservation on demand instead.
    expected_file_bytes: u64,
}

impl CompactionLocalStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        root: PathBuf,
        cfg: CompactionWriterConfig,
        expected_file_bytes: u64,
    ) -> Self {
        Self {
            inner,
            root,
            cfg,
            expected_file_bytes,
        }
    }

    /// Resolve an object-store `Path` to a filesystem path under `root`.
    /// Compaction snapshot paths are simple ASCII (`<uuid>.vortex` under the
    /// snapshot dir), so a raw segment join matches `LocalFileSystem`'s mapping.
    fn to_fs_path(&self, location: &Path) -> PathBuf {
        let mut p = self.root.clone();
        for part in location.parts() {
            p.push(part.as_ref());
        }
        p
    }
}

impl fmt::Display for CompactionLocalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactionLocalStore({})", self.root.display())
    }
}

#[async_trait]
impl ObjectStore for CompactionLocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // Single-shot puts are small/metadata writes that don't benefit from the
        // compaction treatment — delegate to the real store unchanged.
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let dest = self.to_fs_path(location);
        match CompactionUpload::create(dest, self.cfg, self.expected_file_bytes) {
            Ok(upload) => Ok(Box::new(upload)),
            // Best-effort: if the custom writer can't be set up for this output
            // (staging create/open failure, permission issue, …), fall back to the
            // inner local store so a writer-setup error never fails a compaction.
            // Mirrors the session-level fallback in `table.rs`
            // (`compaction_session_context`), which only covers session setup, not
            // this per-output `create`.
            Err(error) => {
                tracing::debug!(
                    target: "cayenne::compaction",
                    %error,
                    location = %location,
                    "compaction writer setup failed for output; falling back to inner store"
                );
                self.inner.put_multipart_opts(location, opts).await
            }
        }
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
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

/// Map a `std::io::Error` to an `object_store` error tagged to this store.
fn io_err(source: std::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "CompactionLocalStore",
        source: Box::new(source),
    }
}

// ---------------------------------------------------------------------------
// The upload: a handle over a dedicated writer thread.
// ---------------------------------------------------------------------------

enum Msg {
    Part(
        PutPayload,
        tokio::sync::oneshot::Sender<std::io::Result<()>>,
    ),
    Complete(tokio::sync::oneshot::Sender<std::io::Result<PutResult>>),
}

/// `MultipartUpload` handle: forwards parts to a dedicated writer thread (so all
/// blocking, alignment-sensitive I/O runs sequentially off the async runtime).
#[derive(Debug)]
pub(crate) struct CompactionUpload {
    tx: Option<mpsc::Sender<Msg>>,
    handle: Option<std::thread::JoinHandle<()>>,
    staging: PathBuf,
}

impl CompactionUpload {
    /// Open the staging file (with an `O_DIRECT`→buffered fallback) and spawn the
    /// writer thread. `expected_file_bytes` seeds the up-front `fallocate`. Errors
    /// here surface as a failed `put_multipart`.
    fn create(
        dest: PathBuf,
        cfg: CompactionWriterConfig,
        expected_file_bytes: u64,
    ) -> std::io::Result<Self> {
        let parent = dest
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "output path has no parent",
                )
            })?
            .to_path_buf();
        std::fs::create_dir_all(&parent)?;

        // Same-dir staging file so the final rename is atomic on one filesystem.
        let staging = staging_path(&dest);
        let (file, direct) = open_staging(&staging, cfg.direct_io)?;

        let (tx, rx) = mpsc::channel::<Msg>();
        let writer = Writer::new(
            file,
            dest,
            parent,
            staging.clone(),
            cfg,
            direct,
            expected_file_bytes,
        );
        let handle = std::thread::Builder::new()
            .name("cayenne-compaction-writer".to_string())
            .spawn(move || writer.run(&rx))?;

        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
            staging,
        })
    }
}

#[async_trait]
impl MultipartUpload for CompactionUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let Some(tx) = self.tx.as_ref() else {
            return futures::future::ready(Err(io_err(std::io::Error::other(
                "put_part after complete/abort",
            ))))
            .boxed();
        };
        let (ack, rx) = tokio::sync::oneshot::channel();
        // Synchronous send preserves submission order on the writer thread.
        if tx.send(Msg::Part(data, ack)).is_err() {
            return futures::future::ready(Err(io_err(std::io::Error::other(
                "compaction writer thread is gone",
            ))))
            .boxed();
        }
        async move {
            match rx.await {
                Ok(r) => r.map_err(io_err),
                Err(_) => Err(io_err(std::io::Error::other(
                    "compaction writer dropped ack",
                ))),
            }
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let tx = self
            .tx
            .take()
            .ok_or_else(|| io_err(std::io::Error::other("complete called twice")))?;
        let (ack, rx) = tokio::sync::oneshot::channel();
        tx.send(Msg::Complete(ack))
            .map_err(|_| io_err(std::io::Error::other("compaction writer thread is gone")))?;
        drop(tx); // let the thread's recv loop end after it handles Complete
        let result = rx
            .await
            .map_err(|_| io_err(std::io::Error::other("compaction writer dropped ack")))?
            .map_err(io_err);
        // Join the (now-exiting) thread off the reactor.
        if let Some(handle) = self.handle.take() {
            let _ = tokio::task::spawn_blocking(move || handle.join()).await;
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        // Drop the sender: the thread's recv returns Err, it removes the staging
        // file and exits (see `Writer::run`). Best-effort cleanup also in Drop.
        self.tx.take();
        if let Some(handle) = self.handle.take() {
            let _ = tokio::task::spawn_blocking(move || handle.join()).await;
        }
        let _ = std::fs::remove_file(&self.staging);
        Ok(())
    }
}

impl Drop for CompactionUpload {
    fn drop(&mut self) {
        // Dropped without complete()/abort() (e.g. a cancelled write future): close
        // the channel so the writer thread's `recv` returns `Err` and it stops.
        // Do NOT `join()` here — `Drop` can run on a tokio worker, and joining would
        // block that worker for the duration of an in-flight write. Detach instead:
        // the thread observes the closed channel and removes its own staging file
        // (see `Writer::run`); complete()/abort() still join off-reactor via
        // `spawn_blocking`. The `remove_file` below is a non-blocking best-effort
        // backstop for the (panic) case where the thread never reaches its cleanup.
        self.tx.take();
        drop(self.handle.take()); // detach the writer thread; never block in Drop
        let _ = std::fs::remove_file(&self.staging);
    }
}

/// `<dest>#cayenne-<rand>` in the same directory (atomic rename target).
fn staging_path(dest: &FsPath) -> PathBuf {
    // uuid v7 is monotonic + process-unique; good enough for a temp suffix.
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    let mut s = dest.as_os_str().to_owned();
    s.push("#cayenne-");
    s.push(suffix);
    PathBuf::from(s)
}

// ---------------------------------------------------------------------------
// The writer (runs entirely on the dedicated thread).
// ---------------------------------------------------------------------------

struct Writer {
    file: std::fs::File,
    dest: PathBuf,
    parent: PathBuf,
    staging: PathBuf,
    cfg: CompactionWriterConfig,
    direct: bool,
    /// Next file offset to write at (always `BLOCK`-aligned in `O_DIRECT` mode).
    offset: u64,
    /// True bytes accepted (the final file length).
    logical_len: u64,
    /// Bytes reserved by `fallocate` so far.
    allocated: u64,
    /// `offset` at the last `sync_file_range` (buffered rate-smoothing).
    last_sync: u64,
    /// Aligned bounce buffer (Some only in `O_DIRECT` mode).
    buf: Option<AlignedBuf>,
    /// Bytes currently held in `buf`.
    buf_len: usize,
    /// `fallocate`/`sync_file_range` disabled after a filesystem rejection.
    fs_hints: bool,
}

impl Writer {
    fn new(
        file: std::fs::File,
        dest: PathBuf,
        parent: PathBuf,
        staging: PathBuf,
        cfg: CompactionWriterConfig,
        direct: bool,
        expected_file_bytes: u64,
    ) -> Self {
        // Seed ONE up-front reservation from the caller's target file size so the
        // O(table) sequential write lands in few extents instead of churning
        // ext4/xfs delayed-allocation metadata; `reserve` grows past it for files
        // that exceed the estimate, and `finish` truncates any unused tail away.
        let allocated = Self::preallocate_upfront(&file, cfg, expected_file_bytes);
        Self {
            file,
            dest,
            parent,
            staging,
            cfg,
            direct,
            offset: 0,
            logical_len: 0,
            allocated,
            last_sync: 0,
            buf: if direct {
                Some(AlignedBuf::new(ODIRECT_BUF_CAP))
            } else {
                None
            },
            buf_len: 0,
            fs_hints: true,
        }
    }

    /// One up-front `fallocate(KEEP_SIZE)` of `min(expected, MAX_UPFRONT_PREALLOC)`.
    /// Returns the bytes actually reserved — `0` when preallocation is disabled,
    /// the size is unknown, or the filesystem rejects the hint (in which case
    /// `fs_hints` stays enabled so on-demand `reserve` still tries as the file
    /// grows). Best-effort: preallocation never affects correctness, only layout.
    fn preallocate_upfront(
        file: &std::fs::File,
        cfg: CompactionWriterConfig,
        expected: u64,
    ) -> u64 {
        if !cfg.fallocate || expected == 0 {
            return 0;
        }
        let want = expected.min(MAX_UPFRONT_PREALLOC);
        match fallocate_keep_size(file, 0, want) {
            Ok(()) => want,
            Err(_) => 0,
        }
    }

    fn run(mut self, rx: &mpsc::Receiver<Msg>) {
        while let Ok(msg) = rx.recv() {
            match msg {
                Msg::Part(payload, ack) => {
                    let _ = ack.send(self.write_part(&payload));
                }
                Msg::Complete(ack) => {
                    let _ = ack.send(self.finish());
                    return;
                }
            }
        }
        // Sender dropped without Complete (abort/drop): discard the staging file.
        let _ = std::fs::remove_file(&self.staging);
    }

    fn write_part(&mut self, payload: &PutPayload) -> std::io::Result<()> {
        for bytes in payload {
            if self.direct {
                self.write_direct(bytes)?;
            } else {
                self.write_buffered(bytes)?;
            }
        }
        Ok(())
    }

    fn write_buffered(&mut self, src: &[u8]) -> std::io::Result<()> {
        self.reserve(self.offset + src.len() as u64);
        write_all_at(&self.file, src, self.offset)?;
        self.offset += src.len() as u64;
        self.logical_len = self.offset;
        // Rate-smoothing async writeback (Linux). Buffered mode only.
        if self.fs_hints
            && self.cfg.bytes_per_sync > 0
            && self.offset - self.last_sync >= self.cfg.bytes_per_sync
        {
            let from = self.last_sync;
            let len = self.offset - self.last_sync;
            if sync_file_range_write(&self.file, from, len).is_err() {
                self.fs_hints = false;
            } else {
                self.last_sync = self.offset;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::expect_used,
        reason = "self.buf is Some in O_DIRECT mode, the only mode that reaches write_direct"
    )]
    fn write_direct(&mut self, mut src: &[u8]) -> std::io::Result<()> {
        let cap = self.buf.as_ref().expect("o_direct buffer present").cap;
        while !src.is_empty() {
            let at = self.buf_len;
            let n = (cap - at).min(src.len());
            // Scoped &mut borrow of the buffer: must not span `self.reserve`.
            self.buf
                .as_mut()
                .expect("o_direct buffer present")
                .as_mut_slice()[at..at + n]
                .copy_from_slice(&src[..n]);
            self.buf_len += n;
            self.logical_len += n as u64;
            src = &src[n..];
            if self.buf_len == cap {
                // Full buffer is a BLOCK multiple → aligned O_DIRECT write.
                self.reserve(self.offset + cap as u64);
                let off = self.offset;
                let buf = self.buf.as_ref().expect("o_direct buffer present");
                write_all_at(&self.file, &buf.as_slice()[..cap], off)?;
                self.offset += cap as u64;
                self.buf_len = 0;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<PutResult> {
        if self.direct {
            self.flush_direct_tail()?; // pads, writes, then truncates to logical_len
        } else if self.allocated > self.logical_len {
            // Buffered path: best-effort release of the up-front / on-demand
            // `fallocate` reservation past the true end. `set_len(logical_len)`
            // drops the KEEP_SIZE-preallocated tail extents beyond `logical_len` on
            // ext4/xfs (truncation frees blocks past the new end, not merely
            // i_size). This is a disk-LAYOUT hint, not correctness — the file's
            // logical length is already `logical_len` from the writes — so it is
            // `let _` best-effort: a bounded residual over-allocation on a
            // filesystem that will not shrink it is harmless and must never fail
            // the compaction. (The O_DIRECT path's `flush_direct_tail` truncate is
            // load-bearing — it shrinks i_size off the alignment padding — and so
            // keeps its `?`.)
            let _ = truncate(&self.file, self.logical_len);
        }
        if self.cfg.final_fsync {
            // Contents durable BEFORE the rename publishes the name — the local-FS
            // content fsync object_store's writer omits. `robust_fsync` tolerates
            // filesystems that reject the strongest sync (SMB/some NFS refuse
            // `F_FULLFSYNC`): it falls back to a plain fsync, then to the
            // directory-fsync + snapshot durability floor, so the networked tier
            // this writer targets never fails a compaction on the content sync.
            robust_fsync(&self.file)?;
        }
        std::fs::rename(&self.staging, &self.dest)?;
        // Persist the rename (dirent) so the published name survives a crash. Gate
        // on `final_fsync` (the durability knob that also gates the content fsync
        // above), NOT `fs_hints`: `fs_hints` is cleared when an optional perf hint
        // (fallocate / sync_file_range) is rejected by the filesystem — which is
        // unrelated to, and must not silently disable, dirent durability. Best-
        // effort: a dir-fsync failure must not fail the compaction.
        if self.cfg.final_fsync {
            let _ = fsync_dir(&self.parent);
        }
        // Drop this output's page-cache footprint NOW (the timely fast path), while
        // we still hold the just-written fd and know whether O_DIRECT kept it out of
        // cache entirely. `evict_compaction_output_pages` also runs later as a
        // backstop (and covers any fallback-to-buffered output). Best-effort: a
        // cache hint must never fail a compaction.
        evict_own(&self.file, self.direct);
        let metadata = std::fs::metadata(&self.dest)?;
        Ok(PutResult {
            e_tag: Some(etag(&metadata)),
            version: None,
        })
    }

    /// Write the final partial block under `O_DIRECT`: zero-pad up to a `BLOCK`
    /// boundary, do the aligned write, then `ftruncate` to the true length.
    #[expect(
        clippy::expect_used,
        reason = "self.buf is Some in O_DIRECT mode, the only mode that reaches flush_direct_tail"
    )]
    fn flush_direct_tail(&mut self) -> std::io::Result<()> {
        if self.buf_len > 0 {
            let buf_len = self.buf_len;
            let padded = buf_len.div_ceil(BLOCK) * BLOCK;
            // Scoped &mut borrow of the buffer: must not span `self.reserve`.
            self.buf
                .as_mut()
                .expect("o_direct buffer present")
                .as_mut_slice()[buf_len..padded]
                .fill(0);
            self.reserve(self.offset + padded as u64);
            let off = self.offset;
            let buf = self.buf.as_ref().expect("o_direct buffer present");
            write_all_at(&self.file, &buf.as_slice()[..padded], off)?;
            self.offset += padded as u64;
            self.buf_len = 0;
        }
        // Drop the alignment padding so the file is exactly `logical_len` bytes.
        truncate(&self.file, self.logical_len)?;
        Ok(())
    }

    /// Preallocate (`fallocate FALLOC_FL_KEEP_SIZE`) past the write frontier in
    /// `FALLOC_CHUNK` steps. Best-effort: disabled after a filesystem rejection.
    fn reserve(&mut self, needed_end: u64) {
        if !self.fs_hints || !self.cfg.fallocate || needed_end <= self.allocated {
            return;
        }
        let new_alloc = needed_end.div_ceil(FALLOC_CHUNK) * FALLOC_CHUNK;
        match fallocate_keep_size(&self.file, self.allocated, new_alloc - self.allocated) {
            Ok(()) => self.allocated = new_alloc,
            Err(_) => self.fs_hints = false,
        }
    }
}

/// A heap buffer aligned to [`BLOCK`] (required for the `O_DIRECT` user buffer).
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    cap: usize,
    layout: std::alloc::Layout,
}

impl AlignedBuf {
    fn new(cap: usize) -> Self {
        assert!(
            cap.is_multiple_of(BLOCK) && cap > 0,
            "aligned buf cap must be a BLOCK multiple"
        );
        // SAFETY: `BLOCK` is a non-zero power of two, and `cap` (asserted a positive
        // `BLOCK` multiple, only ever `ODIRECT_BUF_CAP`) rounded up to `BLOCK` cannot
        // overflow `isize` — the two preconditions of `from_size_align_unchecked`.
        let layout = unsafe { std::alloc::Layout::from_size_align_unchecked(cap, BLOCK) };
        // SAFETY: layout has non-zero size; we check the result for null.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr =
            std::ptr::NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self { ptr, cap, layout }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` owns `cap` initialized (zeroed) bytes for our lifetime.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.cap) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: exclusive borrow; `ptr` owns `cap` bytes.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`layout` are exactly what `alloc_zeroed` returned in `new`.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

// SAFETY: the buffer is owned exclusively by the single writer thread; the raw
// pointer is never shared. Send is needed to move it into the spawned thread.
unsafe impl Send for AlignedBuf {}

// ---------------------------------------------------------------------------
// Platform syscalls. Linux gets the real primitives; other targets fall back to
// a plain buffered write so the module still compiles and is correct.
// ---------------------------------------------------------------------------

/// Open the staging file. On Linux, honour `direct_io` (`O_DIRECT`) with a
/// transparent fallback to buffered when the filesystem rejects it (`EINVAL` on
/// tmpfs/overlay). Returns the file and whether `O_DIRECT` is actually active.
#[cfg(target_os = "linux")]
fn open_staging(path: &FsPath, direct_io: bool) -> std::io::Result<(std::fs::File, bool)> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create_new(true);
    if direct_io {
        opts.custom_flags(libc::O_DIRECT);
        match opts.open(path) {
            Ok(f) => Ok((f, true)),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                tracing::debug!(
                    target: "cayenne::compaction",
                    path = %path.display(),
                    "O_DIRECT unsupported on this filesystem; compaction writer falling back to buffered"
                );
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map(|f| (f, false))
            }
            Err(e) => Err(e),
        }
    } else {
        opts.open(path).map(|f| (f, false))
    }
}

/// Non-Linux hosts. macOS honours the direct knob via `F_NOCACHE` (uncached I/O
/// with no alignment requirement — so the buffered write path is reused and
/// `direct` stays false); other targets open plain buffered. The hint never
/// fails the open.
#[cfg(not(target_os = "linux"))]
fn open_staging(path: &FsPath, direct_io: bool) -> std::io::Result<(std::fs::File, bool)> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    maybe_set_nocache(&file, direct_io);
    Ok((file, false))
}

/// macOS: disable page caching for this fd (`F_NOCACHE`) when the direct knob is
/// on — the Darwin analog of `O_DIRECT` for a write-then-evict workload, without
/// the alignment demands. Best-effort; a failure just leaves caching enabled.
#[cfg(target_os = "macos")]
fn maybe_set_nocache(file: &std::fs::File, direct_io: bool) {
    if direct_io {
        use std::os::fd::AsRawFd;
        // SAFETY: valid fd for the borrow; F_NOCACHE takes an int flag argument.
        let _ = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    }
}

/// Non-macOS, non-Linux: no portable uncached-write hint. No-op.
#[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
fn maybe_set_nocache(_file: &std::fs::File, _direct_io: bool) {}

/// `pwrite` the whole buffer at `offset`, looping over partial writes / `EINTR`.
fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !buf.is_empty() {
        match file.write_at(buf, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "compaction writer wrote 0 bytes",
                ));
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn fallocate_keep_size(file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if len == 0 {
        return Ok(());
    }
    // SAFETY: valid fd for the borrow; offset/len are in-range i64.
    let rc = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_KEEP_SIZE,
            i64::try_from(offset).unwrap_or(i64::MAX),
            i64::try_from(len).unwrap_or(i64::MAX),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// macOS preallocation via `F_PREALLOCATE`. Reserves blocks WITHOUT changing the
/// file size (like Linux `FALLOC_FL_KEEP_SIZE`); `F_PEOFPOSMODE` anchors the
/// reservation at the current logical EOF, so a call reserves `len` more bytes
/// past the write frontier (`offset` is the Linux-precise bookkeeping and is
/// ignored here — any over-reservation is released by the final `set_len` in
/// `finish`). Tries a contiguous extent first, then any layout on a fragmented
/// volume.
#[cfg(target_os = "macos")]
fn fallocate_keep_size(file: &std::fs::File, _offset: u64, len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if len == 0 {
        return Ok(());
    }
    let length = i64::try_from(len).unwrap_or(i64::MAX);
    let mut store = libc::fstore_t {
        // `F_ALLOCATECONTIG`/`F_ALLOCATEALL` are already `c_uint` — no cast needed.
        fst_flags: libc::F_ALLOCATECONTIG | libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: length,
        fst_bytesalloc: 0,
    };
    // SAFETY: valid fd for the borrow; `store` outlives the fcntl call.
    let mut rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
    if rc == -1 {
        // Contiguous request failed (fragmented volume) — retry allowing any layout.
        store.fst_flags = libc::F_ALLOCATEALL;
        // SAFETY: as above.
        rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
    }
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Non-macOS, non-Linux: no portable preallocation primitive. Unsupported, so
/// `reserve` disables the hint.
#[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
fn fallocate_keep_size(_file: &std::fs::File, _offset: u64, _len: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "fallocate is not supported on this platform",
    ))
}

#[cfg(target_os = "linux")]
fn sync_file_range_write(file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SYNC_FILE_RANGE_WRITE = async writeback kick (no WAIT) — RocksDB's
    // bytes_per_sync. SAFETY: valid fd for the borrow.
    let rc = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            i64::try_from(offset).unwrap_or(i64::MAX),
            i64::try_from(len).unwrap_or(i64::MAX),
            libc::SYNC_FILE_RANGE_WRITE,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn sync_file_range_write(_file: &std::fs::File, _offset: u64, _len: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "sync_file_range is Linux-only",
    ))
}

/// Drop the finished output's page-cache footprint using the fd we already hold
/// (no re-open). Linux: flush any dirty buffered pages so `DONTNEED` can drop
/// them (a no-op after `sync_all`, or under `O_DIRECT` where ~none were resident),
/// then `POSIX_FADV_DONTNEED`. Best-effort — every failure is ignored, since a
/// cache hint must never fail a compaction.
#[cfg(target_os = "linux")]
fn evict_own(file: &std::fs::File, direct: bool) {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    if !direct {
        // A buffered output may still hold dirty pages if `final_fsync` was off;
        // write them back so DONTNEED can invalidate them. offset=0, nbytes=0 =>
        // whole file. SAFETY: valid fd for the borrow.
        let _ = unsafe {
            libc::sync_file_range(
                fd,
                0,
                0,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
        };
    }
    // Drop the now-clean pages (under O_DIRECT ~none were ever resident).
    // NOTE: `posix_fadvise` returns the errno directly; we discard it (best-effort).
    // SAFETY: valid fd for the borrow.
    let _ = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
}

/// Non-Linux: macOS already kept the write uncached via `F_NOCACHE` (when the
/// direct knob is on); no portable post-hoc page-drop exists otherwise. No-op.
#[cfg(not(target_os = "linux"))]
fn evict_own(_file: &std::fs::File, _direct: bool) {}

fn truncate(file: &std::fs::File, len: u64) -> std::io::Result<()> {
    file.set_len(len)
}

/// fsync a directory so a rename (dirent change) is persisted.
fn fsync_dir(dir: &FsPath) -> std::io::Result<()> {
    let handle = std::fs::File::open(dir)?;
    robust_fsync(&handle)
}

/// fsync `file`, tolerating filesystems that do not support the strongest sync.
/// `File::sync_all` maps to `fcntl(F_FULLFSYNC)` on macOS, which returns `ENOTSUP`
/// on SMB (and some NFS servers); fall back to a plain `fsync(2)`, and if that is
/// also unsupported, degrade to a no-op — the compaction's directory fsync plus
/// snapshot-manifest recovery are the durability floor `object_store`'s default
/// `LocalFileSystem` writer already relies on. Genuine I/O errors (e.g. `EIO`)
/// still propagate: we must never publish output we could not write.
fn robust_fsync(file: &std::fs::File) -> std::io::Result<()> {
    match file.sync_all() {
        Ok(()) => Ok(()),
        Err(e) if is_fsync_unsupported(&e) => match plain_fsync(file) {
            Ok(()) => Ok(()),
            Err(e2) if is_fsync_unsupported(&e2) => {
                tracing::debug!(
                    target: "cayenne::compaction",
                    "content fsync unsupported on this filesystem; relying on directory-fsync + snapshot durability"
                );
                Ok(())
            }
            Err(e2) => Err(e2),
        },
        Err(e) => Err(e),
    }
}

/// Whether an fsync error means the filesystem does not support the operation (vs
/// a genuine I/O failure). `==` comparisons (not an or-pattern) because on Linux
/// `ENOTSUP` and `EOPNOTSUPP` are the same value — an or-pattern would be an
/// unreachable-pattern warning there.
fn is_fsync_unsupported(e: &std::io::Error) -> bool {
    let code = e.raw_os_error();
    code == Some(libc::ENOTSUP) || code == Some(libc::EOPNOTSUPP) || code == Some(libc::ENOSYS)
}

/// A plain `fsync(2)` — weaker than macOS `F_FULLFSYNC`, which some network
/// filesystems reject.
fn plain_fsync(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: valid fd for the borrow.
    let rc = unsafe { libc::fsync(file.as_raw_fd()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Best-effort etag mirroring `LocalFileSystem`'s shape (changes when the file
/// changes), good enough for downstream cache-keying.
fn etag(metadata: &std::fs::Metadata) -> String {
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    format!("{}-{mtime}", metadata.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;

    fn store_over(dir: &FsPath, cfg: CompactionWriterConfig) -> CompactionLocalStore {
        store_over_expected(dir, cfg, 8 << 20)
    }

    /// Build a store with an explicit target-file-size hint. `store_over` seeds a
    /// deliberately GENEROUS 8 MiB so every round-trip exercises the up-front
    /// `fallocate` + final truncate-release path — the on-disk length must still
    /// equal the exact byte count regardless of over-reservation.
    fn store_over_expected(
        dir: &FsPath,
        cfg: CompactionWriterConfig,
        expected_file_bytes: u64,
    ) -> CompactionLocalStore {
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"));
        CompactionLocalStore::new(inner, dir.to_path_buf(), cfg, expected_file_bytes)
    }

    fn cfg(direct: bool) -> CompactionWriterConfig {
        CompactionWriterConfig {
            direct_io: direct,
            fallocate: true,
            bytes_per_sync: 1 << 16, // exercise the rate-smoothing branch
            final_fsync: true,
        }
    }

    /// The storage-tier gate installs the `O_DIRECT` writer ONLY on the
    /// network-attached block-storage tier (`Ebs`) for local-FS compaction —
    /// never on local SSD/NVMe (incl. AWS EC2 `NVMe` instance storage → `LocalSsd`),
    /// tmpfs, undetected storage, non-`Maintenance` writes, or S3.
    #[test]
    fn direct_writer_gated_to_ebs_tier_only() {
        let local = "/data/cayenne/table";
        // Network block storage + compaction + local FS → installed.
        assert!(use_direct_writer_for(
            StorageClass::Ebs,
            WriteClass::Maintenance,
            local
        ));
        // Local SSD/NVMe — INCLUDING AWS EC2 `NVMe` instance storage (classified
        // LocalSsd) — must NOT engage O_DIRECT.
        assert!(!use_direct_writer_for(
            StorageClass::LocalSsd,
            WriteClass::Maintenance,
            local
        ));
        // tmpfs (RAM) and undetected storage: never enable without positive
        // evidence of the networked tier.
        assert!(!use_direct_writer_for(
            StorageClass::Tmpfs,
            WriteClass::Maintenance,
            local
        ));
        assert!(!use_direct_writer_for(
            StorageClass::Unknown,
            WriteClass::Maintenance,
            local
        ));
        // EBS but an S3 object-store path → not installed (no page cache).
        assert!(!use_direct_writer_for(
            StorageClass::Ebs,
            WriteClass::Maintenance,
            "s3://bucket/table"
        ));
        // EBS + a non-Maintenance (delta/append) write → not installed.
        assert!(!use_direct_writer_for(
            StorageClass::Ebs,
            WriteClass::Delta,
            local
        ));
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "i % 251 is always in [0, 250], so the u8 cast never truncates"
    )]
    async fn round_trip(direct: bool, total: usize) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_over(dir.path(), cfg(direct));
        let location = Path::from("snap/part-0.vortex");

        // Non-trivial, position-dependent bytes so a misplaced write is caught.
        let data: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();

        let mut upload = store
            .put_multipart_opts(&location, PutMultipartOptions::default())
            .await
            .expect("begin multipart");
        // Feed odd-sized parts (NOT block-aligned), awaiting each like
        // object_store's writer does — exercises the O_DIRECT re-chunk path and
        // the unaligned final-block padding.
        let mut off = 0;
        while off < data.len() {
            let end = (off + 7919).min(data.len());
            upload
                .put_part(data[off..end].to_vec().into())
                .await
                .expect("put_part");
            off = end;
        }
        upload.complete().await.expect("complete multipart");

        let got = std::fs::read(dir.path().join("snap/part-0.vortex")).expect("read back");
        assert_eq!(got.len(), total, "length mismatch (direct={direct})");
        assert_eq!(got, data, "content mismatch (direct={direct})");
    }

    #[tokio::test]
    async fn buffered_round_trip_various_sizes() {
        for total in [
            0usize,
            1,
            4095,
            4096,
            4097,
            100_000,
            1 << 20,
            (1 << 20) + 123,
        ] {
            round_trip(false, total).await;
        }
    }

    #[tokio::test]
    async fn direct_round_trip_various_sizes() {
        // O_DIRECT falls back to buffered if the fs rejects it; correctness holds
        // either way, which is what we assert.
        for total in [
            0usize,
            1,
            4095,
            4096,
            4097,
            100_000,
            1 << 20,
            (1 << 20) + 123,
        ] {
            round_trip(true, total).await;
        }
    }

    #[tokio::test]
    async fn abort_removes_staging_and_leaves_no_dest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_over(dir.path(), cfg(false));
        let location = Path::from("snap/part-0.vortex");
        let mut upload = store
            .put_multipart_opts(&location, PutMultipartOptions::default())
            .await
            .expect("begin multipart");
        upload
            .put_part(vec![1u8; 10_000].into())
            .await
            .expect("part");
        upload.abort().await.expect("abort");

        assert!(
            !dir.path().join("snap/part-0.vortex").exists(),
            "abort must not publish the destination"
        );
        let staging: Vec<_> = std::fs::read_dir(dir.path().join("snap"))
            .map(|rd| rd.filter_map(Result::ok).collect())
            .unwrap_or_default();
        assert!(
            staging.is_empty(),
            "abort must remove the staging file, found {staging:?}"
        );
    }

    /// A generous target-size hint preallocates up front, but the finished file
    /// must occupy EXACTLY its logical bytes — the final truncate releases the
    /// unused reservation tail. Asserted for the buffered and `O_DIRECT` paths (the
    /// latter falls back to buffered where the fs rejects `O_DIRECT`; the invariant
    /// holds either way).
    #[tokio::test]
    async fn prealloc_tail_released_to_exact_size() {
        for direct in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            // 4 MiB reserved up front; only 5000 bytes actually written.
            let store = store_over_expected(dir.path(), cfg(direct), 4 << 20);
            let location = Path::from("snap/part-0.vortex");
            let mut upload = store
                .put_multipart_opts(&location, PutMultipartOptions::default())
                .await
                .expect("begin multipart");
            upload
                .put_part(vec![0x5A_u8; 5000].into())
                .await
                .expect("put_part");
            upload.complete().await.expect("complete multipart");

            let meta =
                std::fs::metadata(dir.path().join("snap/part-0.vortex")).expect("stat output");
            assert_eq!(
                meta.len(),
                5000,
                "finished file must be exactly its logical size (direct={direct})"
            );
        }
    }

    /// A `0` target-size hint (size-rolling disabled) skips up-front preallocation;
    /// the writer must still round-trip exactly via on-demand growth.
    #[tokio::test]
    async fn zero_expected_size_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_over_expected(dir.path(), cfg(false), 0);
        let location = Path::from("snap/part-0.vortex");
        let data = vec![0xC3_u8; 200_000];
        let mut upload = store
            .put_multipart_opts(&location, PutMultipartOptions::default())
            .await
            .expect("begin multipart");
        upload
            .put_part(data.clone().into())
            .await
            .expect("put_part");
        upload.complete().await.expect("complete multipart");
        let got = std::fs::read(dir.path().join("snap/part-0.vortex")).expect("read back");
        assert_eq!(got, data, "zero-expected write must round-trip exactly");
    }
}
