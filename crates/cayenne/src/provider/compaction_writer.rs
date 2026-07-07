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
//! # Safety posture
//!
//! Default OFF (env-gated). Linux + local-FS + compaction-output only; S3 and
//! non-Linux are untouched (non-Linux compiles to a buffered fallback). Atomic
//! semantics mirror `LocalFileSystem`: write to a same-dir staging file, fsync
//! contents (new — closes the long-standing local-FS content-durability gap),
//! rename into place, then fsync the parent dir. An `O_DIRECT` open that the
//! filesystem rejects (`EINVAL` on tmpfs/overlay) transparently falls back to
//! buffered. **Gated off pending an HTAP validation run before it is enabled.**

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

/// The `MultipartUpload::put_part` return type (`object_store`'s `UploadPart`
/// alias), written explicitly to avoid depending on its crate-root re-export.
type UploadPart = futures::future::BoxFuture<'static, object_store::Result<()>>;

/// Logical block size for `O_DIRECT` alignment. 4 KiB is the safe superset of
/// every common device/filesystem logical block size (512/4096).
const BLOCK: usize = 4096;
/// Aligned bounce-buffer capacity for the `O_DIRECT` path (a `BLOCK` multiple).
const ODIRECT_BUF_CAP: usize = 1 << 20; // 1 MiB
/// `fallocate` reservation granularity — preallocate this much past the write
/// frontier at a time (`FALLOC_FL_KEEP_SIZE`, so the file size is unchanged).
const FALLOC_CHUNK: u64 = 64 << 20; // 64 MiB

/// Runtime configuration, parsed once from the environment. All knobs default
/// OFF; `enabled()` gates whether the wrapper is installed at all.
#[derive(Debug, Clone, Copy)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent on/off compaction-writer knobs parsed from the environment; plain bools read more clearly here than two-variant enums"
)]
pub(crate) struct CompactionWriterConfig {
    enabled: bool,
    direct_io: bool,
    fallocate: bool,
    bytes_per_sync: u64,
    final_fsync: bool,
}

impl CompactionWriterConfig {
    /// Parse the gate from the environment (cached by the caller). Master switch
    /// is `CAYENNE_COMPACTION_DIRECT_WRITER`; everything else only applies when
    /// that is on.
    pub(crate) fn from_env() -> Self {
        let truthy = |key: &str, default: bool| {
            std::env::var(key).map_or(default, |v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        };
        let enabled = truthy("CAYENNE_COMPACTION_DIRECT_WRITER", false);
        Self {
            enabled,
            direct_io: truthy("CAYENNE_COMPACTION_O_DIRECT", false),
            fallocate: truthy("CAYENNE_COMPACTION_FALLOCATE", true),
            bytes_per_sync: std::env::var("CAYENNE_COMPACTION_BYTES_PER_SYNC")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8 << 20), // 8 MiB
            final_fsync: truthy("CAYENNE_COMPACTION_FINAL_FSYNC", true),
        }
    }

    /// Whether the custom writer should be installed for compaction-output writes
    /// at all. When false, callers leave the default `LocalFileSystem` in place.
    pub(crate) fn enabled(self) -> bool {
        self.enabled
    }
}

/// The process-wide compaction-writer gate, parsed from the environment once.
pub(crate) fn config() -> CompactionWriterConfig {
    static CFG: std::sync::OnceLock<CompactionWriterConfig> = std::sync::OnceLock::new();
    *CFG.get_or_init(CompactionWriterConfig::from_env)
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
}

impl CompactionLocalStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        root: PathBuf,
        cfg: CompactionWriterConfig,
    ) -> Self {
        Self { inner, root, cfg }
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
        match CompactionUpload::create(dest, self.cfg) {
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
    /// writer thread. Errors here surface as a failed `put_multipart`.
    fn create(dest: PathBuf, cfg: CompactionWriterConfig) -> std::io::Result<Self> {
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
        let writer = Writer::new(file, dest, parent, staging.clone(), cfg, direct);
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
    ) -> Self {
        Self {
            file,
            dest,
            parent,
            staging,
            cfg,
            direct,
            offset: 0,
            logical_len: 0,
            allocated: 0,
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
            self.flush_direct_tail()?;
        }
        if self.cfg.final_fsync {
            // Contents durable BEFORE the rename publishes the name — the
            // local-FS content fsync object_store's writer omits.
            self.file.sync_all()?;
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

#[cfg(not(target_os = "linux"))]
fn open_staging(path: &FsPath, _direct_io: bool) -> std::io::Result<(std::fs::File, bool)> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map(|f| (f, false))
}

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

#[cfg(not(target_os = "linux"))]
fn fallocate_keep_size(_file: &std::fs::File, _offset: u64, _len: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "fallocate is Linux-only",
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

fn truncate(file: &std::fs::File, len: u64) -> std::io::Result<()> {
    file.set_len(len)
}

/// fsync a directory so a rename (dirent change) is persisted.
fn fsync_dir(dir: &FsPath) -> std::io::Result<()> {
    let handle = std::fs::File::open(dir)?;
    handle.sync_all()
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
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"));
        CompactionLocalStore::new(inner, dir.to_path_buf(), cfg)
    }

    fn cfg(direct: bool) -> CompactionWriterConfig {
        CompactionWriterConfig {
            enabled: true,
            direct_io: direct,
            fallocate: true,
            bytes_per_sync: 1 << 16, // exercise the rate-smoothing branch
            final_fsync: true,
        }
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
}
