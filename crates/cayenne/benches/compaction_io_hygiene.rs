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

//! Micro-bench for the two OS-level IO-hygiene changes in PR #11495
//! (`lukim/cayenne-compaction-fadvise`). Both are syscall-level primitives on
//! the compaction / staged-commit path; this bench isolates each one's cost and,
//! for the page-cache hint, its *value*.
//!
//! # 1. Compaction page-cache eviction (`provider/fadvise_tier::flush_and_evict`)
//!
//! A compaction pass rewrites `O(table)` bytes of fresh output. Those pages land
//! in the page cache and, on a memory-bound host, evict the hot *query* working
//! set — the scan-under-compaction p99 regression. Once the output is durable we
//! drop its clean pages so the next query, not background maintenance, owns the
//! cache. The shipped primitive (Linux only) is:
//!
//! ```ignore
//! sync_file_range(fd, 0, 0, WAIT_BEFORE | WRITE | WAIT_AFTER); // dirty -> clean
//! posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);                // drop clean pages
//! ```
//!
//! The `sync_file_range` is load-bearing: `object_store`'s `LocalFileSystem`
//! never fsyncs file *contents* (Cayenne fsyncs only the snapshot *directory*),
//! so at the call site the output pages are still **dirty**, and a bare
//! `DONTNEED` would skip them and drop nothing.
//!
//! This bench measures, with the **real syscalls on the real page cache**:
//!
//! - `compaction_output_evict/flush_and_evict/<size>` — the full hint on a freshly
//!   written (dirty) output file. This is the wall-clock the eviction step adds to
//!   the (off-reactor) compaction finalize. The `sync_file_range` portion is
//!   writeback *pulled forward* — that output is durable and would be written back
//!   anyway — not net-new work.
//! - `compaction_output_evict/dontneed_only/<size>` — `posix_fadvise(DONTNEED)`
//!   alone on an already-clean file. This isolates the *marginal* page-drop cost
//!   (a page-tree walk); the gap up to `flush_and_evict` is the writeback term.
//! - `compaction_output_read_back/warm/<size>` vs `.../evicted/<size>` — read a
//!   file whose pages are resident vs. one just dropped by `flush_and_evict`. The
//!   warm lane is served from the page cache (memcpy-bound); the evicted lane
//!   faults from the device. **The delta is exactly the per-byte penalty a query
//!   pays when its working set was evicted** — i.e. the cost the change shifts
//!   onto disposable compaction output instead of the hot query set.
//!
//! A one-shot `mincore(2)` check (printed once to stderr at bench start) proves
//! `DONTNEED` actually drops resident pages on this filesystem and guards against
//! the tmpfs false-negative (see *Caveats*).
//!
//! # 2. The removed post-WAL-unlink staging-dir fsync (`staging_wal.rs`)
//!
//! The companion change drops one ordering-tier directory `fsync(2)` from EVERY
//! staged commit (append and delete-staged). It only persisted the WAL marker's
//! *unlink* (recovery hygiene): the durable commit boundary — the move-target dir
//! fsync in `move_staging_files_local` — already fired before it, and the very
//! next line `remove_dir`s the same directory without a sync, so persisting the
//! inner unlink bought nothing end-to-end. On Linux `sync_snapshot_dir` is
//! `File::open(dir).sync_all()`; on ext4/EBS a directory `fsync` after a dirent
//! change forces a journal commit — a billed, capped barrier.
//!
//! - `staged_commit_dir_fsync/open_and_fsync_dir` — the cost of that one removed
//!   barrier (open the staging dir + `sync_all` after a dirent change). This is
//!   the per-staged-commit saving.
//!
//! # 3. The Tier 2/3 compaction writer (`provider/compaction_writer`)
//!
//! The custom writer owns the output fd to add `fallocate` (preallocate the whole
//! output up front) and `O_DIRECT` (bypass the page cache entirely, so the
//! `O(table)` rewrite never populates — hence never evicts — the hot query
//! working set). This group finalizes a full output file each way — preallocate,
//! write, `fsync`, rename, parent-dir `fsync` — and reports throughput:
//!
//! - `compaction_writer_throughput/buffered/<size>` — the baseline `pwrite` path
//!   (what `object_store`'s `LocalFileSystem` does, plus the content fsync).
//! - `compaction_writer_throughput/o_direct/<size>` — the aligned `O_DIRECT` path
//!   (4 MiB bounce buffer, padded-tail `ftruncate`). Falls back to buffered if the
//!   filesystem rejects `O_DIRECT` (`EINVAL`).
//!
//! A one-shot `mincore(2)` readout (stderr) proves the payoff: after a buffered
//! write the whole file is resident; after an `O_DIRECT` write ~0 pages are — the
//! direct path leaves nothing behind to displace the query working set.
//!
//! # Faithfulness
//!
//! `flush_and_evict` here mirrors `provider/fadvise_tier::flush_and_evict`
//! syscall-for-syscall, and `open_and_fsync_dir` mirrors
//! `CayenneTableProvider::sync_snapshot_dir` (→ `fsync_tier::ordering_sync_dir_std`,
//! `dir.sync_all()` on Linux). Those shipped fns are `pub(crate)` and unreachable
//! from a bench crate, so — as elsewhere in this bench suite (e.g.
//! `checkpoint_fence_stall`) — the pattern is reproduced locally. The shipped
//! functions' correctness is locked by unit tests in their own modules.
//!
//! # Platform & caveats
//!
//! - **Linux only** (`posix_fadvise` / `sync_file_range`). On other platforms the
//!   bench compiles to a no-op `main`, matching `fadvise_tier`'s `cfg`.
//! - **tmpfs is a no-op**: `DONTNEED` on shmem pages drops nothing, so on a
//!   tmpfs-backed temp dir the warm/evicted lanes are identical and meaningless.
//!   The temp dir follows `TMPDIR` (else `/tmp`); the `mincore` check prints a
//!   loud warning if eviction is ineffective. Point `TMPDIR` at a disk-backed
//!   path (ext4/xfs) if `/tmp` is tmpfs.
//! - The warm/evicted delta is widest on **EBS / network block storage** (the
//!   production p99 target); on local NVMe it is smaller but still present.
//!
//! # Running
//!
//! ```text
//! cargo bench --bench compaction_io_hygiene -p cayenne
//! ```

#![allow(clippy::expect_used)]

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::fs::{File, OpenOptions};
    use std::hint::black_box;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::path::Path;
    use std::ptr;
    use std::sync::Once;
    use std::time::{Duration, Instant};

    use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
    use tempfile::TempDir;

    /// Output-file sizes. A real compaction output is `O(table)` (often GB); these
    /// modest sizes keep the bench bounded while still exposing the per-byte
    /// scaling of writeback, page-drop, and cold-read.
    const SIZES: &[(&str, usize)] = &[("8MiB", 8 << 20), ("32MiB", 32 << 20), ("64MiB", 64 << 20)];

    /// A disk-backed temp dir (honours `TMPDIR`). `DONTNEED` is a no-op on tmpfs;
    /// the `mincore` check warns if this dir happens to be shmem-backed.
    fn tempdir() -> TempDir {
        tempfile::Builder::new()
            .prefix("cayenne_io_hygiene_")
            .tempdir()
            .expect("create disk-backed temp dir")
    }

    /// Write `size` bytes to `path` (truncating). With `sync`, also `fsync` so the
    /// pages are **clean + resident**; without it the pages are left **dirty +
    /// resident** (the real post-compaction-write state before any sync).
    fn write_bytes(path: &Path, size: usize, sync: bool) {
        // Non-trivial bytes so nothing downstream can elide or dedup the data.
        let chunk: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 251) as u8).collect();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .expect("open output file for write");
        let mut written = 0;
        while written < size {
            let n = chunk.len().min(size - written);
            file.write_all(&chunk[..n]).expect("write output chunk");
            written += n;
        }
        file.flush().expect("flush output file");
        if sync {
            file.sync_all().expect("fsync output file");
        }
    }

    /// Faithful mirror of `provider/fadvise_tier::flush_and_evict`: flush the
    /// file's dirty pages to the device (so `DONTNEED` can drop them), then drop
    /// the now-clean pages. Re-opens `O_RDWR` exactly as the shipped fn does.
    fn flush_and_evict(path: &Path) -> std::io::Result<()> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let fd = file.as_raw_fd();
        // dirty -> clean. offset=0, nbytes=0 => whole file.
        // SAFETY: `fd` is valid for the borrow of `file`.
        let rc = unsafe {
            libc::sync_file_range(
                fd,
                0,
                0,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Drop the now-clean pages. posix_fadvise returns the errno directly.
        // SAFETY: same valid-fd borrow.
        let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
        if ret != 0 {
            return Err(std::io::Error::from_raw_os_error(ret));
        }
        Ok(())
    }

    /// `posix_fadvise(DONTNEED)` alone (no `sync_file_range`). Only drops pages
    /// that are already clean — used to isolate the marginal page-drop cost.
    fn dontneed_only(path: &Path) -> std::io::Result<()> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let fd = file.as_raw_fd();
        // SAFETY: `fd` is valid for the borrow of `file`.
        let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
        if ret != 0 {
            return Err(std::io::Error::from_raw_os_error(ret));
        }
        Ok(())
    }

    /// Read the whole file with a buffered scan (what an `object_store`
    /// `LocalFileSystem` read does). Returns a checksum so the reads can't be
    /// elided.
    fn read_all(path: &Path) -> u64 {
        let mut file = File::open(path).expect("open file for read");
        let mut buf = vec![0u8; 256 << 10];
        let mut checksum: u64 = 0;
        loop {
            let n = file.read(&mut buf).expect("read chunk");
            if n == 0 {
                break;
            }
            // One sampled byte per chunk is enough to defeat dead-read elimination
            // without adding the CPU cost of summing every byte to the I/O timing.
            checksum = checksum.wrapping_add(u64::from(buf[0]));
        }
        checksum
    }

    /// Count resident (in-page-cache) pages for `path` via `mincore(2)`. Returns
    /// `(resident_pages, total_pages)`. Does not itself fault the file in.
    fn resident_pages(path: &Path) -> (usize, usize) {
        let file = File::open(path).expect("open file for mincore");
        let len = file.metadata().expect("stat file").len() as usize;
        if len == 0 {
            return (0, 0);
        }
        // SAFETY: _SC_PAGESIZE is always defined; the cast is to usize.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let npages = len.div_ceil(page);
        // SAFETY: mmap a read-only shared view of a valid fd; we munmap below and
        // mincore writes exactly `npages` bytes into a buffer of that length.
        unsafe {
            let addr = libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            );
            assert!(addr != libc::MAP_FAILED, "mmap failed for mincore");
            let mut vec = vec![0u8; npages];
            let rc = libc::mincore(addr, len, vec.as_mut_ptr());
            assert!(
                rc == 0,
                "mincore failed: {}",
                std::io::Error::last_os_error()
            );
            let resident = vec.iter().filter(|b| *b & 1 == 1).count();
            libc::munmap(addr, len);
            (resident, npages)
        }
    }

    /// One-shot proof (printed to stderr) that `flush_and_evict` actually drops
    /// resident pages on this filesystem. Loud warning on a tmpfs no-op, where the
    /// warm/evicted lanes would be meaningless.
    fn verify_eviction_once(dir: &Path) {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let path = dir.join("verify_evict.bin");
            write_bytes(&path, 8 << 20, true); // clean + durable
            black_box(read_all(&path)); // populate the page cache
            let (before, total) = resident_pages(&path);
            flush_and_evict(&path).expect("flush_and_evict during verification");
            let (after, _) = resident_pages(&path);
            eprintln!(
                "[compaction_io_hygiene] eviction check in {}: resident {before}/{total} pages \
                 BEFORE, {after}/{total} AFTER flush_and_evict.",
                dir.display()
            );
            // Allow a small residual (read-ahead may refill a few pages between the
            // evict and the fresh mincore mmap). >5% still resident => ineffective.
            if after.saturating_mul(20) > before.max(1) {
                eprintln!(
                    "[compaction_io_hygiene] WARNING: DONTNEED dropped little/nothing — this dir is \
                     likely tmpfs. The warm/evicted lanes will be ~equal and NOT meaningful. Set \
                     TMPDIR to a disk-backed (ext4/xfs) path and re-run."
                );
            } else {
                eprintln!(
                    "[compaction_io_hygiene] OK: DONTNEED is effective here; the warm vs evicted \
                     read-back delta is real."
                );
            }
        });
    }

    /// Group 1: cost of the eviction hint added per compaction-output file.
    fn bench_evict_cost(c: &mut Criterion) {
        let dir = tempdir();
        verify_eviction_once(dir.path());

        let mut group = c.benchmark_group("compaction_output_evict");
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(3));

        for &(label, size) in SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            // Full hint on a DIRTY file: sync_file_range (writeback) + DONTNEED.
            let dirty_path = dir.path().join(format!("evict_{label}.bin"));
            group.bench_with_input(
                BenchmarkId::new("flush_and_evict", label),
                &(dirty_path, size),
                |b, (path, size)| {
                    b.iter_custom(|iters| {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iters {
                            write_bytes(path, *size, false); // dirty + resident (untimed)
                            let start = Instant::now();
                            flush_and_evict(path).expect("flush_and_evict");
                            elapsed += start.elapsed();
                        }
                        elapsed
                    });
                },
            );

            // Marginal page-drop only, on an already-CLEAN file.
            let clean_path = dir.path().join(format!("drop_{label}.bin"));
            group.bench_with_input(
                BenchmarkId::new("dontneed_only", label),
                &(clean_path, size),
                |b, (path, size)| {
                    b.iter_custom(|iters| {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iters {
                            write_bytes(path, *size, true); // clean + resident (untimed)
                            let start = Instant::now();
                            dontneed_only(path).expect("dontneed_only");
                            elapsed += start.elapsed();
                        }
                        elapsed
                    });
                },
            );
        }
        group.finish();
    }

    /// Group 2: the value of the hint — read-back latency of resident vs evicted
    /// pages. The delta is the per-byte penalty a query pays on an evicted working
    /// set (what the change shifts onto disposable compaction output).
    fn bench_read_back(c: &mut Criterion) {
        let dir = tempdir();
        verify_eviction_once(dir.path());

        let mut group = c.benchmark_group("compaction_output_read_back");
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(3));

        for &(label, size) in SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            // WARM: pages stay resident across samples (nothing evicts them), so
            // every timed read is served from the page cache.
            let warm_path = dir.path().join(format!("warm_{label}.bin"));
            write_bytes(&warm_path, size, true);
            black_box(read_all(&warm_path)); // prime the cache
            group.bench_with_input(BenchmarkId::new("warm", label), &warm_path, |b, path| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        black_box(read_all(path));
                        elapsed += start.elapsed();
                    }
                    elapsed
                });
            });

            // EVICTED: re-drop the pages before each timed read (untimed), so every
            // timed read faults cold from the device.
            let cold_path = dir.path().join(format!("cold_{label}.bin"));
            write_bytes(&cold_path, size, true);
            group.bench_with_input(BenchmarkId::new("evicted", label), &cold_path, |b, path| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        flush_and_evict(path).expect("evict before cold read");
                        let start = Instant::now();
                        black_box(read_all(path));
                        elapsed += start.elapsed();
                    }
                    elapsed
                });
            });
        }
        group.finish();
    }

    /// Group 3: the per-staged-commit saving from dropping the post-WAL-unlink
    /// staging-dir fsync. Models `sync_snapshot_dir` on Linux: open the dir +
    /// `sync_all()` after a dirent change (the WAL marker unlink).
    fn bench_staged_commit_dir_fsync(c: &mut Criterion) {
        let dir = tempdir();
        let staging = dir.path().join("staging");
        std::fs::create_dir_all(&staging).expect("create staging dir");

        let mut group = c.benchmark_group("staged_commit_dir_fsync");
        group.sample_size(50);

        group.bench_function("open_and_fsync_dir", |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for i in 0..iters {
                    // Dirty the directory's dirents exactly as a commit does: the
                    // WAL marker is created then unlinked, leaving a pending dirent
                    // change for the fsync to persist. Untimed.
                    let marker = staging.join(format!("wal_marker_{i}"));
                    std::fs::write(&marker, b"x").expect("create wal marker");
                    std::fs::remove_file(&marker).expect("unlink wal marker");

                    let start = Instant::now();
                    let handle = File::open(&staging).expect("open staging dir");
                    handle.sync_all().expect("fsync staging dir"); // the removed barrier
                    elapsed += start.elapsed();
                }
                elapsed
            });
        });
        group.finish();
    }

    // -----------------------------------------------------------------------
    // Group 4: the Tier 2/3 compaction WRITER path (fallocate + O_DIRECT).
    // -----------------------------------------------------------------------

    /// One up-front `fallocate(KEEP_SIZE)` of `size` bytes (best-effort), mirroring
    /// the writer's up-front reservation.
    fn prealloc(file: &File, size: u64) {
        if size == 0 {
            return;
        }
        // SAFETY: valid fd for the borrow; offset/len are in-range i64.
        let _ = unsafe {
            libc::fallocate(
                file.as_raw_fd(),
                libc::FALLOC_FL_KEEP_SIZE,
                0,
                i64::try_from(size).unwrap_or(i64::MAX),
            )
        };
    }

    /// Mirror of `compaction_writer`'s buffered finalize: preallocate, write `size`
    /// bytes with plain `pwrite`, then `fsync` the contents. Leaves the staging
    /// file for `publish`.
    fn write_buffered_staging(staging: &Path, size: usize) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(staging)?;
        prealloc(&file, size as u64);
        let chunk: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 251) as u8).collect();
        let mut offset = 0u64;
        let mut written = 0usize;
        while written < size {
            let n = chunk.len().min(size - written);
            let mut b = &chunk[..n];
            while !b.is_empty() {
                let w = file.write_at(b, offset)?;
                b = &b[w..];
                offset += w as u64;
            }
            written += n;
        }
        file.sync_all()
    }

    /// A `BLOCK`-aligned heap buffer for the O_DIRECT probe, freed on `Drop` so an
    /// early `?` on a write error can never leak it. The bench-crate analog of the
    /// production `AlignedBuf` (which is `pub(crate)` and unreachable from here).
    struct AlignedProbeBuf {
        ptr: *mut u8,
        layout: std::alloc::Layout,
        cap: usize,
    }

    impl AlignedProbeBuf {
        fn new(cap: usize, align: usize) -> Self {
            let layout = std::alloc::Layout::from_size_align(cap, align).expect("aligned layout");
            // SAFETY: non-zero cap; null-checked below.
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null(), "aligned alloc failed");
            Self { ptr, layout, cap }
        }

        fn as_mut_slice(&mut self) -> &mut [u8] {
            // SAFETY: `ptr` owns `cap` zeroed, writable bytes for our lifetime.
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
        }
    }

    impl Drop for AlignedProbeBuf {
        fn drop(&mut self) {
            // SAFETY: `ptr`/`layout` are exactly what `alloc_zeroed` returned in `new`.
            unsafe { std::alloc::dealloc(self.ptr, self.layout) };
        }
    }

    /// Mirror of `compaction_writer`'s O_DIRECT finalize: aligned writes through a
    /// 4 MiB bounce buffer, pad the tail to a block, `ftruncate` to the exact
    /// length, then `fsync`. Returns `Ok(false)` if the filesystem rejects
    /// `O_DIRECT` (`EINVAL`) — the module then falls back to buffered.
    fn write_odirect_staging(staging: &Path, size: usize) -> std::io::Result<bool> {
        use std::os::unix::fs::{FileExt, OpenOptionsExt};
        const BLOCK: usize = 4096;
        const CAP: usize = 4 << 20;
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(staging)
        {
            Ok(f) => f,
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => return Ok(false),
            Err(e) => return Err(e),
        };
        prealloc(&file, size as u64);
        // RAII: the aligned buffer frees on Drop, so the `?`s below can early-return
        // on a write/fsync error without leaking it.
        let mut aligned = AlignedProbeBuf::new(CAP, BLOCK);
        let buf = aligned.as_mut_slice();
        let chunk: Vec<u8> = (0..CAP).map(|i| (i % 251) as u8).collect();
        let mut offset = 0u64;
        let mut written = 0usize;
        while written < size {
            let n = CAP.min(size - written);
            buf[..n].copy_from_slice(&chunk[..n]);
            let padded = n.div_ceil(BLOCK) * BLOCK;
            if padded > n {
                buf[n..padded].fill(0);
            }
            let mut b = &buf[..padded];
            while !b.is_empty() {
                let w = file.write_at(b, offset)?;
                b = &b[w..];
                offset += w as u64;
            }
            written += n;
        }
        file.set_len(size as u64)?;
        file.sync_all()?;
        Ok(true)
    }

    /// The publish half of the writer's `finish`: atomic rename + parent-dir fsync
    /// (the content fsync already happened inside the write helper).
    fn publish(staging: &Path, dest: &Path) -> std::io::Result<()> {
        std::fs::rename(staging, dest)?;
        if let Some(parent) = dest.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    /// One-shot proof (stderr) that the O_DIRECT writer leaves ~0 resident pages
    /// while the buffered writer populates the whole file — the core value of the
    /// direct path (it never displaces the hot query working set).
    fn verify_odirect_footprint_once(dir: &Path) {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let size = 32 << 20;
            let bpath = dir.join("footprint_buffered.bin");
            write_buffered_staging(&bpath, size).expect("buffered footprint write");
            let (buffered_resident, total) = resident_pages(&bpath);

            let dpath = dir.join("footprint_odirect.bin");
            match write_odirect_staging(&dpath, size) {
                Ok(true) => {
                    let (odirect_resident, _) = resident_pages(&dpath);
                    eprintln!(
                        "[compaction_io_hygiene] writer footprint in {}: buffered \
                         {buffered_resident}/{total} pages resident, O_DIRECT \
                         {odirect_resident}/{total} resident — O_DIRECT should be ~0 (never \
                         populates the cache).",
                        dir.display()
                    );
                }
                Ok(false) => eprintln!(
                    "[compaction_io_hygiene] O_DIRECT rejected by this filesystem (EINVAL); the \
                     o_direct lane falls back to buffered and is NOT meaningful here."
                ),
                Err(e) => {
                    eprintln!("[compaction_io_hygiene] O_DIRECT footprint probe failed: {e}");
                }
            }
        });
    }

    /// Group 4: write-path throughput of the buffered vs O_DIRECT compaction
    /// writer, each fully finalized (preallocate, write, fsync, rename, dir-fsync).
    fn bench_writer_throughput(c: &mut Criterion) {
        let dir = tempdir();
        verify_odirect_footprint_once(dir.path());

        let mut group = c.benchmark_group("compaction_writer_throughput");
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(3));

        for &(label, size) in SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            let staging = dir.path().join(format!("wb_{label}.staging"));
            let dest = dir.path().join(format!("wb_{label}.vortex"));
            group.bench_with_input(BenchmarkId::new("buffered", label), &size, |b, &size| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let _ = std::fs::remove_file(&dest);
                        let start = Instant::now();
                        write_buffered_staging(&staging, size).expect("buffered write");
                        publish(&staging, &dest).expect("publish");
                        elapsed += start.elapsed();
                    }
                    elapsed
                });
            });

            let staging_d = dir.path().join(format!("wd_{label}.staging"));
            let dest_d = dir.path().join(format!("wd_{label}.vortex"));
            group.bench_with_input(BenchmarkId::new("o_direct", label), &size, |b, &size| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let _ = std::fs::remove_file(&dest_d);
                        let start = Instant::now();
                        // If the filesystem rejects O_DIRECT (EINVAL → Ok(false)), no
                        // staging file was written; fall back to the buffered writer
                        // (mirrors the production writer) so `publish` has a file to
                        // rename instead of panicking on a missing staging path.
                        if !write_odirect_staging(&staging_d, size).expect("odirect write") {
                            write_buffered_staging(&staging_d, size).expect("buffered fallback");
                        }
                        publish(&staging_d, &dest_d).expect("publish");
                        elapsed += start.elapsed();
                    }
                    elapsed
                });
            });
        }
        group.finish();
    }

    criterion_group!(
        benches,
        bench_evict_cost,
        bench_read_back,
        bench_staged_commit_dir_fsync,
        bench_writer_throughput
    );
}

#[cfg(target_os = "linux")]
criterion::criterion_main!(linux_impl::benches);

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!(
        "compaction_io_hygiene: Linux-only (posix_fadvise / sync_file_range); skipped on this \
         platform — see fadvise_tier's cfg gate."
    );
}
