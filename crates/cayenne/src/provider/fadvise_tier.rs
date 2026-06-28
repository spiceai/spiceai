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

//! Page-cache hygiene for BACKGROUND COMPACTION (Linux `POSIX_FADV_DONTNEED`).
//!
//! ## Why this module exists
//!
//! A compaction pass rewrites `O(table)` bytes into a fresh snapshot. Those
//! just-written pages land in the page cache and displace the hot query working
//! set — the scan-under-compaction p99 regression (`vs_duckdb_scan_under_compaction`,
//! `vs_chdb_scan_under_compaction`). Once the merged output is durable we drop
//! its pages so the next *query* — not background maintenance — owns the cache.
//! This is the `RocksDB` / `DuckDB` posture: compaction reorganizes data, it does
//! not warm the cache; reads warm the cache on demand.
//!
//! ## Data safety (unconditional)
//!
//! `POSIX_FADV_DONTNEED` invalidates only **clean** pages — it can never discard
//! unflushed data. Callers must already have made the output durable
//! (`sync_snapshot_dir`) before calling here.
//!
//! ## Dirty-page caveat (load-bearing — without this the hint is a no-op)
//!
//! Compaction outputs are written through `object_store`'s `LocalFileSystem`,
//! which never fsyncs file *contents*; Cayenne only fsyncs the snapshot
//! *directory* (`fsync_tier::ordering_sync_dir_std`). So at the call site the
//! output `.vortex` pages are still **dirty**, and a bare `DONTNEED` would skip
//! them and drop nothing. We first `sync_file_range(WAIT_BEFORE|WRITE|WAIT_AFTER)`
//! — `RocksDB`'s `RangeSync` — to write the page cache back to the device WITHOUT
//! a device-cache flush or metadata-journal barrier (cheaper than `fdatasync`,
//! adds no FUA barrier on EBS), leaving the pages clean and droppable.
//!
//! ## Platform scope
//!
//! Linux only. macOS has no `posix_fadvise`; `F_NOCACHE` is forward-only and
//! cannot drop already-cached pages post-hoc, and an `mmap`+`madvise` path is
//! unreliable for Darwin buffer-cache pages and would `mmap` a multi-GB file —
//! so macOS compiles to a no-op (it is the dev tier; the p99 target is
//! Linux/EBS). S3 has no page cache and is gated out by callers; tmpfs passes
//! the gate but `DONTNEED` on shmem pages is a harmless no-op.

#[cfg(target_os = "linux")]
use std::io;
use std::path::PathBuf;

/// Flush `path`'s dirty pages to the device, then drop its now-clean pages from
/// the page cache. Best-effort: callers log and ignore the result.
///
/// `O_RDONLY` is sufficient — `sync_file_range`/`posix_fadvise` act on the
/// inode's page cache, not the fd's write mode. Cayenne holds no fd for the
/// output (object_store closed the writer), so we re-open.
#[cfg(target_os = "linux")]
pub(crate) fn flush_and_evict(path: &std::path::Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let file = std::fs::File::open(path)?;
    let fd = file.as_raw_fd();

    // 1) Make dirty pages clean so DONTNEED can actually drop them. offset=0,
    //    nbytes=0 => whole file. `sync_file_range` returns -1 + errno on failure.
    //    SAFETY: `fd` is valid for the borrow of `file`.
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
        return Err(io::Error::last_os_error());
    }

    // 2) Drop the now-clean pages. NOTE: `posix_fadvise` returns the errno
    //    DIRECTLY (0 = ok, >0 = errno) — it does NOT set errno / return -1 — so
    //    map with `from_raw_os_error`, not `last_os_error`. Use the symbol, not
    //    a literal: `POSIX_FADV_DONTNEED` is 6 on s390x/musl, 4 elsewhere.
    //    SAFETY: same valid-fd borrow.
    let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    if ret != 0 {
        return Err(io::Error::from_raw_os_error(ret));
    }
    Ok(())
}

/// Non-Linux hosts: no portable post-hoc page-drop exists. No-op.
#[cfg(not(target_os = "linux"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "signature must match the fallible Linux variant so callers are platform-agnostic"
)]
pub(crate) fn flush_and_evict(_path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
}

/// Drop the page cache for a batch of just-written compaction output files, off
/// the reactor (`sync_file_range` with `WAIT_AFTER` blocks until writeback
/// completes). Best-effort and self-logging: it never returns an error, so a
/// failed hint can never fail a compaction. A no-op when `paths` is empty or on
/// a non-Linux host.
pub(crate) async fn evict_files(paths: Vec<PathBuf>) {
    if paths.is_empty() {
        return;
    }
    let _ = tokio::task::spawn_blocking(move || {
        for path in &paths {
            if let Err(error) = flush_and_evict(path) {
                tracing::debug!(
                    target: "cayenne::compaction",
                    path = %path.display(),
                    %error,
                    "compaction page-cache evict hint failed (best-effort, ignored)"
                );
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// `flush_and_evict` succeeds on a freshly written+closed file (exercises
    /// the real `sync_file_range`+`posix_fadvise` syscalls on Linux; a no-op
    /// elsewhere). It must never error on a valid, durable file.
    #[test]
    fn flush_and_evict_succeeds_on_written_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("probe.vortex");
        {
            let mut file = std::fs::File::create(&path).expect("create probe file");
            file.write_all(&vec![0xA5_u8; 1 << 20])
                .expect("write 1 MiB probe");
            file.sync_all().expect("sync probe file");
        }
        flush_and_evict(&path).expect("flush_and_evict must succeed on a durable file");
    }

    /// The batch wrapper drains a mixed list without panicking and is a no-op on
    /// an empty list. Missing paths are swallowed (best-effort), never panic.
    #[tokio::test]
    async fn evict_files_is_best_effort_and_bounded() {
        evict_files(Vec::new()).await; // empty: no-op

        let dir = tempfile::tempdir().expect("tempdir");
        let present = dir.path().join("present.vortex");
        std::fs::write(&present, b"data").expect("write present file");
        let missing = dir.path().join("does_not_exist.vortex");

        // Must complete (and log, not panic) even though one path is missing.
        evict_files(vec![present.clone(), missing]).await;
        assert!(present.exists(), "evict must not delete the file it hints");
    }
}
