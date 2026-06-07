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

//! Ordering-tier fsync for the staged-commit hot path.
//!
//! ## Why this module exists (measured, not assumed)
//!
//! On macOS, **both** `File::sync_all` *and* `File::sync_data` map to
//! `fcntl(F_FULLFSYNC)` — a full drive-cache flush. Measured on an Apple
//! Silicon laptop (64 KiB file, 20 iterations): `sync_all` ≈ 4.5 ms,
//! `sync_data` ≈ 5.0 ms, raw `fcntl(F_BARRIERFSYNC)` ≈ 0.46 ms, and plain
//! `fsync(2)` ≈ 66 µs. A staged CDC batch pays 5-7 such barriers (data dir,
//! staging WAL file + dir, deletion-vector file + dir, move-target dir), so
//! the full tier put a ~25-30 ms fixed floor under every staged commit on
//! macOS — the dominant cost of small upserts (`vs_duckdb_upsert_scaling`).
//!
//! ## Why plain `fsync(2)` is the right tier on macOS
//!
//! Durability here can't exceed the weakest link, and the weakest link is the
//! metastore: `SQLite` runs `journal_mode=WAL, synchronous=NORMAL` with no
//! `fullfsync` pragma, so the catalog transaction that makes staged files
//! *visible* uses plain `fsync(2)` semantics on macOS (NORMAL does not even
//! fsync on every commit). A power-loss window that loses fsync-tier data
//! necessarily also loses the catalog rows referencing it. Plain `fsync(2)`
//! is likewise the macOS default for `DuckDB` and `PostgreSQL` — it is the
//! durability bar the rest of the storage industry sets on this platform.
//! Crash-consistency (process crash, not power loss) is unaffected: the page
//! cache survives a process crash, and the staging-WAL recovery protocol
//! (`ensure_no_incomplete_write`) heals interrupted commits either way.
//!
//! On non-macOS platforms the REGULAR-FILE helpers ([`ordering_sync_std`] /
//! [`ordering_sync_tokio_file`]) are `File::sync_data` — on Linux that is
//! `fdatasync(2)`, which still issues a device flush and skips only metadata
//! journaling irrelevant to reading the bytes back. The DIRECTORY variants
//! ([`ordering_sync_dir_std`] / [`ordering_sync_dir_tokio_file`]) are the
//! deliberate exception: they keep full `sync_all` on non-macOS, because
//! `fdatasync`'s "metadata required to retrieve the data" wording leaves
//! directory-entry durability implementation-defined, and a dirent that
//! vanishes after power loss un-publishes the file it referenced. On macOS
//! both file and directory variants are plain `fsync(2)` (the metastore
//! weakest-link argument above applies to either handle kind there).
//!
//! Cold paths (table create/drop, metastore initialization, partition
//! creation) intentionally do NOT use this module and keep `sync_all`.

use std::io;

/// Ordering-tier sync for a regular-file [`std::fs::File`].
///
/// For directory handles use [`ordering_sync_dir_std`] — on non-macOS
/// platforms `sync_data` is only guaranteed to flush "metadata required to
/// retrieve the data", and whether that wording covers directory entries is
/// implementation-defined.
///
/// Call from a blocking context (`spawn_blocking`) — on macOS this issues a
/// blocking `fsync(2)` (~tens of µs); elsewhere `sync_data` may block on a
/// device flush.
pub(crate) fn ordering_sync_std(file: &std::fs::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: `fsync` is async-signal-safe and takes a valid open fd; the
        // borrow of `file` keeps the fd alive for the duration of the call.
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_data()
    }
}

/// Ordering-tier sync for a **directory** handle.
///
/// Directories get full `fsync` on non-macOS: POSIX only guarantees
/// `fdatasync` flushes "metadata required to retrieve the data", and whether
/// that covers directory entries is implementation-defined (Linux treats
/// them as the directory's data; other platforms may not). `PostgreSQL` and
/// `SQLite` both use full `fsync` on directories for the same reason. On
/// Linux `fsync` and `fdatasync` are equivalent for directories (same
/// journal commit + device flush), so this costs nothing. On macOS the
/// ordering tier is plain `fsync(2)` either way.
pub(crate) fn ordering_sync_dir_std(dir: &std::fs::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        ordering_sync_std(dir)
    }
    #[cfg(not(target_os = "macos"))]
    {
        dir.sync_all()
    }
}

/// Ordering-tier sync for a regular-file [`tokio::fs::File`].
///
/// For directory handles use [`ordering_sync_dir_tokio_file`] (see
/// [`ordering_sync_dir_std`] for why directories need full `fsync` on
/// non-macOS platforms).
///
/// On macOS the `fsync(2)` runs on the blocking pool (mirroring what tokio's
/// own `sync_data` does internally); elsewhere this delegates to
/// `tokio::fs::File::sync_data`.
pub(crate) async fn ordering_sync_tokio_file(file: &tokio::fs::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // Cancellation safety: `spawn_blocking` tasks are detached — if this
        // future is dropped at the `.await`, the blocking closure keeps
        // running. A raw fd captured from `file` could then outlive the
        // caller's `File` (closed/reused fd under a live `libc::fsync` —
        // unsound). Move an OWNED duplicate of the fd into the closure
        // instead: the dup stays open for exactly as long as the fsync needs
        // it, regardless of caller cancellation. One `dup(2)` (~µs) per sync.
        let owned_dup = file.try_clone().await?.into_std().await;
        tokio::task::spawn_blocking(move || ordering_sync_std(&owned_dup))
            .await
            .map_err(io::Error::other)?
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_data().await
    }
}

/// Ordering-tier sync for a **directory** [`tokio::fs::File`] handle.
///
/// See [`ordering_sync_dir_std`] for why directories use full `fsync` on
/// non-macOS platforms.
pub(crate) async fn ordering_sync_dir_tokio_file(dir: &tokio::fs::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        ordering_sync_tokio_file(dir).await
    }
    #[cfg(not(target_os = "macos"))]
    {
        dir.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn ordering_sync_std_succeeds_on_file_and_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("probe.bin");
        let mut file = std::fs::File::create(&file_path).expect("create probe file");
        file.write_all(b"ordering-tier probe").expect("write");
        ordering_sync_std(&file).expect("file ordering sync");

        let dir_handle = std::fs::File::open(dir.path()).expect("open dir");
        ordering_sync_dir_std(&dir_handle).expect("dir ordering sync");
    }

    #[tokio::test]
    async fn ordering_sync_dir_tokio_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir_handle = tokio::fs::File::open(dir.path())
            .await
            .expect("open dir handle");
        ordering_sync_dir_tokio_file(&dir_handle)
            .await
            .expect("tokio dir ordering sync");
    }

    #[tokio::test]
    async fn ordering_sync_tokio_file_succeeds() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("probe_tokio.bin");
        let mut file = tokio::fs::File::create(&file_path)
            .await
            .expect("create probe file");
        file.write_all(b"ordering-tier probe").await.expect("write");
        ordering_sync_tokio_file(&file)
            .await
            .expect("tokio ordering sync");
    }
}
