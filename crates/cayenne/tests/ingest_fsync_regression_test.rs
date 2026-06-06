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

//! Structural regression tests for fsync work on the Cayenne ingestion hot
//! path.
//!
//! Two duplicate-fsync regressions slipped onto the local-FS staged-append
//! commit path during the ACID Durability hardening sweep:
//!
//!   1. `move_staging_files_local` (provider/table.rs) fsync'd the target
//!      snapshot directory twice back-to-back. Each call is a
//!      `spawn_blocking(File::open + fsync)` on the same path with no
//!      filesystem mutation in between, so the second call is wasted work on
//!      every commit (~ms on SSD, more on slow disks; doubled at high commit
//!      rate).
//!
//!   2. `write_deletion_file` (`provider/delete/vector_io.rs`) fsync'd the
//!      deletion-vector file twice: once via the `FileWriter`'s inner fd, then
//!      again via a fresh `OpenOptions::write(true).open(...)` + `sync_all()`.
//!      No data is written between the two calls, so the second is a redundant
//!      open + fsync on every deletion vector flush.
//!
//! These checks are deliberately structural: they grep the source for the
//! exact duplicated patterns rather than time the commit (which would be
//! flaky on shared CI hosts) or stub out the filesystem (which would require
//! changing public APIs just for the test). The trade-off is that the test is
//! brittle to refactors of the same functions; the failure messages explain
//! the regression contract so anyone refactoring can update the assertions
//! confidently.

#![allow(clippy::expect_used)]

const TABLE_SRC: &str = include_str!("../src/provider/table.rs");
const DELETE_VECTOR_IO_SRC: &str = include_str!("../src/provider/delete/vector_io.rs");
const STREAMING_SRC: &str = include_str!("../src/provider/streaming.rs");

/// Extract the body of a named `async fn`/`fn` from a Rust source file.
///
/// Greps for the function definition by name and returns the slice from
/// `fn <name>` to the matching closing brace at the function's indentation
/// level. Returns `None` if the function is not found.
///
/// This is intentionally naive — it's sufficient for the regression checks
/// here because the targeted functions live at consistent indentation in
/// `impl` blocks. If a future refactor relocates them, update the helpers
/// alongside the assertions.
fn extract_fn_body<'a>(src: &'a str, fn_name: &str) -> Option<&'a str> {
    // Find a line that contains `fn <fn_name>(` with a `{` somewhere after it
    // on the same line (or up to a few lines later).
    let needle = format!("fn {fn_name}(");
    let start_idx = src.find(&needle)?;

    // Walk forward to the opening `{`.
    let open_brace_rel = src[start_idx..].find('{')?;
    let open_brace_idx = start_idx + open_brace_rel;

    // Walk forward tracking brace depth.
    let bytes = src.as_bytes();
    let mut depth = 0usize;
    for (i, &b) in bytes[open_brace_idx..].iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&src[open_brace_idx..=open_brace_idx + i]);
                }
            }
            _ => {}
        }
    }
    None
}

#[test]
fn move_staging_files_local_fsyncs_target_dir_at_most_once() {
    let body = extract_fn_body(TABLE_SRC, "move_staging_files_local")
        .expect("move_staging_files_local function not found in table.rs");

    // Count `sync_snapshot_dir(&target_dir)` callsites in the function body.
    // Any reference to the helper that takes `target_dir` is a directory fsync
    // of the post-rename target snapshot directory. We want EXACTLY ONE.
    let target_dir_fsync_count = body.matches("sync_snapshot_dir(&target_dir)").count();

    assert_eq!(
        target_dir_fsync_count, 1,
        "move_staging_files_local must fsync the target snapshot directory \
         exactly once per call. Found {target_dir_fsync_count} call(s). \
         A duplicate fsync was previously introduced when a new conditional \
         `if moved_count > 0 {{ sync_snapshot_dir(...) }}` was added without \
         removing the pre-existing unconditional one; the result was 2 dir \
         fsyncs on every staged-append commit. If you genuinely need a \
         second fsync, update this assertion AND document the reason in the \
         function body."
    );

    // Defense-in-depth: forbid the older unconditional `sync_snapshot_dir(&target_dir).await?;`
    // line from creeping back in alongside the conditional one. (If the file
    // is ever refactored to drop the `?` propagation, update the substring.)
    let unconditional = "Self::sync_snapshot_dir(&target_dir).await?;";
    let conditional_marker = "if moved_count > 0 {";

    if body.contains(unconditional) {
        assert!(
            body.contains(conditional_marker),
            "move_staging_files_local uses an unconditional `sync_snapshot_dir(&target_dir).await?;` \
             but no `if moved_count > 0` guard. Either keep the guard OR ensure no other \
             fsync of the same directory exists in the function."
        );
    }
}

#[test]
fn move_staging_files_local_skips_dir_fsync_when_no_files_moved() {
    let body = extract_fn_body(TABLE_SRC, "move_staging_files_local")
        .expect("move_staging_files_local function not found in table.rs");

    // The optimization the duplicate-fsync removal preserved: when no files
    // were renamed there is no directory entry update to flush, so the
    // (otherwise unconditional) dir fsync should be guarded by the
    // `moved_count > 0` predicate.
    assert!(
        body.contains("moved_count > 0"),
        "move_staging_files_local must skip the dir fsync when `moved_count == 0` \
         to avoid a no-op `spawn_blocking(File::open + fsync)` on every commit \
         that didn't actually rename any files."
    );
}

#[test]
fn write_deletion_file_fsyncs_inner_fd_not_a_reopened_fd() {
    let body = extract_fn_body(DELETE_VECTOR_IO_SRC, "write_deletion_file")
        .expect("write_deletion_file function not found in delete/vector_io.rs");

    // The deletion-vector writer must sync the FileWriter's inner fd
    // (`inner.sync_data()?`) — this is the cheap path that uses the open fd
    // we already have. A previous revision additionally re-opened the file
    // with `OpenOptions::new().write(true).open(...)` to fsync it AGAIN,
    // which doubled the per-deletion fsync cost. The reopen must NOT come
    // back without a documented reason.
    //
    // Ordering tier (`fsync_tier::ordering_sync_std`), not `sync_all` or
    // `sync_data`: on macOS BOTH std calls are `fcntl(F_FULLFSYNC)` (~4-5 ms
    // full drive-cache flush per call, measured), while the helper issues
    // plain `fsync(2)` (~66 µs) — the same macOS tier as the SQLite
    // metastore (`synchronous=NORMAL`, no fullfsync pragma) whose commit
    // makes this file visible, so a full-tier flush here could not raise
    // end-to-end durability. See `provider/fsync_tier.rs`.
    assert!(
        body.contains("ordering_sync_std(&inner)"),
        "write_deletion_file must call `fsync_tier::ordering_sync_std(&inner)` \
         on the FileWriter's inner std::fs::File — this is the fsync that \
         writes the deletion vector data through before we record the path \
         in the catalog."
    );

    let reopened_fsync_pattern =
        "OpenOptions::new().write(true).open(&output_path)?;\n        f.sync_all()";
    assert!(
        !body.contains(reopened_fsync_pattern),
        "write_deletion_file must NOT re-open the deletion vector file and \
         fsync it a second time. That reopen+fsync pattern is redundant work \
         after `inner.sync_all()` on the writer's inner fd and was previously \
         a per-deletion regression."
    );

    // Also assert there is exactly one file-level sync call on the deletion
    // vector file in this function. The parent directory fsync
    // (`ordering_sync_std(&dir)`) is allowed and distinct.
    let file_sync_count = body
        .lines()
        .filter(|line| {
            let line = line.trim();
            line.contains("ordering_sync_std(&inner)")
        })
        .count();
    assert_eq!(
        file_sync_count, 1,
        "write_deletion_file must sync the deletion vector file exactly once \
         (the writer's inner fd, via `ordering_sync_std(&inner)`). Found \
         {file_sync_count} occurrence(s). Parent-directory fsync via \
         `ordering_sync_std(&dir)` is distinct and not counted here."
    );

    // Tier guard: no full-tier `sync_all` — and no direct `sync_data`, which
    // on macOS is ALSO F_FULLFSYNC — may creep back onto this per-deletion
    // hot path. The ordering tier (`fsync_tier::ordering_sync_std`) is the
    // documented contract; a full-tier flush re-introduces ~4-5 ms per
    // deletion vector on macOS for zero end-to-end durability gain (the
    // catalog commit it protects is SQLite synchronous=NORMAL).
    assert!(
        !body.contains(".sync_all()") && !body.contains(".sync_data()"),
        "write_deletion_file must not call `.sync_all()`/`.sync_data()` \
         directly (both are F_FULLFSYNC on macOS) — route through \
         `fsync_tier::ordering_sync_std` and see provider/fsync_tier.rs."
    );
}

#[test]
fn write_deletion_file_still_fsyncs_parent_dir() {
    // The companion fsync — the parent directory — must still happen, so the
    // dirent for the new deletion-vector file is durable before the catalog
    // is updated. This is the load-bearing half of the ACID-Durability fix
    // that the removed reopen was *replicating*; we want to keep this one.
    let body = extract_fn_body(DELETE_VECTOR_IO_SRC, "write_deletion_file")
        .expect("write_deletion_file function not found in delete/vector_io.rs");

    assert!(
        body.contains("ordering_sync_dir_std(&dir)"),
        "write_deletion_file must still fsync (ordering tier, \
         `ordering_sync_dir_std(&dir)`) the parent directory of the deletion \
         vector file. Without this, a crash after the catalog records the \
         path can leave the directory entry unwritten — the catalog now \
         references a file that does not exist on restart."
    );
}

#[test]
fn write_staging_wal_local_uses_single_open_write_fsync() {
    let body = extract_fn_body(STAGING_WAL_SRC, "write_staging_wal_local")
        .expect("write_staging_wal_local function not found in staging_wal.rs");

    // After the durability hardening sweep, the local staging WAL write path
    // was updated to a single open + write + fsync pattern (no redundant
    // reopen of the file just to call sync_all a second time). This removes
    // a per-write fsync + open cost on every staged append (ingestion).
    //
    // We assert the presence of a proper write path and that there is at most
    // one file-level sync_all (directory syncs are allowed and distinct).
    assert!(
        body.contains("tokio::fs::File::create") || body.contains("OpenOptions"),
        "write_staging_wal_local must open the WAL file for writing"
    );

    let file_sync_count = body
        .lines()
        .filter(|line| {
            let line = line.trim();
            (line.contains(".sync_all()")
                || line.contains(".sync_data()")
                || line.contains("ordering_sync_tokio_file("))
                && !line.contains("staging_dir")
                && !line.contains("parent")
        })
        .count();

    assert!(
        file_sync_count <= 1,
        "write_staging_wal_local must perform at most one file-level sync \
         after writing the WAL content (efficient single open+write+fsync). \
         Found {file_sync_count} file syncs. A redundant reopen+fsync was \
         previously present on the hot ingestion path and has been removed."
    );

    // Tier guard: the WAL marker sync must be the ordering tier
    // (`fsync_tier::ordering_sync_tokio_file` → plain fsync on macOS,
    // fdatasync on Linux), never `sync_all`/`sync_data` — BOTH are
    // F_FULLFSYNC (~4-5 ms per call) on macOS. Losing this marker in a
    // power-loss window only orphans staging files that
    // `ensure_no_incomplete_write` audits and discards, and the metastore's
    // visibility commits (SQLite synchronous=NORMAL) are no stronger — so a
    // full-tier flush here is pure per-batch latency. See
    // `provider/fsync_tier.rs` for the measurements.
    assert!(
        body.contains("ordering_sync_tokio_file(&file)"),
        "write_staging_wal_local must sync the WAL marker with the ordering \
         tier (`fsync_tier::ordering_sync_tokio_file(&file)`)."
    );
    assert!(
        !body.contains(".sync_all()") && !body.contains(".sync_data()"),
        "write_staging_wal_local must not call `.sync_all()`/`.sync_data()` \
         directly (both are F_FULLFSYNC on macOS) on the staged-append hot \
         path — route through fsync_tier and see provider/fsync_tier.rs."
    );
}

#[test]
fn staged_commit_hot_path_uses_ordering_tier_syncs() {
    // The staged-commit hot path pays one barrier per call site per batch.
    // On macOS, BOTH std `sync_all` AND `sync_data` map to
    // `fcntl(F_FULLFSYNC)` — a full drive-cache flush measured at ~4-5 ms per
    // call — while plain `fsync(2)` (what `fsync_tier::ordering_sync_*`
    // issues there) is ~66 µs. On Linux the helper is `fdatasync`, still a
    // device flush.
    //
    // Full-tier barriers on this path bought no end-to-end durability: the
    // SQLite metastore runs `journal_mode=WAL, synchronous=NORMAL` with no
    // `fullfsync` pragma, so the catalog transaction that makes staged files
    // visible is itself only plain-fsync durable on macOS (NORMAL does not
    // even fsync at every commit). A power-loss window that loses
    // ordering-tier data necessarily also loses the catalog rows referencing
    // it. Before this contract, a single 2,000-row staged upsert paid 5-7
    // F_FULLFSYNCs ≈ ~25 ms of pure barrier latency — the dominant fixed cost
    // of small staged batches (vs_duckdb_upsert_scaling).
    //
    // `sync_snapshot_dir` is the shared dir-barrier helper used by the write,
    // staging-WAL, move/publish, and compaction paths; pin its tier here.
    let body = extract_fn_body(TABLE_SRC, "sync_snapshot_dir")
        .expect("sync_snapshot_dir function not found in table.rs");
    assert!(
        body.contains("ordering_sync_dir_std(&dir)"),
        "sync_snapshot_dir must flush directory entries with the ordering \
         tier (`fsync_tier::ordering_sync_dir_std(&dir)`), not `sync_all` or \
         `sync_data` (both F_FULLFSYNC on macOS). See the durability-tier \
         rationale in the function body and provider/fsync_tier.rs."
    );
    assert!(
        !body.contains(".sync_all()") && !body.contains(".sync_data()"),
        "sync_snapshot_dir must not call `.sync_all()`/`.sync_data()` \
         directly: every staged CDC batch pays this barrier multiple times, \
         both are F_FULLFSYNC on macOS, and the metastore's visibility \
         commits (SQLite synchronous=NORMAL) are no stronger — the full \
         flush is pure latency with no durability gain."
    );

    // And the helper itself must implement the macOS ordering tier with a
    // plain `libc::fsync` (std offers no cheaper-than-F_FULLFSYNC call).
    assert!(
        FSYNC_TIER_SRC.contains("libc::fsync"),
        "provider/fsync_tier.rs must issue plain `libc::fsync` on macOS — \
         std `sync_all`/`sync_data` are both F_FULLFSYNC there (measured \
         ~4-5 ms vs ~66 µs for plain fsync)."
    );
}

const FSYNC_TIER_SRC: &str = include_str!("../src/provider/fsync_tier.rs");

// -----------------------------------------------------------------------------
// Devil's Advocate / "Be really sure" analysis (for the recurring /loop task)
// -----------------------------------------------------------------------------
//
// Claim under investigation: "Cayenne ingestion performance has regressed."
//
// Evidence for "real regression that must be fixed":
// - The duplicate `sync_snapshot_dir(&target_dir)` in move_staging_files_local
//   (unconditional + `if moved_count > 0`) was pure wasted work: two identical
//   `File::open + sync_all` with zero filesystem mutation between them. The
//   structural test + the `single_dir_fsync` vs `duplicate_dir_fsync` bench
//   quantify the cost (~2× on every staged-append commit on local FS).
// - The redundant reopen+fsync in write_deletion_file after `inner.sync_all()`
//   on the FileWriter's fd was likewise pure waste (the fd was already synced).
// - The per-write S3 GET of `_staging/_wal.json` (even on 404) in the hot
//   `ensure_no_incomplete_write` path (introduced by the S3 pre-recovery audit
//   durability work) added a network round-trip + auth + error-handling tax on
//   *every* append, including the tiny-inline CDC case. The AtomicBool flag
//   (init true for open-time safety, set on WAL write/remove) eliminates it in
//   steady state while preserving every recovery edge case.
//
// Evidence for the opposite ("the regression is expected/acceptable durability cost"):
// - All *remaining* fsyncs (WAL write + dir sync, post-rename target dir when
//   files actually moved, deletion-vector dir, staging-dir unlink sync, S3
//   tmp→final atomic key discipline) are load-bearing for the documented
//   durability contract ("WAL absent on disk/S3 ⇒ the preceding staged append
//   is durable and will be recovered on restart"). Removing any of them would
//   re-introduce the exact crash scenarios the durability PRs were written to
//   close.
// - On real spinning disks, high-latency EBS, or non-S3-Express object stores,
//   the fsyncs will still be the dominant cost for small-append workloads. That
//   is the inherent price of local-FS ACID durability; users who need higher
//   ingest throughput at the cost of durability can choose DuckDB (file mode),
//   Arrow in-memory, or a remote accelerator with its own durability model.
// - The pre-WAL orphan-file case (crash after `clear_staging_dir` but before
//   `write_staging_wal`) still requires a real `list` + delete on the staging
//   prefix on the *first* write after restart. A second "may_have_files" flag
//   could optimize the steady-state path further, but the safety argument is
//   tighter than for the WAL-present flag; we therefore left the conservative
//   behavior and documented the trade-off.
//
// Conclusion after rigorous review: the duplicate-fsync and per-write-S3-GET
// issues were unambiguous performance bugs with zero durability downside.
// They have been fixed (source changes + this structural test + bench
// quantification). The remaining fsyncs and the conservative clear_staging_dir
// on first post-restart write are intentional and correct. The claim "Cayenne
// ingestion performance has regressed" was true for the accidental duplicates
// and the hot-path GET; it is no longer true after these changes.
//
// Edge cases covered by the combination of this test + existing staged_append_test
// + the mutation_writer benches:
// - 0 files moved (skip dir fsync)
// - >0 files moved (exactly one dir fsync)
// - deletion vector flush (exactly one inner fd sync + one parent dir sync)
// - many tiny inline appends (exercises the ensure fast-path flag)
// - pre-WAL crash orphan files on restart (still triggers clear)
// - WAL-present recovery on open and on next write after in-process drop
// - S3 vs local paths (the flag short-circuit applies to both)
//
// If a future refactor moves the fsync sites or changes the clear/ensure
// call sites, update both the structural assertions *and* the bench
// single/duplicate quantification so the regression signal remains loud.
//
// Additional hot-path optimization (fourth iteration of the recurring task):
// clear_staging_dir (the S3 List+DeletePrefix or local remove_dir_all+create
// that was performed on *every* append, even pure-inline tiny ones) was given
// the same AtomicBool fast-path treatment as the WAL presence check
// (staging_may_have_files flag, init true, set true before any write into
// staging, set false on successful clear or on successful remove after move).
// This removes the last unconditional per-write I/O tax on the hottest
// ingestion path (small appends that stay in the inline memtable tier) while
// preserving the exact safety properties for the pre-WAL orphan crash case
// and all recovery paths. The existing staged_append_test scenarios plus
// high-iteration tiny-append benchmarks now exercise and protect this path.

// -----------------------------------------------------------------------------
// StreamingExec lock-discipline regression tests
// -----------------------------------------------------------------------------
//
// StreamingExec wraps the input RecordBatch stream that feeds the Vortex writer
// during every Cayenne append. A previous revision stored the stream behind a
// `tokio::sync::Mutex<Option<DFStream>>` and, inside the per-batch generator,
// did:
//
//     let mut stream = stream_mutex.lock().await;
//     while let Some(batch) = stream.next().await { yield batch; }
//
// That held the MutexGuard across every `.await` for the entire write (often
// many seconds across hundreds of batches), violating the project rule
// "Never hold locks across `.await`" and adding per-batch acquisition cost
// plus Tokio scheduler convoying during mixed read+ingest workloads.
//
// The fix replaces the inner lock with a `parking_lot::Mutex` whose only role
// is a one-time synchronous take in `execute(...)` — released before any
// await — and forwards batches with an owning unfold state machine. These
// structural assertions ensure the lock-across-await regression cannot quietly
// reappear.

#[test]
fn streaming_exec_does_not_use_async_mutex_for_inner_stream() {
    // The bug we're guarding against: `tokio::sync::Mutex` over the inner
    // `DFStream`. That type's `lock().await` returns a `MutexGuard` that
    // implements `Drop` (releases on drop), so the guard naturally lives
    // across the subsequent `.await` points unless the author very carefully
    // scopes it — which the original code did NOT do.
    //
    // `parking_lot::Mutex` is fine here because the lock is taken
    // *synchronously* and released before any await; the project guideline
    // explicitly prefers parking_lot for this case (fast, no poisoning).
    let banned_field = "stream: tokio::sync::Mutex<";
    assert!(
        !STREAMING_SRC.contains(banned_field),
        "streaming.rs must NOT wrap the inner stream in `tokio::sync::Mutex`. \
         The previous revision did, and the lock was held across `.await` for \
         the entire write — convoying the Tokio scheduler under mixed \
         read+ingest workloads. Use a synchronous `parking_lot::Mutex` (taken \
         once in `execute(...)` and released before any await) instead."
    );
}

#[test]
fn streaming_exec_takes_inner_stream_synchronously() {
    // The fix transfers ownership of the inner stream out of the mutex with a
    // single synchronous `lock()` (parking_lot) followed by `take()`. The
    // structural marker for that pattern is the parking_lot Mutex import
    // (or a fully-qualified reference) and the absence of a `.lock().await`
    // call on `self.stream` inside `execute`.
    //
    // Use `extract_fn_body` to scope the search to the `execute` method
    // (parking_lot may be imported even if execute itself reverts to a bad
    // pattern).
    let execute_body = extract_fn_body(STREAMING_SRC, "execute")
        .expect("execute method not found in streaming.rs");

    // Forbid awaiting on the stream mutex acquisition. This is the bright-line
    // structural marker for the old `tokio::sync::Mutex` regression — that
    // type's `lock()` returns a future you must `.await`, so any callsite
    // with `.await` on the lock acquisition is using the wrong mutex.
    assert!(
        !execute_body.contains("self.stream.lock().await")
            && !execute_body.contains("self.stream.try_lock().await"),
        "StreamingExec::execute must not call `self.stream.lock().await` or \
         `self.stream.try_lock().await`. Awaiting on the lock acquisition is \
         the structural marker for the old `tokio::sync::Mutex` regression \
         that held the guard across every subsequent `.await` for the entire \
         write. Use a synchronous parking_lot lock and release the guard \
         before any await."
    );

    // Affirmatively assert: some form of `self.stream.{lock,try_lock}()` is
    // taken, and `take()` is called somewhere in the body to consume the
    // Option<DFStream>. Both `lock()` and `try_lock()` are synchronous on
    // `parking_lot::Mutex` — only the *awaited* form is banned.
    let acquires_sync_lock = execute_body.contains("self.stream.lock()")
        || execute_body.contains("self.stream.try_lock()");
    let calls_take = execute_body.contains(".take()") || execute_body.contains("guard.take()");
    assert!(
        acquires_sync_lock && calls_take,
        "StreamingExec::execute must take ownership of the inner stream with \
         a synchronous `self.stream.lock()` (or `try_lock()`) + `take()` \
         before forwarding (found acquires_sync_lock={acquires_sync_lock}, \
         calls_take={calls_take}). If the implementation moved to a different \
         structural pattern (e.g. OnceLock or a Mutex<Option<_>> alternative), \
         update this assertion."
    );
}

#[test]
fn streaming_exec_does_not_hold_lock_across_await_in_execute() {
    // Defense in depth for the lock-discipline rule. Even with parking_lot,
    // it is technically possible to write `let g = self.stream.lock(); ...await...`
    // and have the MutexGuard live across the await (parking_lot guards are
    // `!Send`, so this typically fails to compile under multi-thread runtimes,
    // but on single-thread runtimes it would compile and silently re-introduce
    // convoying).
    //
    // The committed pattern explicitly scopes the lock and `take()`s the
    // Option in a single expression so the guard is dropped immediately. We
    // assert the structural marker: there is no `let mut <name> = self.stream.lock();`
    // followed by an `await` later in the same function body.
    let execute_body = extract_fn_body(STREAMING_SRC, "execute")
        .expect("execute method not found in streaming.rs");

    // Find the first line that acquires the stream lock (lock() or try_lock()).
    let lines: Vec<&str> = execute_body.lines().collect();
    let lock_idx = lines.iter().position(|line| {
        let line = line.trim();
        line.contains("self.stream.lock()") || line.contains("self.stream.try_lock()")
    });

    let Some(lock_idx) = lock_idx else {
        panic!(
            "StreamingExec::execute does not acquire the stream lock at all — \
             this is unexpected; the function must take ownership of the \
             stream via `self.stream.lock()` or `self.stream.try_lock()`."
        );
    };

    // Within the next 10 lines after the lock acquisition, a `.take()` must
    // appear AND no `.await` must precede it. The window is generous enough
    // for the typical multi-line `try_lock().ok_or_else(|| { ... })?;` +
    // separate `take()` statement (~6 lines total) and tight enough to
    // prevent an `.await` from sneaking between the lock and the take
    // (which would hold the MutexGuard across the await).
    let window_end = (lock_idx + 10).min(lines.len());
    let mut take_found = false;
    for line in &lines[lock_idx..window_end] {
        if line.contains(".take()") {
            take_found = true;
            break;
        }
        assert!(
            !line.contains(".await"),
            "StreamingExec::execute contains an `.await` between the stream \
             lock acquisition and the `.take()` that consumes the Option. \
             Offending line (trimmed): `{}`. Holding the MutexGuard across \
             an `.await` re-introduces the convoying regression.",
            line.trim()
        );
    }

    assert!(
        take_found,
        "StreamingExec::execute acquires the stream lock at line {lock_idx} \
         of its body, but no `.take()` appears within the next 10 lines. This \
         risks the MutexGuard living past a subsequent `.await`, \
         re-introducing the lock-across-await regression. Drop the guard \
         immediately by chaining `.take()` after the lock or binding both on \
         adjacent lines."
    );
}

// -----------------------------------------------------------------------------
// WAL serialization regression tests
// -----------------------------------------------------------------------------
//
// Both the per-partition `StagingWal` (local FS + S3) and the cross-partition
// `PartitionedWal` (local FS + S3) are JSON markers written on every staged
// append commit. A previous revision serialized them with
// `serde_json::to_string_pretty(...)`, which:
//
//   - Inflates the payload roughly 2-3x (whitespace, newlines, indentation).
//   - Adds CPU time on the hot path for whitespace formatting.
//   - Writes more bytes to disk → more dirty pages → larger fsync cost.
//   - On S3, more bytes billed and slower upload.
//
// These WAL files are machine-only coordination markers — they are never
// inspected by humans during normal operation, and `serde_json::from_str`
// (the reader) is whitespace-tolerant, so legacy pretty-printed WALs from
// older builds still load correctly. Switching to compact serialization is a
// pure performance win with zero observable behavior change.
//
// These structural assertions guard against the pretty-print pattern silently
// reappearing in a future refactor.

const STAGING_WAL_SRC: &str = include_str!("../src/provider/staging_wal.rs");
const PARTITIONED_WAL_SRC: &str = include_str!("../src/provider/partitioned_wal.rs");

#[test]
fn staging_wal_uses_compact_json_serialization() {
    // Both the local-FS and S3 writers must use `to_string` (compact) rather
    // than `to_string_pretty` for the on-disk / on-S3 WAL payload.
    let pretty_uses = STAGING_WAL_SRC.matches("to_string_pretty").count();
    assert_eq!(
        pretty_uses, 0,
        "staging_wal.rs must not call `serde_json::to_string_pretty` for the \
         WAL payload. Found {pretty_uses} usage(s). Pretty-printing the WAL \
         inflates the payload ~2-3x and adds CPU on the ingestion hot path. \
         Use `serde_json::to_string` (compact) instead. The JSON reader is \
         whitespace-tolerant, so legacy pretty WALs from older builds load \
         fine."
    );

    // Affirmative: both writers serialize via to_string.
    let compact_uses = STAGING_WAL_SRC
        .matches("serde_json::to_string(&wal)")
        .count();
    assert!(
        compact_uses >= 2,
        "staging_wal.rs must serialize the StagingWal with compact \
         `serde_json::to_string(&wal)` in both the local-FS and S3 writers. \
         Found {compact_uses} occurrence(s); expected at least 2."
    );
}

#[test]
fn partitioned_wal_uses_compact_json_serialization() {
    // Both `write_to` (local FS) and `write_to_object_store` (S3) must use
    // `to_string` (compact) for the on-disk / on-S3 WAL payload.
    let pretty_uses = PARTITIONED_WAL_SRC.matches("to_string_pretty").count();
    assert_eq!(
        pretty_uses, 0,
        "partitioned_wal.rs must not call `serde_json::to_string_pretty` for \
         the WAL payload. Found {pretty_uses} usage(s). Pretty-printing the \
         coordination WAL inflates every cross-partition commit's payload \
         ~2-3x and adds CPU on the hot path. Use `serde_json::to_string` \
         (compact) instead."
    );

    let compact_uses = PARTITIONED_WAL_SRC
        .matches("serde_json::to_string(self)")
        .count();
    assert!(
        compact_uses >= 2,
        "partitioned_wal.rs must serialize the PartitionedWal with compact \
         `serde_json::to_string(self)` in both `write_to` (local FS) and \
         `write_to_object_store` (S3). Found {compact_uses} occurrence(s); \
         expected at least 2."
    );
}

#[test]
fn compact_wal_payload_is_smaller_than_pretty_for_realistic_payloads() {
    // Behavioral sanity check (in addition to the structural assertions
    // above): for any realistic WAL with N staged files, the compact
    // serialization MUST be strictly smaller than the pretty serialization.
    // If a future serde change ever made the two equivalent we'd lose the
    // perf justification; this test fails loudly in that (unlikely) case.
    use serde::Serialize;

    #[derive(Serialize)]
    struct FakeWal<'a> {
        table_name: &'a str,
        target_snapshot: &'a str,
        staged_files: Vec<String>,
        created_at: &'a str,
    }

    for file_count in [0_usize, 1, 8, 64] {
        let staged_files: Vec<String> = (0..file_count)
            .map(|i| format!("part-{i:05}-c5a8b6e0-vortex.vortex"))
            .collect();

        let wal = FakeWal {
            table_name: "perf_regression_test_table",
            target_snapshot: "01234567-89ab-7def-8123-456789abcdef",
            staged_files,
            created_at: "2026-05-15T19:00:00+00:00",
        };

        let compact = serde_json::to_string(&wal).expect("compact serialize");
        let pretty = serde_json::to_string_pretty(&wal).expect("pretty serialize");

        assert!(
            compact.len() < pretty.len(),
            "Compact JSON ({} bytes) is not smaller than pretty JSON ({} bytes) \
             for a WAL with {file_count} staged files. Either serde has \
             changed its semantics or the test inputs are too degenerate.",
            compact.len(),
            pretty.len(),
        );

        // The strict `compact.len() < pretty.len()` check above is the
        // load-bearing property. We deliberately do not assert a stricter
        // ratio bound here because serde_json's pretty-print overhead per
        // array element is small and roughly constant (~5 bytes for a
        // newline + 2-space indent), so even for 64-element WALs the
        // reduction is only in the 10-15% range. That is still a real
        // hot-path saving — it's ~80 bytes of avoided disk write + page
        // dirty + S3 byte cost per cross-partition commit, multiplied by
        // every staged append — but locking in a specific ratio threshold
        // is fragile. The structural `*_uses_compact_json_serialization`
        // tests above are the real regression guards; this test exists to
        // make the `to_string` vs `to_string_pretty` semantic difference
        // visible (`compact < pretty` must always hold) even on small
        // payloads.
        let _ = file_count;
    }
}

// -----------------------------------------------------------------------------
// WAL write single-open regression tests
// -----------------------------------------------------------------------------
//
// The local-FS WAL writers (staging WAL and partitioned WAL) previously used
// the pattern:
//
//     tokio::fs::write(&path, content.as_bytes()).await?;  // open + write + drop
//     let file = tokio::fs::File::open(&path).await?;     // open AGAIN
//     file.sync_all().await?;
//
// That's two `open(2)` syscalls per WAL write — one inside
// `tokio::fs::write` (create+truncate+write+drop) and another to re-acquire
// an fd for `sync_all`. The fix keeps the fd from a single
// `OpenOptions::new().write(true).create(true).truncate(true).open(...)`
// through to `AsyncWriteExt::write_all` and `sync_all`, dropping one
// `open(2)` per WAL write. At high ingestion rates the saving adds up:
// every staged append writes one staging WAL and every cross-partition
// commit additionally writes one partitioned WAL.
//
// These structural assertions catch regressions to the two-open pattern.

#[test]
fn staging_wal_local_writer_uses_single_open() {
    let body = extract_fn_body(STAGING_WAL_SRC, "write_staging_wal_local")
        .expect("write_staging_wal_local not found in staging_wal.rs");

    // The bad pattern: `tokio::fs::write(...)` immediately followed (with no
    // intervening rename) by `tokio::fs::File::open(...)` to fsync. If both
    // exist in the same function body, we are paying the extra open.
    let bad_pattern_present = body.contains("tokio::fs::write(&wal_path")
        && body.contains("tokio::fs::File::open(&wal_path)");
    assert!(
        !bad_pattern_present,
        "write_staging_wal_local must not use `tokio::fs::write(&wal_path, ...)` \
         followed by `tokio::fs::File::open(&wal_path)` for the fsync. That \
         pattern issues two `open(2)` syscalls per WAL write — one inside \
         `tokio::fs::write` and one for `File::open`. Use \
         `tokio::fs::OpenOptions::new().write(true).create(true).truncate(true).open(...)` \
         and call `write_all` + `sync_all` on the same fd."
    );

    // Affirmative marker: the single-open pattern uses OpenOptions and
    // AsyncWriteExt::write_all.
    assert!(
        body.contains("OpenOptions::new()") && body.contains(".write_all("),
        "write_staging_wal_local must use `tokio::fs::OpenOptions::new()` + \
         `AsyncWriteExt::write_all` to keep the fd through `sync_all`. If a \
         future refactor uses a different single-open primitive, update this \
         assertion accordingly."
    );
}

#[test]
fn partitioned_wal_local_writer_uses_single_open_for_tmp_file() {
    let body = extract_fn_body(PARTITIONED_WAL_SRC, "write_to")
        .expect("write_to not found in partitioned_wal.rs");

    // Same bad pattern as the staging WAL — applied to the tmp file used by
    // the atomic tmp+rename discipline.
    let bad_pattern_present = body.contains("tokio::fs::write(&tmp_path")
        && body.contains("tokio::fs::File::open(&tmp_path)");
    assert!(
        !bad_pattern_present,
        "PartitionedWal::write_to must not use `tokio::fs::write(&tmp_path, ...)` \
         followed by `tokio::fs::File::open(&tmp_path)` for the fsync. That \
         issues two `open(2)` syscalls per cross-partition commit. Use \
         `OpenOptions` + `write_all` + `sync_all` on a single fd."
    );

    assert!(
        body.contains("OpenOptions::new()") && body.contains(".write_all("),
        "PartitionedWal::write_to must use `tokio::fs::OpenOptions::new()` + \
         `AsyncWriteExt::write_all` for the tmp file. If a future refactor \
         uses a different single-open primitive, update this assertion."
    );
}

// -----------------------------------------------------------------------------
// DeletionIndex incremental-bloom regression tests
// -----------------------------------------------------------------------------
//
// The layered deletion index's shared `extend` core is called on every
// PK-aware upsert/delete (via `extend_max_deletes` / `extend_max_conflicts`)
// to merge new (pk → tombstone) entries into the cached deletion snapshot.
// A previous revision rebuilt the bloom filter from scratch on every call,
// turning each per-row update into O(N) work where N is the cumulative
// cache size. The cumulative cost across M writes was O(M·N), the root
// cause of the ~200% ingestion regression on upsert-heavy workloads with
// growing deletion sets (fix landed in commit e8abb4cac4; the layered
// rewrite preserves the same amortization invariants in `extend`).

const DELETION_INDEX_SRC: &str = include_str!("../src/provider/deletion_index.rs");

#[test]
fn deletion_index_extend_tracks_new_keys_for_incremental_bloom() {
    let body = extract_fn_body(DELETION_INDEX_SRC, "extend")
        .expect("extend function not found in deletion_index.rs");

    assert!(
        body.contains("new_delete_hashes"),
        "the layered index's `extend` must track newly-deleted keys \
         (`new_delete_hashes`) so the bloom can be updated incrementally \
         for the K new keys instead of being rebuilt from scratch over all \
         N entries. This keeps the per-call cost O(K) amortized."
    );

    assert!(
        body.contains("outcome.new_delete"),
        "`extend` must record a bloom hash only for keys gaining their \
         first deletion (`outcome.new_delete`) so repeat-deletes of the \
         same key do not inflate the incremental insert set."
    );
}

#[test]
fn deletion_index_extend_has_amortized_rebuild_trigger() {
    let body = extract_fn_body(DELETION_INDEX_SRC, "extend")
        .expect("extend function not found in deletion_index.rs");

    assert!(
        body.contains("delete_count > self.bloom_capacity"),
        "`extend` must rebuild the bloom only when deletion growth has \
         outpaced the sized capacity (`delete_count > self.bloom_capacity`). \
         Rebuilding more eagerly re-introduces the O(M·N) regression."
    );

    assert!(
        body.contains("bloom_capacity_for"),
        "`extend` must size the rebuilt bloom via `bloom_capacity_for` \
         (2x headroom) so the rebuild cadence stays geometric and the \
         filter never spends long windows saturated."
    );
}

#[test]
fn deletion_index_does_not_unconditionally_rebuild_bloom() {
    let body = extract_fn_body(DELETION_INDEX_SRC, "extend")
        .expect("extend function not found in deletion_index.rs");

    // The pre-fix implementation rebuilt the bloom over every entry on every
    // call. The layered `extend` must keep the full rebuild behind the
    // capacity guard and otherwise insert only the new hashes in place.
    assert!(
        body.contains("for hash in new_delete_hashes"),
        "`extend` must have an incremental branch that inserts only the new \
         deletion hashes into the shared bloom (O(K) work), instead of \
         walking every entry on every call (O(N²) cumulative on upsert \
         workloads)."
    );
}

#[test]
fn deletion_index_tracks_bloom_capacity_field() {
    assert!(
        DELETION_INDEX_SRC.contains("bloom_capacity: usize"),
        "DeletionIndex / KeyDeletionIndex must carry a `bloom_capacity: usize` \
         field so `extend_max` can decide when to rebuild."
    );

    let occurrences = DELETION_INDEX_SRC.matches("bloom_capacity:").count();
    assert!(
        occurrences >= 2,
        "Both DeletionIndex and KeyDeletionIndex must declare `bloom_capacity`. \
         Found {occurrences}; expected at least 2 (Int64Pk + composite-PK)."
    );
}

// -----------------------------------------------------------------------------
// Partition lookup read-lock fast-path regression test
// -----------------------------------------------------------------------------
//
// `CayennePartitionedInsertStrategy::get_or_create_partition_provider` is
// called once per row group on partitioned ingestion. A previous revision
// unconditionally acquired `partitions.write().await`, serializing all
// writers through a single exclusive lock — a global write barrier across
// the table. Fix: read-lock fast path + double-checked write-lock slow
// path (commit cc953f0262).

const PARTITIONED_INSERT_STRATEGY_SRC: &str =
    include_str!("../../runtime/src/dataaccelerator/cayenne/partitioned_insert_strategy.rs");

#[test]
fn partition_lookup_uses_read_lock_fast_path() {
    assert!(
        PARTITIONED_INSERT_STRATEGY_SRC.contains("self.partitions.read().await"),
        "get_or_create_partition_provider must include a `self.partitions.read().await` \
         fast-path BEFORE acquiring the write lock. Without it, every per-row \
         partition lookup goes through the exclusive write lock, serializing \
         all writers across the partitioned table."
    );

    assert!(
        PARTITIONED_INSERT_STRATEGY_SRC.contains("self.partitions.write().await"),
        "get_or_create_partition_provider must still acquire \
         `self.partitions.write().await` on the slow path (partition not yet \
         created). Without it, two concurrent writers creating the same new \
         partition would race."
    );
}

// -----------------------------------------------------------------------------
// Position-based deletion-cache Arc-wrap regression test
// -----------------------------------------------------------------------------
//
// `cached_deleted_row_ids` is published through `ArcSwap`. Every per-batch
// position-based delete writes a fresh snapshot via
// `cached_deleted_row_ids.store(Arc::new(updated_map))`. If the inner value
// type is `RoaringBitmap` (NOT wrapped in `Arc`), the
// `(*old_arc).clone()` step deep-clones every file's bitmap on every commit,
// turning each delete into O(total deleted rows across all files) per call.
// On long-lived tables with many files the per-batch cost grows without
// bound.
//
// The fix wraps each per-file deletion vector in `Arc<PositionDeletionVector>`
// (type alias `PositionBitmap`). The outer HashMap clone now only iterates
// `Arc` pointers (O(F) cheap Arc::clones), not the bitmap/access-plan data.
// Per-batch cost becomes O(F + K_new) where K_new is the number of files
// actually touched by THIS commit.

const DELETION_STRATEGY_SRC: &str = include_str!("../src/provider/deletion_strategy.rs");
const POSITION_BASED_SINK_SRC: &str = include_str!("../src/provider/delete/sink/position_based.rs");

#[test]
fn position_bitmap_type_wraps_bitmap_in_arc() {
    // The shared type alias MUST hold an Arc-wrapped per-file deletion vector.
    // Storing bare `RoaringBitmap` re-introduces the O(total deleted rows)
    // deep-clone on every position-based delete commit and loses the prebuilt
    // scan-time access plan.
    let expected = "type PositionBitmap = HashMap<String, Arc<PositionDeletionVector>>;";
    assert!(
        DELETION_STRATEGY_SRC.contains(expected),
        "PositionBitmap must be `HashMap<String, Arc<PositionDeletionVector>>`. \
         The per-file deletion vector wrap in `Arc` is what lets \
         `cached_deleted_row_ids.store(Arc::new(updated_map))` publish a fresh \
         snapshot without deep-cloning every bitmap/access-plan. A bare \
         `HashMap<String, RoaringBitmap>` re-introduces the O(total deleted rows) \
         per-commit clone and the per-scan bitmap-to-treemap rebuild."
    );
}

#[test]
fn position_based_sink_uses_arc_wrapped_bitmaps() {
    // Sanity-check the writer-side updates use `Arc<PositionDeletionVector>`
    // for the cache_updates map and avoid the bare-clone pattern. Both checks
    // are structural — the failure modes are subtle (correctness still works
    // either way, but perf collapses).
    assert!(
        POSITION_BASED_SINK_SRC.contains("HashMap<String, Arc<PositionDeletionVector>>"),
        "position_based.rs must build cache_updates as \
         `HashMap<String, Arc<PositionDeletionVector>>` so the published \
         snapshot doesn't deep-clone each entry at store time and scan planning \
         can reuse prebuilt access plans. Bare `HashMap<String, RoaringBitmap>` \
         types here force bitmap clones and per-scan treemap rebuilds."
    );

    // The pre-fix regressed pattern: cloning the entire outer map via
    // `(*cached_deleted_row_ids.load_full()).clone()` works equally for both
    // value types BUT only the Arc<_> form keeps the clone cheap. Make sure
    // the pre-fix one-line `RoaringBitmap` deref+clone is gone.
    let bare_bitmap_clone = "let mut updated_map: HashMap<String, RoaringBitmap> =\n            (*cached_deleted_row_ids.load_full()).clone();";
    assert!(
        !POSITION_BASED_SINK_SRC.contains(bare_bitmap_clone),
        "position_based.rs must NOT clone a `HashMap<String, RoaringBitmap>` \
         from the ArcSwap snapshot — that pattern deep-clones every file's \
         bitmap on every commit (the regression). Use the Arc-wrapped form: \
         `HashMap<String, Arc<RoaringBitmap>>`."
    );
}

#[test]
fn position_based_sink_rebuilds_only_changed_deletion_vectors() {
    // When rebuilding a single file's updated bitmap, construct one fresh
    // `PositionDeletionVector` for that file. Unchanged files stay shared
    // through their existing Arc entries in the outer map.
    assert!(
        POSITION_BASED_SINK_SRC.contains("Arc::new(PositionDeletionVector::new(updated_bitmap))"),
        "position_based.rs should build a fresh PositionDeletionVector only for \
         files changed by the current delete commit. Unchanged files must remain \
         shared through the existing outer-map Arc entries."
    );
}

// -----------------------------------------------------------------------------
// Inline-memtable pressure check fast-path regression test
// -----------------------------------------------------------------------------
//
// `checkpoint_inlined_data_if_memtable_pressure_exceeded` is called after
// every inline-write commit to decide whether to flush the level-0 inline
// memtable to Vortex. The pre-fix implementation unconditionally issued a
// `get_inlined_data_stats` SQL query (per-write catalog round trip) just to
// read three integer counters that the in-process atomic
// `inlined_row_count` already tracks accurately. On network catalogs
// (Turso, PostgreSQL metastore) each round trip costs 10-50 ms, dominating
// the small-batch CDC ingestion path.
//
// The fix consults the cached `inlined_row_count` first: when far below the
// segments-threshold-implied row count, neither the segments threshold nor
// the bytes threshold can have been crossed (each commit adds at most one
// inline entry, and INLINE_MAX_BYTES caps per-write payload), so the SQL
// query is unnecessary. This is the same fast-path treatment the parallel
// agents already applied to `clear_staging_dir`,
// `ensure_no_incomplete_write`, and the compaction trigger.

#[test]
fn checkpoint_inlined_pressure_has_cached_fast_path() {
    let body = extract_fn_body(
        TABLE_SRC,
        "checkpoint_inlined_data_if_memtable_pressure_exceeded",
    )
    .expect("checkpoint_inlined_data_if_memtable_pressure_exceeded function not found in table.rs");

    // The fast path must use the cached atomic before the catalog call.
    assert!(
        body.contains("inlined_row_count.load"),
        "checkpoint_inlined_data_if_memtable_pressure_exceeded must consult \
         `self.inlined_row_count.load(...)` BEFORE the catalog round trip. \
         Without the cached-atomic fast path, every inline write pays a \
         `get_inlined_data_stats` SQL query — ~ms on SQLite and 10-50 ms on \
         network catalogs — even though the in-process atomic counter is \
         accurate within a single Cayenne writer."
    );

    // The early-return must happen BEFORE the catalog call. We check
    // ordering by string position — but the function body's doc comments
    // may mention `get_inlined_data_stats` by name (e.g. to explain why
    // the fast path matters), so we search for the actual CALL prefix
    // `self.catalog` immediately followed by the method, not the bare
    // function name (which appears in comments).
    let load_idx = body
        .find("self.inlined_row_count.load")
        .or_else(|| body.find("inlined_row_count.load"))
        .expect("cached load not found");
    // Look for the actual catalog call. The lib uses `self.catalog` then
    // a builder-style chain ending in `.get_inlined_data_stats(...)`. The
    // call is uniquely identified by the `.get_inlined_data_stats(` token
    // — the doc comment, by contrast, references the function by its bare
    // identifier `get_inlined_data_stats` with no preceding period.
    let catalog_idx = body
        .find(".get_inlined_data_stats(")
        .expect("catalog call .get_inlined_data_stats(...) not found");
    assert!(
        load_idx < catalog_idx,
        "checkpoint_inlined_data_if_memtable_pressure_exceeded must check \
         the cached row count BEFORE the `.get_inlined_data_stats(...)` \
         SQL call (load_idx={load_idx}, catalog_idx={catalog_idx}). \
         Loading the atomic AFTER the catalog round trip defeats the \
         purpose — the SQL query has already happened. Reorder so the \
         fast path returns before any catalog work."
    );

    // The fast-path threshold should reference at least one inline-flush
    // threshold constant. The current implementation uses
    // `INLINE_FLUSH_MAX_BYTES / INLINE_MAX_BYTES` since this is the
    // tightest of the three (bytes, entries, rows) thresholds when reasoning
    // about an upper bound from cached_rows alone.
    assert!(
        body.contains("INLINE_FLUSH_MAX_BYTES")
            || body.contains("INLINE_FLUSH_MAX_SEGMENTS")
            || body.contains("INLINE_FLUSH_MAX_ROWS"),
        "checkpoint_inlined_data_if_memtable_pressure_exceeded must compare \
         the cached row count against a meaningful threshold constant \
         (INLINE_FLUSH_MAX_BYTES / INLINE_FLUSH_MAX_SEGMENTS / \
         INLINE_FLUSH_MAX_ROWS) for the fast path to be a load-bearing \
         invariant. A bare numeric literal decouples the fast path from \
         the threshold definitions and risks silent drift."
    );
}
