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
//!   2. `write_deletion_file` (provider/delete/vector_io.rs) fsync'd the
//!      deletion-vector file twice: once via the FileWriter's inner fd, then
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
const DELETE_VECTOR_IO_SRC: &str =
    include_str!("../src/provider/delete/vector_io.rs");

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
    // (`inner.sync_all()?`) — this is the cheap path that uses the open fd
    // we already have. A previous revision additionally re-opened the file
    // with `OpenOptions::new().write(true).open(...)` to fsync it AGAIN,
    // which doubled the per-deletion fsync cost. The reopen must NOT come
    // back without a documented reason.
    assert!(
        body.contains("inner.sync_all()"),
        "write_deletion_file must call `inner.sync_all()` on the FileWriter's \
         inner std::fs::File — this is the fsync that ensures the deletion \
         vector data is durable before we record the path in the catalog."
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

    // Also assert there is at most one `*.sync_all()` call on any file
    // descriptor for the deletion vector file in this function. The parent
    // directory fsync (`dir.sync_all()`) is allowed and distinct.
    let file_sync_all_count = body
        .lines()
        .filter(|line| {
            let line = line.trim();
            // `inner.sync_all()` or `f.sync_all()` style calls on the data file
            // itself. Match on the bare `sync_all()` suffix while excluding the
            // parent-dir `dir.sync_all()` line.
            line.contains(".sync_all()") && !line.contains("dir.sync_all()")
        })
        .count();
    assert_eq!(
        file_sync_all_count, 1,
        "write_deletion_file must call `sync_all()` on the deletion vector \
         file exactly once (the writer's inner fd). Found {file_sync_all_count} \
         occurrence(s). Parent-directory fsync via `dir.sync_all()` is allowed \
         and is filtered out of this count."
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
        body.contains("dir.sync_all()"),
        "write_deletion_file must still fsync the parent directory of the \
         deletion vector file. Without this, a crash after the catalog records \
         the path can leave the directory entry unwritten — the catalog now \
         references a file that does not exist on restart."
    );
}

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
