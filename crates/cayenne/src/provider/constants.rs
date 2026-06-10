/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Constants used throughout the Cayenne provider module.

/// Reserved directory name for staged append writes.
///
/// Each staged append writes into its own isolated subdirectory
/// `{table_path}/{table_id}/_staging/<id>/` (the `<id>` is a `UUIDv7`, so
/// concurrent appends never share a staging dir), then the files are moved to
/// the target snapshot directory on success. On error, the append's staging
/// subdirectory is cleaned up (best-effort) and the target snapshot remains
/// unchanged.
pub const STAGING_DIR_NAME: &str = "_staging";

/// Filename for the staging write-ahead log (WAL).
///
/// Written at `_staging/<id>/_wal.json` after all data files are staged but
/// before the move-to-snapshot operation begins. Records which files need to
/// be moved and to which snapshot (current or protected). Removed after a
/// successful move. A WAL directly at `_staging/_wal.json` is a legacy layout
/// and is rejected with an error.
///
/// If this file exists on table open, or before new writes, the previous
/// staged append was interrupted mid-move.
/// `CayenneTableProvider::ensure_no_incomplete_write` then attempts automated
/// recovery (re-driving the move and removing the WAL) and only surfaces an
/// `IncompleteWrite` error when recovery would be unsafe — e.g. the current
/// snapshot has moved on, or WAL-listed files are missing from both the
/// staging and target directories.
pub const STAGING_WAL_FILENAME: &str = "_wal.json";

/// Temporary filename used during atomic staging WAL writes.
///
/// The local-FS WAL writer writes content here first, fsyncs, and then renames
/// to [`STAGING_WAL_FILENAME`] to make the WAL appear atomically. A leftover
/// `_wal.json.tmp` from a process killed mid-write is ignored by recovery
/// (only [`STAGING_WAL_FILENAME`] is consulted) and overwritten on the next
/// staging attempt.
pub const STAGING_WAL_TMP_FILENAME: &str = "_wal.json.tmp";
