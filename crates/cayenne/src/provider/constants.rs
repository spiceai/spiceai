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
/// Append writes are first written to `{table_path}/{table_id}/_staging/`,
/// then moved to the current snapshot directory on success. On error, the
/// staging directory is cleaned up (best-effort) and the current snapshot
/// remains unchanged.
pub const STAGING_DIR_NAME: &str = "_staging";

/// Filename for the staging write-ahead log (WAL).
///
/// Written inside `_staging/` after all data files are staged but before
/// the move-to-snapshot operation begins. Records which files need to be
/// moved and to which snapshot. Removed after a successful move.
///
/// If this file exists on table open, or before new writes, the previous staged append was
/// interrupted mid-move and the table may be in an inconsistent state.
pub const STAGING_WAL_FILENAME: &str = "_wal.json";

/// Temporary filename used during atomic staging WAL writes.
///
/// The local-FS WAL writer writes content here first, fsyncs, and then renames
/// to [`STAGING_WAL_FILENAME`] to make the WAL appear atomically. A leftover
/// `_wal.json.tmp` from a process killed mid-write is ignored by recovery
/// (only [`STAGING_WAL_FILENAME`] is consulted) and overwritten on the next
/// staging attempt.
pub const STAGING_WAL_TMP_FILENAME: &str = "_wal.json.tmp";
