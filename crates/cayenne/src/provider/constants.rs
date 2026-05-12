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

/// Error message for poisoned `RwLock` on the listing table.
///
/// Lock poisoning occurs when a thread panics while holding the lock, leaving it in an
/// inconsistent state. This is a critical error that typically requires restarting the runtime.
pub const LISTING_TABLE_LOCK_POISONED: &str = "Lock poisoned on listing table: a thread panicked while holding this lock. \
    This indicates an internal error that requires restarting the runtime.";

/// Error message for poisoned `RwLock` on protected snapshots.
///
/// Lock poisoning occurs when a thread panics while holding the lock, leaving it in an
/// inconsistent state. This is a critical error that typically requires restarting the runtime.
pub const PROTECTED_SNAPSHOTS_LOCK_POISONED: &str = "Lock poisoned on protected snapshots: a thread panicked while holding this lock. \
    This indicates an internal error that requires restarting the runtime.";

/// Default data file ID used for non-partitioned tables.
///
/// In Cayenne, this represents the single data file in a non-partitioned table.
pub const DEFAULT_DATA_FILE_ID: i64 = 0;

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
