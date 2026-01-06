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
pub const LISTING_TABLE_LOCK_POISONED: &str =
    "Lock poisoned on listing table: a thread panicked while holding this lock. \
    This indicates an internal error that requires restarting the runtime.";

/// Error message for poisoned `RwLock` on the deletion cache.
///
/// Lock poisoning occurs when a thread panics while holding the lock, leaving it in an
/// inconsistent state. This is a critical error that typically requires restarting the runtime.
pub const DELETION_CACHE_LOCK_POISONED: &str =
    "Lock poisoned on deletion cache: a thread panicked while holding this lock. \
    This indicates an internal error that requires restarting the runtime.";

/// Default data file ID used for non-partitioned tables.
///
/// In Cayenne, this represents the single data file in a non-partitioned table.
pub const DEFAULT_DATA_FILE_ID: i64 = 0;
