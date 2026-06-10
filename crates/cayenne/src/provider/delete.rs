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

//! Deletion logic for Cayenne tables.
//!
//! This module provides deletion filtering strategies:
//!
//! - **Position-based**: Per-file deletion vectors (`RoaringBitmap` of file-local
//!   row positions) pushed down directly into the Vortex scan via
//!   `Selection::ExcludeRoaring` for efficient row skipping. The only strategy
//!   for tables WITHOUT a primary key; PK tables under `deletion_mode: position`
//!   (the `auto` default) also record per-file position vectors for rows whose
//!   `(file, position)` is known, falling back to the key filters below for
//!   unlocated rows.
//!
//! - **Int64 PK-based (`Int64PkDeletionFilterExec`)**: For tables with a single-column
//!   Int64 primary key. Probes a bloom-prefiltered
//!   [`deletion_index::DeletionIndex`](super::deletion_index::DeletionIndex)
//!   of fused delete/insert-sequence tombstones — no key serialization overhead.
//!   This is the most efficient key-based strategy for the common case.
//!
//! - **RowConverter-based (`KeyBasedDeletionFilterExec`)**: For tables with composite
//!   or non-integer primary keys. Uses Arrow's `RowConverter` to create deterministic
//!   byte keys and probes a
//!   [`deletion_index::KeyDeletionIndex`](super::deletion_index::KeyDeletionIndex)
//!   (keyed by the XXH3-128 hash of the key bytes). More overhead but handles
//!   all PK types.
//!
//! Also provides:
//! - `CayenneDeletionSink`: Handles writing deletion vectors to storage
//!
//! # Module Structure
//!
//! - [`vector_io`]: Deletion vector file I/O - reads/writes Arrow IPC files containing deleted row identifiers
//! - [`filter_exec`]: Query-time deletion filters - execution plans that exclude deleted rows during scans
//! - [`sink`]: Deletion orchestration - scans for matching rows, persists deletion vectors, updates caches

mod filter_exec;
mod sink;
mod vector_io;

// Public API - re-exported in provider/mod.rs
pub use sink::CayenneDeletionSink;
pub(crate) use sink::file_based::FileBasedDeletionSink;

// Crate-internal types used by table.rs
pub(crate) use filter_exec::{
    InsertRecordHandling, Int64PkDeletionFilterExec, KeyBasedDeletionFilterExec, is_pk_visible_i64,
};
pub(crate) use vector_io::{
    DeletionIdentifier, DeletionVectorWriteResult, DeletionVectorWriteSpec, DeletionVectorWriter,
    detect_deletion_type_and_read,
};
