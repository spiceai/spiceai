/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Common utilities for file-based object stores (FTP, SFTP, SMB, NFS).
//!
//! This module provides shared functionality to reduce code duplication across
//! network filesystem connectors.

use std::ops::Range;

use chrono::{DateTime, Utc};
use object_store::{GetRange, ListResult, ObjectMeta, path::Path};

/// Create a generic object store error for a given store type.
#[inline]
pub fn generic_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    store: &'static str,
    error: T,
) -> object_store::Error {
    object_store::Error::Generic {
        store,
        source: error.into(),
    }
}

/// Represents a directory entry returned from a filesystem walk.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// The name of the file or directory (not the full path).
    pub name: String,
    /// Whether this entry is a directory.
    pub is_dir: bool,
    /// File size in bytes (0 for directories).
    pub size: u64,
    /// Last modification time.
    pub last_modified: DateTime<Utc>,
}

impl DirEntry {
    /// Create a new file entry.
    #[must_use]
    pub fn file(name: String, size: u64, last_modified: DateTime<Utc>) -> Self {
        Self {
            name,
            is_dir: false,
            size,
            last_modified,
        }
    }

    /// Create a new directory entry.
    #[must_use]
    pub fn directory(name: String) -> Self {
        Self {
            name,
            is_dir: true,
            size: 0,
            last_modified: Utc::now(),
        }
    }
}

/// Build the full path for a directory entry given its parent path.
#[inline]
#[must_use]
pub fn build_full_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// Convert a `DirEntry` to an `ObjectMeta` for files.
#[inline]
#[must_use]
pub fn entry_to_object_meta(full_path: String, entry: &DirEntry) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(full_path),
        size: entry.size,
        last_modified: entry.last_modified,
        e_tag: None,
        version: None,
    }
}

/// Check if a directory entry name should be skipped (. or ..).
#[inline]
#[must_use]
pub fn should_skip_entry(name: &str) -> bool {
    name == "." || name == ".."
}

/// Process directory entries from a listing, returning files as `ObjectMeta`
/// and directories to add to the traversal queue.
///
/// Returns a tuple of (files, directories).
pub fn process_directory_entries(
    parent_path: &str,
    entries: impl IntoIterator<Item = DirEntry>,
) -> (Vec<ObjectMeta>, Vec<String>) {
    let mut files = Vec::new();
    let mut dirs = Vec::new();

    for entry in entries {
        if should_skip_entry(&entry.name) {
            continue;
        }

        let full_path = build_full_path(parent_path, &entry.name);

        if entry.is_dir {
            dirs.push(full_path);
        } else {
            files.push(entry_to_object_meta(full_path, &entry));
        }
    }

    (files, dirs)
}

/// Process directory entries for a shallow listing (`list_with_delimiter`).
/// Returns a `ListResult` with objects (files) and `common_prefixes` (directories).
pub fn process_directory_entries_shallow(
    prefix: &str,
    entries: impl IntoIterator<Item = DirEntry>,
) -> ListResult {
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();

    for entry in entries {
        if should_skip_entry(&entry.name) {
            continue;
        }

        let full_path = build_full_path(prefix, &entry.name);

        if entry.is_dir {
            // Directory paths are stored without trailing slashes (Path normalizes them)
            common_prefixes.push(Path::from(full_path));
        } else {
            objects.push(entry_to_object_meta(full_path, &entry));
        }
    }

    ListResult {
        common_prefixes,
        objects,
    }
}

/// Resolve a `GetRange` option to a concrete byte range given the total file size.
///
/// Returns `(start, end, bytes_to_read)` where:
/// - `start` is the starting byte offset (0-indexed)
/// - `end` is the ending byte offset (exclusive)
/// - `bytes_to_read` is `end - start`
///
/// Handles all `GetRange` variants:
/// - `Bounded(range)`: Use the specified range directly
/// - `Offset(n)`: Read from byte `n` to end of file
/// - `Suffix(n)`: Read the last `n` bytes of the file
#[must_use]
pub fn resolve_range(range: Option<&GetRange>, file_size: u64) -> (u64, u64, u64) {
    match range {
        Some(GetRange::Bounded(r)) => {
            let end = r.end.min(file_size);
            let start = r.start.min(end);
            (start, end, end.saturating_sub(start))
        }
        Some(GetRange::Offset(offset)) => {
            let start = (*offset).min(file_size);
            (start, file_size, file_size.saturating_sub(start))
        }
        Some(GetRange::Suffix(n)) => {
            let start = file_size.saturating_sub(*n);
            (start, file_size, file_size.saturating_sub(start))
        }
        None => (0, file_size, file_size),
    }
}

/// Build a byte `Range` from resolved start and end offsets.
#[inline]
#[must_use]
pub fn build_byte_range(start: u64, end: u64) -> Range<u64> {
    Range { start, end }
}

/// Create an `ObjectMeta` from basic file information.
#[must_use]
pub fn build_object_meta(location: Path, size: u64, last_modified: DateTime<Utc>) -> ObjectMeta {
    ObjectMeta {
        location,
        size,
        last_modified,
        e_tag: None,
        version: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_debug_snapshot;

    #[test]
    fn test_build_full_path() {
        assert_eq!(build_full_path("", "file.txt"), "file.txt");
        assert_eq!(build_full_path("dir", "file.txt"), "dir/file.txt");
        assert_eq!(build_full_path("a/b/c", "file.txt"), "a/b/c/file.txt");
    }

    #[test]
    fn test_should_skip_entry() {
        assert!(should_skip_entry("."));
        assert!(should_skip_entry(".."));
        assert!(!should_skip_entry("file.txt"));
        assert!(!should_skip_entry(".hidden"));
    }

    #[test]
    fn test_resolve_range_none() {
        let (start, end, to_read) = resolve_range(None, 1000);
        assert_eq!((start, end, to_read), (0, 1000, 1000));
    }

    #[test]
    fn test_resolve_range_bounded() {
        let range = GetRange::Bounded(100..500);
        let (start, end, to_read) = resolve_range(Some(&range), 1000);
        assert_eq!((start, end, to_read), (100, 500, 400));
    }

    #[test]
    fn test_resolve_range_bounded_past_end() {
        let range = GetRange::Bounded(800..1500);
        let (start, end, to_read) = resolve_range(Some(&range), 1000);
        assert_eq!((start, end, to_read), (800, 1000, 200));
    }

    #[test]
    fn test_resolve_range_offset() {
        let range = GetRange::Offset(500);
        let (start, end, to_read) = resolve_range(Some(&range), 1000);
        assert_eq!((start, end, to_read), (500, 1000, 500));
    }

    #[test]
    fn test_resolve_range_offset_past_end() {
        let range = GetRange::Offset(1500);
        let (start, end, to_read) = resolve_range(Some(&range), 1000);
        assert_eq!((start, end, to_read), (1000, 1000, 0));
    }

    #[test]
    fn test_resolve_range_suffix() {
        let range = GetRange::Suffix(100);
        let (start, end, to_read) = resolve_range(Some(&range), 1000);
        assert_eq!((start, end, to_read), (900, 1000, 100));
    }

    #[test]
    fn test_resolve_range_suffix_larger_than_file() {
        let range = GetRange::Suffix(2000);
        let (start, end, to_read) = resolve_range(Some(&range), 1000);
        assert_eq!((start, end, to_read), (0, 1000, 1000));
    }

    #[test]
    fn test_process_directory_entries() {
        let entries = vec![
            DirEntry::file("file1.txt".to_string(), 100, Utc::now()),
            DirEntry::directory("subdir".to_string()),
            DirEntry::file("file2.txt".to_string(), 200, Utc::now()),
            DirEntry::directory(".".to_string()),
            DirEntry::directory("..".to_string()),
        ];

        let (files, dirs) = process_directory_entries("parent", entries);

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].location.to_string(), "parent/file1.txt");
        assert_eq!(files[1].location.to_string(), "parent/file2.txt");

        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0], "parent/subdir");
    }

    #[test]
    fn test_process_directory_entries_shallow() {
        let timestamp =
            chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp for test");
        let entries = vec![
            DirEntry::file("file1.txt".to_string(), 100, timestamp),
            DirEntry::directory("subdir".to_string()),
            DirEntry::file("file2.csv".to_string(), 2048, timestamp),
            DirEntry::directory(".".to_string()),
            DirEntry::directory("..".to_string()),
        ];

        let result = process_directory_entries_shallow("data", entries);

        assert_debug_snapshot!(result);
    }

    #[test]
    fn test_build_object_meta() {
        let timestamp =
            chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp for test");
        let meta = build_object_meta(Path::from("test/file.parquet"), 4096, timestamp);

        assert_debug_snapshot!(meta);
    }

    #[test]
    fn test_dir_entry_constructors() {
        let timestamp =
            chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp for test");
        let file = DirEntry::file("data.csv".to_string(), 1024, timestamp);
        let dir = DirEntry::directory("subdir".to_string());

        assert_eq!(file.name, "data.csv");
        assert!(!file.is_dir);
        assert_eq!(file.size, 1024);

        assert_eq!(dir.name, "subdir");
        assert!(dir.is_dir);
        assert_eq!(dir.size, 0);
    }
}
