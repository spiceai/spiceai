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

use chrono::{DateTime, Utc};
use object_store::{ObjectMeta, path::Path};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
