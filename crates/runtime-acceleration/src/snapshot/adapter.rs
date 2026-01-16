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

//! Snapshot adapter configuration for different accelerator types.
//!
//! This module defines how different accelerators store their data and how
//! that data should be snapshotted. File-based accelerators (`DuckDB`, `SQLite`)
//! use single-file snapshots, while directory-based accelerators (Cayenne)
//! require archiving multiple directories.

use std::path::PathBuf;

/// Describes how an accelerator's data should be snapshotted.
///
/// Different accelerators store their data in different ways:
/// - File-based (`DuckDB`, `SQLite`, Turso): Single database file
/// - Directory-based (Cayenne): Multiple directories (metadata + data)
#[derive(Debug, Clone, Default)]
pub enum SnapshotAdapter {
    /// No snapshot support for this accelerator.
    #[default]
    None,

    /// Single file-based snapshot (e.g., `DuckDB`, `SQLite`).
    ///
    /// The file at the specified path will be copied and uploaded.
    File {
        /// Path to the database file to snapshot.
        path: PathBuf,
    },

    /// Directory-based snapshot requiring tar archival (e.g., Cayenne).
    ///
    /// Multiple directories will be archived into a single tar file
    /// before upload. Each directory is stored with a prefix in the archive.
    Directories {
        /// List of (`directory_path`, `archive_prefix`) tuples.
        /// - `directory_path`: The filesystem path to archive
        /// - `archive_prefix`: The prefix in the tar archive (e.g., "metadata/", "data/")
        dirs: Vec<(PathBuf, String)>,
    },
}

impl SnapshotAdapter {
    /// Creates a file-based snapshot adapter.
    #[must_use]
    pub fn file(path: PathBuf) -> Self {
        Self::File { path }
    }

    /// Creates a directory-based snapshot adapter for Cayenne.
    ///
    /// # Arguments
    ///
    /// * `metadata_dir` - Path to the metadata directory
    /// * `data_dir` - Path to the data directory
    #[must_use]
    pub fn cayenne(metadata_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self::Directories {
            dirs: vec![
                (metadata_dir, "metadata/".to_string()),
                (data_dir, "data/".to_string()),
            ],
        }
    }

    /// Creates a directory-based snapshot adapter with custom directories.
    #[must_use]
    pub fn directories(dirs: Vec<(PathBuf, String)>) -> Self {
        Self::Directories { dirs }
    }

    /// Returns true if this adapter supports snapshots.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Returns the primary path for this adapter (for compatibility with existing code).
    ///
    /// For file-based adapters, returns the file path.
    /// For directory-based adapters, returns the first directory path.
    /// For None, returns None.
    #[must_use]
    pub fn primary_path(&self) -> Option<&PathBuf> {
        match self {
            Self::None => None,
            Self::File { path } => Some(path),
            Self::Directories { dirs } => dirs.first().map(|(p, _)| p),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_adapter_none() {
        let adapter = SnapshotAdapter::None;
        assert!(!adapter.is_enabled());
        assert!(adapter.primary_path().is_none());
    }

    #[test]
    fn test_snapshot_adapter_file() {
        let path = PathBuf::from("/data/test.db");
        let adapter = SnapshotAdapter::file(path.clone());
        assert!(adapter.is_enabled());
        assert_eq!(adapter.primary_path(), Some(&path));
    }

    #[test]
    fn test_snapshot_adapter_cayenne() {
        let metadata_dir = PathBuf::from("/data/metadata");
        let data_dir = PathBuf::from("/data/data");
        let adapter = SnapshotAdapter::cayenne(metadata_dir.clone(), data_dir.clone());

        assert!(adapter.is_enabled());
        assert_eq!(adapter.primary_path(), Some(&metadata_dir));

        if let SnapshotAdapter::Directories { dirs } = adapter {
            assert_eq!(dirs.len(), 2);
            assert_eq!(dirs[0], (metadata_dir, "metadata/".to_string()));
            assert_eq!(dirs[1], (data_dir, "data/".to_string()));
        } else {
            panic!("Expected Directories variant");
        }
    }

    #[test]
    fn test_snapshot_adapter_directories() {
        let dirs = vec![
            (PathBuf::from("/data/dir1"), "prefix1/".to_string()),
            (PathBuf::from("/data/dir2"), "prefix2/".to_string()),
        ];
        let adapter = SnapshotAdapter::directories(dirs.clone());

        assert!(adapter.is_enabled());
        assert_eq!(adapter.primary_path(), Some(&PathBuf::from("/data/dir1")));

        if let SnapshotAdapter::Directories { dirs: stored_dirs } = adapter {
            assert_eq!(stored_dirs, dirs);
        } else {
            panic!("Expected Directories variant");
        }
    }

    #[test]
    fn test_default_is_none() {
        let adapter = SnapshotAdapter::default();
        assert!(!adapter.is_enabled());
    }
}
