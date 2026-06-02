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

//! Acceleration layout configuration for different accelerator types.
//!
//! This module defines how different accelerators store their data on disk.
//! File-based accelerators (`DuckDB`, `SQLite`) use a single database file,
//! while directory-based accelerators (Cayenne) use multiple directories.
//!
//! This layout information is used for:
//! - Snapshots: knowing what files/directories to copy and archive
//! - Metrics: calculating the total size of the acceleration storage

use std::path::PathBuf;

/// Describes the storage layout of an accelerator's data on disk.
///
/// Different accelerators store their data in different ways:
/// - File-based (`DuckDB`, `SQLite`, Turso): Single database file
/// - Directory-based (Cayenne): Multiple directories (metadata + data)
///
/// This enum is used for both snapshot operations and size metrics.
#[derive(Debug, Clone, Default)]
pub enum AccelerationLayout {
    /// No persistent storage for this accelerator (e.g., in-memory Arrow).
    #[default]
    None,

    /// Single file-based storage (e.g., `DuckDB`, `SQLite`).
    File {
        /// Path to the database file.
        path: PathBuf,
    },

    /// Directory-based storage (e.g., Cayenne).
    ///
    /// Multiple directories may be used (e.g., metadata + data directories).
    /// Each directory is stored with a prefix for archive operations.
    Directories {
        /// List of (`directory_path`, `archive_prefix`) tuples.
        /// - `directory_path`: The filesystem path
        /// - `archive_prefix`: The prefix in tar archives (e.g., "metadata/", "data/")
        dirs: Vec<(PathBuf, String)>,
    },
}

impl AccelerationLayout {
    /// Creates a file-based layout.
    #[must_use]
    pub fn file(path: PathBuf) -> Self {
        Self::File { path }
    }

    /// Creates a directory-based layout for Cayenne.
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

    /// Creates a directory-based layout with custom directories.
    #[must_use]
    pub fn directories(dirs: Vec<(PathBuf, String)>) -> Self {
        Self::Directories { dirs }
    }

    /// Returns true if this layout has storage paths (i.e., not `None`).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Returns the primary path for this layout.
    ///
    /// For file-based layouts, returns the file path.
    /// For directory-based layouts, returns the first directory path.
    /// For None, returns None.
    #[must_use]
    pub fn primary_path(&self) -> Option<&PathBuf> {
        match self {
            Self::None => None,
            Self::File { path } => Some(path),
            Self::Directories { dirs } => dirs.first().map(|(p, _)| p),
        }
    }

    /// Returns the total size in bytes of all files in this layout.
    ///
    /// For file-based layouts, returns the file size.
    /// For directory-based layouts, recursively calculates the total size of all files.
    /// For None, returns 0.
    #[must_use]
    pub fn total_size(&self) -> u64 {
        match self {
            Self::None => 0,
            Self::File { path } => std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
            Self::Directories { dirs } => dirs
                .iter()
                .map(|(dir, _)| Self::calculate_directory_size(dir))
                .sum(),
        }
    }

    /// Recursively calculates the total size of all files in a directory.
    fn calculate_directory_size(path: &std::path::Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_file() {
                    total += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                } else if p.is_dir() {
                    total += Self::calculate_directory_size(&p);
                }
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acceleration_layout_none() {
        let layout = AccelerationLayout::None;
        assert!(!layout.is_enabled());
        assert!(layout.primary_path().is_none());
        assert_eq!(layout.total_size(), 0);
    }

    #[test]
    fn test_acceleration_layout_file() {
        let path = PathBuf::from("/data/test.db");
        let layout = AccelerationLayout::file(path.clone());
        assert!(layout.is_enabled());
        assert_eq!(layout.primary_path(), Some(&path));
    }

    #[test]
    fn test_acceleration_layout_cayenne() {
        let metadata_dir = PathBuf::from("/data/metadata");
        let data_dir = PathBuf::from("/data/data");
        let layout = AccelerationLayout::cayenne(metadata_dir.clone(), data_dir.clone());

        assert!(layout.is_enabled());
        assert_eq!(layout.primary_path(), Some(&metadata_dir));

        if let AccelerationLayout::Directories { dirs } = layout {
            assert_eq!(dirs.len(), 2);
            assert_eq!(dirs[0], (metadata_dir, "metadata/".to_string()));
            assert_eq!(dirs[1], (data_dir, "data/".to_string()));
        } else {
            panic!("Expected Directories variant");
        }
    }

    #[test]
    fn test_acceleration_layout_directories() {
        let dirs = vec![
            (PathBuf::from("/data/dir1"), "prefix1/".to_string()),
            (PathBuf::from("/data/dir2"), "prefix2/".to_string()),
        ];
        let layout = AccelerationLayout::directories(dirs.clone());

        assert!(layout.is_enabled());
        assert_eq!(layout.primary_path(), Some(&PathBuf::from("/data/dir1")));

        if let AccelerationLayout::Directories { dirs: stored_dirs } = layout {
            assert_eq!(stored_dirs, dirs);
        } else {
            panic!("Expected Directories variant");
        }
    }

    #[test]
    fn test_default_is_none() {
        let layout = AccelerationLayout::default();
        assert!(!layout.is_enabled());
    }
}
