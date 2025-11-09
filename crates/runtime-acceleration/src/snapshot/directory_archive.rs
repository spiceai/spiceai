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

//! Directory archival support for Cayenne snapshots.
//!
//! This module provides fast archival and extraction of directory trees for Cayenne acceleration,
//! which stores data across multiple directories (metadata + data).
//!
//! Uses tar with minimal/no compression since Vortex files are already highly compressed.

use snafu::prelude::*;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Snafu)]
pub enum ArchiveError {
    #[snafu(display("Failed to create archive from {}: {source}", path.display()))]
    CreateArchive {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to extract archive to {}: {source}", path.display()))]
    ExtractArchive {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to read directory {}: {source}", path.display()))]
    ReadDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write to archive: {source}"))]
    WriteArchive { source: std::io::Error },

    #[snafu(display("Failed to read from archive: {source}"))]
    ReadArchive { source: std::io::Error },
}

type Result<T> = std::result::Result<T, ArchiveError>;

/// Archive multiple directories into a single tar stream.
///
/// This function creates a tar archive containing all files from the specified directories.
/// The archive is written to the provided writer.
///
/// # Arguments
///
/// * `dirs` - A slice of tuples containing (`directory_path`, `archive_prefix`)
///   - `directory_path`: The actual filesystem path to archive
///   - `archive_prefix`: The prefix to use in the tar archive (e.g., "metadata/", "data/")
/// * `writer` - An async writer to write the tar archive to
///
/// # Returns
///
/// The total number of bytes written to the archive.
///
/// # Errors
///
/// Returns an error if:
/// - Any directory cannot be read
/// - Files cannot be added to the archive
/// - Writing to the output fails
pub async fn archive_directories<W>(dirs: &[(PathBuf, String)], writer: W) -> Result<u64>
where
    W: AsyncWrite + Unpin + Send,
{
    use tar::Builder;
    use tokio::io::AsyncWriteExt;
    use tokio::task::spawn_blocking;

    let dirs = dirs.to_vec();

    // Use spawn_blocking since tar operations are synchronous
    let (total_bytes, tar_data) = spawn_blocking(move || {
        let mut buffer = Vec::new();
        {
            let mut archive = Builder::new(&mut buffer);

            for (dir_path, archive_prefix) in &dirs {
                if !dir_path.exists() {
                    tracing::warn!("Directory {} does not exist, skipping", dir_path.display());
                    continue;
                }

                if !dir_path.is_dir() {
                    tracing::warn!("{} is not a directory, skipping", dir_path.display());
                    continue;
                }

                // Add all files from this directory recursively
                add_directory_to_archive(&mut archive, dir_path, archive_prefix).map_err(|e| {
                    ArchiveError::CreateArchive {
                        path: dir_path.clone(),
                        source: e,
                    }
                })?;
            }

            // Finish the archive
            archive
                .finish()
                .map_err(|source| ArchiveError::WriteArchive { source })?;
        } // Drop archive here to release the mutable borrow

        let total_bytes = buffer.len() as u64;
        Ok::<(u64, Vec<u8>), ArchiveError>((total_bytes, buffer))
    })
    .await
    .map_err(|e| ArchiveError::WriteArchive {
        source: std::io::Error::other(e.to_string()),
    })??;

    // Write the tar data to the async writer
    let mut writer = writer;
    writer
        .write_all(&tar_data)
        .await
        .map_err(|source| ArchiveError::WriteArchive { source })?;
    writer
        .flush()
        .await
        .map_err(|source| ArchiveError::WriteArchive { source })?;

    Ok(total_bytes)
}

/// Extract a tar archive to a target directory.
///
/// # Arguments
///
/// * `reader` - An async reader containing the tar archive data
/// * `target_dir` - The directory to extract the archive into
///
/// # Errors
///
/// Returns an error if:
/// - The archive cannot be read
/// - Files cannot be extracted
/// - The target directory cannot be created
pub async fn extract_archive<R>(reader: R, target_dir: &Path) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    use tar::Archive;
    use tokio::io::AsyncReadExt;
    use tokio::task::spawn_blocking;

    // Read all archive data into memory
    let mut reader = reader;
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .map_err(|source| ArchiveError::ReadArchive { source })?;

    let target_dir = target_dir.to_path_buf();
    let target_dir_for_error = target_dir.clone();

    // Extract in a blocking task
    spawn_blocking(move || {
        let mut archive = Archive::new(&buffer[..]);

        // Ensure target directory exists
        std::fs::create_dir_all(&target_dir).map_err(|source| ArchiveError::ExtractArchive {
            path: target_dir.clone(),
            source,
        })?;

        // Extract all files
        archive
            .unpack(&target_dir)
            .map_err(|source| ArchiveError::ExtractArchive {
                path: target_dir.clone(),
                source,
            })?;

        Ok::<(), ArchiveError>(())
    })
    .await
    .map_err(|e| ArchiveError::ExtractArchive {
        path: target_dir_for_error,
        source: std::io::Error::other(e.to_string()),
    })??;

    Ok(())
}

/// Recursively add a directory and its contents to a tar archive.
fn add_directory_to_archive<W: std::io::Write>(
    archive: &mut tar::Builder<W>,
    dir_path: &Path,
    archive_prefix: &str,
) -> std::io::Result<()> {
    use std::fs;

    fn visit_dirs<W: std::io::Write>(
        archive: &mut tar::Builder<W>,
        dir: &Path,
        base_path: &Path,
        archive_prefix: &str,
    ) -> std::io::Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                let relative_path = path.strip_prefix(base_path).map_err(|_| {
                    std::io::Error::other(format!("Failed to strip prefix from {}", path.display()))
                })?;

                let archive_path = if archive_prefix.is_empty() {
                    relative_path.to_path_buf()
                } else {
                    PathBuf::from(archive_prefix).join(relative_path)
                };

                if path.is_dir() {
                    visit_dirs(archive, &path, base_path, archive_prefix)?;
                } else {
                    archive.append_path_with_name(&path, &archive_path)?;
                }
            }
        }
        Ok(())
    }

    visit_dirs(archive, dir_path, dir_path, archive_prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_archive_and_extract() -> Result<()> {
        // Create test directories
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let metadata_dir = test_dir.path().join("metadata");
        let data_dir = test_dir.path().join("data");

        std::fs::create_dir_all(&metadata_dir).expect("Failed to create metadata dir");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");

        // Create some test files
        std::fs::write(metadata_dir.join("schema.json"), b"{\"type\":\"test\"}")
            .expect("Failed to write schema");
        std::fs::write(data_dir.join("file1.vortex"), b"test data 1")
            .expect("Failed to write file1");
        std::fs::write(data_dir.join("file2.vortex"), b"test data 2")
            .expect("Failed to write file2");

        // Archive the directories
        let mut archive_buffer = Vec::new();
        let dirs = vec![
            (metadata_dir.clone(), "metadata/".to_string()),
            (data_dir.clone(), "data/".to_string()),
        ];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract to a new location
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer.clone()), extract_dir.path()).await?;

        // Verify extracted files
        let extracted_schema =
            std::fs::read_to_string(extract_dir.path().join("metadata/schema.json"))
                .expect("Failed to read extracted schema");
        assert_eq!(extracted_schema, "{\"type\":\"test\"}");

        let extracted_file1 = std::fs::read_to_string(extract_dir.path().join("data/file1.vortex"))
            .expect("Failed to read extracted file1");
        assert_eq!(extracted_file1, "test data 1");

        Ok(())
    }
}
