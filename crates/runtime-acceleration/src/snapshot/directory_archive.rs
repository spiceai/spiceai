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
        source: std::io::Error::other(e),
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

/// Options for controlling archive extraction behavior.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExtractOptions {
    /// If true, skip files that already exist on disk instead of overwriting them.
    /// This is useful for Cayenne snapshots where multiple datasets share a metadata
    /// directory - the first dataset's snapshot extracts the metadata, and subsequent
    /// datasets skip the metadata files because they already exist.
    pub skip_if_exists: bool,
}

impl ExtractOptions {
    /// Creates options with skip_if_exists enabled.
    #[must_use]
    pub fn skip_existing() -> Self {
        Self {
            skip_if_exists: true,
        }
    }
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
    extract_archive_with_options(reader, target_dir, ExtractOptions::default()).await
}

/// Extract a tar archive to a target directory with options.
///
/// # Arguments
///
/// * `reader` - An async reader containing the tar archive data
/// * `target_dir` - The directory to extract the archive into
/// * `options` - Options controlling extraction behavior
///
/// # Errors
///
/// Returns an error if:
/// - The archive cannot be read
/// - Files cannot be extracted
/// - The target directory cannot be created
pub async fn extract_archive_with_options<R>(
    reader: R,
    target_dir: &Path,
    options: ExtractOptions,
) -> Result<()>
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

        if options.skip_if_exists {
            // Custom extraction that skips existing files
            extract_with_skip_existing(&mut archive, &target_dir)?;
        } else {
            // Standard extraction that overwrites existing files
            archive
                .unpack(&target_dir)
                .map_err(|source| ArchiveError::ExtractArchive {
                    path: target_dir.clone(),
                    source,
                })?;
        }

        Ok::<(), ArchiveError>(())
    })
    .await
    .map_err(|e| ArchiveError::ExtractArchive {
        path: target_dir_for_error,
        source: std::io::Error::other(e),
    })??;

    Ok(())
}

/// Extract archive entries, skipping files that already exist.
fn extract_with_skip_existing<R: std::io::Read>(
    archive: &mut tar::Archive<R>,
    target_dir: &Path,
) -> Result<()> {
    use std::fs;
    use std::io::{Read, Write};

    for entry_result in archive.entries().map_err(|source| ArchiveError::ReadArchive { source })? {
        let mut entry = entry_result.map_err(|source| ArchiveError::ReadArchive { source })?;
        let entry_path = entry
            .path()
            .map_err(|source| ArchiveError::ReadArchive { source })?;
        let dest_path = target_dir.join(&entry_path);

        // Skip if the file already exists
        if dest_path.exists() {
            tracing::debug!(
                "Skipping existing file during archive extraction: {}",
                dest_path.display()
            );
            continue;
        }

        // Create parent directories if needed
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).map_err(|source| ArchiveError::ExtractArchive {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        // Check if entry is a directory or file
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            fs::create_dir_all(&dest_path).map_err(|source| ArchiveError::ExtractArchive {
                path: dest_path.clone(),
                source,
            })?;
        } else if entry_type.is_file() {
            // Extract file contents
            let mut file =
                fs::File::create(&dest_path).map_err(|source| ArchiveError::ExtractArchive {
                    path: dest_path.clone(),
                    source,
                })?;

            let mut contents = Vec::new();
            entry
                .read_to_end(&mut contents)
                .map_err(|source| ArchiveError::ReadArchive { source })?;

            file.write_all(&contents)
                .map_err(|source| ArchiveError::ExtractArchive {
                    path: dest_path.clone(),
                    source,
                })?;

            // Preserve file permissions if available
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(mode) = entry.header().mode() {
                    let permissions = fs::Permissions::from_mode(mode);
                    let _ = fs::set_permissions(&dest_path, permissions);
                }
            }
        }
        // Skip other entry types (symlinks, etc.) for now
    }

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

    #[tokio::test]
    async fn test_archive_empty_directory() -> Result<()> {
        // Create an empty directory
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let empty_dir = test_dir.path().join("empty");
        std::fs::create_dir_all(&empty_dir).expect("Failed to create empty dir");

        // Archive the empty directory
        let mut archive_buffer = Vec::new();
        let dirs = vec![(empty_dir.clone(), "empty/".to_string())];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        // Empty tar archive should still have some bytes (tar header)
        assert!(bytes_written > 0);

        // Extract and verify
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        // The empty directory structure should be extractable
        assert!(extract_dir.path().exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_archive_nested_directories() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let nested_dir = test_dir.path().join("level1/level2/level3");
        std::fs::create_dir_all(&nested_dir).expect("Failed to create nested dirs");

        // Create files at different nesting levels
        std::fs::write(test_dir.path().join("level1/file1.txt"), b"level 1 content")
            .expect("Failed to write file1");
        std::fs::write(
            test_dir.path().join("level1/level2/file2.txt"),
            b"level 2 content",
        )
        .expect("Failed to write file2");
        std::fs::write(
            test_dir.path().join("level1/level2/level3/file3.txt"),
            b"level 3 content",
        )
        .expect("Failed to write file3");

        // Archive the nested directory
        let mut archive_buffer = Vec::new();
        let dirs = vec![(test_dir.path().join("level1"), "data/".to_string())];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract and verify
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        // Verify nested files were preserved
        let file1_content =
            std::fs::read_to_string(extract_dir.path().join("data/file1.txt")).expect("read file1");
        assert_eq!(file1_content, "level 1 content");

        let file2_content =
            std::fs::read_to_string(extract_dir.path().join("data/level2/file2.txt"))
                .expect("read file2");
        assert_eq!(file2_content, "level 2 content");

        let file3_content =
            std::fs::read_to_string(extract_dir.path().join("data/level2/level3/file3.txt"))
                .expect("read file3");
        assert_eq!(file3_content, "level 3 content");

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_nonexistent_directory_skipped() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let existing_dir = test_dir.path().join("existing");
        std::fs::create_dir_all(&existing_dir).expect("Failed to create existing dir");
        std::fs::write(existing_dir.join("file.txt"), b"content").expect("Failed to write file");

        // Include a non-existent directory - should be skipped, not error
        let nonexistent_dir = test_dir.path().join("nonexistent");

        let mut archive_buffer = Vec::new();
        let dirs = vec![
            (nonexistent_dir.clone(), "missing/".to_string()),
            (existing_dir.clone(), "existing/".to_string()),
        ];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract and verify only existing dir content is present
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        assert!(extract_dir.path().join("existing/file.txt").exists());
        assert!(!extract_dir.path().join("missing").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_binary_files() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");

        // Create binary file with all byte values 0-255
        let binary_content: Vec<u8> = (0..=255).collect();
        std::fs::write(data_dir.join("binary.bin"), &binary_content)
            .expect("Failed to write binary file");

        // Create a file with null bytes
        let null_content = vec![0u8; 1024];
        std::fs::write(data_dir.join("nulls.bin"), &null_content)
            .expect("Failed to write null file");

        // Archive
        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), "data/".to_string())];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract and verify binary content is preserved exactly
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        let extracted_binary = std::fs::read(extract_dir.path().join("data/binary.bin"))
            .expect("Failed to read extracted binary");
        assert_eq!(extracted_binary, binary_content);

        let extracted_nulls = std::fs::read(extract_dir.path().join("data/nulls.bin"))
            .expect("Failed to read extracted nulls");
        assert_eq!(extracted_nulls, null_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_large_files() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");

        // Create a 10MB file
        let large_content = vec![0xABu8; 10 * 1024 * 1024];
        std::fs::write(data_dir.join("large.bin"), &large_content)
            .expect("Failed to write large file");

        // Archive
        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), "data/".to_string())];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written >= 10 * 1024 * 1024);

        // Extract and verify
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        let extracted_large = std::fs::read(extract_dir.path().join("data/large.bin"))
            .expect("Failed to read extracted large file");
        assert_eq!(extracted_large.len(), large_content.len());
        assert_eq!(extracted_large, large_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_multiple_directories_with_same_filenames() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let dir1 = test_dir.path().join("dir1");
        let dir2 = test_dir.path().join("dir2");
        std::fs::create_dir_all(&dir1).expect("Failed to create dir1");
        std::fs::create_dir_all(&dir2).expect("Failed to create dir2");

        // Create files with same names but different content
        std::fs::write(dir1.join("config.json"), b"{\"source\": \"dir1\"}")
            .expect("Failed to write dir1 config");
        std::fs::write(dir2.join("config.json"), b"{\"source\": \"dir2\"}")
            .expect("Failed to write dir2 config");

        // Archive with different prefixes
        let mut archive_buffer = Vec::new();
        let directory_list = vec![
            (dir1.clone(), "metadata/".to_string()),
            (dir2.clone(), "data/".to_string()),
        ];
        let bytes_written =
            archive_directories(&directory_list, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract and verify both files are preserved with correct content
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        let config1 = std::fs::read_to_string(extract_dir.path().join("metadata/config.json"))
            .expect("read config1");
        assert_eq!(config1, "{\"source\": \"dir1\"}");

        let config2 = std::fs::read_to_string(extract_dir.path().join("data/config.json"))
            .expect("read config2");
        assert_eq!(config2, "{\"source\": \"dir2\"}");

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_file_instead_of_directory_skipped() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let file_path = test_dir.path().join("notadir.txt");
        std::fs::write(&file_path, b"I am a file").expect("Failed to write file");

        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file.txt"), b"valid content").expect("write valid file");

        // Archive with a file path (should be skipped) and a valid directory
        let mut archive_buffer = Vec::new();
        let dirs = vec![
            (file_path.clone(), "file/".to_string()),
            (data_dir.clone(), "data/".to_string()),
        ];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract and verify only valid directory content is present
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        assert!(extract_dir.path().join("data/file.txt").exists());
        // The file prefix should not create a directory
        assert!(!extract_dir.path().join("file/notadir.txt").exists());

        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "Test data patterns are always within u8 range"
    )]
    async fn test_archive_preserves_file_content_integrity() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");

        // Create files with specific content patterns to verify integrity
        let patterns = vec![
            ("zeros.bin", vec![0u8; 4096]),
            ("ones.bin", vec![0xFFu8; 4096]),
            (
                "alternating.bin",
                (0..4096).map(|i| (i % 2) as u8).collect(),
            ),
            (
                "sequential.bin",
                (0..4096).map(|i| (i % 256) as u8).collect(),
            ),
        ];

        for (name, content) in &patterns {
            std::fs::write(data_dir.join(name), content).expect("Failed to write pattern file");
        }

        // Archive
        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), String::new())];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        // Extract
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        // Verify each file's content
        for (name, expected_content) in &patterns {
            let extracted = std::fs::read(extract_dir.path().join(name)).expect("read file");
            assert_eq!(
                &extracted, expected_content,
                "Content mismatch for file {name}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_empty_prefix() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file.txt"), b"content").expect("write file");

        // Archive with empty prefix - files should be at root of archive
        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), String::new())];
        let bytes_written = archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;
        assert!(bytes_written > 0);

        // Extract and verify file is at root
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        assert!(extract_dir.path().join("file.txt").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_skip_if_exists() -> Result<()> {
        // Create test directories and files for two "datasets" sharing metadata
        let test_dir = TempDir::new().expect("Failed to create temp dir");

        // Dataset 1: metadata + data1
        let metadata_dir = test_dir.path().join("ds1_metadata");
        let data1_dir = test_dir.path().join("ds1_data");
        std::fs::create_dir_all(&metadata_dir).expect("Failed to create metadata dir");
        std::fs::create_dir_all(&data1_dir).expect("Failed to create data1 dir");
        std::fs::write(metadata_dir.join("catalog.db"), b"shared_metadata")
            .expect("Failed to write catalog");
        std::fs::write(data1_dir.join("data1.vortex"), b"data1_content")
            .expect("Failed to write data1");

        // Archive dataset 1
        let mut archive1_buffer = Vec::new();
        let dirs1 = vec![
            (metadata_dir.clone(), "metadata/".to_string()),
            (data1_dir.clone(), "data/".to_string()),
        ];
        archive_directories(&dirs1, Cursor::new(&mut archive1_buffer)).await?;

        // Dataset 2: metadata + data2
        let metadata_dir2 = test_dir.path().join("ds2_metadata");
        let data2_dir = test_dir.path().join("ds2_data");
        std::fs::create_dir_all(&metadata_dir2).expect("Failed to create metadata2 dir");
        std::fs::create_dir_all(&data2_dir).expect("Failed to create data2 dir");
        std::fs::write(metadata_dir2.join("catalog.db"), b"metadata_from_ds2")
            .expect("Failed to write catalog2");
        std::fs::write(data2_dir.join("data2.vortex"), b"data2_content")
            .expect("Failed to write data2");

        // Archive dataset 2
        let mut archive2_buffer = Vec::new();
        let dirs2 = vec![
            (metadata_dir2.clone(), "metadata/".to_string()),
            (data2_dir.clone(), "data/".to_string()),
        ];
        archive_directories(&dirs2, Cursor::new(&mut archive2_buffer)).await?;

        // Extract both archives to the same target directory, using skip_if_exists
        let extract_dir = TempDir::new().expect("Failed to create extract dir");

        // Extract dataset 1 first (normal extraction)
        extract_archive_with_options(
            Cursor::new(archive1_buffer.clone()),
            extract_dir.path(),
            ExtractOptions::skip_existing(),
        )
        .await?;

        // Verify dataset 1 content
        let catalog = std::fs::read_to_string(extract_dir.path().join("metadata/catalog.db"))
            .expect("read catalog");
        assert_eq!(catalog, "shared_metadata");
        let data1 = std::fs::read_to_string(extract_dir.path().join("data/data1.vortex"))
            .expect("read data1");
        assert_eq!(data1, "data1_content");

        // Extract dataset 2 with skip_if_exists - metadata should be skipped
        extract_archive_with_options(
            Cursor::new(archive2_buffer),
            extract_dir.path(),
            ExtractOptions::skip_existing(),
        )
        .await?;

        // Verify metadata was NOT overwritten (still from dataset 1)
        let catalog_after =
            std::fs::read_to_string(extract_dir.path().join("metadata/catalog.db"))
                .expect("read catalog after");
        assert_eq!(
            catalog_after, "shared_metadata",
            "Metadata should not be overwritten when skip_if_exists is true"
        );

        // Verify dataset 2's data was extracted (to its own data path)
        let data2 = std::fs::read_to_string(extract_dir.path().join("data/data2.vortex"))
            .expect("read data2");
        assert_eq!(data2, "data2_content");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_without_skip_overwrites() -> Result<()> {
        // Test that default extraction DOES overwrite existing files
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file.txt"), b"original_content")
            .expect("Failed to write file");

        // Archive with new content
        std::fs::write(data_dir.join("file.txt"), b"new_content").expect("Failed to update file");

        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), "data/".to_string())];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        // Pre-create the file in extract dir with different content
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        std::fs::create_dir_all(extract_dir.path().join("data")).expect("create data dir");
        std::fs::write(
            extract_dir.path().join("data/file.txt"),
            b"pre_existing_content",
        )
        .expect("write pre-existing file");

        // Extract WITHOUT skip_if_exists (default) - should overwrite
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        let content = std::fs::read_to_string(extract_dir.path().join("data/file.txt"))
            .expect("read extracted file");
        assert_eq!(
            content, "new_content",
            "File should be overwritten without skip_if_exists"
        );

        Ok(())
    }
}
