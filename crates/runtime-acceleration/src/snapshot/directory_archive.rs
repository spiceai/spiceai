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

use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    path::{Component, Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};

/// Global lock map for coordinating concurrent extractions to shared directories.
/// This prevents race conditions when multiple Cayenne datasets try to extract
/// snapshots to the same metadata directory simultaneously.
static DIRECTORY_EXTRACTION_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedPrefixMapping {
    prefix: PathBuf,
    target_dir: PathBuf,
}

fn normalize_prefix_mappings(
    mappings: Option<&Vec<(String, PathBuf)>>,
) -> Result<Vec<NormalizedPrefixMapping>> {
    let Some(mappings) = mappings else {
        return Ok(Vec::new());
    };
    let mut normalized = mappings
        .iter()
        .map(|(prefix, target_dir)| {
            Ok(NormalizedPrefixMapping {
                prefix: validate_archive_relative_path(Path::new(prefix))?,
                target_dir: target_dir.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Longest segment prefix wins. Lexical tie-breakers make the result stable
    // regardless of caller-provided mapping order.
    normalized.sort_by(|left, right| {
        right
            .prefix
            .components()
            .count()
            .cmp(&left.prefix.components().count())
            .then_with(|| left.prefix.cmp(&right.prefix))
            .then_with(|| left.target_dir.cmp(&right.target_dir))
    });
    for pair in normalized.windows(2) {
        if pair[0].prefix == pair[1].prefix && pair[0].target_dir != pair[1].target_dir {
            return Err(ArchiveError::UnsafeArchivePath {
                path: pair[0].prefix.clone(),
                reason: "duplicate archive prefix maps to multiple destination directories"
                    .to_string(),
            });
        }
    }
    normalized.dedup();
    Ok(normalized)
}

async fn canonical_lock_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| ArchiveError::ExtractArchive {
                path: path.to_path_buf(),
                source,
            })?
            .join(path)
    };
    let mut probe = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match tokio::fs::canonicalize(probe).await {
            Ok(mut canonical) => {
                for component in missing.iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = probe.file_name() else {
                    return Err(ArchiveError::ExtractArchive {
                        path: absolute,
                        source: error,
                    });
                };
                missing.push(name.to_os_string());
                let Some(parent) = probe.parent() else {
                    return Err(ArchiveError::ExtractArchive {
                        path: absolute,
                        source: error,
                    });
                };
                probe = parent;
            }
            Err(source) => {
                return Err(ArchiveError::ExtractArchive {
                    path: absolute,
                    source,
                });
            }
        }
    }
}

/// Acquire every destination-root lock in canonical lexical order. Holding all
/// roots prevents two archives with different default targets but a shared
/// prefix mapping from concurrently mutating that mapped directory.
async fn acquire_extraction_locks(
    target_dir: &Path,
    mappings: &[NormalizedPrefixMapping],
) -> Result<Vec<tokio::sync::OwnedMutexGuard<()>>> {
    let mut roots = Vec::with_capacity(mappings.len() + 1);
    roots.push(canonical_lock_path(target_dir).await?);
    for mapping in mappings {
        roots.push(canonical_lock_path(&mapping.target_dir).await?);
    }
    roots.sort();
    roots.dedup();

    let lock_handles = {
        let mut locks = DIRECTORY_EXTRACTION_LOCKS.lock().await;
        locks.retain(|_, lock| lock.upgrade().is_some());
        roots
            .into_iter()
            .map(|root| {
                if let Some(lock) = locks.get(&root).and_then(Weak::upgrade) {
                    lock
                } else {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(root, Arc::downgrade(&lock));
                    lock
                }
            })
            .collect::<Vec<_>>()
    };

    let mut guards = Vec::with_capacity(lock_handles.len());
    for lock in lock_handles {
        guards.push(lock.lock_owned().await);
    }
    Ok(guards)
}

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

    #[snafu(display(
        "Data integrity violation: existing file {} has checksum {actual_checksum}, expected {expected_checksum}. \
         This may indicate data corruption or concurrent modification.",
        path.display()
    ))]
    ChecksumMismatch {
        path: PathBuf,
        expected_checksum: String,
        actual_checksum: String,
    },

    #[snafu(display("Unsafe archive path {}: {reason}", path.display()))]
    UnsafeArchivePath { path: PathBuf, reason: String },
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
    archive_directories_with_plan(dirs, writer, &[], &[]).await
}

/// Like [`archive_directories`], but allows excluding files (`skip_relative_paths`,
/// matched against each entry's path *relative to its source directory*) and
/// appending extra in-memory entries (`extras`) at the end of the tar.
///
/// `extras[i]` is added to the archive with `archive_path = extras[i].0` and
/// `bytes = extras[i].1`. The bytes count toward the returned total.
///
/// # Errors
///
/// Returns an error if writing the tar archive fails (I/O error on `writer`,
/// missing source directory, or any of the recursive walks/append calls
/// against `dirs` or `extras` fail).
pub async fn archive_directories_with_plan<W>(
    dirs: &[(PathBuf, String)],
    writer: W,
    skip_relative_paths: &[PathBuf],
    extras: &[(String, Vec<u8>)],
) -> Result<u64>
where
    W: AsyncWrite + Unpin + Send,
{
    use tar::Builder;
    use tokio::io::AsyncWriteExt;
    use tokio::task::spawn_blocking;

    let dirs = dirs.to_vec();
    let skip: HashSet<PathBuf> = skip_relative_paths.iter().cloned().collect();
    let extras = extras.to_vec();

    // Use spawn_blocking since tar operations are synchronous
    let (total_bytes, tar_data) = spawn_blocking(move || {
        let mut buffer = Vec::new();
        {
            let mut archive = Builder::new(&mut buffer);

            for (dir_path, archive_prefix) in &dirs {
                let metadata = match std::fs::symlink_metadata(dir_path) {
                    Ok(metadata) => metadata,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        tracing::warn!("Directory {} does not exist, skipping", dir_path.display());
                        continue;
                    }
                    Err(source) => {
                        return Err(ArchiveError::CreateArchive {
                            path: dir_path.clone(),
                            source,
                        });
                    }
                };

                if metadata.file_type().is_symlink() {
                    tracing::warn!(
                        "Directory {} is a symbolic link, skipping",
                        dir_path.display()
                    );
                    continue;
                }

                if !metadata.is_dir() {
                    tracing::warn!("{} is not a directory, skipping", dir_path.display());
                    continue;
                }

                // Add all files from this directory recursively
                let mut members: HashSet<String> = HashSet::new();
                add_directory_to_archive_filtered(
                    &mut archive,
                    dir_path,
                    archive_prefix,
                    &skip,
                    &mut members,
                )
                .map_err(|e| ArchiveError::CreateArchive {
                    path: dir_path.clone(),
                    source: e,
                })?;
            }

            // Append in-memory extras after the on-disk content.
            for (archive_path, bytes) in &extras {
                let mut header = tar::Header::new_gnu();
                header.set_size(bytes.len() as u64);
                header.set_mode(0o644);
                header.set_entry_type(tar::EntryType::Regular);
                header.set_mtime(0);
                archive
                    .append_data(&mut header, archive_path.as_str(), bytes.as_slice())
                    .map_err(|source| ArchiveError::WriteArchive { source })?;
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

/// Archive directories directly into a filesystem file without materializing
/// the tar payload in memory.
///
/// # Errors
/// Returns an error when the destination file or any archive input cannot be
/// read or written.
/// What an archive run produced: its size, and the set of paths actually written into it.
///
/// The member set is what lets a caller verify the archive it is about to publish, rather
/// than re-inspecting the filesystem the archive was built from — which answers a
/// different question, and answers it wrong whenever the two disagree (a skipped symlink,
/// a path outside the archived directories, a file recreated after the walk passed it).
#[derive(Debug, Default)]
pub struct ArchiveOutcome {
    pub bytes: u64,
    pub members: HashSet<String>,
}

/// Archive directories directly into a filesystem file without materializing the tar
/// payload in memory, reporting what was written.
///
/// # Errors
/// Returns an error when the destination file or any archive input cannot be read or
/// written.
pub async fn archive_directories_to_file_with_plan(
    dirs: &[(PathBuf, String)],
    destination: &Path,
    skip_relative_paths: &[PathBuf],
    extras: &[(String, Vec<u8>)],
) -> Result<ArchiveOutcome> {
    let dirs = dirs.to_vec();
    let destination = destination.to_path_buf();
    let skip: HashSet<PathBuf> = skip_relative_paths.iter().cloned().collect();
    let extras = extras.to_vec();

    tokio::task::spawn_blocking(move || {
        let file =
            std::fs::File::create(&destination).map_err(|source| ArchiveError::CreateArchive {
                path: destination.clone(),
                source,
            })?;
        let mut archive = tar::Builder::new(file);
        let mut members: HashSet<String> = HashSet::new();

        for (dir_path, archive_prefix) in &dirs {
            let metadata = match std::fs::symlink_metadata(dir_path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!("Directory {} does not exist, skipping", dir_path.display());
                    continue;
                }
                Err(source) => {
                    return Err(ArchiveError::CreateArchive {
                        path: dir_path.clone(),
                        source,
                    });
                }
            };
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                tracing::warn!(
                    "Directory {} is not archivable, skipping",
                    dir_path.display()
                );
                continue;
            }
            add_directory_to_archive_filtered(
                &mut archive,
                dir_path,
                archive_prefix,
                &skip,
                &mut members,
            )
            .map_err(|source| ArchiveError::CreateArchive {
                path: dir_path.clone(),
                source,
            })?;
        }

        for (archive_path, bytes) in &extras {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mtime(0);
            archive
                .append_data(&mut header, archive_path.as_str(), bytes.as_slice())
                .map_err(|source| ArchiveError::WriteArchive { source })?;
            members.insert(archive_path.clone());
        }
        archive
            .finish()
            .map_err(|source| ArchiveError::WriteArchive { source })?;
        let mut file = archive
            .into_inner()
            .map_err(|source| ArchiveError::WriteArchive { source })?;
        std::io::Write::flush(&mut file).map_err(|source| ArchiveError::WriteArchive { source })?;
        file.metadata()
            .map(|metadata| ArchiveOutcome {
                bytes: metadata.len(),
                members,
            })
            .map_err(|source| ArchiveError::CreateArchive {
                path: destination,
                source,
            })
    })
    .await
    .map_err(|source| ArchiveError::WriteArchive {
        source: std::io::Error::other(source),
    })?
}

/// Options for controlling archive extraction behavior.
#[derive(Debug, Clone, Default)]
pub struct ExtractOptions {
    /// If true, skip files that already exist on disk instead of overwriting them.
    /// This is useful for Cayenne snapshots where multiple datasets share a metadata
    /// directory - the first dataset's snapshot extracts the metadata, and subsequent
    /// datasets skip the metadata files because they already exist.
    pub skip_if_exists: bool,

    /// If true, verify checksums of existing files when `skip_if_exists` is enabled.
    /// If a file exists but its checksum doesn't match the expected value in the archive,
    /// an error is returned to indicate data corruption.
    pub verify_existing_checksums: bool,

    /// Expected checksums for files, keyed by their archive path.
    /// If provided and `verify_existing_checksums` is true, existing files are verified
    /// against these checksums.
    pub expected_checksums: Option<HashMap<String, String>>,

    /// Mapping from archive prefixes to target directories.
    /// When set, archive entries starting with a prefix will be extracted to the
    /// corresponding target directory instead of `target_dir`.
    ///
    /// For example, if the mapping contains `("data/", "/spice/data/my_table")`,
    /// an archive entry `data/file.parquet` will be extracted to
    /// `/spice/data/my_table/file.parquet` instead of `{target_dir}/data/file.parquet`.
    pub prefix_mappings: Option<Vec<(String, PathBuf)>>,
}

impl ExtractOptions {
    /// Creates options with `skip_if_exists` enabled and checksum verification enabled.
    /// This is the recommended option for Cayenne snapshot extraction where data
    /// integrity is critical.
    #[must_use]
    pub fn skip_existing() -> Self {
        Self {
            skip_if_exists: true,
            verify_existing_checksums: true,
            expected_checksums: None, // Checksums will be computed from archive contents
            prefix_mappings: None,
        }
    }

    /// Creates options with `skip_if_exists` enabled but checksum verification disabled.
    /// Use with caution - this trades off safety for performance.
    #[must_use]
    pub fn skip_existing_no_verify() -> Self {
        Self {
            skip_if_exists: true,
            verify_existing_checksums: false,
            expected_checksums: None,
            prefix_mappings: None,
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
/// This function coordinates access to shared directories using internal locks
/// to prevent race conditions when multiple Cayenne datasets extract to the same
/// metadata directory.
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
/// - Checksum verification fails (if enabled)
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

    let normalized_mappings = normalize_prefix_mappings(options.prefix_mappings.as_ref())?;
    let _lock_guards = acquire_extraction_locks(target_dir, &normalized_mappings).await?;

    tracing::debug!(
        "Acquired extraction locks for directory roots including: {}",
        target_dir.display()
    );

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

        extract_with_skip_existing_and_verify(
            &mut archive,
            &target_dir,
            &options,
            &normalized_mappings,
        )?;

        Ok::<(), ArchiveError>(())
    })
    .await
    .map_err(|e| ArchiveError::ExtractArchive {
        path: target_dir_for_error,
        source: std::io::Error::other(e),
    })??;

    Ok(())
}

/// Extract an archive directly from a filesystem file without first reading the
/// entire archive into memory.
///
/// # Errors
/// Returns an error if the archive cannot be opened, validated, or extracted.
pub async fn extract_archive_file_with_options(
    archive_path: &Path,
    target_dir: &Path,
    options: ExtractOptions,
) -> Result<()> {
    let normalized_mappings = normalize_prefix_mappings(options.prefix_mappings.as_ref())?;
    let _lock_guards = acquire_extraction_locks(target_dir, &normalized_mappings).await?;
    let archive_path = archive_path.to_path_buf();
    let target_dir = target_dir.to_path_buf();
    let target_dir_for_error = target_dir.clone();

    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&archive_path)
            .map_err(|source| ArchiveError::ReadArchive { source })?;
        let mut archive = tar::Archive::new(file);
        std::fs::create_dir_all(&target_dir).map_err(|source| ArchiveError::ExtractArchive {
            path: target_dir.clone(),
            source,
        })?;
        extract_with_skip_existing_and_verify(
            &mut archive,
            &target_dir,
            &options,
            &normalized_mappings,
        )
    })
    .await
    .map_err(|source| ArchiveError::ExtractArchive {
        path: target_dir_for_error,
        source: std::io::Error::other(source),
    })??;

    Ok(())
}

/// Compute SHA-256 checksum of a file and return as hex string.
fn compute_file_checksum(path: &Path) -> Result<String> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path).map_err(|source| ArchiveError::ExtractArchive {
        path: path.to_path_buf(),
        source,
    })?;

    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024]; // 64KB heap-allocated buffer

    loop {
        let bytes_read = file
            .read(&mut buffer)
            .map_err(|source| ArchiveError::ExtractArchive {
                path: path.to_path_buf(),
                source,
            })?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);
    }

    let digest = hasher.finalize();
    Ok(hex_encode(&digest))
}

fn compute_reader_checksum<R: std::io::Read>(reader: &mut R) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .map_err(|source| ArchiveError::ReadArchive { source })?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hex_encode(&hasher.finalize()))
}

fn copy_reader_to_writer<R: std::io::Read, W: std::io::Write>(
    reader: &mut R,
    writer: &mut W,
    write_path: &Path,
) -> Result<u64> {
    std::io::copy(reader, writer).map_err(|source| ArchiveError::ExtractArchive {
        path: write_path.to_path_buf(),
        source,
    })
}

/// Encode bytes as lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Remap an archive entry path to its destination path using prefix mappings.
///
/// If `prefix_mappings` is provided and the entry path starts with one of the prefixes,
/// the prefix is replaced with the corresponding target directory. Otherwise, the entry
/// is extracted to `default_target_dir`.
fn validate_archive_relative_path(path: &Path) -> Result<PathBuf> {
    let mut sanitized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Normal(part) => sanitized.push(part),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(ArchiveError::UnsafeArchivePath {
                    path: path.to_path_buf(),
                    reason: "archive entries must be relative paths without parent components"
                        .to_string(),
                });
            }
        }
    }

    if sanitized.as_os_str().is_empty() {
        return Err(ArchiveError::UnsafeArchivePath {
            path: path.to_path_buf(),
            reason: "archive entry path is empty".to_string(),
        });
    }

    Ok(sanitized)
}

fn safe_destination_path(target_dir: &Path, relative_path: &Path) -> Result<PathBuf> {
    let relative_path = validate_archive_relative_path(relative_path)?;
    let dest_path = target_dir.join(relative_path);
    if !dest_path.starts_with(target_dir) {
        return Err(ArchiveError::UnsafeArchivePath {
            path: dest_path,
            reason: format!(
                "archive entry escapes target directory {}",
                target_dir.display()
            ),
        });
    }
    Ok(dest_path)
}

fn ensure_no_symlink_ancestors(path: &Path, root: &Path) -> Result<()> {
    use std::fs;

    if !path.starts_with(root) {
        return Err(ArchiveError::UnsafeArchivePath {
            path: path.to_path_buf(),
            reason: format!("path is outside target directory {}", root.display()),
        });
    }

    let relative_path = path
        .strip_prefix(root)
        .map_err(|_| ArchiveError::UnsafeArchivePath {
            path: path.to_path_buf(),
            reason: format!("path is outside target directory {}", root.display()),
        })?;

    let mut current = root.to_path_buf();
    if let Ok(metadata) = fs::symlink_metadata(&current)
        && metadata.file_type().is_symlink()
    {
        return Err(ArchiveError::UnsafeArchivePath {
            path: current,
            reason: "target directory is a symbolic link".to_string(),
        });
    }

    for component in relative_path.components() {
        current.push(component.as_os_str());

        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ArchiveError::UnsafeArchivePath {
                    path: current,
                    reason: "archive extraction would traverse a symbolic link".to_string(),
                });
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(ArchiveError::ExtractArchive {
                    path: current,
                    source,
                });
            }
        }

        if current == path {
            break;
        }
    }

    Ok(())
}

fn ensure_existing_destination_is_safe(dest_path: &Path, target_dir: &Path) -> Result<()> {
    use std::fs;

    ensure_no_symlink_ancestors(dest_path, target_dir)?;
    if let Ok(metadata) = fs::symlink_metadata(dest_path)
        && metadata.file_type().is_symlink()
    {
        return Err(ArchiveError::UnsafeArchivePath {
            path: dest_path.to_path_buf(),
            reason: "archive extraction would overwrite or read through a symbolic link"
                .to_string(),
        });
    }

    Ok(())
}

fn create_new_temp_file(temp_path: &Path, target_dir: &Path) -> Result<std::fs::File> {
    use std::fs;

    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)
    {
        Ok(file) => Ok(file),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure_existing_destination_is_safe(temp_path, target_dir)?;
            if temp_path.is_dir() {
                return Err(ArchiveError::UnsafeArchivePath {
                    path: temp_path.to_path_buf(),
                    reason: "archive temporary path conflicts with an existing directory"
                        .to_string(),
                });
            }
            fs::remove_file(temp_path).map_err(|source| ArchiveError::ExtractArchive {
                path: temp_path.to_path_buf(),
                source,
            })?;
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(temp_path)
                .map_err(|source| ArchiveError::ExtractArchive {
                    path: temp_path.to_path_buf(),
                    source,
                })
        }
        Err(source) => Err(ArchiveError::ExtractArchive {
            path: temp_path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(unix)]
fn set_archive_permissions(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    // Never restore setuid/setgid/sticky bits from an external archive.
    let permissions = std::fs::Permissions::from_mode(mode & 0o777);
    std::fs::set_permissions(path, permissions).map_err(|source| ArchiveError::ExtractArchive {
        path: path.to_path_buf(),
        source,
    })
}

fn remap_entry_path(
    entry_path: &Path,
    default_target_dir: &Path,
    prefix_mappings: &[NormalizedPrefixMapping],
) -> Result<(PathBuf, PathBuf)> {
    let entry_path = validate_archive_relative_path(entry_path)?;
    for mapping in prefix_mappings {
        if entry_path.starts_with(&mapping.prefix) {
            let relative = entry_path.strip_prefix(&mapping.prefix).map_err(|_| {
                ArchiveError::UnsafeArchivePath {
                    path: entry_path.clone(),
                    reason: "failed to strip a matched archive prefix".to_string(),
                }
            })?;
            let dest_path = if relative.as_os_str().is_empty() {
                mapping.target_dir.clone()
            } else {
                safe_destination_path(&mapping.target_dir, relative)?
            };
            return Ok((dest_path, mapping.target_dir.clone()));
        }
    }

    // No matching prefix, use default target directory
    let dest_path = safe_destination_path(default_target_dir, &entry_path)?;
    Ok((dest_path, default_target_dir.to_path_buf()))
}

/// Extract archive entries, skipping files that already exist with optional checksum verification.
///
/// When `options.verify_existing_checksums` is enabled, this function computes the checksum
/// of existing files and compares them against the expected checksum from the archive contents.
/// This ensures data integrity by detecting file corruption or unintended modifications.
fn extract_with_skip_existing_and_verify<R: std::io::Read>(
    archive: &mut tar::Archive<R>,
    target_dir: &Path,
    options: &ExtractOptions,
    prefix_mappings: &[NormalizedPrefixMapping],
) -> Result<()> {
    use std::fs;

    for entry_result in archive
        .entries()
        .map_err(|source| ArchiveError::ReadArchive { source })?
    {
        let mut entry = entry_result.map_err(|source| ArchiveError::ReadArchive { source })?;
        let entry_path = entry
            .path()
            .map_err(|source| ArchiveError::ReadArchive { source })?;
        let (dest_path, dest_root) = remap_entry_path(&entry_path, target_dir, prefix_mappings)?;

        let entry_type = entry.header().entry_type();
        #[cfg(unix)]
        let entry_mode = entry.header().mode().ok();

        if !entry_type.is_dir() && !entry_type.is_file() {
            tracing::debug!(
                "Skipping unsupported archive entry type: {}",
                entry_path.display()
            );
            continue;
        }

        // Handle existing files
        if dest_path.exists() {
            ensure_existing_destination_is_safe(&dest_path, &dest_root)?;
            if entry_type.is_dir() {
                if !dest_path.is_dir() {
                    return Err(ArchiveError::UnsafeArchivePath {
                        path: dest_path,
                        reason: "archive directory entry conflicts with an existing non-directory"
                            .to_string(),
                    });
                }
                #[cfg(unix)]
                if !options.skip_if_exists
                    && let Some(mode) = entry_mode
                {
                    set_archive_permissions(&dest_path, mode)?;
                }
                continue;
            }

            if dest_path.is_dir() {
                return Err(ArchiveError::UnsafeArchivePath {
                    path: dest_path,
                    reason: "archive file entry conflicts with an existing directory".to_string(),
                });
            }

            if options.skip_if_exists && options.verify_existing_checksums {
                let expected_checksum = options
                    .expected_checksums
                    .as_ref()
                    .and_then(|expected_checksums| {
                        expected_checksums.get(entry_path.to_string_lossy().as_ref())
                    })
                    .cloned()
                    .map_or_else(|| compute_reader_checksum(&mut entry), Ok)?;

                // Compute checksum of existing file
                let actual_checksum = compute_file_checksum(&dest_path)?;

                if actual_checksum != expected_checksum {
                    return Err(ArchiveError::ChecksumMismatch {
                        path: dest_path,
                        expected_checksum,
                        actual_checksum,
                    });
                }

                tracing::debug!(
                    "Verified checksum of existing file: {} (SHA-256: {})",
                    dest_path.display(),
                    &actual_checksum[..16] // Log first 16 chars of checksum
                );
            } else if options.skip_if_exists {
                tracing::debug!(
                    "Skipping existing file during archive extraction: {}",
                    dest_path.display()
                );
                continue;
            }

            if options.skip_if_exists {
                continue;
            }
        }

        // Create parent directories if needed
        if let Some(parent) = dest_path.parent() {
            ensure_no_symlink_ancestors(parent, &dest_root)?;
            fs::create_dir_all(parent).map_err(|source| ArchiveError::ExtractArchive {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        // Handle by entry type
        if entry_type.is_dir() {
            ensure_no_symlink_ancestors(&dest_path, &dest_root)?;
            fs::create_dir_all(&dest_path).map_err(|source| ArchiveError::ExtractArchive {
                path: dest_path.clone(),
                source,
            })?;
            #[cfg(unix)]
            if let Some(mode) = entry_mode {
                set_archive_permissions(&dest_path, mode)?;
            }
        } else if entry_type.is_file() {
            // Write to file atomically by writing to temp file then renaming
            let temp_path = dest_path.with_extension(format!("{}.tmp", std::process::id()));
            let bytes_written;

            {
                let mut file = create_new_temp_file(&temp_path, &dest_root)?;

                bytes_written = copy_reader_to_writer(&mut entry, &mut file, &temp_path)?;

                file.sync_all()
                    .map_err(|source| ArchiveError::ExtractArchive {
                        path: temp_path.clone(),
                        source,
                    })?;
            }

            #[cfg(windows)]
            if dest_path.exists() {
                ensure_existing_destination_is_safe(&dest_path, &dest_root)?;
                if dest_path.is_dir() {
                    let _ = fs::remove_file(&temp_path);
                    return Err(ArchiveError::UnsafeArchivePath {
                        path: dest_path,
                        reason: "archive file entry conflicts with an existing directory"
                            .to_string(),
                    });
                }
                fs::remove_file(&dest_path).map_err(|source| {
                    let _ = fs::remove_file(&temp_path);
                    ArchiveError::ExtractArchive {
                        path: dest_path.clone(),
                        source,
                    }
                })?;
            }

            // Atomic rename
            fs::rename(&temp_path, &dest_path).map_err(|source| {
                // Cleanup temp file on rename failure
                let _ = fs::remove_file(&temp_path);
                ArchiveError::ExtractArchive {
                    path: dest_path.clone(),
                    source,
                }
            })?;

            #[cfg(unix)]
            if let Some(parent) = dest_path.parent() {
                let parent_dir =
                    fs::File::open(parent).map_err(|source| ArchiveError::ExtractArchive {
                        path: parent.to_path_buf(),
                        source,
                    })?;
                parent_dir
                    .sync_all()
                    .map_err(|source| ArchiveError::ExtractArchive {
                        path: parent.to_path_buf(),
                        source,
                    })?;
            }

            #[cfg(unix)]
            if let Some(mode) = entry_mode {
                set_archive_permissions(&dest_path, mode)?;
            }

            tracing::trace!(
                "Extracted file: {} ({} bytes)",
                dest_path.display(),
                bytes_written
            );
        }
    }

    Ok(())
}

/// Walks `dir_path` recursively and appends each file to `archive` under
/// `archive_prefix`, skipping any file whose path *relative to `dir_path`*
/// is contained in `skip_relative_paths`.
fn add_directory_to_archive_filtered<W: std::io::Write>(
    archive: &mut tar::Builder<W>,
    dir_path: &Path,
    archive_prefix: &str,
    skip_relative_paths: &HashSet<PathBuf>,
    members: &mut HashSet<String>,
) -> std::io::Result<()> {
    use std::fs;

    fn visit_dirs<W: std::io::Write>(
        archive: &mut tar::Builder<W>,
        dir: &Path,
        base_path: &Path,
        archive_prefix: &str,
        skip_relative_paths: &HashSet<PathBuf>,
        members: &mut HashSet<String>,
    ) -> std::io::Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                let relative_path = path.strip_prefix(base_path).map_err(|_| {
                    std::io::Error::other(format!("Failed to strip prefix from {}", path.display()))
                })?;

                if skip_relative_paths.contains(relative_path) {
                    tracing::debug!(
                        "Skipping {} during archive creation (engine plan)",
                        path.display()
                    );
                    continue;
                }

                let archive_path = if archive_prefix.is_empty() {
                    relative_path.to_path_buf()
                } else {
                    PathBuf::from(archive_prefix).join(relative_path)
                };

                let metadata = fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() {
                    tracing::debug!(
                        "Skipping symbolic link during archive creation: {}",
                        path.display()
                    );
                    continue;
                }

                if metadata.is_dir() {
                    visit_dirs(
                        archive,
                        &path,
                        base_path,
                        archive_prefix,
                        skip_relative_paths,
                        members,
                    )?;
                } else if metadata.is_file() {
                    archive.append_path_with_name(&path, &archive_path)?;
                    members.insert(archive_path.to_string_lossy().into_owned());
                }
            }
        }
        Ok(())
    }

    visit_dirs(
        archive,
        dir_path,
        dir_path,
        archive_prefix,
        skip_relative_paths,
        members,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
    use tar::Header;
    use tempfile::TempDir;

    fn archive_with_file(path: &str, contents: &[u8]) -> Vec<u8> {
        archive_with_file_mode(path, contents, 0o644)
    }

    fn archive_with_file_mode(path: &str, contents: &[u8], mode: u32) -> Vec<u8> {
        let mut buffer = Vec::new();

        let mut header = Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(mode);
        header.set_entry_type(tar::EntryType::Regular);
        let path_bytes = path.as_bytes();
        assert!(
            path_bytes.len() <= 100,
            "test archive path must fit in tar name field"
        );
        header.as_mut_bytes()[..path_bytes.len()].copy_from_slice(path_bytes);
        header.set_cksum();

        buffer
            .write_all(header.as_bytes())
            .expect("archive header should be written");
        buffer
            .write_all(contents)
            .expect("archive contents should be written");
        let padding_len = (512 - (contents.len() % 512)) % 512;
        buffer.extend(std::iter::repeat_n(0, padding_len));
        buffer.extend(std::iter::repeat_n(0, 1024));

        buffer
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_extract_preserves_file_permissions() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let archive_buffer = archive_with_file_mode("data/executable.sh", b"#!/bin/sh\n", 0o755);

        extract_archive(Cursor::new(archive_buffer), test_dir.path()).await?;

        let extracted_file = test_dir.path().join("data/executable.sh");
        let mode = std::fs::metadata(&extracted_file)
            .expect("extracted file metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755);

        Ok(())
    }

    /// Regression: setuid/setgid/sticky bits from an archived entry must NOT be
    /// restored on extract (a malicious snapshot must not drop a setuid binary);
    /// the ordinary permission bits are preserved.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_extract_strips_setuid_setgid_sticky_bits() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let test_dir = TempDir::new().expect("Failed to create temp dir");
        // 0o7755 = setuid + setgid + sticky + rwxr-xr-x.
        let archive_buffer = archive_with_file_mode("data/suid.sh", b"#!/bin/sh\n", 0o7755);

        extract_archive(Cursor::new(archive_buffer), test_dir.path()).await?;

        let full_mode = std::fs::metadata(test_dir.path().join("data/suid.sh"))
            .expect("extracted file metadata should be readable")
            .permissions()
            .mode();
        assert_eq!(
            full_mode & 0o7000,
            0,
            "setuid/setgid/sticky bits must be stripped on extract"
        );
        assert_eq!(
            full_mode & 0o777,
            0o755,
            "ordinary permission bits must be preserved"
        );

        Ok(())
    }

    /// Regression: the streaming (never-fully-in-memory) archive/extract APIs
    /// used for large snapshots must round-trip directory contents and `extras`.
    #[tokio::test]
    async fn test_archive_to_file_and_extract_from_file_round_trip() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file1.vortex"), b"stream data 1").expect("write file1");
        std::fs::write(data_dir.join("file2.vortex"), b"stream data 2").expect("write file2");

        let archive_path = test_dir.path().join("snapshot.tar");
        let dirs = vec![(data_dir.clone(), "data/".to_string())];
        let extras = vec![("meta.json".to_string(), b"{\"v\":1}".to_vec())];
        let archived =
            archive_directories_to_file_with_plan(&dirs, &archive_path, &[], &extras).await?;
        assert!(archived.bytes > 0);
        assert!(archive_path.exists());
        // The member list is what a caller verifies the archive against, so it must name
        // both the walked file and the in-memory extra.
        assert!(
            archived.members.contains("meta.json"),
            "extras are archive members too: {:?}",
            archived.members
        );
        assert!(
            archived.members.iter().any(|m| m.starts_with("data/")),
            "walked files are reported under their archive prefix: {:?}",
            archived.members
        );

        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive_file_with_options(
            &archive_path,
            extract_dir.path(),
            ExtractOptions::default(),
        )
        .await?;

        assert_eq!(
            std::fs::read_to_string(extract_dir.path().join("data/file1.vortex"))
                .expect("read extracted file1"),
            "stream data 1"
        );
        assert_eq!(
            std::fs::read_to_string(extract_dir.path().join("data/file2.vortex"))
                .expect("read extracted file2"),
            "stream data 2"
        );
        assert_eq!(
            std::fs::read_to_string(extract_dir.path().join("meta.json"))
                .expect("read extracted extra"),
            "{\"v\":1}"
        );

        Ok(())
    }

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
    async fn test_extract_rejects_parent_path_traversal() {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let extract_dir = test_dir.path().join("extract");
        std::fs::create_dir_all(&extract_dir).expect("Failed to create extract dir");

        let archive_buffer = archive_with_file("../outside.txt", b"owned");
        let result = extract_archive(Cursor::new(archive_buffer), &extract_dir).await;

        assert!(matches!(
            result,
            Err(ArchiveError::UnsafeArchivePath { .. })
        ));
        assert!(!test_dir.path().join("outside.txt").exists());
    }

    #[tokio::test]
    async fn test_extract_rejects_prefix_mapping_traversal() {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let extract_dir = test_dir.path().join("extract");
        let mapped_dir = test_dir.path().join("mapped");
        std::fs::create_dir_all(&extract_dir).expect("Failed to create extract dir");
        std::fs::create_dir_all(&mapped_dir).expect("Failed to create mapped dir");

        let archive_buffer = archive_with_file("data/../outside.txt", b"owned");
        let options = ExtractOptions {
            prefix_mappings: Some(vec![("data/".to_string(), mapped_dir)]),
            ..Default::default()
        };

        let result =
            extract_archive_with_options(Cursor::new(archive_buffer), &extract_dir, options).await;

        assert!(matches!(
            result,
            Err(ArchiveError::UnsafeArchivePath { .. })
        ));
        assert!(!test_dir.path().join("outside.txt").exists());
    }

    #[tokio::test]
    async fn test_prefix_mapping_uses_longest_segment_prefix() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let default_dir = test_dir.path().join("default");
        let broad_dir = test_dir.path().join("broad");
        let specific_dir = test_dir.path().join("specific");
        let mappings = vec![
            ("data".to_string(), broad_dir),
            ("data/nested".to_string(), specific_dir.clone()),
        ];
        let normalized = normalize_prefix_mappings(Some(&mappings))?;

        let (specific_path, _) = remap_entry_path(
            Path::new("data/nested/file.vortex"),
            &default_dir,
            &normalized,
        )?;
        assert_eq!(specific_path, specific_dir.join("file.vortex"));

        // Segment-aware matching must not route `database/...` through `data`.
        let (default_path, _) =
            remap_entry_path(Path::new("database/file.vortex"), &default_dir, &normalized)?;
        assert_eq!(default_path, default_dir.join("database/file.vortex"));
        Ok(())
    }

    #[tokio::test]
    async fn test_extraction_lock_map_prunes_completed_roots() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let first = test_dir.path().join("first");
        let second = test_dir.path().join("second");
        let first_key = canonical_lock_path(&first).await?;
        let second_key = canonical_lock_path(&second).await?;

        let first_guards = acquire_extraction_locks(&first, &[]).await?;
        assert!(
            DIRECTORY_EXTRACTION_LOCKS
                .lock()
                .await
                .contains_key(&first_key)
        );
        drop(first_guards);

        let second_guards = acquire_extraction_locks(&second, &[]).await?;
        let locks = DIRECTORY_EXTRACTION_LOCKS.lock().await;
        assert!(!locks.contains_key(&first_key));
        assert!(
            locks
                .get(&second_key)
                .is_some_and(|lock| lock.upgrade().is_some())
        );
        drop(locks);
        drop(second_guards);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_archive_skips_symbolic_links() -> Result<()> {
        use std::os::unix::fs::symlink;

        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let source_dir = test_dir.path().join("source");
        let outside_dir = test_dir.path().join("outside");
        std::fs::create_dir_all(&source_dir).expect("Failed to create source dir");
        std::fs::create_dir_all(&outside_dir).expect("Failed to create outside dir");
        std::fs::write(outside_dir.join("secret.txt"), b"secret")
            .expect("Failed to write outside file");
        symlink(&outside_dir, source_dir.join("linked_outside")).expect("Failed to create symlink");

        let mut archive_buffer = Vec::new();
        let dirs = vec![(source_dir, "data/".to_string())];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        assert!(
            !extract_dir
                .path()
                .join("data/linked_outside/secret.txt")
                .exists()
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_extract_rejects_symlink_destination_directory() {
        use std::os::unix::fs::symlink;

        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let extract_dir = test_dir.path().join("extract");
        let outside_dir = test_dir.path().join("outside");
        std::fs::create_dir_all(&extract_dir).expect("Failed to create extract dir");
        std::fs::create_dir_all(&outside_dir).expect("Failed to create outside dir");
        symlink(&outside_dir, extract_dir.join("linked_outside"))
            .expect("Failed to create symlink");

        let archive_buffer = archive_with_file("linked_outside/secret.txt", b"secret");
        let result = extract_archive(Cursor::new(archive_buffer), &extract_dir).await;

        assert!(matches!(
            result,
            Err(ArchiveError::UnsafeArchivePath { .. })
        ));
        assert!(!outside_dir.join("secret.txt").exists());
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
        // Use consistent metadata content across both datasets (simulating atomic snapshot)
        let shared_metadata = b"shared_metadata_content_v1";
        std::fs::write(metadata_dir.join("catalog.db"), shared_metadata)
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

        // Dataset 2: same metadata content + data2
        // In a real scenario, both datasets would have identical metadata from the same
        // atomic snapshot, which is why the checksums should match.
        let metadata_dir2 = test_dir.path().join("ds2_metadata");
        let data2_dir = test_dir.path().join("ds2_data");
        std::fs::create_dir_all(&metadata_dir2).expect("Failed to create metadata2 dir");
        std::fs::create_dir_all(&data2_dir).expect("Failed to create data2 dir");
        // Use the SAME metadata content as dataset 1 (this is the expected case)
        std::fs::write(metadata_dir2.join("catalog.db"), shared_metadata)
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

        // Extract dataset 1 first
        extract_archive_with_options(
            Cursor::new(archive1_buffer.clone()),
            extract_dir.path(),
            ExtractOptions::skip_existing(),
        )
        .await?;

        // Verify dataset 1 content
        let catalog =
            std::fs::read(extract_dir.path().join("metadata/catalog.db")).expect("read catalog");
        assert_eq!(catalog, shared_metadata);
        let data1 = std::fs::read_to_string(extract_dir.path().join("data/data1.vortex"))
            .expect("read data1");
        assert_eq!(data1, "data1_content");

        // Extract dataset 2 with skip_if_exists - metadata should be verified and skipped
        extract_archive_with_options(
            Cursor::new(archive2_buffer),
            extract_dir.path(),
            ExtractOptions::skip_existing(),
        )
        .await?;

        // Verify metadata was NOT overwritten (still from dataset 1, verified via checksum)
        let catalog_after = std::fs::read(extract_dir.path().join("metadata/catalog.db"))
            .expect("read catalog after");
        assert_eq!(
            catalog_after, shared_metadata,
            "Metadata should not be overwritten when skip_if_exists is true"
        );

        // Verify dataset 2's data was extracted
        let data2 = std::fs::read_to_string(extract_dir.path().join("data/data2.vortex"))
            .expect("read data2");
        assert_eq!(data2, "data2_content");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_skip_if_exists_checksum_mismatch() -> Result<()> {
        // Test that checksum mismatch is detected when existing file differs from archive
        let test_dir = TempDir::new().expect("Failed to create temp dir");

        // Create original files
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file.txt"), b"original_content")
            .expect("Failed to write file");

        // Archive the original content
        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), "data/".to_string())];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        // Create extract directory with DIFFERENT content (simulating corruption)
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        std::fs::create_dir_all(extract_dir.path().join("data")).expect("create data dir");
        std::fs::write(
            extract_dir.path().join("data/file.txt"),
            b"corrupted_or_different_content", // Different from archive content
        )
        .expect("write pre-existing file");

        // Extract with skip_if_exists AND checksum verification - should FAIL
        let result = extract_archive_with_options(
            Cursor::new(archive_buffer),
            extract_dir.path(),
            ExtractOptions::skip_existing(), // This enables verification
        )
        .await;

        assert!(
            result.is_err(),
            "Expected checksum mismatch error when existing file differs from archive"
        );

        let error = result.expect_err("Should have error");
        let error_str = error.to_string();
        assert!(
            error_str.contains("Data integrity violation")
                || error_str.contains("ChecksumMismatch"),
            "Error should indicate checksum mismatch: {error_str}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_skip_no_verify() -> Result<()> {
        // Test that skip_existing_no_verify skips without checksum verification
        let test_dir = TempDir::new().expect("Failed to create temp dir");

        // Create files
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file.txt"), b"archive_content")
            .expect("Failed to write file");

        // Archive
        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir.clone(), "data/".to_string())];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        // Create extract directory with DIFFERENT content
        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        std::fs::create_dir_all(extract_dir.path().join("data")).expect("create data dir");
        std::fs::write(
            extract_dir.path().join("data/file.txt"),
            b"different_pre_existing_content",
        )
        .expect("write pre-existing file");

        // Extract with skip_if_exists but NO verification - should succeed (skip without checking)
        let result = extract_archive_with_options(
            Cursor::new(archive_buffer),
            extract_dir.path(),
            ExtractOptions::skip_existing_no_verify(),
        )
        .await;

        assert!(
            result.is_ok(),
            "Expected success when using skip_existing_no_verify: {result:?}"
        );

        // Verify original file was NOT overwritten (skipped)
        let content =
            std::fs::read_to_string(extract_dir.path().join("data/file.txt")).expect("read file");
        assert_eq!(
            content, "different_pre_existing_content",
            "File should be skipped, not overwritten"
        );

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

    #[tokio::test]
    async fn test_extract_removes_stale_temp_file() -> Result<()> {
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = test_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::write(data_dir.join("file.txt"), b"archive_content")
            .expect("Failed to write archive file");

        let mut archive_buffer = Vec::new();
        let dirs = vec![(data_dir, "data/".to_string())];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        let extract_dir = TempDir::new().expect("Failed to create extract dir");
        let extract_data_dir = extract_dir.path().join("data");
        std::fs::create_dir_all(&extract_data_dir).expect("create extract data dir");

        let dest_path = extract_data_dir.join("file.txt");
        std::fs::write(&dest_path, b"existing_content").expect("write existing destination");

        let temp_path = dest_path.with_extension(format!("{}.tmp", std::process::id()));
        std::fs::write(&temp_path, b"stale_temp_content").expect("write stale temp file");

        extract_archive(Cursor::new(archive_buffer), extract_dir.path()).await?;

        let content = std::fs::read_to_string(&dest_path).expect("read extracted file");
        assert_eq!(content, "archive_content");
        assert!(!temp_path.exists(), "stale temp file should be replaced");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_with_prefix_mapping() -> Result<()> {
        // Test that prefix_mappings correctly remaps archive paths to different target directories.
        // This is critical for Cayenne snapshot restore where the archive has "data/" prefix
        // but the actual data directory might be named after the dataset (e.g., "my_table").
        let test_dir = TempDir::new().expect("Failed to create temp dir");
        let metadata_dir = test_dir.path().join("metadata");
        let data_dir = test_dir.path().join("my_table"); // Different from archive prefix "data/"
        std::fs::create_dir_all(&metadata_dir).expect("Failed to create metadata dir");
        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");

        // Create test files
        std::fs::write(metadata_dir.join("catalog.db"), b"metadata content")
            .expect("Failed to write catalog");
        std::fs::write(data_dir.join("table.parquet"), b"table data")
            .expect("Failed to write table");

        // Archive with standard prefixes (metadata/ and data/)
        let mut archive_buffer = Vec::new();
        let dirs = vec![
            (metadata_dir.clone(), "metadata/".to_string()),
            (data_dir.clone(), "data/".to_string()), // Archive prefix is "data/" but dir is "my_table"
        ];
        archive_directories(&dirs, Cursor::new(&mut archive_buffer)).await?;

        // Extract to a new location with prefix mappings
        let extract_base = TempDir::new().expect("Failed to create extract dir");
        let extract_metadata = extract_base.path().join("metadata");
        let extract_data = extract_base.path().join("restored_table"); // Different name again

        // Create prefix mappings: archive "data/" -> extract to "restored_table/"
        let prefix_mappings = vec![
            ("metadata/".to_string(), extract_metadata.clone()),
            ("data/".to_string(), extract_data.clone()),
        ];

        let mut options = ExtractOptions::skip_existing();
        options.prefix_mappings = Some(prefix_mappings);

        extract_archive_with_options(
            Cursor::new(archive_buffer),
            extract_base.path(), // This is the fallback, but prefix mappings should override
            options,
        )
        .await?;

        // Verify files are extracted to the mapped directories, not the archive prefix paths
        assert!(
            extract_metadata.join("catalog.db").exists(),
            "metadata/catalog.db should be extracted to mapped metadata dir"
        );
        assert!(
            extract_data.join("table.parquet").exists(),
            "data/table.parquet should be extracted to mapped data dir (restored_table), not literal 'data/'"
        );

        // Verify the content is correct
        let catalog_content =
            std::fs::read_to_string(extract_metadata.join("catalog.db")).expect("read catalog");
        assert_eq!(catalog_content, "metadata content");

        let table_content =
            std::fs::read_to_string(extract_data.join("table.parquet")).expect("read table");
        assert_eq!(table_content, "table data");

        // Verify that without prefix mapping, files would go to wrong location
        // (This is what was broken before the fix)
        assert!(
            !extract_base.path().join("data/table.parquet").exists(),
            "data/table.parquet should NOT exist at literal archive path"
        );

        Ok(())
    }
}
