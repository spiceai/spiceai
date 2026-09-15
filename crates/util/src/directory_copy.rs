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

//! Recursive directory copying that rejects symbolic links.
//!
//! Used by snapshot and index staging paths that copy a directory tree onto local disk before
//! archiving or installing it: a symlink in the source tree could point outside it, so it is
//! always treated as a hard error rather than followed or silently skipped.

use std::path::Path;

/// Recursively copies every file and subdirectory from `source` into `destination`.
///
/// `destination` (and any subdirectory of it) is created if missing. A symbolic link anywhere
/// under `source` is a hard error rather than being followed or skipped, since it could point
/// outside `source`. `link_description` names what is being copied, so the error can say what
/// contained the symlink (e.g. `"index directory"`, `"snapshot index"`).
///
/// # Errors
///
/// Returns an error if a filesystem operation (reading a directory, reading metadata, copying a
/// file) fails, or if a symbolic link is found anywhere under `source`.
pub fn copy_directory_rejecting_symlinks(
    source: &Path,
    destination: &Path,
    link_description: &str,
) -> std::io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&source_path)?;
        if metadata.file_type().is_symlink() {
            return Err(std::io::Error::other(format!(
                "{link_description} contains a symbolic link"
            )));
        }
        if metadata.is_dir() {
            copy_directory_rejecting_symlinks(&source_path, &destination_path, link_description)?;
        } else if metadata.is_file() {
            std::fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}
