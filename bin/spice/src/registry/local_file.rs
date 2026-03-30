/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Local file system registry for Spicepods.

use super::{Error, IoSnafu, Result};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Registry that fetches Spicepods from the local file system.
pub struct LocalFileRegistry;

impl LocalFileRegistry {
    #[expect(
        clippy::unused_async,
        reason = "Async for API consistency with SpicerackRegistry"
    )]
    pub async fn get_pod(
        &self,
        pod_path: &str,
        pods_dir: &Path,
        _headers: &HashMap<String, String>,
        _http_client: &reqwest::Client,
    ) -> Result<PathBuf> {
        // Handle file:// URLs
        let path_str = pod_path.strip_prefix("file://").unwrap_or(pod_path);
        let source_path = Path::new(path_str);

        // Check if source exists
        let metadata = std::fs::metadata(source_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::DirectoryNotFound {
                    path: pod_path.to_string(),
                }
            } else {
                Error::Io {
                    operation: "read",
                    path: pod_path.to_string(),
                    source: e,
                }
            }
        })?;

        if !metadata.is_dir() {
            return Err(Error::InvalidSpicepod {
                path: pod_path.to_string(),
            });
        }

        // Get absolute path
        let source_path = if source_path.is_absolute() {
            source_path.to_path_buf()
        } else {
            std::fs::canonicalize(source_path).context(IoSnafu {
                operation: "canonicalize",
                path: pod_path,
            })?
        };

        // Get pod name from directory name
        let pod_name = source_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_lowercase();

        // Check for spicepod.yaml in the source directory
        let manifest_name = format!("{pod_name}.yaml");
        let source_manifest = source_path.join(&manifest_name);
        if !source_manifest.exists() {
            // Also check for spicepod.yaml (generic name)
            let generic_manifest = source_path.join("spicepod.yaml");
            if !generic_manifest.exists() {
                return Err(Error::InvalidSpicepod {
                    path: source_path.display().to_string(),
                });
            }
        }

        // Create destination directory
        std::fs::create_dir_all(pods_dir).context(IoSnafu {
            operation: "create directory",
            path: pods_dir.display().to_string(),
        })?;

        // Copy all files from source to pods_dir
        copy_dir_recursive(&source_path, pods_dir)?;

        // Return path to the manifest in pods_dir
        let dest_manifest = pods_dir.join(&manifest_name);
        Ok(dest_manifest)
    }
}

/// Recursively copy a directory and its contents.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    for entry in std::fs::read_dir(src).context(IoSnafu {
        operation: "read directory",
        path: src.display().to_string(),
    })? {
        let entry = entry.context(IoSnafu {
            operation: "read directory entry",
            path: src.display().to_string(),
        })?;

        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if entry
            .file_type()
            .context(IoSnafu {
                operation: "get file type",
                path: src_path.display().to_string(),
            })?
            .is_dir()
        {
            std::fs::create_dir_all(&dst_path).context(IoSnafu {
                operation: "create directory",
                path: dst_path.display().to_string(),
            })?;
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path).context(IoSnafu {
                operation: "copy file",
                path: src_path.display().to_string(),
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_file_url_strip() {
        let path = "file:///path/to/pod";
        let stripped = path.strip_prefix("file://").unwrap_or(path);
        assert_eq!(stripped, "/path/to/pod");
    }
}
