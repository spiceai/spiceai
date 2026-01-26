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

//! Spicerack.org registry for Spicepods.

use super::{Error, Result, ZipExtractionSnafu};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

/// Base URL for spicerack API.
fn get_spicerack_base_url() -> String {
    if let Ok(url) = std::env::var("SPICERACK_BASE_URL") {
        return url;
    }

    let version = env!("CARGO_PKG_VERSION");
    if version.ends_with("-dev") || version.ends_with("-unstable") {
        "https://dev-data.spiceai.io/v1".to_string()
    } else {
        "https://api.spicerack.org/v1".to_string()
    }
}

/// Registry that fetches Spicepods from spicerack.org.
pub struct SpicerackRegistry;

impl SpicerackRegistry {
    pub async fn get_pod(
        &self,
        pod_full_path: &str,
        pods_dir: &Path,
        headers: &HashMap<String, String>,
        http_client: &reqwest::Client,
    ) -> Result<std::path::PathBuf> {
        // Parse pod path and optional version (e.g., "spiceai/quickstart@v1.0")
        let (pod_path, pod_version) = if let Some(idx) = pod_full_path.find('@') {
            let (path, version) = pod_full_path.split_at(idx);
            (path, Some(&version[1..])) // Skip the '@'
        } else {
            (pod_full_path, None)
        };

        // Build URL
        let base_url = get_spicerack_base_url();
        let url = match pod_version {
            Some(version) => format!("{base_url}/spicepods/{pod_path}/{version}"),
            None => format!("{base_url}/spicepods/{pod_path}"),
        };

        // Make request
        let mut request = http_client.get(&url).header("Accept", "application/zip");

        for (key, value) in headers {
            request = request.header(key, value);
        }

        let response = request.send().await.map_err(|e| Error::FetchFailed {
            pod: pod_full_path.to_string(),
            message: e.to_string(),
        })?;

        // Check response status
        let status = response.status();
        if status.as_u16() == 404 {
            return Err(Error::NotFound {
                path: pod_full_path.to_string(),
            });
        }

        if !status.is_success() {
            return Err(Error::FetchFailed {
                pod: pod_full_path.to_string(),
                message: format!("HTTP {status}"),
            });
        }

        // Download to temp file
        let bytes = response.bytes().await.map_err(|e| Error::FetchFailed {
            pod: pod_full_path.to_string(),
            message: e.to_string(),
        })?;

        let mut temp_file = tempfile::NamedTempFile::new().map_err(|e| Error::Io {
            operation: "create temp file",
            path: "tempfile".to_string(),
            source: e,
        })?;

        temp_file.write_all(&bytes).map_err(|e| Error::Io {
            operation: "write temp file",
            path: "tempfile".to_string(),
            source: e,
        })?;

        // Create destination directory
        let dest_dir = pods_dir.join(pod_path);
        std::fs::create_dir_all(&dest_dir).map_err(|e| Error::Io {
            operation: "create directory",
            path: dest_dir.display().to_string(),
            source: e,
        })?;

        // Extract zip
        let file = std::fs::File::open(temp_file.path()).map_err(|e| Error::Io {
            operation: "open temp file",
            path: temp_file.path().display().to_string(),
            source: e,
        })?;

        let mut archive = zip::ZipArchive::new(file).context(ZipExtractionSnafu)?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).context(ZipExtractionSnafu)?;

            // Sanitize path to prevent traversal attacks
            let file_name = match file.enclosed_name() {
                Some(name) => name.clone(),
                None => continue, // Skip files with invalid paths
            };

            let dest_path = dest_dir.join(&file_name);

            // Ensure destination is within dest_dir
            if !dest_path.starts_with(&dest_dir) {
                continue; // Skip files that would escape the destination
            }

            if file.is_dir() {
                std::fs::create_dir_all(&dest_path).map_err(|e| Error::Io {
                    operation: "create directory",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;
            } else {
                // Ensure parent directory exists
                if let Some(parent) = dest_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| Error::Io {
                        operation: "create directory",
                        path: parent.display().to_string(),
                        source: e,
                    })?;
                }

                let mut outfile = std::fs::File::create(&dest_path).map_err(|e| Error::Io {
                    operation: "create file",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;

                let mut contents = Vec::new();
                file.read_to_end(&mut contents).map_err(|e| Error::Io {
                    operation: "read from archive",
                    path: file_name.display().to_string(),
                    source: e,
                })?;

                outfile.write_all(&contents).map_err(|e| Error::Io {
                    operation: "write file",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;
            }
        }

        Ok(dest_dir)
    }
}
