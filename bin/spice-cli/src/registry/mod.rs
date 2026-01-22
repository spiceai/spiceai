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

//! Registry implementations for fetching Spicepods from various sources.
//!
//! Supports:
//! - Local file system paths
//! - Spicerack.org (spicepod registry)

mod local_file;
mod spicerack;

use snafu::Snafu;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use local_file::LocalFileRegistry;
pub use spicerack::SpicerackRegistry;

/// Result type for registry operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can occur during registry operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// Spicepod not found at the specified path
    #[snafu(display("No Spicepod found at '{path}'"))]
    NotFound { path: String },

    /// Directory does not exist
    #[snafu(display("The Spicepod directory '{path}' does not exist"))]
    DirectoryNotFound { path: String },

    /// Not a valid Spicepod directory
    #[snafu(display(
        "The directory '{path}' does not contain a spicepod.yaml. Is it a valid Spicepod?"
    ))]
    InvalidSpicepod { path: String },

    /// IO error during registry operations
    #[snafu(display("Failed to {operation} '{path}': {source}"))]
    Io {
        operation: &'static str,
        path: String,
        source: std::io::Error,
    },

    /// HTTP error fetching from spicerack
    #[snafu(display("Failed to fetch Spicepod '{pod}' from spicerack.org: {message}"))]
    FetchFailed { pod: String, message: String },

    /// Zip extraction error
    #[snafu(display("Failed to extract Spicepod archive: {source}"))]
    ZipExtraction { source: zip::result::ZipError },
}

/// Fetch a Spicepod from the appropriate registry.
///
/// Automatically determines the registry based on the pod path:
/// - Local file registry for absolute paths, relative paths starting with `../`,
///   `file://` URLs, or existing local directories
/// - Spicerack registry for everything else
///
/// # Arguments
///
/// * `pod_path` - Path or identifier for the Spicepod
/// * `pods_dir` - Target directory for downloaded pods
/// * `headers` - Optional HTTP headers (for authenticated requests)
/// * `http_client` - HTTP client for making requests
///
/// # Returns
///
/// The path to the downloaded/copied Spicepod directory.
#[expect(clippy::implicit_hasher, reason = "HashMap is sufficient for this API")]
pub async fn get_pod(
    pod_path: &str,
    pods_dir: &Path,
    headers: &HashMap<String, String>,
    http_client: &reqwest::Client,
) -> Result<PathBuf> {
    if is_local_path(pod_path) {
        LocalFileRegistry
            .get_pod(pod_path, pods_dir, headers, http_client)
            .await
    } else {
        SpicerackRegistry
            .get_pod(pod_path, pods_dir, headers, http_client)
            .await
    }
}

/// Check if a pod path refers to a local file system path.
fn is_local_path(pod_path: &str) -> bool {
    pod_path.starts_with('/')
        || pod_path.starts_with("../")
        || pod_path.starts_with("file://")
        || std::path::Path::new(pod_path).exists()
}
